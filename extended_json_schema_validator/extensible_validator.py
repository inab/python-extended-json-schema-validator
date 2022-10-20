#!/usr/bin/env python
# -*- coding: utf-8 -*-

import abc
import copy
import hashlib
import json
import logging
import os
from typing import NamedTuple, TYPE_CHECKING, cast

import jsonschema as JSV
import uritools  # type: ignore[import]
import yaml

from .extend_validator_helpers import (
	PLAIN_VALIDATOR_MAPPER,
	REF_FEATURE,
	export_resolved_references,
	extendValidator,
	flattenTraverseListSet,
	traverseJSONSchema,
)
from .extensions.fk_check import ForeignKey
from .extensions.pk_check import PrimaryKey

# Augmenting the supported types
from .extensions.unique_check import UniqueKey

if TYPE_CHECKING:
	from typing import (
		Any,
		ClassVar,
		Iterator,
		Mapping,
		MutableMapping,
		MutableSequence,
		Optional,
		Sequence,
		Tuple,
		Type,
		Union,
	)

	from typing_extensions import Protocol, TypedDict, runtime_checkable

	from .extend_validator_helpers import (
		CustomTypeCheckerCallable,
		RefSchemaListSet,
	)
	from .extensions.abstract_check import (
		AbstractCustomFeatureValidator,
		BootstrapErrorDict,
		CheckContext,
		Id2ElemId,
		JsonPointer2Val,
		KeyRefs,
		RefSchemaTuple,
		SchemaHashEntry,
		SecondPassErrorDict,
	)

	ExtensibleValidatorConfig = Mapping[str, Any]

	@runtime_checkable
	class CustomFormatProtocol(Protocol):
		FormatName: str = "(unset)"

		@classmethod
		@abc.abstractmethod
		def IsCorrectFormat(cls, value: Any, schema: Optional[Any] = None) -> bool:
			pass

	class ParsedContentEntry(TypedDict, total=False):
		file: str
		json: Any
		schema_hash: str
		schema_id: str
		errors: MutableSequence[BootstrapErrorDict]


class LoadedSchemasStats(NamedTuple):
	numFileOK: int = 0
	numDirOK: int = 0
	numFileIgnore: int = 0
	numFileFail: int = 0
	numDirFail: int = 0
	numSchemaConsistent: int = 0
	numSchemaInconsistent: int = 0


class ExtensibleValidator(object):
	CustomBaseValidators: "ClassVar[Mapping[Optional[str], Sequence[Type[AbstractCustomFeatureValidator]]]]" = {
		None: [UniqueKey, PrimaryKey, ForeignKey]
	}

	SCHEMA_KEY = "$schema"
	ALT_SCHEMA_KEYS: "Sequence[str]" = ["@schema", "_schema", SCHEMA_KEY]

	def __init__(
		self,
		customFormats: "Sequence[CustomFormatProtocol]" = [],
		customTypes: "Mapping[str, CustomTypeCheckerCallable]" = {},
		customValidators: "Mapping[Optional[str], Sequence[Type[AbstractCustomFeatureValidator]]]" = CustomBaseValidators,
		config: "ExtensibleValidatorConfig" = {},
		jsonRootTag: "Optional[str]" = None,
		isRW: bool = True,
	):
		self.logger = logging.getLogger(self.__class__.__name__)

		self.schemaHash: "MutableMapping[str, SchemaHashEntry]" = {}
		self.refSchemaCache: "JsonPointer2Val" = {}
		self.refSchemaSet: "MutableMapping[str, RefSchemaTuple]" = {}
		self.customFormatCheckerInstance = JSV.FormatChecker()

		# Registering the custom formats, in order to use them
		for customFormat in customFormats:
			self.customFormatCheckerInstance.checks(customFormat.FormatName)(
				customFormat.IsCorrectFormat
			)

		self.customTypes = customTypes
		self.customValidators = customValidators
		self.config = config
		self.jsonRootTag = jsonRootTag
		self.isRW = isRW
		self.doNotValidateNoId = not bool(config.get("validate-no-id", True))

	def _loadAndCacheJsonSchemas(
		self,
		args: "Sequence[Union[str, SchemaHashEntry]]",
		logLevel: int = logging.DEBUG,
		refSchemaCache: "JsonPointer2Val" = {},
		refSchemaSet: "MutableMapping[str, RefSchemaTuple]" = {},
	) -> "Tuple[Sequence[SchemaHashEntry], JsonPointer2Val, MutableMapping[str, RefSchemaTuple], LoadedSchemasStats]":
		# Schema validation stats
		numDirOK = 0
		numDirFail = 0
		numFileIgnore = 0
		numFileFail = 0

		self.logger.log(logLevel, "PASS 0.a: JSON schema loading and cache generation")
		jsonSchemaPossibles = list(args)
		jsonSchemaNext = []
		refSchemaFile: "MutableMapping[str, str]" = {}
		inlineCounter = 0
		for jsonSchemaPossible in jsonSchemaPossibles:
			# schemaObj = None

			if isinstance(jsonSchemaPossible, dict):
				schemaObj: "SchemaHashEntry" = jsonSchemaPossible
				errors = schemaObj.get("errors")
				if errors is None:
					self.logger.log(
						logLevel,
						"\tIGNORE: cached schema does not have the mandatory 'errors' attribute, so it cannot be processed",
					)
					numFileIgnore += 1
					continue

				jsonSchema = schemaObj.get("schema")
				if jsonSchema is None:
					self.logger.log(
						logLevel,
						"\tIGNORE: cached schema does not have the mandatory 'schema' attribute, so it cannot be processed",
					)
					errors.append(
						{
							"reason": "unexpected",
							"description": "The cached schema is missing",
						}
					)
					numFileIgnore += 1
					continue

				schemaObj["schema_hash"] = self.GetNormalizedJSONHash(jsonSchema)

				if "file" not in schemaObj:
					schemaObj["file"] = "(inline schema {})".format(inlineCounter)
					inlineCounter += 1
				jsonSchemaFile = schemaObj["file"]
			elif os.path.isdir(jsonSchemaPossible):
				jsonSchemaDir = jsonSchemaPossible
				# It's a possible JSON Schema directory, not a JSON Schema file
				try:
					for relJsonSchemaFile in os.listdir(jsonSchemaDir):
						if relJsonSchemaFile[0] == ".":
							continue

						newJsonSchemaFile = os.path.join(
							jsonSchemaDir, relJsonSchemaFile
						)
						if (
							os.path.isdir(newJsonSchemaFile)
							or ".json" in relJsonSchemaFile
							or ".yaml" in relJsonSchemaFile
						):
							jsonSchemaPossibles.append(newJsonSchemaFile)
					numDirOK += 1
				except IOError as ioe:
					self.logger.critical(
						"FATAL ERROR: Unable to open JSON schema directory {0}. Reason: {1}".format(
							jsonSchemaDir, ioe.strerror
						)
					)
					numDirFail += 1

				continue
			else:
				jsonSchemaFile = jsonSchemaPossible
				self.logger.log(logLevel, "* Loading schema {0}".format(jsonSchemaFile))
				try:
					try:
						with open(
							jsonSchemaFile, mode="r", encoding="utf-8"
						) as sHandle:
							jsonSchema = json.load(sHandle)
					except json.decoder.JSONDecodeError as jde:
						with open(
							jsonSchemaFile, mode="r", encoding="utf-8"
						) as sHandle:
							try:
								jsonSchema = yaml.safe_load(sHandle)
							except yaml.error.MarkedYAMLError as mye:
								self.logger.critical(
									"\t- ERROR: Unable to parse schema file {0} neither as JSON or YAML. Reasons: {1} {2}".format(
										jsonSchemaFile, str(jde), str(mye)
									)
								)
								numFileFail += 1
								continue
				except IOError as ioe:
					self.logger.critical(
						"FATAL ERROR: Unable to open/read schema file {0}. Reason: {1}".format(
							jsonSchemaFile, ioe.strerror
						)
					)
					numFileFail += 1
					continue
				else:
					errors = []
					schemaObj = {
						"schema": jsonSchema,
						"schema_hash": self.GetNormalizedJSONHash(jsonSchema),
						"file": jsonSchemaFile,
						"errors": errors,
					}

			schemaValId = jsonSchema.get(self.SCHEMA_KEY)
			if schemaValId is None:
				self.logger.log(
					logLevel,
					"\tIGNORE: {0} does not have the mandatory '{1}' attribute, so it cannot be validated".format(
						jsonSchemaFile, self.SCHEMA_KEY
					),
				)
				errors.append(
					{
						"reason": "no_schema",
						"description": "JSON Schema attribute '{}' is missing".format(
							self.SCHEMA_KEY
						),
					}
				)
				numFileIgnore += 1
				continue

			if PLAIN_VALIDATOR_MAPPER.get(schemaValId) is None:
				self.logger.log(
					logLevel,
					"\tIGNORE/FIXME: The JSON Schema id {0} is not being acknowledged by this validator".format(
						schemaValId
					),
				)
				errors.append(
					{
						"reason": "schema_unknown",
						"description": "'$schema' id {0} is not being acknowledged by this validator".format(
							schemaValId
						),
					}
				)
				numFileIgnore += 1
				continue

			# Getting the JSON Schema URI, needed by this
			idKey = "$id" if "$id" in jsonSchema else "id"
			jsonSchemaURI = jsonSchema.get(idKey)
			if jsonSchemaURI is not None:
				schemaObj["id_key"] = idKey
				if jsonSchemaURI in refSchemaFile:
					self.logger.error(
						"\tERROR: schema in {0} and schema in {1} have the same id".format(
							jsonSchemaFile, refSchemaFile[jsonSchemaURI]
						)
					)
					errors.append(
						{
							"reason": "dup_id",
							"description": "schema in {0} and schema in {1} have the same id".format(
								jsonSchemaFile, refSchemaFile[jsonSchemaURI]
							),
						}
					)
					numFileFail += 1
					continue
				else:
					refSchemaCache[jsonSchemaURI] = jsonSchema
					refSchemaFile[jsonSchemaURI] = jsonSchemaFile
			else:
				numFileIgnore += 1
				self.logger.log(
					logLevel,
					"\tIGNORE: Schema in {0} has no id attribute".format(
						jsonSchemaFile
					),
				)
				if self.doNotValidateNoId:
					errors.append(
						{
							"reason": "no_id",
							"description": "JSON Schema attributes '$id' (Draft06 onward) and 'id' (Draft04) are missing in {}".format(
								jsonSchemaFile
							),
						}
					)
					numFileIgnore += 1
					continue

			# We need to store these before creating the validators
			# in order to build the RefSchema cache
			jsonSchemaNext.append(schemaObj)

		loadedSchemasStats = LoadedSchemasStats(
			numDirOK=numDirOK,
			numFileIgnore=numFileIgnore,
			numFileFail=numFileFail,
			numDirFail=numDirFail,
		)

		return jsonSchemaNext, refSchemaCache, refSchemaSet, loadedSchemasStats

	def _validateJSONSchemas(
		self,
		jsonSchemaNext: "Sequence[SchemaHashEntry]",
		refSchemaCache: "JsonPointer2Val",
		logLevel: int = logging.DEBUG,
	) -> "Tuple[RefSchemaListSet, LoadedSchemasStats]":
		p_schemaHash = self.schemaHash
		numFileOK = 0
		numFileIgnore = 0
		numFileFail = 0

		self.logger.log(logLevel, "PASS 0.b: JSON schema validation")

		refSchemaListSet: "RefSchemaListSet" = {}
		for schemaObj in jsonSchemaNext:
			jsonSchema = schemaObj["schema"]
			jsonSchemaFile = schemaObj["file"]
			errors = schemaObj["errors"]

			# Errors related to these are captured in the previous loop
			schemaValId = jsonSchema.get(self.SCHEMA_KEY)
			plain_validator = PLAIN_VALIDATOR_MAPPER.get(schemaValId)
			assert plain_validator is not None

			# Getting the JSON Schema URI, needed by this
			idKey = schemaObj["id_key"]
			jsonSchemaURI = jsonSchema.get(idKey)

			validator, customFormatInstances = extendValidator(
				jsonSchemaURI,
				plain_validator,
				self.customTypes,
				self.customValidators,
				config=self.config,
				jsonSchemaSource=jsonSchemaFile,
				isRW=self.isRW,
			)

			schemaObj["customFormatInstances"] = customFormatInstances
			schemaObj["validator"] = validator

			# Validate the extended JSON schema properly
			metaSchema = validator.META_SCHEMA
			if len(customFormatInstances) > 0:
				metaSchema = metaSchema.copy()
				metaSchema["properties"] = metaProps = metaSchema["properties"].copy()

				for customFormatInstance in customFormatInstances:
					for kF, vF in customFormatInstance.triggerJSONSchemaDef.items():
						if kF in metaProps:
							# Multiple declarations
							vM = metaProps[kF].copy()
							if "anyOf" not in vM:
								newDecl = {"anyOf": [vM]}
								vM = metaProps[kF] = newDecl
							else:
								metaProps[kF] = vM

							vM["anyOf"].append(vF)
						else:
							metaProps[kF] = vF

			# We need to shadow the original schema
			id_prop = "$id" if "$id" in metaSchema else "id"
			localRefSchemaCache = copy.copy(refSchemaCache)
			localRefSchemaCache[metaSchema[id_prop]] = metaSchema
			if metaSchema[id_prop][-1] == "#":
				localRefSchemaCache[metaSchema[id_prop][0:-1]] = metaSchema
			cachedSchemasResolver = JSV.RefResolver(
				base_uri=jsonSchemaURI, referrer=jsonSchema, store=localRefSchemaCache
			)

			valErrors = [
				valError
				for valError in validator(
					metaSchema, resolver=cachedSchemasResolver
				).iter_errors(jsonSchema)
			]
			if len(valErrors) > 0:
				self.logger.error(
					"\t- ERRORS:\n"
					+ "\n".join(
						map(
							lambda se: "\t\tPath: {0} . Message: {1}".format(
								"/" + "/".join(map(lambda e: str(e), se.path)),
								se.message,
							),
							valErrors,
						)
					)
				)
				for valError in valErrors:
					errors.append(
						{
							"reason": "schema_error",
							"description": "Path: {0} . Message: {1}".format(
								"/" + "/".join(map(lambda e: str(e), valError.path)),
								valError.message,
							),
						}
					)
				numFileFail += 1
			elif jsonSchemaURI is not None:
				# Getting the JSON Pointer object instance of the augmented schema
				# my $jsonSchemaP = $v->schema($jsonSchema)->schema;
				# This step is done, so we fetch a complete schema
				# $jsonSchema = $jsonSchemaP->data;

				if jsonSchemaURI in p_schemaHash:
					self.logger.error(
						"\tERROR: validated, but schema in {0} and schema in {1} have the same id".format(
							jsonSchemaFile, p_schemaHash[jsonSchemaURI]["file"]
						)
					)
					errors.append(
						{
							"reason": "dup_id",
							"description": "JSON Schema validated, but schema in {0} and schema in {1} have the same id".format(
								jsonSchemaFile, p_schemaHash[jsonSchemaURI]["file"]
							),
						}
					)
					numFileFail += 1
				else:
					self.logger.log(logLevel, "\t- Validated {0}".format(jsonSchemaURI))

					# Reverse mappings, needed later
					triggeringFeatures: "MutableMapping[str, AbstractCustomFeatureValidator]" = (
						{}
					)
					for cFI in customFormatInstances:
						for triggerAttribute, _ in cFI.getValidators():
							triggeringFeatures[triggerAttribute] = cFI

					traverseJSONSchema(
						jsonSchema,
						schemaURI=jsonSchemaURI,
						keys=triggeringFeatures,
						refSchemaListSet=refSchemaListSet,
					)

					schemaObj["ref_resolver"] = cachedSchemasResolver
					p_schemaHash[jsonSchemaURI] = schemaObj
					numFileOK += 1
			else:
				# This is here to capture cases where we wanted to validate an
				# unidentified schema for its correctness
				self.logger.log(
					logLevel,
					"\tIGNORE: validated, but schema in {0} has no id attribute".format(
						jsonSchemaFile
					),
				)
				errors.append(
					{
						"reason": "no_id",
						"description": "JSON Schema attributes '$id' (Draft06 onward) and 'id' (Draft04) are missing",
					}
				)
				numFileIgnore += 1

		loadedSchemasStats = LoadedSchemasStats(
			numFileOK=numFileOK,
			numFileIgnore=numFileIgnore,
			numFileFail=numFileFail,
		)

		return refSchemaListSet, loadedSchemasStats

	def loadJSONSchemasExt(
		self, *args: "Union[str, SchemaHashEntry]", verbose: "Optional[bool]" = None
	) -> LoadedSchemasStats:
		p_schemaHash = self.schemaHash
		# Schema validation stats
		numDirOK = 0
		numDirFail = 0
		numFileOK = 0
		numFileIgnore = 0
		numFileFail = 0

		if verbose:
			logLevel = logging.INFO
		else:
			logLevel = logging.DEBUG

		# Pass 0.a
		(
			jsonSchemaNext,
			refSchemaCache,
			refSchemaSet,
			initialStats,
		) = self._loadAndCacheJsonSchemas(args=args, logLevel=logLevel)
		self.refSchemaCache = refSchemaCache
		self.refSchemaSet = refSchemaSet
		numDirOK = initialStats.numDirOK
		numDirFail = initialStats.numDirFail
		numFileIgnore = initialStats.numFileIgnore
		numFileFail = initialStats.numFileFail

		# Pass 0.b
		refSchemaListSet, valStats = self._validateJSONSchemas(
			jsonSchemaNext, refSchemaCache, logLevel=logLevel
		)
		numFileIgnore += valStats.numFileIgnore
		numFileFail += valStats.numFileFail
		numFileOK += valStats.numFileOK

		self.logger.log(
			logLevel,
			"SCHEMA VALIDATION STATS: loaded {0} schemas from {1} directories, ignored {2} schemas, failed {3} schemas and {4} directories".format(
				numFileOK, numDirOK, numFileIgnore, numFileFail, numDirFail
			),
		)

		self.logger.log(logLevel, "PASS 0.c: JSON schema set consistency checks")

		# Circular references check is based on having two levels
		# one unmodified, another being built from the first, taking
		# into account already visited schemas
		refSchemaSetBase = {}
		for jsonSchemaURI, traverseListSet in refSchemaListSet.items():
			# Time to implode each one of the elements from refSchemaListSet
			# for further usage
			refSchemaSetBase[jsonSchemaURI] = flattenTraverseListSet(traverseListSet)

		for jsonSchemaURI, jsonSchemaSet in refSchemaSetBase.items():
			id2ElemId, keyRefs, jp2val = jsonSchemaSet

			# referenced schemas id2ElemId and keyRefs
			if REF_FEATURE in keyRefs:
				# Unlinking references on keyRefs
				keyRefs_augmented: "KeyRefs" = {}
				for featName, featList in keyRefs.items():
					keyRefs_augmented[featName] = list(featList)

				# Unlinking references on id2ElemId
				id2ElemId_augmented: "Id2ElemId" = {}
				for i2e_k, featDict in id2ElemId.items():
					id2ElemId_augmented[i2e_k] = {}
					for featName, l_uniqId in featDict.items():
						id2ElemId_augmented[i2e_k][featName] = list(l_uniqId)

				# And on the $ref case
				refList = keyRefs_augmented[REF_FEATURE]

				# Initializing the visitedURIs through
				# $ref fetching
				visitedURIs = set([jsonSchemaURI])

				# This $ref list can be increased through the process
				for fLoc in refList:
					theRef = fLoc.context[REF_FEATURE]
					# Computing the absolute schema URI
					if uritools.isabsuri(jsonSchemaURI):
						abs_ref_schema_id, _ = uritools.uridefrag(
							uritools.urijoin(jsonSchemaURI, theRef)
						)
					else:
						abs_ref_schema_id, _ = uritools.uridefrag(
							uritools.urijoin(jsonSchemaURI, theRef)
						)

					# Circular references detection check
					if abs_ref_schema_id in visitedURIs:
						continue

					visitedURIs.add(abs_ref_schema_id)

					# Now, time to get the referenced, gathered data
					refSet = refSchemaSetBase.get(abs_ref_schema_id)
					if refSet is not None:
						ref_id2ElemId, ref_keyRefs, ref_jp2val = refSet

						# TODO: properly augment refSchemaSet id2ElemId and keyRefs with
						# This is needed to have a proper bootstrap

						for ref_pAddr_k, ref_pAddr_v in ref_id2ElemId.items():
							featDict = id2ElemId_augmented.setdefault(ref_pAddr_k, {})
							for ref_feat_k, ref_feat_v in ref_pAddr_v.items():
								featDict.setdefault(ref_feat_k, []).extend(ref_feat_v)

						for ref_kR_k, ref_kR_v in ref_keyRefs.items():
							keyRefs_augmented.setdefault(ref_kR_k, []).extend(ref_kR_v)
					else:
						# TODO: error handling
						self.logger.critical("UNHANDLED ERROR")

				# Recomposing the tuple
				jsonSchemaSet = (id2ElemId_augmented, keyRefs_augmented, jp2val)

			refSchemaSet[jsonSchemaURI] = jsonSchemaSet

		# Last, bootstrapping the extensions
		# Now, we check whether the declared foreign keys are pointing to loaded JSON schemas
		numSchemaConsistent = 0
		numSchemaInconsistent = 0
		for jsonSchemaURI, p_schema in p_schemaHash.items():
			jsonSchemaFile = p_schema["file"]
			self.logger.log(logLevel, "* Checking {0}".format(jsonSchemaFile))
			customFormatInstances = p_schema["customFormatInstances"]
			isValid = True
			if len(customFormatInstances) > 0:
				(id2ElemId, keyRefs, jp2val) = refSchemaSet[jsonSchemaURI]

				for cFI in customFormatInstances:
					if cFI.needsBootstrapping:
						doBootstrap = False
						for triggerAttribute, _ in cFI.getValidators():
							if triggerAttribute in keyRefs:
								doBootstrap = True
								break

						if doBootstrap:
							# Bootstrapping the schema
							# By default this is a no-op
							b_errors = cFI.bootstrap(
								refSchemaTuple=(id2ElemId, keyRefs, self.refSchemaCache)
							)
							if b_errors:
								for error in b_errors:
									self.logger.error(
										"\t- ERROR: {}".format(error["description"])
									)

								p_schema["errors"].extend(b_errors)
								isValid = False

			if isValid:
				self.logger.log(logLevel, "\t- Consistent!")
				numSchemaConsistent += 1
			else:
				numSchemaInconsistent += 1

		self.logger.log(
			logLevel,
			"SCHEMA CONSISTENCY STATS: {0} schemas right, {1} with inconsistencies".format(
				numSchemaConsistent, numSchemaInconsistent
			),
		)

		return LoadedSchemasStats(
			numFileOK=numFileOK,
			numDirOK=numDirOK,
			numFileIgnore=numFileIgnore,
			numFileFail=numFileFail,
			numDirFail=numDirFail,
			numSchemaConsistent=numSchemaConsistent,
			numSchemaInconsistent=numSchemaInconsistent,
		)

	def loadJSONSchemas(
		self, *args: "Union[str, SchemaHashEntry]", verbose: "Optional[bool]" = None
	) -> int:
		l_stats = self.loadJSONSchemasExt(verbose=verbose, *args)

		return len(self.getValidSchemas().keys())

	def getValidSchemas(
		self, do_resolve: bool = False
	) -> "Mapping[str, SchemaHashEntry]":
		if do_resolve:
			for jsonSchemaURI, schemaObj in self.schemaHash.items():
				if "resolved_schema" not in schemaObj:
					resolvedSchema = export_resolved_references(
						jsonSchemaURI, schemaObj["schema"], self.schemaHash
					)
					schemaObj["resolved_schema"] = resolvedSchema

		return self.schemaHash

	def getRefSchemaSet(self) -> "Mapping[str, RefSchemaTuple]":
		return self.refSchemaSet

	# This method invalidates the different cached elements as much
	# as possible,
	def invalidateCaches(self) -> None:
		p_schemasObj = self.getValidSchemas()

		for schemaObj in p_schemasObj.values():
			dynSchemaVal = schemaObj["customFormatInstances"]
			for dynVal in dynSchemaVal:
				dynVal.invalidateCaches()

	# This method warms up the different cached elements as much
	# as possible,
	def warmUpCaches(
		self,
		dynValList: "Optional[Sequence[AbstractCustomFeatureValidator]]" = None,
		verbose: "Optional[bool]" = None,
	) -> None:
		if not dynValList:
			newDynValList: "MutableSequence[AbstractCustomFeatureValidator]" = []
			p_schemasObj = self.getValidSchemas()

			for schemaObj in p_schemasObj.values():
				newDynValList.extend(schemaObj["customFormatInstances"])

			dynValList = newDynValList

		for dynVal in dynValList:
			dynVal.warmUpCaches()

	def doSecondPass(
		self,
		dynValList: "Sequence[AbstractCustomFeatureValidator]",
		verbose: "Optional[bool]" = None,
	) -> "Tuple[int, int, Mapping[str, Sequence[SecondPassErrorDict]]]":
		secondPassOK = 0
		secondPassFails = 0
		secondPassErrors: "MutableMapping[str, MutableSequence[SecondPassErrorDict]]" = (
			{}
		)

		# First, gather the list of contexts
		gatheredContexts: "MutableMapping[str, MutableSequence[CheckContext]]" = {}
		for dynVal in dynValList:
			dynContext = dynVal.getContext()
			if dynContext is not None:
				gatheredContexts.setdefault(dynVal.__class__.__name__, []).append(
					dynContext
				)

		# We have to run this even when there is no gathered context
		# because there could be validators wanting to complain
		secondPassProcessed = set()
		secondPassFailed = set()
		for dynVal in dynValList:
			processed, failed, errors = dynVal.doSecondPass(gatheredContexts)
			secondPassProcessed.update(processed)
			secondPassFailed.update(failed)
			for error in errors:
				secondPassErrors.setdefault(error["file"], []).append(error)

		secondPassFails = len(secondPassFailed)
		secondPassOK = len(secondPassProcessed) - secondPassFails

		return secondPassOK, secondPassFails, secondPassErrors

	def _resetDynamicValidators(
		self,
		dynValList: "Sequence[AbstractCustomFeatureValidator]",
		verbose: "Optional[bool]" = None,
	) -> None:
		for dynVal in dynValList:
			dynVal.cleanup()

	@classmethod
	def GetNormalizedJSONHash(cls, json_data: "Any") -> "str":
		# First, we serialize it in a reproducible way
		json_canon = json.dumps(
			json_data, sort_keys=True, indent=None, separators=(",", ":")
		)

		return hashlib.sha1(json_canon.encode("utf-8")).hexdigest()

	def jsonValidateIter(
		self, *args: "Union[str, ParsedContentEntry]", verbose: "Optional[bool]" = None
	) -> "Iterator[Any]":
		"""
		This method validates a given list of JSON contents.
		These contents can be either already in memory, or
		files. The in memory contents are dictionaries with
		three keys: `json`, `errors` and `file`. The first
		one will contain the content to be validated, the second
		one is an array of errors (originally empty) and the last
		is a symbolic name, which could be either a real filename
		or other meaning.

		It returns an iterator of dictionaries, each one
		corresponding to each validated input, which will have
		the very same `json`, `errors` and `file` keys, already
		described above.
		"""
		p_schemaHash = self.schemaHash

		if verbose:
			logLevel = logging.INFO
		else:
			logLevel = logging.DEBUG

		# JSON validation stats
		numDirOK = 0
		numDirFail = 0
		numFilePass1OK = 0
		numFilePass1Ignore = 0
		numFilePass1Fail = 0
		numFilePass2OK = 0
		numFilePass2Fail = 0

		dynSchemaSet = set()
		dynSchemaValList: "MutableSequence[AbstractCustomFeatureValidator]" = []

		# This step is needed for cases where external sources can populate
		# the structures used to validate, like the list of primary keys
		for jsonSchemaId, schemaObj in p_schemaHash.items():
			if jsonSchemaId not in dynSchemaSet:
				dynSchemaSet.add(jsonSchemaId)
				localDynSchemaVal = schemaObj["customFormatInstances"]
				if localDynSchemaVal:
					# We reset them, in case they were dirty
					self._resetDynamicValidators(localDynSchemaVal)
					dynSchemaValList.extend(localDynSchemaVal)

		# First pass, check against JSON schema, as well as primary keys unicity
		self.logger.log(logLevel, "PASS 1: Schema validation and PK checks")
		iJsonPossible = -1
		jsonPossibles: "MutableSequence[Optional[Union[str, ParsedContentEntry]]]" = (
			list(args)
		)
		for jsonPossible in jsonPossibles:
			iJsonPossible += 1
			jsonObj = None

			assert jsonPossible is not None
			if isinstance(jsonPossible, dict):
				jsonObj = jsonPossible
				errors = jsonObj.get("errors")
				if errors is None:
					self.logger.log(
						logLevel,
						"\tIGNORE: cached JSON does not have the mandatory 'errors' attribute, so it cannot be processed",
					)
					numFilePass1Ignore += 1

					# For the report
					jsonObj.setdefault(
						"errors",
						[
							{
								"reason": "ignored",
								"description": "Programming error: uninitialized error structures",
							}
						],
					)
					yield jsonObj

					# Masking it for the pass 2 loop
					jsonPossibles[iJsonPossible] = None
					continue

				jsonDoc = jsonObj.get("json")
				if jsonDoc is None:
					self.logger.log(
						logLevel,
						"\tIGNORE: cached JSON does not have the mandatory 'json' attribute, so it cannot be processed",
					)
					errors.append(
						{
							"reason": "ignored",
							"description": "Programming error: the cached json is missing",
						}
					)
					numFilePass1Ignore += 1

					# For the report
					yield jsonObj

					# Masking it for the pass 2 loop
					jsonPossibles[iJsonPossible] = None
					continue

				jsonFile = jsonObj.setdefault("file", "(inline)")
			elif os.path.isdir(jsonPossible):
				jsonDir = jsonPossible
				# It's a possible JSON directory, not a JSON file
				try:
					for relJsonFile in os.listdir(jsonDir):
						# Skipping hidden files / directories
						if relJsonFile[0] == ".":
							continue

						newJsonFile = os.path.join(jsonDir, relJsonFile)
						if (
							os.path.isdir(newJsonFile)
							or ".json" in relJsonFile
							or ".yaml" in relJsonFile
						):
							jsonPossibles.append(newJsonFile)

					numDirOK += 1
				except IOError as ioe:
					self.logger.critical(
						"FATAL ERROR: Unable to open/process JSON directory {0}. Reason: {1}".format(
							jsonDir, ioe.strerror
						)
					)
					yield {
						"file": jsonDir,
						"errors": [
							{
								"reason": "fatal",
								"description": "Unable to open/process JSON directory",
							}
						],
					}
					numDirFail += 1
				finally:
					# Masking it for the pass 2 loop
					jsonPossibles[iJsonPossible] = None

				continue
			else:
				jsonFile = jsonPossible
				try:
					self.logger.log(logLevel, "* Validating {0}".format(jsonFile))
					try:
						with open(jsonFile, mode="r", encoding="utf-8") as jHandle:
							jsonDoc = json.load(jHandle)
					except json.decoder.JSONDecodeError as jde:
						try:
							with open(jsonFile, mode="r", encoding="utf-8") as jHandle:
								jsonDoc = yaml.safe_load(jHandle)
						except yaml.error.MarkedYAMLError as mye:
							self.logger.error(
								"\t- ERROR: Unable to parse file {0} neither as JSON or YAML. Reasons: {1} {2}".format(
									jsonFile, str(jde), str(mye)
								)
							)
							# Masking it for the next loop
							yield {
								"file": jsonFile,
								"errors": [
									{
										"reason": "fatal",
										"description": "Unable to parse file",
									}
								],
							}
							jsonPossibles[iJsonPossible] = None
							numFilePass1Fail += 1
							continue

				except IOError as ioe:
					self.logger.error(
						"\t- ERROR: Unable to open/read file {0}. Reason: {1}".format(
							jsonFile, ioe.strerror
						)
					)
					# Masking it for the next loop
					yield {
						"file": jsonFile,
						"errors": [
							{
								"reason": "fatal",
								"description": "Unable to open/read file",
							}
						],
					}
					jsonPossibles[iJsonPossible] = None
					numFilePass1Fail += 1
					continue

				else:
					errors = []
					jsonObj = {"file": jsonFile, "json": jsonDoc, "errors": errors}
					# Upgrading for the next loop
					jsonPossibles[iJsonPossible] = jsonObj

			# Getting the schema id to locate the proper schema to validate against
			if (self.jsonRootTag is not None) and (self.jsonRootTag in jsonDoc):
				jsonRoot = jsonDoc[self.jsonRootTag]
			else:
				jsonRoot = jsonDoc

			jsonSchemaIdVal: "Optional[str]" = None
			for altSchemaKey in self.ALT_SCHEMA_KEYS:
				if altSchemaKey in jsonRoot:
					jsonSchemaIdVal = jsonRoot[altSchemaKey]
					break

			if jsonSchemaIdVal is not None:
				if jsonSchemaIdVal in p_schemaHash:
					self.logger.log(
						logLevel, "\t- Using {0} schema".format(jsonSchemaIdVal)
					)

					schemaObj = p_schemaHash[jsonSchemaIdVal]

					for customFormatInstance in schemaObj["customFormatInstances"]:
						customFormatInstance.setCurrentJSONFilename(jsonFile)

					# Registering the dynamic validators to be cleaned up
					# when the validator finishes the session
					if jsonSchemaIdVal not in dynSchemaSet:
						dynSchemaSet.add(jsonSchemaIdVal)
						localDynSchemaVal = schemaObj["customFormatInstances"]
						if localDynSchemaVal:
							# We reset them, in case they were dirty
							self._resetDynamicValidators(localDynSchemaVal)
							dynSchemaValList.extend(localDynSchemaVal)

					jsonSchema = schemaObj["schema"]
					validator = schemaObj["validator"]
					jsonObj["schema_hash"] = schemaObj["schema_hash"]
					jsonObj["schema_id"] = jsonSchemaIdVal

					cachedSchemasResolver = JSV.RefResolver(
						base_uri=jsonSchemaIdVal,
						referrer=jsonSchema,
						store=self.refSchemaCache,
					)

					valErrors = [
						error
						for error in validator(
							jsonSchema,
							format_checker=self.customFormatCheckerInstance,
							resolver=cachedSchemasResolver,
						).iter_errors(jsonDoc)
					]

					if len(valErrors) > 0:
						self.logger.error(
							"\t- ERRORS:\n"
							+ "\n".join(
								map(
									lambda se: "\t\tPath: {0} . Message: {1}".format(
										"/" + "/".join(map(lambda e: str(e), se.path)),
										se.message,
									),
									valErrors,
								)
							)
						)
						for valError in valErrors:
							if isinstance(valError.validator_value, dict):
								schema_error_reason = valError.validator_value.get(
									"reason", "schema_error"
								)
							else:
								schema_error_reason = "schema_error"

							errPath = "/" + "/".join(
								map(lambda e: str(e), valError.path)
							)
							errors.append(
								{
									"reason": schema_error_reason,
									"description": "Path: {0} . Message: {1}".format(
										errPath, valError.message
									),
									"path": errPath,
								}
							)

						# Masking it for the next loop
						yield jsonPossibles[iJsonPossible]
						jsonPossibles[iJsonPossible] = None
						numFilePass1Fail += 1
					else:
						# Does the schema contain a PK declaration?
						isValid = True
						self.logger.log(logLevel, "\t- Validated!")
						numFilePass1OK += 1
				else:
					self.logger.log(
						logLevel,
						"\t- Skipping schema validation (schema with URI {0} not found)".format(
							jsonSchemaIdVal
						),
					)
					errors.append(
						{
							"reason": "schema_unknown",
							"description": "Schema with URI {0} was not loaded".format(
								jsonSchemaIdVal
							),
						}
					)
					# Masking it for the next loop
					yield jsonPossibles[iJsonPossible]
					jsonPossibles[iJsonPossible] = None
					numFilePass1Ignore += 1
			else:
				self.logger.log(
					logLevel,
					"\t- Skipping schema validation (no one declared for {0})".format(
						jsonFile
					),
				)
				errors.append(
					{
						"reason": "no_id",
						"description": "No hint to identify the correct JSON Schema to be used to validate",
					}
				)
				# Masking it for the next loop
				yield jsonPossibles[iJsonPossible]
				jsonPossibles[iJsonPossible] = None
				numFilePass1Ignore += 1

		# use Data::Dumper;
		#
		# print Dumper(\%PKvals),"\n";

		if dynSchemaValList:
			# Second pass, check foreign keys against gathered primary keys
			self.logger.log(logLevel, "PASS 2: additional checks (foreign keys and so)")
			self.warmUpCaches(dynSchemaValList, verbose)
			numFilePass2OK, numFilePass2Fail, secondPassErrors = self.doSecondPass(
				dynSchemaValList, verbose
			)
			# Reset the dynamic validators
			self._resetDynamicValidators(dynSchemaValList, verbose)

			# use Data::Dumper;
			# print Dumper(@jsonFiles),"\n";
			for jsonObjPossible in cast(
				"MutableSequence[Optional[ParsedContentEntry]]", jsonPossibles
			):
				if jsonObjPossible is None:
					continue

				# Adding this survivor to the report
				jsonFile = jsonObjPossible["file"]
				self.logger.log(logLevel, "* Additional checks on {0}".format(jsonFile))

				errorList = secondPassErrors.get(jsonFile)
				if errorList:
					jsonObjPossible["errors"].extend(errorList)
					self.logger.error("\t- ERRORS:")
					self.logger.error(
						"\n".join(
							map(
								lambda e: "\t\tPath: {0} . Message: {1}".format(
									e["path"], e["description"]
								),
								errorList,
							)
						)
					)
				else:
					self.logger.log(logLevel, "\t- Validated!")

				yield jsonObjPossible
		else:
			self.logger.log(logLevel, "PASS 2: (skipped)")

		self.logger.log(
			logLevel,
			"VALIDATION STATS:\n\t- directories ({0} OK, {1} failed)\n\t- File PASS 1 ({2} OK, {3} ignored, {4} error)\n\t- File PASS 2 ({5} OK, {6} error)".format(
				numDirOK,
				numDirFail,
				numFilePass1OK,
				numFilePass1Ignore,
				numFilePass1Fail,
				numFilePass2OK,
				numFilePass2Fail,
			),
		)

	def jsonValidate(
		self, *args: "Union[str, ParsedContentEntry]", verbose: "Optional[bool]" = None
	) -> "Sequence[Any]":
		"""
		This method validates a given list of JSON contents.
		These contents can be either already in memory, or
		files. The in memory contents are dictionaries with
		three keys: `json`, `errors` and `file`. The first
		one will contain the content to be validated, the second
		one is an array of errors (originally empty) and the last
		is a symbolic name, which could be either a real filename
		or other meaning.

		It returns a list of dictionaries, each one
		corresponding to each validated input, which will have
		the very same `json`, `errors` and `file` keys, already
		described above.
		"""

		return list(self.jsonValidateIter(verbose=verbose, *args))
