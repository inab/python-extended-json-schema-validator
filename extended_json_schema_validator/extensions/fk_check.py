#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import abc

from typing import TYPE_CHECKING, NamedTuple, cast

import uritools  # type: ignore[import]

from .abstract_check import AbstractCustomFeatureValidator

# We need this for its class methods
from .pk_check import PrimaryKey
from .index_check import (
	IndexKey,
	ALLOWED_ATOMIC_VALUE_TYPES,
	IndexContext,
)

if TYPE_CHECKING:
	from typing import (
		Any,
		Iterator,
		Mapping,
		MutableMapping,
		MutableSequence,
		MutableSet,
		Optional,
		Sequence,
		Set,
		Tuple,
		Union,
	)

	from typing_extensions import Type

	import jsonschema as JSV
	from jsonschema.exceptions import ValidationError
	from typing_extensions import Final

	from .abstract_check import (
		BootstrapErrorDict,
		CheckContext,
		FeatureValidatorConfig,
		RefSchemaTuple,
		SecondPassErrorDict,
	)

	from .index_check import (
		IndexDef,
		IndexedValues,
	)


class FKVal(NamedTuple):
	value: "Union[str, int, float, bool]"
	where: str  # the JSON file where it happens


class FKLoc(NamedTuple):
	schemaURI: str
	refSchemaURI: str
	path: str
	values: "MutableSequence[FKVal]"


class FKDef(NamedTuple):
	fkLoc: FKLoc
	members: "Sequence[str]"
	refers_to: "Optional[str]"


class PKKeys(NamedTuple):
	schemaURI: str
	vals: "MutableSequence[IndexedValues]" = []
	by_name: "MutableMapping[str, IndexDef]" = {}


class AbstractRefKey(AbstractCustomFeatureValidator):
	# Each instance represents the set of keys from one ore more JSON Schemas
	def __init__(
		self,
		schemaURI: str,
		joinClass: "Type[IndexKey]",
		jsonSchemaSource: str = "(unknown)",
		config: "FeatureValidatorConfig" = {},
		isRW: bool = True,
	):
		super().__init__(
			schemaURI,
			jsonSchemaSource=jsonSchemaSource,
			config=config,
			isRW=isRW,
		)
		self.FKWorld: "MutableMapping[str, MutableMapping[str, FKDef]]" = dict()
		self.joinClass = joinClass

	@property
	def triggerJSONSchemaDef(self) -> "Mapping[str, Any]":
		return {
			self.triggerAttribute: {
				"type": "array",
				"items": {
					"type": "object",
					"properties": {
						"schema_id": {
							"type": "string",
							"format": "uri-reference",
							"minLength": 1,
						},
						"members": {
							"type": "array",
							"uniqueItems": True,
							"minItems": 1,
							"items": {"type": "string", "minLength": 1},
						},
						"refers_to": {
							"title": "The specific key name being referred. If unset, it refers any primary key",
							"type": "string",
							"minLength": 1,
						},
						"on_delete_hint": {
							"type": "string",
							"enum": ["RESTRICT", "CASCADE", "DELETE"],
							"default": "RESTRICT",
						},
					},
					"required": ["schema_id", "members"],
				},
				"uniqueItems": True,
			}
		}

	@property
	@abc.abstractmethod
	def _errorReason(self) -> str:
		pass

	@property
	@abc.abstractmethod
	def _danglingErrorReason(self) -> "str":
		pass

	@property
	def needsBootstrapping(self) -> bool:
		return True

	@property
	def needsSecondPass(self) -> bool:
		return True

	def bootstrap(
		self, refSchemaTuple: "RefSchemaTuple" = ({}, {}, {})
	) -> "Sequence[BootstrapErrorDict]":
		(id2ElemId, keyRefs, refSchemaCache) = refSchemaTuple

		keyList = keyRefs[self.triggerAttribute]
		errors: "MutableSequence[BootstrapErrorDict]" = []
		# Saving the unique locations
		# based on information from FeatureLoc elems
		for loc in keyList:
			fk_defs = loc.context[self.triggerAttribute]
			fk_defs_gid = str(id(loc.context))

			# fk_defs_gid = loc.path
			for fk_loc_i, p_FK_decl in enumerate(fk_defs):
				fk_loc_id = fk_defs_gid + "_" + str(fk_loc_i)
				ref_schema_id = p_FK_decl["schema_id"]
				if uritools.isabsuri(self.schemaURI):
					abs_ref_schema_id = uritools.urijoin(self.schemaURI, ref_schema_id)
				else:
					abs_ref_schema_id = ref_schema_id

				if abs_ref_schema_id not in refSchemaCache:
					errors.append(
						{
							"reason": "fk_no_schema",
							"description": "No schema with {0} id, required by {1} ({2})".format(
								abs_ref_schema_id, self.jsonSchemaSource, self.schemaURI
							),
						}
					)

				fk_members = p_FK_decl.get("members", [])
				fk_pk_name = p_FK_decl.get("refers_to")
				fkLoc = FKLoc(
					schemaURI=self.schemaURI,
					refSchemaURI=abs_ref_schema_id,
					path=loc.path + "/" + str(fk_loc_i),
					values=list(),
				)
				fk_id = abs_ref_schema_id
				fkDefH = self.FKWorld.setdefault(fk_id, {})

				# This control is here for same primary key referenced from multiple cases
				fkDefH[fk_loc_id] = FKDef(
					fkLoc=fkLoc, members=fk_members, refers_to=fk_pk_name
				)

		return errors

	# This step is only going to gather all the foreign keys
	def validate(
		self,
		validator: "JSV.validators._Validator",
		fk_defs: "Any",
		value: "Any",
		schema: "Any",
	) -> "Iterator[ValidationError]":
		# Next is needed to avoid mypy complaining about
		# missing return or yield
		if False:
			yield
		if fk_defs and isinstance(fk_defs, (list, tuple)):
			fk_defs_gid = str(id(schema))
			for fk_loc_i, p_FK_decl in enumerate(fk_defs):
				fk_loc_id = fk_defs_gid + "_" + str(fk_loc_i)
				ref_schema_id = p_FK_decl["schema_id"]
				if uritools.isabsuri(self.schemaURI):
					abs_ref_schema_id = uritools.urijoin(self.schemaURI, ref_schema_id)
				else:
					abs_ref_schema_id = ref_schema_id

				# Group the values to be checked
				# fk_id = id(p_FK_decl)  # id(schema)
				fk_id = abs_ref_schema_id

				# The common dictionary for this declaration where all the FK values are kept
				fkDefs = self.FKWorld.setdefault(fk_id, {})
				fkDef = fkDefs.get(fk_loc_id)
				if fkDef is None:
					fk_members = p_FK_decl.get("members", [])
					fk_pk_name = p_FK_decl.get("refers_to")
					fkDef = FKDef(
						fkLoc=FKLoc(
							schemaURI=self.schemaURI,
							refSchemaURI=abs_ref_schema_id,
							path="(unknown {})".format(fk_loc_id),
							values=list(),
						),
						members=fk_members,
						refers_to=fk_pk_name,
					)
					fkDefs[fk_loc_id] = fkDef

				if isinstance(fkDef.members, list):
					obtainedValues = IndexKey.GetKeyValues(value, fkDef.members)
				else:
					obtainedValues = ([value],)

				isAtomicValue = (
					len(obtainedValues) == 1
					and len(obtainedValues[0]) == 1
					and isinstance(obtainedValues[0][0], ALLOWED_ATOMIC_VALUE_TYPES)
				)

				theValues: "Tuple[Union[str, int, float, bool], ...]"
				if isAtomicValue:
					theValues = (obtainedValues[0][0],)
				else:
					theValues = IndexKey.GenKeyStrings(obtainedValues)

				fkLoc = fkDef.fkLoc

				fkVals = fkLoc.values

				# Second pass will do the validation
				for theValue in theValues:
					fkVals.append(FKVal(where=self.currentJSONFile, value=theValue))

	# Now, time to check
	def doSecondPass(
		self, l_customFeatureValidatorsContext: "Mapping[str, Sequence[CheckContext]]"
	) -> "Tuple[Set[str], Set[str], Sequence[SecondPassErrorDict]]":
		errors: "MutableSequence[SecondPassErrorDict]" = []

		# First level: by schema id
		# second level: PKKeys tuple with three components
		# - schema URI (str)
		# - MutableSequence of IndexedValues
		# - MutableMapping by key name of IndexDef
		pkContextsHash: "MutableMapping[str, PKKeys]" = {}
		for className, pkContexts in l_customFeatureValidatorsContext.items():
			# This instance is only interested in primary keys
			if className == self.joinClass.__name__:
				for pkContext in pkContexts:
					# Getting the path correspondence
					assert isinstance(pkContext.context, IndexContext)
					for pkDef in pkContext.context.index_world.values():
						pkLoc = pkDef.indexLoc
						# As there can be nested keys from other schemas
						# ignore the schemaURI from the context, and use
						# the one in the unique location
						pkKeys = pkContextsHash.get(pkLoc.schemaURI)
						if len(pkDef.values) > 0:
							if pkKeys is None:
								pkKeys = PKKeys(
									schemaURI=pkLoc.schemaURI,
								)
								pkContextsHash[pkLoc.schemaURI] = pkKeys
							pkKeys.vals.append(pkDef.values)

						if pkKeys is not None:
							if pkDef.name in pkKeys.by_name:
								self.logger.debug(
									f"Repeated primary key '{pkDef.name}'. Be prepared for foreign key hairy responses."
								)
							else:
								pkKeys.by_name[pkDef.name] = pkDef
						else:
							self.logger.debug(f"Unhandled PK to {pkLoc.schemaURI}")

		# Now, at last, check!!!!!!!
		uniqueWhere: "MutableSet[str]" = set()
		uniqueFailedWhere: "MutableSet[str]" = set()
		# For each registered set of foreign key definitions in this JSON Schema,
		# clustered by referenced schema URI
		for refSchemaURI, fkDefH in self.FKWorld.items():
			# Get the dictionary of keys which can be checked
			# from the referenced schema URI
			checkValuesKeys = pkContextsHash.get(refSchemaURI)
			if checkValuesKeys is not None:
				# For each registered foreign key of the JSON Schema
				# referring the schema URI with the primary key
				for fk_loc_id, fkDef in fkDefH.items():
					# Get the details of the foreign key
					fkLoc = fkDef.fkLoc
					fkPath = fkLoc.path

					# Select the source of validation
					# It could be a named public key
					checkValuesList: "Sequence[IndexedValues]"
					if fkDef.refers_to is not None:
						uDef = checkValuesKeys.by_name.get(fkDef.refers_to)
						# If the named key is not found, fail
						if uDef is None:
							for fkVal in fkLoc.values:
								uniqueWhere.add(fkVal.where)
								uniqueFailedWhere.add(fkVal.where)
								errors.append(
									{
										"reason": "stale_fk",
										"description": "Unmatching FK ({0}) in {1} to schema {2} (key {3} not found)".format(
											fkVal.value,
											fkVal.where,
											refSchemaURI,
											fkDef.refers_to,
										),
										"file": fkVal.where,
										"path": fkPath,
									}
								)
							continue

						# As it was found, go ahead
						checkValuesList = [uDef.values]
					else:
						# When the key has no name,
						# or it is not targetted
						# then check against all
						# the keys in the context
						checkValuesList = checkValuesKeys.vals

					# Now, checktime!!!!
					for fkVal in fkLoc.values:
						uniqueWhere.add(fkVal.where)

						fkString = fkVal.value
						found = False
						for checkValues in checkValuesList:
							if fkString in checkValues:
								found = True
								break

						if not found:
							uniqueFailedWhere.add(fkVal.where)
							errors.append(
								{
									"reason": "stale_fk",
									"description": "Unmatching FK ({0}) in {1} to schema {2} (key {3})".format(
										fkString,
										fkVal.where,
										refSchemaURI,
										fkDef.refers_to,
									),
									"file": fkVal.where,
									"path": fkPath,
								}
							)
			else:
				# For each registered foreign key of the JSON Schema
				# referring the schema URI with the primary key
				for fk_loc_id, fkDef in fkDefH.items():
					# Get the details of the foreign key
					fkLoc = fkDef.fkLoc
					fkPath = fkLoc.path
					# To report there is no way to
					# check because there is no document
					# available
					for fkVal in fkLoc.values:
						uniqueWhere.add(fkVal.where)
						uniqueFailedWhere.add(fkVal.where)
						errors.append(
							{
								"reason": self._danglingErrorReason,
								"description": "No available documents from {0} schema, required by {1}".format(
									refSchemaURI, self.schemaURI
								),
								"file": fkVal.where,
								"path": fkPath,
							}
						)

		return (
			cast("Set[str]", uniqueWhere),
			cast("Set[str]", uniqueFailedWhere),
			errors,
		)

	def cleanup(self) -> None:
		# In order to not destroying the bootstrapping work
		# only remove the recorded values
		for fkDefH in self.FKWorld.values():
			for fkDef in fkDefH.values():
				fkDef.fkLoc.values.clear()


class ForeignKey(AbstractRefKey):
	KeyAttributeNameFK: "Final[str]" = "foreign_keys"
	SchemaErrorReasonFK: "Final[str]" = "stale_fk"
	DanglingFKErrorReason: "Final[str]" = "dangling_fk"

	# Each instance represents the set of keys from one ore more JSON Schemas
	def __init__(
		self,
		schemaURI: str,
		jsonSchemaSource: str = "(unknown)",
		config: "FeatureValidatorConfig" = {},
		isRW: bool = True,
	):
		super().__init__(
			schemaURI,
			joinClass=PrimaryKey,
			jsonSchemaSource=jsonSchemaSource,
			config=config,
			isRW=isRW,
		)

	@property
	def triggerAttribute(self) -> str:
		return self.KeyAttributeNameFK

	@property
	def _errorReason(self) -> str:
		return self.SchemaErrorReasonFK

	@property
	def _danglingErrorReason(self) -> "str":
		return self.DanglingFKErrorReason
