#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from typing import TYPE_CHECKING, NamedTuple, cast

import uritools  # type: ignore[import]

from .abstract_check import AbstractCustomFeatureValidator

# We need this for its class methods
from .pk_check import PrimaryKey
from .unique_check import ALLOWED_ATOMIC_VALUE_TYPES

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


class ForeignKey(AbstractCustomFeatureValidator):
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
		super().__init__(schemaURI, jsonSchemaSource, config, isRW=isRW)
		self.FKWorld: "MutableMapping[str, MutableMapping[str, FKDef]]" = dict()

	@property
	def triggerAttribute(self) -> str:
		return self.KeyAttributeNameFK

	@property
	def triggerJSONSchemaDef(self) -> "Mapping[str, Any]":
		return {
			self.KeyAttributeNameFK: {
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
	def _errorReason(self) -> str:
		return self.SchemaErrorReasonFK

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
				fkLoc = FKLoc(
					schemaURI=self.schemaURI,
					refSchemaURI=abs_ref_schema_id,
					path=loc.path + "/" + str(fk_loc_i),
					values=list(),
				)
				fk_id = abs_ref_schema_id
				fkDefH = self.FKWorld.setdefault(fk_id, {})

				# This control is here for same primary key referenced from multiple cases
				fkDefH[fk_loc_id] = FKDef(fkLoc=fkLoc, members=fk_members)

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

				fk_members = p_FK_decl.get("members", [])
				if isinstance(fk_members, list):
					obtainedValues = PrimaryKey.GetKeyValues(value, fk_members)
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
					theValues = PrimaryKey.GenKeyStrings(obtainedValues)

				# Group the values to be checked
				# fk_id = id(p_FK_decl)  # id(schema)
				fk_id = abs_ref_schema_id

				# The common dictionary for this declaration where all the FK values are kept
				fkDef = self.FKWorld.setdefault(fk_id, {}).setdefault(
					fk_loc_id,
					FKDef(
						fkLoc=FKLoc(
							schemaURI=self.schemaURI,
							refSchemaURI=abs_ref_schema_id,
							path="(unknown {})".format(fk_loc_id),
							values=list(),
						),
						members=fk_members,
					),
				)

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

		pkContextsHash: "MutableMapping[str, MutableSequence[MutableMapping[str, str]]]" = (
			{}
		)
		for className, pkContexts in l_customFeatureValidatorsContext.items():
			# This instance is only interested in primary keys
			if className == PrimaryKey.__name__:
				for pkContext in pkContexts:
					# Getting the path correspondence
					for pkDef in pkContext.context.values():
						pkLoc = pkDef.uniqueLoc
						# As there can be nested keys from other schemas
						# ignore the schemaURI from the context, and use
						# the one in the unique location
						if len(pkDef.values) > 0:
							pkVals = pkContextsHash.setdefault(pkLoc.schemaURI, [])
							pkVals.append(pkDef.values)

		# Now, at last, check!!!!!!!
		uniqueWhere: "MutableSet[str]" = set()
		uniqueFailedWhere: "MutableSet[str]" = set()
		for refSchemaURI, fkDefH in self.FKWorld.items():
			for fk_loc_id, fkDef in fkDefH.items():
				fkLoc = fkDef.fkLoc
				fkPath = fkLoc.path
				checkValuesList = pkContextsHash.get(refSchemaURI)
				if checkValuesList is not None:
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
									"description": "Unmatching FK ({0}) in {1} to schema {2}".format(
										fkString, fkVal.where, refSchemaURI
									),
									"file": fkVal.where,
									"path": fkPath,
								}
							)
				else:
					for fkVal in fkLoc.values:
						uniqueWhere.add(fkVal.where)
						uniqueFailedWhere.add(fkVal.where)
						errors.append(
							{
								"reason": self.DanglingFKErrorReason,
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
