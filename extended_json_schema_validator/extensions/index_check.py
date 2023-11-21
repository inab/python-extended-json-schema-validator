#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
import re

from .abstract_check import AbstractCustomFeatureValidator, CheckContext

ALLOWED_KEY_TYPES = (bytes, str)
ALLOWED_ATOMIC_VALUE_TYPES = (int, bytes, str, float, bool, type(None))

from typing import TYPE_CHECKING, NamedTuple

if TYPE_CHECKING:
	from typing import (
		Any,
		Iterator,
		Mapping,
		MutableMapping,
		MutableSequence,
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
		FeatureValidatorConfig,
		RefSchemaTuple,
	)

	IndexedValues = MutableMapping[Union[str, int, float, bool, None], Set[str]]


class IndexLoc(NamedTuple):
	schemaURI: str
	path: str


class IndexDef(NamedTuple):
	indexLoc: IndexLoc
	members: "Union[bool, Sequence[str]]"
	values: "IndexedValues"
	name: str
	# This is for backward compatibility
	limit_scope: bool = False


if TYPE_CHECKING:
	IndexWorldType = MutableMapping[int, IndexDef]
	IndexWorldByNameType = MutableMapping[str, IndexDef]


class IndexContext(NamedTuple):
	index_world: "IndexWorldType"
	index_world_by_name: "IndexWorldByNameType"


class IndexKey(AbstractCustomFeatureValidator):
	KeyAttributeNameIK: "Final[str]" = "index"
	SchemaErrorReasonIK: "Final[str]" = "err_index"

	# Each instance represents the set of keys from one ore more JSON Schemas
	def __init__(
		self,
		schemaURI: str,
		jsonSchemaSource: str = "(unknown)",
		config: "FeatureValidatorConfig" = {},
		isRW: bool = True,
	):
		super().__init__(schemaURI, jsonSchemaSource, config, isRW=isRW)
		self.IndexWorld: "IndexWorldType" = dict()
		self.IndexWorldByName: "IndexWorldByNameType" = dict()

	@property
	def triggerAttribute(self) -> str:
		return self.KeyAttributeNameIK

	@property
	def randomKeyPrefix(self) -> str:
		return "index"

	@property
	def triggerJSONSchemaDef(self) -> "Mapping[str, Any]":
		return {
			self.triggerAttribute: {
				"oneOf": [
					{"type": "boolean"},
					{
						"type": "array",
						"items": {"type": "string", "minLength": 1},
						"uniqueItems": True,
						"minItems": 1,
					},
					{
						"type": "object",
						"properties": {
							"members": {
								"oneOf": [
									{"type": "boolean"},
									{
										"type": "array",
										"items": {"type": "string", "minLength": 1},
										"uniqueItems": True,
										"minItems": 1,
									},
								]
							},
							"limit_scope": {
								"type": "boolean",
								"default": False,
							},
							"name": {
								"type": "string",
								"minLength": 1,
							},
						},
						"required": [
							"members",
						],
					},
				]
			}
		}

	@property
	def _errorReason(self) -> str:
		return self.SchemaErrorReasonIK

	@property
	def needsBootstrapping(self) -> bool:
		return True

	def bootstrap(
		self, refSchemaTuple: "RefSchemaTuple" = ({}, {}, {})
	) -> "Sequence[BootstrapErrorDict]":
		(id2ElemId, keyRefs, _) = refSchemaTuple

		keyList = keyRefs[self.triggerAttribute]
		# Saving the indexed locations
		# based on information from FeatureLoc elems
		for loc in keyList:
			iLoc = IndexLoc(schemaURI=loc.schemaURI, path=loc.path)
			iId = id(loc.context)

			iDef = self.IndexWorld.get(iId)

			# This control is here for multiple inheritance cases
			if iDef is not None:
				iDef = iDef._replace(indexLoc=iLoc)
				self.IndexWorld[iId] = iDef
				self.IndexWorldByName[iDef.name] = iDef
			else:
				poss_members = loc.context[self.triggerAttribute]
				if isinstance(poss_members, dict):
					index_members = poss_members["members"]
					index_name = poss_members.get("name")
					limit_scope_v = poss_members.get("limit_scope", False)
					limit_scope = False if limit_scope_v is None else limit_scope_v
				else:
					index_members = poss_members
					index_name = None
					limit_scope = False
				# Assigning a random name
				if index_name is None:
					index_name = f"{self.randomKeyPrefix}_{iId}"
				iDef = IndexDef(
					indexLoc=iLoc,
					members=index_members,
					name=index_name,
					limit_scope=limit_scope,
					values=dict(),
				)
				self.IndexWorld[iId] = iDef
				if index_name in self.IndexWorldByName:
					self.logger.warning(
						f"Repeated named {self.randomKeyPrefix} '{index_name}'. Be prepared for hairy responses."
					)
				else:
					self.IndexWorldByName[index_name] = iDef

		return []

	JStepPat = re.compile(r"^([^\[]+)\[(0|[1-9][0-9]+)?\]$")

	@classmethod
	def MaterializeJPath(cls, jsonDoc: "Any", jPath: str) -> "Sequence[Any]":
		objectives: "MutableSequence[Any]" = [jsonDoc]
		jSteps = jPath.split(".") if jPath not in (".", "") else (None,)
		for jStep in jSteps:
			newObjectives = []
			isArray = False
			arrayIndex = None
			if jStep is not None:
				jStepMatch = cls.JStepPat.search(jStep)
				if jStepMatch is not None:
					isArray = True
					if jStepMatch.group(2) is not None:
						arrayIndex = int(jStepMatch.group(2))
					jStep = jStepMatch.group(1)
			for objective in objectives:
				isAvailable = False
				if jStep is not None:
					if isinstance(objective, dict):
						if jStep in objective:
							value = objective[jStep]
							isAvailable = True
					# else:
					# 	# Failing
					# 	return None
				else:
					value = objective
					isAvailable = True

				if isAvailable:
					if isinstance(value, (list, tuple)):
						if arrayIndex is not None:
							if arrayIndex >= 0 and arrayIndex < len(value):
								newObjectives.append(value[arrayIndex])
							# else:
							# 	return None
						else:
							newObjectives.extend(value)
					else:
						newObjectives.append(value)
				# else:
				# 	# Failing
				# 	return None

			objectives = newObjectives

		# Flattening it (we return a reference to a list of atomic values)
		for iobj, objective in enumerate(objectives):
			if not isinstance(objective, ALLOWED_ATOMIC_VALUE_TYPES):
				objectives[iobj] = json.dumps(objective, sort_keys=True)

		return objectives

	# It fetches the values from a JSON, based on the given paths to the members of the key
	@classmethod
	def GetKeyValues(
		cls, jsonDoc: "Any", p_members: "Sequence[str]"
	) -> "Tuple[Sequence[Any], ...]":
		return tuple(cls.MaterializeJPath(jsonDoc, member) for member in p_members)

	@classmethod
	def _aggPKhelper(
		cls, basePK: "Sequence[Any]", curPKvalue: "Any"
	) -> "Sequence[Any]":
		newPK = list(basePK)
		newPK.append(curPKvalue)
		return newPK

	# It generates indexable strings from a set of values
	@classmethod
	def GenKeyStrings(cls, keyTuple: "Tuple[Sequence[Any], ...]") -> "Tuple[str, ...]":
		numPKcols = len(keyTuple)
		if numPKcols == 0:
			return tuple()

		# Exiting in case some of the inputs is undefined
		for curPKvalues in keyTuple:
			# If there is no found value, generate nothing
			if not isinstance(curPKvalues, (list, tuple)) or len(curPKvalues) == 0:
				return tuple()

		pkStrings: "MutableSequence[Any]" = list(map(lambda elem: [elem], keyTuple[0]))

		for curPKvalues in keyTuple[1:]:
			newPKstrings: "MutableSequence[Any]" = []

			for curPKvalue in curPKvalues:
				newPKstrings.extend(
					map(lambda basePK: cls._aggPKhelper(basePK, curPKvalue), pkStrings)
				)

			pkStrings = newPKstrings

		return tuple(
			map(
				lambda pkString: json.dumps(
					pkString, sort_keys=True, separators=(",", ":")
				),
				pkStrings,
			)
		)

	def validate(
		self,
		validator: "JSV.validators._Validator",
		index_state: "Any",
		value: "Any",
		schema: "Any",
	) -> "Iterator[ValidationError]":
		# Next is needed to avoid mypy complaining about
		# missing return or yield
		if False:
			yield
		if index_state:
			# Check the unicity
			index_id = id(schema)

			# The common dictionary for this declaration where all the indexed values are kept
			indexDef = self.IndexWorld.get(index_id)
			if indexDef is None:
				if isinstance(index_state, dict):
					index_members = index_state["members"]
					index_name = index_state.get("name")
					limit_scope_v = index_state.get("limit_scope", False)
					limit_scope = False if limit_scope_v is None else limit_scope_v
				else:
					index_members = index_state
					index_name = None
					limit_scope = False
				# Assigning a random name
				if index_name is None:
					index_name = f"{self.randomKeyPrefix}_{index_id}"

				indexDef = IndexDef(
					indexLoc=IndexLoc(schemaURI=self.schemaURI, path="(unknown)"),
					members=index_members,
					name=index_name,
					limit_scope=limit_scope,
					values=dict(),
				)
				self.IndexWorld[index_id] = indexDef
				if index_name in self.IndexWorldByName:
					self.logger.warning(
						f"Repeated named {self.randomKeyPrefix} '{index_name}'. Be prepared for hairy responses."
					)
				else:
					self.IndexWorldByName[index_name] = indexDef

			if isinstance(indexDef.members, list):
				obtainedValues = self.GetKeyValues(value, indexDef.members)
			else:
				obtainedValues = ([value],)

			# We are adding another "indirection"
			if indexDef.limit_scope:
				obtainedValues = ([self.currentJSONFile], *obtainedValues)
				isAtomicValue = False
			else:
				isAtomicValue = (
					len(obtainedValues) == 1
					and len(obtainedValues[0]) == 1
					and isinstance(obtainedValues[0][0], ALLOWED_ATOMIC_VALUE_TYPES)
				)

			theValues: "Tuple[Union[str, int, float, bool, None], ...]"
			if isAtomicValue:
				theValues = (obtainedValues[0][0],)
			else:
				theValues = self.GenKeyStrings(obtainedValues)

			indexValues = indexDef.values
			# Should it complain about this?
			for theValue in theValues:
				# No error, as it is only indexing for the join_keys
				if theValue not in indexValues:
					indexValues[theValue] = set()

				indexValues[theValue].add(self.currentJSONFile)

	def forget(self, the_json_file: "str") -> "bool":
		"""
		This method "forgets" what it was gathered for the input json file.
		This is needed when we are guessing schemas
		"""
		removed = False
		for indexDef in self.IndexWorld.values():
			keys_to_free = []
			for indexValue, indexSet in indexDef.values.items():
				if the_json_file in indexSet:
					if len(indexSet) > 1:
						indexSet.remove(the_json_file)
					else:
						keys_to_free.append(indexValue)
					removed = True
			# These values are not registered any more
			if len(keys_to_free) > 0:
				for indexValue in keys_to_free:
					del indexDef.values[indexValue]
		return removed

	def getContext(self) -> "Optional[CheckContext]":
		return CheckContext(
			schemaURI=self.schemaURI,
			context=IndexContext(
				index_world=self.IndexWorld,
				index_world_by_name=self.IndexWorldByName,
			),
		)

	def cleanup(self) -> None:
		# In order to not destroying the bootstrapping work
		# only remove the recorded values
		for uDef in self.IndexWorld.values():
			uDef.values.clear()
