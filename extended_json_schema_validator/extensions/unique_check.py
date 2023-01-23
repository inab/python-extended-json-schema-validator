#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
import re

from jsonschema.exceptions import ValidationError

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
		Tuple,
		Union,
	)

	import jsonschema as JSV
	from typing_extensions import Final

	from .abstract_check import (
		BootstrapErrorDict,
		FeatureValidatorConfig,
		RefSchemaTuple,
	)

	UniqueValues = MutableMapping[Union[str, int, float, bool, None], str]


class UniqueLoc(NamedTuple):
	schemaURI: str
	path: str


class UniqueDef(NamedTuple):
	uniqueLoc: UniqueLoc
	members: "Union[bool, Sequence[str]]"
	values: "UniqueValues"
	name: str


class UniqueContext(NamedTuple):
	unique_world: "MutableMapping[int, UniqueDef]"
	unique_world_by_name: "MutableMapping[str, UniqueDef]"


class UniqueKey(AbstractCustomFeatureValidator):
	KeyAttributeNameUK: "Final[str]" = "unique"
	SchemaErrorReasonUK: "Final[str]" = "dup_unique"

	# Each instance represents the set of keys from one ore more JSON Schemas
	def __init__(
		self,
		schemaURI: str,
		jsonSchemaSource: str = "(unknown)",
		config: "FeatureValidatorConfig" = {},
		isRW: bool = True,
	):
		super().__init__(schemaURI, jsonSchemaSource, config, isRW=isRW)
		self.UniqueWorld: "MutableMapping[int, UniqueDef]" = dict()
		self.UniqueWorldByName: "MutableMapping[str, UniqueDef]" = dict()

	@property
	def triggerAttribute(self) -> str:
		return self.KeyAttributeNameUK

	@property
	def randomKeyPrefix(self) -> str:
		return "unique"

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
		return self.SchemaErrorReasonUK

	@property
	def needsBootstrapping(self) -> bool:
		return True

	def bootstrap(
		self, refSchemaTuple: "RefSchemaTuple" = ({}, {}, {})
	) -> "Sequence[BootstrapErrorDict]":
		(id2ElemId, keyRefs, _) = refSchemaTuple

		keyList = keyRefs[self.triggerAttribute]
		# Saving the unique locations
		# based on information from FeatureLoc elems
		for loc in keyList:
			uLoc = UniqueLoc(schemaURI=loc.schemaURI, path=loc.path)
			uId = id(loc.context)

			uDef = self.UniqueWorld.get(uId)

			# This control is here for multiple inheritance cases
			if uDef is not None:
				uDef = uDef._replace(uniqueLoc=uLoc)
				self.UniqueWorld[uId] = uDef
				self.UniqueWorldByName[uDef.name] = uDef
			else:
				poss_members = loc.context[self.triggerAttribute]
				if isinstance(poss_members, dict):
					unique_members = poss_members["members"]
					unique_name = poss_members.get("name")
				else:
					unique_members = poss_members
					unique_name = None
				# Assigning a random name
				if unique_name is None:
					unique_name = f"{self.randomKeyPrefix}_{uId}"
				uDef = UniqueDef(
					uniqueLoc=uLoc,
					members=unique_members,
					name=unique_name,
					values=dict(),
				)
				self.UniqueWorld[uId] = uDef
				if unique_name in self.UniqueWorldByName:
					self.logger.warning(
						f"Repeated named {self.randomKeyPrefix} '{unique_name}'. Be prepared for hairy responses."
					)
				else:
					self.UniqueWorldByName[unique_name] = uDef

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
	) -> "Tuple[Any, ...]":
		return tuple(cls.MaterializeJPath(jsonDoc, member) for member in p_members)

	@classmethod
	def _aggPKhelper(
		cls, basePK: "Sequence[Any]", curPKvalue: "Any"
	) -> "Sequence[Any]":
		newPK = list(basePK)
		newPK.append(curPKvalue)
		return newPK

	# It generates unique strings from a set of values
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
		unique_state: "Any",
		value: "Any",
		schema: "Any",
	) -> "Iterator[ValidationError]":
		if unique_state:
			# Check the unicity
			unique_id = id(schema)

			# The common dictionary for this declaration where all the unique values are kept
			uniqueDef = self.UniqueWorld.get(unique_id)
			if uniqueDef is None:
				if isinstance(unique_state, dict):
					unique_members = unique_state["members"]
					unique_name = unique_state.get("name")
				else:
					unique_members = unique_state
					unique_name = None
				# Assigning a random name
				if unique_name is None:
					unique_name = f"{self.randomKeyPrefix}_{unique_id}"

				uniqueDef = UniqueDef(
					uniqueLoc=UniqueLoc(schemaURI=self.schemaURI, path="(unknown)"),
					members=unique_members,
					name=unique_name,
					values=dict(),
				)
				self.UniqueWorld[unique_id] = uniqueDef
				if unique_name in self.UniqueWorldByName:
					self.logger.warning(
						f"Repeated named {self.randomKeyPrefix} '{unique_name}'. Be prepared for hairy responses."
					)
				else:
					self.UniqueWorldByName[unique_name] = uniqueDef

			if isinstance(uniqueDef.members, list):
				obtainedValues = self.GetKeyValues(value, uniqueDef.members)
			else:
				obtainedValues = ([value],)

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

			uniqueSet = uniqueDef.values
			# Should it complain about this?
			for theValue in theValues:
				if theValue in uniqueSet:
					yield ValidationError(
						"Duplicated {0} value for UK {1} -=> {2} <=-  (got from {3}, appeared in {4})".format(
							self.triggerAttribute,
							uniqueDef.name,
							theValue,
							uniqueDef.members,
							uniqueSet[theValue],
						),
						validator_value={"reason": self._errorReason},
					)
				else:
					uniqueSet[theValue] = self.currentJSONFile

	def getContext(self) -> "Optional[CheckContext]":
		return CheckContext(
			schemaURI=self.schemaURI,
			context=UniqueContext(self.UniqueWorld, self.UniqueWorldByName),
		)

	def cleanup(self) -> None:
		# In order to not destroying the bootstrapping work
		# only remove the recorded values
		for uDef in self.UniqueWorld.values():
			uDef.values.clear()
