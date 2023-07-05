#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import copy
import urllib.error
from typing import TYPE_CHECKING, cast
from urllib.parse import urljoin, urlparse
from urllib.request import Request, urlopen

from jsonschema.exceptions import ValidationError

from .abstract_check import CheckContext
from .index_check import (
	ALLOWED_ATOMIC_VALUE_TYPES,
	IndexDef,
	IndexLoc,
	IndexContext,
)
from .unique_check import (
	UniqueKey,
)

if TYPE_CHECKING:
	from typing import (
		Any,
		Iterator,
		Mapping,
		MutableMapping,
		Optional,
		Sequence,
		Tuple,
		Union,
	)

	import jsonschema as JSV
	from typing_extensions import Final, TypedDict

	from .abstract_check import FeatureValidatorConfig

	InlineAtomicPKVal = Union[bool, int, float, str, None]
	InlinePKVal = Union[InlineAtomicPKVal, Mapping[str, Any], Sequence[Any]]

	class PKConfigDict(TypedDict, total=False):
		schema_prefix: str
		accept: str
		provider: Union[str, Sequence[str]]
		inline_provider: Mapping[str, Sequence[InlinePKVal]]


class PrimaryKey(UniqueKey):
	KeyAttributeNamePK: "Final[str]" = "primary_key"
	SchemaErrorReasonPK: "Final[str]" = "dup_pk"

	# Each instance represents the set of keys from one ore more JSON Schemas
	def __init__(
		self,
		schemaURI: str,
		jsonSchemaSource: str = "(unknown)",
		config: "FeatureValidatorConfig" = {},
		isRW: bool = True,
	):
		super().__init__(schemaURI, jsonSchemaSource, config, isRW=isRW)
		self.doPopulate = False
		self.gotIdsSet: "Optional[MutableMapping[str, Sequence[InlinePKVal]]]" = None
		self.warmedUp = False
		self.PopulatedPKWorld: "MutableMapping[int, IndexDef]" = dict()
		self.PopulatedPKWorldByName: "MutableMapping[str, IndexDef]" = dict()

	@property
	def triggerAttribute(self) -> str:
		return self.KeyAttributeNamePK

	@property
	def randomKeyPrefix(self) -> str:
		return "pk"

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
		return self.SchemaErrorReasonPK

	###
	# Bootstrapping is done by index_check implementation
	# which is inherited
	###

	def warmUpCaches(self) -> None:
		if not self.warmedUp:
			self.warmedUp = True

			setup = cast(
				"Optional[PKConfigDict]", self.config.get(self.triggerAttribute)
			)
			if setup is not None:
				# The provided inline list of ids
				schema2id = setup.get("inline_provider", {})
				gotIds = schema2id.get(self.schemaURI)
				if isinstance(gotIds, list):
					if self.gotIdsSet is None:
						self.gotIdsSet = {}
					self.gotIdsSet["__inline__"] = gotIds

				prefix = setup.get("schema_prefix")
				accept = setup.get("accept")
				if prefix != self.schemaURI and accept is not None:
					if self.gotIdsSet is None:
						self.gotIdsSet = {}

					# The list of sources
					url_base_list_raw = setup.get("provider", [])
					if isinstance(url_base_list_raw, (list, tuple)):
						url_base_list = url_base_list_raw
					else:
						url_base_list = [url_base_list_raw]

					for url_base in url_base_list:
						# Fetch the ids, based on the id
						relColId = urlparse(self.schemaURI).path.split("/")[-1]
						compURL = urljoin(url_base, relColId + "/")
						r = Request(compURL, headers={"Accept": accept})

						try:
							with urlopen(r) as f:
								if f.getcode() == 200:
									gotIds = list(
										filter(
											lambda l: l != "",
											str(f.read(), "utf-8").split("\n"),
										)
									)
									if gotIds:
										self.gotIdsSet[compURL] = gotIds
										self.doPopulate = True
						except urllib.error.HTTPError as he:
							self.logger.error(
								"ERROR: Unable to fetch remote keys data from {0} [{1}]: {2}".format(
									compURL, he.code, he.reason
								)
							)
						except urllib.error.URLError as ue:
							self.logger.error(
								"ERROR: Unable to fetch remote keys data from {0}: {1}".format(
									compURL, ue.reason
								)
							)
						except:
							self.logger.exception(
								"ERROR: Unable to parse remote keys data from "
								+ compURL
							)

	def doDefaultPopulation(
		self,
		unique_id: int = -1,
		unique_members: "Union[bool, Sequence[str]]" = [],
		unique_name: "Optional[str]" = None,
	) -> None:
		if self.doPopulate:
			# Deactivate future populations
			self.doPopulate = False

			if self.gotIdsSet:
				# The common dictionary for this declaration where all the unique values are kept
				allow_provider_duplicates = self.config.get(
					self.triggerAttribute, {}
				).get(
					"allow_provider_duplicates", False
				)
				if allow_provider_duplicates:
					UniqueWorld = self.PopulatedPKWorld
					UniqueWorldByName = self.PopulatedPKWorldByName
				else:
					UniqueWorld = self.IndexWorld
					UniqueWorldByName = self.IndexWorldByName

				uniqueDef = UniqueWorld.get(unique_id)
				if uniqueDef is None:
					# Assigning a random name
					if unique_name is None:
						unique_name = f"{self.randomKeyPrefix}_{unique_id}"
					uniqueDef = UniqueWorld.setdefault(
						unique_id,
						IndexDef(
							indexLoc=IndexLoc(
								schemaURI=self.schemaURI, path="(unknown)"
							),
							members=unique_members,
							name=unique_name,
							limit_scope=False,
							values=dict(),
						),
					)
					UniqueWorld[unique_id] = uniqueDef
					if unique_name in UniqueWorldByName:
						self.logger.warning(
							f"Repeated named {self.randomKeyPrefix} '{unique_name}'. Be prepared for hairy responses."
						)
					else:
						UniqueWorldByName[unique_name] = uniqueDef
				uniqueSet = uniqueDef.values

				# Should it complain about this?
				for compURL, gotIds in self.gotIdsSet.items():
					collision_urls = set()
					for theValue in gotIds:
						if uniqueDef.limit_scope:
							isAtomicValue = False
							if isinstance(theValue, (list, tuple)):
								theValue = [compURL, *theValue]
							else:
								theValue = [compURL, theValue]
						else:
							isAtomicValue = isinstance(
								theValue, ALLOWED_ATOMIC_VALUE_TYPES
							)

						key_string: "InlineAtomicPKVal"
						if isAtomicValue:
							key_string = cast("InlineAtomicPKVal", theValue)
						else:
							if isinstance(theValue, (list, tuple)):
								proc_value = theValue
							else:
								proc_value = [theValue]
							ks_answer = self.GenKeyStrings(
								tuple(map(lambda tv: [tv], proc_value))
							)

							# This can happen with some
							# data types, like dictionaries
							if len(ks_answer) == 0:
								continue
							key_string = ks_answer[0]

						other_compURL = uniqueSet.get(key_string)
						# Only complain about collisions
						# from different pid providers
						if (other_compURL is not None) and (
							compURL not in other_compURL
						):
							if allow_provider_duplicates:
								collision_urls.update(other_compURL)
							else:
								raise ValidationError(
									"Duplicated {0} value from {1} for PK {2} -=> {3} <=-  (got from {4}, appeared in {5})".format(
										self.triggerAttribute,
										compURL,
										uniqueDef.name,
										key_string,
										uniqueDef.members,
										other_compURL,
									),
									validator_value={"reason": self._errorReason},
								)
						elif other_compURL is None:
							uniqueSet[key_string] = set([compURL])

					if len(collision_urls) > 0:
						self.logger.warning(
							f"Public keys from {compURL} collided with public keys from {', '.join(collision_urls)}"
						)

	def validate(
		self,
		validator: "JSV.validators._Validator",
		unique_state: "Any",
		value: "Any",
		schema: "Any",
	) -> "Iterator[ValidationError]":
		self.warmUpCaches()

		# Populating before the validation itself
		if unique_state:
			# Needed to populate the cache of ids
			# and the unicity check
			unique_id = id(schema)
			uniqueDef = self.IndexWorld.get(unique_id)
			if uniqueDef is None:
				if isinstance(unique_state, dict):
					unique_members = unique_state["members"]
					unique_name = unique_state.get("name")
					limit_scope_v = unique_state.get("limit_scope", False)
					limit_scope = False if limit_scope_v is None else limit_scope_v
				else:
					unique_members = unique_state
					unique_name = None
					limit_scope = False
				# Assigning a random name
				if unique_name is None:
					unique_name = f"{self.randomKeyPrefix}_{unique_id}"

				uniqueDef = self.IndexWorld.setdefault(
					unique_id,
					IndexDef(
						indexLoc=IndexLoc(schemaURI=self.schemaURI, path="(unknown)"),
						members=unique_members,
						name=unique_name,
						limit_scope=limit_scope,
						values=dict(),
					),
				)
				self.IndexWorld[unique_id] = uniqueDef

			self.doDefaultPopulation(
				unique_id=unique_id,
				unique_members=uniqueDef.members,
				unique_name=uniqueDef.name,
			)

			if isinstance(uniqueDef.members, list):
				obtainedValues = self.GetKeyValues(value, uniqueDef.members)
			else:
				obtainedValues = ([value],)

			# We are adding another "indirection"
			if uniqueDef.limit_scope:
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

			# The common dictionary for this declaration where all the unique values are kept
			uniqueSet = uniqueDef.values

			# Should it complain about this?
			for theValue in theValues:
				if theValue in uniqueSet:
					yield ValidationError(
						"Duplicated {0} value for PK {1} -=> {2} <=-  (got from {3}, appeared in {4})".format(
							self.triggerAttribute,
							uniqueDef.name,
							theValue,
							uniqueDef.members,
							uniqueSet[theValue],
						),
						validator_value={"reason": self._errorReason},
					)
				else:
					uniqueSet[theValue] = set([self.currentJSONFile])

	# inherited "forget" method does not need a reimplementation
	# because PopulatedPKWorld is not changed when a file is checked

	def getContext(self) -> "Optional[CheckContext]":
		# These are needed to assure the context is always completely populated
		self.warmUpCaches()
		self.doDefaultPopulation()

		if len(self.PopulatedPKWorld) > 0:
			ConsolidatedUniqueWorld = copy.copy(self.PopulatedPKWorld)
			ConsolidatedUniqueWorldByName = copy.copy(self.PopulatedPKWorldByName)

			for unique_id, uniqueDef in self.IndexWorld.items():
				baseUniqueDef = ConsolidatedUniqueWorld.get(unique_id)
				if baseUniqueDef is None:
					newUniqueDef = uniqueDef
				else:
					newUniqueSet = copy.copy(baseUniqueDef.values)
					newUniqueSet.update(uniqueDef.values)
					newUniqueDef = IndexDef(
						indexLoc=baseUniqueDef.indexLoc,
						members=baseUniqueDef.members,
						name=baseUniqueDef.name,
						limit_scope=baseUniqueDef.limit_scope,
						values=newUniqueSet,
					)
				ConsolidatedUniqueWorld[unique_id] = newUniqueDef
				ConsolidatedUniqueWorldByName[uniqueDef.name] = newUniqueDef
		else:
			ConsolidatedUniqueWorld = self.IndexWorld
			ConsolidatedUniqueWorldByName = self.IndexWorldByName

		return CheckContext(
			schemaURI=self.schemaURI,
			context=IndexContext(
				ConsolidatedUniqueWorld, ConsolidatedUniqueWorldByName
			),
		)

	def invalidateCaches(self) -> None:
		self.warmedUp = False
		self.doPopulate = False
		self.gotIdsSet = None

	def cleanup(self) -> None:
		super().cleanup()
		if self.warmedUp:
			self.doPopulate = True
