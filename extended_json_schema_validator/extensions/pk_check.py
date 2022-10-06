#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import copy
import urllib.error
from typing import TYPE_CHECKING, cast
from urllib.parse import urljoin, urlparse
from urllib.request import Request, urlopen

from jsonschema.exceptions import ValidationError

from .abstract_check import CheckContext
from .unique_check import (
	ALLOWED_ATOMIC_VALUE_TYPES,
	UniqueDef,
	UniqueKey,
	UniqueLoc,
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

	class PKConfigDict(TypedDict, total=False):
		schema_prefix: str
		accept: str
		provider: Union[str, Sequence[str]]


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
		self.gotIdsSet: "Optional[MutableMapping[str, Sequence[str]]]" = None
		self.warmedUp = False
		self.PopulatedPKWorld: "MutableMapping[int, Any]" = dict()

	@property
	def triggerAttribute(self) -> str:
		return self.KeyAttributeNamePK

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
					},
				]
			}
		}

	@property
	def _errorReason(self) -> str:
		return self.SchemaErrorReasonPK

	###
	# Bootstrapping is done by unique_check implementation
	# which is inherited
	###

	def warmUpCaches(self) -> None:
		if not self.warmedUp:
			self.warmedUp = True

			setup = cast(
				"Optional[PKConfigDict]", self.config.get(self.triggerAttribute)
			)
			if setup is not None:
				prefix = setup.get("schema_prefix")
				accept = setup.get("accept")
				if prefix != self.schemaURI and accept is not None:
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
									gotIds = str(f.read(), "utf-8").split("\n")
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
		self, unique_id: int = -1, unique_state: "Union[bool, Sequence[str]]" = []
	) -> None:
		if self.doPopulate:
			# Deactivate future populations
			self.doPopulate = False

			if self.gotIdsSet:
				# The common dictionary for this declaration where all the unique values are kept
				allow_provider_duplicates = self.config.get(
					self.triggerAttribute, {}
				).get("allow_provider_duplicates", False)
				if allow_provider_duplicates:
					UniqueWorld = self.PopulatedPKWorld
				else:
					UniqueWorld = self.UniqueWorld

				uniqueDef = UniqueWorld.setdefault(
					unique_id,
					UniqueDef(
						uniqueLoc=UniqueLoc(schemaURI=self.schemaURI, path="(unknown)"),
						members=unique_state,
						values=dict(),
					),
				)
				uniqueSet = uniqueDef.values

				# Should it complain about this?
				for compURL, gotIds in self.gotIdsSet.items():
					for theValue in gotIds:
						if theValue in uniqueSet:
							raise ValidationError(
								"Duplicated {0} value -=> {1} <=-  (appeared in {2})".format(
									self.triggerAttribute, theValue, uniqueSet[theValue]
								),
								validator_value={"reason": self._errorReason},
							)
						else:
							uniqueSet[theValue] = compURL

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
			self.doDefaultPopulation(
				unique_id=unique_id,
				unique_state=cast("Union[bool, Sequence[str]]", unique_state),
			)

			if isinstance(unique_state, list):
				obtainedValues = self.GetKeyValues(value, unique_state)
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

			# The common dictionary for this declaration where all the unique values are kept
			uniqueDef = self.UniqueWorld.setdefault(
				unique_id,
				UniqueDef(
					uniqueLoc=UniqueLoc(schemaURI=self.schemaURI, path="(unknown)"),
					members=unique_state,
					values=dict(),
				),
			)
			uniqueSet = uniqueDef.values

			# Should it complain about this?
			for theValue in theValues:
				if theValue in uniqueSet:
					yield ValidationError(
						"Duplicated {0} value -=> {1} <=-  (appeared in {2})".format(
							self.triggerAttribute, theValue, uniqueSet[theValue]
						),
						validator_value={"reason": self._errorReason},
					)
				else:
					uniqueSet[theValue] = self.currentJSONFile

	def getContext(self) -> "Optional[CheckContext]":
		# These are needed to assure the context is always completely populated
		self.warmUpCaches()
		self.doDefaultPopulation()

		if len(self.PopulatedPKWorld) > 0:
			ConsolidatedUniqueWorld = copy.copy(self.PopulatedPKWorld)

			for unique_id, uniqueDef in self.UniqueWorld.items():
				baseUniqueDef = ConsolidatedUniqueWorld.get(unique_id)
				if baseUniqueDef is None:
					ConsolidatedUniqueWorld[unique_id] = uniqueDef
				else:
					newUniqueSet = baseUniqueDef.values.copy()
					newUniqueSet.update(uniqueDef.values)
					newUniqueDef = UniqueDef(
						uniqueLoc=baseUniqueDef.uniqueLoc,
						members=baseUniqueDef.members,
						values=newUniqueSet,
					)
					ConsolidatedUniqueWorld[unique_id] = newUniqueDef
		else:
			ConsolidatedUniqueWorld = self.UniqueWorld

		return CheckContext(schemaURI=self.schemaURI, context=ConsolidatedUniqueWorld)

	def invalidateCaches(self) -> None:
		self.warmedUp = False
		self.doPopulate = False
		self.gotIdsSet = None

	def cleanup(self) -> None:
		super().cleanup()
		if self.warmedUp:
			self.doPopulate = True
