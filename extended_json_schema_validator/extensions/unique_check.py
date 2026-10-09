#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# SPDX-License-Identifier: LGPL-2.1-or-later
# Copyright 2018-2026 Barcelona Supercomputing Center (BSC), Spain
#
# This library is free software; you can redistribute it and/or modify
# it under the terms of the GNU Lesser General Public License as
# published by the Free Software Foundation; either version 2.1 of the
# License, or (at your option) any later version.
#
# This library is distributed in the hope that it will be useful, but
# WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU Lesser General Public License for more details.
#
# You should have received a copy of the GNU Lesser General Public
# License along with this library; if not, write to the
# Free Software Foundation, Inc., 51 Franklin Street, Fifth Floor,
# Boston, MA 02110-1301 USA

from jsonschema.exceptions import ValidationError

from .index_check import (
	IndexDef,
	IndexKey,
	IndexLoc,
	ALLOWED_ATOMIC_VALUE_TYPES,
)

from typing import TYPE_CHECKING

if TYPE_CHECKING:
	from typing import (
		Any,
		Iterator,
		Optional,
		Tuple,
		Union,
	)

	import jsonschema as JSV
	from typing_extensions import Final

	from .abstract_check import (
		FeatureValidatorConfig,
	)


class UniqueKey(IndexKey):
	KeyAttributeNameUK: "Final[str]" = "unique"
	SchemaErrorReasonUK: "Final[str]" = "dup_unique"

	# Each instance represents the set of keys from one ore more JSON Schemas
	def __init__(
		self,
		schemaURI: str,
		jsonSchemaSource: str = "(unknown)",
		config: "Optional[FeatureValidatorConfig]" = None,
		isRW: bool = True,
	):
		super().__init__(
			schemaURI, jsonSchemaSource=jsonSchemaSource, config=config, isRW=isRW
		)

	@property
	def triggerAttribute(self) -> str:
		return self.KeyAttributeNameUK

	@property
	def randomKeyPrefix(self) -> str:
		return "unique"

	@property
	def _errorReason(self) -> str:
		return self.SchemaErrorReasonUK

	###
	# Bootstrapping is done by index_check implementation
	# which is inherited
	###

	def validate(
		self,
		validator: "JSV.validators._Validator",
		unique_state: "Any",
		value: "Any",
		schema: "Any",
	) -> "Iterator[ValidationError]":
		if unique_state:
			# Check the unicity
			unique_siblings_id = id(schema)

			# The common dictionary for this declaration where all the unique values are kept
			unique_siblings = self.IndexWorldSiblings.get(unique_siblings_id, set())
			for unique_id in unique_siblings:
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

					uniqueDef = IndexDef(
						indexLoc=IndexLoc(schemaURI=self.schemaURI, path="(unknown)"),
						members=unique_members,
						name=unique_name,
						limit_scope=limit_scope,
						values=dict(),
					)
					self.IndexWorld[unique_id] = uniqueDef
					if unique_name in self.IndexWorldByName:
						self.logger.warning(
							f"Repeated named {self.randomKeyPrefix} '{unique_name}'. Be prepared for hairy responses."
						)
					else:
						self.IndexWorldByName[unique_name] = uniqueDef

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
						uniqueSet[theValue] = set([self.currentJSONFile])
