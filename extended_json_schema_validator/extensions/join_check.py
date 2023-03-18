#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from typing import TYPE_CHECKING

# We need this for its class methods
from .fk_check import AbstractRefKey
from .index_check import (
	IndexKey,
)

if TYPE_CHECKING:
	from typing_extensions import Final

	from .abstract_check import (
		FeatureValidatorConfig,
	)


class JoinKey(AbstractRefKey):
	KeyAttributeNameJK: "Final[str]" = "join_keys"
	SchemaErrorReasonJK: "Final[str]" = "stale_jk"
	DanglingJKErrorReason: "Final[str]" = "dangling_jk"

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
			joinClass=IndexKey,
			jsonSchemaSource=jsonSchemaSource,
			config=config,
			isRW=isRW,
		)

	@property
	def triggerAttribute(self) -> str:
		return self.KeyAttributeNameJK

	@property
	def _errorReason(self) -> str:
		return self.SchemaErrorReasonJK

	@property
	def _danglingErrorReason(self) -> "str":
		return self.DanglingJKErrorReason
