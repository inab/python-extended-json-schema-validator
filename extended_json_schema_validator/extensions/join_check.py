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

from typing import TYPE_CHECKING

# We need this for its class methods
from .fk_check import AbstractRefKey
from .index_check import (
	IndexKey,
)

if TYPE_CHECKING:
	from typing import (
		Optional,
	)
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
		config: "Optional[FeatureValidatorConfig]" = None,
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
