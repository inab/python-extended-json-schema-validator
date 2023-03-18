#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import abc
import inspect
import logging
import os
import shutil
import tempfile
from typing import TYPE_CHECKING, NamedTuple, cast

import xdg  # type: ignore[import]

import jsonschema as JSV


class FeatureLoc(NamedTuple):
	id: str
	schemaURI: "str"
	fragment: "Optional[str]"
	path: str
	context: "Mapping[str, Any]"


if TYPE_CHECKING:
	from typing import (
		Any,
		Callable,
		ClassVar,
		Iterator,
		Mapping,
		MutableMapping,
		MutableSequence,
		Optional,
		Sequence,
		Set,
		Tuple,
		Type,
	)

	import jsonschema as JSV
	from jsonschema.exceptions import ValidationError
	from typing_extensions import Final, TypedDict

	class BootstrapErrorDict(TypedDict, total=False):
		reason: str
		description: str
		path: str
		schema_id: str

	class SecondPassErrorDict(BootstrapErrorDict):
		file: str

	class SchemaHashEntry(TypedDict, total=False):
		file: "str"
		schema: "Any"
		schema_hash: "str"
		errors: "MutableSequence[BootstrapErrorDict]"
		customFormatInstances: "Sequence[AbstractCustomFeatureValidator]"
		validator: "Type[JSV.validators._Validator]"
		ref_resolver: "JSV.RefResolver"
		resolved_schema: "Any"
		id_key: "str"
		uri: "str"

	KeyRefs = MutableMapping[str, MutableSequence[FeatureLoc]]
	Id2ElemId = MutableMapping[int, MutableMapping[str, MutableSequence[Any]]]
	JsonPointer2Val = MutableMapping[str, Any]
	RefSchemaTuple = Tuple[Id2ElemId, KeyRefs, JsonPointer2Val]

	FeatureValidatorConfig = Mapping[str, Any]

	ValidateCallable = Callable[[Any, Any, Any, Any], Iterator[ValidationError]]


class CheckContext(NamedTuple):
	schemaURI: str
	context: "Any"


class AbstractCustomFeatureValidator(abc.ABC):
	FRAGMENT_VALIDATOR: "Final[Type[JSV.validators._Validator]]" = (
		JSV.validators.Draft7Validator
	)

	def __init__(
		self,
		schemaURI: str,
		jsonSchemaSource: str = "(unknown)",
		config: "FeatureValidatorConfig" = {},
		isRW: bool = True,
	):
		self.logger = logging.getLogger(
			dict(inspect.getmembers(self))["__module__"]
			+ "::"
			+ self.__class__.__name__
		)

		self.schemaURI = schemaURI
		self.jsonSchemaSource = jsonSchemaSource
		self.config = config
		self.isRW = isRW
		self.bootstrapMessages = None
		self.currentJSONFile = "(unset)"

	CacheSubdir: "ClassVar[Optional[str]]" = None
	CachePathProp: "ClassVar[Optional[str]]" = None
	CacheProp: "ClassVar[Optional[str]]" = None
	TempCachePath: "ClassVar[Optional[str]]" = None

	@classmethod
	def GetCachePath(cls, cachePath: "Optional[str]" = None) -> "Optional[str]":
		if cls.CachePathProp is None:
			return cachePath

		if not hasattr(cls, cls.CachePathProp):
			doTempDir = False
			if cachePath is None:
				try:
					cachePath = xdg.BaseDirectory.save_cache_path(
						"es.elixir.jsonValidator"
					)
					# Is the directory writable?
					if not os.access(cachePath, os.W_OK):
						doTempDir = True
				except OSError as e:
					# As it was not possible to create the
					# directory at the cache path, go to the
					# temporary directory
					doTempDir = True

			assert cachePath is not None

			if doTempDir:
				if cls.TempCachePath is None:
					# The temporary directory should be
					# removed when the application using this
					# class finishes
					# cachePath = tempfile.mkdtemp(prefix="term", suffix="cache")
					# atexit.register(shutil.rmtree, cachePath, ignore_errors=True)
					cachePath = os.path.join(
						tempfile.gettempdir(), "cache_es.elixir.jsonValidator"
					)
					os.makedirs(cachePath, exist_ok=True)
					# This is needed to avoid creating several
					# temporary directories, one for each
					# extension, when cachePath is None
					cls.TempCachePath = cachePath
				else:
					cachePath = cls.TempCachePath

			# Does it need its own directory?
			if cls.CacheSubdir is not None:
				cachePath = os.path.join(cachePath, cls.CacheSubdir)
				os.makedirs(cachePath, exist_ok=True)

			setattr(cls, cls.CachePathProp, cachePath)

		return cast("Optional[str]", getattr(cls, cls.CachePathProp))

	@classmethod
	def InvalidateCache(cls, cachePath: "Optional[str]" = None) -> None:
		if (cls.CacheProp is not None) and hasattr(cls, cls.CacheProp):
			# Get the shared Cache instance
			cache = getattr(cls, cls.CacheProp)
			delattr(cls, cls.CacheProp)

			# Check whether it has invalidate method
			invalidate = getattr(cache, "invalidate", None)
			if callable(invalidate):
				invalidate()
			del cache

		if cls.CachePathProp is not None:
			rmCachePath = cls.GetCachePath(cachePath=cachePath)
			delattr(cls, cls.CachePathProp)
			if rmCachePath is not None:
				shutil.rmtree(rmCachePath, ignore_errors=True)

			# This second call assures the directory is again created
			cls.GetCachePath(cachePath=cachePath)

	@abc.abstractmethod
	def validate(
		self,
		validator: "JSV.validators._Validator",
		schema_attr_val: "Any",
		value: "Any",
		schema: "Any",
	) -> "Iterator[ValidationError]":
		pass

	@property
	@abc.abstractmethod
	def triggerAttribute(self) -> str:
		pass

	# It returns the list of validation methods,
	# along with the attributes to be hooked to
	def getValidators(self) -> "Sequence[Tuple[str, ValidateCallable]]":
		return [(self.triggerAttribute, self.validate)]

	@property
	@abc.abstractmethod
	def triggerJSONSchemaDef(self) -> "Mapping[str, Any]":
		pass

	@property
	def needsBootstrapping(self) -> bool:
		return False

	@property
	def needsSecondPass(self) -> bool:
		return False

	def _fragment_validate(self, j_to_val: "Any") -> "Iterator[ValidationError]":
		"""
		There can be more than one element to validate
		"""
		trigger_schema = {
			"$schema": self.FRAGMENT_VALIDATOR.META_SCHEMA["$schema"],
			"properties": self.triggerJSONSchemaDef,
		}

		return self.FRAGMENT_VALIDATOR(trigger_schema).iter_errors(j_to_val)

	# @property.currentJ.setter
	def setCurrentJSONFilename(self, newVal: str = "(unset)") -> None:
		self.currentJSONFile = newVal

	# This method should be used to initialize caches
	# and do some validations, returning errors in an array
	def bootstrap(
		self, refSchemaTuple: "RefSchemaTuple" = ({}, {}, {})
	) -> "Sequence[BootstrapErrorDict]":
		return []

	# This method should be used to invalidate the cached contents
	# needed for the proper work of the extension
	def invalidateCaches(self) -> None:
		self.InvalidateCache(
			cachePath=cast("Optional[str]", self.config.get("cacheDir"))
		)

	# This method should be used to warm up the cached contents
	# needed for the proper work of the extension
	# It is forcedly run before the second validation pass
	def warmUpCaches(self) -> None:
		pass

	# This method should be used to apply a second pass in this instance, with all
	# the information from other instances. It returns an array of ValidationErrors
	# It is run after the forced cached warmup, and before the cleanup
	def doSecondPass(
		self, l_customFeatureValidators: "Mapping[str, Sequence[CheckContext]]"
	) -> "Tuple[Set[str], Set[str], Sequence[SecondPassErrorDict]]":
		return set(), set(), []

	# This method should be used to share the context of the extension
	# which is usually needed on second pass works. It must return
	# "CheckContext" named tuples
	def getContext(self) -> "Optional[CheckContext]":
		return None

	# It should be run after all the second validation passes are run
	# By default, it is a no-op
	def cleanup(self) -> None:
		pass
