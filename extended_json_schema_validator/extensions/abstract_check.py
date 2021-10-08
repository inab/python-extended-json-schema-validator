#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import abc
import atexit
import copy
import logging
import os
import shutil
import xdg
import tempfile
from typing import Any, NamedTuple

class CheckContext(NamedTuple):
	schemaURI: str
	context: Any

class AbstractCustomFeatureValidator(abc.ABC):
	def __init__(self, schemaURI, jsonSchemaSource='(unknown)', config = {}, isRW = True):
		self.logger = logging.getLogger(self.__class__.__name__)
		
		self.schemaURI = schemaURI
		self.jsonSchemaSource = jsonSchemaSource
		self.config = config
		self.isRW = isRW
		self.bootstrapMessages = None
		self.currentJSONFile = '(unset)'
	
	CacheSubdir = None
	CachePathProp = None
	CacheProp = None
	
	@classmethod
	def GetCachePath(cls, cachePath = None):
		if cls.CachePathProp is None:
			return cachePath
		
		if not hasattr(cls, cls.CachePathProp):
			doTempDir = False
			if cachePath is None:
				try:
					cachePath = xdg.BaseDirectory.save_cache_path('es.elixir.jsonValidator')
					# Is the directory writable?
					if not os.access(cachePath,os.W_OK):
						doTempDir = True
				except OSError as e:
					# As it was not possible to create the
					# directory at the cache path, go to the
					# temporary directory
					doTempDir = True
			
			if doTempDir:
				# The temporary directory should be
				# removed when the application using this
				# class finishes
				#cachePath = tempfile.mkdtemp(prefix="term", suffix="cache")
				#atexit.register(shutil.rmtree, cachePath, ignore_errors=True)
				cachePath = os.path.join(tempfile.gettempdir(),'cache_es.elixir.jsonValidator')
				os.makedirs(cachePath, exist_ok=True)
			
			# Does it need its own directory?
			if cls.CacheSubdir is not None:
				cachePath = os.path.join(cachePath, cls.CacheSubdir)
				os.makedirs(cachePath, exist_ok=True)
			
			setattr(cls, cls.CachePathProp, cachePath)
		
		return getattr(cls, cls.CachePathProp)
	
	@classmethod
	def InvalidateCache(cls, cachePath=None):
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
			cachePath = cls.GetCachePath(cachePath=cachePath)
			delattr(cls, cls.CachePathProp)
			shutil.rmtree(cachePath, ignore_errors=True)
	
	@abc.abstractmethod
	def validate(self,validator,schema_attr_val,value,schema):
		pass
	
	@property
	@abc.abstractmethod
	def triggerAttribute(self):
		pass
	
	# It returns the list of validation methods,
	# along with the attributes to be hooked to
	def getValidators(self):
		return [(self.triggerAttribute,self.validate)]
	
	@property
	@abc.abstractmethod
	def triggerJSONSchemaDef(self):
		pass
	
	@property
	def needsBootstrapping(self):
		return False
	
	@property
	def needsSecondPass(self):
		return False
	
	#@property.currentJ.setter
	def setCurrentJSONFilename(self,newVal='(unset)'):
		self.currentJSONFile = newVal
	
	# This method should be used to initialize caches
	# and do some validations, returning errors in an array
	def bootstrap(self, refSchemaTuple = tuple()):
		return []
	
	# This method should be used to invalidate the cached contents
	# needed for the proper work of the extension
	def invalidateCaches(self):
		self.InvalidateCache(cachePath=self.config.get('cacheDir'))
	
	# This method should be used to warm up the cached contents
	# needed for the proper work of the extension
	# It is forcedly run before the second validation pass
	def warmUpCaches(self):
		pass
	
	# This method should be used to apply a second pass in this instance, with all
	# the information from other instances. It returns an array of ValidationErrors
	# It is run after the forced cached warmup, and before the cleanup
	def doSecondPass(self,l_customFeatureValidators):
		return set() , set() , []
	
	# This method should be used to share the context of the extension
	# which is usually needed on second pass works. It must return
	# "CheckContext" named tuples
	def getContext(self):
		return None
	
	# It should be run after all the second validation passes are run
	# By default, it is a no-op
	def cleanup(self):
		pass
