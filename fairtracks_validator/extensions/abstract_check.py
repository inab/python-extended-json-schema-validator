#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import abc
from collections import namedtuple

import copy

CheckContext = namedtuple('CheckContext',['schemaURI','context'])

class AbstractCustomFeatureValidator(abc.ABC):
	def __init__(self,schemaURI, jsonSchemaSource='(unknown)', config = {}):
		self.schemaURI = schemaURI
		self.jsonSchemaSource = jsonSchemaSource
		self.config = config
		self.bootstrapMessages = None
		self.currentJSONFile = '(unset)'

	@abc.abstractmethod
	def validate(self,validator,schema_attr_val,value,schema):
		pass
	
	@property
	@abc.abstractmethod
	def triggerAttribute(self):
		pass
	
	@property
	def bootstrapAttribute(self):
		return self.triggerAttribute
	
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
		pass
	
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
