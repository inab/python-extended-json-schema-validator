#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import abc
from jsonschema.exceptions import ValidationError
from collections import namedtuple

import copy

from ..extend_validator import extendValidator, PLAIN_VALIDATOR_MAPPER

CheckContext = namedtuple('CheckContext',['schemaURI','context'])

class AbstractCustomFeatureValidator(abc.ABC):
	FAIL_KEY = 'fail'
	FAIL_MSG = 'Found'
	
	def __init__(self,schemaURI, config = {}):
		self.schemaURI = schemaURI
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
	
	# This method is there to fail, so as side effect, we collect where
	# the key is happening
	def _trackFailOcurrences(self,validator,schema_attr_val,value,schema):
		yield ValidationError(self.FAIL_MSG,validator_value={'f_id': id(value),'f_val': value})
	
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
	# By default, it is saving all at self.bootstrapMessages
	# so all the subclasses should call this if the topology is needed
	def bootstrap(self,metaSchemaURI,jsonSchema):
		if self.needsBootstrapping and (self.bootstrapMessages is None):
			plain_validator = PLAIN_VALIDATOR_MAPPER.get(metaSchemaURI)
			
			extSchemaDef = copy.deepcopy(self.triggerJSONSchemaDef)
			bootstrapAttribute = self.bootstrapAttribute
			for attrName, elem in extSchemaDef.items():
				if (bootstrapAttribute is None) or (attrName == bootstrapAttribute):
					elem[self.FAIL_KEY] = True
					#notIsOf = True
					#for ofKey in ('oneOf','anyOf','allOf'):
					#	if ofKey in elem:
					#		notIsOf = False
					#		for subelem in elem[ofKey]:
					#			subelem[self.FAIL_KEY] = True
					#if notIsOf:
					#	elem[self.FAIL_KEY] = True
			
			bootstrapJSONSchema = plain_validator.META_SCHEMA.copy()
			bootstrapJSONSchema['properties'].update(extSchemaDef)
			
			# The way to get the location of all the occurrences of the key is using a custom validator
			# which always fails when the key is found
			bootstrap_validator , _ = extendValidator(metaSchemaURI, plain_validator, {}, { self.FAIL_KEY: self._trackFailOcurrences })
			
			self.bootstrapMessages = []
			
			for valError in bootstrap_validator(bootstrapJSONSchema).iter_errors(jsonSchema):
				# Capture only ourselves
				if valError.message == self.FAIL_MSG:
					self.bootstrapMessages.append({ 'path': "/".join(map(lambda e: str(e),valError.path)), 'v': valError.validator_value })
	
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
		return []
	
	# This method should be used to share the context of the extension
	# which is usually needed on second pass works. It must return
	# "CheckContext" named tuples
	def getContext(self):
		return None
	
	# It should be run after all the second validation passes are run
	# By default, it is a no-op
	def cleanup(self):
		pass
