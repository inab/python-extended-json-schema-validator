#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from jsonschema.exceptions import FormatError, ValidationError

from .unique_check import UniqueKey, UniqueDef, UniqueLoc, ALLOWED_KEY_TYPES, ALLOWED_ATOMIC_VALUE_TYPES

import sys
import re
import json

from urllib.request import Request, urlopen
from urllib.parse import urlparse, urljoin
import urllib.error

import codecs

class PrimaryKey(UniqueKey):
	KeyAttributeName = 'primary_key'
	SchemaErrorReason = 'dup_pk'
	
	# Each instance represents the set of keys from one ore more JSON Schemas
	def __init__(self,schemaURI, jsonSchemaSource='(unknown)', config={}, isRW=True):
		super().__init__(schemaURI, jsonSchemaSource, config, isRW=isRW)
		self.doPopulate = False
		self.gotIdsSet = None
		self.warmedUp = False
	
	@property
	def triggerAttribute(self):
		return self.KeyAttributeName
	
	@property
	def triggerJSONSchemaDef(self):
		return {
			self.triggerAttribute : {
				"oneOf": [
					{
						"type": "boolean"
					}
					,
					{
						"type": "array",
						"items": {
							"type": "string",
							"minLength": 1
						},
						"uniqueItems": True
					}
				]
			}
		}
	
	@property
	def _errorReason(self):
		return self.SchemaErrorReason
	
	###
	# Bootstrapping is done by unique_check implementation
	# which is inherited
	###
	
	def warmUpCaches(self):
		if not self.warmedUp:
			self.warmedUp = True
			
			setup = self.config.get(self.KeyAttributeName)
			if setup is not None:
				prefix = setup.get('schema_prefix')
				accept = setup.get('accept')
				if prefix != self.schemaURI and accept is not None:
					self.gotIdsSet = {}
					
					# The list of sources
					url_base_list = setup.get('provider',[])
					if not isinstance(url_base_list,(list,tuple)):
						url_base_list = [ url_base_list ]
					
					for url_base in url_base_list:
						# Fetch the ids, based on the id
						relColId = urlparse(self.schemaURI).path.split('/')[-1]
						compURL = urljoin(url_base,relColId + '/')
						r = Request(compURL,headers={'Accept': accept})
						
						try:
							with urlopen(r) as f:
								if f.getcode() == 200:
									gotIds = str(f.read(),'utf-8').split()
									if gotIds:
										self.gotIdsSet[compURL] = gotIds
										self.doPopulate = True
						except urllib.error.HTTPError as he:
							self.logger.error("ERROR: Unable to fetch remote keys data from {0} [{1}]: {2}".format(compURL,he.code,he.reason))
						except urllib.error.URLError as ue:
							self.logger.error("ERROR: Unable to fetch remote keys data from {0}: {1}".format(compURL,ue.reason))
						except:
							self.logger.exception("ERROR: Unable to parse remote keys data from "+compURL)
	
	def doDefaultPopulation(self):
		if self.doPopulate:
			# Deactivate future populations
			self.doPopulate = False
			
			unique_id = -1
			if self.gotIdsSet:
				# The common dictionary for this declaration where all the unique values are kept
				uniqueDef = self.UniqueWorld.setdefault(unique_id,UniqueDef(uniqueLoc=UniqueLoc(schemaURI=self.schemaURI,path='(unknown)'),members=[],values=dict()))
				uniqueSet = uniqueDef.values
				
				# Should it complain about this?
				for compURL, gotIds in self.gotIdsSet.items():
					for theValue in gotIds:
						if theValue in uniqueSet:
							raise ValidationError("Duplicated {0} value -=> {1} <=-  (appeared in {2})".format(self.triggerAttribute, theValue,uniqueSet[theValue]),validator_value={"reason": self._errorReason})
						else:
							uniqueSet[theValue] = compURL
		
	
	def validate(self,validator,unique_state,value,schema):
		self.warmUpCaches()
		
		# Populating before the validation itself
		if unique_state:
			# Needed to populate the cache of ids
			# and the unicity check
			unique_id = id(schema)
			if self.doPopulate:
				# Deactivate future populations
				self.doPopulate = False
				if self.gotIdsSet:
					# The common dictionary for this declaration where all the unique values are kept
					uniqueDef = self.UniqueWorld.setdefault(unique_id,UniqueDef(uniqueLoc=UniqueLoc(schemaURI=self.schemaURI,path='(unknown)'),members=unique_state,values=dict()))
					uniqueSet = uniqueDef.values
					
					# Should it complain about this?
					for compURL, gotIds in self.gotIdsSet.items():
						for theValue in gotIds:
							if theValue in uniqueSet:
								yield ValidationError("Duplicated {0} value -=> {1} <=-  (appeared in {2})".format(self.triggerAttribute, theValue,uniqueSet[theValue]),validator_value={"reason": self._errorReason})
							else:
								uniqueSet[theValue] = compURL
			
			if isinstance(unique_state,list):
				obtainedValues = self.GetKeyValues(value,unique_state)
			else:
				obtainedValues = [(value,)]
			
			isAtomicValue = len(obtainedValues) == 1 and len(obtainedValues[0]) == 1 and isinstance(obtainedValues[0][0], ALLOWED_ATOMIC_VALUE_TYPES)
			
			if isAtomicValue:
				theValues = [ obtainedValues[0][0] ]
			else:
				theValues = self.GenKeyStrings(obtainedValues)
			
			# The common dictionary for this declaration where all the unique values are kept
			uniqueDef = self.UniqueWorld.setdefault(unique_id,UniqueDef(uniqueLoc=UniqueLoc(schemaURI=self.schemaURI,path='(unknown)'),members=unique_state,values=dict()))
			uniqueSet = uniqueDef.values
			
			# Should it complain about this?
			for theValue in theValues:
				if theValue in uniqueSet:
					yield ValidationError("Duplicated {0} value -=> {1} <=-  (appeared in {2})".format(self.triggerAttribute, theValue,uniqueSet[theValue]),validator_value={"reason": self._errorReason})
				else:
					uniqueSet[theValue] = self.currentJSONFile
	
	def getContext(self):
		# These are needed to assure the context is always completely populated
		self.warmUpCaches()
		self.doDefaultPopulation()
		
		return super().getContext()
	
	def invalidateCaches(self):
		self.warmedUp = False
		self.doPopulate = False
		self.gotIdsSet = None
	
	def cleanup(self):
		super().cleanup()
		if self.warmedUp:
			self.doPopulate = True
