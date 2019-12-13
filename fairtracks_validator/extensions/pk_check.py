#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from jsonschema.exceptions import FormatError, ValidationError

from .unique_check import UniqueKey, ALLOWED_KEY_TYPES, ALLOWED_ATOMIC_VALUE_TYPES

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
	def __init__(self,schemaURI,config={}):
		super().__init__(schemaURI,config)
		self.doPopulate = False
		self.gotIds = None
		self.warmedUp = False
		self.compURL = None
	
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
	
	def warmUpCaches(self):
		self.warmedUp = True
		setup = self.config.get(self.KeyAttributeName)
		if setup is not None:
			prefix = setup.get('schema_prefix')
			if prefix != self.schemaURI:
				url_base = setup.get('provider')
				accept = setup.get('accept')
				if (url_base is not None) and (accept is not None):
					# Fetch the ids, based on the id
					relColId = urlparse(self.schemaURI).path.split('/')[-1]
					compURL = urljoin(url_base,relColId + '/')
					r = Request(compURL,headers={'Accept': accept})
					
					try:
						with urlopen(r) as f:
							if f.getcode() == 200:
								self.gotIds = str(f.read(),'utf-8').split()
								self.doPopulate = len(self.gotIds) > 0
								if self.doPopulate:
									self.compURL = compURL
					except urllib.error.HTTPError as he:
						print("ERROR: Unable to fetch remote keys data [{0}]: {1}".format(he.code,he.reason), file=sys.stderr)
					except urllib.error.URLError as ue:
						print("ERROR: Unable to fetch remote keys data: {0}".format(ue.reason), file=sys.stderr)
					except:
						print("ERROR: Unable to parse remote keys data from "+compURL, file=sys.stderr)
	
	def validate(self,validator,unique_state,value,schema):
		if not self.warmedUp:
			self.warmUpCaches()
		
		if unique_state and self.doPopulate:
			# Deactivate future populations
			self.doPopulate = False
			if self.gotIds:
				# Needed to populate the cache of ids
				unique_id = id(schema)
				
				# The common dictionary for this declaration where all the unique values are kept
				uniqueDef = self.UniqueWorld.setdefault(unique_id,UniqueDef(uniqueLoc=UniqueLoc(schemaURI=self.schemaURI,path='(unknown)'),members=unique_state,values=dict()))
				uniqueSet = uniqueDef.values
				
				# Should it complain about this?
				for theValue in self.gotIds:
					if theValue in uniqueSet:
						yield ValidationError("Duplicated {0} value -=> {1} <=-  (appeared in {2})".format(self.triggerAttribute, theValue,uniqueSet[theValue]),validator_value={"reason": self._errorReason})
					else:
						uniqueSet[theValue] = self.compURL
		
		super().validate(validator,unique_state,value,schema)
	
	def invalidateCaches(self):
		self.warmedUp = False
		self.gotIds = None
	
	def cleanup(self):
		super().cleanup()
		if self.warmedUp:
			self.doPopulate = True