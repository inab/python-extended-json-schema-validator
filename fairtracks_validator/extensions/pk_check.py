#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from jsonschema.exceptions import FormatError, ValidationError

from .unique_check import UniqueKey, ALLOWED_KEY_TYPES, ALLOWED_ATOMIC_VALUE_TYPES

import sys
import re
import json

class PrimaryKey(UniqueKey):
	KeyAttributeName = 'primary_key'
	SchemaErrorReason = 'dup_pk'
	
	# Each instance represents the set of keys from one ore more JSON Schemas
	def __init__(self,schemaURI):
		super().__init__(schemaURI)
	
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
	
	#@property
	#def needsBootstrapping(self):
	#	return True
		
	#def bootstrap(self, metaSchemaURI, jsonSchema):
	#	super().bootstrap(metaSchemaURI,jsonSchema)
	#	
	#	# Now, save the physical occurrence of the 
	#	import pprint , sys
	#	pprint.pprint(self.bootstrapMessages,stream=sys.stderr)
	#	sys.stderr.flush()
	
	#def validate(self,validator,pk_state,value,schema):
	#	if pk_state:
	#		print(id(pk_state),file=sys.stderr)
	#		print(id(value),file=sys.stderr)
	#		print(id(schema),file=sys.stderr)
	#		sys.stderr.flush()
	#		
	#		if isinstance(pk_state,list):
	#			obtainedValues = self.GetKeyValues(value,pk_state)
	#		else:
	#			obtainedValues = [(value,)]
	#		
	#		isAtomicValue = len(obtainedValues) == 1 and len(obtainedValues[0]) == 1 and isinstance(obtainedValues[0][0], ALLOWED_ATOMIC_VALUE_TYPES)
	#		
	#		if isAtomicValue:
	#			theValues = [ obtainedValues[0][0] ]
	#		else:
	#			theValues = self.GenKeyStrings(obtainedValues)
	#		
	#		# Check the unicity
	#		pk_id = id(schema)
	#		
	#		# The common dictionary where all the unique values are kept
	#		pkSet = self.UniqueWorld.setdefault(pk_id,set())
	#		
	#		# Should it complain about this?
	#		for theValue in theValues:
	#			if theValue in pkSet:
	#				yield ValidationError("Value -=> {0} <=-  is duplicated".format(theValue))
	#			else:
	#				pkSet.add(theValue)
