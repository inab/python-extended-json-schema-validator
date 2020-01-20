#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from collections import namedtuple

from .abstract_check import AbstractCustomFeatureValidator

# We need this for its class methods
from .unique_check import UniqueKey, ALLOWED_KEY_TYPES, ALLOWED_ATOMIC_VALUE_TYPES

from jsonschema.exceptions import FormatError, ValidationError

import sys
import re
import json

import uritools

FPDef = namedtuple('FPDef',['schemaURI','refSchemaURI','path','refPath','values'])
FPVal = namedtuple('FPVal',['value','where'])

class ForeignProperty(AbstractCustomFeatureValidator):
	KeyAttributeName = 'foreignProperty'
	SchemaAttributeName = '@schema'
	SchemaErrorReason = 'stale_fp'
	DanglingFPErrorReason = 'dangling_fp'
	
	# Each instance represents the set of keys from one ore more JSON Schemas
	def __init__(self,schemaURI, jsonSchemaSource='(unknown)',config={}):
		super().__init__(schemaURI,jsonSchemaSource,config)
		self.FPWorld = dict()
	
	@property
	def triggerAttribute(self):
		return self.KeyAttributeName
	
	@property
	def triggerJSONSchemaDef(self):
		return {
			self.KeyAttributeName : {
				"type": "string",
				"format": "uri-reference",
				"minLenght": 1
			}
		}
	
	@property
	def _errorReason(self):
		return self.SchemaErrorReason
	
	@property
	def needsBootstrapping(self):
		return True
	
	@property
	def needsSecondPass(self):
		return True
	
	def bootstrap(self, refSchemaTuple = tuple()):
		(id2ElemId , keyRefs , refSchemaCache) = refSchemaTuple
		
		keyList = keyRefs[self.triggerAttribute]
		errors = []
		# Saving the unique locations
		# based on information from FeatureLoc elems
		for loc in keyList:
			fp_def = loc.context[self.triggerAttribute]
			fp_loc_id = id(loc.context)
			
			# Getting the absolute schema id and the route
			if uritools.isabsuri(self.schemaURI):
				abs_ref_schema_id , rel_json_pointer = uritools.uridefrag(uritools.urijoin(self.schemaURI,fp_def))
			else:
				abs_ref_schema_id , rel_json_pointer = uritools.uridefrag(fp_def)
			
			if abs_ref_schema_id not in refSchemaCache:
				errors.append({
					'reason': 'fp_no_schema',
					'description': "No schema with {0} id, required by {1} ({2})".format(abs_ref_schema_id,self.jsonSchemaSource,self.schemaURI)
				})
				
			fpDefH = self.FPWorld.setdefault(abs_ref_schema_id,{})
			
			# This control is here for same primary key referenced from multiple cases
			fpDefH[fp_loc_id] = FPDef(schemaURI=self.schemaURI,refSchemaURI=abs_ref_schema_id,path=loc.path,refPath=rel_json_pointer,values=list())
		
		return errors
	
	# This step is only going to gather all the values tied to foreign properties
	def validate(self,validator,fp_def,value,schema):
		if fp_def and isinstance(fp_def,str):
			fp_loc_id = id(schema)
			
			# Getting the absolute schema id and the route
			if uritools.isabsuri(self.schemaURI):
				abs_ref_schema_id , rel_json_pointer = uritools.uridefrag(uritools.urijoin(self.schemaURI,fp_def))
			else:
				abs_ref_schema_id , rel_json_pointer = uritools.uridefrag(fp_def)
			fpDef = self.FPWorld.setdefault(abs_ref_schema_id,{}).get(fp_loc_id)
			
			# And getting the foreign property definition
			if fpDef is None:
				fpDef = FPDef(schemaURI=self.schemaURI,refSchemaURI=abs_ref_schema_id,path='(unknown {})'.format(fp_loc_id),refPath=rel_json_pointer,values=list())
				self.FPWorld[abs_ref_schema_id][fp_loc_id] = fpDef
			
			obtainedValues = [(value,)]
			
			isAtomicValue = len(obtainedValues) == 1 and len(obtainedValues[0]) == 1 and isinstance(obtainedValues[0][0], ALLOWED_ATOMIC_VALUE_TYPES)
			
			if isAtomicValue:
				theValues = [ obtainedValues[0][0] ]
			else:
				theValues = UniqueKey.GenKeyStrings(obtainedValues)
			
			fpVals = fpDef.values
			
			# Second pass will do the validation
			for theValue in theValues:
				fpVals.append(FPVal(where=self.currentJSONFile,value=theValue))
	
	# Now, time to check
	def doSecondPass(self,l_customFeatureValidatorsContext):
		errors = []
		
		uniqueContextsHash = {}
		for className, uniqueContexts in l_customFeatureValidatorsContext.items():
			# This instance is only interested in primary keys
			if className == UniqueKey.__name__:
				for uniqueContext in uniqueContexts:
					# Getting the path correspondence
					for uniqueDef in uniqueContext.context.values():
						uLoc = uniqueDef.uniqueLoc
						# As there can be nested keys from other schemas
						# ignore the schemaURI from the context, and use
						# the one in the unique location
						uCH = uniqueContextsHash.setdefault(uLoc.schemaURI,{})
						# As this is a path inside the JSON schema instead of
						# the JSON, translate it
						transPath = uLoc.path
						for keyword in ['properties','items','anyOf','allOf','someOf']:
							transPath = transPath.replace('/'+keyword+'/','/')
						if transPath.endswith('/'+UniqueKey.KeyAttributeName):
							transPath = transPath[0:-(len(UniqueKey.KeyAttributeName)+1)]
						
						uCH.setdefault(transPath,[]).append(uniqueDef.values)
		
		# Now, at last, check!!!!!!!
		uniqueWhere = set()
		uniqueFailedWhere = set()
		for refSchemaURI,fpDefH in self.FPWorld.items():
			for fp_loc_id , fpDef in fpDefH.items():
				fpPath = '/' + fpDef.refPath
				checkValuesList = None
				uCH = uniqueContextsHash.get(refSchemaURI)
				if uCH is not None:
					checkValuesList = uCH.get(fpPath)
				
				if checkValuesList is not None:
					for fpVal in fpDef.values:
						uniqueWhere.add(fpVal.where)
						
						fpString = fpVal.value
						found = False
						for checkValues in checkValuesList:
							if fpString in checkValues:
								found = True
								break
						
						if not found:
							uniqueFailedWhere.add(fpVal.where)
							errors.append({
								'reason': 'stale_fp',
								'description': "Unmatching foreign property ({0}) in {1} to schema {2} in {3}".format(fpString,fpVal.where,refSchemaURI,fpDef.refPath),
								'file': fpVal.where,
								'path': fpDef.path
							})
				else:
					for fpVal in fpDef.values:
						uniqueWhere.add(fpVal.where)
						uniqueFailedWhere.add(fpVal.where)
						errors.append({
							'reason': self.DanglingFPErrorReason,
							'description': "No available documents from {0} schema, required by {1}".format(refSchemaURI,self.schemaURI),
							'file': fpVal.where,
							'path': fpDef.path
						})
		
		return uniqueWhere,uniqueFailedWhere,errors
	
	def cleanup(self):
		# In order to not destroying the bootstrapping work
		# only remove the recorded values
		for fpDefH in self.FPWorld.values():
			for fpDef in fpDefH.values():
				fpDef.values.clear()
