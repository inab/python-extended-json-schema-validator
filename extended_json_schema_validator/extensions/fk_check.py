#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from collections import namedtuple

from .abstract_check import AbstractCustomFeatureValidator
from .unique_check import ALLOWED_KEY_TYPES, ALLOWED_ATOMIC_VALUE_TYPES

# We need this for its class methods
from .pk_check import PrimaryKey

from jsonschema.exceptions import FormatError, ValidationError

import sys
import re
import json

import uritools

FKLoc = namedtuple('FKLoc',['schemaURI','refSchemaURI','path','values'])
FKDef = namedtuple('FKDef',['fkLoc','members'])
FKVal = namedtuple('FKVal',['value','where'])

class ForeignKey(AbstractCustomFeatureValidator):
	KeyAttributeName = 'foreign_keys'
	SchemaErrorReason = 'stale_fk'
	DanglingFKErrorReason = 'dangling_fk'
	
	# Each instance represents the set of keys from one ore more JSON Schemas
	def __init__(self,schemaURI, jsonSchemaSource='(unknown)', config={}, isRW=True):
		super().__init__(schemaURI, jsonSchemaSource, config, isRW=isRW)
		self.FKWorld = dict()
	
	@property
	def triggerAttribute(self):
		return self.KeyAttributeName
	
	@property
	def triggerJSONSchemaDef(self):
		return {
			self.KeyAttributeName : {
				"type": "array",
				"items": {
					"type": "object",
					"properties": {
						"schema_id": {
							"type": "string",
							"format": "uri-reference",
							"minLength": 1
						},
						"members": {
							"type": "array",
							"uniqueItems": True,
							"minItems": 1,
							"items": {
								"type": "string",
								"minLength": 1
							}
							
						}
					},
					"required": ["schema_id","members"]
				},
				"uniqueItems": True
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
			fk_defs = loc.context[self.triggerAttribute]
			fk_defs_gid = str(id(loc.context))
			
			#fk_defs_gid = loc.path
			for fk_loc_i, p_FK_decl in enumerate(fk_defs):
				fk_loc_id = fk_defs_gid + '_' + str(fk_loc_i)
				ref_schema_id = p_FK_decl['schema_id']
				if uritools.isabsuri(self.schemaURI):
					abs_ref_schema_id = uritools.urijoin(self.schemaURI,ref_schema_id)
				else:
					abs_ref_schema_id = ref_schema_id
				
				if abs_ref_schema_id not in refSchemaCache:
					errors.append({
						'reason': 'fk_no_schema',
						'description': "No schema with {0} id, required by {1} ({2})".format(abs_ref_schema_id,self.jsonSchemaSource,self.schemaURI)
					})
				
				fk_members = p_FK_decl.get('members',[])
				fkLoc = FKLoc(schemaURI=self.schemaURI,refSchemaURI=abs_ref_schema_id,path=loc.path+'/'+str(fk_loc_i),values=list())
				fk_id = abs_ref_schema_id
				fkDefH = self.FKWorld.setdefault(fk_id,{})
				
				# This control is here for same primary key referenced from multiple cases
				fkDefH[fk_loc_id] = FKDef(fkLoc=fkLoc,members=fk_members)
		
		return errors
	
	# This step is only going to gather all the foreign keys
	def validate(self,validator,fk_defs,value,schema):
		if fk_defs and isinstance(fk_defs,(list,tuple)):
			fk_defs_gid = str(id(schema))
			for fk_loc_i, p_FK_decl in enumerate(fk_defs):
				fk_loc_id = fk_defs_gid + '_' + str(fk_loc_i)
				ref_schema_id = p_FK_decl['schema_id']
				if uritools.isabsuri(self.schemaURI):
					abs_ref_schema_id = uritools.urijoin(self.schemaURI,ref_schema_id)
				else:
					abs_ref_schema_id = ref_schema_id
				
				fk_members = p_FK_decl.get('members',[])
				if isinstance(fk_members,list):
					obtainedValues = PrimaryKey.GetKeyValues(value,fk_members)
				else:
					obtainedValues = [(value,)]
				
				isAtomicValue = len(obtainedValues) == 1 and len(obtainedValues[0]) == 1 and isinstance(obtainedValues[0][0], ALLOWED_ATOMIC_VALUE_TYPES)
				
				if isAtomicValue:
					theValues = [ obtainedValues[0][0] ]
				else:
					theValues = PrimaryKey.GenKeyStrings(obtainedValues)
				
				# Group the values to be checked
				#fk_id = id(p_FK_decl)  # id(schema)
				fk_id = abs_ref_schema_id
				
				# The common dictionary for this declaration where all the FK values are kept
				fkDef = self.FKWorld.setdefault(fk_id,{}).setdefault(fk_loc_id,FKDef(fkLoc=FKLoc(schemaURI=self.schemaURI,refSchemaURI=abs_ref_schema_id,path='(unknown {})'.format(fk_loc_id),values=list()),members=fk_members))
				
				fkLoc = fkDef.fkLoc
				
				fkVals = fkLoc.values
				
				# Second pass will do the validation
				for theValue in theValues:
					fkVals.append(FKVal(where=self.currentJSONFile,value=theValue))
	
	# Now, time to check
	def doSecondPass(self,l_customFeatureValidatorsContext):
		errors = []
		
		pkContextsHash = {}
		for className, pkContexts in l_customFeatureValidatorsContext.items():
			# This instance is only interested in primary keys
			if className == PrimaryKey.__name__:
				for pkContext in pkContexts:
					# Getting the path correspondence
					for pkDef in pkContext.context.values():
						pkLoc = pkDef.uniqueLoc
						# As there can be nested keys from other schemas
						# ignore the schemaURI from the context, and use
						# the one in the unique location
						if len(pkDef.values) > 0:
							pkVals = pkContextsHash.setdefault(pkLoc.schemaURI,[])
							pkVals.append(pkDef.values)
		
		# Now, at last, check!!!!!!!
		uniqueWhere = set()
		uniqueFailedWhere = set()
		for refSchemaURI,fkDefH in self.FKWorld.items():
			for fk_loc_id,fkDef in fkDefH.items():
				fkLoc = fkDef.fkLoc
				fkPath = fkLoc.path
				checkValuesList = pkContextsHash.get(refSchemaURI)
				if checkValuesList is not None:
					for fkVal in fkLoc.values:
						uniqueWhere.add(fkVal.where)
						
						fkString = fkVal.value
						found = False
						for checkValues in checkValuesList:
							if fkString in checkValues:
								found = True
								break
						
						if not found:
							uniqueFailedWhere.add(fkVal.where)
							errors.append({
								'reason': 'stale_fk',
								'description': "Unmatching FK ({0}) in {1} to schema {2}".format(fkString,fkVal.where,refSchemaURI),
								'file': fkVal.where,
								'path': fkPath
							})
				else:
					for fkVal in fkLoc.values:
						uniqueWhere.add(fkVal.where)
						uniqueFailedWhere.add(fkVal.where)
						errors.append({
							'reason': self.DanglingFKErrorReason,
							'description': "No available documents from {0} schema, required by {1}".format(refSchemaURI,self.schemaURI),
							'file': fkVal.where,
							'path': fkPath
						})
		
		return uniqueWhere,uniqueFailedWhere,errors
	
	def cleanup(self):
		# In order to not destroying the bootstrapping work
		# only remove the recorded values
		for fkDefH in self.FKWorld.values():
			for fkDef in fkDefH.values():
				fkDef.fkLoc.values.clear()
