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
FKDef = namedtuple('FKDef',['fkLocH','members'])
FKVal = namedtuple('FKVal',['value','where'])

class ForeignKey(AbstractCustomFeatureValidator):
	KeyAttributeName = 'foreign_keys'
	SchemaErrorReason = 'stale_fk'
	DanglingFKErrorReason = 'dangling_fk'
	
	# Each instance represents the set of keys from one ore more JSON Schemas
	def __init__(self,schemaURI,config={}):
		super().__init__(schemaURI,config)
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
	
	def bootstrap(self, metaSchemaURI, jsonSchema):
		super().bootstrap(metaSchemaURI,jsonSchema)
		
		# Saving the unique locations
		for loc in self.bootstrapMessages:
			fk_defs = loc['v']['f_val']
			fk_defs_gid = str(loc['v']['f_id'])
			for fk_loc_i, p_FK_decl in enumerate(fk_defs):
				fk_loc_id = fk_defs_gid + '_' + str(fk_loc_i)
				ref_schema_id = p_FK_decl['schema_id']
				abs_ref_schema_id = uritools.urijoin(self.schemaURI,ref_schema_id)
				
				fk_members = p_FK_decl.get('members',[])
				fkLoc = FKLoc(schemaURI=self.schemaURI,refSchemaURI=abs_ref_schema_id,path=loc['path'],values=list())
				# fk_id = id(p_FK_decl)  # loc['v']['f_id']
				fk_id = abs_ref_schema_id
				fkDef = self.FKWorld.get(fk_id)
				
				# This control is here for multiple inheritance cases
				if fkDef is not None:
					fkDef.fkLocH[fk_loc_id] = fkLoc
				else:
					fkDef = FKDef(fkLocH={fk_loc_id : fkLoc},members=fk_members)
					self.FKWorld[fk_id] = fkDef
	
	# This step is only going to gather all the foreign keys
	def validate(self,validator,fk_defs,value,schema):
		if fk_defs and isinstance(fk_defs,(list,tuple)):
			fk_defs_gid = str(id(fk_defs))
			for fk_loc_i, p_FK_decl in enumerate(fk_defs):
				fk_loc_id = fk_defs_gid + '_' + str(fk_loc_i)
				ref_schema_id = p_FK_decl['schema_id']
				abs_ref_schema_id = uritools.urijoin(self.schemaURI,ref_schema_id)
				
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
				
				fkDef = self.FKWorld.setdefault(fk_id,FKDef(fkLocH={},members=fk_members))
				
				fkLoc = fkDef.fkLocH.setdefault(fk_loc_id,FKLoc(schemaURI=self.schemaURI,refSchemaURI=abs_ref_schema_id,path='(unknown {})'.format(fk_loc_id),values=list()))
				
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
					pkContextsHash[pkContext.schemaURI] = pkContext.context
		
		# Now, at last, check!!!!!!!
		uniqueWhere = set()
		uniqueFailedWhere = set()
		for refSchemaURI,fkDef in self.FKWorld.items():
			for fkLoc in fkDef.fkLocH.values():
				fkPath = fkLoc.path
				if refSchemaURI in pkContextsHash:
					checkValues = list(pkContextsHash[refSchemaURI].values())
					for fkVal in fkLoc.values:
						uniqueWhere.add(fkVal.where)
						
						fkString = fkVal.value
						found = False
						for checkValuesSingle in checkValues:
							if fkString in checkValuesSingle.values:
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
		self.FKWorld = dict()
