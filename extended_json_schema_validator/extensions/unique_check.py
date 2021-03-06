#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from collections import namedtuple

from .abstract_check import AbstractCustomFeatureValidator, CheckContext

from jsonschema.exceptions import FormatError, ValidationError

import sys
import re
import json

ALLOWED_KEY_TYPES=(bytes,str)
ALLOWED_ATOMIC_VALUE_TYPES=(int,bytes,str,float,bool,type(None))

UniqueLoc = namedtuple('UniqueLoc',['schemaURI','path'])
UniqueDef = namedtuple('UniqueDef',['uniqueLoc','members','values'])

class UniqueKey(AbstractCustomFeatureValidator):
	KeyAttributeName = 'unique'
	SchemaErrorReason = 'dup_unique'
	
	# Each instance represents the set of keys from one ore more JSON Schemas
	def __init__(self, schemaURI, jsonSchemaSource='(unknown)', config={}, isRW=True):
		super().__init__(schemaURI, jsonSchemaSource, config, isRW=isRW)
		self.UniqueWorld = dict()
	
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
	
	@property
	def needsBootstrapping(self):
		return True
	
	def bootstrap(self, refSchemaTuple = tuple()):
		(id2ElemId , keyRefs , _) = refSchemaTuple
		
		keyList = keyRefs[self.triggerAttribute]
		# Saving the unique locations
		# based on information from FeatureLoc elems
		for loc in keyList:
			uLoc = UniqueLoc(schemaURI=loc.schemaURI,path=loc.path)
			uId = id(loc.context)
			
			uDef = self.UniqueWorld.get(uId)
			
			# This control is here for multiple inheritance cases
			if uDef is not None:
				uDef.uniqueLoc = uLoc
			else:
				uDef = UniqueDef(uniqueLoc=uLoc,members=loc.context[self.triggerAttribute],values=dict())
				self.UniqueWorld[uId] = uDef
		
		return []
		
	JStepPat = re.compile(r"^([^\[]+)\[(0|[1-9][0-9]+)?\]$")

	@classmethod
	def MaterializeJPath(cls,jsonDoc, jPath):
		objectives = [ jsonDoc ]
		jSteps = jPath.split('.') if jPath not in ('.','') else (None,)
		for jStep in jSteps:
			newObjectives = []
			isArray = False
			arrayIndex = None
			if jStep is not None:
				jStepMatch = cls.JStepPat.search(jStep)
				if jStepMatch is not None:
					isArray = True
					if jStepMatch.group(2) is not None:
						arrayIndex = int(jStepMatch.group(2))
					jStep = jStepMatch.group(1)
			for objective in objectives:
				isAvailable = False
				if jStep is not None:
					if isinstance(objective,dict):
						if jStep in objective:
							value = objective[jStep]
							isAvailable = True
					#else:
					#	# Failing
					#	return None
				else:
					value = objective
					isAvailable = True
				
				if isAvailable:
					if isinstance(value,(list,tuple)):
						if arrayIndex is not None:
							if arrayIndex >= 0 and arrayIndex < len(value):
								newObjectives.append(value[arrayIndex])
							#else:
							#	return None
						else:
							newObjectives.extend(value)
					else:
						newObjectives.append(value)
				#else:
				#	# Failing
				#	return None
			
			objectives = newObjectives
		
		# Flattening it (we return a reference to a list of atomic values)
		for iobj, objective in enumerate(objectives):
			if not isinstance(objective,ALLOWED_ATOMIC_VALUE_TYPES):
				objectives[iobj] = json.dumps(objective, sort_keys=True)
		
		return objectives


	# It fetches the values from a JSON, based on the given paths to the members of the key
	@classmethod
	def GetKeyValues(cls,jsonDoc,p_members):
		return tuple(cls.MaterializeJPath(jsonDoc,member) for member in p_members)

	@classmethod
	def _aggPKhelper(cls,basePK,curPKvalue):
		newPK = list(basePK)
		newPK.append(curPKvalue)
		return newPK

	# It generates unique strings from a set of values
	@classmethod
	def GenKeyStrings(cls,keyTuple):
		numPKcols = len(keyTuple)
		if numPKcols == 0:
			return []
		
		# Exiting in case some of the inputs is undefined
		for curPKvalues in keyTuple:
			# If there is no found value, generate nothing
			if not isinstance(curPKvalues,(list, tuple)) or len(curPKvalues) == 0:
				return []
		
		pkStrings = list(map(lambda elem: [ elem ], keyTuple[0]))
		
		for curPKvalues in keyTuple[1:]:
			newPKstrings = []
			
			for curPKvalue in curPKvalues:
				newPKstrings.extend(map(lambda basePK: cls._aggPKhelper(basePK,curPKvalue) , pkStrings))
			
			pkStrings = newPKstrings
		
		return tuple(map(lambda pkString: json.dumps(pkString, sort_keys=True, separators=(',',':')) , pkStrings))

	def validate(self,validator,unique_state,value,schema):
		if unique_state:
			# Check the unicity
			unique_id = id(schema)
			
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
		return CheckContext(schemaURI = self.schemaURI, context = self.UniqueWorld)
	
	def cleanup(self):
		# In order to not destroying the bootstrapping work
		# only remove the recorded values
		for uDef in self.UniqueWorld.values():
			uDef.values.clear()
