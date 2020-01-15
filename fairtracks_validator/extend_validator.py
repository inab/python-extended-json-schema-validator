#!/usr/bin/env python
# -*- coding: utf-8 -*-

import jsonschema as JSV

# This method returns both the extended Validator instance and the dynamic validators
# to be reset on command

PLAIN_VALIDATOR_MAPPER = {
	'http://json-schema.org/draft-04/schema#': JSV.validators.Draft4Validator,
	'http://json-schema.org/draft-04/hyper-schema#': JSV.validators.Draft4Validator,
	'http://json-schema.org/draft-06/schema#': JSV.validators.Draft6Validator,
	'http://json-schema.org/draft-06/hyper-schema#': JSV.validators.Draft4Validator,
	'http://json-schema.org/draft-07/schema#': JSV.validators.Draft7Validator,
	'http://json-schema.org/draft-07/hyper-schema#': JSV.validators.Draft7Validator
}


def extendValidator(schemaURI, validator, inputCustomTypes, inputCustomValidators,config={}, jsonSchemaSource='(unknown)'):
	extendedValidators = validator.VALIDATORS.copy()
	customValidatorsInstances = []
	
	# Validators which must be instantiated
	if None in inputCustomValidators:
		instancedCustomValidators = inputCustomValidators.copy()
		
		# Removing the special entry
		del instancedCustomValidators[None]
		
		# Now, populating
		for dynamicValidatorClass in inputCustomValidators[None]:
			dynamicValidator = dynamicValidatorClass(schemaURI,jsonSchemaSource,config)
			customValidatorsInstances.append(dynamicValidator)
			
			if dynamicValidator.triggerAttribute in instancedCustomValidators:
				raise AssertionError("FATAL: Two custom validators are using the same triggering attribute: {}".format(dynamicValidator.triggerAttribute))
			
			# The method must exist, and accept the parameters
			# declared on next documentation
			# https://python-jsonschema.readthedocs.io/en/stable/creating/
			instancedCustomValidators[dynamicValidator.triggerAttribute] = dynamicValidator.validate
	else:
		instancedCustomValidators = inputCustomValidators
	
	extendedValidators.update(instancedCustomValidators)
	
	extendedChecker = validator.TYPE_CHECKER.redefine_many(inputCustomTypes)
	
	return JSV.validators.extend(validator, validators=extendedValidators , type_checker=extendedChecker) , customValidatorsInstances
	
from collections import namedtuple

FeatureLoc = namedtuple('FeatureLoc',['id','schemaURI','path','context'])

REF_FEATURE='$ref'

# It returns the set of values' ids 
def traverseJSONSchema(jsonObj, schemaURI, keys=set()):
	# Dictionary from name of the feature
	# to be capture to arrays of FeatureLoc named tuples
	keyRefs = {}
	
	# Dictionary from Python address
	# to dictionaries containing the features
	# to the features they contain
	# It's a dictionary of dictionaries of unique ids
	# First level: python address
	# Second level: name of the feature
	# Third level: unique ids
	id2ElemId = {}
	
	# Dictionary from JSON Pointer
	# to unique ids
	jp2val = {}
	
	# Translating it into an set
	keySet = keys  if isinstance(keys,set)  else set(keys)
	
	# And adding the '$ref' feature
	keySet.add(REF_FEATURE)
	
	def _traverse_dict(j,jp=""):
		theId = id(j)
		theIdStr = str(theId)
		
		# Does the dictionary contain a '$ref'?
		isRef = REF_FEATURE in j
		
		for k,v in j.items():
			# Following JSON Schema standards, we have to
			# ignore other keys when there is a $ref one
			if isRef and k != REF_FEATURE:
				continue
			
			elemId = theIdStr + ':' + k
			
			elemPath = jp + '/' + k
			jp2val[elemPath] = elemId
			
			# Is the key among the "special ones"?
			if k in keySet:
				# Saving the correspondence from Python address
				# to unique id of the feature
				id2ElemId.setdefault(theId,{})[k] = elemId
				keyRefs.setdefault(k,[]).append(FeatureLoc(schemaURI=schemaURI,path=elemPath,context=j,id=elemId))
			
			if isinstance(v,dict):
				_traverse_dict(v,elemPath)
			elif isinstance(v,list):
				_traverse_list(v,elemPath)
	
	def _traverse_list(j,jp=""):
		theIdStr = str(id(j))
		for vi, v in enumerate(j):
			str_vi = str(vi)
			elemId = theIdStr + ':' + str_vi
			
			elemPath = jp + '/' + str_vi
			jp2val[elemPath] = elemId
			
			if isinstance(v,dict):
				_traverse_dict(v,elemPath)
			elif isinstance(v,list):
				_traverse_list(v,elemPath)
	
	if isinstance(jsonObj,dict):
		_traverse_dict(jsonObj)
	elif isinstance(jsonObj,list):
		_traverse_list(jsonObj)
	
	return (id2ElemId , keyRefs , jp2val)
