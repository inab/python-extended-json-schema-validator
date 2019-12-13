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


def extendValidator(schemaURI, validator, inputCustomTypes, inputCustomValidators):
	extendedValidators = validator.VALIDATORS.copy()
	customValidatorsInstances = []
	
	# Validators which must be instantiated
	if None in inputCustomValidators:
		instancedCustomValidators = inputCustomValidators.copy()
		
		# Removing the special entry
		del instancedCustomValidators[None]
		
		# Now, populating
		for dynamicValidatorClass in inputCustomValidators[None]:
			dynamicValidator = dynamicValidatorClass(schemaURI)
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
