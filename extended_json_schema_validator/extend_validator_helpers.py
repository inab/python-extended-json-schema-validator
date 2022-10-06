#!/usr/bin/env python
# -*- coding: utf-8 -*-

import copy
from typing import TYPE_CHECKING, cast

import jsonschema as JSV
import uritools  # type: ignore[import]

if TYPE_CHECKING:
	from typing import (
		Any,
		Callable,
		Mapping,
		MutableMapping,
		MutableSequence,
		MutableSet,
		Optional,
		Protocol,
		Sequence,
		Tuple,
		Type,
		Union,
	)

	from .extensions.abstract_check import (
		AbstractCustomFeatureValidator,
		FeatureLoc,
		Id2ElemId,
		JsonPointer2Val,
		KeyRefs,
		RefSchemaTuple,
		ValidateCallable,
	)

	CustomTypeCheckerCallable = Callable[[Callable[[Any], Any], Any], bool]

	RefSchemaListSet = MutableMapping[str, MutableSequence[RefSchemaTuple]]


# This method returns both the extended Validator instance and the dynamic validators
# to be reset on command

PLAIN_VALIDATOR_MAPPER: "Mapping[str, Type[JSV.validators._Validator]]" = {
	"http://json-schema.org/draft-04/schema#": JSV.validators.Draft4Validator,
	"http://json-schema.org/draft-04/hyper-schema#": JSV.validators.Draft4Validator,
	"http://json-schema.org/draft-06/schema#": JSV.validators.Draft6Validator,
	"http://json-schema.org/draft-06/hyper-schema#": JSV.validators.Draft4Validator,
	"http://json-schema.org/draft-07/schema#": JSV.validators.Draft7Validator,
	"http://json-schema.org/draft-07/hyper-schema#": JSV.validators.Draft7Validator,
}


def extendValidator(
	schemaURI: str,
	validator: "Type[JSV.validators._Validator]",
	inputCustomTypes: "Mapping[str, CustomTypeCheckerCallable]",
	inputCustomValidators: "Mapping[Optional[str], Union[ValidateCallable, Sequence[Type[AbstractCustomFeatureValidator]]]]",
	config: "Mapping[str, Any]" = {},
	jsonSchemaSource: str = "(unknown)",
	isRW: bool = True,
) -> "Tuple[Type[JSV.validators._Validator], Sequence[AbstractCustomFeatureValidator]]":
	extendedValidators = validator.VALIDATORS.copy()
	customValidatorsInstances = []

	# Validators which must be instantiated
	instancedCustomValidators: "Mapping[str, ValidateCallable]"
	if None in inputCustomValidators:
		_instancedCustomValidators = cast(
			"MutableMapping[Optional[str], Union[ValidateCallable, Sequence[Type[AbstractCustomFeatureValidator]]]]",
			copy.copy(inputCustomValidators),
		)

		# Removing the special entry
		del _instancedCustomValidators[None]

		# Now, populating
		for dynamicValidatorClass in cast(
			"Sequence[Type[AbstractCustomFeatureValidator]]",
			inputCustomValidators[None],
		):

			dynamicValidator = dynamicValidatorClass(
				schemaURI, jsonSchemaSource, config=config, isRW=isRW
			)
			customValidatorsInstances.append(dynamicValidator)

			for (
				triggerAttribute,
				triggeredValidation,
			) in dynamicValidator.getValidators():
				if triggerAttribute in _instancedCustomValidators:
					raise AssertionError(
						"FATAL: Two custom validators are using the same triggering attribute: {}".format(
							triggerAttribute
						)
					)

				# The method must exist, and accept the parameters
				# declared on next documentation
				# https://python-jsonschema.readthedocs.io/en/stable/creating/
				_instancedCustomValidators[triggerAttribute] = triggeredValidation
		instancedCustomValidators = cast(
			"Mapping[str, ValidateCallable]", _instancedCustomValidators
		)
	else:
		instancedCustomValidators = cast(
			"Mapping[str, ValidateCallable]", inputCustomValidators
		)

	extendedValidators.update(instancedCustomValidators)

	extendedChecker = validator.TYPE_CHECKER.redefine_many(inputCustomTypes)

	return (
		JSV.validators.extend(
			validator, validators=extendedValidators, type_checker=extendedChecker
		),
		customValidatorsInstances,
	)


REF_FEATURE = "$ref"

# It returns the set of values' ids
def traverseJSONSchema(
	jsonObj: "Any",
	schemaURI: "Optional[str]" = None,
	keys: "Union[MutableSet[str], Sequence[str]]" = set(),
	fragment: "Optional[str]" = None,
	refSchemaListSet: "RefSchemaListSet" = {},
) -> "Optional[RefSchemaListSet]":
	# Should we try getting it?
	if schemaURI is None:
		if isinstance(jsonObj, dict):
			startingSchemaURI = jsonObj.get("$id")
			if startingSchemaURI is None:
				startingSchemaURI = jsonObj.get("id")

			# End / fail fast
			if startingSchemaURI is None:
				return None

			schemaURI, fragment = uritools.uridefrag(startingSchemaURI)
		else:
			# End / fail fast
			return None

	assert schemaURI is not None

	# Dictionary from name of the feature
	# to be captured to arrays of FeatureLoc named tuples
	keyRefs: "KeyRefs" = {}

	# Dictionary from Python address
	# to dictionaries containing the features
	# to the features they contain
	# It's a dictionary of dictionaries of unique ids
	# First level: python address
	# Second level: name of the feature
	# Third level: unique ids
	id2ElemId: "Id2ElemId" = {}

	# Dictionary from JSON Pointer
	# to unique ids
	jp2val: "JsonPointer2Val" = {}

	refSchemaListSet.setdefault(schemaURI, []).append((id2ElemId, keyRefs, jp2val))

	# Translating it into an set
	keySet = keys if isinstance(keys, set) else set(keys)

	# And adding the '$ref' feature
	keySet.add(REF_FEATURE)

	def _traverse_dict(
		schemaURI: str,
		j: "Mapping[str, Any]",
		jp: str = "",
		fragment: "Optional[str]" = None,
	) -> None:
		# Pre-processing
		newPartialSchemaURI = j.get("$id")
		if newPartialSchemaURI:
			# Computing the absolute schema URI
			if uritools.isabsuri(schemaURI):
				newSchemaURI, uriFragment = uritools.uridefrag(
					uritools.urijoin(schemaURI, newPartialSchemaURI)
				)
			else:
				newSchemaURI, uriFragment = uritools.uridefrag(newPartialSchemaURI)
		else:
			newSchemaURI = schemaURI

		# Are we jumping to a different place?
		if newSchemaURI == schemaURI:
			theId = id(j)
			theIdStr = str(theId)

			# Does the dictionary contain a '$ref'?
			isRef = REF_FEATURE in j

			for k, v in j.items():
				# Following JSON reference standards, we have to
				# ignore other keys when there is a $ref one
				# https://tools.ietf.org/html/draft-pbryan-zyp-json-ref-03#section-3
				if isRef and (k != REF_FEATURE):
					continue

				elemId = theIdStr + ":" + k

				elemPath = jp + "/" + k
				jp2val[elemPath] = elemId

				# Is the key among the "special ones"?
				if k in keySet:
					# Saving the correspondence from Python address
					# to unique id of the feature
					id2ElemId.setdefault(theId, {})[k] = [elemId]
					keyRefs.setdefault(k, []).append(
						FeatureLoc(
							schemaURI=schemaURI,
							fragment=fragment,
							path=elemPath,
							context=j,
							id=elemId,
						)
					)

				if isinstance(v, dict):
					# Fragment must not be propagated to children
					_traverse_dict(schemaURI, v, jp=elemPath)
				elif isinstance(v, list):
					_traverse_list(schemaURI, v, jp=elemPath)
		else:
			traverseJSONSchema(
				j,
				schemaURI=newSchemaURI,
				fragment=uriFragment,
				keys=keys,
				refSchemaListSet=refSchemaListSet,
			)

	def _traverse_list(schemaURI: str, j: "Sequence[Any]", jp: str = "") -> None:
		theIdStr = str(id(j))
		for vi, v in enumerate(j):
			str_vi = str(vi)
			elemId = theIdStr + ":" + str_vi

			elemPath = jp + "/" + str_vi
			jp2val[elemPath] = elemId

			if isinstance(v, dict):
				_traverse_dict(schemaURI, v, jp=elemPath)
			elif isinstance(v, list):
				_traverse_list(schemaURI, v, jp=elemPath)

	if isinstance(jsonObj, dict):
		_traverse_dict(schemaURI, jsonObj, fragment=fragment)
	elif isinstance(jsonObj, list):
		_traverse_list(schemaURI, jsonObj)

	return refSchemaListSet


def flattenTraverseListSet(
	traverseListSet: "Sequence[RefSchemaTuple]",
) -> "RefSchemaTuple":
	# Dictionary from name of the feature
	# to be captured to arrays of FeatureLoc named tuples
	keyRefs: "KeyRefs" = {}

	# Dictionary from Python address
	# to dictionaries containing the features
	# to the features they contain
	# It's a dictionary of dictionaries of unique ids
	# First level: python address
	# Second level: name of the feature
	# Third level: unique ids
	id2ElemId: "Id2ElemId" = {}

	# Dictionary from JSON Pointer
	# to unique ids
	jp2val: "JsonPointer2Val" = {}

	# First pass
	for traverseSet in traverseListSet:
		t_id2ElemId, t_keyRefs, t_jp2val = traverseSet

		# Keyrefs
		for t_kr_k, t_kr_v in t_keyRefs.items():
			keyRefs.setdefault(t_kr_k, []).extend(t_kr_v)

		# id2ElemId
		for t_i2e_k, t_i2e_v in t_id2ElemId.items():
			featDict = id2ElemId.setdefault(t_i2e_k, {})
			for featName, l_uniqId in t_i2e_v.items():
				featDict.setdefault(featName, []).extend(l_uniqId)

		# jp2val
		jp2val.update(t_jp2val)

	# Second pass
	# list of FeatureLoc being truly unique
	for kr_k, feats in keyRefs.items():
		if len(feats) > 1:
			unique_feats = []
			unique_feat_id = set()
			reassign = False

			# Arrays of features
			for feat in feats:
				if feat.id not in unique_feat_id:
					unique_feats.append(feat)
					unique_feat_id.add(feat.id)
				else:
					reassign = True

			if reassign:
				keyRefs[kr_k] = unique_feats

	# list of unique ids truly unique
	for i2e_k, featDict in id2ElemId.items():
		for featName, l_uniqId in featDict.items():
			len_l_uniqId = len(l_uniqId)
			if len_l_uniqId > 1:
				s_uniqId = set(l_uniqId)

				if len(s_uniqId) != len_l_uniqId:
					featDict[featName] = list(s_uniqId)

	return (id2ElemId, keyRefs, jp2val)
