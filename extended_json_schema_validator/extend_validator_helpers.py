#!/usr/bin/env python
# -*- coding: utf-8 -*-

import copy
import logging
from typing import TYPE_CHECKING, cast
from urllib.parse import urldefrag

import jsonschema as JSV
import uritools  # type: ignore[import]

from .extensions.abstract_check import (
	FeatureLoc,
)

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
		Id2ElemId,
		JsonPointer2Val,
		KeyRefs,
		RefSchemaTuple,
		SchemaHashEntry,
		ValidateCallable,
	)

	CustomTypeCheckerCallable = Callable[[Callable[[Any], Any], Any], bool]

	RefSchemaListSet = MutableMapping[str, MutableSequence[RefSchemaTuple]]

module_logger = logging.getLogger(__name__)

# This introspective work allows supporting all the validator
# variants supported by the jsonschema library
INTROSPECT_VALIDATOR_MAPPER: "Mapping[str, Type[JSV.validators._Validator]]" = {
	j_valid.META_SCHEMA["$schema"]: cast("Type[JSV.validators._Validator]", j_valid)
	for j_valid in filter(
		lambda j_val: hasattr(j_val, "META_SCHEMA")
		and isinstance(j_val.META_SCHEMA, dict),
		JSV.validators.__dict__.values(),
	)
}

PLAIN_VALIDATOR_MAPPER: "Mapping[str, Type[JSV.validators._Validator]]" = {
	"http://json-schema.org/draft-04/hyper-schema#": JSV.validators.Draft4Validator,
	"http://json-schema.org/draft-06/hyper-schema#": JSV.validators.Draft4Validator,
	"http://json-schema.org/draft-07/hyper-schema#": JSV.validators.Draft7Validator,
	**INTROSPECT_VALIDATOR_MAPPER  # fmt: skip
}

# This method returns both the extended Validator instance and the dynamic validators
# to be reset on command


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
	keys: "Mapping[str, AbstractCustomFeatureValidator]" = {},
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

	# Translating it
	keySet = cast(
		"MutableMapping[str, Optional[AbstractCustomFeatureValidator]]", copy.copy(keys)
	)

	# And adding the '$ref' feature
	keySet[REF_FEATURE] = None

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
					legit = True
					cFI = keySet.get(k)
					val_err: "Optional[Sequence[JSV.exceptions.ValidationError]]" = None
					if cFI is not None:
						val_err_iter = cFI._fragment_validate(j)
						val_err = list(val_err_iter)
						legit = len(val_err) == 0

					# Saving the correspondence from Python address
					# to unique id of the feature
					if legit:
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
					else:
						module_logger.debug(
							f"At {schemaURI}, f {fragment} p {elemPath} discarded for key {k} class {cFI.__class__.__name__}\n{val_err}"
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


def refResolver_find_in_subschemas(
	refResolver: "JSV.RefResolver", url: str
) -> "Optional[Tuple[str, Any]]":
	subschemas = refResolver._get_subschemas_cache()["$id"]  # type: ignore[attr-defined]
	if not subschemas:
		return None

	uri, fragment = urldefrag(url)
	for subschema in subschemas:
		if isinstance(subschema["$id"], str):
			target_uri = refResolver._urljoin_cache(  # type: ignore[attr-defined]
				refResolver.resolution_scope,
				subschema["$id"],
			)

			if target_uri.rstrip("/") == uri.rstrip("/"):
				if fragment:
					subschema = refResolver.resolve_fragment(subschema, fragment)  # type: ignore[no-untyped-call]
				refResolver.store[url] = subschema
				return url, subschema

	return None


def refResolver_resolve(
	refResolver: "JSV.RefResolver", ref: str
) -> "Optional[Tuple[str, Any]]":
	"""
	Resolve the given reference.
	"""
	url = refResolver._urljoin_cache(refResolver.resolution_scope, ref).rstrip("/")  # type: ignore[attr-defined]

	match = refResolver_find_in_subschemas(refResolver, url)
	if match is not None:
		return match

	return url, refResolver._remote_cache(url)  # type: ignore[attr-defined]


def export_resolved_references(
	contextSchemaURI: "str",
	schema: "Any",
	schemaHash: "MutableMapping[str, SchemaHashEntry]",
	resolved: "MutableSet[str]" = set(),
) -> "Any":
	"""
	Resolves json references and merges them into a consolidated schema for validation purposes.
	Inspired in https://github.com/python-jsonschema/jsonschema/pull/419
	:param schema:
	:return: schema merged with resolved references
	"""

	schema_out = schema
	pending_copy = True
	if isinstance(schema, dict):
		for key, value in schema.items():
			if key == "$ref":
				contextSchemaURI_E, fragment_E = uritools.uridefrag(contextSchemaURI)
				schemaObj = schemaHash.get(contextSchemaURI_E)

				if schemaObj is None:
					raise Exception(f"Unable to resolve {contextSchemaURI}")

				refResolver = schemaObj["ref_resolver"]

				# One step resolution
				ref_schema = refResolver_resolve(refResolver, value)
				if ref_schema:
					# return ref_schema[1]
					if contextSchemaURI in resolved:
						print(f"RECURSION DETECTED {contextSchemaURI} {ref_schema[1]}")
						return ref_schema[1]

					resolved = set(resolved)
					resolved.add(contextSchemaURI)
					return export_resolved_references(
						ref_schema[0], ref_schema[1], schemaHash, resolved
					)
				else:
					raise Exception(
						f"Unable to finish resolution (related to {contextSchemaURI})"
					)

			# When key is not "$ref"
			resolved_ref = export_resolved_references(
				contextSchemaURI, value, schemaHash, resolved
			)
			if resolved_ref and resolved_ref != value:
				if pending_copy:
					schema_out = copy.copy(schema)
					pending_copy = False

				schema_out[key] = resolved_ref
	elif isinstance(schema, list):
		for (idx, value) in enumerate(schema):
			resolved_ref = export_resolved_references(
				contextSchemaURI, value, schemaHash, resolved
			)
			if resolved_ref and resolved_ref != value:
				if pending_copy:
					schema_out = copy.copy(schema)
					pending_copy = False

				schema_out[idx] = resolved_ref

	return schema_out
