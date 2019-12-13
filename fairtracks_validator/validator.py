#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import print_function
import sys
import os
import re
import json
import jsonschema as JSV
import uritools
import hashlib

from collections import namedtuple

# This is needed to assure open suports encoding parameter
if sys.version_info[0] > 2:
	ALLOWED_KEY_TYPES=(bytes,str)
	ALLOWED_ATOMIC_VALUE_TYPES=(int,bytes,str,float,bool)
	# py3k
	pass
else:
	ALLOWED_KEY_TYPES=(str,unicode)
	ALLOWED_ATOMIC_VALUE_TYPES=(int,long,str,unicode,float,bool)
	# py2
	import codecs
	import warnings
	def open(file, mode='r', buffering=-1, encoding=None, errors=None, newline=None, closefd=True, opener=None):
		if newline is not None:
			warnings.warn('newline is not supported in py2')
		if not closefd:
			warnings.warn('closefd is not supported in py2')
		if opener is not None:
			warnings.warn('opener is not supported in py2')
		return codecs.open(filename=file, mode=mode, encoding=encoding, errors=errors, buffering=buffering)


# Augmenting the supported types
from fairtracks_validator.extensions.curie_search import CurieSearch
from fairtracks_validator.extensions.ontology_term import OntologyTerm
from fairtracks_validator.extensions.unique_check import UniqueKey
from fairtracks_validator.extensions.pk_check import PrimaryKey

from .extend_validator import extendValidator , PLAIN_VALIDATOR_MAPPER

class ExtensibleValidator(object):
	CustomBaseValidators = {
		None: [
			UniqueKey,
			PrimaryKey
		]
	}
	
	SCHEMA_KEY = '$schema'
	ALT_SCHEMA_KEYS = [
		'@schema',
		'_schema',
		SCHEMA_KEY
	]
	
	def __init__(self,customFormats=[], customTypes={}, customValidators=CustomBaseValidators, config={}):
		self.schemaHash = {}
		self.CustomFormatCheckerInstance = JSV.FormatChecker()

		# Registering the custom formats, in order to use them
		for customFormat in customFormats:
			self.CustomFormatCheckerInstance.checks(customFormat.FormatName)(customFormat.IsCorrectFormat)
		
		self.customTypes = customTypes
		self.customValidators = customValidators
		self.config = config
	
	@classmethod
	def FindFKs(cls,jsonSchema,jsonSchemaURI,prefix=""):
		FKs = []
		
		if isinstance(jsonSchema,dict):
			# First, this level's foreign keys
			isArray = False
			
			if 'items' in jsonSchema and isinstance(jsonSchema['items'],dict):
				jsonSchema = jsonSchema['items']
				isArray = True
				
				if prefix!='':
					prefix += '[]'
			
			if 'foreign_keys' in jsonSchema and isinstance(jsonSchema['foreign_keys'],(list,tuple)):
				for fk_def in jsonSchema['foreign_keys']:
					# Only valid declarations are taken into account
					if isinstance(fk_def,dict) and 'schema_id' in fk_def and 'members' in fk_def:
						ref_schema_id = fk_def['schema_id']
						members = fk_def['members']
						
						if isinstance(members,(list,tuple)):
							# Translating to absolute URI (in case it is relative)
							abs_ref_schema_id = uritools.urijoin(jsonSchemaURI,ref_schema_id)
							
							# Translating the paths
							components = tuple(map(lambda component: prefix + '.' + component  if component not in ['.','']  else prefix, members))
							
							FKs.append((abs_ref_schema_id,components))
			
			# Then, the foreign keys inside sublevels
			if 'properties' in jsonSchema and isinstance(jsonSchema['properties'],dict):
				if prefix != '':
					prefix += '.'
				p = jsonSchema['properties']
				for k,subSchema in p.items():
					FKs.extend(cls.FindFKs(subSchema,jsonSchemaURI,prefix+k))
		
		return FKs
	
	def loadJSONSchemas(self,*args,verbose=None):
		p_schemaHash = self.schemaHash
		# Schema validation stats
		numDirOK = 0
		numDirFail = 0
		numFileOK = 0
		numFileIgnore = 0
		numFileFail = 0
		
		if verbose:
			print("PASS 0.a: JSON schema loading and validation")
		jsonSchemaPossibles = list(args)
		for jsonSchemaPossible in jsonSchemaPossibles:
			schemaObj = None
			
			if isinstance(jsonSchemaPossible,dict):
				schemaObj = jsonSchemaPossible
				errors = schemaObj.get('errors')
				if errors is None:
					if verbose:
						print("\tIGNORE: cached schema does not have the mandatory 'errors' attribute, so it cannot be processed")
					numFileIgnore += 1
					continue
				
				jsonSchema = schemaObj.get('schema')
				if jsonSchema is None:
					if verbose:
						print("\tIGNORE: cached schema does not have the mandatory 'schema' attribute, so it cannot be processed")
					errors.append({
						'reason': 'unexpected',
						'description': "The cached schema is missing"
					})
					numFileIgnore += 1
					continue
				
				schemaObj['schema_hash'] = self.GetNormalizedJSONHash(jsonSchema)
				
				jsonSchemaFile = schemaObj.setdefault('file','(inline)')
			elif os.path.isdir(jsonSchemaPossible):
				jsonSchemaDir = jsonSchemaPossible
				# It's a possible JSON Schema directory, not a JSON Schema file
				try:
					for relJsonSchemaFile in os.listdir(jsonSchemaDir):
						if relJsonSchemaFile[0]=='.':
							continue
						
						newJsonSchemaFile = os.path.join(jsonSchemaDir,relJsonSchemaFile)
						if os.path.isdir(newJsonSchemaFile) or '.json' in relJsonSchemaFile:
							jsonSchemaPossibles.append(newJsonSchemaFile)
					numDirOK += 1
				except IOError as ioe:
					if verbose:
						print("FATAL ERROR: Unable to open JSON schema directory {0}. Reason: {1}\n".format(jsonSchemaDir,ioe.strerror),file=sys.stderr)
					numDirFail += 1
				
				continue
			else:
				jsonSchemaFile = jsonSchemaPossible
				if verbose:
					print("* Loading schema {0}".format(jsonSchemaFile))
				try:
					with open(jsonSchemaFile,mode="r",encoding="utf-8") as sHandle:
						jsonSchema = json.load(sHandle)
				except IOError as ioe:
					if verbose:
						print("FATAL ERROR: Unable to open schema file {0}. Reason: {1}".format(jsonSchemaFile,ioe.strerror),file=sys.stderr)
					numFileFail += 1
					continue
				else:
					errors = []
					schemaObj = {
						'schema': jsonSchema,
						'schema_hash': self.GetNormalizedJSONHash(jsonSchema),
						'file': jsonSchemaFile,
						'errors': errors
					}
			
			schemaValId = jsonSchema.get(self.SCHEMA_KEY)
			if schemaValId is None:
				if verbose:
					print("\tIGNORE: {0} does not have the mandatory '{1}' attribute, so it cannot be validated".format(jsonSchemaFile,self.SCHEMA_KEY))
				errors.append({
					'reason': 'no_schema',
					'description': "JSON Schema attribute '$schema' is missing"
				})
				numFileIgnore += 1
				continue
			
			plain_validator = PLAIN_VALIDATOR_MAPPER.get(schemaValId)
			if plain_validator is None:
				if verbose:
					print("\tIGNORE/FIXME: The JSON Schema id {0} is not being acknowledged by this validator".format(schemaValId))
				errors.append({
					'reason': 'schema_unknown',
					'description': "'$schema' id {0} is not being acknowledged by this validator".format(schemaValId)
				})
				numFileIgnore += 1
				continue
			
			# Getting the JSON Schema URI, needed by this
			idKey = '$id'  if '$id' in jsonSchema else 'id'
			jsonSchemaURI = jsonSchema.get(idKey)
			
			validator , customFormatInstances = extendValidator(jsonSchemaURI, plain_validator, self.customTypes, self.customValidators,self.config)
			schemaObj['customFormatInstances'] = customFormatInstances
			schemaObj['validator'] = validator
			
			metaSchema = validator.META_SCHEMA
			if len(customFormatInstances) > 0:
				metaSchema = metaSchema.copy()
				metaProps = metaSchema['properties']

				for customFormatInstance in customFormatInstances:
					for kF, vF in customFormatInstance.triggerJSONSchemaDef.items():
						if kF in metaProps:
							# Multiple declarations
							vM = metaProps[kF]
							
							if 'anyOf' not in vM:
								newDecl = {
									'anyOf': [
										vM
									]
								}
								vM = metaProps[kF] = newDecl
							
							vM['anyOf'].append(vF)
						else:
							metaProps[kF] = vF
			
			valErrors = [ valError  for valError in validator(metaSchema).iter_errors(jsonSchema) ]
			if len(valErrors) > 0:
				if verbose:
					print("\t- ERRORS:\n"+"\n".join(map(lambda se: "\t\tPath: {0} . Message: {1}".format("/"+"/".join(map(lambda e: str(e),se.path)),se.message) , valErrors))+"\n")
				for valError in valErrors:
					errors.append({
						'reason': 'schema_error',
						'description': "Path: {0} . Message: {1}".format("/"+"/".join(map(lambda e: str(e),valError.path)),valError.message)
					})
				numFileFail += 1
			elif jsonSchemaURI is not None:
				# Getting the JSON Pointer object instance of the augmented schema
				# my $jsonSchemaP = $v->schema($jsonSchema)->schema;
				# This step is done, so we fetch a complete schema
				# $jsonSchema = $jsonSchemaP->data;
				
				if jsonSchemaURI in p_schemaHash:
					if verbose:
						print("\tERROR: validated, but schema in {0} and schema in {1} have the same id".format(jsonSchemaFile,p_schemaHash[jsonSchemaURI]['file']),file=sys.stderr)
					errors.append({
						'reason': 'dup_id',
						'description': "JSON Schema validated, but schema in {0} and schema in {1} have the same id".format(jsonSchemaFile,p_schemaHash[jsonSchemaURI]['file'])
					})
					numFileFail += 1
				else:
					if verbose:
						print("\t- Validated {0}".format(jsonSchemaURI))
					
					if len(customFormatInstances) > 0:
						for cFI in customFormatInstances:
							# Bootstrapping the schema
							# By default this is a no-op
							cFI.bootstrap(schemaValId, jsonSchema)
					
					# Curating the primary key
					p_PK = None
					if 'primary_key' in jsonSchema:
						p_PK = jsonSchema['primary_key']
						if isinstance(p_PK,(list,tuple)):
							for key in p_PK:
								#if type(key) not in ALLOWED_ATOMIC_VALUE_TYPES:
								if type(key) not in ALLOWED_KEY_TYPES:
									if verbose:
										print("\tWARNING: primary key in {0} is not composed by strings defining its attributes. Ignoring it".format(jsonSchemaFile),file=sys.stderr)
									p_PK = None
									break
						else:
							p_PK = None
					
					schemaObj['pk'] = p_PK
					
					# Gather foreign keys
					FKs = self.FindFKs(jsonSchema,jsonSchemaURI)
					
					schemaObj['fk'] = FKs
					
					#print(FKs,file=sys.stderr)
					
					p_schemaHash[jsonSchemaURI] = schemaObj
					numFileOK += 1
			else:
				if verbose:
					print("\tIGNORE: validated, but schema in {0} has no id attribute".format(jsonSchemaFile),file=sys.stderr)
				errors.append({
					'reason': 'no_id',
					'description': "JSON Schema attributes '$id' (Draft06 onward) and 'id' (Draft04) are missing"
				})
				numFileIgnore += 1
		
		if verbose:
			print("\nSCHEMA VALIDATION STATS: loaded {0} schemas from {1} directories, ignored {2} schemas, failed {3} schemas and {4} directories".format(numFileOK,numDirOK,numFileIgnore,numFileFail,numDirFail))
		
			print("\nPASS 0.b: JSON schema set consistency checks")
		
		# Now, we check whether the declared foreign keys are pointing to loaded JSON schemas
		numSchemaConsistent = 0
		numSchemaInconsistent = 0
		for jsonSchemaURI , p_schema in p_schemaHash.items():
			jsonSchemaFile = p_schema['file']
			p_FKs = p_schema['fk']
			if verbose:
				print("* Checking {0}".format(jsonSchemaFile))
			
			isValid = True
			for p_FK_decl in p_FKs:
				fkPkSchemaId , p_FK_def = p_FK_decl
				
				if fkPkSchemaId not in p_schemaHash:
					if verbose:
						print("\t- FK ERROR: No schema with {0} id, required by {1} ({2})".format(fkPkSchemaId,jsonSchemaFile,jsonSchemaURI),file=sys.stderr)
					p_schema['errors'].append({
						'reason': 'fk_no_schema',
						'description': "No schema with {0} id, required by {1} ({2})".format(fkPkSchemaId,jsonSchemaFile,jsonSchemaURI)
					})
					
					isValid = False
			
			if isValid:
				if verbose:
					print("\t- Consistent!")
				numSchemaConsistent += 1
			else:
				numSchemaInconsistent += 1
		
		if verbose:
			print("\nSCHEMA CONSISTENCY STATS: {0} schemas right, {1} with inconsistencies".format(numSchemaConsistent,numSchemaInconsistent))
		
		return len(self.schemaHash.keys())
		
	def getValidSchemas(self):
		return self.schemaHash
	
	# This method invalidates the different cached elements as much
	# as possible, 
	def invalidateCaches(self):
		p_schemasObj = self.getValidSchemas()
		
		for schemaObj in p_schemasObj.values():
			dynSchemaVal = schemaObj['customFormatInstances']
			for dynVal in dynSchemaVal:
				dynVal.invalidateCaches()
	
	# This method warms up the different cached elements as much
	# as possible, 
	def warmUpCaches(self):
		p_schemasObj = self.getValidSchemas()
		
		for schemaObj in p_schemasObj.values():
			dynSchemaVal = schemaObj['customFormatInstances']
			for dynVal in dynSchemaVal:
				dynVal.warmUpCaches()
	
	def _resetDynamicValidators(self,dynValList):
		for dynVal in dynValList:
			dynVal.cleanup()
	
	@classmethod
	def GetNormalizedJSONHash(cls,json_data):
		# First, we serialize it in a reproducible way
		json_canon = json.dumps(json_data,sort_keys=True,indent=None,separators=(',',':'))
		
		return hashlib.sha1(json_canon.encode('utf-8')).hexdigest()
	
	def jsonValidate(self,*args,verbose=None):
		p_schemaHash = self.schemaHash
		
		# A two level hash, in order to check primary key restrictions
		PKvals = dict()
		
		# JSON validation stats
		numDirOK = 0
		numDirFail = 0
		numFilePass1OK = 0
		numFilePass1Ignore = 0
		numFilePass1Fail = 0
		numFilePass2OK = 0
		numFilePass2Fail = 0
		
		report = []
		dynSchemaSet = set()
		dynSchemaValList = []
		
		# First pass, check against JSON schema, as well as primary keys unicity
		if verbose:
			print("\nPASS 1: Schema validation and PK checks")
		iJsonPossible = -1
		jsonPossibles = list(args)
		for jsonPossible in jsonPossibles:
			iJsonPossible += 1
			jsonObj = None
			if isinstance(jsonPossible,dict):
				jsonObj = jsonPossible
				errors = jsonObj.get('errors')
				if errors is None:
					if verbose:
						print("\tIGNORE: cached JSON does not have the mandatory 'errors' attribute, so it cannot be processed")
					numFileIgnore += 1
					
					# For the report
					jsonObj.setDefault('errors',[{'reason': 'ignored', 'description': 'Programming error: uninitialized error structures'}])
					report.append(jsonObj)
					
					# Masking it for the pass 2 loop
					jsonPossibles[iJsonPossible] = None
					continue
				
				jsonDoc = jsonObj.get('json')
				if jsonDoc is None:
					if verbose:
						print("\tIGNORE: cached JSON does not have the mandatory 'json' attribute, so it cannot be processed")
					errors.append({
						'reason': 'ignored',
						'description': "Programming error: the cached json is missing"
					})
					numFileIgnore += 1
					
					# For the report
					report.append(jsonObj)
					
					# Masking it for the pass 2 loop
					jsonPossibles[iJsonPossible] = None
					continue
				
				jsonFile = jsonObj.setdefault('file','(inline)')
			elif os.path.isdir(jsonPossible):
				jsonDir = jsonPossible
				# It's a possible JSON directory, not a JSON file
				try:
					for relJsonFile in os.listdir(jsonDir):
						# Skipping hidden files / directories
						if relJsonFile[0]=='.':
							continue
						
						newJsonFile = os.path.join(jsonDir,relJsonFile)
						if os.path.isdir(newJsonFile) or '.json' in relJsonFile:
							jsonPossibles.append(newJsonFile)
					
					numDirOK += 1
				except IOError as ioe:
					if verbose:
						print("FATAL ERROR: Unable to open/process JSON directory {0}. Reason: {1}".format(jsonDir,ioe.strerror),file=sys.stderr)
					report.append({'file': jsonDir,'errors': [{'reason': 'fatal', 'description': 'Unable to open/process JSON directory'}]})
					numDirFail += 1
				finally:
					# Masking it for the pass 2 loop
					jsonPossibles[iJsonPossible] = None
				
				continue
			else:
				jsonFile = jsonPossible
				try:
					with open(jsonFile,mode="r",encoding="utf-8") as jHandle:
						if verbose:
							print("* Validating {0}".format(jsonFile))
						jsonDoc = json.load(jHandle)
						
				except IOError as ioe:
					if verbose:
						print("\t- ERROR: Unable to open file {0}. Reason: {1}".format(jsonFile,ioe.strerror),file=sys.stderr)
					# Masking it for the next loop
					report.append({'file': jsonFile,'errors': [{'reason': 'fatal', 'description': 'Unable to open/parse JSON file'}]})
					jsonPossibles[iJsonPossible] = None
					numFilePass1Fail += 1
					continue
				
				else:
					errors = []
					jsonObj = {
						'file': jsonFile,
						'json': jsonDoc,
						'errors': errors
					}
					# Upgrading for the next loop
					jsonPossibles[iJsonPossible] = jsonObj
			
			# Getting the schema id to locate the proper schema to validate against
			jsonRoot = jsonDoc['fair_tracks']  if 'fair_tracks' in jsonDoc  else jsonDoc
			
			jsonSchemaId = None
			for altSchemaKey in self.ALT_SCHEMA_KEYS:
				if altSchemaKey in jsonRoot:
					jsonSchemaId = jsonRoot[altSchemaKey]
					break
			
			if jsonSchemaId is not None:
				if jsonSchemaId in p_schemaHash:
					if verbose:
						print("\t- Using {0} schema".format(jsonSchemaId))
					
					schemaObj = p_schemaHash[jsonSchemaId]
					
					for customFormatInstance in schemaObj['customFormatInstances']:
						customFormatInstance.setCurrentJSONFilename(jsonFile)
					
					# Registering the dynamic validators to be cleaned up
					# when the validator finishes the session
					if jsonSchemaId not in dynSchemaSet:
						dynSchemaSet.add(jsonSchemaId)
						localDynSchemaVal = schemaObj['customFormatInstances']
						if localDynSchemaVal:
							# We reset them, in case they were dirty
							self._resetDynamicValidators(localDynSchemaVal)
							dynSchemaValList.extend(localDynSchemaVal)
					
					jsonSchema = schemaObj['schema']
					validator = schemaObj['validator']
					jsonObj['schema_hash'] = schemaObj['schema_hash']
					jsonObj['schema_id'] = jsonSchemaId
					
					valErrors = [ error  for error in validator(jsonSchema, format_checker = self.CustomFormatCheckerInstance).iter_errors(jsonDoc) ]
					
					if len(valErrors) > 0:
						if verbose:
							print("\t- ERRORS:\n"+"\n".join(map(lambda se: "\t\tPath: {0} . Message: {1}".format("/"+"/".join(map(lambda e: str(e),se.path)),se.message) , valErrors))+"\n")
						for valError in valErrors:
							if isinstance(valError.validator_value,dict):
								schema_error_reason = valError.validator_value.get('reason','schema_error')
							else:
								schema_error_reason = 'schema_error'
							
							errors.append({
								'reason': schema_error_reason,
								'description': "Path: {0} . Message: {1}".format("/"+"/".join(map(lambda e: str(e),valError.path)),valError.message)
							})
						
						# Masking it for the next loop
						report.append(jsonPossibles[iJsonPossible])
						jsonPossibles[iJsonPossible] = None
						numFilePass1Fail += 1
					else:
						# Does the schema contain a PK declaration?
						isValid = True
						p_PK_def = p_schemaHash[jsonSchemaId]['pk']
						if p_PK_def is not None:
							p_PK = None
							if jsonSchemaId in PKvals:
								p_PK = PKvals[jsonSchemaId]
							else:
								PKvals[jsonSchemaId] = p_PK = {}
							
							pkValues = UniqueKey.GetKeyValues(jsonDoc,p_PK_def)
							pkStrings = UniqueKey.GenKeyStrings(pkValues)
							# Pass 1.a: check duplicate keys
							for pkString in pkStrings:
								if pkString in p_PK:
									if verbose:
										print("\t- PK ERROR: Duplicate PK in {0} and {1}\n".format(p_PK[pkString],jsonFile),file=sys.stderr)
									errors.append({
										'reason': 'dup_pk',
										'description': "Duplicate PK in {0} and {1}\n".format(p_PK[pkString],jsonFile)
									})
									isValid = False
							
							# Pass 1.b: record keys
							if isValid:
								for pkString in pkStrings:
									p_PK[pkString] = jsonFile
							else:
								# Masking it for the next loop if there was an error
								report.append(jsonPossibles[iJsonPossible])
								jsonPossibles[iJsonPossible] = None
								numFilePass1Fail += 1
								
						if isValid:
							if verbose:
								print("\t- Validated!\n")
							numFilePass1OK += 1
					
				else:
					if verbose:
						print("\t- Skipping schema validation (schema with URI {0} not found)".format(jsonSchemaId))
					errors.append({
						'reason': 'schema_unknown',
						'description': "Schema with URI {0} was not loaded".format(jsonSchemaId)
					})
					# Masking it for the next loop
					report.append(jsonPossibles[iJsonPossible])
					jsonPossibles[iJsonPossible] = None
					numFilePass1Ignore += 1
			else:
				if verbose:
					print("\t- Skipping schema validation (no one declared for {0})".format(jsonFile))
				errors.append({
					'reason': 'no_id',
					'description': "No hint to identify the correct JSON Schema to be used to validate"
				})
				# Masking it for the next loop
				report.append(jsonPossibles[iJsonPossible])
				jsonPossibles[iJsonPossible] = None
				numFilePass1Ignore += 1
		
		#use Data::Dumper;
		#
		#print Dumper(\%PKvals),"\n";
		
		# Second pass, check foreign keys against gathered primary keys
		if verbose:
			print("PASS 2: foreign keys checks")
		#use Data::Dumper;
		#print Dumper(@jsonFiles),"\n";
		for jsonObj in jsonPossibles:
			if jsonObj is None:
				continue
			
			# Adding this survivor to the report
			report.append(jsonObj)
			
			if verbose:
				print("* Checking FK on {0}".format(jsonFile))
			jsonDoc = jsonObj['json']
			errors = jsonObj['errors']
			jsonFile = jsonObj['file']
			
			jsonRoot = jsonDoc['fair_tracks']  if 'fair_tracks' in jsonDoc  else jsonDoc
			
			jsonSchemaId = None
			for altSchemaKey in self.ALT_SCHEMA_KEYS:
				if altSchemaKey in jsonRoot:
					jsonSchemaId = jsonRoot[altSchemaKey]
					break
			
			if jsonSchemaId is not None:
				if jsonSchemaId in p_schemaHash:
					if verbose:
						print("\t- Using {0} schema".format(jsonSchemaId))
					
					p_FKs = p_schemaHash[jsonSchemaId]['fk']
					
					isValid = True
					#print(p_schemaHash[jsonSchemaId])
					for p_FK_decl in p_FKs:
						fkPkSchemaId, p_FK_def = p_FK_decl
						
						fkValues = UniqueKey.GetKeyValues(jsonDoc,p_FK_def)
						
						#print(fkValues,file=sys.stderr);
						
						fkStrings = UniqueKey.GenKeyStrings(fkValues)
						
						if len(fkStrings) > 0:
							if fkPkSchemaId in PKvals:
								p_PK = PKvals[fkPkSchemaId]
								for fkString in fkStrings:
									if fkString is not None:
										#print STDERR "DEBUG FK ",$fkString,"\n";
										if fkString not in p_PK:
											if verbose:
												print("\t- FK ERROR: Unmatching FK ({0}) in {1} to schema {2}".format(fkString,jsonFile,fkPkSchemaId),file=sys.stderr)
											errors.append({
												'reason': 'stale_fk',
												'description': "Unmatching FK ({0}) in {1} to schema {2}".format(fkString,jsonFile,fkPkSchemaId)
											})
											isValid = False
									#else:
									#	use Data::Dumper;
									#	print Dumper($p_FK_def),"\n";
							else:
								if verbose:
									print("\t- FK ERROR: No available documents from {0} schema, required by {1}".format(fkPkSchemaId,jsonFile),file=sys.stderr)
								errors.append({
									'reason': 'dangling_fk',
									'description': "No available documents from {0} schema, required by {1}".format(fkPkSchemaId,jsonFile)
								})
								
								isValid = False
					if isValid:
						if verbose:
							print("\t- Validated!")
						numFilePass2OK += 1
					else:
						numFilePass2Fail += 1
				else:
					if verbose:
						print("\t- ASSERTION ERROR: Skipping schema validation (schema with URI {0} not found)".format(jsonSchemaId))
					errors.append({
						'reason': 'schema_unknown',
						'description': "Schema with URI {0} was not loaded".format(jsonSchemaId)
					})
					numFilePass2Fail += 1
			else:
				if verbose:
					print("\t- ASSERTION ERROR: Skipping schema validation (no one declared for {0})".format(jsonFile))
				errors.append({
					'reason': 'no_id',
					'description': "No hint to identify the correct JSON Schema to be used to validate"
				})
				numFilePass2Fail += 1
			if verbose:
				print()
		
		if verbose:
			print("\nVALIDATION STATS:\n\t- directories ({0} OK, {1} failed)\n\t- PASS 1 ({2} OK, {3} ignored, {4} error)\n\t- PASS 2 ({5} OK, {6} error)".format(numDirOK,numDirFail,numFilePass1OK,numFilePass1Ignore,numFilePass1Fail,numFilePass2OK,numFilePass2Fail))
		
		# Reset the dynamic validators
		if dynSchemaValList:
			self._resetDynamicValidators(dynSchemaValList)
		
		return report

class FairGTracksValidator(ExtensibleValidator):
	# This has been commented out, as we are following the format validation path
	CustomTypes = {
	#	'curie': CurieSearch.IsCurie,
	#	'term': OntologyTerm.IsTerm
	}

	CustomFormats = [
		CurieSearch,
		OntologyTerm
	]
	
	CustomValidators = {
		None: [
			CurieSearch,
			OntologyTerm,
			UniqueKey,
			PrimaryKey
		]
	}
	
	def __init__(self,customFormats=CustomFormats, customTypes=CustomTypes, customValidators=CustomValidators, config=None):
		super().__init__(customFormats,customTypes,customValidators,config)
