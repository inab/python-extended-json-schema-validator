#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import rfc3987
from jsonschema.compat import str_types
from jsonschema.exceptions import FormatError, ValidationError

import owlready2
import xdg.BaseDirectory
import os
import urllib
import urllib.error
import sys
import tempfile
import shutil
import atexit
import hashlib

import json

from .abstract_check import AbstractCustomFeatureValidator
from ..downloader import download_file

class OntologyTerm(AbstractCustomFeatureValidator):
	VALID_MATCHES = {
		'exact': 'iri',
		'suffix': 'iri',
		'label': 'label'
	}
	
	VALID_SCHEMES = { 'http', 'https', 'ftp' }
	
	FormatName = 'term'
	KeyAttributeName = 'ontology'
	MatchTypeAttrName = 'matchType'
	AncestorsAttrName = 'ancestors'
	
	def __init__(self,schemaURI, jsonSchemaSource='(unknown)',config={}):
		super().__init__(schemaURI,jsonSchemaSource,config)
		self.ontologies = []
	
	@property
	def triggerAttribute(self):
		return self.KeyAttributeName
	
	@property
	def triggerJSONSchemaDef(self):
		return {
			self.triggerAttribute : {
				"oneOf": [
					{
						"type": "string",
						"format": "uri"
					}
					,
					{
						"type": "array",
						"items": {
							"type": "string",
							"format": "uri",
							"minLength": 1
						},
						"minItems": 1
					}
				]
			},
			self.MatchTypeAttrName : {
				"type": "string",
				"enum": list(self.VALID_MATCHES.keys())
			},
			self.AncestorsAttrName : {
				"oneOf": [
					{
						"type": "string",
						"format": "uri"
					}
					,
					{
						"type": "array",
						"items": {
							"type": "string",
							"format": "uri",
							"minLength": 1
						},
						"minItems": 1
					}
				]
			}
		}
	
	@property
	def needsBootstrapping(self):
		return True
	
	def bootstrap(self, refSchemaTuple = tuple()):
		(id2ElemId , keyRefs , _) = refSchemaTuple
		
		keyList = keyRefs[self.triggerAttribute]
		# Saving the unique locations
		# based on information from FeatureLoc elems
		uIdSet = set()
		for loc in keyList:
			uId = id(loc.context)
			ontlist = loc.context[self.triggerAttribute]
			
			if uId not in uIdSet:
				if isinstance(ontlist,list):
					self.ontologies.extend(ontlist)
				else:
					self.ontologies.append(ontlist)
				uIdSet.add(uId)
		
		return []
	
	def invalidateCaches(self):
		self.InvalidateAllWorlds(self.config.get('cacheDir'))
	
	def warmUpCaches(self):
		cachePath = self.config.get('cacheDir')
		doReasoner = self.config.get(self.KeyAttributeName,{}).get('do-reasoning',False)
		for ontology in self.ontologies:
			self.GetOntology(ontology, doReasoner=doReasoner, cachePath=cachePath)
	
	@classmethod
	def GetCachePath(cls):
		doTempDir = False
		cachePath = None
		try:
			cachePath = xdg.BaseDirectory.save_cache_path('es.elixir.jsonValidator')
			# Is the directory writable?
			if not os.access(cachePath,os.W_OK):
				doTempDir = True
		except OSError as e:
			# As it was not possible to create the
			# directory at the cache path, go to the
			# temporary directory
			doTempDir = True
		
		if doTempDir:
			# The temporary directory should be
			# removed when the application using this
			# class finishes
			#cachePath = tempfile.mkdtemp(prefix="term", suffix="cache")
			#atexit.register(shutil.rmtree, cachePath, ignore_errors=True)
			cachePath = os.path.join(tempfile.gettempdir(),'cache_es.elixir.jsonValidator')
			os.makedirs(cachePath, exist_ok=True)
		
		return cachePath
	
	MetadataPaths = {}
	
	@classmethod
	def GetMetadataPath(cls, iri_hash, cachePath=None):
		metadataPath = cls.MetadataPaths.get(iri_hash)
		if metadataPath is None:
			if cachePath is None:
				cachePath = cls.GetCachePath()
			
			metadataPath = os.path.join(cachePath,'metadata_{0}.json'.format(iri_hash))
			cls.MetadataPaths[iri_hash] = metadataPath
		
		return metadataPath
	
	TermWorldsPaths = {}
	
	@classmethod
	def GetWorldDBPath(cls, iri_hash , cachePath=None):
		termWorldPath = cls.TermWorldsPaths.get(iri_hash)
		if termWorldPath is None:
			if cachePath is None:
				cachePath = cls.GetCachePath()
			
			termWorldPath = os.path.join(cachePath,'owlready2_{0}.sqlite3'.format(iri_hash))
			cls.TermWorldsPaths[iri_hash] = termWorldPath
		
		return termWorldPath
	
	@classmethod
	def GetOntologyPath(cls, iri_hash, cachePath=None):
		if cachePath is None:
			cachePath = cls.GetCachePath()
		
		ontologyPath = os.path.join(cachePath,'ontology_{0}.owl'.format(iri_hash))
		
		return ontologyPath
	
	ONTO_CACHE = {}
	
	@classmethod
	def InvalidateWorld(cls, iri, cachePath=None):
		# First, close the world and dispose its instance
		iri_hash = hashlib.sha1(iri.encode('utf-8')).hexdigest()
		
		w = cls.TermWorlds.pop(iri_hash)
		if w:
			# Removing the reference to the ontology
			onto = cls.ONTO_CACHE.pop(iri_hash)
			if onto:
				del onto
			w.close()
			del w
		
		# Then, remove the metadata
		metadataPath = cls.MetadataPaths.pop(iri_hash)
		if metadataPath and os.path.exists(metadataPath):
			os.unlink(metadataPath)
		
		# Last, remove the world database
		worldDBPath = cls.TermWorldsPaths.pop(iri_hash)
		if worldDBPath and os.path.exists(worldDBPath):
			os.unlink(worldDBPath)
		
		ontologyPath = cls.GetOntologyPath(iri_hash,cachePath)
		if os.path.exists(ontologyPath):
			os.unlink(ontologyPath)
	
	@classmethod
	def InvalidateAllWorlds(cls, cachePath=None):
		# First, close the world and dispose its instance
		if cls.TermWorlds:
			for iri_hash, w in cls.TermWorlds.items():
				# Removing the reference to the ontology
				onto = cls.ONTO_CACHE.pop(iri_hash)
				if onto:
					del onto
				
				w.close()
				del w
				
				# Then, remove the metadata
				metadataPath = cls.MetadataPaths.pop(iri_hash)
				if metadataPath and os.path.exists(metadataPath):
					os.unlink(metadataPath)
				
				# Last, remove the world database
				worldDBPath = cls.TermWorldsPaths.pop(iri_hash)
				if worldDBPath and os.path.exists(worldDBPath):
					os.unlink(worldDBPath)
				
				ontologyPath = cls.GetOntologyPath(iri_hash,cachePath)
				if os.path.exists(ontologyPath):
					os.unlink(ontologyPath)
			
			cls.TermWorlds.clear()
	
	IRI_HASH = {}
	TermWorlds = {}
	
	@classmethod
	def GetOntology(cls, iri, doReasoner=False, cachePath=None):
		iri_hash = cls.IRI_HASH.get(iri)
		
		if iri_hash is None:
			cls.IRI_HASH[iri] = iri_hash = hashlib.sha1(iri.encode('utf-8')).hexdigest()
		
		onto = cls.ONTO_CACHE.get(iri_hash)
		if onto is None:
			worldDB = cls.TermWorlds.get(iri_hash)
			
			if worldDB is None:
				worldDBPath = cls.GetWorldDBPath(iri_hash,cachePath)
				
				# Activate this only if you want to save a copy of the ontologies
				#ontologiesPath = os.path.join(cachePath,'ontologies')
				#os.makedirs(ontologiesPath,exist_ok=True)
				#owlready2.onto_path.append(ontologiesPath)
				worldDB = owlready2.World(filename=worldDBPath, exclusive=False)
				cls.TermWorlds[iri_hash] = worldDB
			
			# Trying to get the metadata useful for an optimal ontology download
			metadataPath = cls.GetMetadataPath(iri_hash,cachePath)
			if os.path.exists(metadataPath):
				try:
					with open(metadataPath,mode='r',encoding='utf-8') as metadata_fh:
						metadata = json.load(metadata_fh)
				except:
					# A corrupted cache should not disturb
					metadata = {}
			else:
				metadata = {}
			
			ontologyPath = cls.GetOntologyPath(iri_hash,cachePath)
			gotPath = None
			gotMetadata = None
			try:
				gotPath,gotMetadata = download_file(iri,ontologyPath,metadata)
			except urllib.error.HTTPError as he:
				if he.code < 500:
					raise he
				else:
					print("WARNING: transient error fetching {}. {}".format(iri, he),file=sys.stderr)
			if gotPath:
				gotMetadata['orig_url'] = iri
				# Reading the ontology
				with open(ontologyPath,mode="rb") as onto_fh:
					onto = worldDB.get_ontology(iri).load(fileobj=onto_fh,reload=True)
				
				# Save the metadata
				with open(metadataPath,mode="w",encoding="utf-8") as metadata_fh:
					json.dump(gotMetadata,metadata_fh)
				
				# Re-save once the reasoner has run
				if doReasoner:
					worldDB.save()
					owlready2.sync_reasoner(onto)
			else:
				onto = worldDB.get_ontology(iri).load()
			worldDB.save()
			
			# And now unlink the ontology (if exists)
			if os.path.exists(ontologyPath):
				os.unlink(ontologyPath)
			
			cls.ONTO_CACHE[iri_hash] = onto
		
		return onto
	
	def isValid(self,validator,ontlist,term,schema):
		# Getting the potential parents
		ancestors = schema.get(self.AncestorsAttrName,[])
		partialMatches = str(schema.get(self.MatchTypeAttrName,'exact'))
		
		if partialMatches not in self.VALID_MATCHES:
			raise ValidationError("attribute '{0}' is {1} but it must be one of the next values: {2}".format(self.MatchTypeAttrName,partialMatches,self.VALID_MATCHES.keys()))
		
		if not isinstance(ancestors,list):
			ancestors = [ ancestors ]
		
		if partialMatches == 'suffix':
			ancestorPats = [ '*' + ancestor   for ancestor in ancestors ]
		else:
			ancestorPats = ancestors
		
		searchType = self.VALID_MATCHES[partialMatches]
		
		termPat = '*' + term  if partialMatches == 'suffix' else term
		queryParams = {
			searchType: termPat 
		}
		isValid = False
		invalidAncestors = False
		cachePath = self.config.get('cacheDir')
		doReasoner = self.config.get(self.KeyAttributeName,{}).get('do-reasoning',False)
		for ontology in ontlist:
			onto = self.GetOntology(ontology, doReasoner = doReasoner, cachePath = cachePath)
			
			foundTerms = onto.search(**queryParams)
			# Is the term findable with these conditions?
			if foundTerms:
				if ancestors:
					# Searching ancestors' terms
					foundAncestors = []
					for ancestorPat in ancestorPats:
						foundAncestors.extend(onto.search(iri = ancestorPat))
					
					# Skip to the next if the list is empty
					if not foundAncestors:
						continue
					
					foundAncestorsSet = set(foundAncestors)
					
					# Now, find terms with these ancestors validate against the parents
					for foundTerm in foundTerms:
						termAncestors = foundTerm.ancestors()
						if termAncestors.intersection(foundAncestorsSet):
							isValid = True
							break
					
					# Continue searching
					if not isValid:
						invalidAncestors = True
						continue
				else:
					isValid = True
				break
		
		if not isValid:
			if invalidAncestors:
				raise ValidationError("Term {0} does not have as ancestor(s) any of {1} in these ontologies: {2}".format(term,' , '.join(ancestors),' '.join(ontlist)))
			else:
				raise ValidationError("Term {0} was not found in these ontologies: {1}".format(term,' '.join(ontlist)))
		return True
	
	@classmethod
	def IsTerm(cls,checker,instance):
		if not isinstance(instance, str_types):
			return False
		
		# Right now we are only considering fully qualified terms, i.e. URIs
		return rfc3987.parse(instance, rule="URI")
	
	def validate(self,validator,ontology,term,schema):
		"""
		This method is here to be registered with custom validators
		"""
		# We do the validation only when the format is defined
		if schema.get("format") == self.FormatName:
			if ontology is None:
				yield ValidationError("Attribute {0} has not been defined".format(self.KeyAttributeName))
			
			ontlist = ontology  if isinstance(ontology,list) else [ ontology ]
			
			if len(ontlist) == 0:
				yield ValidationError("Attribute {0} does not have any ontology".format(self.KeyAttributeName))
			
			# First, having something workable
			try:
				isValid = True
				for ont in ontlist:
					parsed_ont = rfc3987.parse(ont, rule="URI")
					
					scheme = parsed_ont.get('scheme')
					if scheme not in self.VALID_SCHEMES:
						isValid = False
						yield ValidationError("Ontology {0} is not public available".format(ont))
				
				# Now, let's check against the list of ontologies!
				if isValid and not self.isValid(validator,ontlist,term,schema):
					yield ValidationError("Term {0} was not found in these ontologies: {1}".format(term,ontlist))
			except SyntaxError:
				t,v,tr = sys.exc_info()
				import pprint
				pprint.pprint(t)
				pprint.pprint(v)
				pprint.pprint(tr)
				sys.exit(1)
			#except:
			#	t,v,tr = sys.exc_info()
			#	import pprint
			#	pprint.pprint(t)
			#	pprint.pprint(v)
			#	pprint.pprint(tr)
			#	sys.exit(1)
			except ValidationError as v:
				yield v
			except ValueError as ve:
				yield ValidationError("Unable to parse ontology {0}: {1}".format(ontlist,str(ve)))
			except urllib.error.HTTPError as he:
				yield ValidationError("Unable to fetch ontology {0} [{1}]: {2}".format(ontlist,he.code,he.reason))
			except urllib.error.URLError as ue:
				yield ValidationError("Unable to fetch ontology {0}: {1}".format(ontlist,ue.reason))
			except BaseException as be:
				yield ValidationError("Unexpected error: {}".format(str(be)))

	
	@classmethod
	def IsCorrectFormat(cls, value, schema = None):
		"""
		In empty context
		return true
		"""
		if schema is None:
			return True
		else:
			termIns = cls(None)
			for val in termIns.validate(None,schema.get(cls.KeyAttributeName),value,schema):
				if isinstance(val,ValidationError):
					return False
			return True
