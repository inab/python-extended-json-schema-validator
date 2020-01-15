#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import rfc3987
from jsonschema.compat import str_types
from jsonschema.exceptions import FormatError, ValidationError

import owlready2
import xdg.BaseDirectory
import os
import urllib
import sys
import tempfile
import shutil
import atexit

from .abstract_check import AbstractCustomFeatureValidator

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
		(id2ElemId , keyList , _) = refSchemaTuple
		
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
		self.InvalidateWorld()
	
	def warmUpCaches(self):
		w = self.GetWorld()
		for ontology in self.ontologies:
			onto = w.get_ontology(ontology).load()
			w.save()
			# Only activate this if you want a copy of the ontology,
			# but it fires revalidations everytime!
			#onto.save()
	
	@classmethod
	def GetWorldDBPath(cls):
		if not hasattr(cls,'TermWorldPath'):
			doTempDir = False
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
		
			setattr(cls,'TermWorldPath',os.path.join(cachePath,'owlready2.sqlite3'))
		
		return getattr(cls,'TermWorldPath')
	
	@classmethod
	def InvalidateWorld(cls):
		# First, close the world and dispose its instance
		if hasattr(cls,'TermWorld'):
			w = getattr(cls,'TermWorld')
			w.close()
			del w
			delattr(cls,'TermWorld')
		
		# Then, remove the world database
		worldDBPath = cls.GetWorldDBPath()
		delattr(cls,'TermWorldPath')
		if os.path.exists(worldDBPath):
			os.unlink(worldDBPath)
	
	@classmethod
	def GetWorld(cls):
		if not hasattr(cls,'TermWorld'):
			worldDBPath = cls.GetWorldDBPath()
			
			# Activate this only if you want to save a copy of the ontologies
			#ontologiesPath = os.path.join(cachePath,'ontologies')
			#os.makedirs(ontologiesPath,exist_ok=True)
			#owlready2.onto_path.append(ontologiesPath)
			setattr(cls,'TermWorld',owlready2.World(filename=worldDBPath, exclusive=False))
		
		return getattr(cls,'TermWorld')
	
	def isValid(self,validator,ontlist,term,schema):
		w = self.GetWorld()
		
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
		for ontology in ontlist:
			onto = w.get_ontology(ontology).load()
			w.save()
			# Only activate this if you want a copy of the ontology,
			# but it fires revalidations everytime!
			#onto.save()
			
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
