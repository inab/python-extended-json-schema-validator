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

class OntologyTerm(object):
	VALID_MATCHES = {
		'exact': 'iri',
		'suffix': 'iri',
		'label': 'label'
	}
	
	FormatName = 'term'
	KeyAttributeName = 'ontology'
	
	def __init__(self,term):
		self.term = term
	
	@classmethod
	def GetWorld(cls):
		if not hasattr(cls,'TermWorld'):
			cachePath = xdg.BaseDirectory.save_cache_path('es.elixir.jsonValidator')
			
			# Activate this only if you want to save a copy of the ontologies
			#ontologiesPath = os.path.join(cachePath,'ontologies')
			#os.makedirs(ontologiesPath,exist_ok=True)
			#owlready2.onto_path.append(ontologiesPath)
			setattr(cls,'TermWorld',owlready2.World(filename=os.path.join(cachePath,'owlready2.sqlite3'), exclusive=False))
		
		return getattr(cls,'TermWorld')
	
	def isValid(self,validator,ontlist,origValue,schema):
		w = OntologyTerm.GetWorld()
		
		# Getting the potential parents
		ancestors = schema.get('ancestors',[])
		partialMatches = str(schema.get('matchType','exact'))
		
		if partialMatches not in OntologyTerm.VALID_MATCHES:
			raise ValidationError("attribute 'matchType' is {0} but it must be one of the next values: {1}".format(partialMatches,OntologyTerm.VALID_MATCHES.keys()))
		
		if not isinstance(ancestors,list):
			ancestors = [ ancestors ]
		
		if partialMatches == 'suffix':
			ancestorPats = [ '*' + ancestor   for ancestor in ancestors ]
		else:
			ancestorPats = ancestors
		
		searchType = OntologyTerm.VALID_MATCHES[partialMatches]
		
		termPat = '*' + self.term  if partialMatches == 'suffix' else self.term
		queryParams = {
			searchType: termPat 
		}
		isValid = False
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
						continue
				else:
					isValid = True
				break
		
		if not isValid:
			if ancestors:
				raise ValidationError("Term {0} , forced to have ancestors {1} was not found in these ontologies: {2}".format(self.term,ancestors,ontlist))
			else:
				raise ValidationError("Term {0} was not found in these ontologies: {1}".format(self.term,ontlist))
		return True
	
	@classmethod
	def IsTerm(cls,checker,instance):
		if not isinstance(instance, str_types):
			return False
		
		# Right now we are only considering fully qualified terms, i.e. URIs
		return rfc3987.parse(instance, rule="URI")
	
	@classmethod
	def IsValidTerm(cls,validator,ontology,value,schema):
		"""
		This method is here to be registered with custom validators
		"""
		# We do the validation only when the format is defined
		if schema.get("format") == cls.FormatName:
			if ontology is None:
				yield ValidationError("Attribute {0} has not been defined".format(cls.KeyAttributeName))
			
			ontlist = ontology  if isinstance(ontology,list) else [ ontology ]
			
			if len(ontlist) == 0:
				yield ValidationError("Attribute {0} does not have any ontology".format(cls.KeyAttributeName))
			
			# First, having something workable
			if isinstance(value,cls):
				term = value
			else:
				term = cls(value)
			
			try:
				isValid = True
				for ont in ontlist:
					parsed_ont = rfc3987.parse(ont, rule="URI")
					
					scheme = parsed_ont.get('scheme')
					if scheme != 'http' and scheme != 'https':
						isValid = False
						yield ValidationError("Ontology {0} is not public available".format(ont))
				
				# Now, let's check against the list of ontologies!
				if isValid and not term.isValid(validator,ontlist,value,schema):
					yield ValidationError("Term {0} was not found in these ontologies: {1}".format(term.term,ontlist))
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
			for val in cls.IsValidTerm(None,schema.get(cls.KeyAttributeName),value,schema):
				if isinstance(val,ValidationError):
					return False
			return True
