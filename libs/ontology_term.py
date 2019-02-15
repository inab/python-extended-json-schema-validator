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
	def __init__(self,term):
		self.term = term
	
	@classmethod
	def GetWorld(cls):
		if not hasattr(cls,'TermWorld'):
			cachePath = xdg.BaseDirectory.save_cache_path('es.elixir.jsonValidator')
			ontologiesPath = os.path.join(cachePath,'ontologies')
			os.makedirs(ontologiesPath,exist_ok=True)
			owlready2.onto_path.append(ontologiesPath)
			setattr(cls,'TermWorld',owlready2.World(filename=os.path.join(cachePath,'owlready2.sqlite3'), exclusive=False))
		
		return getattr(cls,'TermWorld')
	
	def isValid(self,validator,ontlist,origValue,schema):
		w = OntologyTerm.GetWorld()
		
		isValid = False
		for ontology in ontlist:
			onto = w.get_ontology(ontology).load()
			onto.save()
			
			if len(onto.search(iri = ('*' + self.term))) > 0:
				isValid = True
				break
		
		return isValid
	
	@classmethod
	def IsTerm(cls,checker,instance):
		if not isinstance(instance, str_types):
			return False
		
		# Right now we are only considering fully qualified terms, i.e. URIs
		return rfc3987.parse(instance, rule="URI")
	
	@classmethod
	def IsValidTerm(cls,validator,ontology,value,schema):
		# First, having something workable
		if isinstance(value,OntologyTerm):
			term = value
		else:
			term = OntologyTerm(value)
		ontlist = ontology  if isinstance(ontology,list) else [ ontology ]
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
		except ValueError as ve:
			yield ValidationError("Unable to parse ontology {0}: {1}".format(ontlist,str(ve)))
		except urllib.error.HTTPError as he:
			yield ValidationError("Unable to fetch ontology {0} [{1}]: {2}".format(ontlist,he.code,he.reason))
		except urllib.error.URLError as ue:
			yield ValidationError("Unable to fetch ontology {0}: {1}".format(ontlist,ue.reason))
		except BaseException as be:
			yield ValidationError("Unexpected error: {}".format(str(be)))
