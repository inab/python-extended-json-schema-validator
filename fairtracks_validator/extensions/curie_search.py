#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import rfc3987
import uritools
from jsonschema.compat import str_types
from jsonschema.exceptions import FormatError, ValidationError

from .curie_cache import CurieCache, Curie
import re
import xdg
import os, sys
import tempfile
import shutil
import atexit

class CurieSearch(object):
	VALID_MATCHES = {
		'canonical': None,
		'loose': None,
		'basic': None
	}
	
	FormatName = 'curie'
	KeyAttributeName = 'namespace'
	
	def __init__(self,curie):
		if isinstance(curie,object):
			self.curie = curie
		else:
			self.curie = uritools.urisplit(curie)
	
	def isValid(self,validator,nslist,origValue,schema):
		found = False
		matchType = str(schema.get('matchType','loose'))  if schema is not None  else 'canonical'
		if matchType not in CurieSearch.VALID_MATCHES:
			raise ValidationError("attribute 'matchType' is {0} but it must be one of the next values: {1}".format(matchType,CurieSearch.VALID_MATCHES.keys()))
		
		cache = CurieSearch.GetCurieCache()
		
		parsed = None
		try:
			parsed = rfc3987.parse(origValue, rule="URI")
		except BaseException as be:
			if matchType != 'loose':
				raise be
		# Trying to decide the matching mode
		prefix = None
		if parsed:
			prefix = parsed.get('scheme')
			if prefix:
				if (len(prefix) > 0) and (matchType == 'loose'):
					matchType = 'canonical'

		if matchType == 'basic':
			# Basic mode is like canonical, but without querying identifiers.org cache
			found = nslist and (prefix in nslist)
		elif matchType == 'loose':
			if nslist:
				valToVal = origValue
				validatedCURIEs = list(filter(lambda curie: curie is not None,map(lambda namespace: cache.get(namespace),nslist)))
				if not validatedCURIEs:
					raise ValidationError('No namespace from {} was found in identifiers.org cache'.format(nslist))
				
				# Looking for a match
				for curie in validatedCURIEs:
					pat = re.compile(curie.pattern)
					if pat.search(valToVal):
						found = True
						break
			else:
				raise ValidationError('In "loose" mode, at least one namespace must be declared')
		elif prefix is None:
			raise ValidationError('In "canonical" mode, the value must be prefixed by the namespace')
		else:
			# Searching in canonical mode. To do that, we have to remove the prefix
			valToVal = origValue[(origValue.find(':')+1):]
			# The case where the namespace list is empty
			if nslist and (prefix not in nslist):
				raise ValidationError('The namespace {} is not in the list of the accepted ones: {}'.format(prefix,nslist))
			
			curie = cache.get(prefix)
			if not curie:
				raise ValidationError('The namespace {} was not found in identifiers.org cache'.format(prefix))
		
			pat = re.compile(curie.pattern)
			found = pat.search(valToVal) or pat.search(origVal)
		
		return found
	
	@classmethod
	def GetCurieCache(cls):
		if not hasattr(cls,'CurieCache'):
			doTempDir = False
			try:
				cachePath = xdg.BaseDirectory.save_cache_path('es.elixir.jsonValidator')
				# Is the directory writable?
				if not os.access(cachePath,os.W_OK):
					doTempDir = True
			except OSError as e:
				# As it was not possible to create the
				# directory at the cache path, create a
				# temporary directory
				doTempDir = True
			
			if doTempDir:
				# The temporary directory should be
				# removed when the application using this
				# class finishes
				#cachePath = tempfile.mkdtemp(prefix="curie", suffix="cache")
				#atexit.register(shutil.rmtree, cachePath, ignore_errors=True)
				cachePath = os.path.join(tempfile.gettempdir(),'cache_es.elixir.jsonValidator')
				os.makedirs(cachePath, exist_ok=True)
			
			setattr(cls,'CurieCache',CurieCache(filename=os.path.join(cachePath,'CURIE_cache.sqlite3')))
		
		return getattr(cls,'CurieCache')
	
	@classmethod
	def IsCurie(cls,checker,instance):
		if isinstance(instance,Curie):
			return True
		
		if not isinstance(instance, str_types):
			return False
		
		parsed = rfc3987.parse(instance, rule="URI")
		if parsed and parsed.get('scheme'):
			cache = cls.GetCurieCache()
			curie = cache.get(parsed.get('scheme'))
			pat = re.compile(curie.pattern)
			return pat.search(instance[(instance.find(':')+1):])
		else:
			return False
	
	@classmethod
	def IsValidCurie(cls,validator,namespace,value,schema):
		"""
		This method is here to be registered with custom validators
		"""
		# We do the validation only when the format is defined
		if (schema is None) or (schema.get("format") == cls.FormatName):
			# Managing the different cases
			if namespace is None:
				nslist = []
			elif isinstance(namespace,list):
				nslist = namespace
			else:
				nslist = [ namespace ]
			
			try:
				# First, having something workable
				if isinstance(value,cls):
					curieS = value
				else:
					curieS = cls(value)
				
				# Now, let's check!
				if not curieS.isValid(validator,nslist,value,schema):
					yield ValidationError("Value '{0}' does not validate to any of the allowed schemes: {1}".format(value,nslist))
			except ValidationError as v:
				yield v
			except ValueError as ve:
				yield ValidationError("Unable to parse CURIE {0}: {1}".format(value,str(ve)))
			except BaseException as be:
				import traceback
				traceback.print_exc()
				yield ValidationError("Unexpected error: {}".format(str(be)))
	
	@classmethod
	def IsCorrectFormat(cls, value, schema = None):
		"""
		In empty context where the value is not a CURIE
		return true
		"""
		if schema and (':' in str(value)):
			for val in cls.IsValidCurie(None,schema.get(cls.KeyAttributeName) if schema else None,value,schema):
				if isinstance(val,ValidationError):
					print(val.message,file=sys.stderr)
					return False
		return True
