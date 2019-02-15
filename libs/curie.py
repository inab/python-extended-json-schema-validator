#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import rfc3987
import uritools
from jsonschema.compat import str_types
from jsonschema.exceptions import FormatError, ValidationError

class Curie(object):
	def __init__(self,curie):
		if isinstance(curie,object):
			self.curie = curie
		else:
			self.curie = uritools.urisplit(curie)
	
	def isValid(self,validator,nslist,origValue,schema):
		found = False
		for namespace in nslist:
			if self.curie[0] == namespace:
				found = True
				break
		
		return found
	
	@classmethod
	def IsCurie(cls,checker,instance):
		if not isinstance(instance, str_types):
			return False
		
		return rfc3987.parse(instance, rule="URI")
	
	@classmethod
	def IsValidCurie(cls,validator,namespace,value,schema):
		# First, having something workable
		try:
			if isinstance(value,Curie):
				curie = value
			else:
				curie = Curie(value)
			
			nslist = namespace  if isinstance(namespace,list) else [ namespace ]
			
			# Now, let's check!
			if not curie.isValid(validator,nslist,value,schema):
				yield ValidationError("CURIE {0} does not belong to one of the allowed schemes: {1}".format(value,nslist))
		except ValueError as ve:
			yield ValidationError("Unable to parse CURIE {0}: {1}".format(value,str(ve)))
		except BaseException as be:
			yield ValidationError("Unexpected error: {}".format(str(be)))
