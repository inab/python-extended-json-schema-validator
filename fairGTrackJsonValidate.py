#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import print_function
import sys
import os

# This is needed to assure open suports encoding parameter
if sys.version_info[0] == 2:
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

# From http://stackoverflow.com/a/3678114
def disable_outerr_buffering():
	# Appending to gc.garbage is a way to stop an object from being
	# destroyed.  If the old sys.stdout is ever collected, it will
	# close() stdout, which is not good.
	# gc.garbage.append(sys.stdout)
	sys.stdout = os.fdopen(sys.stdout.fileno(), 'w', 0)
	# gc.garbage.append(sys.stderr)
	sys.stderr = os.fdopen(sys.stderr.fileno(), 'w', 0)

if sys.version_info[0] == 2:
	disable_outerr_buffering()

from fairtracks_validator.validator import FairGTracksValidator

if len(sys.argv) > 1:
	jsonSchemaDir = sys.argv[1]
	
	jsonSchema = None
	
	fgv = FairGTracksValidator()
	numSchemas = fgv.loadJSONSchemas(jsonSchemaDir,verbose=True)
	
	if len(sys.argv) > 2:
		if numSchemas == 0:
			print("FATAL ERROR: No schema was successfuly loaded. Exiting...\n",file=sys.stderr)
			sys.exit(1)
		
		args = tuple(sys.argv[2:])
		fgv.jsonValidate(*args,verbose=True)
else:
	print("Usage: {0} {{JSON schema}} {{JSON file}}*".format(sys.argv[0]),file=sys.stderr)
	sys.exit(1)
