#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import print_function
import sys
import os
import argparse
import time

import yaml
# We have preference for the C based loader and dumper, but the code
# should fallback to default implementations when C ones are not present
try:
	from yaml import CLoader as YAMLLoader, CDumper as YAMLDumper
except ImportError:
	from yaml import Loader as YAMLLoader, Dumper as YAMLDumper

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
	if sys.version_info[0] == 2:
		# Appending to gc.garbage is a way to stop an object from being
		# destroyed.  If the old sys.stdout is ever collected, it will
		# close() stdout, which is not good.
		# gc.garbage.append(sys.stdout)
		sys.stdout = os.fdopen(sys.stdout.fileno(), 'w', 0)
		# gc.garbage.append(sys.stderr)
		sys.stderr = os.fdopen(sys.stderr.fileno(), 'w', 0)
	else:
		import io
		sys.stdout = io.TextIOWrapper(open(sys.stdout.fileno(), 'wb', 0), write_through=True)
		sys.stderr = io.TextIOWrapper(open(sys.stderr.fileno(), 'wb', 0), write_through=True)

disable_outerr_buffering()

from fairtracks_validator.validator import FairGTracksValidator

if __name__ == "__main__":
	ap = argparse.ArgumentParser(description="Validate JSON against JSON Schemas with extensions")
	ap.add_argument('-C','--config',dest="configFilename",help="Configuration file (used by extensions)")
	ap.add_argument('--cache-dir',dest="cacheDir",help="Caching directory (used by extensions)")
	ap.add_argument('--invalidate',help="Caches are invalidated on startup", action='store_true')
	grp = ap.add_mutually_exclusive_group()
	grp.add_argument('--warm-up',dest="warmUp",help="Caches are warmed up on startup", action='store_const', const=True)
	grp.add_argument('--lazy-load',dest="warmUp",help="Caches are warmed up in a lazy way", action='store_false')
	ap.add_argument('jsonSchemaDir', metavar='json_schema', help='The JSON Schema file or directory to validate and use')
	ap.add_argument('json_files', metavar='json_file', nargs='*',help='The JSON files or directories to be validated')
	args = ap.parse_args()
	
	# First, try loading the configuration file
	if args.configFilename:
		with open(args.configFilename,"r",encoding="utf-8") as cf:
			local_config = yaml.load(cf,Loader=YAMLLoader)
	else:
		local_config = {}
	
	# Then, override based on parameters and flags
	if args.cacheDir:
		local_config['cacheDir'] = args.cacheDir
	
	# In any case, assuring the cache directory does exist
	cacheDir = local_config.get('cacheDir')
	if cacheDir:
		os.makedirs(cacheDir, exist_ok=True)
	
	fgv = FairGTracksValidator(config=local_config)
	
	numSchemas = fgv.loadJSONSchemas(args.jsonSchemaDir,verbose=True)
	
	if numSchemas > 0:
		# Should we invalidate caches?
		if args.invalidate:
			print("\n* Invalidating caches.")
			fgv.invalidateCaches()

		if args.warmUp:
			print("\n* Warming up caches...")
			t0 = time.time()
			fgv.warmUpCaches()
			t1 = time.time()
			print("\t{} seconds".format(t1-t0))
			
			
	if len(sys.argv) > 2:
		if numSchemas == 0:
			print("FATAL ERROR: No schema was successfuly loaded. Exiting...\n",file=sys.stderr)
			sys.exit(1)
		
		jsonFiles = tuple(args.json_files)
		fgv.jsonValidate(*jsonFiles,verbose=True)
