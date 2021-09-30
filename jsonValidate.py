#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
import os
import argparse
import logging
import time
import json

import yaml
# We have preference for the C based loader and dumper, but the code
# should fallback to default implementations when C ones are not present
try:
	from yaml import CLoader as YAMLLoader, CDumper as YAMLDumper
except ImportError:
	from yaml import Loader as YAMLLoader, Dumper as YAMLDumper

from extended_json_schema_validator import version as ejsv_version
from extended_json_schema_validator.extensible_validator import ExtensibleValidator

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

DEFAULT_LOGGING_FORMAT = '%(asctime)-15s - [%(levelname)s] %(message)s'

if __name__ == "__main__":
	disable_outerr_buffering()
	ap = argparse.ArgumentParser(description=f"Validate JSON against JSON Schemas with extensions (version {ejsv_version})")
	ap.add_argument('--log-file', dest="logFilename", help='Store messages in a file instead of using standard error and standard output')
	ap.add_argument('--log-format', dest='logFormat', help='Format of log messages', default=DEFAULT_LOGGING_FORMAT)
	ap.add_argument('-q', '--quiet', dest='logLevel', action='store_const', const=logging.WARNING, help='Only show engine warnings and errors')
	ap.add_argument('-v', '--verbose', dest='logLevel', action='store_const', const=logging.INFO, help='Show verbose (informational) messages')
	ap.add_argument('-d', '--debug', dest='logLevel', action='store_const', const=logging.DEBUG, help='Show debug messages (use with care, as it could potentially disclose sensitive contents)')
	ap.add_argument('-C','--config',dest="configFilename",help="Configuration file (used by extensions)")
	ap.add_argument('--cache-dir',dest="cacheDir",help="Caching directory (used by extensions)")
	
	ap.add_argument('--report',dest="reportFilename",help="Store validation report (in JSON format) in a file")
	ap.add_argument('--verbose-report',dest="isQuietReport",help="When this flag is enabled, the report also embeds the json contents which were validated", action='store_false', default=True)
	
	grp0 = ap.add_mutually_exclusive_group()
	grp0.add_argument('--invalidate', help="Caches are invalidated on startup", action='store_true')
	grp0.add_argument('--read-only', dest="isRWCache",help="When this flag is enabled, the caches are read-only, avoiding expensive operations related to the caches", action='store_false', default=True)
	grp = ap.add_mutually_exclusive_group()
	grp.add_argument('--warm-up',dest="warmUp",help="Caches are warmed up on startup", action='store_const', const=True)
	grp.add_argument('--lazy-load',dest="warmUp",help="Caches are warmed up in a lazy way", action='store_false')
	ap.add_argument('jsonSchemaDir', metavar='json_schema_or_dir', help='The JSON Schema file or directory to validate and use')
	ap.add_argument('json_files', metavar='json_file_or_dir', nargs='*', help='The JSON files or directories to be validated')
	ap.add_argument('-V', '--version', action='version', version='%(prog)s version ' + ejsv_version)
	args = ap.parse_args()
	
	loggingConfig = {
		'format': args.logFormat
	}
	
	logLevel = logging.INFO
	if args.logLevel:
		logLevel = args.logLevel
	loggingConfig['level'] = logLevel
	
	if args.logFilename is not None:
		loggingConfig['filename'] = args.logFilename
	#	loggingConfig['encoding'] = 'utf-8'
	
	logging.basicConfig(**loggingConfig)
	
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
	
	ev = ExtensibleValidator(config=local_config, isRW=args.isRWCache)
	
	isVerbose = logLevel <= logging.INFO
	numSchemas = ev.loadJSONSchemas(args.jsonSchemaDir, verbose=isVerbose)
	
	if numSchemas > 0:
		# Should we invalidate caches?
		if args.invalidate:
			logging.info("\n* Invalidating caches.")
			ev.invalidateCaches()

		if args.warmUp:
			logging.info("\n* Warming up caches...")
			t0 = time.time()
			ev.warmUpCaches()
			t1 = time.time()
			logging.info("\t{} seconds".format(t1-t0))
			
			
	if len(sys.argv) > 2:
		if numSchemas == 0:
			logging.critical("FATAL ERROR: No schema was successfully loaded. Exiting...\n")
			sys.exit(1)
		
		jsonFiles = tuple(args.json_files)
		report = ev.jsonValidate(*jsonFiles,verbose=isVerbose)
		
		if args.reportFilename is not None:
			logging.info("* Storing JSON report at {}".format(args.reportFilename))
			if args.isQuietReport:
				for rep in report:
					del rep['json']
			with open(args.reportFilename,mode='w',encoding='utf-8') as repH:
				json.dump(report,repH,indent=4,sort_keys=True)
		
		exitCode = 0
		for rep in report:
			if len(rep['errors']) > 0:
				exitCode = 2
				break
		
		sys.exit(exitCode)