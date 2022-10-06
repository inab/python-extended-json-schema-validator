#!/usr/bin/env python
# -*- coding: utf-8 -*-

import argparse
import copy
import json
import logging
import os
import sys
import time

import jsonpath_ng  # type: ignore[import]
import jsonpath_ng.ext  # type: ignore[import]
import yaml

from . import version as ejsv_version
from .extensible_validator import ExtensibleValidator

from typing import cast, TYPE_CHECKING

if TYPE_CHECKING:
	from typing import (
		Any,
		MutableMapping,
	)

# This is needed to assure open suports encoding parameter
if sys.version_info[0] == 2:
	# py2
	import codecs
	import warnings

	def open(
		file,
		mode="r",
		buffering=-1,
		encoding=None,
		errors=None,
		newline=None,
		closefd=True,
		opener=None,
	):
		if newline is not None:
			warnings.warn("newline is not supported in py2")
		if not closefd:
			warnings.warn("closefd is not supported in py2")
		if opener is not None:
			warnings.warn("opener is not supported in py2")
		return codecs.open(
			filename=file,
			mode=mode,
			encoding=encoding,
			errors=errors,
			buffering=buffering,
		)


# From http://stackoverflow.com/a/3678114
def disable_outerr_buffering() -> None:
	if sys.version_info[0] == 2:
		# Appending to gc.garbage is a way to stop an object from being
		# destroyed.  If the old sys.stdout is ever collected, it will
		# close() stdout, which is not good.
		# gc.garbage.append(sys.stdout)
		sys.stdout = os.fdopen(sys.stdout.fileno(), "w", 0)
		# gc.garbage.append(sys.stderr)
		sys.stderr = os.fdopen(sys.stderr.fileno(), "w", 0)
	else:
		import io

		sys.stdout = io.TextIOWrapper(
			open(sys.stdout.fileno(), "wb", 0), write_through=True
		)
		sys.stderr = io.TextIOWrapper(
			open(sys.stderr.fileno(), "wb", 0), write_through=True
		)


DEFAULT_LOGGING_FORMAT = "%(asctime)-15s - [%(levelname)s] %(message)s"


def main() -> None:
	disable_outerr_buffering()
	ap = argparse.ArgumentParser(
		description=f"Validate JSON against JSON Schemas with extensions (version {ejsv_version})"
	)
	ap.add_argument(
		"--log-file",
		dest="logFilename",
		help="Store messages in a file instead of using standard error and standard output",
	)
	ap.add_argument(
		"--log-format",
		dest="logFormat",
		help="Format of log messages",
		default=DEFAULT_LOGGING_FORMAT,
	)
	ap.add_argument(
		"-q",
		"--quiet",
		dest="logLevel",
		action="store_const",
		const=logging.WARNING,
		help="Only show engine warnings and errors",
	)
	ap.add_argument(
		"-v",
		"--verbose",
		dest="logLevel",
		action="store_const",
		const=logging.INFO,
		help="Show verbose (informational) messages",
	)
	ap.add_argument(
		"-d",
		"--debug",
		dest="logLevel",
		action="store_const",
		const=logging.DEBUG,
		help="Show debug messages (use with care, as it could potentially disclose sensitive contents)",
	)
	ap.add_argument(
		"-C",
		"--config",
		dest="configFilename",
		help="Configuration file (used by extensions)",
	)
	ap.add_argument(
		"--cache-dir", dest="cacheDir", help="Caching directory (used by extensions)"
	)

	ap.add_argument(
		"--report",
		dest="reportFilename",
		help="Store validation report (in JSON format) in a file",
	)
	ap.add_argument(
		"--annotation",
		dest="annotReport",
		help="JSON Path (accepted by jsonpath-ng) to extract an annotation to include from validated JSON in the report (for instance, '$._id')",
	)
	ap.add_argument(
		"--verbose-report",
		dest="isQuietReport",
		help="When this flag is enabled, the report also embeds the json contents which were validated",
		action="store_false",
		default=True,
	)
	ap.add_argument(
		"--error-report",
		dest="isErrorReport",
		help="When this flag is enabled, the report only includes the entries with errors",
		action="store_true",
		default=False,
	)

	grp0 = ap.add_mutually_exclusive_group()
	grp0.add_argument(
		"--invalidate", help="Caches are invalidated on startup", action="store_true"
	)
	grp0.add_argument(
		"--read-only",
		dest="isRWCache",
		help="When this flag is enabled, the caches are read-only, avoiding expensive operations related to the caches",
		action="store_false",
		default=True,
	)
	grp = ap.add_mutually_exclusive_group()
	grp.add_argument(
		"--warm-up",
		dest="warmUp",
		help="Caches are warmed up on startup",
		action="store_const",
		const=True,
	)
	grp.add_argument(
		"--lazy-load",
		dest="warmUp",
		help="Caches are warmed up in a lazy way",
		action="store_false",
	)
	ap.add_argument(
		"jsonSchemaDir",
		metavar="json_schema_or_dir",
		help="The JSON Schema file or directory to validate and use",
	)
	ap.add_argument(
		"json_files",
		metavar="json_file_or_dir",
		nargs="*",
		help="The JSON files or directories to be validated",
	)
	ap.add_argument(
		"-V", "--version", action="version", version="%(prog)s version " + ejsv_version
	)
	args = ap.parse_args()

	loggingConfig = {"format": args.logFormat}

	logLevel = logging.INFO
	if args.logLevel:
		logLevel = args.logLevel
	loggingConfig["level"] = logLevel

	if args.logFilename is not None:
		loggingConfig["filename"] = args.logFilename
	# 	loggingConfig['encoding'] = 'utf-8'

	# tan goes into a stack overflow unless we tell not process it
	logging.basicConfig(**loggingConfig)  # fmt: skip

	# First, try loading the configuration file
	if args.configFilename:
		with open(args.configFilename, "r", encoding="utf-8") as cf:
			local_config = yaml.safe_load(cf)
	else:
		local_config = {}

	# Then, override based on parameters and flags
	if args.cacheDir:
		local_config["cacheDir"] = args.cacheDir

	# In any case, assuring the cache directory does exist
	cacheDir = local_config.get("cacheDir")
	if cacheDir:
		os.makedirs(cacheDir, exist_ok=True)

	ev = ExtensibleValidator(config=local_config, isRW=args.isRWCache)

	isVerbose = logLevel <= logging.INFO
	loadedSchemasStats = ev.loadJSONSchemasExt(args.jsonSchemaDir, verbose=isVerbose)

	exitCode = 0
	report = []
	if args.reportFilename is not None:
		if args.annotReport:
			annotP = jsonpath_ng.ext.parse(args.annotReport)
		else:
			annotP = None

		for loadedSchema in loadedSchemasStats.loadedSchemas.values():
			rep: "MutableMapping[str, Any]" = copy.copy(
				cast("MutableMapping[str, Any]", loadedSchema)
			)

			# Removing annoying instances
			if "customFormatInstances" in rep:
				del rep["customFormatInstances"]
			if "validator" in rep:
				del rep["validator"]

			if len(rep["errors"]) > 0:
				exitCode = 3
			elif args.isErrorReport:
				continue

			if annotP is not None:
				for match in annotP.find(rep["schema"]):
					rep["annot"] = match.value
					break
			if args.isQuietReport:
				del rep["schema"]

			report.append(rep)
	elif len(sys.argv) == 2:
		if loadedSchemasStats.numFileFail > 0:
			exitCode = 3

	if len(sys.argv) > 2:
		numSchemas = len(loadedSchemasStats.loadedSchemas.keys())
		if numSchemas == 0:
			logging.critical(
				"FATAL ERROR: No schema was successfully loaded. Exiting...\n"
			)
			sys.exit(1)

		# Should we invalidate caches before parsing?
		if args.invalidate:
			logging.info("\n* Invalidating caches.")
			ev.invalidateCaches()

		if args.warmUp:
			logging.info("\n* Warming up caches...")
			t0 = time.time()
			ev.warmUpCaches()
			t1 = time.time()
			logging.info("\t{} seconds".format(t1 - t0))

		# Now, time to parse
		jsonFiles = tuple(args.json_files)
		reportIter = ev.jsonValidateIter(*jsonFiles, verbose=isVerbose)

		exitCode = 0
		if args.reportFilename is not None:

			if args.annotReport:
				annotP = jsonpath_ng.ext.parse(args.annotReport)
			else:
				annotP = None

			for rep in reportIter:
				if len(rep["errors"]) > 0:
					exitCode = 2
				elif args.isErrorReport:
					continue

				if annotP is not None:
					for match in annotP.find(rep["json"]):
						rep["annot"] = match.value
						break
				if args.isQuietReport:
					del rep["json"]

				report.append(rep)
		else:
			for rep in reportIter:
				if len(rep["errors"]) > 0:
					exitCode = 2
					break

	if args.reportFilename is not None:
		logging.info("* Storing validation report at {}".format(args.reportFilename))
		with open(args.reportFilename, mode="w", encoding="utf-8") as repH:
			json.dump(report, repH, indent=4, sort_keys=True)

	sys.exit(exitCode)


if __name__ == "__main__":
	main()
