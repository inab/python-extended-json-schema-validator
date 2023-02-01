# Extended JSON Schema validator, Python edition

This library and program validates both JSON Schema and JSON-like contents.
The contents can be physically represented either as JSON or as YAML files.

The JSON schemas should be compliant with JSON Schema versions supported
by the installed [`jsonschema`](https://python-jsonschema.readthedocs.io/en/stable/) library.
As of version 4.16 they are Draft04, Draft06, Draft07, Draft2019-09 and Draft2020-12 specifications.

The installation instructions are in [INSTALL.md](INSTALL.md) .

## History

The roots of this code come from [https://github.com/inab/benchmarking-data-model/tree/0.4.0/toolsForValidation](https://github.com/inab/benchmarking-data-model/tree/0.4.0/toolsForValidation).

Its development was later followed at [https://github.com/fairtracks/fairtracks_validator/], where implementations in other programming languages were also hosted. Python code gained traction and real modularisation there.

As this Python code is relevant to several research projects, it has been split from its latest repo, keeping the core and original extensions here at [https://github.com/inab/python-extended-json-schema-validator](https://github.com/inab/python-extended-json-schema-validator), and keeping specific FAIRTracks extensions at [https://github.com/fairtracks/fairtracks_validator_python](https://github.com/fairtracks/fairtracks_validator_python).

## Extensions

A description of the base JSON Schema extensions implemented in this repository is available at [README-extensions.md](README-extensions.md).

## Usage

```bash
python jsonValidate.py --help
# or if the package was installed through pypi
ext-json-validate -h
```
```
usage: jsonValidate.py [-h] [--log-file LOGFILENAME] [--log-format LOGFORMAT] [-q] [-v] [-d] [-C CONFIGFILENAME]
                       [--cache-dir CACHEDIR] [-c] [--schema_id_path SCHEMA_ID_PATH]
                       [--guess-schema | --use-schemas USE_SCHEMAS [USE_SCHEMAS ...]] [--fix] [--report REPORTFILENAME]
                       [--annotation ANNOTREPORT] [--verbose-report] [--error-report] [--dot-report FILENAME TITLE]
                       [--invalidate | --read-only] [--warm-up | --lazy-load] [-V]
                       json_schema_or_dir [json_file_or_dir [json_file_or_dir ...]]

Validate JSON against JSON Schemas with extensions (version 0.12.1)

positional arguments:
  json_schema_or_dir    The JSON Schema, either in JSON or YAML file format, or directory with them to validate and use
  json_file_or_dir      The JSONs, either in JSON or YAML file formats, or directories with them to be validated (default: None)

optional arguments:
  -h, --help            show this help message and exit
  --log-file LOGFILENAME
                        Store messages in a file instead of using standard error and standard output (default: None)
  --log-format LOGFORMAT
                        Format of log messages (default: %(asctime)-15s - [%(levelname)s] %(message)s)
  -q, --quiet           Only show engine warnings and errors (default: None)
  -v, --verbose         Show verbose (informational) messages (default: None)
  -d, --debug           Show debug messages (use with care, as it could potentially disclose sensitive contents) (default: None)
  -C CONFIGFILENAME, --config CONFIGFILENAME
                        Configuration file (used by extensions) (default: None)
  --cache-dir CACHEDIR  Caching directory (used by extensions) (default: None)
  -c, --continue        Show all the error messages instead of stopping on the first one (default when a report file is
                        requested) (default: False)
  --schema_id_path SCHEMA_ID_PATH
                        When the content read from the file is going to be validated, JSON Path (accepted by jsonpath-ng) used
                        to get the schema id which identifies the schema to be used by the validator (default:
                        "@schema"|"_schema"|"$schema")
  --guess-schema        Try to validate the input JSONs against all the loaded schemas (default: False)
  --use-schemas USE_SCHEMAS [USE_SCHEMAS ...]
                        Try to validate the input JSONs against any of the loaded schemas matching these URIs (default: None)
  --fix                 When some validation error arises, an editor instance (from $EDITOR environment variable) is launched
                        giving the chance to fix the files, and then it is validated again. The cycle is repeated until all the
                        files are correct or the program is interrupted (default: False)
  --report REPORTFILENAME
                        Store validation report (in JSON format) in a file (default: None)
  --annotation ANNOTREPORT
                        JSON Path (accepted by jsonpath-ng) to extract an annotation to include from validated JSON in the
                        report (for instance, '$._id') (default: None)
  --verbose-report      When this flag is enabled, the report also embeds the json contents which were validated (default: True)
  --error-report        When this flag is enabled, the report only includes the entries with errors (default: False)
  --dot-report FILENAME TITLE
                        Depict the schemas in a file using DOT format, providing the title given in the second param (default:
                        None)
  --invalidate          Caches managed by the extensions are invalidated on startup (default: False)
  --read-only           When this flag is enabled, the caches managed by the extensions are read-only, avoiding expensive
                        operations related to the caches (default: True)
  --warm-up             Caches managed by the extensions are warmed up on startup (default: None)
  --lazy-load           Caches managed by the extensions are warmed up in a lazy way (default: True)
  -V, --version         show program's version number and exit
```

Next lines run validations using test data:

```bash
python jsonValidate.py test-data/foreign_key_example/schemas/ test-data/foreign_key_example/good_validation/
python jsonValidate.py test-data/foreign_key_example/schemas/ test-data/foreign_key_example/bad_validation/
```

If your JSON schemas are properly defined, but you are fixing issues in a set of JSON files, you can run it in an iterative way:

```bash
git clone https://github.com/inab/benchmarking-data-model
EDITOR="geany -i" python jsonValidate.py --fix benchmarking-data-model/json-schemas/1.0.x benchmarking-data-model/prototype-data/1.0.x/QfO-fail

And this is an asciinema (to be updated) recording a previous version of FAIRTracks Validator:

[![asciicast](https://asciinema.org/a/279252.svg)](https://asciinema.org/a/279252)


