#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import re
import sys

import setuptools

# In this way, we are sure we are getting
# the installer's version of the library
# not the system's one
setupBaseDir = os.path.dirname(__file__)
sys.path.insert(0, setupBaseDir)

from extended_json_schema_validator import version as extended_validator_version

# Populating the long description
with open(os.path.join(setupBaseDir, "README.md"), "r", encoding="utf-8") as fh:
	long_description = fh.read()

# Populating the install requirements
with open(os.path.join(setupBaseDir, "requirements.txt"), "r", encoding="utf-8") as f:
	requirements = []
	egg = re.compile(r"#[^#]*egg=([^=&]+)")
	for line in f.read().splitlines():
		m = egg.search(line)
		requirements.append(line if m is None else m.group(1))


setuptools.setup(
	name="extended_json_schema_validator",
	version=extended_validator_version,
	scripts=["jsonValidate.py"],
	author="José Mª Fernández",
	author_email="jose.m.fernandez@bsc.es",
	description="Extended JSON Schema Validator",
	license="LGPLv2",
	long_description=long_description,
	long_description_content_type="text/markdown",
	url="https://github.com/inab/python-extended-json-schema-validator",
	project_urls={
		"Bug Tracker": "https://github.com/inab/python-extended-json-schema-validator/issues"
	},
	packages=setuptools.find_packages(),
	package_data={
		"extended_json_schema_validator": [
			"README-extensions.md",
			"test-data",
			"py.typed",
		]
	},
	install_requires=requirements,
	entry_points={
		"console_scripts": [
			"ext-json-validate=extended_json_schema_validator.__main__:main",
		],
	},
	classifiers=[
		"Programming Language :: Python :: 3",
		"License :: OSI Approved :: GNU Lesser General Public License v2 (LGPLv2)",
		"Operating System :: OS Independent",
	],
	python_requires=">=3.6",
)
