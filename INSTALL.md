# Extended JSON Schema validation install instructions, Python edition

Although this package is already available at PyPI as [extended-json-schema-validator](https://pypi.org/project/extended-json-schema-validator/), you might want to install it by hand.

In order to install the dependencies you need `virtualenv` or `venv`, and `pip`. `pip` is available in many Linux distributions (Ubuntu package `python-pip` and `python3-pip`, CentOS EPEL package `python-pip`), and also as [pip](https://pip.pypa.io/en/stable/) Python package.

- `venv` is part of Python 3.3 and later, but in some Linux distributions is available as a separate package (`python3-venv` in Ubuntu, for instance). Next instructions will work on Ubuntu for Python3:
  ```bash
  sudo apt-get install python3-pip python3-venv
  ```

- The creation and activation of a virtual environment and installation of the dependencies in that environment is done running something like:
  ```bash
  python3 -m venv .py3env
  source .py3env/bin/activate
  ```
  
Within the activated environment, the installation of the dependencies is done with next commands. As the constraints depend on the python version, you have to use the most appropriate one:
  ```bash
  pip install --upgrade pip wheel
  # For instance, python 3.8
  pip install -r requirements.txt -c constraints-3.8.txt
  ```

## Development tips

All the development dependencies are declared at [dev-requirements.txt](dev-requirements.txt) and [mypy-requirements.txt](mypy-requirements.txt).

```bash
pip install -r requirements.txt
pip install -r dev-requirements.txt
pip install -r mypy-requirements.txt
```

One of these dependencies is [pre-commit](https://pre-commit.com/), whose rules are declared at [.pre-commit-config.yaml](.pre-commit-config.yaml) (there are special versions of these rules for GitHub).

The rules run both [pylint](https://pypi.org/project/pylint/),
[mypy](http://mypy-lang.org/) and [tan](https://pypi.org/project/tan/) (a fork of `black` which allows tabs through a flag), among others.

The pre-commit development hook which runs these tools before any commit is installed just running:

```bash
pre-commit install
```

If you want to explicitly run the hooks at any moment, even before doing the commit itself, you only have to run:

```bash
pre-commit run -a
```

As these checks are applied only to the python version currently being used in the development,
there is a GitHub workflow at [.github/workflows/pre-commit.yml](.github/workflows/pre-commit.yml)
which runs them on several Python versions.

If you have lots of cores, fast disks and docker installed, you can locally run the pre-commit GitHub workflow using [act](https://github.com/nektos/act):

```bash
act -j pre-commit
```

# License
* © 2020-2022 Barcelona Supercomputing Center (BSC), ES

Licensed under LGPL-2.1-or-later <https://spdx.org/licenses/LGPL-2.1-or-later.html>, see the file `LICENSE` for details.
