# Extended JSON Schema validator, Python edition

The JSON schemas should be compliant with JSON Schema Draft04, Draft06 or Draft07 specifications.

So, this validation program uses libraries compliant with that specification.

The installation instructions are in [INSTALL.md](INSTALL.md) .

## Usage

The program can be run using next command line:

```bash
git clone https://github.com/inab/benchmarking-data-model/
python jsonValidate.py benchmarking-data-model/json-schemas benchmarking-data-model/prototype-data/prototype-data/1.0.x/CAMEO
```
[![asciicast](https://asciinema.org/a/279252.svg)](https://asciinema.org/a/279252)

The roots of this program come from [https://github.com/inab/benchmarking-data-model/tree/0.4.0/toolsForValidation](https://github.com/inab/benchmarking-data-model/tree/0.4.0/toolsForValidation)

## History

The roots of this code come from [https://github.com/inab/benchmarking-data-model/tree/0.4.0/toolsForValidation](https://github.com/inab/benchmarking-data-model/tree/0.4.0/toolsForValidation).

Its development was later followed at [https://github.com/fairtracks/fairtracks_validator/], where implementations in other programming languages were also hosted. Python code gained traction and real modularisation there.

As Python code is relevant to several research projects, it has been split from its latest repo, keeping the core and original extensions here at [https://github.com/inab/python-extended-json-schema-validator](https://github.com/inab/python-extended-json-schema-validator), and keeping specific FAIRTracks extensions at [https://github.com/fairtracks/fairtracks_validator_python](https://github.com/fairtracks/fairtracks_validator_python).


