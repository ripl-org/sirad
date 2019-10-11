# Secure Infrastructure for Research with Administrative Data (SIRAD)

`sirad` is an integration framework for data from administrative systems. It
deidentifies administrative data by removing and replacing personally
identifiable information (PII) with a global anonymized identifier, allowing
researchers to securely join data on an individual from multiple tables without
knowing the individual's identity. It is developed by
[Research Improving People's Lives (RIPL)](https://ripl.org).

For a worked example and further details, please see
[sirad-example](https://github.com/ripl-org/sirad-example).

To learn more about the motivation for creating this package and its potential
uses, please see our article in *Communications of the ACM*:

> J.S. Hastings, M. Howison, T. Lawless, J. Ucles, P. White. (2019).
> Unlocking Data to Improve Public Policy. *Communications of the ACM* **62**(10): 48-53.
> doi:[10.1145/3335150](https://doi.org/10.1145/3335150)

## Installation

Requires Python 3.7 or later.

To install from PyPI using **pip**:  
`pip install sirad`

To install using **Anaconda Python**:  
`conda install -c ripl-org sirad`

To install a **development version** from the current directory:  
`pip install -e .`

## Running
There is a single command line script included, `sirad`.

`sirad` supports the following arguments:
* `process` - split raw data files into data and PII files
* `research` - create a versioned set of research files with a unique
  anonymous identifier

## Configuration

To set configuration options, create a file called `sirad_config.py` and place
either in the directory where you are executing the `sirad` command or
somewhere else on your Python path. See `_options` in `config.py` for a
complete list of possible options and default values.

The following options are available:

* `DATA_SALT`: secret salt used for hashing data values. This shouldn't be
  shared. A warning will be outputted if it is not set. Defaults to None.

* `PII_SALT`: secret salt used for hashing pii values. This shouldn't be
  shared. A warning will be issued if it is not set. Defaults to None.

* `LAYOUTS`: directory that contains layout files. Defaults to `layouts/`.

* `RAW_DIR`, `DATA_DIR`, `PII_DIR`, `LINK_DIR`, `RESEARCH_DIR`: paths to where
   the original data, the processed files, and the research files will be saved.

* `VERSION`: the current version number of the processed and research files.

## Layout files

`sirad` uses YAML files to define the layout, or structure, of raw data files.
These YAML files define each column in the incoming data and how it should be
processed. More documentation to come on this YAML format.

The following file formats are supported:
* csv - change delimiter with delimiter option
* fixed with
* xlsx (xls not currently supported)

## Development

Sample test data is randomly generated using
[Faker](https://github.com/joke2k/faker); none of the information identifies
real individuals.

* tax.txt - sample tax return data. Includes first, last, DOB and SSN.
* credit_scores.txt - sample credit score information. Includes first, last and
  DOB but no SSN.

Run unit tests as:

`python -m unittest discover`

`sirad` can also be used as an API from custom Python code. Documentation to come.

## Authors
* Mark Howison
* Ted Lawless
* John Ucles
* Preston White
