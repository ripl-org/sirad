# Secure Infrastructure for Research with Administrative Data (SIRAD)

`sirad` is an integration framework for data from administrative systems. It
deidentifies administrative data by removing and replacing personally
identifiable information (PII) with a global anonymized identifier, allowing
researchers to securely join data on an individual from multiple tables without
knowing the individual's identity.

For a worked example and further details, please see
[sirad-example](https://github.com/riipl-org/sirad-example).

## Installation

Requires Python 3.6 or later.

To install from PyPI using **pip**:  
`pip install sirad`

To install using **Anaconda Python**:  
`conda install -c riipl-org sirad`

To install a **development version** from the current directory:  
`pip install -e .`

## Running
There is a single command line script included, `sirad`.

`sirad` supports the following arguments:
* `process` - convert raw data files into deidentified files.
* `stage` - stage the processed data files into a local sqlite database.
* `research` - create a research version of the database, with a unique
  anonymous identifier (sirad_id), with the staged tables.

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

* `RAW`: default directory where raw data files are stored. Defaults to `raw/`.

* `LAYOUTS`: directory that contains layout files. Defaults to `layouts/`.

* `PROCESSED`: path to where processed files will be saved. Defaults to
  `processed/`.

* `RESEARCH`: the template for naming the `research` database. Defaults to
  `research_{}.db`.

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
