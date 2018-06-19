"""
Configuration options
"""
import logging
import os
import sys
import yaml

from sirad.dataset import Dataset

# Available options with defaults set
_options = {
    "DATA_SALT": None,
    "PII_SALT": None,
    "RAW": "raw",
    "PROCESSED": "processed",
    "LAYOUTS": "layouts",
    "STAGED": "staged",
    "RESEARCH": "research_v{}"
}

# TODO determine if UTC will work across Dbs or if this needs to be a configuration.
DATE_FORMAT = "%Y-%m-%d %H:%M:%S"

NULL_VALUES = frozenset(("", "NULL", "null", "NA", "na", "N/A", "#N/A", "NaN", "nan", ".", "#NULL!"))

DATASETS = []


class Config(object):

    loaded = False

    def setup(self):
        """
        Load sirad_config.py from working directory or Python path.
        """
        if self.loaded is True:
            return
        sys.path.insert(0, os.getcwd())
        try:
            import sirad_config as lc
        except ImportError:
            logging.info("No local config found")
            setattr(self, "loaded", True)
            return
        config_key = _options.keys()
        for ck in config_key:
            try:
                set_option(ck, getattr(lc, ck))
            except AttributeError:
                pass
        setattr(self, "loaded", True)



this_config = Config()


def load_config(self):
    """
    Call load  - compatible with older style.
    """
    this_config.setup()


def get_option(key):
    if this_config.loaded is False:
        this_config.setup()
    return _options[key]


def set_option(key, value):
    _options[key] = value


def set_options(options):
    _options.update(options)


def parse_layouts():
    """
    Parse YAML layout files in LAYOUTS directory.
    """
    for root, _, filenames in os.walk(get_option("LAYOUTS")):
        for filename in filenames:
            name = os.path.splitext(filename)[0]
            layout = yaml.load(open(os.path.join(root, filename)))
            DATASETS.append(Dataset(name, layout))
