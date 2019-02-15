"""
Configuration options
"""
import logging
import os
import sys
import yaml
from pathlib import Path

from sirad.dataset import Dataset

# Available options with defaults set
_options = {
    "LAYOUTS_DIR": "layouts",
    "RAW_DIR": "raw",
    "DATA_DIR": "data",
    "PII_DIR": "pii",
    "LINK_DIR": "link",
    "RESEARCH_DIR": "research",
    "VERSION": 1,
    "PROJECT": "",
    "DATA_SALT": None,
    "PII_SALT": None,
}

DATE_FORMAT = "%Y-%m-%d"

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


def get_path(name, subdir):
    path = os.path.join(get_option("{}_DIR".format(subdir.upper())),
                        "{}_V{}".format(get_option("PROJECT"), get_option("VERSION")))
    Path(path).mkdir(parents=True, exist_ok=True)
    return os.path.join(path, "{}.txt".format(name))


def set_option(key, value):
    _options[key] = value


def set_options(options):
    _options.update(options)


def parse_layouts():
    """
    Parse YAML layout files in LAYOUTS directory.
    """
    for root, _, filenames in os.walk(get_option("LAYOUTS_DIR")):
        for filename in filenames:
            logging.info("Loading config {}".format(filename))
            name = os.path.splitext(filename)[0]
            layout = yaml.load(open(os.path.join(root, filename)))
            DATASETS.append(Dataset(name, layout))
