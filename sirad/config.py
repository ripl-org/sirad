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
    "LAYOUTS_DIR": "layouts",
    "RAW_DIR": "raw",
    "DATA_DIR": "data",
    "PII_DIR": "pii",
    "LINK_DIR": "link",
    "RESEARCH_DIR": "research",
    "CENSUS_STREET_FILE": "census/streets.csv",
    "CENSUS_STREET_NUM_FILE": "census/street_nums.csv",
    "VERSION": 1,
    "PROJECT": "",
    "DATA_SALT": None,
    "PII_SALT": None,
}

DATE_FORMAT = "%Y-%m-%d"

NULL_VALUES = frozenset(("", "NULL", "null", "NA", "na", "N/A", "#N/A", "NaN", "nan", ".", "#NULL!"))

LOADED = False
DATASETS = []
FINISHED = []


def get_path(name, subdir):
    path = os.path.join(get_option("{}_DIR".format(subdir.upper())),
                        "{}_V{}".format(get_option("PROJECT"), get_option("VERSION")),
                        "{}.txt".format(name))
    d = os.path.dirname(path)
    if not os.path.exists(d):
        print("Creating output directory:", d)
        os.makedirs(d, exist_ok=True)
    return path


def load_process_log():
    """
    Load an existing sirad.log to determine finished datasets.
    """
    global FINISHED
    global _options
    if "PROCESS_LOG" not in _options or not _options["PROCESS_LOG"]:
        _options["PROCESS_LOG"] = os.path.join(_options["DATA_DIR"],
                                               "{}_V{}".format(_options["PROJECT"], _options["VERSION"]),
                                               "process_log.csv")
    if os.path.exists(_options["PROCESS_LOG"]):
        with open(_options["PROCESS_LOG"]) as f:
            FINISHED = frozenset([row.partition(",")[0] for row in f])
    else:
        d = os.path.dirname(_options["PROCESS_LOG"])
        if not os.path.exists(d):
            print("Creating output directory:", d)
            os.makedirs(d, exist_ok=True)
        with open(_options["PROCESS_LOG"], "w") as f:
            f.write("DATASET,NROWS,ELAPSED\n")


def load_config():
    """
    Load sirad_config.py from working directory or Python path.
    """
    global LOADED
    if LOADED is True:
        return
    sys.path.insert(0, os.getcwd())
    try:
        import sirad_config as lc
    except ImportError:
        logging.info("No local config found")
        LOADED = True
        return
    config_key = _options.keys()
    for ck in config_key:
        try:
            set_option(ck, getattr(lc, ck))
        except AttributeError:
            pass
    load_process_log()
    LOADED = True


def get_option(key):
    if LOADED is False:
        load_config()
    return _options[key]


def set_option(key, value):
    global _options
    _options[key] = value


def set_options(options):
    global _options
    _options.update(options)


def parse_layouts(process_log=False):
    """
    Parse YAML layout files in LAYOUTS directory.
    """
    global DATASETS
    for root, _, filenames in os.walk(get_option("LAYOUTS_DIR")):
        for filename in filenames:
            name = os.path.join(root.partition("/")[2], os.path.splitext(filename)[0])
            if process_log and name in FINISHED:
                logging.info("Found process log for {}".format(name))
            else:
                layout = yaml.safe_load(open(os.path.join(root, filename)))
                DATASETS.append(Dataset(name, layout))
                logging.info("Loaded config for {}".format(name))
    DATASETS = sorted(DATASETS, key=lambda x: x.name)

