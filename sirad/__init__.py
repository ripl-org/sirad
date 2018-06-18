import logging
logging.basicConfig(level=logging.INFO)

from . import config
from . import dialect
from .process import Process
from .stage import Stage
from .research import Research


def process_all():
    config.parse_layouts()
    for dataset in config.DATASETS:
        logging.info("Processing {}.".format(dataset.name))
        Process(dataset)


def stage_all():
    config.parse_layouts()
    for dataset in config.DATASETS:
        logging.info("Staging {}.".format(dataset.name))
        Stage(dataset)


