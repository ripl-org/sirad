"""
SIRAD: Secure Infrastructure for Research with Administrative Data

https://github.com/ripl-org/sirad

Please cite:
J.S. Hastings, M. Howison, T. Lawless, J. Ucles, P. White. (2019).
Unlocking Data to Improve Public Policy. Communications of the ACM 62(10): 48-53.
doi:10.1145/3335150
"""

import logging
from importlib import resources
from sirad import dialect

__version__ = resources.read_text(__name__, "VERSION").strip()

class Log(object):
    """
    Extends the built-in logging module to support
    """

    def __init__(self, *names):
        self.name = ":".join(names)
        self.log = logging.getLogger(self.name)

    def debug(self, *message, sep=" "):
        self.log.debug(" {}".format(sep.join(map(str, message))))

    def error(self, *message, sep=" "):
        self.log.error(" {}".format(sep.join(map(str, message))))

    def info(self, *message, sep=" "):
        self.log.info(" {}".format(sep.join(map(str, message))))

    def warn(self, *message, sep=" "):
        self.log.warn(" {}".format(sep.join(map(str, message))))

