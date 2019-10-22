"""
SIRAD: Secure Infrastructure for Research with Administrative Data

https://github.com/ripl-org/sirad

Please cite:
J.S. Hastings, M. Howison, T. Lawless, J. Ucles, P. White. (2019).
Unlocking Data to Improve Public Policy. Communications of the ACM 62(10): 48-53.
doi:10.1145/3335150
"""

from importlib import resources
from sirad import dialect

__version__ = resources.read_text(__name__, "VERSION").strip()
