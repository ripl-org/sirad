"""
Custom CSV dialect.
"""

import csv

class SiradDialect(csv.Dialect):
    delimiter = "|"
    lineterminator = "\n"
    quotechar = '"'
    doublequote = True
    skipinitialspace = False
    quoting = csv.QUOTE_MINIMAL

csv.register_dialect("sirad", SiradDialect)
