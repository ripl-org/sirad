"""
Provides a function to validate a dataset config file for syntax errors
and against the column names in the raw file.
"""

import logging

from sirad import config
from openpyxl import load_workbook

def Validate(dataset):

    logging.info("Validating {}".format(dataset.name))
    nwarnings = 0
    firstline = None

    # Read first line of input file
    if dataset.type == "csv":
        with open(dataset.source, "r", encoding=dataset.encoding, newline="") as f:
            firstline = [c.strip().strip('"').upper() for c in next(f).split(dataset.delimiter)]
    elif dataset.type == "xlsx":
        wb = load_workbook(filename=dataset.source, read_only=True, keep_links=False)
        firstline = [c.value.strip().upper() for c in next(wb.active.rows)]
        wb.close()
    if firstline is not None:
        if dataset.header:
            fields = frozenset(firstline)
            for f in dataset.fields:
                if f.name.upper() not in fields:
                    logging.warn("Dataset {} has no column named {}".format(dataset.name, f.name.upper()))
                    nwarnings += 1
        else:
            if len(firstline) != len(dataset.fields):
                logging.warn("Dataset {} has {} columns in layout and {} columns in the raw file".format(dataset.name, len(dataset.fields), len(firstline)))
                nwarnings += 1
    if nwarnings:
        logging.info("Header for {}: {}".format(dataset.name, str(firstline)))

    return nwarnings
