"""
Provides a method to process a dataset into data, pii, and link files.
"""

import csv
import random
import os
from pathlib import Path

from sirad import config


def path(name, outdir):
    outdir = os.path.join(config.get_option("PROCESSED"), outdir)
    Path(outdir).mkdir(parents=True, exist_ok=True)
    return os.path.join(outdir, "{}.txt".format(name))


def Process(dataset):
    # Cache all pii rows, to later shuffle their record numbers
    prows = []

    # Split and write the data file
    data_path = path(dataset.name, "data")
    with open(data_path, "w") as f:
        writer = csv.writer(f, dialect="sirad")
        writer.writerow(dataset.data_header)
        for record_id, (drow, prow) in enumerate(dataset.split(), start=1):
            drow.insert(0, record_id)
            writer.writerow(drow)
            if dataset.has_pii:
                prow.insert(0, record_id)
                prows.append(prow)

    # Shuffle and write the pii and link files
    if dataset.has_pii:
        pii_path  = path(dataset.name, "pii")
        link_path = path(dataset.name, "link")
        with open(pii_path, "w") as f1, open(link_path, "w") as f2:
            pwriter = csv.writer(f1, dialect="sirad")
            pwriter.writerow(dataset.pii_header)
            lwriter = csv.writer(f2, dialect="sirad")
            lwriter.writerow(dataset.link_header)
            random.shuffle(prows)
            for pii_id, row in enumerate(prows, start=1):
                lwriter.writerow((row[0], pii_id))
                row[0] = pii_id
                pwriter.writerow(row)
    else:
        pii_path  = None
        link_path = None

    return data_path, pii_path, link_path

