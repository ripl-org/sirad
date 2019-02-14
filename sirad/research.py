"""
Create a research release.
"""

import numpy as np
import os
import pandas as pd
from pathlib import Path

from sirad import config
from sirad.soundex import soundex

def Research():
    """
    """
    datasets = set()
    pii = []
    for dataset in [d for d in config.DATASETS if d.has_pii]:
        print("Loading PII from", dataset.name)
        columns = frozenset(dataset.pii_header)
        id_fields = ["pii_id"]
        has_id = False
        assert "pii_id" in columns
        if "ssn" in columns:
            has_id = True
            assert "valid_ssn" in columns
            id_fields += ["ssn", "valid_ssn"]
        if "first_name" in columns and "last_name" in columns and "dob" in columns:
            has_id = True
            id_fields += ["first_name", "last_name", "dob"]
        if has_id:
            datasets.add(dataset.name)
            df = pd.read_csv(config.get_path(dataset.name, "pii"),
                             sep="|",
                             usecols=id_fields)
            df["first_sdx"] = df["first_name"].apply(soundex)
            df["dsn"] = dataset.name
            pii.append(df)

    print("Concatenating PII")
    pii = pd.concat(pii, ignore_index=True, sort=False)

    print("Matching DOB/names to distinct valid SSN")
    dob_names = pii[pii.valid_ssn == 0]\
                  .groupby(["dob", "last_name", "first_sdx"])\
                  .filter(lambda x: x.ssn.nunique() == 1)\
                  .groupby(["dob", "last_name", "first_sdx"])\
                  .agg({"ssn": "max"}).reset_index()

    print("Filling missing SSNs with DOB/name match")
    pii = pii.merge(dob_names,
                    on=["dob", "last_name", "first_sdx"],
                    how="left",
                    suffixes=("", "_dob_names"))
    merged = pii.ssn_dob_names.notnull()
    pii.loc[merged, "ssn"] = pii.loc[merged, "ssn_dob_names"]
    pii.loc[merged, "valid_ssn"] = 0

    print("Creating keys for valid SSNs")
    pii["key"] = np.nan
    valid_ssn = pii.valid_ssn == 0
    pii.loc[valid_ssn, "key"] = pii.loc[valid_ssn, "ssn"]

    print("Creating keys for valid DOB/names")
    valid_dob = (~valid_ssn) & pii.dob.notnull() & pii.last_name.notnull() & pii.first_sdx.notnull()
    pii.loc[valid_dob, "key"] = pii.loc[valid_dob].apply(lambda x: "{}_{}_{}".format(x.dob, x.last_name, x.first_sdx), axis=1)

    print("Generating SIRAD_ID as randomized dense rank over keys")
    key = pii.key[pii.key.notnull()].unique()
    np.random.shuffle(key)
    sirad_id = pd.DataFrame({"key": key, "sirad_id": np.arange(1, len(key)+1)})
    pii = pii.merge(sirad_id, on="key", how="left")
    pii["sirad_id"] = pii.sirad_id.fillna(0).astype(int)
    pii = pii[["dsn", "pii_id", "sirad_id"]].set_index("dsn")

    for dataset in config.DATASETS:
        data_path = config.get_path(dataset.name, "data")
        res_path = config.get_path(dataset.name, "research")
        if dataset.name in datasets:
            print("Attaching SIRAD_ID to", dataset.name)
            link = pd.read_csv(config.get_path(dataset.name, "link"), sep="|")\
                    .sort_values("record_id")\
                    .merge(pii.loc[dataset.name], on="pii_id", how="left")
            assert link.sirad_id.notnull().all()
            with open(data_path, "r") as f1, open(res_path, "w") as f2:
                f2.write("sirad_id|{}".format(next(f1)))
                for row, ids in zip(f1, link.itertuples()):
                    assert int(row.partition("|")[0]) == ids.record_id
                    f2.write("{}|{}".format(ids.sirad_id, row))
        else:
            print("Hard-linking", dataset.name)
            if os.path.exists(res_path):
                os.unlink(res_path)
            os.link(data_path, res_path)
