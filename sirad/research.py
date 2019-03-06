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
            assert "ssn_invalid" in columns
            id_fields += ["ssn", "ssn_invalid"]
        if "first_name" in columns and "last_name" in columns and "dob" in columns:
            has_id = True
            id_fields += ["first_name", "last_name", "dob"]
        if has_id:
            df = pd.read_csv(config.get_path(dataset.name, "pii"),
                             sep="|",
                             usecols=id_fields)
            if len(df) > 0:
                if "first_name" in columns:
                    df["first_sdx"] = np.nan
                    valid_name = df.first_name.notnull()
                    df.loc[valid_name, "first_sdx"] = df.loc[valid_name, "first_name"].apply(soundex)
                df["dsn"] = dataset.name
                datasets.add(dataset.name)
                pii.append(df)

    stats = pd.DataFrame(index=datasets)

    print("Concatenating PII")
    pii = pd.concat(pii, ignore_index=True)
    stats["n_all_pii"] = pii["dsn"].value_counts()

    print("Matching DOB/names to distinct valid SSN")
    valid = ((pii.ssn_invalid == 0) &
             (pii.dob.notnull() & pii.last_name.notnull() & pii.first_sdx.notnull()))
    dob_names = pii.loc[valid]\
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
    pii.loc[merged, "ssn_invalid"] = 0
    stats["n_ssn_fills"] = pii.loc[merged, "dsn"].value_counts()

    print("Creating keys for valid SSNs")
    pii["key"] = np.nan
    valid_ssn = pii.ssn_invalid == 0
    pii.loc[valid_ssn, "key"] = pii.loc[valid_ssn, "ssn"]
    stats["n_ssn_keys"] = pii.loc[valid_ssn, "dsn"].value_counts()

    print("Creating keys for valid DOB/names")
    valid_dobn = (~valid_ssn) & pii.dob.notnull() & pii.last_name.notnull() & pii.first_sdx.notnull()
    pii.loc[valid_dobn, "key"] = pii.loc[valid_dobn].apply(lambda x: "{}_{}_{}".format(x.dob, x.last_name, x.first_sdx), axis=1)
    stats["n_dobn_keys"] = pii.loc[valid_dobn, "dsn"].value_counts()

    print("Generating SIRAD_ID as randomized dense rank over keys")
    key = pii.key[pii.key.notnull()].unique()
    np.random.shuffle(key)
    sirad_id = pd.DataFrame({"key": key, "sirad_id": np.arange(1, len(key)+1)})
    pii = pii.merge(sirad_id, on="key", how="left")
    stats["n_ids"] = pii.loc[pii.sirad_id.notnull(), "dsn"].value_counts()

    pii["sirad_id"] = pii.sirad_id.fillna(0).astype(int)
    pii = pii[["dsn", "pii_id", "sirad_id"]].set_index("dsn")

    stats.to_csv(config.get_path("sirad_id_stats", "research"), float_format="%g")
    print(stats)

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
