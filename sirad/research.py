"""
Create a research release.
"""

import numpy as np
import os
import pandas as pd
import usaddress

from sirad import config, Log
from sirad.soundex import soundex
from multiprocessing import Process, Queue

_address_prefixes = ("home", "employer", "mailing", "employer1", "employer2", "employer3")


def _split_address(x):
    """
    Run usaddress tag method on full street address field to split it into
    street name and number.
    """
    try:
        return usaddress.tag(x)[0]
    except usaddress.RepeatedLabelError:
        return usaddress.tag("")[0]


def _str_format(x):
    """
    Reformat NaN and floating point numbers for CSV output.
    """
    if isinstance(x, float): return "{:.0f}".format(x)
    return str(x)


def Censuscode(dataset, prefix, addresses):
    """
    Determine the census blockgroup for an address based on
    zip code, street name, and street number.
    """
    info = Log(__name__, "Censuscoding", prefix, dataset.name).info
    filename = "{}.censuscode.{}.csv".format(config.get_path(dataset.name, "pii").rpartition(".")[0], prefix)
    logname = "{}.censuscode.{}.log".format(config.get_path(dataset.name, "research").rpartition(".")[0], prefix)

    zip = "{}_zip5".format(prefix)
    city = "{}_city".format(prefix)
    street = "{}_street".format(prefix)
    street_num = "{}_street_num".format(prefix)

    # Clean city
    addresses[city] = addresses[city].str.upper().str.extract("([A-Z ]+)", expand=False)

    N = [len(addresses)]
    geo_level = ("blkgrp", "{}_blkgrp".format(prefix))

    with open(logname, "w") as log:

        info("Loading lookup files")
        streets = pd.read_csv(config.get_option("CENSUS_STREET_FILE"), low_memory=False)\
                    .rename(columns={geo_level[0]: geo_level[1]})\
                    .drop_duplicates(["street", "zip"])
        print(len(streets), "distinct street names", file=log)
        nums = pd.read_csv(config.get_option("CENSUS_STREET_NUM_FILE"), low_memory=False)\
                 .rename(columns={geo_level[0]: geo_level[1]})\
                 .drop_duplicates(["street_num", "street", "zip"])
        print(len(nums), "distinct street name/numbers", file=log)

        info("Building range look-up for street nums")
        num_lookup = {}
        for index, group in nums.groupby(["street", "zip"]):
            group = group.sort_values("street_num")
            num_lookup[index] = (group["street_num"].values, group[geo_level[1]].values)
        print(len(num_lookup), "look-ups for street number ranges", file=log)

        info("Filtering records with non-missing zip codes")
        addresses = addresses[addresses[zip].notnull()]
        N.append(len(addresses))
        print(N[-1], "records with non-missing zip codes", file=log)

        info("Filtering records with valid integer zip codes")
        if addresses[zip].dtype == "O":
            addresses[zip] = addresses[zip].str.extract("(\d+)", expand=False)
            addresses = addresses[addresses[zip].notnull()]
        addresses[zip] = addresses[zip].astype(int)
        addresses = addresses[addresses[zip].isin(streets.zip.unique())]
        N.append(len(addresses))
        print(N[-1], "records with valid integer zip codes", file=log)

        info("Filtering records with valid street names")
        addresses[street] = addresses[street].str.upper().str.extract("([0-9A-Z ]+)", expand=False)
        addresses = addresses[addresses[street].notnull()]
        N.append(len(addresses))
        print(N[-1], "records with valid street names", file=log)

        info("Merge 1 on distinct street name")
        addresses = addresses.merge(streets,
                                    how="left",
                                    left_on=[street, zip],
                                    right_on=["street", "zip"],
                                    validate="many_to_one")
        assert len(addresses) == N[-1]
        merged = addresses[geo_level[1]].notnull()
        addresses.loc[merged, ["pii_id", city, zip, geo_level[1]]].to_csv(filename, float_format="%.0f", index=False)
        print("merged", merged.sum(), "records on distinct street name", file=log)

        # Remove merged addresses.
        addresses = addresses[~merged]
        del addresses[geo_level[1]]
        N.append(len(addresses))
        print(N[-1], "records remaining", file=log)

        # Keep records with valid integer street nums.
        if addresses[street_num].dtype == "O":
            addresses[street_num] = addresses[street_num].str.extract("(\d+)", expand=False)
        addresses = addresses[addresses[street_num].notnull()]
        addresses[street_num] = addresses[street_num].astype(int)
        N.append(len(addresses))
        print(N[-1], "records with valid integer street nums", file=log)

        info("Merge 2 on distinct street name/num")
        addresses = addresses.merge(nums,
                                    how="left",
                                    left_on=[street_num, street, zip],
                                    right_on=["street_num", "street", "zip"],
                                    validate="many_to_one")
        assert len(addresses) == N[-1]
        merged = addresses[geo_level[1]].notnull()
        addresses.loc[merged, ["pii_id", city, zip, geo_level[1]]].to_csv(filename, float_format="%.0f", index=False, mode="a", header=False)
        print("merged", merged.sum(), "records on distinct street name/num", file=log)

        # Remove merged addresses.
        addresses = addresses[~merged]
        del addresses[geo_level[1]]
        N.append(len(addresses))
        print(N[-1], "records remaining", file=log)

        info("Merge 3 with street number range search")
        merged = []
        for _, row in addresses.iterrows():
            l = num_lookup.get((row[street], row[zip]))
            if l is not None:
                i = np.searchsorted(l[0], row[street_num], side="right")
                merged.append((row["pii_id"], row[city], row[zip], l[1][max(0, i-1)]))
        print("merged", len(merged), "records on nearest street name/num", file=log)
        with open(filename, "a") as f:
            for row in merged:
                print(*row, sep=",", file=f)
        N.append(N[-1] - len(merged))
        print(N[-1], "records remain unmerged", file=log)
        print("overall match rate: {:.1f}%".format(100.0 * (N[0] - (N[0] - N[2]) - N[-1]) / N[0]), file=log)

    info("Done")


def Addresses(dataset):
    """
    Identify and clean address PII fields and perform censuscoding
    if sufficient address components are available.
    """
    columns = frozenset(dataset.pii_header)
    assert "pii_id" in columns
    output = []

    # Loop over address type.
    for prefix in _address_prefixes:

        info = Log(__name__, "Addresses", prefix, dataset.name).info

        address_fields = ["pii_id"]

        # Identify which address PII fields are present in the data set.
        contains = dict((name, "{}_{}".format(prefix, name) in columns)
                         for name in ("zip5", "zip9", "city", "address", "street", "street_num"))

        # Census coding requires a zip code and street name at a minimum
        if (contains["zip5"] or contains["zip9"]) and (contains["address"] or contains["street"]):
            address_fields += sorted("{}_{}".format(prefix, name) for name in contains if contains[name])

        # If address PII fields are present, load them from the PII file.
        if len(address_fields) > 1:
            df = pd.read_csv(config.get_path(dataset.name, "pii"),
                             sep="|",
                             usecols=address_fields,
                             low_memory=False)

            if len(df) > 0:
                zip = "{}_zip5".format(prefix)
                city = "{}_city".format(prefix)
                street = "{}_street".format(prefix)
                street_num = "{}_street_num".format(prefix)

                if contains["zip9"] and not contains["zip5"]:
                    df[zip] = df["{}_zip9".format(prefix)].astype(str).str.slice(0, 5)

                if contains["address"]:
                    address = pd.DataFrame(df["{}_address".format(prefix)].str.upper().str.extract("([0-9A-Z ]+)", expand=False).fillna("").apply(_split_address).tolist())
                    if "StreetNamePreDirectional" in address.columns:
                        df[street] = np.where(address.StreetNamePreDirectional.notnull(), address.StreetNamePreDirectional + " " + address.StreetName, address.StreetName)
                    else:
                        df[street] = address.StreetName
                    df[street_num] = np.where(address.AddressNumber.str.isdigit(), address.AddressNumber, np.nan)

                if zip in df.columns and street in df.columns and street_num in df.columns:
                    if not contains["city"]:
                        df[city] = ""
                    Censuscode(dataset, prefix, df[["pii_id", zip, city, street, street_num]])

                else:
                    info("Unable to restructure address PII columns (zip: {}, street: {}, street_num: {})".format(
                        zip in df.columns,
                        street in df.columns,
                        street_num in df.columns))
            else:
                info("No PII records")
        else:
            if sum(contains.values()) == 0:
                info("No address PII columns")
            else:
                info("Not enough address PII columns ({})".format(str(contains)))


def SiradID():
    """
    Stack PII from all data sets to construct a global anonymous ID
    called the SIRAD ID.
    """
    info = Log(__name__, "SiradID").info
    datasets = set()
    pii = []

    for dataset in [d for d in config.DATASETS if d.has_pii]:


        info("Loading PII for", dataset.name)
        columns = frozenset(dataset.pii_header)
        id_fields = ["pii_id"]
        assert "pii_id" in columns

        # Identify which PII columns are present.
        if "ssn" in columns:
            assert "ssn_invalid" in columns
            id_fields += ["ssn", "ssn_invalid"]
        if "first_name" in columns and "last_name" in columns and "dob" in columns:
            id_fields += ["first_name", "last_name", "dob"]

        # Either the SSN or name/DOB fields must be available to construct
        # a SIRAD ID for the dataset.
        if len(id_fields) > 1:
            df = pd.read_csv(config.get_path(dataset.name, "pii"),
                             sep="|",
                             usecols=id_fields,
                             low_memory=False)
            if len(df) > 0:
                if "first_name" in id_fields:
                    # Convert first name to Soundex value.
                    df["first_sdx"] = np.nan
                    valid_name = df.first_name.notnull()
                    df.loc[valid_name, "first_sdx"] = df.loc[valid_name, "first_name"].apply(soundex)
                df["dsn"] = dataset.name
                datasets.add(dataset.name)
                pii.append(df)

    # Keep track of statistics while constructing the SIRAD ID.
    stats = pd.DataFrame(index=datasets)

    info("Concatenating PII")
    pii = pd.concat(pii, ignore_index=True, sort=False)
    stats["n_all_pii"] = pii["dsn"].value_counts()

    info("Matching DOB/names to distinct valid SSN")
    valid = ((pii.ssn_invalid == 0) &
             (pii.dob.notnull() & pii.last_name.notnull() & pii.first_sdx.notnull()))
    # Keep first record for distinct name/DOB/SSN,
    # then drop records that have more than one SSN per name/DOB
    dob_names = pii.loc[valid]\
                   .drop_duplicates(["dob", "last_name", "first_sdx", "ssn"])\
                   .drop_duplicates(["dob", "last_name", "first_sdx"], keep=False)

    info("Filling missing SSNs with DOB/name match")
    pii = pii.merge(dob_names,
                    on=["dob", "last_name", "first_sdx"],
                    how="left",
                    suffixes=("", "_dob_names"))
    merged = pii.ssn_dob_names.notnull()
    pii.loc[merged, "ssn"] = pii.loc[merged, "ssn_dob_names"]
    pii.loc[merged, "ssn_invalid"] = 0
    stats["n_ssn_fills"] = pii.loc[merged, "dsn"].value_counts()

    info("Creating keys for valid SSNs")
    pii["key"] = np.nan
    valid_ssn = pii.ssn_invalid == 0
    pii.loc[valid_ssn, "key"] = pii.loc[valid_ssn, "ssn"]
    stats["n_ssn_keys"] = pii.loc[valid_ssn, "dsn"].value_counts()

    info("Creating keys for valid DOB/names")
    valid_dobn = (~valid_ssn) & pii.dob.notnull() & pii.last_name.notnull() & pii.first_sdx.notnull()
    pii.loc[valid_dobn, "key"] = pii.loc[valid_dobn].apply(lambda x: "{}_{}_{}".format(x.dob, x.last_name, x.first_sdx), axis=1)
    stats["n_dobn_keys"] = pii.loc[valid_dobn, "dsn"].value_counts()

    info("Generating SIRAD_ID as randomized dense rank over keys")
    key = pii.key[pii.key.notnull()].unique()
    np.random.shuffle(key)
    sirad_id = pd.DataFrame({"key": key, "sirad_id": np.arange(1, len(key)+1)})
    pii = pii.merge(sirad_id, on="key", how="left")
    stats["n_ids"] = pii.loc[pii.sirad_id.notnull(), "dsn"].value_counts()

    pii["sirad_id"] = pii.sirad_id.fillna(0).astype(int)
    pii = pii[["dsn", "pii_id", "sirad_id"]].set_index("dsn")

    # Save SIRAD ID statistics to a file in the research output directory.
    stats.to_csv(config.get_path("sirad_id_stats", "research"), float_format="%g")

    info("Done")
    return pii


def ResearchWorker(tasks, results):
    """
    Helper function for concurrently running Addresses and SiradID.
    """
    while not tasks.empty():
        task = tasks.get()
        if task[0] == "SiradID":
            ids = SiradID()
            results.put(ids)
        elif task[0] == "Addresses":
            Addresses(task[1])


def Research(nthreads=1, seed=0):
    """
    Concurrently generate the SIRAD ID and perform censuscoding using PII,
    then attach the results to the deidentified data files to generate the
    final anonymoized research release.
    """
    info = Log(__name__, "Research").info

    if seed:
        np.random.seed(seed)

    # Concurrently run Addresses and SiradID if multiple threads are available.
    if nthreads > 1:
        # Define tasks
        tasks = Queue()
        for dataset in config.DATASETS:
           if dataset.has_pii:
               tasks.put(("Addresses", dataset))
        tasks.put(("SiradID",))
        # Run tasks
        results = Queue()
        pool = []
        for n in range(nthreads):
            p = Process(target=ResearchWorker, args=(tasks, results))
            pool.append(p)
            p.start()
        for p in pool:
            p.join()
        ids = results.get() # Only the SiradID process returns a result
    else:
        for dataset in config.DATASETS:
           if dataset.has_pii:
               Addresses(dataset)
        ids = SiradID()

    # Attach SIRAD ID and/or addresses to each data set to produce the
    # final set of research files.

    id_dsns = frozenset(ids.index)

    for dataset in config.DATASETS:

        # Setup paths
        data_path = config.get_path(dataset.name, "data")
        res_path = config.get_path(dataset.name, "research")

        # Identify SIRAD ID and/or address links.
        link = None
        if dataset.name in id_dsns:
            info("Attaching SIRAD_ID to", dataset.name)
            link = pd.read_csv(config.get_path(dataset.name, "link"), sep="|", low_memory=False)\
                     .sort_values("record_id")\
                     .merge(ids.loc[[dataset.name]], on="pii_id", how="left")
            assert link.sirad_id.notnull().all()
        for prefix in _address_prefixes:
            filename = "{}.censuscode.{}.csv".format(config.get_path(dataset.name, "pii").rpartition(".")[0], prefix)
            if os.path.exists(filename):
                info("Attaching censuscoded", prefix, "addresses to", dataset.name)
                if link is None:
                    link = pd.read_csv(config.get_path(dataset.name, "link"), sep="|", low_memory=False)\
                             .sort_values("record_id")
                link = link.merge(pd.read_csv(filename, low_memory=False), on="pii_id", how="left")

        # Write out a new research file with attached data if available,
        # otherwise use the data file as-is via a hard link.
        if link is not None:
            link = link.fillna("")
            with open(data_path, "r") as f1, open(res_path, "w") as f2:
                f2.write("|".join(link.columns[2:]))
                f2.write("|")
                f2.write(next(f1))
                for link_row, data_row in zip(link.itertuples(), f1):
                    assert int(data_row.partition("|")[0]) == link_row.record_id
                    f2.write("|".join(map(_str_format, link_row[3:])))
                    f2.write("|")
                    f2.write(data_row)
        else:
            info("Hard-linking", dataset.name)
            if os.path.exists(res_path):
                os.unlink(res_path)
            os.link(data_path, res_path)

    info("Done")

