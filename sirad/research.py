"""
Create a research release.
"""

import numpy as np
import os
import pandas as pd
import usaddress

from sirad import config
from sirad.soundex import soundex


def _split_address(x):
    """
    Run usaddress tag method on full street address field to split it into
    street name and number.
    """
    try:
        return usaddress.tag(x)[0]
    except usaddress.RepeatedLabelError:
        return usaddress.tag("")[0]


def Censuscode(dataset, prefix, addresses):
    """
    Determine the census blockgroup for an address based on
    zip code, street name, and street number.
    """

    filename = config.get_path(dataset.name, "pii").rpartition(".")[0] + ".censuscode.txt"
    logname = config.get_path(dataset.name, "research").rpartition(".")[0] + ".censuscode.log"
    N = [len(addresses)]
    geo_level = "BlockGroup"

    with open(logname, "w") as log:

        # Load the lookup files.
        streets = pd.read_csv(street_file)
        print(len(streets), "distinct street names", file=log)
        nums = pd.read_csv(num_file)
        print(len(nums), "distinct street name/numbers", file=log)

        # Build range look-up for street nums.
        num_lookup = {}
        for index, group in nums.groupby(["street", "zip"]):
            group = group.sort_values("street_num")
            num_lookup[index] = (group.street_num.values, group[geo_level].values)
        print(len(num_lookup), "look-ups for street number ranges", file=log)

        # Only retain records with valid integer zip codes.
        addresses["zip"] 
        addresses["zip"] = addresses.zip.astype(int)
        addresses = addresses[addresses.zip.isin(streets.zip.unique())]
        N.append(len(addresses))
        print(N[-1], "records with valid zip codes")

        # Merge on distinct street name.
        addresses["street"] = addresses.street.str.upper().str.extract("([0-9A-Z ]+)", expand=False)
        addresses = addresses[addresses.street.notnull()]
        N.append(len(addresses))
        print("removed", N[-2] - N[-1], "records with missing street name")








def Addresses(dataset):
    """
    Identify and clean address PII fields, perform censuscoding,
    and return the paths to censuscoded output in the PII directory.
    """

    print("Loading address PII from", dataset.name)
    columns = frozenset(dataset.pii_header)
    assert "pii_id" in columns
    output = []

    # Loop over address type.
    for t in ("home", "employer"):

        address_fields = ["pii_id"]

        # Identify which address PII fields are present in the data set.
        contains = dict((name, "{}_{}".format(t, name) in columns)
                         for name in ("zip5", "zip9", "city", "address", "street", "street_num"))

        # Census coding requires a zip code and street name at a minimum
        if (contains["zip5"] or contains["zip9"]) and (contains["address"] or contains["street"]):
            address_fields += sorted(name for name in contains if contains[name])

        # If address PII fields are present, load them from the PII file.
        if len(address_fields) > 1:
            df = pd.read_csv(config.get_path(dataset.name, "pii"),
                             sep="|",
                             usecols=address_fields)
        if len(df) > 0:
            zip = "{}_zip5".format(t)
            street = "{}_street".format(t)
            street_num = "{}_street_num".format(t)
            if contains["zip9"] and not contains["zip5"]:
                df[zip] = df["{}_zip9".format(t)].astype(str).str.slice(5).astype(int)
            if contains["address"]:
                address = pd.DataFrame(df["{}_address".format(t)].str.upper().str.extract("([0-9A-Z ]+)", expand=False).apply(_split_address).tolist())
                df[street] = np.where(address.StreetNamePreDirectional.notnull(), address.StreetNamePreDirectional + " " + address.StreetName, address.StreetName)
                df[street_num] = np.where(address.AddressNumber.str.isdigit(), address.AddressNumber, np.nan)
            if zip in df.columns and street in df.columns and street_num in df.columns:
                output.append(Censuscode(dataset, t, df[["pii_id", zip, street, street_num]]))

    # Merge address types into a single output dataframe.
    if len(output) == 1:
        return output[0]
    elif len(output) > 1:
        return reduce(lambda x, y: x.merge(y, on="pii_id", how="outer"), output)
    else:
        return None


def SiradID():
    """
    Stack PII from all data sets to construct a global anonymous ID
    called the SIRAD ID.
    """

    datasets = set()
    pii = []

    for dataset in [d for d in config.DATASETS if d.has_pii]:

        print("Loading PII from", dataset.name)
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
                             usecols=id_fields)
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

    print("Concatenating PII")
    pii = pd.concat(pii, ignore_index=True, sort=False)
    stats["n_all_pii"] = pii["dsn"].value_counts()

    print("Matching DOB/names to distinct valid SSN")
    valid = ((pii.ssn_invalid == 0) &
             (pii.dob.notnull() & pii.last_name.notnull() & pii.first_sdx.notnull()))
    # Keep first record for distinct name/DOB/SSN,
    # then drop records that have more than one SSN per name/DOB
    dob_names = pii.loc[valid]\
                   .drop_duplicates(["dob", "last_name", "first_sdx", "ssn"])\
                   .drop_duplicates(["dob", "last_name", "first_sdx"], keep=False)

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

    # Save SIRAD ID statistics to a file in the research output directory.
    stats.to_csv(config.get_path("sirad_id_stats", "research"), float_format="%g")
    print(stats)

    return pii


def Research():
    """
    Concurrently generate the SIRAD ID and perform censuscoding using PII,
    then attach the results to the deidentified data files to generate the
    final anonymoized research release.
    """

    # Start SIRAD ID in one thread.
    ids = SiradID()
    id_dsns = frozenset(ids.index)

    # Addresses
    for dataset in [d for d in config.DATASETS if d.has_pii]:

    # Attach SIRAD ID and/or addresses to each data set to produce the
    # final set of research files.
    for dataset in config.DATASETS:

        # Setup paths
        data_path = config.get_path(dataset.name, "data")
        res_path = config.get_path(dataset.name, "research")

        # Identify SIRAD ID and/or address links.
        link = None
        if dataset.name in id_dsns:
            print("Attaching SIRAD_ID to", dataset.name)
            link = pd.read_csv(config.get_path(dataset.name, "link"), sep="|")\
                     .sort_values("record_id")\
                     .merge(ids.loc[[dataset.name]], on="pii_id", how="left")
            assert link.sirad_id.notnull().all()
        if dataset.name in addresses:
            print("Attaching censuscoded addresses to", dataset.name)
            if link is None:
                link = pd.read_csv(config.get_path(dataset.name, "link"), sep="|")\
                         .sort_values("record_id")
            link = link.merge(pd.read_csv(addresses[dataset.name], sep="|"),
                              on="pii_id", how="left")

        # Write out a new research file with attached data if available,
        # otherwise use the data file as-is via a hard link.
        if link:
            with open(data_path, "r") as f1, open(res_path, "w") as f2:
                f2.write("|".join(link.columns[2:])
                f2.write("|")
                f2.write(next(f1))
                for link_row, data_row in zip(link.itertuples(), f1):
                    assert int(data_row.partition("|")[0]) == link_row.record_id
                    f2.write("|".join(link_row[2:]))
                    f2.write("|")
                    f2.write(data_row)
        else:
            print("Hard-linking", dataset.name)
            if os.path.exists(res_path):
                os.unlink(res_path)
            os.link(data_path, res_path)


