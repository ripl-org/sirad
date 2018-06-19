"""
Create a research release.
"""

import logging
import os

from pathlib import Path
from sqlalchemy import create_engine
from sqlalchemy.engine import reflection

from sirad import config
from sirad import stage


class SqliteDB(object):
    """
    Wrapper around a SQLAlchemy engine for sqlite.
    """

    def __init__(self, version):
        self.version = version
        self.path = config.get_option("RESEARCH").format(version)
        Path(os.path.dirname(self.path)).mkdir(parents=True, exist_ok=True)
        # Overwrite any existing database
        if os.path.exists(self.path):
            os.unlink(self.path)
        self.engine = create_engine('sqlite:///{}'.format(self.path))
        # Attach the staged data, pii and link databsaes
        self.cxn = self.engine.raw_connection()
        self.cxn.execute("ATTACH DATABASE '{}' AS data".format(stage.data.engine.url.database))
        self.cxn.execute("ATTACH DATABASE '{}' AS pii".format(stage.pii.engine.url.database))
        self.cxn.execute("ATTACH DATABASE '{}' AS link".format(stage.link.engine.url.database))

    def __exit__(self):
        self.cxn.close()


def Research(version):
    """
    """
    # Create a view that concatenates all pii
    pii_tables = {}
    pii_info = reflection.Inspector.from_engine(stage.pii.engine)
    for table in pii_info.get_table_names():
        columns = frozenset(c["name"] for c in pii_info.get_columns(table))
        ssn = valid_ssn = first_sdx = last_name = dob = "NULL"
        has_sirad_id = False
        assert "pii_id" in columns
        if "ssn" in columns:
            has_sirad_id = True
            assert "valid_ssn" in columns
            ssn = "ssn"
            valid_ssn = "valid_ssn"
        if "first_name" in columns and "last_name" in columns and "dob" in columns:
            has_sirad_id = True
            first_sdx = "SOUNDEX(first_name)"
            last_name = "last_name"
            dob = "dob"
        if has_sirad_id:
            pii_tables[table] = """
                SELECT '{table}' AS dsn,
                       pii_id,
                       {ssn} AS ssn,
                       {valid_ssn} AS valid_ssn,
                       {dob} AS dob,
                       {last_name} AS last_name,
                       {first_sdx} AS first_sdx
                  FROM {table}
                """.format(**locals())

    unioned = "\nUNION ALL\n".join(pii_tables.values())
    view = """
           SELECT *
             FROM (
                   {}
                  )
         ORDER BY ssn, last_name, dob
           """.format(unioned)
    stage.pii.create_view("sirad_id_pii", view)

    # Add SSNs to rows that don't have it matching on dob, last, first sdx,
    # but only if there is a distinct SSN for the match
    view = """
           SELECT MAX(s.ssn)       AS ssn,
                                      p.dsn,
                                      p.pii_id,
                  MIN(s.valid_ssn) AS valid_ssn
             FROM sirad_id_pii p
             JOIN sirad_id_pii s
               ON p.dob=s.dob             AND
                  p.last_name=s.last_name AND
                  p.first_sdx=s.first_sdx
            WHERE (p.valid_ssn <> 0 OR p.valid_ssn IS NULL) AND s.valid_ssn = 0
         GROUP BY p.dsn, p.pii_id
           HAVING COUNT(DISTINCT s.ssn) = 1
           """
    stage.pii.create_view("sirad_id_ssn_match", view)

    # Create keys for the SSN and name/DOB matches
    view = """
           SELECT p.dsn,
                  p.pii_id,
                  CASE WHEN (s.ssn IS NULL OR s.valid_ssn <> 0) AND (dob IS NULL OR last_name IS NULL OR first_sdx IS NULL) THEN NULL
                       WHEN (s.ssn IS NOT NULL AND s.valid_ssn = 0) THEN HASH_KEY('S_' || s.ssn)
                       ELSE HASH_KEY('NDOB_' || first_sdx || dob || last_name)
                  END AS key
             FROM sirad_id_pii p
        LEFT JOIN sirad_id_ssn_match s
               ON p.dsn=s.dsn AND p.pii_id=s.pii_id
           """
    stage.pii.create_view("sirad_id_keys", view)

    # Create the sirad_id as a dense rank over the keys
    table = """
            SELECT CASE WHEN k.key IS NULL THEN 0
                   ELSE (SELECT CAST(COUNT() + 1 AS INT)
                           FROM (
                                 SELECT DISTINCT key
                                   FROM sirad_id_keys dk
                                  WHERE dk.key < k.key
                                )
                        )
                   END AS sirad_id,
                   p.dsn AS dsn,
                   p.pii_id AS pii_id,
                   p.ssn AS ssn,
                   p.valid_ssn AS valid_ssn,
                   p.dob AS dob,
                   p.last_name AS last_name,
                   p.first_sdx AS first_sdx
              FROM sirad_id_keys k
              JOIN sirad_id_pii p
                ON k.dsn=p.dsn AND k.pii_id=p.pii_id
          ORDER BY key
            """
    stage.pii.create_table("sirad_id", table)

    # Populate research database with all data tables, linking in
    # the sirad_id for tables with staged pii
    research = SqliteDB(version)
    pii_tables = frozenset(pii_tables)
    for table in stage.data.engine.table_names():
        if table in pii_tables:
            sql = """
                  CREATE TABLE {table} AS
                  SELECT sid.sirad_id,
                         d.*
                    FROM pii.sirad_id sid
                    JOIN pii.{table} p
                      ON sid.pii_id=p.pii_id AND sid.dsn="{table}"
                    JOIN link.{table} l
                      ON l.pii_id=p.pii_id
                    JOIN data.{table} d
                      ON d.record_id=l.record_id
                  """.format(table=table)
        else:
            sql = """
                  CREATE TABLE {table} AS
                  SELECT * FROM {table}
                  """.format(table=table)
        logging.info(sql)
        research.cxn.execute(sql)

