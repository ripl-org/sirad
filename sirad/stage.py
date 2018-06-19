"""
Method to stage processed files in a relational database.
"""

import csv
import hashlib
import logging
import os

from datetime import datetime
from jellyfish import soundex
from pathlib import Path
from sqlalchemy import create_engine
from sqlalchemy import MetaData
from sqlalchemy import Table
from sqlalchemy import Column
from sqlalchemy import Integer, String, DateTime
from sqlalchemy.exc import OperationalError


from sirad import config
from sirad.process import path as process_path


def str_convertor(x):
    if x == "": return None
    return x


def int_convertor(x):
    if x == "": return None
    return int(x)


def date_convertor(x):
    if x == "": return None
    return datetime.strptime(x, config.DATE_FORMAT)


convertors = {"int": int_convertor, "date": date_convertor}
typemap = {"int": Integer, "date": DateTime}


def hash_key(value):
    value = (str(value) + config.get_option("PII_SALT")).encode("utf-8")
    return hashlib.sha1(value).hexdigest()


class SqliteDB(object):
    """
    Wrapper around a SQLAlchemy engine for sqlite.
    """

    def __init__(self, name):
        Path(config.get_option("STAGED")).mkdir(parents=True, exist_ok=True)
        self.dbname = name
        self.path = os.path.join(config.get_option("STAGED"), "{}.db".format(name))
        self.engine = create_engine('sqlite:///{}'.format(self.path))
        self.metadata = MetaData(self.engine)
        # Add custom functions
        self.cxn = self.engine.raw_connection()
        self.cxn.create_function("SOUNDEX", 1, soundex)
        self.cxn.create_function("HASH_KEY", 1, hash_key)

    def __exit__(self):
        self.cxn.close()

    def load(self, name, columns):
        """
        Create table and load the processed file using bulk inserts.
        """
        table = Table(name, self.metadata)
        # First column is the primary_key
        for n, t in [columns[0]]:
            table.append_column(Column(n, typemap.get(t, String(255)), primary_key=True))
        for n, t in columns[1:]:
            table.append_column(Column(n, typemap.get(t, String(255))))
        # Append an import timestamp
        table.append_column(Column("import_dt",
                                   DateTime,
                                   default=datetime.utcnow))
        # Try to drop the table, in case it already exists
        try:
            table.drop(self.engine)
        except OperationalError:
            pass
        table.create(self.engine)
        convertmap = dict((n, convertors.get(t, str_convertor)) for n, t in columns)
        def convert(rows):
            for row in rows:
                yield dict((n, convertmap[n](row[n])) for n in row)
        path = process_path(name, self.dbname)
        with open(path) as f:
            self.engine.execute(table.insert(),
                                list(convert(csv.DictReader(f, dialect="sirad"))))

    def create_table(self, name, sql):
        self.cxn.execute("DROP TABLE IF EXISTS {}".format(name))
        sql = "CREATE TABLE {} AS\n{}".format(name, sql)
        logging.info(sql)
        self.cxn.execute(sql)

    def create_view(self, name, sql):
        self.cxn.execute("DROP VIEW IF EXISTS {}".format(name))
        sql = "CREATE VIEW {} AS\n{}".format(name, sql)
        logging.info(sql)
        self.cxn.execute(sql)


data = SqliteDB("data")
pii  = SqliteDB("pii")
link = SqliteDB("link")


def Stage(dataset):
    data.load(dataset.name, dataset.data_cols)
    if dataset.has_pii:
        pii.load(dataset.name, dataset.pii_cols)
        link.load(dataset.name, dataset.link_cols)

