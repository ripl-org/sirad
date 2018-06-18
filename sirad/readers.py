"""
Readers for delimited text and fixed width files.

Code borrowed/inspired from: https://github.com/wireservice/agate
"""
from collections import namedtuple
import csv

from openpyxl import load_workbook


class CsvReader(object):

    def __init__(self, f, **kwargs):
        self.reader = csv.reader(f, **kwargs)

    def __iter__(self):
        return self

    def __next__(self):
        try:
            row = next(self.reader)
        except csv.Error as e:
            raise e

        return row

    @property
    def dialect(self):
        return self.reader.dialect


def csv_reader(*args, **kwargs):
    """
    """
    return CsvReader(*args, **kwargs)


FixedField = namedtuple('Field', ['name', 'start', 'length'])


class FixedReader(object):
    """
    """
    def __init__(self, f, field_offsets):
        self.file = f
        self.fields = []

        start = 0
        for name, offset in field_offsets:
            end = start + offset
            self.fields.append(
                FixedField(name, int(start), end)
            )
            start = end


    def __iter__(self):
        return self

    def __next__(self):
        line = next(self.file)

        values = []

        for field in self.fields:
            values.append(line[field.start:field.start + field.length].strip())

        return values


def fixed_reader(*args, **kwargs):
    """
    """
    return FixedReader(*args, **kwargs)


def xlsx_reader(filename, **kwargs):
    wb = load_workbook(filename=filename, read_only=True, keep_links=False)
    return iter([[c.value for c in r] for r in wb.active.rows])
