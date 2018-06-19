"""
Dataset object represents the parsed layout for a dataset and provides
a method to split the raw data into data and pii rows based on the layout.
"""

import io
import os

from sirad import config
from sirad import extract
from sirad import readers
from sirad import validate


class Field(object):
    """
    Object for abstracting a field in a dataset.
    """

    options = frozenset(("data", "format", "hash", "pii", "ssn", "type", "offsets", "skip"))

    def __init__(self, name, options={}, dataset=""):
        # Defaults
        self.name = name
        self.data = False
        self.format = "%Y%m%d"
        self.hash = False
        self.pii = False
        self.ssn = False
        self.type = "varchar"
        self.skip = False
        # Parse options
        for k in options:
            if k not in self.options:
                raise ValueError("unknown '{}' option in field '{}/{}'".format(k, dataset, name))
            setattr(self, k, options[k])
        # Default to data if pii was not specified
        if (self.skip is False) and (not self.pii):
            self.data = True


class Dataset(object):
    """
    Object for abstracting a dataset that is defined by a YAML layout file.
    """

    options = frozenset(("name", "source", "type", "delimiter", "header"))

    def __init__(self, name, layout):
        # Defaults
        self.name = name
        self.type = "csv"
        self.delimiter = ","
        self.header = True
        self.fields = []
        # Test for required options
        if "source" not in layout:
            raise ValueError("no 'source' specified in layout for '{}'".format(name))
        if "fields" not in layout:
            raise ValueError("no 'fields' specified in layout for '{}'".format(name))
        # Parse options
        fields = layout.pop("fields")
        for k in layout:
            if k not in self.options:
                raise ValueError("unknown '{}' option in layout for '{}'".format(k, name))
            setattr(self, k, layout[k])
        for field in fields:
            if isinstance(field, str):
                self.fields.append(Field(field))
            else:
                for name, options in field.items():
                    self.fields.append(Field(name, options, dataset=self.name))
        self.has_pii = sum(1 for f in self.fields if f.pii)
        # Setup column lists
        self.data_cols = [("record_id", "int")] + \
                         [(f.name, f.type) for f in self.fields if f.data] + \
                         [("valid_{}".format(f.name), "int") for f in self.fields if (f.ssn and f.data)]
        self.pii_cols =  [("pii_id", "int")] + \
                         [(f.pii, f.type) for f in self.fields if f.pii] + \
                         [("valid_{}".format(f.pii), "int") for f in self.fields if (f.ssn and f.pii)]
        self.link_cols = [("record_id", "int"), ("pii_id", "int")]
        # Setup headers
        self.data_header = [c[0] for c in self.data_cols]
        self.pii_header  = [c[0] for c in self.pii_cols]
        self.link_header = [c[0] for c in self.link_cols]
        # Setup paths
        self.source = os.path.join(config.get_option("RAW"), self.source)

    def get_reader(self):
        """
        Return either a CSV, fixed-format, or Excel reader depending on the dataset's type.
        """
        if self.type == "xlsx":
            return readers.xlsx_reader(self.source), None
        else:
            f = io.open(self.source)
            if self.type == "fixed":
                column_offsets = [(fld.name, fld.offsets) for fld in self.fields if hasattr(fld, "offsets")]
                reader = readers.fixed_reader(f, column_offsets)
            else:
                reader = readers.csv_reader(f, delimiter=self.delimiter)
            return reader, f

    def split(self):
        """
        Split the raw data. Yields separate data rows and pii rows.
        """
        reader, file_handle = self.get_reader()
        if self.header:
            next(reader)
        for row in reader:
            out_data = []
            out_pii = []
            append_data = []
            append_pii = []
            ssn_fields = []
            dob_obj = None
            for value, field in zip(row, self.fields):
                if field.ssn:
                    ssn_fields.append((value, field))
                if field.pii == "dob":
                    dob_obj = extract.dob(value, field)
                data_value = extract.data(value, field)
                pii_value = extract.pii(value, field)
                if data_value is not None:
                    out_data.append(data_value)
                if pii_value is not None:
                    out_pii.append(pii_value)

            for value, field in ssn_fields:
                ssn_valid = validate.ssn(value, dob_obj)
                if field.data:
                    append_data.append(ssn_valid)
                if field.pii:
                    append_pii.append(ssn_valid)

            yield out_data + append_data, out_pii + append_pii

        if file_handle is not None:
            file_handle.close()

