import unittest
import csv
import os
import shutil

import yaml

from sirad import process
from sirad import config
from sirad.dataset import Dataset
from sirad.readers import fixed_reader, xlsx_reader

project_dir = os.path.dirname(os.path.abspath(__file__))

def get_file_path(dir, name):
    return os.path.join(project_dir, "data", dir, name)


class ThisTester(unittest.TestCase):

    def setUp(self):
        self.output_dir = os.path.join(project_dir, "processed")
        config.set_option("DATA_DIR", os.path.join(self.output_dir, "data"))
        config.set_option("PII_DIR", os.path.join(self.output_dir, "pii"))
        config.set_option("LINK_DIR", os.path.join(self.output_dir, "link"))
        config.set_option("DATA_SALT", "testcode")
        config.set_option("PII_SALT", "testcode")
        config.set_option("RAW_DIR", os.path.join(project_dir, "data", "raw"))
        config.set_option("PROJECT", "Test")
        config.set_option("PROCESS_LOG", os.path.join(self.output_dir, "data", "sirad.log"))
        self.clean_up = True

    def processed_reader(self, path):
        with open(path) as f:
            d = {}
            for n, row in enumerate(csv.DictReader(f, delimiter="|")):
                d[n] = row
        return d

    def processed_xlsx_reader(self, path, header):
        d = {}
        for n, row in enumerate(xlsx_reader(path, header)):
            d[n] = row
        return d

    def load_layout(self, name):
        path = get_file_path("layouts", name)
        with open(path) as f:
            return yaml.safe_load(f)

    def tearDown(self):
        if self.clean_up is True:
            shutil.rmtree(self.output_dir)


class TestSplitTax(ThisTester):
    def setUp(self):
        super(TestSplitTax, self).setUp()
        self.data_file = get_file_path("raw", "tax.txt")
        self.layout_file = self.load_layout("tax.yaml")

    def test_process(self):
        dataset = Dataset("tax", self.layout_file)
        df, pf, lf = process.Process(dataset)
        self.assertIn("data", df)

        # Join data and pii using link and check that values from raw file
        # are still properly linked.
        d_rows = self.processed_reader(df)
        l_rows = self.processed_reader(lf)
        p_rows = self.processed_reader(pf)
        raw = self.processed_reader(self.data_file)
        raw_values = [(r['last'], r['first']) for r in raw.values()]

        self.assertIn("record_id", d_rows[0].keys())
        self.assertIn("pii_id", p_rows[0].keys())
        self.assertIn("ssn", p_rows[0].keys())

        for x in range(1, len(d_rows) + 1):
            lids = [d for d in l_rows.values() if d['record_id'] == str(x)][0]
            pv = [d for d in p_rows.values() if d['pii_id'] == lids['pii_id']][0]
            dv = [d for d in d_rows.values() if d['record_id'] == lids['record_id']][0]

            self.assertTrue("job" in dv.keys())
            self.assertFalse("job" in pv.keys())
            self.assertIsNone(dv.get("ssn"))
            self.assertIsNotNone(pv.get("ssn"))

            to_check = (pv['last_name'], pv['first_name'])
            self.assertIn(to_check, raw_values)


class TestSplitCredit(ThisTester):
    def setUp(self):
        self.data_file = get_file_path("raw", "credit_scores.txt")
        self.layout_file = self.load_layout("credit_score.yaml")
        super(TestSplitCredit, self).setUp()

    def test_process(self):
        dataset = Dataset("credit", self.layout_file)
        df, pf, lf = process.Process(dataset)
        self.assertIn("data", df)

        # Join data and pii using link and check that values from raw file
        # are still properly linked.
        d_rows = self.processed_reader(df)
        l_rows = self.processed_reader(lf)
        p_rows = self.processed_reader(pf)
        raw = self.processed_reader(self.data_file)
        raw_values = [(r['last'], r['first'], r['credit_score']) for r in raw.values()]

        # Check expected number of columns
        self.assertEqual(len(d_rows[0].keys()), 2)

        self.assertIn("record_id", d_rows[0].keys())
        self.assertIn("pii_id", p_rows[0].keys())
        self.assertIn("dob", p_rows[0].keys())

        for x in range(1, len(d_rows) + 1):
            lids = [d for d in l_rows.values() if d['record_id'] == str(x)][0]
            pv = [d for d in p_rows.values() if d['pii_id'] == lids['pii_id']][0]
            dv = [d for d in d_rows.values() if d['record_id'] == lids['record_id']][0]

            to_check = (pv['last_name'], pv['first_name'], dv['credit_score'])
            self.assertIn(to_check, raw_values)


class TestSplitFixedTax(ThisTester):
    def setUp(self):
        self.data_file = get_file_path("raw", "tax_fixed.txt")
        self.layout_file = self.load_layout("tax_fixed.yaml")
        super(TestSplitFixedTax, self).setUp()

    def test_process(self):
        dataset = Dataset("tax", self.layout_file)
        df, pf, lf = process.Process(dataset)
        self.assertIn("data", df)

        # Join data and pii using link and check that values from raw file
        # are still properly linked.
        d_rows = self.processed_reader(df)
        l_rows = self.processed_reader(lf)
        p_rows = self.processed_reader(pf)

        self.assertIn("record_id", d_rows[0].keys())
        self.assertIn("pii_id", p_rows[0].keys())
        self.assertIn("ssn", p_rows[0].keys())

        for x in range(1, len(d_rows) + 1):
            lids = [d for d in l_rows.values() if d['record_id'] == str(x)][0]
            pv = [d for d in p_rows.values() if d['pii_id'] == lids['pii_id']][0]
            dv = [d for d in d_rows.values() if d['record_id'] == lids['record_id']][0]


class TestSplitCreditXLSX(ThisTester):
    def setUp(self):
        self.data_file = get_file_path("raw", "credit_scores.xlsx")
        self.layout_file = self.load_layout("credit_score_xlsx.yaml")
        super(TestSplitCreditXLSX, self).setUp()

    def test_process(self):
        dataset = Dataset("credit", self.layout_file)
        df, pf, lf = process.Process(dataset)
        self.assertIn("data", df)

        # Join data and pii using link and check that values from raw file
        # are still properly linked.
        d_rows = self.processed_reader(df)
        l_rows = self.processed_reader(lf)
        p_rows = self.processed_reader(pf)
        raw = self.processed_xlsx_reader(self.data_file, dataset.header)
        # Excel reader converts values to integers. Test string value.
        raw_values = [(r[1], r[0], str(r[3])) for r in raw.values()]

        self.assertIn("record_id", d_rows[0].keys())
        self.assertIn("pii_id", p_rows[0].keys())
        self.assertIn("dob", p_rows[0].keys())

        for x in range(1, len(d_rows) + 1):
            lids = [d for d in l_rows.values() if d['record_id'] == str(x)][0]
            pv = [d for d in p_rows.values() if d['pii_id'] == lids['pii_id']][0]
            dv = [d for d in d_rows.values() if d['record_id'] == lids['record_id']][0]

            to_check = (pv['last_name'], pv['first_name'], dv['credit_score'])
            self.assertIn(to_check, raw_values)
