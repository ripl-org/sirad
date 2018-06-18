import unittest
from datetime import date

from sirad import validate

class TestSSNvalidate(unittest.TestCase):

    def test_invalid_ranges(self):
        dob = date(1970, 1, 1)
        self.assertEqual("1", validate.ssn("000-11-1111", dob))
        self.assertEqual("1", validate.ssn("123-00-1111", dob))
        self.assertEqual("1", validate.ssn("666-11-1111", dob))
        self.assertEqual("1", validate.ssn("900-11-1111", dob))
        self.assertEqual("1", validate.ssn("111-11-0000", dob))

    def test_date_ranges(self):
        dob = date(2011, 6, 1)
        dob1 = date(2011, 6, 30)
        dob2 = date(1963, 6, 1)
        #Valid SSN after 2011-06-25
        self.assertEqual("2", validate.ssn("590-11-1111", dob))
        self.assertEqual("0", validate.ssn("590-11-1111", dob1))
        #Valid SSN before 1963-07-01
        self.assertEqual("2", validate.ssn("710-11-1111", dob))
        self.assertEqual("0", validate.ssn("710-11-1111", dob2))
        #Valid SSN always
        self.assertEqual("0", validate.ssn("680-11-1111", dob))
        self.assertEqual("0", validate.ssn("680-11-1111", dob1))
        self.assertEqual("0", validate.ssn("680-11-1111", dob2))