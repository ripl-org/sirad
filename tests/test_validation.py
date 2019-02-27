import unittest
from sirad import validate

class TestSSNvalidate(unittest.TestCase):

    def test_invalid_ranges(self):
        self.assertEqual("1", validate.ssn("000111111"))
        self.assertEqual("1", validate.ssn("123001111"))
        self.assertEqual("1", validate.ssn("666111111"))
        self.assertEqual("1", validate.ssn("900111111"))
        self.assertEqual("1", validate.ssn("111110000"))
        self.assertEqual("1", validate.ssn("078051120"))
        self.assertEqual("1", validate.ssn("219099999"))

    def test_valid_ranges(self):
        self.assertEqual("0", validate.ssn("590111111"))
        self.assertEqual("0", validate.ssn("710111111"))
        self.assertEqual("0", validate.ssn("680111111"))
