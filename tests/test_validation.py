import unittest
from sirad.dataset import validate_ssn

class TestSSNvalidate(unittest.TestCase):

    def test_invalid_ranges(self):
        self.assertEqual("1", validate_ssn("000111111"))
        self.assertEqual("1", validate_ssn("123001111"))
        self.assertEqual("1", validate_ssn("666111111"))
        self.assertEqual("1", validate_ssn("900111111"))
        self.assertEqual("1", validate_ssn("111110000"))
        self.assertEqual("1", validate_ssn("078051120"))
        self.assertEqual("1", validate_ssn("219099999"))

    def test_valid_ranges(self):
        self.assertEqual("0", validate_ssn("590111111"))
        self.assertEqual("0", validate_ssn("710111111"))
        self.assertEqual("0", validate_ssn("680111111"))
