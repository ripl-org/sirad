"""
Readers for delimited text and fixed width files.

Code borrowed/inspired from: https://github.com/wireservice/agate
"""
import csv
from collections import namedtuple
from openpyxl import load_workbook

# Character mapping to remove control and protected characters (newlines and |)
# for the output dialect, and to transliterate accented characters.
char_mapping = dict.fromkeys(chr(x) for x in range(32)) # Remove control characters (including newlines)
char_mapping.pop("\t") # ...except tabs
char_mapping["|"] = None # Remove protected character for delimiter in output dialect
char_mapping[u"\x80"] = None # Remove control character
char_mapping[u"\x81"] = None # Remove control character
char_mapping[u"\x82"] = None # Remove control character
char_mapping[u"\x83"] = None # Remove control character
char_mapping[u"\x84"] = None # Remove control character
char_mapping[u"\x85"] = None # Remove control character
char_mapping[u"\x86"] = None # Remove control character
char_mapping[u"\x87"] = None # Remove control character
char_mapping[u"\x88"] = None # Remove control character
char_mapping[u"\x89"] = None # Remove control character
char_mapping[u"\x8a"] = None # Remove control character
char_mapping[u"\x8b"] = None # Remove control character
char_mapping[u"\x8c"] = None # Remove control character
char_mapping[u"\x8d"] = None # Remove control character
char_mapping[u"\x8e"] = None # Remove control character
char_mapping[u"\x8f"] = None # Remove control character
char_mapping[u"\x90"] = None # Remove control character
char_mapping[u"\x91"] = None # Remove control character
char_mapping[u"\x92"] = None # Remove null byte
char_mapping[u"\x93"] = None # Remove control character
char_mapping[u"\x94"] = None # Remove control character
char_mapping[u"\x95"] = None # Remove control character
char_mapping[u"\x96"] = None # Remove control character
char_mapping[u"\x97"] = None # Remove control character
char_mapping[u"\x98"] = None # Remove control character
char_mapping[u"\x99"] = None # Remove control character
char_mapping[u"\x9a"] = None # Remove control character
char_mapping[u"\x9b"] = None # Remove control character
char_mapping[u"\x9c"] = None # Remove control character
char_mapping[u"\x9d"] = None # Remove control character
char_mapping[u"\x9e"] = None # Remove control character
char_mapping[u"\x9f"] = None # Remove control character
char_mapping[u"\xa0"] = " " # Replace non-breaking space
char_mapping[u"\xa1"] = "!" # Invert explanation mark 
char_mapping[u"\xa2"] = " cents" # Replace cents sign with cents 
char_mapping[u"\xa5"] = " Yen" # Remove control character
char_mapping[u"\xa6"] = None # Remove broken bar
char_mapping[u"\xa7"] = "Sec. " # Remove section char
char_mapping[u"\xa8"] = None # Remove diaeresis
char_mapping[u"\xa9"] = " Copyright" # Remove control character
char_mapping[u"\xab"] = "<<" # Replace left pointing double angle quotation mark with <<
char_mapping[u"\xad"] = "-" # Replace soft hyphen with hyphen
char_mapping[u"\xae"] = " Registered" # Replace registered symbol
char_mapping[u"\xaf"] = None # Remove macron
char_mapping[u"\xb0"] = " degrees" # Replace degree symbol
char_mapping[u"\xb1"] = "+/-" # Replace +/- sign
char_mapping[u"\xb4"] = None # Remove accent
char_mapping[u"\xb5"] = " micro" # Replace micro sign
char_mapping[u"\xb6"] = None # Remove pilcrow sign
char_mapping[u"\xb7"] = "." # Replace middle dot with period
char_mapping[u"\xb8"] = None # Remove cedilla
char_mapping[u"\xbb"] = ">>" # Replace right pointing double angle quotation mark with >>
char_mapping[u"\xbc"] = " 1/4 " # Replace 1/4 sign
char_mapping[u"\xbd"] = " 1/2 " # Replace 1/2 sign
char_mapping[u"\xbe"] = " 3/4 " # Replace 3/4 sign
char_mapping[u"\xbf"] = "?" # Invert question mark
char_mapping[u"\xc1"] = "A" # Remove accent mark over A
char_mapping[u"\xc2"] = "A" # Remove hat mark over A
char_mapping[u"\xc3"] = "A" # Remove ~ over A
char_mapping[u"\xc5"] = "A" # Remove ring over A
char_mapping[u"\xc9"] = "E" # Remove acute over E
char_mapping[u"\xcf"] = "I" # Remove diaeresis over I
char_mapping[u"\xd1"] = "N" # Remove ~ on top of N
char_mapping[u"\xd2"] = "O" # Remove grave over O
char_mapping[u"\xd4"] = "O" # Remove hat over O
char_mapping[u"\xd5"] = "O" # Remove ~ over O
char_mapping[u"\xd6"] = "O" # Remove diaeresis over O
char_mapping[u"\xd7"] = "x" # Replace multiplication sign
char_mapping[u"\xd9"] = "U" # Remove grave over U
char_mapping[u"\xda"] = "U" # Remove acute over U
char_mapping[u"\xdb"] = "U" # Remove hat over U
char_mapping[u"\xdc"] = "U" # Remove diaeresis over U
char_mapping[u"\xdd"] = "Y" # Remove acute over Y
char_mapping[u"\xde"] = None # Remove capital letter thorn
char_mapping[u"\xdf"] = "s" # Replace eszett with single s (assuming lower case) 
char_mapping[u"\xe0"] = "a" # Remove grave above a 
char_mapping[u"\xe1"] = "a" # Remove acute above a 
char_mapping[u"\xe2"] = "a" # Remove circumflex above a 
char_mapping[u"\xe3"] = "a" # Remove ~ above a
char_mapping[u"\xe4"] = "a" # Remove diaeresis above a   
char_mapping[u"\xe5"] = "a" # Remove ring above a 
char_mapping[u"\xe6"] = "ae" # Replace ae single letter with ae 
char_mapping[u"\xe7"] = "c" # Remove tail on c
char_mapping[u"\xe8"] = "e" # Remove grave on e
char_mapping[u"\xe9"] = "e" # Remove acute on e
char_mapping[u"\xef"] = "i" # Remove diaeresis on i
char_mapping[u"\xf1"] = "n" # Remove ~ on top of n
char_mapping[u"\xf4"] = "o" # Remove hat over o
char_mapping[u"\xf6"] = "o" # Remove diaeresis over o 
char_mapping[u"\xf7"] = "/" # Replace division symbol
char_mapping[u"\xf9"] = "u" # Remove grave over u 
char_mapping[u"\xfa"] = "u" # Remove acute over u 
char_mapping[u"\xfb"] = "u" # Remove hat over u
char_mapping[u"\xfc"] = "u" # Remove diaeresis over u 
char_mapping[u"\xfd"] = "y" # Replace accent over y
char_mapping[u"\xfe"] = None # remove lower case thorn
char_mapping[u"\xff"] = "y" # Remove diaeresis over y
char_mapping = str.maketrans(char_mapping)


### CSV ###

class CsvReader(object):

    def __init__(self, f, header, **kwargs):
        self.header = header
        if self.header:
            self.reader = csv.DictReader(f, **kwargs)
            # Don't allow leading or trailing spaces in column names (unsupported in YAML format)
            self.reader.fieldnames = [c.strip().upper() for c in self.reader.fieldnames]
            self.header = [c.upper() for c in self.header]
        else:
            self.reader = csv.reader(f, **kwargs)

    def __iter__(self):
        return self

    def __next__(self):
        row = next(self.reader)
        if self.header:
            row = [row[x].translate(char_mapping).strip() for x in self.header if row[x] is not None]
        else:
            row = [x.translate(char_mapping).strip() for x in row]
        if len(row) != len(self.header):
            return self.__next__()
        return row

    @property
    def dialect(self):
        return self.reader.dialect

def csv_reader(*args, **kwargs):
    """
    """
    return CsvReader(*args, **kwargs)


### Fixed Width ###

FixedField = namedtuple("Field", ["name", "start", "end"])

class FixedReader(object):
    """
    """
    def __init__(self, f, widths):
        self.f = f
        self.fields = []

        start = 0
        for name, width in widths:
            end = start + width
            self.fields.append(
                FixedField(name, start, end)
            )
            start = end

    def __iter__(self):
        return self

    def __next__(self):
        line = next(self.f)

        values = []

        for field in self.fields:
            values.append(line[field.start:field.end].translate(char_mapping).strip())

        return values

def fixed_reader(*args, **kwargs):
    """
    """
    return FixedReader(*args, **kwargs)


### Excel ###

def xlsx_reader(filename, header, **kwargs):
    wb = load_workbook(filename=filename, read_only=True, keep_links=False)
    if header:
        mapping = dict((c.value.strip().upper(), i) for i, c in enumerate(next(wb.active.rows)))
        columns = [mapping[c.upper()] for c in header]
        return iter([["" if r[i].value is None else r[i].value for i in columns] for r in wb.active.rows][1:])
    else:
        return iter([["" if c.value is None else c.value for c in r] for r in wb.active.rows])
