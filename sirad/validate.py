import re
from datetime import date
import logging

log = logging.getLogger(__name__)

nondigits_regex = re.compile(r"[^\d]")

ssn_exclude = [
    "012345678",
    "123456789",
    "234567890",
    "345678901",
    "456789012",
    "567890123",
    "678901234",
    "789012345",
    "890123456",
    "901234567",
    "987654321",
    "987654320",
    "987654329",
    "219099999",
    "078051120"
    ]


def ssn(raw, dob):
    """
    Clean SSN and check if valid using information from ssa.gov website
    See : https://www.ssa.gov/employer/stateweb.htm

    ToDo: add dob checks
    """
    valid = "0"
    invalid = "1"
    maybe = "2"
    ssn_digits = re.sub(nondigits_regex, "", raw)
    area = ssn_digits[0:3]
    group = ssn_digits[3:5]
    serial = ssn_digits[5:9]
    if len(ssn_digits) != 9:
        return invalid
    elif ssn_digits in ssn_exclude:
        return invalid
    # Area 666
    elif area in ["666"]:
        return invalid
    # Areas 900-999
    elif area.startswith("9"):
        return invalid
    else:
        chunks = (area, group, serial)
        # check for 0 only
        for c in chunks:
            if c.strip("0") == "":
                return invalid
    if dob is None:
        return maybe
    # Year and Area checks
    elif dob < date(2011, 6, 25):
        if ("587" <= area <= "679") or ("681" <= area <= "699") or ("734" <= area <= "899"):
            return maybe
        elif (dob > date(1963, 7, 1)) and ("700" <= area <= "728"):
            return maybe
    return valid
