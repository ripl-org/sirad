def ssn(digits):
    """
    Clean SSN and check if valid using information from ssa.gov website
    See : https://www.ssa.gov/employer/stateweb.htm
    """
    valid = "0"
    invalid = "1"
    # Wrong length
    if len(digits) != 9:
        return invalid
    # Any component is all 0
    elif digits[:3] == "000" or digits[3:5] == "00" or digits[5:9] == "0000":
        return invalid
    # Area 666
    elif digits[:3] == "666":
        return invalid
    # Areas 900-999
    elif digits.startswith("9"):
        return invalid
    # used in an ad by the Social Security Administration
    elif digits == "219099999":
        return invalid
    # Woolworth Wallet Fiasco
    elif digits == "078051120":
        return invalid
    return valid
