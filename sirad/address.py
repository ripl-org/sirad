import usaddress
from sirad import Log

info = Log(__name__).info


def _clean(address):
    """
    Remove problematic street types that confuse uaddress
    """
    address = "".join(c for c in address.upper() if c.isalnum() or c==" ")
    if address.endswith(" STATE PARK"):
        return address.rstrip(" STATE PARK") + " ST"
    elif address.endswith(" TRAILER PARK"):
        return address.rstrip(" TRAILER PARK") + " ST"
    elif address.endswith(" SHOPPING PARK"):
        return address.rstrip(" SHOPPING PARK") + " ST"
    elif address.endswith(" PARK"):
        return address.rstrip(" PARK") + " ST"
    elif address.endswith(" MOBILE MNR"):
        return address.rstrip(" MOBILE MNR") + " ST"
    elif address.endswith(" CT"):
        return address.rstrip(" CT") + " ST"
    elif address.endswith(" HWY"):
        return address.rstrip(" HWY") + " ST"
    elif address.endswith(" COUNTY RD"):
        return address.rstrip(" RD") + " ST" # Only remove RD
    elif address.endswith(" STATE RD"):
        return address.rstrip(" RD") + " ST" # Only remove RD
    else:
        return address


def _tag(address):
    """
    Tag address with usaddress
    """
    try:
        tags = usaddress.tag(address)
    except:
        info(f"cannot tag address {address}")
        return None
    if len(tags) < 1:
        info(f"empty tags for address {address}")
        return None
    else:
        return tags


def normalize_street(address):
    """
    Tag address with usaddress and interpret tags to determine
    a normalized street name.
    """
    tags = _tag(_clean(address))
    # Interpret tags to determine street name
    if tags is not None:
        tags = tags[0]
        if "StreetName" in tags and "StreetNamePreDirectional" in tags:
            return f"{tags['StreetNamePreDirectional']} {tags['StreetName']}"
        elif "StreetName" in tags:
            return tags["StreetName"]
        elif "StreetNamePreDirectional" in tags:
            return tags["StreetNamePreDirectional"]
        elif "StreetNamePostDirectional" in tags:
            return tags["StreetNamePostDirectional"]
        else:
            info(f"missing street name for address {address}")
    return ""


def extract_street_num(address):
    """
    Tag address with usaddress and return street num
    """
    tags = _tag(_clean(address))
    if tags is not None:
        tags = tags[0]
        if "AddressNumber" in tags:
            return tags["AddressNumber"]
        else:
            info(f"missing street num for address {address}")
    return ""
