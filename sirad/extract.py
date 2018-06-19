"""
Helper functions for extracting values from fields.
"""

import hashlib
import logging

from sirad import config
from datetime import datetime

log = logging.getLogger(__name__)

def hash(value, salt):
    if value in config.NULL_VALUES:
        return ""
    else:
        if salt is None:
            k = value.encode("utf-8")
        else:
            k = (value + salt).encode("utf-8")
        return hashlib.sha1(k).hexdigest()

def standard_datetime(dobj):
    return datetime.strftime(dobj, config.DATE_FORMAT)

def date(raw, date_format):
    """
    Extract date from value given date format. If date
    can't be extracted, return empty string.
    """
    dobj = None
    for fmt in date_format.split("|"):
        try:
            dobj = datetime.strptime(raw, fmt)
            break
        except ValueError:
            log.debug("Unable to process {} as date with format {}.".format(raw, fmt))
            pass
    if dobj is None:
        return ""
    else:
        return standard_datetime(dobj)

def dob(raw, field):
    if raw in config.NULL_VALUES:
        return None
    elif isinstance(raw, datetime):
        dobj = raw
    else:
        try:
            dobj = datetime.strptime(raw, field.format)
        except ValueError:
            log.debug("Unable to process DOB {} as date with format {}.".format(raw, field.format))
            return None
    return dobj.date()

def data(raw, field):
    """
    Extract data value
    """
    if not field.data:
        return None
    if raw in config.NULL_VALUES:
        return ""
    else:
        if field.hash:
            return hash(raw, config.get_option("DATA_SALT"))
        elif field.type == "date":
            if isinstance(raw, datetime):
                return standard_datetime(raw)
            else:
                return date(raw, field.format)
        else:
            return raw

def pii(raw, field):
    """
    Extract pii value
    """
    if not field.pii:
        return None
    if raw in config.NULL_VALUES:
        return ""
    else:
        if field.hash:
            return hash(raw, config.get_option("PII_SALT"))
        elif field.type == "date":
            if isinstance(raw, datetime):
                return standard_datetime(raw)
            else:
                return date(raw, field.format)
        else:
            return raw
