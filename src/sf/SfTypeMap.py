"""SfTypeMap.py"""
from __future__ import annotations

import datetime
import json
import logging
import re
from decimal import Decimal
from typing import Any

from src.models import PythonTypes

logger: logging.Logger = logging.getLogger(__name__)

SF_TYPE_MAP: dict[str, PythonTypes] = {
    'id': PythonTypes.string,
    'string': PythonTypes.string,
    'textarea': PythonTypes.string,
    'email': PythonTypes.string,
    'phone': PythonTypes.string,
    'url': PythonTypes.string,
    'encryptedstring': PythonTypes.string,
    'picklist': PythonTypes.string,
    'multipicklist': PythonTypes.string,
    'combobox': PythonTypes.string,
    'reference': PythonTypes.string,
    'anytype': PythonTypes.string,
    'int': PythonTypes.integer,
    'integer': PythonTypes.integer,
    'long': PythonTypes.integer,
    'double': PythonTypes.float,
    'currency': PythonTypes.float,
    'percent': PythonTypes.float,
    'boolean': PythonTypes.boolean,
    'date': PythonTypes.date,
    'datetime': PythonTypes.datetime,
    'time': PythonTypes.time,
    'base64': PythonTypes.byte,
    'complexvalue': PythonTypes.json,
    'address': PythonTypes.json,
    'location': PythonTypes.json,
}

def sf_type_to_python(sf_type: str) -> PythonTypes:
    """Map a Salesforce describe field ``type`` to a PythonType.

    Unknown/unmapped types fall back to ``string`` (stored as VARCHAR2/CLOB on
    Oracle) with a warning, so a single exotic field type can't abort a full
    schema migration. Add explicit mappings above as real types are discovered.
    """
    mapped = SF_TYPE_MAP.get(sf_type.lower())
    if mapped is None:
        logger.warning(
            "Unmapped Salesforce field type '%s'; defaulting to string.", sf_type
        )
        return PythonTypes.string
    return mapped

# FieldDefinition.DataType (bulk describe) → PythonTypes
def _normalize_fielddef_type(raw: str) -> str:
    return re.sub(r'\(.*\)', '', raw).strip().lower()

SF_FIELDDEF_TYPE_MAP: dict[str, PythonTypes] = {
    'id': PythonTypes.string,
    'text': PythonTypes.string,
    'textarea': PythonTypes.string,
    'longtextarea': PythonTypes.string,
    'html': PythonTypes.string,
    'richtextarea': PythonTypes.string,
    'phone': PythonTypes.string,
    'url': PythonTypes.string,
    'email': PythonTypes.string,
    'picklist': PythonTypes.string,
    'multi-select picklist': PythonTypes.string,
    'combobox': PythonTypes.string,
    'reference': PythonTypes.string,
    'autonumber': PythonTypes.string,
    'encryptedtext': PythonTypes.string,
    'hierarchy': PythonTypes.string,
    'anytype': PythonTypes.string,
    'number': PythonTypes.float,
    'currency': PythonTypes.float,
    'percent': PythonTypes.float,
    'double': PythonTypes.float,
    'integer': PythonTypes.integer,
    'long': PythonTypes.integer,
    'checkbox': PythonTypes.boolean,
    'boolean': PythonTypes.boolean,
    'date': PythonTypes.date,
    'date/time': PythonTypes.datetime,
    'datetime': PythonTypes.datetime,
    'time': PythonTypes.time,
    'base64': PythonTypes.byte,
    'file': PythonTypes.byte,
    'json': PythonTypes.json,
    'address': PythonTypes.json,
    'location': PythonTypes.json,
}

def _to_bool(v: Any) -> bool:
    if isinstance(v, bool):
        return v
    return str(v).lower() == 'true'

def _to_datetime(v: str) -> datetime.datetime:
    if v.endswith('Z'):
        v = v[:-1] + '+00:00'
    elif len(v) > 5 and v[-5] in '+-' and ':' not in v[-5:]:
        v = v[:-2] + ':' + v[-2:]
    return datetime.datetime.fromisoformat(v)

def _to_time(v: str) -> datetime.time:
    if isinstance(v, str) and v.endswith('Z'):
        v = v[:-1]
    return datetime.time.fromisoformat(v)

def _to_decimal(v: Any) -> Decimal:
    return Decimal(str(v))

_SF_CONVERTERS: dict[str, Any] = {
    'int': int,
    'integer': int,
    'long': int,
    'double': float,
    'currency': _to_decimal,
    'percent': float,
    'boolean': _to_bool,
    'date': datetime.date.fromisoformat,
    'datetime': _to_datetime,
    'time': _to_time,
}

def sf_to_python(sf_type: str, value: Any) -> Any:
    """Convert a Salesforce field value to its native Python type."""
    if value is None or value == '':
        return None
        
    converter = _SF_CONVERTERS.get(sf_type)
    if converter:
        try:
            return converter(value)
        except (ValueError, TypeError):
            return value
    return value

def cast_record(record: dict[str, Any], field_types: dict[str, str]) -> dict[str, Any]:
    """Apply sf_to_python to each field in a record using a {field_name: sf_type} map."""
    return {
        k: sf_to_python(field_types[k], v) if k in field_types else v 
        for k, v in record.items()
    }

_CLEAR = object()  # sentinel for explicit null writes to SF

def python_to_sf(value: Any) -> str:
    """Convert a Python value to its Salesforce API string representation."""
    if value is None:
        return ''
    if isinstance(value, bool):
        return 'true' if value else 'false'
    if isinstance(value, datetime.datetime):
        return value.isoformat()
    if isinstance(value, datetime.date):
        return value.isoformat()
    if isinstance(value, datetime.time):
        return value.isoformat()
    if isinstance(value, (dict, list)):
        return json.dumps(value)
    return str(value)

def prepare_record(record: dict[str, Any]) -> dict[str, Any]:
    """Convert Python values to SF API representation.
    Use _CLEAR as a value to explicitly null a field in SF. 
    Omit a key entirely to leave the field unchanged.
    """
    out = {}
    for k, v in record.items():
        if v is _CLEAR:
            out[k] = None
        elif v is not None:
            out[k] = python_to_sf(v)
    return out

def date_to_iso8601(date: datetime.date) -> str:
    datetimestr = date.strftime('%Y-%m-%dT%H:%M:%S')
    timezonestr = date.strftime('%z') or '+0000' # Fallback to UTC
    return (
        f'{datetimestr}{timezonestr[0:3]}:{timezonestr[3:5]}'
        .replace(':', '%3A')
        .replace('+', '%2B')
    )

