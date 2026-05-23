"""SfTypeMap.py"""
from __future__ import annotations
import datetime, json, re
from decimal import Decimal
from typing import Any

from src.models import PythonTypes

SF_TYPE_MAP: dict[str, PythonTypes] = {
    'id':              PythonTypes.STRING,
    'string':          PythonTypes.STRING,
    'textarea':        PythonTypes.STRING,
    'email':           PythonTypes.STRING,
    'phone':           PythonTypes.STRING,
    'url':             PythonTypes.STRING,
    'encryptedstring': PythonTypes.STRING,
    'picklist':        PythonTypes.STRING,
    'multipicklist':   PythonTypes.STRING,
    'combobox':        PythonTypes.STRING,
    'reference':       PythonTypes.STRING,
    'anytype':         PythonTypes.STRING,
    'int':             PythonTypes.INTEGER,
    'integer':         PythonTypes.INTEGER,
    'double':          PythonTypes.FLOAT,
    'currency':        PythonTypes.FLOAT,
    'percent':         PythonTypes.FLOAT,
    'boolean':         PythonTypes.BOOLEAN,
    'date':            PythonTypes.DATE,
    'datetime':        PythonTypes.DATETIME,
    'time':            PythonTypes.TIME,
    'base64':          PythonTypes.BYTE,
    'complexvalue':    PythonTypes.JSON,
    'address':         PythonTypes.JSON,
    'location':        PythonTypes.JSON,
}


def sf_type_to_python(sf_type: str) -> PythonTypes | None:
    return SF_TYPE_MAP.get(sf_type.lower())

# FieldDefinition.DataType (bulk describe) → PythonTypes
def _normalize_fielddef_type(raw: str) -> str:
    """Strip parameters from FieldDefinition DataType strings.
    e.g. 'Text(80)' -> 'text', 'Number(18, 0)' -> 'number'
    """
    return re.sub(r'\(.*\)', '', raw).strip().lower()

SF_FIELDDEF_TYPE_MAP: dict[str, PythonTypes] = {
    'id':                    PythonTypes.STRING,
    'text':                  PythonTypes.STRING,
    'textarea':              PythonTypes.STRING,
    'longtextarea':          PythonTypes.STRING,
    'html':                  PythonTypes.STRING,
    'richtextarea':          PythonTypes.STRING,
    'phone':                 PythonTypes.STRING,
    'url':                   PythonTypes.STRING,
    'email':                 PythonTypes.STRING,
    'picklist':              PythonTypes.STRING,
    'multi-select picklist': PythonTypes.STRING,
    'combobox':              PythonTypes.STRING,
    'reference':             PythonTypes.STRING,
    'autonumber':            PythonTypes.STRING,
    'encryptedtext':         PythonTypes.STRING,
    'hierarchy':             PythonTypes.STRING,
    'anytype':               PythonTypes.STRING,
    'number':                PythonTypes.FLOAT,
    'currency':              PythonTypes.FLOAT,
    'percent':               PythonTypes.FLOAT,
    'double':                PythonTypes.FLOAT,
    'integer':               PythonTypes.INTEGER,
    'long':                  PythonTypes.INTEGER,
    'checkbox':              PythonTypes.BOOLEAN,
    'boolean':               PythonTypes.BOOLEAN,
    'date':                  PythonTypes.DATE,
    'date/time':             PythonTypes.DATETIME,
    'datetime':              PythonTypes.DATETIME,
    'time':                  PythonTypes.TIME,
    'base64':                PythonTypes.BYTE,
    'file':                  PythonTypes.BYTE,
    'json':                  PythonTypes.JSON,
    'address':               PythonTypes.JSON,
    'location':              PythonTypes.JSON,
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
    'int':      int,
    'integer':  int,
    'double':   float,
    'currency': _to_decimal,
    'percent':  float,
    'boolean':  _to_bool,
    'date':     datetime.date.fromisoformat,
    'datetime': _to_datetime,
    'time':     _to_time,
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
    """Returns an ISO8601 string from a date"""
    datetimestr = date.strftime('%Y-%m-%dT%H:%M:%S')
    timezonestr = date.strftime('%z')
    return (
        f'{datetimestr}{timezonestr[0:3]}:{timezonestr[3:5]}'
        .replace(':', '%3A')
        .replace('+', '%2B')
    )
