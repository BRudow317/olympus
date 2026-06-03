"""SfDialect.py"""
from __future__ import annotations

import re
import urllib.parse
from datetime import date, datetime, timezone
from string import Formatter
from typing import Any, AnyStr, Iterable, cast

soql_escapes = str.maketrans({
    '\\': '\\\\',
    '\'': '\\\'',
    '"': '\\"',
    '\n': '\\n',
    '\r': '\\r',
    '\t': '\\t',
    '\b': '\\b',
    '\f': '\\f',
})

soql_like_escapes = str.maketrans({
    '%': '\\%',
    '_': '\\_',
})

class SoqlFormatter(Formatter):
    """Custom formatter to apply quoting or the :literal format spec"""
    
    def format_field(self, value: Any, format_spec: str) -> Any:
        if not format_spec:
            return quote_soql_value(value)
        if format_spec == 'literal':
            return value
        if format_spec == 'like':
            return (
                str(value)
                .translate(soql_escapes)
                .translate(soql_like_escapes)
            )
        return super().format_field(value, format_spec)

def format_soql(query: str, *args: Any, **kwargs: Any) -> str:
    """Insert values quoted for SOQL into a format string"""
    return SoqlFormatter().vformat(query, args, kwargs)

def quote_soql_value(value: Any) -> str:
    """Quote/escape either an individual value or a list of values for a SOQL value expression"""
    if isinstance(value, str):
        return "'" + value.translate(soql_escapes) + "'"
    if value is True:
        return 'true'
    if value is False:
        return 'false'
    if value is None:
        return 'null'
    if isinstance(value, (int, float)):
        return str(value)
    if isinstance(value, (list, set, tuple)):
        quoted_items = [quote_soql_value(member) for member in cast(Iterable[Any], value)]
        return '(' + ','.join(quoted_items) + ')'
    if isinstance(value, datetime):
        value = value.replace(microsecond=0)
        value = value.astimezone(tz=timezone.utc)
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
        
    raise ValueError('unquotable value type')

def format_external_id(field: str, value: str | bytes) -> str:
    """Create an external ID string for use with get() or upsert()"""
    return field + '/' + urllib.parse.quote(value, safe='')

def _escape_soql_value(value: Any) -> str:
    """Escape a scalar value for safe SOQL interpolation."""
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (int, float)):
        return str(value)
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
        
    escaped = str(value).replace("\\", "\\\\").replace("'", "\\'")
    return f"'{escaped}'"

def get_object_from_soql(soql: str) -> str | None:
    """Attempt to parse the main object name from a SOQL query string."""
    soql_no_parens = re.sub(r'\(.*?\)', '', soql)
    match = re.search(r'\bFROM\s+([a-zA-Z0-9_]+)', soql_no_parens, re.IGNORECASE)
    return match.group(1) if match else None

def build_count_soql(object_name: str) -> str:
    return f"SELECT COUNT() FROM {object_name}"

def build_null_check_soql(object_name: str, column_name: str) -> str:
    return f"SELECT COUNT() FROM {object_name} WHERE {column_name} = null"

def filter_null_bytes(b: AnyStr) -> AnyStr:
    """https://github.com/airbytehq/airbyte/issues/8300"""
    if isinstance(b, str):
        return b.replace("\x00", "")
    if isinstance(b, bytes):
        return b.replace(b"\x00", b"")
    raise TypeError("Expected str or bytes")