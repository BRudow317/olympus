from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from typing import Any
import re

_NULL_BYTE_RE = re.compile(r'x\00')
_COMMA_RE = re.compile(r',')
_DATE_FMT = '%Y-%m-%d'
_TIMESTAMP_FMTS = ['%Y-%m-%dT%H:%M:%S.%fZ', '%Y-%m-%dT%H:%M:%S', '%Y-%m-%d %H:%M:%S']
_TZ_OFFSET_RE = re.compile(r'[+-]\d{2}:\d{2}$')

def normalize_cell(raw: str, data_type: str) -> Any:
    value = _NULL_BYTE_RE.sub('', raw)
    if not value.strip(): return None
    if data_type == 'NUMBER': return _to_decimal(value)
    elif data_type == 'DATE': return _to_date(value)
    elif data_type == 'TIMESTAMP': return _to_datetime(value)
    else: return normalize_apos(value.strip())

def strip_null_bytes(value: str) -> str: return _NULL_BYTE_RE.sub('', value)
def is_empty(value: str) -> bool: return not _NULL_BYTE_RE.sub('', value).strip()

def _to_decimal(value: str) -> Decimal | None:
    cleaned = _COMMA_RE.sub('', value.strip())
    try: return Decimal(cleaned)
    except InvalidOperation: return None

def _to_date(value: str) -> date | str:
    stripped = value.strip()
    try: return datetime.strptime(stripped[:10], _DATE_FMT).date()
    except ValueError: return stripped

def _to_datetime(value: str) -> datetime | str:
    stripped = value.strip(); cleaned = _TZ_OFFSET_RE.sub('', stripped)
    for fmt in _TIMESTAMP_FMTS:
        try: return datetime.strptime(cleaned, fmt)
        except ValueError: continue
    return stripped

def normalize_quotes(name: str) -> str:
    return '"' + (name or '').replace('"', '""') + '"'

def normalize_apos(cell: str) -> str:
    return (cell or '').replace("'", "''")
