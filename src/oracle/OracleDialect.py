"""OracleDialect.py"""
from __future__ import annotations
import logging
logger: logging.Logger = logging.getLogger(__name__)
from typing import Any
import re
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from collections.abc import Iterable

NULL_BYTE_RE: re.Pattern[str] = re.compile(r'\x00')
COMMA_RE: re.Pattern[str] = re.compile(r',')
DATE_FMT = '%Y-%m-%d'
TIMESTAMP_FMTS: list[str] = ['%Y-%m-%dT%H:%M:%S.%f', '%Y-%m-%dT%H:%M:%S', '%Y-%m-%d %H:%M:%S']
TZ_OFFSET_RE: re.Pattern[str] = re.compile(r'[+-]\d{2}:\d{2}$')

ORACLE_MAX_VARCHAR2_CHAR = 4000
varchar2_growth_buffer = 50

ORACLE_RESERVED: frozenset[str] = frozenset({
    "ACCESS", "ADD", "ALL", "ALTER", "AND", "ANY", "AS", "ASC", "AUDIT", "BETWEEN",
    "BY", "CHAR", "CHECK", "CLUSTER", "COLUMN",
    "COMMENT", "COMPRESS", "CONNECT", "CREATE", "CURRENT", "DATE", "DECIMAL",
    "DEFAULT", "DELETE", "DESC", "DISTINCT", "DROP", "ELSE",
    "EXCLUSIVE", "EXISTS", "FILE", "FLOAT", "FOR", "FROM", "GRANT", "GROUP",
    "HAVING", "IDENTIFIED", "IMMEDIATE", "IN", "INCREMENT",
    "INDEX", "INITIAL", "INSERT", "INTEGER", "INTERSECT", "INTO", "IS", "LEVEL",
    "LIKE", "LOCK", "LONG", "MAXEXTENTS", "MINUS",
    "MLSLABEL", "MODE", "MODIFY", "NOAUDIT", "NOCOMPRESS", "NOT", "NOWAIT", "NULL",
    "NUMBER", "OF", "OFFLINE", "ON", "ONLINE",
    "OPTION", "OR", "ORDER", "PCTFREE", "PRIOR", "PRIVILEGES", "PUBLIC", "RAW",
    "RENAME", "RESOURCE", "REVOKE", "ROW", "ROWID",
    "ROWNUM", "ROWS", "SELECT", "SESSION", "SET", "SHARE", "SIZE", "SMALLINT",
    "START", "SUCCESSFUL", "SYNONYM", "SYSDATE",
    "TABLE", "THEN", "TO", "TRIGGER", "UID", "UNION", "UNIQUE", "UPDATE", "USER",
    "VALIDATE", "VALUES", "VARCHAR", "VARCHAR2",
    "VIEW", "WHENEVER", "WHERE", "WITH", "CROSS", "CUBE", "FETCH", "FULL", "INNER",
    "JOIN", "LEFT", "MERGE", "NATURAL", "OFFSET",
    "OUTER", "RIGHT", "ROLLUP", "USING", "WHEN"
    # Additional Oracle reserved words not in the legacy list "CASE",
})

def to_oracle_snake(value: str, max_len: int = 128, reserved: Iterable[str] = ORACLE_RESERVED, reserved_prefix: str | None = None) -> str:
    s: str = str(value).strip()
    if not s: return 'COL'
    s: str = re.sub(r'([a-z0-9])([A-Z])', r'\1_\2', s)
    s: str = re.sub(r'([A-Za-z])([0-9])', r'\1_\2', s)
    s: str = re.sub(r'([0-9])([A-Za-z])', r'\1_\2', s)
    s: str = re.sub(r'[^A-Za-z0-9_]+', '_', s)
    s: str = re.sub(r'_+', '_', s)
    s: str = s.strip('_').upper()
    if not s:
        return 'COL'
    if s[0].isdigit(): s: str = 'C_' + s
    if s in reserved: s: str = f'{reserved_prefix}{s}' if reserved_prefix else f'{s}_COL'
    if len(s) > max_len:
        prefix: str = reserved_prefix or ''
        if reserved_prefix and s.startswith(prefix):
            s: str = f'{prefix}{s[len(prefix):max_len].rstrip("_")}'
        elif s.endswith('_COL'):
            base: str = s[:max_len - 4].rstrip('_')
            s: str = f'{base}_COL'
        else:
            s: str = s[:max_len].rstrip('_')
    return s if s else 'COL'

def to_decimal(value: str) -> Decimal | None:
    cleaned: str = COMMA_RE.sub('', value.strip())
    try: return Decimal(cleaned)
    except InvalidOperation: return None

def to_date(value: str) -> date | str:
    stripped: str = value.strip()
    try: return datetime.strptime(stripped[:10], DATE_FMT).date()
    except ValueError: return stripped

def to_datetime(value: str) -> datetime | str:
    stripped: str = value.strip()
    cleaned: str = TZ_OFFSET_RE.sub('', stripped)
    for fmt in TIMESTAMP_FMTS:
        try: return datetime.strptime(cleaned, fmt)
        except ValueError: continue
    return stripped

def normalize_cell(raw_type: str, raw: str) -> Any:
    value: str = NULL_BYTE_RE.sub('', raw)
    if not value.strip(): return None
    if raw_type == 'NUMBER': return to_decimal(value)
    elif raw_type == 'DATE': return to_date(value)
    elif raw_type == 'TIMESTAMP': return to_datetime(value)
    else: return value.strip()

SQL_ALL_TAB_COLUMNS = """
SELECT
    t.table_name,
    c.column_name,
    c.column_id,
    c.data_type,
    c.data_length,
    c.char_length,
    c.data_precision,
    c.data_scale,
    c.nullable,
    c.data_default
FROM all_tables t
JOIN all_tab_columns c
    ON t.owner       = c.owner
    AND t.table_name = c.table_name
WHERE t.owner = :owner
ORDER BY t.table_name, c.column_id
"""

SQL_SCHEMA_PKS = """
SELECT
    col.table_name,
    col.column_name
FROM all_constraints con
JOIN all_cons_columns col
    ON con.constraint_name = col.constraint_name
    AND con.owner          = col.owner
WHERE con.constraint_type = 'P'
  AND con.owner = :owner
"""

SQL_SCHEMA_FKS = """
SELECT
    fk_col.table_name AS table_name,
    fk_col.column_name AS column_name,
    pk_col.table_name AS ref_table,
    pk_col.column_name AS ref_column
FROM all_constraints fk_con
JOIN all_cons_columns fk_col
    ON fk_con.constraint_name = fk_col.constraint_name
    AND fk_con.owner          = fk_col.owner
JOIN all_constraints pk_con
    ON fk_con.r_constraint_name = pk_con.constraint_name
    AND fk_con.owner          = pk_con.owner
JOIN all_cons_columns pk_col
    ON pk_con.constraint_name = pk_col.constraint_name
    AND pk_con.owner          = pk_col.owner
    AND fk_col.position       = pk_col.position
WHERE fk_con.constraint_type = 'R'
  AND fk_con.owner = :owner
"""

SQL_SCHEMA_INDEXES = """
SELECT
    i.table_name,
    ic.column_name,
    i.uniqueness
FROM all_indexes i
JOIN all_ind_columns ic
    ON i.index_name   = ic.index_name
    AND i.owner       = ic.index_owner
WHERE i.owner       = :owner
  AND i.index_type != 'LOB'
  AND i.generated   = 'N'
"""

SQL_TABLE_COLUMNS = """
SELECT
    column_name,
    column_id,
    data_type,
    data_length,
    char_length,
    data_precision,
    data_scale,
    nullable,
    data_default
FROM all_tab_columns
WHERE owner       = :owner
  AND table_name  = :table_name
ORDER BY column_id
"""

SQL_TABLE_PKS = """
SELECT col.column_name
FROM all_constraints con
JOIN all_cons_columns col
    ON con.constraint_name = col.constraint_name
    AND con.owner          = col.owner
WHERE con.constraint_type = 'P'
  AND con.owner           = :owner
  AND col.table_name      = :table_name
"""

SQL_TABLE_FKS = """
SELECT
    fk_col.column_name AS column_name,
    pk_col.table_name AS ref_table,
    pk_col.column_name AS ref_column
FROM all_constraints fk_con
JOIN all_cons_columns fk_col
    ON fk_con.constraint_name = fk_col.constraint_name
    AND fk_con.owner          = fk_col.owner
JOIN all_constraints pk_con
    ON fk_con.r_constraint_name = pk_con.constraint_name
    AND fk_con.owner          = pk_con.owner
JOIN all_cons_columns pk_col
    ON pk_con.constraint_name = pk_col.constraint_name
    AND pk_con.owner          = pk_col.owner
    AND fk_col.position       = pk_col.position
WHERE fk_con.constraint_type = 'R'
  AND fk_con.owner           = :owner
  AND fk_col.table_name      = :table_name
"""

SQL_TABLE_INDEXES = """
SELECT
    ic.column_name,
    i.uniqueness
FROM all_indexes i
JOIN all_ind_columns ic
    ON i.index_name   = ic.index_name
    AND i.owner       = ic.index_owner
WHERE i.owner       = :owner
  AND i.table_name  = :table_name
  AND i.index_type != 'LOB'
  AND i.generated   = 'N'
"""