"""OracleModels.py"""
from __future__ import annotations

from dataclasses import dataclass #, field
from typing import Any, Literal
import re
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from collections.abc import Iterable

import oracledb

from src.models import Table, Column, PythonTypes


NULL_BYTE_RE: re.Pattern[str] = re.compile(r'\x00')
COMMA_RE: re.Pattern[str] = re.compile(r',')
DATE_FMT = '%Y-%m-%d'
TIMESTAMP_FMTS: list[str] = ['%Y-%m-%dT%H:%M:%S.%f', '%Y-%m-%dT%H:%M:%S', '%Y-%m-%d %H:%M:%S']
TZ_OFFSET_RE: re.Pattern[str] = re.compile(r'[+-]\d{2}:\d{2}$')

ORACLE_MAX_VARCHAR2_CHAR = 4000
varchar2_growth_buffer = 50

ORACLE_RESERVED: frozenset[str] = frozenset({
    "ACCESS", "ADD", "ALL", "ALTER", "AND", "ANY", "AS", "ASC", "AUDIT", 
    "BETWEEN", "BY", "CHAR", "CHECK", "CLUSTER", "COLUMN", "COMMENT", 
    "COMPRESS", "CONNECT", "COUNT", "CREATE", "CURRENT", "DATE", "DECIMAL", 
    "DEFAULT", "DELETE", "DESC", "DISTINCT", "DROP", "ELSE", "EXCLUSIVE", 
    "EXISTS", "FILE", "FLOAT", "FOR", "FROM", "GRANT", "GROUP", "HAVING", 
    "IDENTIFIED", "IMMEDIATE", "IN", "INCREMENT", "INDEX", "INITIAL", 
    "INSERT", "INTEGER", "INTERSECT", "INTO", "IS", "LEVEL", "LIKE", 
    "LOCK", "LONG", "MAXEXTENTS", "MINUS", "MLSLABEL", "MODE", "MODIFY", 
    "NOAUDIT", "NOCOMPRESS", "NOT", "NOWAIT", "NULL", "NUMBER", "OF", 
    "OFFLINE", "ON", "ONLINE", "OPTION", "OR", "ORDER", "PCTFREE", "PRIOR", 
    "PRIVILEGES", "PUBLIC", "RAW", "RENAME", "RESOURCE", "REVOKE", "ROW", 
    "ROWID", "ROWNUM", "ROWS", "SELECT", "SESSION", "SET", "SHARE", "SIZE", 
    "SMALLINT", "START", "SUCCESSFUL", "SYNONYM", "SYSDATE", "TABLE", 
    "THEN", "TO", "TRIGGER", "UID", "UNION", "UNIQUE", "UPDATE", "USER", 
    "VALIDATE", "VALUES", "VARCHAR", "VARCHAR2", "VIEW", "WHENEVER", 
    "WHERE", "WITH", "CROSS", "CUBE", "FETCH", "FULL", "INNER", "JOIN", 
    "LEFT", "MERGE", "NATURAL", "OFFSET", "OUTER", "RIGHT", "ROLLUP", 
    "USING", "WHEN", 
    # Additional Oracle reserved words not in the legacy list 
    "CASE",
})


def to_oracle_snake(
    value: str, 
    max_len: int = 128, 
    reserved: Iterable[str] = ORACLE_RESERVED, 
    force_prefix: bool = False,
    force_suffix: bool = False,
    prefix_value: str = 'SF',
    suffix_value: str = 'COL',
) -> str:
    # 1. Clean input and convert all non-alphanumeric/non-underscore chars to underscores
    s: str = str(value).strip()        
    s = re.sub(r'[^A-Za-z0-9_]+', '_', s)
    s = s.strip('_').upper()
    
    # 2. Reject empty strings after cleaning
    if not s:
        raise ValueError(f"ValueError: to_oracle_snake(value='{value}'")
        
    # 3. Oracle identifiers cannot start with a number. Prefix them accordingly.
    if s[0].isdigit():
        if force_prefix:
            prefix_value=prefix_value.rstrip("_")
            s = f'{prefix_value}_{s}'
        else:
            s = f'C_{s}'
        
    # 4. Handle collision protection for Oracle reserved keyword conflicts
    if s in reserved:
        if force_prefix:
            prefix_value=prefix_value.rstrip("_")
            s = f'{prefix_value}_{s}'
        elif force_suffix:
            s = f'{s}_{suffix_value}'
        else:
            s = f'{prefix_value}_{s}'
        
    # 5. Enforce strict character length rules by truncation at the max length boundary
    if len(s) > max_len:
        s = s[:max_len].rstrip('_')
            
    # 6. Safety fallback check to prevent returning empty values if edge case truncation wipes the string
    return s if s else f"{prefix_value}_{suffix_value}"



# def to_oracle_snake(
#     value: str, 
#     max_len: int = 128, 
#     reserved: Iterable[str] = ORACLE_RESERVED, 
#     forced_prefix: str | None = None, 
#     optional_suffix: str = 'COL',
# ) -> str:
#     s: str = str(value).strip()
#     if not s:
#         return optional_suffix
        
#     s: str = re.sub(r'([a-z0-9])([A-Z])', r'\1_\2', s)
#     s: str = re.sub(r'([A-Za-z])([0-9])', r'\1_\2', s)
#     s: str = re.sub(r'([0-9])([A-Za-z])', r'\1_\2', s)
#     s: str = re.sub(r'[^A-Za-z0-9_]+', '_', s)
#     # s: str = re.sub(r'_+', '_', s)
#     s: str = s.strip('_').upper()
    
#     if not s:
#         return optional_suffix
        
#     if s[0].isdigit():
#         s: str = f'{forced_prefix}_{s}' if forced_prefix else f'C_{s}'
        
#     if s in reserved:
#         s: str = f'{forced_prefix}_{s}' if forced_prefix else f'{s}_{optional_suffix}'
        
#     if len(s) > max_len:
#         prefix: str = forced_prefix or ''
#         if forced_prefix and s.startswith(prefix):
#             s: str = f'{s[len(prefix):max_len].rstrip("_")}'
#         elif s.endswith(f'_{optional_suffix}'):
#             base: str = s[:max_len - len(optional_suffix) - 1].rstrip('_')
#             s: str = f'{base}_{optional_suffix}'
#         else:
#             s: str = s[:max_len].rstrip('_')
            
#     return s if s else optional_suffix

@dataclass(kw_only=True)
class OracleColumn(Column):
    char_length: int | None = None
    char_used: str | None = None
    is_new: bool = False
    # When False, NOT NULL is recorded as DISABLE NOVALIDATE: documented in the
    # catalog but not enforced on DML. Used for Salesforce sources, whose
    # describe nillable=false metadata does not reflect the real data.
    enforce_constraints: bool = True
    serialized_null_value: str | None = "NULL"
    _oracle_name: str | None = None

    @property
    def oracle_name(self) -> str:
        if self._oracle_name:
            return self._oracle_name
        return to_oracle_snake(self.name)

    @property
    def bind_name(self) -> str:
        return self.oracle_name or self.name

    @property
    def effective_max_varchar2(self) -> int:
        observed: int = max((self.char_length or 0), self.max_length or 0)
        
        if observed > ORACLE_MAX_VARCHAR2_CHAR:
            raise ValueError(
                f"observed_char_len {observed} exceeds Oracle max {ORACLE_MAX_VARCHAR2_CHAR}"
            )
            
        buffered: int = observed + varchar2_growth_buffer
        return min(buffered, ORACLE_MAX_VARCHAR2_CHAR)

    def column_definition(self) -> str:
        rt: str | None = self.raw_type
        
        if rt == "VARCHAR2":
            if self.python_type == PythonTypes.boolean:
                type_clause: str = "VARCHAR2(1 CHAR)"
            else:
                type_clause = f"VARCHAR2({self.effective_max_varchar2} CHAR)"
        elif rt == "NUMBER":
            type_clause = "NUMBER"
        elif rt == "DATE":
            type_clause = "DATE"
        elif "TIMESTAMP" in str(rt):
            type_clause = "TIMESTAMP"
        elif rt == "CLOB":
            type_clause = "CLOB"
        elif rt == "BLOB":
            type_clause = "BLOB"
        elif rt == "JSON":
            type_clause = "JSON"
        else:
            raise ValueError(
                f"Unrecognized raw_type '{rt}' on column '{self.bind_name}'"
            )
            
        if self.is_nullable:
            null_clause = " NULL"
        elif self.enforce_constraints:
            null_clause = " NOT NULL"
        else:
            # Record the constraint for documentation/optimizer metadata, but do
            # not enforce it: the source's nullability claim isn't trustworthy.
            null_clause = " NOT NULL DISABLE NOVALIDATE"
        return f"{self.bind_name} {type_clause}{null_clause}"

    @property
    def oracledb_input_size(self) -> Any:
        rt = self.raw_type.upper() if self.raw_type else None
        
        if rt is None:
            return None
            
        if rt in ("VARCHAR2", "NVARCHAR2", "CHAR"):
            return int(self.char_length or self.max_length or 4000)
            
        if rt in ("NUMBER", "FLOAT", "BINARY_FLOAT", "BINARY_DOUBLE"):
            return oracledb.DB_TYPE_NUMBER
            
        if rt == "DATE":
            return oracledb.DB_TYPE_DATE
            
        if rt.startswith("TIMESTAMP"):
            return getattr(oracledb, "DB_TYPE_TIMESTAMP_TZ", oracledb.DB_TYPE_TIMESTAMP)
            
        if rt in ("CLOB", "NCLOB"):
            return oracledb.DB_TYPE_CLOB
            
        if rt in ("BLOB", "BFILE"):
            return oracledb.DB_TYPE_BLOB
            
        if rt in ("RAW", "LONG RAW"):
            return oracledb.DB_TYPE_RAW
            
        if rt == "JSON":
            return getattr(oracledb, "DB_TYPE_JSON", oracledb.DB_TYPE_CLOB)
            
        return None
    
@dataclass(kw_only=True)
class OracleTable(Table[OracleColumn]):
    _fetched_columns: list[dict[str, Any]] | None = None
    _active_plan_cache: list[tuple] | None = None

    @property
    def qualified_name(self) -> str:
        if self.namespace:
            return f"{self.namespace}.{self.name}"
        return self.name

    @property
    def column_map(self) -> dict[str, OracleColumn]:
        return {c.name: c for c in self.columns}

    @property
    def primary_key_columns(self) -> list[OracleColumn]:
        return [c for c in self.columns if c.is_primary_key]

    def column_input_sizes(self) -> dict[str, Any]:
        sizes: dict[str, Any] = {}
        for col in self.columns:
            if not col.raw_type:
                continue
            sizes[col.bind_name] = col.oracledb_input_size
        return sizes

    def insert_sql(self) -> str:
        cols: list[str] = []
        binds: list[str] = []
        
        for col in self.columns:
            bn: str = col.bind_name
            cols.append(bn)
            binds.append(bn if bn.startswith(":") else f":{bn}")
            
        return (
            f"INSERT INTO {self.qualified_name} "
            f"({', '.join(cols)}) VALUES ({', '.join(binds)})"
        )

    def update_sql(self) -> str:
        pk_names: list[str] = [col.bind_name for col in self.columns if col.is_primary_key]
        data_names: list[str] = [col.bind_name for col in self.columns if not col.is_primary_key]
        
        update_assigns: str = ", ".join([f"{col} = :{col}" for col in data_names])
        where_conds: str = " AND ".join([f"{col} = :{col}" for col in pk_names])
        
        return f"""
            UPDATE {self.qualified_name} 
            SET {update_assigns} 
            WHERE {where_conds}
        """.strip()

    def merge_sql(self) -> str:
        pk_names: list[str] = [col.bind_name for col in self.columns if col.is_primary_key]
        data_names: list[str] = [col.bind_name for col in self.columns if not col.is_primary_key]
        all_cols: list[str] = pk_names + data_names

        if not pk_names:
            raise ValueError(
                f"Cannot build an upsert MERGE for '{self.qualified_name}': no primary-key "
                f"column is identified, so there is no key to match on. Upsert requires a key "
                f"(for Salesforce sources this is the Id/ID column)."
            )
        
        match_conds: str = " AND ".join([f"target.{col} = source.{col}" for col in pk_names])
        update_assigns: str = ", ".join([f"target.{col} = source.{col}" for col in data_names])
        insert_cols: str = ", ".join(all_cols)
        source_cols: str = ", ".join([f"source.{col}" for col in all_cols])
        source_selects: str = ", ".join([f":{col} AS {col}" for col in all_cols])
        
        return f"""
            MERGE INTO {self.qualified_name} target 
            USING (SELECT {source_selects} FROM dual) source 
            ON ({match_conds}) 
            WHEN MATCHED THEN 
                UPDATE SET {update_assigns} 
            WHEN NOT MATCHED THEN 
                INSERT ({insert_cols}) 
                VALUES ({source_cols})
        """.strip()

    def clear_caches(self) -> None:
        self._fetched_columns = None
        self._active_plan_cache = None


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
    ON t.owner = c.owner 
    AND t.table_name = c.table_name
WHERE t.owner = :owner
ORDER BY t.table_name, c.column_id
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
WHERE owner = :owner 
  AND table_name = :table_name
ORDER BY column_id
"""

SQL_SCHEMA_PKS = """
SELECT 
    col.table_name, 
    col.column_name
FROM all_constraints con
JOIN all_cons_columns col 
    ON con.constraint_name = col.constraint_name 
    AND con.owner = col.owner
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
    AND fk_con.owner = fk_col.owner
JOIN all_constraints pk_con 
    ON fk_con.r_constraint_name = pk_con.constraint_name 
    AND fk_con.owner = pk_con.owner
JOIN all_cons_columns pk_col 
    ON pk_con.constraint_name = pk_col.constraint_name 
    AND pk_con.owner = pk_col.owner 
    AND fk_col.position = pk_col.position
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
    ON i.index_name = ic.index_name 
    AND i.owner = ic.index_owner
WHERE i.owner = :owner 
  AND i.index_type != 'LOB' 
  AND i.generated = 'N'
"""

SQL_TABLE_PKS = """
SELECT 
    col.column_name
FROM all_constraints con
JOIN all_cons_columns col 
    ON con.constraint_name = col.constraint_name 
    AND con.owner = col.owner
WHERE con.constraint_type = 'P' 
  AND con.owner = :owner 
  AND col.table_name = :table_name
"""

SQL_TABLE_FKS = """
SELECT 
    fk_col.column_name AS column_name, 
    pk_col.table_name AS ref_table, 
    pk_col.column_name AS ref_column
FROM all_constraints fk_con
JOIN all_cons_columns fk_col 
    ON fk_con.constraint_name = fk_col.constraint_name 
    AND fk_con.owner = fk_col.owner
JOIN all_constraints pk_con 
    ON fk_con.r_constraint_name = pk_con.constraint_name 
    AND fk_con.owner = pk_con.owner
JOIN all_cons_columns pk_col 
    ON pk_con.constraint_name = pk_col.constraint_name 
    AND pk_con.owner = pk_col.owner 
    AND fk_col.position = pk_col.position
WHERE fk_con.constraint_type = 'R' 
  AND fk_con.owner = :owner 
  AND fk_col.table_name = :table_name
"""

SQL_TABLE_INDEXES = """
SELECT 
    ic.column_name, 
    i.uniqueness
FROM all_indexes i
JOIN all_ind_columns ic 
    ON i.index_name = ic.index_name 
    AND i.owner = ic.index_owner
WHERE i.owner = :owner 
  AND i.table_name = :table_name 
  AND i.index_type != 'LOB' 
  AND i.generated = 'N'
"""