"""Oracle.py
"""
from __future__ import annotations
import logging
logger = logging.getLogger(__name__)
from typing import Any

from src.models.dto import Records
from src.models.dto import Catalog, Column, Table
from src.oracle.OracleClient import OracleClient
from src.oracle.OracleTypeMap import oracle_to_python, python_to_oracledb_input_size

_SQL_CATALOG_COLUMNS = """
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
    ON  t.owner      = c.owner
    AND t.table_name = c.table_name
WHERE t.owner = :owner
ORDER BY t.table_name, c.column_id
"""

_SQL_CATALOG_PKS = """
SELECT
    col.table_name,
    col.column_name
FROM all_constraints con
JOIN all_cons_columns col
    ON  con.constraint_name = col.constraint_name
    AND con.owner           = col.owner
WHERE con.constraint_type = 'P'
  AND con.owner = :owner
"""

_SQL_CATALOG_FKS = """
SELECT
    fk_col.table_name  AS table_name,
    fk_col.column_name AS column_name,
    pk_col.table_name  AS ref_table,
    pk_col.column_name AS ref_column
FROM all_constraints fk_con
JOIN all_cons_columns fk_col
    ON  fk_con.constraint_name = fk_col.constraint_name
    AND fk_con.owner           = fk_col.owner
JOIN all_constraints pk_con
    ON  fk_con.r_constraint_name = pk_con.constraint_name
    AND fk_con.r_owner           = pk_con.owner
JOIN all_cons_columns pk_col
    ON  pk_con.constraint_name = pk_col.constraint_name
    AND pk_con.owner           = pk_col.owner
    AND fk_col.position        = pk_col.position
WHERE fk_con.constraint_type = 'R'
  AND fk_con.owner = :owner
"""

_SQL_CATALOG_INDEXES = """
SELECT
    i.table_name,
    ic.column_name,
    i.uniqueness
FROM all_indexes i
JOIN all_ind_columns ic
    ON  i.index_name  = ic.index_name
    AND i.owner       = ic.index_owner
WHERE i.owner      = :owner
  AND i.index_type != 'LOB'
  AND i.generated  = 'N'
"""

_SQL_TABLE_COLUMNS = """
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
WHERE owner      = :owner
  AND table_name = :table_name
ORDER BY column_id
"""

_SQL_TABLE_PKS = """
SELECT col.column_name
FROM all_constraints con
JOIN all_cons_columns col
    ON  con.constraint_name = col.constraint_name
    AND con.owner           = col.owner
WHERE con.constraint_type = 'P'
  AND con.owner      = :owner
  AND col.table_name = :table_name
"""

_SQL_TABLE_FKS = """
SELECT
    fk_col.column_name AS column_name,
    pk_col.table_name  AS ref_table,
    pk_col.column_name AS ref_column
FROM all_constraints fk_con
JOIN all_cons_columns fk_col
    ON  fk_con.constraint_name = fk_col.constraint_name
    AND fk_con.owner           = fk_col.owner
JOIN all_constraints pk_con
    ON  fk_con.r_constraint_name = pk_con.constraint_name
    AND fk_con.r_owner           = pk_con.owner
JOIN all_cons_columns pk_col
    ON  pk_con.constraint_name = pk_col.constraint_name
    AND pk_con.owner           = pk_col.owner
    AND fk_col.position        = pk_col.position
WHERE fk_con.constraint_type = 'R'
  AND fk_con.owner      = :owner
  AND fk_col.table_name = :table_name
"""

_SQL_TABLE_INDEXES = """
SELECT
    ic.column_name,
    i.uniqueness
FROM all_indexes i
JOIN all_ind_columns ic
    ON  i.index_name  = ic.index_name
    AND i.owner       = ic.index_owner
WHERE i.owner      = :owner
  AND i.table_name = :table_name
  AND i.index_type != 'LOB'
  AND i.generated  = 'N'
"""

def _build_column(row: dict[str, Any]) -> Column:
    """Construct a Column DTO from an ALL_TAB_COLUMNS row."""
    raw_type   = str(row.get("DATA_TYPE") or "")
    scale_raw  = row.get("DATA_SCALE")
    prec_raw   = row.get("DATA_PRECISION")
    len_raw    = row.get("CHAR_LENGTH") or row.get("DATA_LENGTH")
    scale      = int(scale_raw) if scale_raw is not None else None
    precision  = int(prec_raw)  if prec_raw  is not None else None
    max_length = int(len_raw)   if len_raw   is not None else None
    return Column(
        name             = str(row["COLUMN_NAME"]),
        raw_type         = raw_type,
        python_type      = oracle_to_python(raw_type, scale),
        ordinal_position = int(row.get("COLUMN_ID") or 0),
        precision        = precision,
        scale            = scale,
        max_length       = max_length,
        is_nullable      = str(row.get("NULLABLE", "Y")) == "Y",
        default_value    = row.get("DATA_DEFAULT"),
    )


def _input_sizes(table: Table) -> dict[str, Any] | None:
    """Build an oracledb setinputsizes map from a Table's Column DTOs."""
    if not table.columns:
        return None
    sizes = {
        col.name: python_to_oracledb_input_size(col)
        for col in table.columns
        if python_to_oracledb_input_size(col) is not None
    }
    return sizes or None


def _ok() -> Catalog:
    return Catalog(ok=True, code=200)


def _err(exc: Exception, code: int = 500) -> Catalog:
    return Catalog(ok=False, code=code, message=str(exc))


class Oracle:
    """
    Protocol-compliant Oracle data interface.
    Wraps OracleClient and maps operations to Catalog / Table / Column DTOs.
    """

    _client: OracleClient

    def __init__(self, client: OracleClient) -> None:
        self._client = client

    def _schema(self) -> str:
        return self._client.current_schema.upper()

    def _binds(self, table: Table) -> dict[str, str]:
        return {
            "owner":      (table.namespace or self._schema()).upper(),
            "table_name": table.name.upper(),
        }

    def describe_catalog(self, namespace: str | None = None) -> Catalog:
        """Return all tables with fully hydrated columns for the given schema."""
        try:
            schema = (namespace or self._schema()).upper()
            binds  = {"owner": schema}

            pk_set: dict[str, set[str]] = {}
            for row in self._client.query(_SQL_CATALOG_PKS, binds):
                pk_set.setdefault(row["TABLE_NAME"], set()).add(row["COLUMN_NAME"])

            fk_map: dict[str, dict[str, dict[str, str]]] = {}
            for row in self._client.query(_SQL_CATALOG_FKS, binds):
                fk_map.setdefault(row["TABLE_NAME"], {}).setdefault(
                    row["COLUMN_NAME"], {}
                )[row["REF_TABLE"]] = row["REF_COLUMN"]

            idx_map: dict[str, dict[str, bool]] = {}
            for row in self._client.query(_SQL_CATALOG_INDEXES, binds):
                tbl, col = row["TABLE_NAME"], row["COLUMN_NAME"]
                idx_map.setdefault(tbl, {})
                idx_map[tbl][col] = idx_map[tbl].get(col, False) or (row["UNIQUENESS"] == "UNIQUE")

            tables: dict[str, list[Column]] = {}
            for row in self._client.query(_SQL_CATALOG_COLUMNS, binds):
                tbl = row["TABLE_NAME"]
                col = _build_column(row)
                col.is_primary_key          = col.name in pk_set.get(tbl, set())
                col.is_foreign_key          = col.name in fk_map.get(tbl, {})
                col.foreign_key_mapping     = fk_map.get(tbl, {}).get(col.name, {})
                col.is_foreign_key_enforced = col.is_foreign_key
                col.is_indexed              = col.name in idx_map.get(tbl, {})
                col.is_unique               = idx_map.get(tbl, {}).get(col.name, False)
                tables.setdefault(tbl, []).append(col)

            entities = [
                Table(name=tbl, namespace=schema, columns=cols)
                for tbl, cols in sorted(tables.items())
            ]
            return Catalog(name=schema, entities=entities, ok=True, code=200)

        except Exception as exc:
            logger.error("describe_catalog failed: %s", exc)
            return _err(exc)

    def describe_table(self, table: Table) -> Catalog:
        """
        Return the Table with columns hydrated.
        If the Table already has columns set, only those columns are hydrated.
        """
        try:
            binds      = self._binds(table)
            col_filter = {c.name.upper() for c in table.columns} if table.columns else None

            pk_set: set[str] = {
                r["COLUMN_NAME"]
                for r in self._client.query(_SQL_TABLE_PKS, binds)
            }

            fk_map: dict[str, dict[str, str]] = {}
            for r in self._client.query(_SQL_TABLE_FKS, binds):
                fk_map.setdefault(r["COLUMN_NAME"], {})[r["REF_TABLE"]] = r["REF_COLUMN"]

            idx_map: dict[str, bool] = {}
            for r in self._client.query(_SQL_TABLE_INDEXES, binds):
                col = r["COLUMN_NAME"]
                idx_map[col] = idx_map.get(col, False) or (r["UNIQUENESS"] == "UNIQUE")

            col_rows = [
                r for r in self._client.query(_SQL_TABLE_COLUMNS, binds)
                if col_filter is None or r["COLUMN_NAME"] in col_filter
            ]

            columns: list[Column] = []
            for row in col_rows:
                col = _build_column(row)
                col.is_primary_key          = col.name in pk_set
                col.is_foreign_key          = col.name in fk_map
                col.foreign_key_mapping     = fk_map.get(col.name, {})
                col.is_foreign_key_enforced = col.is_foreign_key
                col.is_indexed              = col.name in idx_map
                col.is_unique               = idx_map.get(col.name, False)
                columns.append(col)

            entity = Table(name=table.name, namespace=table.namespace, columns=columns)
            return Catalog(entities=[entity], ok=True, code=200)

        except Exception as exc:
            logger.error("describe_table failed for %r: %s", table.name, exc)
            return _err(exc)

    def query(self, sql: str, binds: dict[str, Any] | None = None) -> Catalog:
        """Execute raw SQL. Catalog.records is a lazy Iterator[dict[str, Any]]."""
        try:
            return Catalog(records=self._client.query(sql, binds), ok=True, code=200)
        except Exception as exc:
            logger.error("query failed: %s", exc)
            return _err(exc)

    def get_records(
        self,
        table: Table,
        columns: list[str] | None = None,
        filters: dict[str, Any] | None = None,
    ) -> Catalog:
        """
        SELECT from a table.
        columns: column names to project; None selects all.
        filters: simple equality WHERE conditions {column: value}, ANDed together.
        Catalog.records is a lazy Iterator[dict[str, Any]].
        """
        try:
            schema  = (table.namespace or self._schema()).upper()
            col_str = ", ".join(columns) if columns else "*"
            sql     = f"SELECT {col_str} FROM {schema}.{table.name}"
            binds: dict[str, Any] = {}

            if filters:
                sql  += " WHERE " + " AND ".join(f"{k} = :{k}" for k in filters)
                binds = dict(filters)

            return Catalog(records=self._client.query(sql, binds), ok=True, code=200)

        except Exception as exc:
            logger.error("get_records failed for %r: %s", table.name, exc)
            return _err(exc)

    

    