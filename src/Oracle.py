"""Oracle.py"""
from __future__ import annotations
import logging
logger = logging.getLogger(__name__)
import os
from typing import Any

# from src.DTO import Records
from src.models import DataSource, Schema, Column, Table, System, Records
from src.OracleClient import OracleClient
from src.OracleTypeMap import oracle_to_python
from src.OracleDialect import (
    SQL_Schema_COLUMNS,
    SQL_Schema_PKS,
    SQL_Schema_FKS,
    SQL_Schema_INDEXES,
    SQL_Table_COLUMNS,
    SQL_Table_PKS,
    SQL_Table_FKS,
    SQL_Table_INDEXES,
)



class Oracle(DataSource):
    _client: OracleClient
    _default_schema: str | None

    def __init__(self, environment: str, schema: str | None = None) -> None:
        self._default_schema = schema.upper() if schema else None
        self._construct_oracle(environment)

    def _schema(self, namespace: str | None = None) -> str:
        return (namespace or self._default_schema or self._client.current_schema).upper()

    def _binds(self, table: Table) -> dict[str, str]:
        return {
            "owner":      self._schema(table.namespace),
            "table_name": table.name.upper(),
        }
    def _construct_oracle(self, environment: str) -> None:
        _user    = os.getenv(f"ORACLE_{environment}_USER", "")
        _pass    = os.getenv(f"ORACLE_{environment}_PASS", "")
        _host    = os.getenv(f"ORACLE_{environment}_HOST", "")
        _port    = os.getenv(f"ORACLE_{environment}_PORT", 1521)
        _service = os.getenv(f"ORACLE_{environment}_SERVICE") or os.getenv(f"ORACLE_{environment}_SERVICE_NAME", "")
        if not all([_user, _pass, _host, _service]):
            raise ValueError(
                f"Missing Oracle connection info for environment '{environment}'. "
                f"Required: ORACLE_{environment}_USER, ORACLE_{environment}_PASS, "
                f"ORACLE_{environment}_HOST, ORACLE_{environment}_SERVICE"
            )
        self._client = OracleClient(
            oracle_user=_user,
            oracle_pass=_pass,
            oracle_host=_host,
            oracle_port=int(_port) if _port else 1521,
            oracle_service=_service,
        )
    def describe_schema(self, namespace: str | None = None) -> Schema:
        schema = self._schema(namespace)
        binds  = {"owner": schema}

        pk_set: dict[str, set[str]] = {}
        for row in self._client.query(SQL_Schema_PKS, binds):
            pk_set.setdefault(row["TABLE_NAME"], set()).add(row["COLUMN_NAME"])

        fk_map: dict[str, dict[str, dict[str, str]]] = {}
        for row in self._client.query(SQL_Schema_FKS, binds):
            fk_map.setdefault(row["TABLE_NAME"], {}).setdefault(
                row["COLUMN_NAME"], {}
            )[row["REF_TABLE"]] = row["REF_COLUMN"]

        idx_map: dict[str, dict[str, bool]] = {}
        for row in self._client.query(SQL_Schema_INDEXES, binds):
            tbl, col = row["TABLE_NAME"], row["COLUMN_NAME"]
            idx_map.setdefault(tbl, {})
            idx_map[tbl][col] = idx_map[tbl].get(col, False) or (row["UNIQUENESS"] == "UNIQUE")

        tables: dict[str, list[Column]] = {}
        for row in self._client.query(SQL_Schema_COLUMNS, binds):
            tbl = row["TABLE_NAME"]
            col = self.build_column(row)
            col.is_primary_key          = col.name in pk_set.get(tbl, set())
            col.is_foreign_key          = col.name in fk_map.get(tbl, {})
            col.foreign_key_mapping     = fk_map.get(tbl, {}).get(col.name, {})
            col.is_foreign_key_enforced = col.is_foreign_key
            col.is_indexed              = col.name in idx_map.get(tbl, {})
            col.is_unique               = idx_map.get(tbl, {}).get(col.name, False)
            tables.setdefault(tbl, []).append(col)

        tables_list = [
            Table(name=tbl, namespace=schema, system=System.ORACLE, columns=cols)
            for tbl, cols in sorted(tables.items())
        ]
        return Schema(system=System.ORACLE, tables=tables_list, code=200, message='ok')

    def describe_table(self, table: Table) -> Table:

        binds      = self._binds(table)
        col_filter = {c.name.upper() for c in table.columns} if table.columns else None

        pk_set: set[str] = {
            r["COLUMN_NAME"]
            for r in self._client.query(SQL_Table_PKS, binds)
        }

        fk_map: dict[str, dict[str, str]] = {}
        for r in self._client.query(SQL_Table_FKS, binds):
            fk_map.setdefault(r["COLUMN_NAME"], {})[r["REF_TABLE"]] = r["REF_COLUMN"]

        idx_map: dict[str, bool] = {}
        for r in self._client.query(SQL_Table_INDEXES, binds):
            col = r["COLUMN_NAME"]
            idx_map[col] = idx_map.get(col, False) or (r["UNIQUENESS"] == "UNIQUE")

        col_rows = [
            r for r in self._client.query(SQL_Table_COLUMNS, binds)
            if col_filter is None or r["COLUMN_NAME"] in col_filter
        ]

        columns: list[Column] = []
        for row in col_rows:
            col = self.build_column(row)
            col.is_primary_key          = col.name in pk_set
            col.is_foreign_key          = col.name in fk_map
            col.foreign_key_mapping     = fk_map.get(col.name, {})
            col.is_foreign_key_enforced = col.is_foreign_key
            col.is_indexed              = col.name in idx_map
            col.is_unique               = idx_map.get(col.name, False)
            columns.append(col)

        return Table(
            name=table.name,
            system=System.ORACLE,
            namespace=self._schema(table.namespace),
            columns=columns,
        )

    def query(self, statement: str, **kwargs) -> Records:
        return Records(data=self._client.query(statement, kwargs), code=200, message='ok')

    def get_records(self, table: Table, limit: int = 200) -> Records:
        try:
            col_str = ", ".join(c.name for c in table.columns) if table.columns else "*"
            sql     = f"SELECT {col_str} FROM {self._schema(table.namespace)}.{table.name} FETCH FIRST :limit ROWS ONLY"
            binds: dict[str, Any] = {"limit": limit}
            data=self._client.query(sql, binds)
        
            return Records(data=data, code=200, message='ok')
        except Exception as e:
            logger.error(f"Error in get_records: {e}")
            return Records(data=iter([]), code=500, message=str(e))
    def build_column(self, row: dict[str, Any]) -> Column:
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

    

    