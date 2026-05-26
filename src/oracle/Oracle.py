# src/oracle/Oracle.py
from __future__ import annotations
import os
import logging
logger: logging.Logger = logging.getLogger(__name__)
from typing import Any
from itertools import islice
from collections.abc import Iterator

import oracledb

from src.models import DataSource, Schema, System, Records, Table, Column
from src.oracle.OracleClient import OracleClient
from src.oracle.OracleTypeMap import oracle_to_python #, python_to_oracle #, raw_type_to_oracledb_input_size
from src.oracle.OracleModels import OracleColumn, OracleTable, to_oracle_snake, to_oracle_table

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


class Oracle(DataSource):
    def __init__(self,
                 environment: str,
                 namespace: str | None = None
                 ) -> None:
        self._default_schema: str | None = namespace.upper() if namespace else None
        self._construct_oracle_client(environment.upper())

    def _construct_oracle_client(self, environment: str) -> None:
        user: str = os.getenv(f"ORACLE_{environment}_USER") or os.getenv(f"ORACLE_{environment.lower()}_USER") or ""
        pwd: str = os.getenv(f"ORACLE_{environment}_PASS") or os.getenv(f"ORACLE_{environment.lower()}_PASS") or ""
        host: str = os.getenv(f"ORACLE_{environment}_HOST") or os.getenv(f"ORACLE_{environment.lower()}_HOST") or ""
        port: str | int = os.getenv(f"ORACLE_{environment}_PORT") or os.getenv(f"ORACLE_{environment.lower()}_PORT") or 1521
        svc: str = os.getenv(f"ORACLE_{environment}_SERVICE") or os.getenv(f"ORACLE_{environment.lower()}_SERVICE") or ""

        if not all([user, pwd, host, svc]):
            raise ValueError(f"Missing Oracle env vars for '{environment}'")

        self._client = OracleClient(
            oracle_user=user,
            oracle_pass=pwd,
            oracle_host=host,
            oracle_port=int(port),
            oracle_service=svc,
        )

    def _schema(self, namespace: str | None = None) -> str:
        return (
            namespace
            or self._default_schema
            or self._client.current_schema
            or self._client.user.upper()
            or ""
        ).upper()

    def describe_schema(self, namespace: str | None = None) -> Schema:
        sql = """SELECT DISTINCT TABLE_NAME FROM ALL_TABLES WHERE OWNER = :schema"""
        schema: str = self._schema(namespace)

        table_names: list[Any] = sorted({
            row["TABLE_NAME"]
            for row in self._client.query(sql, {"schema": schema})
        })

        tables: list[Any] = []
        for tbl in table_names:
            t: Table[Any] = Table(name=tbl, system=System.oracle, namespace=schema)
            full_table: Table[Any] = self.describe_table(t)
            tables.append(full_table)

        return Schema(
            namespace=schema,
            system=System.oracle,
            tables=tables
        )

    def describe_table(self, table: Table) -> Table:
        ora_table: OracleTable = to_oracle_table(table)
        binds: dict[str, str] = {"owner": self._schema(ora_table.namespace), "table_name": ora_table.name.upper()}
        col_filter: set[str] | None = (
            {c.oracle_name or to_oracle_snake(c.name) for c in ora_table.columns}
            if ora_table.columns else None
        )

        pk_set: set[Any] = {
            r["COLUMN_NAME"] for r in self._client.query(SQL_TABLE_PKS, binds)
        }

        fk_map: dict[str, dict[str, str]] = {}
        for r in self._client.query(SQL_TABLE_FKS, binds):
            fk_map.setdefault(r["COLUMN_NAME"], {})[r["REF_TABLE"]] = r["REF_COLUMN"]

        idx_map: dict[str, bool] = {}
        for r in self._client.query(SQL_TABLE_INDEXES, binds):
            idx_map[r["COLUMN_NAME"]] = idx_map.get(r["COLUMN_NAME"], False) \
                or (r["UNIQUENESS"] == "UNIQUE")

        col_rows: list[dict[str, Any]] = [
            r for r in self._client.query(SQL_TABLE_COLUMNS, binds)
            if col_filter is None or r["COLUMN_NAME"] in col_filter
        ]

        columns: list[Any] = []
        for row in col_rows:
            col_name = str(row.get("COLUMN_NAME"))
            raw = str(row.get("DATA_TYPE") or "")
            scale: int | None = int(row["DATA_SCALE"]) if row.get("DATA_SCALE") is not None else None
            prec: int | None = int(row["DATA_PRECISION"]) if row.get("DATA_PRECISION") is not None else None
            length: Any | None = None if raw in ("CLOB", "NCLOB") else (row.get("CHAR_LENGTH") or row.get("DATA_LENGTH"))
            max_length: int | None = int(length) if length is not None else None

            col = OracleColumn(
                name=col_name,
                raw_type=raw,
                python_type=oracle_to_python(raw, scale, max_length),
                ordinal_position=int(row.get("COLUMN_ID") or 0),
                precision=prec,
                scale=scale,
                max_length=max_length,
                is_nullable=str(row.get("NULLABLE", "Y")) == "Y",
                default_value=row.get("DATA_DEFAULT"),
                is_primary_key = col_name in pk_set,
                is_foreign_key = col_name in fk_map,
                foreign_key_mapping = fk_map.get(col_name, {}),
                is_indexed = col_name in idx_map,
                is_unique = idx_map.get(col_name, False),
                serialized_null_value = "NULL",
            )
            columns.append(col)

        return Table(
            name=table.name,
            system=System.oracle,
            namespace=self._schema(table.namespace),
            columns=columns,
        )

    def query(self, statement: str, **kwargs) -> Records:
        return Records(data=self._client.lazy_query(statement, kwargs))

    def get_records(self, table: Table, **kwargs) -> Records:
        # cursor = self._client.cursor()
        try:
            col_str = ", ".join(c.name for c in table.columns) if table.columns else "*"
            sql: str = f"SELECT {col_str} FROM {self._schema(table.namespace)}.{table.name}"

            binds: dict[str, Any] = kwargs.get("binds") or {}
            conditions: list[str] = [f"{k} = :{k}" for k in binds.keys()]
            if conditions:
                sql: str = f"{sql} WHERE {' AND '.join(conditions)}"

            # columns = [col[0] for col in cursor.description or []]

            data: Iterator[dict[str, Any]] = self._client.lazy_query(sql, binds)
            return Records(data=data, columns=table.columns)
        except Exception as e:
            logger.error(f"Error in get_records: {e}")
            return Records(data=iter([]), code=500, message=str(e))

    def load_records(self,
                     action: str,
                     table: Table | OracleTable,
                     records: Records | list[dict[str, Any]],
                     batch_size = 50_000,
                     **kwargs
                     ) -> int:

        if isinstance(records, Records):
            data = records.data
        else:
            data = records
        data = ({to_oracle_snake(k): v for k, v in row.items()} for row in data)

        connection: oracledb.Connection = self._client.connect()
        cursor: oracledb.Cursor = connection.cursor()
        try:
            oracle_table: OracleTable = to_oracle_table(table)
            # self.mutate_table(oracle_table)
            input_sizes: dict[str, Any] = oracle_table.column_input_sizes()
            if input_sizes:
                cursor.setinputsizes(**input_sizes)

            if action == "insert":
                statement: str = oracle_table.insert_sql()
            elif action == "upsert":
                statement: str = oracle_table.merge_sql()
            elif action == "update":
                statement: str = oracle_table.update_sql()
            elif action == "reset":
                self._client.execute(f"TRUNCATE TABLE {oracle_table.qualified_name}")
                statement: str = oracle_table.insert_sql()
            else:
                raise RuntimeError(f"class Oracle, function: load_records, action: Unknown value entered: {action} ")
            
            logger.debug(f"Executing action: {action} statement: {statement} with batch size: {batch_size}")
            while True:
                chunk = list(islice(iter(data), batch_size))
                if not chunk:
                    break
                cursor.executemany(statement, chunk, batcherrors=True)
                batch_errors: list[Any] = cursor.getbatcherrors()
                if batch_errors:
                    message = ""
                    for err in batch_errors:
                        message += f"\n{str(err)}"
                    raise RuntimeError(message)
            connection.commit()
        except Exception as e:
            connection.rollback()
            raise
        finally:
            cursor.close()
        return 1


    def mutate_table(self, table: Table | OracleTable) -> Table:
        ora_table: OracleTable = to_oracle_table(table)
        ora_table.namespace = self._schema()
        while True:
            fetched: list[dict[str, str]] = self._client.all_tab_columns(
                str(ora_table.namespace),
                ora_table.name
            )

            # Build new table
            if not fetched:
                self.mutate_create_table(ora_table)
                ora_table.clear_caches()
                continue

            db_col_map: dict[str, dict[str, str]] = {row["COLUMN_NAME"]: row for row in fetched}
            new_cols: list[Any] = []

            for col in ora_table.columns:
                lookup_key = col.oracle_name or to_oracle_snake(col.name)
                row: dict[str, str] | None = db_col_map.get(lookup_key)
                if not row:
                    # new column
                    col.is_new = True
                    new_cols.append(col)
                else:
                    # existing column
                    col.raw_type = row["DATA_TYPE"]
                    col.char_used = row.get("CHAR_USED")
                    col.is_nullable = row.get("NULLABLE") == "Y"
                    col.is_new = False

                    cur_len = int(row.get("CHAR_LENGTH") or 0)
                    if col.raw_type == "VARCHAR2" and (col.max_length or 0) > cur_len:
                        col.char_length = cur_len
                        self.mutate_existing_column(ora_table, col)
                        ora_table.clear_caches()
                        continue

            if new_cols:
                self.mutate_add_columns(ora_table, new_cols)
                ora_table.clear_caches()
                continue

            break
        return self.describe_table(
            OracleTable(name=ora_table.name, system=System.oracle, namespace=ora_table.namespace)
        )

    def mutate_create_table(self, table: OracleTable) -> None:
        col_defs: list[Any] = []
        for col in table.columns:
            # col.oracle_name = to_oracle_snake(col.name)
            col.is_new = True
            col_defs.append(col.column_definition())

        sql: str = f"CREATE TABLE {table.qualified_name} ({', '.join(col_defs)})"
        with self._client.connect().cursor() as cur:
            cur.execute(sql)

    def mutate_add_columns(self, table: OracleTable, new_columns: list[OracleColumn]) -> None:
        col_defs: str = ", ".join(col.column_definition() for col in new_columns)
        sql: str = f"ALTER TABLE {table.qualified_name} ADD ({col_defs})"
        with self._client.connect().cursor() as cur:
            cur.execute(sql)

    def mutate_existing_column(self, table: OracleTable, col: OracleColumn) -> None:
        sql: str = (
            f"ALTER TABLE {table.qualified_name} "
            f"MODIFY ({col.bind_name} VARCHAR2({col.effective_max_varchar2} CHAR))"
        )
        with self._client.connect().cursor() as cur:
            cur.execute(sql)
