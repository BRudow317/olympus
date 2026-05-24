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
from src.oracle.OracleDialect import (
    SQL_ALL_TAB_COLUMNS, SQL_SCHEMA_PKS, SQL_SCHEMA_FKS,
    SQL_SCHEMA_INDEXES,
    SQL_TABLE_COLUMNS, SQL_TABLE_PKS, SQL_TABLE_FKS, SQL_TABLE_INDEXES,
    ORACLE_MAX_VARCHAR2_CHAR, to_oracle_snake,
)
from src.oracle.OracleTypeMap import oracle_to_python
from src.oracle.OracleModels import OracleColumn, OracleTable

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

    def to_oracle_table(self, table: Table | OracleTable) -> OracleTable:
        if isinstance(table, Table) and not isinstance(table, OracleTable):
            from src.oracle.OracleTypeMap import python_to_oracle
            cols: list[OracleColumn] = []
            for c in table.columns:
                ora_raw = (
                    python_to_oracle(c).split("(")[0].strip()
                    if c.python_type is not None
                    else c.raw_type
                )
                cols.append(OracleColumn(**{**vars(c), "raw_type": ora_raw}))
            return OracleTable(
                name=table.name,
                system=table.system,
                namespace=table.namespace,
                environment=getattr(table, "environment", None),
                columns=cols,
                properties=table.properties.copy(),
            )
        else:
            return table

    def build_ora_column(self, row: dict[str, Any]) -> OracleColumn:
        raw = str(row.get("DATA_TYPE") or "")
        scale: int | None = int(row["DATA_SCALE"]) if row.get("DATA_SCALE") is not None else None
        prec: int | None = int(row["DATA_PRECISION"]) if row.get("DATA_PRECISION") is not None else None
        length: Any | None = row.get("CHAR_LENGTH") or row.get("DATA_LENGTH")
        max_length: int | None = int(length) if length is not None else None

        return OracleColumn(
            name=str(row["COLUMN_NAME"]),
            raw_type=raw,
            python_type=oracle_to_python(raw, scale),
            ordinal_position=int(row.get("COLUMN_ID") or 0),
            precision=prec,
            scale=scale,
            max_length=max_length,
            is_nullable=str(row.get("NULLABLE", "Y")) == "Y",
            default_value=row.get("DATA_DEFAULT"),
        )

    def describe_schema(self, namespace: str | None = None) -> Schema:
        sql = """SELECT DISTINCT TABLE_NAME FROM ALL_TABLES WHERE OWNER = :schema"""
        schema: str = self._schema(namespace)

        table_names: list[Any] = sorted({
            row["TABLE_NAME"]
            for row in self._client.query(sql, {"schema": schema})
        })

        tables: list[Any] = []
        for tbl in table_names:
            t: Table[Any] = Table(name=tbl, system=System.ORACLE, namespace=schema)
            full_table: Table[Any] = self.describe_table(t)
            tables.append(full_table)

        return Schema(
            namespace=schema,
            system=System.ORACLE,
            tables=tables
        )

    def describe_table(self, table: Table) -> Table:
        ora_table: OracleTable = self.to_oracle_table(table)
        binds: dict[str, str] = {"owner": self._schema(ora_table.namespace), "table_name": ora_table.name.upper()}
        col_filter: set[str] | None = {c.name.upper() for c in ora_table.columns} if ora_table.columns else None

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
                python_type=oracle_to_python(raw, scale),
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
            # col = self.build_ora_column(row)
            columns.append(col)

        return Table(
            name=table.name,
            system=System.ORACLE,
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
            oracle_table: OracleTable = self.to_oracle_table(table)
            # self.mutate_table(oracle_table)
            input_sizes: dict[str, Any] = self.get_input_sizes(oracle_table)
            if input_sizes:
                cursor.setinputsizes(**input_sizes)

            if action == "insert":
                statement: str = self.get_insert_sql(oracle_table)
            elif action == "upsert":
                statement: str = self.get_merge_sql(oracle_table)
            elif action == "update":
                statement: str = self.get_update_sql(oracle_table)
            elif action == "reset":
                self._client.execute(f"TRUNCATE TABLE {oracle_table.qualified_name}")
                statement: str = self.get_insert_sql(oracle_table)
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

    def get_input_sizes(self, table: OracleTable) -> dict[str, Any]:
        if table.input_sizes_cache:
            return table.input_sizes_cache

        sizes  = {}
        for col in table.columns:
            bind_name: str = col.bind_name
            if not col.raw_type:
                continue

            if col.raw_type in ("VARCHAR2", "NVARCHAR2", "CHAR"):
                max_len: int = col.char_length or col.max_length or 4000
                sizes[bind_name] = int(max_len)
            elif col.raw_type in ("NUMBER", "FLOAT"):
                sizes[bind_name] = oracledb.DB_TYPE_NUMBER
            elif col.raw_type == "DATE":
                sizes[bind_name] = oracledb.DB_TYPE_DATE
            elif col.raw_type.startswith("TIMESTAMP"):
                sizes[bind_name] = oracledb.DB_TYPE_TIMESTAMP
            elif col.raw_type == "CLOB":
                sizes[bind_name] = oracledb.DB_TYPE_LONG
            elif col.raw_type == "BLOB":
                sizes[bind_name] = oracledb.DB_TYPE_BLOB
            elif col.raw_type == "RAW":
                sizes[bind_name] = oracledb.DB_TYPE_RAW
            elif col.raw_type == "JSON":
                sizes[bind_name] = oracledb.DB_TYPE_JSON
            else:
                sizes[bind_name] = None

        table.input_sizes_cache = sizes
        return sizes

    def get_insert_sql(self, table: OracleTable) -> str:
        # if hasattr(table, "_insert_sql_stmt_cache") and table._insert_sql_stmt_cache:
        #     return table._insert_sql_stmt_cache

        cols: list[Any] = []
        binds: list[Any] = []
        for col in table.columns:
            bn: str = col.bind_name
            cols.append(bn)
            binds.append(bn if bn.startswith(":") else f":{bn}")

        statement: str = (
            f"INSERT INTO {table.qualified_name} "
            f"({', '.join(cols)}) VALUES ({', '.join(binds)})"
        )
        # table._insert_sql_stmt_cache = statement
        return statement

    def get_merge_sql(self, table: OracleTable) -> str:
        pk_names: list[str] = [col.name for col in table.columns if col.is_primary_key]
        data_names: list[str] = [col.name for col in table.columns if col.name not in pk_names]
        all_cols: list[str] = pk_names + data_names

        match_conds: str = " AND ".join([f"target.{col} = source.{col}" for col in pk_names])
        update_assigns: str = ", ".join([f"target.{col} = source.{col}" for col in data_names])

        insert_cols: str = ", ".join(all_cols)
        source_cols: str = ", ".join([f"source.{col}" for col in all_cols])

        # Notice we now use named binds matching the column names: :ID, :NAME, etc.
        source_selects: str = ", ".join([f":{col} AS {col}" for col in all_cols])

        return f"""
        MERGE INTO {table.qualified_name} target
        USING (SELECT {source_selects} FROM dual) source
        ON ({match_conds})
        WHEN MATCHED THEN
            UPDATE SET {update_assigns}
        WHEN NOT MATCHED THEN
            INSERT ({insert_cols}) VALUES ({source_cols})
        """.strip()

    def get_update_sql(self, table: OracleTable) -> str:
        pk_names: list[str] = [col.name for col in table.columns if col.is_primary_key]
        data_names: list[str] = [col.name for col in table.columns if col.name not in pk_names]
        update_assigns: str = ", ".join([f"{col} = :{col}" for col in data_names])
        where_conds: str = " AND ".join([f"{col} = :{col}" for col in pk_names])

        return f"""
        UPDATE {table.qualified_name}
        SET {update_assigns}
        WHERE {where_conds}
        """.strip()

    def mutate_table(self, table: Table | OracleTable) -> Table:
        ora_table: OracleTable = self.to_oracle_table(table)
        ora_table.namespace = self._schema()
        while True:
            fetched: list[dict[str, str]] = self._client.get_columns(
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
                    col.oracle_name = to_oracle_snake(col.name)
                    col.is_new = True
                    new_cols.append(col)
                else:
                    # existing column
                    col.oracle_name = row["COLUMN_NAME"]
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
            OracleTable(name=ora_table.name, system=System.ORACLE, namespace=ora_table.namespace)
        )

    def mutate_create_table(self, table: OracleTable) -> None:
        col_defs: list[Any] = []
        for col in table.columns:
            col.oracle_name = to_oracle_snake(col.name)
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
