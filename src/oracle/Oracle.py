# src/oracle/Oracle.py
from __future__ import annotations
import re
import logging

logger: logging.Logger = logging.getLogger(__name__)

from typing import Any
from itertools import islice
from collections.abc import Iterator, Iterable

import oracledb
from src.models import DataSource, Schema, System, Records, Table, Column, PythonTypes
from src.oracle.OracleClient import OracleClient
from src.oracle.OracleTypeMap import to_oracle_table, oracle_to_python, normalize_cell, immutable_constraints #, python_to_oracle #, raw_type_to_oracledb_input_size
from src.oracle.OracleModels import (
    OracleColumn,
    OracleTable,
    to_oracle_snake,
    SQL_TABLE_PKS,
    SQL_TABLE_FKS,
    SQL_TABLE_INDEXES,
    SQL_TABLE_COLUMNS,
    ORACLE_MAX_VARCHAR2_CHAR,
    varchar2_growth_buffer,
)

def _normalize_ora_type(raw: str) -> str:
    """Canonicalize an Oracle catalog DATA_TYPE for drift comparison.

    Oracle reports temporal types with an inline precision TIMESTAMP
    comes back as TIMESTAMP(6), while our column metadata stores the bare
    type name. Strip the <n> precision so TIMESTAMP(6) compares equal
    to TIMESTAMP and MODIFY drift-sync actually converges.
    """
    return re.sub(r"\(\s*\d+\s*\)", "", str(raw or "")).strip().upper()

# ORA-12899: value too large for column "S"."T"."COL" (actual: 113, maximum: 90)
_ORA_12899_RE: re.Pattern[str] = re.compile(
    r'column\s+(?:"[^"]+"\.)*"(?P<col>[^"]+)"\s*'
    r'\(actual:\s*(?P<actual>\d+),\s*maximum:\s*(?P<max>\d+)\)'
)

def _parse_value_too_large(message: str) -> tuple[str, int] | None:
    """Extract (oracle_column_name, actual_length) from an ORA-12899 message."""
    m = _ORA_12899_RE.search(message or "")
    if not m:
        return None
    return m.group("col"), int(m.group("actual"))

# ORA-01400: cannot insert NULL into ("S"."T"."COL")
_ORA_01400_RE: re.Pattern[str] = re.compile(
    r'cannot insert NULL into\s*\((?:"[^"]+"\.)*"(?P<col>[^"]+)"\)'
)

def _parse_cannot_insert_null(message: str) -> str | None:
    """Extract the oracle_column_name from an ORA-01400 message."""
    m = _ORA_01400_RE.search(message or "")
    return m.group("col") if m else None

class Oracle(DataSource):
    client: OracleClient
    environment: str | None
    namespace: str | None

    def __init__(self, environment: str, namespace: str | None = None) -> None:
        # self._get_oracle_client(environment.upper())
        self.environment = environment.upper()
        self.client: OracleClient = OracleClient.client_constructor(self.environment)
        self.namespace = namespace.upper() if namespace else self.client.user.upper()

    def schema(self, namespace: str | None = None) -> str:
        return (
            namespace or
            self.namespace or
            self.client.current_schema or
            self.client.user.upper() or ""
        ).upper()

    def is_healthy(self) -> bool:
        """Return True if a live connection to the Oracle database can be made."""
        try:
            self.client.connect()
            return self.client.is_healthy()
        except Exception as e:
            logger.error("Oracle health check failed for %s: %s", self.environment, e)
            return False

    def describe_schema(self, namespace: str | None = None) -> Schema:
        sql = """SELECT DISTINCT TABLE_NAME FROM ALL_TABLES WHERE OWNER = :schema"""
        schema: str = self.schema(namespace)
        
        table_names: list[Any] = sorted({
            row["TABLE_NAME"] for row in self.client.query(sql, {"schema": schema})
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
        binds: dict[str, str] = {
            "owner": self.schema(ora_table.namespace), 
            "table_name": ora_table.name.upper()
        }
        
        col_filter: set[str] | None = (
            {c.oracle_name or to_oracle_snake(c.name) for c in ora_table.columns} 
            if ora_table.columns else None
        )
        
        pk_set: set[Any] = {
            r["COLUMN_NAME"] for r in self.client.query(SQL_TABLE_PKS, binds)
        }
        
        fk_map: dict[str, dict[str, str]] = {}
        for r in self.client.query(SQL_TABLE_FKS, binds):
            fk_map.setdefault(r["COLUMN_NAME"], {})[r["REF_TABLE"]] = r["REF_COLUMN"]
        
        idx_map: dict[str, bool] = {}
        for r in self.client.query(SQL_TABLE_INDEXES, binds):
            idx_map[r["COLUMN_NAME"]] = (
                idx_map.get(r["COLUMN_NAME"], False) or (r["UNIQUENESS"] == "UNIQUE")
            )
        
        col_rows: list[dict[str, Any]] = [
            r for r in self.client.query(SQL_TABLE_COLUMNS, binds) 
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
                is_primary_key=col_name in pk_set,
                is_foreign_key=col_name in fk_map,
                foreign_key_mapping=fk_map.get(col_name, {}),
                is_indexed=col_name in idx_map,
                is_unique=idx_map.get(col_name, False),
                serialized_null_value="NULL",
            )
            columns.append(col)

        return Table(
            name=table.name,
            system=System.oracle,
            environment=table.environment,
            alias=table.alias,
            namespace=self.schema(table.namespace),
            prefix=table.prefix,
            columns=columns,
        )
    
    def query(self, statement: str, **kwargs) -> Records:
        return Records(data=self.client.lazy_query(statement, kwargs))

    def get_records(self, table: Table, **kwargs) -> Records:
        try:
            col_str = ", ".join(c.name for c in table.columns) if table.columns else "*"
            sql: str = f"SELECT {col_str} FROM {self.schema(table.namespace)}.{table.name}"
            
            binds: dict[str, Any] = kwargs.get("binds") or {}
            conditions: list[str] = [f"{k} = :{k}" for k in binds.keys()]
            
            if conditions:
                sql: str = f"{sql} WHERE {' AND '.join(conditions)}"
                
            data: Iterator[dict[str, Any]] = self.client.lazy_query(sql, binds)
            return Records(data=data, columns=table.columns)
            
        except Exception as e:
            logger.error(f"Error in get_records: {e}")
            return Records(data=iter([]), code=500, message=str(e))

    # ------------------------------------------------------------------ #
    # MERGE match-column index
    #
    # The engine's generated MERGE (OracleTable.merge_sql) matches on the
    # is_primary_key columns -- always the Salesforce 'Id' for SF sources. The
    # SF_* staging tables are created without any constraints (SF data is not
    # trusted), so without an explicit index the MERGE full-scans the target
    # once per row per batch and the job hangs at scale. These helpers ensure a
    # single-column, NON-UNIQUE index exists on that match column.
    #
    # Why non-unique: gives the MERGE a probe without enforcing uniqueness on
    # untrusted SF data, so a duplicate Id still loads rather than aborting.
    # Why single-column: a composite index could be cascade-dropped by a CLOB
    # column promotion (DROP COLUMN CASCADE CONSTRAINTS); single-column on the
    # never-promoted Id keeps the index and the expansion path independent.
    # ------------------------------------------------------------------ #
    def _column_index_names(self, schema: str, table_name: str, column_name: str) -> list[str]:
        """Return names of any indexes covering `column_name` on the table.

        Reads ALL_IND_COLUMNS, which the app schema can see without DBA grants.
        """
        sql = """
            SELECT index_name
            FROM all_ind_columns
            WHERE table_owner = :owner
              AND table_name  = :table_name
              AND column_name = :column_name
        """
        binds = {
            "owner": schema.upper(),
            "table_name": table_name.upper(),
            "column_name": column_name.upper(),
        }
        return [r["INDEX_NAME"] for r in self.client.query(sql, binds)]

    def _match_index_name(self, table_name: str, column_name: str) -> str:
        """Build a collision-safe, <=128 char index identifier for the match column.

        Convention is IX_<table>_<col>; swap here if the DBA expects a different
        naming scheme. Single-column by invariant (see class-level note).
        """
        base = re.sub(r"[^A-Z0-9_]+", "_", f"IX_{table_name}_{column_name}".upper()).strip("_")
        if len(base) <= 128:
            return base
        keep = f"_{column_name}".upper()[:40]
        return f"{base[:128 - len(keep)]}{keep}"

    def _ensure_match_index(self, oracle_table: OracleTable) -> None:
        """Idempotently ensure the MERGE match column is indexed.

        Called before an upsert/update (where the MERGE needs the probe) and at
        table creation. Self-heals existing keyless staging tables: the first
        upsert into one builds the missing index before the MERGE runs.
        """
        match_cols = [c.oracle_name for c in oracle_table.columns if c.is_primary_key]
        if not match_cols:
            # merge_sql() itself raises on a missing PK; nothing to index here.
            return
        if len(match_cols) > 1:
            # SF sources are single-column Id. A composite match column is
            # unexpected, and a composite index could be cascade-dropped by a
            # column promotion, so refuse rather than build something fragile.
            raise RuntimeError(
                f"_ensure_match_index: expected one match column on "
                f"'{oracle_table.qualified_name}', got {match_cols}."
            )
        schema = str(oracle_table.namespace)
        col_name = match_cols[0]
        if self._column_index_names(schema, oracle_table.name, col_name):
            return  # already indexed
        index_name = self._match_index_name(oracle_table.name, col_name)
        sql = f"CREATE INDEX {schema}.{index_name} ON {oracle_table.qualified_name} ({col_name})"
        logger.info(
            "Creating MERGE match index '%s' on '%s.%s' so upsert probes "
            "instead of full-scanning.",
            index_name, oracle_table.qualified_name, col_name,
        )
        try:
            self.client.execute(sql)
            self.client.commit()
        except oracledb.Error as e:
            # ORA-00955 (name already used) / ORA-01408 (column already indexed)
            # mean a concurrent run beat us to it; treat as success.
            err = e.args[0] if e.args else None
            if getattr(err, "code", None) in (955, 1408):
                logger.info("MERGE match index already present on '%s'; continuing.", col_name)
                return
            raise

    def load_records(
        self, 
        action: str, 
        table: Table | OracleTable, 
        records: Records | list[dict[str, Any]], 
        batch_size=50_000, 
        **kwargs 
    ) -> None:
        oracle_table: OracleTable = to_oracle_table(table)
        mutated_records = self.mutate_records(oracle_table, records)
        data = mutated_records.data
        
        if action == 'reset':
            if self.client.check_object_exists( oracle_table.qualified_name, schema=str(oracle_table.namespace), object_type="TABLE"):
                self.client.execute(f"TRUNCATE TABLE {oracle_table.qualified_name} CASCADE")
                self.client.commit()

        connection: oracledb.Connection = self.client.connect()
        cursor: oracledb.Cursor = connection.cursor()
        
        try:
            input_sizes: dict[str, Any] = oracle_table.column_input_sizes()
            if input_sizes:
                cursor.setinputsizes(**input_sizes)

            if action == "insert":
                statement: str = oracle_table.insert_sql()
            elif action == "upsert":
                # MERGE needs an index on the match column or it full-scans the
                # target per batch and hangs. Runs before any batch DML is sent,
                # so the index DDL's implicit commit cannot tear a load.
                self._ensure_match_index(oracle_table)
                statement: str = oracle_table.merge_sql()
            elif action == "update":
                self._ensure_match_index(oracle_table)
                statement: str = oracle_table.update_sql()
            elif action in ("reset", "full_load"):
                statement: str = oracle_table.insert_sql()
            else:
                raise RuntimeError(f"class Oracle, function: load_records, action: Unknown value entered: {action} ")
                
            logger.debug(f"Executing action: {action} statement: {statement} with batch size: {batch_size}")
            
            while True:
                chunk = list(islice(iter(data), batch_size))
                if not chunk:
                    break

                self._load_chunk(cursor, connection, oracle_table, statement, chunk)

            connection.commit()

        except Exception as e:
            logger.error(f"oracle_table: {oracle_table.__repr__()}")
            connection.rollback()
            raise e

        finally:
            cursor.close()

    def _load_chunk(
        self,
        cursor: oracledb.Cursor,
        connection: oracledb.Connection,
        oracle_table: OracleTable,
        statement: str,
        chunk: list[dict[str, Any]],
    ) -> None:
        """Execute one batch, widening VARCHAR2 columns that overflow and
        retrying only the failed rows.
        """
        pending: list[dict[str, Any]] = chunk
        max_passes = 2 * len(oracle_table.columns) + 3
        for _ in range(max_passes):
            cursor.executemany(statement, pending, batcherrors=True)
            errors: list[Any] = cursor.getbatcherrors()
            if not errors:
                return
                
            widen: dict[str, int] = {}
            promote: set[str] = set()
            relax_null: set[str] = set()
            retry_rows: list[dict[str, Any]] = []
            
            for err in errors:
                code = getattr(err, "code", None)
                message = getattr(err, "message", str(err))
                too_large = _parse_value_too_large(message)
                null_col = _parse_cannot_insert_null(message)
                
                if code == 12899 and too_large is not None:
                    col_name, actual = too_large
                    if actual > ORACLE_MAX_VARCHAR2_CHAR:
                        promote.add(col_name)
                    else:
                        widen[col_name] = max(widen.get(col_name, 0), actual)
                    retry_rows.append(pending[err.offset])
                elif code == 1400 and null_col is not None:
                    relax_null.add(null_col)
                    retry_rows.append(pending[err.offset])
                else:
                    raise RuntimeError(
                        "".join(f"\n{err}" for err in errors)
                    )
                    
            for col_name in promote:
                widen.pop(col_name, None)
                
            if widen:
                self._widen_columns(connection, cursor, oracle_table, widen)
            if promote:
                self._promote_columns_to_clob(connection, cursor, oracle_table, promote)
            if relax_null:
                self._relax_not_null(connection, cursor, oracle_table, relax_null)
                
            pending = retry_rows
            
        raise RuntimeError(
            f"load_records: on-the-fly column adaptation for "
            f"'{oracle_table.qualified_name}' did not converge after {max_passes} passes."
        )

    def _widen_columns(
        self,
        connection: oracledb.Connection,
        cursor: oracledb.Cursor,
        oracle_table: OracleTable,
        widen: dict[str, int],
    ) -> None:
        """Grow VARCHAR2 columns to fit oversized data (capped at the Oracle max)."""
        col_by_name: dict[str, OracleColumn] = {c.oracle_name: c for c in oracle_table.columns}
        connection.commit()
        
        for col_name, required in widen.items():
            col = col_by_name.get(col_name)
            if col is None or col.raw_type != "VARCHAR2":
                raise RuntimeError(
                    f"Cannot widen column '{oracle_table.qualified_name}.{col_name}': "
                    f"not a VARCHAR2 column in the table definition."
                )
            if required > ORACLE_MAX_VARCHAR2_CHAR:
                raise RuntimeError(
                    f"Column '{oracle_table.qualified_name}.{col_name}' needs {required} chars, "
                    f"exceeding the VARCHAR2 maximum of {ORACLE_MAX_VARCHAR2_CHAR}."
                )
            buf = varchar2_growth_buffer()
            new_size = min(required + buf, ORACLE_MAX_VARCHAR2_CHAR)
            alter_sql = f"ALTER TABLE {oracle_table.qualified_name} MODIFY ({col_name} VARCHAR2({new_size} CHAR))"
            logger.info(
                "Widening '%s.%s' to VARCHAR2(%d CHAR) to fit oversized source data (actual %d).",
                oracle_table.qualified_name, col_name, new_size, required,
            )
            self.client.execute(alter_sql)
            col.char_length = new_size
            col.max_length = new_size
            
        input_sizes = oracle_table.column_input_sizes()
        if input_sizes:
            cursor.setinputsizes(**input_sizes)

    def _temp_column_name(self, oracle_table: OracleTable, base: str) -> str:
        """A unique helper-column name for the CLOB clone (Oracle id <= 128 chars)."""
        existing = {c.oracle_name.upper() for c in oracle_table.columns}
        candidate = f"{base[:120]}_CLOB"
        suffix = 0
        while candidate.upper() in existing:
            suffix += 1
            candidate = f"{base[:116]}_CLOB{suffix}"
        return candidate

    def _promote_columns_to_clob(
        self,
        connection: oracledb.Connection,
        cursor: oracledb.Cursor,
        oracle_table: OracleTable,
        col_names: Iterable[str],
    ) -> None:
        """Convert VARCHAR2 columns that overflow past the VARCHAR2 max to CLOB."""
        connection.commit()
        q = oracle_table.qualified_name
        schema = str(oracle_table.namespace)
        col_by_name: dict[str, OracleColumn] = {c.oracle_name: c for c in oracle_table.columns}
        
        for name in col_names:
            col = col_by_name.get(name)
            if col is None:
                raise RuntimeError(f"Cannot promote unknown column '{q}.{name}' to CLOB.")
            if col.raw_type == "CLOB":
                continue
            if col.raw_type != "VARCHAR2":
                raise RuntimeError(
                    f"Refusing to promote '{q}.{name}' to CLOB: it is {col.raw_type}, not VARCHAR2."
                )

            # Seatbelt: never promote an indexed column. The DROP COLUMN CASCADE
            # CONSTRAINTS below would silently take the index with it, and the
            # next upsert would full-scan and hang. For SF sources the match
            # index is on Id, which is fixed-width and never promoted, so this
            # never fires in practice -- it is protection for future changes to
            # the match column. Drop the index deliberately first if a genuine
            # indexed-column promotion is ever intended.
            covering = self._column_index_names(schema, oracle_table.name, name)
            if covering:
                raise RuntimeError(
                    f"Refusing to promote '{q}.{name}' to CLOB: covered by "
                    f"index(es) {', '.join(covering)} that DROP COLUMN would "
                    f"cascade away. Drop the index deliberately first if intended."
                )

            constraints = self.client.all_constraints(
                schema=schema, table_name=oracle_table.name, column_name=name
            )
            blocking = immutable_constraints(constraints)
            if blocking:
                detail = ", ".join(
                    f"{c.get('CONSTRAINT_NAME')} ({c.get('CONSTRAINT_TYPE_DESC')})"
                    for c in blocking
                )
                raise RuntimeError(
                    f"Cannot promote '{q}.{name}' to CLOB: it participates in immutable "
                    f"constraint(s) that must not be dropped: {detail}."
                )
                
            tmp = self._temp_column_name(oracle_table, name)
            logger.info(
                "Promoting '%s.%s' VARCHAR2 -> CLOB to fit oversized source data "
                "(dropping %d column constraint(s)).",
                q, name, len(constraints),
            )
            self.client.execute(f"ALTER TABLE {q} ADD ({tmp} CLOB)")
            self.client.execute(f"UPDATE {q} SET {tmp} = {name}")
            self.client.commit()
            self.client.execute(f"ALTER TABLE {q} DROP COLUMN {name} CASCADE CONSTRAINTS")
            self.client.execute(f"ALTER TABLE {q} RENAME COLUMN {tmp} TO {name}")
            
            col.raw_type = "CLOB"
            col.max_length = None
            col.char_length = None
            col.is_nullable = True
            
        input_sizes = oracle_table.column_input_sizes()
        if input_sizes:
            cursor.setinputsizes(**input_sizes)

    def _relax_not_null(
        self,
        connection: oracledb.Connection,
        cursor: oracledb.Cursor,
        oracle_table: OracleTable,
        col_names: Iterable[str],
    ) -> None:
        """Drop the NOT NULL constraint on columns the source falsely marked non-nullable."""
        connection.commit()
        q = oracle_table.qualified_name
        schema = str(oracle_table.namespace)
        col_by_name: dict[str, OracleColumn] = {c.oracle_name: c for c in oracle_table.columns}
        
        for name in col_names:
            constraints = self.client.all_constraints(
                schema=schema, table_name=oracle_table.name, column_name=name
            )
            logger.info(
                "Relaxing NOT NULL on '%s.%s': source metadata claimed non-nullable "
                "but the data is null (constraints: %s).",
                q, name, ", ".join(str(c.get("CONSTRAINT_NAME")) for c in constraints) or "none",
            )
            self.client.execute(f"ALTER TABLE {q} MODIFY ({name} NULL)")
            col = col_by_name.get(name)
            if col is not None:
                col.is_nullable = True

    def mutate_records(self, table: OracleTable, records: Records | list[dict[str, Any]]) -> Records:
        if isinstance(records, Records):
            raw_data = records.data
            cols = records.columns
        else:
            raw_data = iter(records)
            cols = []
            
        schema_map = {col.oracle_name: col for col in table.columns}
        
        def cleaning_generator() -> Iterator[dict[str, Any]]:
            for row in raw_data:
                cleaned_row = {}
                for k, v in row.items():
                    col_schema = schema_map.get(to_oracle_snake(k))
                    if not col_schema:
                        raise KeyError(
                            f"Pipeline Definition Error: Field '{k}' was received from the source dataset, "
                            f"but it does not exist in your Table configuration for '{table.name}'."
                        )
                    cleaned_row[col_schema.oracle_name] = normalize_cell(
                        raw_type=str(col_schema.raw_type),
                        raw=v,
                        python_type=col_schema.python_type
                    )
                yield cleaned_row
                
        return Records(
            data=cleaning_generator(),
            columns=cols,
            code=200,
            message='ok'
        )

    def mutate_table(self, table: Table | OracleTable, source_system: System | None = None, **kwargs) -> Table:
        enforce_constraints: bool = (
            source_system is None or System(source_system) != System.salesforce
        )
        ora_table: OracleTable = to_oracle_table(table, enforce_constraints=enforce_constraints)
        ora_table.namespace = self.schema(table.namespace)
        
        if kwargs.get("action") == "full_load" and self.client.check_object_exists(
            ora_table.name, schema=str(ora_table.namespace), object_type="TABLE"
        ):
            logger.info(
                "full_load: dropping '%s' so it is recreated from scratch.",
                ora_table.qualified_name,
            )
            self.client.execute(f"DROP TABLE {ora_table.qualified_name} CASCADE CONSTRAINTS")
            self.client.commit()
            
        fetched = self.client.all_tab_columns(str(ora_table.namespace), ora_table.name)
        if not fetched:
            self.mutate_create_table(ora_table)
            ora_table.clear_caches()
            fetched = [
                {
                    "COLUMN_NAME": col.oracle_name,
                    "DATA_TYPE": col.raw_type,
                    "CHAR_USED": col.char_used or "C",
                    "NULLABLE": "Y" if col.is_nullable else "N",
                    "CHAR_LENGTH": col.char_length or col.max_length or 255,
                    "DATA_LENGTH": col.max_length or 255,
                    "DATA_PRECISION": col.precision,
                    "DATA_SCALE": col.scale
                }
                for col in ora_table.columns
            ]
            
        max_passes = 2 * len(ora_table.columns) + 2
        for _pass in range(max_passes):
            db_col_map: dict[str, dict[str, Any]] = {row["COLUMN_NAME"]: row for row in fetched}
            new_cols: list[OracleColumn] = []
            column_mutated = False
            
            for col in ora_table.columns:
                lookup_key = col.oracle_name
                row: dict[str, Any] | None = db_col_map.get(lookup_key)
                
                if not row:
                    col.is_new = True
                    new_cols.append(col)
                else:
                    db_raw_type = str(row.get("DATA_TYPE") or "").upper()
                    db_nullable = str(row.get("NULLABLE", "Y")) == "Y"
                    alter_clauses: list[str] = []
                    
                    if col.raw_type == "VARCHAR2" and db_raw_type == "VARCHAR2":
                        db_length = int(row.get("CHAR_LENGTH") or row.get("DATA_LENGTH") or 0)
                        if col.python_type == PythonTypes.boolean:
                            required = 1
                            grown = 1
                        else:
                            required = max(col.char_length or 0, col.max_length or 0)
                            grown = col.effective_max_varchar2
                        if required > db_length:
                            alter_clauses.append(f"MODIFY {col.oracle_name} VARCHAR2({grown} CHAR)")
                    elif col.raw_type and col.raw_type.upper() != _normalize_ora_type(db_raw_type):
                        definition_clause = col.column_definition()
                        type_part = definition_clause.replace(col.bind_name, "").replace("NOT NULL", "").replace("NULL", "").strip()
                        alter_clauses.append(f"MODIFY {col.oracle_name} {type_part}")
                        
                    if col.is_nullable != db_nullable:
                        if not col.is_nullable and not enforce_constraints:
                            pass
                        else:
                            null_toggle = "NULL" if col.is_nullable else "NOT NULL"
                            alter_clauses.append(f"MODIFY {col.oracle_name} {null_toggle}")
                            
                    if alter_clauses:
                        for clause in alter_clauses:
                            alter_sql = f"ALTER TABLE {ora_table.qualified_name} {clause}"
                            logger.info(f"Syncing schema drift on '{ora_table.qualified_name}': {alter_sql}")
                            self.client.execute(alter_sql)
                        column_mutated = True
                        
            if new_cols:
                self.mutate_add_columns(ora_table, new_cols)
                ora_table.clear_caches()
                fetched = self.client.all_tab_columns(str(ora_table.namespace), ora_table.name)
                continue
                
            if column_mutated:
                ora_table.clear_caches()
                fetched = self.client.all_tab_columns(str(ora_table.namespace), ora_table.name)
                column_mutated = False
                continue
                
            break
        else:
            raise RuntimeError(
                f"Schema drift sync for '{ora_table.qualified_name}' failed to converge "
                f"after {max_passes} passes; a MODIFY/ADD clause is not resolving."
            )
            
        described: Table[Any] = self.describe_table(
            OracleTable(name=ora_table.name, system=System.oracle, namespace=ora_table.namespace)
        )
        source_key = {c.oracle_name: c for c in ora_table.columns}
        for col in described.columns:
            src = source_key.get(col.oracle_name)
            if src is not None:
                col.is_primary_key = col.is_primary_key or src.is_primary_key
                col.is_unique = col.is_unique or src.is_unique
                
        return described

    def mutate_create_table(self, table: OracleTable) -> None:
        col_defs: list[Any] = []
        for col in table.columns:
            col.is_new = True
            col_defs.append(col.column_definition())
            
        sql: str = f"CREATE TABLE {table.qualified_name} ({', '.join(col_defs)})"
        logger.debug(f"Executing CREATE TABLE for table_name = {table.name} in {self.schema()}\nstatement = {sql}")
        self.client.execute(sql)
        self.client.commit()
        # Index the MERGE match column at creation so the first upsert probes
        # instead of full-scanning. No-op for tables with no PK metadata.
        self._ensure_match_index(table)

    def mutate_add_columns(self, table: OracleTable, new_columns: list[OracleColumn]) -> None:
        col_defs: str = ", ".join(col.column_definition() for col in new_columns)
        sql: str = f"ALTER TABLE {table.qualified_name} ADD ({col_defs})"
        with self.client.connect().cursor() as cur:
            cur.execute(sql)