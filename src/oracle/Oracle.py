# src/oracle/Oracle.py
from __future__ import annotations
import os
import re
import logging

logger: logging.Logger = logging.getLogger(__name__)


def _normalize_ora_type(raw: str) -> str:
    """Canonicalize an Oracle catalog DATA_TYPE for drift comparison.

    Oracle reports temporal types with an inline precision (e.g. ``TIMESTAMP``
    comes back as ``TIMESTAMP(6)``), while our column metadata stores the bare
    type name. Strip the ``(<n>)`` precision so ``TIMESTAMP(6)`` compares equal
    to ``TIMESTAMP`` and ``MODIFY`` drift-sync actually converges.
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

from typing import Any
from itertools import islice
from collections.abc import Iterator
import oracledb

from src.models import DataSource, Schema, System, Records, Table, Column, PythonTypes
from src.oracle.OracleClient import OracleClient
from src.oracle.OracleTypeMap import to_oracle_table, oracle_to_python, normalize_cell #, python_to_oracle #, raw_type_to_oracledb_input_size
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
            self.client.user.upper() or
            ""
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
        
        connection: oracledb.Connection = self.client.connect()
        cursor: oracledb.Cursor = connection.cursor()
        
        try:
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
                self.client.execute(f"TRUNCATE TABLE {oracle_table.qualified_name} CASCADE")
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

        Salesforce's describe under-reports field lengths, so real data can
        exceed the column size we created. On ORA-12899 we grow the offending
        column (capped at Oracle's VARCHAR2 max) and re-apply the failed rows
        with the same statement (an upsert/merge when action='upsert'). Rows that
        succeeded in a pass are already applied and are not retried. Any other
        batch error, or a required width beyond VARCHAR2 max, is fatal.
        """
        pending: list[dict[str, Any]] = chunk
        # Bounded: every pass either widens at least one column or raises, and a
        # column only grows, so progress is monotonic.
        for _ in range(len(oracle_table.columns) + 2):
            cursor.executemany(statement, pending, batcherrors=True)
            errors: list[Any] = cursor.getbatcherrors()
            if not errors:
                return

            widen: dict[str, int] = {}
            retry_rows: list[dict[str, Any]] = []
            for err in errors:
                parsed = _parse_value_too_large(getattr(err, "message", str(err)))
                if getattr(err, "code", None) == 12899 and parsed is not None:
                    col_name, actual = parsed
                    widen[col_name] = max(widen.get(col_name, 0), actual)
                    retry_rows.append(pending[err.offset])
                else:
                    # Non-widenable error: surface every error in the batch.
                    raise RuntimeError(
                        "".join(f"\n{err}" for err in errors)
                    )

            self._widen_columns(connection, cursor, oracle_table, widen)
            pending = retry_rows

        raise RuntimeError(
            f"load_records: column widening for '{oracle_table.qualified_name}' "
            f"did not converge after {len(oracle_table.columns) + 2} passes."
        )

    def _widen_columns(
        self,
        connection: oracledb.Connection,
        cursor: oracledb.Cursor,
        oracle_table: OracleTable,
        widen: dict[str, int],
    ) -> None:
        """Grow VARCHAR2 columns to fit oversized data (capped at the Oracle max).

        Commits already-applied rows first: the ALTER is DDL (implicit commit)
        and needs the row locks released, so we make that commit explicit rather
        than leave it as an opaque side effect.
        """
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
                # Beyond VARCHAR2 capacity; do not widen — let it hard fail.
                raise RuntimeError(
                    f"Column '{oracle_table.qualified_name}.{col_name}' needs {required} chars, "
                    f"exceeding the VARCHAR2 maximum of {ORACLE_MAX_VARCHAR2_CHAR}."
                )

            new_size = min(required + varchar2_growth_buffer, ORACLE_MAX_VARCHAR2_CHAR)
            alter_sql = f"ALTER TABLE {oracle_table.qualified_name} MODIFY ({col_name} VARCHAR2({new_size} CHAR))"
            logger.info(
                "Widening '%s.%s' to VARCHAR2(%d CHAR) to fit oversized source data (actual %d).",
                oracle_table.qualified_name, col_name, new_size, required,
            )
            self.client.execute(alter_sql)
            col.char_length = new_size
            col.max_length = new_size

        # Refresh bind sizes so they match the widened columns.
        input_sizes = oracle_table.column_input_sizes()
        if input_sizes:
            cursor.setinputsizes(**input_sizes)
    def mutate_records(self, table: OracleTable, records: Records | list[dict[str, Any]]) -> Records:
        if isinstance(records, Records):
            raw_data = records.data
            cols = records.columns
        else:
            raw_data = iter(records)
            cols = []
            
        # Key by the canonical Oracle column name so source keys in any casing
        # convention (SF 'LastName', Oracle 'LAST_NAME') resolve correctly.
        schema_map = {col.oracle_name: col for col in table.columns}

        def cleaning_generator() -> Iterator[dict[str, Any]]:
            for row in raw_data:
                cleaned_row = {}
                for k, v in row.items():
                    col_schema = schema_map.get(to_oracle_snake(k))

                    # FAIL FAST: If the column is missing from your schema, raise an error immediately
                    if not col_schema:
                        raise KeyError(
                            f"Pipeline Definition Error: Field '{k}' was received from the source dataset, "
                            f"but it does not exist in your Table configuration for '{table.name}'."
                        )
                        
                    # 3. Clean and map using exclusively explicit, validated schema metadata
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

    def mutate_table(self, table: Table | OracleTable, source_system: System | None = None) -> Table:
        # Salesforce's describe nullability (and other constraint metadata) does
        # not reflect the real data, so constraints sourced from SF are recorded
        # but not enforced. Oracle->Oracle keeps hard enforcement.
        enforce_constraints: bool = (
            source_system is None or System(source_system) != System.salesforce
        )

        # 1. Cast the incoming table
        ora_table: OracleTable = to_oracle_table(table, enforce_constraints=enforce_constraints)
        ora_table.namespace = self.schema(table.namespace)
        
        # Pull current structural data straight from the database
        fetched = self.client.all_tab_columns(str(ora_table.namespace), ora_table.name)
        
        if not fetched:
            # Table is missing, compile and execute creation scripts
            self.mutate_create_table(ora_table)
            ora_table.clear_caches()

            # fetch the column data
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
            
        # One pass per ADD round-trip plus one per MODIFY round-trip is the most
        # progress a well-formed schema can need; cap iterations so a clause that
        # never converges raises instead of looping forever.
        max_passes = 2 * len(ora_table.columns) + 2
        for _pass in range(max_passes):
            db_col_map: dict[str, dict[str, Any]] = {row["COLUMN_NAME"]: row for row in fetched}
            new_cols: list[OracleColumn] = []
            column_mutated = False
            
            for col in ora_table.columns:
                lookup_key = col.oracle_name
                row: dict[str, Any] | None = db_col_map.get(lookup_key)
                
                if not row:
                    # Column exists in codebase schema but is missing from Oracle DB; flag for ALTER ADD
                    col.is_new = True
                    new_cols.append(col)
                else:
                    # --- RESTORED COLUMN SYNC & ALTERATION MAPPING LOGIC ---
                    db_raw_type = str(row.get("DATA_TYPE") or "").upper()
                    db_nullable = str(row.get("NULLABLE", "Y")) == "Y"
                    
                    # Track structural target changes (Type adjustments or NULL constraints)
                    alter_clauses: list[str] = []
                    
                    # A. Evaluate Datatype Mismatches (e.g. promoting VARCHAR2 sizes dynamically)
                    if col.raw_type == "VARCHAR2" and db_raw_type == "VARCHAR2":
                        db_length = int(row.get("CHAR_LENGTH") or row.get("DATA_LENGTH") or 0)
                        # Booleans are fixed at VARCHAR2(1 CHAR) ('Y'/'N'); mirror the
                        # special-case in OracleColumn.column_definition() so the growth
                        # buffer in effective_max_varchar2 doesn't widen them.
                        target_length = 1 if col.python_type == PythonTypes.boolean else col.effective_max_varchar2
                        if target_length > db_length:
                            alter_clauses.append(f"MODIFY {col.oracle_name} VARCHAR2({target_length} CHAR)")
                    elif col.raw_type and col.raw_type.upper() != _normalize_ora_type(db_raw_type):
                        # Direct Datatype modification clause (safeguarded for compatible conversions like NUMBER sizes)
                        definition_clause = col.column_definition()
                        # Extract type part only out of 'COLUMN_NAME TYPE NULL' structure
                        type_part = definition_clause.replace(col.bind_name, "").replace("NOT NULL", "").replace("NULL", "").strip()
                        alter_clauses.append(f"MODIFY {col.oracle_name} {type_part}")
                        
                    # B. Evaluate Nullability Alterations
                    if col.is_nullable != db_nullable:
                        if not col.is_nullable and not enforce_constraints:
                            # Soft (DISABLE NOVALIDATE) NOT NULL constraints are
                            # recorded at create time but report NULLABLE='Y' in the
                            # catalog, so this mismatch is expected. Don't re-tighten:
                            # it would loop and reject legitimately-null source data.
                            pass
                        else:
                            null_toggle = "NULL" if col.is_nullable else "NOT NULL"
                            alter_clauses.append(f"MODIFY {col.oracle_name} {null_toggle}")
                        
                    # C. Execute ALTER MODIFY statements immediately if structural drift is discovered
                    if alter_clauses:
                        for clause in alter_clauses:
                            alter_sql = f"ALTER TABLE {ora_table.qualified_name} {clause}"
                            logger.info(f"Syncing schema drift on '{ora_table.qualified_name}': {alter_sql}")
                            self.client.execute(alter_sql)
                        column_mutated = True
                        
            # D. Handle Column Appends (ALTER TABLE ADD)
            if new_cols:
                self.mutate_add_columns(ora_table, new_cols)
                ora_table.clear_caches()
                # Re-fetch catalog mappings from database and re-evaluate changes loop
                fetched = self.client.all_tab_columns(str(ora_table.namespace), ora_table.name)
                continue
                
            # E. If alterations occurred, clear cache maps and verify alignment state
            if column_mutated:
                ora_table.clear_caches()
                fetched = self.client.all_tab_columns(str(ora_table.namespace), ora_table.name)
                column_mutated = False
                continue

            break
        else:
            # Loop ran the full budget without reaching a stable (no-drift) state.
            raise RuntimeError(
                f"Schema drift sync for '{ora_table.qualified_name}' failed to converge "
                f"after {max_passes} passes; a MODIFY/ADD clause is not resolving."
            )

        # 3. Maintain Abstract Contract: Pass clean identity profile to describe_table for generic Table exit
        described: Table[Any] = self.describe_table(
            OracleTable(name=ora_table.name, system=System.oracle, namespace=ora_table.namespace)
        )

        # The catalog carries no PK/unique constraints (we create columns only),
        # so describe_table can't recover the logical key. Carry it over from the
        # source-derived schema by column name, otherwise merge_sql() has no
        # ON-clause keys and upserts fail with ORA-00936.
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
            # col.oracle_name = to_oracle_snake(col.name)
            col.is_new = True
            col_defs.append(col.column_definition())
            
        sql: str = f"CREATE TABLE {table.qualified_name} ({', '.join(col_defs)})"
        logger.debug(f"Executing CREATE TABLE for table_name = {table.name} in {self.schema()}\nstatement = {sql}")
        self.client.execute(sql)
        self.client.commit()

    def mutate_add_columns(self, table: OracleTable, new_columns: list[OracleColumn]) -> None:
        col_defs: str = ", ".join(col.column_definition() for col in new_columns)
        sql: str = f"ALTER TABLE {table.qualified_name} ADD ({col_defs})"
        
        with self.client.connect().cursor() as cur:
            cur.execute(sql)