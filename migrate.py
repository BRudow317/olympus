"""
Salesforce -> Oracle schema migration.

Describes each requested SObject, syncs the Oracle schema (CREATE TABLE or
ALTER TABLE ADD missing columns), then streams all records and bulk-loads
them via oracledb named binds.

Usage:
    python boot.py --config ../.secrets/.env --exec python migrate.py Account Contact Lead
    python boot.py --config ../.secrets/.env --exec python migrate.py --schema MYSCHEMA Account

Environment variables (loaded by boot.py or set directly):
    SF_BASE_URL         Salesforce instance URL  (e.g. https://myorg.my.salesforce.com)
    SF_CONSUMER_KEY     Connected App consumer key
    SF_CONSUMER_SECRET  Connected App consumer secret
    ORACLE_USER         Oracle username
    ORACLE_PASS         Oracle password
    ORACLE_HOST         Oracle host
    ORACLE_PORT         Oracle port (default 1521)
    ORACLE_SERVICE      Oracle service name
    ORACLE_SCHEMA       Target schema (default: ORACLE_USER)
    BATCH_SIZE          Rows per executemany call (default 1000)
    BULK_THRESHOLD      Record count above which Bulk 2.0 is used (default 50000)
    VARCHAR2_GROWTH     Buffer added to SF field length when sizing VARCHAR2 (default 50)
"""
from __future__ import annotations

import argparse
import csv
import io
import logging
import os
import re
from typing import Any

import oracledb

from sf.engines.SfClient import SfClient
from sf.engines.SfRestEngine import SfRest
from sf.engines.SfBulk2Engine import Bulk2
from sf.models.SfTypeMap import SF_TYPE_MAP, cast_record
from oracle.ora import OracleClient

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

_BATCH_SIZE     = int(os.getenv("BATCH_SIZE", "1000"))
_BULK_THRESHOLD = int(os.getenv("BULK_THRESHOLD", "50000"))
_VARCHAR2_GROWTH = int(os.getenv("VARCHAR2_GROWTH", "50"))
_MAX_VARCHAR2   = 4000

# ---------------------------------------------------------------------------
# Identifier sanitizer
# ---------------------------------------------------------------------------

_INVALID = re.compile(r"[^A-Z0-9_]")
_RESERVED: frozenset[str] = frozenset({
    "SELECT", "FROM", "WHERE", "AND", "OR", "NOT", "IN", "IS", "AS",
    "HAVING", "ORDER", "GROUP", "BY", "DISTINCT", "ALL", "ANY",
    "UNION", "INTERSECT", "MINUS", "EXCEPT",
    "CREATE", "ALTER", "DROP", "TRUNCATE", "RENAME", "TABLE", "VIEW",
    "INDEX", "SYNONYM", "SEQUENCE", "TRIGGER", "PROCEDURE", "FUNCTION",
    "INSERT", "INTO", "VALUES", "UPDATE", "SET", "DELETE", "MERGE",
    "JOIN", "INNER", "LEFT", "RIGHT", "OUTER", "FULL", "CROSS", "ON",
    "EXISTS", "WITH", "COMMIT", "ROLLBACK", "GRANT", "REVOKE",
    "NULL", "TRUE", "FALSE", "ROWID", "ROWNUM", "LEVEL",
})


def _sanitize(name: str, max_len: int = 30) -> str:
    upper = name.upper()
    clean = _INVALID.sub("_", upper)
    if not clean or not clean[0].isalpha():
        clean = "C_" + clean
    if clean in _RESERVED:
        clean = clean + "_COL"
    return clean[:max_len]

# ---------------------------------------------------------------------------
# SF field -> Oracle DDL / oracledb input size
# ---------------------------------------------------------------------------

def _field_ddl(field: dict[str, Any]) -> str:
    sf_type = field.get("type", "string")
    length  = field.get("length") or 255
    prec    = field.get("precision") or 18
    scale   = field.get("scale") or 0

    if sf_type in ("id", "reference"):
        return "VARCHAR2(18 CHAR)"
    if sf_type in ("string", "phone", "email", "url", "encryptedstring",
                   "combobox", "anyType", "picklist"):
        size = min(max(length + _VARCHAR2_GROWTH, 1), _MAX_VARCHAR2)
        return f"VARCHAR2({size} CHAR)"
    if sf_type in ("textarea", "multipicklist"):
        return f"VARCHAR2({_MAX_VARCHAR2} CHAR)"
    if sf_type == "int":
        return "NUMBER(10)"
    if sf_type in ("double", "percent"):
        return "NUMBER"
    if sf_type == "currency":
        return f"NUMBER({prec}, {scale})" if prec and scale else "NUMBER(18, 2)"
    if sf_type == "boolean":
        return "NUMBER(1)"
    if sf_type == "date":
        return "DATE"
    if sf_type == "datetime":
        return "TIMESTAMP WITH TIME ZONE"
    if sf_type == "time":
        return "VARCHAR2(20 CHAR)"
    if sf_type == "base64":
        return "BLOB"
    return "VARCHAR2(255 CHAR)"


def _field_input_size(field: dict[str, Any]) -> Any:
    sf_type = field.get("type", "string")
    if sf_type in ("id", "reference"):
        return 18
    if sf_type in ("string", "textarea", "phone", "email", "url",
                   "encryptedstring", "combobox", "anyType",
                   "picklist", "multipicklist"):
        return oracledb.DB_TYPE_VARCHAR
    if sf_type in ("int", "double", "currency", "percent", "boolean"):
        return oracledb.DB_TYPE_NUMBER
    if sf_type == "date":
        return oracledb.DB_TYPE_DATE
    if sf_type == "datetime":
        return oracledb.DB_TYPE_TIMESTAMP_TZ
    if sf_type == "base64":
        return oracledb.DB_TYPE_BLOB
    return None

# ---------------------------------------------------------------------------
# Oracle schema management
# ---------------------------------------------------------------------------

def _fetch_oracle_cols(ora: OracleClient, schema: str, table: str) -> set[str]:
    """Return the set of existing column names (upper-case) for schema.table."""
    existing: set[str] = set()
    with ora.cursor() as cur:
        cur.execute(
            "SELECT COLUMN_NAME FROM ALL_TAB_COLUMNS "
            "WHERE OWNER = :owner AND TABLE_NAME = :tbl",
            {"owner": schema.upper(), "tbl": table.upper()},
        )
        for row in cur:
            existing.add(str(row[0]).upper())
    return existing


def _sync_schema(
    ora: OracleClient,
    schema: str,
    table: str,
    fields: list[dict[str, Any]],
) -> None:
    """CREATE TABLE if new; ALTER TABLE ADD any columns missing from Oracle."""
    col_specs = [
        (_sanitize(f["name"]), _field_ddl(f), f.get("nillable", True))
        for f in fields
    ]
    existing = _fetch_oracle_cols(ora, schema, table)

    if not existing:
        parts = [
            f"{name} {ddl} {'NULL' if null else 'NOT NULL'}"
            for name, ddl, null in col_specs
        ]
        sql = f"CREATE TABLE {schema}.{table} ({', '.join(parts)})"
        logger.info("CREATE TABLE %s.%s", schema, table)
        ora.execute_ddl(sql)
        ora.commit()
        return

    missing = [
        (name, ddl, null)
        for name, ddl, null in col_specs
        if name not in existing
    ]
    if missing:
        parts = [
            f"{name} {ddl} {'NULL' if null else 'NOT NULL'}"
            for name, ddl, null in missing
        ]
        sql = f"ALTER TABLE {schema}.{table} ADD ({', '.join(parts)})"
        logger.info("ALTER TABLE %s.%s ADD %d column(s)", schema, table, len(missing))
        ora.execute_ddl(sql)
        ora.commit()

# ---------------------------------------------------------------------------
# Data pipeline helpers
# ---------------------------------------------------------------------------

def _csv_bytes_to_records(
    csv_bytes: bytes,
    field_types: dict[str, str],
    oracle_map: dict[str, str],
) -> list[dict[str, Any]]:
    """Parse raw Bulk 2.0 CSV bytes into a list of oracle-keyed dicts."""
    text = csv_bytes.decode("utf-8-sig").replace("\x00", "")
    reader = csv.DictReader(io.StringIO(text))
    out: list[dict[str, Any]] = []
    for row in reader:
        row.pop("attributes", None)
        typed = cast_record(
            {k: v for k, v in row.items() if k in field_types},
            field_types,
        )
        out.append({oracle_map[k]: v for k, v in typed.items() if k in oracle_map})
    return out


def _flush_batch(
    cursor: Any,
    sql: str,
    input_sizes: dict[str, Any],
    batch: list[dict[str, Any]],
) -> int:
    """executemany a batch; return error count."""
    sized = {k: v for k, v in input_sizes.items() if v is not None}
    if sized:
        cursor.setinputsizes(**sized)
    cursor.executemany(sql, batch, batcherrors=True)
    errors = cursor.getbatcherrors()
    for e in errors:
        logger.warning("Batch error at offset %d: %s", e.offset, e.message)
    return len(errors)

# ---------------------------------------------------------------------------
# Per-object migration
# ---------------------------------------------------------------------------

def migrate_object(
    rest: SfRest,
    bulk2: Bulk2,
    ora: OracleClient,
    schema: str,
    sobject: str,
) -> None:
    logger.info("=== %s: describe ===", sobject)
    describe = getattr(rest, sobject).describe()

    # Keep only fields with a known Python type mapping; skip compound types
    fields = [
        f for f in describe.get("fields", [])
        if f.get("type") not in ("address", "location")
        and f.get("type") in SF_TYPE_MAP
    ]
    if not fields:
        logger.warning("%s: no mappable fields — skipping", sobject)
        return

    field_types  = {f["name"]: f["type"] for f in fields}
    oracle_map   = {f["name"]: _sanitize(f["name"]) for f in fields}
    oracle_names = [oracle_map[f["name"]] for f in fields]
    insert_sql   = (
        f"INSERT INTO {schema}.{sobject} ({', '.join(oracle_names)}) "
        f"VALUES ({', '.join(':' + n for n in oracle_names)})"
    )
    input_sizes  = {oracle_map[f["name"]]: _field_input_size(f) for f in fields}
    soql         = f"SELECT {', '.join(f['name'] for f in fields)} FROM {sobject}"

    # Sync Oracle schema
    _sync_schema(ora, schema, sobject, fields)

    # Count to choose REST vs Bulk 2.0
    count = rest.query(f"SELECT COUNT() FROM {sobject}").get("totalSize", 0)
    logger.info("%s: %d record(s) — using %s", sobject, count,
                "Bulk 2.0" if count > _BULK_THRESHOLD else "REST")

    total_errors = 0

    if count > _BULK_THRESHOLD:
        with ora.cursor() as cur:
            for csv_bytes in getattr(bulk2, sobject).query(soql):
                batch = _csv_bytes_to_records(csv_bytes, field_types, oracle_map)
                if batch:
                    total_errors += _flush_batch(cur, insert_sql, input_sizes, batch)
        ora.commit()

    else:
        batch: list[dict[str, Any]] = []
        with ora.cursor() as cur:
            for record in rest.query_all_iter(soql):
                record.pop("attributes", None)
                typed = cast_record(record, field_types)
                batch.append({oracle_map[k]: v for k, v in typed.items() if k in oracle_map})
                if len(batch) >= _BATCH_SIZE:
                    total_errors += _flush_batch(cur, insert_sql, input_sizes, batch)
                    batch = []
            if batch:
                total_errors += _flush_batch(cur, insert_sql, input_sizes, batch)
        ora.commit()

    logger.info("=== %s: complete — %d batch error(s) ===", sobject, total_errors)

# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Migrate Salesforce SObjects to Oracle"
    )
    parser.add_argument("sobjects", nargs="+", help="SObject API names (e.g. Account Contact)")
    parser.add_argument(
        "--schema",
        default=os.getenv("ORACLE_SCHEMA", ""),
        help="Target Oracle schema (default: ORACLE_USER env var)",
    )
    args = parser.parse_args()

    sf = SfClient(
        base_url=os.getenv("SF_BASE_URL"),
        consumer_key=os.getenv("SF_CONSUMER_KEY"),
        consumer_secret=os.getenv("SF_CONSUMER_SECRET"),
    )
    rest  = SfRest(sf)
    bulk2 = Bulk2(sf)

    ora = OracleClient(
        oracle_user=os.getenv("ORACLE_USER", ""),
        oracle_pass=os.getenv("ORACLE_PASS", ""),
        oracle_host=os.getenv("ORACLE_HOST", ""),
        oracle_port=os.getenv("ORACLE_PORT", "1521"),
        oracle_service=os.getenv("ORACLE_SERVICE", ""),
    )

    schema = (args.schema or ora._oracle_user).upper()
    logger.info("Target schema: %s", schema)

    for sobject in args.sobjects:
        try:
            migrate_object(rest, bulk2, ora, schema, sobject)
        except Exception:
            logger.exception("Failed to migrate %s — continuing", sobject)

    sf.close()
    ora.close()


if __name__ == "__main__":
    main()
