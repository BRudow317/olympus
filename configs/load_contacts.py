"""load_contacts.py — load a contacts CSV into Oracle via OracleClient using polars.

Run through charon so .env, PYTHONPATH, and logging are configured:

    python charon.py --exec jobs/load_contacts.py
    python charon.py --exec jobs/load_contacts.py --env DWH --table CONTACTS \
        --csv "Q:/library/DemoData/data factory/customers-50k.csv"

The target table is (re)created to match the CSV, then rows are streamed into
Oracle with OracleClient.execute_many in batches. polars reads/types the CSV;
the OracleClient owns the connection, cursor, and batching.
"""
from __future__ import annotations

import argparse
import logging
from pathlib import Path

import polars as pl

from ..src.oracle.OracleClient import OracleClient
from ..src.oracle.OracleModels import to_oracle_snake

logger = logging.getLogger("load_contacts")

DEFAULT_CSV = r"Q:\library\DemoData\data factory\customers-50k.csv"
DEFAULT_ENV = "dev01"
DEFAULT_TABLE = "SF_CONTACTS"

# CSV header -> Oracle DDL type. Order defines the table's column order. The
# Oracle column name is derived from the header via to_oracle_snake (e.g.
# "Customer Id" -> CUSTOMER_ID, and the reserved word "Index" -> INDEX_COL).
CSV_SCHEMA: list[tuple[str, str]] = [
    ("Index",             "NUMBER(10)"),
    ("Customer Id",       "VARCHAR2(50 CHAR)"),
    ("First Name",        "VARCHAR2(100 CHAR)"),
    ("Last Name",         "VARCHAR2(100 CHAR)"),
    ("Company",           "VARCHAR2(200 CHAR)"),
    ("City",              "VARCHAR2(100 CHAR)"),
    ("Country",           "VARCHAR2(150 CHAR)"),
    ("Phone 1",           "VARCHAR2(50 CHAR)"),
    ("Phone 2",           "VARCHAR2(50 CHAR)"),
    ("Email",             "VARCHAR2(255 CHAR)"),
    ("Subscription Date", "DATE"),
    ("Website",           "VARCHAR2(255 CHAR)"),
]


def oracle_name(csv_header: str) -> str:
    return to_oracle_snake(csv_header)


def load_frame(csv_path: Path) -> pl.DataFrame:
    """Read the CSV as strings, cast typed columns, and rename to Oracle names."""
    if not csv_path.is_file():
        raise FileNotFoundError(f"CSV not found: {csv_path}")

    # infer_schema_length=0 reads every column as Utf8, so messy/quoted data never
    # trips type inference; we then cast only the columns that aren't text.
    df = pl.read_csv(csv_path, infer_schema_length=0)

    missing = [h for h, _ in CSV_SCHEMA if h not in df.columns]
    if missing:
        raise ValueError(f"CSV is missing expected columns: {missing}")

    casts: list[pl.Expr] = []
    for header, ddl in CSV_SCHEMA:
        if ddl.startswith("NUMBER"):
            casts.append(pl.col(header).cast(pl.Int64, strict=False))
        elif ddl == "DATE":
            casts.append(pl.col(header).str.strptime(pl.Date, "%Y-%m-%d", strict=False))

    df = df.with_columns(casts).select([h for h, _ in CSV_SCHEMA])
    return df.rename({h: oracle_name(h) for h, _ in CSV_SCHEMA})


def create_table(client: OracleClient, table: str) -> None:
    """Drop (if present) and recreate the target table to match CSV_SCHEMA."""
    try:
        client.execute(f"DROP TABLE {table} CASCADE CONSTRAINTS")
        logger.info("Dropped existing table %s", table)
    except Exception:
        pass  # table did not exist

    col_defs = ", ".join(f"{oracle_name(h)} {ddl}" for h, ddl in CSV_SCHEMA)
    client.execute(f"CREATE TABLE {table} ({col_defs})")
    logger.info("Created table %s", table)


def insert_frame(client: OracleClient, table: str, df: pl.DataFrame, batch_size: int) -> list:
    """Bulk-insert the frame; returns any per-row batch errors."""
    names = df.columns
    cols = ", ".join(names)
    binds = ", ".join(f":{n}" for n in names)
    sql = f"INSERT INTO {table} ({cols}) VALUES ({binds})"

    # iter_rows(named=True) streams dicts keyed by the Oracle column names, so the
    # whole 50k set is never duplicated into a second in-memory list.
    errors = client.execute_many(sql, df.iter_rows(named=True), batch_size=batch_size)
    client.commit()
    return errors


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(prog="load_contacts")
    p.add_argument("--csv", default=DEFAULT_CSV, help="Path to the contacts CSV.")
    p.add_argument("--env", default=DEFAULT_ENV, help="Oracle environment name (ORACLE_<ENV>_*).")
    p.add_argument("--table", default=DEFAULT_TABLE, help="Target Oracle table name.")
    p.add_argument("--batch-size", type=int, default=5000, help="Rows per executemany batch.")
    p.add_argument("--append", action="store_true", help="Insert into the existing table instead of recreating it.")
    return p.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)

    df = load_frame(Path(args.csv))
    logger.info("Read %d rows / %d columns from %s", df.height, df.width, args.csv)

    client = OracleClient.client_constructor(args.env)

    if not args.append:
        create_table(client, args.table)

    errors = insert_frame(client, args.table, df, args.batch_size)
    if errors:
        logger.error("Insert finished with %d row error(s); first: %s", len(errors), errors[0])

    logger.info("Done. %s now holds %d rows.", args.table, client.record_count(args.table))
    return 1 if errors else 0


if __name__ == "__main__":
    raise SystemExit(main())
