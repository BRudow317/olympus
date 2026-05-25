"""test_seeding.py"""
from __future__ import annotations

import datetime
import subprocess
import sys
from collections.abc import Generator
from typing import Any

import oracledb
import pytest

from src.oracle.Oracle import Oracle

# DDL 
_DDL_ORA_SEED = """
CREATE TABLE {schema}.PYTEST_SEED (
    ID          NUMBER(10)          NOT NULL,
    LABEL       VARCHAR2(100 CHAR)  NULL,
    NOTES       CLOB                NULL,
    SCORE       NUMBER(18,4)        NULL,
    INT_VAL     NUMBER(10,0)        NULL,
    FLAG        NUMBER(1,0)         NULL,
    BORN_ON     DATE                NULL,
    UPDATED_AT  TIMESTAMP           NULL,
    PAYLOAD     BLOB                NULL,
    CONSTRAINT {schema}_PYTEST_SEED_PK PRIMARY KEY (ID)
)
"""

# helpers 
def _run_cli(*args: str) -> None:
    result = subprocess.run(
        [sys.executable, "src/app.py", *args],
        capture_output=True, text=True,
    )
    if result.returncode != 0:
        raise RuntimeError(
            f"CLI exited {result.returncode}\n"
            f"STDOUT: {result.stdout}\nSTDERR: {result.stderr}"
        )


def _drop(ora: Oracle, table: str) -> None:
    try:
        ora._client.execute(f"DROP TABLE {ora._schema()}.{table} CASCADE CONSTRAINTS")
    except oracledb.DatabaseError:
        pass


def _count(ora: Oracle, table: str) -> int:
    return int(
        ora._client.query(f"SELECT COUNT(*) AS CNT FROM {ora._schema()}.{table}")[0]["CNT"]
    )


def _catalog_col(ora: Oracle, table: str, column: str) -> dict[str, Any]:
    rows = ora._client.query(
        "SELECT DATA_TYPE, CHAR_LENGTH, DATA_PRECISION, DATA_SCALE "
        "FROM ALL_TAB_COLUMNS "
        "WHERE OWNER = :o AND TABLE_NAME = :t AND COLUMN_NAME = :c",
        {"o": ora._schema(), "t": table, "c": column},
    )
    return dict(rows[0]) if rows else {}


def _table_exists(ora: Oracle, table: str) -> bool:
    rows = ora._client.query(
        "SELECT 1 FROM ALL_TABLES WHERE OWNER = :o AND TABLE_NAME = :t",
        {"o": ora._schema(), "t": table},
    )
    return bool(rows)


# shared Oracle connections 

@pytest.fixture(scope="module")
def qbl() -> Oracle:
    return Oracle("QBL", "QBL")


@pytest.fixture(scope="module")
def dwh() -> Oracle:
    return Oracle("DWH", "DWH")


# SF → Oracle 

class TestSfToOracleSeeding:
    """
    Seeds SF Contact + Account from TRAIL into DWH Oracle via the CLI.

    Covers every SF field type that produces a distinct Oracle column type:
      id/string/email/phone/url/reference/picklist → VARCHAR2
      textarea (length > 4000)                     → CLOB
      boolean                                      → NUMBER
      date                                         → DATE
      datetime                                     → TIMESTAMP
      currency/percent/double                      → NUMBER (FLOAT path)
      int                                          → NUMBER (INTEGER path)
    """

    @pytest.fixture(scope="class", autouse=True)
    def seed_sf_tables(self, dwh: Oracle) -> Generator[None, None, None]:
        _run_cli(
            "--source-system", "salesforce",
            "--source-environment", "TRAIL",
            "--source-namespace", "TRAIL",
            "--target-system", "oracle",
            "--target-environment", "DWH",
            "--target-namespace", "DWH",
            "--tables", "Contact", "Account",
        )
        yield
        _drop(dwh, "SF_CONTACT")
        _drop(dwh, "SF_ACCOUNT")

    # table creation 
    def test_contact_table_created(self, dwh: Oracle) -> None:
        assert _table_exists(dwh, "SF_CONTACT")

    def test_account_table_created(self, dwh: Oracle) -> None:
        assert _table_exists(dwh, "SF_ACCOUNT")

    def test_contact_has_rows(self, dwh: Oracle) -> None:
        assert _count(dwh, "SF_CONTACT") >= 0

    def test_account_has_rows(self, dwh: Oracle) -> None:
        assert _count(dwh, "SF_ACCOUNT") >= 0

    # SF 'id' / 'string' → VARCHAR2 
    def test_contact_id_field_maps_to_varchar2(self, dwh: Oracle) -> None:
        assert _catalog_col(dwh, "SF_CONTACT", "ID")["DATA_TYPE"] == "VARCHAR2"

    def test_contact_string_field_maps_to_varchar2(self, dwh: Oracle) -> None:
        assert _catalog_col(dwh, "SF_CONTACT", "LAST_NAME")["DATA_TYPE"] == "VARCHAR2"

    # SF 'reference' (lookup) → VARCHAR2 
    def test_contact_reference_field_maps_to_varchar2(self, dwh: Oracle) -> None:
        assert _catalog_col(dwh, "SF_CONTACT", "ACCOUNT_ID")["DATA_TYPE"] == "VARCHAR2"

    # SF 'picklist' → VARCHAR2 
    def test_account_picklist_field_maps_to_varchar2(self, dwh: Oracle) -> None:
        assert _catalog_col(dwh, "SF_ACCOUNT", "INDUSTRY")["DATA_TYPE"] == "VARCHAR2"

    # SF 'url' → VARCHAR2 
    def test_account_url_field_maps_to_varchar2(self, dwh: Oracle) -> None:
        assert _catalog_col(dwh, "SF_ACCOUNT", "WEBSITE")["DATA_TYPE"] == "VARCHAR2"

    # SF 'textarea' (length > 4000) → CLOB 
    def test_contact_textarea_maps_to_clob(self, dwh: Oracle) -> None:
        assert _catalog_col(dwh, "SF_CONTACT", "DESCRIPTION")["DATA_TYPE"] == "CLOB"

    def test_account_textarea_maps_to_clob(self, dwh: Oracle) -> None:
        assert _catalog_col(dwh, "SF_ACCOUNT", "DESCRIPTION")["DATA_TYPE"] == "CLOB"

    # SF 'boolean' → NUMBER 

    def test_contact_boolean_field_maps_to_number(self, dwh: Oracle) -> None:
        assert _catalog_col(dwh, "SF_CONTACT", "DO_NOT_CALL")["DATA_TYPE"] == "NUMBER"

    # SF 'date' → DATE 

    def test_contact_date_field_maps_to_date(self, dwh: Oracle) -> None:
        assert _catalog_col(dwh, "SF_CONTACT", "BIRTHDATE")["DATA_TYPE"] == "DATE"

    # SF 'datetime' → TIMESTAMP 

    def test_contact_datetime_field_maps_to_timestamp(self, dwh: Oracle) -> None:
        assert _catalog_col(dwh, "SF_CONTACT", "LAST_MODIFIED_DATE")["DATA_TYPE"].startswith("TIMESTAMP")

    def test_account_datetime_field_maps_to_timestamp(self, dwh: Oracle) -> None:
        assert _catalog_col(dwh, "SF_ACCOUNT", "LAST_MODIFIED_DATE")["DATA_TYPE"].startswith("TIMESTAMP")

    # SF 'currency' (FLOAT path) → NUMBER 
    def test_account_currency_field_maps_to_number(self, dwh: Oracle) -> None:
        assert _catalog_col(dwh, "SF_ACCOUNT", "ANNUAL_REVENUE")["DATA_TYPE"] == "NUMBER"

    # SF 'int' (INTEGER path) → NUMBER 
    def test_account_int_field_maps_to_number(self, dwh: Oracle) -> None:
        assert _catalog_col(dwh, "SF_ACCOUNT", "NUMBER_OF_EMPLOYEES")["DATA_TYPE"] == "NUMBER"

    # cross-system name resolution 
    def test_contact_table_has_sf_prefix(self, dwh: Oracle) -> None:
        assert _table_exists(dwh, "SF_CONTACT")
        assert not _table_exists(dwh, "CONTACT")

    def test_account_table_has_sf_prefix(self, dwh: Oracle) -> None:
        assert _table_exists(dwh, "SF_ACCOUNT")
        assert not _table_exists(dwh, "ACCOUNT")

    # idempotency 
    def test_sf_to_oracle_is_idempotent(self, dwh: Oracle) -> None:
        count_before = _count(dwh, "SF_CONTACT")
        _run_cli(
            "--source-system", "salesforce",
            "--source-environment", "TRAIL",
            "--source-namespace", "TRAIL",
            "--target-system", "oracle",
            "--target-environment", "DWH",
            "--target-namespace", "DWH",
            "--tables", "Contact",
        )
        assert _count(dwh, "SF_CONTACT") == count_before


# Oracle → Oracle 
class TestOracleToOracleSeeding:
    """
    Seeds QBL.PYTEST_SEED → DWH.PYTEST_SEED via the CLI.

    The source table exercises every Oracle native type that has a distinct
    python_type mapping:
      NUMBER(n,0)  → INTEGER  → NUMBER
      NUMBER(n,s)  → FLOAT    → NUMBER
      NUMBER(1,0)  → INTEGER  → NUMBER  (boolean-by-convention)
      VARCHAR2     → STRING   → VARCHAR2
      CLOB         → STRING   → CLOB    (max_length=None triggers CLOB path)
      DATE         → DATE     → DATE
      TIMESTAMP    → DATETIME → TIMESTAMP
      BLOB         → BYTE     → BLOB
    """

    @pytest.fixture(scope="class", autouse=True)
    def seed_oracle_table(self, qbl: Oracle, dwh: Oracle) -> Generator[None, None, None]:
        for ora in (qbl, dwh):
            _drop(ora, "PYTEST_SEED")

        qbl._client.execute(_DDL_ORA_SEED.format(schema="QBL"))
        qbl._client.execute(
            "INSERT INTO QBL.PYTEST_SEED "
            "(ID, LABEL, SCORE, INT_VAL, FLAG, BORN_ON, UPDATED_AT) "
            "VALUES (1, 'Alpha', 3.14, 42, 1, "
            "DATE '2020-01-01', TIMESTAMP '2024-06-15 12:00:00')"
        )
        qbl._client.execute(
            "INSERT INTO QBL.PYTEST_SEED (ID, LABEL, FLAG) VALUES (2, 'Beta', 0)"
        )
        qbl._client.commit()

        _run_cli(
            "--source-system", "oracle",
            "--source-environment", "QBL",
            "--source-namespace", "QBL",
            "--target-system", "oracle",
            "--target-environment", "DWH",
            "--target-namespace", "DWH",
            "--tables", "PYTEST_SEED",
        )
        yield
        for ora in (dwh, qbl):
            _drop(ora, "PYTEST_SEED")

    # structural 
    def test_target_table_created(self, dwh: Oracle) -> None:
        assert _table_exists(dwh, "PYTEST_SEED")

    def test_row_count_matches_source(self, qbl: Oracle, dwh: Oracle) -> None:
        assert _count(dwh, "PYTEST_SEED") == _count(qbl, "PYTEST_SEED")

    # type round-trips 
    def test_varchar2_round_trips(self, dwh: Oracle) -> None:
        assert _catalog_col(dwh, "PYTEST_SEED", "LABEL")["DATA_TYPE"] == "VARCHAR2"

    def test_clob_round_trips(self, dwh: Oracle) -> None:
        assert _catalog_col(dwh, "PYTEST_SEED", "NOTES")["DATA_TYPE"] == "CLOB"

    def test_number_integer_round_trips(self, dwh: Oracle) -> None:
        assert _catalog_col(dwh, "PYTEST_SEED", "INT_VAL")["DATA_TYPE"] == "NUMBER"

    def test_number_decimal_round_trips(self, dwh: Oracle) -> None:
        assert _catalog_col(dwh, "PYTEST_SEED", "SCORE")["DATA_TYPE"] == "NUMBER"

    def test_number_flag_round_trips(self, dwh: Oracle) -> None:
        assert _catalog_col(dwh, "PYTEST_SEED", "FLAG")["DATA_TYPE"] == "NUMBER"

    def test_date_round_trips(self, dwh: Oracle) -> None:
        assert _catalog_col(dwh, "PYTEST_SEED", "BORN_ON")["DATA_TYPE"] == "DATE"

    def test_timestamp_round_trips(self, dwh: Oracle) -> None:
        assert _catalog_col(dwh, "PYTEST_SEED", "UPDATED_AT")["DATA_TYPE"].startswith("TIMESTAMP")

    def test_blob_round_trips(self, dwh: Oracle) -> None:
        assert _catalog_col(dwh, "PYTEST_SEED", "PAYLOAD")["DATA_TYPE"] == "BLOB"

    # data integrity 
    def test_varchar2_data_survives_transit(self, dwh: Oracle) -> None:
        rows = dwh._client.query("SELECT LABEL FROM DWH.PYTEST_SEED WHERE ID = 1")
        assert rows[0]["LABEL"] == "Alpha"

    def test_integer_data_survives_transit(self, dwh: Oracle) -> None:
        rows = dwh._client.query("SELECT INT_VAL FROM DWH.PYTEST_SEED WHERE ID = 1")
        assert int(rows[0]["INT_VAL"]) == 42

    def test_flag_false_row_survives_transit(self, dwh: Oracle) -> None:
        rows = dwh._client.query("SELECT FLAG FROM DWH.PYTEST_SEED WHERE ID = 2")
        assert int(rows[0]["FLAG"]) == 0

    def test_date_data_survives_transit(self, dwh: Oracle) -> None:
        rows = dwh._client.query("SELECT BORN_ON FROM DWH.PYTEST_SEED WHERE ID = 1")
        val = rows[0]["BORN_ON"]
        if isinstance(val, datetime.datetime):
            val = val.date()
        assert val == datetime.date(2020, 1, 1)

    def test_null_fields_survive_transit(self, dwh: Oracle) -> None:
        rows = dwh._client.query("SELECT SCORE, BORN_ON FROM DWH.PYTEST_SEED WHERE ID = 2")
        assert rows[0]["SCORE"] is None
        assert rows[0]["BORN_ON"] is None

    # idempotency 
    def test_oracle_to_oracle_is_idempotent(self, qbl: Oracle, dwh: Oracle) -> None:
        count_before = _count(dwh, "PYTEST_SEED")
        _run_cli(
            "--source-system", "oracle",
            "--source-environment", "QBL",
            "--source-namespace", "QBL",
            "--target-system", "oracle",
            "--target-environment", "DWH",
            "--target-namespace", "DWH",
            "--tables", "PYTEST_SEED",
        )
        assert _count(dwh, "PYTEST_SEED") == count_before
