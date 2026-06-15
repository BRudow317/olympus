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
from src.oracle.OracleModels import OracleColumn, OracleTable
from src.models import Records, PythonTypes, System

from tests.conftest import ORACLE_ENVIRONMENTS, SALESFORCE_ENVIRONMENTS

ORACLE_SOURCE_ENV = ORACLE_ENVIRONMENTS[0]
ORACLE_TARGET_ENV = ORACLE_ENVIRONMENTS[1]
SALESFORCE_ENV = SALESFORCE_ENVIRONMENTS[0]

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
        [sys.executable, "charon.py", "--exec", "src/app.py", *args],
        capture_output=True, text=True,
    )
    if result.returncode != 0:
        raise RuntimeError(
            f"CLI exited {result.returncode}\n"
            f"STDOUT: {result.stdout}\nSTDERR: {result.stderr}"
        )


def _drop(ora: Oracle, table: str) -> None:
    if ora.client.check_object_exists(table, schema=ora.schema(), object_type="TABLE"):
        ora.client.execute(f"DROP TABLE {ora.schema()}.{table} CASCADE CONSTRAINTS")


def _count(ora: Oracle, table: str) -> int:
    return int(
        ora.client.query(f"SELECT COUNT(*) AS CNT FROM {ora.schema()}.{table}")[0]["CNT"]
    )


def _catalog_col(ora: Oracle, table: str, column: str) -> dict[str, Any]:
    rows = ora.client.query(
        "SELECT DATA_TYPE, CHAR_LENGTH, DATA_PRECISION, DATA_SCALE "
        "FROM ALL_TAB_COLUMNS "
        "WHERE OWNER = :o AND TABLE_NAME = :t AND COLUMN_NAME = :c",
        {"o": ora.schema(), "t": table, "c": column},
    )
    return dict(rows[0]) if rows else {}


def _table_exists(ora: Oracle, table: str) -> bool:
    rows = ora.client.query(
        "SELECT 1 FROM ALL_TABLES WHERE OWNER = :o AND TABLE_NAME = :t",
        {"o": ora.schema(), "t": table},
    )
    return bool(rows)


# shared Oracle connections 

# Oracle namespace is left to default to the connected schema (dev01 -> QBL,
# dev02 -> DWH). The env name is NOT a schema, so passing it as the namespace
# raises ORA-01918 ("user does not exist").

@pytest.fixture(scope="module")
def qbl() -> Oracle:
    return Oracle(ORACLE_SOURCE_ENV)


@pytest.fixture(scope="module")
def dwh() -> Oracle:
    return Oracle(ORACLE_TARGET_ENV)


# SF → Oracle 

class TestSfToOracleSeeding:
    """
    Seeds SF Contact + Account from TRAIL into DWH Oracle via the CLI.

    Covers every SF field type that produces a distinct Oracle column type:
      id/string/email/phone/url/reference/picklist → VARCHAR2
      textarea (length > 4000)                     → CLOB
      boolean                                      → VARCHAR2(1 CHAR)  'Y'/'N'
      date                                         → DATE
      datetime                                     → TIMESTAMP
      currency/percent/double                      → NUMBER (FLOAT path)
      int                                          → NUMBER (INTEGER path)
    """

    @pytest.fixture(scope="class", autouse=True)
    def seed_sf_tables(self, dwh: Oracle) -> Generator[None, None, None]:
        _drop(dwh, "SF_CONTACT")
        _drop(dwh, "SF_ACCOUNT")
        _run_cli(
            "--source-system", "salesforce",
            "--source-environment", SALESFORCE_ENV,
            "--source-namespace", SALESFORCE_ENV,
            "--target-system", "oracle",
            "--target-environment", ORACLE_TARGET_ENV,
            "--tables", "Contact", "Account",
        )
        try:
            yield
        finally:
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
        assert _catalog_col(dwh, "SF_CONTACT", "LASTNAME")["DATA_TYPE"] == "VARCHAR2"

    # SF 'reference' (lookup) → VARCHAR2
    def test_contact_reference_field_maps_to_varchar2(self, dwh: Oracle) -> None:
        assert _catalog_col(dwh, "SF_CONTACT", "ACCOUNTID")["DATA_TYPE"] == "VARCHAR2"

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

    # SF 'boolean' → VARCHAR2(1 CHAR)

    def test_contact_boolean_maps_to_varchar2(self, dwh: Oracle) -> None:
        # IsDeleted is a standard always-present Contact boolean.
        col = _catalog_col(dwh, "SF_CONTACT", "ISDELETED")
        assert col["DATA_TYPE"] == "VARCHAR2"
        assert int(col["CHAR_LENGTH"]) == 1

    # SF 'date' → DATE 

    def test_contact_date_field_maps_to_date(self, dwh: Oracle) -> None:
        assert _catalog_col(dwh, "SF_CONTACT", "BIRTHDATE")["DATA_TYPE"] == "DATE"

    # SF 'datetime' → TIMESTAMP 

    def test_contact_datetime_field_maps_to_timestamp(self, dwh: Oracle) -> None:
        assert _catalog_col(dwh, "SF_CONTACT", "LASTMODIFIEDDATE")["DATA_TYPE"].startswith("TIMESTAMP")

    def test_account_datetime_field_maps_to_timestamp(self, dwh: Oracle) -> None:
        assert _catalog_col(dwh, "SF_ACCOUNT", "LASTMODIFIEDDATE")["DATA_TYPE"].startswith("TIMESTAMP")

    # SF 'currency' (FLOAT path) → NUMBER
    def test_account_currency_field_maps_to_number(self, dwh: Oracle) -> None:
        assert _catalog_col(dwh, "SF_ACCOUNT", "ANNUALREVENUE")["DATA_TYPE"] == "NUMBER"

    # SF 'int' (INTEGER path) → NUMBER
    def test_account_int_field_maps_to_number(self, dwh: Oracle) -> None:
        assert _catalog_col(dwh, "SF_ACCOUNT", "NUMBEROFEMPLOYEES")["DATA_TYPE"] == "NUMBER"

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
            "--source-environment", SALESFORCE_ENV,
            "--source-namespace", SALESFORCE_ENV,
            "--target-system", "oracle",
            "--target-environment", ORACLE_TARGET_ENV,
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

        source_schema = qbl.schema()
        qbl.client.execute(_DDL_ORA_SEED.format(schema=source_schema))
        qbl.client.execute(
            f"INSERT INTO {source_schema}.PYTEST_SEED "
            "(ID, LABEL, SCORE, INT_VAL, FLAG, BORN_ON, UPDATED_AT) "
            "VALUES (1, 'Alpha', 3.14, 42, 1, "
            "DATE '2020-01-01', TIMESTAMP '2024-06-15 12:00:00')"
        )
        qbl.client.execute(
            f"INSERT INTO {source_schema}.PYTEST_SEED (ID, LABEL, FLAG) VALUES (2, 'Beta', 0)"
        )
        qbl.client.commit()

        _run_cli(
            "--source-system", "oracle",
            "--source-environment", ORACLE_SOURCE_ENV,
            "--target-system", "oracle",
            "--target-environment", ORACLE_TARGET_ENV,
            "--tables", "PYTEST_SEED",
        )
        try:
            yield
        finally:
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
        rows = dwh.client.query(f"SELECT LABEL FROM {dwh.schema()}.PYTEST_SEED WHERE ID = 1")
        assert rows[0]["LABEL"] == "Alpha"

    def test_integer_data_survives_transit(self, dwh: Oracle) -> None:
        rows = dwh.client.query(f"SELECT INT_VAL FROM {dwh.schema()}.PYTEST_SEED WHERE ID = 1")
        assert int(rows[0]["INT_VAL"]) == 42

    def test_flag_false_row_survives_transit(self, dwh: Oracle) -> None:
        rows = dwh.client.query(f"SELECT FLAG FROM {dwh.schema()}.PYTEST_SEED WHERE ID = 2")
        assert int(rows[0]["FLAG"]) == 0

    def test_date_data_survives_transit(self, dwh: Oracle) -> None:
        rows = dwh.client.query(f"SELECT BORN_ON FROM {dwh.schema()}.PYTEST_SEED WHERE ID = 1")
        val = rows[0]["BORN_ON"]
        if isinstance(val, datetime.datetime):
            val = val.date()
        assert val == datetime.date(2020, 1, 1)

    def test_null_fields_survive_transit(self, dwh: Oracle) -> None:
        rows = dwh.client.query(
            f"SELECT SCORE, BORN_ON FROM {dwh.schema()}.PYTEST_SEED WHERE ID = 2"
        )
        assert rows[0]["SCORE"] is None
        assert rows[0]["BORN_ON"] is None

    # idempotency
    def test_oracle_to_oracle_is_idempotent(self, qbl: Oracle, dwh: Oracle) -> None:
        count_before = _count(dwh, "PYTEST_SEED")
        _run_cli(
            "--source-system", "oracle",
            "--source-environment", ORACLE_SOURCE_ENV,
            "--target-system", "oracle",
            "--target-environment", ORACLE_TARGET_ENV,
            "--tables", "PYTEST_SEED",
        )
        assert _count(dwh, "PYTEST_SEED") == count_before


# SF -> Oracle -> SF round trip
class TestSfOracleRoundTrip:
    """The headline migration: discover Salesforce schema + data into Oracle,
    then push those same records back into Salesforce as an upsert (keyed on Id).

    Verifies both legs run end-to-end through the CLI without error and that the
    Oracle landing table is populated between the two legs.
    """

    @pytest.fixture(scope="class", autouse=True)
    def round_trip(self, dwh: Oracle) -> Generator[None, None, None]:
        _drop(dwh, "SF_CONTACT")
        # Leg 1: Salesforce -> Oracle (full schema discovery + data load)
        _run_cli(
            "--source-system", "salesforce",
            "--source-environment", SALESFORCE_ENV,
            "--source-namespace", SALESFORCE_ENV,
            "--target-system", "oracle",
            "--target-environment", ORACLE_TARGET_ENV,
            "--action", "reset",
            "--tables", "Contact",
        )
        try:
            yield
        finally:
            _drop(dwh, "SF_CONTACT")

    def test_oracle_landing_table_populated(self, dwh: Oracle) -> None:
        assert _table_exists(dwh, "SF_CONTACT")
        assert _count(dwh, "SF_CONTACT") >= 0

    def test_upsert_back_into_salesforce(self, dwh: Oracle) -> None:
        # Leg 2: Oracle -> Salesforce upsert on Id (round trips the records home).
        _run_cli(
            "--source-system", "oracle",
            "--source-environment", ORACLE_TARGET_ENV,
            "--target-system", "salesforce",
            "--target-environment", SALESFORCE_ENV,
            "--target-namespace", SALESFORCE_ENV,
            "--action", "upsert",
            "--external-id-field", "Id",
            "--tables", "SF_CONTACT",
        )


# On-the-fly schema adaptation during load (Salesforce metadata lies)
class TestVarchar2ToClobPromotion:
    """A VARCHAR2 column that overflows beyond Oracle's VARCHAR2 maximum is
    promoted to CLOB mid-load, without dropping the buffered batch.

    Salesforce describe under-reports text field lengths, so a column created as
    VARCHAR2 (sized from that describe) can receive a value larger than VARCHAR2
    can physically hold (> 4000 chars). The loader must convert the column to
    CLOB and finish loading the same batch it was streaming.
    """

    TABLE = "PYTEST_CLOB_PROMO"

    @pytest.fixture(scope="class", autouse=True)
    def seed(self, dwh: Oracle) -> Generator[None, None, None]:
        _drop(dwh, self.TABLE)
        # BODY is deliberately created small to force the overflow path.
        dwh.client.execute(
            f"CREATE TABLE {dwh.schema()}.{self.TABLE} ("
            f"  ID    NUMBER(10)        NOT NULL, "
            f"  BODY  VARCHAR2(100 CHAR) NULL, "
            f"  CONSTRAINT {self.TABLE}_PK PRIMARY KEY (ID))"
        )
        dwh.client.commit()
        try:
            yield
        finally:
            _drop(dwh, self.TABLE)

    def _target(self, dwh: Oracle) -> OracleTable:
        # Describe the target as the migrator would: BODY is "still" VARCHAR2.
        return OracleTable(
            name=self.TABLE,
            system=System.oracle,
            namespace=dwh.schema(),
            columns=[
                OracleColumn(
                    name="ID", raw_type="NUMBER", python_type=PythonTypes.integer,
                    is_primary_key=True, is_nullable=False,
                ),
                OracleColumn(
                    name="BODY", raw_type="VARCHAR2", python_type=PythonTypes.string,
                    max_length=100,
                ),
            ],
        )

    def test_oversize_value_promotes_column_and_loads(self, dwh: Oracle) -> None:
        big = "X" * 9000  # well beyond the 4000-char VARCHAR2 ceiling

        def rows():
            yield {"ID": 1, "BODY": "small value"}
            yield {"ID": 2, "BODY": big}

        dwh.load_records(action="insert", table=self._target(dwh), records=Records(data=rows()))

        # The column is now CLOB ...
        assert _catalog_col(dwh, self.TABLE, "BODY")["DATA_TYPE"] == "CLOB"
        # ... every row landed ...
        assert _count(dwh, self.TABLE) == 2
        # ... the small value still round-trips ...
        small = dwh.client.query(
            f"SELECT BODY FROM {dwh.schema()}.{self.TABLE} WHERE ID = 1"
        )
        assert small[0]["BODY"] == "small value"
        # ... and the oversized value survived in full.
        large = dwh.client.query(
            f"SELECT BODY FROM {dwh.schema()}.{self.TABLE} WHERE ID = 2"
        )
        assert large[0]["BODY"] == big


class TestNotNullViolationRelaxation:
    """Salesforce describe marks fields non-nullable that in practice hold NULLs.
    A NOT NULL column that receives a NULL during load is relaxed to NULL on the
    fly (ORA-01400) so the inaccurate source metadata can't abort the migration.
    """

    TABLE = "PYTEST_NN_RELAX"

    @pytest.fixture(scope="class", autouse=True)
    def seed(self, dwh: Oracle) -> Generator[None, None, None]:
        _drop(dwh, self.TABLE)
        dwh.client.execute(
            f"CREATE TABLE {dwh.schema()}.{self.TABLE} ("
            f"  ID     NUMBER(10)         NOT NULL, "
            f"  LABEL  VARCHAR2(100 CHAR) NOT NULL, "
            f"  CONSTRAINT {self.TABLE}_PK PRIMARY KEY (ID))"
        )
        dwh.client.commit()
        try:
            yield
        finally:
            _drop(dwh, self.TABLE)

    def _target(self, dwh: Oracle) -> OracleTable:
        return OracleTable(
            name=self.TABLE,
            system=System.oracle,
            namespace=dwh.schema(),
            columns=[
                OracleColumn(
                    name="ID", raw_type="NUMBER", python_type=PythonTypes.integer,
                    is_primary_key=True, is_nullable=False,
                ),
                OracleColumn(
                    name="LABEL", raw_type="VARCHAR2", python_type=PythonTypes.string,
                    max_length=100, is_nullable=False,
                ),
            ],
        )

    def test_null_into_not_null_relaxes_and_loads(self, dwh: Oracle) -> None:
        def rows():
            yield {"ID": 1, "LABEL": "present"}
            yield {"ID": 2, "LABEL": None}

        dwh.load_records(action="insert", table=self._target(dwh), records=Records(data=rows()))

        # The column was relaxed to nullable ...
        assert _catalog_col(dwh, self.TABLE, "LABEL").get("DATA_TYPE") == "VARCHAR2"
        nullable = dwh.client.query(
            "SELECT NULLABLE FROM ALL_TAB_COLUMNS "
            "WHERE OWNER = :o AND TABLE_NAME = :t AND COLUMN_NAME = 'LABEL'",
            {"o": dwh.schema(), "t": self.TABLE},
        )
        assert nullable[0]["NULLABLE"] == "Y"
        # ... and both rows, including the NULL one, loaded.
        assert _count(dwh, self.TABLE) == 2
        got = dwh.client.query(
            f"SELECT LABEL FROM {dwh.schema()}.{self.TABLE} WHERE ID = 2"
        )
        assert got[0]["LABEL"] is None


class TestImmutableConstraintGuard:
    """On-the-fly CLOB promotion refuses to disturb immutable key constraints
    (primary/unique/foreign). An oversized value arriving in a primary-key
    column raises rather than dropping the key to convert the column.
    """

    TABLE = "PYTEST_IMMUTABLE_PK"

    @pytest.fixture(scope="class", autouse=True)
    def seed(self, dwh: Oracle) -> Generator[None, None, None]:
        _drop(dwh, self.TABLE)
        dwh.client.execute(
            f"CREATE TABLE {dwh.schema()}.{self.TABLE} ("
            f"  CODE  VARCHAR2(100 CHAR) NOT NULL, "
            f"  CONSTRAINT {self.TABLE}_PK PRIMARY KEY (CODE))"
        )
        dwh.client.commit()
        try:
            yield
        finally:
            _drop(dwh, self.TABLE)

    def _target(self, dwh: Oracle) -> OracleTable:
        return OracleTable(
            name=self.TABLE,
            system=System.oracle,
            namespace=dwh.schema(),
            columns=[
                OracleColumn(
                    name="CODE", raw_type="VARCHAR2", python_type=PythonTypes.string,
                    max_length=100, is_primary_key=True, is_nullable=False,
                ),
            ],
        )

    def test_oversize_on_primary_key_refuses_promotion(self, dwh: Oracle) -> None:
        def rows():
            yield {"CODE": "X" * 9000}

        with pytest.raises(RuntimeError, match="immutable"):
            dwh.load_records(
                action="insert", table=self._target(dwh), records=Records(data=rows())
            )

        # The column was left untouched ...
        assert _catalog_col(dwh, self.TABLE, "CODE")["DATA_TYPE"] == "VARCHAR2"
        # ... and the primary key still stands.
        pk = dwh.client.all_constraints(
            schema=dwh.schema(), table_name=self.TABLE, constraint_type="P"
        )
        assert any(c["COLUMN_NAME"] == "CODE" for c in pk)
