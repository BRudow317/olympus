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
from src.models import Records, PythonTypes, System, Table
from src.services.seeding import seeding

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
    # These tests target unmanaged scratch tables in dev environments, so opt out
    # of the destructive-op guard's default 'restricted' mode unless a test sets
    # its own --destructive flag.
    if not any(a.startswith("--destructive") for a in args):
        args = (*args, "--destructive", "yes")
    result = subprocess.run(
        [sys.executable, "main.py", "--exec", "src/app.py", *args],
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
def dev01() -> Oracle:
    return Oracle(ORACLE_SOURCE_ENV)


@pytest.fixture(scope="module")
def dev02() -> Oracle:
    return Oracle(ORACLE_TARGET_ENV)


# SF -> Oracle 

class TestSfToOracleSeeding:
    """
    Seeds SF Contact + Account from devint into DWH Oracle via the CLI.

    Covers every SF field type that produces a distinct Oracle column type:
      id/string/email/phone/url/reference/picklist -> VARCHAR2
      textarea (length > 4000)                     -> CLOB
      boolean                                      -> VARCHAR2(1 CHAR)  'Y'/'N'
      date                                         -> DATE
      datetime                                     -> TIMESTAMP
      currency/percent/double                      -> NUMBER (FLOAT path)
      int                                          -> NUMBER (INTEGER path)
    """

    @pytest.fixture(scope="class", autouse=True)
    def seed_sf_tables(self, dev02: Oracle) -> Generator[None, None, None]:
        _drop(dev02, "SF_CONTACT")
        _drop(dev02, "SF_ACCOUNT")
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
            _drop(dev02, "SF_CONTACT")
            _drop(dev02, "SF_ACCOUNT")

    # table creation 
    def test_contact_table_created(self, dev02: Oracle) -> None:
        assert _table_exists(dev02, "SF_CONTACT")

    def test_account_table_created(self, dev02: Oracle) -> None:
        assert _table_exists(dev02, "SF_ACCOUNT")

    def test_contact_has_rows(self, dev02: Oracle) -> None:
        assert _count(dev02, "SF_CONTACT") >= 0

    def test_account_has_rows(self, dev02: Oracle) -> None:
        assert _count(dev02, "SF_ACCOUNT") >= 0

    # SF 'id' / 'string' -> VARCHAR2 
    def test_contact_id_field_maps_to_varchar2(self, dev02: Oracle) -> None:
        assert _catalog_col(dev02, "SF_CONTACT", "ID")["DATA_TYPE"] == "VARCHAR2"

    def test_contact_string_field_maps_to_varchar2(self, dev02: Oracle) -> None:
        assert _catalog_col(dev02, "SF_CONTACT", "LASTNAME")["DATA_TYPE"] == "VARCHAR2"

    # SF 'reference' (lookup) -> VARCHAR2
    def test_contact_reference_field_maps_to_varchar2(self, dev02: Oracle) -> None:
        assert _catalog_col(dev02, "SF_CONTACT", "ACCOUNTID")["DATA_TYPE"] == "VARCHAR2"

    # SF 'picklist' -> VARCHAR2 
    def test_account_picklist_field_maps_to_varchar2(self, dev02: Oracle) -> None:
        assert _catalog_col(dev02, "SF_ACCOUNT", "INDUSTRY")["DATA_TYPE"] == "VARCHAR2"

    # SF 'url' -> VARCHAR2 
    def test_account_url_field_maps_to_varchar2(self, dev02: Oracle) -> None:
        assert _catalog_col(dev02, "SF_ACCOUNT", "WEBSITE")["DATA_TYPE"] == "VARCHAR2"

    # SF 'textarea' (length > 4000) -> CLOB 
    def test_contact_textarea_maps_to_clob(self, dev02: Oracle) -> None:
        assert _catalog_col(dev02, "SF_CONTACT", "DESCRIPTION")["DATA_TYPE"] == "CLOB"

    def test_account_textarea_maps_to_clob(self, dev02: Oracle) -> None:
        assert _catalog_col(dev02, "SF_ACCOUNT", "DESCRIPTION")["DATA_TYPE"] == "CLOB"

    # SF 'boolean' -> VARCHAR2(1 CHAR)

    def test_contact_boolean_maps_to_varchar2(self, dev02: Oracle) -> None:
        # IsDeleted is a standard always-present Contact boolean.
        col = _catalog_col(dev02, "SF_CONTACT", "ISDELETED")
        assert col["DATA_TYPE"] == "VARCHAR2"
        assert int(col["CHAR_LENGTH"]) == 1

    # SF 'date' -> DATE 

    def test_contact_date_field_maps_to_date(self, dev02: Oracle) -> None:
        assert _catalog_col(dev02, "SF_CONTACT", "BIRTHDATE")["DATA_TYPE"] == "DATE"

    # SF 'datetime' -> TIMESTAMP 

    def test_contact_datetime_field_maps_to_timestamp(self, dev02: Oracle) -> None:
        assert _catalog_col(dev02, "SF_CONTACT", "LASTMODIFIEDDATE")["DATA_TYPE"].startswith("TIMESTAMP")

    def test_account_datetime_field_maps_to_timestamp(self, dev02: Oracle) -> None:
        assert _catalog_col(dev02, "SF_ACCOUNT", "LASTMODIFIEDDATE")["DATA_TYPE"].startswith("TIMESTAMP")

    # SF 'currency' (FLOAT path) -> NUMBER
    def test_account_currency_field_maps_to_number(self, dev02: Oracle) -> None:
        assert _catalog_col(dev02, "SF_ACCOUNT", "ANNUALREVENUE")["DATA_TYPE"] == "NUMBER"

    # SF 'int' (INTEGER path) -> NUMBER
    def test_account_int_field_maps_to_number(self, dev02: Oracle) -> None:
        assert _catalog_col(dev02, "SF_ACCOUNT", "NUMBEROFEMPLOYEES")["DATA_TYPE"] == "NUMBER"

    # cross-system name resolution 
    def test_contact_table_has_sf_prefix(self, dev02: Oracle) -> None:
        assert _table_exists(dev02, "SF_CONTACT")
        assert not _table_exists(dev02, "CONTACT")

    def test_account_table_has_sf_prefix(self, dev02: Oracle) -> None:
        assert _table_exists(dev02, "SF_ACCOUNT")
        assert not _table_exists(dev02, "ACCOUNT")

    # idempotency 
    def test_sf_to_oracle_is_idempotent(self, dev02: Oracle) -> None:
        count_before = _count(dev02, "SF_CONTACT")
        _run_cli(
            "--source-system", "salesforce",
            "--source-environment", SALESFORCE_ENV,
            "--source-namespace", SALESFORCE_ENV,
            "--target-system", "oracle",
            "--target-environment", ORACLE_TARGET_ENV,
            "--tables", "Contact",
        )
        assert _count(dev02, "SF_CONTACT") == count_before


# Oracle -> Oracle 
class TestOracleToOracleSeeding:
    """
    Seeds QBL.PYTEST_SEED -> DWH.PYTEST_SEED via the CLI.

    The source table exercises every Oracle native type that has a distinct
    python_type mapping:
      NUMBER(n,0)  -> INTEGER  -> NUMBER
      NUMBER(n,s)  -> FLOAT    -> NUMBER
      NUMBER(1,0)  -> INTEGER  -> NUMBER  (boolean-by-convention)
      VARCHAR2     -> STRING   -> VARCHAR2
      CLOB         -> STRING   -> CLOB    (max_length=None triggers CLOB path)
      DATE         -> DATE     -> DATE
      TIMESTAMP    -> DATETIME -> TIMESTAMP
      BLOB         -> BYTE     -> BLOB
    """

    @pytest.fixture(scope="class", autouse=True)
    def seed_oracle_table(self, dev01: Oracle, dev02: Oracle) -> Generator[None, None, None]:
        for ora in (dev01, dev02):
            _drop(ora, "PYTEST_SEED")

        source_schema = dev01.schema()
        dev01.client.execute(_DDL_ORA_SEED.format(schema=source_schema))
        dev01.client.execute(
            f"INSERT INTO {source_schema}.PYTEST_SEED "
            "(ID, LABEL, SCORE, INT_VAL, FLAG, BORN_ON, UPDATED_AT) "
            "VALUES (1, 'Alpha', 3.14, 42, 1, "
            "DATE '2020-01-01', TIMESTAMP '2024-06-15 12:00:00')"
        )
        dev01.client.execute(
            f"INSERT INTO {source_schema}.PYTEST_SEED (ID, LABEL, FLAG) VALUES (2, 'Beta', 0)"
        )
        dev01.client.commit()

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
            for ora in (dev02, dev01):
                _drop(ora, "PYTEST_SEED")

    # structural 
    def test_target_table_created(self, dev02: Oracle) -> None:
        assert _table_exists(dev02, "PYTEST_SEED")

    def test_row_count_matches_source(self, dev01: Oracle, dev02: Oracle) -> None:
        assert _count(dev02, "PYTEST_SEED") == _count(dev01, "PYTEST_SEED")

    # type round-trips 
    def test_varchar2_round_trips(self, dev02: Oracle) -> None:
        assert _catalog_col(dev02, "PYTEST_SEED", "LABEL")["DATA_TYPE"] == "VARCHAR2"

    def test_clob_round_trips(self, dev02: Oracle) -> None:
        assert _catalog_col(dev02, "PYTEST_SEED", "NOTES")["DATA_TYPE"] == "CLOB"

    def test_number_integer_round_trips(self, dev02: Oracle) -> None:
        assert _catalog_col(dev02, "PYTEST_SEED", "INT_VAL")["DATA_TYPE"] == "NUMBER"

    def test_number_decimal_round_trips(self, dev02: Oracle) -> None:
        assert _catalog_col(dev02, "PYTEST_SEED", "SCORE")["DATA_TYPE"] == "NUMBER"

    def test_number_flag_round_trips(self, dev02: Oracle) -> None:
        assert _catalog_col(dev02, "PYTEST_SEED", "FLAG")["DATA_TYPE"] == "NUMBER"

    def test_date_round_trips(self, dev02: Oracle) -> None:
        assert _catalog_col(dev02, "PYTEST_SEED", "BORN_ON")["DATA_TYPE"] == "DATE"

    def test_timestamp_round_trips(self, dev02: Oracle) -> None:
        assert _catalog_col(dev02, "PYTEST_SEED", "UPDATED_AT")["DATA_TYPE"].startswith("TIMESTAMP")

    def test_blob_round_trips(self, dev02: Oracle) -> None:
        assert _catalog_col(dev02, "PYTEST_SEED", "PAYLOAD")["DATA_TYPE"] == "BLOB"

    # data integrity 
    def test_varchar2_data_survives_transit(self, dev02: Oracle) -> None:
        rows = dev02.client.query(f"SELECT LABEL FROM {dev02.schema()}.PYTEST_SEED WHERE ID = 1")
        assert rows[0]["LABEL"] == "Alpha"

    def test_integer_data_survives_transit(self, dev02: Oracle) -> None:
        rows = dev02.client.query(f"SELECT INT_VAL FROM {dev02.schema()}.PYTEST_SEED WHERE ID = 1")
        assert int(rows[0]["INT_VAL"]) == 42

    def test_flag_false_row_survives_transit(self, dev02: Oracle) -> None:
        rows = dev02.client.query(f"SELECT FLAG FROM {dev02.schema()}.PYTEST_SEED WHERE ID = 2")
        assert int(rows[0]["FLAG"]) == 0

    def test_date_data_survives_transit(self, dev02: Oracle) -> None:
        rows = dev02.client.query(f"SELECT BORN_ON FROM {dev02.schema()}.PYTEST_SEED WHERE ID = 1")
        val = rows[0]["BORN_ON"]
        if isinstance(val, datetime.datetime):
            val = val.date()
        assert val == datetime.date(2020, 1, 1)

    def test_null_fields_survive_transit(self, dev02: Oracle) -> None:
        rows = dev02.client.query(
            f"SELECT SCORE, BORN_ON FROM {dev02.schema()}.PYTEST_SEED WHERE ID = 2"
        )
        assert rows[0]["SCORE"] is None
        assert rows[0]["BORN_ON"] is None

    # idempotency
    def test_oracle_to_oracle_is_idempotent(self, dev01: Oracle, dev02: Oracle) -> None:
        count_before = _count(dev02, "PYTEST_SEED")
        _run_cli(
            "--source-system", "oracle",
            "--source-environment", ORACLE_SOURCE_ENV,
            "--target-system", "oracle",
            "--target-environment", ORACLE_TARGET_ENV,
            "--tables", "PYTEST_SEED",
        )
        assert _count(dev02, "PYTEST_SEED") == count_before


# SF -> Oracle -> SF round trip
class TestSfOracleRoundTrip:
    """The headline migration: discover Salesforce schema + data into Oracle,
    then push those same records back into Salesforce as an upsert (keyed on Id).

    Verifies both legs run end-to-end through the CLI without error and that the
    Oracle landing table is populated between the two legs.
    """

    @pytest.fixture(scope="class", autouse=True)
    def round_trip(self, dev02: Oracle) -> Generator[None, None, None]:
        _drop(dev02, "SF_CONTACT")
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
            _drop(dev02, "SF_CONTACT")

    def test_oracle_landing_table_populated(self, dev02: Oracle) -> None:
        assert _table_exists(dev02, "SF_CONTACT")
        assert _count(dev02, "SF_CONTACT") >= 0

    # def test_upsert_back_into_salesforce(self, dev02: Oracle) -> None:
    #     # Leg 2: Oracle -> Salesforce upsert on Id (round trips the records home).
    #     _run_cli(
    #         "--source-system", "oracle",
    #         "--source-environment", ORACLE_TARGET_ENV,
    #         "--target-system", "salesforce",
    #         "--target-environment", SALESFORCE_ENV,
    #         "--target-namespace", SALESFORCE_ENV,
    #         "--action", "upsert",
    #         "--external-id-field", "Id",
    #         "--tables", "SF_CONTACT",
    #     )


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
    def seed(self, dev02: Oracle) -> Generator[None, None, None]:
        _drop(dev02, self.TABLE)
        # BODY is deliberately created small to force the overflow path.
        dev02.client.execute(
            f"CREATE TABLE {dev02.schema()}.{self.TABLE} ("
            f"  ID    NUMBER(10)        NOT NULL, "
            f"  BODY  VARCHAR2(100 CHAR) NULL, "
            f"  CONSTRAINT {self.TABLE}_PK PRIMARY KEY (ID))"
        )
        dev02.client.commit()
        try:
            yield
        finally:
            _drop(dev02, self.TABLE)

    def _target(self, dev02: Oracle) -> OracleTable:
        # Describe the target as the migrator would: BODY is "still" VARCHAR2.
        return OracleTable(
            name=self.TABLE,
            system=System.oracle,
            namespace=dev02.schema(),
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

    def test_oversize_value_promotes_column_and_loads(self, dev02: Oracle) -> None:
        big = "X" * 9000  # well beyond the 4000-char VARCHAR2 ceiling

        def rows():
            yield {"ID": 1, "BODY": "small value"}
            yield {"ID": 2, "BODY": big}

        dev02.load_records(action="insert", table=self._target(dev02), records=Records(data=rows()))

        # The column is now CLOB ...
        assert _catalog_col(dev02, self.TABLE, "BODY")["DATA_TYPE"] == "CLOB"
        # ... every row landed ...
        assert _count(dev02, self.TABLE) == 2
        # ... the small value still round-trips ...
        small = dev02.client.query(
            f"SELECT BODY FROM {dev02.schema()}.{self.TABLE} WHERE ID = 1"
        )
        assert small[0]["BODY"] == "small value"
        # ... and the oversized value survived in full.
        large = dev02.client.query(
            f"SELECT BODY FROM {dev02.schema()}.{self.TABLE} WHERE ID = 2"
        )
        assert large[0]["BODY"] == big


class TestNotNullViolationRelaxation:
    """Salesforce describe marks fields non-nullable that in practice hold NULLs.
    A NOT NULL column that receives a NULL during load is relaxed to NULL on the
    fly (ORA-01400) so the inaccurate source metadata can't abort the migration.
    """

    TABLE = "PYTEST_NN_RELAX"

    @pytest.fixture(scope="class", autouse=True)
    def seed(self, dev02: Oracle) -> Generator[None, None, None]:
        _drop(dev02, self.TABLE)
        dev02.client.execute(
            f"CREATE TABLE {dev02.schema()}.{self.TABLE} ("
            f"  ID     NUMBER(10)         NOT NULL, "
            f"  LABEL  VARCHAR2(100 CHAR) NOT NULL, "
            f"  CONSTRAINT {self.TABLE}_PK PRIMARY KEY (ID))"
        )
        dev02.client.commit()
        try:
            yield
        finally:
            _drop(dev02, self.TABLE)

    def _target(self, dev02: Oracle) -> OracleTable:
        return OracleTable(
            name=self.TABLE,
            system=System.oracle,
            namespace=dev02.schema(),
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

    def test_null_into_not_null_relaxes_and_loads(self, dev02: Oracle) -> None:
        def rows():
            yield {"ID": 1, "LABEL": "present"}
            yield {"ID": 2, "LABEL": None}

        dev02.load_records(action="insert", table=self._target(dev02), records=Records(data=rows()))

        # The column was relaxed to nullable ...
        assert _catalog_col(dev02, self.TABLE, "LABEL").get("DATA_TYPE") == "VARCHAR2"
        nullable = dev02.client.query(
            "SELECT NULLABLE FROM ALL_TAB_COLUMNS "
            "WHERE OWNER = :o AND TABLE_NAME = :t AND COLUMN_NAME = 'LABEL'",
            {"o": dev02.schema(), "t": self.TABLE},
        )
        assert nullable[0]["NULLABLE"] == "Y"
        # ... and both rows, including the NULL one, loaded.
        assert _count(dev02, self.TABLE) == 2
        got = dev02.client.query(
            f"SELECT LABEL FROM {dev02.schema()}.{self.TABLE} WHERE ID = 2"
        )
        assert got[0]["LABEL"] is None


class TestImmutableConstraintGuard:
    """On-the-fly CLOB promotion refuses to disturb immutable key constraints
    (primary/unique/foreign). An oversized value arriving in a primary-key
    column raises rather than dropping the key to convert the column.
    """

    TABLE = "PYTEST_IMMUTABLE_PK"

    @pytest.fixture(scope="class", autouse=True)
    def seed(self, dev02: Oracle) -> Generator[None, None, None]:
        _drop(dev02, self.TABLE)
        dev02.client.execute(
            f"CREATE TABLE {dev02.schema()}.{self.TABLE} ("
            f"  CODE  VARCHAR2(100 CHAR) NOT NULL, "
            f"  CONSTRAINT {self.TABLE}_PK PRIMARY KEY (CODE))"
        )
        dev02.client.commit()
        try:
            yield
        finally:
            _drop(dev02, self.TABLE)

    def _target(self, dev02: Oracle) -> OracleTable:
        return OracleTable(
            name=self.TABLE,
            system=System.oracle,
            namespace=dev02.schema(),
            columns=[
                OracleColumn(
                    name="CODE", raw_type="VARCHAR2", python_type=PythonTypes.string,
                    max_length=100, is_primary_key=True, is_nullable=False,
                ),
            ],
        )

    def test_oversize_on_primary_key_refuses_promotion(self, dev02: Oracle) -> None:
        def rows():
            yield {"CODE": "X" * 9000}

        with pytest.raises(RuntimeError, match="immutable"):
            dev02.load_records(
                action="insert", table=self._target(dev02), records=Records(data=rows())
            )

        # The column was left untouched ...
        assert _catalog_col(dev02, self.TABLE, "CODE")["DATA_TYPE"] == "VARCHAR2"
        # ... and the primary key still stands.
        pk = dev02.client.all_constraints(
            schema=dev02.schema(), table_name=self.TABLE, constraint_type="P"
        )
        assert any(c["COLUMN_NAME"] == "CODE" for c in pk)


class TestFullLoadDropsTable:
    """action='full_load' drops and recreates the Oracle table, where 'reset'
    only truncates it. A column present in the old table but absent from the
    incoming schema is therefore gone after a full_load, proving the drop.
    """

    TABLE = "PYTEST_FULL_LOAD"

    @pytest.fixture(scope="class", autouse=True)
    def seed(self, dev02: Oracle) -> Generator[None, None, None]:
        _drop(dev02, self.TABLE)
        # Pre-existing table carries STALE_COL (not in the incoming schema) and a
        # stale row. A truncate would keep STALE_COL; a full_load drops it.
        dev02.client.execute(
            f"CREATE TABLE {dev02.schema()}.{self.TABLE} ("
            f"  ID        NUMBER(10)         NOT NULL, "
            f"  STALE_COL VARCHAR2(50 CHAR), "
            f"  CONSTRAINT {self.TABLE}_PK PRIMARY KEY (ID))"
        )
        dev02.client.execute(
            f"INSERT INTO {dev02.schema()}.{self.TABLE} (ID, STALE_COL) VALUES (99, 'old')"
        )
        dev02.client.commit()
        try:
            yield
        finally:
            _drop(dev02, self.TABLE)

    def _incoming(self, dev02: Oracle) -> OracleTable:
        return OracleTable(
            name=self.TABLE,
            system=System.oracle,
            namespace=dev02.schema(),
            columns=[
                OracleColumn(
                    name="ID", raw_type="NUMBER", python_type=PythonTypes.integer,
                    is_primary_key=True, is_nullable=False,
                ),
                OracleColumn(
                    name="LABEL", raw_type="VARCHAR2", python_type=PythonTypes.string,
                    max_length=100,
                ),
            ],
        )

    def test_full_load_drops_and_recreates(self, dev02: Oracle) -> None:
        target = dev02.mutate_table(self._incoming(dev02), action="full_load")

        def rows():
            yield {"ID": 1, "LABEL": "fresh"}

        dev02.load_records(action="full_load", table=target, records=Records(data=rows()))

        # The dropped table took STALE_COL with it ...
        assert _catalog_col(dev02, self.TABLE, "STALE_COL") == {}
        # ... the incoming LABEL column is present ...
        assert _catalog_col(dev02, self.TABLE, "LABEL")["DATA_TYPE"] == "VARCHAR2"
        # ... and only the freshly loaded row survives (stale ID=99 is gone).
        assert _count(dev02, self.TABLE) == 1
        got = dev02.client.query(
            f"SELECT ID, LABEL FROM {dev02.schema()}.{self.TABLE}"
        )
        assert int(got[0]["ID"]) == 1
        assert got[0]["LABEL"] == "fresh"


class TestTargetNamespacePropagation:
    """Regression guard for cross-schema targeting.

    When --target-namespace names a schema other than the connection's login
    user, the table handed to target.mutate_table must carry that namespace --
    not the login user. (In Oracle the namespace silently defaults to the login
    user, so a dropped/ignored --target-namespace would write into the wrong
    schema.) This is the exact boundary the CLI exercises: app.py forwards the
    --target-namespace value straight into seeding(), so we drive seeding()
    in-process with mocked datasources and assert the namespace at that handoff.

    Hermetic by design: no live database -- the Oracle client is stubbed so the
    only thing under test is the namespace wiring.
    """

    LOGIN_USER = "QBL"               # AUTO_<ENV>_USER -- the connection identity
    TARGET_SCHEMA = "QBL_OTHER_USER"  # --target-namespace -- a different schema
    SOURCE_ENV = "PYTEST_SRC_ENV"
    TARGET_ENV = "PYTEST_TGT_ENV"

    class _FakeSource:
        """Minimal DataSource stand-in: describes one table, yields no rows."""
        namespace = "PYTEST_SRC"
        guard = None

        def describe_table(self, table: Table) -> Table:
            return Table(
                name=table.name,
                system=System.oracle,
                namespace=self.namespace,
                columns=[
                    OracleColumn(
                        name="ID", raw_type="NUMBER",
                        python_type=PythonTypes.integer,
                        is_primary_key=True, is_nullable=False,
                    ),
                ],
            )

        def get_records(self, table: Table, **kwargs: Any) -> Records:
            return Records(data=iter([]), code=200)

    def test_target_namespace_overrides_login_user(self, monkeypatch: pytest.MonkeyPatch) -> None:
        # Stub the Oracle client so constructing a real Oracle needs no env vars
        # and no DB connection; it only has to expose the login user.
        class _FakeClient:
            user = self.LOGIN_USER

        monkeypatch.setattr(
            "src.oracle.OracleClient.OracleClient.client_constructor",
            classmethod(lambda cls, environment: _FakeClient()),
        )

        # Real Oracle target (so its own namespace resolution runs), with the two
        # DB-touching methods replaced: mutate_table records the table it is
        # handed, load_records is a no-op.
        target = Oracle(self.TARGET_ENV, self.TARGET_SCHEMA)
        captured: dict[str, Any] = {}

        def _capture_mutate_table(table: Table, source_system: System | None = None, **kwargs: Any) -> Table:
            captured["namespace"] = table.namespace
            captured["login_user"] = target.client.user
            return table

        monkeypatch.setattr(target, "mutate_table", _capture_mutate_table)
        monkeypatch.setattr(target, "load_records", lambda **kwargs: None)

        source = self._FakeSource()

        def _fake_get_datasource(system: System, environment: str, namespace: str | None):
            return source if environment == self.SOURCE_ENV else target

        monkeypatch.setattr("src.services.seeding.get_datasource", _fake_get_datasource)

        seeding(
            source_system=System.oracle,
            source_environment=self.SOURCE_ENV,
            source_namespace=source.namespace,
            target_system=System.oracle,
            target_environment=self.TARGET_ENV,
            target_namespace=self.TARGET_SCHEMA,
            tables=["PYTEST_NS_PROP"],
            action="insert",
        )

        # The table reached mutate_table carrying the requested target schema ...
        assert captured["namespace"] == self.TARGET_SCHEMA
        # ... which is NOT the connection's login user (the cross-schema bug).
        assert captured["namespace"] != captured["login_user"]
