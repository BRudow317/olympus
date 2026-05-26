"""test_oracle.py"""
from __future__ import annotations

from collections.abc import Generator
from typing import Any

import oracledb
import pytest

from src.models import Records, System, Table
from src.oracle.Oracle import Oracle
from src.oracle.OracleModels import varchar2_growth_buffer
from src.seeding import seeding_test

_DDL_PYTEST = """
CREATE TABLE {schema}.PYTEST (
    ID    NUMBER(10)        NOT NULL,
    NAME  VARCHAR2(50 CHAR) NOT NULL,
    CONSTRAINT {schema}_PYTEST_PK PRIMARY KEY (ID)
)
"""

# Per-test table; VAL column DDL is injected by each test
_DDL_PYTEST_MUT = """
CREATE TABLE {schema}.PYTEST_MUT (
    ID   NUMBER(10) NOT NULL,
    VAL  {val_ddl}  NULL,
    CONSTRAINT {schema}_PYTEST_MUT_PK PRIMARY KEY (ID)
)
"""

_SIMPLE_ROWS = [
    {"ID": 1, "NAME": "Alpha"},
    {"ID": 2, "NAME": "Beta"},
]

@pytest.fixture(scope="module")
def qbl() -> Oracle:
    return Oracle("QBL", "QBL")


@pytest.fixture(scope="module")
def dwh() -> Oracle:
    return Oracle("DWH", "DWH")


def _drop(ora: Oracle, table: str) -> None:
    try:
        ora._client.execute(f"DROP TABLE {ora._schema()}.{table} CASCADE CONSTRAINTS")
    except oracledb.DatabaseError:
        pass


@pytest.fixture(scope="module", autouse=True)
def setup_tables(qbl: Oracle, dwh: Oracle) -> Generator[None, None, None]:
    for ora in (qbl, dwh):
        _drop(ora, "PYTEST")
        ora._client.execute(_DDL_PYTEST.format(schema=ora._schema()))
    yield
    for ora in (dwh, qbl):
        _drop(ora, "PYTEST")

@pytest.fixture(autouse=True)
def clean_dwh(dwh: Oracle) -> None:
    dwh._client.execute(f"TRUNCATE TABLE {dwh._schema()}.PYTEST")

@pytest.fixture()
def dwh_pytest(dwh: Oracle) -> Table:
    return dwh.describe_table(Table(name="PYTEST", system=System.oracle, namespace="DWH"))

def _count(ora: Oracle, table: str = "PYTEST") -> int:
    return int(ora._client.query(
        f"SELECT COUNT(*) AS CNT FROM {ora._schema()}.{table}"
    )[0]["CNT"])

def _fetch_row(ora: Oracle, row_id: int, table: str = "PYTEST") -> dict:
    rows = ora._client.query(
        f"SELECT * FROM {ora._schema()}.{table} WHERE ID = :id", {"id": row_id}
    )
    return rows[0] if rows else {}

def _catalog_col(ora: Oracle, table: str, column: str) -> dict[str, Any]:
    """Return ALL_TAB_COLUMNS row for a specific column, or {} if absent."""
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

class TestMutateColumnTypes:

    @pytest.fixture(autouse=True)
    def mut_tables(self, qbl: Oracle, dwh: Oracle, request: pytest.FixtureRequest
                   ) -> Generator[None, None, None]:
        val_ddl: str = request.param if hasattr(request, "param") else "VARCHAR2(10 CHAR)"
        for ora in (qbl, dwh):
            _drop(ora, "PYTEST_MUT")
        qbl._client.execute(_DDL_PYTEST_MUT.format(schema="QBL", val_ddl=val_ddl))
        yield
        for ora in (qbl, dwh):
            _drop(ora, "PYTEST_MUT")

    def _mutate_and_get_col(self, qbl: Oracle, dwh: Oracle) -> dict[str, Any]:
        source = qbl.describe_table(
            Table(name="PYTEST_MUT", system=System.oracle, namespace="QBL")
        )
        dwh.mutate_table(source)
        return _catalog_col(dwh, "PYTEST_MUT", "VAL")

    @pytest.mark.parametrize("mut_tables", ["VARCHAR2(50 CHAR)"], indirect=True)
    def test_varchar2_maps_to_varchar2(self, qbl: Oracle, dwh: Oracle) -> None:
        col = self._mutate_and_get_col(qbl, dwh)
        assert col["DATA_TYPE"] == "VARCHAR2"

    @pytest.mark.parametrize("mut_tables", ["VARCHAR2(50 CHAR)"], indirect=True)
    def test_varchar2_created_with_growth_buffer(self, qbl: Oracle, dwh: Oracle) -> None:
        self._mutate_and_get_col(qbl, dwh)
        assert _catalog_col(dwh, "PYTEST_MUT", "VAL")["CHAR_LENGTH"] == 50 + varchar2_growth_buffer

    @pytest.mark.parametrize("mut_tables", ["NUMBER"], indirect=True)
    def test_number_maps_to_number(self, qbl: Oracle, dwh: Oracle) -> None:
        col = self._mutate_and_get_col(qbl, dwh)
        assert col["DATA_TYPE"] == "NUMBER"

    @pytest.mark.parametrize("mut_tables", ["NUMBER(10,4)"], indirect=True)
    def test_number_decimal_maps_to_number(self, qbl: Oracle, dwh: Oracle) -> None:
        col = self._mutate_and_get_col(qbl, dwh)
        assert col["DATA_TYPE"] == "NUMBER"

    @pytest.mark.parametrize("mut_tables", ["DATE"], indirect=True)
    def test_date_maps_to_date(self, qbl: Oracle, dwh: Oracle) -> None:
        col = self._mutate_and_get_col(qbl, dwh)
        assert col["DATA_TYPE"] == "DATE"

    @pytest.mark.parametrize("mut_tables", ["TIMESTAMP"], indirect=True)
    def test_timestamp_maps_to_timestamp(self, qbl: Oracle, dwh: Oracle) -> None:
        col = self._mutate_and_get_col(qbl, dwh)
        assert col["DATA_TYPE"].startswith("TIMESTAMP")

    @pytest.mark.parametrize("mut_tables", ["TIMESTAMP(6)"], indirect=True)
    def test_timestamp6_maps_to_timestamp(self, qbl: Oracle, dwh: Oracle) -> None:
        col = self._mutate_and_get_col(qbl, dwh)
        assert col["DATA_TYPE"].startswith("TIMESTAMP")

    @pytest.mark.parametrize("mut_tables", ["CLOB"], indirect=True)
    def test_clob_maps_to_clob(self, qbl: Oracle, dwh: Oracle) -> None:
        col = self._mutate_and_get_col(qbl, dwh)
        assert col["DATA_TYPE"] == "CLOB"

    @pytest.mark.parametrize("mut_tables", ["CHAR(10)"], indirect=True)
    def test_char_normalizes_to_varchar2(self, qbl: Oracle, dwh: Oracle) -> None:
        col = self._mutate_and_get_col(qbl, dwh)
        assert col["DATA_TYPE"] == "VARCHAR2"

    @pytest.mark.parametrize("mut_tables", ["RAW(32)"], indirect=True)
    def test_raw_normalizes_to_blob(self, qbl: Oracle, dwh: Oracle) -> None:
        col = self._mutate_and_get_col(qbl, dwh)
        assert col["DATA_TYPE"] == "BLOB"

class TestMutateVarchar2Widening:

    @pytest.fixture(autouse=True)
    def narrow_mut_table(self, qbl: Oracle, dwh: Oracle) -> Generator[None, None, None]:
        for ora in (qbl, dwh):
            _drop(ora, "PYTEST_MUT")
        qbl._client.execute(
            _DDL_PYTEST_MUT.format(schema="QBL", val_ddl="VARCHAR2(20 CHAR)")
        )
        yield
        for ora in (qbl, dwh):
            _drop(ora, "PYTEST_MUT")

    def _initial_mutate(self, qbl: Oracle, dwh: Oracle) -> None:
        source = qbl.describe_table(
            Table(name="PYTEST_MUT", system=System.oracle, namespace="QBL")
        )
        dwh.mutate_table(source)

    def test_no_widen_when_source_fits(self, qbl: Oracle, dwh: Oracle) -> None:
        self._initial_mutate(qbl, dwh)
        original_len = _catalog_col(dwh, "PYTEST_MUT", "VAL")["CHAR_LENGTH"]

        # Source stays at 20 — no change expected
        source = qbl.describe_table(
            Table(name="PYTEST_MUT", system=System.oracle, namespace="QBL")
        )
        dwh.mutate_table(source)
        assert _catalog_col(dwh, "PYTEST_MUT", "VAL")["CHAR_LENGTH"] == original_len

    def test_widens_when_source_grows(self, qbl: Oracle, dwh: Oracle) -> None:
        self._initial_mutate(qbl, dwh)
        original_len = _catalog_col(dwh, "PYTEST_MUT", "VAL")["CHAR_LENGTH"]

        qbl._client.execute("ALTER TABLE QBL.PYTEST_MUT MODIFY (VAL VARCHAR2(200 CHAR))")
        source = qbl.describe_table(
            Table(name="PYTEST_MUT", system=System.oracle, namespace="QBL")
        )
        dwh.mutate_table(source)
        assert _catalog_col(dwh, "PYTEST_MUT", "VAL")["CHAR_LENGTH"] > original_len

    def test_does_not_shrink_when_source_shrinks(self, qbl: Oracle, dwh: Oracle) -> None:
        # First widen QBL to 200, let DWH grow to 250
        qbl._client.execute("ALTER TABLE QBL.PYTEST_MUT MODIFY (VAL VARCHAR2(200 CHAR))")
        self._initial_mutate(qbl, dwh)
        widened_len = _catalog_col(dwh, "PYTEST_MUT", "VAL")["CHAR_LENGTH"]

        # Shrink QBL back to 20 — DWH must not shrink
        qbl._client.execute("ALTER TABLE QBL.PYTEST_MUT MODIFY (VAL VARCHAR2(20 CHAR))")
        source = qbl.describe_table(
            Table(name="PYTEST_MUT", system=System.oracle, namespace="QBL")
        )
        dwh.mutate_table(source)
        assert _catalog_col(dwh, "PYTEST_MUT", "VAL")["CHAR_LENGTH"] == widened_len

class TestMutateStructural:

    @pytest.fixture(autouse=True)
    def mut_tables(self, qbl: Oracle, dwh: Oracle) -> Generator[None, None, None]:
        for ora in (qbl, dwh):
            _drop(ora, "PYTEST_MUT")
        qbl._client.execute(
            _DDL_PYTEST_MUT.format(schema="QBL", val_ddl="VARCHAR2(30 CHAR)")
        )
        yield
        for ora in (qbl, dwh):
            _drop(ora, "PYTEST_MUT")

    def test_creates_missing_target_table(self, qbl: Oracle, dwh: Oracle) -> None:
        source = qbl.describe_table(
            Table(name="PYTEST_MUT", system=System.oracle, namespace="QBL")
        )
        dwh.mutate_table(source)
        assert _table_exists(dwh, "PYTEST_MUT")

    def test_adds_new_column_to_existing_table(self, qbl: Oracle, dwh: Oracle) -> None:
        source = qbl.describe_table(
            Table(name="PYTEST_MUT", system=System.oracle, namespace="QBL")
        )
        dwh.mutate_table(source)

        qbl._client.execute("ALTER TABLE QBL.PYTEST_MUT ADD (SCORE NUMBER)")
        source = qbl.describe_table(
            Table(name="PYTEST_MUT", system=System.oracle, namespace="QBL")
        )
        dwh.mutate_table(source)

        assert _catalog_col(dwh, "PYTEST_MUT", "SCORE")["DATA_TYPE"] == "NUMBER"

    def test_returns_described_target_table(self, qbl: Oracle, dwh: Oracle) -> None:
        source = qbl.describe_table(
            Table(name="PYTEST_MUT", system=System.oracle, namespace="QBL")
        )
        result = dwh.mutate_table(source)
        assert result.namespace == "DWH"
        assert result.name == "PYTEST_MUT"

class TestLoadRecordsActions:
    """Verify each action commits correctly and produces the right outcome."""

    def test_insert_commits(self, dwh_pytest: Table, dwh: Oracle) -> None:
        dwh.load_records("insert", dwh_pytest, Records(data=iter(_SIMPLE_ROWS)))
        assert _count(dwh) == 2

    def test_reset_truncates_then_loads(self, dwh_pytest: Table, dwh: Oracle) -> None:
        dwh.load_records("insert", dwh_pytest, Records(data=iter(_SIMPLE_ROWS)))
        dwh.load_records("reset",  dwh_pytest, Records(data=iter([_SIMPLE_ROWS[0]])))
        assert _count(dwh) == 1

    def test_upsert_inserts_and_updates(self, dwh_pytest: Table, dwh: Oracle) -> None:
        dwh.load_records("insert", dwh_pytest, Records(data=iter([_SIMPLE_ROWS[0]])))
        dwh.load_records("upsert", dwh_pytest, Records(data=iter([
            {**_SIMPLE_ROWS[0], "NAME": "Alpha Updated"},
            _SIMPLE_ROWS[1],
        ])))
        assert _count(dwh) == 2
        assert _fetch_row(dwh, 1)["NAME"] == "Alpha Updated"

    def test_update_modifies_existing(self, dwh_pytest: Table, dwh: Oracle) -> None:
        dwh.load_records("insert", dwh_pytest, Records(data=iter(_SIMPLE_ROWS)))
        dwh.load_records("update", dwh_pytest, Records(data=iter([
            {**_SIMPLE_ROWS[0], "NAME": "Alpha Modified"}
        ])))
        assert _fetch_row(dwh, 1)["NAME"] == "Alpha Modified"
        assert _count(dwh) == 2

    def test_unknown_action_raises(self, dwh_pytest: Table, dwh: Oracle) -> None:
        with pytest.raises(RuntimeError, match="Unknown value"):
            dwh.load_records("teleport", dwh_pytest, Records(data=iter([])))

    def test_duplicate_pk_raises(self, dwh_pytest: Table, dwh: Oracle) -> None:
        dwh.load_records("insert", dwh_pytest, Records(data=iter(_SIMPLE_ROWS)))
        with pytest.raises(RuntimeError):
            dwh.load_records("insert", dwh_pytest, Records(data=iter(_SIMPLE_ROWS)))

class TestSeedingPipeline:

    @pytest.fixture(autouse=True)
    def seed_tables(self, qbl: Oracle, dwh: Oracle) -> Generator[None, None, None]:
        """Use PYTEST_MUT (supported types only) so seeding can create it fresh."""
        for ora in (qbl, dwh):
            _drop(ora, "PYTEST_MUT")
        qbl._client.execute(
            _DDL_PYTEST_MUT.format(schema="QBL", val_ddl="VARCHAR2(30 CHAR)")
        )
        yield
        for ora in (qbl, dwh):
            _drop(ora, "PYTEST_MUT")

    def _run(self) -> None:
        seeding_test(
            source_system=System.oracle, source_environment="QBL", source_namespace="QBL",
            target_system=System.oracle, target_environment="DWH", target_namespace="DWH",
            tables=["PYTEST_MUT"],
        )

    def test_creates_target_and_loads_data(self, qbl: Oracle, dwh: Oracle) -> None:
        qbl._client.execute("INSERT INTO QBL.PYTEST_MUT (ID, VAL) VALUES (1, 'hello')")
        qbl._client.commit()

        self._run()

        assert _table_exists(dwh, "PYTEST_MUT")
        assert _count(dwh, "PYTEST_MUT") == 1
        assert _fetch_row(dwh, 1, "PYTEST_MUT")["VAL"] == "hello"

    def test_is_idempotent(self, qbl: Oracle, dwh: Oracle) -> None:
        qbl._client.execute("INSERT INTO QBL.PYTEST_MUT (ID, VAL) VALUES (1, 'hello')")
        qbl._client.commit()

        self._run()
        self._run()

        assert _count(dwh, "PYTEST_MUT") == 1

    def test_empty_source_leaves_target_empty(self, dwh: Oracle) -> None:
        self._run()
        assert _count(dwh, "PYTEST_MUT") == 0
