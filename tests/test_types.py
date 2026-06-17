"""test_types.py -- pure (no-DB) consistency tests for the type system.

Verifies that Python types map consistently to/from each Salesforce type and
each Oracle type, that those mappings round-trip, and that the cross-system
name/column resolution used by the migrator is stable in every direction.
"""
from __future__ import annotations

import datetime
from decimal import Decimal

import pytest

from src.models import Column, PythonTypes, System
from src.sf.SfTypeMap import (
    SF_TYPE_MAP,
    sf_type_to_python,
    sf_to_python,
    python_to_sf,
    cast_record,
)
from src.oracle.OracleTypeMap import (
    oracle_to_python,
    python_to_oracle,
    normalize_cell,
)
from src.oracle.OracleModels import OracleColumn, to_oracle_snake, ORACLE_RESERVED
from src.services.seeding import resolve_cross_system_name, _build_rename_map



# Salesforce <-> Python

class TestSalesforceTypeConsistency:
    def test_every_sf_type_maps_to_a_python_type(self) -> None:
        for sf_type in SF_TYPE_MAP:
            assert isinstance(sf_type_to_python(sf_type), PythonTypes)

    def test_sf_type_map_is_case_insensitive(self) -> None:
        assert sf_type_to_python("ID") == PythonTypes.string
        assert sf_type_to_python("Boolean") == PythonTypes.boolean

    @pytest.mark.parametrize(
        "sf_type, expected",
        [
            ("id", PythonTypes.string),
            ("string", PythonTypes.string),
            ("reference", PythonTypes.string),
            ("picklist", PythonTypes.string),
            ("int", PythonTypes.integer),
            ("double", PythonTypes.float),
            ("currency", PythonTypes.float),
            ("boolean", PythonTypes.boolean),
            ("date", PythonTypes.date),
            ("datetime", PythonTypes.datetime),
            ("time", PythonTypes.time),
            ("base64", PythonTypes.byte),
            ("address", PythonTypes.json),
        ],
    )
    def test_known_sf_types(self, sf_type: str, expected: PythonTypes) -> None:
        assert sf_type_to_python(sf_type) == expected

    @pytest.mark.parametrize(
        "sf_type, raw, expected",
        [
            ("boolean", "true", True),
            ("boolean", "false", False),
            ("int", "42", 42),
            ("double", "3.14", 3.14),
            ("currency", "100.50", Decimal("100.50")),
            ("date", "2020-01-01", datetime.date(2020, 1, 1)),
            ("time", "12:30:00.000Z", datetime.time(12, 30)),
        ],
    )
    def test_sf_value_to_python(self, sf_type: str, raw: str, expected) -> None:
        assert sf_to_python(sf_type, raw) == expected

    def test_sf_datetime_value_parses_with_timezone(self) -> None:
        result = sf_to_python("datetime", "2024-06-15T12:00:00.000+0000")
        assert isinstance(result, datetime.datetime)
        assert result.tzinfo is not None

    @pytest.mark.parametrize("empty", ["", None])
    def test_sf_empty_value_is_none(self, empty) -> None:
        assert sf_to_python("string", empty) is None

    @pytest.mark.parametrize(
        "value, expected",
        [
            (True, "true"),
            (False, "false"),
            (None, ""),
            (42, "42"),
            (datetime.date(2020, 1, 1), "2020-01-01"),
            ({"a": 1}, '{"a": 1}'),
        ],
    )
    def test_python_to_sf(self, value, expected: str) -> None:
        assert python_to_sf(value) == expected

    def test_cast_record_applies_per_field_types(self) -> None:
        record = {"Id": "001", "Age": "30", "Active": "true", "Untyped": "x"}
        field_types = {"Id": "id", "Age": "int", "Active": "boolean"}
        out = cast_record(record, field_types)
        assert out == {"Id": "001", "Age": 30, "Active": True, "Untyped": "x"}



# Oracle <-> Python

class TestOracleTypeConsistency:
    @pytest.mark.parametrize(
        "raw_type, scale, expected",
        [
            ("VARCHAR2", None, PythonTypes.string),
            ("NVARCHAR2", None, PythonTypes.string),
            ("CHAR", None, PythonTypes.string),
            ("CLOB", None, PythonTypes.string),
            ("NUMBER", 0, PythonTypes.integer),
            ("NUMBER", 4, PythonTypes.float),
            ("FLOAT", None, PythonTypes.float),
            ("BINARY_DOUBLE", None, PythonTypes.float),
            ("DATE", None, PythonTypes.date),
            ("TIMESTAMP", None, PythonTypes.datetime),
            ("TIMESTAMP(6) WITH TIME ZONE", None, PythonTypes.datetime),
            ("BLOB", None, PythonTypes.byte),
            ("RAW", None, PythonTypes.byte),
            ("JSON", None, PythonTypes.json),
        ],
    )
    def test_oracle_type_to_python(self, raw_type, scale, expected) -> None:
        assert oracle_to_python(raw_type, scale) == expected

    @pytest.mark.parametrize(
        "python_type, expected_prefix",
        [
            (PythonTypes.string, "VARCHAR2"),
            (PythonTypes.integer, "NUMBER"),
            (PythonTypes.float, "NUMBER"),
            (PythonTypes.boolean, "VARCHAR2"),
            (PythonTypes.datetime, "TIMESTAMP"),
            (PythonTypes.date, "DATE"),
            (PythonTypes.time, "VARCHAR2"),
            (PythonTypes.byte, "BLOB"),
            (PythonTypes.json, "JSON"),
        ],
    )
    def test_python_to_oracle(self, python_type, expected_prefix) -> None:
        col = Column(name="c", python_type=python_type, max_length=80, precision=10, scale=2)
        assert python_to_oracle(col).startswith(expected_prefix)

    def test_string_without_length_becomes_clob(self) -> None:
        col = Column(name="c", python_type=PythonTypes.string, max_length=None)
        assert python_to_oracle(col) == "CLOB"

    def test_long_string_becomes_clob(self) -> None:
        col = Column(name="c", python_type=PythonTypes.string, max_length=5000)
        assert python_to_oracle(col) == "CLOB"

    @pytest.mark.parametrize("python_type", list(PythonTypes))
    def test_every_python_type_produces_valid_oracle_ddl(self, python_type) -> None:
        """python_to_oracle output must yield a column OracleColumn can render as DDL."""
        col = Column(name="c", python_type=python_type, max_length=80, precision=10, scale=2)
        raw = python_to_oracle(col).split("(")[0].strip()
        ora_col = OracleColumn(
            name="c", raw_type=raw, python_type=python_type, max_length=80,
            precision=10, scale=2,
        )
        ddl = ora_col.column_definition()
        assert ddl.startswith("C ")  # "<name> <type> <null>"

    @pytest.mark.parametrize(
        "python_type, scale",
        [
            (PythonTypes.string, None),
            (PythonTypes.integer, 0),
            (PythonTypes.float, 2),
            (PythonTypes.datetime, None),
            (PythonTypes.date, None),
            (PythonTypes.byte, None),
            (PythonTypes.json, None),
        ],
    )
    def test_python_oracle_round_trip(self, python_type, scale) -> None:
        """Python type -> Oracle raw type -> Python type is lossless for these."""
        col = Column(name="c", python_type=python_type, max_length=80, precision=10, scale=scale)
        raw = python_to_oracle(col).split("(")[0].strip()
        assert oracle_to_python(raw, scale) == python_type


class TestNormalizeCell:
    def test_none_passes_through(self) -> None:
        assert normalize_cell("VARCHAR2", None) is None

    def test_blank_string_becomes_none(self) -> None:
        assert normalize_cell("VARCHAR2", "   ") is None

    def test_number_parses_decimal(self) -> None:
        assert normalize_cell("NUMBER", "3.14") == Decimal("3.14")

    def test_number_strips_thousands_separators(self) -> None:
        assert normalize_cell("NUMBER", "1,234") == Decimal("1234")

    def test_date_parses(self) -> None:
        assert normalize_cell("DATE", "2020-01-01") == datetime.date(2020, 1, 1)

    def test_timestamp_parses(self) -> None:
        result = normalize_cell("TIMESTAMP", "2024-06-15T12:00:00")
        assert result == datetime.datetime(2024, 6, 15, 12, 0, 0)

    def test_varchar_passthrough(self) -> None:
        assert normalize_cell("VARCHAR2", "hello") == "hello"

    @pytest.mark.parametrize("value, expected", [(True, "Y"), (False, "N")])
    def test_boolean_becomes_y_or_n(self, value, expected) -> None:
        assert normalize_cell("VARCHAR2", value, PythonTypes.boolean) == expected



# Cross-system name / column resolution

class TestOracleSnake:
    @pytest.mark.parametrize(
        "value, expected",
        [
            ("LastName", "LASTNAME"),
            ("AccountId", "ACCOUNTID"),
            ("Id", "ID"),
            ("already_snake", "ALREADY_SNAKE"),
            ("LAST_NAME", "LAST_NAME"),
            ("InPRS_PensionIdentification__c", "INPRS_PENSIONIDENTIFICATION__C"),
        ],
    )
    def test_snake_cases(self, value, expected) -> None:
        assert to_oracle_snake(value) == expected

    def test_idempotent_on_its_own_output(self) -> None:
        once = to_oracle_snake("LastName")
        assert to_oracle_snake(once) == once

    def test_camelcase_is_not_split(self) -> None:
        assert to_oracle_snake("LastName") == "LASTNAME"
        assert to_oracle_snake("LastName") != to_oracle_snake("LAST_NAME")

    def test_reserved_word_is_escaped(self) -> None:
        result = to_oracle_snake("Select")
        assert result not in ORACLE_RESERVED
        assert "SELECT" in result

    def test_leading_digit_is_prefixed(self) -> None:
        result = to_oracle_snake("123abc")
        assert not result[0].isdigit()


class TestCrossSystemName:
    def test_sf_to_oracle_prepends_sf(self) -> None:
        assert resolve_cross_system_name("Contact", System.salesforce, System.oracle) == "SF_Contact"

    def test_oracle_to_sf_strips_sf_prefix_on_return(self) -> None:
        assert resolve_cross_system_name("SF_CONTACT", System.oracle, System.salesforce) == "CONTACT"

    def test_oracle_to_sf_prefixes(self) -> None:
        assert resolve_cross_system_name("ACCOUNT", System.oracle, System.salesforce) == "ora_account__c"

    def test_sf_to_oracle_strips_ora_prefix_on_return(self) -> None:
        assert resolve_cross_system_name("ora_account__c", System.salesforce, System.oracle) == "account"

    def test_same_system_is_unchanged(self) -> None:
        assert resolve_cross_system_name("Foo", System.oracle, System.oracle) == "Foo"

    def test_sf_oracle_sf_round_trip(self) -> None:
        ora = resolve_cross_system_name("Contact", System.salesforce, System.oracle)
        back = resolve_cross_system_name(ora, System.oracle, System.salesforce)
        assert back.upper() == "CONTACT"


class TestRenameMap:
    # Columns correlate by their canonical to_oracle_snake() signature. Oracle
    # target columns are derived from the SF source names through that same
    # function, so a camelCase SF name and its upper-cased Oracle counterpart
    # share a signature ("LastName" -> "LASTNAME" <- "LASTNAME").
    def test_sf_to_oracle_renames_keys(self) -> None:
        source = [Column(name="LastName"), Column(name="Id")]
        target = [Column(name="LASTNAME"), Column(name="ID")]
        assert _build_rename_map(source, target) == {"LastName": "LASTNAME", "Id": "ID"}

    def test_oracle_to_sf_renames_keys(self) -> None:
        source = [Column(name="LASTNAME"), Column(name="ID")]
        target = [Column(name="LastName"), Column(name="Id")]
        assert _build_rename_map(source, target) == {"LASTNAME": "LastName", "ID": "Id"}

    def test_unmatched_columns_are_omitted(self) -> None:
        source = [Column(name="LastName"), Column(name="Orphan")]
        target = [Column(name="LASTNAME")]
        assert _build_rename_map(source, target) == {"LastName": "LASTNAME"}
