"""test_rules.py -- pure (no-DB) tests for the field-rules + redaction layer."""
from __future__ import annotations

import pytest

from src.models import Column, PythonTypes, System
from src.oracle.OracleTypeMap import python_to_oracle
from src.rules import (
    Redactor,
    Rule,
    SSN_FIELD,
    SSN_SCAN,
    apply_structural_rules,
    normalize_redaction_mode,
    rule_for,
    ssn_fields,
    FORCE_TYPE_PROPERTY,
    REDACTION_SECRET_ENV,
)


# --- mode normalization ----------------------------------------------------

class TestRedactionModeNormalization:
    @pytest.mark.parametrize(
        "raw, expected",
        [
            ("none", "none"),
            ("full-mask", "full_mask"),
            ("full_mask", "full_mask"),
            ("part-mask", "part_mask"),
            ("part_mask", "part_mask"),
            ("hash-mask", "hash_mask"),
            ("HASH_MASK", "hash_mask"),
            ("  Full-Mask  ", "full_mask"),
        ],
    )
    def test_accepts_hyphen_and_underscore(self, raw: str, expected: str) -> None:
        assert normalize_redaction_mode(raw) == expected

    def test_rejects_unknown_mode(self) -> None:
        with pytest.raises(ValueError):
            normalize_redaction_mode("scramble")


# --- registry lookup -------------------------------------------------------

class TestRuleLookup:
    def test_lookup_is_case_insensitive(self) -> None:
        assert rule_for("salesforce", "CASE", "Description") is not None
        assert rule_for(System.salesforce, "case", "DESCRIPTION") is not None

    def test_unknown_field_returns_none(self) -> None:
        assert rule_for("salesforce", "Case", "Subject") is None

    def test_ssn_fields_map(self) -> None:
        cols = [Column(name="Description"), Column(name="SSN__c"), Column(name="Subject")]
        # Description lives on Case, SSN__c on Contact -- check each object scope.
        assert ssn_fields("salesforce", "Case", cols) == {"Description": SSN_SCAN}
        assert ssn_fields("salesforce", "Contact", cols) == {"SSN__c": SSN_FIELD}


# --- structural overrides --------------------------------------------------

class TestStructuralRules:
    def test_force_clob_stamps_property_and_clears_length(self) -> None:
        col = Column(name="Description", python_type=PythonTypes.string, max_length=255)
        apply_structural_rules("salesforce", "Case", [col])
        assert col.properties[FORCE_TYPE_PROPERTY] == "CLOB"
        assert col.max_length is None

    def test_forced_type_wins_in_python_to_oracle(self) -> None:
        col = Column(name="Description", python_type=PythonTypes.string, max_length=255)
        # Without the rule this short string maps to a bounded VARCHAR2.
        assert python_to_oracle(col).startswith("VARCHAR2")
        apply_structural_rules("salesforce", "Case", [col])
        assert python_to_oracle(col) == "CLOB"

    def test_no_rule_leaves_column_untouched(self) -> None:
        col = Column(name="Subject", python_type=PythonTypes.string, max_length=80)
        apply_structural_rules("salesforce", "Case", [col])
        assert FORCE_TYPE_PROPERTY not in col.properties
        assert col.max_length == 80


# --- redaction strategies --------------------------------------------------

class TestRedactor:
    def test_none_is_disabled_noop(self) -> None:
        r = Redactor("none")
        assert r.enabled is False
        assert r.redact_value("123-45-6789") == "123-45-6789"
        assert r.scan_text("ssn 123-45-6789") == "ssn 123-45-6789"

    def test_full_mask_whole_field(self) -> None:
        r = Redactor("full-mask")
        assert r.redact_value("123-45-6789") == "***-**-****"
        assert r.redact_value("123456789") == "***-**-****"

    def test_part_mask_keeps_last_four(self) -> None:
        r = Redactor("part-mask")
        assert r.redact_value("123-45-6789") == "***-**-6789"
        assert r.redact_value("123456789") == "***-**-6789"

    def test_whole_field_preserves_none_and_blank(self) -> None:
        r = Redactor("full-mask")
        assert r.redact_value(None) is None
        assert r.redact_value("   ") == "   "

    @pytest.mark.parametrize(
        "text, masked",
        [
            ("pt SSN 123-45-6789, call back", "pt SSN ***-**-****, call back"),
            ("spaced 123 45 6789 here", "spaced ***-**-**** here"),
            ("bare 123456789 nine", "bare ***-**-**** nine"),
            ("no ssn here", "no ssn here"),
        ],
    )
    def test_scan_text_redacts_inline(self, text: str, masked: str) -> None:
        assert Redactor("full-mask").scan_text(text) == masked

    def test_scan_text_part_mask_inline(self) -> None:
        assert Redactor("part-mask").scan_text("x 123-45-6789 y") == "x ***-**-6789 y"

    def test_hash_requires_secret(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.delenv(REDACTION_SECRET_ENV, raising=False)
        with pytest.raises(RuntimeError):
            Redactor("hash-mask")

    def test_hash_is_deterministic_and_irreversible(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv(REDACTION_SECRET_ENV, "unit-test-key")
        r = Redactor("hash-mask")
        a = r.redact_value("123-45-6789")
        b = r.redact_value("123456789")  # same digits, different format
        c = r.redact_value("987-65-4321")
        assert isinstance(a, str)
        assert a == b               # stable across formatting
        assert a != c               # distinct SSNs -> distinct tokens
        assert "6789" not in a      # raw digits not present
        assert a.startswith("SSN:")
