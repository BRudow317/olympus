"""test_redaction.py -- pure (no-DB) tests for the standalone redaction package.

Imports only through the package boundary (src.redaction), mirroring how the
program is meant to use it.
"""
from __future__ import annotations

import pytest

from src.redaction import (
    Redactor,
    RedactionScope,
    REDACTION_SECRET_ENV,
    normalize_redaction_mode,
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


# --- record boundary primitive ---------------------------------------------

class TestRedactRecord:
    def test_applies_plan_by_scope(self) -> None:
        r = Redactor("full-mask")
        row = {"SSN__c": "123-45-6789", "Description": "call re 222-33-4444", "Subject": "hi"}
        plan = {"SSN__c": RedactionScope.field, "Description": RedactionScope.scan}
        out = r.redact_record(row, plan)
        assert out["SSN__c"] == "***-**-****"           # whole-field
        assert out["Description"] == "call re ***-**-****"  # scanned in place
        assert out["Subject"] == "hi"                   # untouched

    def test_missing_field_is_skipped(self) -> None:
        r = Redactor("full-mask")
        out = r.redact_record({"a": 1}, {"SSN__c": RedactionScope.field})
        assert out == {"a": 1}

    def test_disabled_redactor_returns_plain_dict_copy(self) -> None:
        r = Redactor("none")
        row = {"SSN__c": "123-45-6789"}
        out = r.redact_record(row, {"SSN__c": RedactionScope.field})
        assert out == row
        assert out is not row
