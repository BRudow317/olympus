"""test_rules.py -- pure (no-DB) tests for the field-rules registry.

Covers the program's side of the boundary: force-type overrides and building the
redaction plan. The redaction engine itself is tested in test_redaction.py.
"""
from __future__ import annotations

from src.models import Column, PythonTypes, System
from src.oracle.OracleTypeMap import python_to_oracle
from src.redaction import RedactionScope
from src.settings.rules import (
    apply_structural_rules,
    redaction_fields,
    rule_for,
    FORCE_TYPE_PROPERTY,
)


# --- registry lookup -------------------------------------------------------

class TestRuleLookup:
    def test_lookup_is_case_insensitive(self) -> None:
        assert rule_for("salesforce", "CASE", "Description") is not None
        assert rule_for(System.salesforce, "case", "DESCRIPTION") is not None

    def test_unknown_field_returns_none(self) -> None:
        assert rule_for("salesforce", "Case", "Subject") is None

    def test_redaction_plan(self) -> None:
        cols = [Column(name="Description"), Column(name="SSN__c"), Column(name="Subject")]
        # Description lives on Case, SSN__c on Contact -- check each object scope.
        assert redaction_fields("salesforce", "Case", cols) == {"Description": RedactionScope.scan}
        assert redaction_fields("salesforce", "Contact", cols) == {"SSN__c": RedactionScope.field}


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
