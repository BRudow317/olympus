"""settings.rules -- per-(system, object, field) ETL overrides.

A self-contained orchestration-layer component: the curated registry, its schema
(Rule), and the lookup/apply logic live together so an admin edits one file.
Rules are keyed on the SOURCE system + object + field (e.g. the Salesforce object
'Case' and field 'Description').

Two independent kinds of rule, both expressed on the same Rule object:

  * force_type -- create the target column as a specific Oracle type instead of
    the one inferred from the source describe (e.g. Case.Description -> CLOB), so
    we provision it up front rather than relying on Oracle's reactive
    VARCHAR2->CLOB promotion on overflow.

  * redact -- the field is sensitive. RedactionScope.field means the whole value
    is redacted; RedactionScope.scan means it is free text to be scanned for PII.
    This module only decides WHICH fields and at what scope; the actual redaction
    engine lives behind the boundary in src/redaction. HOW values are redacted
    (full / partial / hashed) is a runtime choice (the --redaction CLI flag).
"""
from __future__ import annotations

import logging
from dataclasses import dataclass

from src.models import Column, System
from src.redaction import RedactionScope

logger = logging.getLogger(__name__)


# --- rule registry ---------------------------------------------------------

@dataclass(frozen=True)
class Rule:
    force_type: str | None = None              # e.g. "CLOB"
    redact: RedactionScope | None = None       # field | scan | None


# Keyed by (system, object, field). Object/field are matched case-insensitively
# (see _rule_key); the system is the SOURCE system the object lives in.
FIELD_RULES: dict[tuple[str, str, str], Rule] = {
    ("salesforce", "case", "description"): Rule(force_type="CLOB", redact=RedactionScope.scan),
    ("salesforce", "contact", "ssn__c"): Rule(redact=RedactionScope.field),
}


def _rule_key(system: System | str, object_name: str, field_name: str) -> tuple[str, str, str]:
    return (str(System(system)), object_name.strip().lower(), field_name.strip().lower())


def rule_for(system: System | str, object_name: str, field_name: str) -> Rule | None:
    """Return the Rule registered for a source field, or None."""
    return FIELD_RULES.get(_rule_key(system, object_name, field_name))


# --- structural overrides --------------------------------------------------

# Stashed on Column.properties; honored by OracleTypeMap.python_to_oracle so the
# forced type wins over the type inferred from the column's python_type.
FORCE_TYPE_PROPERTY = "force_oracle_type"


def apply_structural_rules(
    system: System | str, object_name: str, columns: list[Column]
) -> None:
    """Stamp force_type overrides onto the source columns in place.

    Run before the target builds its table so the override flows through the
    normal type translation. Only mutates columns that have a matching rule.
    """
    for col in columns:
        rule = rule_for(system, object_name, col.name)
        if rule is None or rule.force_type is None:
            continue
        col.properties[FORCE_TYPE_PROPERTY] = rule.force_type
        if rule.force_type.upper() in ("CLOB", "NCLOB", "BLOB"):
            # LOBs have no character length; clear it so downstream sizing logic
            # never treats the column as a bounded VARCHAR2.
            col.max_length = None
        logger.info(
            "Rule: forcing %s.%s -> %s", object_name, col.name, rule.force_type
        )


# --- redaction plan (handed across the boundary to src/redaction) ----------

def redaction_fields(
    system: System | str, object_name: str, columns: list[Column]
) -> dict[str, RedactionScope]:
    """Build the {source field name -> RedactionScope} plan for a table.

    This is the program's side of the redaction boundary: it decides which
    fields are sensitive and at what scope, then hands the plan to a Redactor.
    """
    plan: dict[str, RedactionScope] = {}
    for col in columns:
        rule = rule_for(system, object_name, col.name)
        if rule is not None and rule.redact is not None:
            plan[col.name] = rule.redact
    return plan
