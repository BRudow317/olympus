"""rules.py -- per-(system, object, field) ETL overrides.

This is the single source of truth for field-level policy that deviates from the
default describe-driven behavior. Rules are keyed on the SOURCE system + object +
field (e.g. the Salesforce object 'Case' and field 'Description'), because that
is where the policy is meaningful; the orchestrator applies them before the
target's own name/type translation runs.

Two independent kinds of rule, both expressed on the same Rule object:

  * force_type -- create the target column as a specific Oracle type instead of
    the one inferred from the source describe. Used for fields the source
    under-reports as a bounded string but which are really free text (e.g.
    Case.Description -> CLOB), so we provision the CLOB up front rather than
    relying on Oracle's reactive VARCHAR2->CLOB promotion on overflow.

  * ssn -- the field carries Social Security data. 'field' means the whole value
    is an SSN (e.g. SSN__c); 'scan' means it is free text that may contain SSNs
    embedded in it (e.g. Case.Description). WHICH fields are SSN-bearing lives
    here; HOW they are redacted (full / partial / hashed mask, or not at all) is
    a runtime decision driven by the --redaction CLI flag, not baked in here.
"""
from __future__ import annotations

import hashlib
import hmac
import os
import re
from dataclasses import dataclass

from src.models import Column, System

logger = __import__("logging").getLogger(__name__)


# --- rule registry ---------------------------------------------------------

# ssn rule kinds
SSN_FIELD = "field"   # the entire value is an SSN
SSN_SCAN = "scan"     # free text; redact SSN patterns found inside it


@dataclass(frozen=True)
class Rule:
    force_type: str | None = None   # e.g. "CLOB"
    ssn: str | None = None          # SSN_FIELD | SSN_SCAN | None


# Keyed by (system, object, field). Object/field are matched case-insensitively
# (see _rule_key); the system is the SOURCE system the object lives in.
FIELD_RULES: dict[tuple[str, str, str], Rule] = {
    ("salesforce", "case", "description"): Rule(force_type="CLOB", ssn=SSN_SCAN),
    ("salesforce", "contact", "ssn__c"): Rule(ssn=SSN_FIELD),
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


# --- redaction -------------------------------------------------------------

# Canonical redaction modes (underscored). The CLI accepts the hyphenated forms
# too (full-mask, part-mask, hash-mask) -- see normalize_redaction_mode.
REDACTION_NONE = "none"
REDACTION_FULL = "full_mask"
REDACTION_PART = "part_mask"
REDACTION_HASH = "hash_mask"
REDACTION_MODES: tuple[str, ...] = (
    REDACTION_NONE, REDACTION_FULL, REDACTION_PART, REDACTION_HASH,
)

# Env var holding the secret key for hash_mask (injected from .env by charon.py).
REDACTION_SECRET_ENV = "REDACTION_SECRET"

# Matches 123-45-6789, 123 45 6789, and the bare 9-digit 123456789. The optional
# separators mean this also catches zip+4-shaped strings -- that recall/precision
# tradeoff (formatted + bare 9-digit) is an intentional, configured choice.
SSN_RE: re.Pattern[str] = re.compile(r"\b\d{3}[-\s]?\d{2}[-\s]?\d{4}\b")

_NON_DIGIT_RE: re.Pattern[str] = re.compile(r"\D")


def normalize_redaction_mode(value: str) -> str:
    """Coerce a CLI redaction value to a canonical mode.

    Accepts hyphen or underscore separators (full-mask == full_mask) because the
    calling library passes whichever it forces. Raises ValueError on an unknown
    mode so argparse surfaces a clear error.
    """
    normalized = value.strip().lower().replace("-", "_")
    if normalized not in REDACTION_MODES:
        allowed = ", ".join(REDACTION_MODES)
        raise ValueError(f"invalid redaction mode '{value}' (choose from: {allowed})")
    return normalized


class Redactor:
    """Applies a single redaction strategy to SSN values and free text.

    The strategy is fixed at construction from the runtime mode; the registry
    decides which fields it touches. mode='none' yields a disabled redactor.
    """

    def __init__(self, mode: str = REDACTION_NONE) -> None:
        self.mode = normalize_redaction_mode(mode)
        self._key: bytes | None = None
        if self.mode == REDACTION_HASH:
            secret = os.environ.get(REDACTION_SECRET_ENV)
            if not secret:
                raise RuntimeError(
                    f"hash-mask redaction requires {REDACTION_SECRET_ENV} in the "
                    f"environment (charon.py injects it from .env)."
                )
            self._key = secret.encode()

    @property
    def enabled(self) -> bool:
        return self.mode != REDACTION_NONE

    def _token(self, digits: str) -> str:
        """Render the masked replacement for the given SSN digits."""
        if self.mode == REDACTION_FULL:
            return "***-**-****"
        if self.mode == REDACTION_PART:
            last4 = digits[-4:] if len(digits) >= 4 else "****"
            return f"***-**-{last4}"
        # hash_mask: stable, irreversible token; same SSN -> same token, so
        # de-dupe and joins survive without exposing the value.
        assert self._key is not None  # guaranteed by __init__ for this mode
        mac = hmac.new(self._key, digits.encode(), hashlib.sha256).hexdigest()[:12]
        return f"SSN:{mac}"

    def redact_value(self, value: object) -> object:
        """Whole-field redaction: the value itself is an SSN."""
        if not self.enabled or value is None:
            return value
        text = str(value).strip()
        if not text:
            return value
        return self._token(_NON_DIGIT_RE.sub("", text))

    def scan_text(self, value: object) -> object:
        """Free-text redaction: replace any SSN patterns found in place."""
        if not self.enabled or value is None:
            return value
        text = str(value)
        return SSN_RE.sub(
            lambda m: self._token(_NON_DIGIT_RE.sub("", m.group(0))), text
        )


def ssn_fields(
    system: System | str, object_name: str, columns: list[Column]
) -> dict[str, str]:
    """Map source field name -> ssn kind (SSN_FIELD | SSN_SCAN) for a table."""
    out: dict[str, str] = {}
    for col in columns:
        rule = rule_for(system, object_name, col.name)
        if rule is not None and rule.ssn is not None:
            out[col.name] = rule.ssn
    return out
