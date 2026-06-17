"""redaction.modes -- redaction strategy selectors and field-targeting scope.

Pure configuration: enums + a normalizer, no behavior. Kept separate from the
engine so the CLI and the program's rule registry can reference these without
importing the strategy/discovery machinery.
"""
from __future__ import annotations

from enum import StrEnum

# Env var holding the secret key for hash_mask (injected from .env by charon.py).
REDACTION_SECRET_ENV = "REDACTION_SECRET"


class RedactionMode(StrEnum):
    """How a detected sensitive value is transformed."""
    none = "none"            # redaction disabled
    full_mask = "full_mask"  # ***-**-****
    part_mask = "part_mask"  # keep last 4: ***-**-1234
    hash_mask = "hash_mask"  # deterministic, irreversible HMAC token


REDACTION_MODES: tuple[str, ...] = tuple(m.value for m in RedactionMode)
DEFAULT_REDACTION_MODE: str = RedactionMode.none.value


class RedactionScope(StrEnum):
    """How a field is targeted for redaction.

    field -- the whole value is sensitive (e.g. an SSN column); redact it outright.
    scan  -- free text that may contain PII embedded in it; find and redact the
             matches in place, leaving the surrounding text intact.
    """
    field = "field"
    scan = "scan"


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
