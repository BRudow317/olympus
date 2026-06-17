"""redaction -- a standalone, system-agnostic library of redactive actions.

This package is the program's single stable boundary for data redaction. It
operates only on plain values and dict records (it never imports Records,
Column, or any data-source type), so it can be reused unchanged against any
future data source or asset.

    The clear boundary: import only from `src.redaction`.

Everything below (`discovery`, `strategies`, `redactor`, `modes`) is internal
and may change; the names re-exported here are the supported surface. The
program's job is to (1) construct a Redactor from the CLI mode and (2) hand it a
{field -> scope} plan via redact_record. Deciding *which* fields are sensitive is
the program's concern (see src/rules.py) and stays on its side of the boundary.

Roadmap (already-declared deps cryptography + polars enable this):
  * cryptography-backed FedRAMP strategies -- format-preserving tokenization and
    reversible AES-GCM envelope encryption, added in strategies.py behind the
    existing MaskStrategy protocol.
  * polars-vectorized discovery -- bulk/columnar PII scanning over batches,
    consuming the same discovery.PII_PATTERNS registry.
"""
from __future__ import annotations

from src.redaction.modes import (
    DEFAULT_REDACTION_MODE,
    REDACTION_MODES,
    REDACTION_SECRET_ENV,
    RedactionMode,
    RedactionScope,
    normalize_redaction_mode,
)
from src.redaction.redactor import Redactor

__all__ = [
    "Redactor",
    "RedactionMode",
    "RedactionScope",
    "REDACTION_MODES",
    "DEFAULT_REDACTION_MODE",
    "REDACTION_SECRET_ENV",
    "normalize_redaction_mode",
]
