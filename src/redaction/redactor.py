"""redaction.redactor -- the orchestrator and the program-facing boundary.

Redactor binds a mode to a strategy and exposes three levels of use:
  * redact_value(value)        -- whole value is sensitive
  * scan_text(value)           -- free text; redact embedded PII in place
  * redact_record(row, plan)   -- apply a {field -> scope} plan to one record

redact_record is the primitive the program integrates against: it takes a plain
dict and a plain field->scope mapping, so the engine stays system-agnostic (it
never sees Records, Columns, or any data-source type).
"""
from __future__ import annotations

import os
from collections.abc import Mapping
from typing import Any

from src.redaction.discovery import PII_PATTERNS
from src.redaction.modes import (
    REDACTION_SECRET_ENV,
    RedactionMode,
    RedactionScope,
    normalize_redaction_mode,
)
from src.redaction.strategies import MaskStrategy, build_strategy


class Redactor:
    """Applies a single redaction strategy. mode='none' yields a disabled redactor."""

    def __init__(self, mode: str = RedactionMode.none.value) -> None:
        self.mode = RedactionMode(normalize_redaction_mode(mode))
        secret: bytes | None = None
        if self.mode == RedactionMode.hash_mask:
            raw = os.environ.get(REDACTION_SECRET_ENV)
            if not raw:
                raise RuntimeError(
                    f"hash-mask redaction requires {REDACTION_SECRET_ENV} in the "
                    f"environment (charon.py injects it from .env)."
                )
            secret = raw.encode()
        self._strategy: MaskStrategy | None = build_strategy(self.mode, secret=secret)

    @property
    def enabled(self) -> bool:
        return self._strategy is not None

    def redact_value(self, value: Any) -> Any:
        """Whole-field redaction: the value itself is sensitive."""
        strategy = self._strategy
        if strategy is None or value is None:
            return value
        text = str(value).strip()
        if not text:
            return value
        return strategy.apply(text)

    def scan_text(self, value: Any) -> Any:
        """Free-text redaction: replace every known PII pattern found, in place."""
        strategy = self._strategy
        if strategy is None or value is None:
            return value
        result = str(value)
        for pii in PII_PATTERNS:
            result = pii.pattern.sub(lambda m: strategy.apply(m.group(0)), result)
        return result

    def redact_record(
        self, row: Mapping[str, Any], plan: Mapping[str, RedactionScope | str]
    ) -> dict[str, Any]:
        """Apply a {field -> scope} plan to one record, returning a new dict.

        Fields absent from the row are skipped. A no-op (returns row as a dict)
        when the redactor is disabled or the plan is empty.
        """
        if self._strategy is None or not plan:
            return dict(row)
        new_row = dict(row)
        for field, scope in plan.items():
            if field not in new_row:
                continue
            if RedactionScope(scope) == RedactionScope.field:
                new_row[field] = self.redact_value(new_row[field])
            else:
                new_row[field] = self.scan_text(new_row[field])
        return new_row
