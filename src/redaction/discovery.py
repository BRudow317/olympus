"""redaction.discovery -- PII pattern detection.

The growth surface for sensitive-data discovery. Today it holds the SSN pattern;
new PII types (credit-card, phone, email, MRN, ...) are added by appending a
PiiPattern to PII_PATTERNS, and every free-text scan picks them up automatically.

A future polars-vectorized discovery path (bulk scanning of columnar batches)
would consume the same PiiPattern registry, so detection rules are defined once
here regardless of whether scanning is row-wise or columnar.
"""
from __future__ import annotations

import re
from dataclasses import dataclass

_NON_DIGIT_RE: re.Pattern[str] = re.compile(r"\D")


def digits_only(value: str) -> str:
    """Strip everything but digits (e.g. '123-45-6789' -> '123456789')."""
    return _NON_DIGIT_RE.sub("", value)


@dataclass(frozen=True)
class PiiPattern:
    """A named detector for one kind of sensitive value."""
    name: str
    pattern: re.Pattern[str]


# SSN: matches 123-45-6789, 123 45 6789, and bare 123456789. The optional
# separators mean this also catches zip+4-shaped strings -- that recall-over-
# precision tradeoff (formatted + bare 9-digit) is an intentional choice.
SSN_PATTERN = PiiPattern("ssn", re.compile(r"\b\d{3}[-\s]?\d{2}[-\s]?\d{4}\b"))


# Every pattern a free-text scan looks for. Append to grow coverage.
PII_PATTERNS: tuple[PiiPattern, ...] = (SSN_PATTERN,)
