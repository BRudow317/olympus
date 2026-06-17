"""redaction.strategies -- the transforms applied to a detected sensitive value.

Each strategy implements MaskStrategy.apply(matched) -> replacement. New
transforms (and this is the path to FedRAMP-grade redaction) implement the same
protocol and are wired in build_strategy:

  * format-preserving encryption / tokenization (cryptography-backed, reversible
    under a managed key),
  * AES-GCM envelope encryption for reversible vault storage,

all behind the same boundary, so callers never change. HashMaskStrategy already
uses the `cryptography` primitives (HMAC-SHA256) rather than stdlib hashlib so
the hashing path can run under a FIPS-validated provider.
"""
from __future__ import annotations

from typing import Protocol, runtime_checkable

from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives import hmac as crypto_hmac

from src.redaction.discovery import digits_only
from src.redaction.modes import RedactionMode


@runtime_checkable
class MaskStrategy(Protocol):
    def apply(self, matched: str) -> str:
        """Return the replacement for a single matched sensitive value."""
        ...


class FullMaskStrategy:
    """Irreversible fixed mask."""

    def apply(self, matched: str) -> str:
        return "***-**-****"


class PartialMaskStrategy:
    """Mask all but the last four digits."""

    def apply(self, matched: str) -> str:
        digits = digits_only(matched)
        last4 = digits[-4:] if len(digits) >= 4 else "****"
        return f"***-**-{last4}"


class HashMaskStrategy:
    """Deterministic, irreversible token via HMAC-SHA256.

    Same input -> same token, so de-dupe and joins survive without exposing the
    value. Uses the `cryptography` HMAC primitive (FIPS-validatable) keyed by a
    secret the caller resolves from the environment.
    """

    def __init__(self, key: bytes) -> None:
        self._key = key

    def apply(self, matched: str) -> str:
        mac = crypto_hmac.HMAC(self._key, hashes.SHA256())
        mac.update(digits_only(matched).encode())
        return f"SSN:{mac.finalize().hex()[:12]}"


def build_strategy(mode: RedactionMode, *, secret: bytes | None = None) -> MaskStrategy | None:
    """Construct the strategy for a mode, or None when redaction is disabled.

    Raises ValueError if hash_mask is requested without a secret; the caller
    (Redactor) resolves the secret from the environment and reports the missing
    env var with a friendlier message before reaching here.
    """
    if mode == RedactionMode.none:
        return None
    if mode == RedactionMode.full_mask:
        return FullMaskStrategy()
    if mode == RedactionMode.part_mask:
        return PartialMaskStrategy()
    if mode == RedactionMode.hash_mask:
        if not secret:
            raise ValueError("hash_mask strategy requires a non-empty secret key")
        return HashMaskStrategy(secret)
    raise ValueError(f"unsupported redaction mode: {mode}")
