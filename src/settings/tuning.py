"""settings.tuning -- the program's canonical operator-tunable knobs.

These are values an operator might adjust for performance/behavior. They are the
program's *preferred* values, but the Oracle/SF connectors deliberately do NOT
import this module: each connector reads its knob from a CHARON_* environment
variable at use-time with its OWN local fallback default, so a connector keeps
working (on its default) if this settings package is removed -- which is what
lets the connectors be extracted as standalone packages later.

apply() publishes these values into the environment via setdefault, so:

    explicit env / .env  >  settings.tuning (this file)  >  connector's own default

Call apply() once at the composition root (it runs at the start of the seeding
service) before any connector work, so use-time reads observe the published values.
"""
from __future__ import annotations

import os

# --- Oracle ---
# Extra chars added when auto-widening a VARCHAR2 to fit oversized source data.
VARCHAR2_GROWTH_BUFFER = 50

# --- Salesforce request routing ---
# Encoded SOQL length above which a query is sent via Bulk API 2.0 (POST body)
# instead of the GET /query request line (HTTP 431 cap).
SF_SOQL_GET_MAX_ENCODED = 6000
# Object row count above which extraction is routed through Bulk API 2.0.
SF_BULK_RECORD_THRESHOLD = 2000


# Canonical knob -> CHARON_* env var the connectors read.
_ENV_VALUES: dict[str, int] = {
    "CHARON_VARCHAR2_GROWTH_BUFFER": VARCHAR2_GROWTH_BUFFER,
    "CHARON_SF_SOQL_GET_MAX_ENCODED": SF_SOQL_GET_MAX_ENCODED,
    "CHARON_SF_BULK_RECORD_THRESHOLD": SF_BULK_RECORD_THRESHOLD,
}


def apply() -> None:
    """Publish tuning values into the environment (setdefault: explicit env wins)."""
    for key, value in _ENV_VALUES.items():
        os.environ.setdefault(key, str(value))
