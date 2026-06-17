"""settings -- the orchestration layer's policy + utility components.

Self-contained components (functions and their constants together) that the
services layer composes. Each may later externalize its data to env/JSON, but for
now is hardcoded in its own file:

  * environments.py -- environment classification (which envs are production).
  * guard.py        -- the destructive-op safety guard (truncate/drop/alter).
  * rules.py        -- per-field force-type / redaction overrides.
  * tuning.py       -- operator-tunable knobs published to the env for the
                       Oracle/SF connectors (which keep their own fallbacks).

Note: the Oracle/SF *connectors* still import settings.environments/guard for
their guard hooks (a deferred coupling -- the long-term fix is to invert it
behind a port in the kernel). The connectors deliberately do NOT import
settings.tuning (see that file).
"""
from __future__ import annotations

from src.settings.tuning import apply

__all__ = ["apply"]
