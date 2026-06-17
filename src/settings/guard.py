"""guard.py -- the destructive-operation safety guard.

The guard answers one question at each dangerous SQL/DML site: "may I perform
this destructive operation on this table, in this environment, right now?" Its
real design goal is extensibility -- new safety checks should be *added*, never
woven into existing logic -- so it is built as a pipeline of independent veto
rules rather than one branching switch:

    permitted  <=>  no rule returns a denial reason

A rule's permits() returns None to abstain (no objection) or a human-readable
string to veto. Adding "safety check X" later means writing one GuardRule and
appending it to the guard; the existing rules never change.

Two rules ship today:

  * ModeRule -- the four-level --destructive dial:
      no         : block every destructive op.
      restricted : allow only on tool-managed tables (Oracle SF_*, Salesforce
                   ora_*__c) -- i.e. tables this tool itself created.
      dev_only   : allow only in non-production environments.
      yes        : abstain (allow), subject to the rules below.
  * ProdBreakGlassRule -- independent of the mode, vetoes DATA-LOSS ops
    (TRUNCATE / DROP) in a production environment unless an explicit break-glass
    env var is set. This is what guarantees no single dropdown value can wipe
    prod, even with --destructive yes.

Additive ALTERs (add column, widen, promote-to-CLOB) are classed STRUCTURAL, not
DATA_LOSS, so they pass the break-glass rule -- routine loads that reconcile
schema are never blocked just for touching a managed table.
"""
from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from enum import StrEnum
from typing import Protocol, runtime_checkable

from src.models import EnvironmentClass, System
from src.settings.environments import is_production

logger = logging.getLogger(__name__)

# Env var that lifts the production break-glass stop for TRUNCATE/DROP.
PROD_BREAK_GLASS_ENV = "ALLOW_PROD_DESTRUCTIVE"


class DestructiveOp(StrEnum):
    truncate = "truncate"   # data loss: empties a table
    drop = "drop"           # data loss: removes a table
    alter = "alter"         # structural: add/widen/promote a column (additive)


# Ops that destroy data vs. ops that only change structure.
DATA_LOSS_OPS: frozenset[DestructiveOp] = frozenset({DestructiveOp.truncate, DestructiveOp.drop})


class DestructiveMode(StrEnum):
    no = "no"
    restricted = "restricted"
    dev_only = "dev_only"
    yes = "yes"


DESTRUCTIVE_MODES: tuple[str, ...] = tuple(m.value for m in DestructiveMode)
DEFAULT_DESTRUCTIVE_MODE = DestructiveMode.restricted.value


def normalize_destructive_mode(value: str) -> str:
    """Coerce a CLI value to a canonical mode, accepting hyphen or underscore.

    'dev-only' and 'dev_only' are equivalent (the calling library forces one or
    the other). Raises ValueError on an unknown mode so argparse reports it.
    """
    normalized = value.strip().lower().replace("-", "_")
    if normalized not in DESTRUCTIVE_MODES:
        allowed = ", ".join(DESTRUCTIVE_MODES)
        raise ValueError(f"invalid destructive mode '{value}' (choose from: {allowed})")
    return normalized


@dataclass(frozen=True)
class GuardRequest:
    """The facts a rule needs to decide one destructive operation."""
    op: DestructiveOp
    system: System
    environment: str
    env_class: EnvironmentClass
    table_name: str
    is_managed: bool


@runtime_checkable
class GuardRule(Protocol):
    def permits(self, req: GuardRequest) -> str | None:
        """Return None to abstain/allow, or a denial reason to veto."""
        ...


class DestructivePermissionError(RuntimeError):
    """Raised when the guard refuses a destructive operation."""

    def __init__(self, req: GuardRequest, reasons: list[str]) -> None:
        self.request = req
        self.reasons = reasons
        joined = "; ".join(reasons)
        super().__init__(
            f"Destructive {req.op.value.upper()} on '{req.table_name}' "
            f"({req.system.value}/{req.environment}, classified {req.env_class.value}) "
            f"was blocked: {joined}."
        )


class ModeRule:
    """The four-level --destructive dial, applied uniformly to every op."""

    def __init__(self, mode: str) -> None:
        self.mode = DestructiveMode(normalize_destructive_mode(mode))

    def permits(self, req: GuardRequest) -> str | None:
        if self.mode == DestructiveMode.yes:
            return None
        if self.mode == DestructiveMode.no:
            return "all destructive operations are disabled (--destructive no)"
        if self.mode == DestructiveMode.restricted:
            if req.is_managed:
                return None
            return (
                f"--destructive restricted only permits tool-managed tables "
                f"(Oracle SF_*, Salesforce ora_*__c); '{req.table_name}' is not managed"
            )
        # dev_only
        if is_production(req.env_class):
            return (
                f"--destructive dev-only forbids destructive ops in "
                f"'{req.environment}' (classified {req.env_class.value})"
            )
        return None


class ProdBreakGlassRule:
    """Hard stop on data-loss ops in production unless break-glass is set.

    Independent of the mode, so it still fires under --destructive yes. Reads the
    env var at evaluation time so it can be flipped per run.
    """

    def permits(self, req: GuardRequest) -> str | None:
        if req.op not in DATA_LOSS_OPS:
            return None
        if not is_production(req.env_class):
            return None
        if _truthy(os.environ.get(PROD_BREAK_GLASS_ENV)):
            logger.warning(
                "Break-glass: permitting %s on production '%s' because %s is set.",
                req.op.value.upper(), req.table_name, PROD_BREAK_GLASS_ENV,
            )
            return None
        return (
            f"production data-loss requires break-glass: set {PROD_BREAK_GLASS_ENV}=1 "
            f"to allow {req.op.value.upper()} in '{req.environment}'"
        )


def _truthy(value: str | None) -> bool:
    return (value or "").strip().lower() in ("1", "true", "yes", "on")


class DestructiveGuard:
    """Composes guard rules; enforce() raises unless every rule permits.

    Extend the policy by passing extra rules (or appending to .rules) -- the
    default pipeline is [ModeRule(mode), ProdBreakGlassRule()].
    """

    def __init__(self, mode: str = DEFAULT_DESTRUCTIVE_MODE, *, rules: list[GuardRule] | None = None) -> None:
        self.mode = normalize_destructive_mode(mode)
        self.rules: list[GuardRule] = rules if rules is not None else [
            ModeRule(self.mode),
            ProdBreakGlassRule(),
        ]

    def permits(self, req: GuardRequest) -> list[str]:
        """Return the list of denial reasons (empty list == permitted)."""
        return [reason for rule in self.rules if (reason := rule.permits(req))]

    def enforce(self, req: GuardRequest) -> None:
        reasons = self.permits(req)
        if reasons:
            raise DestructivePermissionError(req, reasons)
        logger.debug(
            "Guard permitted %s on '%s' (%s/%s, mode=%s).",
            req.op.value, req.table_name, req.system.value, req.environment, self.mode,
        )
