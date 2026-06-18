#!/usr/bin/env python3.11
"""mcp_server.py

MCP (Model Context Protocol) front-end for charon.

This is a thin alternative entry point that sits *beside* `main.py`/`src/app.py`,
not above them. It exposes charon's cross-system seeding as an MCP tool so an MCP
client (Claude Desktop / Claude Code / etc.) can drive migrations.

Design: the tool does NOT import charon internals or call `seeding()` in-process.
It shells out to the real, documented execution path:

    python main.py [-v] --exec src/app.py [app args...]

`main.py` owns env/.env parsing, venv resolution, PYTHONPATH, logging bootstrap,
and child-process streaming (see CLAUDE.md). Going through it keeps this server a
faithful mirror of the CLI instead of a second, divergent code path.

Run standalone (stdio transport):

    python mcp_server.py

Registered for clients via `configs/mcp config.json`.
"""
from __future__ import annotations

import subprocess
import sys
from pathlib import Path

from mcp.server.fastmcp import FastMCP

# Repo root == this file's directory (main.py lives here too).
PROGRAM_ROOT = Path(__file__).resolve().parent
MAIN_PY = PROGRAM_ROOT / "main.py"
APP_PY = PROGRAM_ROOT / "src" / "app.py"

# Mirrors of src/app.py / src/settings / src/redaction choices. Kept as literals
# (not imports) so this server stays decoupled from the package and runnable from
# any cwd. If the CLI choices change, update these to match — they are only used
# for the tool schema / docstring; the CLI is the real validator.
SYSTEMS = ("oracle", "salesforce")
ACTIONS = ("reset", "insert", "upsert", "update", "full_load")
REDACTION_MODES = ("none", "full-mask", "part-mask", "hash-mask")
DESTRUCTIVE_MODES = ("no", "restricted", "dev-only", "yes")

mcp = FastMCP("charon")


def _flag(name: str, value: str | None, cmd: list[str]) -> None:
    """Append `--name value` if value is set."""
    if value is not None and value != "":
        cmd += [name, str(value)]


@mcp.tool()
def migrate(
    source_system: str,
    source_environment: str,
    target_system: str,
    target_environment: str,
    source_namespace: str | None = None,
    target_namespace: str | None = None,
    action: str = "reset",
    tables: list[str] | None = None,
    external_id_field: str | None = None,
    scripts: list[str] | None = None,
    redaction: str = "none",
    destructive: str = "restricted",
    verbose: bool = False,
    config: str | None = None,
    venv: str | None = None,
    timeout_seconds: int = 3600,
) -> str:
    """Run a charon cross-system data migration (seeding).

    Moves records from a source to a target in any direction
    (sf->oracle, oracle->sf, oracle->oracle, sf->sf). Wraps the
    `python main.py --exec src/app.py ...` CLI; output is the combined
    stdout/stderr plus the exit code.

    Args:
        source_system: Source connector. One of: oracle, salesforce.
        source_environment: Source env name (e.g. dev01, sit01, devint).
        target_system: Target connector. One of: oracle, salesforce.
        target_environment: Target env name.
        source_namespace: Optional source namespace/schema (Oracle schema or SF namespace).
        target_namespace: Optional target namespace/schema.
        action: How records are written. One of: reset, insert, upsert, update,
            full_load. Default 'reset' (truncate then insert). 'full_load' drops
            and recreates (Oracle) / deletes all rows then inserts (Salesforce).
        tables: Table/object names to migrate, or ['*'] for the whole schema (default).
        external_id_field: External id field for Salesforce upsert (defaults to 'Id').
        scripts: Optional post-migration .sql scripts to run on the target (Oracle).
        redaction: SSN redaction mode. One of: none, full-mask, part-mask, hash-mask.
            'hash-mask' requires REDACTION_SECRET in the environment.
        destructive: Safety gate for truncate/drop/alter. One of: no, restricted,
            dev-only, yes. Default 'restricted' (only tool-managed tables). Production
            truncate/drop additionally requires ALLOW_PROD_DESTRUCTIVE in the env.
        verbose: Enable debug logging in the child process.
        config: Path to an env config file (key=value) for main.py to load.
        venv: Path to a venv for the child process (default: repo .venv / inherit).
        timeout_seconds: Max seconds to wait before aborting the run (default 3600).

    Returns:
        A text report with the resolved command, exit code, and captured output.
    """
    if source_system not in SYSTEMS:
        return f"ERROR: source_system must be one of {SYSTEMS}, got {source_system!r}"
    if target_system not in SYSTEMS:
        return f"ERROR: target_system must be one of {SYSTEMS}, got {target_system!r}"
    if action not in ACTIONS:
        return f"ERROR: action must be one of {ACTIONS}, got {action!r}"
    if redaction not in REDACTION_MODES:
        return f"ERROR: redaction must be one of {REDACTION_MODES}, got {redaction!r}"
    if destructive not in DESTRUCTIVE_MODES:
        return f"ERROR: destructive must be one of {DESTRUCTIVE_MODES}, got {destructive!r}"

    # main.py flags come BEFORE --exec; app.py flags come after.
    cmd: list[str] = [sys.executable, str(MAIN_PY)]
    if verbose:
        cmd.append("-v")
    _flag("--config", config, cmd)
    _flag("--venv", venv, cmd)

    cmd += ["--exec", str(APP_PY)]
    cmd += ["--source-system", source_system]
    cmd += ["--source-environment", source_environment]
    _flag("--source-namespace", source_namespace, cmd)
    cmd += ["--target-system", target_system]
    cmd += ["--target-environment", target_environment]
    _flag("--target-namespace", target_namespace, cmd)
    cmd += ["--action", action]
    _flag("--external-id-field", external_id_field, cmd)
    cmd += ["--redaction", redaction]
    cmd += ["--destructive", destructive]
    if tables:
        cmd += ["--tables", *tables]
    if scripts:
        cmd += ["--scripts", *scripts]

    try:
        result = subprocess.run(
            cmd,
            cwd=str(PROGRAM_ROOT),
            capture_output=True,
            text=True,
            timeout=timeout_seconds,
        )
    except subprocess.TimeoutExpired:
        return f"ERROR: migration timed out after {timeout_seconds}s\nCommand: {' '.join(cmd)}"

    status = "SUCCESS" if result.returncode == 0 else "FAILED"
    return (
        f"[{status}] exit code {result.returncode}\n"
        f"Command: {' '.join(cmd)}\n\n"
        f"--- stdout ---\n{result.stdout or '(empty)'}\n"
        f"--- stderr ---\n{result.stderr or '(empty)'}"
    )


@mcp.tool()
def options() -> str:
    """List the valid values charon's `migrate` tool accepts for its choice fields."""
    return (
        f"systems: {', '.join(SYSTEMS)}\n"
        f"actions: {', '.join(ACTIONS)}\n"
        f"redaction modes: {', '.join(REDACTION_MODES)}\n"
        f"destructive modes: {', '.join(DESTRUCTIVE_MODES)}"
    )


if __name__ == "__main__":
    mcp.run()
