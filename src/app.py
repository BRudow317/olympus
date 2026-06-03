#!/usr/bin/env python3
"""app.py

    python ./charon.py -v -l ./.logs \
        --exec ./src/app.py \
        --source-system salesforce \
        --source-environment TRAIL \
        --source-namespace TRAIL \
        --target-system oracle \
        --target-environment DWH \
        --target-namespace DWH \
        --action reset \
        --tables Contact Account
"""
from __future__ import annotations

import argparse
import logging
import os
from src.models import System
from src.seeding import seeding

logger: logging.Logger = logging.getLogger(__name__)

# Hardcoded: not a stage/rundeck-scripts/.env config value, so it is not read
# from the environment. charon.py owns all env/.env handling.
PROGRAM_NAME = os.getenv("PROGRAM_NAME", "charon")
os.environ["PROGRAM_NAME"] = PROGRAM_NAME


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog=PROGRAM_NAME, add_help=True)
    parser.add_argument("--source-system", choices=list(System), required=True, type=System)
    parser.add_argument("--source-environment", required=True, type=str)
    parser.add_argument("--source-namespace", required=False, type=str, default=None)
    parser.add_argument("--target-system", choices=list(System), required=True, type=System)
    parser.add_argument("--target-environment", required=True, type=str)
    parser.add_argument("--target-namespace", required=False, type=str, default=None)
    parser.add_argument(
        "--action",
        required=False,
        type=str,
        default="reset",
        choices=["reset", "insert", "upsert", "update"],
        help="How records are written to the target (default: reset).",
    )
    parser.add_argument(
        "--external-id-field",
        required=False,
        type=str,
        default=None,
        help="External id field for Salesforce upsert (defaults to 'Id').",
    )
    parser.add_argument(
        "--tables",
        required=False,
        type=str,
        default=["*"],
        nargs="+",
        help="list of Table/object names to migrate separated by a space, or '*' for the whole schema.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args: argparse.Namespace = parse_args(argv)
    return seeding(
        source_system=args.source_system,
        source_environment=args.source_environment,
        source_namespace=args.source_namespace,
        target_system=args.target_system,
        target_environment=args.target_environment,
        target_namespace=args.target_namespace,
        tables=args.tables,
        action=args.action,
        external_id_field=args.external_id_field,
    )


def cmd_line() -> int:
    try:
        result: int = main()
        if result != 0:
            raise RuntimeError(f"Migration failed with exit code {result}")
        return result
    except KeyboardInterrupt:
        return 2


if __name__ == "__main__":
    raise SystemExit(cmd_line())
