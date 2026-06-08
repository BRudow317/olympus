#!/usr/bin/env python3
"""build_wheelhouse.py - thin manual/CI entry to charon's vendoring.

You do NOT normally need this. On Windows, just run the job; charon vendors the
wheels automatically when pyproject.toml changes. This script is here for the
case where you want to pre-build the wheelhouse without running the job (e.g. a
CI step), or refresh it on demand.

Run from anywhere:
    python configs/build_wheelhouse.py        (Linux/macOS - will refuse, by design)
    python configs\\build_wheelhouse.py        (Windows - downloads + pins)

It reads dependencies from <repo>/pyproject.toml and writes <repo>/vendor/wheels
and <repo>/requirements.lock. Windows-only: off Windows the host is treated as
offline and the build refuses, because the server must consume vendored wheels.
"""
import logging
import sys
from pathlib import Path

# charon.py lives at the repo root, one level up from configs/.
REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))
from .. import charon  # noqa: E402


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    log = logging.getLogger("build")
    managed, deps = charon._load_project(REPO_ROOT)
    if not managed:
        raise SystemExit(
            f"{REPO_ROOT / 'pyproject.toml'} has no [tool.charon] table; "
            f"this repo is not charon-managed, nothing to build."
        )
    charon.build_wheelhouse(REPO_ROOT, deps, log)
    print(f"\nwrote {REPO_ROOT / 'requirements.lock'} and {REPO_ROOT / 'vendor' / 'wheels'}")
    print("commit pyproject.toml + requirements.lock + vendor/wheels/, then deploy the zip.")


if __name__ == "__main__":
    main()