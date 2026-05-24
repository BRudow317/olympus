"""conftest.py — load .env before any test imports Oracle clients."""
from __future__ import annotations
import os
from pathlib import Path

from main import parse_config_file

_ENV_PATH = Path(r"Q:\.secrets\.env")


def _load_env() -> None:
    for k, v in parse_config_file(_ENV_PATH).items():
        if k not in os.environ:
            os.environ[k] = v


_load_env()
