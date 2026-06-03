"""conftest.py -- load .env before any test imports Oracle/Salesforce clients."""
from __future__ import annotations
import os
from pathlib import Path

from charon import parse_config_file




def _load_env() -> None:

    for k, v in parse_config_file().items():
        if k not in os.environ:
            os.environ[k] = v

_load_env()
