"""test_connection.py -- live connection health checks via is_healthy().

Each test requires real credentials in Q:\\.secrets\\.env and network access to
the target system.
"""
from __future__ import annotations

import pytest

from src.oracle.Oracle import Oracle
from src.sf.Salesforce import Salesforce

# (env_name, namespace) pairs to probe. HOME has no DB connection vars
# (only ORACLE_HOME_PATH, a filesystem path), so it is not a connectable env.
ORACLE_ENVIRONMENTS = ["QBL", "DWH", "HOMELAB"]
SALESFORCE_ENVIRONMENTS = ["TRAIL"]


@pytest.mark.parametrize("env", ORACLE_ENVIRONMENTS)
def test_oracle_is_healthy(env: str) -> None:
    ora = Oracle(env, env)
    assert ora.is_healthy() is True


@pytest.mark.parametrize("env", SALESFORCE_ENVIRONMENTS)
def test_salesforce_is_healthy(env: str) -> None:
    sf = Salesforce(env, env)
    assert sf.is_healthy() is True
