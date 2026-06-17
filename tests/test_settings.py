"""test_settings.py -- the settings/ override pattern and connector fallbacks.

Verifies that connector tuning knobs prefer the settings-published CHARON_* env
vars but fall back to their own local defaults (so the connectors work without
the settings package), and that settings.apply() is setdefault (explicit env wins).
"""
from __future__ import annotations

import pytest

from src import settings
from src.oracle.OracleModels import varchar2_growth_buffer, _VARCHAR2_GROWTH_BUFFER_DEFAULT
from src.sf.Salesforce import (
    _soql_get_max_encoded,
    _bulk_record_threshold,
    _SOQL_GET_MAX_ENCODED_DEFAULT,
    _BULK_RECORD_THRESHOLD_DEFAULT,
)
from src.settings import tuning


class TestConnectorTuningFallback:
    def test_growth_buffer_default_without_env(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.delenv("CHARON_VARCHAR2_GROWTH_BUFFER", raising=False)
        assert varchar2_growth_buffer() == _VARCHAR2_GROWTH_BUFFER_DEFAULT

    def test_growth_buffer_env_override(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("CHARON_VARCHAR2_GROWTH_BUFFER", "123")
        assert varchar2_growth_buffer() == 123

    def test_sf_thresholds_default_without_env(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.delenv("CHARON_SF_SOQL_GET_MAX_ENCODED", raising=False)
        monkeypatch.delenv("CHARON_SF_BULK_RECORD_THRESHOLD", raising=False)
        assert _soql_get_max_encoded() == _SOQL_GET_MAX_ENCODED_DEFAULT
        assert _bulk_record_threshold() == _BULK_RECORD_THRESHOLD_DEFAULT

    def test_sf_thresholds_env_override(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("CHARON_SF_SOQL_GET_MAX_ENCODED", "999")
        monkeypatch.setenv("CHARON_SF_BULK_RECORD_THRESHOLD", "7")
        assert _soql_get_max_encoded() == 999
        assert _bulk_record_threshold() == 7


class TestSettingsApply:
    def test_apply_publishes_canonical_values(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.delenv("CHARON_VARCHAR2_GROWTH_BUFFER", raising=False)
        settings.apply()
        assert varchar2_growth_buffer() == tuning.VARCHAR2_GROWTH_BUFFER

    def test_apply_does_not_override_explicit_env(self, monkeypatch: pytest.MonkeyPatch) -> None:
        # An explicit env/.env value must win over the settings default (setdefault).
        monkeypatch.setenv("CHARON_VARCHAR2_GROWTH_BUFFER", "5")
        settings.apply()
        assert varchar2_growth_buffer() == 5


class TestEnvironmentClassification:
    def test_registry_still_classifies_dev(self) -> None:
        # settings.environments holds the registry + logic; the dev envs
        # registered there must still classify as dev.
        from src.settings.environments import classify_environment
        from src.models import EnvironmentClass, System
        assert classify_environment(System.oracle, "dev01") == EnvironmentClass.dev
        assert classify_environment(System.salesforce, "DEVINT") == EnvironmentClass.dev
