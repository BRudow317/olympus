"""test_guard.py -- pure (no-DB) tests for the destructive-op guard + env registry."""
from __future__ import annotations

import pytest

from src.models import EnvironmentClass, System
from src.settings.environments import classify_environment, is_production
from src.settings.guard import (
    DestructiveGuard,
    DestructiveOp,
    DestructivePermissionError,
    GuardRequest,
    ModeRule,
    ProdBreakGlassRule,
    PROD_BREAK_GLASS_ENV,
    normalize_destructive_mode,
)


def _req(
    op: DestructiveOp = DestructiveOp.drop,
    *,
    env_class: EnvironmentClass = EnvironmentClass.dev,
    is_managed: bool = True,
    system: System = System.oracle,
    environment: str = "DEV01",
    table_name: str = "SF_CASE",
) -> GuardRequest:
    return GuardRequest(
        op=op, system=system, environment=environment,
        env_class=env_class, table_name=table_name, is_managed=is_managed,
    )


# --- mode normalization ----------------------------------------------------

class TestModeNormalization:
    @pytest.mark.parametrize(
        "raw, expected",
        [
            ("no", "no"),
            ("restricted", "restricted"),
            ("dev-only", "dev_only"),
            ("dev_only", "dev_only"),
            ("YES", "yes"),
            ("  Dev-Only ", "dev_only"),
        ],
    )
    def test_accepts_hyphen_and_underscore(self, raw: str, expected: str) -> None:
        assert normalize_destructive_mode(raw) == expected

    def test_rejects_unknown(self) -> None:
        with pytest.raises(ValueError):
            normalize_destructive_mode("maybe")


# --- environment registry --------------------------------------------------

class TestEnvironmentRegistry:
    def test_registered_dev_envs(self) -> None:
        assert classify_environment(System.oracle, "dev01") == EnvironmentClass.dev
        assert classify_environment(System.salesforce, "DEVINT") == EnvironmentClass.dev

    def test_unknown_env_defaults_to_unknown_and_is_prod(self) -> None:
        cls = classify_environment(System.oracle, "MYSTERY")
        assert cls == EnvironmentClass.unknown
        assert is_production(cls) is True

    def test_unknown_is_treated_as_production(self) -> None:
        assert is_production(EnvironmentClass.unknown) is True
        assert is_production(EnvironmentClass.prod) is True
        assert is_production(EnvironmentClass.dev) is False
        assert is_production(EnvironmentClass.qa) is False

    def test_env_override_wins(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("ENV_CLASS_ORACLE_DEV01", "prod")
        assert classify_environment(System.oracle, "dev01") == EnvironmentClass.prod

    def test_invalid_override_is_ignored(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("ENV_CLASS_ORACLE_DEV01", "banana")
        assert classify_environment(System.oracle, "dev01") == EnvironmentClass.dev


# --- mode rule -------------------------------------------------------------

class TestModeRule:
    def test_no_blocks_everything(self) -> None:
        rule = ModeRule("no")
        for op in DestructiveOp:
            assert rule.permits(_req(op, is_managed=True)) is not None

    def test_restricted_allows_managed_blocks_unmanaged(self) -> None:
        rule = ModeRule("restricted")
        assert rule.permits(_req(is_managed=True)) is None
        assert rule.permits(_req(is_managed=False)) is not None

    def test_dev_only_blocks_prod_allows_dev(self) -> None:
        rule = ModeRule("dev-only")
        assert rule.permits(_req(env_class=EnvironmentClass.dev)) is None
        assert rule.permits(_req(env_class=EnvironmentClass.prod)) is not None
        # unknown is prod-ish -> blocked
        assert rule.permits(_req(env_class=EnvironmentClass.unknown)) is not None

    def test_yes_abstains(self) -> None:
        rule = ModeRule("yes")
        assert rule.permits(_req(env_class=EnvironmentClass.prod, is_managed=False)) is None


# --- prod break-glass ------------------------------------------------------

class TestProdBreakGlass:
    def test_blocks_prod_data_loss(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.delenv(PROD_BREAK_GLASS_ENV, raising=False)
        rule = ProdBreakGlassRule()
        assert rule.permits(_req(DestructiveOp.drop, env_class=EnvironmentClass.prod)) is not None
        assert rule.permits(_req(DestructiveOp.truncate, env_class=EnvironmentClass.prod)) is not None

    def test_allows_prod_structural_alter(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.delenv(PROD_BREAK_GLASS_ENV, raising=False)
        rule = ProdBreakGlassRule()
        # additive ALTER is not data-loss, so it passes break-glass even in prod
        assert rule.permits(_req(DestructiveOp.alter, env_class=EnvironmentClass.prod)) is None

    def test_allows_nonprod_data_loss(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.delenv(PROD_BREAK_GLASS_ENV, raising=False)
        assert ProdBreakGlassRule().permits(_req(DestructiveOp.drop, env_class=EnvironmentClass.dev)) is None

    def test_break_glass_env_lifts_the_stop(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv(PROD_BREAK_GLASS_ENV, "1")
        assert ProdBreakGlassRule().permits(_req(DestructiveOp.drop, env_class=EnvironmentClass.prod)) is None


# --- composed guard --------------------------------------------------------

class TestDestructiveGuard:
    def test_yes_still_blocks_prod_drop_without_break_glass(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.delenv(PROD_BREAK_GLASS_ENV, raising=False)
        guard = DestructiveGuard("yes")
        with pytest.raises(DestructivePermissionError):
            guard.enforce(_req(DestructiveOp.drop, env_class=EnvironmentClass.prod))

    def test_yes_allows_dev_drop(self) -> None:
        DestructiveGuard("yes").enforce(_req(DestructiveOp.drop, env_class=EnvironmentClass.dev))

    def test_restricted_blocks_unmanaged_truncate(self) -> None:
        guard = DestructiveGuard("restricted")
        with pytest.raises(DestructivePermissionError):
            guard.enforce(_req(DestructiveOp.truncate, is_managed=False))

    def test_restricted_allows_managed_dev_truncate(self) -> None:
        DestructiveGuard("restricted").enforce(
            _req(DestructiveOp.truncate, is_managed=True, env_class=EnvironmentClass.dev)
        )

    def test_restricted_managed_prod_drop_needs_break_glass(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.delenv(PROD_BREAK_GLASS_ENV, raising=False)
        guard = DestructiveGuard("restricted")
        # managed passes ModeRule, but prod data-loss is still vetoed
        with pytest.raises(DestructivePermissionError):
            guard.enforce(_req(DestructiveOp.drop, is_managed=True, env_class=EnvironmentClass.prod))

    def test_error_carries_reasons(self) -> None:
        guard = DestructiveGuard("no")
        try:
            guard.enforce(_req(DestructiveOp.drop))
        except DestructivePermissionError as e:
            assert e.reasons
            assert "DROP" in str(e)
        else:
            pytest.fail("expected DestructivePermissionError")

    def test_extensible_with_a_custom_rule(self) -> None:
        # A new safety check is added without touching existing rules: append a
        # rule that vetoes any table named like an audit table.
        class NoAuditTables:
            def permits(self, req: GuardRequest) -> str | None:
                if "AUDIT" in req.table_name.upper():
                    return "audit tables are protected"
                return None

        guard = DestructiveGuard("yes")
        guard.rules.append(NoAuditTables())
        guard.enforce(_req(DestructiveOp.drop, table_name="SF_CASE", env_class=EnvironmentClass.dev))
        with pytest.raises(DestructivePermissionError):
            guard.enforce(_req(DestructiveOp.drop, table_name="SF_AUDIT_LOG", env_class=EnvironmentClass.dev))
