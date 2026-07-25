"""Unit tests for environment-driven configuration."""

from __future__ import annotations

import pytest

from agent_runtime.config import get_settings
from agent_runtime.errors import ConfigError

VALID_ENV = {
    "AGENT_RUNTIME_DB_ADMIN_DSN": "postgresql://ar_admin:pw@localhost:5432/agent_runtime",
    "AGENT_RUNTIME_DB_APP_DSN": "postgresql://ar_app:pw@localhost:5432/agent_runtime",
    "AGENT_RUNTIME_REDIS_URL": "redis://localhost:6379/0",
}


def _apply_env(monkeypatch: pytest.MonkeyPatch, mapping: dict[str, str]) -> None:
    for key, value in mapping.items():
        monkeypatch.setenv(key, value)


def test_loads_from_env(monkeypatch: pytest.MonkeyPatch) -> None:
    _apply_env(monkeypatch, VALID_ENV)
    get_settings.cache_clear()

    settings = get_settings()
    assert str(settings.db_admin_dsn).startswith("postgresql://")
    assert settings.env == "dev"
    assert settings.is_production is False


def test_production_flag(monkeypatch: pytest.MonkeyPatch) -> None:
    _apply_env(monkeypatch, {**VALID_ENV, "AGENT_RUNTIME_ENV": "production"})
    get_settings.cache_clear()

    assert get_settings().is_production is True


def test_invalid_dsn_raises_config_error(monkeypatch: pytest.MonkeyPatch) -> None:
    _apply_env(monkeypatch, {**VALID_ENV, "AGENT_RUNTIME_DB_ADMIN_DSN": "not-a-dsn"})
    get_settings.cache_clear()

    with pytest.raises(ConfigError):
        get_settings()
