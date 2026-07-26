"""Unit tests for the per-tool egress allowlist policy."""

from __future__ import annotations

import pytest

from agent_runtime_tools.egress import EgressDeniedError, EgressPolicy


def test_allows_listed_host() -> None:
    EgressPolicy({"example.com"}).check("https://example.com/path?q=1")


def test_denies_unlisted_host() -> None:
    with pytest.raises(EgressDeniedError):
        EgressPolicy({"example.com"}).check("https://evil.test/")


def test_denies_non_http_scheme() -> None:
    with pytest.raises(EgressDeniedError):
        EgressPolicy({"example.com"}).check("file:///etc/passwd")


def test_empty_allowlist_denies_all() -> None:
    with pytest.raises(EgressDeniedError):
        EgressPolicy(set()).check("https://example.com/")


def test_from_env_parses_comma_list(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("AR_WEB_FETCH_ALLOWLIST", "a.com, b.com")
    policy = EgressPolicy.from_env()
    policy.check("https://a.com/")
    policy.check("https://b.com/")
    with pytest.raises(EgressDeniedError):
        policy.check("https://c.com/")
