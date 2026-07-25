"""Unit tests for the typed exception hierarchy."""

from __future__ import annotations

from agent_runtime.errors import (
    AgentRuntimeError,
    ConfigError,
    IndeterminateError,
    RetryableError,
    TerminalError,
)


def test_categories_subclass_root() -> None:
    for cls in (RetryableError, TerminalError, IndeterminateError):
        assert issubclass(cls, AgentRuntimeError)


def test_config_error_is_terminal() -> None:
    err = ConfigError("bad")
    assert isinstance(err, TerminalError)
    assert isinstance(err, AgentRuntimeError)


def test_context_is_copied_not_aliased() -> None:
    source = {"k": 1}
    err = AgentRuntimeError("x", context=source)
    source["k"] = 2
    assert err.context == {"k": 1}


def test_message_preserved() -> None:
    err = RetryableError("boom")
    assert err.message == "boom"
    assert str(err) == "boom"
