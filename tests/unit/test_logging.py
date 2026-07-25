"""Unit tests for structured logging."""

from __future__ import annotations

import json

import pytest

from agent_runtime.logging import configure_logging, get_logger


def test_emits_json_with_expected_fields(capsys: pytest.CaptureFixture[str]) -> None:
    configure_logging("INFO")
    log = get_logger("test.logger")
    log.info("hello", key="val")

    record = json.loads(capsys.readouterr().out.strip().splitlines()[-1])
    assert record["event"] == "hello"
    assert record["key"] == "val"
    assert record["level"] == "info"
    assert "timestamp" in record


def test_respects_configured_level(capsys: pytest.CaptureFixture[str]) -> None:
    configure_logging("WARNING")
    log = get_logger("test.level")
    log.info("suppressed")
    log.warning("shown")

    out = capsys.readouterr().out
    assert "suppressed" not in out
    assert "shown" in out
