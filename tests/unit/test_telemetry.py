"""Unit tests for tracing, metrics, and log/trace correlation."""

from __future__ import annotations

import json
from collections.abc import Iterator
from decimal import Decimal

import pytest
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

from agent_runtime.logging import configure_logging, get_logger
from agent_runtime.telemetry.metrics import (
    record_llm,
    record_node,
    record_run,
    record_tool,
    render_metrics,
)
from agent_runtime.telemetry.tracing import configure_tracing, get_tracer

_EXPORTER = InMemorySpanExporter()


@pytest.fixture(scope="module", autouse=True)
def _tracing() -> None:
    configure_tracing(exporter=_EXPORTER)


@pytest.fixture(autouse=True)
def _clear_spans() -> Iterator[None]:
    _EXPORTER.clear()
    yield


def test_span_is_exported() -> None:
    with get_tracer("test").start_as_current_span("unit.span"):
        pass
    assert [span.name for span in _EXPORTER.get_finished_spans()] == ["unit.span"]


def test_metrics_record_and_render() -> None:
    record_run("succeeded")
    record_node("succeeded", 0.01)
    record_llm(10, 5, Decimal("0.5"))
    record_tool("completed")

    text = render_metrics().decode()
    assert "agentruntime_runs_total" in text
    assert "agentruntime_nodes_total" in text
    assert "agentruntime_llm_calls_total" in text
    assert "agentruntime_llm_cost_usd_total" in text
    assert "agentruntime_tool_calls_total" in text


def test_log_carries_trace_id_within_span(capsys: pytest.CaptureFixture[str]) -> None:
    configure_logging("INFO")
    with get_tracer("test").start_as_current_span("logged.span"):
        get_logger("t").info("hello")

    record = json.loads(capsys.readouterr().out.strip().splitlines()[-1])
    assert "trace_id" in record
    assert "span_id" in record


def test_log_omits_trace_id_without_span(capsys: pytest.CaptureFixture[str]) -> None:
    configure_logging("INFO")
    get_logger("t").info("no span here")

    record = json.loads(capsys.readouterr().out.strip().splitlines()[-1])
    assert "trace_id" not in record
