"""Prometheus metrics for runs, nodes, LLM calls, and tool calls.

Counters and a histogram live in the default in-process registry. The ``record_*``
helpers are the instrumentation surface the runtime calls; ``render_metrics``
produces the exposition text served at ``/metrics``.
"""

from __future__ import annotations

from decimal import Decimal

from prometheus_client import Counter, Histogram, generate_latest

_RUNS = Counter("agentruntime_runs_total", "Runs by terminal status", ["status"])
_NODES = Counter("agentruntime_nodes_total", "Node executions by terminal status", ["status"])
_NODE_DURATION = Histogram("agentruntime_node_duration_seconds", "Node execution wall time")
_LLM_CALLS = Counter("agentruntime_llm_calls_total", "LLM calls")
_LLM_TOKENS = Counter("agentruntime_llm_tokens_total", "LLM tokens", ["kind"])
_LLM_COST = Counter("agentruntime_llm_cost_usd_total", "LLM dollar cost")
_TOOL_CALLS = Counter("agentruntime_tool_calls_total", "Tool calls by outcome", ["outcome"])


def record_run(status: str) -> None:
    _RUNS.labels(status=status).inc()


def record_node(status: str, duration_seconds: float) -> None:
    _NODES.labels(status=status).inc()
    _NODE_DURATION.observe(duration_seconds)


def record_llm(input_tokens: int, output_tokens: int, cost_usd: Decimal) -> None:
    _LLM_CALLS.inc()
    _LLM_TOKENS.labels(kind="input").inc(input_tokens)
    _LLM_TOKENS.labels(kind="output").inc(output_tokens)
    _LLM_COST.inc(float(cost_usd))


def record_tool(outcome: str) -> None:
    _TOOL_CALLS.labels(outcome=outcome).inc()


def render_metrics() -> bytes:
    """Return the Prometheus exposition for all registered metrics."""
    return generate_latest()
