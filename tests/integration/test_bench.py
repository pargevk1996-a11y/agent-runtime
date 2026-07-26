"""Smoke tests: the benchmarks run and produce sane numbers at tiny scale."""

from __future__ import annotations

from collections.abc import AsyncIterator

import asyncpg
import pytest

from agent_runtime.dag.events import register_dag_events
from agent_runtime.db.pool import create_pool
from agent_runtime.events.registry import EventRegistry
from agent_runtime.runs.events import register_run_events
from agent_runtime.tools.events import register_tool_events
from agent_runtime_bench.runner import measure_cost, measure_recovery, measure_throughput
from agent_runtime_bench.workloads import cev_task, fan_out_in, linear_chain

pytestmark = pytest.mark.integration


def _registry() -> EventRegistry:
    registry = EventRegistry()
    register_run_events(registry)
    register_dag_events(registry)
    register_tool_events(registry)
    return registry


@pytest.fixture
async def pool(pg: dict[str, str]) -> AsyncIterator[asyncpg.Pool[asyncpg.Record]]:
    created = await create_pool(pg["app_dsn"])
    try:
        yield created
    finally:
        await created.close()


async def test_throughput_benchmarks_run(pool: asyncpg.Pool[asyncpg.Record]) -> None:
    registry = _registry()
    assert await measure_throughput(pool, registry, lambda: linear_chain(3), count=2) > 0
    assert await measure_throughput(pool, registry, lambda: fan_out_in(3), count=2) > 0
    assert await measure_throughput(pool, registry, cev_task, count=2) > 0


async def test_recovery_benchmark_runs(pool: asyncpg.Pool[asyncpg.Record]) -> None:
    result = await measure_recovery(pool, _registry(), lambda: linear_chain(5), repeats=2)
    assert result["full_fold_ms"] >= 0
    assert result["snapshot_ms"] >= 0


async def test_cost_benchmark_runs(pool: asyncpg.Pool[asyncpg.Record]) -> None:
    # 2 nodes/run at $0.002 each (1000 in @ $1/Mtok + 500 out @ $2/Mtok).
    cost = await measure_cost(pool, _registry(), nodes_per_run=2, runs=2)
    assert cost == pytest.approx(0.004)
