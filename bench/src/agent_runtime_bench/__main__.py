"""Run all benchmarks against the infrastructure named in the environment.

    make up
    uv run python -m agent_runtime_bench

Reads the app DSN and Redis URL from ``AGENT_RUNTIME_*`` settings, runs each
benchmark, and prints the results as JSON.
"""

from __future__ import annotations

import asyncio
import json

from agent_runtime.config import get_settings
from agent_runtime.dag.events import register_dag_events
from agent_runtime.db.pool import create_pool
from agent_runtime.events.registry import EventRegistry
from agent_runtime.runs.events import register_run_events
from agent_runtime.tools.events import register_tool_events
from agent_runtime_bench.runner import measure_cost, measure_recovery, measure_throughput
from agent_runtime_bench.workloads import cev_task, fan_out_in, linear_chain


def _registry() -> EventRegistry:
    registry = EventRegistry()
    register_run_events(registry)
    register_dag_events(registry)
    register_tool_events(registry)
    return registry


async def main() -> None:
    settings = get_settings()
    pool = await create_pool(str(settings.db_app_dsn))
    registry = _registry()
    try:
        results = {
            "throughput_runs_per_sec": {
                "linear_10": await measure_throughput(
                    pool, registry, lambda: linear_chain(10), count=20
                ),
                "fan_out_20": await measure_throughput(
                    pool, registry, lambda: fan_out_in(20), count=20
                ),
                "cev": await measure_throughput(pool, registry, cev_task, count=20),
            },
            "recovery_ms": await measure_recovery(
                pool, registry, lambda: linear_chain(50), repeats=20
            ),
            "cost_per_task_usd": await measure_cost(pool, registry, nodes_per_run=5, runs=10),
        }
        print(json.dumps(results, indent=2))
    finally:
        await pool.close()


if __name__ == "__main__":
    asyncio.run(main())
