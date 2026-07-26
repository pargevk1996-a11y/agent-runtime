"""Integration tests for the CEV composition driven by the real scheduler."""

from __future__ import annotations

from collections.abc import AsyncIterator, Mapping
from datetime import timedelta

import asyncpg
import pytest

from agent_runtime.dag.events import register_dag_events
from agent_runtime.dag.scheduler import Scheduler
from agent_runtime.db.pool import create_pool
from agent_runtime.events.registry import EventRegistry
from agent_runtime.events.store import EventStore
from agent_runtime.ids import new_run_id, new_tenant_id
from agent_runtime.runs.events import register_run_events
from agent_runtime.runs.state import RunStatus
from agent_runtime.runs.store import RunStore
from agent_runtime_agents.cev import CEVConfig, CEVExecutor, seed_cev

pytestmark = pytest.mark.integration


def _at_least_five(proposal: dict[str, object]) -> str | None:
    value = proposal.get("value", 0)
    return None if isinstance(value, int) and value >= 5 else "value must be >= 5"


def _registry() -> EventRegistry:
    registry = EventRegistry()
    register_run_events(registry)
    register_dag_events(registry)
    return registry


@pytest.fixture
async def pool(pg: dict[str, str]) -> AsyncIterator[asyncpg.Pool[asyncpg.Record]]:
    created = await create_pool(pg["app_dsn"])
    try:
        yield created
    finally:
        await created.close()


async def _run_cev(
    pool: asyncpg.Pool[asyncpg.Record], config: CEVConfig, *, max_reflection_depth: int = 3
) -> RunStatus:
    run_store = RunStore(pool, EventStore(pool, registry=_registry()))
    tenant, run = new_tenant_id(), new_run_id()
    await run_store.create_run(tenant, run, input={})
    lease = await run_store.acquire_lease(tenant, run, worker="w", ttl=timedelta(minutes=5))
    await run_store.append_events(tenant, run, lease=lease, payloads=seed_cev())

    scheduler = Scheduler(run_store, CEVExecutor(config), max_reflection_depth=max_reflection_depth)
    await scheduler.run(tenant, run, lease)
    return (await run_store.load_state(tenant, run)).status


async def test_succeeds_after_reflection(pool: asyncpg.Pool[asyncpg.Record]) -> None:
    # First proposal (no feedback) is bad; once the critic's feedback arrives, good.
    def proposer(feedback: Mapping[str, object]) -> dict[str, object]:
        return {"value": 10 if feedback else 0}

    config = CEVConfig(proposer=proposer, constraints=(_at_least_five,))
    assert await _run_cev(pool, config) is RunStatus.SUCCEEDED


async def test_fails_when_reflection_depth_exhausted(pool: asyncpg.Pool[asyncpg.Record]) -> None:
    def proposer(feedback: Mapping[str, object]) -> dict[str, object]:
        return {"value": 0}  # never satisfies the constraint

    config = CEVConfig(proposer=proposer, constraints=(_at_least_five,))
    assert await _run_cev(pool, config, max_reflection_depth=1) is RunStatus.FAILED


async def test_verifier_rejection_fails_run(pool: asyncpg.Pool[asyncpg.Record]) -> None:
    def proposer(feedback: Mapping[str, object]) -> dict[str, object]:
        return {"value": 10}  # passes the critic

    def verifier(proposal: dict[str, object]) -> str | None:
        return "verifier always rejects"

    config = CEVConfig(proposer=proposer, constraints=(_at_least_five,), verifier=verifier)
    assert await _run_cev(pool, config) is RunStatus.FAILED


async def test_first_proposal_passes_without_reflection(
    pool: asyncpg.Pool[asyncpg.Record],
) -> None:
    def proposer(feedback: Mapping[str, object]) -> dict[str, object]:
        return {"value": 7}  # good immediately

    config = CEVConfig(proposer=proposer, constraints=(_at_least_five,))
    assert await _run_cev(pool, config) is RunStatus.SUCCEEDED
