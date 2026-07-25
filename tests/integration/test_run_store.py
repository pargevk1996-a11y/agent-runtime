"""Integration tests for RunStore: projection, lease/fencing, coordinated append."""

from __future__ import annotations

from collections.abc import AsyncIterator
from datetime import timedelta

import asyncpg
import pytest

from agent_runtime.db.pool import create_pool, tenant_connection
from agent_runtime.events.registry import EventRegistry
from agent_runtime.events.store import EventStore
from agent_runtime.ids import new_run_id, new_tenant_id
from agent_runtime.runs.errors import (
    LeaseHeldError,
    RunAlreadyExistsError,
    RunNotFoundError,
    StaleLeaseError,
)
from agent_runtime.runs.events import RunStarted, RunSucceeded, register_run_events
from agent_runtime.runs.state import RunStatus
from agent_runtime.runs.store import RunStore

pytestmark = pytest.mark.integration


def _registry() -> EventRegistry:
    registry = EventRegistry()
    register_run_events(registry)
    return registry


@pytest.fixture
async def pool(pg: dict[str, str]) -> AsyncIterator[asyncpg.Pool[asyncpg.Record]]:
    created = await create_pool(pg["app_dsn"])
    try:
        yield created
    finally:
        await created.close()


@pytest.fixture
def run_store(pool: asyncpg.Pool[asyncpg.Record]) -> RunStore:
    return RunStore(pool, EventStore(pool, registry=_registry()))


async def test_create_and_load_pending(run_store: RunStore) -> None:
    tenant, run = new_tenant_id(), new_run_id()
    created = await run_store.create_run(tenant, run, input={"goal": 1})
    assert created.seq == 1

    state = await run_store.load_state(tenant, run)
    assert state.status is RunStatus.PENDING
    assert state.input == {"goal": 1}


async def test_duplicate_create_rejected(run_store: RunStore) -> None:
    tenant, run = new_tenant_id(), new_run_id()
    await run_store.create_run(tenant, run, input={})
    with pytest.raises(RunAlreadyExistsError):
        await run_store.create_run(tenant, run, input={})


async def test_acquire_lease_bumps_fencing_token(run_store: RunStore) -> None:
    tenant, run = new_tenant_id(), new_run_id()
    await run_store.create_run(tenant, run, input={})
    lease = await run_store.acquire_lease(tenant, run, worker="w1", ttl=timedelta(minutes=5))
    assert lease.fencing_token == 1


async def test_lease_held_by_live_worker_rejected(run_store: RunStore) -> None:
    tenant, run = new_tenant_id(), new_run_id()
    await run_store.create_run(tenant, run, input={})
    await run_store.acquire_lease(tenant, run, worker="w1", ttl=timedelta(minutes=5))
    with pytest.raises(LeaseHeldError):
        await run_store.acquire_lease(tenant, run, worker="w2", ttl=timedelta(minutes=5))


async def test_expired_lease_can_be_stolen(
    pool: asyncpg.Pool[asyncpg.Record], run_store: RunStore
) -> None:
    tenant, run = new_tenant_id(), new_run_id()
    await run_store.create_run(tenant, run, input={})
    first = await run_store.acquire_lease(tenant, run, worker="w1", ttl=timedelta(minutes=5))

    async with tenant_connection(pool, tenant) as conn:
        await conn.execute(
            "UPDATE runs SET lease_expires_at = now() - interval '1 hour' WHERE run_id = $1", run
        )

    second = await run_store.acquire_lease(tenant, run, worker="w2", ttl=timedelta(minutes=5))
    assert second.fencing_token == first.fencing_token + 1


async def test_append_advances_projection(run_store: RunStore) -> None:
    tenant, run = new_tenant_id(), new_run_id()
    await run_store.create_run(tenant, run, input={})
    lease = await run_store.acquire_lease(tenant, run, worker="w1", ttl=timedelta(minutes=5))

    appended = await run_store.append_events(
        tenant, run, lease=lease, payloads=[RunStarted(worker="w1")]
    )
    assert appended[0].seq == 2

    state = await run_store.load_state(tenant, run)
    assert state.status is RunStatus.RUNNING


async def test_full_lifecycle_to_succeeded(run_store: RunStore) -> None:
    tenant, run = new_tenant_id(), new_run_id()
    await run_store.create_run(tenant, run, input={"goal": "x"})
    lease = await run_store.acquire_lease(tenant, run, worker="w1", ttl=timedelta(minutes=5))
    await run_store.append_events(tenant, run, lease=lease, payloads=[RunStarted(worker="w1")])
    await run_store.append_events(
        tenant, run, lease=lease, payloads=[RunSucceeded(result={"ok": 1})]
    )

    state = await run_store.load_state(tenant, run)
    assert state.status is RunStatus.SUCCEEDED
    assert state.result == {"ok": 1}


async def test_stale_lease_append_rejected(
    pool: asyncpg.Pool[asyncpg.Record], run_store: RunStore
) -> None:
    tenant, run = new_tenant_id(), new_run_id()
    await run_store.create_run(tenant, run, input={})
    stale = await run_store.acquire_lease(tenant, run, worker="w1", ttl=timedelta(minutes=5))

    # Expire and let a second worker steal the lease, advancing the token.
    async with tenant_connection(pool, tenant) as conn:
        await conn.execute(
            "UPDATE runs SET lease_expires_at = now() - interval '1 hour' WHERE run_id = $1", run
        )
    await run_store.acquire_lease(tenant, run, worker="w2", ttl=timedelta(minutes=5))

    with pytest.raises(StaleLeaseError):
        await run_store.append_events(tenant, run, lease=stale, payloads=[RunStarted(worker="w1")])


async def test_conn_composition_rolls_back_together(
    pool: asyncpg.Pool[asyncpg.Record], run_store: RunStore
) -> None:
    tenant, run = new_tenant_id(), new_run_id()
    await run_store.create_run(tenant, run, input={})
    store = EventStore(pool, registry=_registry())

    with pytest.raises(RuntimeError):
        async with tenant_connection(pool, tenant) as conn:
            await store.append(tenant, run, after_seq=1, payload=RunStarted(worker="w"), conn=conn)
            raise RuntimeError("boom")

    # The append shared the outer transaction, so it rolled back with it.
    state = await run_store.load_state(tenant, run)
    assert state.status is RunStatus.PENDING


async def test_missing_run_raises(run_store: RunStore) -> None:
    with pytest.raises(RunNotFoundError):
        await run_store.load_state(new_tenant_id(), new_run_id())
    with pytest.raises(RunNotFoundError):
        await run_store.acquire_lease(
            new_tenant_id(), new_run_id(), worker="w", ttl=timedelta(minutes=5)
        )
