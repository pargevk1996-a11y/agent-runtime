"""Integration tests for CheckpointManager: snapshots, pure-cache, recovery."""

from __future__ import annotations

from collections.abc import AsyncIterator
from datetime import timedelta

import asyncpg
import pytest

from agent_runtime.db.pool import create_pool, tenant_connection
from agent_runtime.events.registry import EventRegistry
from agent_runtime.events.store import EventStore
from agent_runtime.ids import RunId, TenantId, new_run_id, new_tenant_id
from agent_runtime.runs.events import RunStarted, RunSucceeded, register_run_events
from agent_runtime.runs.snapshots import CheckpointManager
from agent_runtime.runs.state import RunStatus, fold
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


async def _drive_to_success(rs: RunStore, tenant: TenantId, run: RunId) -> None:
    await rs.create_run(tenant, run, input={"goal": 1})
    lease = await rs.acquire_lease(tenant, run, worker="w", ttl=timedelta(minutes=5))
    await rs.append_events(tenant, run, lease=lease, payloads=[RunStarted(worker="w")])
    await rs.append_events(tenant, run, lease=lease, payloads=[RunSucceeded(result={"ok": 1})])


async def test_save_and_load_latest_round_trip(pool: asyncpg.Pool[asyncpg.Record]) -> None:
    tenant, run = new_tenant_id(), new_run_id()
    store = EventStore(pool, registry=_registry())
    checkpoints = CheckpointManager(pool)
    await _drive_to_success(RunStore(pool, store, checkpoints), tenant, run)

    full = fold(await store.read(tenant, run))
    await checkpoints.save(tenant, run, full, at_seq=full.last_seq)

    loaded = await checkpoints.load_latest(tenant, run)
    assert loaded is not None
    state, at_seq = loaded
    assert at_seq == full.last_seq
    assert state == full  # status, timestamps, result all survive the round trip


@pytest.mark.parametrize("snapshot_seq", [1, 2, 3])
async def test_load_state_matches_full_fold_at_any_snapshot(
    pool: asyncpg.Pool[asyncpg.Record], snapshot_seq: int
) -> None:
    tenant, run = new_tenant_id(), new_run_id()
    store = EventStore(pool, registry=_registry())
    checkpoints = CheckpointManager(pool)
    with_snapshots = RunStore(pool, store, checkpoints)
    await _drive_to_success(with_snapshots, tenant, run)

    prefix = fold(await store.read(tenant, run, to_seq=snapshot_seq))
    await checkpoints.save(tenant, run, prefix, at_seq=snapshot_seq)

    reference = await RunStore(pool, store).load_state(tenant, run)  # no snapshots
    via_snapshot = await with_snapshots.load_state(tenant, run)
    assert via_snapshot == reference
    assert via_snapshot.status is RunStatus.SUCCEEDED


async def test_snapshot_is_pure_cache(pool: asyncpg.Pool[asyncpg.Record]) -> None:
    tenant, run = new_tenant_id(), new_run_id()
    store = EventStore(pool, registry=_registry())
    checkpoints = CheckpointManager(pool)
    run_store = RunStore(pool, store, checkpoints)
    await _drive_to_success(run_store, tenant, run)

    await checkpoints.save(tenant, run, fold(await store.read(tenant, run, to_seq=2)), at_seq=2)
    before = await run_store.load_state(tenant, run)

    async with tenant_connection(pool, tenant) as conn:
        await conn.execute("DELETE FROM run_snapshots WHERE run_id = $1", run)

    after = await run_store.load_state(tenant, run)
    assert after == before


async def test_prune_keeps_newest(pool: asyncpg.Pool[asyncpg.Record]) -> None:
    tenant, run = new_tenant_id(), new_run_id()
    store = EventStore(pool, registry=_registry())
    checkpoints = CheckpointManager(pool)
    await _drive_to_success(RunStore(pool, store, checkpoints), tenant, run)

    for seq in (1, 2, 3):
        await checkpoints.save(
            tenant, run, fold(await store.read(tenant, run, to_seq=seq)), at_seq=seq
        )

    removed = await checkpoints.prune(tenant, run, keep=1)
    assert removed == 2
    latest = await checkpoints.load_latest(tenant, run)
    assert latest is not None
    assert latest[1] == 3


async def test_recovery_from_fresh_process(pg: dict[str, str]) -> None:
    tenant, run = new_tenant_id(), new_run_id()

    # "Process 1": create, start, snapshot mid-run, then succeed — and die.
    pool1 = await create_pool(pg["app_dsn"])
    try:
        store1 = EventStore(pool1, registry=_registry())
        cp1 = CheckpointManager(pool1)
        rs1 = RunStore(pool1, store1, cp1)
        await rs1.create_run(tenant, run, input={"goal": 1})
        lease = await rs1.acquire_lease(tenant, run, worker="w", ttl=timedelta(minutes=5))
        await rs1.append_events(tenant, run, lease=lease, payloads=[RunStarted(worker="w")])
        mid_state = await rs1.load_state(tenant, run)
        await cp1.save(tenant, run, mid_state, at_seq=mid_state.last_seq)
        await rs1.append_events(tenant, run, lease=lease, payloads=[RunSucceeded(result={"ok": 1})])
    finally:
        await pool1.close()

    # "Process 2": brand-new objects, no in-memory state — recover from storage.
    pool2 = await create_pool(pg["app_dsn"])
    try:
        rs2 = RunStore(pool2, EventStore(pool2, registry=_registry()), CheckpointManager(pool2))
        recovered = await rs2.load_state(tenant, run)
        assert recovered.status is RunStatus.SUCCEEDED
        assert recovered.result == {"ok": 1}
    finally:
        await pool2.close()
