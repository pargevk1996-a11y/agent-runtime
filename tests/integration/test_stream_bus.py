"""Integration tests for the Redis stream bus and RunStore publishing."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import cast

import pytest
import redis.asyncio as aioredis

from agent_runtime.dag.events import NodeAdded, register_dag_events
from agent_runtime.dag.executor import NodeContext, NodeResult
from agent_runtime.dag.model import NodeBudget, NodeRole, RetryPolicy
from agent_runtime.dag.scheduler import Scheduler
from agent_runtime.db.pool import create_pool
from agent_runtime.events.envelope import Envelope
from agent_runtime.events.registry import EventRegistry
from agent_runtime.events.store import EventStore
from agent_runtime.ids import new_event_id, new_node_id, new_run_id, new_tenant_id
from agent_runtime.runs.events import RunCreated, register_run_events
from agent_runtime.runs.store import RunStore
from agent_runtime.stream.bus import RedisStreamBus


class _NoopExecutor:
    async def execute(self, ctx: NodeContext) -> NodeResult:
        return NodeResult(output={})


pytestmark = pytest.mark.integration

_TENANT = new_tenant_id()
_NOW = datetime(2020, 1, 1, tzinfo=UTC)


def _env(run: object, seq: int) -> Envelope:
    return Envelope(
        event_id=new_event_id(),
        tenant_id=_TENANT,
        run_id=run,  # type: ignore[arg-type]
        seq=seq,
        event_type="run.created",
        payload_version=1,
        payload=RunCreated(input={}),
        occurred_at=_NOW,
        recorded_at=_NOW,
    )


def _registry() -> EventRegistry:
    registry = EventRegistry()
    register_run_events(registry)
    return registry


async def test_publish_and_tail_in_order(redis_client: aioredis.Redis) -> None:
    bus = RedisStreamBus(redis_client)
    run = new_run_id()
    await bus.publish(run, [_env(run, 1), _env(run, 2), _env(run, 3)])

    seqs: list[int] = []
    async for entry in bus.tail(run, after_seq=0):
        seqs.append(entry.seq)
        if len(seqs) == 3:
            break
    assert seqs == [1, 2, 3]


async def test_after_seq_skips_already_seen(redis_client: aioredis.Redis) -> None:
    bus = RedisStreamBus(redis_client)
    run = new_run_id()
    await bus.publish(run, [_env(run, 1), _env(run, 2), _env(run, 3)])

    seqs: list[int] = []
    async for entry in bus.tail(run, after_seq=1):
        seqs.append(entry.seq)
        if len(seqs) == 2:
            break
    assert seqs == [2, 3]


async def test_bounded_window_trims_old_entries(redis_client: aioredis.Redis) -> None:
    # Redis trims approximate MAXLEN at macro-node boundaries, so publish enough
    # to cross one and observe the window stay bounded well below what was sent.
    bus = RedisStreamBus(redis_client, maxlen=50)
    run = new_run_id()
    await bus.publish(run, [_env(run, seq) for seq in range(1, 301)])
    length = await redis_client.xlen(f"run:{run}:events")
    assert length <= 200  # 300 published, window stays bounded


async def test_run_store_publishes_committed_appends(
    pg: dict[str, str], redis_client: aioredis.Redis
) -> None:
    pool = await create_pool(pg["app_dsn"])
    try:
        bus = RedisStreamBus(redis_client)
        run_store = RunStore(pool, EventStore(pool, registry=_registry()), publisher=bus)
        tenant, run = new_tenant_id(), new_run_id()

        await run_store.create_run(tenant, run, input={})

        async for entry in bus.tail(run, after_seq=0):
            assert entry.seq == 1
            assert entry.event_type == "run.created"
            break
    finally:
        await pool.close()


async def test_scheduler_driven_appends_are_all_published(
    pg: dict[str, str], redis_client: aioredis.Redis
) -> None:
    # Regression: a worker-driven run must publish *every* event (not just
    # run.created), or a live subscriber tailing Redis would hang forever.
    registry = EventRegistry()
    register_run_events(registry)
    register_dag_events(registry)
    pool = await create_pool(pg["app_dsn"])
    try:
        bus = RedisStreamBus(redis_client)
        run_store = RunStore(pool, EventStore(pool, registry=registry), publisher=bus)
        tenant, run, node = new_tenant_id(), new_run_id(), new_node_id()

        await run_store.create_run(tenant, run, input={})
        lease = await run_store.acquire_lease(tenant, run, worker="w", ttl=timedelta(minutes=5))
        node_added = NodeAdded(
            node_id=node,
            role=NodeRole.TASK,
            dependencies=(),
            retry_policy=RetryPolicy(),
            budget=NodeBudget(),
        )
        await run_store.append_events(tenant, run, lease=lease, payloads=[node_added])
        await Scheduler(run_store, _NoopExecutor()).run(tenant, run, lease)

        log_types = [e.event_type for e in await run_store.read_events(tenant, run)]
        raw = await redis_client.xrange(f"run:{run}:events")
        stream = cast("list[tuple[str, dict[str, str]]]", raw)
        stream_types = [fields["event_type"] for _entry_id, fields in stream]
        assert stream_types == log_types
        assert "run.succeeded" in stream_types
    finally:
        await pool.close()
