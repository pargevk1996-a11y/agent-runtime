"""Integration tests for the SDK client against the control-plane app."""

from __future__ import annotations

from collections.abc import AsyncIterator
from datetime import timedelta

import pytest
import redis.asyncio as aioredis
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient

from agent_runtime.dag.events import NodeAdded
from agent_runtime.dag.executor import NodeContext, NodeResult
from agent_runtime.dag.model import NodeBudget, NodeRole, RetryPolicy
from agent_runtime.dag.scheduler import Scheduler
from agent_runtime.db.pool import create_pool
from agent_runtime.events.store import EventStore
from agent_runtime.ids import TenantId, new_node_id, new_run_id, new_tenant_id
from agent_runtime.runs.snapshots import CheckpointManager
from agent_runtime.runs.store import RunStore
from agent_runtime.stream.bus import RedisStreamBus
from agent_runtime_api.app import build_app, full_registry
from agent_runtime_sdk.client import AgentRuntimeClient

pytestmark = pytest.mark.integration


class _NoopExecutor:
    async def execute(self, ctx: NodeContext) -> NodeResult:
        return NodeResult(output={})


@pytest.fixture
async def env(
    pg: dict[str, str], redis_client: aioredis.Redis
) -> AsyncIterator[tuple[FastAPI, RunStore]]:
    pool = await create_pool(pg["app_dsn"])
    try:
        bus = RedisStreamBus(redis_client)
        event_store = EventStore(pool, registry=full_registry())
        run_store = RunStore(pool, event_store, CheckpointManager(pool), publisher=bus)
        yield build_app(run_store, event_store, bus), run_store
    finally:
        await pool.close()


def _sdk(app: FastAPI, tenant: TenantId) -> AgentRuntimeClient:
    http = AsyncClient(transport=ASGITransport(app=app), base_url="http://test")
    return AgentRuntimeClient("http://test", tenant, client=http)


async def test_create_get_and_replay(env: tuple[FastAPI, RunStore]) -> None:
    app, _ = env
    async with _sdk(app, new_tenant_id()) as client:
        run_id = await client.create_run({"goal": 1})
        status = await client.get_run(run_id)
        assert status["status"] == "pending"

        events = await client.replay(run_id)
        assert events[0]["event_type"] == "run.created"


async def test_cancel(env: tuple[FastAPI, RunStore]) -> None:
    app, _ = env
    async with _sdk(app, new_tenant_id()) as client:
        run_id = await client.create_run()
        await client.cancel_run(run_id)  # raises on non-2xx; 202 is fine


async def test_subscribe_streams_until_terminal(env: tuple[FastAPI, RunStore]) -> None:
    app, run_store = env
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

    async with _sdk(app, tenant) as client:
        event_types = [event["event_type"] async for event in client.subscribe(run)]

    assert event_types[0] == "run.created"
    assert event_types[-1] == "run.succeeded"
