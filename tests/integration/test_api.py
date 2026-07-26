"""Integration tests for the FastAPI control plane via httpx ASGI transport."""

from __future__ import annotations

import json
from collections.abc import AsyncIterator
from dataclasses import dataclass
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
from agent_runtime.ids import new_node_id, new_run_id, new_tenant_id
from agent_runtime.runs.snapshots import CheckpointManager
from agent_runtime.runs.store import RunStore
from agent_runtime.stream.bus import RedisStreamBus
from agent_runtime_api.app import build_app, full_registry

pytestmark = pytest.mark.integration


class _NoopExecutor:
    async def execute(self, ctx: NodeContext) -> NodeResult:
        return NodeResult(output={})


@dataclass
class _Api:
    app: FastAPI
    run_store: RunStore


@pytest.fixture
async def api(pg: dict[str, str], redis_client: aioredis.Redis) -> AsyncIterator[_Api]:
    pool = await create_pool(pg["app_dsn"])
    try:
        bus = RedisStreamBus(redis_client)
        event_store = EventStore(pool, registry=full_registry())
        run_store = RunStore(pool, event_store, CheckpointManager(pool), publisher=bus)
        yield _Api(app=build_app(run_store, event_store, bus), run_store=run_store)
    finally:
        await pool.close()


def _client(app: FastAPI) -> AsyncClient:
    return AsyncClient(transport=ASGITransport(app=app), base_url="http://test")


async def test_create_and_get_run(api: _Api) -> None:
    tenant = new_tenant_id()
    headers = {"X-Tenant-Id": str(tenant)}
    async with _client(api.app) as client:
        created = await client.post("/runs", json={"input": {"goal": 1}}, headers=headers)
        assert created.status_code == 201
        run_id = created.json()["run_id"]

        status = await client.get(f"/runs/{run_id}", headers=headers)
        assert status.status_code == 200
        assert status.json()["status"] == "pending"


async def test_missing_tenant_header_is_422(api: _Api) -> None:
    async with _client(api.app) as client:
        assert (await client.post("/runs", json={"input": {}})).status_code == 422


async def test_invalid_tenant_header_is_400(api: _Api) -> None:
    async with _client(api.app) as client:
        resp = await client.post("/runs", json={"input": {}}, headers={"X-Tenant-Id": "nope"})
        assert resp.status_code == 400


async def test_unknown_run_is_404(api: _Api) -> None:
    headers = {"X-Tenant-Id": str(new_tenant_id())}
    async with _client(api.app) as client:
        assert (await client.get(f"/runs/{new_run_id()}", headers=headers)).status_code == 404


async def test_cancel_returns_202(api: _Api) -> None:
    headers = {"X-Tenant-Id": str(new_tenant_id())}
    async with _client(api.app) as client:
        created = await client.post("/runs", json={"input": {}}, headers=headers)
        run_id = created.json()["run_id"]
        assert (await client.post(f"/runs/{run_id}/cancel", headers=headers)).status_code == 202


async def test_replay_returns_events(api: _Api) -> None:
    headers = {"X-Tenant-Id": str(new_tenant_id())}
    async with _client(api.app) as client:
        created = await client.post("/runs", json={"input": {}}, headers=headers)
        run_id = created.json()["run_id"]
        replay = await client.get(f"/runs/{run_id}/replay", headers=headers)
        assert replay.status_code == 200
        assert replay.json()[0]["event_type"] == "run.created"


async def test_sse_streams_events_until_terminal(api: _Api) -> None:
    tenant, run, node = new_tenant_id(), new_run_id(), new_node_id()
    await api.run_store.create_run(tenant, run, input={})
    lease = await api.run_store.acquire_lease(tenant, run, worker="w", ttl=timedelta(minutes=5))
    node_added = NodeAdded(
        node_id=node,
        role=NodeRole.TASK,
        dependencies=(),
        retry_policy=RetryPolicy(),
        budget=NodeBudget(),
    )
    await api.run_store.append_events(tenant, run, lease=lease, payloads=[node_added])
    await Scheduler(api.run_store, _NoopExecutor()).run(tenant, run, lease)

    event_types: list[str] = []
    async with _client(api.app) as client:
        async with client.stream(
            "GET", f"/runs/{run}/events", headers={"X-Tenant-Id": str(tenant)}
        ) as response:
            assert response.status_code == 200
            async for line in response.aiter_lines():
                if line.startswith("data: "):
                    event_types.append(json.loads(line[6:])["event_type"])

    assert event_types[0] == "run.created"
    assert event_types[-1] == "run.succeeded"
