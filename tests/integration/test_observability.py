"""Integration tests: spans and metrics emitted while a run executes."""

from __future__ import annotations

from collections.abc import AsyncIterator, Iterator
from datetime import timedelta
from decimal import Decimal

import asyncpg
import pytest
import redis.asyncio as aioredis
from httpx import ASGITransport, AsyncClient
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

from agent_runtime.cost.ledger import CostLedger
from agent_runtime.dag.events import NodeAdded
from agent_runtime.dag.executor import NodeContext, NodeResult
from agent_runtime.dag.model import NodeBudget, NodeRole, RetryPolicy
from agent_runtime.dag.scheduler import Scheduler
from agent_runtime.db.pool import create_pool
from agent_runtime.events.store import EventStore
from agent_runtime.ids import new_node_id, new_run_id, new_tenant_id
from agent_runtime.llm.fake import FakeProvider
from agent_runtime.llm.gateway import LLMGateway
from agent_runtime.llm.pricing import ModelPrice, PriceBook
from agent_runtime.llm.provider import LLMRequest, Message
from agent_runtime.runs.store import RunStore
from agent_runtime.stream.bus import RedisStreamBus
from agent_runtime.telemetry.metrics import render_metrics
from agent_runtime.telemetry.tracing import configure_tracing
from agent_runtime.tools.dispatcher import ToolDispatcher
from agent_runtime.tools.model import IdempotencyClass, ToolResult, ToolSpec
from agent_runtime_api.app import build_app, full_registry

pytestmark = pytest.mark.integration

_EXPORTER = InMemorySpanExporter()
_PRICES = PriceBook({("fake", "m"): ModelPrice(Decimal("1"), Decimal("1"))})
_SPECS = {"echo": ToolSpec(name="echo", idempotency=IdempotencyClass.IDEMPOTENT)}
_REQUEST = LLMRequest(model="m", messages=(Message(role="user", content="hi"),))


@pytest.fixture(scope="module", autouse=True)
def _tracing() -> None:
    configure_tracing(exporter=_EXPORTER)


@pytest.fixture(autouse=True)
def _clear_spans() -> Iterator[None]:
    _EXPORTER.clear()
    yield


class _EchoTransport:
    async def invoke(
        self, tool: str, args: dict[str, object], *, idempotency_key: str
    ) -> ToolResult:
        return ToolResult(output={"ok": True})


class _InstrumentedExecutor:
    def __init__(self, gateway: LLMGateway, specs: dict[str, ToolSpec]) -> None:
        self._gateway = gateway
        self._transport = _EchoTransport()
        self._specs = specs

    async def execute(self, ctx: NodeContext) -> NodeResult:
        await self._gateway.complete(
            _REQUEST, tenant_id=ctx.tenant_id, run_id=ctx.run_id, node_id=ctx.node.node_id
        )
        dispatcher = ToolDispatcher(
            ctx.journal,
            self._transport,
            self._specs,
            run_id=ctx.run_id,
            node_id=ctx.node.node_id,
            attempt=ctx.attempt,
        )
        await dispatcher.call("echo", {})
        return NodeResult(output={})


@pytest.fixture
async def pool(pg: dict[str, str]) -> AsyncIterator[asyncpg.Pool[asyncpg.Record]]:
    created = await create_pool(pg["app_dsn"])
    try:
        yield created
    finally:
        await created.close()


async def test_run_emits_spans_and_metrics(pool: asyncpg.Pool[asyncpg.Record]) -> None:
    run_store = RunStore(pool, EventStore(pool, registry=full_registry()))
    gateway = LLMGateway(FakeProvider(name="fake"), _PRICES, CostLedger(pool))
    executor = _InstrumentedExecutor(gateway, _SPECS)

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
    await Scheduler(run_store, executor).run(tenant, run, lease)

    span_names = {span.name for span in _EXPORTER.get_finished_spans()}
    assert {"node.execute", "llm.complete", "tool.call"} <= span_names

    metrics = render_metrics().decode()
    for name in (
        "agentruntime_runs_total",
        "agentruntime_nodes_total",
        "agentruntime_llm_calls_total",
        "agentruntime_tool_calls_total",
    ):
        assert name in metrics


async def test_metrics_endpoint_serves_exposition(
    pool: asyncpg.Pool[asyncpg.Record], redis_client: aioredis.Redis
) -> None:
    bus = RedisStreamBus(redis_client)
    event_store = EventStore(pool, registry=full_registry())
    app = build_app(RunStore(pool, event_store, publisher=bus), event_store, bus)

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        response = await client.get("/metrics")
    assert response.status_code == 200
    assert "agentruntime_" in response.text
