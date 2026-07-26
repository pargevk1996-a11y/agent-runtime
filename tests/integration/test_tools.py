"""Integration test: durable tool dispatch end-to-end through the scheduler."""

from __future__ import annotations

from collections.abc import AsyncIterator
from datetime import timedelta

import asyncpg
import pytest

from agent_runtime.dag.events import NodeAdded, register_dag_events
from agent_runtime.dag.executor import NodeContext, NodeResult
from agent_runtime.dag.model import NodeBudget, NodeRole, RetryPolicy
from agent_runtime.dag.scheduler import Scheduler
from agent_runtime.db.pool import create_pool
from agent_runtime.events.registry import EventRegistry
from agent_runtime.events.store import EventStore
from agent_runtime.ids import new_node_id, new_run_id, new_tenant_id
from agent_runtime.runs.events import register_run_events
from agent_runtime.runs.state import RunStatus
from agent_runtime.runs.store import RunStore
from agent_runtime.tools.dispatcher import ToolDispatcher
from agent_runtime.tools.events import ToolCallCompleted, ToolCallRequested, register_tool_events
from agent_runtime.tools.model import IdempotencyClass, ToolResult, ToolSpec

pytestmark = pytest.mark.integration

_SPECS = {"echo": ToolSpec(name="echo", idempotency=IdempotencyClass.IDEMPOTENT)}


class _EchoTransport:
    def __init__(self) -> None:
        self.calls = 0

    async def invoke(
        self, tool: str, args: dict[str, object], *, idempotency_key: str
    ) -> ToolResult:
        self.calls += 1
        return ToolResult(output={"echo": args})


class _ToolExecutor:
    def __init__(self, transport: _EchoTransport) -> None:
        self._transport = transport

    async def execute(self, ctx: NodeContext) -> NodeResult:
        dispatcher = ToolDispatcher(
            ctx.journal,
            self._transport,
            _SPECS,
            run_id=ctx.run_id,
            node_id=ctx.node.node_id,
            attempt=ctx.attempt,
        )
        result = await dispatcher.call("echo", {"n": 1})
        return NodeResult(output=dict(result.output))


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


async def test_tool_call_is_recorded_in_the_log(pool: asyncpg.Pool[asyncpg.Record]) -> None:
    tenant, run, node = new_tenant_id(), new_run_id(), new_node_id()
    run_store = RunStore(pool, EventStore(pool, registry=_registry()))
    transport = _EchoTransport()

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

    await Scheduler(run_store, _ToolExecutor(transport)).run(tenant, run, lease)

    assert (await run_store.load_state(tenant, run)).status is RunStatus.SUCCEEDED
    assert transport.calls == 1

    events = await run_store.read_events(tenant, run)
    requested = [e for e in events if isinstance(e.payload, ToolCallRequested)]
    completed = [e for e in events if isinstance(e.payload, ToolCallCompleted)]
    assert len(requested) == 1
    assert len(completed) == 1
    # intent is recorded before the result
    assert requested[0].seq < completed[0].seq
