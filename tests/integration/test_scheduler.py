"""Integration tests for the DAG scheduler: ordering, retry, failure, recovery."""

from __future__ import annotations

from collections.abc import AsyncIterator, Callable
from datetime import timedelta

import asyncpg
import pytest

from agent_runtime.dag.events import NodeAdded, NodeStarted, register_dag_events
from agent_runtime.dag.executor import NodeContext
from agent_runtime.dag.model import NodeBudget, NodeRole, NodeStatus, RetryPolicy
from agent_runtime.dag.scheduler import Scheduler
from agent_runtime.dag.state import fold_dag
from agent_runtime.db.pool import create_pool
from agent_runtime.errors import RetryableError, TerminalError
from agent_runtime.events.registry import EventRegistry
from agent_runtime.events.store import EventStore
from agent_runtime.ids import NodeId, new_node_id, new_run_id, new_tenant_id
from agent_runtime.runs.events import RunStarted, register_run_events
from agent_runtime.runs.state import RunStatus
from agent_runtime.runs.store import RunStore

pytestmark = pytest.mark.integration

_Behavior = Callable[[], dict[str, object]]


def _registry() -> EventRegistry:
    registry = EventRegistry()
    register_run_events(registry)
    register_dag_events(registry)
    return registry


class _Executor:
    """Records calls; per-node behaviour defaults to success."""

    def __init__(self) -> None:
        self.calls: list[NodeId] = []
        self.behaviors: dict[NodeId, _Behavior] = {}

    async def execute(self, ctx: NodeContext) -> dict[str, object]:
        self.calls.append(ctx.node.node_id)
        behavior = self.behaviors.get(ctx.node.node_id)
        return behavior() if behavior is not None else {}


def _always_fail(error: Exception) -> _Behavior:
    def behavior() -> dict[str, object]:
        raise error

    return behavior


def _fail_then_ok(times: int, error: Exception) -> _Behavior:
    counter = {"n": 0}

    def behavior() -> dict[str, object]:
        if counter["n"] < times:
            counter["n"] += 1
            raise error
        return {}

    return behavior


def _node(node_id: NodeId, deps: tuple[NodeId, ...] = (), max_attempts: int = 1) -> NodeAdded:
    return NodeAdded(
        node_id=node_id,
        role=NodeRole.TASK,
        dependencies=deps,
        retry_policy=RetryPolicy(max_attempts=max_attempts),
        budget=NodeBudget(),
    )


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


async def test_linear_dag_runs_in_order(run_store: RunStore) -> None:
    tenant, run = new_tenant_id(), new_run_id()
    a, b, c = new_node_id(), new_node_id(), new_node_id()
    await run_store.create_run(tenant, run, input={})
    lease = await run_store.acquire_lease(tenant, run, worker="w", ttl=timedelta(minutes=5))
    await run_store.append_events(
        tenant, run, lease=lease, payloads=[_node(a), _node(b, (a,)), _node(c, (b,))]
    )

    executor = _Executor()
    await Scheduler(run_store, executor).run(tenant, run, lease)

    assert (await run_store.load_state(tenant, run)).status is RunStatus.SUCCEEDED
    dag = fold_dag(await run_store.read_events(tenant, run))
    assert all(n.status is NodeStatus.SUCCEEDED for n in dag.nodes.values())
    assert executor.calls == [a, b, c]


async def test_parallel_join_runs_child_after_parents(run_store: RunStore) -> None:
    tenant, run = new_tenant_id(), new_run_id()
    a, b, c = new_node_id(), new_node_id(), new_node_id()
    await run_store.create_run(tenant, run, input={})
    lease = await run_store.acquire_lease(tenant, run, worker="w", ttl=timedelta(minutes=5))
    await run_store.append_events(
        tenant, run, lease=lease, payloads=[_node(a), _node(b), _node(c, (a, b))]
    )

    executor = _Executor()
    await Scheduler(run_store, executor).run(tenant, run, lease)

    assert (await run_store.load_state(tenant, run)).status is RunStatus.SUCCEEDED
    assert executor.calls.index(c) > executor.calls.index(a)
    assert executor.calls.index(c) > executor.calls.index(b)


async def test_retry_then_success(run_store: RunStore) -> None:
    tenant, run = new_tenant_id(), new_run_id()
    a = new_node_id()
    await run_store.create_run(tenant, run, input={})
    lease = await run_store.acquire_lease(tenant, run, worker="w", ttl=timedelta(minutes=5))
    await run_store.append_events(tenant, run, lease=lease, payloads=[_node(a, max_attempts=2)])

    executor = _Executor()
    executor.behaviors[a] = _fail_then_ok(1, RetryableError("transient"))
    await Scheduler(run_store, executor).run(tenant, run, lease)

    assert (await run_store.load_state(tenant, run)).status is RunStatus.SUCCEEDED
    events = await run_store.read_events(tenant, run)
    starts = [e for e in events if isinstance(e.payload, NodeStarted) and e.payload.node_id == a]
    assert len(starts) == 2


async def test_terminal_failure_skips_dependents_and_fails_run(run_store: RunStore) -> None:
    tenant, run = new_tenant_id(), new_run_id()
    a, b = new_node_id(), new_node_id()
    await run_store.create_run(tenant, run, input={})
    lease = await run_store.acquire_lease(tenant, run, worker="w", ttl=timedelta(minutes=5))
    await run_store.append_events(tenant, run, lease=lease, payloads=[_node(a), _node(b, (a,))])

    executor = _Executor()
    executor.behaviors[a] = _always_fail(TerminalError("boom"))
    await Scheduler(run_store, executor).run(tenant, run, lease)

    assert (await run_store.load_state(tenant, run)).status is RunStatus.FAILED
    dag = fold_dag(await run_store.read_events(tenant, run))
    assert dag.nodes[a].status is NodeStatus.FAILED
    assert dag.nodes[b].status is NodeStatus.SKIPPED


async def test_retry_exhausted_fails_run(run_store: RunStore) -> None:
    tenant, run = new_tenant_id(), new_run_id()
    a = new_node_id()
    await run_store.create_run(tenant, run, input={})
    lease = await run_store.acquire_lease(tenant, run, worker="w", ttl=timedelta(minutes=5))
    await run_store.append_events(tenant, run, lease=lease, payloads=[_node(a, max_attempts=2)])

    executor = _Executor()
    executor.behaviors[a] = _always_fail(RetryableError("always"))
    await Scheduler(run_store, executor).run(tenant, run, lease)

    assert (await run_store.load_state(tenant, run)).status is RunStatus.FAILED
    assert executor.calls.count(a) == 2


async def test_recovery_resumes_running_node(run_store: RunStore) -> None:
    tenant, run = new_tenant_id(), new_run_id()
    a = new_node_id()
    await run_store.create_run(tenant, run, input={})
    lease = await run_store.acquire_lease(tenant, run, worker="w", ttl=timedelta(minutes=5))
    # Simulate a scheduler that started the run and node a, then crashed.
    await run_store.append_events(tenant, run, lease=lease, payloads=[_node(a)])
    await run_store.append_events(
        tenant,
        run,
        lease=lease,
        payloads=[RunStarted(worker="w"), NodeStarted(node_id=a, attempt=1)],
    )

    executor = _Executor()
    await Scheduler(run_store, executor).run(tenant, run, lease)

    assert (await run_store.load_state(tenant, run)).status is RunStatus.SUCCEEDED
    assert executor.calls == [a]  # re-executed once on recovery
    dag = fold_dag(await run_store.read_events(tenant, run))
    assert dag.nodes[a].status is NodeStatus.SUCCEEDED
