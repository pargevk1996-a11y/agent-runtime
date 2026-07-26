"""Integration tests for the run worker."""

from __future__ import annotations

from collections.abc import AsyncIterator

import pytest

from agent_runtime.dag.events import NodeAdded, register_dag_events
from agent_runtime.dag.executor import NodeContext, NodeResult
from agent_runtime.dag.model import NodeBudget, NodeRole, RetryPolicy
from agent_runtime.db.pool import create_pool
from agent_runtime.events.registry import EventRegistry
from agent_runtime.events.store import EventStore
from agent_runtime.ids import new_node_id, new_run_id, new_tenant_id
from agent_runtime.runs.events import register_run_events
from agent_runtime.runs.state import RunStatus
from agent_runtime.runs.store import RunStore
from agent_runtime_agents.worker import Worker

pytestmark = pytest.mark.integration


class _NoopExecutor:
    async def execute(self, ctx: NodeContext) -> NodeResult:
        return NodeResult(output={})


def _one_task_plan() -> list[NodeAdded]:
    return [
        NodeAdded(
            node_id=new_node_id(),
            role=NodeRole.TASK,
            dependencies=(),
            retry_policy=RetryPolicy(),
            budget=NodeBudget(),
        )
    ]


def _registry() -> EventRegistry:
    registry = EventRegistry()
    register_run_events(registry)
    register_dag_events(registry)
    return registry


@pytest.fixture
async def run_store(pg: dict[str, str]) -> AsyncIterator[RunStore]:
    pool = await create_pool(pg["app_dsn"])
    try:
        yield RunStore(pool, EventStore(pool, registry=_registry()))
    finally:
        await pool.close()


async def test_worker_plans_and_executes_run(run_store: RunStore) -> None:
    tenant, run = new_tenant_id(), new_run_id()
    await run_store.create_run(tenant, run, input={})

    worker = Worker(run_store, _NoopExecutor(), planner=_one_task_plan)
    state = await worker.execute_run(tenant, run)
    assert state.status is RunStatus.SUCCEEDED


async def test_claim_next_picks_up_pending_run(run_store: RunStore) -> None:
    tenant, run = new_tenant_id(), new_run_id()
    await run_store.create_run(tenant, run, input={})

    worker = Worker(run_store, _NoopExecutor(), planner=_one_task_plan)
    claimed = await worker.claim_next(tenant)
    assert claimed == run
    assert (await run_store.load_state(tenant, run)).status is RunStatus.SUCCEEDED


async def test_claim_next_returns_none_when_idle(run_store: RunStore) -> None:
    worker = Worker(run_store, _NoopExecutor(), planner=_one_task_plan)
    assert await worker.claim_next(new_tenant_id()) is None
