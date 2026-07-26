"""Integration tests for the cost ledger, gateway metering, and budget enforcement."""

from __future__ import annotations

from collections.abc import AsyncIterator
from datetime import timedelta
from decimal import Decimal

import asyncpg
import pytest

from agent_runtime.cost.ledger import CostLedger
from agent_runtime.dag.events import NodeAdded, register_dag_events
from agent_runtime.dag.executor import NodeContext, NodeResult
from agent_runtime.dag.model import NodeBudget, NodeRole, RetryPolicy
from agent_runtime.dag.scheduler import Scheduler
from agent_runtime.db.pool import create_pool
from agent_runtime.events.registry import EventRegistry
from agent_runtime.events.store import EventStore
from agent_runtime.ids import new_node_id, new_run_id, new_tenant_id
from agent_runtime.llm.errors import BudgetExceededError
from agent_runtime.llm.fake import FakeProvider
from agent_runtime.llm.gateway import LLMGateway
from agent_runtime.llm.pricing import ModelPrice, PriceBook
from agent_runtime.llm.provider import LLMRequest, Message
from agent_runtime.runs.events import register_run_events
from agent_runtime.runs.state import RunStatus
from agent_runtime.runs.store import RunStore

pytestmark = pytest.mark.integration

# One million input tokens at $3/Mtok = $3 per call.
_PRICES = PriceBook({("fake", "m"): ModelPrice(Decimal("3"), Decimal("0"))})
_REQUEST = LLMRequest(model="m", messages=(Message(role="user", content="hi"),))


def _gateway(pool: asyncpg.Pool[asyncpg.Record]) -> LLMGateway:
    provider = FakeProvider(name="fake", content="ok", input_tokens=1_000_000, output_tokens=0)
    return LLMGateway(provider, _PRICES, CostLedger(pool))


def _registry() -> EventRegistry:
    registry = EventRegistry()
    register_run_events(registry)
    register_dag_events(registry)
    return registry


class _LLMExecutor:
    """A node executor that makes one metered LLM call under the node's budget."""

    def __init__(self, gateway: LLMGateway) -> None:
        self._gateway = gateway

    async def execute(self, ctx: NodeContext) -> NodeResult:
        budget = ctx.node.budget
        max_cost = Decimal(str(budget.max_cost_usd)) if budget.max_cost_usd is not None else None
        response = await self._gateway.complete(
            _REQUEST,
            tenant_id=ctx.tenant_id,
            run_id=ctx.run_id,
            node_id=ctx.node.node_id,
            max_cost_usd=max_cost,
            max_tokens=budget.max_tokens,
        )
        return NodeResult(output={"content": response.content})


@pytest.fixture
async def pool(pg: dict[str, str]) -> AsyncIterator[asyncpg.Pool[asyncpg.Record]]:
    created = await create_pool(pg["app_dsn"])
    try:
        yield created
    finally:
        await created.close()


async def test_gateway_records_and_aggregates(pool: asyncpg.Pool[asyncpg.Record]) -> None:
    tenant, run, node = new_tenant_id(), new_run_id(), new_node_id()
    ledger = CostLedger(pool)

    await _gateway(pool).complete(_REQUEST, tenant_id=tenant, run_id=run, node_id=node)

    run_summary = await ledger.run_cost(tenant, run)
    assert run_summary.cost_usd == Decimal("3")
    assert run_summary.calls == 1
    assert run_summary.input_tokens == 1_000_000

    node_summary = await ledger.node_cost(tenant, run, node)
    assert node_summary.cost_usd == Decimal("3")


async def test_gateway_enforces_node_cost_budget(pool: asyncpg.Pool[asyncpg.Record]) -> None:
    tenant, run, node = new_tenant_id(), new_run_id(), new_node_id()
    ledger = CostLedger(pool)

    with pytest.raises(BudgetExceededError):
        await _gateway(pool).complete(
            _REQUEST, tenant_id=tenant, run_id=run, node_id=node, max_cost_usd=Decimal("1")
        )

    # The over-budget call is still recorded — the money was spent.
    assert (await ledger.run_cost(tenant, run)).calls == 1


async def test_rls_isolates_ledger(pool: asyncpg.Pool[asyncpg.Record]) -> None:
    tenant_a, tenant_b = new_tenant_id(), new_tenant_id()
    run, node = new_run_id(), new_node_id()
    ledger = CostLedger(pool)

    await _gateway(pool).complete(_REQUEST, tenant_id=tenant_a, run_id=run, node_id=node)

    assert (await ledger.run_cost(tenant_b, run)).calls == 0
    assert (await ledger.tenant_cost(tenant_b)).cost_usd == Decimal("0")
    assert (await ledger.run_cost(tenant_a, run)).calls == 1


async def test_scheduler_fails_run_when_budget_exceeded(
    pool: asyncpg.Pool[asyncpg.Record],
) -> None:
    tenant, run, node = new_tenant_id(), new_run_id(), new_node_id()
    run_store = RunStore(pool, EventStore(pool, registry=_registry()))
    ledger = CostLedger(pool)
    executor = _LLMExecutor(_gateway(pool))

    await run_store.create_run(tenant, run, input={})
    lease = await run_store.acquire_lease(tenant, run, worker="w", ttl=timedelta(minutes=5))
    node_added = NodeAdded(
        node_id=node,
        role=NodeRole.TASK,
        dependencies=(),
        retry_policy=RetryPolicy(),
        budget=NodeBudget(max_cost_usd=1.0),
    )
    await run_store.append_events(tenant, run, lease=lease, payloads=[node_added])

    await Scheduler(run_store, executor).run(tenant, run, lease)

    assert (await run_store.load_state(tenant, run)).status is RunStatus.FAILED
    assert (await ledger.run_cost(tenant, run)).calls == 1
