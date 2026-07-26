"""Benchmark measurements: throughput, recovery latency, cost-per-task.

Each takes a connection pool and event registry, builds the stores it needs, and
returns numbers. They are driven either by ``__main__`` (against infrastructure
named in the environment) or by a smoke test (against test containers).
"""

from __future__ import annotations

from datetime import timedelta
from decimal import Decimal
from time import perf_counter

import asyncpg

from agent_runtime.cost.ledger import CostLedger
from agent_runtime.dag.events import NodeAdded
from agent_runtime.dag.executor import NodeContext, NodeExecutor, NodeResult
from agent_runtime.dag.scheduler import Scheduler
from agent_runtime.events.registry import EventRegistry
from agent_runtime.events.store import EventStore
from agent_runtime.ids import RunId, TenantId, new_run_id, new_tenant_id
from agent_runtime.llm.fake import FakeProvider
from agent_runtime.llm.gateway import LLMGateway
from agent_runtime.llm.pricing import ModelPrice, PriceBook
from agent_runtime.llm.provider import LLMRequest, Message
from agent_runtime.runs.snapshots import CheckpointManager
from agent_runtime.runs.store import RunStore
from agent_runtime_bench.workloads import Workload, linear_chain

_LEASE_TTL = timedelta(minutes=5)
_REQUEST = LLMRequest(model="m", messages=(Message(role="user", content="benchmark"),))


async def _drive(
    run_store: RunStore, executor: NodeExecutor, nodes: list[NodeAdded]
) -> tuple[TenantId, RunId]:
    tenant, run = new_tenant_id(), new_run_id()
    await run_store.create_run(tenant, run, input={})
    lease = await run_store.acquire_lease(tenant, run, worker="bench", ttl=_LEASE_TTL)
    await run_store.append_events(tenant, run, lease=lease, payloads=nodes)
    await Scheduler(run_store, executor).run(tenant, run, lease)
    return tenant, run


async def measure_throughput(
    pool: asyncpg.Pool[asyncpg.Record], registry: EventRegistry, workload: Workload, *, count: int
) -> float:
    """Runs of ``workload`` completed per second."""
    run_store = RunStore(pool, EventStore(pool, registry=registry))
    start = perf_counter()
    for _ in range(count):
        nodes, executor = workload()
        await _drive(run_store, executor, nodes)
    elapsed = perf_counter() - start
    return count / elapsed if elapsed > 0 else float("inf")


async def measure_recovery(
    pool: asyncpg.Pool[asyncpg.Record], registry: EventRegistry, workload: Workload, *, repeats: int
) -> dict[str, float]:
    """Average ``load_state`` latency (ms) folding the whole log vs a snapshot."""
    event_store = EventStore(pool, registry=registry)
    plain = RunStore(pool, event_store)
    nodes, executor = workload()
    tenant, run = await _drive(plain, executor, nodes)

    async def _avg_ms(store: RunStore) -> float:
        start = perf_counter()
        for _ in range(repeats):
            await store.load_state(tenant, run)
        return (perf_counter() - start) / repeats * 1000

    full_fold_ms = await _avg_ms(plain)

    checkpoints = CheckpointManager(pool)
    state = await plain.load_state(tenant, run)
    await checkpoints.save(tenant, run, state, at_seq=state.last_seq)
    snapshot_ms = await _avg_ms(RunStore(pool, event_store, checkpoints))

    return {"full_fold_ms": full_fold_ms, "snapshot_ms": snapshot_ms}


class _LLMExecutor:
    def __init__(self, gateway: LLMGateway) -> None:
        self._gateway = gateway

    async def execute(self, ctx: NodeContext) -> NodeResult:
        await self._gateway.complete(
            _REQUEST, tenant_id=ctx.tenant_id, run_id=ctx.run_id, node_id=ctx.node.node_id
        )
        return NodeResult(output={})


async def measure_cost(
    pool: asyncpg.Pool[asyncpg.Record],
    registry: EventRegistry,
    *,
    nodes_per_run: int,
    runs: int,
) -> float:
    """Average dollar cost per run for a chain of LLM-calling nodes."""
    ledger = CostLedger(pool)
    prices = PriceBook({("fake", "m"): ModelPrice(Decimal("1"), Decimal("2"))})
    provider = FakeProvider(name="fake", input_tokens=1000, output_tokens=500)
    executor = _LLMExecutor(LLMGateway(provider, prices, ledger))
    run_store = RunStore(pool, EventStore(pool, registry=registry))

    total = Decimal(0)
    for _ in range(runs):
        nodes, _ = linear_chain(nodes_per_run)
        tenant, run = await _drive(run_store, executor, nodes)
        total += (await ledger.run_cost(tenant, run)).cost_usd
    return float(total / runs)
