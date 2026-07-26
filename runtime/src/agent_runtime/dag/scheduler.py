"""The DAG scheduler: drives a run's task graph to completion.

The scheduler is the single writer for a run (it holds the lease). Each loop it
projects the DAG from the log, dispatches ready nodes with bounded concurrency,
and appends node events as work completes — so the log is always the source of
truth and a crash simply resumes from it. A node found ``RUNNING`` on a fresh
start (a prior crash) is re-executed; execution is at-least-once.

Failure is fail-fast: the first terminal node failure skips still-pending nodes
and fails the run. In-flight nodes are allowed to finish (no forced cancellation
here — that arrives with cancellation support).
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from opentelemetry.trace import Span

from agent_runtime.dag.errors import MaxReflectionDepthError
from agent_runtime.dag.events import (
    EdgeAdded,
    NodeAdded,
    NodeFailed,
    NodeSkipped,
    NodeStarted,
    NodeSucceeded,
)
from agent_runtime.dag.executor import NodeContext, NodeExecutor, NodeResult
from agent_runtime.dag.model import EdgeType, NodeStatus
from agent_runtime.dag.state import DagOutcome, DagState, Node, fold_dag
from agent_runtime.errors import AgentRuntimeError, IndeterminateError, RetryableError
from agent_runtime.events.envelope import EventPayload
from agent_runtime.ids import NodeId, RunId, TenantId
from agent_runtime.logging import get_logger
from agent_runtime.runs.events import RunCancelled, RunFailed, RunStarted, RunSucceeded
from agent_runtime.runs.journal import LeaseJournal
from agent_runtime.runs.state import RunStatus
from agent_runtime.runs.store import Lease, RunStore
from agent_runtime.telemetry.metrics import record_node, record_run
from agent_runtime.telemetry.tracing import get_tracer

_log = get_logger(__name__)
_tracer = get_tracer(__name__)


@asynccontextmanager
async def _span(name: str) -> AsyncIterator[Span]:
    """Async wrapper over the sync span CM, so it composes in one ``async with``."""
    with _tracer.start_as_current_span(name) as span:
        yield span


_RETRYABLE = "retryable"
_INDETERMINATE = "indeterminate"
_TERMINAL = "terminal"


def categorize(exc: BaseException) -> str:
    """Map an exception to a retry category from the error taxonomy."""
    if isinstance(exc, RetryableError):
        return _RETRYABLE
    if isinstance(exc, IndeterminateError):
        return _INDETERMINATE
    return _TERMINAL


class Scheduler:
    """Drives one run's DAG, appending node/run events under a held lease."""

    def __init__(
        self,
        run_store: RunStore,
        executor: NodeExecutor,
        *,
        concurrency: int = 8,
        max_reflection_depth: int = 3,
    ) -> None:
        self._runs = run_store
        self._executor = executor
        self._sem = asyncio.Semaphore(concurrency)
        self._max_reflection_depth = max_reflection_depth

    async def run(self, tenant_id: TenantId, run_id: RunId, lease: Lease) -> None:
        """Drive the run to a terminal state (SUCCEEDED or FAILED)."""
        await self._ensure_started(tenant_id, run_id, lease)

        in_flight: set[NodeId] = set()
        tasks: dict[asyncio.Task[None], NodeId] = {}
        while True:
            if await self._runs.is_cancel_requested(tenant_id, run_id):
                await self._drain(tasks, in_flight)
                final = fold_dag(await self._runs.read_events(tenant_id, run_id))
                await self._finalize_cancelled(tenant_id, run_id, lease, final)
                return

            state = fold_dag(await self._runs.read_events(tenant_id, run_id))
            outcome = state.outcome()

            if outcome is DagOutcome.FAILED:
                await self._drain(tasks, in_flight)
                final = fold_dag(await self._runs.read_events(tenant_id, run_id))
                await self._finalize_failed(tenant_id, run_id, lease, final)
                return

            dispatchable = self._dispatchable(state, in_flight)
            if not dispatchable and not tasks:
                if outcome is DagOutcome.SUCCEEDED or not state.nodes:
                    await self._finalize_succeeded(tenant_id, run_id, lease)
                else:
                    await self._finalize_deadlock(tenant_id, run_id, lease)
                return

            for node in dispatchable:
                inputs = self._collect_inputs(state, node)
                task = asyncio.create_task(
                    self._execute_node(tenant_id, run_id, lease, node, inputs)
                )
                tasks[task] = node.node_id
                in_flight.add(node.node_id)

            done, _ = await asyncio.wait(set(tasks), return_when=asyncio.FIRST_COMPLETED)
            for task in done:
                node_id = tasks.pop(task)
                in_flight.discard(node_id)
                task.result()  # surface scheduler-level failures (not node failures)

    async def _ensure_started(self, tenant_id: TenantId, run_id: RunId, lease: Lease) -> None:
        state = await self._runs.load_state(tenant_id, run_id)
        if state.status is RunStatus.PENDING:
            await self._runs.append_events(
                tenant_id, run_id, lease=lease, payloads=[RunStarted(worker=lease.worker)]
            )

    def _dispatchable(self, state: DagState, in_flight: set[NodeId]) -> list[Node]:
        ready = [node for node in state.ready_set() if node.node_id not in in_flight]
        # RUNNING but not tracked in this process = a prior crash; re-execute it.
        recovering = sorted(
            (
                node
                for node in state.nodes.values()
                if node.status is NodeStatus.RUNNING and node.node_id not in in_flight
            ),
            key=lambda n: n.node_id,
        )
        return ready + recovering

    def _collect_inputs(self, state: DagState, node: Node) -> dict[NodeId, dict[str, object]]:
        deps = [
            edge.from_node
            for edge in state.edges
            if edge.edge_type is EdgeType.DEPENDENCY and edge.to_node == node.node_id
        ]
        return {dep: (state.nodes[dep].output or {}) for dep in deps}

    async def _execute_node(
        self,
        tenant_id: TenantId,
        run_id: RunId,
        lease: Lease,
        node: Node,
        inputs: dict[NodeId, dict[str, object]],
    ) -> None:
        started = time.perf_counter()
        async with self._sem, _span("node.execute") as span:
            span.set_attribute("run_id", str(run_id))
            span.set_attribute("node_id", str(node.node_id))
            span.set_attribute("node_role", node.role.value)
            # Recovering a RUNNING node reuses its attempt so tool idempotency keys
            # match and completed calls are reused; a fresh/retry node advances it.
            attempt = node.attempt if node.status is NodeStatus.RUNNING else node.attempt + 1
            journal = LeaseJournal(self._runs, tenant_id, run_id, lease)
            while True:
                await self._runs.append_events(
                    tenant_id,
                    run_id,
                    lease=lease,
                    payloads=[NodeStarted(node_id=node.node_id, attempt=attempt)],
                )
                try:
                    context = NodeContext(
                        node=node,
                        inputs=inputs,
                        tenant_id=tenant_id,
                        run_id=run_id,
                        journal=journal,
                        attempt=attempt,
                    )
                    result = await self._executor.execute(context)
                except AgentRuntimeError as exc:
                    policy = node.retry_policy
                    can_retry = attempt < policy.max_attempts and categorize(exc) in policy.retry_on
                    if can_retry:
                        attempt += 1
                        continue
                    await self._append_failure(tenant_id, run_id, lease, node, attempt, exc)
                    record_node("failed", time.perf_counter() - started)
                    return
                except Exception as exc:
                    # Isolate a node's failure (even an untyped bug) from the scheduler.
                    await self._append_failure(tenant_id, run_id, lease, node, attempt, exc)
                    record_node("failed", time.perf_counter() - started)
                    return
                else:
                    ok = await self._append_success(tenant_id, run_id, lease, node, attempt, result)
                    record_node("succeeded" if ok else "failed", time.perf_counter() - started)
                    return

    async def _append_success(
        self,
        tenant_id: TenantId,
        run_id: RunId,
        lease: Lease,
        node: Node,
        attempt: int,
        result: NodeResult,
    ) -> bool:
        """Append the node's success (and any expansion); False if it instead failed."""
        too_deep = [s for s in result.spawn if s.reflection_depth > self._max_reflection_depth]
        if too_deep:
            await self._append_failure(
                tenant_id,
                run_id,
                lease,
                node,
                attempt,
                MaxReflectionDepthError(
                    "reflection would exceed the maximum depth",
                    context={"max": self._max_reflection_depth},
                ),
            )
            return False

        payloads: list[EventPayload] = [NodeSucceeded(node_id=node.node_id, output=result.output)]
        for spawn in result.spawn:
            payloads.append(
                NodeAdded(
                    node_id=spawn.node_id,
                    role=spawn.role,
                    dependencies=spawn.dependencies,
                    retry_policy=spawn.retry_policy,
                    budget=spawn.budget,
                    reflection_depth=spawn.reflection_depth,
                )
            )
        for edge in result.edges:
            payloads.append(
                EdgeAdded(from_node=edge.from_node, to_node=edge.to_node, edge_type=edge.edge_type)
            )
        await self._runs.append_events(tenant_id, run_id, lease=lease, payloads=payloads)
        return True

    async def _append_failure(
        self,
        tenant_id: TenantId,
        run_id: RunId,
        lease: Lease,
        node: Node,
        attempt: int,
        exc: BaseException,
    ) -> None:
        await self._runs.append_events(
            tenant_id,
            run_id,
            lease=lease,
            payloads=[
                NodeFailed(
                    node_id=node.node_id,
                    attempt=attempt,
                    error_class=type(exc).__name__,
                    message=str(exc),
                )
            ],
        )

    async def _drain(self, tasks: dict[asyncio.Task[None], NodeId], in_flight: set[NodeId]) -> None:
        if not tasks:
            return
        await asyncio.gather(*tasks, return_exceptions=True)
        for node_id in tasks.values():
            in_flight.discard(node_id)
        tasks.clear()

    async def _finalize_succeeded(self, tenant_id: TenantId, run_id: RunId, lease: Lease) -> None:
        await self._runs.append_events(
            tenant_id, run_id, lease=lease, payloads=[RunSucceeded(result={})]
        )
        record_run("succeeded")

    async def _finalize_failed(
        self, tenant_id: TenantId, run_id: RunId, lease: Lease, state: DagState
    ) -> None:
        pending = sorted(
            (n for n in state.nodes.values() if n.status is NodeStatus.PENDING),
            key=lambda n: n.node_id,
        )
        failed = next((n for n in state.nodes.values() if n.status is NodeStatus.FAILED), None)
        payloads: list[EventPayload] = [
            NodeSkipped(node_id=n.node_id, reason="upstream failure") for n in pending
        ]
        payloads.append(
            RunFailed(
                error_class=(failed.error_class if failed else None) or "SchedulerError",
                message=(failed.error_message if failed else None) or "run failed",
            )
        )
        await self._runs.append_events(tenant_id, run_id, lease=lease, payloads=payloads)
        record_run("failed")

    async def _finalize_cancelled(
        self, tenant_id: TenantId, run_id: RunId, lease: Lease, state: DagState
    ) -> None:
        pending = sorted(
            (n for n in state.nodes.values() if n.status is NodeStatus.PENDING),
            key=lambda n: n.node_id,
        )
        payloads: list[EventPayload] = [
            NodeSkipped(node_id=n.node_id, reason="run cancelled") for n in pending
        ]
        payloads.append(RunCancelled(reason="cancelled"))
        await self._runs.append_events(tenant_id, run_id, lease=lease, payloads=payloads)
        record_run("cancelled")

    async def _finalize_deadlock(self, tenant_id: TenantId, run_id: RunId, lease: Lease) -> None:
        _log.warning("scheduler_deadlock", run_id=str(run_id))
        await self._runs.append_events(
            tenant_id,
            run_id,
            lease=lease,
            payloads=[RunFailed(error_class="DeadlockError", message="no runnable nodes remain")],
        )
        record_run("failed")
