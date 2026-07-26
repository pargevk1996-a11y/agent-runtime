"""A run worker: claim a run, plan it, and drive it to completion.

The worker acquires the run's lease, optionally seeds its DAG via a planner (for
a freshly created run with no graph yet), and runs the scheduler with the
configured node executor. ``claim_next`` finds a pending run for a tenant and
executes it; cross-tenant fair scheduling is an ops concern left out of scope.
"""

from __future__ import annotations

from collections.abc import Callable
from datetime import timedelta

from agent_runtime.dag.events import NodeAdded
from agent_runtime.dag.executor import NodeExecutor
from agent_runtime.dag.scheduler import Scheduler
from agent_runtime.dag.state import fold_dag
from agent_runtime.ids import RunId, TenantId
from agent_runtime.runs.state import RunState
from agent_runtime.runs.store import RunStore

Planner = Callable[[], list[NodeAdded]]


class Worker:
    """Executes runs by leasing them and driving the scheduler."""

    def __init__(
        self,
        run_store: RunStore,
        executor: NodeExecutor,
        *,
        planner: Planner | None = None,
        worker_id: str = "worker",
        lease_ttl: timedelta = timedelta(minutes=5),
        concurrency: int = 8,
        max_reflection_depth: int = 3,
    ) -> None:
        self._runs = run_store
        self._executor = executor
        self._planner = planner
        self._worker_id = worker_id
        self._lease_ttl = lease_ttl
        self._concurrency = concurrency
        self._max_reflection_depth = max_reflection_depth

    async def execute_run(self, tenant_id: TenantId, run_id: RunId) -> RunState:
        """Lease, (plan if empty), and run a single run to a terminal state."""
        lease = await self._runs.acquire_lease(
            tenant_id, run_id, worker=self._worker_id, ttl=self._lease_ttl
        )
        if self._planner is not None:
            dag = fold_dag(await self._runs.read_events(tenant_id, run_id))
            if not dag.nodes:
                await self._runs.append_events(
                    tenant_id, run_id, lease=lease, payloads=self._planner()
                )

        scheduler = Scheduler(
            self._runs,
            self._executor,
            concurrency=self._concurrency,
            max_reflection_depth=self._max_reflection_depth,
        )
        await scheduler.run(tenant_id, run_id, lease)
        return await self._runs.load_state(tenant_id, run_id)

    async def claim_next(self, tenant_id: TenantId) -> RunId | None:
        """Execute one pending run for the tenant; return its id, or ``None``."""
        run_id = await self._runs.find_pending(tenant_id)
        if run_id is None:
            return None
        await self.execute_run(tenant_id, run_id)
        return run_id
