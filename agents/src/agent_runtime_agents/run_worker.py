"""A reference worker process: drain a tenant's pending runs.

    AR_WORKER_TENANT=<uuid> python -m agent_runtime_agents.run_worker

Polls for pending runs of one tenant and drives each to completion. The node
executor here is a no-op — a real deployment supplies its own executor (e.g. a
CEV composition wired to an LLM gateway) and, if runs are created without a DAG,
a planner. Cross-tenant fair scheduling is an ops concern (see ADR/worker notes).
"""

from __future__ import annotations

import asyncio
import os
from uuid import UUID

from agent_runtime.config import get_settings
from agent_runtime.dag.events import register_dag_events
from agent_runtime.dag.executor import NodeContext, NodeResult
from agent_runtime.db.pool import create_pool
from agent_runtime.events.registry import EventRegistry
from agent_runtime.events.store import EventStore
from agent_runtime.ids import TenantId
from agent_runtime.logging import get_logger
from agent_runtime.runs.events import register_run_events
from agent_runtime.runs.store import RunStore
from agent_runtime.tools.events import register_tool_events
from agent_runtime_agents.worker import Worker

_log = get_logger(__name__)
_IDLE_SLEEP_SECONDS = 2.0


class _NoopExecutor:
    async def execute(self, ctx: NodeContext) -> NodeResult:
        return NodeResult(output={})


def _registry() -> EventRegistry:
    registry = EventRegistry()
    register_run_events(registry)
    register_dag_events(registry)
    register_tool_events(registry)
    return registry


async def main() -> None:
    tenant = TenantId(UUID(os.environ["AR_WORKER_TENANT"]))
    settings = get_settings()
    pool = await create_pool(str(settings.db_app_dsn))
    worker = Worker(RunStore(pool, EventStore(pool, registry=_registry())), _NoopExecutor())
    _log.info("worker_started", tenant_id=str(tenant))
    try:
        while True:
            claimed = await worker.claim_next(tenant)
            if claimed is None:
                await asyncio.sleep(_IDLE_SLEEP_SECONDS)
            else:
                _log.info("run_executed", run_id=str(claimed))
    finally:
        await pool.close()


if __name__ == "__main__":
    asyncio.run(main())
