"""The LLM-call cost ledger.

Writes one row per metered call and aggregates spend by run, node, or tenant.
All access is tenant-scoped so Row-Level Security isolates one tenant's costs
from another's.
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal

import asyncpg

from agent_runtime.db.pool import tenant_connection
from agent_runtime.ids import NodeId, RunId, TenantId, partition_month, uuid7
from agent_runtime.llm.provider import Usage


@dataclass(frozen=True)
class CostSummary:
    """Aggregated spend over a set of ledger rows."""

    cost_usd: Decimal
    input_tokens: int
    output_tokens: int
    calls: int

    @property
    def total_tokens(self) -> int:
        return self.input_tokens + self.output_tokens


_INSERT = (
    "INSERT INTO llm_calls "
    "(partition_key, id, tenant_id, run_id, node_id, provider, model, "
    " input_tokens, output_tokens, cost_usd, latency_ms) "
    "VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)"
)
_SUMMARY_SELECT = (
    "SELECT COALESCE(SUM(cost_usd), 0) AS cost, "
    "COALESCE(SUM(input_tokens), 0) AS input_tokens, "
    "COALESCE(SUM(output_tokens), 0) AS output_tokens, "
    "COUNT(*) AS calls FROM llm_calls"
)
_RUN_COST = _SUMMARY_SELECT + " WHERE run_id = $1"
_NODE_COST = _SUMMARY_SELECT + " WHERE run_id = $1 AND node_id = $2"
_TENANT_COST = _SUMMARY_SELECT


class CostLedger:
    """Records and aggregates LLM-call costs."""

    def __init__(self, pool: asyncpg.Pool[asyncpg.Record]) -> None:
        self._pool = pool

    async def record(
        self,
        *,
        tenant_id: TenantId,
        run_id: RunId,
        node_id: NodeId | None,
        provider: str,
        model: str,
        usage: Usage,
        cost_usd: Decimal,
        latency_ms: int,
    ) -> None:
        """Append one call to the ledger."""
        async with tenant_connection(self._pool, tenant_id) as conn:
            await conn.execute(
                _INSERT,
                partition_month(run_id),
                uuid7(),
                tenant_id,
                run_id,
                node_id,
                provider,
                model,
                usage.input_tokens,
                usage.output_tokens,
                cost_usd,
                latency_ms,
            )

    async def run_cost(self, tenant_id: TenantId, run_id: RunId) -> CostSummary:
        """Total spend for a run."""
        return await self._summarize(tenant_id, _RUN_COST, run_id)

    async def node_cost(self, tenant_id: TenantId, run_id: RunId, node_id: NodeId) -> CostSummary:
        """Total spend for a single node within a run."""
        return await self._summarize(tenant_id, _NODE_COST, run_id, node_id)

    async def tenant_cost(self, tenant_id: TenantId) -> CostSummary:
        """Total spend for the whole tenant (RLS scopes the rows)."""
        return await self._summarize(tenant_id, _TENANT_COST)

    async def _summarize(self, tenant_id: TenantId, query: str, *args: object) -> CostSummary:
        async with tenant_connection(self._pool, tenant_id) as conn:
            row = await conn.fetchrow(query, *args)
        return CostSummary(
            cost_usd=Decimal(row["cost"]),
            input_tokens=int(row["input_tokens"]),
            output_tokens=int(row["output_tokens"]),
            calls=int(row["calls"]),
        )
