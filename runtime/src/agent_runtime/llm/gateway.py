"""Metered access to an LLM provider.

The gateway wraps a raw :class:`LLMProvider` and, for every call, measures
latency, prices it, records it in the cost ledger, and enforces budgets. A call
that pushes a node past its cost or token budget raises
:class:`BudgetExceededError` — the failed call is still recorded (it was spent),
but the node then fails.

Budgets are passed as plain limits, not a DAG type, so this layer stays
independent of the scheduler; the caller translates a node's budget into them.
"""

from __future__ import annotations

import time
from decimal import Decimal

from agent_runtime.cost.ledger import CostLedger
from agent_runtime.ids import NodeId, RunId, TenantId
from agent_runtime.llm.errors import BudgetExceededError
from agent_runtime.llm.pricing import PriceBook
from agent_runtime.llm.provider import LLMProvider, LLMRequest, LLMResponse


class LLMGateway:
    """A provider wrapper that meters cost and enforces budgets."""

    def __init__(self, provider: LLMProvider, pricebook: PriceBook, ledger: CostLedger) -> None:
        self._provider = provider
        self._pricebook = pricebook
        self._ledger = ledger

    async def complete(
        self,
        request: LLMRequest,
        *,
        tenant_id: TenantId,
        run_id: RunId,
        node_id: NodeId | None = None,
        max_cost_usd: Decimal | None = None,
        max_tokens: int | None = None,
    ) -> LLMResponse:
        """Run a metered completion, recording it and enforcing node budgets."""
        start = time.perf_counter()
        response = await self._provider.complete(request)
        latency_ms = int((time.perf_counter() - start) * 1000)

        cost = self._pricebook.cost(self._provider.name, response.model, response.usage)
        await self._ledger.record(
            tenant_id=tenant_id,
            run_id=run_id,
            node_id=node_id,
            provider=self._provider.name,
            model=response.model,
            usage=response.usage,
            cost_usd=cost,
            latency_ms=latency_ms,
        )

        if node_id is not None and (max_cost_usd is not None or max_tokens is not None):
            spent = await self._ledger.node_cost(tenant_id, run_id, node_id)
            if max_cost_usd is not None and spent.cost_usd > max_cost_usd:
                raise BudgetExceededError(
                    "node cost budget exceeded",
                    context={"spent": str(spent.cost_usd), "limit": str(max_cost_usd)},
                )
            if max_tokens is not None and spent.total_tokens > max_tokens:
                raise BudgetExceededError(
                    "node token budget exceeded",
                    context={"spent": spent.total_tokens, "limit": max_tokens},
                )
        return response
