"""Model pricing and cost computation.

Costs are money, so they are :class:`~decimal.Decimal`, never float. Prices are
quoted per one million tokens and are illustrative/configurable — wire real
prices at deployment. The default book carries a few representative entries.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from decimal import Decimal

from agent_runtime.llm.errors import UnknownModelError
from agent_runtime.llm.provider import Usage

_PER_MILLION = Decimal(1_000_000)


@dataclass(frozen=True)
class ModelPrice:
    """Per-million-token input and output prices for one model."""

    input_per_mtok: Decimal
    output_per_mtok: Decimal


class PriceBook:
    """Maps ``(provider, model)`` to prices and computes call costs."""

    def __init__(self, prices: Mapping[tuple[str, str], ModelPrice]) -> None:
        self._prices = dict(prices)

    def cost(self, provider: str, model: str, usage: Usage) -> Decimal:
        """Dollar cost of a call. Raises :class:`UnknownModelError` if unpriced.

        Invariant: cost is non-negative and non-decreasing in either token count.
        """
        price = self._prices.get((provider, model))
        if price is None:
            raise UnknownModelError(
                "no price for model", context={"provider": provider, "model": model}
            )
        input_cost = Decimal(usage.input_tokens) * price.input_per_mtok
        output_cost = Decimal(usage.output_tokens) * price.output_per_mtok
        return (input_cost + output_cost) / _PER_MILLION

    @classmethod
    def default(cls) -> PriceBook:
        """A small illustrative price book. Replace with real prices in prod."""
        return cls(
            {
                ("anthropic", "claude-sonnet-5"): ModelPrice(Decimal("3"), Decimal("15")),
                ("openai", "gpt-4o"): ModelPrice(Decimal("2.5"), Decimal("10")),
                ("vllm", "local"): ModelPrice(Decimal("0"), Decimal("0")),
            }
        )
