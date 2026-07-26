"""Unit and property tests for LLM types, pricing, and the fake provider."""

from __future__ import annotations

from decimal import Decimal

import pytest
from hypothesis import given
from hypothesis import strategies as st

from agent_runtime.llm.errors import UnknownModelError
from agent_runtime.llm.fake import FakeProvider
from agent_runtime.llm.pricing import ModelPrice, PriceBook
from agent_runtime.llm.provider import LLMRequest, Message, Usage

_BOOK = PriceBook({("p", "m"): ModelPrice(Decimal("3"), Decimal("15"))})
_MAX_TOKENS = 10_000_000


def test_cost_is_exact_per_million() -> None:
    assert _BOOK.cost("p", "m", Usage(input_tokens=1_000_000, output_tokens=0)) == Decimal("3")
    assert _BOOK.cost("p", "m", Usage(input_tokens=0, output_tokens=1_000_000)) == Decimal("15")


def test_unknown_model_raises() -> None:
    with pytest.raises(UnknownModelError):
        _BOOK.cost("p", "unpriced", Usage(input_tokens=1, output_tokens=1))


def test_usage_total_tokens() -> None:
    assert Usage(input_tokens=3, output_tokens=4).total_tokens == 7


@given(
    input_tokens=st.integers(min_value=0, max_value=_MAX_TOKENS),
    output_tokens=st.integers(min_value=0, max_value=_MAX_TOKENS),
    extra_input=st.integers(min_value=0, max_value=_MAX_TOKENS),
    extra_output=st.integers(min_value=0, max_value=_MAX_TOKENS),
)
def test_cost_is_non_negative_and_monotonic(
    input_tokens: int, output_tokens: int, extra_input: int, extra_output: int
) -> None:
    base = _BOOK.cost("p", "m", Usage(input_tokens=input_tokens, output_tokens=output_tokens))
    more = _BOOK.cost(
        "p",
        "m",
        Usage(input_tokens=input_tokens + extra_input, output_tokens=output_tokens + extra_output),
    )
    assert base >= 0
    assert more >= base


def test_default_price_book_prices_known_models() -> None:
    book = PriceBook.default()
    assert book.cost("vllm", "local", Usage(input_tokens=100, output_tokens=100)) == Decimal("0")
    assert book.cost("anthropic", "claude-sonnet-5", Usage(input_tokens=0, output_tokens=0)) == 0


async def test_fake_provider_is_deterministic() -> None:
    provider = FakeProvider(content="hi", input_tokens=7, output_tokens=2)
    request = LLMRequest(model="m", messages=(Message(role="user", content="q"),))

    first = await provider.complete(request)
    second = await provider.complete(request)
    assert first == second
    assert first.content == "hi"
    assert first.model == "m"
    assert first.usage.total_tokens == 9
    assert provider.name == "fake"
