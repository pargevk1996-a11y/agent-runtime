"""A deterministic in-process provider for tests and local development.

Returns a fixed response and usage without any network, so tests are fast and
reproducible. Real vendor adapters live behind the same :class:`LLMProvider`
protocol and are wired in separately.
"""

from __future__ import annotations

from agent_runtime.llm.provider import LLMRequest, LLMResponse, Usage


class FakeProvider:
    """A configurable, deterministic :class:`LLMProvider` implementation."""

    def __init__(
        self,
        *,
        name: str = "fake",
        content: str = "ok",
        input_tokens: int = 10,
        output_tokens: int = 5,
        finish_reason: str = "stop",
    ) -> None:
        self.name = name
        self._content = content
        self._usage = Usage(input_tokens=input_tokens, output_tokens=output_tokens)
        self._finish_reason = finish_reason

    async def complete(self, request: LLMRequest) -> LLMResponse:
        return LLMResponse(
            content=self._content,
            usage=self._usage,
            model=request.model,
            finish_reason=self._finish_reason,
        )
