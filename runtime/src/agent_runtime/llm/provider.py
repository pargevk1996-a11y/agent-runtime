"""Provider-agnostic chat-completion interface.

Every provider (Anthropic, OpenAI, vLLM) is adapted to this one interface so the
rest of the runtime never depends on a specific vendor SDK. Types are frozen —
a request and its response are immutable records of one exchange.
"""

from __future__ import annotations

from typing import Literal, Protocol

from pydantic import BaseModel, ConfigDict

Role = Literal["system", "user", "assistant"]


class Message(BaseModel):
    """One chat message."""

    model_config = ConfigDict(frozen=True)

    role: Role
    content: str


class Usage(BaseModel):
    """Token counts reported by the provider for a single call."""

    model_config = ConfigDict(frozen=True)

    input_tokens: int
    output_tokens: int

    @property
    def total_tokens(self) -> int:
        return self.input_tokens + self.output_tokens


class LLMRequest(BaseModel):
    """A provider-agnostic completion request."""

    model_config = ConfigDict(frozen=True)

    model: str
    messages: tuple[Message, ...]
    max_tokens: int | None = None
    temperature: float | None = None
    stop: tuple[str, ...] = ()


class LLMResponse(BaseModel):
    """A completion result plus the usage needed for accounting."""

    model_config = ConfigDict(frozen=True)

    content: str
    usage: Usage
    model: str
    finish_reason: str
    provider_request_id: str | None = None


class LLMProvider(Protocol):
    """A chat-completion backend.

    ``name`` identifies the vendor (e.g. ``"anthropic"``) and, with the model,
    keys into the price book for cost accounting.
    """

    name: str

    async def complete(self, request: LLMRequest) -> LLMResponse:
        """Run a completion, or raise a typed provider error."""
        ...
