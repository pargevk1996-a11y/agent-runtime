"""Errors raised by the LLM layer, mapped onto the recovery taxonomy."""

from __future__ import annotations

from agent_runtime.errors import RetryableError, TerminalError


class ProviderError(TerminalError):
    """The provider rejected the request (bad request, auth, unsupported model)."""


class ProviderUnavailableError(RetryableError):
    """The provider was transiently unavailable (rate limit, 5xx, timeout)."""


class UnknownModelError(TerminalError):
    """No price is registered for the requested provider/model."""


class BudgetExceededError(TerminalError):
    """A call would push spend past a node or run budget.

    Terminal for the node: retrying the same call cannot fit the budget. The
    scheduler fails the node, which fails the run (fail-fast).
    """
