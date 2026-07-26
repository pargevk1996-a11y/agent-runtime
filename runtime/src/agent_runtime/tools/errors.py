"""Errors raised by the tool layer, mapped onto the recovery taxonomy."""

from __future__ import annotations

from agent_runtime.errors import IndeterminateError, RetryableError, TerminalError


class ToolError(TerminalError):
    """The tool rejected the call or failed permanently (bad args, tool bug)."""


class ToolUnavailableError(RetryableError):
    """The tool process was transiently unavailable; retrying may succeed."""


class ToolTimeoutError(RetryableError):
    """The tool did not respond within its timeout."""


class ToolIndeterminateError(IndeterminateError):
    """A non-idempotent call was in flight at a crash; its outcome is unknown."""


class EgressDeniedError(ToolError):
    """A tool attempted a network destination not on its allowlist."""


class IsolateError(ToolError):
    """The sandbox failed to run the code (setup failure, killed, over limits)."""
