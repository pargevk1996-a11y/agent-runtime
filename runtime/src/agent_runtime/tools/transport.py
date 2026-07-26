"""The tool transport: how a tool call actually reaches a tool process.

The dispatcher layers durability (idempotency, recovery) on top of a transport.
The MCP transport talks to tool processes over the Model Context Protocol; tests
use an in-process fake. The ``idempotency_key`` is passed through so a tool that
honours it can collapse a duplicate re-dispatch.
"""

from __future__ import annotations

from typing import Protocol

from agent_runtime.tools.model import ToolResult


class ToolTransport(Protocol):
    """Invokes a named tool with arguments, returning its result."""

    async def invoke(
        self, tool: str, args: dict[str, object], *, idempotency_key: str
    ) -> ToolResult:
        """Call ``tool``; raise a typed tool error on failure."""
        ...
