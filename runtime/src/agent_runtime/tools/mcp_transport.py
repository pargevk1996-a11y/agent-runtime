"""MCP transport: talk to a tool process over the Model Context Protocol.

Connects to a tool server over stdio and keeps the session open for reuse across
calls. The idempotency key is passed in the call metadata so a tool that honours
it can collapse a re-dispatch. Results come back as structured content when the
tool returns structured data, else as concatenated text.
"""

from __future__ import annotations

from contextlib import AsyncExitStack
from typing import Any

from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client
from mcp.types import CallToolResult, TextContent

from agent_runtime.tools.errors import ToolError, ToolUnavailableError
from agent_runtime.tools.model import ToolResult


def _text(result: CallToolResult) -> str:
    return "".join(block.text for block in result.content if isinstance(block, TextContent))


class MCPStdioTransport:
    """A tool transport backed by an MCP server subprocess over stdio."""

    def __init__(self, params: StdioServerParameters) -> None:
        self._params = params
        self._stack: AsyncExitStack | None = None
        self._session: ClientSession | None = None

    async def connect(self) -> None:
        """Start the tool process and initialize the MCP session."""
        stack = AsyncExitStack()
        read, write = await stack.enter_async_context(stdio_client(self._params))
        session = await stack.enter_async_context(ClientSession(read, write))
        await session.initialize()
        self._stack = stack
        self._session = session

    async def aclose(self) -> None:
        """Close the session and stop the tool process."""
        if self._stack is not None:
            await self._stack.aclose()
            self._stack = None
            self._session = None

    async def __aenter__(self) -> MCPStdioTransport:
        # connect/aclose must run in the same task (anyio cancel-scope rule), so
        # prefer using the transport as a context manager over split lifecycle.
        await self.connect()
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self.aclose()

    async def invoke(
        self, tool: str, args: dict[str, object], *, idempotency_key: str
    ) -> ToolResult:
        if self._session is None:
            raise ToolUnavailableError("transport is not connected", context={"tool": tool})
        arguments: dict[str, Any] = dict(args)
        result = await self._session.call_tool(
            tool, arguments=arguments, meta={"idempotency_key": idempotency_key}
        )
        if result.isError:
            raise ToolError(
                "tool returned an error", context={"tool": tool, "detail": _text(result)}
            )
        if result.structuredContent is not None:
            return ToolResult(output=dict(result.structuredContent))
        return ToolResult(output={"content": _text(result)})
