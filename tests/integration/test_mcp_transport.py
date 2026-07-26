"""Integration test for the MCP stdio transport against a real tool subprocess."""

from __future__ import annotations

import sys

import pytest
from mcp import StdioServerParameters

from agent_runtime.tools.errors import ToolError
from agent_runtime.tools.mcp_transport import MCPStdioTransport

pytestmark = pytest.mark.integration

# Use the transport as a context manager: its connect/close must run in one task.
_ECHO_SERVER = StdioServerParameters(
    command=sys.executable, args=["-m", "agent_runtime_tools.echo_server"]
)


async def test_echo_round_trip() -> None:
    async with MCPStdioTransport(_ECHO_SERVER) as transport:
        result = await transport.invoke("echo", {"message": "hi"}, idempotency_key="k1")
    assert result.output == {"echo": "hi"}


async def test_unknown_tool_raises_tool_error() -> None:
    async with MCPStdioTransport(_ECHO_SERVER) as transport:
        with pytest.raises(ToolError):
            await transport.invoke("does_not_exist", {}, idempotency_key="k2")
