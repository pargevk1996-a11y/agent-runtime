"""Integration tests for the light subprocess isolate and code_exec tool."""

from __future__ import annotations

import sys

import pytest
from mcp import StdioServerParameters

from agent_runtime.tools.mcp_transport import MCPStdioTransport
from agent_runtime_tools.isolate import ExecLimits, IsolateError, SubprocessIsolate

pytestmark = pytest.mark.integration

_CODE_EXEC_SERVER = StdioServerParameters(
    command=sys.executable, args=["-m", "agent_runtime_tools.code_exec_server"]
)


async def test_runs_safe_code() -> None:
    result = await SubprocessIsolate().run("print(2 + 2)", ExecLimits())
    assert result.exit_code == 0
    assert result.stdout.strip() == "4"
    assert not result.timed_out


async def test_timeout_kills_busy_loop() -> None:
    result = await SubprocessIsolate().run("while True:\n    pass", ExecLimits(timeout_seconds=1.0))
    assert result.timed_out


async def test_memory_cap_fails_large_allocation() -> None:
    result = await SubprocessIsolate().run(
        "bytearray(1_000_000_000)", ExecLimits(memory_mb=128, timeout_seconds=5.0)
    )
    assert result.exit_code != 0


async def test_network_is_blocked() -> None:
    code = "import urllib.request; urllib.request.urlopen('http://example.com')"
    result = await SubprocessIsolate().run(code, ExecLimits(timeout_seconds=3.0))
    assert result.exit_code != 0


def test_refuses_to_start_in_production() -> None:
    with pytest.raises(IsolateError):
        SubprocessIsolate(environment="production")


async def test_code_exec_tool_end_to_end() -> None:
    async with MCPStdioTransport(_CODE_EXEC_SERVER) as transport:
        result = await transport.invoke("run_code", {"code": "print(6 * 7)"}, idempotency_key="k1")
    assert str(result.output["stdout"]).strip() == "42"
    assert result.output["exit_code"] == 0
