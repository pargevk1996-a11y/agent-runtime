"""Integration tests for the web_fetch and sql_query reference tools."""

from __future__ import annotations

import os
import sys

import asyncpg
import pytest
from mcp import StdioServerParameters

from agent_runtime.tools.errors import ToolError
from agent_runtime.tools.mcp_transport import MCPStdioTransport
from agent_runtime_tools.sql_query_server import run_readonly_query

pytestmark = pytest.mark.integration


async def test_web_fetch_denies_host_off_allowlist() -> None:
    server = StdioServerParameters(
        command=sys.executable,
        args=["-m", "agent_runtime_tools.web_fetch_server"],
        env={**os.environ, "AR_WEB_FETCH_ALLOWLIST": "example.com"},
    )
    async with MCPStdioTransport(server) as transport:
        # A disallowed host is rejected before any network access happens.
        with pytest.raises(ToolError):
            await transport.invoke("fetch", {"url": "http://evil.test/"}, idempotency_key="k1")


async def test_sql_query_returns_rows(pg: dict[str, str]) -> None:
    rows = await run_readonly_query(pg["admin_dsn"], "SELECT 1 AS n, 'x' AS label")
    assert rows == [{"n": 1, "label": "x"}]


async def test_sql_query_rejects_writes(pg: dict[str, str]) -> None:
    with pytest.raises(asyncpg.PostgresError):
        await run_readonly_query(pg["admin_dsn"], "CREATE TABLE should_not_exist (id int)")
