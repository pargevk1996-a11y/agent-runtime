"""Reference MCP tool server: read-only SQL queries.

Runs a query inside a read-only transaction, so the database itself rejects any
write — the enforcement is not a fragile string check. The DSN comes from
``AR_SQL_DSN``. Runs as its own process:

    python -m agent_runtime_tools.sql_query_server
"""

from __future__ import annotations

import os

import asyncpg
from mcp.server.fastmcp import FastMCP

mcp = FastMCP("sql_query")


async def run_readonly_query(dsn: str, sql: str) -> list[dict[str, object]]:
    """Execute ``sql`` in a read-only transaction and return the rows."""
    conn = await asyncpg.connect(dsn)
    try:
        async with conn.transaction(readonly=True):
            rows = await conn.fetch(sql)
        return [dict(row) for row in rows]
    finally:
        await conn.close()


@mcp.tool()
async def query(sql: str) -> dict[str, object]:
    """Run a read-only SQL query and return its rows."""
    rows = await run_readonly_query(os.environ["AR_SQL_DSN"], sql)
    return {"rows": rows}


def main() -> None:
    mcp.run()


if __name__ == "__main__":
    main()
