"""A minimal reference MCP tool server.

Exposes one idempotent ``echo`` tool. Runs as its own process over stdio:

    python -m agent_runtime_tools.echo_server

It exists to exercise the runtime's MCP transport end to end without any network
or side effects.
"""

from __future__ import annotations

from mcp.server.fastmcp import FastMCP

mcp = FastMCP("echo")


@mcp.tool()
def echo(message: str) -> dict[str, str]:
    """Return the message unchanged."""
    return {"echo": message}


def main() -> None:
    mcp.run()


if __name__ == "__main__":
    main()
