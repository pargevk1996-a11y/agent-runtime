"""Reference MCP tool server: HTTP GET with an egress allowlist.

The allowlist comes from ``AR_WEB_FETCH_ALLOWLIST`` (comma-separated hosts). A URL
is checked against the policy before any network access, so a disallowed host
never leaves the process. Runs as its own process:

    python -m agent_runtime_tools.web_fetch_server
"""

from __future__ import annotations

import asyncio
import urllib.request

from mcp.server.fastmcp import FastMCP

from agent_runtime_tools.egress import EgressPolicy

_MAX_BODY = 100_000

mcp = FastMCP("web_fetch")
_policy = EgressPolicy.from_env()


@mcp.tool()
async def fetch(url: str, timeout_seconds: float = 10.0) -> dict[str, object]:
    """Fetch ``url`` (http/https, allowlisted host only) and return status + body."""
    _policy.check(url)  # raises EgressDeniedError before any network access

    def _get() -> dict[str, object]:
        # Scheme (http/https) and host are already enforced by _policy.check above.
        request = urllib.request.Request(url, headers={"User-Agent": "agent-runtime"})  # noqa: S310
        with urllib.request.urlopen(request, timeout=timeout_seconds) as response:  # noqa: S310
            body = response.read(_MAX_BODY).decode(errors="replace")
            body = response.read(_MAX_BODY).decode(errors="replace")
            return {"status": response.status, "body": body}

    return await asyncio.to_thread(_get)


def main() -> None:
    mcp.run()


if __name__ == "__main__":
    main()
