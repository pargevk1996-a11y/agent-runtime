"""Reference MCP tool server: sandboxed code execution.

Exposes a ``run_code`` tool that runs Python in the light isolate under a timeout
and memory cap, with networking blocked. Runs as its own process:

    python -m agent_runtime_tools.code_exec_server
"""

from __future__ import annotations

from mcp.server.fastmcp import FastMCP

from agent_runtime_tools.isolate import ExecLimits, SubprocessIsolate

mcp = FastMCP("code_exec")
_isolate = SubprocessIsolate()


@mcp.tool()
async def run_code(
    code: str, timeout_seconds: float = 5.0, memory_mb: int = 256
) -> dict[str, object]:
    """Run Python ``code`` in the sandbox and return its captured output."""
    result = await _isolate.run(
        code, ExecLimits(timeout_seconds=timeout_seconds, memory_mb=memory_mb)
    )
    return {
        "stdout": result.stdout,
        "stderr": result.stderr,
        "exit_code": result.exit_code,
        "timed_out": result.timed_out,
    }


def main() -> None:
    mcp.run()


if __name__ == "__main__":
    main()
