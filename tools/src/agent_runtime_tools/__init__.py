"""Reference MCP tool servers.

Each tool (web_fetch, code_exec, sql_query) runs as its own MCP-compliant
process; the runtime is the MCP client. No tool logic executes inside the agent
or worker process. Code execution goes through a sandboxed isolate.
"""

__version__ = "0.1.0"
