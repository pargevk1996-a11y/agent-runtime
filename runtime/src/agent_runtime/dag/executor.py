"""The node execution contract.

The scheduler owns *when* and *whether* a node runs; a :class:`NodeExecutor` owns
*how*. An executor receives a node and its dependency outputs, and either returns
an output mapping or raises a typed error (whose category drives retry). Real
executors — LLM calls, MCP tools — arrive in later phases; the scheduler depends
only on this protocol.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol

from agent_runtime.dag.state import Node
from agent_runtime.ids import NodeId


@dataclass(frozen=True)
class NodeContext:
    """Everything a node needs to run: its definition and its inputs.

    ``inputs`` maps each dependency's node id to that dependency's output.
    """

    node: Node
    inputs: dict[NodeId, dict[str, object]]


class NodeExecutor(Protocol):
    """Executes a single node's work."""

    async def execute(self, ctx: NodeContext) -> dict[str, object]:
        """Run the node, returning its output, or raise a typed error on failure."""
        ...
