"""The node execution contract.

The scheduler owns *when* and *whether* a node runs; a :class:`NodeExecutor` owns
*how*. An executor receives a node and its dependency outputs, and either returns
an output mapping or raises a typed error (whose category drives retry). Real
executors — LLM calls, MCP tools — arrive in later phases; the scheduler depends
only on this protocol.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Protocol

from agent_runtime.dag.model import EdgeType, NodeBudget, NodeRole, RetryPolicy
from agent_runtime.dag.state import Node
from agent_runtime.ids import NodeId


@dataclass(frozen=True)
class NodeContext:
    """Everything a node needs to run: its definition and its inputs.

    ``inputs`` maps each dependency's node id to that dependency's output.
    """

    node: Node
    inputs: dict[NodeId, dict[str, object]]


@dataclass(frozen=True)
class SpawnNode:
    """A new node a running node asks the scheduler to add to the graph.

    A reflection is a spawn whose ``reflection_depth`` is one greater than its
    origin's, paired with a REFLECTION edge back to that origin.
    """

    node_id: NodeId
    role: NodeRole
    dependencies: tuple[NodeId, ...] = ()
    retry_policy: RetryPolicy = field(default_factory=RetryPolicy)
    budget: NodeBudget = field(default_factory=NodeBudget)
    reflection_depth: int = 0


@dataclass(frozen=True)
class SpawnEdge:
    """A new edge a running node asks the scheduler to add."""

    from_node: NodeId
    to_node: NodeId
    edge_type: EdgeType


@dataclass(frozen=True)
class NodeResult:
    """What a node produced: its output plus any graph expansion it requests."""

    output: dict[str, object]
    spawn: tuple[SpawnNode, ...] = ()
    edges: tuple[SpawnEdge, ...] = ()


class NodeExecutor(Protocol):
    """Executes a single node's work."""

    async def execute(self, ctx: NodeContext) -> NodeResult:
        """Run the node, returning its result, or raise a typed error on failure."""
        ...
