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
from agent_runtime.ids import NodeId, RunId, TenantId
from agent_runtime.journal import RunJournal


@dataclass(frozen=True)
class NodeContext:
    """Everything a node needs to run: its definition, its inputs, and identity.

    ``inputs`` maps each dependency's node id to that dependency's output.
    ``tenant_id``/``run_id`` attribute LLM and tool calls without the scheduler
    needing to know about them. ``journal`` is the lease-bound log for durable
    tool dispatch, and ``attempt`` is the current attempt number (so tool
    idempotency keys are stable across crash recovery of the same attempt).
    """

    node: Node
    inputs: dict[NodeId, dict[str, object]]
    tenant_id: TenantId
    run_id: RunId
    journal: RunJournal
    attempt: int


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
