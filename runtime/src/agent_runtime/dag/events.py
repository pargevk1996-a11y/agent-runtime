"""Task DAG domain events.

Node and edge mutations are events in the same run stream as the run lifecycle,
so the graph is append-only and reconstructible from the log. Registered via
:func:`register_dag_events` alongside the run events.
"""

from __future__ import annotations

from agent_runtime.dag.model import EdgeType, NodeBudget, NodeRole, RetryPolicy
from agent_runtime.events.envelope import EventPayload
from agent_runtime.events.registry import EventRegistry
from agent_runtime.ids import NodeId

NODE_ADDED = "node.added"
EDGE_ADDED = "edge.added"
NODE_STARTED = "node.started"
NODE_SUCCEEDED = "node.succeeded"
NODE_FAILED = "node.failed"
NODE_SKIPPED = "node.skipped"


class NodeAdded(EventPayload):
    """A node was added to the graph. Dependencies must already exist."""

    node_id: NodeId
    role: NodeRole
    dependencies: tuple[NodeId, ...]
    retry_policy: RetryPolicy
    budget: NodeBudget
    reflection_depth: int = 0


class EdgeAdded(EventPayload):
    """An edge was added between two existing nodes (e.g. a reflection edge)."""

    from_node: NodeId
    to_node: NodeId
    edge_type: EdgeType


class NodeStarted(EventPayload):
    """A node began executing (attempt is 1-based)."""

    node_id: NodeId
    attempt: int


class NodeSucceeded(EventPayload):
    """A node completed successfully with an output."""

    node_id: NodeId
    output: dict[str, object]


class NodeFailed(EventPayload):
    """A node's attempt failed with a typed error."""

    node_id: NodeId
    attempt: int
    error_class: str
    message: str


class NodeSkipped(EventPayload):
    """A node was skipped (e.g. an upstream dependency failed)."""

    node_id: NodeId
    reason: str


def register_dag_events(registry: EventRegistry) -> None:
    """Register all DAG payloads with ``registry`` at version 1."""
    registry.register(NODE_ADDED, NodeAdded)
    registry.register(EDGE_ADDED, EdgeAdded)
    registry.register(NODE_STARTED, NodeStarted)
    registry.register(NODE_SUCCEEDED, NodeSucceeded)
    registry.register(NODE_FAILED, NodeFailed)
    registry.register(NODE_SKIPPED, NodeSkipped)
