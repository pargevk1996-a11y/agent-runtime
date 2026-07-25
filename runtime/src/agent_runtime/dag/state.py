"""The task-DAG projection and its pure fold from the event log.

:func:`fold_dag` replays a run's events into a :class:`DagState`, ignoring
run-lifecycle events (both projections share one event stream). Dependency cycles
are impossible by construction — a node's dependencies must already exist when it
is added — and dynamically added DEPENDENCY edges are cycle-checked; REFLECTION
and REJECT edges may form cycles.
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass, replace
from enum import StrEnum

from agent_runtime.dag.errors import CycleError, DuplicateNodeError, UnknownNodeError
from agent_runtime.dag.events import (
    EdgeAdded,
    NodeAdded,
    NodeFailed,
    NodeSkipped,
    NodeStarted,
    NodeSucceeded,
)
from agent_runtime.dag.model import (
    Edge,
    EdgeType,
    NodeBudget,
    NodeRole,
    NodeStatus,
    RetryPolicy,
)
from agent_runtime.events.envelope import Envelope
from agent_runtime.ids import NodeId


class DagOutcome(StrEnum):
    """Whole-graph outcome derived from node statuses."""

    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"


@dataclass(frozen=True)
class Node:
    """A node's static definition plus its runtime status."""

    node_id: NodeId
    role: NodeRole
    dependencies: tuple[NodeId, ...]
    retry_policy: RetryPolicy
    budget: NodeBudget
    reflection_depth: int
    status: NodeStatus
    attempt: int
    output: dict[str, object] | None = None
    error_class: str | None = None
    error_message: str | None = None


@dataclass(frozen=True)
class DagState:
    """Immutable snapshot of the graph at a point in the log."""

    nodes: dict[NodeId, Node]
    edges: tuple[Edge, ...]

    def _dependencies(self) -> dict[NodeId, list[NodeId]]:
        deps: dict[NodeId, list[NodeId]] = {}
        for edge in self.edges:
            if edge.edge_type is EdgeType.DEPENDENCY:
                deps.setdefault(edge.to_node, []).append(edge.from_node)
        return deps

    def ready_set(self) -> list[Node]:
        """Nodes eligible to start: PENDING with every dependency SUCCEEDED.

        Returned sorted by node id so scheduling is deterministic.
        """
        deps = self._dependencies()

        def _ready(node: Node) -> bool:
            if node.status is not NodeStatus.PENDING:
                return False
            return all(
                self.nodes[d].status is NodeStatus.SUCCEEDED for d in deps.get(node.node_id, [])
            )

        ready = [node for node in self.nodes.values() if _ready(node)]
        return sorted(ready, key=lambda n: n.node_id)

    def outcome(self) -> DagOutcome:
        """FAILED if any node failed (fail-fast); SUCCEEDED if all terminal-ok."""
        if not self.nodes:
            return DagOutcome.RUNNING
        statuses = [node.status for node in self.nodes.values()]
        if any(status is NodeStatus.FAILED for status in statuses):
            return DagOutcome.FAILED
        if all(status in (NodeStatus.SUCCEEDED, NodeStatus.SKIPPED) for status in statuses):
            return DagOutcome.SUCCEEDED
        return DagOutcome.RUNNING


def _require(nodes: dict[NodeId, Node], node_id: NodeId) -> Node:
    node = nodes.get(node_id)
    if node is None:
        raise UnknownNodeError("unknown node", context={"node_id": str(node_id)})
    return node


def _reaches(dep_edges: list[Edge], src: NodeId, dst: NodeId) -> bool:
    """Whether ``dst`` is reachable from ``src`` along DEPENDENCY edges."""
    adjacency: dict[NodeId, list[NodeId]] = {}
    for edge in dep_edges:
        adjacency.setdefault(edge.from_node, []).append(edge.to_node)
    stack = [src]
    seen: set[NodeId] = set()
    while stack:
        current = stack.pop()
        if current == dst:
            return True
        if current in seen:
            continue
        seen.add(current)
        stack.extend(adjacency.get(current, []))
    return False


def fold_dag(events: Iterable[Envelope]) -> DagState:
    """Project a run's event log into a :class:`DagState`.

    Run-lifecycle events are ignored. Raises :class:`DuplicateNodeError`,
    :class:`UnknownNodeError`, or :class:`CycleError` on an inconsistent log.
    """
    nodes: dict[NodeId, Node] = {}
    edges: list[Edge] = []
    dep_edges: list[Edge] = []

    for event in events:
        payload = event.payload
        if isinstance(payload, NodeAdded):
            if payload.node_id in nodes:
                raise DuplicateNodeError(
                    "node already added", context={"node_id": str(payload.node_id)}
                )
            for dep in payload.dependencies:
                _require(nodes, dep)
            nodes[payload.node_id] = Node(
                node_id=payload.node_id,
                role=payload.role,
                dependencies=payload.dependencies,
                retry_policy=payload.retry_policy,
                budget=payload.budget,
                reflection_depth=payload.reflection_depth,
                status=NodeStatus.PENDING,
                attempt=0,
            )
            for dep in payload.dependencies:
                edge = Edge(dep, payload.node_id, EdgeType.DEPENDENCY)
                edges.append(edge)
                dep_edges.append(edge)
        elif isinstance(payload, EdgeAdded):
            _require(nodes, payload.from_node)
            _require(nodes, payload.to_node)
            if payload.edge_type is EdgeType.DEPENDENCY and _reaches(
                dep_edges, payload.to_node, payload.from_node
            ):
                raise CycleError(
                    "dependency edge would create a cycle",
                    context={"from": str(payload.from_node), "to": str(payload.to_node)},
                )
            edge = Edge(payload.from_node, payload.to_node, payload.edge_type)
            edges.append(edge)
            if payload.edge_type is EdgeType.DEPENDENCY:
                dep_edges.append(edge)
        elif isinstance(payload, NodeStarted):
            node = _require(nodes, payload.node_id)
            nodes[payload.node_id] = replace(
                node, status=NodeStatus.RUNNING, attempt=payload.attempt
            )
        elif isinstance(payload, NodeSucceeded):
            node = _require(nodes, payload.node_id)
            nodes[payload.node_id] = replace(
                node, status=NodeStatus.SUCCEEDED, output=payload.output
            )
        elif isinstance(payload, NodeFailed):
            node = _require(nodes, payload.node_id)
            nodes[payload.node_id] = replace(
                node,
                status=NodeStatus.FAILED,
                attempt=payload.attempt,
                error_class=payload.error_class,
                error_message=payload.message,
            )
        elif isinstance(payload, NodeSkipped):
            node = _require(nodes, payload.node_id)
            nodes[payload.node_id] = replace(node, status=NodeStatus.SKIPPED)
        # Any other payload (run lifecycle) is not this projection's concern.

    return DagState(nodes=nodes, edges=tuple(edges))
