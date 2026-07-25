"""Value types for the task DAG: statuses, roles, edges, retry, and budget.

These are the building blocks referenced by both the DAG events (what gets stored)
and the DAG projection (what the scheduler reads). Node I/O is an opaque ``dict``
at this layer; typed contracts are enforced by node roles/executors in later
phases.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum

from pydantic import BaseModel, ConfigDict

from agent_runtime.ids import NodeId


class NodeStatus(StrEnum):
    """Stored lifecycle of a node. READY is derived (see ``DagState.ready_set``)."""

    PENDING = "pending"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    SKIPPED = "skipped"


class NodeRole(StrEnum):
    """Scheduler-visible role metadata. Behaviour attaches in later phases."""

    TASK = "task"
    PLANNER = "planner"
    EXECUTOR = "executor"
    CRITIC = "critic"
    VERIFIER = "verifier"


class EdgeType(StrEnum):
    """Edge kinds. DEPENDENCY must stay acyclic; REFLECTION/REJECT may form cycles."""

    DEPENDENCY = "dependency"
    REFLECTION = "reflection"
    REJECT = "reject"


class RetryPolicy(BaseModel):
    """When to retry a failed node.

    ``retry_on`` holds error-category names (``"retryable"``, ``"indeterminate"``,
    ``"terminal"``) rather than exception types, so it serializes to JSON. A node
    is retried while its attempt count is below ``max_attempts`` and the failure's
    category is in ``retry_on``; ``TerminalError`` is never retried by default.
    """

    model_config = ConfigDict(frozen=True)

    max_attempts: int = 1
    retry_on: frozenset[str] = frozenset({"retryable"})


class NodeBudget(BaseModel):
    """Per-node cost ceiling. Fields exist now; enforcement lands in Phase 5."""

    model_config = ConfigDict(frozen=True)

    max_cost_usd: float | None = None
    max_tokens: int | None = None


@dataclass(frozen=True)
class Edge:
    """A directed edge between two nodes."""

    from_node: NodeId
    to_node: NodeId
    edge_type: EdgeType
