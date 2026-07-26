"""Tool-call domain events, linked by ``idempotency_key``.

Intent-first: ``ToolCallRequested`` is committed before dispatch, so recovery can
tell a call was in flight. Exactly one of Completed/Failed/Indeterminate resolves
each request. These live in the same run stream as everything else.
"""

from __future__ import annotations

from agent_runtime.events.envelope import EventPayload
from agent_runtime.events.registry import EventRegistry
from agent_runtime.ids import NodeId

TOOL_CALL_REQUESTED = "tool.requested"
TOOL_CALL_COMPLETED = "tool.completed"
TOOL_CALL_FAILED = "tool.failed"
TOOL_CALL_INDETERMINATE = "tool.indeterminate"


class ToolCallRequested(EventPayload):
    """A tool call is about to be dispatched."""

    node_id: NodeId
    tool: str
    args: dict[str, object]
    idempotency_key: str


class ToolCallCompleted(EventPayload):
    """A tool call returned successfully."""

    idempotency_key: str
    result: dict[str, object]


class ToolCallFailed(EventPayload):
    """A tool call failed with a typed error."""

    idempotency_key: str
    error_class: str
    message: str


class ToolCallIndeterminate(EventPayload):
    """A tool call's outcome could not be determined after a crash."""

    idempotency_key: str
    reason: str


def register_tool_events(registry: EventRegistry) -> None:
    """Register all tool-call payloads with ``registry`` at version 1."""
    registry.register(TOOL_CALL_REQUESTED, ToolCallRequested)
    registry.register(TOOL_CALL_COMPLETED, ToolCallCompleted)
    registry.register(TOOL_CALL_FAILED, ToolCallFailed)
    registry.register(TOOL_CALL_INDETERMINATE, ToolCallIndeterminate)
