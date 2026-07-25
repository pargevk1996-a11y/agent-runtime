"""Run lifecycle domain events.

These are the first concrete payload types — the open payload union from Phase 2
exists precisely so these can be added now without touching the envelope. They
are registered explicitly via :func:`register_run_events` (no import-time side
effects), which application bootstrap calls once against the default registry.
"""

from __future__ import annotations

from agent_runtime.events.envelope import EventPayload
from agent_runtime.events.registry import EventRegistry

RUN_CREATED = "run.created"
RUN_STARTED = "run.started"
RUN_SUCCEEDED = "run.succeeded"
RUN_FAILED = "run.failed"
RUN_CANCELLED = "run.cancelled"


class RunCreated(EventPayload):
    """A run has been created (always the first event, seq 1)."""

    input: dict[str, object]


class RunStarted(EventPayload):
    """A worker acquired the lease and began executing the run."""

    worker: str


class RunSucceeded(EventPayload):
    """The run finished successfully."""

    result: dict[str, object]


class RunFailed(EventPayload):
    """The run terminated with an error."""

    error_class: str
    message: str


class RunCancelled(EventPayload):
    """The run was cancelled before reaching a natural terminal state."""

    reason: str


def register_run_events(registry: EventRegistry) -> None:
    """Register all run lifecycle payloads with ``registry`` at version 1."""
    registry.register(RUN_CREATED, RunCreated)
    registry.register(RUN_STARTED, RunStarted)
    registry.register(RUN_SUCCEEDED, RunSucceeded)
    registry.register(RUN_FAILED, RunFailed)
    registry.register(RUN_CANCELLED, RunCancelled)
