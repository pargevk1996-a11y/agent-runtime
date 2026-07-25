"""Run state machine and the pure projection of an event log into state.

:func:`fold` replays a run's events into a :class:`RunState`. It is pure and
deterministic — the same events always yield the same state — which is what lets
snapshots be a mere cache: folding from zero and folding a snapshot's tail must
agree. :func:`apply` is the single-event transition and the sole place transition
legality is enforced.
"""

from __future__ import annotations

from collections.abc import Iterable
from datetime import datetime
from enum import StrEnum

from pydantic import BaseModel, ConfigDict

from agent_runtime.events.envelope import Envelope
from agent_runtime.runs.errors import InvalidTransitionError
from agent_runtime.runs.events import (
    RunCancelled,
    RunCreated,
    RunFailed,
    RunStarted,
    RunSucceeded,
)


class RunStatus(StrEnum):
    """Lifecycle status of a run. Terminal: SUCCEEDED, FAILED, CANCELLED."""

    PENDING = "pending"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    CANCELLED = "cancelled"


class RunError(BaseModel):
    """The error a run failed with."""

    model_config = ConfigDict(frozen=True)

    error_class: str
    message: str


class RunState(BaseModel):
    """The folded state of a run at a particular sequence number.

    Invariant: ``last_seq`` is the sequence of the most recent event folded in,
    so a snapshot at ``last_seq = S`` plus events with ``seq > S`` reconstructs
    the same state as folding from the beginning.
    """

    model_config = ConfigDict(frozen=True)

    status: RunStatus
    last_seq: int
    input: dict[str, object] | None = None
    created_at: datetime | None = None
    started_at: datetime | None = None
    finished_at: datetime | None = None
    result: dict[str, object] | None = None
    error: RunError | None = None
    cancel_reason: str | None = None
    worker: str | None = None

    @property
    def is_terminal(self) -> bool:
        return self.status in (RunStatus.SUCCEEDED, RunStatus.FAILED, RunStatus.CANCELLED)


def apply(state: RunState | None, event: Envelope) -> RunState:
    """Apply one event, returning the next state.

    :raises InvalidTransitionError: if the event is not a legal transition from
        the current state, arrives before ``RunCreated``, or is out of sequence.
    """
    payload = event.payload
    seq = event.seq

    if state is None:
        if isinstance(payload, RunCreated):
            return RunState(
                status=RunStatus.PENDING,
                last_seq=seq,
                input=payload.input,
                created_at=event.occurred_at,
            )
        raise InvalidTransitionError(
            "first event must be RunCreated", context={"event_type": event.event_type}
        )

    if seq <= state.last_seq:
        raise InvalidTransitionError(
            "events applied out of order",
            context={"last_seq": state.last_seq, "event_seq": seq},
        )

    status = state.status
    if status is RunStatus.PENDING and isinstance(payload, RunStarted):
        return state.model_copy(
            update={
                "status": RunStatus.RUNNING,
                "started_at": event.occurred_at,
                "worker": payload.worker,
                "last_seq": seq,
            }
        )
    if status is RunStatus.RUNNING and isinstance(payload, RunSucceeded):
        return state.model_copy(
            update={
                "status": RunStatus.SUCCEEDED,
                "finished_at": event.occurred_at,
                "result": payload.result,
                "last_seq": seq,
            }
        )
    if status is RunStatus.RUNNING and isinstance(payload, RunFailed):
        return state.model_copy(
            update={
                "status": RunStatus.FAILED,
                "finished_at": event.occurred_at,
                "error": RunError(error_class=payload.error_class, message=payload.message),
                "last_seq": seq,
            }
        )
    if status in (RunStatus.PENDING, RunStatus.RUNNING) and isinstance(payload, RunCancelled):
        return state.model_copy(
            update={
                "status": RunStatus.CANCELLED,
                "finished_at": event.occurred_at,
                "cancel_reason": payload.reason,
                "last_seq": seq,
            }
        )
    raise InvalidTransitionError(
        "illegal transition",
        context={"status": status.value, "event_type": event.event_type},
    )


def fold(events: Iterable[Envelope], *, initial: RunState | None = None) -> RunState:
    """Fold events into a :class:`RunState`, optionally starting from ``initial``.

    Passing ``initial`` (a snapshot) and only the events after it must produce the
    same result as folding the whole log from the start.

    :raises ValueError: if there are no events and no ``initial`` state.
    """
    state = initial
    for event in events:
        state = apply(state, event)
    if state is None:
        raise ValueError("cannot fold an empty event stream without an initial state")
    return state
