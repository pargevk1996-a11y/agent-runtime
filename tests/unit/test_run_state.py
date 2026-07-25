"""Unit and property tests for the run state machine and fold."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest
from hypothesis import given
from hypothesis import strategies as st

from agent_runtime.events.envelope import Envelope, EventPayload
from agent_runtime.ids import new_event_id, new_run_id, new_tenant_id
from agent_runtime.runs.errors import InvalidTransitionError
from agent_runtime.runs.events import (
    RUN_CANCELLED,
    RUN_CREATED,
    RUN_FAILED,
    RUN_STARTED,
    RUN_SUCCEEDED,
    RunCancelled,
    RunCreated,
    RunFailed,
    RunStarted,
    RunSucceeded,
)
from agent_runtime.runs.state import RunState, RunStatus, apply, fold

_TENANT = new_tenant_id()
_RUN = new_run_id()
_T0 = datetime(2020, 1, 1, tzinfo=UTC)


def _env(seq: int, payload: EventPayload, event_type: str) -> Envelope:
    when = _T0 + timedelta(seconds=seq)
    return Envelope(
        event_id=new_event_id(),
        tenant_id=_TENANT,
        run_id=_RUN,
        seq=seq,
        event_type=event_type,
        payload_version=1,
        payload=payload,
        occurred_at=when,
        recorded_at=when,
    )


def _created(seq: int = 1) -> Envelope:
    return _env(seq, RunCreated(input={"x": 1}), RUN_CREATED)


def _started(seq: int = 2) -> Envelope:
    return _env(seq, RunStarted(worker="w1"), RUN_STARTED)


def _succeeded(seq: int = 3) -> Envelope:
    return _env(seq, RunSucceeded(result={"ok": True}), RUN_SUCCEEDED)


def _failed(seq: int = 3) -> Envelope:
    return _env(seq, RunFailed(error_class="X", message="boom"), RUN_FAILED)


def _cancelled(seq: int) -> Envelope:
    return _env(seq, RunCancelled(reason="stop"), RUN_CANCELLED)


def test_success_lifecycle() -> None:
    state = fold([_created(), _started(), _succeeded()])
    assert state.status is RunStatus.SUCCEEDED
    assert state.last_seq == 3
    assert state.input == {"x": 1}
    assert state.result == {"ok": True}
    assert state.created_at is not None
    assert state.started_at is not None
    assert state.finished_at is not None
    assert state.is_terminal


def test_failure_lifecycle() -> None:
    state = fold([_created(), _started(), _failed()])
    assert state.status is RunStatus.FAILED
    assert state.error is not None
    assert state.error.error_class == "X"


def test_cancel_from_running() -> None:
    state = fold([_created(), _started(), _cancelled(3)])
    assert state.status is RunStatus.CANCELLED
    assert state.cancel_reason == "stop"


def test_cancel_from_pending() -> None:
    state = fold([_created(), _cancelled(2)])
    assert state.status is RunStatus.CANCELLED


def test_first_event_must_be_created() -> None:
    with pytest.raises(InvalidTransitionError):
        fold([_started(seq=1)])


def test_illegal_transition_succeed_before_start() -> None:
    with pytest.raises(InvalidTransitionError):
        fold([_created(), _succeeded(seq=2)])


def test_out_of_order_sequence_rejected() -> None:
    with pytest.raises(InvalidTransitionError):
        fold([_created(seq=1), _started(seq=1)])


def test_terminal_state_rejects_further_events() -> None:
    with pytest.raises(InvalidTransitionError):
        fold([_created(), _started(), _succeeded(), _cancelled(4)])


def test_empty_without_initial_raises() -> None:
    with pytest.raises(ValueError):
        fold([])


def test_apply_can_continue_from_prior_state() -> None:
    pending = apply(None, _created())
    running = apply(pending, _started())
    assert running.status is RunStatus.RUNNING
    assert isinstance(running, RunState)


@given(split=st.integers(min_value=0, max_value=3))
def test_snapshot_tail_matches_full_fold(split: int) -> None:
    events = [_created(), _started(), _succeeded()]
    full = fold(events)
    if split == 0:
        assert fold(events) == full
    else:
        prefix = fold(events[:split])
        assert fold(events[split:], initial=prefix) == full
