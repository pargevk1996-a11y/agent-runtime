"""Unit tests for the frozen event envelope."""

from __future__ import annotations

from datetime import UTC, datetime

import pytest
from pydantic import ValidationError

from agent_runtime.events.envelope import Envelope, EventPayload
from agent_runtime.ids import new_event_id, new_run_id, new_tenant_id


class _Sample(EventPayload):
    a: int
    b: str


def _make_envelope(payload: EventPayload) -> Envelope:
    now = datetime(2020, 1, 1, tzinfo=UTC)
    return Envelope(
        event_id=new_event_id(),
        tenant_id=new_tenant_id(),
        run_id=new_run_id(),
        seq=1,
        event_type="sample",
        payload_version=1,
        payload=payload,
        occurred_at=now,
        recorded_at=now,
    )


def test_envelope_is_frozen() -> None:
    env = _make_envelope(_Sample(a=1, b="x"))
    with pytest.raises(ValidationError):
        env.seq = 2


def test_payload_forbids_extra_fields() -> None:
    with pytest.raises(ValidationError):
        _Sample(a=1, b="x", c=99)  # type: ignore[call-arg]


def test_serialize_as_any_emits_concrete_payload_fields() -> None:
    env = _make_envelope(_Sample(a=7, b="hello"))
    dumped = env.model_dump()
    assert dumped["payload"] == {"a": 7, "b": "hello"}


def test_optional_causation_and_correlation_default_none() -> None:
    env = _make_envelope(_Sample(a=1, b="x"))
    assert env.causation_id is None
    assert env.correlation_id is None
