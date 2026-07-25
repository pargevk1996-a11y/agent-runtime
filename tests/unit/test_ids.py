"""Unit and property-based tests for UUIDv7 generation and typed IDs."""

from __future__ import annotations

from uuid import RFC_4122

from hypothesis import given
from hypothesis import strategies as st

from agent_runtime.ids import (
    new_event_id,
    new_node_id,
    new_run_id,
    new_tenant_id,
    uuid7,
)

MAX_48 = (1 << 48) - 1


@given(ms=st.integers(min_value=0, max_value=MAX_48))
def test_uuid7_has_version_and_variant(ms: int) -> None:
    u = uuid7(ms)
    assert u.version == 7
    assert u.variant == RFC_4122


@given(
    a=st.integers(min_value=0, max_value=MAX_48),
    b=st.integers(min_value=0, max_value=MAX_48),
)
def test_uuid7_is_time_sortable(a: int, b: int) -> None:
    # A smaller millisecond timestamp must yield a smaller UUID integer, because
    # the timestamp occupies the most-significant 48 bits.
    if a < b:
        assert uuid7(a).int < uuid7(b).int


def test_typed_constructors_yield_distinct_ids() -> None:
    ids = [new_run_id(), new_node_id(), new_event_id(), new_tenant_id()]
    assert len(set(ids)) == 4
