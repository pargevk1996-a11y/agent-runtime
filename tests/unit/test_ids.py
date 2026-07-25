"""Unit and property-based tests for UUIDv7 generation and typed IDs."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from uuid import RFC_4122, uuid4

import pytest
from hypothesis import given
from hypothesis import strategies as st

from agent_runtime.ids import (
    new_event_id,
    new_node_id,
    new_run_id,
    new_tenant_id,
    uuid7,
    uuid7_timestamp,
)

MAX_48 = (1 << 48) - 1
_UNIX_EPOCH = datetime(1970, 1, 1, tzinfo=UTC)
# Largest millisecond timestamp a `datetime` can represent (year 9999); the full
# 48-bit field reaches far beyond this.
_MAX_REPRESENTABLE_MS = int(
    (datetime(9999, 12, 31, 23, 59, 59, tzinfo=UTC) - _UNIX_EPOCH).total_seconds() * 1000
)


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


@given(ms=st.integers(min_value=0, max_value=_MAX_REPRESENTABLE_MS))
def test_uuid7_timestamp_round_trips(ms: int) -> None:
    assert uuid7_timestamp(uuid7(ms)) == _UNIX_EPOCH + timedelta(milliseconds=ms)


def test_uuid7_timestamp_rejects_non_v7() -> None:
    with pytest.raises(ValueError):
        uuid7_timestamp(uuid4())


def test_uuid7_timestamp_rejects_out_of_range() -> None:
    # A 48-bit timestamp at its maximum lands past year 9999, which datetime
    # cannot represent; the function must raise ValueError, not OverflowError.
    with pytest.raises(ValueError, match="out of representable range"):
        uuid7_timestamp(uuid7(MAX_48))
