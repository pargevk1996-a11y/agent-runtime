"""Unit tests for the injectable clock."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from agent_runtime.clock import ManualClock, SystemClock


def test_system_clock_is_utc_aware() -> None:
    now = SystemClock().now()
    assert now.tzinfo is not None
    assert now.utcoffset() == timedelta(0)


def test_system_clock_now_ms_tracks_now() -> None:
    clock = SystemClock()
    assert abs(clock.now_ms() - int(clock.now().timestamp() * 1000)) < 1000


def test_manual_clock_defaults_to_fixed_instant() -> None:
    assert ManualClock().now() == datetime(2020, 1, 1, tzinfo=UTC)


def test_manual_clock_advance() -> None:
    clock = ManualClock(datetime(2020, 1, 1, tzinfo=UTC))
    clock.advance(timedelta(seconds=5))
    expected = datetime(2020, 1, 1, 0, 0, 5, tzinfo=UTC)
    assert clock.now() == expected
    assert clock.now_ms() == int(expected.timestamp() * 1000)


def test_manual_clock_set() -> None:
    clock = ManualClock()
    target = datetime(2021, 6, 1, 12, tzinfo=UTC)
    clock.set(target)
    assert clock.now() == target


def test_manual_clock_rejects_naive_start() -> None:
    with pytest.raises(ValueError):
        ManualClock(datetime(2020, 1, 1))


def test_manual_clock_rejects_naive_set() -> None:
    clock = ManualClock()
    with pytest.raises(ValueError):
        clock.set(datetime(2021, 1, 1))
