"""Unit tests for the snapshot cadence policy."""

from __future__ import annotations

import pytest

from agent_runtime.runs.snapshots import SnapshotPolicy


def test_due_when_interval_reached_from_scratch() -> None:
    policy = SnapshotPolicy(every=100)
    assert policy.due(None, 99) is False
    assert policy.due(None, 100) is True


def test_due_relative_to_last_snapshot() -> None:
    policy = SnapshotPolicy(every=100)
    assert policy.due(50, 149) is False
    assert policy.due(50, 150) is True


def test_rejects_non_positive_interval() -> None:
    with pytest.raises(ValueError):
        SnapshotPolicy(every=0)
