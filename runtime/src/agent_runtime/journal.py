"""The run journal abstraction.

A narrow, lease-bound view of a single run's event log: append and read. The
scheduler provides an implementation to node executors so executor-level
facilities (tool dispatch) can record durable events without depending on the
run store or holding the lease themselves.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Protocol

from agent_runtime.events.envelope import Envelope, EventPayload


class RunJournal(Protocol):
    """Append to and read from one run's event log, under the caller's lease."""

    async def append(self, payloads: Sequence[EventPayload]) -> list[Envelope]:
        """Append events to the run, returning the stored envelopes."""
        ...

    async def read(self) -> list[Envelope]:
        """Read the run's full event log."""
        ...
