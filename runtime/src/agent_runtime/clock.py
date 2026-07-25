"""Injectable time source.

Time is a dependency, not a global. Deterministic replay and property-based
tests both require the ability to control "now", so all runtime code reads time
through a :class:`Clock` rather than calling :func:`datetime.now` directly.

Invariant across every implementation: :meth:`Clock.now` returns a timezone-aware
UTC ``datetime`` — never naive, never a local timezone — and :meth:`Clock.now_ms`
is derived from the same instant so the two can never disagree.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Protocol, runtime_checkable


@runtime_checkable
class Clock(Protocol):
    """A source of the current time."""

    def now(self) -> datetime:
        """Current instant as a timezone-aware UTC ``datetime``."""
        ...

    def now_ms(self) -> int:
        """Current instant as unix epoch milliseconds."""
        ...


def _to_ms(instant: datetime) -> int:
    return int(instant.timestamp() * 1000)


class SystemClock:
    """Real wall-clock time in UTC. The default in production code paths."""

    def now(self) -> datetime:
        return datetime.now(UTC)

    def now_ms(self) -> int:
        return _to_ms(self.now())


class ManualClock:
    """A clock whose time only moves when explicitly told to.

    Used by deterministic replay and by tests. Constructed at a fixed instant;
    :meth:`advance` and :meth:`set` move it forward. Naive datetimes passed in
    are rejected to preserve the UTC-aware invariant.
    """

    _EPOCH = datetime(2020, 1, 1, tzinfo=UTC)

    def __init__(self, start: datetime | None = None) -> None:
        self._now = self._require_aware(start) if start is not None else self._EPOCH

    @staticmethod
    def _require_aware(instant: datetime) -> datetime:
        if instant.tzinfo is None:
            raise ValueError("ManualClock requires a timezone-aware datetime")
        return instant.astimezone(UTC)

    def now(self) -> datetime:
        return self._now

    def now_ms(self) -> int:
        return _to_ms(self._now)

    def advance(self, delta: timedelta) -> None:
        """Move the clock forward by ``delta``."""
        self._now = self._now + delta

    def set(self, instant: datetime) -> None:
        """Set the clock to a specific (timezone-aware) instant."""
        self._now = self._require_aware(instant)
