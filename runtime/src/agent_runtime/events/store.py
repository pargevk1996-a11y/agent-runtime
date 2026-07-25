"""Append-only event storage over PostgreSQL.

The :class:`EventStore` is the write/read boundary for the event log. Appends use
optimistic concurrency: the caller states the sequence number it last saw, the
new event takes the next number, and the ``UNIQUE (run_id, seq)`` constraint
rejects a second writer racing for the same slot. That rejection surfaces as a
:class:`ConcurrencyError`, which is retryable — the caller re-reads and retries.
"""

from __future__ import annotations

from collections.abc import AsyncIterator, Sequence
from contextlib import asynccontextmanager
from datetime import date, datetime
from uuid import UUID

import asyncpg

from agent_runtime.clock import Clock, SystemClock
from agent_runtime.db.pool import tenant_connection
from agent_runtime.db.rows import record_to_envelope
from agent_runtime.events.envelope import Envelope, EventPayload
from agent_runtime.events.errors import ConcurrencyError
from agent_runtime.events.registry import EventRegistry, default_registry
from agent_runtime.ids import EventId, RunId, TenantId, new_event_id, partition_month

_INSERT = (
    "INSERT INTO events "
    "(partition_key, run_id, seq, event_id, tenant_id, event_type, "
    " payload_version, payload, occurred_at, causation_id, correlation_id) "
    "VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11) "
    "RETURNING recorded_at"
)

_SELECT = (
    "SELECT run_id, seq, event_id, tenant_id, event_type, payload_version, "
    "       payload, occurred_at, recorded_at, causation_id, correlation_id "
    "FROM events "
    "WHERE partition_key = $1 AND run_id = $2 AND seq > $3 "
    "  AND ($4::bigint IS NULL OR seq <= $4) "
    "ORDER BY seq"
)


def _partition_key(run_id: RunId) -> date:
    """The run's monthly partition key, derived from its UUIDv7 creation time."""
    return partition_month(run_id)


class EventStore:
    """Reads and appends events for a run, enforcing per-run ordering."""

    def __init__(
        self,
        pool: asyncpg.Pool[asyncpg.Record],
        registry: EventRegistry = default_registry,
        clock: Clock | None = None,
    ) -> None:
        self._pool = pool
        self._registry = registry
        self._clock: Clock = clock or SystemClock()

    @asynccontextmanager
    async def _acquire(
        self, tenant_id: TenantId, conn: asyncpg.Connection[asyncpg.Record] | None
    ) -> AsyncIterator[asyncpg.Connection[asyncpg.Record]]:
        """Yield the caller's connection, or open a fresh tenant-scoped one.

        When ``conn`` is passed the caller owns the transaction and must already
        have bound the tenant context; this store then composes into it.
        """
        if conn is not None:
            yield conn
        else:
            async with tenant_connection(self._pool, tenant_id) as owned:
                yield owned

    async def append(
        self,
        tenant_id: TenantId,
        run_id: RunId,
        *,
        after_seq: int,
        payload: EventPayload,
        occurred_at: datetime | None = None,
        causation_id: EventId | None = None,
        correlation_id: UUID | None = None,
        conn: asyncpg.Connection[asyncpg.Record] | None = None,
    ) -> Envelope:
        """Append a single event; see :meth:`append_batch` for semantics."""
        events = await self.append_batch(
            tenant_id,
            run_id,
            after_seq=after_seq,
            payloads=[payload],
            occurred_at=occurred_at,
            causation_id=causation_id,
            correlation_id=correlation_id,
            conn=conn,
        )
        return events[0]

    async def append_batch(
        self,
        tenant_id: TenantId,
        run_id: RunId,
        *,
        after_seq: int,
        payloads: Sequence[EventPayload],
        occurred_at: datetime | None = None,
        causation_id: EventId | None = None,
        correlation_id: UUID | None = None,
        conn: asyncpg.Connection[asyncpg.Record] | None = None,
    ) -> list[Envelope]:
        """Atomically append events with sequence numbers ``after_seq+1 ...``.

        All events land in one transaction, so the batch is all-or-nothing. A
        conflict on any sequence number rolls the whole batch back and raises
        :class:`ConcurrencyError`. Pass ``conn`` to compose into an outer
        transaction (see :meth:`_acquire`).

        :raises ValueError: if ``after_seq`` is negative or ``payloads`` is empty.
        """
        if after_seq < 0:
            raise ValueError("after_seq must be >= 0")
        if not payloads:
            raise ValueError("payloads must be non-empty")

        partition_key = _partition_key(run_id)
        when = occurred_at or self._clock.now()
        envelopes: list[Envelope] = []
        try:
            async with self._acquire(tenant_id, conn) as active:
                for offset, payload in enumerate(payloads):
                    seq = after_seq + 1 + offset
                    event_type = self._registry.event_type_for(payload)
                    version = self._registry.current_version(event_type)
                    event_id = new_event_id()
                    row = await active.fetchrow(
                        _INSERT,
                        partition_key,
                        run_id,
                        seq,
                        event_id,
                        tenant_id,
                        event_type,
                        version,
                        payload.model_dump(mode="json"),
                        when,
                        causation_id,
                        correlation_id,
                    )
                    # INSERT ... RETURNING always yields exactly one row.
                    envelopes.append(
                        Envelope(
                            event_id=event_id,
                            tenant_id=tenant_id,
                            run_id=run_id,
                            seq=seq,
                            event_type=event_type,
                            payload_version=version,
                            payload=payload,
                            occurred_at=when,
                            recorded_at=row["recorded_at"],
                            causation_id=causation_id,
                            correlation_id=correlation_id,
                        )
                    )
        except asyncpg.UniqueViolationError as exc:
            raise ConcurrencyError(
                "a concurrent writer already appended at the expected sequence",
                context={"run_id": str(run_id), "after_seq": after_seq},
            ) from exc
        return envelopes

    async def read(
        self,
        tenant_id: TenantId,
        run_id: RunId,
        *,
        from_seq: int = 0,
        to_seq: int | None = None,
        conn: asyncpg.Connection[asyncpg.Record] | None = None,
    ) -> list[Envelope]:
        """Return a run's events in ``seq`` order.

        ``from_seq`` is exclusive (events strictly after it) and ``to_seq`` is
        inclusive; the defaults return the whole run. Payloads are upcast to the
        current schema version on the way out. Pass ``conn`` to read within an
        outer transaction.
        """
        partition_key = _partition_key(run_id)
        async with self._acquire(tenant_id, conn) as acquired:
            rows = await acquired.fetch(_SELECT, partition_key, run_id, from_seq, to_seq)
        return [record_to_envelope(row, self._registry) for row in rows]
