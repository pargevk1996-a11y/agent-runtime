"""Run projection, lease/fencing, and coordinated appends.

``RunStore`` owns the ``runs`` projection and is the single writer's gate: a
worker acquires a lease (bumping a fencing token), and every append verifies its
token against the row under a ``FOR UPDATE`` lock before writing. A worker that
lost its lease (its token is now behind) is rejected with :class:`StaleLeaseError`.

Lease timing uses the database clock (``now()``), not the injectable ``Clock``:
all workers must compare expiry against one clock to be safe under skew. The
injectable clock governs domain event timestamps, not lease timing.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta

import asyncpg

from agent_runtime.db.pool import tenant_connection
from agent_runtime.events.envelope import Envelope, EventPayload
from agent_runtime.events.store import EventStore
from agent_runtime.ids import RunId, TenantId, partition_month
from agent_runtime.logging import get_logger
from agent_runtime.runs.errors import (
    LeaseHeldError,
    RunAlreadyExistsError,
    RunNotFoundError,
    StaleLeaseError,
)
from agent_runtime.runs.events import RunCreated
from agent_runtime.runs.snapshots import CheckpointManager
from agent_runtime.runs.state import RunState, RunStatus, apply, fold
from agent_runtime.stream.bus import StreamPublisher

_log = get_logger(__name__)


@dataclass(frozen=True)
class Lease:
    """Proof of run ownership: a fencing token that gates appends."""

    run_id: RunId
    worker: str
    fencing_token: int
    expires_at: datetime


_INSERT_RUN = (
    "INSERT INTO runs (partition_key, run_id, tenant_id, status, last_seq) "
    "VALUES ($1, $2, $3, $4, $5)"
)
_EXISTS_RUN = "SELECT 1 FROM runs WHERE partition_key = $1 AND run_id = $2"
_ACQUIRE = (
    "UPDATE runs SET lease_owner = $3, lease_expires_at = now() + $4, "
    "  fencing_token = fencing_token + 1, updated_at = now() "
    "WHERE partition_key = $1 AND run_id = $2 "
    "  AND (lease_owner IS NULL OR lease_expires_at < now()) "
    "RETURNING fencing_token, lease_expires_at"
)
_LOCK_RUN = (
    "SELECT status, last_seq, fencing_token FROM runs "
    "WHERE partition_key = $1 AND run_id = $2 FOR UPDATE"
)
_UPDATE_PROJECTION = (
    "UPDATE runs SET status = $3, last_seq = $4, updated_at = now() "
    "WHERE partition_key = $1 AND run_id = $2"
)


class RunStore:
    """Coordinates run creation, leasing, and lease-gated event appends."""

    def __init__(
        self,
        pool: asyncpg.Pool[asyncpg.Record],
        event_store: EventStore,
        checkpoints: CheckpointManager | None = None,
        publisher: StreamPublisher | None = None,
    ) -> None:
        self._pool = pool
        self._events = event_store
        self._checkpoints = checkpoints
        self._publisher = publisher

    async def _publish(self, run_id: RunId, envelopes: list[Envelope]) -> None:
        # Best-effort live fan-out: the log is the source of truth, so a publish
        # failure must not fail the append that already committed.
        if self._publisher is None:
            return
        try:
            await self._publisher.publish(run_id, envelopes)
        except Exception:
            _log.warning("stream_publish_failed", run_id=str(run_id), exc_info=True)

    async def create_run(
        self, tenant_id: TenantId, run_id: RunId, *, input: dict[str, object]
    ) -> Envelope:
        """Create a run: append ``RunCreated`` (seq 1) and its projection row.

        :raises RunAlreadyExistsError: if a run with this id already exists.
        """
        partition_key = partition_month(run_id)
        async with tenant_connection(self._pool, tenant_id) as conn:
            try:
                await conn.execute(
                    _INSERT_RUN, partition_key, run_id, tenant_id, RunStatus.PENDING.value, 1
                )
            except asyncpg.UniqueViolationError as exc:
                raise RunAlreadyExistsError(
                    "run already exists", context={"run_id": str(run_id)}
                ) from exc
            created = await self._events.append(
                tenant_id, run_id, after_seq=0, payload=RunCreated(input=input), conn=conn
            )
        await self._publish(run_id, [created])
        return created

    async def acquire_lease(
        self, tenant_id: TenantId, run_id: RunId, *, worker: str, ttl: timedelta
    ) -> Lease:
        """Acquire (or steal an expired) lease, bumping the fencing token.

        :raises RunNotFoundError: if the run does not exist.
        :raises LeaseHeldError: if a live worker currently holds the lease.
        """
        partition_key = partition_month(run_id)
        async with tenant_connection(self._pool, tenant_id) as conn:
            row = await conn.fetchrow(_ACQUIRE, partition_key, run_id, worker, ttl)
            if row is None:
                exists = await conn.fetchrow(_EXISTS_RUN, partition_key, run_id)
                if exists is None:
                    raise RunNotFoundError("no such run", context={"run_id": str(run_id)})
                raise LeaseHeldError(
                    "lease held by another worker", context={"run_id": str(run_id)}
                )
        return Lease(
            run_id=run_id,
            worker=worker,
            fencing_token=row["fencing_token"],
            expires_at=row["lease_expires_at"],
        )

    async def append_events(
        self,
        tenant_id: TenantId,
        run_id: RunId,
        *,
        lease: Lease,
        payloads: Sequence[EventPayload],
    ) -> list[Envelope]:
        """Append events as the lease holder, updating the projection atomically.

        The next sequence number is read from the projection under a row lock, so
        the lease holder is the sole writer. Payloads must form legal state
        transitions.

        :raises RunNotFoundError: if the run does not exist.
        :raises StaleLeaseError: if the lease's fencing token is behind the run's.
        """
        partition_key = partition_month(run_id)
        async with tenant_connection(self._pool, tenant_id) as conn:
            row = await conn.fetchrow(_LOCK_RUN, partition_key, run_id)
            if row is None:
                raise RunNotFoundError("no such run", context={"run_id": str(run_id)})
            if row["fencing_token"] != lease.fencing_token:
                raise StaleLeaseError(
                    "lease has been superseded",
                    context={"held": lease.fencing_token, "current": row["fencing_token"]},
                )

            appended = await self._events.append_batch(
                tenant_id, run_id, after_seq=row["last_seq"], payloads=payloads, conn=conn
            )

            state: RunState = RunState(status=RunStatus(row["status"]), last_seq=row["last_seq"])
            for event in appended:
                state = apply(state, event)
            # last_seq tracks the actual last sequence (including DAG events, which
            # run fold ignores); status comes from the run-lifecycle fold.
            await conn.execute(
                _UPDATE_PROJECTION, partition_key, run_id, state.status.value, appended[-1].seq
            )
        await self._publish(run_id, appended)
        return appended

    async def read_events(self, tenant_id: TenantId, run_id: RunId) -> list[Envelope]:
        """Read a run's full event log (for projecting the DAG, for example)."""
        return await self._events.read(tenant_id, run_id)

    async def find_pending(self, tenant_id: TenantId) -> RunId | None:
        """Return a pending, unleased run for this tenant, or ``None``.

        Non-atomic: two workers may find the same run, but only one wins the
        lease, so the race is harmless.
        """
        async with tenant_connection(self._pool, tenant_id) as conn:
            row = await conn.fetchrow(
                "SELECT run_id FROM runs "
                "WHERE status = 'pending' AND (lease_owner IS NULL OR lease_expires_at < now()) "
                "ORDER BY created_at LIMIT 1"
            )
        return RunId(row["run_id"]) if row is not None else None

    async def request_cancel(self, tenant_id: TenantId, run_id: RunId) -> None:
        """Flag a run for cancellation. Requires no lease; the scheduler observes it."""
        partition_key = partition_month(run_id)
        async with tenant_connection(self._pool, tenant_id) as conn:
            await conn.execute(
                "UPDATE runs SET cancel_requested = true, updated_at = now() "
                "WHERE partition_key = $1 AND run_id = $2",
                partition_key,
                run_id,
            )

    async def is_cancel_requested(self, tenant_id: TenantId, run_id: RunId) -> bool:
        """Whether cancellation has been requested for this run."""
        partition_key = partition_month(run_id)
        async with tenant_connection(self._pool, tenant_id) as conn:
            row = await conn.fetchrow(
                "SELECT cancel_requested FROM runs WHERE partition_key = $1 AND run_id = $2",
                partition_key,
                run_id,
            )
        return bool(row["cancel_requested"]) if row is not None else False

    async def load_state(self, tenant_id: TenantId, run_id: RunId) -> RunState:
        """Reconstruct a run's state, folding from the latest snapshot if any.

        With a checkpoint manager configured, the latest snapshot is loaded and
        only the events after it are folded; without one, the whole log is folded.
        Both paths yield identical state — the snapshot is a pure cache.

        :raises RunNotFoundError: if the run has no events.
        """
        async with tenant_connection(self._pool, tenant_id) as conn:
            snapshot = (
                await self._checkpoints.load_latest(tenant_id, run_id, conn=conn)
                if self._checkpoints is not None
                else None
            )
            if snapshot is None:
                events = await self._events.read(tenant_id, run_id, conn=conn)
                if not events:
                    raise RunNotFoundError("no such run", context={"run_id": str(run_id)})
                return fold(events)
            state, at_seq = snapshot
            tail = await self._events.read(tenant_id, run_id, from_seq=at_seq, conn=conn)
            return fold(tail, initial=state)
