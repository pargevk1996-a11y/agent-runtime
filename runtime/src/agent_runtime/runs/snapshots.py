"""Checkpoint manager: snapshots of folded run state for fast recovery.

A snapshot is a pure cache: it stores the ``RunState`` folded up to some sequence
number, so recovery folds only the tail (events after the snapshot) instead of the
whole log. Deleting every snapshot must never change the reconstructed state —
the event log remains the single source of truth. Snapshots live in their own
``run_snapshots`` side-table, never in the event log itself.
"""

from __future__ import annotations

import asyncpg

from agent_runtime.db.pool import tenant_connection
from agent_runtime.ids import RunId, TenantId, partition_month
from agent_runtime.runs.state import RunState

_INSERT = (
    "INSERT INTO run_snapshots (partition_key, run_id, tenant_id, at_seq, state) "
    "VALUES ($1, $2, $3, $4, $5) "
    "ON CONFLICT (partition_key, run_id, at_seq) DO NOTHING"
)
_LOAD_LATEST = (
    "SELECT state, at_seq FROM run_snapshots "
    "WHERE partition_key = $1 AND run_id = $2 "
    "ORDER BY at_seq DESC LIMIT 1"
)
_PRUNE = (
    "DELETE FROM run_snapshots "
    "WHERE partition_key = $1 AND run_id = $2 AND at_seq NOT IN ("
    "  SELECT at_seq FROM run_snapshots "
    "  WHERE partition_key = $1 AND run_id = $2 ORDER BY at_seq DESC LIMIT $3"
    ")"
)


class SnapshotPolicy:
    """Decides when a new snapshot is due, based on event count.

    Pure and stateless: given the sequence of the last snapshot (or ``None`` if
    there is none) and the current sequence, it says whether a fresh snapshot
    should be taken now.
    """

    def __init__(self, every: int) -> None:
        if every < 1:
            raise ValueError("every must be >= 1")
        self._every = every

    def due(self, last_snapshot_seq: int | None, current_seq: int) -> bool:
        """True if ``current_seq`` has advanced at least ``every`` past the last."""
        baseline = last_snapshot_seq if last_snapshot_seq is not None else 0
        return current_seq - baseline >= self._every


class CheckpointManager:
    """Saves and loads run-state snapshots in the ``run_snapshots`` side-table."""

    def __init__(self, pool: asyncpg.Pool[asyncpg.Record]) -> None:
        self._pool = pool

    async def save(
        self,
        tenant_id: TenantId,
        run_id: RunId,
        state: RunState,
        *,
        at_seq: int,
        conn: asyncpg.Connection[asyncpg.Record] | None = None,
    ) -> None:
        """Persist ``state`` as the snapshot at ``at_seq`` (idempotent per seq)."""
        partition_key = partition_month(run_id)
        payload = state.model_dump(mode="json")
        if conn is not None:
            await conn.execute(_INSERT, partition_key, run_id, tenant_id, at_seq, payload)
            return
        async with tenant_connection(self._pool, tenant_id) as owned:
            await owned.execute(_INSERT, partition_key, run_id, tenant_id, at_seq, payload)

    async def load_latest(
        self,
        tenant_id: TenantId,
        run_id: RunId,
        *,
        conn: asyncpg.Connection[asyncpg.Record] | None = None,
    ) -> tuple[RunState, int] | None:
        """Return the most recent snapshot as ``(state, at_seq)``, or ``None``."""
        partition_key = partition_month(run_id)
        if conn is not None:
            row = await conn.fetchrow(_LOAD_LATEST, partition_key, run_id)
        else:
            async with tenant_connection(self._pool, tenant_id) as owned:
                row = await owned.fetchrow(_LOAD_LATEST, partition_key, run_id)
        if row is None:
            return None
        return RunState.model_validate(row["state"]), row["at_seq"]

    async def prune(
        self,
        tenant_id: TenantId,
        run_id: RunId,
        *,
        keep: int,
        conn: asyncpg.Connection[asyncpg.Record] | None = None,
    ) -> int:
        """Delete all but the newest ``keep`` snapshots; return the number removed."""
        if keep < 1:
            raise ValueError("keep must be >= 1")
        partition_key = partition_month(run_id)
        if conn is not None:
            status = await conn.execute(_PRUNE, partition_key, run_id, keep)
        else:
            async with tenant_connection(self._pool, tenant_id) as owned:
                status = await owned.execute(_PRUNE, partition_key, run_id, keep)
        # asyncpg returns e.g. "DELETE 3"; the trailing integer is the row count.
        return int(status.split()[-1])
