"""Integration tests for migrations and Row-Level Security tenant isolation."""

from __future__ import annotations

from datetime import UTC, datetime

import asyncpg
import pytest

from agent_runtime.db.migrations import apply_migrations
from agent_runtime.db.pool import create_pool, tenant_connection
from agent_runtime.ids import new_event_id, new_run_id, new_tenant_id, uuid7_timestamp

pytestmark = pytest.mark.integration

_INSERT = (
    "INSERT INTO events "
    "(partition_key, run_id, seq, event_id, tenant_id, event_type, "
    " payload_version, payload, occurred_at) "
    "VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)"
)


async def test_migrations_are_idempotent(pg: dict[str, str]) -> None:
    conn = await asyncpg.connect(pg["admin_dsn"])
    try:
        # The fixture already applied 0001; a second run must be a no-op.
        assert await apply_migrations(conn) == []
        row = await conn.fetchrow(
            "SELECT count(*) AS n FROM schema_migrations WHERE version = '0001_event_store'"
        )
        assert row is not None
        assert row["n"] == 1
    finally:
        await conn.close()


async def test_rls_isolates_tenants(pg: dict[str, str]) -> None:
    pool = await create_pool(pg["app_dsn"])
    try:
        tenant_a = new_tenant_id()
        tenant_b = new_tenant_id()
        run = new_run_id()
        partition_key = uuid7_timestamp(run).date().replace(day=1)
        now = datetime.now(UTC)

        async with tenant_connection(pool, tenant_a) as conn:
            await conn.execute(
                _INSERT, partition_key, run, 1, new_event_id(), tenant_a, "sample", 1, "{}", now
            )

        async with tenant_connection(pool, tenant_b) as conn:
            assert await conn.fetch("SELECT 1 FROM events WHERE run_id = $1", run) == []

        async with tenant_connection(pool, tenant_a) as conn:
            rows = await conn.fetch("SELECT 1 FROM events WHERE run_id = $1", run)
            assert len(rows) == 1
    finally:
        await pool.close()


async def test_rls_with_check_blocks_foreign_tenant_insert(pg: dict[str, str]) -> None:
    pool = await create_pool(pg["app_dsn"])
    try:
        tenant_a = new_tenant_id()
        tenant_b = new_tenant_id()
        run = new_run_id()
        partition_key = uuid7_timestamp(run).date().replace(day=1)
        now = datetime.now(UTC)

        # Bound to tenant_a but inserting a tenant_b row violates the WITH CHECK.
        with pytest.raises(asyncpg.PostgresError):
            async with tenant_connection(pool, tenant_a) as conn:
                await conn.execute(
                    _INSERT, partition_key, run, 1, new_event_id(), tenant_b, "sample", 1, "{}", now
                )
    finally:
        await pool.close()
