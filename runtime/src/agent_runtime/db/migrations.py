"""Numbered SQL migration runner.

Migrations are plain ``NNNN_name.sql`` files applied in filename order. State is
tracked in ``schema_migrations`` so a re-run is a no-op — the runner is
idempotent. No Alembic: with hand-written SQL and no ORM, a twenty-line runner is
less machinery than a migration framework buys us.

The connection passed in must belong to the schema-owning role (``ar_admin``),
which owns the tables and therefore bypasses Row-Level Security while applying
DDL.
"""

from __future__ import annotations

from pathlib import Path

import asyncpg

from agent_runtime.logging import get_logger

_log = get_logger(__name__)
_MIGRATIONS_DIR = Path(__file__).parent / "migrations"


async def apply_migrations(conn: asyncpg.Connection[asyncpg.Record]) -> list[str]:
    """Apply all pending migrations in version order.

    :returns: the versions applied by this call (empty if already up to date).

    Invariant: each migration runs in its own transaction together with the
    ``schema_migrations`` insert, so a failure mid-file leaves that migration
    unrecorded and unapplied — never half-applied-but-recorded.
    """
    await conn.execute(
        "CREATE TABLE IF NOT EXISTS schema_migrations ("
        "version text PRIMARY KEY, applied_at timestamptz NOT NULL DEFAULT now())"
    )
    rows = await conn.fetch("SELECT version FROM schema_migrations")
    applied: set[str] = {str(row["version"]) for row in rows}

    ran: list[str] = []
    for path in sorted(_MIGRATIONS_DIR.glob("*.sql")):
        version = path.stem
        if version in applied:
            continue
        sql = path.read_text(encoding="utf-8")
        async with conn.transaction():
            await conn.execute(sql)
            await conn.execute("INSERT INTO schema_migrations (version) VALUES ($1)", version)
        ran.append(version)
        _log.info("migration_applied", version=version)
    return ran
