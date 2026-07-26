"""Apply database migrations from the command line.

    make migrate            # or: uv run python -m agent_runtime.db.migrate

Connects as the admin role (which owns the schema and bypasses RLS) using the
configured DSN, and applies any pending migrations. Idempotent.
"""

from __future__ import annotations

import asyncio

import asyncpg

from agent_runtime.config import get_settings
from agent_runtime.db.migrations import apply_migrations


async def _connect_with_retry(
    dsn: str, *, attempts: int = 15, delay: float = 1.0
) -> asyncpg.Connection[asyncpg.Record]:
    last: Exception | None = None
    for _ in range(attempts):
        try:
            return await asyncpg.connect(dsn)
        except (OSError, asyncpg.PostgresError) as exc:  # DB may still be starting
            last = exc
            await asyncio.sleep(delay)
    raise RuntimeError(f"could not connect to the database after {attempts} attempts: {last}")


async def main() -> None:
    settings = get_settings()
    conn = await _connect_with_retry(str(settings.db_admin_dsn))
    try:
        applied = await apply_migrations(conn)
        print(f"applied migrations: {applied or 'none (already up to date)'}")
    finally:
        await conn.close()


if __name__ == "__main__":
    asyncio.run(main())
