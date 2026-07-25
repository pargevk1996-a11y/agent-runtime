"""Connection pool and tenant-scoped sessions.

Every runtime read/write of tenant data must go through :func:`tenant_connection`,
which opens a transaction and binds ``app.tenant_id`` for that transaction so the
Row-Level Security policy on ``events`` filters to the caller's tenant. Acquiring
a raw connection and querying tenant tables without this is a bug: with no tenant
GUC set the RLS policy denies all rows.
"""

from __future__ import annotations

import json
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import asyncpg

from agent_runtime.ids import TenantId


async def _init_connection(conn: asyncpg.Connection[asyncpg.Record]) -> None:
    """Decode JSONB as Python objects rather than raw text.

    asyncpg returns ``jsonb`` as a string by default; registering this codec once
    per pooled connection means payloads cross the boundary as ``dict`` in both
    directions, so no caller hand-rolls ``json.loads`` / ``json.dumps``.
    """
    await conn.set_type_codec("jsonb", encoder=json.dumps, decoder=json.loads, schema="pg_catalog")


async def create_pool(
    dsn: str, *, min_size: int = 1, max_size: int = 10
) -> asyncpg.Pool[asyncpg.Record]:
    """Create an asyncpg connection pool for ``dsn`` with the JSONB codec installed."""
    return await asyncpg.create_pool(
        dsn, min_size=min_size, max_size=max_size, init=_init_connection
    )


@asynccontextmanager
async def tenant_connection(
    pool: asyncpg.Pool[asyncpg.Record], tenant_id: TenantId
) -> AsyncIterator[asyncpg.Connection[asyncpg.Record]]:
    """Yield a connection inside a transaction bound to ``tenant_id``.

    Uses ``set_config(..., is_local => true)`` rather than ``SET LOCAL`` because
    the latter cannot be parameterized; passing the id as a bind parameter keeps
    it injection-safe. The binding lasts only for the surrounding transaction.
    """
    async with pool.acquire() as conn, conn.transaction():
        await conn.execute("SELECT set_config('app.tenant_id', $1, true)", str(tenant_id))
        yield conn
