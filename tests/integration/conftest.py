"""Integration-test fixtures: a real PostgreSQL via testcontainers.

The container is started once per session. On startup the ``ar_app`` runtime role
is created and migrations are applied as the superuser (which stands in for the
schema-owning ``ar_admin`` in tests). Tests receive DSNs for both the admin role
(bypasses RLS) and the app role (subject to RLS).
"""

from __future__ import annotations

import asyncio
from collections.abc import Iterator

import asyncpg
import pytest
from testcontainers.postgres import PostgresContainer
from testcontainers.redis import RedisContainer

from agent_runtime.db.migrations import apply_migrations

APP_PASSWORD = "app_pw"

_CREATE_APP_ROLE = (
    "DO $$ BEGIN "
    "IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'ar_app') "
    "THEN CREATE ROLE ar_app LOGIN PASSWORD 'app_pw'; END IF; "
    "END $$;"
)


def _dsn(host: str, port: str, user: str, password: str, db: str) -> str:
    return f"postgresql://{user}:{password}@{host}:{port}/{db}"


@pytest.fixture(scope="session")
def pg() -> Iterator[dict[str, str]]:
    with PostgresContainer("postgres:16") as container:
        host = container.get_container_host_ip()
        port = container.get_exposed_port(5432)
        admin_dsn = _dsn(host, port, container.username, container.password, container.dbname)
        app_dsn = _dsn(host, port, "ar_app", APP_PASSWORD, container.dbname)

        async def _bootstrap() -> None:
            conn = await asyncpg.connect(admin_dsn)
            try:
                await conn.execute(_CREATE_APP_ROLE)
                await apply_migrations(conn)
            finally:
                await conn.close()

        asyncio.run(_bootstrap())
        yield {"admin_dsn": admin_dsn, "app_dsn": app_dsn}


@pytest.fixture(scope="session")
def redis_url() -> Iterator[str]:
    with RedisContainer("redis:7") as container:
        host = container.get_container_host_ip()
        port = container.get_exposed_port(6379)
        yield f"redis://{host}:{port}/0"
