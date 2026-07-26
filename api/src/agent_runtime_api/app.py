"""FastAPI application factory for the control plane.

``build_app`` wires pre-built stores directly onto app state — used by tests.
``create_app`` builds the pool, Redis client, and stores from configuration in a
lifespan, closing them on shutdown — used in production.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import redis.asyncio as aioredis
from fastapi import FastAPI

from agent_runtime.dag.events import register_dag_events
from agent_runtime.db.pool import create_pool
from agent_runtime.events.registry import EventRegistry
from agent_runtime.events.store import EventStore
from agent_runtime.runs.events import register_run_events
from agent_runtime.runs.snapshots import CheckpointManager
from agent_runtime.runs.store import RunStore
from agent_runtime.stream.bus import RedisStreamBus
from agent_runtime.tools.events import register_tool_events
from agent_runtime_api.routes import router

_TITLE = "agent-runtime control plane"


def full_registry() -> EventRegistry:
    """A registry with every event type the runtime produces registered."""
    registry = EventRegistry()
    register_run_events(registry)
    register_dag_events(registry)
    register_tool_events(registry)
    return registry


def build_app(run_store: RunStore, event_store: EventStore, bus: RedisStreamBus) -> FastAPI:
    """Build an app around already-constructed stores (for tests)."""
    app = FastAPI(title=_TITLE)
    app.state.run_store = run_store
    app.state.event_store = event_store
    app.state.bus = bus
    app.include_router(router)
    return app


def create_app(app_dsn: str, redis_url: str) -> FastAPI:
    """Build a production app that owns its pool and Redis client."""

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        pool = await create_pool(app_dsn)
        redis: aioredis.Redis = aioredis.from_url(redis_url, decode_responses=True)
        bus = RedisStreamBus(redis)
        event_store = EventStore(pool, registry=full_registry())
        run_store = RunStore(pool, event_store, CheckpointManager(pool), publisher=bus)
        app.state.run_store = run_store
        app.state.event_store = event_store
        app.state.bus = bus
        try:
            yield
        finally:
            await pool.close()
            await redis.aclose()

    app = FastAPI(title=_TITLE, lifespan=lifespan)
    app.include_router(router)
    return app
