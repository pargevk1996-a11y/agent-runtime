"""Integration and property-based tests for the EventStore."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from datetime import UTC, datetime

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from agent_runtime.db.pool import create_pool, tenant_connection
from agent_runtime.events.envelope import EventPayload
from agent_runtime.events.errors import ConcurrencyError
from agent_runtime.events.registry import EventRegistry
from agent_runtime.events.store import EventStore
from agent_runtime.ids import new_event_id, new_run_id, new_tenant_id, uuid7_timestamp

pytestmark = pytest.mark.integration


class Noted(EventPayload):
    text: str


class CountedV2(EventPayload):
    text: str
    count: int


def _v1_to_v2(data: dict[str, object]) -> dict[str, object]:
    return {**data, "count": 0}


def _registry() -> EventRegistry:
    registry = EventRegistry()
    registry.register("noted", Noted)
    return registry


_RAW_INSERT = (
    "INSERT INTO events (partition_key, run_id, seq, event_id, tenant_id, "
    " event_type, payload_version, payload, occurred_at) "
    "VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)"
)


@pytest.fixture
async def store(pg: dict[str, str]) -> AsyncIterator[EventStore]:
    pool = await create_pool(pg["app_dsn"])
    try:
        yield EventStore(pool, registry=_registry())
    finally:
        await pool.close()


async def test_append_and_read_round_trip(store: EventStore) -> None:
    tenant, run = new_tenant_id(), new_run_id()
    appended = await store.append(tenant, run, after_seq=0, payload=Noted(text="hi"))
    assert appended.seq == 1

    events = await store.read(tenant, run)
    assert len(events) == 1
    assert events[0].event_id == appended.event_id
    assert events[0].payload == Noted(text="hi")
    assert events[0].recorded_at is not None


async def test_batch_assigns_contiguous_sequences(store: EventStore) -> None:
    tenant, run = new_tenant_id(), new_run_id()
    payloads = [Noted(text=f"n{i}") for i in range(5)]

    appended = await store.append_batch(tenant, run, after_seq=0, payloads=payloads)
    assert [e.seq for e in appended] == [1, 2, 3, 4, 5]

    events = await store.read(tenant, run)
    assert [e.seq for e in events] == [1, 2, 3, 4, 5]
    assert [e.payload for e in events] == payloads


async def test_conflicting_append_raises_concurrency_error(store: EventStore) -> None:
    tenant, run = new_tenant_id(), new_run_id()
    await store.append(tenant, run, after_seq=0, payload=Noted(text="first"))
    with pytest.raises(ConcurrencyError):
        await store.append(tenant, run, after_seq=0, payload=Noted(text="racer"))


async def test_read_bounds_are_from_exclusive_to_inclusive(store: EventStore) -> None:
    tenant, run = new_tenant_id(), new_run_id()
    await store.append_batch(
        tenant, run, after_seq=0, payloads=[Noted(text=str(i)) for i in range(5)]
    )
    windowed = await store.read(tenant, run, from_seq=1, to_seq=3)
    assert [e.seq for e in windowed] == [2, 3]


async def test_rls_read_hides_other_tenant(store: EventStore) -> None:
    tenant_a, tenant_b, run = new_tenant_id(), new_tenant_id(), new_run_id()
    await store.append(tenant_a, run, after_seq=0, payload=Noted(text="secret"))
    assert await store.read(tenant_b, run) == []


async def test_rejects_empty_and_negative_input(store: EventStore) -> None:
    tenant, run = new_tenant_id(), new_run_id()
    with pytest.raises(ValueError):
        await store.append_batch(tenant, run, after_seq=0, payloads=[])
    with pytest.raises(ValueError):
        await store.append(tenant, run, after_seq=-1, payload=Noted(text="x"))


async def test_read_upcasts_stored_old_version(pg: dict[str, str]) -> None:
    tenant, run = new_tenant_id(), new_run_id()
    partition_key = uuid7_timestamp(run).date().replace(day=1)
    now = datetime.now(UTC)

    pool = await create_pool(pg["app_dsn"])
    try:
        # Write a v1 row directly, then read through a registry whose model is v2.
        async with tenant_connection(pool, tenant) as conn:
            await conn.execute(
                _RAW_INSERT,
                partition_key,
                run,
                1,
                new_event_id(),
                tenant,
                "counted",
                1,
                {"text": "old"},
                now,
            )
        registry = EventRegistry()
        registry.register("counted", CountedV2, version=2, upcasters={1: _v1_to_v2})
        store = EventStore(pool, registry=registry)

        events = await store.read(tenant, run)
        assert events[0].payload == CountedV2(text="old", count=0)
        assert events[0].payload_version == 2
    finally:
        await pool.close()


@settings(max_examples=20, deadline=None)
@given(n=st.integers(min_value=1, max_value=10))
def test_property_appended_events_read_back_contiguous(pg: dict[str, str], n: int) -> None:
    async def _body() -> None:
        pool = await create_pool(pg["app_dsn"])
        try:
            store = EventStore(pool, registry=_registry())
            tenant, run = new_tenant_id(), new_run_id()
            payloads = [Noted(text=str(i)) for i in range(n)]
            await store.append_batch(tenant, run, after_seq=0, payloads=payloads)
            events = await store.read(tenant, run)
            assert [e.seq for e in events] == list(range(1, n + 1))
            assert [e.payload for e in events] == payloads
        finally:
            await pool.close()

    asyncio.run(_body())
