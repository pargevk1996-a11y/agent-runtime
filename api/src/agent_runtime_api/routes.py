"""Control-plane HTTP routes: create, status, cancel, replay, and live stream.

Tenancy comes from the ``X-Tenant-Id`` header. Before streaming a run's live
events, ownership is verified against the log (Row-Level Security), so a caller
cannot tail another tenant's Redis stream by guessing a run id.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from typing import Annotated, cast
from uuid import UUID

from fastapi import APIRouter, Depends, Header, HTTPException, Request
from fastapi.responses import StreamingResponse

from agent_runtime.events.store import EventStore
from agent_runtime.ids import RunId, TenantId, new_run_id
from agent_runtime.runs.errors import RunNotFoundError
from agent_runtime.runs.events import RUN_CANCELLED, RUN_FAILED, RUN_SUCCEEDED
from agent_runtime.runs.store import RunStore
from agent_runtime.stream.bus import RedisStreamBus
from agent_runtime_api.schemas import CreateRunRequest, CreateRunResponse, RunStatusResponse

router = APIRouter()

_TERMINAL_EVENTS = frozenset({RUN_SUCCEEDED, RUN_FAILED, RUN_CANCELLED})


def _tenant(x_tenant_id: Annotated[str, Header(alias="X-Tenant-Id")]) -> TenantId:
    try:
        return TenantId(UUID(x_tenant_id))
    except ValueError as exc:
        raise HTTPException(status_code=400, detail="invalid X-Tenant-Id") from exc


Tenant = Annotated[TenantId, Depends(_tenant)]


def _run_store(request: Request) -> RunStore:
    return cast(RunStore, request.app.state.run_store)


def _event_store(request: Request) -> EventStore:
    return cast(EventStore, request.app.state.event_store)


def _bus(request: Request) -> RedisStreamBus:
    return cast(RedisStreamBus, request.app.state.bus)


@router.post("/runs", status_code=201)
async def create_run(request: Request, body: CreateRunRequest, tenant: Tenant) -> CreateRunResponse:
    run_id = new_run_id()
    await _run_store(request).create_run(tenant, run_id, input=body.input)
    return CreateRunResponse(run_id=run_id)


@router.get("/runs/{run_id}")
async def get_run(request: Request, run_id: UUID, tenant: Tenant) -> RunStatusResponse:
    try:
        state = await _run_store(request).load_state(tenant, RunId(run_id))
    except RunNotFoundError as exc:
        raise HTTPException(status_code=404, detail="run not found") from exc
    return RunStatusResponse(run_id=run_id, status=state.status.value, last_seq=state.last_seq)


@router.post("/runs/{run_id}/cancel", status_code=202)
async def cancel_run(request: Request, run_id: UUID, tenant: Tenant) -> dict[str, str]:
    await _run_store(request).request_cancel(tenant, RunId(run_id))
    return {"status": "cancel_requested"}


@router.get("/runs/{run_id}/replay")
async def replay_run(
    request: Request, run_id: UUID, tenant: Tenant, after_seq: int = 0
) -> list[dict[str, object]]:
    events = await _event_store(request).read(tenant, RunId(run_id), from_seq=after_seq)
    return [event.model_dump(mode="json") for event in events]


@router.get("/runs/{run_id}/events")
async def stream_run_events(
    request: Request, run_id: UUID, tenant: Tenant, after_seq: int = 0
) -> StreamingResponse:
    rid = RunId(run_id)
    # Ownership gate: the run must be visible to this tenant before we tail Redis.
    try:
        await _run_store(request).load_state(tenant, rid)
    except RunNotFoundError as exc:
        raise HTTPException(status_code=404, detail="run not found") from exc

    event_store = _event_store(request)
    bus = _bus(request)

    async def generate() -> AsyncIterator[str]:
        last_seq = after_seq
        for envelope in await event_store.read(tenant, rid, from_seq=after_seq):
            yield f"data: {envelope.model_dump_json()}\n\n"
            last_seq = envelope.seq
            if envelope.event_type in _TERMINAL_EVENTS:
                return
        async for entry in bus.tail(rid, after_seq=last_seq):
            if await request.is_disconnected():
                return
            yield f"data: {entry.data}\n\n"
            if entry.event_type in _TERMINAL_EVENTS:
                return

    return StreamingResponse(generate(), media_type="text/event-stream")
