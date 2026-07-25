"""The frozen event envelope and payload base.

The envelope's *shape* is frozen for the life of the system: every stored event,
of every type and version, has exactly these fields. Payloads are the open part
— concrete payload types are added per phase and registered in the registry.

Both models are immutable (``frozen=True``): events are facts that already
happened and are never mutated in place.
"""

from __future__ import annotations

from datetime import datetime
from uuid import UUID

from pydantic import BaseModel, ConfigDict, SerializeAsAny

from agent_runtime.ids import EventId, RunId, TenantId


class EventPayload(BaseModel):
    """Base class for all event payloads.

    Concrete payloads subclass this and are registered with an ``event_type``
    string and a version. ``extra="forbid"`` makes an unexpected field a loud
    validation error rather than silently-dropped data.
    """

    model_config = ConfigDict(frozen=True, extra="forbid")


class Envelope(BaseModel):
    """An event as it lives in the log: frozen identity, ordering, and payload.

    Invariants:
    * ``seq`` is the sole authority on ordering within a run (1-based, monotonic);
      ``occurred_at`` / ``recorded_at`` are informational only.
    * ``payload`` is annotated ``SerializeAsAny`` so dumping an envelope emits the
      concrete payload's fields, not just the base class's.
    """

    model_config = ConfigDict(frozen=True)

    event_id: EventId
    tenant_id: TenantId
    run_id: RunId
    seq: int
    event_type: str
    payload_version: int
    payload: SerializeAsAny[EventPayload]
    occurred_at: datetime
    recorded_at: datetime
    causation_id: EventId | None = None
    correlation_id: UUID | None = None
