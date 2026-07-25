"""Typed mapping from asyncpg rows to domain objects.

This module is the one place that touches asyncpg's untyped ``Record`` and turns
it into a strongly-typed :class:`Envelope`. Localizing the ``Any`` here keeps the
rest of the codebase type-safe under ``mypy --strict``.
"""

from __future__ import annotations

import asyncpg

from agent_runtime.events.envelope import Envelope
from agent_runtime.events.registry import EventRegistry
from agent_runtime.ids import EventId, RunId, TenantId


def record_to_envelope(record: asyncpg.Record, registry: EventRegistry) -> Envelope:
    """Reconstruct an :class:`Envelope` from a stored ``events`` row.

    The row's ``payload_version`` drives upcasting only; the returned envelope's
    ``payload_version`` is the *current* registered version, matching the upcast
    payload instance it carries — the two never disagree.
    """
    event_type: str = record["event_type"]
    stored_version: int = record["payload_version"]
    payload = registry.decode(event_type, stored_version, record["payload"])

    causation = record["causation_id"]
    return Envelope(
        event_id=EventId(record["event_id"]),
        tenant_id=TenantId(record["tenant_id"]),
        run_id=RunId(record["run_id"]),
        seq=record["seq"],
        event_type=event_type,
        payload_version=registry.current_version(event_type),
        payload=payload,
        occurred_at=record["occurred_at"],
        recorded_at=record["recorded_at"],
        causation_id=EventId(causation) if causation is not None else None,
        correlation_id=record["correlation_id"],
    )
