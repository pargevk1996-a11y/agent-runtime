"""Identifier generation and typed ID wrappers.

The runtime uses UUIDv7 (RFC 9562) for every identifier: time-sortable, so it
indexes well and orders chronologically across shards, yet generated entirely in
the application *before* the row is written. Application-side generation is what
makes deterministic idempotency keys and retry-safe inserts possible — a
database-assigned ``bigserial`` would require a round trip to learn one's own id.

Python 3.12's stdlib has no ``uuid.uuid7`` (added in 3.14), so it is implemented
here per the RFC rather than pulling a dependency for ~15 lines of bit-twiddling.

Each ID kind is a distinct :func:`typing.NewType` over ``UUID`` so that
``mypy --strict`` rejects passing a ``RunId`` where a ``NodeId`` is expected.
"""

from __future__ import annotations

import os
import time
from typing import NewType
from uuid import UUID


def uuid7(ms: int | None = None) -> UUID:
    """Return a UUIDv7 (RFC 9562).

    Layout, most-significant bit first: 48-bit unix-millisecond timestamp,
    4-bit version (0b0111), 12 random bits, 2-bit variant (0b10), 62 random bits.

    :param ms: unix epoch milliseconds to embed; defaults to the current time.
        Passing it explicitly makes generation deterministic for tests.

    Invariant: the returned UUID always has ``version == 7`` and RFC-4122
    variant. Ordering is monotonic *across* distinct milliseconds; two UUIDs
    minted within the same millisecond are ordered only by their random tail
    (per-run event ``seq`` provides strict ordering where it matters).
    """
    if ms is None:
        ms = time.time_ns() // 1_000_000

    rand = int.from_bytes(os.urandom(10), "big")  # 80 random bits; 74 are used
    rand_a = rand & 0xFFF  # 12 bits
    rand_b = (rand >> 12) & 0x3FFF_FFFF_FFFF_FFFF  # 62 bits

    value = (ms & 0xFFFF_FFFF_FFFF) << 80  # 48-bit timestamp
    value |= 0x7 << 76  # version
    value |= rand_a << 64  # rand_a
    value |= 0b10 << 62  # variant
    value |= rand_b  # rand_b
    return UUID(int=value)


RunId = NewType("RunId", UUID)
NodeId = NewType("NodeId", UUID)
EventId = NewType("EventId", UUID)
TenantId = NewType("TenantId", UUID)


def new_run_id() -> RunId:
    """Mint a fresh :data:`RunId`."""
    return RunId(uuid7())


def new_node_id() -> NodeId:
    """Mint a fresh :data:`NodeId`."""
    return NodeId(uuid7())


def new_event_id() -> EventId:
    """Mint a fresh :data:`EventId`."""
    return EventId(uuid7())


def new_tenant_id() -> TenantId:
    """Mint a fresh :data:`TenantId`."""
    return TenantId(uuid7())
