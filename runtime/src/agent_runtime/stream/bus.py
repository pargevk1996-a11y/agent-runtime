"""Redis Streams event bus: publish a run's events and tail them live.

Each run has a bounded stream ``run:{id}:events``. Publishing trims to a maximum
length, so a slow subscriber never grows memory without bound — it simply misses
aged-out entries and backfills them from the log. Entries carry the event's
``seq`` so a subscriber can pick up exactly after a known point.
"""

from __future__ import annotations

from collections.abc import AsyncIterator, Sequence
from dataclasses import dataclass
from typing import Protocol, cast

import redis.asyncio as aioredis

from agent_runtime.events.envelope import Envelope
from agent_runtime.ids import RunId

# Shape of a decode_responses=True XREAD reply: [(stream, [(id, {field: value})])].
_XReadReply = list[tuple[str, list[tuple[str, dict[str, str]]]]]


def _stream_key(run_id: RunId) -> str:
    return f"run:{run_id}:events"


@dataclass(frozen=True)
class StreamEntry:
    """One event as it travels over the stream: ordering, type, and payload JSON."""

    seq: int
    event_type: str
    data: str


class StreamPublisher(Protocol):
    """Publishes a run's committed events for live fan-out."""

    async def publish(self, run_id: RunId, envelopes: Sequence[Envelope]) -> None:
        """Publish already-committed envelopes; best-effort, never blocking."""
        ...


class RedisStreamBus:
    """A :class:`StreamPublisher` and live tail backed by Redis Streams."""

    def __init__(self, client: aioredis.Redis, *, maxlen: int = 1000) -> None:
        self._redis = client
        self._maxlen = maxlen

    async def publish(self, run_id: RunId, envelopes: Sequence[Envelope]) -> None:
        key = _stream_key(run_id)
        for envelope in envelopes:
            await self._redis.xadd(
                key,
                {
                    "seq": str(envelope.seq),
                    "event_type": envelope.event_type,
                    "data": envelope.model_dump_json(),
                },
                maxlen=self._maxlen,
                approximate=True,
            )

    async def tail(
        self, run_id: RunId, *, after_seq: int = 0, block_ms: int = 1000
    ) -> AsyncIterator[StreamEntry]:
        """Yield stream entries with ``seq > after_seq``, blocking for new ones.

        Reads from the start of the (bounded) stream and follows it live. The
        caller stops iterating when it sees a terminal event or the client goes
        away.
        """
        key = _stream_key(run_id)
        last_id = "0-0"
        while True:
            raw = await self._redis.xread({key: last_id}, block=block_ms, count=100)
            response = cast("_XReadReply | None", raw)
            if not response:
                continue
            for _stream, entries in response:
                for entry_id, fields in entries:
                    last_id = entry_id
                    seq = int(fields["seq"])
                    if seq > after_seq:
                        yield StreamEntry(
                            seq=seq, event_type=fields["event_type"], data=fields["data"]
                        )
