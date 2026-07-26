"""A lease-bound :class:`RunJournal` over the run store.

Binds a run's identity and lease so callers append/read without repeating them.
Structurally satisfies ``agent_runtime.journal.RunJournal``.
"""

from __future__ import annotations

from collections.abc import Sequence

from agent_runtime.events.envelope import Envelope, EventPayload
from agent_runtime.ids import RunId, TenantId
from agent_runtime.runs.store import Lease, RunStore


class LeaseJournal:
    """Appends and reads one run's log under a held lease."""

    def __init__(
        self, run_store: RunStore, tenant_id: TenantId, run_id: RunId, lease: Lease
    ) -> None:
        self._runs = run_store
        self._tenant_id = tenant_id
        self._run_id = run_id
        self._lease = lease

    async def append(self, payloads: Sequence[EventPayload]) -> list[Envelope]:
        return await self._runs.append_events(
            self._tenant_id, self._run_id, lease=self._lease, payloads=payloads
        )

    async def read(self) -> list[Envelope]:
        return await self._runs.read_events(self._tenant_id, self._run_id)
