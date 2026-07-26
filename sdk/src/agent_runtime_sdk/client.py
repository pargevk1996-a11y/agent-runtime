"""A thin async client for the agent-runtime control plane.

Talks to the HTTP API like any external consumer — it deliberately depends on no
runtime internals, only ``httpx``. Every request carries the tenant header.
"""

from __future__ import annotations

import json
from collections.abc import AsyncIterator
from types import TracebackType
from uuid import UUID

import httpx


class AgentRuntimeClient:
    """Create, inspect, cancel, replay, and subscribe to runs over HTTP."""

    def __init__(
        self, base_url: str, tenant_id: str | UUID, *, client: httpx.AsyncClient | None = None
    ) -> None:
        self._headers = {"X-Tenant-Id": str(tenant_id)}
        self._client = client or httpx.AsyncClient(base_url=base_url)

    async def create_run(self, run_input: dict[str, object] | None = None) -> UUID:
        """Create a run and return its id."""
        response = await self._client.post(
            "/runs", json={"input": run_input or {}}, headers=self._headers
        )
        response.raise_for_status()
        return UUID(response.json()["run_id"])

    async def get_run(self, run_id: UUID) -> dict[str, object]:
        """Return a run's status projection."""
        response = await self._client.get(f"/runs/{run_id}", headers=self._headers)
        response.raise_for_status()
        return dict(response.json())

    async def cancel_run(self, run_id: UUID) -> None:
        """Request cancellation of a run."""
        response = await self._client.post(f"/runs/{run_id}/cancel", headers=self._headers)
        response.raise_for_status()

    async def replay(self, run_id: UUID, *, after_seq: int = 0) -> list[dict[str, object]]:
        """Return a run's events after ``after_seq`` from the log (not live)."""
        response = await self._client.get(
            f"/runs/{run_id}/replay", params={"after_seq": after_seq}, headers=self._headers
        )
        response.raise_for_status()
        return list(response.json())

    async def subscribe(
        self, run_id: UUID, *, after_seq: int = 0
    ) -> AsyncIterator[dict[str, object]]:
        """Yield a run's events live over SSE until it reaches a terminal state."""
        async with self._client.stream(
            "GET",
            f"/runs/{run_id}/events",
            params={"after_seq": after_seq},
            headers=self._headers,
        ) as response:
            response.raise_for_status()
            async for line in response.aiter_lines():
                if line.startswith("data: "):
                    yield json.loads(line[6:])

    async def aclose(self) -> None:
        await self._client.aclose()

    async def __aenter__(self) -> AgentRuntimeClient:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        await self.aclose()
