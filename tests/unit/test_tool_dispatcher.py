"""Unit tests for durable, idempotent tool dispatch and its recovery paths."""

from __future__ import annotations

from collections.abc import Sequence
from datetime import UTC, datetime

import pytest

from agent_runtime.events.envelope import Envelope, EventPayload
from agent_runtime.ids import new_event_id, new_node_id, new_run_id, new_tenant_id
from agent_runtime.tools.dispatcher import ToolDispatcher
from agent_runtime.tools.errors import ToolError, ToolIndeterminateError
from agent_runtime.tools.events import (
    ToolCallCompleted,
    ToolCallFailed,
    ToolCallRequested,
)
from agent_runtime.tools.model import IdempotencyClass, ToolResult, ToolSpec, idempotency_key

_TENANT = new_tenant_id()
_RUN = new_run_id()
_NODE = new_node_id()
_NOW = datetime(2020, 1, 1, tzinfo=UTC)


class _FakeJournal:
    def __init__(self) -> None:
        self._events: list[Envelope] = []

    async def append(self, payloads: Sequence[EventPayload]) -> list[Envelope]:
        appended: list[Envelope] = []
        for payload in payloads:
            envelope = Envelope(
                event_id=new_event_id(),
                tenant_id=_TENANT,
                run_id=_RUN,
                seq=len(self._events) + 1,
                event_type="x",
                payload_version=1,
                payload=payload,
                occurred_at=_NOW,
                recorded_at=_NOW,
            )
            self._events.append(envelope)
            appended.append(envelope)
        return appended

    async def read(self) -> list[Envelope]:
        return list(self._events)


class _FakeTransport:
    def __init__(self, *, fail: bool = False) -> None:
        self.invocations: list[str] = []
        self._fail = fail

    async def invoke(
        self, tool: str, args: dict[str, object], *, idempotency_key: str
    ) -> ToolResult:
        self.invocations.append(idempotency_key)
        if self._fail:
            raise ToolError("boom")
        return ToolResult(output={"tool": tool})


def _dispatcher(
    journal: _FakeJournal, transport: _FakeTransport, idempotency: IdempotencyClass
) -> ToolDispatcher:
    specs = {"t": ToolSpec(name="t", idempotency=idempotency)}
    return ToolDispatcher(journal, transport, specs, run_id=_RUN, node_id=_NODE, attempt=1)


def _key(call_index: int = 0) -> str:
    return idempotency_key(_RUN, _NODE, 1, call_index)


async def test_fresh_call_records_intent_then_result() -> None:
    journal, transport = _FakeJournal(), _FakeTransport()
    result = await _dispatcher(journal, transport, IdempotencyClass.IDEMPOTENT).call("t", {})

    assert result.output == {"tool": "t"}
    assert transport.invocations == [_key()]
    payload_types = [type(e.payload) for e in await journal.read()]
    assert payload_types == [ToolCallRequested, ToolCallCompleted]


async def test_successive_calls_use_distinct_keys() -> None:
    journal, transport = _FakeJournal(), _FakeTransport()
    dispatcher = _dispatcher(journal, transport, IdempotencyClass.IDEMPOTENT)
    await dispatcher.call("t", {})
    await dispatcher.call("t", {})
    assert transport.invocations == [_key(0), _key(1)]


async def test_completed_call_is_reused_without_reinvoking() -> None:
    journal, transport = _FakeJournal(), _FakeTransport()
    await journal.append([ToolCallCompleted(idempotency_key=_key(), result={"cached": True})])

    result = await _dispatcher(journal, transport, IdempotencyClass.NON_IDEMPOTENT).call("t", {})
    assert result.output == {"cached": True}
    assert transport.invocations == []  # never re-invoked


async def test_in_flight_non_idempotent_is_indeterminate() -> None:
    journal, transport = _FakeJournal(), _FakeTransport()
    await journal.append(
        [ToolCallRequested(node_id=_NODE, tool="t", args={}, idempotency_key=_key())]
    )

    with pytest.raises(ToolIndeterminateError):
        await _dispatcher(journal, transport, IdempotencyClass.NON_IDEMPOTENT).call("t", {})
    assert transport.invocations == []


async def test_in_flight_idempotent_is_redispatched() -> None:
    journal, transport = _FakeJournal(), _FakeTransport()
    await journal.append(
        [ToolCallRequested(node_id=_NODE, tool="t", args={}, idempotency_key=_key())]
    )

    result = await _dispatcher(journal, transport, IdempotencyClass.IDEMPOTENT).call("t", {})
    assert result.output == {"tool": "t"}
    assert transport.invocations == [_key()]


async def test_previously_failed_call_replays_error() -> None:
    journal, transport = _FakeJournal(), _FakeTransport()
    await journal.append(
        [ToolCallFailed(idempotency_key=_key(), error_class="ToolError", message="boom")]
    )

    with pytest.raises(ToolError):
        await _dispatcher(journal, transport, IdempotencyClass.IDEMPOTENT).call("t", {})
    assert transport.invocations == []


async def test_transport_failure_records_failed_event() -> None:
    journal, transport = _FakeJournal(), _FakeTransport(fail=True)

    with pytest.raises(ToolError):
        await _dispatcher(journal, transport, IdempotencyClass.IDEMPOTENT).call("t", {})
    payload_types = [type(e.payload) for e in await journal.read()]
    assert payload_types == [ToolCallRequested, ToolCallFailed]
