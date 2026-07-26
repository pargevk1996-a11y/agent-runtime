"""Durable, idempotent tool dispatch.

Bound to one node execution (run, node, attempt), the dispatcher assigns each
call a deterministic idempotency key and records it intent-first. Before invoking
it scans the log for that key so recovery is exact:

* a **completed** call returns its stored result without re-invoking (safe for
  every idempotency class);
* a **failed** or **indeterminate** call replays its outcome deterministically;
* a bare **requested** call (in flight at a crash) is re-dispatched only if the
  tool is idempotent — otherwise it is recorded indeterminate and raised.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass

from agent_runtime.errors import AgentRuntimeError
from agent_runtime.events.envelope import Envelope
from agent_runtime.ids import NodeId, RunId
from agent_runtime.journal import RunJournal
from agent_runtime.telemetry.metrics import record_tool
from agent_runtime.telemetry.tracing import get_tracer
from agent_runtime.tools.errors import ToolError, ToolIndeterminateError
from agent_runtime.tools.events import (
    ToolCallCompleted,
    ToolCallFailed,
    ToolCallIndeterminate,
    ToolCallRequested,
)
from agent_runtime.tools.model import IdempotencyClass, ToolResult, ToolSpec, idempotency_key
from agent_runtime.tools.transport import ToolTransport

_tracer = get_tracer(__name__)


@dataclass
class _Resolution:
    requested: bool = False
    completed: dict[str, object] | None = None
    failed: bool = False
    indeterminate: bool = False


def _scan(events: list[Envelope], key: str) -> _Resolution:
    resolution = _Resolution()
    for event in events:
        payload = event.payload
        if isinstance(payload, ToolCallRequested) and payload.idempotency_key == key:
            resolution.requested = True
        elif isinstance(payload, ToolCallCompleted) and payload.idempotency_key == key:
            resolution.completed = payload.result
        elif isinstance(payload, ToolCallFailed) and payload.idempotency_key == key:
            resolution.failed = True
        elif isinstance(payload, ToolCallIndeterminate) and payload.idempotency_key == key:
            resolution.indeterminate = True
    return resolution


class ToolDispatcher:
    """Dispatches tool calls durably for one node execution."""

    def __init__(
        self,
        journal: RunJournal,
        transport: ToolTransport,
        specs: Mapping[str, ToolSpec],
        *,
        run_id: RunId,
        node_id: NodeId,
        attempt: int,
    ) -> None:
        self._journal = journal
        self._transport = transport
        self._specs = dict(specs)
        self._run_id = run_id
        self._node_id = node_id
        self._attempt = attempt
        self._call_index = 0

    async def call(self, tool: str, args: dict[str, object]) -> ToolResult:
        """Dispatch ``tool`` with ``args``, recording it durably."""
        key = idempotency_key(self._run_id, self._node_id, self._attempt, self._call_index)
        self._call_index += 1

        with _tracer.start_as_current_span("tool.call") as span:
            span.set_attribute("tool", tool)
            span.set_attribute("idempotency_key", key)

            resolution = _scan(await self._journal.read(), key)
            if resolution.completed is not None:
                record_tool("reused")
                return ToolResult(output=resolution.completed)
            if resolution.indeterminate:
                record_tool("indeterminate")
                raise ToolIndeterminateError(
                    "tool call previously indeterminate", context={"key": key}
                )
            if resolution.failed:
                record_tool("failed")
                raise ToolError("tool call previously failed", context={"key": key})
            if resolution.requested:
                return await self._recover(tool, args, key)

            await self._journal.append(
                [
                    ToolCallRequested(
                        node_id=self._node_id, tool=tool, args=args, idempotency_key=key
                    )
                ]
            )
            return await self._invoke(tool, args, key)

    async def _recover(self, tool: str, args: dict[str, object], key: str) -> ToolResult:
        spec = self._specs.get(tool)
        klass = spec.idempotency if spec is not None else IdempotencyClass.UNKNOWN
        if klass is IdempotencyClass.IDEMPOTENT:
            return await self._invoke(tool, args, key)
        record_tool("indeterminate")
        await self._journal.append(
            [
                ToolCallIndeterminate(
                    idempotency_key=key, reason="non-idempotent call in flight at recovery"
                )
            ]
        )
        raise ToolIndeterminateError(
            "non-idempotent tool call was in flight at a crash",
            context={"tool": tool, "key": key},
        )

    async def _invoke(self, tool: str, args: dict[str, object], key: str) -> ToolResult:
        try:
            result = await self._transport.invoke(tool, args, idempotency_key=key)
        except AgentRuntimeError as exc:
            record_tool("failed")
            await self._journal.append(
                [
                    ToolCallFailed(
                        idempotency_key=key,
                        error_class=type(exc).__name__,
                        message=str(exc),
                    )
                ]
            )
            raise
        record_tool("completed")
        await self._journal.append([ToolCallCompleted(idempotency_key=key, result=result.output)])
        return result
