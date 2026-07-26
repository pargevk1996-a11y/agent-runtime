"""Value types for tool dispatch: idempotency class, tool spec, result, keys."""

from __future__ import annotations

from enum import StrEnum

from pydantic import BaseModel, ConfigDict

from agent_runtime.ids import NodeId, RunId


class IdempotencyClass(StrEnum):
    """How safe a tool is to re-dispatch after an indeterminate crash.

    * IDEMPOTENT     — re-dispatching with the same key is safe.
    * NON_IDEMPOTENT — re-dispatch may double a side effect; never retry blindly.
    * UNKNOWN        — treated as non-idempotent (the safe default).
    """

    IDEMPOTENT = "idempotent"
    NON_IDEMPOTENT = "non_idempotent"
    UNKNOWN = "unknown"


class ToolSpec(BaseModel):
    """Static description of a tool the runtime may call."""

    model_config = ConfigDict(frozen=True)

    name: str
    idempotency: IdempotencyClass = IdempotencyClass.UNKNOWN


class ToolResult(BaseModel):
    """A tool's successful output."""

    model_config = ConfigDict(frozen=True)

    output: dict[str, object]


def idempotency_key(run_id: RunId, node_id: NodeId, attempt: int, call_index: int) -> str:
    """A deterministic key for one tool call.

    Stable across crash recovery of the *same* attempt (so a completed call is
    reused), but distinct for a fresh retry attempt (which re-does its calls).
    """
    return f"{run_id}:{node_id}:{attempt}:{call_index}"
