"""Request and response models for the control-plane API."""

from __future__ import annotations

from uuid import UUID

from pydantic import BaseModel, Field


class CreateRunRequest(BaseModel):
    """Body for creating a run."""

    input: dict[str, object] = Field(default_factory=dict)


class CreateRunResponse(BaseModel):
    """Identifies the newly created run."""

    run_id: UUID


class RunStatusResponse(BaseModel):
    """A run's current status projection."""

    run_id: UUID
    status: str
    last_seq: int
