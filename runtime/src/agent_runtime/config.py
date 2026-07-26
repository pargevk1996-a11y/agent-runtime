"""Runtime configuration, loaded from the environment.

All configuration enters the process through :class:`Settings`, a frozen
pydantic-settings model populated from ``AGENT_RUNTIME_``-prefixed environment
variables (see ``.env.example``). Nothing else reads ``os.environ`` directly.

Two PostgreSQL DSNs exist by design: the hard multi-tenant model uses Row-Level
Security, so the ``admin`` role owns the schema and may bypass RLS (migrations),
while the ``app`` role runs the API and worker under RLS and cannot bypass it.
"""

from __future__ import annotations

from functools import lru_cache
from typing import Literal

from pydantic import PostgresDsn, RedisDsn, ValidationError
from pydantic_settings import BaseSettings, SettingsConfigDict

from agent_runtime.errors import ConfigError


class Settings(BaseSettings):
    """Immutable, validated runtime configuration.

    Invariant: instances are frozen — configuration is read once at startup and
    never mutated, so every component sees a consistent view.
    """

    model_config = SettingsConfigDict(
        env_prefix="AGENT_RUNTIME_",
        env_file=".env",
        extra="ignore",
        frozen=True,
    )

    env: Literal["dev", "production"] = "dev"
    db_admin_dsn: PostgresDsn
    db_app_dsn: PostgresDsn
    redis_url: RedisDsn
    otel_endpoint: str | None = None
    log_level: str = "INFO"
    snapshot_interval_events: int = 100
    scheduler_concurrency: int = 8
    max_reflection_depth: int = 3

    @property
    def is_production(self) -> bool:
        """Whether this process runs in the production environment.

        Consulted by the code isolate (Phase 6): the light subprocess backend
        refuses to start when this is true.
        """
        return self.env == "production"


@lru_cache
def get_settings() -> Settings:
    """Load and cache the process configuration.

    A pydantic ``ValidationError`` (missing or malformed env) is wrapped in
    :class:`ConfigError` — terminal, since bad configuration is not retryable.
    """
    try:
        # pydantic-settings populates required fields from the environment, which
        # mypy cannot see; the ignore is scoped to that single call.
        return Settings()  # type: ignore[call-arg]
    except ValidationError as exc:
        raise ConfigError("invalid configuration", context={"errors": exc.errors()}) from exc
