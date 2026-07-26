"""ASGI entrypoint for the control plane.

    uvicorn agent_runtime_api.asgi:app

Builds the production app from configuration (DSN + Redis URL) read from the
``AGENT_RUNTIME_*`` environment at import time.
"""

from __future__ import annotations

from agent_runtime.config import get_settings
from agent_runtime_api.app import create_app

_settings = get_settings()
app = create_app(str(_settings.db_app_dsn), str(_settings.redis_url))
