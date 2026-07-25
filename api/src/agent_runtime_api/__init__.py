"""FastAPI control plane.

Exposes the runtime over HTTP: create runs, subscribe to their event streams,
cancel in-flight runs, and replay completed ones. Runs as its own process,
separate from the worker; the two communicate only through Postgres and Redis.
"""

__version__ = "0.1.0"
