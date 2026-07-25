"""PostgreSQL access layer: connection pool, tenant sessions, and migrations.

Hand-written SQL over asyncpg on the hot path (no ORM). This package owns the
raw database boundary; higher layers (the event store) build on the pool and the
tenant-scoped session helper defined here.
"""
