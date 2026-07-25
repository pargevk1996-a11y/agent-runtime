"""Event store: the frozen envelope, payload registry, and storage layer.

The event log is the runtime's single source of truth. This package defines the
immutable event *envelope* (identity, ordering, tenancy, causation, time,
versioning), the mechanism for evolving payload schemas via in-memory upcasting
on read, and the append/read API over PostgreSQL.
"""
