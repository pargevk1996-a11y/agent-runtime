# ADR 0003: Frozen event envelope with in-memory upcasting

- **Status**: Accepted
- **Date**: 2026-07-22

## Context

The event schema is determined by every phase, but the event store is built
early. Freezing the full schema up front is impossible; silently mutating it
later is forbidden. We need to evolve payloads without rewriting history.

## Decision

Freeze the **envelope** (identity, ordering, tenancy, causation, timestamps,
`event_type`, `payload_version`) forever. Treat the payload as an open
discriminated union stored as JSONB. Each phase registers new payload types.
Old rows are migrated **in memory on read** by a chain of upcaster functions
(`vN -> vN+1`); rows on disk are never rewritten.

## Consequences

- Adding an event type is an additive change plus a test, not a migration.
- The Pydantic model is the only schema authority (JSONB is not validated by the
  DB), so a bad deploy could write an unreadable event — mitigated by validation
  on write and a replay-all test.
- A single ordered append/replay path is possible (vs a table per event type).
