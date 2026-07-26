# ADR 0001: Event-sourced durable execution

- **Status**: Accepted
- **Date**: 2026-07-22

## Context

Agents run for minutes to hours and must survive process death: killing the
worker at any point and restarting must resume with no data loss. We need a
representation of run state that is durable, auditable, and reconstructible.

## Decision

Every run is an event-sourced state machine. The append-only event log in
PostgreSQL is the single source of truth; run state is a pure fold over the log.
Snapshots are a derived cache that can be deleted at any time without loss.

## Consequences

- Recovery is re-reading the log (optionally from a snapshot) and continuing.
- A complete audit trail falls out for free — every decision is an event.
- All mutation goes through one append path; projections (`runs`, cost ledger,
  Redis streams) are derived and rebuildable.
- Cost: state is never mutated in place, so all readers fold; snapshots
  (ADR-driven) bound the fold length.
