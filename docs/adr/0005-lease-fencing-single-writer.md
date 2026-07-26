# ADR 0005: Per-run lease with a fencing token for single-writer safety

- **Status**: Accepted
- **Date**: 2026-07-25

## Context

A run must have exactly one writer at a time, even across a network partition
where a resumed worker believes it still owns the run while another has taken
over. Optimistic per-run sequence numbers stop interleaving but not a stale
worker.

## Decision

Each run row carries a lease (`lease_owner`, `lease_expires_at`) and a monotonic
`fencing_token`. Acquiring a free or expired lease bumps the token. Every append
goes through `RunStore` under a `FOR UPDATE` row lock and verifies its token; a
stale token is rejected with `StaleLeaseError`. Lease timing uses the database
clock (`now()`), never an app clock, so all workers compare against one clock.

## Consequences

- Two workers can race to claim a pending run; only one wins the lease, so
  discovery of pending work can be non-atomic.
- The lease holder is the sole writer, which lets the scheduler read the next
  sequence number under the lock and run nodes in parallel safely.
- A worker that lost its lease stops with a terminal error rather than corrupting
  the log.
