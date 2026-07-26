# ADR 0004: Hard multi-tenancy via RLS; monthly partitioning by run-month

- **Status**: Accepted
- **Date**: 2026-07-22

## Context

The system serves several independent companies; tenant isolation is a security
boundary, not just an attribution column. Separately, runs can be long and
high-volume, and events must be retained for a bounded time (one year) without
the table growing forever.

## Decision

Enforce isolation in the database with **Row-Level Security**: `tenant_id NOT
NULL` on every table, policies keyed on `current_setting('app.tenant_id')`, and
an app role that cannot bypass RLS (a separate admin role owns the schema).

Partition event/run tables by **month**, using a `partition_key` derived purely
from the run's UUIDv7 creation time. All of a run's rows share one partition, so
`(run_id, seq)` stays unique and retention is dropping whole monthly partitions.

## Consequences

- A caller cannot read another tenant's data even by guessing ids; the API's
  stream endpoint additionally gates ownership through the log before tailing.
- Retention is a partition `DROP`; a run, its snapshots, and its cost rows drop
  together.
- Deriving the partition key from `run_id` avoids a `runs` lookup on the write
  path. Missing partitions fail loudly (no default partition) — ops must
  pre-create ahead.
