# ADR 0008: Streaming via Redis dual-write with log backfill

- **Status**: Accepted
- **Date**: 2026-07-26

## Context

Clients subscribe to a run and receive typed events live, but a slow client must
never stall the agent (backpressure), and no event may be lost.

## Decision

After an append commits to the log, publish the envelopes to a bounded Redis
Stream (`run:{id}:events`, trimmed by `MAXLEN`) — a best-effort, derived buffer.
A subscriber first **backfills from the log** (`EventStore.read` after its last
seen `seq`), then live-tails Redis for newer events, filtering by `seq` at the
handoff. Publishing never blocks on the client.

## Consequences

- A slow client that falls behind the bounded window loses nothing: it reads the
  gap from the log. The agent is never throttled by subscribers.
- Redis is best-effort — a publish failure is logged, not fatal, because the log
  is the source of truth.
- Chosen over a CDC relay for simplicity; the dual-write's small inconsistency
  window is covered by log backfill.
