# ADR 0006: Intent-before-execution for tool dispatch

- **Status**: Accepted
- **Date**: 2026-07-26

## Context

Tool calls are decisions that must survive crashes and be reconstructible. We
need to know, on recovery, that a call was in flight — not just re-run the whole
node and hope.

## Decision

Record intent first. `tool.requested` (carrying a deterministic idempotency key
derived from run, node, attempt, and call index) is committed **before**
dispatch; exactly one of `tool.completed` / `tool.failed` / `tool.indeterminate`
resolves it. Before invoking, the dispatcher scans the log for that key: a
completed call returns its stored result without re-invoking; a failed/
indeterminate call replays deterministically; a bare request (in flight at a
crash) is re-dispatched only if the tool is idempotent (see ADR-0002).

## Consequences

- Crash recovery reuses completed tool results even for non-idempotent tools —
  the key insight of intent-first logging.
- Recovery of the *same* attempt reuses keys; a fresh retry attempt gets new
  keys and re-does its calls.
- Reading the log per call adds cost on the hot path; acceptable for correctness,
  optimizable later with caching.
