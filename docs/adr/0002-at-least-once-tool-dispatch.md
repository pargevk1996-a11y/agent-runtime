# ADR 0002: At-least-once tool dispatch, not exactly-once

- **Status**: Accepted
- **Date**: 2026-07-22

## Context

The brief asked for "no duplicate side effects" across crashes. For our own
state that is achievable; for external tools it is not. There is an irreducible
window: the process can die after a request leaves the socket but before the
result is committed, and on recovery we cannot distinguish "never sent" from
"sent, executed, response lost."

## Decision

We deliver **at-least-once** dispatch with a deterministic idempotency key, not
exactly-once. Tools are classified `IDEMPOTENT`, `NON_IDEMPOTENT`, or `UNKNOWN`.
On recovery: a completed call is reused from the log; an in-flight idempotent
call is re-dispatched with the same key; an in-flight non-idempotent/unknown call
is recorded as `tool.indeterminate` and surfaced, never blindly retried.

## Consequences

- Idempotent tools collapse duplicates via the key; the runtime stays correct.
- Non-idempotent side effects have an honest indeterminate outcome a human or
  higher layer must reconcile — we do not pretend otherwise.
- Cancellation is "stop waiting / kill the sandbox," not "retract"; a delivered
  request cannot be undone.
