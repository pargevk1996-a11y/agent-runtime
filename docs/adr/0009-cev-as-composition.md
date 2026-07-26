# ADR 0009: Critic-executor-verifier as a composition, not an engine primitive

- **Status**: Accepted
- **Date**: 2026-07-26

## Context

The brief asked for CEV as a "first-class primitive." But the project's
positioning is that frameworks sit on top of this runtime; baking one agent
pattern into the engine is exactly what a framework does.

## Decision

Make the *mechanisms* first-class in the engine — node roles as scheduler-visible
metadata, dynamic graph expansion, bounded reflection, typed constraints, and the
retryable/terminal/indeterminate error taxonomy — and ship CEV in `agents/` as a
supported composition built only on the public API. A rejecting critic spawns a
fresh executor+critic one reflection level deeper (carrying feedback); the
scheduler enforces the depth bound; a passing critic spawns the verifier.

## Consequences

- Users can build other patterns without waiting for the engine to grow.
- That CEV composes with no engine change is the proof the mechanisms are
  first-class — if it could not, that would signal a missing primitive.
- The engine stays a general durable-execution scheduler, not an agent framework.
