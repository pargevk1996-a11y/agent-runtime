# ADR 0007: Cost accounting in a ledger, not the event log

- **Status**: Accepted
- **Date**: 2026-07-26

## Context

Every LLM call must record tokens, latency, and dollar cost attributed to run,
node, and tenant. The obvious place is the event log, but tenant/agent-level cost
questions ("how much did this tenant spend?") would then require scanning many
runs' logs.

## Decision

Record cost in a dedicated append-only `llm_calls` ledger table, indexed by
`(run_id, node_id)` and `tenant_id`, written directly by the LLM gateway. The
LLM *result* still lives in the node's output in the log (so replay is
deterministic); only the derived dollar accounting lives in the ledger.

## Consequences

- Spend aggregates by run, node, or tenant are single SQL queries.
- The scheduler stays LLM-agnostic; cost is an executor-level concern.
- "Reconstructible from the event log alone" still holds for run *state*; cost is
  supplementary observability, not run state.
- Budget enforcement queries the ledger; exceeding a node budget raises
  `BudgetExceededError`, failing the node (and run, fail-fast).
