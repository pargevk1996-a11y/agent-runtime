"""Cost accounting: the LLM-call ledger and spend aggregation.

Every metered LLM call lands in an append-only ledger attributed to tenant, run,
and node. Aggregates over it answer "how much did this run/node/tenant spend",
and back the budget checks the gateway enforces.
"""
