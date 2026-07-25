"""Task DAG: the graph of work a run schedules.

Agents don't just loop — they emit an append-only task graph the runtime
schedules. This package defines the DAG's node/edge model, its domain events, the
pure projection from events to :class:`DagState`, the executor contract, and the
scheduler that drives a run's graph to completion.
"""
