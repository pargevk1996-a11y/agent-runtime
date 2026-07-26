"""Streaming: fan-out of a run's events to live subscribers via Redis Streams.

The event log is the source of truth; Redis is a bounded, best-effort buffer for
live subscription. Publishing never blocks the agent on slow clients, and a
subscriber that falls behind the bounded window backfills the gap from the log.
"""
