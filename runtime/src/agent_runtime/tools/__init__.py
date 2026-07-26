"""Tool client: durable, idempotent dispatch of external capabilities.

Tools run as separate MCP processes; the runtime is the client. Every call is
recorded in the event log intent-first (requested before dispatched) with a
deterministic idempotency key, so a crash mid-call resumes correctly: a completed
call is reused, and an in-flight non-idempotent call is surfaced as indeterminate
rather than blindly retried.
"""
