"""Typed exception hierarchy for the runtime.

Every error the runtime raises is a subclass of :class:`AgentRuntimeError`, and
every *concrete* error inherits from exactly one of three recovery categories:

* :class:`RetryableError`     — transient; retrying the same operation may succeed.
* :class:`TerminalError`      — retrying will not help; the operation must fail.
* :class:`IndeterminateError` — we cannot know whether a side effect occurred
  (the exactly-once window on external tool dispatch). It must NOT be blindly
  retried; recovery logic decides what to do.

This split is load-bearing: the Phase 4 retry policy and the Phase 6 recovery
policy both key their behaviour off which category an error belongs to. Concrete
leaf errors are added by the phase that first needs them; only errors with a
consumer in the current phase live here.
"""

from __future__ import annotations

from collections.abc import Mapping


class AgentRuntimeError(Exception):
    """Root of all runtime errors.

    Invariant: this base is never raised directly. Callers raise a concrete
    subclass of one of the three recovery categories below, so that any handler
    can classify an error purely by ``isinstance`` against the category.

    ``context`` carries structured, log-safe key/values describing where and why
    the error arose; it is copied so the caller's mapping cannot mutate it.
    """

    def __init__(self, message: str, *, context: Mapping[str, object] | None = None) -> None:
        super().__init__(message)
        self.message = message
        self.context: dict[str, object] = dict(context or {})

    def __repr__(self) -> str:
        return f"{type(self).__name__}({self.message!r}, context={self.context!r})"


class RetryableError(AgentRuntimeError):
    """Transient failure; the same operation may succeed if retried.

    Examples (added in later phases): network blips, lock contention, provider
    rate limits (HTTP 429) and 5xx responses.
    """


class TerminalError(AgentRuntimeError):
    """Permanent failure; retrying the identical operation cannot succeed.

    Examples (added in later phases): input validation failures, budget
    exceeded, permission denied.
    """


class IndeterminateError(AgentRuntimeError):
    """The outcome of an external side effect is unknown.

    Raised when the process could not observe whether a dispatched effect (e.g.
    a non-idempotent tool call) actually happened. Blind retry is unsafe; the
    recovery policy must reconcile the state, typically by surfacing an explicit
    indeterminate event rather than re-dispatching.
    """


class ConfigError(TerminalError):
    """Invalid or missing configuration.

    Terminal because no amount of retrying repairs a bad configuration — the
    operator must fix the environment. This is the first concrete error with a
    consumer (``config.get_settings``) and demonstrates the category split.
    """
