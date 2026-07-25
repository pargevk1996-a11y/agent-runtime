"""Errors raised by the run state machine and coordination layer.

All are terminal: an illegal transition, a lost lease, or a missing run are not
conditions a retry of the identical operation can fix.
"""

from __future__ import annotations

from agent_runtime.errors import TerminalError


class InvalidTransitionError(TerminalError):
    """An event does not form a legal transition from the current run state.

    Includes applying an event before ``RunCreated`` and applying events out of
    sequence order. Terminal: the event log is inconsistent with the state
    machine and cannot be folded further.
    """


class StaleLeaseError(TerminalError):
    """The caller's fencing token is behind the run's current token.

    Raised when a worker that lost its lease (e.g. after a network partition and
    a lease steal) attempts to append. Terminal for that worker: it no longer
    owns the run and must not retry.
    """


class RunNotFoundError(TerminalError):
    """No run exists for the given id under the current tenant context."""
