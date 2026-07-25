"""Errors raised by the event store.

Each maps onto the recovery category it semantically belongs to (see
``agent_runtime.errors``), so retry and recovery logic can classify by category
alone. Deliberately no shared ``EventStoreError`` base: that would span two
categories and blur the retryable/terminal distinction.
"""

from __future__ import annotations

from agent_runtime.errors import RetryableError, TerminalError


class ConcurrencyError(RetryableError):
    """A concurrent writer already appended at the expected sequence number.

    Retryable: the caller re-reads the latest ``seq`` and retries the append.
    """


class UnknownEventTypeError(TerminalError):
    """No payload type is registered for the given ``event_type``.

    Terminal: the row cannot be decoded until code registering that type is
    deployed; blind retry will not help.
    """


class EventDecodeError(TerminalError):
    """A stored payload could not be upcast and validated into its model.

    Terminal: a missing upcaster, a future ``payload_version``, or data that
    fails validation. Retrying the identical decode cannot succeed.
    """
