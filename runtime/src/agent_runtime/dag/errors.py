"""Errors raised while building or folding the task DAG. All terminal."""

from __future__ import annotations

from agent_runtime.errors import TerminalError


class UnknownNodeError(TerminalError):
    """An event references a node id that has not been added."""


class DuplicateNodeError(TerminalError):
    """A node id was added more than once."""


class CycleError(TerminalError):
    """A dependency edge would introduce a cycle in the dependency graph."""


class MaxReflectionDepthError(TerminalError):
    """A reflection would exceed the configured maximum reflection depth."""
