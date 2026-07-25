"""Structured logging configuration.

The runtime logs structured JSON to stdout via ``structlog`` — never ``print``,
never free-form strings. Structured records are what make runs greppable and
machine-analysable, which the audit-trail requirement depends on.

The processor chain is ordered so that Phase 9 can insert an OpenTelemetry
trace/span-id processor at a single marked point without touching anything else;
OpenTelemetry is deliberately *not* a dependency yet (it belongs to Phase 9).
"""

from __future__ import annotations

import logging
from typing import cast

import structlog
from structlog.typing import FilteringBoundLogger


def configure_logging(level: str = "INFO") -> None:
    """Configure process-wide structured JSON logging. Idempotent-safe to call.

    :param level: minimum level name (e.g. ``"INFO"``); unknown names fall back
        to ``INFO`` rather than raising, so a bad env value degrades gracefully.
    """
    level_no = logging.getLevelNamesMapping().get(level.upper(), logging.INFO)
    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="iso", utc=True),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            # Phase 9: insert an OpenTelemetry trace/span-id processor here.
            structlog.processors.JSONRenderer(),
        ],
        wrapper_class=structlog.make_filtering_bound_logger(level_no),
        logger_factory=structlog.PrintLoggerFactory(),
        cache_logger_on_first_use=True,
    )


def get_logger(name: str) -> FilteringBoundLogger:
    """Return a bound structured logger, configuring defaults on first use.

    The ``cast`` documents structlog's dynamically-typed ``get_logger`` return;
    the configured ``wrapper_class`` guarantees a ``FilteringBoundLogger``.
    """
    if not structlog.is_configured():
        configure_logging()
    return cast(FilteringBoundLogger, structlog.get_logger(name))
