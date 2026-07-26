"""Per-tool network egress policy.

A tool may reach only an explicit allowlist of hosts over http/https; everything
else is denied. Deny-by-default: an empty allowlist blocks all egress.
"""

from __future__ import annotations

import os
from collections.abc import Iterable
from urllib.parse import urlparse

_ALLOWED_SCHEMES = ("http", "https")


class EgressDeniedError(Exception):
    """A tool attempted a destination not permitted by its egress policy."""


class EgressPolicy:
    """Checks outbound URLs against an allowlist of hosts."""

    def __init__(self, allowed_hosts: Iterable[str]) -> None:
        self._allowed = frozenset(allowed_hosts)

    def check(self, url: str) -> None:
        """Raise :class:`EgressDeniedError` if ``url`` is not permitted."""
        parsed = urlparse(url)
        if parsed.scheme not in _ALLOWED_SCHEMES:
            raise EgressDeniedError(f"scheme {parsed.scheme!r} is not allowed")
        if parsed.hostname is None or parsed.hostname not in self._allowed:
            raise EgressDeniedError(f"host {parsed.hostname!r} is not on the allowlist")

    @classmethod
    def from_env(cls, var: str = "AR_WEB_FETCH_ALLOWLIST") -> EgressPolicy:
        """Build a policy from a comma-separated host list in ``var``."""
        raw = os.environ.get(var, "")
        return cls(host.strip() for host in raw.split(",") if host.strip())
