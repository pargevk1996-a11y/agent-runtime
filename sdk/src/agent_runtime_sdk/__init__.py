"""Thin client SDK.

A small HTTP client over the control-plane API for creating runs, subscribing
to event streams, cancelling, and replaying. Deliberately depends on no runtime
internals — it talks to the API over the wire like any external consumer.
"""

__version__ = "0.1.0"
