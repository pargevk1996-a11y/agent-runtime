"""OpenTelemetry tracing setup and access.

``get_tracer`` returns a tracer from whatever provider is configured (a no-op one
until :func:`configure_tracing` installs an SDK). Applications call
``configure_tracing`` at startup with a real exporter; tests pass an in-memory
one to assert on emitted spans.
"""

from __future__ import annotations

from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor, SpanExporter
from opentelemetry.trace import Tracer


def get_tracer(name: str) -> Tracer:
    """Return a tracer for ``name`` from the configured provider."""
    return trace.get_tracer(name)


def configure_tracing(exporter: SpanExporter | None = None) -> TracerProvider:
    """Install an SDK tracer provider, optionally exporting to ``exporter``.

    The global provider can only be set once per process; a second call is
    ignored by OpenTelemetry. Returns the provider that was installed.
    """
    provider = TracerProvider()
    if exporter is not None:
        provider.add_span_processor(SimpleSpanProcessor(exporter))
    trace.set_tracer_provider(provider)
    return provider
