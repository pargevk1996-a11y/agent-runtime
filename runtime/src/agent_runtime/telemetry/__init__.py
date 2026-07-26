"""Observability: OpenTelemetry tracing and Prometheus metrics.

Core code instruments with the OpenTelemetry *API*, which is a no-op until an SDK
is configured — so there is no hard dependency on a running collector and no cost
when tracing is off. Metrics use an in-process Prometheus registry, always on and
cheap. Every event, LLM call, and tool call is thus observable, and logs carry the
active trace/span ids.
"""
