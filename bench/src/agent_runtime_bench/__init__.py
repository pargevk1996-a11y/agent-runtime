"""Reproducible benchmarks.

Measures throughput, checkpoint recovery latency, and cost-per-task across three
reference workloads. Drives the system through the SDK/API like a real client so
numbers reflect the whole stack, not isolated internals.
"""

__version__ = "0.1.0"
