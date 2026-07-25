"""Run lifecycle: domain events, state machine, projection, and checkpoints.

A run is an event-sourced state machine. This package defines the run's domain
events, the pure fold that projects an event log into a :class:`RunState`, the
``runs`` projection with lease/fencing for single-writer safety, and the
checkpoint manager that snapshots state for fast recovery.
"""
