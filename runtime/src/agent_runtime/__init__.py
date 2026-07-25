"""Core durable-execution engine.

Houses the event store, run state machine, checkpoint manager, DAG scheduler,
and streaming bus. This is the lowest layer of agent-runtime; every other
package depends (directly or indirectly) on the primitives defined here.
"""

__version__ = "0.1.0"
