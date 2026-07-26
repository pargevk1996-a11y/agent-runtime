"""Reference workloads for the benchmarks.

Each builder returns the initial DAG nodes plus the executor that runs them:
a linear chain, a fan-out/fan-in, and a CEV task that reflects once. All are
deterministic so benchmark runs are reproducible.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping

from agent_runtime.dag.events import NodeAdded
from agent_runtime.dag.executor import NodeContext, NodeExecutor, NodeResult
from agent_runtime.dag.model import NodeBudget, NodeRole, RetryPolicy
from agent_runtime.ids import NodeId, new_node_id
from agent_runtime_agents.cev import CEVConfig, CEVExecutor, seed_cev

Workload = Callable[[], "tuple[list[NodeAdded], NodeExecutor]"]


class NoopExecutor:
    """Returns an empty result — measures scheduling overhead, not work."""

    async def execute(self, ctx: NodeContext) -> NodeResult:
        return NodeResult(output={})


def _task(node_id: NodeId, deps: tuple[NodeId, ...] = ()) -> NodeAdded:
    return NodeAdded(
        node_id=node_id,
        role=NodeRole.TASK,
        dependencies=deps,
        retry_policy=RetryPolicy(),
        budget=NodeBudget(),
    )


def linear_chain(length: int) -> tuple[list[NodeAdded], NodeExecutor]:
    """A chain of ``length`` nodes, each depending on the previous."""
    nodes: list[NodeAdded] = []
    previous: NodeId | None = None
    for _ in range(length):
        node_id = new_node_id()
        nodes.append(_task(node_id, (previous,) if previous is not None else ()))
        previous = node_id
    return nodes, NoopExecutor()


def fan_out_in(width: int) -> tuple[list[NodeAdded], NodeExecutor]:
    """A root, ``width`` parallel middle nodes, and a sink joining them."""
    root = new_node_id()
    middles = [new_node_id() for _ in range(width)]
    sink = new_node_id()
    nodes = [_task(root)]
    nodes += [_task(mid, (root,)) for mid in middles]
    nodes.append(_task(sink, tuple(middles)))
    return nodes, NoopExecutor()


_MIN_VALUE = 5


def _at_least_five(proposal: dict[str, object]) -> str | None:
    value = proposal.get("value", 0)
    return None if isinstance(value, int) and value >= _MIN_VALUE else "value must be >= 5"


def cev_task() -> tuple[list[NodeAdded], NodeExecutor]:
    """A CEV task whose first proposal is rejected, then fixed via reflection."""

    def proposer(feedback: Mapping[str, object]) -> dict[str, object]:
        return {"value": 10 if feedback else 0}

    config = CEVConfig(proposer=proposer, constraints=(_at_least_five,))
    return seed_cev(), CEVExecutor(config)
