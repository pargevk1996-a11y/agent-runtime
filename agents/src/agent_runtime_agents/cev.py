"""The critic-executor-verifier pattern, as a composition on the public API.

CEV is *not* baked into the engine — it is built here from the scheduler's public
primitives: node roles, dynamic graph expansion (``NodeResult`` spawns), bounded
reflection, and the typed error taxonomy. An executor proposes, a critic checks
it against typed constraints, and a verifier confirms; a rejecting critic spawns a
fresh executor+critic (with feedback) one reflection level deeper, and the
scheduler enforces the depth bound. That this composes with no engine change is
the proof the runtime's mechanisms are first-class.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass

from agent_runtime.dag.events import NodeAdded
from agent_runtime.dag.executor import NodeContext, NodeResult, SpawnEdge, SpawnNode
from agent_runtime.dag.model import EdgeType, NodeBudget, NodeRole, RetryPolicy
from agent_runtime.errors import TerminalError
from agent_runtime.ids import new_node_id

# A proposer turns feedback (empty on the first attempt, else the critic's
# rejection output) into a proposal. A constraint returns None if satisfied, else
# a violation message. A verifier returns None if the result holds, else why not.
Proposer = Callable[[Mapping[str, object]], dict[str, object]]
Constraint = Callable[[dict[str, object]], str | None]
Verifier = Callable[[dict[str, object]], str | None]


class ConstraintViolationError(TerminalError):
    """A proposal failed a constraint or verification and cannot be salvaged."""


@dataclass(frozen=True)
class CEVConfig:
    """The proposer, constraints, and optional verifier for a CEV task."""

    proposer: Proposer
    constraints: tuple[Constraint, ...]
    verifier: Verifier | None = None


def seed_cev() -> list[NodeAdded]:
    """Build the initial executor and critic nodes for a CEV run (the "planner")."""
    executor_id = new_node_id()
    critic_id = new_node_id()
    executor = NodeAdded(
        node_id=executor_id,
        role=NodeRole.EXECUTOR,
        dependencies=(),
        retry_policy=RetryPolicy(),
        budget=NodeBudget(),
    )
    critic = NodeAdded(
        node_id=critic_id,
        role=NodeRole.CRITIC,
        dependencies=(executor_id,),
        retry_policy=RetryPolicy(),
        budget=NodeBudget(),
    )
    return [executor, critic]


class CEVExecutor:
    """A :class:`NodeExecutor` that runs executor/critic/verifier nodes."""

    def __init__(self, config: CEVConfig) -> None:
        self._config = config

    async def execute(self, ctx: NodeContext) -> NodeResult:
        role = ctx.node.role
        if role is NodeRole.EXECUTOR:
            return self._propose(ctx)
        if role is NodeRole.CRITIC:
            return self._critique(ctx)
        if role is NodeRole.VERIFIER:
            return self._verify(ctx)
        raise ConstraintViolationError("node role is not part of a CEV", context={"role": role})

    def _propose(self, ctx: NodeContext) -> NodeResult:
        feedback = self._single_input(ctx)
        proposal = self._config.proposer(feedback)
        return NodeResult(output={"proposal": proposal})

    def _critique(self, ctx: NodeContext) -> NodeResult:
        proposal = self._proposal(ctx)
        violations = [msg for c in self._config.constraints if (msg := c(proposal)) is not None]
        if not violations:
            verifier = SpawnNode(
                node_id=new_node_id(),
                role=NodeRole.VERIFIER,
                dependencies=(ctx.node.node_id,),
                reflection_depth=ctx.node.reflection_depth,
            )
            return NodeResult(output={"proposal": proposal}, spawn=(verifier,))

        # Reject: spawn a fresh executor+critic one level deeper, carrying the
        # violations back as feedback. The scheduler enforces the depth bound.
        depth = ctx.node.reflection_depth + 1
        executor_id = new_node_id()
        critic_id = new_node_id()
        new_executor = SpawnNode(
            node_id=executor_id,
            role=NodeRole.EXECUTOR,
            dependencies=(ctx.node.node_id,),
            reflection_depth=depth,
        )
        new_critic = SpawnNode(
            node_id=critic_id,
            role=NodeRole.CRITIC,
            dependencies=(executor_id,),
            reflection_depth=depth,
        )
        reflection = SpawnEdge(
            from_node=ctx.node.node_id, to_node=executor_id, edge_type=EdgeType.REFLECTION
        )
        return NodeResult(
            output={"violations": violations, "proposal": proposal},
            spawn=(new_executor, new_critic),
            edges=(reflection,),
        )

    def _verify(self, ctx: NodeContext) -> NodeResult:
        proposal = self._proposal(ctx)
        if self._config.verifier is not None:
            violation = self._config.verifier(proposal)
            if violation is not None:
                raise ConstraintViolationError(
                    "verification failed", context={"violation": violation}
                )
        return NodeResult(output={"result": proposal})

    @staticmethod
    def _single_input(ctx: NodeContext) -> Mapping[str, object]:
        for output in ctx.inputs.values():
            return output
        return {}

    @staticmethod
    def _proposal(ctx: NodeContext) -> dict[str, object]:
        for output in ctx.inputs.values():
            proposal = output.get("proposal")
            if isinstance(proposal, dict):
                return proposal
        return {}
