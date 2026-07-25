"""Unit and property tests for the DAG model, fold, and ready-set."""

from __future__ import annotations

from collections.abc import Sequence
from datetime import UTC, datetime

import pytest
from hypothesis import given
from hypothesis import strategies as st

from agent_runtime.dag.errors import CycleError, DuplicateNodeError, UnknownNodeError
from agent_runtime.dag.events import (
    EDGE_ADDED,
    NODE_ADDED,
    NODE_FAILED,
    NODE_STARTED,
    NODE_SUCCEEDED,
    EdgeAdded,
    NodeAdded,
    NodeFailed,
    NodeStarted,
    NodeSucceeded,
)
from agent_runtime.dag.model import EdgeType, NodeBudget, NodeRole, NodeStatus, RetryPolicy
from agent_runtime.dag.state import DagOutcome, fold_dag
from agent_runtime.events.envelope import Envelope, EventPayload
from agent_runtime.ids import NodeId, new_event_id, new_node_id, new_run_id, new_tenant_id
from agent_runtime.runs.events import RUN_CREATED, RunCreated

_TENANT = new_tenant_id()
_RUN = new_run_id()
_T0 = datetime(2020, 1, 1, tzinfo=UTC)


def _env(seq: int, payload: EventPayload, event_type: str) -> Envelope:
    return Envelope(
        event_id=new_event_id(),
        tenant_id=_TENANT,
        run_id=_RUN,
        seq=seq,
        event_type=event_type,
        payload_version=1,
        payload=payload,
        occurred_at=_T0,
        recorded_at=_T0,
    )


def _added(seq: int, node_id: NodeId, deps: Sequence[NodeId] = ()) -> Envelope:
    payload = NodeAdded(
        node_id=node_id,
        role=NodeRole.TASK,
        dependencies=tuple(deps),
        retry_policy=RetryPolicy(),
        budget=NodeBudget(),
    )
    return _env(seq, payload, NODE_ADDED)


def _started(seq: int, node_id: NodeId, attempt: int = 1) -> Envelope:
    return _env(seq, NodeStarted(node_id=node_id, attempt=attempt), NODE_STARTED)


def _succeeded(seq: int, node_id: NodeId) -> Envelope:
    return _env(seq, NodeSucceeded(node_id=node_id, output={}), NODE_SUCCEEDED)


def _failed(seq: int, node_id: NodeId, attempt: int = 1) -> Envelope:
    payload = NodeFailed(node_id=node_id, attempt=attempt, error_class="X", message="boom")
    return _env(seq, payload, NODE_FAILED)


def _edge(seq: int, source: NodeId, target: NodeId, edge_type: EdgeType) -> Envelope:
    return _env(seq, EdgeAdded(from_node=source, to_node=target, edge_type=edge_type), EDGE_ADDED)


def test_single_node_is_ready() -> None:
    a = new_node_id()
    state = fold_dag([_added(1, a)])
    assert [n.node_id for n in state.ready_set()] == [a]
    assert state.nodes[a].status is NodeStatus.PENDING


def test_dependent_node_ready_only_after_dependency_succeeds() -> None:
    a, b = new_node_id(), new_node_id()
    events = [_added(1, a), _added(2, b, deps=[a])]
    assert {n.node_id for n in fold_dag(events).ready_set()} == {a}

    events += [_started(3, a), _succeeded(4, a)]
    assert {n.node_id for n in fold_dag(events).ready_set()} == {b}


def test_unknown_dependency_rejected() -> None:
    a, b = new_node_id(), new_node_id()
    with pytest.raises(UnknownNodeError):
        fold_dag([_added(1, b, deps=[a])])


def test_duplicate_node_rejected() -> None:
    a = new_node_id()
    with pytest.raises(DuplicateNodeError):
        fold_dag([_added(1, a), _added(2, a)])


def test_dependency_edge_creating_cycle_rejected() -> None:
    a, b = new_node_id(), new_node_id()
    events = [_added(1, a), _added(2, b, deps=[a]), _edge(3, b, a, EdgeType.DEPENDENCY)]
    with pytest.raises(CycleError):
        fold_dag(events)


def test_reflection_edge_cycle_allowed() -> None:
    a, b = new_node_id(), new_node_id()
    events = [_added(1, a), _added(2, b, deps=[a]), _edge(3, b, a, EdgeType.REFLECTION)]
    state = fold_dag(events)
    assert len(state.edges) == 2


def test_outcome_running_succeeded_failed() -> None:
    a = new_node_id()
    running = fold_dag([_added(1, a)])
    succeeded = fold_dag([_added(1, a), _started(2, a), _succeeded(3, a)])
    failed = fold_dag([_added(1, a), _started(2, a), _failed(3, a)])
    assert running.outcome() is DagOutcome.RUNNING
    assert succeeded.outcome() is DagOutcome.SUCCEEDED
    assert failed.outcome() is DagOutcome.FAILED


def test_fold_dag_ignores_run_events() -> None:
    a = new_node_id()
    events = [_env(1, RunCreated(input={}), RUN_CREATED), _added(2, a)]
    assert set(fold_dag(events).nodes) == {a}


def test_fold_is_deterministic() -> None:
    a, b = new_node_id(), new_node_id()
    events = [_added(1, a), _added(2, b, deps=[a])]
    assert fold_dag(events) == fold_dag(events)


@given(n=st.integers(min_value=1, max_value=6), data=st.data())
def test_ready_nodes_have_all_dependencies_succeeded(n: int, data: st.DataObject) -> None:
    node_ids = [new_node_id() for _ in range(n)]
    events: list[Envelope] = []
    seq = 0
    for index, node_id in enumerate(node_ids):
        earlier = node_ids[:index]
        deps = data.draw(st.lists(st.sampled_from(earlier), unique=True)) if earlier else []
        seq += 1
        events.append(_added(seq, node_id, deps=deps))

    succeed_count = data.draw(st.integers(min_value=0, max_value=n))
    for node_id in node_ids[:succeed_count]:
        seq += 1
        events.append(_started(seq, node_id))
        seq += 1
        events.append(_succeeded(seq, node_id))

    state = fold_dag(events)
    for node in state.ready_set():
        assert node.status is NodeStatus.PENDING
        for dep in node.dependencies:
            assert state.nodes[dep].status is NodeStatus.SUCCEEDED
