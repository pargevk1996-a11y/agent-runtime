# Event schema reference

_Generated from the event registry by `docs/generate_events.py` — do not edit by hand._

Every event shares the frozen envelope (identity, ordering, tenancy, causation,
timestamps, versioning); only the payload varies. Payloads below are grouped by
prefix: `run.*` lifecycle, `node.*`/`edge.*` DAG, `tool.*` tool calls.

## `edge.added` — v1

Payload model: `EdgeAdded`

- `from_node`: `NodeId`
- `to_node`: `NodeId`
- `edge_type`: `EdgeType`

## `node.added` — v1

Payload model: `NodeAdded`

- `node_id`: `NodeId`
- `role`: `NodeRole`
- `dependencies`: `tuple`
- `retry_policy`: `RetryPolicy`
- `budget`: `NodeBudget`
- `reflection_depth`: `int`

## `node.failed` — v1

Payload model: `NodeFailed`

- `node_id`: `NodeId`
- `attempt`: `int`
- `error_class`: `str`
- `message`: `str`

## `node.skipped` — v1

Payload model: `NodeSkipped`

- `node_id`: `NodeId`
- `reason`: `str`

## `node.started` — v1

Payload model: `NodeStarted`

- `node_id`: `NodeId`
- `attempt`: `int`

## `node.succeeded` — v1

Payload model: `NodeSucceeded`

- `node_id`: `NodeId`
- `output`: `dict`

## `run.cancelled` — v1

Payload model: `RunCancelled`

- `reason`: `str`

## `run.created` — v1

Payload model: `RunCreated`

- `input`: `dict`

## `run.failed` — v1

Payload model: `RunFailed`

- `error_class`: `str`
- `message`: `str`

## `run.started` — v1

Payload model: `RunStarted`

- `worker`: `str`

## `run.succeeded` — v1

Payload model: `RunSucceeded`

- `result`: `dict`

## `tool.completed` — v1

Payload model: `ToolCallCompleted`

- `idempotency_key`: `str`
- `result`: `dict`

## `tool.failed` — v1

Payload model: `ToolCallFailed`

- `idempotency_key`: `str`
- `error_class`: `str`
- `message`: `str`

## `tool.indeterminate` — v1

Payload model: `ToolCallIndeterminate`

- `idempotency_key`: `str`
- `reason`: `str`

## `tool.requested` — v1

Payload model: `ToolCallRequested`

- `node_id`: `NodeId`
- `tool`: `str`
- `args`: `dict`
- `idempotency_key`: `str`
