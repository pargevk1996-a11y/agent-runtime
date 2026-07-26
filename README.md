<div align="center">

# ⚙️ agent-runtime

**A durable-execution engine for LLM multi-agent systems.**

*Not a wrapper around LangChain or CrewAI — the lower-level runtime such frameworks would sit on top of.*
*Closer in spirit to Temporal / Cadence than to an agent library: a workflow core that happens to schedule LLM work.*

<br/>

![Python](https://img.shields.io/badge/python-3.12-3776AB?style=for-the-badge&logo=python&logoColor=white)
![Typing](https://img.shields.io/badge/mypy-strict-2EA44F?style=for-the-badge&logo=python&logoColor=white)
![Lint](https://img.shields.io/badge/ruff-checked-261230?style=for-the-badge&logo=ruff&logoColor=white)
![Tests](https://img.shields.io/badge/pytest%20+%20hypothesis-passing-0A9EDC?style=for-the-badge&logo=pytest&logoColor=white)
![Status](https://img.shields.io/badge/status-pre--alpha-FF8800?style=for-the-badge)
![Roadmap](https://img.shields.io/badge/roadmap-8%20of%2010-7B3FE4?style=for-the-badge)

</div>

---

## Table of contents

- [Why this exists](#why-this-exists)
- [Capabilities](#capabilities)
- [Architecture at a glance](#architecture-at-a-glance)
- [The event log](#the-event-log)
- [Repository layout](#repository-layout)
- [Getting started](#getting-started)
- [Roadmap](#roadmap)

---

## Why this exists

Real agents run for minutes to hours, crash mid-execution, call external tools
with side effects, burn tokens that cost money, and must leave an audit trail a
human can trust. `agent-runtime` treats those as **first-class engineering
problems**, not afterthoughts:

> Kill the process at any point, restart it, and the run resumes from its last
> committed event — with idempotent tool dispatch so side effects don't double-fire.

---

## Capabilities

| | |
|---|---|
| 🧱 **Durable execution** | Every run is an event-sourced state machine in PostgreSQL. Resume from the last committed event, zero data loss. |
| 🕸️ **DAG-based planning** | Agents emit an append-only task graph the runtime schedules — typed I/O, dependencies, retry policies, per-node cost budgets, bounded self-reflection cycles. |
| 🔌 **Tools over MCP** | External capabilities run as separate MCP tool processes. No tool logic in the agent process. |
| 🛡️ **Sandboxed code exec** | Generated code runs in a swappable isolate — light subprocess backend for dev/CI, heavy isolate in production. Never `exec()` in-process. |
| 🌊 **Streaming + backpressure** | Subscribers get typed events over Redis Streams; a slow client never stalls the agent. |
| 💰 **Cost & token accounting** | Every LLM and tool call records tokens, latency, and dollar cost per run / agent / tenant, with budget enforcement. |
| 🔭 **Full observability** | OpenTelemetry spans on every event and call; every run reconstructible from its event log alone. |

---

## Architecture at a glance

```mermaid
flowchart LR
    SDK["SDK / HTTP client"]

    subgraph control["Control plane · process"]
        API["FastAPI<br/>create · subscribe · cancel · replay"]
    end

    subgraph state["Durable state"]
        PG[("PostgreSQL<br/>event store")]
        RS[("Redis Streams<br/>event fan-out")]
    end

    subgraph worker["Worker · process"]
        SCH["Scheduler / DAG executor"]
        CKP["Checkpoint manager"]
    end

    subgraph mcp["MCP tool processes"]
        WF["web_fetch"]
        SQ["sql_query"]
        CE["code_exec"]
    end

    ISO[["Sandboxed isolate"]]

    SDK -- "create / subscribe / cancel" --> API
    API -- "append + read" --> PG
    RS -- "fan-out to subscribers" --> API
    SCH -- "append / replay" --> PG
    CKP -. "snapshot cache" .-> PG
    SCH -- "publish events" --> RS
    SCH -- "MCP calls (idempotent)" --> WF & SQ & CE
    CE --> ISO
```

The **control plane** and the **worker** are separate processes that share
nothing but PostgreSQL and Redis — so they scale and fail independently.

---

## The event log

The log is the single source of truth. Every event is an immutable **envelope**
with a frozen shape; only the `payload` varies by type and grows per phase.

| Field | Meaning |
|---|---|
| `event_id` · `run_id` · `tenant_id` | UUIDv7 identity, ownership, tenant (RLS boundary) |
| `seq` | per-run monotonic order — the **only** authority on ordering |
| `event_type` · `payload_version` | discriminator + schema version for upcasting on read |
| `payload` | typed body (open union, JSONB at rest, Pydantic is the schema authority) |
| `occurred_at` · `recorded_at` | when it happened / when it was persisted |
| `causation_id` · `correlation_id` | audit chain and originating-request grouping |

**Partitioning & retention.** A run's partition key is the *month of its
`run_id`* (UUIDv7 embeds creation time), so all of a run's events live in one
monthly partition and `(run_id, seq)` stays unique. Retention is a partition
`DROP`. **Tenant isolation** is enforced in the database via Row-Level Security,
not application checks. **Schema evolution** never rewrites rows: old payloads
are upcast `vN → vN+1` in memory on read.

```mermaid
sequenceDiagram
    participant W as Worker
    participant S as EventStore
    participant DB as PostgreSQL
    W->>S: append(run, after_seq = N, payload)
    S->>DB: INSERT seq = N+1  (UNIQUE run_id, seq)
    alt sequence already taken
        DB-->>S: unique violation
        S-->>W: ConcurrencyError (retryable)
    else committed
        DB-->>S: ok
        S-->>W: Envelope(seq = N+1)
    end
    Note over W,DB: 💥 process killed
    W->>S: read(run) after restart
    S->>DB: SELECT … ORDER BY seq
    DB-->>S: rows (JSONB)
    S-->>W: replay — each payload upcast to current version
```

---

## Repository layout

| Package    | Role                                                          |
|------------|--------------------------------------------------------------|
| `runtime/` | Core engine: event store, scheduler, checkpoints, streaming  |
| `api/`     | FastAPI control plane (create · subscribe · cancel · replay)  |
| `agents/`  | Reference agents (executor · critic · verifier · planner)     |
| `tools/`   | Reference MCP tool servers (web_fetch · code_exec · sql_query)|
| `sdk/`     | Thin Python client SDK                                        |
| `bench/`   | Reproducible benchmarks                                       |
| `docs/`    | Architecture decisions (ADR), sequence diagrams, schemas     |

Managed as a [uv](https://docs.astral.sh/uv/) workspace — one member package per deliverable.

---

## Getting started

Requires **[uv](https://docs.astral.sh/uv/)** and **Docker**.

```sh
make install          # sync workspace + dev tools
cp .env.example .env  # then edit as needed
make up               # start Postgres, Redis, OTEL, Prometheus
make lint typecheck   # ruff + strict mypy
make test             # unit tests
make test-integration # integration tests (real infra via testcontainers)
```

Run `make help` for the full list of targets.

---

## Roadmap

Built in strict phase order — no phase begins before the previous one lands.

- [x] **1 · Skeleton** — uv workspace, tooling, CI shape
- [x] **2 · Event store** — envelope, registry/upcasting, partitioned schema, RLS
  - [x] Frozen envelope, payload registry, in-memory upcasting
  - [x] Partitioned schema, RLS tenant isolation, migration runner
  - [x] `EventStore` append / read with optimistic concurrency + property tests
- [x] **3 · Run state machine** + checkpoint manager
  - [x] Run lifecycle events, state machine, pure event fold
  - [x] `runs` projection, lease + fencing token, snapshot side-table
  - [x] Crash recovery — snapshot + tail replay, verified from a fresh process
- [x] **4 · Scheduler** + DAG executor
  - [x] DAG model, events, pure fold with ready-set & cycle checks
  - [x] Scheduler drive-loop: bounded-parallel, retry, fail-fast, crash recovery
  - [x] Dynamic graph expansion, bounded reflection cycles, cooperative cancellation
- [x] **5 · LLM provider interface** + cost accounting
  - [x] Provider-agnostic interface, fake provider, Decimal pricing
  - [x] Cost ledger (per run/node/tenant) with RLS isolation
  - [x] Metered gateway with node budget enforcement
- [x] **6 · MCP tool client** + sandboxed code executor
  - [x] Durable idempotent dispatch (intent-first, crash-safe recovery)
  - [x] MCP stdio transport + reference tool servers
  - [x] Subprocess isolate (rlimits, timeout, no-net) + code_exec
  - [x] web_fetch egress allowlist + read-only sql_query
- [x] **7 · Streaming bus** + FastAPI control plane
  - [x] Redis Streams event bus with bounded fan-out
  - [x] Control plane: create / status / cancel / replay + SSE stream
  - [x] Thin Python SDK client
- [x] **8 · Reference agents** (executor / critic / verifier)
  - [x] CEV composition on the public API (reflection-driven retry)
  - [x] Run worker: lease, plan, and drive runs to completion
- [ ] **9 · Observability** (OTEL + Prometheus + dashboards) — 🚧 *next*
- [ ] **10 · Benchmarks** + docs + ADRs

<div align="center">
<sub>Architecture decisions are recorded under <code>docs/adr/</code>.</sub>
</div>
