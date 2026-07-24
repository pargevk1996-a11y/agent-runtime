# agent-runtime

A production-grade orchestration runtime for LLM-based multi-agent systems.

This is **not** a wrapper around LangChain / CrewAI. It is the lower-level engine
such frameworks would sit on top of: a durable-execution core that happens to
schedule LLM work, closer in spirit to Temporal/Cadence than to an agent library.

## What it does

- **Durable execution.** Every run is an event-sourced state machine in Postgres.
  Kill the process at any point and it resumes from the last committed event.
- **DAG-based planning.** Agents emit an append-only task graph the runtime
  schedules — typed I/O, dependencies, retry policies, per-node cost budgets,
  bounded self-reflection cycles.
- **Tools over MCP.** External capabilities run as separate MCP tool processes;
  no tool logic lives in the agent process.
- **Sandboxed code execution** through a swappable isolate (light subprocess
  backend for dev/CI, heavy isolate in production).
- **Streaming with backpressure** to subscribers via Redis Streams.
- **Cost & token accounting** per run / agent / tenant, with budget enforcement.
- **Full observability** — OpenTelemetry spans on every event, LLM and tool call;
  every run reconstructible from its event log alone.

## Repository layout

| Package    | Role                                                        |
|------------|-------------------------------------------------------------|
| `runtime/` | Core engine: event store, scheduler, checkpoints, streaming |
| `api/`     | FastAPI control plane (create run, subscribe, cancel, replay) |
| `agents/`  | Reference agents (executor, critic, verifier, planner)      |
| `tools/`   | Reference MCP tool servers (web_fetch, code_exec, sql_query)|
| `sdk/`     | Thin Python client SDK                                       |
| `bench/`   | Reproducible benchmarks                                      |
| `docs/`    | Architecture decisions (ADR), sequence diagrams, schemas    |

## Development

Requires [uv](https://docs.astral.sh/uv/) and Docker.

```sh
make install          # sync workspace + dev tools
cp .env.example .env  # then edit as needed
make up               # start Postgres, Redis, OTEL, Prometheus
make lint typecheck   # ruff + strict mypy
make test             # unit tests
make test-integration # integration tests (testcontainers)
```

Architecture decisions are recorded under `docs/adr/`.
