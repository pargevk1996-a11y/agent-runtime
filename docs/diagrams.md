# Sequence diagrams

Key flows through the runtime. Diagrams render natively on GitHub.

## Durable append and crash recovery

```mermaid
sequenceDiagram
    participant W as Worker (lease holder)
    participant RS as RunStore
    participant DB as PostgreSQL
    participant R as Redis
    W->>RS: append_events(lease, payloads)
    RS->>DB: FOR UPDATE runs; verify fencing token
    alt token stale
        RS-->>W: StaleLeaseError
    else token current
        RS->>DB: INSERT events (seq = last+1 …), UPDATE projection
        RS->>R: publish (best-effort)
        RS-->>W: envelopes
    end
    Note over W,DB: 💥 process dies
    W->>RS: load_state on restart
    RS->>DB: latest snapshot + events after it
    RS-->>W: folded RunState (resume)
```

## Durable tool dispatch and recovery

```mermaid
sequenceDiagram
    participant E as Executor
    participant D as ToolDispatcher
    participant J as Journal (log)
    participant T as Tool (MCP process)
    E->>D: call(tool, args)
    D->>J: scan for idempotency key
    alt completed in log
        D-->>E: stored result (no re-invoke)
    else in-flight (requested only), non-idempotent
        D->>J: append tool.indeterminate
        D-->>E: ToolIndeterminateError
    else fresh (or idempotent recovery)
        D->>J: append tool.requested
        D->>T: invoke(args, idempotency_key)
        T-->>D: result
        D->>J: append tool.completed
        D-->>E: result
    end
```

## Live subscription (backfill + tail)

```mermaid
sequenceDiagram
    participant C as Client (SDK)
    participant API as Control plane
    participant DB as PostgreSQL
    participant R as Redis
    C->>API: GET /runs/{id}/events?after_seq=N
    API->>DB: ownership check (RLS) + read events > N
    API-->>C: SSE backfill (data: …)
    loop live
        API->>R: XREAD block (seq > last)
        R-->>API: new entries
        API-->>C: SSE (data: …)
    end
    Note over API,C: closes on run.succeeded/failed/cancelled or disconnect
```

## CEV reflection loop

```mermaid
sequenceDiagram
    participant S as Scheduler
    participant X as CEVExecutor
    S->>X: run executor node
    X-->>S: proposal
    S->>X: run critic node (typed constraints)
    alt constraints pass
        X-->>S: spawn verifier
        S->>X: run verifier
        X-->>S: result → run SUCCEEDED
    else constraints fail
        X-->>S: spawn executor+critic (depth+1, with feedback)
        Note over S,X: scheduler enforces max_reflection_depth;<br/>exceeding → MaxReflectionDepthError → run FAILED
    end
```
