# ADR 0010: Swappable code isolate — light for dev/CI, heavy for production

- **Status**: Accepted
- **Date**: 2026-07-26

## Context

Generated code must never run in the runtime process. gVisor and Firecracker are
the production options, but neither runs in unprivileged CI (Firecracker needs
KVM; gVisor needs a Docker runtime), so sandbox tests could not run.

## Decision

Define an `Isolate` protocol with swappable backends. The light
`SubprocessIsolate` (POSIX rlimits for memory/CPU, wall-clock timeout with
process-group kill, best-effort network block) serves dev and CI and **refuses to
start when the environment is production**. A heavy isolate (gVisor/Firecracker)
is the production backend; the specific choice is deferred to deployment. Tools
run as separate MCP processes, so no tool logic — including the isolate — lives
in the runtime.

## Consequences

- The test suite is honest about what it verifies: resource limits and timeouts
  are testable unprivileged; the light network block is explicitly *not* a
  security boundary.
- Per-tool network egress is an allowlist (deny-by-default) enforced before any
  network access.
- Production must supply a heavy isolate; the light one will not start there.
