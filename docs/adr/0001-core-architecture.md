# ADR 0001: Local-first event-driven core

Status: accepted for V0

## Context

Supervisor must survive process crashes and machine restarts, supervise heterogeneous native
and remote workers, and serve both human and machine observers without treating conversational
history as state. Cyber Office already defines a read-only REST/WebSocket contract.

## Decision

1. Python 3.12+, asyncio, FastAPI and SQLite form the V0 runtime.
2. SQLite in WAL mode is canonical. State mutations and their normalized event are committed in the
   same transaction.
3. `events.sequence` is a global monotonic resume cursor. REST pagination and WebSocket replay use
   the same committed sequence.
4. A serialized write lock is used in-process. Cross-process duplicate supervisors are rejected by
   an instance lease before dispatch is enabled.
5. Domain code keeps Node, Worker, Harness, Provider, Model, Session, Task, Event, Usage and
   ResourceState separate. Provider flags and raw formats live only in adapters.
6. Scheduling is a pure deterministic function over one frozen world snapshot. Hard constraints
   run before weighted scoring. Worker ID is the final tie-break. All selected and rejected reasons
   are persisted.
7. Native CLI workers are supervised as child process groups with stdout/stderr capture, deadlines,
   heartbeat timestamps and explicit cancellation escalation. Remote local workers use their
   versioned HTTP protocol.
8. A local-model worker is read-only/non-code by policy. Both scheduler and LocalWorkerAdapter must
   reject a code-writing task.
9. The public V0 API is observation-only and requires a scoped supervisor-issued bearer token when
   non-loopback access is enabled. Tokens are hashed at rest.
10. Worktrees are created only for authorized code tasks in Git repositories and are tracked as
    durable entities. Conflicting branches are never merged automatically.

## Consequences

- SQLite write transactions must remain short, and API projections must not become an alternate
  source of truth.
- WebSocket delivery is at-least-once; clients deduplicate by sequence.
- Missing telemetry requires explicit unavailable metadata instead of null-to-zero coercion.
- Full private Mac-to-PC operation remains gated on an authenticated overlay/proxy configuration;
  no public or ordinary-LAN exposure is an acceptable substitute.
