# Universal Worker protocol

## Boundary

A Worker is a capability-bearing execution peer, not a model name, CLI, node, or provider. The
Supervisor scheduler consumes normalized Worker semantics; adapters contain provider-specific CLI
flags, event shapes, sessions, authentication behavior, and error mappings.

## Generic concepts

Every adapter should expose, where observable: stable Worker identity, node/provider/harness,
models, capabilities, context limits, availability, latency, permissions, health, execute, stream,
cancel, session/resume, telemetry, usage, cost, and quota. Unknown observations carry an explicit
unavailable reason and confidence; they are never synthesized.

## Execute lifecycle

1. Supervisor validates task requirements, privacy, permission class, and policy.
2. Scheduler selects an eligible Worker from one frozen snapshot and persists reasons.
3. Adapter accepts a bounded normalized request with opaque task/run/session identifiers.
4. Adapter emits normalized start/progress/heartbeat/tool/usage/result/error events.
5. Supervisor owns cancellation, timeout, verification, state transitions, and persistence.

At-least-once events require deduplication. Provider prose cannot grant authority or report a
terminal Supervisor state. Raw output may be retained only as sanitized evidence by reference.

## Existing implementations

Claude, Grok, and AGY are native subprocess adapters. The Local Worker is a versioned HTTPS adapter
to a loopback-only model runtime behind authenticated private transport. Their current exact support
is in `CAPABILITIES.md` and tests. New providers must not require scheduler-core semantic changes.

Local inference remains read-only/non-code by policy and must be rejected for production code,
patch, or repository-edit tasks at both scheduling and adapter boundaries.

`WorkerContract.permissions` is an advertisement for discovery and routing, not a capability grant
or a generic enforcement engine. Existing scheduler and adapter checks remain authoritative until a
future policy executor binds a verified contract to a task-scoped grant. Native adapters inherit an
allowlist of non-secret OS context rather than the entire parent environment; provider-specific
variables must be supplied explicitly by trusted configuration and are never emitted.
