# Protocol

This is the canonical normalized state/event contract. The generic Worker and Node boundaries are in
[`docs/WORKER_PROTOCOL.md`](docs/WORKER_PROTOCOL.md) and
[`docs/NODE_PROTOCOL.md`](docs/NODE_PROTOCOL.md). Bootstrap discovery/planning is versioned
separately in [`docs/BOOTSTRAP_PROTOCOL.md`](docs/BOOTSTRAP_PROTOCOL.md). Project_Bridge messages, if
enabled, are non-canonical and must fall back to this structured contract.

## Principles

- JSON property names at external interfaces use lower camel case.
- Durable identifiers are opaque strings and must not be parsed for meaning.
- UTC timestamps use RFC 3339 with a trailing `Z` where possible.
- Events are append-only and globally ordered by integer `sequence`.
- Missing telemetry uses an explicit unavailable reason and evidence confidence.
- Provider-specific raw messages never become the public protocol.

## Resume cursor

Consumers request events where `sequence > afterSequence`, process in ascending order, and persist
the highest committed sequence. Delivery is at-least-once; reconnecting consumers must deduplicate.
The CLI equivalent is `logs --after <sequence>`.

## Task states

Core states are `draft`, `queued`, `ready`, `running`, `waiting`, `reviewing`, `blocked`,
`interrupted`, `succeeded`, `failed`, and `cancelled`. Transitions are validated by the state machine;
clients must not infer transitions from worker text.

Dependency edges are same-project DAG edges. `taskDependencyAdded` records a newly committed edge.
A terminal failed/cancelled prerequisite produces a machine-readable
`DEPENDENCY_TERMINAL_FAILURE` blocker on the dependent Task; a non-terminal prerequisite merely
prevents dispatch until a later scheduling pass.

Task dispatch is claimed transactionally with selected Worker reservations, a dependency recheck,
durable STARTING Worker runs, and a renewable execution-lease generation. Lease events make current
ownership inspectable, and recovery does not interrupt an unexpired owner. `succeeded` requires an
explicit verification policy record; a Worker process result or evaluator completion proposal cannot
silently promote canonical success. Deterministic criterion rows and the Task transition commit in
one transaction, preventing competing verifiers from persisting a mixed canonical outcome.

## Autonomous Goals

An autonomous Goal is a durable control-plane object, separate from any one Task or Worker session.
Its loop is `evaluate → plan → dispatch → collect → verify`, followed by either explicit
termination or another iteration. A Task entering `reviewing` or `succeeded` is evidence for the
Goal evaluator; it is never implicit proof that the Goal is complete.

Goal states are `created`, `running`, `softPaused`, `hardPaused`, `stopped`, and `terminated`.
Terminal reasons are `SUCCESS`, `BLOCKED`, `NO_PROGRESS`, `ITERATION_LIMIT`, `BUDGET_LIMIT`,
`REPEATED_FAILURE`, `SAFETY_BOUNDARY`, `PERMISSION_REQUIRED`, `HUMAN_ESCALATION`, and
`USER_STOPPED`. Evaluations, plans, generated actions, steers, verification decisions, controls,
and termination decisions are journaled. See `schemas/goal-v1.schema.json`,
`schemas/goal-control-v1.schema.json`, and `docs/AUTONOMOUS_ITERATION.md`.

## Normalized event envelope

```json
{
  "sequence": 42,
  "eventID": "evt-opaque",
  "kind": "taskStateChanged",
  "severity": "notice",
  "entityType": "task",
  "entityID": "task-opaque",
  "projectID": "project-opaque",
  "taskID": "task-opaque",
  "summary": "Task state ready -> running",
  "payload": {"from": "ready", "to": "running"},
  "actor": "supervisor",
  "createdAt": "2026-08-08T00:00:00Z"
}
```

The current store uses snake_case internally; API projections own the stable external casing.

## Telemetry

Known values include `value`, unit, and confidence. Unknown values include an unavailable reason such
as `notReported`, `notSupported`, `permissionDenied`, `stale`, or `offline`. Do not serialize unknown
as zero or compute a synthetic quota percentage.

V0.2 records normalized invocations by Goal, Task, Worker, provider, observed model, node, outcome,
and task role. Duration is Supervisor-observed. Provider tokens, cached tokens, cost, and remaining
quota remain unavailable unless actually reported. Aggregates expose known and unavailable counts;
Codex offload excludes unknown executor identity, and quality-adjusted offload remains unavailable
without an explicit quality signal.

## Authorization

Bearer tokens are capability-scoped. `observe:read` is sufficient only for read-only monitoring.
Goal mutations require the separate `goal:control` scope and an explicit server setting; even the
development-only unauthenticated loopback observation mode cannot mutate Goals. Other mutation
scopes must be separated by task/worker/node capability, and RED actions also require
a durable human approval record. A bearer token never grants arbitrary shell or filesystem access.
Future privileged node work also requires the capability-grant and Privilege Broker rules in
`docs/CAPABILITY_MODEL.md`; scope names alone do not create authority.

## Worker adapters

Adapters normalize process/session/model/usage/tool/result signals and retain sanitized raw evidence
by reference. Cancellation and timeout are Supervisor decisions, not instructions inferred from model
text. Session identifiers are opaque and provider-specific below the adapter boundary.

Read-only clients can inspect normalized execution attempts through `GET /v1/runs` and
`GET /v1/runs/{runID}`. List filters are `taskID`, `workerID`, and normalized run `state`; the detail
projection exposes canonical identifiers, lifecycle timestamps, usage availability, immutable
structured result fields, and a redacted evidence reference without reading artifact contents.
Absolute host filesystem paths and `file:` URIs are never exposed by that reference.
