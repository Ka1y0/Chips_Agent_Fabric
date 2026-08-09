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

## Authorization

Bearer tokens are capability-scoped. `observe:read` is sufficient only for read-only monitoring.
Future mutation scopes must be separated by task/worker/node capability, and RED actions also require
a durable human approval record. A bearer token never grants arbitrary shell or filesystem access.
Future privileged node work also requires the capability-grant and Privilege Broker rules in
`docs/CAPABILITY_MODEL.md`; scope names alone do not create authority.

## Worker adapters

Adapters normalize process/session/model/usage/tool/result signals and retain sanitized raw evidence
by reference. Cancellation and timeout are Supervisor decisions, not instructions inferred from model
text. Session identifiers are opaque and provider-specific below the adapter boundary.
