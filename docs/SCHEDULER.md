# Deterministic Hybrid Engine

## Implemented V0

Routing is a pure deterministic function over one frozen world snapshot. It applies hard constraints
before weighted scoring, uses Worker ID as the final tie-break, and persists selected/rejected
reasons. Supported topologies are `SINGLE`, `PRIMARY_REVIEWER`, `PARALLEL_PANEL`,
`CHEAP_FIRST_ESCALATION`, and `FALLBACK`.

Hard constraints include required capability, permission class, privacy/local-only policy, code-write
policy, health/availability, and topology eligibility. A score cannot override a hard rejection.

## Hybrid Engine V1 foundation

Normalized routing inputs should add context requirement, node/model availability, observable quota,
latency, cost, historical reliability, node load, and expected quality. Missing observations remain
unknown and are handled by explicit policy—not treated as zero, free, or unlimited.

Execution history should record task type, Worker/provider/model/node, topology, latency, outcome,
retry, observable tokens/cost, review outcome, and human acceptance. Telemetry is append-only input
for later analysis. V0.1 does not introduce opaque self-learning or silently change routing weights.

Every route must remain reproducible from snapshot, policy version, requirements, candidates, and
reasons. Adaptive routing requires a future explicit policy/ADR and deterministic safety envelope.
