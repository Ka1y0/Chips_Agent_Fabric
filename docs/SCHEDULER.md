# Deterministic Hybrid Engine

## Implemented V0

Routing is a pure deterministic function over one frozen world snapshot. It applies hard constraints
before weighted scoring, uses Worker ID as the final tie-break, and persists selected/rejected
reasons. Supported topologies are `SINGLE`, `PRIMARY_REVIEWER`, `PARALLEL_PANEL`,
`CHEAP_FIRST_ESCALATION`, and `FALLBACK`.

Hard constraints include required capability, permission class, privacy/local-only policy, code-write
policy, health/availability, and topology eligibility. A score cannot override a hard rejection.

## V0.4 resource-aware deterministic routing

The production runtime freezes the latest applicable quota-pool observation for each Worker into
every routing decision. A usable observation must be fresh, provider-consistent, explicitly
provenanced, and non-UNKNOWN. Stale, missing, provider-mismatched, unknown-confidence, or
unknown-provenance evidence becomes `quotaState: unknown` with a machine-readable reason; it is
never converted to available, unlimited, free, or zero-cost capacity.

Known exhausted pools are excluded when another capable pool exists and dispatch fails closed when
all capable pools are observably exhausted. Known scarce configured-premium pools can be avoided
when a capable healthier alternative exists. Otherwise UNKNOWN remains eligible but carries an
explicit lower quota-health score. Provider-reported, locally measured, and inferred observations
retain distinct provenance and confidence in the persisted routing explanation.

Quota policy never weakens the original hard constraints. Operationally busy Workers produce a
durable `dispatchDeferred` event and leave the Task READY when they would otherwise satisfy it;
bounded event-driven retry serializes sibling actions after capacity returns. Offline,
unauthorized, privacy-ineligible, capability-mismatched, or code-write-forbidden Workers retain
their original hard rejection and cannot be rehabilitated by favorable quota evidence.

Repeated identical routing/deferral decisions are stored idempotently. If capacity does not become
available before the configured dispatch-wait limit, the Task enters `blocked` with
`DISPATCH_WAIT_TIMEOUT`; it is never allowed to spin and append observations indefinitely.

## Crash-safe Task claim and DAG gate

Eligible Tasks are considered by descending priority with stable creation/ID tie-breaks. Before any
adapter invocation, one SQLite transaction compares the Task version/state, rechecks every
prerequisite, reserves all selected idle Workers, increments the attempt, creates STARTING Worker
runs, acquires a renewable execution lease, and journals the claim. A stale peer cannot execute the
same Task through a different Worker snapshot. Worker activation then atomically verifies that both
the Task and STARTING run remain active, fencing cancellation before provider invocation.

Dependency insertion rejects self-edges, cross-project edges, transitive cycles, and graph changes
after execution begins. Dispatch waits
for every prerequisite to succeed. Failed/cancelled prerequisites block dependents with explicit
IDs and states instead of leaving them indefinitely READY; independent Tasks can still launch in the
same pass when separate Worker capacity exists.

## Hybrid Engine V1 foundation

Normalized routing inputs include context requirement, node/model availability, observable quota,
latency, configured cost score, historical reliability, node load, and expected quality. Missing
observations remain unknown and are handled by explicit policy—not treated as zero, free, or
unlimited.

Execution history should record task type, Worker/provider/model/node, topology, latency, outcome,
retry, observable tokens/cost, review outcome, and human acceptance. Telemetry is append-only input
for later analysis. V0.1 does not introduce opaque self-learning or silently change routing weights.

Every route must remain reproducible from snapshot, policy version, requirements, candidates, and
reasons. Adaptive routing requires a future explicit policy/ADR and deterministic safety envelope.
