# Bootstrap protocol

## Maturity

The discovery and planning phases below are **IMPLEMENTED**. Later phases are a versioned design
**FOUNDATION** and require capability-broker, identity, transport, and node-runtime implementation
before they may be advertised as automated.

## Phases

1. `DISCOVER`: read local non-secret facts; emit `machine-profile`.
2. `PLAN`: derive deterministic proposed steps; emit `bootstrap-plan`.
3. `REVIEW`: detect conflicts and existing canonical state; fail closed.
4. `TRUST`: establish or validate node identity through an approved initial trust root.
5. `PREPARE`: request explicit capability grants for package/service/network operations.
6. `CONFIGURE`: apply only granted, idempotent operations through a Privilege Broker.
7. `ENROLL`: mutually authenticate to an existing Fabric or initialize a new one.
8. `VERIFY`: run protocol, security, isolation, and restart/recovery acceptance.
9. `COMMIT`: persist reviewed node/Worker registration and append audit events.

No phase may silently skip a failed prerequisite. Re-running must preserve stable identity and
canonical state, not create duplicates. Rollback/recovery metadata must accompany every mutation.

## Discovery contract

`schemaVersion: 2` includes host facts, resource candidates, Fabric presence hints, network hints,
and explicit booleans proving that connectivity, listeners, credentials, and authentication were not
inspected. `null`/empty means unavailable, not absent hardware.

## Plan contract

`schemaVersion: 1` identifies `universal-bootstrap-foundation`, `mode: dryRun`, `failClosed: true`,
ordered steps with reason/status/mutation flags, an empty `automaticActions`, and the prohibited
automatic actions. Given the same profile, the plan is byte-stable after canonical JSON encoding.

## Error rules

- Unsupported OS: emit a blocked plan and return nonzero.
- Existing state: require review; never overwrite.
- Partial `--emit` arguments or non-empty output: return nonzero without modifying existing files.
- Missing utility/telemetry: report unavailable and continue discovery.
- Any needed authentication, network, install, or privilege: produce `approvalRequired`; do not act.
