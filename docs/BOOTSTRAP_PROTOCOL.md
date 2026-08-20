# Bootstrap protocol

## Maturity

Discovery, planning, and the durable lifecycle/audit recorder are **IMPLEMENTED**. Later phases are
an approval-scoped **FOUNDATION**: the recorder accepts a structured result only after a separate
authorized executor performed the operation. It is not a Privilege Broker, grant verifier,
installer, identity system, or transport configurator.

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

No phase may silently skip a failed prerequisite. Re-running the same run ID with identical evidence
is idempotent; changed evidence fails closed. A conflicting result idempotency key is rejected.
Rollback/recovery metadata must accompany every real mutation at the future executor boundary.

## Discovery contract

`schemaVersion: 2` includes host facts, resource candidates, Fabric presence hints, network hints,
and explicit booleans proving that connectivity, listeners, credentials, and authentication were not
inspected. `null`/empty means unavailable, not absent hardware.

## Plan contract

`schemaVersion: 2` identifies `universal-bootstrap-lifecycle-foundation`, `mode: dryRun`,
`failClosed: true`, ordered phase/dependency/action records, a scoped capability for every mutating
step, an empty `automaticActions`, and the prohibited automatic actions. Given the same profile, the
plan is byte-stable after canonical JSON encoding.

## Durable run and result contracts

`bootstrap-run-v1` stores only profile/plan digests, ordered step projections, non-secret external
authorization references, and an append-only hash-chained audit. SQLite WAL and `synchronous=FULL`
provide restart durability. The raw profile remains machine-local and is not copied into the run
projection.

`bootstrap-step-result-v1` is bounded to 32 KiB and requires run/step/idempotency identities,
outcome, execution mode, actor, time, evidence, and optional authority reference. Mutating success
requires `externallyExecuted`, the exact planned capability, and a human, enterprise,
Privilege-Broker, or trusted-node authority type. This proves only that the recorder accepted an
attributed assertion; production execution still requires real signature, issuer, expiry, replay,
constraint, target, and rollback validation.

## Error rules

- Unsupported OS: emit a blocked plan and return nonzero.
- Existing state: require review; never overwrite.
- Partial `--emit` arguments or non-empty output: return nonzero without modifying existing files.
- Missing utility/telemetry: report unavailable and continue discovery.
- Any needed authentication, network, install, or privilege: produce `approvalRequired`; do not act.
