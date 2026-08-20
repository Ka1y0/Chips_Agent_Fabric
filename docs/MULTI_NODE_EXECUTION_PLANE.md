# Multi-Node Execution Plane

This document describes the bounded V0.3-dev Phase 2 execution-plane slice. It extends the existing
Supervisor scheduler, durable Worker runs, provider jobs, verification scopes, and Fabric
repositories. It is not a remote-shell system and it does not make Tailscale an orchestrator.

## Authority and observation

A Fabric Node is an operator-owned identity. A Tailscale device identity may be bound to that Node
as transport evidence, but an IP address, DNS name, or reachable peer never creates Node or Worker
authority. Private peer and endpoint identities remain in the control-plane store; public API
projections expose only semantic IDs and one-way evidence.

Capability existence is not runtime executability. Dispatch to a Phase 2 Worker requires a fresh,
append-only execution observation proving every required facet: discovered, configured,
authenticated, authorized, platform-approved when required, reachable, runtime-available, healthy,
and capacity-available. `UNKNOWN` is a hard routing failure. A transport ping alone does not satisfy
runtime availability or health.

When capacity is absent, the Task enters a durable bounded wait. A single recovery owner may run the
typed discovery, probe, bootstrap, and verification sequence. Bootstrap is a dedicated adapter
operation with a closed request schema; it cannot carry a command, argv, environment, working
directory, shell, or PowerShell program. Successful recovery publishes a fresh observation and
returns the same Task to dispatch without consuming an attempt. Retry, backoff, PAUSE/HALT/STEER
checks, and a maximum recovery count bound the process.

`fabric-runtime-start/v1` is the only bootstrap operation in this slice. The immutable request
binds the operator-pinned transport binding and its generation, Node and service identities,
waiting Task requirements, exact authorization version, a durable idempotency identity, deadline,
and a per-binding recovery lease generation. The node-side adapter descriptor must explicitly
guarantee generation, deadline, and idempotency enforcement. Its receipt echoes those identities;
a mismatched or stale receipt is rejected. Acceptance is not recovery: Supervisor performs a new
authenticated identity/health probe and publishes capacity only after that probe succeeds.

The node-side implementation is an independent fixed-profile runtime broker, not a Local Worker
route and not part of the service it may start. Its durable SQLite registry is explicitly
initialized, has stable broker-authority and registry-incarnation identities, pins the Node,
binding, service-profile revision and profile digest, and advances one lease generation at a time.
Production startup refuses a missing, damaged, moved, or identity-rotated registry. A request may
name only the semantic Fabric service profile; the Windows service name and configuration digest
come exclusively from operator-owned broker configuration.

The broker's only mutation route is `POST /v1/fabric/runtime/start`. It disables OpenAPI/docs,
requires a distinct bounded Bearer on every route, and returns `Cache-Control: no-store`. In
production the Bearer is loaded from Windows Credential Manager and the fixed target is controlled
through the Windows SCM API with query/config/start-only access. The broker re-hashes the pinned
service configuration immediately before status or start, including the complete ordered SCM
`MULTI_SZ` dependency chain rather than only its first entry. It never accepts an executable, argv,
environment, cwd, URL, service name, shell, or PowerShell program from a request and never projects
the internal Windows service name, binary path, PID, raw SCM error, or credential.

Receipts distinguish desired-state satisfaction, definite pre-start rejection, start pending, and
outcome unknown. The persisted `dispatching` boundary deliberately closes the dangerous crash
window: after a broker restart, RUNNING or START_PENDING is observed without a second start; a
STOPPED or unobservable service becomes `outcomeUnknown` and blocks a new generation. Client
disconnect does not cancel the shielded durable operation. This provides one logical broker
operation per key; it does not claim universal exactly-once Windows service or downstream side
effects.

The per-binding lease makes concurrent capacity waiters single-flight. A request whose response is
lost is recorded as `outcomeUnknown`; recovery may reconstruct only that exact immutable request
and replay the same POST/idempotency key after claiming a fresh local recovery lease. The broker
re-observes its durable operation and never treats this as authority for a new start identity. A
different generation stays blocked until the unresolved operation has authoritative closure. An
offline or unknown peer, missing/expired authorization, unsatisfied platform approval, or absent
`fabric.runtime.start` action authority results in zero bootstrap calls.

The concrete Mac client calls only `POST /v1/fabric/runtime/start` over HTTPS (loopback HTTP is
allowed for deterministic tests), disables redirects and ambient proxy trust, applies a whole-call
deadline and a 64 KiB decoded response bound, and requires an exact typed receipt. Its Bearer and
endpoint are process-local operator configuration and are not written to the recovery journal or
public API. Durable attempts contain only safe broker/registry semantic identities, the pinned
service-profile revision and SHA-256 digest, and a one-way adapter-instance identity.

## Authorization and data movement

`authorization-envelope/v1` is immutable, versioned execution authority. It binds a project, root
Task, capabilities, actions, resources, evidence packet identities, provider and Worker-class
allowlists, data/action allow and deny classes, a budget, and an expiry. Child work receives a
Supervisor-derived equal-or-narrower envelope in the same transaction as canonical Task creation.
Workers may propose narrowing; they cannot select a parent envelope, widen authority, reduce a deny
set, or self-approve.

User authorization and platform approval are separate facts. An approved user envelope does not
bypass Local Worker, OS, provider, or application approval. Dispatch checks both facts atomically
with the Task, Worker, and run claim.

Evidence packets store an identity, digest, classification, credential tri-state, and byte count,
not raw evidence. Movement records identify the authorized packet, destination class, purpose, and
disclosure state. Credential-bearing or unknown-credential packets cannot be marked disclosed.
Public projections omit source/destination references, transport references, raw content, and
authorization budgets.

## Provider invocation provenance

Legacy invocation telemetry remains a terminal accounting rollup. Phase 2 adds an append-only
provider invocation root and ordered observations so one Worker run may perform multiple
invocations and a later failure cannot erase an earlier success.

`modelUsed` and `dataDisclosed` are independent tri-state projections. A process start or generic
HTTP/transport error does not by itself prove either fact. `YES` requires explicit adapter/provider
evidence, including a canonical terminal result that identifies the actual provider model even
when the invocation failed. The first non-null typed model identity remains canonical; a different
terminal alias is retained as a conflict detail rather than silently replacing it. `NO`
requires authoritative rejection before the respective boundary. Otherwise the state remains
`UNKNOWN`. `outcomeUnknown` is a durable no-relaunch checkpoint, but later authoritative recovery
evidence may close the same invocation. Observation replay is idempotent by event key and semantic
fields even when a reconnect supplies a fresh local timestamp; a different stage/model/detail or
source under that key conflicts, and mutation after a proven terminal boundary fails closed.

Provider parallelism is governed by an operator-configured opaque capacity pool, not inferred from
a brand name. Reservation occurs in the same transaction as Task/Worker/run dispatch. Terminal or
authoritative not-found/pre-launch rejection releases capacity; unreachable or ambiguous jobs
retain it. Canonical Task cancellation alone is not provider quiescence evidence and therefore does
not release a launch that may have crossed the external boundary. TTL expiry alone never frees
external provider capacity. Conversely, boot recovery of an interrupted `prepared` intent proves
the launch boundary was not crossed, releases its pool reservation, and closes the invocation as
rejected before process with model/data both `NO`.

## Normalization, hypotheses, and one writer

Deterministic fusion consumes a normalized contribution bound to an immutable Worker result digest,
current attempt, Task definition, verification scope, and steer version. Callers cannot replace a
Worker result with arbitrary claims while retaining its provenance. Contradictory claims create a
durable hypothesis set; fusion never erases variants or their source identities.

An experiment proposal is bounded structured data. It cannot carry executable, argv, environment,
cwd, or shell authority and must distinguish at least two active hypotheses. Its authorization must
explicitly permit `experiment.run` and remain executable and unexpired.

A project has one operator-configured write owner. A time-bounded write lease is separate authority;
expiry alone never permits takeover. PC Codex may be represented as an external managed executor,
but Supervisor does not pretend it can attach, resume, or control a manual Codex process. A typed
handoff stores only content identity and digest. Experiment results must echo the proposal, come
from the canonical owner while its lease remains current, and satisfy `experiment-result/v1`.
Analysis Workers never inherit CrossFire write authority.

## Interaction recovery

Semantic UI execution checkpoints `observed`, `grounded`, `preconditionChecked`, and
`actionStarted` before invoking the adapter, then records `actionReturned` and
`postconditionVerified`. Each checkpoint is append-only and checked against current Task and UI
resource generations. If a process disappears after `actionStarted`, explicit quiescence recovery
marks the action `outcomeUnknown`; the same Task and plan cannot be blindly replayed. This is a
bounded safety foundation, not a claim of exactly-once GUI side effects.

## Current real boundary

The current Mac host can discover the operator's Windows Tailscale peer and prove direct transport
reachability. During Phase 2 acceptance, the configured remote Local Worker endpoints returned HTTP
502 for health, capabilities, model, and job status. Therefore Windows runtime executability,
remote Fabric dispatch, CrossFire interaction, and real provider dogfood were not authorized.
Supervisor did not create a generic remote shell, send arbitrary PowerShell, bypass platform
approval, call a billable provider, or write CrossFire.

The Mac control plane now has production-safe read-only Tailscale discovery, an authenticated Local
Worker v2 identity probe, the durable typed/fenced bootstrap journal, and the node-side broker
implementation above. Offline real-process acceptance hard-kills a broker after its fixed test
service crosses the start boundary but before receipt persistence; two daemon restarts recover the
same operation with one registry row and one underlying start. This is production code-path
acceptance with a deterministic local controller, not proof of a Windows SCM deployment.

The broker is not currently installed or authenticated on the operator's Windows PC. The next real
acceptance step therefore remains an operator-reviewed Windows service installation: create the
SCM service and least-privilege ACL, initialize the durable registry once, place the distinct token
in Windows Credential Manager, expose only the loopback broker through the existing pinned
Tailscale transport, then obtain and pin its authority/registry/profile identities on Mac. Until
that deployment and a fresh authenticated health observation succeed, private CrossFire evidence
and provider requests remain blocked.

Phase 2B adds the deterministic private Windows application artifact and the explicit
`ISSUE → ENROLL → ADMIT` state machine in `WINDOWS_NODE_ENROLLMENT.md`. Pending enrollment is not
Node authority. Only an exact, unexpired, HMAC-bound receipt with matching host, peer, artifact,
Mac-issued service profile, broker runtime profile, SCM and authenticated-health evidence can
atomically create the Node and transport binding. The artifact remains separate from its bootstrap
secret and never changes Tailscale or the legacy Phase 1 Worker.
