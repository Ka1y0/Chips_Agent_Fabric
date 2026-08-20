# Recovery

SQLite and its append-only event journal are canonical. Provider chat, process stdout, Cyber Office,
Bridge traffic, and in-memory state are not recovery authorities.

## Safe procedure

1. Stop dispatch without deleting state; preserve database/WAL and sanitized logs.
2. Identify the data directory plus instance/Task lease owners. Multiple processes may observe the
   database, but only the current generation may mutate an owned execution.
3. Verify filesystem ownership, package version, migrations, configuration, and integrity backup.
4. Reconcile each persisted non-terminal task/run with its opaque process/job/session handle.
5. Resume only when identity, adapter support, grant validity, and remote ownership are unambiguous.
6. Otherwise record interruption/blockage and require deterministic retry or review.
7. Start loopback/private services, replay events after the last committed sequence, and verify
   idempotent observer behavior.

## Autonomous Goal recovery

Autonomous iterations persist their evaluation, plan, generated actions, dispatch handle, result,
verification, progress fingerprint, and steer-version binding. After restart, the engine reuses a
persisted action identity. A renewable Task execution lease protects a still-live peer. Once that
lease expires, a recovery owner claims a higher generation and reconciles durable provider jobs
before applying the legacy interruption policy. Task, Worker-run, provider-job, result, and Worker
state lifecycle mutations in this path are checked against the current owner and generation. The
planner must not recreate a plan that already committed.

Schema migration startup is serialized across local Supervisor processes, and each migration plus
its version record commits atomically. The migration sidecar lock is coordination metadata, not
project truth. SQL migration checksums and a full event-journal snapshot/rebuild mechanism are not
yet implemented.

SOFT PAUSE completes in-flight work but creates or dispatches nothing new. HARD PAUSE and STOP are
durable control records; a running engine polls them and requests adapter cancellation where
supported. HARD PAUSE keeps the active checkpoint so RESUME can verify or replan from the recorded
state. A missing/ambiguous handle fails closed rather than inventing completion.

## Worker and provider-job recovery

Dispatch persists a provider-job intent, adapter identity, declared restart capabilities, launch
generation, and `supervisor-execution:<run-id>` idempotency key before calling an adapter. It then
records launch start and binds the adapter's opaque handle as soon as the adapter returns it. Handle
lookup identifiers round-trip exactly in private canonical state while provider metadata is
sanitized before storage and public projection is separately allowlisted. This ordering narrows the
crash window; it does not create a
transaction spanning SQLite and an external provider.

Recovery claims an expired Task execution lease at a higher generation and records one of these
provider observations: `knownRunning`, `knownCompleted`, `knownFailed`, `knownCancelled`,
`providerNotFound`, `providerUnreachable`, or `unknown`. The current policy is:

- `knownRunning` reattaches only when the persisted capability and current adapter both support
  resume.
- A known terminal job is ingested only when repeatable collection is advertised.
- A launch checkpoint without a handle may cross `start` again only when both the persisted
  contract and current adapter advertise provider-native idempotency. Recovery reuses the exact
  durable key and still verifies that the returned handle belongs to the same Worker run.
- `providerNotFound` is the only observation that can make a single-job attempt retryable
  automatically. A missing job inside a multi-worker attempt remains ambiguous.
- Unreachable or unknown state, a missing handle/adapter, unsupported resume, or uncertain result
  collection opens one of `EXTERNAL_JOB_UNREACHABLE`, `PROVIDER_STATE_AMBIGUOUS`,
  `IDEMPOTENCY_UNCERTAIN`, `RESUME_UNSUPPORTED`, or `RESULT_COLLECTION_UNCERTAIN` and withholds fresh
  dispatch. Recovery parks the Task in `WAITING`, which is not eligible for a duplicate launch but
  can be reconciled again after provider reachability changes.
- A definitive later observation resolves the corresponding open execution escalation under the
  current lease generation. Collected provider state is terminal-monotonic: late polls cannot reopen
  it or rewrite a Worker state established by a newer assignment or health decision.

The Local Worker HTTP v1 adapter has real handle-based reconciliation: it queries `GET /v1/jobs/{id}`,
reattaches by polling a known running job, repeatably collects the terminal representation, and can
request `POST /v1/jobs/{id}/cancel`. It does not advertise provider-side idempotency or stream
reconnection. After `POST /v1/jobs` crosses the transport boundary, an HTTP error is ambiguous unless
the protocol supplies an explicit rejection receipt; Supervisor does not infer that no job exists.
A timeout becomes terminal only after cancel plus a provider query confirms terminal state. Its
read-only/no-code-write boundary remains unchanged.

Local Worker HTTP v2 closes the v1 response-loss ambiguity for one durable daemon authority. Before
freezing a provider-job intent, Runtime negotiates v2 and persists the observed protocol, authority
identity, provider-idempotency support, durable-registry support, and idempotent-lookup support.
Launch always performs a side-effect-free key lookup first. An existing launch binds its canonical
job handle; authoritative `NOT_SEEN` or a definitely-not-launched `RESERVED` record permits the same
key/digest request; `UNKNOWN`, unreachable registry, or an unreceipted `LAUNCHING` record remains
held. A proven pre-launch rejection receipt is retryable according to normal policy, while an
idempotency conflict is escalated because the original job may exist.

The daemon has its own SQLite registry and atomic child receipt spool. On restart it reconciles
terminal receipts or a running process using authority, job, launch nonce, host, PID-birth, and
job-owned process identity. A mismatched/reused PID is not attached. `RESERVED` is known not to have
crossed spawn; `LAUNCHING` without a trustworthy child receipt is ambiguous and is never spawned
automatically. Terminal representations remain repeatably collectable after daemon restart.

Codex, Claude, Grok, and AGY native CLI adapters use the legacy `execute`/`cancel` interface. Their live
process groups can be cancelled by the owning Supervisor process, but no durable process attachment,
restart resume, repeatable collection, or durable cancellation is claimed. After restart, ambiguous
legacy work is held for reconciliation/escalation rather than assumed dead or relaunched.

Exactly-once applies to canonical ingestion: an identical Worker result replay and provider-result
finalization are idempotent, and their terminal events are emitted once. External launch and cancel
remain best-effort across the database/provider boundary. Exactly-once external launch requires a
provider that actually enforces the advertised idempotency key. Local Worker v1 does not, so a crash
after its POST was accepted but before handle binding is recorded as `IDEMPOTENCY_UNCERTAIN` and is
not retried automatically. Local Worker v2 does enforce one logical launch per key inside one
healthy durable registry authority, including response-loss replay and daemon restart. This narrow
guarantee does not extend to arbitrary effects performed by the child or to other providers.

An explicit Task cancellation fences every active canonical run and releases the previous execution
generation before contacting providers. When a durable adapter can immediately reconcile a terminal
cancel/completion race, Supervisor collects it under a new cancellation-specific generation; the
Task and run remain canonically cancelled while the provider observation stays auditable. An
unconfirmed external cancellation remains uncertain and escalated rather than being called complete.

`GET /v1/runs` and `GET /v1/runs/{run_id}` expose only a public `providerJob` projection: canonical
job ID, safe bounded external ID when one can be projected, launch/reconciliation/result states,
capability flags, timestamps, open escalation summaries, and a short idempotency-key fingerprint.
The nested `providerJob` object omits adapter instance identity, provider session ID, runtime
PID/host/identity, private adapter metadata, the raw idempotency key, and escalation detail/actor
fields. Existing top-level run/session fields retain their separate compatibility and redaction
rules.

Never delete or rewrite events to make state appear consistent. Never regenerate a node identity
over an existing unknown identity. A restore must preserve Fabric/node IDs and document restored
sequence/time. Secrets are restored through their OS/provider process, not database or source archive.

## Node runtime monitor recovery

V0.4 persists one recovery lease and one polling checkpoint per runtime policy. A replacement
monitor may take an expired lease only by incrementing its generation; unfinished prior attempts
become `failed/staleLeaseRecovered`, and the old generation is fenced from later state mutations.
The successor probes first. A runtime that became ready before the crash is observed without another
start request. If it is still unavailable, only an externally authorized typed adapter may receive
`runtime.start` with the new generation, deadline, and durable idempotency key.

Do not manually delete a lease to force progress. Inspect `/v1/nodes/recovery`, monitor heartbeat,
lease expiry/generation, latest attempt, and next observation. Resolve a live owner or trust/binding
problem; stale-owner recovery is automatic after the configured TTL. See
`docs/NODE_RUNTIME_RECOVERY.md` and ADR 0002.

Portable automatic restart/service recovery remains a **FOUNDATION** until exercised on clean macOS,
Windows, and Linux hosts.
