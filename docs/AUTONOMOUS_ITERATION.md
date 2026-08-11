# Autonomous Iteration Core

Status: **IMPLEMENTED in private V0.2 development**, not published as a V0.2 release.

The Autonomous Iteration Core lets a durable Goal continue without a human supplying the next
prompt. It is provider-neutral: the core receives injected evaluator, planner, dispatcher, verifier,
and budget-meter interfaces. The production dispatcher seam submits generated Tasks to the existing
Supervisor runtime and deterministic Hybrid Engine; it does not bypass routing or permission policy.

## Canonical loop

```text
Goal intent + SQLite state + journal + prior attempts + budgets
  → evaluate
  → persist evaluation
  → plan structured actions
  → persist plan and deterministic action identities
  → dispatch through Hybrid Engine
  → collect Worker results
  → verify Goal satisfaction
  → SUCCESS / explicit terminal reason / next iteration
```

A Worker run or Task may succeed while the Goal remains incomplete. Only a persisted Goal
verification decision may terminate the Goal as `SUCCESS`; evaluation can only propose completion.
Before applying that verifier checkpoint, the engine also proves that no associated canonical Task
or Worker run is still active.

An evaluator `satisfied` decision is a completion proposal, not verification. The engine records it,
invokes the independent Goal verifier with the same persisted checkpoint, and terminates with
`SUCCESS` only when that verification also passes. Autonomous action Tasks record a deterministic
`workerExitPolicy` verification (durable completed run plus exit code zero) before entering
`succeeded`; if a Task has a current version-scoped criterion set, the exit policy cannot bypass it
and the Task remains `REVIEWING`. Goal-level acceptance remains a separate decision.

## Dispatch and dependency safety

A dispatch claim atomically changes one READY Task to RUNNING, reserves every selected Worker, and
creates the STARTING Worker-run records in the same SQLite transaction. A competing process may
persist an alternate routing observation, but it cannot claim the same stale Task or invoke an
adapter without a durable run identity. Recovery can therefore account for every claimed attempt,
release its Workers, and enforce `max_attempts` before requeue.

The same claim acquires a renewable, generation-bearing Task execution lease. A peer recovery pass
leaves unexpired work untouched; after a crashed owner's lease expires, bounded recovery polling
interrupts and requeues it exactly once. Adapter activation atomically rechecks both the STARTING
run and RUNNING Task, so cancellation committed before activation prevents provider invocation.
Loss of a live lease stops the stale execution coroutine and requests adapter cancellation.

Task dependency edges are same-project, acyclic, idempotent, and journaled. READY Tasks dispatch only
after every prerequisite succeeds. A failed or cancelled prerequisite deterministically moves its
dependent to BLOCKED with machine-readable blocker identities; other non-terminal prerequisites keep
the dependent eligible for a later scheduling pass. Independent READY Tasks remain parallelizable,
subject to Worker capacity.

## Human controls

- **SOFT PAUSE**: persist `softPaused`; let in-flight work finish; create/dispatch no new work.
- **HARD PAUSE**: persist `hardPaused`; poll in-flight handles and request cancellation where the
  adapter supports it; keep the iteration checkpoint.
- **RESUME**: return a paused Goal to `running` and continue from SQLite state.
- **STEER**: append sanitized guidance, priority, and `preserveValidWork`; the next evaluation sees
  the effective intent plus all still-valid results.
- **STOP**: persist `stopped`, `USER_STOPPED`, the sanitized reason, and completion time; request
  cancellation for in-flight work through the running engine.

REST mutation is disabled by default. When enabled, it requires a bearer with `goal:control` even if
unauthenticated loopback observation was explicitly enabled for development. The local CLI relies on
normal OS access to the selected Supervisor data directory and records actor `human:cli`.

## Guards and terminal reasons

Each Goal has positive limits for iterations, generated Tasks, failures, and repeated no-progress
fingerprints. Optional elapsed-time, token, and cost budgets are checked before another iteration.
If configured token/cost consumption cannot be observed, the engine terminates fail-closed with
`BUDGET_LIMIT`; it never assumes unknown consumption is zero.

Supported reasons are `SUCCESS`, `BLOCKED`, `NO_PROGRESS`, `ITERATION_LIMIT`, `BUDGET_LIMIT`,
`REPEATED_FAILURE`, `SAFETY_BOUNDARY`, `PERMISSION_REQUIRED`, `HUMAN_ESCALATION`, and
`USER_STOPPED`.

Repeated capacity deferral is bounded by `dispatch_wait_timeout_seconds`. Identical routing and
deferral observations are idempotent; exhaustion moves the Task to `blocked` with
`DISPATCH_WAIT_TIMEOUT` instead of spinning forever or flooding the event journal.

## Persistence and replay

Migration `0003_autonomous_iteration.sql` owns Goal, iteration, action, and steer records. Every
evaluation, plan, action transition, control, replan, verification, and termination emits a sanitized
append-only event. Goal context is rebuilt from project-scoped Tasks, events, Worker runs/results,
verifications, failures, iterations, actions, and steers; conversation history is never canonical.

On restart, a persisted running action is recovered using its opaque handle. The normal runtime first
waits for a valid foreign execution lease; after expiry it reconciles a durable provider job when the
adapter truthfully supports that operation. Only an authoritative provider-not-found observation or
a never-launched prepared checkpoint is eligible for normal retry. Ambiguous legacy execution is
parked and escalated rather than blindly relaunched. UUID-based action and Task identities plus the
atomic dispatch claim prevent duplicate Supervisor claims.

Evaluation and verification checkpoint JSON records the observed steer version. Restored decisions
are fenced against newer guidance; ambiguous legacy checkpoints created without that field are
discarded after any steer. Verifier persistence and terminal application are separately restartable,
so a crash between them does not rerun the verifier or bypass it.

This is restart-safe Supervisor bookkeeping, not proof that an external provider job stopped. The
Worker boundary has an optional durable `reconcile`/resume/collect contract; adapters that cannot
truthfully implement it remain explicitly non-resumable. External single-execution still requires
provider-enforced idempotency.

Task verification scopes are opt-in immutable snapshots. Each binding records a criteria version,
semantic Task-definition revision, optional Goal/iteration/plan/steer context, and item hashes.
Dispatch copies that identity onto Worker runs. A scoped verifier must echo the exact scope,
Task-definition revision, and source attempt it evaluated; those values are compared atomically
before evidence is written. Binding also verifies that Goal/plan/steer metadata belongs to the
iteration's steering epoch. Rebinding appends N+1 and preserves N; unscoped Tasks retain the legacy
project-template behavior for compatibility.

## Telemetry

Migration `0004_invocation_telemetry.sql` records each real Worker attempt, including failures,
cancellations, fallback/retry, and restart interruptions. The schema preserves unavailable model,
token, cached-token, cost, quota, and quality values explicitly. Aggregation is available in core,
the read-only `/v1/telemetry` resource, and `project-supervisor telemetry`.

## Deterministic acceptance

```sh
PYTHONDONTWRITEBYTECODE=1 .venv/bin/pytest -q -p no:cacheprovider \
  tests/test_autonomy.py tests/test_autonomous_subprocess_acceptance.py \
  tests/test_telemetry_v02.py tests/test_goal_controls_api.py tests/test_goal_controls_cli.py
```

`scripts/run_autonomous_acceptance.py` also performs a two-iteration Hybrid Engine workflow and an
actual abrupt child-process exit followed by recovery in a new process. It uses only the deterministic
Mock Worker, temporary state, no credentials, and no network.

For the V0.2.1 production process, exact CLI commands, lease ownership, process-lifetime behavior,
and post-Task resource auditing, see [`AUTONOMOUS_HOST.md`](AUTONOMOUS_HOST.md).
