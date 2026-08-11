# Release checklist

This is the fail-closed checklist for the Project_Supervisor V0.2 beta source candidate. Every item
starts unchecked. A code-complete component or historical smoke test does not satisfy a release gate
without current, sanitized evidence. This release is Supervisor-only and requires deterministic
offline worker tests; it does not require a billable provider generation or a Cyber Office change.

Candidate version: `0.2.0b1` / tag `v0.2.0-beta.1`

Revision / commit: `________________`

Release owner: `________________`

Evidence run ID: `________________`

## 1. Scope and repository integrity

- [ ] The candidate revision is immutable and the working tree contains no unexplained changes.
- [ ] All project-owned source, docs, tests, configuration examples, and sanitized artifacts are
      under the canonical Project_Supervisor root.
- [ ] No neighboring project changed except through separately authorized integration work.
- [ ] `CAPABILITIES.md`, `docs/GOAL_PROGRESS.md`, `docs/BLOCKERS.md`, and
      `docs/COMPATIBILITY.md` agree on implemented, blocked, and unsupported surfaces.
- [ ] No GitHub push, public service, purchase, provider login, or machine enrollment occurred as an
      implicit release step.

## 2. Build, test, and package

- [ ] The declared minimum Python 3.12 environment installs from a clean local environment.
- [ ] `.venv/bin/pytest` passes with the exact count recorded in evidence.
- [ ] `.venv/bin/ruff check .` passes.
- [ ] The wheel and source distribution build without undeclared files or credentials.
- [ ] Package metadata and `project_supervisor.__version__` both equal `0.2.0b1`; the release
      manifest and archive use the explicit label `0.2.0-beta.1`.
- [ ] The source release builder rejects a missing/invalid release label and excludes every
      `artifacts/` path even if release policy configuration omits it.
- [ ] A clean-environment install of the built wheel passes CLI `init`, `status`, `tasks`, and `logs`
      smoke tests.
- [ ] Package inspection confirms the SQLite migration SQL is included and usable after wheel
      installation.
- [ ] Dependency versions and licenses are captured; no known critical release-blocking issue is
      accepted silently.

## 3. Core correctness and recovery

- [ ] State-machine tests cover every legal transition and representative illegal transitions.
- [ ] State mutation and event append atomicity is verified under failure injection.
- [ ] Event sequence ordering, pagination, reconnect, replay, and de-duplication pass.
- [ ] Deterministic routing produces identical decisions/tie-breaks for the same frozen snapshot.
- [ ] All five declared execution topologies have deterministic success and failure-path evidence.
- [ ] Timeout, cancellation escalation, malformed worker output, partial output, and independent
      worker failure are verified.
- [ ] Restart/recovery reconciles interrupted runs without duplicate dispatch or lost terminal state.
- [ ] Definition-of-done cannot pass without all required deterministic acceptance criteria.
- [ ] Worktree creation, isolation, cleanup policy, and no-auto-merge behavior pass in a real Git
      fixture.
- [ ] Single-active-supervisor/lease behavior is verified across processes.

## 4. Worker compatibility

- [ ] Mock-adapter end-to-end workflow passes and preserves normalized events/results.
- [ ] Codex, Claude Code, Grok, and Google AGY native dialects pass deterministic fake-CLI E2E tests,
      including malformed output, timeout, cancellation, output bounds, and secret redaction.
- [ ] Installed provider CLI compatibility is inspected only with safe `--help` / `--version`
      commands; no live model quota is consumed as a release test.
- [ ] Capability projection distinguishes Local Worker V2 server idempotency from provider-native
      idempotency/resume, and leaves unavailable model/usage/account data unknown.
- [ ] Concurrent native workers overlap in time and finish independently without cross-cancellation.
- [ ] Windows Local Worker protocol v1 health/create/poll/cancel passes against the real worker.
- [ ] Local AI is rejected for code-writing at both scheduler and adapter boundaries.
- [ ] Provider authentication remains human-controlled and no credential value appears in logs,
      events, state, tests, or evidence.
- [ ] CLI parser compatibility is rechecked after any provider CLI version change.

## 5. API and Cyber Office boundary

- [ ] REST `/v1` envelopes, filters, errors, pagination, and unknown telemetry pass contract tests.
- [ ] WebSocket snapshot/replay/events/keepalive and reconnect/resume pass contract tests.
- [ ] Remote observation requires a valid `observe:read` token; missing/incorrect scopes fail closed.
- [ ] No Cyber Office, Cyber Island, or ArtLab source is included in the Supervisor release commit.
- [ ] Semantic Supervisor projections expose safe run/task/worker/provider-job state without raw
      provider metadata, credentials, tokens, or unsafe filesystem details.
- [ ] A client disconnect cannot affect task/worker execution or canonical state.

## 6. Network and security

- [ ] Default configuration binds only to loopback and starts read-only.
- [ ] The private Mac-to-PC route is authenticated, operator-approved, and independently verified.
- [ ] Non-loopback Supervisor traffic uses TLS with an approved certificate lifecycle.
- [ ] Firewall/overlay evidence proves no public or ordinary-LAN exposure of Supervisor, Worker,
      local model runtime, raw MCP, or shell.
- [ ] API bearer tokens are scoped, expirable, shown once, stored as salted hashes, and held by
      clients only in OS credential storage.
- [ ] RED mutations require durable human approval and cannot be inferred from model output.
- [ ] Local-state permissions, logs, exception text, raw provider lines, and evidence pass a secrets
      review.
- [ ] Security limitations and a private reporting contact are approved before any external release.

## 7. Configuration and operations

- [ ] Runtime `config.json`, environment overrides, and review manifests are not confused; the
      inventory in `docs/CONFIGURATION_INVENTORY.md` is current.
- [ ] Default data paths and explicit `--data-dir` behavior are verified on each supported OS.
- [ ] Backup and restore of SQLite state is documented and tested from a consistent snapshot.
- [ ] Migration forward behavior and failure recovery are tested on a copy of prior-version state.
- [ ] Log retention, evidence retention, worktree retention, and local-state cleanup policies have an
      owner and documented defaults.
- [ ] Troubleshooting covers auth boundary, provider exhaustion, stalled workers, private transport,
      TLS, token scope, and database recovery.
- [ ] Bootstrap remains discovery-first and does not install, authenticate, expose, or enroll without
      an explicit operator action.

## 8. Documentation, evidence, and legal

- [ ] Human quickstart and machine/agent protocol docs match the candidate behavior.
- [ ] Architecture and compatibility documents describe limitations without aspirational PASS claims.
- [ ] One `artifacts/goal-run/<run-id>/manifest.json` validates against the evidence schema.
- [ ] Every referenced evidence file has a SHA-256 digest and contains no credential or unnecessary
      machine/user identifier.
- [ ] FAIL, BLOCKED, SKIPPED, unavailable telemetry, and quota uncertainty remain explicit.
- [ ] Copyright ownership, distribution audience, dependency notices, and license are approved.
- [ ] `pyproject.toml`, the final `LICENSE`, notices, and release manifest express the same decision.

## 9. Deterministic production-like workflow acceptance

- [ ] A nontrivial task is submitted through the supported Supervisor interface.
- [ ] The scheduler records selected and rejected candidates with machine-readable reasons.
- [ ] A safe local subprocess/fake native worker executes through the production Local Worker path;
      no billable cloud generation is required.
- [ ] Failure/retry/replan behavior is exercised without losing event or task history.
- [ ] Deterministic verification gates the terminal success state.
- [ ] Supervisor and Local Worker daemon restart paths recover the durable launch/result without
      duplicate external execution or canonical result ingestion.
- [ ] Any generated acceptance evidence remains excluded from the release commit and archive.

## 9a. CHIPS Agent Fabric V0 gate status

Recorded 2026-08-09. A gate is `PASS` only when the real path was exercised; a rehearsal against a
test double is never a PASS.

```text
PRIVATE_FABRIC_TRANSPORT           = PASS
REMOTE_LOCAL_WORKER_E2E            = PASS
BOOTSTRAP_PACKAGE                  = PASS
RELEASE_PACKAGE_READY              = YES
FIRST_CROSS_DEVICE_FABRIC_WORKFLOW = PASS
CHIPS_AGENT_FABRIC_V0_READY        = YES
```

Evidence is in `artifacts/goal-run/local-worker-remote-v0/`. The real private route returned 13/13
passing Worker gates, the full runtime workflow succeeded, and Cyber Office rendered the persisted
REST/WebSocket lifecycle. This decision is for a private/internal V0 only; publication remains a
separate authorization.

## 10. Release decision

- [ ] Every mandatory item above is PASS, or each approved exception identifies its owner, risk,
      expiry, and compensating control.
- [ ] `docs/BLOCKERS.md` has no unresolved blocker that contradicts the release scope.
- [ ] The release owner signs the manifest and records `GO`, `NO-GO`, or `INTERNAL-ONLY`.
- [ ] Publication/distribution is performed only under explicit authorization, with a normal
      fast-forward push, a new annotated prerelease tag, and no history rewrite or force push.

Decision: `NO-GO / INTERNAL-ONLY / GO`

Signed by: `________________`

Timestamp (UTC): `________________`
