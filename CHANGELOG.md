# Changelog

All notable changes are documented here. The project follows semantic versioning
for public protocol and package compatibility once a public release is tagged.

## 0.2.0-beta.1 — 2026-08-11 (package `0.2.0b1`)

- Adds a durable autonomous evaluate, plan, dispatch, verify, and continue loop with
  dependency-aware scheduling, bounded retries, no-progress guards, and structured escalation.
- Adds durable provider-job identities, generation-fenced execution leases, crash reconciliation,
  exactly-once canonical result ingestion, and task/version-scoped verification criteria.
- Adds durable Pause, Resume, Steer, Stop, cancellation fences, and semantic run/task/job event and
  API projections.
- Adds Local Worker Protocol V2 with authenticated production mode, a durable launch registry,
  server-enforced idempotent launch, rejection/acceptance receipts, lookup, recovery, and restart
  acceptance across real daemon/process boundaries.
- Adds registered native CLI worker paths for Codex, Claude Code, Grok, and Google AGY, with bounded
  subprocess execution, strict provider-dialect parsing, and deterministic offline fixtures.
- Hardens atomic migrations, stale-owner recovery, secure evidence projection, and resource-aware
  worker routing.
- Makes source-release versions explicit and validated, and excludes every `artifacts/` path from
  release archives by a fail-closed code and policy boundary.

Known limitations: Windows Job Object containment remains pending; full reconstruction solely from
the event journal is not implemented; some provider CLIs do not support native resume/reconnect or
provider-side idempotency; and advanced cross-machine recovery still requires operator policy.
Local Worker V2 provides exactly-once logical launch per durable idempotency key at one authority,
not universal exactly-once external side effects.

## 0.1.0-alpha.1 — 2026-08-09 (package `0.1.0a1`)

- Preserves the verified V0 local-first Supervisor baseline.
- Adds provider-neutral worker, transport, identity, capability, and execution
  history foundations.
- Adds deterministic Hybrid Engine routing inputs while retaining existing
  topology behavior.
- Adds safe, dry-run-first cross-platform bootstrap discovery.
- Adds machine-facing operating, trust, recovery, and acceptance documents.
- Defines Project_Bridge as an optional, non-authoritative codec integration
  foundation with deterministic structured fallback.
- Adds privacy-bounded deterministic source archive and audit tooling.

This version is public-release-ready only after every gate in
`RELEASE_CHECKLIST.md` is satisfied. It has not been published.
