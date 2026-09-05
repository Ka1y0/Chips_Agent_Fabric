# Changelog

All notable changes are documented here. The project follows semantic versioning
for public protocol and package compatibility once a public release is tagged.

## Unreleased

- Publishes the newer V0.3 capability, execution, interaction, multi-node, and recovery architecture.
- Adds read-only onboarding presets over the canonical bootstrap plan.
- Adds evidence-backed local-model profiles with explicit unknown capacity and freshness.
- Adds Hybrid Engine topology planning and bounded cluster DAG expansion while retaining scheduler
  authority over concrete Worker selection.
- Adds optional semantic Bridge transfer policy and typed support accounting without canonical
  state or permission authority.
- Adds duplicate-Worker scheduler rejection, platform-explicit credential tests, and isolated CI
  quality/build/subprocess acceptance gates.
- Checks all generated stage capabilities before allowing a privacy-sensitive Hybrid plan and
  preserves explicit Computer Use requirements on the Actor.
- Preserves finite latency/quality/cost routing weights in cluster instances and protocol output.
- Keeps unknown reasoning support distinct from unsupported, validates local capability flags,
  and avoids concurrency/full-offload recommendations without observed memory headroom.
- Rejects credential-bearing or malformed local endpoint values at both discovery and direct
  candidate construction. See `docs/AUDIT_2026_09_05.md` for scope and integration limitations.
- Adds `chips-model-probe`: explicit loopback catalog requests and separately approved synthetic
  inference, with bounded HTTP responses, cancellation, safe error codes and freshness-aware
  projection into the existing local-model profiler. Discovery and settings remain non-mutating.
- Adds an optional bounded `zlib-json/1.0` Bridge wire codec with exact cross-process round trips,
  corruption detection and unchanged authority fallback. No model-token savings are claimed.
- Adds an independent executable-foundations test/build/clean-wheel gate while retaining every
  existing quality and acceptance gate. See `docs/EXECUTION_VERTICAL_SLICES.md` for usage and limits.

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
