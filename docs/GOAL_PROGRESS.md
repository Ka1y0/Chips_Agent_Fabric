# CHIPS Agent Fabric goal progress

Updated: 2026-08-11

## Preserved V0 baseline

The frozen V0 baseline is commit `9bef90e`. Its real private path used a macOS control node,
authenticated encrypted private HTTPS, a loopback-only Windows Local Worker and local GPU runtime,
Supervisor persistence, and Cyber Office REST/WebSocket observation. Private node names, addresses,
hardware identifiers, screenshots, raw logs, and credentials are excluded from public source.

```text
PRIVATE_FABRIC_TRANSPORT           = PASS
REMOTE_LOCAL_WORKER_E2E            = PASS
BOOTSTRAP_PACKAGE                  = PASS
RELEASE_PACKAGE_READY              = YES
FIRST_CROSS_DEVICE_FABRIC_WORKFLOW = PASS
CHIPS_AGENT_FABRIC_V0_READY        = YES
```

V0 evidence remains local under `artifacts/goal-run/` and is not a distributable source input. No
public listener, Funnel, ordinary-LAN Worker/model exposure, raw MCP, or shell endpoint was created.

## V0.1 implemented foundations

- Provider-neutral Worker discovery, health, telemetry, execute, stream, and cancellation contracts.
- Vendor-neutral transport status with separate authentication, encryption, peer identity,
  reachability, latency, and health observations.
- Public-only node identity metadata and durable, scoped, attributable, expiring/revocable
  capability-grant records that execute no privilege.
- Deterministic Hybrid Engine input snapshots and normalized execution-history persistence.
- Dry-run-first `chips bootstrap` discovery for macOS/Windows/Linux foundations, explicit review
  bundle emission, host-redacted output, and no automatic install/login/network/elevation.
- Machine-facing intent, protocol, trust, recovery, scheduler, troubleshooting, and unknown-LLM
  acceptance documents.
- Optional Project_Bridge codec boundary with exact negotiation, bounded validation, full-envelope
  hashes, derived-only artifacts, privacy-safe telemetry, and deterministic structured fallback.
- Apache-2.0 project metadata, provenance notices, deterministic commit-bound source archive,
  hardened archive/privacy auditing, and standard sdist exclusions.

## V0.2 beta implemented foundations

- Durable autonomous evaluate, plan, dispatch, verify, and continue operation with dependency-aware
  scheduling, bounded retry/no-progress/time guards, and structured human escalation.
- Generation-fenced execution leases, durable provider-job identities, deterministic reconciliation,
  exactly-once canonical result ingestion, and restart-safe task lifecycle recovery.
- Durable Pause, Resume, Steer, Stop, cancellation fences, task/version-scoped verification
  criteria, and secure semantic API projections.
- Local Worker Protocol V2: authenticated production profiles, durable server-side launch registry,
  request-digest conflict protection, acceptance/pre-launch rejection receipts, idempotency lookup,
  process identity stronger than PID alone, and real daemon/process restart acceptance.
- Registered bounded native CLI paths for Codex, Claude Code, Grok, and Google AGY. Provider dialect
  and production-runner tests use deterministic fake executables and consume no live model quota.
- Atomic migration/startup locking, stale-owner recovery, resource-aware routing, and strengthened
  bootstrap lifecycle/recovery foundations.
- Explicit SemVer source-release labels and a fail-closed exclusion for all generated `artifacts/`.

## Honest maturity limits

- Zero-touch installation, trust-root enrollment, Privilege Broker execution, service installation,
  automatic transport configuration, Worker registration, upgrades, and recovery services are
  FOUNDATION or PLANNED—not complete.
- Node public identity records are not cryptographic enrollment or proof-of-possession.
- Capability grants are auditable authorization data; no generic privileged executor consumes them.
- Worker permission fields are advertisements. Existing scheduler/adapter enforcement remains the
  authority until a verified generic policy executor exists.
- Tailscale is proven V0 deployment evidence, not a permanent dependency. WireGuard, Headscale, and
  native mTLS providers are interface targets without real-host V0.1 implementations.
- Project_Bridge integration is FOUNDATION only. The inspected unlicensed prototype is experimental,
  has no production service contract, and is not copied into this repository.
- Windows/Linux discovery is contract-tested; full fresh-host installation/enrollment/recovery has
  not been demonstrated.
- The source archive is reproducible. The full dependency environment is not lockfile-reproducible
  yet; dependency ranges and observed versions remain explicit.
- Windows Job Object containment and full reconstruction solely from the event journal remain
  future work.
- Local Worker V2 provides exactly-once logical launch only within one healthy durable authority.
  Universal exactly-once provider side effects are not claimed.
- Provider CLIs without native resume/reconnect/idempotency support remain explicitly unsupported
  for those capabilities; Supervisor recovery fails closed rather than inventing support.
- Advanced cross-machine recovery requires operator policy when provider state is ambiguous or an
  authority is unreachable.

## V0.2 beta release boundary

The selected package version is `0.2.0b1`; the selected prerelease tag is `v0.2.0-beta.1`, provided
the tag is still unused. The release goal authorizes a normal fast-forward GitHub update, annotated
tag, and prerelease only after all current gates pass. It does not authorize force push, history
rewrite, repository visibility changes, or mutation of historical V0.1.

The public source builder requires an explicit SemVer release label, reads immutable blobs from the
selected Git revision, and excludes all `artifacts/` paths by both policy and a code-level boundary.
Private V0 topology and evidence remain local. Package/release publication never implies deployment,
provider login, live quota consumption, node enrollment, network exposure, or authority expansion.
