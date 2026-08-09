# CHIPS Agent Fabric goal progress

Updated: 2026-08-09

## Preserved V0 baseline

The frozen V0 baseline demonstrated a macOS control node, authenticated encrypted private HTTPS, a
loopback-only Windows Local Worker and local GPU runtime, Supervisor persistence, and Cyber Office
REST/WebSocket observation. Private commit identifiers, node names, addresses, hardware identifiers,
screenshots, raw logs, credentials, and Git history are excluded from public source.

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

## Release boundary

The `v0.1.0-alpha.1` public seed is created from the audited clean archive in an isolated repository
with a new root commit. The private development history and private V0 evidence are not publication
inputs. Publication exposes source code only: it does not expose or alter any private Fabric runtime,
service, credential, transport, or node.
