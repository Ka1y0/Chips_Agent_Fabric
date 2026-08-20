# Compatibility matrix

This matrix distinguishes code compatibility from real-host validation. It must be updated from
sanitized evidence for each release candidate; presence in the source tree is not enough.

## Status legend

| Label | Meaning |
|---|---|
| Contract-tested | Automated tests exercise the boundary with deterministic fixtures/mocks |
| Host-gated | A prior isolated real-host gate is recorded in `GOAL_PROGRESS.md` |
| Blocked | A named external or human-controlled prerequisite is absent |
| Unverified | The design may be portable, but no release evidence is recorded |
| Unsupported | Deliberately outside the V0 contract |

## Runtime and host platforms

| Surface | Minimum / expected | Status | Release note |
|---|---|---|---|
| CPython | 3.12+ | Contract-tested on the project Python 3.12 environment | The package metadata requires `>=3.12`; other minor versions need a CI row before release |
| macOS control plane | Current operator Mac | Native provider gates and the private cross-device release workflow passed | Provider authentication may require the normal/elevated user context without exposing credentials |
| Linux control plane | Python 3.12+ | Unverified | Default state path is implemented, but no host/package/service gate is recorded |
| Windows control plane | Python 3.12+ | Unverified | Default state path is implemented; no Windows Supervisor host gate is recorded |
| Windows Local Worker | Worker protocol v1 on loopback | Contract-tested with mocked HTTP and host-gated in the sanitized V0 private topology | Requires an operator-approved authenticated private transport; exact deployment remains local evidence |
| Bundled Local Worker v2 | CPython 3.12+, loopback, local SQLite | Contract-tested plus real daemon/Supervisor/child SIGKILL restart acceptance on macOS | Registered-driver production mode is loopback-only and bearer-authenticated; Windows process-tree cancellation remains unverified |
| SQLite | Python standard-library SQLite with WAL support | Contract-tested | Use a local disk; network/shared-filesystem semantics are unverified |
| Filesystem | Local, permission-restricted state directory | Contract-tested | Project worktrees require a Git repository; do not use shared state across supervisors |

No compatibility claim is made for PyPy, Python 3.11 or older, network-mounted SQLite databases,
containers, Kubernetes, or public-cloud deployment.

## Worker adapters

| Adapter | Invocation/output contract | Session/model behavior | Verification status | Constraints |
|---|---|---|---|---|
| Mock | In-process deterministic result/events | Synthetic session/model fixtures | Contract-tested | Test-only; proves orchestration, not a provider |
| Claude Code | `--print`, `stream-json`, `--verbose` | Parses session, actual model, context variant, usage/cost when emitted; supports explicit resume/model arguments | Parser/process contract-tested; prior native E2E host gate recorded | Resolves `claude` from `PATH` by default; explicit executable override remains supported |
| Grok Build | Print mode with `streaming-json` | Parses session/model/usage when emitted; supports resume/model arguments | Parser/process contract-tested; prior native E2E host gate recorded | Executable must resolve from `PATH`; installed CLI syntax remains authoritative |
| Google AGY | Sandboxed plan/print mode, plain-text stdout plus sanitized diagnostics | May parse conversation/model hints; resume supported; model override intentionally denied | Parser/process contract-tested; prior native E2E host gate recorded | Resolves `agy` from `PATH`; explicit override remains supported; no structured provider event stream |
| Windows Local Worker | HTTP protocol v1: health, create job, poll, cancel | Job ID is session handle; model/usage captured when reported | A sanitized real private-fabric gate passed on a Windows GPU node, including FAST, GENERAL, structured output, cancellation and timeout | Read-only/non-code only; server-side model selection; loopback permits HTTP, while every non-loopback URL requires HTTPS plus a non-empty bearer token; 401/403 map to `authRequired` and 429 to `rateLimited` |
| Local Worker v2 registered runtime | Durable launch lookup, server-owned driver profiles, native runner receipts | Node/authority/registry/runtime/driver identities remain distinct; model/session/usage captured when reported | Offline production-like Codex/Claude/Grok/AGY fake-CLI and hard-restart acceptance | No client executable/argv/env/cwd; deterministic driver rejected in production; real provider quota not exercised |
| Codex CLI | Fixed `codex exec --json`, stdin prompt, bounded `--output-last-message` | Thread/usage metadata only when validated events provide it; provider resume is not claimed | Parser/process/Local Worker contract-tested with an offline fake CLI; installed CLI help/version inspected without model use | Production Local Worker is read-only, approval-never, ephemeral, strict-config, and operator-profiled; account/plan/quota remain unknown |

Provider CLIs are not installed, updated, authenticated, or reconfigured by Project_Supervisor. A
new CLI version needs help/version capture, parser regression tests, and an isolated minimal probe
before its compatibility row can be promoted.

## API and clients

| Client/surface | Contract | Status | Notes |
|---|---|---|---|
| REST observer | `GET /v1/status`, `/nodes`, `/workers`, `/tasks`, `/tasks/{id}`, `/events` | Contract-tested | Stable envelope, cursor/filter tests, read-only |
| WebSocket observer | `/v1/stream` snapshot, replay, events, keepalive | Contract-tested | At-least-once; client deduplicates by sequence/event ID |
| Cyber Office | Same REST/WebSocket read contract | Separately validated client integration; not built or modified by this Supervisor release | Defaults to mock; remote mode is explicit and fail-closed |
| CLI observer | `status`, `tasks`, `logs` with optional JSON | Contract-tested | Does not start a server or dispatch workers |
| Token creation | Scoped, salted-hash-at-rest token | Contract-tested | Secret is displayed once; client stores it in an OS credential manager |
| Agent-native mutation API | Not part of current `/v1` read surface | Unsupported in this release candidate | Requires capability design, approval enforcement, tests, and an ADR |
| MCP control surface | Future work | Unsupported | Raw MCP exposure is explicitly prohibited |

## Network and deployment modes

| Mode | Configuration validation | E2E status | Support decision |
|---|---|---|---|
| Loopback `127.0.0.1:7330` | Implemented and tested | Local CLI/API fixtures tested | Supported default |
| Authenticated private address + TLS | Guardrails implemented | V0 Tailscale route host-gated; other providers unverified | Supported only with separately verified identity, encryption, TLS, firewall, bearer, reconnect, and no-public-exposure evidence |
| Ordinary LAN without authenticated overlay | Rejected | Not run | Unsupported |
| Public Internet listener | Rejected by project policy | Not run | Unsupported |
| Embedded credentials in URL/manifest | Rejected or prohibited | Negative tests/docs | Unsupported |

## Promotion rule

Promoting a row from Contract-tested or Unverified to release-ready requires a sanitized evidence
manifest under `artifacts/goal-run/`, checksums for referenced outputs, exact tool versions, an
explicit PASS assertion, and confirmation that no credential or private machine detail escaped.
