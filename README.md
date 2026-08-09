# CHIPS Agent Fabric

CHIPS Agent Fabric is a local-first, provider-neutral orchestration system for heterogeneous AI
workers and compute nodes. This repository contains **Supervisor**, the headless kernel responsible
for canonical task state, deterministic scheduling, adapters, recovery, security, event journaling,
APIs, and telemetry.

**Cyber Office** is the separate name for every human-facing UI. **Project_Bridge** is an optional,
experimental AI-to-AI communication plane; it never replaces canonical SQLite, events, or
structured artifacts.

The V0.1 milestone preserves the real cross-device V0 baseline and adds portable open-source
foundations. Automated trust-root enrollment, Privilege Broker execution, transport installation,
and zero-touch deployment are not claimed complete.

The **Hybrid Engine** is Supervisor's deterministic, explainable routing layer. It filters Workers
by hard capability, privacy, availability, and policy constraints, then records why a topology and
Worker were selected. V0.1 establishes resource-aware routing inputs and normalized history; it does
not introduce opaque self-learning decisions.

## Start here

Humans and unfamiliar coding agents should begin with [`AGENTS.md`](AGENTS.md) and
[`FABRIC_INTENT.md`](FABRIC_INTENT.md). The machine-facing document map, authority boundaries,
bootstrap steps, validation commands, recovery rules, and maturity vocabulary are all there.

Python 3.12 or newer is required:

```sh
python3.12 -m venv .venv
.venv/bin/python -m pip install -e '.[dev]'
.venv/bin/chips bootstrap --json
.venv/bin/python -m pytest -q
```

Bootstrap is dry-run and read-only by default. It does not install, authenticate, open network
connections, start services, elevate privileges, create keys, or register workers. A repository-only
equivalent is `python3 bootstrap/chips.py bootstrap --json`.

Initialize an isolated local Supervisor explicitly:

```sh
.venv/bin/project-supervisor --data-dir ./local-state init
.venv/bin/project-supervisor --data-dir ./local-state --json status
```

No provider API key, subscription, or running model server is required for deterministic tests and
mock-mode workflows.

A future node joins by running read-only discovery, reviewing or creating its stable identity,
establishing an operator-approved trust root and authenticated encrypted transport, declaring
verified capabilities, registering Workers, and passing acceptance tests. V0.1 generates the safe
bootstrap plan and protocol records; it does not automatically perform those privileged steps.

On Windows PowerShell use `py -3.12 -m venv .venv` and executables under
`.\.venv\Scripts\` (for example `.\.venv\Scripts\chips.exe bootstrap --redact-host --json`). Windows
and Linux bootstrap discovery are contract-tested foundations; they are not yet real-host-verified
zero-touch deployments.

## Safety defaults

- SQLite and the append-only event journal are canonical; chat, UI, and Bridge payloads are not.
- REST and WebSocket observation are read-only by default.
- Services bind to loopback unless an operator supplies an authenticated private transport and TLS.
- Provider credentials remain in approved provider/OS secure storage and never enter normalized
  events or release evidence.
- Missing usage, quota, model, or cost remains explicitly unavailable rather than becoming zero.
- Local models may perform inference/classification/review, but cannot write production code.
- Capability grants are scoped, attributable, revocable, auditable data; the V0.1 Privilege Broker
  is a design foundation and executes no privileged operation.
- Public Worker/model/admin exposure, raw MCP exposure, arbitrary remote shell, hidden persistence,
  and OS-security bypass are unsupported.

## Architecture and operations

- [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md) — system, data flow, and trust boundaries
- [`docs/WORKER_PROTOCOL.md`](docs/WORKER_PROTOCOL.md) — provider-neutral Worker semantics
- [`docs/NODE_PROTOCOL.md`](docs/NODE_PROTOCOL.md) — node identity and transport semantics
- [`docs/CAPABILITY_MODEL.md`](docs/CAPABILITY_MODEL.md) — least-privilege authority model
- [`docs/SCHEDULER.md`](docs/SCHEDULER.md) — deterministic Hybrid Engine foundations
- [`docs/BOOTSTRAP.md`](docs/BOOTSTRAP.md) — portable discovery and safe plan generation
- [`docs/RECOVERY.md`](docs/RECOVERY.md) — restart, migration, and failure recovery
- [`docs/PROJECT_BRIDGE_INTEGRATION.md`](docs/PROJECT_BRIDGE_INTEGRATION.md) — optional codec boundary
- [`docs/UNKNOWN_LLM_ACCEPTANCE.md`](docs/UNKNOWN_LLM_ACCEPTANCE.md) — independent agent protocol
- [`docs/COMPATIBILITY.md`](docs/COMPATIBILITY.md) — tested versus host-verified support
- [`SECURITY.md`](SECURITY.md) — non-negotiable security model and reporting
- [`RELEASE_CHECKLIST.md`](RELEASE_CHECKLIST.md) — fail-closed publication gate

## Release boundary

Source is licensed under Apache-2.0; see [`LICENSE`](LICENSE), [`NOTICE`](NOTICE), and
[`THIRD_PARTY_NOTICES.md`](THIRD_PARTY_NOTICES.md). Private V0 evidence and local Git history contain
operator topology and are not distributable inputs. Release tooling builds a deterministic,
privacy-scanned source archive from the explicit policy in `release/public-release-files.json`.

The `v0.1.0-alpha.1` public seed is released from a privacy-audited clean source archive and a fresh
Git history. It is an alpha foundation, not a claim of production-complete autonomous deployment.
Mirroring and signing remain separate future actions.

```text
ZERO_TOUCH_DEPLOYMENT_COMPLETE      = NO
PROJECT_BRIDGE_INTEGRATION_COMPLETE = NO
```
