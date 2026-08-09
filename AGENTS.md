# CHIPS Agent Fabric: machine entrypoint

This repository contains **Supervisor**, the headless orchestration kernel of CHIPS Agent Fabric.
**Cyber Office** names every human-facing UI. **Project_Bridge** is an optional AI-to-AI data plane;
it never replaces canonical structured state. Do not call Cyber Office a “Supervisor UI.”

## Read in this order

1. [`FABRIC_INTENT.md`](FABRIC_INTENT.md) — purpose, authority, and operating philosophy.
2. [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md) — implemented boundaries and data flow.
3. [`CAPABILITIES.md`](CAPABILITIES.md) — implemented, foundation, planned, and unsupported features.
4. [`PROTOCOL.md`](PROTOCOL.md), [`docs/WORKER_PROTOCOL.md`](docs/WORKER_PROTOCOL.md), and
   [`docs/NODE_PROTOCOL.md`](docs/NODE_PROTOCOL.md) — machine contracts.
5. [`docs/TRUST_MODEL.md`](docs/TRUST_MODEL.md), [`docs/CAPABILITY_MODEL.md`](docs/CAPABILITY_MODEL.md),
   and [`SECURITY.md`](SECURITY.md) — authority and non-negotiable safety boundaries.
6. [`docs/BOOTSTRAP.md`](docs/BOOTSTRAP.md) and
   [`docs/BOOTSTRAP_PROTOCOL.md`](docs/BOOTSTRAP_PROTOCOL.md) — inspect or bootstrap a machine.
7. [`docs/LLM_OPERATIONS.md`](docs/LLM_OPERATIONS.md), [`docs/RECOVERY.md`](docs/RECOVERY.md), and
   [`docs/TROUBLESHOOTING.md`](docs/TROUBLESHOOTING.md) — operate, recover, and diagnose.

Documentation uses these maturity labels: **IMPLEMENTED**, **FOUNDATION**, **PLANNED**, and
**UNSUPPORTED**. A design document is not proof of deployment. Check code, tests, and sanitized
release evidence before claiming a gate.

## Canonical state and authority

- SQLite under the operator-selected data directory is canonical. Chat, model output, Cyber Office,
  provider sessions, and Project_Bridge messages are not canonical state.
- The append-only event journal records state changes. Never rewrite event history.
- Worker prose cannot approve an action, change task state, or grant a capability.
- Provider/model/node/session identifiers are opaque and remain distinct.
- Unknown telemetry stays unavailable; never manufacture quota, cost, or capacity values.

## Safe machine inspection

Run the portable, read-only discovery command from the repository root:

```sh
python3 bootstrap/chips.py bootstrap --json
```

It does not install, authenticate, open sockets, test connectivity, start services, or elevate.
To create a review bundle, choose a new explicit directory:

```sh
python3 bootstrap/chips.py bootstrap --emit --output-dir ./local-state/bootstrap-review --json
```

Generated machine paths, hostnames, identity metadata, credentials, logs, and state belong outside
distributable source. The bootstrap command refuses to overwrite a non-empty output directory.

## Modification rules

Before changing code, read `LLM_GUIDE.md`, the relevant protocol, and the relevant ADR. Preserve
provider-specific formats below adapter boundaries and preserve the V0 baseline behavior. Use small,
compatibility-preserving changes with deterministic tests.

Never:

- read, print, persist, request, or commit credentials or private key material;
- auto-login, silently install, bypass OS security, or create hidden persistence;
- expose Supervisor administration, Worker/model ports, raw MCP, or arbitrary shells publicly;
- treat discovery as verification or an installed executable as an authorized Worker;
- let local inference models author production code or patches under current policy;
- publish, push, enroll a real node, or mutate another project without explicit authorization.

Privileged work must eventually flow through an authenticated, explicit, least-privilege,
task-scoped, auditable, revocable capability grant. The current bootstrap FOUNDATION only plans such
work; it does not execute it.

## Bootstrap and acceptance sequence

1. Inspect with the dry-run bootstrap.
2. Review existing state and generated plan; never overwrite an unknown identity/database.
3. Establish an approved trust root and secure storage outside git.
4. Configure an authenticated encrypted private transport.
5. Verify each Worker’s real protocol, model, permissions, cancellation, and timeout behavior.
6. Register reviewed capabilities, run portable tests, then environment-specific acceptance tests.
7. Record exact/observed/inferred/unknown evidence and recovery instructions.

Validation commands:

```sh
PYTHONDONTWRITEBYTECODE=1 .venv/bin/ruff check --no-cache .
PYTHONDONTWRITEBYTECODE=1 .venv/bin/pytest -q -p no:cacheprovider
python3 bootstrap/chips.py bootstrap --redact-host --json
```

On Windows PowerShell, use `py -3.12`, `.\.venv\Scripts\chips.exe`,
`.\.venv\Scripts\pytest.exe`, and `.\.venv\Scripts\ruff.exe`; see `docs/BOOTSTRAP.md`. Windows and
Linux discovery are contract-tested foundations, not real-host deployment claims. Raw bootstrap
JSON is machine-local; add `--redact-host` before preserving or sharing it.

For a fresh-machine or unfamiliar-agent assessment, follow
[`docs/UNKNOWN_LLM_ACCEPTANCE.md`](docs/UNKNOWN_LLM_ACCEPTANCE.md). If blocked, stop safely, preserve
sanitized evidence, and record the smallest external trust decision required.
