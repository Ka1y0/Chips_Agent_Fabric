# Bootstrap guide

## Status

V0.4 implements a portable **FOUNDATION**: read-only discovery, a deterministic fail-closed plan,
review-bundle emission, and an optional durable SQLite lifecycle recorder. The recorder survives
restart, is idempotent for identical evidence, and maintains a hash-chained append-only audit.
Automated installation, cryptographic identity enrollment, private-transport setup, service
installation, Worker registration, and fleet joining are not yet complete.

## Inspect

From a clone, an unfamiliar agent should first read `AGENTS.md`, then run:

```sh
python3 bootstrap/chips.py bootstrap --json
```

After package installation, the equivalent command is `chips bootstrap --json`. Raw discovery
contains the local hostname, executable paths, and interface names and must stay machine-local. For
a shareable diagnostic projection, add `--redact-host`; this preserves capability presence and the
plan while replacing host identity and paths.

The command uses only local read-only OS/runtime inspection. It never probes ports, invokes an AI
model, checks authentication, contacts a control plane, or reads secure storage.

## Plan and emit

Review stdout, then optionally generate machine-local review material:

```sh
python3 bootstrap/chips.py bootstrap \
  --emit --output-dir /explicit/private/path/bootstrap-review --redact-host --json
```

Use a directory outside the distributable clone where practical. The command refuses a non-empty
directory and emits no credential fields. `supervisor-config.example.json` stays loopback-only and
read-only. It is a candidate, not automatically activated runtime configuration.

## Durable lifecycle foundation

Choose an explicit private path to persist one resumable bootstrap run. This writes lifecycle state
only and still performs no host operation:

```sh
chips bootstrap \
  --state-db /private/operator/path/bootstrap.sqlite3 \
  --run-id bootstrap-default \
  --json

chips bootstrap-status \
  --state-db /private/operator/path/bootstrap.sqlite3 \
  --run-id bootstrap-default \
  --json
```

Re-running with the same run ID and identical discovery/plan evidence is idempotent. Changed
evidence fails closed rather than silently replanning an active enrollment. Every mutating step
remains `awaitingApproval` until an external legitimate trust root or future Privilege Broker has
both authorized and performed its typed operation. Only then may a bounded non-secret result be
recorded:

```sh
chips bootstrap-record-result \
  --state-db /private/operator/path/bootstrap.sqlite3 \
  --result /private/operator/path/step-result.json \
  --json
```

The input must satisfy `schemas/bootstrap-step-result-v1.schema.json`. Command/shell/argv and
credential-shaped fields, model self-approval, mismatched capabilities, out-of-order steps, and
conflicting idempotency keys are rejected. An authorization reference is audit metadata—not a grant
verifier and never permission to execute. `auditChainValid` reports recorder integrity; it does not
prove the external operation was safe or successful.

## Interpret the plan

Plan statuses are `ready`, `notNeeded`, `required`, `approvalRequired`, or `blocked`; durable step
states distinguish pending, ready, review, approval, satisfied, failed, and blocked. Mutating steps
are never automatic. Discovery paths can be machine-specific because they live in generated state; no
canonical source default may embed a personal home path or private hostname.

Before enrollment, validate the trust root, transport identity/encryption, Worker protocol and
policy, secure credential storage, cancellation/timeout behavior, and restart recovery. Follow
`BOOTSTRAP_PROTOCOL.md` for the state machine and `UNKNOWN_LLM_ACCEPTANCE.md` for an independent test.

## Portable validation

Cache-free Unix validation keeps a strict evaluator from modifying the repository:

```sh
PYTHONDONTWRITEBYTECODE=1 .venv/bin/ruff check --no-cache .
PYTHONDONTWRITEBYTECODE=1 .venv/bin/pytest -q -p no:cacheprovider
python3 bootstrap/chips.py bootstrap --redact-host --json
```

### Windows PowerShell foundation

Windows discovery and packaging are contract-tested but not yet real-host verified. With Python
3.12 installed, use:

```powershell
py -3.12 -m venv .venv
.\.venv\Scripts\python.exe -m pip install -e ".[dev]"
.\.venv\Scripts\chips.exe bootstrap --redact-host --json
$env:PYTHONDONTWRITEBYTECODE="1"
.\.venv\Scripts\pytest.exe -q -p no:cacheprovider
.\.venv\Scripts\ruff.exe check --no-cache .
```

Linux follows the Unix commands above. Linux discovery is contract-tested but not yet real-host
verified for installation, service management, identity enrollment, or private transport.
