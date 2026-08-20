# LLM guide

`AGENTS.md` is the canonical entrypoint. This file is the concise runtime companion.

## Product boundaries

Supervisor is the headless CHIPS Agent Fabric control plane. Cyber Office is every human-facing UI.
Worker Fabric executes tasks. Project_Bridge is an optional communication plane and cannot replace
canonical SQLite/task/event/artifact state.

## Discover before acting

1. Read `FABRIC_INTENT.md`, `CAPABILITIES.md`, `PROTOCOL.md`, and the relevant ADR/protocol.
2. Run `python3 bootstrap/chips.py bootstrap --json` for credential-free local discovery.
3. Treat every path/model/runtime as a candidate until a safe real contract gate verifies it.
4. For existing initialized state, run `project-supervisor --json status`, `tasks`, and
   `logs --after SEQUENCE` against the operator-specified data directory.

Do not assume an installed Worker is authenticated, online, eligible, reachable, or authorized.
Bootstrap discovery does not test authentication, connectivity, listeners, or credentials.

## Safe lifecycle

A task carries explicit requirements, privacy, permission class, topology, and acceptance criteria.
The deterministic scheduler applies hard constraints to one frozen snapshot, persists selected and
rejected reasons, and only then dispatches. Monitor normalized events until a terminal state and
retrieve the durable result. Worker text never mutates state or grants authority.

Add nodes/Workers using reviewed, credential-free manifests and real protocol evidence. Human or
enterprise-controlled initial trust remains legitimate. Future routine privileged actions require
authenticated capability grants and a narrow Privilege Broker; V0.1 documents this FOUNDATION but
does not implement automatic privilege execution.

## Evidence and claims

Classify facts as exact, observed/provider-reported, inferred, or unavailable. A successful request
does not prove remaining subscription quota. FOUNDATION does not mean COMPLETE. Preserve only
sanitized evidence, keep local/generated state out of distribution, and follow
`docs/UNKNOWN_LLM_ACCEPTANCE.md` for an independent bootstrap assessment.
