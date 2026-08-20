# LLM guide

`AGENTS.md` is the canonical entrypoint. This file is the concise runtime companion.

## Product boundaries

Supervisor is the headless CHIPS Agent Fabric control plane. Cyber Office is every human-facing UI.
Worker Fabric executes tasks. Project_Bridge is an optional communication plane and cannot replace
canonical SQLite/task/event/artifact state.

## Discover before acting

1. Read `FABRIC_INTENT.md`, `CAPABILITIES.md`, `PROTOCOL.md`, and the relevant ADR/protocol.
2. Run `chips-onboard --json` for a concise first-run preset, then
   `python3 bootstrap/chips.py bootstrap --json` for the complete credential-free snapshot.
3. Treat every path/model/runtime as a candidate until a safe real contract gate verifies it.
4. For existing initialized state, run `project-supervisor --json status`, `tasks`, and
   `logs --after SEQUENCE` against the operator-specified data directory.

Do not assume an installed Worker is authenticated, online, eligible, reachable, or authorized.
Bootstrap discovery does not test authentication, connectivity, listeners, or credentials.

## Local model adaptation

`project_supervisor.local_models` maps LM Studio, Ollama, llama.cpp, or another compatible runtime's
verified observations into conservative control profiles. The profile may recommend context,
parallelism, GPU offload, on-demand reasoning, roles, capabilities, and smoke checks. Unknown memory
or capacity never becomes invented headroom, and a recommendation is not applied until an operator or
approved runtime policy accepts it.

Prefer loopback endpoints. Record TTFT, throughput, memory pressure, context behavior, tool calling,
vision support, and failure modes with timestamps and freshness. Model names alone do not prove any
of these properties.

## Safe lifecycle

A task carries explicit requirements, privacy, permission class, topology, and acceptance criteria.
The deterministic scheduler applies hard constraints to one frozen snapshot, persists selected and
rejected reasons, and only then dispatches. Monitor normalized events until a terminal state and
retrieve the durable result. Worker text never mutates state or grants authority.

Add nodes/Workers using reviewed, credential-free manifests and real protocol evidence. Human or
enterprise-controlled initial trust remains legitimate. Future routine privileged actions require
authenticated capability grants and a narrow Privilege Broker; the current public version documents
this FOUNDATION but does not implement automatic privilege execution.

## Evidence and claims

Classify facts as exact, observed/provider-reported, inferred, or unavailable. A successful request
does not prove remaining subscription quota. FOUNDATION does not mean COMPLETE. Preserve only
sanitized evidence, keep local/generated state out of distribution, and follow
`docs/UNKNOWN_LLM_ACCEPTANCE.md` for an independent bootstrap assessment.
