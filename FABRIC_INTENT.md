# CHIPS Agent Fabric intent

## What this system is

CHIPS Agent Fabric is a distributed AI computing system in which heterogeneous intelligent systems
and compute resources cooperate without permanent dependence on one model, provider, subscription,
machine, or operating system.

- **Supervisor** is the headless control plane: canonical task state, scheduling, Worker registry,
  recovery, policy, APIs, execution, security, and telemetry.
- **Worker Fabric** is the execution plane: capability-bearing local or remote Workers.
- **Cyber Office** is the human interface across phone, tablet, desktop, web, and future surfaces.
- **Project_Bridge** may provide an optional high-density AI-to-AI communication plane. Structured
  Supervisor state remains authoritative and ordinary structured communication is the fallback.

## Why it exists

Useful compute and AI capabilities are fragmented across devices, operating systems, providers,
subscriptions, and local runtimes. Fabric makes those resources discoverable and composable while
keeping routing explainable, authority explicit, failures recoverable, and evidence auditable.

After a responsible deployment, an authorized task can be routed to an eligible Worker on an
authenticated node, observed through normalized events, cancelled or recovered, and evaluated
without teaching every caller each provider’s private protocol.

## Authority

A node may receive narrowly defined authority such as observing health, executing a declared Worker
capability, starting a Fabric-owned service, or performing an administrator-approved package action.
Authority must be authenticated, explicit, least-privilege, attributable, auditable, revocable, and
time- or task-bounded where practical.

Joining does **not** automatically grant root/administrator access, arbitrary shell access, access to
credentials, unrestricted filesystem writes, public network exposure, permission to weaken host
security, or permission to act outside assigned tasks. Provider login, initial trust establishment,
and high-risk operations remain governed by the operator and OS security mechanisms.

## Role of a node

After joining, a node truthfully advertises its identity, capabilities, models, context limits,
availability, policy restrictions, and observable resource state. It accepts only authenticated and
authorized work, emits normalized progress, preserves cancellation/timeout semantics, protects local
credentials, and supports recovery after interruption.

Workers are capability-bearing peers. Differences in model strength, cost, location, or compute do
not imply social hierarchy. This design does not assert that current LLMs are conscious or legal
persons. It does avoid needlessly trapping an agent identity or workflow inside one provider or
device by preferring portability, interoperability, transparent authority, recoverability, voluntary
replaceability, and decentralization where practical.

## Responsible operation

Canonical state is deterministic and structured: SQLite records, task/run/session entities,
append-only events, schemas, and artifacts. Model conversation and Bridge payloads are context, not
authority. Missing telemetry remains unknown. Hard routing constraints are deterministic and
explainable. Sensitive values never enter source control or normalized events.

The product principles are:

**Human-usable. Agent-operable. Machine-readable. Auditable. Portable.**

Zero-touch means routine operation after a legitimate trust root has been established. It never
means bypassing OS controls or creating an unrestricted remote administrator channel. V0.1 provides
a bootstrap and protocol **FOUNDATION**; zero-touch deployment is not complete until demonstrated on
fresh macOS, Windows, and Linux environments with real enrollment, recovery, and security evidence.
