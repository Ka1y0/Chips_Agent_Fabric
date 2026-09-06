# Personal Intelligence Fabric direction

Status labels in this document are **IMPLEMENTED FOUNDATION**, **PLANNED INTEGRATION**, and
**OUT OF SCOPE**. A contract or deterministic planner is not proof of a deployed autonomous system.

## North star

CHIPS Agent Fabric should become a user-controlled intelligence layer above models, devices,
providers, and local compute. A person supplies an intent; Fabric decides which capabilities are
needed, assigns bounded work, preserves evidence, verifies outcomes, and returns for human authority
only when necessary.

Models remain replaceable resources. Canonical state, permissions, identity, audit evidence, and the
right to pause, halt, steer, or stop remain with the user.

## 1. Quiet installation and simple onboarding

**IMPLEMENTED FOUNDATION:** `chips-onboard` converts read-only machine discovery into a concise
preset, safe setup steps, approval-gated steps, detected Worker candidates, and first-run commands.
It performs no installation, login, network probe, service start, privilege escalation, or credential
read.

**PLANNED INTEGRATION:** a signed package installer and platform service executor may complete only
predeclared steps. Every mutation must be attributable, resumable, idempotent, and reversible. Existing
state must never be overwritten silently.

## 2. Hybrid Engine

**IMPLEMENTED FOUNDATION:** `HybridEngine` turns a workload into an explainable single, review,
panel, escalation, or DAG-cluster plan. Concrete Worker selection remains the scheduler's job. Privacy
never silently degrades from local to remote.

**PLANNED INTEGRATION:** feed live capability, quota, subscription, latency, reliability, context, and
resource observations into each stage. Persist the selected topology and reasons before dispatch.

## 3. Local LLM automatic adaptation and control

**IMPLEMENTED FOUNDATION:** local runtime discovery recognizes LM Studio, Ollama, llama.cpp, and
explicitly configured loopback OpenAI-compatible candidates without opening sockets. Verified model
observations can be converted into conservative context, parallelism, GPU-offload, reasoning, role,
capability, and health-check profiles.

Unknown capacity never becomes invented headroom. Thinking or reasoning is on-demand by default.
Profiles are recommendations until a real loopback probe and operator policy accept them.

**PLANNED INTEGRATION:** connect the profile model to `lmstudio-mcp`, Local Worker V2, and equivalent
provider-neutral probes. Record measured TTFT, throughput, memory pressure, context behavior, tool
calling, vision support, and failure modes as evidence with freshness.

## 4. Larger and more efficient Agent clusters

**IMPLEMENTED FOUNDATION:** cluster blueprints decompose work into typed roles and bounded replicas.
Complex computer use is split into fast visual grounding, stronger planning, atomic action, independent
visual review, and synthesis. The cluster layer expands the bounded DAG and reviewer-independence
constraints; it does not choose Workers. The canonical scheduler separately applies capabilities,
leases, locality, quotas, latency, quality, cost, reliability, and stable tie-breaking to each stage.

**PLANNED INTEGRATION:** connect stages to durable DAG tasks, leases, cancellation fences, quotas, and
node-locality policy. Scale by adding Workers, not by allowing unbounded recursive spawning.

## 5. Bridge information sharing

**IMPLEMENTED FOUNDATION:** Bridge remains optional, non-authoritative, bounded, hashed, and
losslessly reversible. The semantic policy can select natural language, structured state, symbolic
form, or lossless transfer from receiver prior, novelty, exactness, integrity, and byte budget.

Support accounting is typed as:

- `transmitted`: supported directly by transferred source material;
- `prior`: already available to the receiver;
- `synergistic`: produced by combining transferred and prior information;
- `invented`: unsupported by either source;
- `contradicted`: conflicts with source evidence.

Confidence never substitutes for hashes, signatures, or canonical source retention.

**PLANNED INTEGRATION:** attach support ledgers to verified inter-Worker exchanges and compare Bridge
against an unchanged structured fallback. Preserve negative results. Do not repeatedly tune a codec
until it wins a benchmark.

## Reconciled architecture boundaries

The public foundations are additive to the newer local Fabric and follow these boundaries:

1. preserve the newer canonical state, capability catalog, interaction loop, and runtime code;
2. retain public CI and cross-platform tests;
3. map onboarding and local-model contracts onto the newer bootstrap and capability APIs;
4. connect cluster blueprints to the newer parallel/recursive governance layer;
5. connect semantic Bridge accounting without granting Bridge canonical authority.

## Permanent boundaries

Fabric must not expose ordinary-LAN or public administrative/model endpoints by default, copy provider
credentials between machines, hide privileged mutations, treat generated text as approval, or claim a
model capability that has not been observed. Pause, halt, steer, audit, and human authority are part of
the architecture, not emergency decorations.
