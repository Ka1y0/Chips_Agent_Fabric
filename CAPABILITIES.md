# Capability status

Labels: **IMPLEMENTED** is code/contract tested; **HOST-VERIFIED** adds sanitized real-host evidence;
**FOUNDATION** is a documented or partial seam not yet an end-to-end feature; **PLANNED** has no
stable implementation; **UNSUPPORTED** is deliberately excluded.

Phase 2 preserves `Capability != Provider` and adds
`Capability existence != runtime executability`. A manifest describes what a Worker can provide;
only a fresh execution-plane observation proves whether it can execute that capability now. See
[`docs/MULTI_NODE_EXECUTION_PLANE.md`](docs/MULTI_NODE_EXECUTION_PLANE.md).

| Capability | Current state | Machine interface |
|---|---|---|
| SQLite canonical state, migrations, append-only events | IMPLEMENTED | Python/store, CLI |
| Task state machine and deterministic scheduling | IMPLEMENTED | Python/domain/scheduler |
| Local initialization/status/task/event inspection | IMPLEMENTED | `project-supervisor` CLI |
| Scoped hashed observer tokens | IMPLEMENTED | CLI `token create` |
| Read-only REST/WebSocket and OpenAPI | IMPLEMENTED | `/v1/*`, `/v1/stream`, `/openapi.json` |
| Codex/Claude/Grok/AGY native adapters | IMPLEMENTED; deterministic process gates | adapter/runtime boundary |
| Local Worker protocol v2 durable launch/reconcile | IMPLEMENTED; real SIGKILL acceptance | `/v2/launches`, durable registry |
| Registered Local Worker driver runtime | IMPLEMENTED; production-like fake CLI acceptance | loopback daemon + server-owned profiles |
| Codex Local Worker driver | IMPLEMENTED; offline production-path acceptance | registered `codex` profile |
| Windows Local Worker over private HTTPS | HOST-VERIFIED in V0 evidence | Local Worker v1 adapter |
| Mac→PC Tailscale transport | HOST-VERIFIED V0 topology | deployment config; not core dependency |
| Cyber Office real REST/WebSocket observation | HOST-VERIFIED V0 topology | generic projections |
| Portable discovery + deterministic dry-run plan | IMPLEMENTED FOUNDATION | `bootstrap/chips.py` |
| Generated bootstrap review bundle | IMPLEMENTED FOUNDATION | `--emit --output-dir PATH` |
| Restart-safe bootstrap lifecycle/audit recorder | IMPLEMENTED FOUNDATION | `--state-db`, `bootstrap-status`, result v1 contract |
| macOS/Windows/Linux bootstrap discovery | IMPLEMENTED FOUNDATION | local read-only utilities |
| Generic Worker/Node/transport semantics | IMPLEMENTED FOUNDATION | Python contracts + JSON schemas |
| Provider-independent capability catalog, immutable Worker manifests, dynamic observations, and capability-aware routing | IMPLEMENTED V0.3 development slice | Python/domain/scheduler + SQLite + `/v1/fabric/capabilities`, `/v1/fabric/routing` |
| Closed-loop semantic interaction, resource leases, trajectories, skill hints, and UI graph | IMPLEMENTED deterministic offline vertical slice | `InteractionWorkerAdapter` + SQLite + read-only semantic projections |
| Real OS/browser/accessibility/local-parser/VLM interaction backend | PLANNED; not demonstrated | contract seams only |
| Fenced node-runtime recovery monitor | IMPLEMENTED control plane; deployment FOUNDATION | SQLite lease/checkpoint + typed adapter schema |
| Capability-scoped Privilege Broker | FOUNDATION design only | none yet |
| Automated identity enrollment/recovery | FOUNDATION design only | none yet |
| Automated install/private transport/registration | FOUNDATION; external executor absent | plan/result contracts keep approval required |
| Zero-touch deployment | MATERIAL FOUNDATION; not demonstrated | durable lifecycle only |
| Project_Bridge integration | separately audited foundation | integration report/document |
| Public/ordinary-LAN Worker/model/admin exposure | UNSUPPORTED | rejected by policy |
| Arbitrary remote shell or hidden persistence | UNSUPPORTED | none |

## CLI discovery

```text
project-supervisor [--data-dir PATH] [--config PATH] [--json] init [--force]
project-supervisor [--data-dir PATH] [--config PATH] [--json] status
project-supervisor [--data-dir PATH] [--config PATH] [--json] tasks [filters]
project-supervisor [--data-dir PATH] [--config PATH] [--json] logs [cursor/filter]
project-supervisor [--data-dir PATH] [--config PATH] [--json] token create [options]
project-supervisor [--data-dir PATH] [--config PATH] [--json] project create [options]
project-supervisor [--data-dir PATH] [--config PATH] [--json] task create [options]
project-supervisor ... autonomous run|serve --local-worker-endpoint WORKER=URL \
  --local-worker-driver WORKER=DRIVER_ID
project-supervisor [--data-dir PATH] [--config PATH] serve [options]
chips bootstrap [--json] [--emit --output-dir PATH] [--state-db PATH --run-id ID]
chips bootstrap-status --state-db PATH [--run-id ID] [--json]
chips bootstrap-record-result --state-db PATH --result FILE [--json]
python3 bootstrap/chips.py bootstrap [same options]
```

Only `serve` starts Supervisor. Bootstrap remains a host-operation dry-run even with `--emit` or a
durable state database; those options write only review/lifecycle state. Recording a result never
executes the referenced operation. No command starts, installs, authenticates, exposes, or registers
a provider Worker
implicitly. Token creation is the only listed command that emits a credential and stores only its
salted hash.

The bundled production Local Worker daemon is loopback-only and requires an owner-only 0600 bearer
file. Its HTTP contract accepts a registered driver ID and bounded semantic inference request, never
an executable, shell string, argv, environment, working directory, plugin, or arbitrary URL. Native
driver profiles are operator-owned files loaded at daemon startup; the deterministic TestDriver and
all test seams are rejected by `--production`.

Codex availability means only that a reviewed executable profile is intact; authentication,
subscription plan, quota, and provider-native resume remain unknown until the CLI reports them.

## Fabric naming boundaries

A capability is a provider-independent description of work, not a provider name and not an
authorization grant. A Worker binds an adapter/runtime to declared capabilities; permission,
privacy, approval, health, quota, and locality remain separate routing inputs. See
[`docs/CAPABILITY_FABRIC.md`](docs/CAPABILITY_FABRIC.md).

An Interaction channel is not a Worker. It identifies the observation/action medium used by a
scheduled semantic interaction execution. The current end-to-end acceptance uses a deterministic
local fixture and does not demonstrate control of a real OS, browser, screen parser, LLM, or VLM.
See [`docs/INTERACTION_FABRIC.md`](docs/INTERACTION_FABRIC.md).
