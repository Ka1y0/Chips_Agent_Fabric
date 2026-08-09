# Capability status

Labels: **IMPLEMENTED** is code/contract tested; **HOST-VERIFIED** adds sanitized real-host evidence;
**FOUNDATION** is a documented or partial seam not yet an end-to-end feature; **PLANNED** has no
stable implementation; **UNSUPPORTED** is deliberately excluded.

| Capability | V0.1 state | Machine interface |
|---|---|---|
| SQLite canonical state, migrations, append-only events | IMPLEMENTED | Python/store, CLI |
| Task state machine and deterministic scheduling | IMPLEMENTED | Python/domain/scheduler |
| Local initialization/status/task/event inspection | IMPLEMENTED | `project-supervisor` CLI |
| Scoped hashed observer tokens | IMPLEMENTED | CLI `token create` |
| Read-only REST/WebSocket and OpenAPI | IMPLEMENTED | `/v1/*`, `/v1/stream`, `/openapi.json` |
| Claude/Grok/AGY adapters | IMPLEMENTED; prior native host gates | adapter/runtime boundary |
| Windows Local Worker over private HTTPS | HOST-VERIFIED in V0 evidence | Local Worker v1 adapter |
| Mac→PC Tailscale transport | HOST-VERIFIED V0 topology | deployment config; not core dependency |
| Cyber Office real REST/WebSocket observation | HOST-VERIFIED V0 topology | generic projections |
| Portable discovery + deterministic dry-run plan | IMPLEMENTED FOUNDATION | `bootstrap/chips.py` |
| Generated bootstrap review bundle | IMPLEMENTED FOUNDATION | `--emit --output-dir PATH` |
| macOS/Windows/Linux bootstrap discovery | IMPLEMENTED FOUNDATION | local read-only utilities |
| Generic Worker/Node/transport semantics | IMPLEMENTED FOUNDATION | Python contracts + JSON schemas |
| Capability-scoped Privilege Broker | FOUNDATION design only | none yet |
| Automated identity enrollment/recovery | FOUNDATION design only | none yet |
| Automated install/private transport/registration | PLANNED | plan marks approval required |
| Zero-touch deployment | PLANNED; not demonstrated | none yet |
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
project-supervisor [--data-dir PATH] [--config PATH] serve [options]
chips bootstrap [--json] [--emit --output-dir PATH]
python3 bootstrap/chips.py bootstrap [--json] [--emit --output-dir PATH]
```

Only `serve` starts Supervisor. Bootstrap remains dry-run even with `--emit`; emit only writes a
review bundle. No command starts, installs, authenticates, exposes, or registers a provider Worker
implicitly. Token creation is the only listed command that emits a credential and stores only its
salted hash.
