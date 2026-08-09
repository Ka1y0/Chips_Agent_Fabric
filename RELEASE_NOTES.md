# CHIPS Agent Fabric v0.1.0-alpha.1

This is the first public alpha release of CHIPS Agent Fabric: a portable, model-independent
orchestration fabric for heterogeneous AI Workers, local compute, and distributed agent systems.

## Included

- The headless Supervisor kernel with canonical SQLite state and append-only events.
- A provider-neutral Worker contract and adapters for Claude Code, Grok Build, AGY, and a local
  Worker protocol where supported by the installed environment.
- Deterministic scheduling and Hybrid Engine routing foundations.
- Read-only REST/WebSocket observation consumed by Cyber Office, the separately named human-facing
  application layer.
- Capability, trust, node identity, and private-transport abstractions.
- Dry-run-first portable bootstrap discovery and machine-facing operating documentation.
- An Unknown-LLM acceptance protocol and deterministic release/audit tooling.
- A default-off Project_Bridge integration foundation with bounded validation and structured
  fallback.

The private V0 development environment previously demonstrated a real cross-device workflow over
authenticated private HTTPS to a loopback-only local GPU Worker. No private topology, runtime state,
credential, or Git history is included in this public release.

## Alpha limitations

```text
ZERO_TOUCH_DEPLOYMENT_COMPLETE      = NO
PROJECT_BRIDGE_INTEGRATION_COMPLETE = NO
```

Automated enrollment, Privilege Broker execution, cross-platform service installation, full
multi-host recovery, and a production Project_Bridge codec are foundations or future work. Public
source publication does not expose a Supervisor, Worker, local model server, raw MCP endpoint, or
private transport.

See `AGENTS.md`, `FABRIC_INTENT.md`, `README.md`, and `SECURITY.md` before operating or extending the
Fabric.
