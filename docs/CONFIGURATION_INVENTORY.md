# Configuration inventory

Supervisor has three distinct configuration classes. They are intentionally not
interchangeable.

## 1. Runtime configuration

The runtime reads `DATA_DIR/config.json`. `project-supervisor init` creates this file with restrictive
local defaults and mode `0600` where supported.

Safe generated example:

```json
{
  "host": "127.0.0.1",
  "log_level": "INFO",
  "port": 7330,
  "private_transport": false,
  "read_only_api": true,
  "tls_certificate": null,
  "tls_private_key": null
}
```

Runtime JSON keys are snake_case. Unknown keys are rejected. The runtime does not accept provider
credentials in this object.

| Key | Type/default | Environment override | Validation / meaning |
|---|---|---|---|
| `host` | string, `127.0.0.1` | `PROJECT_SUPERVISOR_HOST` | Non-loopback requires private transport and TLS |
| `port` | integer, `7330` | `PROJECT_SUPERVISOR_PORT` | 1–65535 |
| `private_transport` | boolean, `false` | `PROJECT_SUPERVISOR_PRIVATE_TRANSPORT` | Operator assertion/guardrail input, not proof of route security |
| `tls_certificate` | path or null | `PROJECT_SUPERVISOR_TLS_CERTIFICATE` | Must be paired with key and exist when validation is active |
| `tls_private_key` | path or null | `PROJECT_SUPERVISOR_TLS_PRIVATE_KEY` | Must be paired with certificate and exist when validation is active |
| `read_only_api` | boolean, `true` | `PROJECT_SUPERVISOR_READ_ONLY_API` | Safe V0 API posture |
| `log_level` | enum, `INFO` | `PROJECT_SUPERVISOR_LOG_LEVEL` | `CRITICAL`, `ERROR`, `WARNING`, `INFO`, or `DEBUG` |

`PROJECT_SUPERVISOR_DATA_DIR` selects the state directory; it is not stored inside `config.json`.
The CLI `--data-dir` option overrides that selection. `--config` selects an explicit runtime JSON
file for read operations. Field-level environment variables override values from that JSON file.

`.env.example` is documentation only. Supervisor does not auto-load `.env`; an operator who
chooses environment configuration must inject it through a trusted process/service manager.

## 2. Review and enrollment manifests

Files in `manifests/` are sanitized, non-operative review templates. They use lowerCamelCase external
contract naming and are not consumed by `load_config`.

| File | Purpose | Applied automatically? | Secret allowed? |
|---|---|---|---|
| `manifests/supervisor.example.json` | Review proposed listener/security posture | No | No |
| `manifests/node.example.json` | Describe a candidate node after verification | No | No |
| `manifests/worker.example.json` | Describe a candidate worker/capability policy | No | No |

In particular, do not copy `supervisor.example.json` over `DATA_DIR/config.json`: keys such as
`privateTransport` and `readOnlyAPI` will be rejected because runtime configuration requires
`private_transport` and `read_only_api`.

Every manifest keeps `containsCredentials: false`. Executable paths, hostnames, model identifiers,
and capabilities must reflect an isolated verification, not discovery alone. Local-model workers
must retain `codeWriteAllowed: false`.

## 3. Provider and client authentication

Provider authentication remains in each provider's official user-controlled credential store.
Supervisor may invoke an already-authenticated CLI but must not copy, inspect, print, or
persist its OAuth token, cookie, API key, or Keychain value.

Remote Local Worker configuration is split deliberately: the Tailscale HTTPS hostname is a normal
operator-supplied endpoint, while the Worker bearer stays in an OS credential manager and is passed
directly to `LocalWorkerAdapter`. The adapter permits unauthenticated HTTP only for loopback
development. Every non-loopback endpoint requires HTTPS plus a non-empty bearer; redirects and URL
credentials remain prohibited.

| Item | Location | Kind |
|---|---|---|
| Worker endpoint | `--base-url` argument to the acceptance/gate scripts | configuration |
| Worker bearer | Keychain service `project-supervisor-local-worker`, synthetic account `worker-node-01` | secret |
| Worker bearer (transient) | `PROJECT_SUPERVISOR_LOCAL_WORKER_TOKEN` in one process environment | secret |

The bearer never appears in `config.json`, `.env`, manifests, evidence, or the event journal.
`project_supervisor.credentials.local_worker_token` is the only supported reader.

Supervisor observer bearer tokens are created with:

```sh
project-supervisor --data-dir /private/state token create \
  --label cyber-office --scope observe:read
```

Only the salted hash is persisted. The one-time plaintext result belongs in the client's OS
credential manager, never `.env`, a manifest, an evidence bundle, a command transcript, or source
control.

## State and generated files

| Path | Owner | Backup/release treatment |
|---|---|---|
| `DATA_DIR/config.json` | Operator | Private; do not package real deployment values |
| `DATA_DIR/supervisor.db` and WAL files | Supervisor | Private canonical state; use a consistent SQLite backup procedure |
| `local-state/`, `logs/`, `worktrees/` | Local runtime | Git-ignored; never release verbatim |
| `artifacts/goal-run/` | Release evidence | Only schema-conformant sanitized evidence is eligible |

## Non-loopback example policy

The Supervisor API remains loopback for the V0 Mac-to-PC workflow; Cyber Office connects locally to
the Mac, so no non-loopback Supervisor listener is required. The outbound Windows Local Worker URL
uses the tailnet-only HTTPS name emitted by Tailscale Serve. If a future Supervisor listener itself
uses a private address, it still requires `private_transport: true` and both TLS paths. Passing
validation is necessary but not sufficient: the release checklist also requires overlay identity,
firewall, certificate lifecycle, scoped-token, reconnect, and no-public-exposure evidence.
