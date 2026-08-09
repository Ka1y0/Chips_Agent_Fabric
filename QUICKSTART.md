# Quickstart

Supervisor is local-first and starts with a loopback-only, read-only API configuration. The
current V0 command surface initializes and inspects durable state; task dispatch is enabled only by
the runtime components documented in `CAPABILITIES.md`.

## Install for development

Python 3.12 or newer is required.

```sh
python3.12 -m venv .venv
.venv/bin/python -m pip install -e '.[dev]'
.venv/bin/project-supervisor --data-dir ./local-state init
.venv/bin/project-supervisor --data-dir ./local-state status
```

`local-state/` contains project-owned configuration, SQLite state, and future runtime logs. Keep it
private and out of source control. `init` never logs into a provider, installs a worker CLI, exposes
a port, or starts a daemon.

## Inspect state

```sh
.venv/bin/project-supervisor --data-dir ./local-state tasks
.venv/bin/project-supervisor --data-dir ./local-state logs --after 0 --limit 100
.venv/bin/project-supervisor --data-dir ./local-state --json status
```

`--json` is intended for agents and scripts. Events use their monotonic `sequence` as a resume
cursor.

## Create a project and typed task

```sh
.venv/bin/project-supervisor --data-dir ./local-state project create \
  --id demo --name Demo --root "$PWD" --goal "Analyze a fixture"

.venv/bin/project-supervisor --data-dir ./local-state task create \
  --project demo --title "Review architecture" \
  --description "Read-only architecture review" \
  --label architecture --capability architecture-review --topology fallback
```

This queues durable work but does not implicitly start provider processes. Runtime adapter
registration is an explicit operator/integrator step.

## Create a monitor token

```sh
.venv/bin/project-supervisor --data-dir ./local-state token create \
  --label cyber-office --scope observe:read
```

The bearer token is displayed once and stored only as a salted hash. Put the displayed value in the
client machine's OS credential manager. Never paste it into logs, chat, source files, or manifests.

## Network safety

The default is `127.0.0.1:7330`. A non-loopback host is rejected unless configuration explicitly
declares an authenticated private transport and supplies both a TLS certificate and private key.
Supervisor does not provision that transport automatically.

For loopback development only, start the read-only API explicitly:

```sh
.venv/bin/project-supervisor --data-dir ./local-state serve \
  --allow-unauthenticated-loopback
```
