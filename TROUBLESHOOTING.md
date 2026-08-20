# Troubleshooting

## `state database not initialized`

Run `project-supervisor --data-dir <private-path> init`, then use the same `--data-dir` for later
commands. `status`, `tasks`, and `logs` refuse to create state implicitly.

## `configuration already exists`

Initialization is idempotence-safe and refuses to overwrite configuration. Inspect the existing
file. Use `init --force` only when intentionally resetting configuration; it does not delete the
SQLite database.

## Non-loopback configuration is rejected

Remote listening needs all of the following: a non-public authenticated private transport,
`private_transport: true`, and existing TLS certificate/private-key paths. Do not work around this by
binding broadly or disabling validation.

## Worker is installed but reports authentication failure

Run the provider's official interactive login manually in the same normal user context that will run
the worker. A sandboxed subprocess may not see an otherwise valid user login. Approve only the narrow
worker command required; do not print or inspect credential stores.

## Events appear duplicated

Delivery is at-least-once. Consumers must persist the highest processed `sequence`, request events
strictly after it, and deduplicate by `sequence` or `event_id`.

## Usage or quota is unknown

This is expected when a provider does not emit authoritative metadata. Do not derive a subscription
percentage from per-request tokens or cost. Preserve the explicit unavailable reason.

## SQLite is locked

Only one dispatching Supervisor instance should own a data directory. Stop the duplicate instance
cleanly. Do not delete WAL files manually; preserve the directory and gather sanitized diagnostics.

## Collect safe diagnostics

Use `project-supervisor --json status` and `project-supervisor --json logs --limit 100`. Review output
before sharing it. Never attach provider auth directories, environment dumps, Keychain output, or raw
worker streams without credential-aware sanitization.
