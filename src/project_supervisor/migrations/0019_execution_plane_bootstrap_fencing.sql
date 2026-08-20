PRAGMA foreign_keys = ON;

-- A capacity wait may nominate the same Node from multiple Supervisor processes.  The mutable
-- lease is the single-flight owner; the generation is also enforced by the node-side typed broker.
CREATE TABLE IF NOT EXISTS node_execution_recovery_leases (
    binding_id TEXT PRIMARY KEY REFERENCES node_transport_bindings(id) ON DELETE RESTRICT,
    owner_id TEXT NOT NULL,
    generation INTEGER NOT NULL CHECK(generation > 0),
    state TEXT NOT NULL CHECK(state IN ('active','released')),
    acquired_at TEXT NOT NULL,
    heartbeat_at TEXT NOT NULL,
    expires_at TEXT NOT NULL,
    released_at TEXT
);

-- Requests are immutable and contain only typed Fabric service authority.  In particular there is
-- no executable, command, shell, argv, cwd, environment, credential, listener or firewall field.
CREATE TABLE IF NOT EXISTS node_execution_recovery_attempts (
    id TEXT PRIMARY KEY,
    binding_id TEXT NOT NULL REFERENCES node_transport_bindings(id) ON DELETE RESTRICT,
    binding_generation INTEGER NOT NULL CHECK(binding_generation > 0),
    task_id TEXT NOT NULL REFERENCES tasks(id) ON DELETE RESTRICT,
    requirements_sha256 TEXT NOT NULL CHECK(length(requirements_sha256)=64),
    authorization_id TEXT NOT NULL REFERENCES authorization_envelopes(id) ON DELETE RESTRICT,
    authorization_version INTEGER NOT NULL CHECK(authorization_version > 0),
    authorization_sha256 TEXT NOT NULL CHECK(length(authorization_sha256)=64),
    owner_id TEXT NOT NULL,
    lease_generation INTEGER NOT NULL CHECK(lease_generation > 0),
    operation TEXT NOT NULL CHECK(operation='fabric.runtime.start'),
    broker_authority_id TEXT NOT NULL,
    broker_registry_id TEXT NOT NULL,
    service_profile_revision INTEGER NOT NULL CHECK(service_profile_revision > 0),
    service_profile_sha256 TEXT NOT NULL CHECK(length(service_profile_sha256)=64),
    idempotency_key_sha256 TEXT NOT NULL CHECK(length(idempotency_key_sha256)=64),
    request_sha256 TEXT NOT NULL CHECK(length(request_sha256)=64),
    requested_at TEXT NOT NULL,
    deadline_at TEXT NOT NULL,
    created_at TEXT NOT NULL,
    UNIQUE(binding_id,idempotency_key_sha256)
);

CREATE TABLE IF NOT EXISTS node_execution_recovery_events (
    id TEXT PRIMARY KEY,
    attempt_id TEXT NOT NULL REFERENCES node_execution_recovery_attempts(id) ON DELETE RESTRICT,
    ordinal INTEGER NOT NULL CHECK(ordinal > 0),
    event_key TEXT NOT NULL,
    event_sha256 TEXT NOT NULL CHECK(length(event_sha256)=64),
    stage TEXT NOT NULL CHECK(stage IN (
        'requested','accepted','rejected','failed','verified','outcomeUnknown'
    )),
    result_generation INTEGER CHECK(result_generation IS NULL OR result_generation > 0),
    reason_code TEXT,
    observed_at TEXT NOT NULL,
    created_at TEXT NOT NULL,
    UNIQUE(attempt_id,ordinal),
    UNIQUE(attempt_id,event_key)
);

CREATE INDEX IF NOT EXISTS node_execution_recovery_attempts_binding
ON node_execution_recovery_attempts(binding_id,requested_at);

CREATE TRIGGER IF NOT EXISTS node_execution_recovery_attempts_no_update
BEFORE UPDATE ON node_execution_recovery_attempts
BEGIN SELECT RAISE(ABORT, 'Node execution recovery attempts are immutable'); END;
CREATE TRIGGER IF NOT EXISTS node_execution_recovery_attempts_no_delete
BEFORE DELETE ON node_execution_recovery_attempts
BEGIN SELECT RAISE(ABORT, 'Node execution recovery attempts are immutable'); END;
CREATE TRIGGER IF NOT EXISTS node_execution_recovery_events_no_update
BEFORE UPDATE ON node_execution_recovery_events
BEGIN SELECT RAISE(ABORT, 'Node execution recovery events are append-only'); END;
CREATE TRIGGER IF NOT EXISTS node_execution_recovery_events_no_delete
BEFORE DELETE ON node_execution_recovery_events
BEGIN SELECT RAISE(ABORT, 'Node execution recovery events are append-only'); END;
