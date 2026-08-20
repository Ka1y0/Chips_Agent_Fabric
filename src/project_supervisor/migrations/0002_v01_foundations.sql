PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS execution_history (
    id TEXT PRIMARY KEY,
    task_id TEXT NOT NULL REFERENCES tasks(id) ON DELETE CASCADE,
    task_type TEXT NOT NULL,
    worker_id TEXT NOT NULL REFERENCES workers(id),
    provider TEXT NOT NULL,
    model TEXT NOT NULL,
    node_id TEXT NOT NULL REFERENCES nodes(id),
    topology TEXT NOT NULL,
    latency_seconds REAL NOT NULL CHECK(latency_seconds >= 0),
    succeeded INTEGER NOT NULL CHECK(succeeded IN (0,1)),
    failure_class TEXT,
    retry_count INTEGER NOT NULL DEFAULT 0 CHECK(retry_count >= 0),
    input_tokens INTEGER CHECK(input_tokens IS NULL OR input_tokens >= 0),
    output_tokens INTEGER CHECK(output_tokens IS NULL OR output_tokens >= 0),
    cost_usd REAL CHECK(cost_usd IS NULL OR cost_usd >= 0),
    review_outcome TEXT,
    human_accepted INTEGER CHECK(human_accepted IS NULL OR human_accepted IN (0,1)),
    recorded_at TEXT NOT NULL,
    CHECK(NOT (succeeded = 1 AND failure_class IS NOT NULL))
);

CREATE INDEX IF NOT EXISTS execution_history_task_time
ON execution_history(task_id, recorded_at, id);

CREATE INDEX IF NOT EXISTS execution_history_worker_time
ON execution_history(worker_id, recorded_at, id);

CREATE TABLE IF NOT EXISTS capability_grants (
    id TEXT PRIMARY KEY,
    capability TEXT NOT NULL,
    subject TEXT NOT NULL,
    requested_by TEXT NOT NULL,
    task_id TEXT NOT NULL REFERENCES tasks(id) ON DELETE CASCADE,
    issued_by TEXT NOT NULL,
    issued_at TEXT NOT NULL,
    expires_at TEXT,
    state TEXT NOT NULL CHECK(state IN ('active','revoked','expired')),
    constraints_json TEXT NOT NULL DEFAULT '{}',
    revoked_at TEXT,
    revoked_by TEXT,
    CHECK(expires_at IS NULL OR expires_at > issued_at),
    CHECK((state = 'revoked' AND revoked_at IS NOT NULL AND revoked_by IS NOT NULL) OR
          (state <> 'revoked' AND revoked_at IS NULL AND revoked_by IS NULL))
);

CREATE INDEX IF NOT EXISTS capability_grants_subject_state
ON capability_grants(subject, state, expires_at);

CREATE TABLE IF NOT EXISTS node_public_identities (
    node_id TEXT NOT NULL REFERENCES nodes(id) ON DELETE CASCADE,
    key_id TEXT NOT NULL,
    algorithm TEXT NOT NULL,
    public_key_fingerprint TEXT NOT NULL UNIQUE,
    created_at TEXT NOT NULL,
    rotated_from_key_id TEXT,
    PRIMARY KEY(node_id, key_id),
    CHECK(length(public_key_fingerprint) = 71 AND
          substr(public_key_fingerprint, 1, 7) = 'sha256:' AND
          substr(public_key_fingerprint, 8) NOT GLOB '*[^0-9a-f]*'),
    CHECK(rotated_from_key_id IS NULL OR rotated_from_key_id <> key_id)
);
