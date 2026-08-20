PRAGMA foreign_keys = ON;

ALTER TABLE node_runtime_recovery_policies
ADD COLUMN monitor_interval_seconds REAL NOT NULL DEFAULT 30.0
CHECK(monitor_interval_seconds BETWEEN 5.0 AND 3600.0);

ALTER TABLE node_runtime_recovery_policies
ADD COLUMN failure_backoff_seconds REAL NOT NULL DEFAULT 60.0
CHECK(failure_backoff_seconds BETWEEN 5.0 AND 86400.0);

ALTER TABLE node_runtime_recovery_policies
ADD COLUMN lease_ttl_seconds REAL NOT NULL DEFAULT 60.0
CHECK(lease_ttl_seconds BETWEEN 15.0 AND 300.0);

ALTER TABLE node_runtime_recovery_attempts ADD COLUMN lease_owner_id TEXT;
ALTER TABLE node_runtime_recovery_attempts ADD COLUMN lease_generation INTEGER;

CREATE TABLE IF NOT EXISTS node_runtime_recovery_leases (
    policy_id TEXT PRIMARY KEY REFERENCES node_runtime_recovery_policies(id) ON DELETE CASCADE,
    owner_id TEXT NOT NULL,
    generation INTEGER NOT NULL CHECK(generation > 0),
    state TEXT NOT NULL CHECK(state IN ('owned','released','lost')),
    acquired_at TEXT NOT NULL,
    heartbeat_at TEXT NOT NULL,
    expires_at TEXT NOT NULL,
    released_at TEXT,
    active_recovery_id TEXT,
    recovery_state TEXT NOT NULL CHECK(recovery_state IN (
        'fresh','resuming','staleOwnerRecovered','complete','lost'
    )),
    previous_owner_id TEXT,
    last_error TEXT
);

CREATE INDEX IF NOT EXISTS node_runtime_recovery_leases_owner
ON node_runtime_recovery_leases(owner_id, state, expires_at, policy_id);

CREATE INDEX IF NOT EXISTS node_runtime_recovery_leases_expiry
ON node_runtime_recovery_leases(state, expires_at, policy_id);

CREATE TABLE IF NOT EXISTS node_runtime_recovery_monitors (
    monitor_id TEXT PRIMARY KEY,
    process_id INTEGER NOT NULL CHECK(process_id > 0),
    state TEXT NOT NULL CHECK(state IN ('starting','running','stopping','stopped','failed')),
    started_at TEXT NOT NULL,
    heartbeat_at TEXT NOT NULL,
    stopped_at TEXT,
    active_policy_count INTEGER NOT NULL DEFAULT 0 CHECK(active_policy_count >= 0),
    observation_count INTEGER NOT NULL DEFAULT 0 CHECK(observation_count >= 0),
    recovery_count INTEGER NOT NULL DEFAULT 0 CHECK(recovery_count >= 0),
    stale_takeover_count INTEGER NOT NULL DEFAULT 0 CHECK(stale_takeover_count >= 0),
    last_error TEXT,
    metadata_json TEXT NOT NULL DEFAULT '{}'
);

CREATE INDEX IF NOT EXISTS node_runtime_recovery_monitors_state_heartbeat
ON node_runtime_recovery_monitors(state, heartbeat_at, monitor_id);

CREATE TABLE IF NOT EXISTS node_runtime_recovery_checkpoints (
    policy_id TEXT PRIMARY KEY REFERENCES node_runtime_recovery_policies(id) ON DELETE CASCADE,
    monitor_id TEXT REFERENCES node_runtime_recovery_monitors(monitor_id) ON DELETE SET NULL,
    last_recovery_id TEXT,
    last_state TEXT,
    consecutive_failures INTEGER NOT NULL DEFAULT 0 CHECK(consecutive_failures >= 0),
    last_observed_at TEXT,
    next_observation_at TEXT NOT NULL,
    last_error TEXT,
    updated_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS node_runtime_recovery_checkpoints_due
ON node_runtime_recovery_checkpoints(next_observation_at, policy_id);
