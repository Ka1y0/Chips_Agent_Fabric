PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS autonomous_hosts (
    host_id TEXT PRIMARY KEY,
    process_id INTEGER NOT NULL CHECK(process_id > 0),
    state TEXT NOT NULL CHECK(state IN ('starting','running','stopping','stopped','failed')),
    started_at TEXT NOT NULL,
    heartbeat_at TEXT NOT NULL,
    stopped_at TEXT,
    active_goal_count INTEGER NOT NULL DEFAULT 0 CHECK(active_goal_count >= 0),
    last_error TEXT,
    metadata_json TEXT NOT NULL DEFAULT '{}'
);

CREATE INDEX IF NOT EXISTS autonomous_hosts_state_heartbeat
ON autonomous_hosts(state, heartbeat_at, host_id);

CREATE TABLE IF NOT EXISTS autonomous_goal_leases (
    goal_id TEXT PRIMARY KEY REFERENCES autonomous_goals(id) ON DELETE CASCADE,
    host_id TEXT NOT NULL REFERENCES autonomous_hosts(host_id),
    generation INTEGER NOT NULL CHECK(generation > 0),
    state TEXT NOT NULL CHECK(state IN ('owned','released','lost')),
    acquired_at TEXT NOT NULL,
    heartbeat_at TEXT NOT NULL,
    expires_at TEXT NOT NULL,
    released_at TEXT,
    current_iteration_id TEXT REFERENCES autonomous_iterations(id) ON DELETE SET NULL,
    current_action_id TEXT REFERENCES autonomous_actions(id) ON DELETE SET NULL,
    in_flight_state TEXT,
    recovery_state TEXT NOT NULL CHECK(recovery_state IN (
        'fresh','resuming','staleOwnerRecovered','complete','lost'
    )),
    previous_host_id TEXT,
    last_error TEXT
);

CREATE INDEX IF NOT EXISTS autonomous_goal_leases_owner
ON autonomous_goal_leases(host_id, state, expires_at, goal_id);

CREATE INDEX IF NOT EXISTS autonomous_goal_leases_expiry
ON autonomous_goal_leases(state, expires_at, goal_id);
