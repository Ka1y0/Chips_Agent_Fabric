PRAGMA foreign_keys = ON;

-- A dispatch claim can outlive the in-memory Runtime that created it.  Recovery may only
-- reconcile an active Task after this renewable ownership lease expires.
CREATE TABLE IF NOT EXISTS task_execution_leases (
    task_id TEXT PRIMARY KEY REFERENCES tasks(id) ON DELETE CASCADE,
    owner_id TEXT NOT NULL,
    generation INTEGER NOT NULL CHECK(generation > 0),
    state TEXT NOT NULL CHECK(state IN ('active','released')),
    acquired_at TEXT NOT NULL,
    heartbeat_at TEXT NOT NULL,
    expires_at TEXT NOT NULL,
    released_at TEXT
);

CREATE INDEX IF NOT EXISTS task_execution_leases_expiry
    ON task_execution_leases(state, expires_at);
