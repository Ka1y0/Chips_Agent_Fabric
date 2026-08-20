PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS node_runtime_recovery_policies (
    id TEXT PRIMARY KEY,
    node_id TEXT NOT NULL REFERENCES nodes(id) ON DELETE CASCADE,
    runtime_id TEXT NOT NULL,
    backend_endpoint TEXT NOT NULL,
    required_capability TEXT NOT NULL,
    expected_models_json TEXT NOT NULL DEFAULT '[]',
    worker_ids_json TEXT NOT NULL DEFAULT '[]',
    enabled INTEGER NOT NULL DEFAULT 0 CHECK(enabled IN (0,1)),
    max_attempts INTEGER NOT NULL DEFAULT 1 CHECK(max_attempts BETWEEN 1 AND 3),
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    UNIQUE(node_id, runtime_id)
);

CREATE TABLE IF NOT EXISTS node_runtime_recovery_attempts (
    id TEXT PRIMARY KEY,
    policy_id TEXT NOT NULL REFERENCES node_runtime_recovery_policies(id) ON DELETE CASCADE,
    node_id TEXT NOT NULL REFERENCES nodes(id) ON DELETE CASCADE,
    runtime_id TEXT NOT NULL,
    state TEXT NOT NULL CHECK(state IN (
        'observedHealthy','degraded','permissionRequired','recovering','verifying','ready','failed'
    )),
    trigger_reason TEXT NOT NULL,
    requested_by TEXT NOT NULL,
    authorization_ref TEXT,
    attempt_count INTEGER NOT NULL DEFAULT 0 CHECK(attempt_count BETWEEN 0 AND 3),
    before_health_json TEXT NOT NULL,
    after_health_json TEXT,
    failure_code TEXT,
    failure_detail TEXT,
    started_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    finished_at TEXT,
    CHECK((state IN ('observedHealthy','ready','failed','permissionRequired') AND
           finished_at IS NOT NULL) OR
          (state NOT IN ('observedHealthy','ready','failed','permissionRequired') AND
           finished_at IS NULL)),
    CHECK((state IN ('failed','permissionRequired') AND failure_code IS NOT NULL) OR
          (state NOT IN ('failed','permissionRequired') AND failure_code IS NULL))
);

CREATE INDEX IF NOT EXISTS node_runtime_recovery_attempts_policy_time
ON node_runtime_recovery_attempts(policy_id, started_at, id);

CREATE INDEX IF NOT EXISTS node_runtime_recovery_attempts_node_time
ON node_runtime_recovery_attempts(node_id, started_at, id);
