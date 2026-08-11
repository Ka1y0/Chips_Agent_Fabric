PRAGMA foreign_keys = ON;

-- A provider job is the durable identity of one Worker run's external side effect.  The
-- pre-launch row and idempotency key exist before an adapter is invoked; the provider/runtime
-- handle is bound afterwards.  Opaque adapter metadata is private canonical state and must not
-- be projected directly to remote clients.
CREATE TABLE IF NOT EXISTS provider_jobs (
    id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL UNIQUE REFERENCES worker_runs(id) ON DELETE CASCADE,
    task_id TEXT NOT NULL REFERENCES tasks(id) ON DELETE CASCADE,
    worker_id TEXT NOT NULL REFERENCES workers(id),
    adapter_type TEXT NOT NULL,
    adapter_instance_id TEXT NOT NULL,
    provider TEXT NOT NULL,
    handle_version INTEGER NOT NULL DEFAULT 1 CHECK(handle_version > 0),
    provider_job_id TEXT,
    provider_session_id TEXT,
    runtime_pid INTEGER CHECK(runtime_pid IS NULL OR runtime_pid > 0),
    runtime_host TEXT,
    runtime_identity TEXT,
    launch_generation INTEGER NOT NULL CHECK(launch_generation > 0),
    idempotency_key TEXT NOT NULL UNIQUE,
    launch_state TEXT NOT NULL CHECK(
        launch_state IN ('prepared','launching','bound','terminal','uncertain')
    ),
    reconciliation_state TEXT NOT NULL DEFAULT 'unknown' CHECK(
        reconciliation_state IN (
            'unknown','knownRunning','knownCompleted','knownFailed','knownCancelled',
            'providerNotFound','providerUnreachable'
        )
    ),
    result_collection_state TEXT NOT NULL DEFAULT 'pending' CHECK(
        result_collection_state IN ('pending','collected','notAvailable','uncertain')
    ),
    supports_reconcile INTEGER NOT NULL DEFAULT 0 CHECK(supports_reconcile IN (0,1)),
    supports_resume INTEGER NOT NULL DEFAULT 0 CHECK(supports_resume IN (0,1)),
    supports_cancel INTEGER NOT NULL DEFAULT 0 CHECK(supports_cancel IN (0,1)),
    supports_durable_cancel INTEGER NOT NULL DEFAULT 0 CHECK(supports_durable_cancel IN (0,1)),
    supports_provider_idempotency INTEGER NOT NULL DEFAULT 0
        CHECK(supports_provider_idempotency IN (0,1)),
    supports_stream_reconnect INTEGER NOT NULL DEFAULT 0
        CHECK(supports_stream_reconnect IN (0,1)),
    supports_repeatable_collect INTEGER NOT NULL DEFAULT 0
        CHECK(supports_repeatable_collect IN (0,1)),
    adapter_metadata_json TEXT NOT NULL DEFAULT '{}',
    created_at TEXT NOT NULL,
    launched_at TEXT,
    last_reconciled_at TEXT,
    result_collected_at TEXT,
    updated_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS provider_jobs_reconcile
ON provider_jobs(reconciliation_state, launch_state, updated_at);

CREATE INDEX IF NOT EXISTS provider_jobs_task
ON provider_jobs(task_id, created_at, id);

-- Minimal first-class escalation lifecycle for reconciliation states that cannot be resolved
-- safely without provider reachability, stronger idempotency, or human authority.
CREATE TABLE IF NOT EXISTS execution_escalations (
    id TEXT PRIMARY KEY,
    project_id TEXT NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    goal_id TEXT REFERENCES autonomous_goals(id) ON DELETE SET NULL,
    task_id TEXT REFERENCES tasks(id) ON DELETE CASCADE,
    run_id TEXT REFERENCES worker_runs(id) ON DELETE CASCADE,
    provider_job_id TEXT REFERENCES provider_jobs(id) ON DELETE CASCADE,
    code TEXT NOT NULL CHECK(
        code IN (
            'PROVIDER_STATE_AMBIGUOUS','EXTERNAL_JOB_UNREACHABLE',
            'IDEMPOTENCY_UNCERTAIN','RESUME_UNSUPPORTED','RESULT_COLLECTION_UNCERTAIN'
        )
    ),
    state TEXT NOT NULL DEFAULT 'open' CHECK(state IN ('open','resolved','dismissed')),
    summary TEXT NOT NULL,
    detail TEXT,
    created_by TEXT NOT NULL,
    resolved_by TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    resolved_at TEXT
);

CREATE UNIQUE INDEX IF NOT EXISTS execution_escalations_one_open_code
ON execution_escalations(provider_job_id, code)
WHERE state = 'open';

CREATE INDEX IF NOT EXISTS execution_escalations_open
ON execution_escalations(state, project_id, created_at, id);
