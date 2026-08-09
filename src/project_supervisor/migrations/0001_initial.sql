PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS projects (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    root_path TEXT NOT NULL,
    goal TEXT NOT NULL,
    phase TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS acceptance_criteria (
    id TEXT PRIMARY KEY,
    project_id TEXT NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    kind TEXT NOT NULL,
    description TEXT NOT NULL,
    command_json TEXT,
    expected_json TEXT,
    state TEXT NOT NULL DEFAULT 'pending',
    evidence_json TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS nodes (
    id TEXT PRIMARY KEY,
    hostname TEXT NOT NULL,
    display_name TEXT NOT NULL,
    role TEXT NOT NULL,
    state TEXT NOT NULL,
    operating_system TEXT,
    hardware_summary TEXT,
    private_endpoint TEXT,
    capabilities_json TEXT NOT NULL DEFAULT '[]',
    last_heartbeat_at TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS models (
    id TEXT PRIMARY KEY,
    provider TEXT NOT NULL,
    identifier TEXT NOT NULL,
    display_name TEXT NOT NULL,
    context_variant TEXT,
    context_window_tokens INTEGER CHECK(context_window_tokens IS NULL OR context_window_tokens > 0),
    metadata_json TEXT NOT NULL DEFAULT '{}',
    UNIQUE(provider, identifier, context_variant)
);

CREATE TABLE IF NOT EXISTS workers (
    id TEXT PRIMARY KEY,
    node_id TEXT NOT NULL REFERENCES nodes(id),
    harness TEXT NOT NULL,
    provider TEXT NOT NULL,
    model_id TEXT REFERENCES models(id),
    state TEXT NOT NULL,
    resource_state TEXT NOT NULL,
    capabilities_json TEXT NOT NULL,
    code_write_allowed INTEGER NOT NULL CHECK(code_write_allowed IN (0,1)),
    privacy_allowed INTEGER NOT NULL CHECK(privacy_allowed IN (0,1)),
    quality_score REAL NOT NULL DEFAULT 0.5 CHECK(quality_score BETWEEN 0 AND 1),
    reliability_score REAL NOT NULL DEFAULT 0.5 CHECK(reliability_score BETWEEN 0 AND 1),
    expected_latency_seconds REAL NOT NULL DEFAULT 30 CHECK(expected_latency_seconds >= 0),
    monetary_cost_score REAL NOT NULL DEFAULT 0.5 CHECK(monetary_cost_score BETWEEN 0 AND 1),
    harness_version TEXT,
    last_heartbeat_at TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS tasks (
    id TEXT PRIMARY KEY,
    project_id TEXT NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    reference TEXT NOT NULL,
    title TEXT NOT NULL,
    description TEXT NOT NULL,
    state TEXT NOT NULL,
    topology TEXT NOT NULL,
    priority INTEGER NOT NULL DEFAULT 50 CHECK(priority BETWEEN 0 AND 100),
    labels_json TEXT NOT NULL,
    required_capabilities_json TEXT NOT NULL,
    permission_class TEXT NOT NULL,
    approval_state TEXT NOT NULL,
    minimum_context_tokens INTEGER CHECK(minimum_context_tokens IS NULL OR minimum_context_tokens > 0),
    privacy_sensitive INTEGER NOT NULL CHECK(privacy_sensitive IN (0,1)),
    code_write_required INTEGER NOT NULL CHECK(code_write_required IN (0,1)),
    panel_size INTEGER NOT NULL DEFAULT 2 CHECK(panel_size > 0),
    preferred_workers_json TEXT NOT NULL DEFAULT '[]',
    attempt_count INTEGER NOT NULL DEFAULT 0 CHECK(attempt_count >= 0),
    version INTEGER NOT NULL DEFAULT 1,
    failure_reason TEXT,
    created_at TEXT NOT NULL,
    started_at TEXT,
    updated_at TEXT NOT NULL,
    finished_at TEXT,
    UNIQUE(project_id, reference)
);

CREATE TABLE IF NOT EXISTS task_dependencies (
    task_id TEXT NOT NULL REFERENCES tasks(id) ON DELETE CASCADE,
    depends_on_task_id TEXT NOT NULL REFERENCES tasks(id) ON DELETE CASCADE,
    PRIMARY KEY(task_id, depends_on_task_id),
    CHECK(task_id <> depends_on_task_id)
);

CREATE TABLE IF NOT EXISTS routing_decisions (
    id TEXT PRIMARY KEY,
    task_id TEXT NOT NULL REFERENCES tasks(id) ON DELETE CASCADE,
    topology TEXT NOT NULL,
    policy_version TEXT NOT NULL,
    selected_workers_json TEXT NOT NULL,
    explanation_json TEXT NOT NULL,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS routing_candidates (
    decision_id TEXT NOT NULL REFERENCES routing_decisions(id) ON DELETE CASCADE,
    worker_id TEXT NOT NULL,
    selected INTEGER NOT NULL CHECK(selected IN (0,1)),
    score REAL,
    components_json TEXT,
    rejection_code TEXT,
    rejection_detail TEXT,
    PRIMARY KEY(decision_id, worker_id, rejection_code)
);

CREATE TABLE IF NOT EXISTS sessions (
    id TEXT PRIMARY KEY,
    worker_id TEXT NOT NULL REFERENCES workers(id),
    provider_session_id TEXT,
    model_id TEXT REFERENCES models(id),
    resume_cursor TEXT,
    state TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    UNIQUE(worker_id, provider_session_id)
);

CREATE TABLE IF NOT EXISTS worker_runs (
    id TEXT PRIMARY KEY,
    task_id TEXT NOT NULL REFERENCES tasks(id) ON DELETE CASCADE,
    worker_id TEXT NOT NULL REFERENCES workers(id),
    session_id TEXT REFERENCES sessions(id),
    state TEXT NOT NULL,
    process_id INTEGER,
    attempt INTEGER NOT NULL CHECK(attempt > 0),
    started_at TEXT,
    last_event_at TEXT,
    timeout_at TEXT,
    ended_at TEXT,
    exit_code INTEGER,
    failure_class TEXT,
    failure_detail TEXT,
    raw_output_reference TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    UNIQUE(task_id, worker_id, attempt)
);

CREATE TABLE IF NOT EXISTS worker_results (
    run_id TEXT PRIMARY KEY REFERENCES worker_runs(id) ON DELETE CASCADE,
    summary TEXT NOT NULL,
    changed_files_json TEXT NOT NULL DEFAULT '[]',
    commands_run_json TEXT NOT NULL DEFAULT '[]',
    tests_json TEXT NOT NULL DEFAULT '[]',
    artifacts_json TEXT NOT NULL DEFAULT '[]',
    commit_hash TEXT,
    blockers_json TEXT NOT NULL DEFAULT '[]',
    confidence REAL CHECK(confidence IS NULL OR confidence BETWEEN 0 AND 1),
    recommended_next_actions_json TEXT NOT NULL DEFAULT '[]',
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS events (
    sequence INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id TEXT UNIQUE,
    kind TEXT NOT NULL,
    severity TEXT NOT NULL,
    entity_type TEXT NOT NULL,
    entity_id TEXT NOT NULL,
    project_id TEXT REFERENCES projects(id) ON DELETE CASCADE,
    task_id TEXT REFERENCES tasks(id) ON DELETE CASCADE,
    worker_id TEXT REFERENCES workers(id),
    run_id TEXT REFERENCES worker_runs(id),
    summary TEXT NOT NULL,
    payload_json TEXT NOT NULL DEFAULT '{}',
    actor TEXT NOT NULL,
    created_at TEXT NOT NULL
);

CREATE TRIGGER IF NOT EXISTS events_no_update
BEFORE UPDATE ON events BEGIN SELECT RAISE(ABORT, 'events are append-only'); END;

CREATE TRIGGER IF NOT EXISTS events_no_delete
BEFORE DELETE ON events BEGIN SELECT RAISE(ABORT, 'events are append-only'); END;

CREATE INDEX IF NOT EXISTS events_task_sequence ON events(task_id, sequence);
CREATE INDEX IF NOT EXISTS events_kind_sequence ON events(kind, sequence);
CREATE INDEX IF NOT EXISTS tasks_project_state ON tasks(project_id, state, updated_at);
CREATE INDEX IF NOT EXISTS worker_runs_active ON worker_runs(state, updated_at);

CREATE TABLE IF NOT EXISTS usage_records (
    id TEXT PRIMARY KEY,
    task_id TEXT REFERENCES tasks(id) ON DELETE CASCADE,
    run_id TEXT REFERENCES worker_runs(id) ON DELETE CASCADE,
    worker_id TEXT REFERENCES workers(id),
    model_id TEXT REFERENCES models(id),
    metric TEXT NOT NULL,
    value REAL,
    unit TEXT NOT NULL,
    confidence TEXT NOT NULL,
    unavailable_reason TEXT,
    provider_reported_at TEXT,
    recorded_at TEXT NOT NULL,
    CHECK((value IS NULL AND unavailable_reason IS NOT NULL) OR
          (value IS NOT NULL AND unavailable_reason IS NULL))
);

CREATE TABLE IF NOT EXISTS resource_states (
    id TEXT PRIMARY KEY,
    node_id TEXT REFERENCES nodes(id),
    worker_id TEXT REFERENCES workers(id),
    state TEXT NOT NULL,
    detail TEXT,
    resets_at TEXT,
    confidence TEXT NOT NULL,
    observed_at TEXT NOT NULL,
    CHECK(node_id IS NOT NULL OR worker_id IS NOT NULL)
);

CREATE TABLE IF NOT EXISTS approvals (
    id TEXT PRIMARY KEY,
    project_id TEXT NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    task_id TEXT REFERENCES tasks(id) ON DELETE CASCADE,
    permission_class TEXT NOT NULL,
    action_type TEXT NOT NULL,
    action_payload_json TEXT NOT NULL,
    state TEXT NOT NULL,
    requested_at TEXT NOT NULL,
    resolved_at TEXT,
    requested_by TEXT NOT NULL,
    resolved_by TEXT,
    reason TEXT
);

CREATE TABLE IF NOT EXISTS checkpoints (
    id TEXT PRIMARY KEY,
    project_id TEXT NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    event_sequence INTEGER NOT NULL,
    snapshot_json TEXT NOT NULL,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS failures (
    id TEXT PRIMARY KEY,
    project_id TEXT REFERENCES projects(id) ON DELETE CASCADE,
    task_id TEXT REFERENCES tasks(id) ON DELETE CASCADE,
    run_id TEXT REFERENCES worker_runs(id) ON DELETE CASCADE,
    classification TEXT NOT NULL,
    summary TEXT NOT NULL,
    detail TEXT,
    retryable INTEGER NOT NULL CHECK(retryable IN (0,1)),
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS worktrees (
    id TEXT PRIMARY KEY,
    task_id TEXT NOT NULL REFERENCES tasks(id) ON DELETE CASCADE,
    worker_id TEXT REFERENCES workers(id),
    repository_root TEXT NOT NULL,
    path TEXT NOT NULL UNIQUE,
    branch TEXT NOT NULL,
    base_commit TEXT NOT NULL,
    resulting_commit TEXT,
    merge_state TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS verifications (
    id TEXT PRIMARY KEY,
    task_id TEXT NOT NULL REFERENCES tasks(id) ON DELETE CASCADE,
    criterion_id TEXT REFERENCES acceptance_criteria(id) ON DELETE SET NULL,
    kind TEXT NOT NULL,
    command_json TEXT,
    exit_code INTEGER,
    passed INTEGER NOT NULL CHECK(passed IN (0,1)),
    evidence_json TEXT NOT NULL,
    verifier TEXT NOT NULL,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS api_tokens (
    id TEXT PRIMARY KEY,
    label TEXT NOT NULL,
    token_salt BLOB NOT NULL,
    token_hash BLOB NOT NULL,
    scopes_json TEXT NOT NULL,
    created_at TEXT NOT NULL,
    expires_at TEXT,
    revoked_at TEXT
);

CREATE TABLE IF NOT EXISTS instance_leases (
    name TEXT PRIMARY KEY,
    holder_id TEXT NOT NULL,
    process_id INTEGER NOT NULL,
    acquired_at TEXT NOT NULL,
    heartbeat_at TEXT NOT NULL,
    expires_at TEXT NOT NULL
);

