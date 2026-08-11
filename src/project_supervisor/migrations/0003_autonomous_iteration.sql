PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS autonomous_goals (
    id TEXT PRIMARY KEY,
    project_id TEXT NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    intent TEXT NOT NULL,
    effective_intent TEXT NOT NULL,
    state TEXT NOT NULL CHECK(state IN (
        'created','running','softPaused','hardPaused','stopped','terminated'
    )),
    pause_mode TEXT CHECK(pause_mode IS NULL OR pause_mode IN ('soft','hard')),
    termination_reason TEXT,
    termination_detail TEXT,
    budgets_json TEXT NOT NULL,
    iteration_count INTEGER NOT NULL DEFAULT 0 CHECK(iteration_count >= 0),
    no_progress_count INTEGER NOT NULL DEFAULT 0 CHECK(no_progress_count >= 0),
    task_count INTEGER NOT NULL DEFAULT 0 CHECK(task_count >= 0),
    failure_count INTEGER NOT NULL DEFAULT 0 CHECK(failure_count >= 0),
    steer_version INTEGER NOT NULL DEFAULT 0 CHECK(steer_version >= 0),
    progress_fingerprint TEXT,
    started_at TEXT,
    last_evaluated_at TEXT,
    finished_at TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    version INTEGER NOT NULL DEFAULT 1 CHECK(version > 0),
    CHECK((state = 'terminated' AND termination_reason IS NOT NULL) OR state <> 'terminated'),
    CHECK((state IN ('softPaused','hardPaused') AND pause_mode IS NOT NULL) OR
          (state NOT IN ('softPaused','hardPaused') AND pause_mode IS NULL))
);

CREATE INDEX IF NOT EXISTS autonomous_goals_project_state
ON autonomous_goals(project_id, state, updated_at);

CREATE TABLE IF NOT EXISTS autonomous_iterations (
    id TEXT PRIMARY KEY,
    goal_id TEXT NOT NULL REFERENCES autonomous_goals(id) ON DELETE CASCADE,
    sequence INTEGER NOT NULL CHECK(sequence > 0),
    state TEXT NOT NULL CHECK(state IN (
        'evaluating','planned','dispatching','collecting','verifying','replanning',
        'completed','interrupted'
    )),
    evaluation_json TEXT,
    plan_json TEXT,
    verification_json TEXT,
    progress_fingerprint TEXT,
    started_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    completed_at TEXT,
    UNIQUE(goal_id, sequence)
);

CREATE INDEX IF NOT EXISTS autonomous_iterations_goal_sequence
ON autonomous_iterations(goal_id, sequence);

CREATE TABLE IF NOT EXISTS autonomous_actions (
    id TEXT PRIMARY KEY,
    goal_id TEXT NOT NULL REFERENCES autonomous_goals(id) ON DELETE CASCADE,
    iteration_id TEXT NOT NULL REFERENCES autonomous_iterations(id) ON DELETE CASCADE,
    ordinal INTEGER NOT NULL CHECK(ordinal >= 0),
    action_key TEXT NOT NULL,
    title TEXT NOT NULL,
    description TEXT NOT NULL,
    role TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    state TEXT NOT NULL CHECK(state IN (
        'planned','dispatching','running','completed','failed','cancelled'
    )),
    task_id TEXT REFERENCES tasks(id) ON DELETE SET NULL,
    dispatch_ref TEXT,
    result_json TEXT,
    error TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    UNIQUE(iteration_id, action_key),
    UNIQUE(iteration_id, ordinal)
);

CREATE INDEX IF NOT EXISTS autonomous_actions_goal_state
ON autonomous_actions(goal_id, state, updated_at);

CREATE TABLE IF NOT EXISTS autonomous_steers (
    id TEXT PRIMARY KEY,
    goal_id TEXT NOT NULL REFERENCES autonomous_goals(id) ON DELETE CASCADE,
    sequence INTEGER NOT NULL CHECK(sequence > 0),
    instruction TEXT NOT NULL,
    priority INTEGER CHECK(priority IS NULL OR priority BETWEEN 0 AND 100),
    preserve_valid_work INTEGER NOT NULL CHECK(preserve_valid_work IN (0,1)),
    actor TEXT NOT NULL,
    created_at TEXT NOT NULL,
    UNIQUE(goal_id, sequence)
);
