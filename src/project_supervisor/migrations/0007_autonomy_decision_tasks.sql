PRAGMA foreign_keys = ON;

-- Provider-neutral evaluator/planner/verifier invocations are ordinary canonical Tasks. This
-- binding makes each decision checkpoint idempotent and auditable without treating model sessions
-- or conversation history as canonical state.
CREATE TABLE IF NOT EXISTS autonomy_decision_tasks (
    task_id TEXT PRIMARY KEY REFERENCES tasks(id) ON DELETE CASCADE,
    goal_id TEXT NOT NULL REFERENCES autonomous_goals(id) ON DELETE CASCADE,
    iteration_id TEXT NOT NULL REFERENCES autonomous_iterations(id) ON DELETE CASCADE,
    iteration_sequence INTEGER NOT NULL CHECK(iteration_sequence > 0),
    phase TEXT NOT NULL CHECK(phase IN ('evaluation','planning','verification')),
    steer_version INTEGER NOT NULL CHECK(steer_version >= 0),
    schema_version TEXT NOT NULL CHECK(schema_version = 'autonomy-decision/v1'),
    input_sha256 TEXT NOT NULL CHECK(length(input_sha256) = 64),
    output_sha256 TEXT CHECK(output_sha256 IS NULL OR length(output_sha256) = 64),
    status TEXT NOT NULL CHECK(status IN ('pending','accepted','rejected')),
    error TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    completed_at TEXT,
    UNIQUE(goal_id, iteration_sequence, phase, steer_version),
    CHECK((status='pending' AND completed_at IS NULL) OR
          (status IN ('accepted','rejected') AND completed_at IS NOT NULL)),
    CHECK((status='accepted' AND output_sha256 IS NOT NULL AND error IS NULL) OR
          (status='rejected' AND error IS NOT NULL) OR
          status='pending')
);

CREATE INDEX IF NOT EXISTS autonomy_decision_tasks_goal_checkpoint
ON autonomy_decision_tasks(goal_id, iteration_sequence, steer_version, phase);

CREATE INDEX IF NOT EXISTS autonomy_decision_tasks_status
ON autonomy_decision_tasks(status, updated_at, task_id);
