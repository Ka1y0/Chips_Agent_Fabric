PRAGMA foreign_keys = ON;

-- A Task verification scope is an immutable snapshot of the exact deterministic criteria that
-- apply to one semantic Task revision. Project acceptance_criteria remain reusable legacy
-- templates; scoped verification never mutates those template rows.
CREATE TABLE IF NOT EXISTS task_verification_scopes (
    id TEXT PRIMARY KEY,
    project_id TEXT NOT NULL REFERENCES projects(id) ON DELETE RESTRICT,
    task_id TEXT NOT NULL REFERENCES tasks(id) ON DELETE RESTRICT,
    criteria_version INTEGER NOT NULL CHECK(criteria_version > 0),
    task_definition_revision INTEGER NOT NULL CHECK(task_definition_revision > 0),
    goal_id TEXT REFERENCES autonomous_goals(id) ON DELETE RESTRICT,
    iteration_id TEXT REFERENCES autonomous_iterations(id) ON DELETE RESTRICT,
    plan_version INTEGER CHECK(plan_version IS NULL OR plan_version > 0),
    steer_version INTEGER CHECK(steer_version IS NULL OR steer_version >= 0),
    schema_version TEXT NOT NULL CHECK(schema_version = 'task-verification-scope/v1'),
    definition_sha256 TEXT NOT NULL CHECK(length(definition_sha256) = 64),
    created_at TEXT NOT NULL,
    sealed_at TEXT,
    UNIQUE(task_id, criteria_version)
);

CREATE TABLE IF NOT EXISTS task_verification_scope_items (
    id TEXT PRIMARY KEY,
    scope_id TEXT NOT NULL REFERENCES task_verification_scopes(id) ON DELETE RESTRICT,
    criterion_id TEXT NOT NULL,
    source_criterion_id TEXT REFERENCES acceptance_criteria(id) ON DELETE RESTRICT,
    ordinal INTEGER NOT NULL CHECK(ordinal >= 0),
    required INTEGER NOT NULL CHECK(required IN (0,1)),
    kind TEXT NOT NULL,
    description TEXT NOT NULL,
    command_json TEXT,
    expected_json TEXT,
    definition_sha256 TEXT NOT NULL CHECK(length(definition_sha256) = 64),
    created_at TEXT NOT NULL,
    UNIQUE(scope_id, criterion_id),
    UNIQUE(scope_id, ordinal)
);

CREATE INDEX IF NOT EXISTS task_verification_scopes_task_version
ON task_verification_scopes(task_id, criteria_version);

CREATE INDEX IF NOT EXISTS task_verification_scope_items_scope
ON task_verification_scope_items(scope_id, ordinal);

ALTER TABLE tasks
ADD COLUMN definition_revision INTEGER NOT NULL DEFAULT 1 CHECK(definition_revision > 0);

ALTER TABLE tasks
ADD COLUMN current_verification_scope_id TEXT
REFERENCES task_verification_scopes(id) ON DELETE RESTRICT;

ALTER TABLE worker_runs
ADD COLUMN verification_scope_id TEXT
REFERENCES task_verification_scopes(id) ON DELETE RESTRICT;

ALTER TABLE worker_runs
ADD COLUMN task_definition_revision INTEGER CHECK(
    task_definition_revision IS NULL OR task_definition_revision > 0
);

ALTER TABLE verifications
ADD COLUMN verification_scope_id TEXT
REFERENCES task_verification_scopes(id) ON DELETE RESTRICT;

ALTER TABLE verifications
ADD COLUMN scope_item_id TEXT
REFERENCES task_verification_scope_items(id) ON DELETE RESTRICT;

ALTER TABLE verifications
ADD COLUMN source_attempt INTEGER CHECK(source_attempt IS NULL OR source_attempt > 0);

-- Scope definitions are constructed and sealed in the same transaction that binds them. Once
-- sealed, neither the definition row nor its criterion set may change. A replan/steer appends N+1
-- and moves only the Task's current pointer; it never edits or removes N.
CREATE TRIGGER IF NOT EXISTS task_verification_scopes_must_start_unsealed
BEFORE INSERT ON task_verification_scopes
WHEN NEW.sealed_at IS NOT NULL
BEGIN SELECT RAISE(ABORT, 'task verification scope must be sealed after criteria insertion'); END;

CREATE TRIGGER IF NOT EXISTS task_verification_scopes_no_update
BEFORE UPDATE ON task_verification_scopes
WHEN NOT (
    OLD.sealed_at IS NULL AND NEW.sealed_at IS NOT NULL
    AND NEW.id IS OLD.id
    AND NEW.project_id IS OLD.project_id
    AND NEW.task_id IS OLD.task_id
    AND NEW.criteria_version IS OLD.criteria_version
    AND NEW.task_definition_revision IS OLD.task_definition_revision
    AND NEW.goal_id IS OLD.goal_id
    AND NEW.iteration_id IS OLD.iteration_id
    AND NEW.plan_version IS OLD.plan_version
    AND NEW.steer_version IS OLD.steer_version
    AND NEW.schema_version IS OLD.schema_version
    AND NEW.definition_sha256 IS OLD.definition_sha256
    AND NEW.created_at IS OLD.created_at
)
BEGIN SELECT RAISE(ABORT, 'task verification scopes are append-only'); END;

CREATE TRIGGER IF NOT EXISTS task_verification_scopes_require_items_before_seal
BEFORE UPDATE OF sealed_at ON task_verification_scopes
WHEN OLD.sealed_at IS NULL AND NEW.sealed_at IS NOT NULL
AND NOT EXISTS (
    SELECT 1 FROM task_verification_scope_items item WHERE item.scope_id=OLD.id
)
BEGIN SELECT RAISE(ABORT, 'task verification scope cannot be sealed without criteria'); END;

CREATE TRIGGER IF NOT EXISTS task_verification_scopes_no_delete
BEFORE DELETE ON task_verification_scopes
BEGIN SELECT RAISE(ABORT, 'task verification scopes are append-only'); END;

CREATE TRIGGER IF NOT EXISTS task_verification_scope_items_no_update
BEFORE UPDATE ON task_verification_scope_items
BEGIN SELECT RAISE(ABORT, 'task verification scope items are append-only'); END;

CREATE TRIGGER IF NOT EXISTS task_verification_scope_items_no_delete
BEFORE DELETE ON task_verification_scope_items
BEGIN SELECT RAISE(ABORT, 'task verification scope items are append-only'); END;

CREATE TRIGGER IF NOT EXISTS task_verification_scope_items_no_insert_after_seal
BEFORE INSERT ON task_verification_scope_items
WHEN EXISTS (
    SELECT 1 FROM task_verification_scopes scope
    WHERE scope.id=NEW.scope_id AND scope.sealed_at IS NOT NULL
)
BEGIN SELECT RAISE(ABORT, 'sealed task verification scope criteria are append-only'); END;

CREATE TRIGGER IF NOT EXISTS tasks_require_sealed_verification_scope
BEFORE UPDATE OF current_verification_scope_id,definition_revision ON tasks
WHEN NEW.current_verification_scope_id IS NOT NULL
AND NOT EXISTS (
    SELECT 1 FROM task_verification_scopes scope
    WHERE scope.id=NEW.current_verification_scope_id
      AND scope.task_id=NEW.id
      AND scope.project_id=NEW.project_id
      AND scope.task_definition_revision=NEW.definition_revision
      AND scope.sealed_at IS NOT NULL
)
BEGIN SELECT RAISE(ABORT, 'task verification scope must be sealed and match its Task revision'); END;
