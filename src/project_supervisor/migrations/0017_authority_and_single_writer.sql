PRAGMA foreign_keys = ON;

-- Phase 2 authority facets are additive.  Worker classes are operator-declared identities, not
-- inferred from provider names or harness brands.
ALTER TABLE workers ADD COLUMN worker_classes_json TEXT NOT NULL DEFAULT '[]';

ALTER TABLE authorization_envelopes ADD COLUMN version INTEGER NOT NULL DEFAULT 1
CHECK(version > 0);
ALTER TABLE authorization_envelopes ADD COLUMN inheritance_policy TEXT NOT NULL
DEFAULT 'narrowOnly' CHECK(inheritance_policy IN ('narrowOnly','noInheritance'));
ALTER TABLE authorization_envelopes ADD COLUMN allowed_providers_json TEXT NOT NULL DEFAULT '[]';
ALTER TABLE authorization_envelopes ADD COLUMN allowed_worker_classes_json TEXT NOT NULL
DEFAULT '[]';
ALTER TABLE authorization_envelopes ADD COLUMN allowed_data_classes_json TEXT NOT NULL DEFAULT '[]';
ALTER TABLE authorization_envelopes ADD COLUMN denied_data_classes_json TEXT NOT NULL DEFAULT '[]';
ALTER TABLE authorization_envelopes ADD COLUMN allowed_action_classes_json TEXT NOT NULL
DEFAULT '[]';
ALTER TABLE authorization_envelopes ADD COLUMN denied_action_classes_json TEXT NOT NULL
DEFAULT '[]';

-- A project write owner is an operator-configured authority.  A lease is a separate, bounded
-- execution right; neither a provider nor a Worker can self-appoint as owner through task prose.
CREATE TABLE IF NOT EXISTS project_write_authorities (
    project_id TEXT PRIMARY KEY REFERENCES projects(id) ON DELETE RESTRICT,
    owner_kind TEXT NOT NULL CHECK(owner_kind IN ('worker','externalManagedExecutor')),
    owner_id TEXT NOT NULL,
    node_id TEXT REFERENCES nodes(id) ON DELETE RESTRICT,
    envelope_id TEXT NOT NULL REFERENCES authorization_envelopes(id) ON DELETE RESTRICT,
    state TEXT NOT NULL CHECK(state IN ('active','disabled')),
    generation INTEGER NOT NULL CHECK(generation > 0),
    configured_by TEXT NOT NULL,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS project_write_leases (
    id TEXT PRIMARY KEY,
    project_id TEXT NOT NULL REFERENCES projects(id) ON DELETE RESTRICT,
    owner_id TEXT NOT NULL,
    task_id TEXT REFERENCES tasks(id) ON DELETE RESTRICT,
    run_id TEXT REFERENCES worker_runs(id) ON DELETE RESTRICT,
    generation INTEGER NOT NULL CHECK(generation > 0),
    state TEXT NOT NULL CHECK(state IN ('active','released','cancelled')),
    acquired_at TEXT NOT NULL,
    expires_at TEXT NOT NULL,
    released_at TEXT,
    release_reason TEXT
);

CREATE UNIQUE INDEX IF NOT EXISTS project_write_leases_one_active
ON project_write_leases(project_id) WHERE state='active';

ALTER TABLE experiment_handoffs ADD COLUMN schema_version TEXT NOT NULL
DEFAULT 'pc-codex-handoff/v1';
ALTER TABLE experiment_handoffs ADD COLUMN project_id TEXT REFERENCES projects(id)
ON DELETE RESTRICT;
ALTER TABLE experiment_handoffs ADD COLUMN owner_id TEXT;
ALTER TABLE experiment_handoffs ADD COLUMN write_lease_id TEXT REFERENCES project_write_leases(id)
ON DELETE RESTRICT;
ALTER TABLE experiment_handoffs ADD COLUMN content_kind TEXT;
ALTER TABLE experiment_handoffs ADD COLUMN expires_at TEXT;

CREATE TRIGGER IF NOT EXISTS project_write_authorities_no_update
BEFORE UPDATE ON project_write_authorities
BEGIN SELECT RAISE(ABORT, 'project write authorities are immutable'); END;
CREATE TRIGGER IF NOT EXISTS project_write_authorities_no_delete
BEFORE DELETE ON project_write_authorities
BEGIN SELECT RAISE(ABORT, 'project write authorities are immutable'); END;

