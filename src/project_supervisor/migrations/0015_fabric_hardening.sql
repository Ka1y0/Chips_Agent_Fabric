PRAGMA foreign_keys = ON;

-- Bind semantic interaction resources to the authoritative Task execution generation whenever
-- the lease is acquired for a canonical Worker run.  Legacy low-level resource leases remain
-- nullable and are never silently promoted to generation-bound authority.
ALTER TABLE interaction_resource_leases ADD COLUMN task_lease_owner_id TEXT;
ALTER TABLE interaction_resource_leases ADD COLUMN task_lease_generation INTEGER;

CREATE INDEX IF NOT EXISTS interaction_resource_task_generation
ON interaction_resource_leases(task_id, task_lease_generation, state);

-- UI graph edges are aggregates.  Immutable trajectory/action evidence makes replay of one
-- verified action idempotent while allowing distinct successful trajectories to strengthen an
-- edge over time.
CREATE TABLE IF NOT EXISTS ui_graph_transition_evidence (
    trajectory_id TEXT NOT NULL REFERENCES interaction_trajectories(id) ON DELETE RESTRICT,
    action_ordinal INTEGER NOT NULL CHECK(action_ordinal >= 0),
    app_id TEXT NOT NULL,
    app_version TEXT NOT NULL,
    from_state_sha256 TEXT NOT NULL CHECK(length(from_state_sha256) = 64),
    semantic_action TEXT NOT NULL,
    to_state_sha256 TEXT NOT NULL CHECK(length(to_state_sha256) = 64),
    verified INTEGER NOT NULL CHECK(verified IN (0,1)),
    confidence REAL NOT NULL CHECK(confidence BETWEEN 0 AND 1),
    created_at TEXT NOT NULL,
    PRIMARY KEY(trajectory_id, action_ordinal)
);

CREATE TRIGGER IF NOT EXISTS ui_graph_transition_evidence_no_update
BEFORE UPDATE ON ui_graph_transition_evidence
BEGIN SELECT RAISE(ABORT, 'UI graph evidence is append-only'); END;

CREATE TRIGGER IF NOT EXISTS ui_graph_transition_evidence_no_delete
BEFORE DELETE ON ui_graph_transition_evidence
BEGIN SELECT RAISE(ABORT, 'UI graph evidence is append-only'); END;
