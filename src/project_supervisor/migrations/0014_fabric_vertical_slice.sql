PRAGMA foreign_keys = ON;

-- V0.3-dev capability requests remain ordinary Tasks. These additive columns preserve every
-- V0.2 row while allowing provider-independent preferences, constraints, explicit reproducibility
-- overrides, and a bounded structured execution specification.
ALTER TABLE tasks ADD COLUMN preferred_capabilities_json TEXT NOT NULL DEFAULT '[]';
ALTER TABLE tasks ADD COLUMN capability_constraints_json TEXT NOT NULL DEFAULT '{}';
ALTER TABLE tasks ADD COLUMN local_only INTEGER NOT NULL DEFAULT 0 CHECK(local_only IN (0,1));
ALTER TABLE tasks ADD COLUMN minimum_quality REAL CHECK(
    minimum_quality IS NULL OR minimum_quality BETWEEN 0 AND 1
);
ALTER TABLE tasks ADD COLUMN max_incremental_cost_usd REAL CHECK(
    max_incremental_cost_usd IS NULL OR max_incremental_cost_usd >= 0
);
ALTER TABLE tasks ADD COLUMN explicit_worker_id TEXT REFERENCES workers(id);
ALTER TABLE tasks ADD COLUMN execution_spec_json TEXT NOT NULL DEFAULT '{}';

-- Static Worker manifests are immutable. The mutable head is a narrow CAS pointer; dynamic
-- observations are append-only so UNKNOWN never gets rewritten into invented precision.
CREATE TABLE IF NOT EXISTS worker_capability_manifests (
    id TEXT PRIMARY KEY,
    worker_id TEXT NOT NULL REFERENCES workers(id) ON DELETE RESTRICT,
    revision INTEGER NOT NULL CHECK(revision > 0),
    schema_version TEXT NOT NULL CHECK(schema_version = 'worker-capability-manifest/v1'),
    catalog_version TEXT NOT NULL,
    provider_id TEXT NOT NULL,
    adapter_kind TEXT NOT NULL,
    definition_sha256 TEXT NOT NULL CHECK(
        length(definition_sha256) = 71 AND substr(definition_sha256,1,7) = 'sha256:'
    ),
    source TEXT NOT NULL,
    observed_at TEXT NOT NULL,
    valid_until TEXT,
    manifest_json TEXT NOT NULL,
    created_at TEXT NOT NULL,
    UNIQUE(worker_id, revision),
    UNIQUE(worker_id, definition_sha256),
    UNIQUE(worker_id, id)
);

CREATE TABLE IF NOT EXISTS worker_capability_manifest_heads (
    worker_id TEXT PRIMARY KEY REFERENCES workers(id) ON DELETE RESTRICT,
    manifest_id TEXT NOT NULL,
    generation INTEGER NOT NULL CHECK(generation > 0),
    updated_at TEXT NOT NULL,
    FOREIGN KEY(worker_id, manifest_id)
        REFERENCES worker_capability_manifests(worker_id, id) ON DELETE RESTRICT
);

CREATE TABLE IF NOT EXISTS worker_capability_observations (
    id TEXT PRIMARY KEY,
    worker_id TEXT NOT NULL REFERENCES workers(id) ON DELETE RESTRICT,
    manifest_id TEXT NOT NULL,
    version INTEGER NOT NULL CHECK(version > 0),
    health TEXT NOT NULL CHECK(health IN ('healthy','degraded','offline','unknown')),
    active_jobs INTEGER CHECK(active_jobs IS NULL OR active_jobs >= 0),
    subscription_state TEXT NOT NULL CHECK(subscription_state IN (
        'available','exhausted','unknown'
    )),
    quota_state TEXT NOT NULL CHECK(quota_state IN (
        'available','scarce','exhausted','unknown'
    )),
    quota_freshness TEXT NOT NULL CHECK(quota_freshness IN ('fresh','stale','unknown')),
    state_json TEXT NOT NULL,
    observed_at TEXT NOT NULL,
    created_at TEXT NOT NULL,
    UNIQUE(worker_id, version),
    FOREIGN KEY(worker_id, manifest_id)
        REFERENCES worker_capability_manifests(worker_id, id) ON DELETE RESTRICT
);

CREATE INDEX IF NOT EXISTS worker_capability_observations_latest
ON worker_capability_observations(worker_id, version DESC);

CREATE TRIGGER IF NOT EXISTS worker_capability_manifests_no_update
BEFORE UPDATE ON worker_capability_manifests
BEGIN SELECT RAISE(ABORT, 'worker capability manifests are immutable'); END;
CREATE TRIGGER IF NOT EXISTS worker_capability_manifests_no_delete
BEFORE DELETE ON worker_capability_manifests
BEGIN SELECT RAISE(ABORT, 'worker capability manifests are immutable'); END;
CREATE TRIGGER IF NOT EXISTS worker_capability_observations_no_update
BEFORE UPDATE ON worker_capability_observations
BEGIN SELECT RAISE(ABORT, 'worker capability observations are append-only'); END;
CREATE TRIGGER IF NOT EXISTS worker_capability_observations_no_delete
BEFORE DELETE ON worker_capability_observations
BEGIN SELECT RAISE(ABORT, 'worker capability observations are append-only'); END;

-- Workers propose child work; only a Supervisor transaction may bind a proposal to a canonical
-- Task. Definitions and decisions are separate immutable records so rejection history is auditable.
CREATE TABLE IF NOT EXISTS child_work_proposals (
    id TEXT PRIMARY KEY,
    goal_id TEXT NOT NULL REFERENCES autonomous_goals(id) ON DELETE RESTRICT,
    parent_task_id TEXT NOT NULL REFERENCES tasks(id) ON DELETE RESTRICT,
    source_run_id TEXT REFERENCES worker_runs(id) ON DELETE RESTRICT,
    proposal_key TEXT NOT NULL,
    semantic_digest TEXT NOT NULL CHECK(length(semantic_digest) = 64),
    source_result_sha256 TEXT CHECK(
        source_result_sha256 IS NULL OR length(source_result_sha256) = 64
    ),
    depth INTEGER NOT NULL CHECK(depth > 0),
    plan_version INTEGER NOT NULL CHECK(plan_version >= 0),
    steer_version INTEGER NOT NULL CHECK(steer_version >= 0),
    task_definition_revision INTEGER NOT NULL CHECK(task_definition_revision > 0),
    specification_json TEXT NOT NULL,
    created_at TEXT NOT NULL,
    UNIQUE(goal_id, parent_task_id, proposal_key),
    UNIQUE(goal_id, semantic_digest)
);

CREATE TABLE IF NOT EXISTS child_work_proposal_decisions (
    id TEXT PRIMARY KEY,
    proposal_id TEXT NOT NULL REFERENCES child_work_proposals(id) ON DELETE RESTRICT,
    revision INTEGER NOT NULL CHECK(revision > 0),
    outcome TEXT NOT NULL CHECK(outcome IN (
        'accepted','rejected','deferred','superseded','escalated'
    )),
    policy_version TEXT NOT NULL,
    reason_code TEXT NOT NULL,
    child_task_id TEXT REFERENCES tasks(id) ON DELETE RESTRICT,
    budget_json TEXT NOT NULL,
    created_at TEXT NOT NULL,
    UNIQUE(proposal_id, revision),
    CHECK((outcome='accepted' AND child_task_id IS NOT NULL) OR
          (outcome<>'accepted' AND child_task_id IS NULL))
);

CREATE TABLE IF NOT EXISTS autonomous_task_bindings (
    task_id TEXT PRIMARY KEY REFERENCES tasks(id) ON DELETE RESTRICT,
    goal_id TEXT NOT NULL REFERENCES autonomous_goals(id) ON DELETE RESTRICT,
    parent_task_id TEXT REFERENCES tasks(id) ON DELETE RESTRICT,
    proposal_id TEXT UNIQUE REFERENCES child_work_proposals(id) ON DELETE RESTRICT,
    depth INTEGER NOT NULL CHECK(depth >= 0),
    plan_version INTEGER NOT NULL CHECK(plan_version >= 0),
    steer_version INTEGER NOT NULL CHECK(steer_version >= 0),
    created_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS autonomous_task_bindings_goal_depth
ON autonomous_task_bindings(goal_id, depth, task_id);

CREATE TRIGGER IF NOT EXISTS child_work_proposals_no_update
BEFORE UPDATE ON child_work_proposals
BEGIN SELECT RAISE(ABORT, 'child work proposals are immutable'); END;
CREATE TRIGGER IF NOT EXISTS child_work_proposals_no_delete
BEFORE DELETE ON child_work_proposals
BEGIN SELECT RAISE(ABORT, 'child work proposals are immutable'); END;
CREATE TRIGGER IF NOT EXISTS child_work_proposal_decisions_no_update
BEFORE UPDATE ON child_work_proposal_decisions
BEGIN SELECT RAISE(ABORT, 'child work proposal decisions are append-only'); END;
CREATE TRIGGER IF NOT EXISTS child_work_proposal_decisions_no_delete
BEFORE DELETE ON child_work_proposal_decisions
BEGIN SELECT RAISE(ABORT, 'child work proposal decisions are append-only'); END;

-- Fusion is immutable and attempt/scope bound. It never substitutes for independent verification.
CREATE TABLE IF NOT EXISTS result_fusion_decisions (
    id TEXT PRIMARY KEY,
    task_id TEXT NOT NULL REFERENCES tasks(id) ON DELETE RESTRICT,
    source_attempt INTEGER NOT NULL CHECK(source_attempt > 0),
    policy_version TEXT NOT NULL,
    input_set_sha256 TEXT NOT NULL CHECK(length(input_set_sha256) = 64),
    classification TEXT NOT NULL CHECK(classification IN (
        'compatible','complementary','contradictory','insufficientEvidence'
    )),
    fused_json TEXT NOT NULL,
    conflicts_json TEXT NOT NULL,
    provenance_json TEXT NOT NULL,
    confidence REAL CHECK(confidence IS NULL OR confidence BETWEEN 0 AND 1),
    verification_required INTEGER NOT NULL CHECK(verification_required = 1),
    created_at TEXT NOT NULL,
    UNIQUE(task_id, source_attempt, policy_version, input_set_sha256)
);

CREATE TABLE IF NOT EXISTS fusion_verification_handoffs (
    id TEXT PRIMARY KEY,
    fusion_id TEXT NOT NULL REFERENCES result_fusion_decisions(id) ON DELETE RESTRICT,
    verification_scope_id TEXT REFERENCES task_verification_scopes(id) ON DELETE RESTRICT,
    task_definition_revision INTEGER NOT NULL CHECK(task_definition_revision > 0),
    source_attempt INTEGER NOT NULL CHECK(source_attempt > 0),
    steer_version INTEGER NOT NULL CHECK(steer_version >= 0),
    token_sha256 TEXT NOT NULL CHECK(length(token_sha256) = 64),
    created_at TEXT NOT NULL,
    UNIQUE(fusion_id, token_sha256)
);

CREATE TRIGGER IF NOT EXISTS result_fusion_decisions_no_update
BEFORE UPDATE ON result_fusion_decisions
BEGIN SELECT RAISE(ABORT, 'fusion decisions are immutable'); END;
CREATE TRIGGER IF NOT EXISTS result_fusion_decisions_no_delete
BEFORE DELETE ON result_fusion_decisions
BEGIN SELECT RAISE(ABORT, 'fusion decisions are immutable'); END;
CREATE TRIGGER IF NOT EXISTS fusion_verification_handoffs_no_update
BEFORE UPDATE ON fusion_verification_handoffs
BEGIN SELECT RAISE(ABORT, 'verification handoffs are immutable'); END;
CREATE TRIGGER IF NOT EXISTS fusion_verification_handoffs_no_delete
BEFORE DELETE ON fusion_verification_handoffs
BEGIN SELECT RAISE(ABORT, 'verification handoffs are immutable'); END;

-- Semantic UI executions use the same canonical DB and event journal. Resource keys are semantic
-- identities; screen coordinates are never lease identities.
CREATE TABLE IF NOT EXISTS interaction_resources (
    resource_key TEXT PRIMARY KEY,
    resource_type TEXT NOT NULL CHECK(resource_type IN (
        'desktopSession','browserContext','window','mouse','keyboard','clipboard','display'
    )),
    scope_id TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS interaction_resource_leases (
    lease_id TEXT NOT NULL,
    resource_key TEXT NOT NULL REFERENCES interaction_resources(resource_key) ON DELETE RESTRICT,
    lease_group_id TEXT NOT NULL,
    owner_id TEXT NOT NULL,
    task_id TEXT REFERENCES tasks(id) ON DELETE RESTRICT,
    run_id TEXT REFERENCES worker_runs(id) ON DELETE RESTRICT,
    generation INTEGER NOT NULL CHECK(generation > 0),
    state TEXT NOT NULL CHECK(state IN ('active','released','expired')),
    acquired_at TEXT NOT NULL,
    heartbeat_at TEXT NOT NULL,
    expires_at TEXT NOT NULL,
    released_at TEXT,
    PRIMARY KEY(lease_id, resource_key),
    UNIQUE(resource_key, generation),
    CHECK((state='active' AND released_at IS NULL) OR
          (state<>'active' AND released_at IS NOT NULL))
);

CREATE UNIQUE INDEX IF NOT EXISTS interaction_resource_one_active_owner
ON interaction_resource_leases(resource_key) WHERE state='active';
CREATE INDEX IF NOT EXISTS interaction_resource_lease_group
ON interaction_resource_leases(lease_group_id, state, resource_key);

CREATE TABLE IF NOT EXISTS interaction_executions (
    id TEXT PRIMARY KEY,
    project_id TEXT REFERENCES projects(id) ON DELETE RESTRICT,
    task_id TEXT REFERENCES tasks(id) ON DELETE RESTRICT,
    run_id TEXT UNIQUE REFERENCES worker_runs(id) ON DELETE RESTRICT,
    worker_id TEXT REFERENCES workers(id) ON DELETE RESTRICT,
    adapter_kind TEXT NOT NULL,
    channel TEXT NOT NULL,
    app_id TEXT NOT NULL,
    app_version TEXT,
    plan_schema TEXT NOT NULL,
    plan_sha256 TEXT NOT NULL CHECK(length(plan_sha256) = 64),
    state TEXT NOT NULL CHECK(state IN (
        'planned','observing','grounding','acting','verifying','succeeded','failed','escalated'
    )),
    observation_count INTEGER NOT NULL DEFAULT 0 CHECK(observation_count >= 0),
    grounding_count INTEGER NOT NULL DEFAULT 0 CHECK(grounding_count >= 0),
    started_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    finished_at TEXT
);

CREATE TABLE IF NOT EXISTS ui_snapshots (
    id TEXT PRIMARY KEY,
    execution_id TEXT NOT NULL REFERENCES interaction_executions(id) ON DELETE RESTRICT,
    previous_snapshot_id TEXT REFERENCES ui_snapshots(id) ON DELETE RESTRICT,
    ordinal INTEGER NOT NULL CHECK(ordinal >= 0),
    app_id TEXT NOT NULL,
    window_id TEXT NOT NULL,
    source TEXT NOT NULL,
    state_sha256 TEXT NOT NULL CHECK(length(state_sha256) = 64),
    safe_tree_json TEXT NOT NULL,
    focus_element_id TEXT,
    observed_at TEXT NOT NULL,
    UNIQUE(execution_id, ordinal)
);

CREATE TABLE IF NOT EXISTS interaction_actions (
    id TEXT PRIMARY KEY,
    execution_id TEXT NOT NULL REFERENCES interaction_executions(id) ON DELETE RESTRICT,
    ordinal INTEGER NOT NULL CHECK(ordinal >= 0),
    semantic_action TEXT NOT NULL,
    locator_json TEXT NOT NULL,
    grounding_confidence REAL NOT NULL CHECK(grounding_confidence BETWEEN 0 AND 1),
    channel TEXT NOT NULL,
    risk TEXT NOT NULL CHECK(risk IN ('low','medium','high')),
    precondition_json TEXT NOT NULL,
    postcondition_json TEXT NOT NULL,
    state TEXT NOT NULL CHECK(state IN (
        'grounded','started','verified','failed','escalated'
    )),
    before_snapshot_id TEXT REFERENCES ui_snapshots(id) ON DELETE RESTRICT,
    after_snapshot_id TEXT REFERENCES ui_snapshots(id) ON DELETE RESTRICT,
    error_code TEXT,
    started_at TEXT,
    finished_at TEXT,
    UNIQUE(execution_id, ordinal)
);

CREATE TABLE IF NOT EXISTS interaction_trajectories (
    id TEXT PRIMARY KEY,
    execution_id TEXT UNIQUE NOT NULL REFERENCES interaction_executions(id) ON DELETE RESTRICT,
    schema_version TEXT NOT NULL CHECK(schema_version = 'interaction-trajectory/v1'),
    verified INTEGER NOT NULL CHECK(verified IN (0,1)),
    trajectory_json TEXT NOT NULL,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS semantic_skills (
    id TEXT PRIMARY KEY,
    app_id TEXT NOT NULL,
    semantic_action TEXT NOT NULL,
    app_version_constraint TEXT,
    lifecycle TEXT NOT NULL CHECK(lifecycle IN (
        'observed','candidate','validated','active','stale','disabled'
    )),
    revision INTEGER NOT NULL CHECK(revision > 0),
    template_json TEXT NOT NULL,
    success_count INTEGER NOT NULL DEFAULT 0 CHECK(success_count >= 0),
    failure_count INTEGER NOT NULL DEFAULT 0 CHECK(failure_count >= 0),
    confidence REAL NOT NULL CHECK(confidence BETWEEN 0 AND 1),
    source_trajectory_id TEXT NOT NULL REFERENCES interaction_trajectories(id) ON DELETE RESTRICT,
    last_verified_at TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    UNIQUE(app_id, semantic_action, revision)
);

CREATE TABLE IF NOT EXISTS semantic_skill_evidence (
    skill_id TEXT NOT NULL REFERENCES semantic_skills(id) ON DELETE RESTRICT,
    trajectory_id TEXT NOT NULL REFERENCES interaction_trajectories(id) ON DELETE RESTRICT,
    outcome TEXT NOT NULL CHECK(outcome IN ('success','failure','invalidated')),
    verified INTEGER NOT NULL CHECK(verified IN (0,1)),
    app_version TEXT,
    observed_at TEXT NOT NULL,
    PRIMARY KEY(skill_id, trajectory_id)
);

CREATE TABLE IF NOT EXISTS ui_graph_edges (
    app_id TEXT NOT NULL,
    app_version TEXT NOT NULL,
    from_state_sha256 TEXT NOT NULL CHECK(length(from_state_sha256) = 64),
    semantic_action TEXT NOT NULL,
    to_state_sha256 TEXT NOT NULL CHECK(length(to_state_sha256) = 64),
    success_count INTEGER NOT NULL DEFAULT 0 CHECK(success_count >= 0),
    failure_count INTEGER NOT NULL DEFAULT 0 CHECK(failure_count >= 0),
    last_verified_at TEXT,
    confidence REAL NOT NULL CHECK(confidence BETWEEN 0 AND 1),
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    PRIMARY KEY(app_id, app_version, from_state_sha256, semantic_action, to_state_sha256)
);

CREATE TRIGGER IF NOT EXISTS ui_snapshots_no_update
BEFORE UPDATE ON ui_snapshots
BEGIN SELECT RAISE(ABORT, 'UI snapshots are immutable'); END;
CREATE TRIGGER IF NOT EXISTS ui_snapshots_no_delete
BEFORE DELETE ON ui_snapshots
BEGIN SELECT RAISE(ABORT, 'UI snapshots are immutable'); END;
CREATE TRIGGER IF NOT EXISTS interaction_trajectories_no_update
BEFORE UPDATE ON interaction_trajectories
BEGIN SELECT RAISE(ABORT, 'interaction trajectories are immutable'); END;
CREATE TRIGGER IF NOT EXISTS interaction_trajectories_no_delete
BEFORE DELETE ON interaction_trajectories
BEGIN SELECT RAISE(ABORT, 'interaction trajectories are immutable'); END;
CREATE TRIGGER IF NOT EXISTS semantic_skill_evidence_no_update
BEFORE UPDATE ON semantic_skill_evidence
BEGIN SELECT RAISE(ABORT, 'skill evidence is append-only'); END;
CREATE TRIGGER IF NOT EXISTS semantic_skill_evidence_no_delete
BEFORE DELETE ON semantic_skill_evidence
BEGIN SELECT RAISE(ABORT, 'skill evidence is append-only'); END;
