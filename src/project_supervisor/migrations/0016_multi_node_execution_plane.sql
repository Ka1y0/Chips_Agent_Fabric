PRAGMA foreign_keys = ON;

-- A Fabric Node is an operator-owned identity.  Transport peer identity is only evidence and is
-- persisted as a one-way fingerprint so private addresses and hostnames never become authority.
CREATE TABLE IF NOT EXISTS node_transport_bindings (
    id TEXT PRIMARY KEY,
    node_id TEXT NOT NULL REFERENCES nodes(id) ON DELETE RESTRICT,
    transport_provider TEXT NOT NULL,
    peer_identity_sha256 TEXT NOT NULL CHECK(length(peer_identity_sha256) = 64),
    service_name TEXT NOT NULL,
    endpoint_ref TEXT NOT NULL,
    expected_platform TEXT,
    enabled INTEGER NOT NULL CHECK(enabled IN (0,1)),
    generation INTEGER NOT NULL CHECK(generation > 0),
    configured_by TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    UNIQUE(transport_provider, peer_identity_sha256),
    UNIQUE(node_id, transport_provider)
);

-- Runtime executability is observed, never inferred from transport reachability or a static
-- capability manifest.  All boolean-like facets retain UNKNOWN as first-class state.
CREATE TABLE IF NOT EXISTS worker_execution_observations (
    id TEXT PRIMARY KEY,
    worker_id TEXT NOT NULL REFERENCES workers(id) ON DELETE RESTRICT,
    node_id TEXT NOT NULL REFERENCES nodes(id) ON DELETE RESTRICT,
    binding_id TEXT REFERENCES node_transport_bindings(id) ON DELETE RESTRICT,
    version INTEGER NOT NULL CHECK(version > 0),
    schema_version TEXT NOT NULL CHECK(schema_version='worker-executability/v1'),
    discovered TEXT NOT NULL CHECK(discovered IN ('yes','no','unknown')),
    configured TEXT NOT NULL CHECK(configured IN ('yes','no','unknown')),
    authenticated TEXT NOT NULL CHECK(authenticated IN ('yes','no','unknown')),
    authorized TEXT NOT NULL CHECK(authorized IN ('yes','no','unknown')),
    platform_approval TEXT NOT NULL CHECK(platform_approval IN (
        'notRequired','approved','pending','rejected','unknown'
    )),
    reachable TEXT NOT NULL CHECK(reachable IN ('yes','no','unknown')),
    runtime_available TEXT NOT NULL CHECK(runtime_available IN ('yes','no','unknown')),
    healthy TEXT NOT NULL CHECK(healthy IN ('yes','no','unknown')),
    capacity_available TEXT NOT NULL CHECK(capacity_available IN ('yes','no','unknown')),
    protocol_version TEXT,
    runtime_identity_sha256 TEXT CHECK(
        runtime_identity_sha256 IS NULL OR length(runtime_identity_sha256)=64
    ),
    reason_codes_json TEXT NOT NULL DEFAULT '[]',
    observed_at TEXT NOT NULL,
    valid_until TEXT NOT NULL,
    created_at TEXT NOT NULL,
    UNIQUE(worker_id, version)
);

CREATE INDEX IF NOT EXISTS worker_execution_observations_latest
ON worker_execution_observations(worker_id, version DESC);

CREATE TRIGGER IF NOT EXISTS worker_execution_observations_no_update
BEFORE UPDATE ON worker_execution_observations
BEGIN SELECT RAISE(ABORT, 'Worker execution observations are append-only'); END;
CREATE TRIGGER IF NOT EXISTS worker_execution_observations_no_delete
BEFORE DELETE ON worker_execution_observations
BEGIN SELECT RAISE(ABORT, 'Worker execution observations are append-only'); END;

-- Recoverable lack of execution capacity is distinct from a terminal Task failure.  A wait is
-- resolved only after a fresh execution observation proves capacity and Task steering still
-- permits admission.
CREATE TABLE IF NOT EXISTS task_capacity_waits (
    task_id TEXT PRIMARY KEY REFERENCES tasks(id) ON DELETE RESTRICT,
    requirements_sha256 TEXT NOT NULL CHECK(length(requirements_sha256)=64),
    state TEXT NOT NULL CHECK(state IN ('waiting','resolved','escalated','cancelled')),
    reason_code TEXT NOT NULL,
    first_observed_at TEXT NOT NULL,
    last_checked_at TEXT NOT NULL,
    next_check_at TEXT NOT NULL,
    recovery_attempts INTEGER NOT NULL DEFAULT 0 CHECK(recovery_attempts >= 0),
    max_recovery_attempts INTEGER NOT NULL CHECK(max_recovery_attempts BETWEEN 1 AND 8),
    recovery_owner_id TEXT,
    recovery_lease_expires_at TEXT,
    resolved_at TEXT,
    version INTEGER NOT NULL DEFAULT 1 CHECK(version > 0)
);

-- Authorization is durable data authority, not a platform permission grant.  Child envelopes are
-- immutable narrowed descendants; task/run bindings make the actual execution authority explicit.
CREATE TABLE IF NOT EXISTS authorization_envelopes (
    id TEXT PRIMARY KEY,
    parent_id TEXT REFERENCES authorization_envelopes(id) ON DELETE RESTRICT,
    project_id TEXT NOT NULL REFERENCES projects(id) ON DELETE RESTRICT,
    goal_id TEXT REFERENCES autonomous_goals(id) ON DELETE RESTRICT,
    root_task_id TEXT NOT NULL REFERENCES tasks(id) ON DELETE RESTRICT,
    subject TEXT NOT NULL,
    schema_version TEXT NOT NULL CHECK(schema_version='authorization-envelope/v1'),
    permission_ceiling TEXT NOT NULL CHECK(permission_ceiling IN ('green','yellow','red')),
    capabilities_json TEXT NOT NULL,
    actions_json TEXT NOT NULL,
    resources_json TEXT NOT NULL,
    data_refs_json TEXT NOT NULL,
    budget_json TEXT NOT NULL,
    user_approval_ref TEXT,
    user_approval_state TEXT NOT NULL CHECK(user_approval_state IN (
        'notRequired','pending','approved','rejected','expired','unknown'
    )),
    platform_approval_required INTEGER NOT NULL CHECK(platform_approval_required IN (0,1)),
    platform_approval_state TEXT NOT NULL CHECK(platform_approval_state IN (
        'notRequired','pending','approved','rejected','unknown'
    )),
    issued_by TEXT NOT NULL,
    issued_at TEXT NOT NULL,
    expires_at TEXT,
    definition_sha256 TEXT NOT NULL CHECK(length(definition_sha256)=64),
    created_at TEXT NOT NULL,
    UNIQUE(parent_id, definition_sha256)
);

CREATE TABLE IF NOT EXISTS authorization_envelope_bindings (
    id TEXT PRIMARY KEY,
    envelope_id TEXT NOT NULL REFERENCES authorization_envelopes(id) ON DELETE RESTRICT,
    task_id TEXT NOT NULL REFERENCES tasks(id) ON DELETE RESTRICT,
    run_id TEXT REFERENCES worker_runs(id) ON DELETE RESTRICT,
    binding_kind TEXT NOT NULL CHECK(binding_kind IN ('task','run','childProposal','experiment')),
    bound_by TEXT NOT NULL,
    created_at TEXT NOT NULL,
    UNIQUE(envelope_id, task_id, run_id, binding_kind)
);

CREATE TRIGGER IF NOT EXISTS authorization_envelopes_no_update
BEFORE UPDATE ON authorization_envelopes
BEGIN SELECT RAISE(ABORT, 'authorization envelopes are immutable'); END;
CREATE TRIGGER IF NOT EXISTS authorization_envelopes_no_delete
BEFORE DELETE ON authorization_envelopes
BEGIN SELECT RAISE(ABORT, 'authorization envelopes are immutable'); END;
CREATE TRIGGER IF NOT EXISTS authorization_envelope_bindings_no_update
BEFORE UPDATE ON authorization_envelope_bindings
BEGIN SELECT RAISE(ABORT, 'authorization envelope bindings are append-only'); END;
CREATE TRIGGER IF NOT EXISTS authorization_envelope_bindings_no_delete
BEFORE DELETE ON authorization_envelope_bindings
BEGIN SELECT RAISE(ABORT, 'authorization envelope bindings are append-only'); END;

-- Evidence is referenced by digest/classification.  Raw evidence and credentials are deliberately
-- outside this journal.  Movement records prove where bounded evidence was intentionally sent.
CREATE TABLE IF NOT EXISTS data_evidence_packets (
    id TEXT PRIMARY KEY,
    project_id TEXT NOT NULL REFERENCES projects(id) ON DELETE RESTRICT,
    task_id TEXT REFERENCES tasks(id) ON DELETE RESTRICT,
    source_kind TEXT NOT NULL,
    source_ref TEXT NOT NULL,
    content_sha256 TEXT NOT NULL CHECK(length(content_sha256)=64),
    classification TEXT NOT NULL CHECK(classification IN (
        'public','internal','confidential','restricted','unknown'
    )),
    byte_count INTEGER CHECK(byte_count IS NULL OR byte_count >= 0),
    contains_credentials TEXT NOT NULL CHECK(contains_credentials IN ('yes','no','unknown')),
    created_by TEXT NOT NULL,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS data_movement_events (
    id TEXT PRIMARY KEY,
    packet_id TEXT NOT NULL REFERENCES data_evidence_packets(id) ON DELETE RESTRICT,
    envelope_id TEXT NOT NULL REFERENCES authorization_envelopes(id) ON DELETE RESTRICT,
    run_id TEXT REFERENCES worker_runs(id) ON DELETE RESTRICT,
    destination_kind TEXT NOT NULL,
    destination_ref TEXT NOT NULL,
    purpose TEXT NOT NULL,
    disclosure_state TEXT NOT NULL CHECK(disclosure_state IN (
        'notDisclosed','disclosed','unknown'
    )),
    transport_ref TEXT,
    occurred_at TEXT NOT NULL,
    recorded_by TEXT NOT NULL,
    created_at TEXT NOT NULL
);

CREATE TRIGGER IF NOT EXISTS data_evidence_packets_no_update
BEFORE UPDATE ON data_evidence_packets
BEGIN SELECT RAISE(ABORT, 'data evidence packets are immutable'); END;
CREATE TRIGGER IF NOT EXISTS data_evidence_packets_no_delete
BEFORE DELETE ON data_evidence_packets
BEGIN SELECT RAISE(ABORT, 'data evidence packets are immutable'); END;
CREATE TRIGGER IF NOT EXISTS data_movement_events_no_update
BEFORE UPDATE ON data_movement_events
BEGIN SELECT RAISE(ABORT, 'data movement events are append-only'); END;
CREATE TRIGGER IF NOT EXISTS data_movement_events_no_delete
BEFORE DELETE ON data_movement_events
BEGIN SELECT RAISE(ABORT, 'data movement events are append-only'); END;

-- One canonical Worker run may perform multiple provider invocations.  Lifecycle observations are
-- monotonic and do not infer MODEL_USED or DATA_DISCLOSED from a successful process exit.
CREATE TABLE IF NOT EXISTS provider_invocations_v2 (
    id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL REFERENCES worker_runs(id) ON DELETE RESTRICT,
    ordinal INTEGER NOT NULL CHECK(ordinal > 0),
    provider TEXT NOT NULL,
    requested_model TEXT,
    envelope_id TEXT REFERENCES authorization_envelopes(id) ON DELETE RESTRICT,
    idempotency_key_sha256 TEXT CHECK(
        idempotency_key_sha256 IS NULL OR length(idempotency_key_sha256)=64
    ),
    created_at TEXT NOT NULL,
    UNIQUE(run_id, ordinal)
);

CREATE TABLE IF NOT EXISTS provider_invocation_observations (
    id TEXT PRIMARY KEY,
    invocation_id TEXT NOT NULL REFERENCES provider_invocations_v2(id) ON DELETE RESTRICT,
    ordinal INTEGER NOT NULL CHECK(ordinal > 0),
    event_key TEXT NOT NULL,
    event_sha256 TEXT NOT NULL CHECK(length(event_sha256)=64),
    stage TEXT NOT NULL CHECK(stage IN (
        'requested','launching','accepted','modelUsed','dataDisclosed','running',
        'completed','failed','cancelled','outcomeUnknown'
    )),
    model_used_state TEXT NOT NULL CHECK(model_used_state IN ('yes','no','unknown')),
    model_used TEXT,
    data_disclosed_state TEXT NOT NULL CHECK(data_disclosed_state IN ('yes','no','unknown')),
    detail_code TEXT,
    observed_at TEXT NOT NULL,
    source TEXT NOT NULL,
    created_at TEXT NOT NULL,
    UNIQUE(invocation_id, ordinal),
    UNIQUE(invocation_id, event_key)
);

CREATE TRIGGER IF NOT EXISTS provider_invocations_v2_no_update
BEFORE UPDATE ON provider_invocations_v2
BEGIN SELECT RAISE(ABORT, 'provider invocations are immutable'); END;
CREATE TRIGGER IF NOT EXISTS provider_invocations_v2_no_delete
BEFORE DELETE ON provider_invocations_v2
BEGIN SELECT RAISE(ABORT, 'provider invocations are immutable'); END;
CREATE TRIGGER IF NOT EXISTS provider_invocation_observations_no_update
BEFORE UPDATE ON provider_invocation_observations
BEGIN SELECT RAISE(ABORT, 'provider invocation observations are append-only'); END;
CREATE TRIGGER IF NOT EXISTS provider_invocation_observations_no_delete
BEFORE DELETE ON provider_invocation_observations
BEGIN SELECT RAISE(ABORT, 'provider invocation observations are append-only'); END;

-- Provider concurrency is an operator-declared capacity pool, not a guess based on a brand name.
-- Reservations stay held across Supervisor restart until canonical provider reconciliation proves
-- terminal or definitely-not-launched state.
CREATE TABLE IF NOT EXISTS provider_capacity_pools (
    id TEXT PRIMARY KEY,
    display_name TEXT NOT NULL,
    max_concurrency INTEGER NOT NULL CHECK(max_concurrency > 0),
    policy_version TEXT NOT NULL,
    enabled INTEGER NOT NULL CHECK(enabled IN (0,1)),
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS provider_capacity_pool_workers (
    pool_id TEXT NOT NULL REFERENCES provider_capacity_pools(id) ON DELETE RESTRICT,
    worker_id TEXT NOT NULL UNIQUE REFERENCES workers(id) ON DELETE RESTRICT,
    created_at TEXT NOT NULL,
    PRIMARY KEY(pool_id, worker_id)
);

CREATE TABLE IF NOT EXISTS provider_capacity_reservations (
    run_id TEXT PRIMARY KEY REFERENCES worker_runs(id) ON DELETE RESTRICT,
    pool_id TEXT NOT NULL REFERENCES provider_capacity_pools(id) ON DELETE RESTRICT,
    task_id TEXT NOT NULL REFERENCES tasks(id) ON DELETE RESTRICT,
    state TEXT NOT NULL CHECK(state IN ('reserved','released')),
    reserved_at TEXT NOT NULL,
    released_at TEXT,
    release_reason TEXT
);

CREATE INDEX IF NOT EXISTS provider_capacity_reservations_active
ON provider_capacity_reservations(pool_id, state, reserved_at);

-- Typed analysis normalization binds claims to one immutable Worker result before deterministic
-- fusion.  Hypotheses and experiments preserve contradictory alternatives rather than erasing them.
CREATE TABLE IF NOT EXISTS analysis_result_contributions (
    id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL UNIQUE REFERENCES worker_runs(id) ON DELETE RESTRICT,
    result_sha256 TEXT NOT NULL CHECK(length(result_sha256)=64),
    schema_version TEXT NOT NULL CHECK(schema_version='analysis-contribution/v1'),
    claims_json TEXT NOT NULL,
    evidence_json TEXT NOT NULL,
    normalized_sha256 TEXT NOT NULL CHECK(length(normalized_sha256)=64),
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS hypothesis_sets (
    id TEXT PRIMARY KEY,
    task_id TEXT NOT NULL REFERENCES tasks(id) ON DELETE RESTRICT,
    source_attempt INTEGER NOT NULL CHECK(source_attempt > 0),
    fusion_id TEXT REFERENCES result_fusion_decisions(id) ON DELETE RESTRICT,
    state TEXT NOT NULL CHECK(state IN ('open','experimentRequired','resolved','escalated')),
    input_sha256 TEXT NOT NULL CHECK(length(input_sha256)=64),
    created_at TEXT NOT NULL,
    UNIQUE(task_id, source_attempt, input_sha256)
);

CREATE TABLE IF NOT EXISTS hypotheses (
    id TEXT PRIMARY KEY,
    set_id TEXT NOT NULL REFERENCES hypothesis_sets(id) ON DELETE RESTRICT,
    claim_key TEXT NOT NULL,
    value_json TEXT NOT NULL,
    value_sha256 TEXT NOT NULL CHECK(length(value_sha256)=64),
    provenance_json TEXT NOT NULL,
    confidence REAL CHECK(confidence IS NULL OR confidence BETWEEN 0 AND 1),
    state TEXT NOT NULL CHECK(state IN ('supported','contradicted','untested','rejected')),
    created_at TEXT NOT NULL,
    UNIQUE(set_id, claim_key, value_sha256)
);

CREATE TABLE IF NOT EXISTS experiment_proposals (
    id TEXT PRIMARY KEY,
    set_id TEXT NOT NULL REFERENCES hypothesis_sets(id) ON DELETE RESTRICT,
    proposal_key TEXT NOT NULL,
    operation TEXT NOT NULL,
    specification_json TEXT NOT NULL,
    risk_class TEXT NOT NULL CHECK(risk_class IN ('green','yellow','red')),
    expected_information_gain REAL CHECK(
        expected_information_gain IS NULL OR expected_information_gain BETWEEN 0 AND 1
    ),
    envelope_id TEXT REFERENCES authorization_envelopes(id) ON DELETE RESTRICT,
    state TEXT NOT NULL CHECK(state IN ('proposed','approved','rejected','executed','cancelled')),
    created_at TEXT NOT NULL,
    UNIQUE(set_id, proposal_key)
);

CREATE TABLE IF NOT EXISTS experiment_handoffs (
    id TEXT PRIMARY KEY,
    proposal_id TEXT NOT NULL REFERENCES experiment_proposals(id) ON DELETE RESTRICT,
    target_worker_id TEXT NOT NULL REFERENCES workers(id) ON DELETE RESTRICT,
    instruction_digest TEXT NOT NULL CHECK(length(instruction_digest)=64),
    single_write_owner INTEGER NOT NULL CHECK(single_write_owner IN (0,1)),
    created_at TEXT NOT NULL,
    UNIQUE(proposal_id, target_worker_id)
);

CREATE TRIGGER IF NOT EXISTS analysis_result_contributions_no_update
BEFORE UPDATE ON analysis_result_contributions
BEGIN SELECT RAISE(ABORT, 'analysis result contributions are immutable'); END;
CREATE TRIGGER IF NOT EXISTS hypothesis_sets_no_update
BEFORE UPDATE ON hypothesis_sets
BEGIN SELECT RAISE(ABORT, 'hypothesis sets are immutable'); END;
CREATE TRIGGER IF NOT EXISTS hypotheses_no_update
BEFORE UPDATE ON hypotheses
BEGIN SELECT RAISE(ABORT, 'hypotheses are immutable'); END;
CREATE TRIGGER IF NOT EXISTS experiment_handoffs_no_update
BEFORE UPDATE ON experiment_handoffs
BEGIN SELECT RAISE(ABORT, 'experiment handoffs are immutable'); END;

-- UI actions need phase evidence before and after a side effect.  An unfinished actionStarted
-- checkpoint is outcomeUnknown on recovery and must never be blindly replayed.
CREATE TABLE IF NOT EXISTS interaction_action_checkpoints (
    id TEXT PRIMARY KEY,
    execution_id TEXT NOT NULL REFERENCES interaction_executions(id) ON DELETE RESTRICT,
    action_ordinal INTEGER NOT NULL CHECK(action_ordinal >= 0),
    phase TEXT NOT NULL CHECK(phase IN (
        'observed','grounded','preconditionChecked','actionStarted','actionReturned',
        'postconditionVerified','failed','outcomeUnknown'
    )),
    task_lease_owner_id TEXT,
    task_lease_generation INTEGER,
    resource_lease_group_id TEXT,
    resource_lease_generation INTEGER,
    snapshot_sha256 TEXT CHECK(snapshot_sha256 IS NULL OR length(snapshot_sha256)=64),
    detail_code TEXT,
    created_at TEXT NOT NULL,
    UNIQUE(execution_id, action_ordinal, phase)
);

CREATE TRIGGER IF NOT EXISTS interaction_action_checkpoints_no_update
BEFORE UPDATE ON interaction_action_checkpoints
BEGIN SELECT RAISE(ABORT, 'interaction action checkpoints are append-only'); END;
CREATE TRIGGER IF NOT EXISTS interaction_action_checkpoints_no_delete
BEFORE DELETE ON interaction_action_checkpoints
BEGIN SELECT RAISE(ABORT, 'interaction action checkpoints are append-only'); END;
