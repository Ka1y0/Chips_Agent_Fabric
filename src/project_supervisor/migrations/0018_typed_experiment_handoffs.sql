PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS structured_executor_handoffs (
    id TEXT PRIMARY KEY,
    schema_version TEXT NOT NULL CHECK(schema_version='pc-codex-handoff/v1'),
    proposal_id TEXT NOT NULL REFERENCES experiment_proposals(id) ON DELETE RESTRICT,
    project_id TEXT NOT NULL REFERENCES projects(id) ON DELETE RESTRICT,
    owner_kind TEXT NOT NULL CHECK(owner_kind IN ('worker','externalManagedExecutor')),
    owner_id TEXT NOT NULL,
    write_lease_id TEXT NOT NULL REFERENCES project_write_leases(id) ON DELETE RESTRICT,
    envelope_id TEXT NOT NULL REFERENCES authorization_envelopes(id) ON DELETE RESTRICT,
    content_kind TEXT NOT NULL CHECK(content_kind IN (
        'analysisPacket','reviewFinding','hypothesisSet','experimentProposal'
    )),
    content_sha256 TEXT NOT NULL CHECK(length(content_sha256)=64),
    expected_result_schema TEXT NOT NULL CHECK(expected_result_schema='experiment-result/v1'),
    state TEXT NOT NULL CHECK(state IN ('awaitingExternalExecutor','ready','completed','cancelled')),
    expires_at TEXT NOT NULL,
    created_at TEXT NOT NULL,
    UNIQUE(proposal_id, owner_id)
);

CREATE TABLE IF NOT EXISTS experiment_results (
    id TEXT PRIMARY KEY,
    handoff_id TEXT NOT NULL REFERENCES structured_executor_handoffs(id) ON DELETE RESTRICT,
    proposal_id TEXT NOT NULL REFERENCES experiment_proposals(id) ON DELETE RESTRICT,
    executor_id TEXT NOT NULL,
    source_run_id TEXT REFERENCES worker_runs(id) ON DELETE RESTRICT,
    result_sha256 TEXT NOT NULL CHECK(length(result_sha256)=64),
    schema_version TEXT NOT NULL CHECK(schema_version='experiment-result/v1'),
    status TEXT NOT NULL CHECK(status IN ('completed','failed','inconclusive')),
    observations_json TEXT NOT NULL,
    created_at TEXT NOT NULL,
    UNIQUE(handoff_id, result_sha256)
);

CREATE TABLE IF NOT EXISTS hypothesis_assessments (
    id TEXT PRIMARY KEY,
    hypothesis_id TEXT NOT NULL REFERENCES hypotheses(id) ON DELETE RESTRICT,
    experiment_result_id TEXT NOT NULL REFERENCES experiment_results(id) ON DELETE RESTRICT,
    relation TEXT NOT NULL CHECK(relation IN ('supports','contradicts','inconclusive')),
    predicate_sha256 TEXT NOT NULL CHECK(length(predicate_sha256)=64),
    created_at TEXT NOT NULL,
    UNIQUE(hypothesis_id, experiment_result_id)
);

CREATE TRIGGER IF NOT EXISTS structured_executor_handoffs_no_update
BEFORE UPDATE ON structured_executor_handoffs
BEGIN SELECT RAISE(ABORT, 'structured executor handoffs are immutable'); END;
CREATE TRIGGER IF NOT EXISTS structured_executor_handoffs_no_delete
BEFORE DELETE ON structured_executor_handoffs
BEGIN SELECT RAISE(ABORT, 'structured executor handoffs are immutable'); END;
CREATE TRIGGER IF NOT EXISTS experiment_results_no_update
BEFORE UPDATE ON experiment_results
BEGIN SELECT RAISE(ABORT, 'experiment results are immutable'); END;
CREATE TRIGGER IF NOT EXISTS experiment_results_no_delete
BEFORE DELETE ON experiment_results
BEGIN SELECT RAISE(ABORT, 'experiment results are immutable'); END;
CREATE TRIGGER IF NOT EXISTS hypothesis_assessments_no_update
BEFORE UPDATE ON hypothesis_assessments
BEGIN SELECT RAISE(ABORT, 'hypothesis assessments are immutable'); END;
CREATE TRIGGER IF NOT EXISTS hypothesis_assessments_no_delete
BEFORE DELETE ON hypothesis_assessments
BEGIN SELECT RAISE(ABORT, 'hypothesis assessments are immutable'); END;

