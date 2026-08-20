PRAGMA foreign_keys = ON;

-- Parallel append-only lifecycle table. 0016 remains untouched and its historical rows remain in
-- place; this table copies them once and adds exact process/disclosure/inference boundary stages.
CREATE TABLE IF NOT EXISTS provider_invocation_events_v2 (
    id TEXT PRIMARY KEY,
    invocation_id TEXT NOT NULL REFERENCES provider_invocations_v2(id) ON DELETE RESTRICT,
    ordinal INTEGER NOT NULL CHECK(ordinal > 0),
    event_key TEXT NOT NULL,
    event_sha256 TEXT NOT NULL CHECK(length(event_sha256)=64),
    stage TEXT NOT NULL CHECK(stage IN (
        'requested','dispatched','launching','accepted','processStarted',
        'disclosureRequested','dataDisclosed','modelUsed','inferenceStarted',
        'inferenceCompleted','rejectedBeforeProcess','rejectedBeforeDisclosure',
        'rejectedBeforeInference','failedAfterStart','running','completed','failed',
        'cancelled','outcomeUnknown'
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

INSERT OR IGNORE INTO provider_invocation_events_v2(
    id,invocation_id,ordinal,event_key,event_sha256,stage,model_used_state,model_used,
    data_disclosed_state,detail_code,observed_at,source,created_at
)
SELECT
    id,invocation_id,ordinal,event_key,event_sha256,stage,model_used_state,model_used,
    data_disclosed_state,detail_code,observed_at,source,created_at
FROM provider_invocation_observations;

CREATE TRIGGER IF NOT EXISTS provider_invocation_events_v2_no_update
BEFORE UPDATE ON provider_invocation_events_v2
BEGIN SELECT RAISE(ABORT, 'provider invocation v2 events are append-only'); END;

CREATE TRIGGER IF NOT EXISTS provider_invocation_events_v2_no_delete
BEFORE DELETE ON provider_invocation_events_v2
BEGIN SELECT RAISE(ABORT, 'provider invocation v2 events are append-only'); END;
