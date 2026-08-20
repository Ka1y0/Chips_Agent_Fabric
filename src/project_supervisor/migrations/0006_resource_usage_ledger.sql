PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS post_task_usage_audits (
    id TEXT PRIMARY KEY,
    audit_key TEXT NOT NULL UNIQUE,
    project_id TEXT NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    goal_id TEXT REFERENCES autonomous_goals(id) ON DELETE SET NULL,
    task_id TEXT NOT NULL REFERENCES tasks(id) ON DELETE CASCADE,
    run_id TEXT REFERENCES worker_runs(id) ON DELETE SET NULL,
    terminal_state TEXT NOT NULL,
    status TEXT NOT NULL CHECK(status IN ('completed','completedWithErrors')),
    observer_count INTEGER NOT NULL DEFAULT 0 CHECK(observer_count >= 0),
    snapshot_count INTEGER NOT NULL DEFAULT 0 CHECK(snapshot_count >= 0),
    errors_json TEXT NOT NULL DEFAULT '[]',
    observed_at TEXT NOT NULL,
    created_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS post_task_usage_audits_goal_time
ON post_task_usage_audits(goal_id, observed_at, id);

CREATE INDEX IF NOT EXISTS post_task_usage_audits_task_time
ON post_task_usage_audits(task_id, observed_at, id);

CREATE TABLE IF NOT EXISTS resource_usage_snapshots (
    id TEXT PRIMARY KEY,
    audit_id TEXT NOT NULL REFERENCES post_task_usage_audits(id) ON DELETE CASCADE,
    project_id TEXT NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    goal_id TEXT REFERENCES autonomous_goals(id) ON DELETE SET NULL,
    task_id TEXT NOT NULL REFERENCES tasks(id) ON DELETE CASCADE,
    run_id TEXT REFERENCES worker_runs(id) ON DELETE SET NULL,
    provider TEXT NOT NULL,
    quota_pool_id TEXT NOT NULL,

    account_scope_value TEXT,
    account_scope_provenance TEXT NOT NULL,
    account_scope_reason TEXT,
    plan_tier_value TEXT,
    plan_tier_provenance TEXT NOT NULL,
    plan_tier_reason TEXT,
    worker_id TEXT REFERENCES workers(id) ON DELETE SET NULL,
    model_value TEXT,
    model_provenance TEXT NOT NULL,
    model_reason TEXT,
    quota_window_value TEXT,
    quota_window_provenance TEXT NOT NULL,
    quota_window_reason TEXT,

    used_value REAL CHECK(used_value IS NULL OR used_value >= 0),
    used_unit TEXT,
    used_provenance TEXT NOT NULL,
    used_reason TEXT,
    remaining_value REAL CHECK(remaining_value IS NULL OR remaining_value >= 0),
    remaining_unit TEXT,
    remaining_provenance TEXT NOT NULL,
    remaining_reason TEXT,

    reset_at_value TEXT,
    reset_at_provenance TEXT NOT NULL,
    reset_at_reason TEXT,

    task_calls_value INTEGER CHECK(task_calls_value IS NULL OR task_calls_value >= 0),
    task_calls_unit TEXT,
    task_calls_provenance TEXT NOT NULL,
    task_calls_reason TEXT,
    input_tokens_value INTEGER CHECK(input_tokens_value IS NULL OR input_tokens_value >= 0),
    input_tokens_unit TEXT,
    input_tokens_provenance TEXT NOT NULL,
    input_tokens_reason TEXT,
    output_tokens_value INTEGER CHECK(output_tokens_value IS NULL OR output_tokens_value >= 0),
    output_tokens_unit TEXT,
    output_tokens_provenance TEXT NOT NULL,
    output_tokens_reason TEXT,
    cached_tokens_value INTEGER CHECK(cached_tokens_value IS NULL OR cached_tokens_value >= 0),
    cached_tokens_unit TEXT,
    cached_tokens_provenance TEXT NOT NULL,
    cached_tokens_reason TEXT,
    cost_value REAL CHECK(cost_value IS NULL OR cost_value >= 0),
    cost_unit TEXT,
    cost_provenance TEXT NOT NULL,
    cost_reason TEXT,

    quota_state TEXT NOT NULL CHECK(quota_state IN (
        'available','warning','critical','exhausted','unknown'
    )),
    quota_state_provenance TEXT NOT NULL CHECK(quota_state_provenance IN (
        'PROVIDER_REPORTED','LOCALLY_MEASURED','INFERRED','UNKNOWN'
    )),
    source TEXT NOT NULL,
    confidence TEXT NOT NULL CHECK(confidence IN (
        'exact','verified','providerReported','inferred','unknown'
    )),
    observed_at TEXT NOT NULL,
    fresh_until TEXT,
    metadata_json TEXT NOT NULL DEFAULT '{}',

    CHECK(account_scope_provenance IN (
        'PROVIDER_REPORTED','LOCALLY_MEASURED','INFERRED','UNKNOWN'
    )),
    CHECK(plan_tier_provenance IN (
        'PROVIDER_REPORTED','LOCALLY_MEASURED','INFERRED','UNKNOWN'
    )),
    CHECK(model_provenance IN (
        'PROVIDER_REPORTED','LOCALLY_MEASURED','INFERRED','UNKNOWN'
    )),
    CHECK(quota_window_provenance IN (
        'PROVIDER_REPORTED','LOCALLY_MEASURED','INFERRED','UNKNOWN'
    )),
    CHECK(used_provenance IN (
        'PROVIDER_REPORTED','LOCALLY_MEASURED','INFERRED','UNKNOWN'
    )),
    CHECK(remaining_provenance IN (
        'PROVIDER_REPORTED','LOCALLY_MEASURED','INFERRED','UNKNOWN'
    )),
    CHECK(reset_at_provenance IN (
        'PROVIDER_REPORTED','LOCALLY_MEASURED','INFERRED','UNKNOWN'
    )),
    CHECK(task_calls_provenance IN (
        'PROVIDER_REPORTED','LOCALLY_MEASURED','INFERRED','UNKNOWN'
    )),
    CHECK(input_tokens_provenance IN (
        'PROVIDER_REPORTED','LOCALLY_MEASURED','INFERRED','UNKNOWN'
    )),
    CHECK(output_tokens_provenance IN (
        'PROVIDER_REPORTED','LOCALLY_MEASURED','INFERRED','UNKNOWN'
    )),
    CHECK(cached_tokens_provenance IN (
        'PROVIDER_REPORTED','LOCALLY_MEASURED','INFERRED','UNKNOWN'
    )),
    CHECK(cost_provenance IN (
        'PROVIDER_REPORTED','LOCALLY_MEASURED','INFERRED','UNKNOWN'
    )),
    CHECK((quota_state='unknown' AND quota_state_provenance='UNKNOWN') OR
          (quota_state<>'unknown' AND quota_state_provenance<>'UNKNOWN')),

    CHECK((account_scope_value IS NULL AND account_scope_provenance='UNKNOWN' AND
           account_scope_reason IS NOT NULL) OR
          (account_scope_value IS NOT NULL AND account_scope_provenance<>'UNKNOWN' AND
           account_scope_reason IS NULL)),
    CHECK((plan_tier_value IS NULL AND plan_tier_provenance='UNKNOWN' AND
           plan_tier_reason IS NOT NULL) OR
          (plan_tier_value IS NOT NULL AND plan_tier_provenance<>'UNKNOWN' AND
           plan_tier_reason IS NULL)),
    CHECK((model_value IS NULL AND model_provenance='UNKNOWN' AND model_reason IS NOT NULL) OR
          (model_value IS NOT NULL AND model_provenance<>'UNKNOWN' AND model_reason IS NULL)),
    CHECK((quota_window_value IS NULL AND quota_window_provenance='UNKNOWN' AND
           quota_window_reason IS NOT NULL) OR
          (quota_window_value IS NOT NULL AND quota_window_provenance<>'UNKNOWN' AND
           quota_window_reason IS NULL)),
    CHECK((used_value IS NULL AND used_unit IS NULL AND used_provenance='UNKNOWN' AND
           used_reason IS NOT NULL) OR
          (used_value IS NOT NULL AND used_unit IS NOT NULL AND used_provenance<>'UNKNOWN' AND
           used_reason IS NULL)),
    CHECK((remaining_value IS NULL AND remaining_unit IS NULL AND
           remaining_provenance='UNKNOWN' AND remaining_reason IS NOT NULL) OR
          (remaining_value IS NOT NULL AND remaining_unit IS NOT NULL AND
           remaining_provenance<>'UNKNOWN' AND remaining_reason IS NULL)),
    CHECK((reset_at_value IS NULL AND reset_at_provenance='UNKNOWN' AND
           reset_at_reason IS NOT NULL) OR
          (reset_at_value IS NOT NULL AND reset_at_provenance<>'UNKNOWN' AND
           reset_at_reason IS NULL)),
    CHECK((task_calls_value IS NULL AND task_calls_unit IS NULL AND
           task_calls_provenance='UNKNOWN' AND task_calls_reason IS NOT NULL) OR
          (task_calls_value IS NOT NULL AND task_calls_unit='calls' AND
           task_calls_provenance<>'UNKNOWN' AND task_calls_reason IS NULL)),
    CHECK((input_tokens_value IS NULL AND input_tokens_unit IS NULL AND
           input_tokens_provenance='UNKNOWN' AND input_tokens_reason IS NOT NULL) OR
          (input_tokens_value IS NOT NULL AND input_tokens_unit='tokens' AND
           input_tokens_provenance<>'UNKNOWN' AND input_tokens_reason IS NULL)),
    CHECK((output_tokens_value IS NULL AND output_tokens_unit IS NULL AND
           output_tokens_provenance='UNKNOWN' AND output_tokens_reason IS NOT NULL) OR
          (output_tokens_value IS NOT NULL AND output_tokens_unit='tokens' AND
           output_tokens_provenance<>'UNKNOWN' AND output_tokens_reason IS NULL)),
    CHECK((cached_tokens_value IS NULL AND cached_tokens_unit IS NULL AND
           cached_tokens_provenance='UNKNOWN' AND cached_tokens_reason IS NOT NULL) OR
          (cached_tokens_value IS NOT NULL AND cached_tokens_unit='tokens' AND
           cached_tokens_provenance<>'UNKNOWN' AND cached_tokens_reason IS NULL)),
    CHECK((cost_value IS NULL AND cost_unit IS NULL AND cost_provenance='UNKNOWN' AND
           cost_reason IS NOT NULL) OR
          (cost_value IS NOT NULL AND cost_unit IS NOT NULL AND cost_provenance<>'UNKNOWN' AND
           cost_reason IS NULL))
);

CREATE INDEX IF NOT EXISTS resource_usage_snapshots_pool_time
ON resource_usage_snapshots(quota_pool_id, observed_at, id);

CREATE INDEX IF NOT EXISTS resource_usage_snapshots_goal_time
ON resource_usage_snapshots(goal_id, observed_at, id);

CREATE INDEX IF NOT EXISTS resource_usage_snapshots_task_time
ON resource_usage_snapshots(task_id, observed_at, id);

CREATE INDEX IF NOT EXISTS resource_usage_snapshots_provider_time
ON resource_usage_snapshots(provider, observed_at, id);

CREATE INDEX IF NOT EXISTS resource_usage_snapshots_worker_time
ON resource_usage_snapshots(worker_id, observed_at, id);
