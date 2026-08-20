PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS invocation_telemetry (
    id TEXT PRIMARY KEY,
    goal_id TEXT REFERENCES autonomous_goals(id) ON DELETE SET NULL,
    task_id TEXT NOT NULL REFERENCES tasks(id) ON DELETE CASCADE,
    run_id TEXT NOT NULL UNIQUE REFERENCES worker_runs(id) ON DELETE CASCADE,
    worker_id TEXT NOT NULL REFERENCES workers(id),
    provider TEXT NOT NULL,
    model_value TEXT,
    model_unavailable_reason TEXT,
    node_id TEXT NOT NULL REFERENCES nodes(id),
    task_role TEXT NOT NULL CHECK(task_role IN (
        'primary','reviewer','panelist','router','verifier','planner','evaluator','fallback','other',
        'unknown'
    )),
    outcome TEXT NOT NULL CHECK(outcome IN (
        'success','failure','cancelled','timedOut','interrupted','authRequired','rateLimited'
    )),
    retry_count INTEGER NOT NULL DEFAULT 0 CHECK(retry_count >= 0),
    fallback INTEGER NOT NULL DEFAULT 0 CHECK(fallback IN (0,1)),
    executor_kind TEXT NOT NULL CHECK(executor_kind IN ('codex','offloaded','unknown')),
    recorded_at TEXT NOT NULL,

    duration_value REAL NOT NULL CHECK(duration_value >= 0),
    duration_unit TEXT NOT NULL CHECK(duration_unit = 'seconds'),
    duration_confidence TEXT NOT NULL,
    duration_reason TEXT,

    input_tokens_value INTEGER CHECK(input_tokens_value IS NULL OR input_tokens_value >= 0),
    input_tokens_unit TEXT,
    input_tokens_confidence TEXT NOT NULL,
    input_tokens_reason TEXT,
    output_tokens_value INTEGER CHECK(output_tokens_value IS NULL OR output_tokens_value >= 0),
    output_tokens_unit TEXT,
    output_tokens_confidence TEXT NOT NULL,
    output_tokens_reason TEXT,
    cache_read_tokens_value INTEGER CHECK(
        cache_read_tokens_value IS NULL OR cache_read_tokens_value >= 0
    ),
    cache_read_tokens_unit TEXT,
    cache_read_tokens_confidence TEXT NOT NULL,
    cache_read_tokens_reason TEXT,
    cache_write_tokens_value INTEGER CHECK(
        cache_write_tokens_value IS NULL OR cache_write_tokens_value >= 0
    ),
    cache_write_tokens_unit TEXT,
    cache_write_tokens_confidence TEXT NOT NULL,
    cache_write_tokens_reason TEXT,
    cost_value REAL CHECK(cost_value IS NULL OR cost_value >= 0),
    cost_unit TEXT,
    cost_confidence TEXT NOT NULL,
    cost_reason TEXT,
    remaining_quota_value REAL CHECK(
        remaining_quota_value IS NULL OR remaining_quota_value >= 0
    ),
    remaining_quota_unit TEXT,
    remaining_quota_confidence TEXT NOT NULL,
    remaining_quota_reason TEXT,
    quality_score_value REAL CHECK(
        quality_score_value IS NULL OR quality_score_value BETWEEN 0 AND 1
    ),
    quality_score_unit TEXT,
    quality_score_confidence TEXT NOT NULL,
    quality_score_reason TEXT,

    CHECK((model_value IS NULL AND model_unavailable_reason IS NOT NULL) OR
          (model_value IS NOT NULL AND model_unavailable_reason IS NULL)),
    CHECK(model_unavailable_reason IS NULL OR model_unavailable_reason IN (
        'notReported','notSupported','permissionDenied','stale','offline','unknown'
    )),
    CHECK(duration_reason IS NULL),
    CHECK(duration_confidence IN ('exact','verified','providerReported','inferred')),
    CHECK(input_tokens_confidence IN ('exact','verified','providerReported','inferred','unknown')),
    CHECK(output_tokens_confidence IN ('exact','verified','providerReported','inferred','unknown')),
    CHECK(cache_read_tokens_confidence IN (
        'exact','verified','providerReported','inferred','unknown'
    )),
    CHECK(cache_write_tokens_confidence IN (
        'exact','verified','providerReported','inferred','unknown'
    )),
    CHECK(cost_confidence IN ('exact','verified','providerReported','inferred','unknown')),
    CHECK(remaining_quota_confidence IN (
        'exact','verified','providerReported','inferred','unknown'
    )),
    CHECK(quality_score_confidence IN (
        'exact','verified','providerReported','inferred','unknown'
    )),
    CHECK((input_tokens_value IS NULL AND input_tokens_unit IS NULL AND
           input_tokens_reason IS NOT NULL) OR
          (input_tokens_value IS NOT NULL AND input_tokens_unit = 'tokens' AND
           input_tokens_reason IS NULL)),
    CHECK((input_tokens_value IS NULL AND input_tokens_confidence = 'unknown') OR
          (input_tokens_value IS NOT NULL AND input_tokens_confidence <> 'unknown')),
    CHECK((output_tokens_value IS NULL AND output_tokens_unit IS NULL AND
           output_tokens_reason IS NOT NULL) OR
          (output_tokens_value IS NOT NULL AND output_tokens_unit = 'tokens' AND
           output_tokens_reason IS NULL)),
    CHECK((output_tokens_value IS NULL AND output_tokens_confidence = 'unknown') OR
          (output_tokens_value IS NOT NULL AND output_tokens_confidence <> 'unknown')),
    CHECK((cache_read_tokens_value IS NULL AND cache_read_tokens_unit IS NULL AND
           cache_read_tokens_reason IS NOT NULL) OR
          (cache_read_tokens_value IS NOT NULL AND cache_read_tokens_unit = 'tokens' AND
           cache_read_tokens_reason IS NULL)),
    CHECK((cache_read_tokens_value IS NULL AND cache_read_tokens_confidence = 'unknown') OR
          (cache_read_tokens_value IS NOT NULL AND cache_read_tokens_confidence <> 'unknown')),
    CHECK((cache_write_tokens_value IS NULL AND cache_write_tokens_unit IS NULL AND
           cache_write_tokens_reason IS NOT NULL) OR
          (cache_write_tokens_value IS NOT NULL AND cache_write_tokens_unit = 'tokens' AND
           cache_write_tokens_reason IS NULL)),
    CHECK((cache_write_tokens_value IS NULL AND cache_write_tokens_confidence = 'unknown') OR
          (cache_write_tokens_value IS NOT NULL AND cache_write_tokens_confidence <> 'unknown')),
    CHECK((cost_value IS NULL AND cost_unit IS NULL AND cost_reason IS NOT NULL) OR
          (cost_value IS NOT NULL AND cost_unit IS NOT NULL AND cost_reason IS NULL)),
    CHECK((cost_value IS NULL AND cost_confidence = 'unknown') OR
          (cost_value IS NOT NULL AND cost_confidence <> 'unknown')),
    CHECK((remaining_quota_value IS NULL AND remaining_quota_unit IS NULL AND
           remaining_quota_reason IS NOT NULL) OR
          (remaining_quota_value IS NOT NULL AND remaining_quota_unit IS NOT NULL AND
           remaining_quota_reason IS NULL)),
    CHECK((remaining_quota_value IS NULL AND remaining_quota_confidence = 'unknown') OR
          (remaining_quota_value IS NOT NULL AND remaining_quota_confidence <> 'unknown')),
    CHECK((quality_score_value IS NULL AND quality_score_unit IS NULL AND
           quality_score_reason IS NOT NULL) OR
          (quality_score_value IS NOT NULL AND quality_score_unit = 'ratio' AND
           quality_score_reason IS NULL)),
    CHECK((quality_score_value IS NULL AND quality_score_confidence = 'unknown') OR
          (quality_score_value IS NOT NULL AND quality_score_confidence <> 'unknown')),
    CHECK(input_tokens_reason IS NULL OR input_tokens_reason IN (
        'notReported','notSupported','permissionDenied','stale','offline','unknown'
    )),
    CHECK(output_tokens_reason IS NULL OR output_tokens_reason IN (
        'notReported','notSupported','permissionDenied','stale','offline','unknown'
    )),
    CHECK(cache_read_tokens_reason IS NULL OR cache_read_tokens_reason IN (
        'notReported','notSupported','permissionDenied','stale','offline','unknown'
    )),
    CHECK(cache_write_tokens_reason IS NULL OR cache_write_tokens_reason IN (
        'notReported','notSupported','permissionDenied','stale','offline','unknown'
    )),
    CHECK(cost_reason IS NULL OR cost_reason IN (
        'notReported','notSupported','permissionDenied','stale','offline','unknown'
    )),
    CHECK(remaining_quota_reason IS NULL OR remaining_quota_reason IN (
        'notReported','notSupported','permissionDenied','stale','offline','unknown'
    )),
    CHECK(quality_score_reason IS NULL OR quality_score_reason IN (
        'notReported','notSupported','permissionDenied','stale','offline','unknown'
    ))
);

CREATE INDEX IF NOT EXISTS invocation_telemetry_goal_time
ON invocation_telemetry(goal_id, recorded_at, id);

CREATE INDEX IF NOT EXISTS invocation_telemetry_task_time
ON invocation_telemetry(task_id, recorded_at, id);

CREATE INDEX IF NOT EXISTS invocation_telemetry_worker_time
ON invocation_telemetry(worker_id, recorded_at, id);
