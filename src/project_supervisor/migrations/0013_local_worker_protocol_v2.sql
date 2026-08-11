PRAGMA foreign_keys = ON;

-- Protocol-v2 launch guarantees are narrower than generic provider idempotency.  Persist the
-- exact capability snapshot observed before dispatch so restart policy and remote inspection do
-- not infer durable lookup/registry support from an adapter or provider name.
ALTER TABLE provider_jobs ADD COLUMN protocol_version INTEGER NOT NULL DEFAULT 1
    CHECK(protocol_version > 0);

ALTER TABLE provider_jobs ADD COLUMN supports_idempotent_launch_lookup INTEGER NOT NULL DEFAULT 0
    CHECK(supports_idempotent_launch_lookup IN (0,1));

ALTER TABLE provider_jobs ADD COLUMN supports_durable_launch_registry INTEGER NOT NULL DEFAULT 0
    CHECK(supports_durable_launch_registry IN (0,1));
