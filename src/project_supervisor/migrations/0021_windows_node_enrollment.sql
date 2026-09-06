PRAGMA foreign_keys = ON;

-- Enrollment is an authority-controlled state machine.  The bearer itself is deliberately absent:
-- only a salted verifier and an opaque secure-store reference are persisted.
CREATE TABLE IF NOT EXISTS pending_node_enrollments (
    id TEXT PRIMARY KEY,
    generation INTEGER NOT NULL CHECK(generation > 0),
    state TEXT NOT NULL CHECK(state IN ('pending','admitted','revoked','expired')),
    request_id TEXT NOT NULL,
    request_sha256 TEXT NOT NULL CHECK(length(request_sha256)=64),
    expected_hostname TEXT NOT NULL,
    expected_machine_binding_sha256 TEXT NOT NULL CHECK(length(expected_machine_binding_sha256)=64),
    expected_peer_identity_sha256 TEXT NOT NULL CHECK(length(expected_peer_identity_sha256)=64),
    expected_platform TEXT NOT NULL CHECK(expected_platform='windows'),
    expected_architecture TEXT NOT NULL CHECK(expected_architecture='x64'),
    artifact_id TEXT NOT NULL,
    artifact_sha256 TEXT NOT NULL CHECK(length(artifact_sha256)=64),
    manifest_sha256 TEXT NOT NULL CHECK(length(manifest_sha256)=64),
    service_profile_id TEXT NOT NULL,
    service_profile_revision INTEGER NOT NULL CHECK(service_profile_revision > 0),
    service_profile_sha256 TEXT NOT NULL CHECK(length(service_profile_sha256)=64),
    issuer_node_id TEXT NOT NULL REFERENCES nodes(id) ON DELETE RESTRICT,
    issuer_key_id TEXT NOT NULL,
    issuer_public_key_fingerprint TEXT NOT NULL,
    authorization_scope_json TEXT NOT NULL,
    requested_capabilities_json TEXT NOT NULL,
    credential_reference TEXT NOT NULL,
    credential_salt TEXT NOT NULL CHECK(length(credential_salt)=32),
    credential_verifier_sha256 TEXT NOT NULL CHECK(length(credential_verifier_sha256)=64),
    bundle_json TEXT NOT NULL,
    bundle_sha256 TEXT NOT NULL CHECK(length(bundle_sha256)=64),
    issued_at TEXT NOT NULL,
    expires_at TEXT NOT NULL,
    consumed_at TEXT,
    revoked_at TEXT,
    admitted_node_id TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    CHECK(expires_at > issued_at),
    CHECK((state='admitted') = (consumed_at IS NOT NULL)),
    CHECK((state='revoked') = (revoked_at IS NOT NULL)),
    UNIQUE(request_sha256, generation),
    UNIQUE(bundle_sha256)
);

CREATE INDEX IF NOT EXISTS pending_node_enrollments_request_state
ON pending_node_enrollments(request_sha256,state,expires_at);

CREATE TABLE IF NOT EXISTS node_enrollment_receipts (
    enrollment_id TEXT PRIMARY KEY REFERENCES pending_node_enrollments(id) ON DELETE RESTRICT,
    receipt_sha256 TEXT NOT NULL UNIQUE CHECK(length(receipt_sha256)=64),
    receipt_json TEXT NOT NULL,
    fabric_node_id TEXT NOT NULL,
    authority_id TEXT NOT NULL,
    registry_id TEXT NOT NULL,
    runtime_instance_id TEXT NOT NULL,
    broker_runtime_profile_sha256 TEXT NOT NULL CHECK(length(broker_runtime_profile_sha256)=64),
    broker_target_service_config_sha256 TEXT NOT NULL CHECK(length(broker_target_service_config_sha256)=64),
    admitted_at TEXT NOT NULL
);

CREATE TRIGGER IF NOT EXISTS node_enrollment_receipts_no_update
BEFORE UPDATE ON node_enrollment_receipts
BEGIN SELECT RAISE(ABORT, 'Node enrollment receipts are append-only'); END;
CREATE TRIGGER IF NOT EXISTS node_enrollment_receipts_no_delete
BEFORE DELETE ON node_enrollment_receipts
BEGIN SELECT RAISE(ABORT, 'Node enrollment receipts are append-only'); END;
