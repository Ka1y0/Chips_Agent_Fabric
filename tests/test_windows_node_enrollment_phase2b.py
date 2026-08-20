from __future__ import annotations

import hashlib
import json
import zipfile
from argparse import Namespace
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from jsonschema.validators import Draft202012Validator

from project_supervisor.domain import NodeState
from project_supervisor.fabric.node_enrollment import (
    ARTIFACT_REQUEST_VERSION,
    ENROLLMENT_RECEIPT_VERSION,
    ArtifactBuildResult,
    EnrollmentIssuerIdentity,
    NodeEnrollmentRepository,
    WindowsNodeArtifactBuilder,
    WindowsNodeArtifactRequest,
    default_windows_service_profile,
    sign_receipt,
    verify_windows_artifact,
    write_bootstrap_secret_sidecar,
)
from project_supervisor.protocols.identity import NodePublicIdentity
from project_supervisor.store import StateStore
from scripts import manage_windows_node_enrollment

ROOT = Path(__file__).parents[1]
SECRET = "phase2b-fixture-broker-credential-" + "0123456789abcdef"
PEER = "nodekey:fixture-fabric-windows-node-tailnet-identity"
MACHINE = "a" * 64


@pytest.fixture(scope="module")
def artifact(tmp_path_factory: pytest.TempPathFactory) -> ArtifactBuildResult:
    return WindowsNodeArtifactBuilder(ROOT).build(tmp_path_factory.mktemp("windows-artifact"))


def _request(now: datetime) -> WindowsNodeArtifactRequest:
    return WindowsNodeArtifactRequest.from_mapping(
        {
            "schemaVersion": ARTIFACT_REQUEST_VERSION,
            "requestID": "request-fabric-windows-node-phase2b",
            "hostName": "FABRIC-WINDOWS-NODE",
            "platform": "windows",
            "architecture": "x64",
            "windowsBuild": "10.0.26200",
            "tailscalePeerIdentity": PEER,
            "tailscaleServeURL": "https://fabric-windows-node.example-tailnet.ts.net/",
            "machineBindingSHA256": MACHINE,
            "requestedArtifactContract": "chips-windows-fabric-node-phase2",
            "requestedAt": now.isoformat().replace("+00:00", "Z"),
            "expiresAt": (now + timedelta(hours=2)).isoformat().replace("+00:00", "Z"),
        }
    )


def _store(tmp_path: Path) -> tuple[StateStore, EnrollmentIssuerIdentity]:
    store = StateStore(tmp_path / "supervisor.db")
    store.upsert_node(
        node_id="node-mac-control-plane",
        hostname="mac-control-plane",
        display_name="Mac enrollment authority",
        role="control-plane",
        state=NodeState.ONLINE,
    )
    identity = NodePublicIdentity(
        node_id="node-mac-control-plane",
        key_id="mac-enrollment-key-v1",
        algorithm="ed25519",
        public_key_fingerprint="sha256:" + "b" * 64,
    )
    store.record_node_public_identity(identity)
    return store, EnrollmentIssuerIdentity(
        node_id=identity.node_id,
        key_id=identity.key_id,
        public_key_fingerprint=identity.public_key_fingerprint,
    )


def _manifest(artifact: ArtifactBuildResult) -> dict[str, object]:
    return json.loads(artifact.manifest_path.read_text(encoding="utf-8"))


def _validate_schema(name: str, value: dict[str, object]) -> None:
    schema = json.loads((ROOT / "schemas" / name).read_text(encoding="utf-8"))
    Draft202012Validator(schema).validate(value)


def _receipt_definition(
    enrollment_id: str,
    artifact: ArtifactBuildResult,
    *,
    now: datetime,
    host: str = "FABRIC-WINDOWS-NODE",
    peer: str = PEER,
) -> dict[str, object]:
    return {
        "schemaVersion": ENROLLMENT_RECEIPT_VERSION,
        "enrollmentID": enrollment_id,
        "hostName": host,
        "fabricNodeID": "node-fabric-windows-node-phase2",
        "tailscalePeerIdentity": peer,
        "machineBindingSHA256": MACHINE,
        "platform": "windows",
        "architecture": "x64",
        "windowsBuild": "10.0.26200",
        "brokerProtocolVersion": "fabric-runtime-broker-profile/v1",
        "brokerRuntimeVersion": "0.3-dev.phase2b",
        "brokerRuntimeProfileSHA256": "d" * 64,
        "brokerTargetServiceConfigSHA256": "e" * 64,
        "authorityID": "broker-authority-fabric-windows-node",
        "registryID": "broker-registry-fabric-windows-node",
        "runtimeInstanceID": "broker-runtime-fabric-windows-node-1",
        "serviceProfileID": default_windows_service_profile().profile_id,
        "serviceProfileRevision": default_windows_service_profile().revision,
        "serviceProfileSHA256": artifact.service_profile_sha256,
        "artifactSHA256": artifact.artifact_sha256,
        "manifestSHA256": artifact.manifest_sha256,
        "credentialProvisioned": True,
        "localAuthenticatedHealth": "passed",
        "tailnetAuthenticatedHealth": "passed",
        "scmAcceptance": "passed",
        "serviceRestartAcceptance": "passed",
        "advertisedCapabilities": ["node-runtime"],
        "createdAt": now.isoformat().replace("+00:00", "Z"),
    }


def test_optional_service_profile_is_deterministic_loopback_and_never_uses_user_codex() -> None:
    first = default_windows_service_profile()
    second = default_windows_service_profile()

    assert first.digest == second.digest
    assert first.to_protocol()["listen"] == {"host": "127.0.0.1", "port": 7331}
    assert first.to_protocol()["tailscale"]["funnelAllowed"] is False
    assert first.to_protocol()["credentialBoundary"] == {
        "brokerUsesInteractiveCodexCredential": False,
        "providerCredentialsCopied": False,
    }


def test_request_bundle_and_receipt_schemas_validate_typed_contracts(
    artifact: ArtifactBuildResult, tmp_path: Path
) -> None:
    now = datetime.now(UTC).replace(microsecond=0)
    request = _request(now)
    _validate_schema("windows-node-artifact-request-v1.schema.json", request.definition)
    store, issuer = _store(tmp_path)
    bundle = NodeEnrollmentRepository(store).issue(
        request=request,
        artifact_manifest=_manifest(artifact),
        manifest_sha256=artifact.manifest_sha256,
        profile=default_windows_service_profile(),
        issuer=issuer,
        bootstrap_secret=SECRET,
        credential_reference="ChipsAgentFabric.RuntimeBroker.node-fabric-windows-node-phase2",
        now=now,
    )
    _validate_schema("node-enrollment-bundle-v1.schema.json", bundle.to_protocol())
    receipt = sign_receipt(
        _receipt_definition(str(bundle.definition["enrollmentID"]), artifact, now=now), SECRET
    )
    _validate_schema("windows-node-enrollment-receipt-v1.schema.json", dict(receipt.definition))


def test_artifact_build_is_deterministic_closed_and_contains_migrations_through_0020(
    tmp_path: Path,
) -> None:
    first = WindowsNodeArtifactBuilder(ROOT).build(tmp_path / "one")
    second = WindowsNodeArtifactBuilder(ROOT).build(tmp_path / "two")

    assert first.artifact_id == second.artifact_id
    assert first.artifact_path.read_bytes() == second.artifact_path.read_bytes()
    assert first.manifest_path.read_bytes() == second.manifest_path.read_bytes()
    manifest = verify_windows_artifact(first.artifact_path, first.manifest_path)
    assert manifest["dirtySource"] is True
    assert manifest["migrationRange"] == {
        "first": "0001_initial",
        "last": "0021_windows_node_enrollment",
    }
    with zipfile.ZipFile(first.artifact_path) as archive:
        assert set(archive.namelist()) == set(first.payload_files)
        assert not any(
            "CrossFire" in name or name.startswith("artifacts/") for name in archive.namelist()
        )


def test_artifact_rejects_tampering_and_extra_files(
    artifact: ArtifactBuildResult, tmp_path: Path
) -> None:
    tampered = tmp_path / "tampered.zip"
    tampered.write_bytes(artifact.artifact_path.read_bytes())
    with zipfile.ZipFile(tampered, "a") as archive:
        archive.writestr("unexpected.exe", b"fixture")
    manifest = _manifest(artifact)
    manifest["artifactSHA256"] = hashlib.sha256(tampered.read_bytes()).hexdigest()
    tampered_manifest = tmp_path / "tampered.manifest.json"
    tampered_manifest.write_text(json.dumps(manifest), encoding="utf-8")

    with pytest.raises(ValueError, match="closed file set"):
        verify_windows_artifact(tampered, tampered_manifest)


def test_issue_pending_admit_and_exact_receipt_replay(
    artifact: ArtifactBuildResult, tmp_path: Path
) -> None:
    now = datetime.now(UTC).replace(microsecond=0)
    store, issuer = _store(tmp_path)
    repository = NodeEnrollmentRepository(store)
    request = _request(now)
    bundle = repository.issue(
        request=request,
        artifact_manifest=_manifest(artifact),
        manifest_sha256=artifact.manifest_sha256,
        profile=default_windows_service_profile(),
        issuer=issuer,
        bootstrap_secret=SECRET,
        credential_reference="ChipsAgentFabric.RuntimeBroker.node-fabric-windows-node-phase2",
        now=now,
    )
    pending = repository.get(str(bundle.definition["enrollmentID"]))
    assert pending["state"] == "pending"
    assert SECRET not in json.dumps(pending)
    assert all(
        SECRET.encode("ascii") not in path.read_bytes()
        for path in tmp_path.glob("supervisor.db*")
        if path.is_file()
    )
    with pytest.raises(ValueError, match="not admitted"):
        repository.admitted_runtime_configuration(str(bundle.definition["enrollmentID"]))
    replayed_bundle = repository.issue(
        request=request,
        artifact_manifest=_manifest(artifact),
        manifest_sha256=artifact.manifest_sha256,
        profile=default_windows_service_profile(),
        issuer=issuer,
        bootstrap_secret=SECRET,
        credential_reference="ChipsAgentFabric.RuntimeBroker.node-fabric-windows-node-phase2",
        now=now,
    )
    assert replayed_bundle.digest == bundle.digest
    with pytest.raises(RuntimeError, match="valid pending enrollment"):
        repository.issue(
            request=request,
            artifact_manifest=_manifest(artifact),
            manifest_sha256=artifact.manifest_sha256,
            profile=default_windows_service_profile(),
            issuer=issuer,
            bootstrap_secret="different-phase2b-broker-credential-0123456789abcdef",
            credential_reference="ChipsAgentFabric.RuntimeBroker.node-fabric-windows-node-phase2",
            now=now,
        )

    invalid_bundle = bundle.to_protocol()
    invalid_bundle["artifact"]["unexpected"] = "rejected"
    with pytest.raises(ValueError, match="unknown=.*unexpected"):
        type(bundle)(invalid_bundle)

    receipt = sign_receipt(
        _receipt_definition(str(bundle.definition["enrollmentID"]), artifact, now=now), SECRET
    )
    admitted = repository.admit(
        receipt=receipt, bootstrap_secret=SECRET, actor="operator.mac", now=now
    )
    replay = repository.admit(
        receipt=receipt, bootstrap_secret=SECRET, actor="operator.mac", now=now
    )
    conflicting_value = _receipt_definition(
        str(bundle.definition["enrollmentID"]), artifact, now=now
    )
    conflicting_value["runtimeInstanceID"] = "broker-runtime-fabric-windows-node-conflict"
    with pytest.raises(RuntimeError, match="receipt replay conflicts"):
        repository.admit(
            receipt=sign_receipt(conflicting_value, SECRET),
            bootstrap_secret=SECRET,
            actor="operator.mac",
            now=now,
        )

    assert admitted["state"] == replay["state"] == "admitted"
    assert admitted["admitted_node_id"] == "node-fabric-windows-node-phase2"
    runtime_config = repository.admitted_runtime_configuration(
        str(bundle.definition["enrollmentID"])
    )
    assert runtime_config["broker_runtime_profile_sha256"] == "d" * 64
    assert runtime_config["broker_target_service_config_sha256"] == "e" * 64
    assert runtime_config["broker_endpoint"] == "https://fabric-windows-node.example-tailnet.ts.net/"
    with store.connect() as connection:
        assert (
            connection.execute("SELECT COUNT(*) FROM node_enrollment_receipts").fetchone()[0] == 1
        )
        binding = connection.execute(
            "SELECT * FROM node_transport_bindings WHERE node_id='node-fabric-windows-node-phase2'"
        ).fetchone()
        assert binding["peer_identity_sha256"] == hashlib.sha256(PEER.encode()).hexdigest()


@pytest.mark.parametrize(
    ("host", "peer", "error"),
    [
        ("WRONG-PC", PEER, "identity"),
        ("FABRIC-WINDOWS-NODE", "nodekey:different-peer", "Tailscale peer"),
    ],
)
def test_admission_rejects_wrong_host_or_peer(
    artifact: ArtifactBuildResult,
    tmp_path: Path,
    host: str,
    peer: str,
    error: str,
) -> None:
    now = datetime.now(UTC).replace(microsecond=0)
    store, issuer = _store(tmp_path)
    repository = NodeEnrollmentRepository(store)
    bundle = repository.issue(
        request=_request(now),
        artifact_manifest=_manifest(artifact),
        manifest_sha256=artifact.manifest_sha256,
        profile=default_windows_service_profile(),
        issuer=issuer,
        bootstrap_secret=SECRET,
        credential_reference="ChipsAgentFabric.RuntimeBroker.node-fabric-windows-node-phase2",
        now=now,
    )
    receipt = sign_receipt(
        _receipt_definition(
            str(bundle.definition["enrollmentID"]), artifact, now=now, host=host, peer=peer
        ),
        SECRET,
    )

    with pytest.raises(ValueError, match=error):
        repository.admit(receipt=receipt, bootstrap_secret=SECRET, actor="operator.mac", now=now)


def test_wrong_profile_expiry_and_receipt_conflict_fail_closed(
    artifact: ArtifactBuildResult, tmp_path: Path
) -> None:
    now = datetime.now(UTC).replace(microsecond=0)
    store, issuer = _store(tmp_path)
    repository = NodeEnrollmentRepository(store)
    bundle = repository.issue(
        request=_request(now),
        artifact_manifest=_manifest(artifact),
        manifest_sha256=artifact.manifest_sha256,
        profile=default_windows_service_profile(),
        issuer=issuer,
        bootstrap_secret=SECRET,
        credential_reference="ChipsAgentFabric.RuntimeBroker.node-fabric-windows-node-phase2",
        ttl=timedelta(seconds=2),
        now=now,
    )
    enrollment_id = str(bundle.definition["enrollmentID"])
    wrong_profile = _receipt_definition(enrollment_id, artifact, now=now)
    wrong_profile["serviceProfileSHA256"] = "f" * 64
    with pytest.raises(ValueError, match="identity"):
        repository.admit(
            receipt=sign_receipt(wrong_profile, SECRET),
            bootstrap_secret=SECRET,
            actor="operator.mac",
            now=now,
        )
    with pytest.raises(ValueError, match="expired"):
        repository.admit(
            receipt=sign_receipt(_receipt_definition(enrollment_id, artifact, now=now), SECRET),
            bootstrap_secret=SECRET,
            actor="operator.mac",
            now=now + timedelta(seconds=3),
        )
    assert repository.get(enrollment_id)["state"] == "expired"


def test_secret_sidecar_is_exclusive_restrictive_and_not_manifested(tmp_path: Path) -> None:
    target = tmp_path / "secrets" / "bootstrap-secret.json"
    write_bootstrap_secret_sidecar(
        target,
        enrollment_id="node-enrollment-fixture",
        bundle_sha256="c" * 64,
        secret=SECRET,
    )
    assert stat_mode(target) == 0o600
    assert SECRET in target.read_text(encoding="utf-8")
    with pytest.raises(FileExistsError):
        write_bootstrap_secret_sidecar(
            target,
            enrollment_id="node-enrollment-fixture",
            bundle_sha256="c" * 64,
            secret=SECRET,
        )


def test_issue_output_failure_revokes_pending_authorization(
    artifact: ArtifactBuildResult, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    now = datetime.now(UTC).replace(microsecond=0)
    database = tmp_path / "supervisor.db"
    store, issuer = _store(tmp_path)
    assert store.path == database
    request_path = tmp_path / "request.json"
    request_path.write_text(json.dumps(_request(now).definition), encoding="utf-8")

    def fail_sidecar(*_args: object, **_kwargs: object) -> None:
        raise OSError("fixture output failure")

    monkeypatch.setattr(
        manage_windows_node_enrollment,
        "write_bootstrap_secret_sidecar",
        fail_sidecar,
    )
    with pytest.raises(RuntimeError, match="pending authorization was revoked"):
        manage_windows_node_enrollment._issue(
            Namespace(
                database=database,
                request=request_path,
                artifact=artifact.artifact_path,
                manifest=artifact.manifest_path,
                issuer_node_id=issuer.node_id,
                issuer_key_id=issuer.key_id,
                issuer_public_key_fingerprint=issuer.public_key_fingerprint,
                expires_in_hours=1.0,
                output_dir=tmp_path / "ordinary",
                secret_dir=tmp_path / "secret",
            )
        )
    with StateStore(database).connect() as connection:
        row = connection.execute("SELECT state FROM pending_node_enrollments").fetchone()
        assert row["state"] == "revoked"


def stat_mode(path: Path) -> int:
    return path.stat().st_mode & 0o777
