from __future__ import annotations

import base64
import csv
import hashlib
import hmac
import io
import json
import os
import re
import secrets
import stat
import subprocess
import tomllib
import uuid
import zipfile
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path, PurePosixPath
from typing import Any

from project_supervisor.domain import EventSeverity
from project_supervisor.store import StateStore, compact_json, timestamp

from .persistence import _event

ARTIFACT_MANIFEST_VERSION = "windows-node-artifact-manifest/v1"
ARTIFACT_REQUEST_VERSION = "windows-node-artifact-request/v1"
ENROLLMENT_BUNDLE_VERSION = "node-enrollment-bundle/v1"
ENROLLMENT_RECEIPT_VERSION = "windows-node-enrollment-receipt/v1"
SERVICE_PROFILE_VERSION = "windows-fabric-node-service-profile/v1"
BUILD_TOOL_VERSION = "windows-node-artifact-builder/v1"
TARGET_PLATFORM = "windows"
TARGET_ARCHITECTURE = "x64"
MIGRATION_FIRST = "0001_initial"
MIGRATION_LAST = "0021_windows_node_enrollment"

_SEMANTIC_ID = re.compile(r"^[A-Za-z][A-Za-z0-9._:-]{0,159}$")
_SAFE_HOST = re.compile(r"^[A-Za-z0-9][A-Za-z0-9.-]{0,252}$")
_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_FINGERPRINT = re.compile(r"^sha256:[0-9a-f]{64}$")
_BEARER = re.compile(r"^[A-Za-z0-9._~-]{32,512}$")
_FIXED_ZIP_TIME = (1980, 1, 1, 0, 0, 0)


def _canonical_bytes(value: Mapping[str, Any] | Sequence[Any]) -> bytes:
    return json.dumps(
        value,
        allow_nan=False,
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")


def _sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        while chunk := stream.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def _parse_time(value: object, field: str) -> datetime:
    if not isinstance(value, str) or len(value) > 64:
        raise ValueError(f"{field} must be a bounded RFC 3339 timestamp")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as error:
        raise ValueError(f"{field} must be an RFC 3339 timestamp") from error
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError(f"{field} must include a timezone")
    return parsed.astimezone(UTC).replace(microsecond=0)


def _time(value: datetime) -> str:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("timestamps must include a timezone")
    return value.astimezone(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _semantic(value: object, field: str) -> str:
    if not isinstance(value, str) or not _SEMANTIC_ID.fullmatch(value):
        raise ValueError(f"{field} must be a bounded semantic ID")
    return value


def _digest(value: object, field: str) -> str:
    if not isinstance(value, str) or not _SHA256.fullmatch(value):
        raise ValueError(f"{field} must be a lowercase SHA-256 digest")
    return value


def _exact(value: Mapping[str, Any], fields: frozenset[str], label: str) -> None:
    unknown = set(value) - fields
    missing = fields - set(value)
    if unknown or missing:
        raise ValueError(
            f"{label} fields do not match its schema; missing={sorted(missing)}, "
            f"unknown={sorted(unknown)}"
        )


def _safe_archive_path(value: str) -> PurePosixPath:
    path = PurePosixPath(value)
    if (
        not value
        or value.startswith(("/", "\\"))
        or "\\" in value
        or path.is_absolute()
        or any(part in {"", ".", ".."} for part in path.parts)
        or ":" in path.parts[0]
    ):
        raise ValueError("artifact member path is unsafe")
    return path


def _zip_info(name: str, *, executable: bool = False) -> zipfile.ZipInfo:
    info = zipfile.ZipInfo(name, _FIXED_ZIP_TIME)
    info.compress_type = zipfile.ZIP_DEFLATED
    info.create_system = 3
    mode = (stat.S_IFREG | (0o755 if executable else 0o644)) << 16
    info.external_attr = mode
    return info


@dataclass(frozen=True, slots=True)
class WindowsFabricNodeServiceProfile:
    definition: Mapping[str, Any]

    def __post_init__(self) -> None:
        value = dict(self.definition)
        if value.get("schemaVersion") != SERVICE_PROFILE_VERSION:
            raise ValueError("unsupported Windows Fabric Node service profile")
        if (
            value.get("targetPlatform") != TARGET_PLATFORM
            or value.get("targetArchitecture") != TARGET_ARCHITECTURE
        ):
            raise ValueError("service profile target is not Windows x64")
        if value.get("listen") != {"host": "127.0.0.1", "port": 7331}:
            raise ValueError("Windows Fabric Node broker must remain on 127.0.0.1:7331")
        if value.get("tailscale", {}).get("funnelAllowed") is not False:
            raise ValueError("Windows Fabric Node profile must prohibit Funnel")
        _semantic(value.get("profileID"), "service profile ID")
        revision = value.get("profileRevision")
        if not isinstance(revision, int) or isinstance(revision, bool) or revision < 1:
            raise ValueError("service profile revision must be positive")

    @property
    def profile_id(self) -> str:
        return str(self.definition["profileID"])

    @property
    def revision(self) -> int:
        return int(self.definition["profileRevision"])

    @property
    def digest(self) -> str:
        return _sha256_bytes(_canonical_bytes(dict(self.definition)))

    def to_protocol(self) -> dict[str, Any]:
        return json.loads(_canonical_bytes(dict(self.definition)))


def default_windows_service_profile() -> WindowsFabricNodeServiceProfile:
    """Return the Mac-issued, request-independent Windows broker service template.

    Values in ``identityBindings`` are the only substitutions an enrollment bootstrapper may make.
    A request can never supply a service name, executable, argv, cwd, environment or account.
    """

    return WindowsFabricNodeServiceProfile(
        {
            "schemaVersion": SERVICE_PROFILE_VERSION,
            "profileID": "chips-windows-fabric-node-broker",
            "profileRevision": 1,
            "deploymentClass": "private-dogfood",
            "targetPlatform": TARGET_PLATFORM,
            "targetArchitecture": TARGET_ARCHITECTURE,
            "installationRoot": r"C:\Program Files\ChipsAgentFabric\Node",
            "stateRoot": r"C:\ProgramData\ChipsAgentFabric\Node",
            "runtime": {
                "layout": "dedicated-venv",
                "python": r"C:\Program Files\ChipsAgentFabric\Node\runtime\Scripts\python.exe",
                "entryPoint": "project_supervisor.local_worker_v2.runtime_broker",
                "packageInstallMode": "wheel-no-editable",
                "minimumPython": "3.12",
            },
            "service": {
                "serviceName": "ChipsFabricNodeBroker",
                "displayName": "CHIPS Fabric Node Runtime Broker",
                "account": r"NT SERVICE\ChipsFabricNodeBroker",
                "startMode": "automaticDelayed",
                "dependencies": ["Tcpip"],
                "errorControl": "normal",
                "recovery": {
                    "firstFailure": "restart",
                    "secondFailure": "restart",
                    "subsequentFailure": "none",
                    "resetPeriodSeconds": 86400,
                    "restartDelaySeconds": 10,
                },
            },
            "listen": {"host": "127.0.0.1", "port": 7331},
            "paths": {
                "brokerState": r"C:\ProgramData\ChipsAgentFabric\Node\broker",
                "registry": r"C:\ProgramData\ChipsAgentFabric\Node\broker\runtime-broker.db",
                "logs": r"C:\ProgramData\ChipsAgentFabric\Node\logs",
                "temporary": r"C:\ProgramData\ChipsAgentFabric\Node\tmp",
            },
            "auth": {
                "type": "windowsCredentialManager",
                "credentialTargetTemplate": "ChipsAgentFabric.RuntimeBroker.{fabricNodeID}",
                "secretInArguments": False,
                "secretInManifest": False,
            },
            "identityBindings": [
                "fabricNodeID",
                "bindingID",
                "brokerAuthorityID",
                "brokerRegistryID",
                "credentialReference",
            ],
            "managedRuntime": {
                "serviceProfileID": "chips-local-worker-v2",
                "targetServiceName": "ChipsFabricLocalWorkerV2",
                "installationState": "requires-pc-enrollment-acceptance",
            },
            "tailscale": {
                "serveBackend": "http://127.0.0.1:7331",
                "existingCorrectRouteAction": "noOp",
                "funnelAllowed": False,
                "resetAllowed": False,
            },
            "legacyRuntime": {
                "path": r"D:\AI\Projects\Project_Supervisor_Worker",
                "policy": "detect-only-never-start-delete-overwrite-or-migrate",
            },
            "credentialBoundary": {
                "brokerUsesInteractiveCodexCredential": False,
                "providerCredentialsCopied": False,
            },
        }
    )


@dataclass(frozen=True, slots=True)
class WindowsNodeArtifactRequest:
    request_id: str
    host_name: str
    tailscale_peer_identity: str
    tailscale_serve_url: str
    machine_binding_sha256: str
    windows_build: str
    requested_at: datetime
    expires_at: datetime
    platform: str = TARGET_PLATFORM
    architecture: str = TARGET_ARCHITECTURE
    requested_artifact_contract: str = "chips-windows-fabric-node-phase2"
    schema_version: str = ARTIFACT_REQUEST_VERSION

    def __post_init__(self) -> None:
        _semantic(self.request_id, "artifact request ID")
        if not _SAFE_HOST.fullmatch(self.host_name):
            raise ValueError("request host name is invalid")
        if not 1 <= len(self.tailscale_peer_identity) <= 512:
            raise ValueError("Tailscale peer identity must be bounded")
        if (
            not self.tailscale_serve_url.startswith("https://")
            or len(self.tailscale_serve_url) > 1024
            or "@" in self.tailscale_serve_url
            or "?" in self.tailscale_serve_url
            or "#" in self.tailscale_serve_url
        ):
            raise ValueError("Tailscale Serve URL must be a credential-free HTTPS origin")
        _digest(self.machine_binding_sha256, "machine binding")
        if self.platform != TARGET_PLATFORM or self.architecture != TARGET_ARCHITECTURE:
            raise ValueError("artifact request target must be Windows x64")
        if self.requested_artifact_contract != "chips-windows-fabric-node-phase2":
            raise ValueError("unsupported Windows artifact contract")
        if self.schema_version != ARTIFACT_REQUEST_VERSION:
            raise ValueError("unsupported Windows artifact request")
        if not self.requested_at < self.expires_at:
            raise ValueError("artifact request expiry must follow its issue time")
        if self.expires_at - self.requested_at > timedelta(days=7):
            raise ValueError("artifact request lifetime cannot exceed seven days")

    @property
    def definition(self) -> dict[str, Any]:
        return {
            "schemaVersion": self.schema_version,
            "requestID": self.request_id,
            "hostName": self.host_name,
            "platform": self.platform,
            "architecture": self.architecture,
            "windowsBuild": self.windows_build,
            "tailscalePeerIdentity": self.tailscale_peer_identity,
            "tailscaleServeURL": self.tailscale_serve_url,
            "machineBindingSHA256": self.machine_binding_sha256,
            "requestedArtifactContract": self.requested_artifact_contract,
            "requestedAt": _time(self.requested_at),
            "expiresAt": _time(self.expires_at),
        }

    @property
    def digest(self) -> str:
        return _sha256_bytes(_canonical_bytes(self.definition))

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> WindowsNodeArtifactRequest:
        fields = frozenset(
            {
                "schemaVersion",
                "requestID",
                "hostName",
                "platform",
                "architecture",
                "windowsBuild",
                "tailscalePeerIdentity",
                "tailscaleServeURL",
                "machineBindingSHA256",
                "requestedArtifactContract",
                "requestedAt",
                "expiresAt",
            }
        )
        _exact(value, fields, "Windows artifact request")
        return cls(
            schema_version=str(value["schemaVersion"]),
            request_id=str(value["requestID"]),
            host_name=str(value["hostName"]),
            platform=str(value["platform"]),
            architecture=str(value["architecture"]),
            windows_build=str(value["windowsBuild"]),
            tailscale_peer_identity=str(value["tailscalePeerIdentity"]),
            tailscale_serve_url=str(value["tailscaleServeURL"]),
            machine_binding_sha256=str(value["machineBindingSHA256"]),
            requested_artifact_contract=str(value["requestedArtifactContract"]),
            requested_at=_parse_time(value["requestedAt"], "requestedAt"),
            expires_at=_parse_time(value["expiresAt"], "expiresAt"),
        )


@dataclass(frozen=True, slots=True)
class EnrollmentIssuerIdentity:
    node_id: str
    key_id: str
    public_key_fingerprint: str

    def __post_init__(self) -> None:
        _semantic(self.node_id, "issuer Node ID")
        _semantic(self.key_id, "issuer key ID")
        if not _FINGERPRINT.fullmatch(self.public_key_fingerprint):
            raise ValueError("issuer public key fingerprint must be sha256:<64 hex>")

    def to_protocol(self) -> dict[str, str]:
        return {
            "nodeID": self.node_id,
            "keyID": self.key_id,
            "publicKeyFingerprint": self.public_key_fingerprint,
        }


@dataclass(frozen=True, slots=True)
class NodeEnrollmentBundle:
    definition: Mapping[str, Any]

    _FIELDS = frozenset(
        {
            "schemaVersion",
            "enrollmentID",
            "generation",
            "issuedAt",
            "expiresAt",
            "requestSHA256",
            "expectedHost",
            "artifact",
            "serviceProfile",
            "controlPlaneIdentity",
            "requestedInitialCapabilities",
            "authorizationScope",
            "credential",
        }
    )

    def __post_init__(self) -> None:
        value = dict(self.definition)
        _exact(value, self._FIELDS, "Node enrollment bundle")
        if value.get("schemaVersion") != ENROLLMENT_BUNDLE_VERSION:
            raise ValueError("unsupported Node enrollment bundle")
        _semantic(value.get("enrollmentID"), "enrollment ID")
        generation = value.get("generation")
        if not isinstance(generation, int) or isinstance(generation, bool) or generation < 1:
            raise ValueError("enrollment generation must be a positive integer")
        _digest(value.get("requestSHA256"), "request digest")
        expected_host = value.get("expectedHost")
        if not isinstance(expected_host, Mapping):
            raise ValueError("enrollment expected host is invalid")
        _exact(
            expected_host,
            frozenset(
                {
                    "hostName",
                    "tailscalePeerIdentity",
                    "tailscaleServeURL",
                    "machineBindingSHA256",
                    "platform",
                    "architecture",
                    "windowsBuild",
                }
            ),
            "enrollment expected host",
        )
        if not _SAFE_HOST.fullmatch(str(expected_host.get("hostName", ""))):
            raise ValueError("enrollment expected host name is invalid")
        peer_identity = expected_host.get("tailscalePeerIdentity")
        if not isinstance(peer_identity, str) or not 1 <= len(peer_identity) <= 512:
            raise ValueError("enrollment Tailscale peer identity must be bounded")
        serve_url = expected_host.get("tailscaleServeURL")
        if (
            not isinstance(serve_url, str)
            or not serve_url.startswith("https://")
            or len(serve_url) > 1024
            or any(marker in serve_url for marker in ("@", "?", "#"))
        ):
            raise ValueError(
                "enrollment Tailscale Serve URL must be a credential-free HTTPS origin"
            )
        _digest(expected_host.get("machineBindingSHA256"), "machine binding")
        windows_build = expected_host.get("windowsBuild")
        if not isinstance(windows_build, str) or not 1 <= len(windows_build) <= 128:
            raise ValueError("enrollment Windows build must be bounded")

        artifact = value.get("artifact")
        if not isinstance(artifact, Mapping):
            raise ValueError("enrollment artifact descriptor is invalid")
        _exact(
            artifact,
            frozenset({"artifactID", "sha256", "manifestSHA256", "sourceSnapshotSHA256"}),
            "enrollment artifact descriptor",
        )
        _semantic(artifact.get("artifactID"), "artifact ID")
        for field in ("sha256", "manifestSHA256", "sourceSnapshotSHA256"):
            _digest(artifact.get(field), f"artifact {field}")

        service_profile = value.get("serviceProfile")
        if not isinstance(service_profile, Mapping):
            raise ValueError("enrollment service profile descriptor is invalid")
        _exact(
            service_profile,
            frozenset({"profileID", "revision", "sha256"}),
            "enrollment service profile descriptor",
        )
        _semantic(service_profile.get("profileID"), "service profile ID")
        revision = service_profile.get("revision")
        if not isinstance(revision, int) or isinstance(revision, bool) or revision < 1:
            raise ValueError("enrollment service profile revision must be positive")
        _digest(service_profile.get("sha256"), "service profile digest")

        control_identity = value.get("controlPlaneIdentity")
        if not isinstance(control_identity, Mapping):
            raise ValueError("enrollment control-plane identity is invalid")
        _exact(
            control_identity,
            frozenset({"nodeID", "keyID", "publicKeyFingerprint"}),
            "enrollment control-plane identity",
        )
        _semantic(control_identity.get("nodeID"), "control-plane Node ID")
        _semantic(control_identity.get("keyID"), "control-plane key ID")
        if not _FINGERPRINT.fullmatch(str(control_identity.get("publicKeyFingerprint", ""))):
            raise ValueError("control-plane public key fingerprint is invalid")

        for field in ("requestedInitialCapabilities", "authorizationScope"):
            items = value.get(field)
            if (
                not isinstance(items, list)
                or not items
                or len(items) > 64
                or len(items) != len(set(items))
                or any(
                    not isinstance(item, str) or not _SEMANTIC_ID.fullmatch(item) for item in items
                )
            ):
                raise ValueError(f"enrollment {field} must contain unique semantic IDs")
        issued = _parse_time(value.get("issuedAt"), "issuedAt")
        expires = _parse_time(value.get("expiresAt"), "expiresAt")
        if not issued < expires or expires - issued > timedelta(hours=24):
            raise ValueError("enrollment bundle lifetime must be positive and at most 24 hours")
        if expected_host.get("platform") != TARGET_PLATFORM:
            raise ValueError("enrollment bundle target must be Windows")
        if expected_host.get("architecture") != TARGET_ARCHITECTURE:
            raise ValueError("enrollment bundle target must be x64")
        credential = value.get("credential")
        if not isinstance(credential, Mapping) or set(credential) != {
            "reference",
            "authModel",
            "rawSecretIncluded",
        }:
            raise ValueError("enrollment credential descriptor is invalid")
        if credential.get("rawSecretIncluded") is not False:
            raise ValueError("enrollment bundle must never contain a raw secret")
        _semantic(credential.get("reference"), "credential reference")
        if credential.get("authModel") != "shared-bearer-hmac/v1":
            raise ValueError("unsupported enrollment credential model")

    @property
    def digest(self) -> str:
        return _sha256_bytes(_canonical_bytes(dict(self.definition)))

    def to_protocol(self) -> dict[str, Any]:
        return json.loads(_canonical_bytes(dict(self.definition)))


@dataclass(frozen=True, slots=True)
class WindowsNodeEnrollmentReceipt:
    definition: Mapping[str, Any]

    _FIELDS = frozenset(
        {
            "schemaVersion",
            "enrollmentID",
            "hostName",
            "fabricNodeID",
            "tailscalePeerIdentity",
            "machineBindingSHA256",
            "platform",
            "architecture",
            "windowsBuild",
            "brokerProtocolVersion",
            "brokerRuntimeVersion",
            "brokerRuntimeProfileSHA256",
            "brokerTargetServiceConfigSHA256",
            "authorityID",
            "registryID",
            "runtimeInstanceID",
            "serviceProfileID",
            "serviceProfileRevision",
            "serviceProfileSHA256",
            "artifactSHA256",
            "manifestSHA256",
            "credentialProvisioned",
            "localAuthenticatedHealth",
            "tailnetAuthenticatedHealth",
            "scmAcceptance",
            "serviceRestartAcceptance",
            "advertisedCapabilities",
            "createdAt",
            "proofHMACSHA256",
        }
    )

    def __post_init__(self) -> None:
        value = dict(self.definition)
        _exact(value, self._FIELDS, "Windows Node enrollment receipt")
        if value.get("schemaVersion") != ENROLLMENT_RECEIPT_VERSION:
            raise ValueError("unsupported Windows Node enrollment receipt")
        for field in (
            "enrollmentID",
            "fabricNodeID",
            "authorityID",
            "registryID",
            "runtimeInstanceID",
            "serviceProfileID",
        ):
            _semantic(value.get(field), field)
        for field in (
            "machineBindingSHA256",
            "serviceProfileSHA256",
            "artifactSHA256",
            "manifestSHA256",
            "brokerRuntimeProfileSHA256",
            "brokerTargetServiceConfigSHA256",
            "proofHMACSHA256",
        ):
            _digest(value.get(field), field)
        if value.get("platform") != TARGET_PLATFORM or value.get("architecture") != (
            TARGET_ARCHITECTURE
        ):
            raise ValueError("receipt target must be Windows x64")
        if not _SAFE_HOST.fullmatch(str(value.get("hostName", ""))):
            raise ValueError("receipt host name is invalid")
        peer_identity = value.get("tailscalePeerIdentity")
        if not isinstance(peer_identity, str) or not 1 <= len(peer_identity) <= 512:
            raise ValueError("receipt Tailscale peer identity must be bounded")
        for field in ("windowsBuild", "brokerProtocolVersion", "brokerRuntimeVersion"):
            item = value.get(field)
            if (
                not isinstance(item, str)
                or not 1 <= len(item) <= 128
                or any(ord(character) < 32 for character in item)
            ):
                raise ValueError(f"receipt {field} must be a bounded printable string")
        if (
            not isinstance(value.get("serviceProfileRevision"), int)
            or isinstance(value.get("serviceProfileRevision"), bool)
            or int(value["serviceProfileRevision"]) < 1
        ):
            raise ValueError("receipt service profile revision must be positive")
        if value.get("credentialProvisioned") is not True:
            raise ValueError("receipt must prove credential provisioning")
        for field in (
            "localAuthenticatedHealth",
            "tailnetAuthenticatedHealth",
            "scmAcceptance",
            "serviceRestartAcceptance",
        ):
            if value.get(field) != "passed":
                raise ValueError(f"receipt {field} must pass before admission")
        capabilities = value.get("advertisedCapabilities")
        if (
            not isinstance(capabilities, list)
            or len(capabilities) > 64
            or any(
                not isinstance(item, str) or not _SEMANTIC_ID.fullmatch(item)
                for item in capabilities
            )
            or len(capabilities) != len(set(capabilities))
        ):
            raise ValueError("receipt capabilities must be unique semantic IDs")
        _parse_time(value.get("createdAt"), "createdAt")

    @property
    def unsigned_definition(self) -> dict[str, Any]:
        return {
            key: value for key, value in dict(self.definition).items() if key != "proofHMACSHA256"
        }

    @property
    def digest(self) -> str:
        return _sha256_bytes(_canonical_bytes(dict(self.definition)))

    def verify_proof(self, secret: str) -> None:
        if not _BEARER.fullmatch(secret):
            raise ValueError("bootstrap credential has an invalid shape")
        expected = hmac.new(
            secret.encode("ascii"),
            _canonical_bytes(self.unsigned_definition),
            hashlib.sha256,
        ).hexdigest()
        if not hmac.compare_digest(expected, str(self.definition["proofHMACSHA256"])):
            raise ValueError("enrollment receipt proof is invalid")


def sign_receipt(definition: Mapping[str, Any], secret: str) -> WindowsNodeEnrollmentReceipt:
    if "proofHMACSHA256" in definition:
        raise ValueError("receipt proof must be generated, not supplied")
    if not _BEARER.fullmatch(secret):
        raise ValueError("bootstrap credential has an invalid shape")
    value = dict(definition)
    value["proofHMACSHA256"] = hmac.new(
        secret.encode("ascii"), _canonical_bytes(value), hashlib.sha256
    ).hexdigest()
    return WindowsNodeEnrollmentReceipt(value)


@dataclass(frozen=True, slots=True)
class ArtifactBuildResult:
    artifact_id: str
    artifact_path: Path
    manifest_path: Path
    service_profile_path: Path
    source_snapshot_sha256: str
    artifact_sha256: str
    manifest_sha256: str
    service_profile_sha256: str
    payload_files: tuple[str, ...]


class WindowsNodeArtifactBuilder:
    """Build a deterministic private Windows application payload from the exact dirty tree."""

    def __init__(self, source_root: Path) -> None:
        self.source_root = Path(source_root).resolve()
        if not (self.source_root / "pyproject.toml").is_file():
            raise ValueError("artifact source root is not a Supervisor checkout")

    def _source_files(self) -> tuple[tuple[str, Path], ...]:
        values: list[tuple[str, Path]] = []
        for prefix in ("src/project_supervisor", "bootstrap"):
            root = self.source_root / prefix
            for path in sorted(root.rglob("*")):
                if not path.is_file() or path.is_symlink():
                    continue
                if "__pycache__" in path.parts or path.suffix not in {".py", ".sql"}:
                    continue
                values.append((path.relative_to(self.source_root).as_posix(), path))
        verifier = self.source_root / "scripts" / "verify_windows_node_artifact.py"
        if not verifier.is_file() or verifier.is_symlink():
            raise ValueError("standalone Windows artifact verifier is unavailable")
        values.append((verifier.relative_to(self.source_root).as_posix(), verifier))
        for name in (
            "windows-node-artifact-request-v1.schema.json",
            "node-enrollment-bundle-v1.schema.json",
            "windows-node-enrollment-receipt-v1.schema.json",
        ):
            schema = self.source_root / "schemas" / name
            if not schema.is_file() or schema.is_symlink():
                raise ValueError(f"Windows enrollment schema is unavailable: {name}")
            values.append((schema.relative_to(self.source_root).as_posix(), schema))
        return tuple(values)

    def _git(self, *arguments: str) -> str:
        result = subprocess.run(
            ("git", *arguments),
            cwd=self.source_root,
            check=True,
            capture_output=True,
            text=True,
            timeout=30,
        )
        return result.stdout.strip()

    def _wheel(self, source_files: tuple[tuple[str, Path], ...]) -> tuple[str, bytes]:
        config = tomllib.loads((self.source_root / "pyproject.toml").read_text(encoding="utf-8"))
        project = config["project"]
        version = str(project["version"])
        wheel_version = version.replace("-", "_")
        distribution = "project_supervisor"
        dist_info = f"{distribution}-{wheel_version}.dist-info"
        payload: dict[str, bytes] = {}
        for relative, path in source_files:
            if relative.startswith("src/"):
                payload[relative.removeprefix("src/")] = path.read_bytes()
            elif relative.startswith("bootstrap/"):
                payload[relative] = path.read_bytes()
        dependencies = "".join(f"Requires-Dist: {item}\n" for item in project["dependencies"])
        payload[f"{dist_info}/METADATA"] = (
            "Metadata-Version: 2.3\n"
            f"Name: {project['name']}\n"
            f"Version: {version}\n"
            f"Summary: {project['description']}\n"
            "Requires-Python: >=3.12\n"
            f"{dependencies}"
        ).encode()
        payload[f"{dist_info}/WHEEL"] = (
            b"Wheel-Version: 1.0\nGenerator: windows-node-artifact-builder/v1\n"
            b"Root-Is-Purelib: true\nTag: py3-none-any\n"
        )
        payload[f"{dist_info}/entry_points.txt"] = (
            b"[console_scripts]\nchips=bootstrap.chips:main\n"
            b"project-supervisor=project_supervisor.cli:main\n"
        )
        rows: list[tuple[str, str, str]] = []
        for name, content in sorted(payload.items()):
            digest = base64.urlsafe_b64encode(hashlib.sha256(content).digest()).rstrip(b"=")
            rows.append((name, f"sha256={digest.decode('ascii')}", str(len(content))))
        record_name = f"{dist_info}/RECORD"
        rows.append((record_name, "", ""))
        record_stream = io.StringIO(newline="")
        csv.writer(record_stream, lineterminator="\n").writerows(rows)
        payload[record_name] = record_stream.getvalue().encode("utf-8")
        archive = io.BytesIO()
        with zipfile.ZipFile(
            archive, "w", compression=zipfile.ZIP_DEFLATED, compresslevel=9
        ) as wheel:
            for name, content in sorted(payload.items()):
                wheel.writestr(_zip_info(name), content, compress_type=zipfile.ZIP_DEFLATED)
        return f"{distribution}-{wheel_version}-py3-none-any.whl", archive.getvalue()

    def build(
        self,
        output_root: Path,
        *,
        profile: WindowsFabricNodeServiceProfile | None = None,
    ) -> ArtifactBuildResult:
        selected_profile = profile or default_windows_service_profile()
        source_files = self._source_files()
        source_entries = [
            {
                "relativePath": relative,
                "size": path.stat().st_size,
                "sha256": _sha256_file(path),
            }
            for relative, path in source_files
        ]
        source_snapshot = _sha256_bytes(_canonical_bytes(source_entries))
        base_head = self._git("rev-parse", "HEAD")
        dirty = bool(self._git("status", "--porcelain", "--untracked-files=all"))
        commit_time = self._git("show", "-s", "--format=%cI", "HEAD")
        build_timestamp = _time(_parse_time(commit_time, "source commit time"))
        artifact_id = (
            "windows-node-"
            + _sha256_bytes(
                (source_snapshot + selected_profile.digest + "windows-x64").encode("ascii")
            )[:24]
        )
        target = Path(output_root).resolve() / artifact_id
        if target.exists() and any(target.iterdir()):
            raise FileExistsError(f"refusing to overwrite non-empty artifact directory: {target}")
        target.mkdir(parents=True, exist_ok=True, mode=0o700)

        wheel_name, wheel_bytes = self._wheel(source_files)
        profile_bytes = _canonical_bytes(selected_profile.to_protocol()) + b"\n"
        verifier_bytes = (self.source_root / "scripts/verify_windows_node_artifact.py").read_bytes()
        requirements = {
            "schemaVersion": "windows-node-runtime-requirements/v1",
            "python": ">=3.12",
            "dependencies": tomllib.loads(
                (self.source_root / "pyproject.toml").read_text(encoding="utf-8")
            )["project"]["dependencies"],
            "installationMode": "dedicated-venv-wheel-no-editable",
            "networkInstallAuthorized": False,
        }
        acceptance = {
            "schemaVersion": "windows-node-acceptance-contract/v1",
            "required": [
                "manifestVerified",
                "targetHostVerified",
                "credentialStoredInWindowsCredentialManager",
                "brokerServiceConfigDigestVerified",
                "brokerLoopbackAuthenticatedHealthPassed",
                "tailnetAuthenticatedHealthPassed",
                "serviceRestartIdentityPassed",
            ],
            "realWindowsRequired": True,
            "macBuildIsNotSCMAcceptance": True,
        }
        rollback = {
            "schemaVersion": "windows-node-rollback-contract/v1",
            "scope": "fabric-owned-paths-and-services-only",
            "legacyRuntimePolicy": "never-modify",
            "tailscalePolicy": "never-reset-or-enable-funnel",
            "pendingEnrollmentMayBeRevokedFromMac": True,
        }
        payload = {
            f"runtime/{wheel_name}": wheel_bytes,
            "config/windows-fabric-node-service-profile.json": profile_bytes,
            "tools/verify_windows_node_artifact.py": verifier_bytes,
            "contracts/runtime-requirements.json": _canonical_bytes(requirements) + b"\n",
            "contracts/windows-node-acceptance.json": _canonical_bytes(acceptance) + b"\n",
            "contracts/rollback.json": _canonical_bytes(rollback) + b"\n",
        }
        for relative, path in source_files:
            if relative.startswith("schemas/"):
                payload[f"contracts/{Path(relative).name}"] = path.read_bytes()
        artifact_path = target / f"{artifact_id}.zip"
        with zipfile.ZipFile(
            artifact_path, "w", compression=zipfile.ZIP_DEFLATED, compresslevel=9
        ) as archive:
            for name, content in sorted(payload.items()):
                archive.writestr(
                    _zip_info(name, executable=name.startswith("tools/")),
                    content,
                    compress_type=zipfile.ZIP_DEFLATED,
                )
        artifact_sha = _sha256_file(artifact_path)
        manifest = {
            "schemaVersion": ARTIFACT_MANIFEST_VERSION,
            "artifactID": artifact_id,
            "artifactVersion": "0.3-dev.phase2b",
            "protocolVersion": 1,
            "deploymentClass": "private-dogfood-not-a-release",
            "targetPlatform": TARGET_PLATFORM,
            "targetArchitecture": TARGET_ARCHITECTURE,
            "sourceBaseHEAD": base_head,
            "dirtySource": dirty,
            "sourceSnapshotSHA256": source_snapshot,
            "sourceFiles": source_entries,
            "artifactSHA256": artifact_sha,
            "buildTimestamp": build_timestamp,
            "buildToolVersion": BUILD_TOOL_VERSION,
            "serviceProfileID": selected_profile.profile_id,
            "serviceProfileRevision": selected_profile.revision,
            "serviceProfileSHA256": selected_profile.digest,
            "migrationRange": {"first": MIGRATION_FIRST, "last": MIGRATION_LAST},
            "entrypoint": {
                "module": "project_supervisor.local_worker_v2.runtime_broker",
                "operation": "fabric.runtime.start",
                "listen": "127.0.0.1:7331",
            },
            "files": [
                {"relativePath": name, "size": len(content), "sha256": _sha256_bytes(content)}
                for name, content in sorted(payload.items())
            ],
        }
        manifest_path = target / f"{artifact_id}.manifest.json"
        manifest_path.write_bytes(_canonical_bytes(manifest) + b"\n")
        service_profile_path = target / "windows-fabric-node-service-profile.json"
        service_profile_path.write_bytes(profile_bytes)
        os.chmod(manifest_path, 0o600)
        os.chmod(service_profile_path, 0o600)
        return ArtifactBuildResult(
            artifact_id=artifact_id,
            artifact_path=artifact_path,
            manifest_path=manifest_path,
            service_profile_path=service_profile_path,
            source_snapshot_sha256=source_snapshot,
            artifact_sha256=artifact_sha,
            manifest_sha256=_sha256_file(manifest_path),
            service_profile_sha256=selected_profile.digest,
            payload_files=tuple(sorted(payload)),
        )


def verify_windows_artifact(artifact_path: Path, manifest_path: Path) -> dict[str, Any]:
    try:
        manifest = json.loads(Path(manifest_path).read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ValueError("artifact manifest is unavailable or malformed") from error
    if not isinstance(manifest, dict) or manifest.get("schemaVersion") != (
        ARTIFACT_MANIFEST_VERSION
    ):
        raise ValueError("unsupported artifact manifest")
    _digest(manifest.get("artifactSHA256"), "artifact digest")
    if _sha256_file(Path(artifact_path)) != manifest["artifactSHA256"]:
        raise ValueError("artifact digest does not match its manifest")
    expected_rows = manifest.get("files")
    if not isinstance(expected_rows, list) or not expected_rows:
        raise ValueError("artifact manifest has no closed file set")
    expected: dict[str, Mapping[str, Any]] = {}
    for row in expected_rows:
        if not isinstance(row, Mapping) or set(row) != {"relativePath", "size", "sha256"}:
            raise ValueError("artifact manifest file row is invalid")
        name = str(row["relativePath"])
        _safe_archive_path(name)
        if name in expected:
            raise ValueError("artifact manifest repeats a file")
        _digest(row["sha256"], "artifact member digest")
        if not isinstance(row["size"], int) or isinstance(row["size"], bool) or row["size"] < 0:
            raise ValueError("artifact member size is invalid")
        expected[name] = row
    with zipfile.ZipFile(artifact_path) as archive:
        infos = archive.infolist()
        names = [info.filename for info in infos]
        if len(names) != len(set(names)) or set(names) != set(expected):
            raise ValueError("artifact closed file set does not match its manifest")
        for info in infos:
            _safe_archive_path(info.filename)
            mode = info.external_attr >> 16
            if stat.S_ISLNK(mode) or info.is_dir():
                raise ValueError("artifact may contain only regular files")
            content = archive.read(info)
            row = expected[info.filename]
            if len(content) != row["size"] or _sha256_bytes(content) != row["sha256"]:
                raise ValueError("artifact member does not match its manifest")
        wheel_names = [name for name in names if name.endswith(".whl")]
        if len(wheel_names) != 1:
            raise ValueError("artifact must contain exactly one runtime wheel")
        wheel_bytes = archive.read(wheel_names[0])
    with zipfile.ZipFile(io.BytesIO(wheel_bytes)) as wheel:
        migrations = sorted(
            PurePosixPath(name).stem
            for name in wheel.namelist()
            if name.startswith("project_supervisor/migrations/") and name.endswith(".sql")
        )
    if not migrations or migrations[0] != MIGRATION_FIRST or migrations[-1] != MIGRATION_LAST:
        raise ValueError("artifact migration chain does not span its declared range")
    prefixes = [int(value.split("_", 1)[0]) for value in migrations]
    if prefixes != list(range(1, 22)):
        raise ValueError("artifact migration chain is not continuous through 0021")
    return manifest


class NodeEnrollmentRepository:
    def __init__(self, store: StateStore) -> None:
        self.store = store

    @staticmethod
    def _credential_verifier(secret: str, salt: str) -> str:
        if not _BEARER.fullmatch(secret):
            raise ValueError("bootstrap credential has an invalid shape")
        return hashlib.sha256(bytes.fromhex(salt) + secret.encode("ascii")).hexdigest()

    def _require_issuer(self, connection: Any, identity: EnrollmentIssuerIdentity) -> None:
        row = connection.execute(
            "SELECT public_key_fingerprint FROM node_public_identities "
            "WHERE node_id=? AND key_id=?",
            (identity.node_id, identity.key_id),
        ).fetchone()
        if row is None or row["public_key_fingerprint"] != identity.public_key_fingerprint:
            raise ValueError("enrollment issuer is not a canonical control-plane identity")

    def active_for_request(
        self, request_sha256: str, *, now: datetime | None = None
    ) -> dict[str, Any] | None:
        _digest(request_sha256, "request digest")
        observed = timestamp(now or datetime.now(UTC))
        with self.store.connect() as connection:
            row = connection.execute(
                "SELECT * FROM pending_node_enrollments WHERE request_sha256=? "
                "AND state='pending' AND expires_at>? ORDER BY generation DESC LIMIT 1",
                (request_sha256, observed),
            ).fetchone()
            return dict(row) if row is not None else None

    def issue(
        self,
        *,
        request: WindowsNodeArtifactRequest,
        artifact_manifest: Mapping[str, Any],
        manifest_sha256: str,
        profile: WindowsFabricNodeServiceProfile,
        issuer: EnrollmentIssuerIdentity,
        bootstrap_secret: str,
        credential_reference: str,
        ttl: timedelta = timedelta(hours=1),
        now: datetime | None = None,
    ) -> NodeEnrollmentBundle:
        if ttl <= timedelta(0) or ttl > timedelta(hours=24):
            raise ValueError("enrollment TTL must be positive and at most 24 hours")
        _semantic(credential_reference, "credential reference")
        _digest(manifest_sha256, "manifest digest")
        if artifact_manifest.get("schemaVersion") != ARTIFACT_MANIFEST_VERSION:
            raise ValueError("unsupported artifact manifest")
        if artifact_manifest.get("serviceProfileSHA256") != profile.digest:
            raise ValueError("artifact and service profile identities differ")
        if (
            artifact_manifest.get("targetPlatform") != request.platform
            or artifact_manifest.get("targetArchitecture") != request.architecture
        ):
            raise ValueError("artifact target does not match the request")
        current = (now or datetime.now(UTC)).astimezone(UTC).replace(microsecond=0)
        if current >= request.expires_at:
            raise ValueError("Windows artifact request has expired")
        expires = min(current + ttl, request.expires_at)
        with self.store.transaction() as connection:
            self._require_issuer(connection, issuer)
            connection.execute(
                "UPDATE pending_node_enrollments SET state='expired',updated_at=? "
                "WHERE request_sha256=? AND state='pending' AND expires_at<=?",
                (timestamp(current), request.digest, timestamp(current)),
            )
            existing = connection.execute(
                "SELECT * FROM pending_node_enrollments WHERE request_sha256=? "
                "AND state='pending' AND expires_at>? ORDER BY generation DESC LIMIT 1",
                (request.digest, timestamp(current)),
            ).fetchone()
            if existing is not None:
                credential_verifier = self._credential_verifier(
                    bootstrap_secret, existing["credential_salt"]
                )
                if not hmac.compare_digest(
                    existing["credential_verifier_sha256"], credential_verifier
                ):
                    raise RuntimeError(
                        "a valid pending enrollment already exists; reuse its secure credential"
                    )
                return NodeEnrollmentBundle(json.loads(existing["bundle_json"]))
            salt = secrets.token_hex(16)
            credential_verifier = self._credential_verifier(bootstrap_secret, salt)
            generation_row = connection.execute(
                "SELECT COALESCE(MAX(generation),0)+1 AS generation "
                "FROM pending_node_enrollments WHERE request_sha256=?",
                (request.digest,),
            ).fetchone()
            generation = int(generation_row["generation"])
            enrollment_id = f"node-enrollment-{uuid.uuid4()}"
            requested_capabilities = ["node-runtime", "local-worker", "codex-present"]
            scope = ["fabric.runtime.health", "fabric.runtime.start", "node.enrollment.receipt"]
            bundle_value = {
                "schemaVersion": ENROLLMENT_BUNDLE_VERSION,
                "enrollmentID": enrollment_id,
                "generation": generation,
                "issuedAt": _time(current),
                "expiresAt": _time(expires),
                "requestSHA256": request.digest,
                "expectedHost": {
                    "hostName": request.host_name,
                    "tailscalePeerIdentity": request.tailscale_peer_identity,
                    "tailscaleServeURL": request.tailscale_serve_url,
                    "machineBindingSHA256": request.machine_binding_sha256,
                    "platform": request.platform,
                    "architecture": request.architecture,
                    "windowsBuild": request.windows_build,
                },
                "artifact": {
                    "artifactID": artifact_manifest["artifactID"],
                    "sha256": artifact_manifest["artifactSHA256"],
                    "manifestSHA256": manifest_sha256,
                    "sourceSnapshotSHA256": artifact_manifest["sourceSnapshotSHA256"],
                },
                "serviceProfile": {
                    "profileID": profile.profile_id,
                    "revision": profile.revision,
                    "sha256": profile.digest,
                },
                "controlPlaneIdentity": issuer.to_protocol(),
                "requestedInitialCapabilities": requested_capabilities,
                "authorizationScope": scope,
                "credential": {
                    "reference": credential_reference,
                    "authModel": "shared-bearer-hmac/v1",
                    "rawSecretIncluded": False,
                },
            }
            bundle = NodeEnrollmentBundle(bundle_value)
            now_text = timestamp(current)
            connection.execute(
                "INSERT INTO pending_node_enrollments(id,generation,state,request_id,"
                "request_sha256,expected_hostname,expected_machine_binding_sha256,"
                "expected_peer_identity_sha256,expected_platform,expected_architecture,"
                "artifact_id,artifact_sha256,manifest_sha256,service_profile_id,"
                "service_profile_revision,service_profile_sha256,issuer_node_id,issuer_key_id,"
                "issuer_public_key_fingerprint,authorization_scope_json,"
                "requested_capabilities_json,credential_reference,credential_salt,"
                "credential_verifier_sha256,bundle_json,bundle_sha256,issued_at,expires_at,"
                "consumed_at,revoked_at,admitted_node_id,created_at,updated_at) "
                "VALUES (?,?, 'pending',?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,"
                "NULL,NULL,NULL,?,?)",
                (
                    enrollment_id,
                    generation,
                    request.request_id,
                    request.digest,
                    request.host_name,
                    request.machine_binding_sha256,
                    hashlib.sha256(request.tailscale_peer_identity.encode("utf-8")).hexdigest(),
                    request.platform,
                    request.architecture,
                    artifact_manifest["artifactID"],
                    artifact_manifest["artifactSHA256"],
                    manifest_sha256,
                    profile.profile_id,
                    profile.revision,
                    profile.digest,
                    issuer.node_id,
                    issuer.key_id,
                    issuer.public_key_fingerprint,
                    compact_json(scope),
                    compact_json(requested_capabilities),
                    credential_reference,
                    salt,
                    credential_verifier,
                    compact_json(bundle.to_protocol()),
                    bundle.digest,
                    now_text,
                    timestamp(expires),
                    now_text,
                    now_text,
                ),
            )
            _event(
                self.store,
                connection,
                kind="nodeEnrollmentIssued",
                entity_type="nodeEnrollment",
                entity_id=enrollment_id,
                summary="Host-bound Windows Node enrollment issued",
                payload={
                    "enrollmentID": enrollment_id,
                    "artifactID": artifact_manifest["artifactID"],
                    "serviceProfileID": profile.profile_id,
                    "generation": generation,
                    "expiresAt": timestamp(expires),
                },
                actor=issuer.node_id,
            )
            return bundle

    def get(self, enrollment_id: str) -> dict[str, Any]:
        _semantic(enrollment_id, "enrollment ID")
        with self.store.connect() as connection:
            row = connection.execute(
                "SELECT * FROM pending_node_enrollments WHERE id=?", (enrollment_id,)
            ).fetchone()
            if row is None:
                raise KeyError(enrollment_id)
            return dict(row)

    def admitted_runtime_configuration(self, enrollment_id: str) -> dict[str, Any]:
        """Return only pinned non-secret broker configuration for autonomous-host wiring."""

        _semantic(enrollment_id, "enrollment ID")
        with self.store.connect() as connection:
            row = connection.execute(
                "SELECT enrollment.*,receipt.authority_id,receipt.registry_id,"
                "receipt.runtime_instance_id,receipt.broker_runtime_profile_sha256,"
                "receipt.broker_target_service_config_sha256,node.private_endpoint,"
                "binding.id AS binding_id,"
                "binding.endpoint_ref,binding.peer_identity_sha256,binding.enabled "
                "FROM pending_node_enrollments enrollment "
                "LEFT JOIN node_enrollment_receipts receipt ON receipt.enrollment_id=enrollment.id "
                "LEFT JOIN nodes node ON node.id=enrollment.admitted_node_id "
                "LEFT JOIN node_transport_bindings binding "
                "ON binding.node_id=enrollment.admitted_node_id "
                "AND binding.transport_provider='tailscale' "
                "WHERE enrollment.id=?",
                (enrollment_id,),
            ).fetchone()
            if row is None:
                raise KeyError(enrollment_id)
            if row["state"] != "admitted":
                raise ValueError("execution-plane enrollment is not admitted")
            if (
                row["authority_id"] is None
                or row["registry_id"] is None
                or row["private_endpoint"] is None
                or row["binding_id"] is None
                or int(row["enabled"]) != 1
            ):
                raise RuntimeError("admitted execution-plane enrollment is incomplete")
            return {
                "enrollment_id": row["id"],
                "node_id": row["admitted_node_id"],
                "binding_id": row["binding_id"],
                "endpoint_ref": row["endpoint_ref"],
                "broker_endpoint": row["private_endpoint"],
                "broker_authority_id": row["authority_id"],
                "broker_registry_id": row["registry_id"],
                "broker_runtime_profile_sha256": row["broker_runtime_profile_sha256"],
                "broker_target_service_config_sha256": row["broker_target_service_config_sha256"],
                "service_profile_revision": int(row["service_profile_revision"]),
                "service_profile_sha256": row["service_profile_sha256"],
                "credential_reference": row["credential_reference"],
            }

    def revoke(self, enrollment_id: str, *, actor: str) -> dict[str, Any]:
        _semantic(actor, "revocation actor")
        with self.store.transaction() as connection:
            row = connection.execute(
                "SELECT * FROM pending_node_enrollments WHERE id=?", (enrollment_id,)
            ).fetchone()
            if row is None:
                raise KeyError(enrollment_id)
            if row["state"] != "pending":
                raise ValueError("only a pending enrollment can be revoked")
            now = timestamp()
            connection.execute(
                "UPDATE pending_node_enrollments SET state='revoked',revoked_at=?,updated_at=? "
                "WHERE id=? AND state='pending'",
                (now, now, enrollment_id),
            )
            _event(
                self.store,
                connection,
                kind="nodeEnrollmentRevoked",
                entity_type="nodeEnrollment",
                entity_id=enrollment_id,
                summary="Pending Windows Node enrollment revoked",
                payload={"enrollmentID": enrollment_id},
                actor=actor,
                severity=EventSeverity.WARNING,
            )
            return dict(
                connection.execute(
                    "SELECT * FROM pending_node_enrollments WHERE id=?", (enrollment_id,)
                ).fetchone()
            )

    def admit(
        self,
        *,
        receipt: WindowsNodeEnrollmentReceipt,
        bootstrap_secret: str,
        actor: str,
        now: datetime | None = None,
    ) -> dict[str, Any]:
        _semantic(actor, "admission actor")
        current = (now or datetime.now(UTC)).astimezone(UTC).replace(microsecond=0)
        enrollment_id = str(receipt.definition["enrollmentID"])
        preflight = self.get(enrollment_id)
        if preflight["state"] == "pending" and timestamp(current) >= preflight["expires_at"]:
            with self.store.transaction() as connection:
                connection.execute(
                    "UPDATE pending_node_enrollments SET state='expired',updated_at=? "
                    "WHERE id=? AND state='pending' AND expires_at<=?",
                    (timestamp(current), enrollment_id, timestamp(current)),
                )
            raise ValueError("enrollment has expired")
        with self.store.transaction() as connection:
            row = connection.execute(
                "SELECT * FROM pending_node_enrollments WHERE id=?", (enrollment_id,)
            ).fetchone()
            if row is None:
                raise KeyError(enrollment_id)
            existing_receipt = connection.execute(
                "SELECT * FROM node_enrollment_receipts WHERE enrollment_id=?",
                (enrollment_id,),
            ).fetchone()
            if existing_receipt is not None:
                if existing_receipt["receipt_sha256"] != receipt.digest:
                    raise RuntimeError("enrollment receipt replay conflicts with admission history")
                return dict(row)
            if row["state"] != "pending":
                raise ValueError("enrollment is not pending")
            if timestamp(current) >= row["expires_at"]:
                raise ValueError("enrollment has expired")
            verifier = self._credential_verifier(bootstrap_secret, row["credential_salt"])
            if not hmac.compare_digest(verifier, row["credential_verifier_sha256"]):
                raise ValueError("bootstrap credential does not match the pending enrollment")
            receipt.verify_proof(bootstrap_secret)
            value = dict(receipt.definition)
            exact_pairs = {
                "hostName": row["expected_hostname"],
                "machineBindingSHA256": row["expected_machine_binding_sha256"],
                "platform": row["expected_platform"],
                "architecture": row["expected_architecture"],
                "serviceProfileID": row["service_profile_id"],
                "serviceProfileRevision": int(row["service_profile_revision"]),
                "serviceProfileSHA256": row["service_profile_sha256"],
                "artifactSHA256": row["artifact_sha256"],
                "manifestSHA256": row["manifest_sha256"],
            }
            if any(value.get(key) != expected for key, expected in exact_pairs.items()):
                raise ValueError("enrollment receipt identity does not match its pending record")
            peer_sha = hashlib.sha256(
                str(value["tailscalePeerIdentity"]).encode("utf-8")
            ).hexdigest()
            if peer_sha != row["expected_peer_identity_sha256"]:
                raise ValueError("enrollment receipt Tailscale peer does not match")
            created_at = _parse_time(value["createdAt"], "createdAt")
            if (
                created_at > current + timedelta(minutes=5)
                or timestamp(created_at) > row["expires_at"]
            ):
                raise ValueError("enrollment receipt time is outside its authorization window")
            node_id = str(value["fabricNodeID"])
            node = connection.execute("SELECT * FROM nodes WHERE id=?", (node_id,)).fetchone()
            if node is not None:
                raise RuntimeError("receipt Node identity is already registered")
            now_text = timestamp(current)
            capabilities = sorted(set(value["advertisedCapabilities"]))
            bundle = json.loads(row["bundle_json"])
            endpoint = bundle["expectedHost"]["tailscaleServeURL"]
            connection.execute(
                "INSERT INTO nodes(id,hostname,display_name,role,state,operating_system,"
                "hardware_summary,private_endpoint,capabilities_json,last_heartbeat_at,"
                "created_at,updated_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    node_id,
                    row["expected_hostname"],
                    row["expected_hostname"],
                    "fabric-execution-node",
                    "online",
                    "Windows",
                    str(value["windowsBuild"]),
                    endpoint,
                    compact_json(capabilities),
                    now_text,
                    now_text,
                    now_text,
                ),
            )
            binding_id = (
                "node-binding-"
                + hashlib.sha256(f"tailscale:{peer_sha}:{node_id}".encode("ascii")).hexdigest()[:24]
            )
            connection.execute(
                "INSERT INTO node_transport_bindings(id,node_id,transport_provider,"
                "peer_identity_sha256,service_name,endpoint_ref,expected_platform,enabled,"
                "generation,configured_by,created_at,updated_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    binding_id,
                    node_id,
                    "tailscale",
                    peer_sha,
                    row["service_profile_id"],
                    f"enrollment:{enrollment_id}",
                    "windows",
                    1,
                    1,
                    actor,
                    now_text,
                    now_text,
                ),
            )
            connection.execute(
                "INSERT INTO node_enrollment_receipts(enrollment_id,receipt_sha256,receipt_json,"
                "fabric_node_id,authority_id,registry_id,runtime_instance_id,"
                "broker_runtime_profile_sha256,broker_target_service_config_sha256,"
                "admitted_at) VALUES (?,?,?,?,?,?,?,?,?,?)",
                (
                    enrollment_id,
                    receipt.digest,
                    compact_json(value),
                    node_id,
                    value["authorityID"],
                    value["registryID"],
                    value["runtimeInstanceID"],
                    value["brokerRuntimeProfileSHA256"],
                    value["brokerTargetServiceConfigSHA256"],
                    now_text,
                ),
            )
            connection.execute(
                "UPDATE pending_node_enrollments SET state='admitted',consumed_at=?,"
                "admitted_node_id=?,updated_at=? WHERE id=? AND state='pending'",
                (now_text, node_id, now_text, enrollment_id),
            )
            _event(
                self.store,
                connection,
                kind="nodeEnrollmentAdmitted",
                entity_type="nodeEnrollment",
                entity_id=enrollment_id,
                summary="Windows Fabric Node enrollment admitted and pinned",
                payload={
                    "enrollmentID": enrollment_id,
                    "nodeID": node_id,
                    "bindingID": binding_id,
                    "serviceProfileID": row["service_profile_id"],
                },
                actor=actor,
            )
            return dict(
                connection.execute(
                    "SELECT * FROM pending_node_enrollments WHERE id=?", (enrollment_id,)
                ).fetchone()
            )


def generate_broker_credential() -> str:
    return secrets.token_urlsafe(48)


def write_bootstrap_secret_sidecar(
    path: Path, *, enrollment_id: str, bundle_sha256: str, secret: str
) -> None:
    _semantic(enrollment_id, "enrollment ID")
    _digest(bundle_sha256, "bundle digest")
    if not _BEARER.fullmatch(secret):
        raise ValueError("bootstrap credential has an invalid shape")
    target = Path(path)
    if target.exists() or target.is_symlink():
        raise FileExistsError(f"refusing to overwrite bootstrap secret sidecar: {target}")
    target.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    descriptor = {
        "schemaVersion": "node-enrollment-secret-sidecar/v1",
        "enrollmentID": enrollment_id,
        "bundleSHA256": bundle_sha256,
        "bootstrapCredential": secret,
        "singlePurpose": True,
        "deleteAfterCredentialProvisioning": True,
    }
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
    descriptor_fd = os.open(target, flags, 0o600)
    try:
        os.write(descriptor_fd, _canonical_bytes(descriptor) + b"\n")
        os.fsync(descriptor_fd)
    finally:
        os.close(descriptor_fd)


def store_windows_credential(target: str, secret: str) -> None:
    """Write the broker bearer to Windows Credential Manager without argv or file exposure."""

    import platform

    _semantic(target, "Windows credential target")
    if not _BEARER.fullmatch(secret):
        raise ValueError("runtime broker credential has an invalid shape")
    if platform.system() != "Windows":
        raise ValueError("Windows Credential Manager is available only on Windows")
    import ctypes
    from ctypes import wintypes

    class Credential(ctypes.Structure):
        _fields_ = [
            ("flags", wintypes.DWORD),
            ("type", wintypes.DWORD),
            ("target_name", wintypes.LPWSTR),
            ("comment", wintypes.LPWSTR),
            ("last_written", wintypes.FILETIME),
            ("blob_size", wintypes.DWORD),
            ("blob", ctypes.POINTER(ctypes.c_ubyte)),
            ("persist", wintypes.DWORD),
            ("attribute_count", wintypes.DWORD),
            ("attributes", ctypes.c_void_p),
            ("target_alias", wintypes.LPWSTR),
            ("user_name", wintypes.LPWSTR),
        ]

    raw = secret.encode("ascii")
    buffer = (ctypes.c_ubyte * len(raw)).from_buffer_copy(raw)
    credential = Credential()
    credential.type = 1  # CRED_TYPE_GENERIC
    credential.target_name = target
    credential.blob_size = len(raw)
    credential.blob = ctypes.cast(buffer, ctypes.POINTER(ctypes.c_ubyte))
    credential.persist = 2  # CRED_PERSIST_LOCAL_MACHINE; DPAPI-protected by Windows
    credential.user_name = "ChipsFabricNodeBroker"
    api = ctypes.WinDLL("advapi32", use_last_error=True)
    api.CredWriteW.argtypes = [ctypes.POINTER(Credential), wintypes.DWORD]
    api.CredWriteW.restype = wintypes.BOOL
    if not api.CredWriteW(ctypes.byref(credential), 0):
        raise OSError("Windows Credential Manager rejected the Fabric broker credential")
