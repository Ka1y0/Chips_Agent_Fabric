from __future__ import annotations

import argparse
import json
import os
import platform
import socket
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from project_supervisor.local_worker_v2.runtime_broker import _load_windows_credential

from .node_enrollment import (
    ARTIFACT_REQUEST_VERSION,
    ENROLLMENT_RECEIPT_VERSION,
    NodeEnrollmentBundle,
    WindowsNodeArtifactRequest,
    sign_receipt,
    store_windows_credential,
)


def _json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ValueError(f"cannot read {path.name}") from error
    if not isinstance(value, dict):
        raise ValueError(f"{path.name} root must be an object")
    return value


def _write_exclusive(path: Path, value: dict[str, Any], *, mode: int = 0o600) -> None:
    if path.exists() or path.is_symlink():
        raise FileExistsError(f"refusing to overwrite {path}")
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, mode)
    try:
        os.write(
            descriptor,
            json.dumps(value, ensure_ascii=True, separators=(",", ":"), sort_keys=True).encode()
            + b"\n",
        )
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _request(args: argparse.Namespace) -> int:
    now = datetime.now(UTC).replace(microsecond=0)
    request = WindowsNodeArtifactRequest(
        schema_version=ARTIFACT_REQUEST_VERSION,
        request_id=args.request_id,
        host_name=args.host_name,
        platform="windows",
        architecture="x64",
        windows_build=args.windows_build,
        tailscale_peer_identity=args.tailscale_peer_identity,
        tailscale_serve_url=args.tailscale_serve_url,
        machine_binding_sha256=args.machine_binding_sha256,
        requested_at=now,
        expires_at=now + timedelta(hours=args.expires_in_hours),
    )
    _write_exclusive(args.output, request.definition)
    print(json.dumps({"status": "requestCreated", "requestID": request.request_id}, sort_keys=True))
    return 0


def _sidecar(path: Path, enrollment_id: str, bundle_sha256: str) -> str:
    value = _json(path)
    if set(value) != {
        "schemaVersion",
        "enrollmentID",
        "bundleSHA256",
        "bootstrapCredential",
        "singlePurpose",
        "deleteAfterCredentialProvisioning",
    }:
        raise ValueError("bootstrap secret sidecar fields are invalid")
    if value.get("schemaVersion") != "node-enrollment-secret-sidecar/v1":
        raise ValueError("unsupported bootstrap secret sidecar")
    if value.get("enrollmentID") != enrollment_id or value.get("bundleSHA256") != bundle_sha256:
        raise ValueError("bootstrap secret sidecar is bound to different enrollment material")
    if value.get("singlePurpose") is not True:
        raise ValueError("bootstrap credential is not marked single-purpose")
    return str(value["bootstrapCredential"])


def _provision_credential(args: argparse.Namespace) -> int:
    if platform.system() != "Windows":
        raise ValueError("credential provisioning must run on Windows")
    bundle = NodeEnrollmentBundle(_json(args.bundle))
    credential = bundle.definition["credential"]
    secret = _sidecar(
        args.secret_sidecar,
        str(bundle.definition["enrollmentID"]),
        bundle.digest,
    )
    store_windows_credential(str(credential["reference"]), secret)
    if args.consume:
        args.secret_sidecar.unlink()
    print(
        json.dumps(
            {
                "status": "credentialProvisioned",
                "enrollmentID": bundle.definition["enrollmentID"],
                "credentialReference": credential["reference"],
                "secretPrinted": False,
                "sidecarConsumed": bool(args.consume),
            },
            sort_keys=True,
        )
    )
    return 0


def _emit_receipt(args: argparse.Namespace) -> int:
    if platform.system() != "Windows":
        raise ValueError("Windows enrollment receipt must be emitted on Windows")
    bundle = NodeEnrollmentBundle(_json(args.bundle))
    expected = bundle.definition["expectedHost"]
    service = bundle.definition["serviceProfile"]
    artifact = bundle.definition["artifact"]
    credential = bundle.definition["credential"]
    secret = _load_windows_credential(str(credential["reference"]))
    value = {
        "schemaVersion": ENROLLMENT_RECEIPT_VERSION,
        "enrollmentID": bundle.definition["enrollmentID"],
        "hostName": socket.gethostname(),
        "fabricNodeID": args.fabric_node_id,
        "tailscalePeerIdentity": args.tailscale_peer_identity,
        "machineBindingSHA256": args.machine_binding_sha256,
        "platform": "windows",
        "architecture": "x64",
        "windowsBuild": platform.version(),
        "brokerProtocolVersion": args.broker_protocol_version,
        "brokerRuntimeVersion": args.broker_runtime_version,
        "brokerRuntimeProfileSHA256": args.broker_runtime_profile_sha256,
        "brokerTargetServiceConfigSHA256": args.broker_target_service_config_sha256,
        "authorityID": args.authority_id,
        "registryID": args.registry_id,
        "runtimeInstanceID": args.runtime_instance_id,
        "serviceProfileID": service["profileID"],
        "serviceProfileRevision": service["revision"],
        "serviceProfileSHA256": service["sha256"],
        "artifactSHA256": artifact["sha256"],
        "manifestSHA256": artifact["manifestSHA256"],
        "credentialProvisioned": True,
        "localAuthenticatedHealth": "passed" if args.local_health_passed else "failed",
        "tailnetAuthenticatedHealth": "passed" if args.tailnet_health_passed else "failed",
        "scmAcceptance": "passed" if args.scm_acceptance_passed else "failed",
        "serviceRestartAcceptance": "passed" if args.restart_acceptance_passed else "failed",
        "advertisedCapabilities": sorted(set(args.capability)),
        "createdAt": datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
    }
    if (
        value["hostName"] != expected["hostName"]
        or value["tailscalePeerIdentity"] != expected["tailscalePeerIdentity"]
        or value["machineBindingSHA256"] != expected["machineBindingSHA256"]
        or value["windowsBuild"] != expected["windowsBuild"]
    ):
        raise ValueError("local Windows identity does not match the enrollment bundle")
    receipt = sign_receipt(value, secret)
    _write_exclusive(args.output, dict(receipt.definition))
    print(
        json.dumps(
            {
                "status": "receiptCreated",
                "enrollmentID": bundle.definition["enrollmentID"],
                "receiptPath": str(args.output),
                "secretPrinted": False,
            },
            sort_keys=True,
        )
    )
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="bounded Windows Fabric Node enrollment helper")
    commands = parser.add_subparsers(dest="command", required=True)
    request = commands.add_parser("request", help="create a machine-bound artifact request")
    request.add_argument("--request-id", required=True)
    request.add_argument("--host-name", required=True)
    request.add_argument("--windows-build", required=True)
    request.add_argument("--tailscale-peer-identity", required=True)
    request.add_argument("--tailscale-serve-url", required=True)
    request.add_argument("--machine-binding-sha256", required=True)
    request.add_argument("--expires-in-hours", type=float, default=24.0)
    request.add_argument("--output", type=Path, required=True)
    request.set_defaults(handler=_request)

    provision = commands.add_parser(
        "provision-credential", help="store one sidecar secret in Windows Credential Manager"
    )
    provision.add_argument("--bundle", type=Path, required=True)
    provision.add_argument("--secret-sidecar", type=Path, required=True)
    provision.add_argument("--consume", action="store_true")
    provision.set_defaults(handler=_provision_credential)

    receipt = commands.add_parser(
        "emit-receipt", help="emit one HMAC-bound receipt after real Windows acceptance"
    )
    receipt.add_argument("--bundle", type=Path, required=True)
    receipt.add_argument("--output", type=Path, required=True)
    receipt.add_argument("--fabric-node-id", required=True)
    receipt.add_argument("--tailscale-peer-identity", required=True)
    receipt.add_argument("--machine-binding-sha256", required=True)
    receipt.add_argument("--broker-protocol-version", required=True)
    receipt.add_argument("--broker-runtime-version", required=True)
    receipt.add_argument("--broker-runtime-profile-sha256", required=True)
    receipt.add_argument("--broker-target-service-config-sha256", required=True)
    receipt.add_argument("--authority-id", required=True)
    receipt.add_argument("--registry-id", required=True)
    receipt.add_argument("--runtime-instance-id", required=True)
    receipt.add_argument("--capability", action="append", default=["node-runtime"])
    receipt.add_argument("--local-health-passed", action="store_true")
    receipt.add_argument("--tailnet-health-passed", action="store_true")
    receipt.add_argument("--scm-acceptance-passed", action="store_true")
    receipt.add_argument("--restart-acceptance-passed", action="store_true")
    receipt.set_defaults(handler=_emit_receipt)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        return int(args.handler(args))
    except (FileExistsError, OSError, RuntimeError, ValueError) as error:
        print(json.dumps({"status": "rejected", "reason": str(error)}, sort_keys=True))
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
