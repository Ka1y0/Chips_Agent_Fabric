#!/usr/bin/env python3
"""Issue, inspect, revoke, and admit one host-bound Windows Node enrollment."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from datetime import timedelta
from pathlib import Path
from typing import Any

from project_supervisor.fabric.node_enrollment import (
    EnrollmentIssuerIdentity,
    NodeEnrollmentRepository,
    WindowsNodeArtifactRequest,
    WindowsNodeEnrollmentReceipt,
    default_windows_service_profile,
    generate_broker_credential,
    verify_windows_artifact,
    write_bootstrap_secret_sidecar,
)
from project_supervisor.store import StateStore


def _json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ValueError(f"cannot read {path.name}") from error
    if not isinstance(value, dict):
        raise ValueError(f"{path.name} root must be an object")
    return value


def _exclusive_json(path: Path, value: dict[str, Any], *, mode: int = 0o600) -> None:
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


def _secret(path: Path) -> str:
    value = _json(path)
    secret = value.get("bootstrapCredential")
    if not isinstance(secret, str):
        raise ValueError("bootstrap secret sidecar has no credential")
    return secret


def _prepare_separated_roots(output_dir: Path, secret_dir: Path) -> None:
    for path, label in ((output_dir, "ordinary output"), (secret_dir, "secret output")):
        if path.is_symlink() or (path.exists() and not path.is_dir()):
            raise ValueError(f"{label} root must be a real directory")
        path.mkdir(parents=True, exist_ok=True, mode=0o700)
    ordinary = output_dir.resolve()
    secret = secret_dir.resolve()
    if ordinary == secret or ordinary.is_relative_to(secret) or secret.is_relative_to(ordinary):
        raise ValueError("ordinary and secret output roots must be disjoint")


def _issue(args: argparse.Namespace) -> int:
    _prepare_separated_roots(args.output_dir, args.secret_dir)
    store = StateStore(args.database)
    repository = NodeEnrollmentRepository(store)
    request = WindowsNodeArtifactRequest.from_mapping(_json(args.request))
    active = repository.active_for_request(request.digest)
    if active is not None:
        raise RuntimeError(
            f"valid pending enrollment already exists: {active['id']}; do not generate a new secret"
        )
    manifest = verify_windows_artifact(args.artifact, args.manifest)
    profile = default_windows_service_profile()
    issuer = EnrollmentIssuerIdentity(
        node_id=args.issuer_node_id,
        key_id=args.issuer_key_id,
        public_key_fingerprint=args.issuer_public_key_fingerprint,
    )
    secret = generate_broker_credential()
    credential_reference = f"ChipsAgentFabric.RuntimeBroker.{request.host_name}"
    bundle = repository.issue(
        request=request,
        artifact_manifest=manifest,
        manifest_sha256=_sha256(args.manifest),
        profile=profile,
        issuer=issuer,
        bootstrap_secret=secret,
        credential_reference=credential_reference,
        ttl=timedelta(hours=args.expires_in_hours),
    )
    enrollment_id = str(bundle.definition["enrollmentID"])
    normal = args.output_dir / enrollment_id
    secret_dir = args.secret_dir / enrollment_id
    bundle_path = normal / "node-enrollment-bundle.json"
    sidecar_path = secret_dir / "node-enrollment-bootstrap-secret.json"
    try:
        normal.mkdir(parents=True, exist_ok=False, mode=0o700)
        _exclusive_json(bundle_path, bundle.to_protocol())
        write_bootstrap_secret_sidecar(
            sidecar_path,
            enrollment_id=enrollment_id,
            bundle_sha256=bundle.digest,
            secret=secret,
        )
        inputs = {
            "schemaVersion": "pc-enrollment-inputs/v1",
            "state": "readyForPCEnrollment",
            "enrollmentID": enrollment_id,
            "artifactFilename": args.artifact.name,
            "artifactSHA256": manifest["artifactSHA256"],
            "manifestFilename": args.manifest.name,
            "manifestSHA256": _sha256(args.manifest),
            "bundleFilename": bundle_path.name,
            "bundleSHA256": bundle.digest,
            "secretSidecarFilename": sidecar_path.name,
            "secretContentIncluded": False,
            "expectedPeerIdentitySHA256": hashlib.sha256(
                request.tailscale_peer_identity.encode("utf-8")
            ).hexdigest(),
            "serviceProfileSHA256": profile.digest,
            "verificationEntrypoint": "tools/verify_windows_node_artifact.py",
            "enrollmentEntrypoint": "python -m project_supervisor.fabric.windows_node_bootstrap",
            "expectedReceiptFilename": "windows-node-enrollment-receipt.json",
        }
        _exclusive_json(normal / "pc-enrollment-inputs.json", inputs)
    except Exception as error:
        try:
            repository.revoke(enrollment_id, actor=issuer.node_id)
        except Exception as revoke_error:
            raise RuntimeError(
                "enrollment output failed and automatic revocation failed; "
                "operator revocation is required"
            ) from revoke_error
        raise RuntimeError("enrollment output failed; pending authorization was revoked") from error
    print(
        json.dumps(
            {
                "status": "issued",
                "enrollmentID": enrollment_id,
                "bundlePath": str(bundle_path),
                "secretSidecarPath": str(sidecar_path),
                "secretPrinted": False,
            },
            sort_keys=True,
        )
    )
    return 0


def _admit(args: argparse.Namespace) -> int:
    repository = NodeEnrollmentRepository(StateStore(args.database))
    receipt = WindowsNodeEnrollmentReceipt(_json(args.receipt))
    result = repository.admit(
        receipt=receipt,
        bootstrap_secret=_secret(args.secret_sidecar),
        actor=args.actor,
    )
    print(
        json.dumps(
            {
                "status": result["state"],
                "enrollmentID": result["id"],
                "nodeID": result["admitted_node_id"],
                "secretPrinted": False,
            },
            sort_keys=True,
        )
    )
    return 0


def _revoke(args: argparse.Namespace) -> int:
    result = NodeEnrollmentRepository(StateStore(args.database)).revoke(
        args.enrollment_id, actor=args.actor
    )
    print(json.dumps({"status": result["state"], "enrollmentID": result["id"]}, sort_keys=True))
    return 0


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        while chunk := stream.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    issue = commands.add_parser("issue")
    issue.add_argument("--database", type=Path, required=True)
    issue.add_argument("--request", type=Path, required=True)
    issue.add_argument("--artifact", type=Path, required=True)
    issue.add_argument("--manifest", type=Path, required=True)
    issue.add_argument("--issuer-node-id", required=True)
    issue.add_argument("--issuer-key-id", required=True)
    issue.add_argument("--issuer-public-key-fingerprint", required=True)
    issue.add_argument("--expires-in-hours", type=float, default=1.0)
    issue.add_argument("--output-dir", type=Path, required=True)
    issue.add_argument("--secret-dir", type=Path, required=True)
    issue.set_defaults(handler=_issue)

    admit = commands.add_parser("admit")
    admit.add_argument("--database", type=Path, required=True)
    admit.add_argument("--receipt", type=Path, required=True)
    admit.add_argument("--secret-sidecar", type=Path, required=True)
    admit.add_argument("--actor", required=True)
    admit.set_defaults(handler=_admit)

    revoke = commands.add_parser("revoke")
    revoke.add_argument("--database", type=Path, required=True)
    revoke.add_argument("enrollment_id")
    revoke.add_argument("--actor", required=True)
    revoke.set_defaults(handler=_revoke)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        return int(args.handler(args))
    except (FileExistsError, KeyError, OSError, RuntimeError, ValueError) as error:
        print(json.dumps({"status": "rejected", "reason": str(error)}, sort_keys=True))
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
