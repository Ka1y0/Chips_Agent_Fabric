#!/usr/bin/env python3
"""Standalone, stdlib-only verifier for a private Windows Fabric Node artifact."""

from __future__ import annotations

import argparse
import hashlib
import io
import json
import stat
import zipfile
from datetime import UTC, datetime
from pathlib import Path, PurePosixPath
from typing import Any

MANIFEST_SCHEMA = "windows-node-artifact-manifest/v1"
BUNDLE_SCHEMA = "node-enrollment-bundle/v1"
REQUEST_SCHEMA = "windows-node-artifact-request/v1"


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        while chunk := stream.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def _safe_path(value: str) -> None:
    path = PurePosixPath(value)
    if (
        not value
        or value.startswith(("/", "\\"))
        or "\\" in value
        or path.is_absolute()
        or any(part in {"", ".", ".."} for part in path.parts)
        or ":" in path.parts[0]
    ):
        raise ValueError("unsafe archive member path")


def _json(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"{path.name} root must be an object")
    return value


def _time(value: object) -> datetime:
    if not isinstance(value, str):
        raise ValueError("timestamp must be a string")
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError("timestamp must include a timezone")
    return parsed.astimezone(UTC)


def verify_artifact(artifact: Path, manifest_path: Path) -> dict[str, Any]:
    manifest = _json(manifest_path)
    if manifest.get("schemaVersion") != MANIFEST_SCHEMA:
        raise ValueError("unsupported artifact manifest")
    if _sha256(artifact) != manifest.get("artifactSHA256"):
        raise ValueError("artifact digest mismatch")
    rows = manifest.get("files")
    if not isinstance(rows, list) or not rows:
        raise ValueError("artifact manifest has no files")
    expected: dict[str, dict[str, Any]] = {}
    for row in rows:
        if not isinstance(row, dict) or set(row) != {"relativePath", "size", "sha256"}:
            raise ValueError("invalid artifact manifest file row")
        name = str(row["relativePath"])
        _safe_path(name)
        if name in expected:
            raise ValueError("duplicate artifact manifest path")
        expected[name] = row
    with zipfile.ZipFile(artifact) as archive:
        infos = archive.infolist()
        names = [info.filename for info in infos]
        if len(names) != len(set(names)) or set(names) != set(expected):
            raise ValueError("artifact contains an unmanifested, missing, or duplicate file")
        for info in infos:
            _safe_path(info.filename)
            mode = info.external_attr >> 16
            if info.is_dir() or stat.S_ISLNK(mode):
                raise ValueError("artifact contains a non-regular member")
            value = archive.read(info)
            row = expected[info.filename]
            if len(value) != row["size"]:
                raise ValueError("artifact member size mismatch")
            if hashlib.sha256(value).hexdigest() != row["sha256"]:
                raise ValueError("artifact member digest mismatch")
        wheel_names = [name for name in names if name.endswith(".whl")]
        if len(wheel_names) != 1:
            raise ValueError("artifact must contain exactly one runtime wheel")
        wheel_bytes = archive.read(wheel_names[0])
    with zipfile.ZipFile(io.BytesIO(wheel_bytes)) as wheel:
        migration_numbers = sorted(
            int(PurePosixPath(name).stem.split("_", 1)[0])
            for name in wheel.namelist()
            if name.startswith("project_supervisor/migrations/") and name.endswith(".sql")
        )
    if migration_numbers != list(range(1, 22)):
        raise ValueError("runtime migration chain must be continuous from 0001 through 0021")
    if manifest.get("targetPlatform") != "windows" or manifest.get("targetArchitecture") != "x64":
        raise ValueError("artifact target is not Windows x64")
    return manifest


def verify_bundle(
    bundle_path: Path,
    request_path: Path,
    manifest: dict[str, Any],
    *,
    now: datetime,
) -> dict[str, Any]:
    bundle = _json(bundle_path)
    request = _json(request_path)
    if bundle.get("schemaVersion") != BUNDLE_SCHEMA:
        raise ValueError("unsupported enrollment bundle")
    if request.get("schemaVersion") != REQUEST_SCHEMA:
        raise ValueError("unsupported enrollment request")
    if now >= _time(bundle.get("expiresAt")) or now >= _time(request.get("expiresAt")):
        raise ValueError("enrollment material has expired")
    expected = bundle.get("expectedHost")
    artifact = bundle.get("artifact")
    service = bundle.get("serviceProfile")
    if (
        not isinstance(expected, dict)
        or not isinstance(artifact, dict)
        or not isinstance(service, dict)
    ):
        raise ValueError("enrollment bundle shape is invalid")
    request_pairs = {
        "hostName": "hostName",
        "tailscalePeerIdentity": "tailscalePeerIdentity",
        "tailscaleServeURL": "tailscaleServeURL",
        "machineBindingSHA256": "machineBindingSHA256",
        "platform": "platform",
        "architecture": "architecture",
        "windowsBuild": "windowsBuild",
    }
    if any(expected.get(left) != request.get(right) for left, right in request_pairs.items()):
        raise ValueError("enrollment bundle does not match the host request")
    if artifact.get("artifactID") != manifest.get("artifactID") or artifact.get(
        "sha256"
    ) != manifest.get("artifactSHA256"):
        raise ValueError("enrollment bundle does not match the artifact")
    if service.get("sha256") != manifest.get("serviceProfileSHA256"):
        raise ValueError("enrollment bundle service profile does not match the artifact")
    credential = bundle.get("credential")
    if not isinstance(credential, dict) or credential.get("rawSecretIncluded") is not False:
        raise ValueError("enrollment bundle credential descriptor is unsafe")
    return bundle


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--artifact", type=Path, required=True)
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--bundle", type=Path)
    parser.add_argument("--request", type=Path)
    args = parser.parse_args(argv)
    if (args.bundle is None) != (args.request is None):
        parser.error("--bundle and --request must be supplied together")
    try:
        manifest = verify_artifact(args.artifact, args.manifest)
        bundle = None
        if args.bundle is not None and args.request is not None:
            bundle = verify_bundle(
                args.bundle,
                args.request,
                manifest,
                now=datetime.now(UTC),
            )
    except (OSError, ValueError, zipfile.BadZipFile, json.JSONDecodeError) as error:
        print(json.dumps({"status": "rejected", "reason": str(error)}, sort_keys=True))
        return 2
    print(
        json.dumps(
            {
                "status": "verified",
                "artifactID": manifest["artifactID"],
                "artifactSHA256": manifest["artifactSHA256"],
                "serviceProfileSHA256": manifest["serviceProfileSHA256"],
                "enrollmentID": bundle.get("enrollmentID") if bundle is not None else None,
                "installationAuthorized": bundle is not None,
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
