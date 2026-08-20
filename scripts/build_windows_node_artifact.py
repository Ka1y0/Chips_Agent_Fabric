#!/usr/bin/env python3
"""Build the private, deterministic Windows Fabric Node Phase 2B artifact."""

from __future__ import annotations

import argparse
import hashlib
import io
import json
import re
import zipfile
from pathlib import Path

from project_supervisor.fabric.node_enrollment import (
    WindowsNodeArtifactBuilder,
    verify_windows_artifact,
)


def _write_supporting_files(result) -> None:  # type: ignore[no-untyped-def]
    target = result.artifact_path.parent
    request_contract = {
        "schemaVersion": "windows-node-artifact-request-contract/v1",
        "requestSchemaVersion": "windows-node-artifact-request/v1",
        "requiredFields": [
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
        ],
        "rawMachineGuidAllowed": False,
        "expectedPlatform": "windows",
        "expectedArchitecture": "x64",
        "requestedArtifactContract": "chips-windows-fabric-node-phase2",
        "maximumLifetimeHours": 168,
        "expectedOutputFilename": "mac-phase2-artifact-request.json",
    }
    request_path = target / "pc-enrollment-request-contract.json"
    request_path.write_text(json.dumps(request_contract, indent=2, sort_keys=True) + "\n")
    inputs = {
        "schemaVersion": "pc-enrollment-inputs/v1",
        "state": "waitingForPCEnrollmentRequest",
        "artifactFilename": result.artifact_path.name,
        "artifactSHA256": result.artifact_sha256,
        "manifestFilename": result.manifest_path.name,
        "manifestSHA256": result.manifest_sha256,
        "serviceProfileFilename": result.service_profile_path.name,
        "serviceProfileSHA256": result.service_profile_sha256,
        "requestContractFilename": request_path.name,
        "bundleFilename": None,
        "secretSidecarFilename": None,
        "expectedEnrollmentID": None,
        "verificationEntrypoint": "tools/verify_windows_node_artifact.py",
        "requestEntrypoint": "python -m project_supervisor.fabric.windows_node_bootstrap request",
        "expectedReceiptFilename": "windows-node-enrollment-receipt.json",
    }
    inputs_path = target / "pc-enrollment-inputs.json"
    inputs_path.write_text(json.dumps(inputs, indent=2, sort_keys=True) + "\n")
    verification_command = (
        "python tools/verify_windows_node_artifact.py "
        f"--artifact {result.artifact_path.name} --manifest {result.manifest_path.name}"
    )
    handoff = f"""# PC enrollment handoff

Status: `WAITING_FOR_PC_ENROLLMENT_REQUEST`

This is a private dogfood artifact, not a GitHub release. Verify it before any mutation:

```text
{verification_command}
```

Artifact SHA-256: `{result.artifact_sha256}`
Manifest SHA-256: `{result.manifest_sha256}`
Service profile SHA-256: `{result.service_profile_sha256}`

The Mac does not have the exact machine-readable host binding yet. On FABRIC-WINDOWS-NODE, use the bundled
`project_supervisor.fabric.windows_node_bootstrap request` command to create
`mac-phase2-artifact-request.json` with the already-derived machine-binding SHA-256, exact
Tailscale peer identity, HTTPS Serve origin, Windows build, and a bounded expiry. Do not include
raw MachineGuid or any credential. Return that request to the Mac enrollment authority.

No enrollment bundle or bootstrap secret has been issued in this directory. Do not install or
create a service until the Mac returns a host-bound bundle and separate secret sidecar.

The bundled runtime is an application payload, not a completed SCM installer. The current console
broker has no bundled Windows ServiceMain host, and an interactive user's Credential Manager entry
is not proven readable by the fixed virtual service account. Generate and return only the request
at this stage; do not report SCM, credential, health, restart, or receipt success until those two
Windows boundaries are implemented and independently accepted.
"""
    (target / "PC_ENROLLMENT_HANDOFF.md").write_text(handoff, encoding="utf-8")
    manifest = verify_windows_artifact(result.artifact_path, result.manifest_path)
    forbidden_paths = 0
    secret_findings = 0
    absolute_mac_paths = 0
    secret_patterns = (
        re.compile(rb"AKIA[0-9A-Z]{16}"),
        re.compile(rb"sk-[A-Za-z0-9_-]{20,}"),
        re.compile(rb"-----BEGIN (?:RSA |EC |OPENSSH )?PRIVATE KEY-----"),
    )

    def inspect(name: str, content: bytes) -> None:
        nonlocal forbidden_paths, secret_findings, absolute_mac_paths
        parts = set(Path(name).parts)
        if parts.intersection({".git", "artifacts", "CyberOffice", "CyberIsland", "ArtLab"}):
            forbidden_paths += 1
        secret_findings += sum(bool(pattern.search(content)) for pattern in secret_patterns)
        absolute_mac_paths += int(b"/Users/" in content or b"/private/var/folders/" in content)

    with zipfile.ZipFile(result.artifact_path) as archive:
        for name in archive.namelist():
            content = archive.read(name)
            inspect(name, content)
            if name.endswith(".whl"):
                with zipfile.ZipFile(io.BytesIO(content)) as wheel:
                    for wheel_name in wheel.namelist():
                        inspect(wheel_name, wheel.read(wheel_name))
    if forbidden_paths or secret_findings or absolute_mac_paths:
        raise RuntimeError("artifact security audit rejected the generated payload")
    audit = {
        "schemaVersion": "windows-node-artifact-security-audit/v1",
        "artifactID": result.artifact_id,
        "artifactSHA256": result.artifact_sha256,
        "closedManifestVerified": True,
        "payloadFileCount": len(result.payload_files),
        "sourceFileCount": len(manifest["sourceFiles"]),
        "symlinks": 0,
        "unmanifestedFiles": 0,
        "forbiddenProjectPayloads": forbidden_paths,
        "credentialsFound": secret_findings,
        "absoluteMacUserPathsFound": absolute_mac_paths,
        "bootstrapSecretIncluded": False,
    }
    audit_path = target / "artifact-security-audit.json"
    audit_path.write_text(json.dumps(audit, indent=2, sort_keys=True) + "\n")
    for path in (request_path, inputs_path, target / "PC_ENROLLMENT_HANDOFF.md", audit_path):
        path.chmod(0o600)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-root", type=Path, default=Path(__file__).parents[1])
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args(argv)
    try:
        result = WindowsNodeArtifactBuilder(args.source_root).build(args.output_dir)
        _write_supporting_files(result)
    except (OSError, RuntimeError, ValueError) as error:
        print(json.dumps({"status": "rejected", "reason": str(error)}, sort_keys=True))
        return 2
    print(
        json.dumps(
            {
                "status": "built",
                "artifactID": result.artifact_id,
                "artifactPath": str(result.artifact_path),
                "artifactSHA256": result.artifact_sha256,
                "manifestPath": str(result.manifest_path),
                "manifestSHA256": result.manifest_sha256,
                "serviceProfilePath": str(result.service_profile_path),
                "serviceProfileSHA256": result.service_profile_sha256,
                "sourceSnapshotSHA256": result.source_snapshot_sha256,
                "handoffSHA256": hashlib.sha256(
                    (result.artifact_path.parent / "PC_ENROLLMENT_HANDOFF.md").read_bytes()
                ).hexdigest(),
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
