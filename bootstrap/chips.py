#!/usr/bin/env python3
"""CHIPS Agent Fabric bootstrap FOUNDATION entrypoint.

The default action is a dry run. ``--emit`` writes only reviewable generated
configuration and state into an explicit output directory.
"""

from __future__ import annotations

import argparse
import contextlib
import copy
import hashlib
import json
import sys
from collections.abc import Sequence
from pathlib import Path
from typing import Any

try:
    from .check_environment import inspect
except ImportError:  # Direct ``python bootstrap/chips.py`` execution.
    from check_environment import inspect


def build_plan(profile: dict[str, Any]) -> dict[str, Any]:
    """Build a stable, explainable bootstrap plan from one discovery snapshot."""

    host = profile["host"]
    fabric = profile["fabric"]
    resources = profile["resources"]
    supported = host["operatingSystem"] in {"Darwin", "Windows", "Linux"}
    steps: list[dict[str, Any]] = [
        {
            "id": "validate-platform",
            "action": "validate",
            "status": "ready" if supported else "blocked",
            "reason": "supported foundation platform"
            if supported
            else "unsupported operating system",
            "mutating": False,
        },
        {
            "id": "review-existing-state",
            "action": "review",
            "status": "required" if fabric["existingStateDatabase"] else "notNeeded",
            "reason": "existing canonical state must never be overwritten"
            if fabric["existingStateDatabase"]
            else "no existing state database discovered",
            "mutating": False,
        },
        {
            "id": "install-supervisor",
            "action": "package.install",
            "status": "notNeeded" if fabric["supervisorExecutable"] else "approvalRequired",
            "reason": "Supervisor executable discovered"
            if fabric["supervisorExecutable"]
            else "installation is outside bootstrap FOUNDATION",
            "mutating": True,
        },
        {
            "id": "establish-node-identity",
            "action": "identity.enroll",
            "status": "notNeeded" if fabric["identityPresent"] else "approvalRequired",
            "reason": "existing identity metadata discovered"
            if fabric["identityPresent"]
            else "trust-root enrollment is not automated by this foundation",
            "mutating": True,
        },
        {
            "id": "configure-private-transport",
            "action": "network.private.configure",
            "status": "approvalRequired",
            "reason": (
                "an installed transport is only discovery evidence; "
                "authenticated reachability was not tested"
            ),
            "mutating": True,
        },
        {
            "id": "register-workers",
            "action": "worker.register",
            "status": "approvalRequired",
            "reason": (
                "discovered executables and model servers are candidates, not verified workers"
            ),
            "mutating": True,
            "candidates": {
                "aiCLIs": sorted(name for name, path in resources["aiCLIs"].items() if path),
                "modelServers": sorted(
                    name for name, path in resources["modelServers"].items() if path
                ),
            },
        },
        {
            "id": "run-acceptance",
            "action": "acceptance.run",
            "status": "blocked",
            "reason": (
                "requires reviewed installation, identity, transport, and worker configuration"
            ),
            "mutating": False,
        },
    ]
    return {
        "schemaVersion": 1,
        "milestone": "universal-bootstrap-foundation",
        "mode": "dryRun",
        "supportedPlatform": supported,
        "failClosed": True,
        "steps": steps,
        "automaticActions": [],
        "prohibitedAutomaticActions": [
            "authentication",
            "credentialAccess",
            "networkConnection",
            "packageInstallation",
            "privilegeElevation",
            "serviceStart",
            "workerRegistration",
        ],
    }


def _canonical_json(payload: object) -> bytes:
    return (json.dumps(payload, indent=2, sort_keys=True) + "\n").encode()


def redact_host_details(profile: dict[str, Any]) -> dict[str, Any]:
    """Return a shareable discovery projection without host identity or paths."""

    result = copy.deepcopy(profile)
    host = result.get("host", {})
    if isinstance(host, dict):
        host["hostname"] = "[REDACTED_LOCAL_HOST]"
    resources = result.get("resources", {})
    if isinstance(resources, dict):
        for values in resources.values():
            if isinstance(values, dict):
                for name, value in values.items():
                    values[name] = "[DETECTED]" if value else None
    fabric = result.get("fabric", {})
    if isinstance(fabric, dict):
        if fabric.get("stateDirectoryCandidate"):
            fabric["stateDirectoryCandidate"] = "[REDACTED_LOCAL_PATH]"
        if fabric.get("supervisorExecutable"):
            fabric["supervisorExecutable"] = "[DETECTED]"
        node_runtimes = fabric.get("nodeRuntimes")
        if isinstance(node_runtimes, dict):
            for name, value in node_runtimes.items():
                node_runtimes[name] = "[DETECTED]" if value else None
    network = result.get("networkHints", {})
    if isinstance(network, dict) and isinstance(network.get("interfaceNames"), list):
        network["interfaceCount"] = len(network["interfaceNames"])
        network["interfaceNames"] = []
    result["hostDetailsRedacted"] = True
    return result


def emit_bundle(output_dir: Path, profile: dict[str, Any], plan: dict[str, Any]) -> list[str]:
    """Write non-secret, review-only generated state into a new empty directory."""

    if output_dir.is_symlink():
        raise ValueError("output directory must not be a symbolic link")
    if output_dir.exists() and any(output_dir.iterdir()):
        raise ValueError("output directory must be absent or empty; refusing to overwrite")
    output_dir.mkdir(parents=True, exist_ok=True)
    with contextlib.suppress(OSError):
        output_dir.chmod(0o700)
    config = {
        "host": "127.0.0.1",
        "port": 7330,
        "private_transport": False,
        "tls_certificate": None,
        "tls_private_key": None,
        "read_only_api": True,
        "log_level": "INFO",
    }
    files = {
        "machine-profile.json": profile,
        "bootstrap-plan.json": plan,
        "supervisor-config.example.json": config,
    }
    for name, payload in files.items():
        path = output_dir / name
        with path.open("xb") as output:
            output.write(_canonical_json(payload))
        with contextlib.suppress(OSError):
            path.chmod(0o600)
    checksums = {
        name: hashlib.sha256((output_dir / name).read_bytes()).hexdigest() for name in sorted(files)
    }
    checksum_path = output_dir / "checksums.json"
    with checksum_path.open("xb") as output:
        output.write(_canonical_json(checksums))
    with contextlib.suppress(OSError):
        checksum_path.chmod(0o600)
    return sorted([*files, "checksums.json"])


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="chips")
    subparsers = parser.add_subparsers(dest="command", required=True)
    bootstrap = subparsers.add_parser(
        "bootstrap", help="discover this machine and create a dry-run plan"
    )
    bootstrap.add_argument("--json", action="store_true", help="emit machine-readable output")
    bootstrap.add_argument(
        "--emit", action="store_true", help="write a review bundle; performs no installation"
    )
    bootstrap.add_argument(
        "--output-dir", type=Path, help="explicit directory for generated machine state"
    )
    bootstrap.add_argument(
        "--redact-host",
        action="store_true",
        help="replace hostnames and local paths for shareable output",
    )
    args = parser.parse_args(argv)
    if args.emit != bool(args.output_dir):
        parser.error("--emit and --output-dir must be supplied together")
    profile = inspect()
    plan = build_plan(profile)
    output_profile = redact_host_details(profile) if args.redact_host else profile
    output: dict[str, Any] = {
        "schemaVersion": 1,
        "command": "bootstrap",
        "dryRun": True,
        "profile": output_profile,
        "plan": plan,
        "filesWritten": [],
    }
    if args.emit:
        try:
            output["filesWritten"] = emit_bundle(args.output_dir, output_profile, plan)
        except (OSError, ValueError) as error:
            print(f"bootstrap refused: {error}", file=sys.stderr)
            return 2
    if args.json:
        print(json.dumps(output, indent=2, sort_keys=True))
    else:
        print("CHIPS Agent Fabric universal bootstrap FOUNDATION")
        print(f"Platform supported: {plan['supportedPlatform']}")
        print("Mode: dry-run; no install, login, network access, service start, or elevation")
        if output["filesWritten"]:
            print(f"Generated review bundle: {args.output_dir}")
    return 0 if plan["supportedPlatform"] else 3


if __name__ == "__main__":
    raise SystemExit(main())
