from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest
from jsonschema import Draft202012Validator, FormatChecker

EVIDENCE_ROOT = Path(__file__).resolve().parents[1] / "artifacts" / "goal-run"


def manifest_paths() -> list[Path]:
    """Every durable manifest: the top-level bundle plus one per run directory."""

    paths = [EVIDENCE_ROOT / "acceptance-evidence.json"]
    paths.extend(sorted(EVIDENCE_ROOT.glob("*/manifest.json")))
    return [path for path in paths if path.is_file()]


def verify_digests(manifest: dict, base: Path) -> int:
    verified = 0
    for artifact in manifest["artifacts"]:
        path = base / artifact["path"]
        if not path.is_file():
            # Large/local runtime artifacts are intentionally .gitignored. When present during the
            # goal run they are checked; a source checkout still validates the durable manifest.
            continue
        assert path.stat().st_size == artifact["bytes"]
        assert hashlib.sha256(path.read_bytes()).hexdigest() == artifact["sha256"]
        verified += 1
    return verified


@pytest.mark.parametrize("manifest_path", manifest_paths(), ids=lambda path: path.parent.name)
def test_evidence_manifest_matches_schema_and_artifact_checksums(manifest_path: Path) -> None:
    schema = json.loads((EVIDENCE_ROOT / "evidence.schema.json").read_text(encoding="utf-8"))
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    Draft202012Validator(schema, format_checker=FormatChecker()).validate(manifest)

    assert manifest["securityReview"]["credentialValuesIncluded"] is False
    # Paths in a run manifest are relative to that run directory; the top-level
    # bundle resolves against the evidence root.
    base = manifest_path.parent
    assert verify_digests(manifest, base) >= 1


def test_real_acceptance_manifest_still_records_a_passing_gate() -> None:
    manifest_path = EVIDENCE_ROOT / "acceptance-evidence.json"
    if not manifest_path.is_file():
        pytest.skip("private real-host acceptance evidence is excluded from public source")
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert manifest["status"] == "PASS"


def test_blocked_assertions_always_name_their_external_prerequisite() -> None:
    for manifest_path in manifest_paths():
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        for assertion in manifest["assertions"]:
            if assertion["status"] == "BLOCKED":
                assert assertion.get("blocker"), f"{manifest_path.name}:{assertion['id']}"
            if assertion["status"] == "PASS":
                assert assertion["evidence"], f"{manifest_path.name}:{assertion['id']}"
