from __future__ import annotations

import gzip
import hashlib
import io
import json
import subprocess
import sys
import tarfile
import tomllib
from pathlib import Path

import pytest

from scripts.audit_public_release import scan_archive, scan_payload
from scripts.build_public_release import ReleasePolicy, build_release


def test_release_policy_excludes_private_v0_evidence_and_integration_harnesses() -> None:
    root = Path(__file__).resolve().parents[1]
    policy = ReleasePolicy.load(root / "release" / "public-release-files.json")
    assert not policy.includes("artifacts/goal-run/local-worker-remote-v0/REPORT.md")
    assert not policy.includes("artifacts/any-future-release/evidence.json")
    assert not policy.includes("CyberOffice/client/private-state.json")
    assert not policy.includes("CyberIsland/world/private-state.json")
    assert not policy.includes("ArtLab/render/private-state.json")
    assert not policy.includes("docs/CLAUDE_GOAL_HANDOFF.md")
    assert not policy.includes("scripts/run_remote_worker_gates.py")
    assert policy.includes("src/project_supervisor/domain.py")
    assert policy.includes("docs/ARCHITECTURE.md")


def test_scanner_detects_secrets_private_paths_and_operator_markers() -> None:
    # Assemble detection fixtures at runtime so the scanner's own public test
    # source never contains a publishable secret-shaped literal.
    payload = (
        b"password="
        + b"realisticsecretvalue123\n"
        + b"/Us"
        + b"ers/alice/private\nnode-operator-01\n"
    )
    kinds = {finding.kind for finding in scan_payload("unsafe.txt", payload, (b"operator-01",))}
    assert {"assigned-secret", "absolute-home-path", "forbidden-local-marker"} <= kinds


def test_scanner_ignores_hashes_and_explicit_placeholders() -> None:
    digest = hashlib.sha256(b"fixture").hexdigest().encode()
    payload = b"token=placeholder-not-a-secret\n" + digest + b"\n"
    assert scan_payload("safe.txt", payload, ()) == []


def test_archive_scan_never_extracts_members(tmp_path: Path) -> None:
    archive = tmp_path / "unsafe.tar.gz"
    with tarfile.open(archive, "w:gz") as writer:
        info = tarfile.TarInfo("../escape.txt")
        payload = b"safe fixture"
        info.size = len(payload)
        writer.addfile(info, io.BytesIO(payload))
    report = scan_archive(archive)
    assert report["filesScanned"] == 0
    assert report["passed"] is False
    assert report["findings"] == [{"kind": "unsafe-member-path", "path": "../escape.txt"}]
    assert not (tmp_path.parent / "escape.txt").exists()


def test_archive_scan_rejects_links_duplicates_and_binary_secrets(tmp_path: Path) -> None:
    archive = tmp_path / "hostile.tar.gz"
    with tarfile.open(archive, "w:gz") as writer:
        link = tarfile.TarInfo("release/link")
        link.type = tarfile.SYMTYPE
        link.linkname = "../../outside"
        writer.addfile(link)
        for payload in (b"safe", b"second"):
            info = tarfile.TarInfo("release/duplicate.txt")
            info.size = len(payload)
            writer.addfile(info, io.BytesIO(payload))
        binary_secret = b"\x00password=" + b"realisticsecretvalue123\n"
        info = tarfile.TarInfo("release/binary.bin")
        info.size = len(binary_secret)
        writer.addfile(info, io.BytesIO(binary_secret))

    report = scan_archive(archive)
    kinds = {finding["kind"] for finding in report["findings"]}
    assert {"unsafe-member-type", "duplicate-member", "assigned-secret"} <= kinds


def test_release_builder_is_byte_reproducible(tmp_path: Path) -> None:
    root = tmp_path / "repo"
    root.mkdir()
    subprocess.run(("git", "init", "-q", str(root)), check=True)
    files = {
        "AGENTS.md": "agent entry\n",
        "FABRIC_INTENT.md": "intent\n",
        "LICENSE": "Apache-2.0 fixture\n",
        "README.md": "readme\n",
        "pyproject.toml": "[project]\nname='fixture'\n",
        "release/public-release-files.json": json.dumps(
            {
                "schemaVersion": 1,
                "includeTrackedFiles": True,
                "excludePrefixes": ["artifacts/"],
                "excludeFiles": [],
            }
        ),
        "src/example.py": "VALUE = 1\n",
        "artifacts/private.txt": "must not ship\n",
    }
    for relative, content in files.items():
        path = root / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content, encoding="utf-8")
    subprocess.run(("git", "-C", str(root), "add", "."), check=True)
    subprocess.run(("git", "-C", str(root), "config", "user.name", "Release Fixture"), check=True)
    subprocess.run(
        ("git", "-C", str(root), "config", "user.email", "release@example.invalid"), check=True
    )
    subprocess.run(("git", "-C", str(root), "commit", "-qm", "fixture"), check=True)
    revision = subprocess.run(
        ("git", "-C", str(root), "rev-parse", "HEAD"),
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()

    first = build_release(
        root,
        tmp_path / "first",
        release_version="0.2.0-beta.1",
        epoch=1_700_000_000,
        revision=revision,
    )
    second = build_release(
        root,
        tmp_path / "second",
        release_version="0.2.0-beta.1",
        epoch=1_700_000_000,
        revision=revision,
    )
    assert first["archive"]["sha256"] == second["archive"]["sha256"]
    assert first["release"] == "0.2.0-beta.1"
    assert first["archive"]["path"] == "chips-agent-fabric-0.2.0-beta.1-source.tar.gz"
    archive = tmp_path / "first" / first["archive"]["path"]
    with (
        gzip.open(archive) as compressed,
        tarfile.open(fileobj=io.BytesIO(compressed.read()), mode="r:") as reader,
    ):
        names = reader.getnames()
    assert all(name.startswith("chips-agent-fabric-0.2.0-beta.1/") for name in names)
    assert all("artifacts/private.txt" not in name for name in names)
    assert scan_archive(archive)["passed"] is True


def test_release_builder_uses_committed_blobs_and_refuses_existing_output(tmp_path: Path) -> None:
    root = tmp_path / "repo"
    root.mkdir()
    subprocess.run(("git", "init", "-q", str(root)), check=True)
    subprocess.run(("git", "-C", str(root), "config", "user.name", "Release Fixture"), check=True)
    subprocess.run(
        ("git", "-C", str(root), "config", "user.email", "release@example.invalid"), check=True
    )
    required = {
        "AGENTS.md": "committed agent entry\n",
        "FABRIC_INTENT.md": "intent\n",
        "LICENSE": "Apache-2.0 fixture\n",
        "README.md": "readme\n",
        "pyproject.toml": "[project]\nname='fixture'\n",
        "release/public-release-files.json": json.dumps(
            {
                "schemaVersion": 1,
                "includeTrackedFiles": True,
                "excludePrefixes": [],
                "excludeFiles": [],
            }
        ),
    }
    for relative, content in required.items():
        path = root / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content, encoding="utf-8")
    subprocess.run(("git", "-C", str(root), "add", "."), check=True)
    subprocess.run(("git", "-C", str(root), "commit", "-qm", "fixture"), check=True)
    revision = subprocess.run(
        ("git", "-C", str(root), "rev-parse", "HEAD"),
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    (root / "README.md").write_text("uncommitted private mutation\n", encoding="utf-8")

    output = tmp_path / "output"
    manifest = build_release(
        root,
        output,
        release_version="0.2.0-beta.1",
        epoch=1_700_000_000,
        revision=revision,
    )
    readme = next(item for item in manifest["files"] if item["path"] == "README.md")
    assert readme["sha256"] == hashlib.sha256(b"readme\n").hexdigest()
    with pytest.raises(ValueError, match="absent or empty"):
        build_release(
            root,
            output,
            release_version="0.2.0-beta.1",
            epoch=1_700_000_000,
            revision=revision,
        )


@pytest.mark.parametrize("invalid", ["../secret", "/absolute", "artifacts/goal-run/x"])
def test_release_policy_rejects_unsafe_or_excluded_paths(invalid: str) -> None:
    policy = ReleasePolicy(("artifacts/goal-run/",), frozenset())
    assert policy.includes(invalid) is False


def test_release_policy_has_fail_closed_artifact_boundary() -> None:
    policy = ReleasePolicy((), frozenset())
    assert policy.includes("artifacts") is False
    assert policy.includes("artifacts/unlisted/future-output.json") is False


@pytest.mark.parametrize("private_root", ["ArtLab", "CyberIsland", "CyberOffice"])
def test_release_policy_has_fail_closed_workspace_boundaries(private_root: str) -> None:
    policy = ReleasePolicy((), frozenset())
    assert policy.includes(private_root) is False
    assert policy.includes(f"{private_root}/unlisted/private-output.json") is False


def test_hatch_build_excludes_all_private_scope_roots() -> None:
    root = Path(__file__).resolve().parents[1]
    config = tomllib.loads((root / "pyproject.toml").read_text(encoding="utf-8"))
    excluded = set(config["tool"]["hatch"]["build"]["exclude"])
    assert {"/artifacts", "/ArtLab", "/CyberIsland", "/CyberOffice"} <= excluded


@pytest.mark.parametrize(
    "invalid",
    ["", "v0.2.0", "01.2.3", "0.2.0 beta.1", "../0.2.0", "0.2"],
)
def test_release_builder_rejects_missing_or_invalid_semver(tmp_path: Path, invalid: str) -> None:
    root = Path(__file__).resolve().parents[1]
    with pytest.raises(ValueError, match="explicit valid SemVer"):
        build_release(root, tmp_path / "output", release_version=invalid)


def test_release_cli_requires_explicit_version(tmp_path: Path) -> None:
    root = Path(__file__).resolve().parents[1]
    completed = subprocess.run(
        (
            sys.executable,
            str(root / "scripts" / "build_public_release.py"),
            "--output-dir",
            str(tmp_path / "output"),
        ),
        check=False,
        capture_output=True,
        text=True,
    )
    assert completed.returncode == 2
    assert "--release-version" in completed.stderr
