#!/usr/bin/env python3
"""Build a deterministic, privacy-bounded CHIPS Agent Fabric source archive."""

from __future__ import annotations

import argparse
import gzip
import hashlib
import io
import json
import subprocess
import tarfile
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Any


@dataclass(frozen=True, slots=True)
class ReleasePolicy:
    exclude_prefixes: tuple[str, ...]
    exclude_files: frozenset[str]

    @classmethod
    def load(cls, path: Path) -> ReleasePolicy:
        raw = json.loads(path.read_text(encoding="utf-8"))
        if raw.get("schemaVersion") != 1 or raw.get("includeTrackedFiles") is not True:
            raise ValueError("unsupported public release policy")
        return cls(
            exclude_prefixes=tuple(str(value) for value in raw["excludePrefixes"]),
            exclude_files=frozenset(str(value) for value in raw["excludeFiles"]),
        )

    def includes(self, relative_path: str) -> bool:
        normalized = PurePosixPath(relative_path).as_posix()
        if normalized.startswith("/") or ".." in PurePosixPath(normalized).parts:
            return False
        if normalized in self.exclude_files:
            return False
        return not any(normalized.startswith(prefix) for prefix in self.exclude_prefixes)


@dataclass(frozen=True, slots=True)
class ReleaseFile:
    path: str
    object_id: str
    executable: bool
    payload: bytes


def _run_git(root: Path, *args: str) -> str:
    completed = subprocess.run(
        ("git", "-C", str(root), *args),
        check=True,
        capture_output=True,
        text=True,
    )
    return completed.stdout.strip()


def _git_bytes(root: Path, *args: str) -> bytes:
    completed = subprocess.run(
        ("git", "-C", str(root), *args),
        check=True,
        capture_output=True,
    )
    return completed.stdout


def revision_files(root: Path, revision: str, policy: ReleasePolicy) -> list[ReleaseFile]:
    """Read immutable regular-file blobs from one Git revision.

    Release inputs never come from the mutable worktree or index. Symlinks,
    submodules, and other special entries fail closed.
    """

    entries: list[ReleaseFile] = []
    listing = _git_bytes(root, "ls-tree", "-r", "-z", "--full-tree", revision)
    for raw_entry in listing.split(b"\0"):
        if not raw_entry:
            continue
        metadata, separator, raw_path = raw_entry.partition(b"\t")
        if not separator:
            raise ValueError("malformed Git tree entry")
        mode, kind, object_id = metadata.decode("ascii").split()
        path = raw_path.decode("utf-8", errors="strict")
        if not policy.includes(path):
            continue
        if kind != "blob" or mode not in {"100644", "100755"}:
            raise ValueError(f"release input must be a regular file: {path}")
        entries.append(
            ReleaseFile(
                path=path,
                object_id=object_id,
                executable=mode == "100755",
                payload=_git_bytes(root, "cat-file", "blob", object_id),
            )
        )
    return sorted(entries, key=lambda entry: entry.path)


def sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _tar_bytes(files: list[ReleaseFile], *, epoch: int, prefix: str) -> bytes:
    buffer = io.BytesIO()
    with tarfile.open(fileobj=buffer, mode="w", format=tarfile.PAX_FORMAT) as archive:
        for release_file in files:
            info = tarfile.TarInfo(f"{prefix}/{release_file.path}")
            info.size = len(release_file.payload)
            info.mtime = epoch
            info.uid = 0
            info.gid = 0
            info.uname = ""
            info.gname = ""
            info.mode = 0o755 if release_file.executable else 0o644
            archive.addfile(info, io.BytesIO(release_file.payload))
    return buffer.getvalue()


def build_release(
    root: Path,
    output_dir: Path,
    *,
    epoch: int | None = None,
    revision: str | None = None,
) -> dict[str, Any]:
    policy = ReleasePolicy.load(root / "release" / "public-release-files.json")
    revision = revision or _run_git(root, "rev-parse", "HEAD")
    epoch = (
        epoch if epoch is not None else int(_run_git(root, "show", "-s", "--format=%ct", revision))
    )
    files = revision_files(root, revision, policy)
    paths = {release_file.path for release_file in files}
    required = {"AGENTS.md", "FABRIC_INTENT.md", "LICENSE", "README.md", "pyproject.toml"}
    missing = sorted(required.difference(paths))
    if missing:
        raise ValueError(f"public release is missing required files: {', '.join(missing)}")

    if output_dir.is_symlink():
        raise ValueError("output directory must not be a symbolic link")
    if output_dir.exists() and any(output_dir.iterdir()):
        raise ValueError("output directory must be absent or empty")
    output_dir.mkdir(parents=True, exist_ok=True)
    prefix = "chips-agent-fabric-0.1.0-alpha.1"
    archive_path = output_dir / f"{prefix}-source.tar.gz"
    tar_payload = _tar_bytes(files, epoch=epoch, prefix=prefix)
    with (
        archive_path.open("xb") as raw,
        gzip.GzipFile(filename="", mode="wb", fileobj=raw, mtime=epoch) as compressed,
    ):
        compressed.write(tar_payload)

    file_entries = [
        {
            "path": release_file.path,
            "bytes": len(release_file.payload),
            "sha256": sha256_bytes(release_file.payload),
            "gitObject": release_file.object_id,
        }
        for release_file in files
    ]
    archive_payload = archive_path.read_bytes()
    manifest = {
        "schemaVersion": 1,
        "release": "0.1.0-alpha.1",
        "revision": revision,
        "sourceDateEpoch": epoch,
        "license": "Apache-2.0",
        "historyPolicy": "clean-public-history-required",
        "archive": {
            "path": archive_path.name,
            "bytes": len(archive_payload),
            "sha256": sha256_bytes(archive_payload),
        },
        "files": file_entries,
        "excludedPrivateHistory": True,
        "publicationStatus": "built-for-publication",
    }
    manifest_path = output_dir / "source-release-manifest.json"
    manifest_payload = (json.dumps(manifest, indent=2, sort_keys=True) + "\n").encode()
    with manifest_path.open("xb") as output:
        output.write(manifest_payload)
    checksums = (
        f"{manifest['archive']['sha256']}  {archive_path.name}\n"
        f"{sha256_bytes(manifest_payload)}  {manifest_path.name}\n"
    )
    with (output_dir / "source-release-checksums.sha256").open("xb") as output:
        output.write(checksums.encode())
    return manifest


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path(__file__).resolve().parents[1])
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--source-date-epoch", type=int)
    args = parser.parse_args()
    manifest = build_release(
        args.root.resolve(),
        args.output_dir.resolve(),
        epoch=args.source_date_epoch,
    )
    print(json.dumps(manifest["archive"], sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
