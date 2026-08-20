#!/usr/bin/env python3
"""Fail-closed secret and privacy scan for a public source archive."""

from __future__ import annotations

import argparse
import json
import math
import re
import tarfile
from dataclasses import dataclass
from pathlib import Path, PurePosixPath

SECRET_PATTERNS = (
    ("private-key", re.compile(rb"-----BEGIN (?:RSA |EC |OPENSSH )?PRIVATE KEY-----")),
    ("openai-key", re.compile(rb"\bsk-[A-Za-z0-9_-]{20,}\b")),
    ("github-token", re.compile(rb"\bgh[pousr]_[A-Za-z0-9]{20,}\b")),
    ("google-key", re.compile(rb"\bAIza[A-Za-z0-9_-]{20,}\b")),
    ("jwt", re.compile(rb"\beyJ[A-Za-z0-9_-]{12,}\.[A-Za-z0-9_-]{12,}\.[A-Za-z0-9_-]{8,}\b")),
    (
        "bearer-value",
        re.compile(rb"(?i)authorization\s*[:=]\s*bearer\s+[A-Za-z0-9._~+/-]{12,}"),
    ),
    (
        "assigned-secret",
        re.compile(
            rb"(?i)(?:api[_-]?key|client[_-]?secret|password|access[_-]?token)\s*[:=]\s*"
            rb"[\"']?(?!example|placeholder|test-only|mock)[A-Za-z0-9._~+/-]{16,}"
        ),
    ),
)
# Split literal platform home prefixes so this fail-closed scanner does not
# flag its own source while still compiling the exact detection expression.
ABSOLUTE_HOME = re.compile(
    rb"(?:/Us"
    rb"ers/[^/\s]+/|/ho"
    rb"me/[^/\s]+/|[A-Za-z]:\\Us"
    rb"ers\\[^\\\s]+\\)"
)
HIGH_ENTROPY = re.compile(rb"\b[A-Za-z0-9_~+/-]{40,}\b")
HEX_DIGEST = re.compile(rb"^[a-fA-F0-9]{40,128}$")
MAX_ARCHIVE_BYTES = 64 * 1024 * 1024
MAX_FILE_BYTES = 16 * 1024 * 1024
MAX_TOTAL_BYTES = 128 * 1024 * 1024
MAX_MEMBERS = 10_000


@dataclass(frozen=True, slots=True)
class Finding:
    kind: str
    path: str
    line: int | None = None

    def as_dict(self) -> dict[str, object]:
        value: dict[str, object] = {"kind": self.kind, "path": self.path}
        if self.line is not None:
            value["line"] = self.line
        return value


def _entropy(value: bytes) -> float:
    if not value:
        return 0.0
    counts = {byte: value.count(byte) for byte in set(value)}
    return -sum((count / len(value)) * math.log2(count / len(value)) for count in counts.values())


def _line_number(payload: bytes, start: int) -> int:
    return payload.count(b"\n", 0, start) + 1


def scan_payload(path: str, payload: bytes, forbidden: tuple[bytes, ...]) -> list[Finding]:
    findings: list[Finding] = []
    for kind, pattern in SECRET_PATTERNS:
        for match in pattern.finditer(payload):
            findings.append(Finding(kind, path, _line_number(payload, match.start())))
    for match in ABSOLUTE_HOME.finditer(payload):
        findings.append(Finding("absolute-home-path", path, _line_number(payload, match.start())))
    lowered = payload.lower()
    for marker in forbidden:
        start = lowered.find(marker.lower())
        if start >= 0:
            findings.append(Finding("forbidden-local-marker", path, _line_number(payload, start)))
    # Always scan binary data for known signatures and forbidden markers. Run
    # heuristic entropy detection only on text-like content to avoid treating
    # compressed/media bytes as credential strings.
    if b"\x00" not in payload[:8192]:
        for match in HIGH_ENTROPY.finditer(payload):
            value = match.group(0)
            if HEX_DIGEST.fullmatch(value) or b"placeholder" in value.lower():
                continue
            if _entropy(value) >= 4.5:
                findings.append(
                    Finding("high-entropy-string", path, _line_number(payload, match.start()))
                )
    return findings


def scan_archive(path: Path, forbidden: tuple[str, ...] = ()) -> dict[str, object]:
    findings: list[Finding] = []
    files_scanned = 0
    if not path.is_file() or path.is_symlink():
        findings.append(Finding("unsafe-archive-path", path.name))
    elif path.stat().st_size > MAX_ARCHIVE_BYTES:
        findings.append(Finding("archive-size-limit", path.name))
    else:
        seen: set[str] = set()
        total_bytes = 0
        encoded_forbidden = tuple(value.encode() for value in forbidden)
        try:
            # Streaming mode bounds metadata traversal and avoids extracting.
            with tarfile.open(path, mode="r|gz") as archive:
                for member_index, member in enumerate(archive, start=1):
                    if member_index > MAX_MEMBERS:
                        findings.append(Finding("archive-member-limit", path.name))
                        break
                    normalized = PurePosixPath(member.name)
                    if (
                        normalized.is_absolute()
                        or ".." in normalized.parts
                        or normalized.as_posix() != member.name
                    ):
                        findings.append(Finding("unsafe-member-path", member.name))
                        continue
                    if member.name in seen:
                        findings.append(Finding("duplicate-member", member.name))
                        continue
                    seen.add(member.name)
                    if member.isdir():
                        continue
                    if not member.isfile():
                        findings.append(Finding("unsafe-member-type", member.name))
                        continue
                    if member.size < 0 or member.size > MAX_FILE_BYTES:
                        findings.append(Finding("member-size-limit", member.name))
                        continue
                    total_bytes += member.size
                    if total_bytes > MAX_TOTAL_BYTES:
                        findings.append(Finding("archive-total-size-limit", path.name))
                        break
                    extracted = archive.extractfile(member)
                    if extracted is None:
                        findings.append(Finding("unreadable-member", member.name))
                        continue
                    payload = extracted.read(MAX_FILE_BYTES + 1)
                    if len(payload) != member.size or len(payload) > MAX_FILE_BYTES:
                        findings.append(Finding("member-size-mismatch", member.name))
                        continue
                    files_scanned += 1
                    findings.extend(scan_payload(member.name, payload, encoded_forbidden))
        except (OSError, tarfile.TarError):
            findings.append(Finding("invalid-archive", path.name))
    return {
        "schemaVersion": 1,
        "archive": path.name,
        "filesScanned": files_scanned,
        "findingCount": len(findings),
        "findings": [finding.as_dict() for finding in findings],
        "passed": not findings,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("archive", type=Path)
    parser.add_argument("--forbid", action="append", default=[])
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    report = scan_archive(args.archive, tuple(args.forbid))
    rendered = json.dumps(report, indent=2, sort_keys=True) + "\n"
    if args.output:
        args.output.write_text(rendered, encoding="utf-8")
    else:
        print(rendered, end="")
    return 0 if report["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
