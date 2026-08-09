from __future__ import annotations

import asyncio
import hashlib
import json
import os
import re
from dataclasses import dataclass, field
from enum import StrEnum
from pathlib import Path
from typing import Any


class VerificationKind(StrEnum):
    COMMAND = "command"
    FILE_EXISTS = "fileExists"
    JSON_EQUALS = "jsonEquals"


@dataclass(frozen=True, slots=True)
class VerificationCriterion:
    id: str
    kind: VerificationKind
    description: str
    required: bool = True
    command: tuple[str, ...] = ()
    relative_path: str | None = None
    expected: Any = None
    timeout_seconds: float = 120.0


@dataclass(frozen=True, slots=True)
class VerificationResult:
    criterion_id: str
    passed: bool
    summary: str
    exit_code: int | None = None
    evidence: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class DefinitionOfDoneResult:
    complete: bool
    results: tuple[VerificationResult, ...]
    required_failures: tuple[str, ...]


class VerificationPolicyError(ValueError):
    pass


class DeterministicVerifier:
    def __init__(self, workspace_root: str | Path, evidence_root: str | Path) -> None:
        self.workspace_root = Path(workspace_root).resolve()
        self.evidence_root = Path(evidence_root).resolve()
        self.evidence_root.mkdir(parents=True, exist_ok=True)

    async def verify(self, criterion: VerificationCriterion) -> VerificationResult:
        if criterion.kind is VerificationKind.COMMAND:
            return await self._command(criterion)
        if criterion.kind is VerificationKind.FILE_EXISTS:
            return self._file_exists(criterion)
        if criterion.kind is VerificationKind.JSON_EQUALS:
            return self._json_equals(criterion)
        raise VerificationPolicyError(f"unsupported verification kind: {criterion.kind}")

    async def verify_definition(
        self, criteria: list[VerificationCriterion]
    ) -> DefinitionOfDoneResult:
        results = tuple([await self.verify(criterion) for criterion in criteria])
        required = {criterion.id for criterion in criteria if criterion.required}
        failures = tuple(
            result.criterion_id
            for result in results
            if not result.passed and result.criterion_id in required
        )
        return DefinitionOfDoneResult(
            complete=bool(required) and not failures,
            results=results,
            required_failures=failures,
        )

    async def _command(self, criterion: VerificationCriterion) -> VerificationResult:
        if not criterion.command:
            raise VerificationPolicyError("command criterion requires argv")
        executable = criterion.command[0]
        if os.path.sep in executable:
            executable_path = Path(executable).expanduser().resolve()
            if not executable_path.exists():
                return VerificationResult(
                    criterion.id,
                    False,
                    f"executable not found: {executable_path}",
                )
        process = await asyncio.create_subprocess_exec(
            *criterion.command,
            cwd=self.workspace_root,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            start_new_session=True,
        )
        try:
            stdout, stderr = await asyncio.wait_for(
                process.communicate(), timeout=criterion.timeout_seconds
            )
            timed_out = False
        except TimeoutError:
            timed_out = True
            process.terminate()
            try:
                await asyncio.wait_for(process.wait(), timeout=3)
            except TimeoutError:
                process.kill()
                await process.wait()
            stdout = b""
            stderr = b"verification timed out"

        stdout_text = self._redact(stdout.decode(errors="replace"))
        stderr_text = self._redact(stderr.decode(errors="replace"))
        evidence_path = self.evidence_root / f"{self._safe_id(criterion.id)}.json"
        evidence = {
            "argv": list(criterion.command),
            "exitCode": process.returncode,
            "timedOut": timed_out,
            "stdoutSHA256": hashlib.sha256(stdout).hexdigest(),
            "stderrSHA256": hashlib.sha256(stderr).hexdigest(),
            "stdout": stdout_text[-4000:],
            "stderr": stderr_text[-4000:],
        }
        evidence_path.write_text(json.dumps(evidence, indent=2), encoding="utf-8")
        passed = not timed_out and process.returncode == 0
        return VerificationResult(
            criterion.id,
            passed,
            "command passed" if passed else "command failed",
            exit_code=process.returncode,
            evidence={"path": str(evidence_path), **evidence},
        )

    def _file_exists(self, criterion: VerificationCriterion) -> VerificationResult:
        path = self._resolve_relative(criterion.relative_path)
        passed = path.is_file()
        return VerificationResult(
            criterion.id,
            passed,
            f"file {'exists' if passed else 'missing'}: {criterion.relative_path}",
            evidence={"path": str(path), "exists": passed},
        )

    def _json_equals(self, criterion: VerificationCriterion) -> VerificationResult:
        path = self._resolve_relative(criterion.relative_path)
        try:
            actual = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as error:
            return VerificationResult(
                criterion.id,
                False,
                f"could not read JSON: {error}",
                evidence={"path": str(path)},
            )
        passed = actual == criterion.expected
        return VerificationResult(
            criterion.id,
            passed,
            "JSON matched" if passed else "JSON differed",
            evidence={"path": str(path), "actual": actual, "expected": criterion.expected},
        )

    def _resolve_relative(self, relative: str | None) -> Path:
        if not relative:
            raise VerificationPolicyError("file criterion requires relative_path")
        candidate = (self.workspace_root / relative).resolve()
        if candidate != self.workspace_root and self.workspace_root not in candidate.parents:
            raise VerificationPolicyError("verification path escapes workspace")
        return candidate

    @staticmethod
    def _safe_id(value: str) -> str:
        return re.sub(r"[^A-Za-z0-9_.-]", "_", value)

    @staticmethod
    def _redact(value: str) -> str:
        patterns = [
            r"(?i)(authorization\s*[:=]\s*)([^\s]+)",
            r"(?i)((?:api[_-]?key|access[_-]?token|refresh[_-]?token|cookie)\s*[:=]\s*)([^\s]+)",
        ]
        result = value
        for pattern in patterns:
            result = re.sub(pattern, r"\1[REDACTED]", result)
        return result
