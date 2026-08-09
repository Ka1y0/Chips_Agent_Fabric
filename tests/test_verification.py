import json
import sys

import pytest

from project_supervisor.verification import (
    DeterministicVerifier,
    VerificationCriterion,
    VerificationKind,
    VerificationPolicyError,
)


@pytest.mark.asyncio
async def test_definition_of_done_requires_every_required_criterion(tmp_path) -> None:
    workspace = tmp_path / "workspace"
    evidence = tmp_path / "evidence"
    workspace.mkdir()
    (workspace / "result.json").write_text(json.dumps({"ok": True}), encoding="utf-8")
    verifier = DeterministicVerifier(workspace, evidence)
    criteria = [
        VerificationCriterion(
            "file",
            VerificationKind.FILE_EXISTS,
            "result exists",
            relative_path="result.json",
        ),
        VerificationCriterion(
            "json",
            VerificationKind.JSON_EQUALS,
            "result matches",
            relative_path="result.json",
            expected={"ok": True},
        ),
        VerificationCriterion(
            "command",
            VerificationKind.COMMAND,
            "process succeeds",
            command=(sys.executable, "-c", "raise SystemExit(0)"),
        ),
    ]
    result = await verifier.verify_definition(criteria)
    assert result.complete
    assert result.required_failures == ()


@pytest.mark.asyncio
async def test_failure_prevents_premature_completion(tmp_path) -> None:
    verifier = DeterministicVerifier(tmp_path, tmp_path / "evidence")
    result = await verifier.verify_definition(
        [
            VerificationCriterion(
                "fail",
                VerificationKind.COMMAND,
                "must fail",
                command=(sys.executable, "-c", "raise SystemExit(3)"),
            )
        ]
    )
    assert not result.complete
    assert result.required_failures == ("fail",)
    assert result.results[0].exit_code == 3


@pytest.mark.asyncio
async def test_timeout_is_failure_and_process_is_cancelled(tmp_path) -> None:
    verifier = DeterministicVerifier(tmp_path, tmp_path / "evidence")
    result = await verifier.verify(
        VerificationCriterion(
            "timeout",
            VerificationKind.COMMAND,
            "must time out",
            command=(sys.executable, "-c", "import time; time.sleep(5)"),
            timeout_seconds=0.05,
        )
    )
    assert not result.passed
    assert result.evidence["timedOut"] is True


@pytest.mark.asyncio
async def test_verifier_rejects_workspace_escape(tmp_path) -> None:
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    verifier = DeterministicVerifier(workspace, tmp_path / "evidence")
    with pytest.raises(VerificationPolicyError):
        await verifier.verify(
            VerificationCriterion(
                "escape",
                VerificationKind.FILE_EXISTS,
                "escape",
                relative_path="../outside",
            )
        )
