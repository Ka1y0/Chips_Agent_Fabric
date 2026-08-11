from __future__ import annotations

import asyncio
import json
import os
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

from project_supervisor.adapters import (
    AgyAdapter,
    ClaudeAdapter,
    CodexAdapter,
    GrokAdapter,
    WorkerRequest,
)

PROJECT_ROOT = Path(__file__).parents[1]
FAKE_CLI_SOURCE = PROJECT_ROOT / "scripts" / "fake_native_worker_cli.py"


def fake_executable(tmp_path: Path, profile: str, scenario: str = "normal") -> Path:
    control = tmp_path / f"{profile}-{scenario}"
    control.mkdir()
    executable = control / f"fake-native-worker-cli--{scenario}"
    shutil.copy2(FAKE_CLI_SOURCE, executable)
    executable.chmod(0o700)
    return executable


@pytest.mark.parametrize(
    ("profile", "adapter_type", "expected_model", "required_arguments"),
    [
        (
            "claude",
            ClaudeAdapter,
            "claude-offline-fixture",
            {"--output-format", "stream-json", "--verbose"},
        ),
        (
            "grok",
            GrokAdapter,
            "grok-offline-fixture",
            {"--output-format", "streaming-json"},
        ),
        (
            "agy",
            AgyAdapter,
            "AGY Offline Fixture",
            {"--log-file", "--sandbox", "--mode", "plan"},
        ),
        (
            "codex",
            CodexAdapter,
            None,
            {
                "exec",
                "--json",
                "--output-last-message",
                "--skip-git-repo-check",
                "--ephemeral",
                "--ignore-user-config",
                "--strict-config",
            },
        ),
    ],
)
async def test_fake_cli_exercises_real_native_adapter_dialects(
    tmp_path: Path,
    profile: str,
    adapter_type: type[ClaudeAdapter] | type[GrokAdapter] | type[AgyAdapter] | type[CodexAdapter],
    expected_model: str | None,
    required_arguments: set[str],
) -> None:
    executable = fake_executable(tmp_path, profile)
    adapter = adapter_type(str(executable), heartbeat_seconds=0.01)
    result = await adapter.execute(
        WorkerRequest(
            run_id=f"native-cli-{profile}",
            prompt="Offline production-like Local Worker acceptance",
            timeout_seconds=5,
        )
    )

    assert result.succeeded
    assert result.final_text == "NATIVE_CLI_OFFLINE_OK"
    assert result.model == expected_model
    assert result.session_id
    invocation = json.loads(
        (executable.parent / "invocations.jsonl").read_text(encoding="utf-8").strip()
    )
    assert invocation["profile"] == profile
    assert invocation["scenario"] == "normal"
    assert invocation["credential_environment_keys"] == []
    assert required_arguments <= set(invocation["argv"])
    if profile == "codex":
        assert invocation["argv"][-1] == "-"
        assert "Offline production-like Local Worker acceptance" not in invocation["argv"]
        assert invocation["prompt_sha256"]
        assert invocation["argv"][invocation["argv"].index("--sandbox") + 1] == "read-only"
        assert invocation["argv"][invocation["argv"].index("--ask-for-approval") + 1] == "never"
        assert not {
            "--dangerously-bypass-approvals-and-sandbox",
            "--full-auto",
            "--search",
            "--add-dir",
        } & set(invocation["argv"])
    else:
        assert invocation["argv"][invocation["argv"].index("-p") + 1] == (
            "Offline production-like Local Worker acceptance"
        )


async def test_fake_codex_cli_supports_schema_and_fails_closed_on_invalid_result(
    tmp_path: Path,
) -> None:
    executable = fake_executable(tmp_path, "codex")
    schema = {
        "type": "object",
        "properties": {"answer": {"type": "string"}},
        "required": ["answer"],
        "additionalProperties": False,
    }
    valid = CodexAdapter(
        str(executable),
        environment={"CHIPS_FAKE_CLI_RESULT": '{"answer":"yes"}'},
    )
    result = await valid.execute(
        WorkerRequest(
            run_id="native-cli-codex-schema",
            prompt="return structured output",
            timeout_seconds=5,
            metadata={"response_schema": schema},
        )
    )

    assert result.succeeded
    assert json.loads(result.final_text) == {"answer": "yes"}
    invocation = json.loads(
        (executable.parent / "invocations.jsonl").read_text(encoding="utf-8").splitlines()[0]
    )
    assert "--output-schema" in invocation["argv"]
    assert "return structured output" not in invocation["argv"]

    invalid_executable = fake_executable(tmp_path, "codex-invalid-schema")
    invalid = CodexAdapter(
        str(invalid_executable),
        environment={"CHIPS_FAKE_CLI_RESULT": '{"wrong":true}'},
    )
    rejected = await invalid.execute(
        WorkerRequest(
            run_id="native-cli-codex-invalid-schema",
            prompt="return structured output",
            timeout_seconds=5,
            metadata={"response_schema": schema},
        )
    )
    assert rejected.state.value == "failed"
    assert rejected.error is not None and "RESULT_INVALID" in rejected.error


async def test_fake_cli_nonzero_and_cancellation_are_real_process_outcomes(
    tmp_path: Path,
) -> None:
    failed_executable = fake_executable(tmp_path, "claude", "nonzero")
    failed = await ClaudeAdapter(str(failed_executable)).execute(
        WorkerRequest(run_id="native-cli-nonzero", prompt="offline", timeout_seconds=5)
    )
    assert failed.state.value == "failed"
    assert failed.exit_code == 17
    assert failed.error == "FAKE_NATIVE_CLI_NONZERO"

    cancelled_executable = fake_executable(tmp_path, "claude", "cancel")
    adapter = ClaudeAdapter(str(cancelled_executable), heartbeat_seconds=0.01)
    request = WorkerRequest(run_id="native-cli-cancel", prompt="offline", timeout_seconds=5)
    running = asyncio.create_task(adapter.execute(request))
    for _ in range(500):
        if (cancelled_executable.parent / "started.json").is_file():
            break
        await asyncio.sleep(0.01)
    assert (cancelled_executable.parent / "started.json").is_file()
    assert await adapter.cancel(request.run_id)
    cancelled = await running

    assert cancelled.state.value == "cancelled"
    assert (cancelled_executable.parent / "cancelled.json").is_file()


@pytest.mark.parametrize(
    "scenario", ["nonzero", "auth-required", "malformed", "wrong-dialect", "flood"]
)
async def test_fake_codex_cli_failure_modes_never_become_success(
    tmp_path: Path, scenario: str
) -> None:
    executable = fake_executable(tmp_path, "codex", scenario)
    result = await CodexAdapter(str(executable), termination_grace_seconds=0.1).execute(
        WorkerRequest(
            run_id=f"native-cli-codex-{scenario}",
            prompt="offline",
            timeout_seconds=5,
        )
    )

    assert result.state.value == "failed"
    if scenario in {"nonzero", "auth-required"}:
        assert result.exit_code == (17 if scenario == "nonzero" else 1)
        assert result.state.value == "failed"
        assert result.state.value != "authRequired"
        if scenario == "auth-required":
            assert "NATIVE_CLI_OFFLINE_OK" not in (result.error or "")
            assert "[REDACTED]" in (result.error or "")
    else:
        assert result.error is not None and (
            "RESULT_INVALID" in result.error or "OUTPUT_LIMIT" in result.error
        )


async def test_fake_codex_cli_timeout_and_cancel_reap_real_processes(tmp_path: Path) -> None:
    timed_executable = fake_executable(tmp_path, "codex", "block")
    timed = await CodexAdapter(
        str(timed_executable), heartbeat_seconds=0.01, termination_grace_seconds=0.1
    ).execute(
        WorkerRequest(run_id="native-cli-codex-timeout", prompt="offline", timeout_seconds=0.05)
    )
    assert timed.state.value == "timedOut"
    assert timed.exit_code is not None

    cancelled_executable = fake_executable(tmp_path, "codex", "cancel")
    adapter = CodexAdapter(
        str(cancelled_executable), heartbeat_seconds=0.01, termination_grace_seconds=0.1
    )
    request = WorkerRequest(run_id="native-cli-codex-cancel", prompt="offline", timeout_seconds=5)
    running = asyncio.create_task(adapter.execute(request))
    for _ in range(500):
        if (cancelled_executable.parent / "started.json").is_file():
            break
        await asyncio.sleep(0.01)
    assert (cancelled_executable.parent / "started.json").is_file()
    assert await adapter.cancel(request.run_id)
    cancelled = await running

    assert cancelled.state.value == "cancelled"
    assert (cancelled_executable.parent / "cancelled.json").is_file()


def test_local_worker_v2_native_cli_real_process_acceptance(tmp_path: Path) -> None:
    output = tmp_path / "native-cli-acceptance.json"
    result = subprocess.run(
        [
            sys.executable,
            str(PROJECT_ROOT / "scripts" / "run_local_worker_v2_native_cli_acceptance.py"),
            "--output",
            str(output),
        ],
        cwd=PROJECT_ROOT,
        env={
            "HOME": str(tmp_path / "home"),
            "PATH": os.environ.get("PATH", "/usr/bin:/bin"),
            "PYTHONPATH": os.pathsep.join((str(PROJECT_ROOT / "src"), str(PROJECT_ROOT))),
            "PYTHONDONTWRITEBYTECODE": "1",
            "TMPDIR": str(tmp_path),
            "NO_COLOR": "1",
        },
        check=False,
        capture_output=True,
        text=True,
        timeout=180,
    )

    assert result.returncode == 0, result.stderr or result.stdout
    report = json.loads(output.read_text(encoding="utf-8"))
    assert report["pass"] is True
    assert report["realDaemonProcesses"] is True
    assert report["realNativeRunnerProcesses"] is True
    assert report["realFakeCLIProcesses"] is True
    assert report["realSupervisorProcess"] is True
    assert report["nativeAdapterDialects"] == ["claude", "grok", "agy", "codex"]

    cases = {case["case"]: case for case in report["cases"]}
    assert set(cases) == {
        "agy-normal",
        "claude-cancel",
        "claude-control-flood",
        "claude-flood",
        "claude-malformed",
        "claude-nonzero",
        "claude-normal",
        "claude-wrong-dialect",
        "codex-auth-required",
        "codex-cancel",
        "codex-flood",
        "codex-malformed",
        "codex-nonzero",
        "codex-normal",
        "codex-wrong-dialect",
        "grok-normal",
    }
    for name in ("claude-normal", "grok-normal", "agy-normal", "codex-normal"):
        assert cases[name]["resultState"] == "completed"
        assert cases[name]["resultText"] == "NATIVE_CLI_OFFLINE_OK"
    for name in (
        "claude-flood",
        "claude-malformed",
        "claude-nonzero",
        "claude-wrong-dialect",
        "codex-auth-required",
        "codex-flood",
        "codex-malformed",
        "codex-nonzero",
        "codex-wrong-dialect",
    ):
        assert cases[name]["resultState"] == "failed"
    assert cases["claude-control-flood"]["resultState"] in {"completed", "failed"}
    assert cases["claude-cancel"]["resultState"] == "cancelled"
    assert cases["claude-cancel"]["cancellationAccepted"] is True
    assert cases["claude-cancel"]["fakeCancelledMarker"] is True
    assert cases["claude-cancel"]["fakeProcessGone"] is True
    assert cases["codex-cancel"]["resultState"] == "cancelled"
    assert cases["codex-cancel"]["cancellationAccepted"] is True
    assert cases["codex-cancel"]["fakeCancelledMarker"] is True
    assert cases["codex-cancel"]["fakeProcessGone"] is True
    assert cases["codex-auth-required"]["resultError"] == "PROVIDER_EXIT_NONZERO"
    for case in cases.values():
        assert case["registryLaunchCount"] == 1
        assert case["fakeInvocationCount"] == 1
        assert case["fakeCredentialEnvironmentKeys"] == []
        assert case["terminalReceiptBytes"] <= 65_536

    restarts = {case["scenario"]: case for case in report["restartCases"]}
    assert set(restarts) == {"supervisor-only", "daemon-only", "both-lost-response"}
    assert restarts["supervisor-only"]["daemonRestarted"] is False
    assert restarts["supervisor-only"]["handleUnboundBeforeRecovery"] is False
    assert restarts["daemon-only"]["sameSupervisorRuntime"] is True
    assert restarts["daemon-only"]["driverType"] == "codex"
    assert restarts["both-lost-response"]["daemonRestarted"] is True
    assert restarts["both-lost-response"]["responseLost"] is True
    assert restarts["both-lost-response"]["handleUnboundBeforeRecovery"] is True
    for case in restarts.values():
        assert case["taskState"] == "reviewing"
        assert case["fakeInvocationCount"] == 1
        assert case["canonicalResultCount"] == 1
        assert case["workerResultRecordedEvents"] == 1
        assert case["providerResultCollectedEvents"] == 1
        assert case["openEscalationCount"] == 0

    mismatch = report["capabilityMismatch"]
    assert mismatch["adapterIdentityChanged"] is True
    assert mismatch["reconcileState"] == "unknown"
    assert mismatch["lookupState"] == "unknown"
    assert mismatch["replayRejectedUncertain"] is True
    assert mismatch["originalFakeInvocationCount"] == 1
    assert mismatch["replacementFakeInvocationCount"] == 0
    assert mismatch["registryLaunchCount"] == 1
    assert mismatch["registryDriverProfileRevision"] == 1
    assert report["credentialsAccessed"] is False
    assert report["networkAccess"] is False
    assert report["billableProviderUsed"] is False
