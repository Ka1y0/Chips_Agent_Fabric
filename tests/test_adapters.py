from __future__ import annotations

import asyncio
import json
import os
import sys
import tempfile
from collections.abc import Mapping, Sequence
from pathlib import Path

import pytest

from project_supervisor.adapters import (
    AgyAdapter,
    ClaudeAdapter,
    GrokAdapter,
    MockAdapter,
    MockBehavior,
    NativeSubprocessAdapter,
    ParsedOutput,
    UnsafeWorkerRequest,
    WorkerRequest,
    child_environment,
    redact,
)
from project_supervisor.domain import RunState


class PythonProcessAdapter(NativeSubprocessAdapter):
    def __init__(self, code: str, **kwargs):  # type: ignore[no-untyped-def]
        super().__init__(sys.executable, **kwargs)
        self.code = code

    def command_arguments(self, request: WorkerRequest) -> Sequence[str]:
        return ("-u", "-c", self.code)

    def consume_stdout_line(self, line: str, parsed: ParsedOutput) -> Mapping[str, object]:
        parsed.text_fragments.append(f"{line}\n")
        return {"providerEventType": "text"}


def test_redaction_recurses_and_scrubs_text_credentials() -> None:
    value = redact(
        {
            "access_token": "must-not-escape",
            # Assemble the leak-shaped fixture at runtime so the public source
            # scanner does not reject its own test corpus.
            "nested": {"message": "Authorization: Bearer " + "abcdefghijklmnop"},
            "safe": "visible",
        }
    )
    assert value["access_token"] == "[REDACTED]"
    assert "abcdefghijklmnop" not in value["nested"]["message"]
    assert value["safe"] == "visible"


def test_native_adapter_defaults_are_portable_and_overridable() -> None:
    assert ClaudeAdapter().executable == "claude"
    assert AgyAdapter().executable == "agy"
    assert ClaudeAdapter(executable="/opt/workers/claude").executable == "/opt/workers/claude"
    assert AgyAdapter(executable="/opt/workers/agy").executable == "/opt/workers/agy"


def test_native_child_environment_is_allowlisted_and_explicitly_extensible() -> None:
    env = child_environment(
        {
            "PATH": "/portable/bin",
            "HOME": "/portable/home",
            "LC_TEST": "locale",
            "HTTP_PROXY": "https://untrusted-proxy.example",
            "UNRELATED_API_KEY": "must-not-inherit",
        },
        {"PROVIDER_EXPLICIT_CONTEXT": "approved"},
    )

    assert env == {
        "PATH": "/portable/bin",
        "HOME": "/portable/home",
        "LC_TEST": "locale",
        "PROVIDER_EXPLICIT_CONTEXT": "approved",
    }


async def test_native_process_supervision_streams_heartbeats_and_redacts() -> None:
    code = (
        "import sys,time; "
        "print('token=super-secret-value', flush=True); "
        "print('diagnostic', file=sys.stderr, flush=True); "
        "time.sleep(.05); print('DONE', flush=True)"
    )
    adapter = PythonProcessAdapter(code, heartbeat_seconds=0.01)
    observed = []
    result = await adapter.execute(
        WorkerRequest(run_id="native-1", prompt="unused"), event_sink=observed.append
    )

    assert result.succeeded
    assert result.pid is not None
    assert result.exit_code == 0
    assert "super-secret-value" not in result.stdout
    assert "[REDACTED]" in result.stdout
    assert result.stderr == "diagnostic\n"
    assert result.final_text.endswith("DONE")
    assert {event.kind for event in observed} >= {
        "processStarted",
        "workerOutput",
        "heartbeat",
        "processExited",
    }


async def test_native_timeout_terminates_worker() -> None:
    adapter = PythonProcessAdapter("import time; time.sleep(10)", heartbeat_seconds=0.01)
    result = await adapter.execute(
        WorkerRequest(run_id="timeout", prompt="unused", timeout_seconds=0.03)
    )
    assert result.state is RunState.TIMED_OUT
    assert result.exit_code is not None


async def test_native_cancel_is_independent_and_normalized() -> None:
    started = asyncio.Event()
    adapter = PythonProcessAdapter("import time; time.sleep(10)", heartbeat_seconds=0.01)

    async def sink(event):  # type: ignore[no-untyped-def]
        if event.kind == "processStarted":
            started.set()

    running = asyncio.create_task(
        adapter.execute(WorkerRequest(run_id="cancel", prompt="unused"), event_sink=sink)
    )
    await asyncio.wait_for(started.wait(), timeout=1)
    assert await adapter.cancel("cancel")
    result = await running
    assert result.state is RunState.CANCELLED
    assert not await adapter.cancel("missing")


def test_claude_parser_uses_result_payload_and_actual_model_metadata() -> None:
    adapter = ClaudeAdapter(executable=sys.executable)
    parsed = ParsedOutput()
    lines = [
        {"type": "system", "session_id": "claude-session"},
        {
            "type": "assistant",
            "message": {
                "model": "claude-opus-5",
                "content": [{"type": "text", "text": "CLAUDE_NATIVE_OK"}],
                "usage": {"input_tokens": 2, "output_tokens": 17},
            },
        },
        {
            "type": "result",
            "session_id": "claude-session",
            "result": "CLAUDE_NATIVE_OK",
            "total_cost_usd": 0.064,
            "usage": {
                "input_tokens": 2,
                "output_tokens": 17,
                "cache_creation_input_tokens": 5947,
                "cache_read_input_tokens": 7709,
            },
            "modelUsage": {"claude-opus-5[1m]": {"contextWindow": 1_000_000}},
        },
    ]
    for line in lines:
        adapter.consume_stdout_line(json.dumps(line), parsed)

    assert parsed.final_text == "CLAUDE_NATIVE_OK"
    assert parsed.session_id == "claude-session"
    assert parsed.model == "claude-opus-5"
    assert parsed.context_variant == "1m"
    assert parsed.usage.input_tokens == 2
    assert parsed.usage.cache_read_tokens == 7709
    assert parsed.usage.cost_usd == pytest.approx(0.064)
    assert parsed.provider_event_types == {"system", "assistant", "result"}


def test_grok_parser_assembles_fragments_and_terminal_metadata() -> None:
    adapter = GrokAdapter(executable=sys.executable)
    parsed = ParsedOutput()
    lines = [
        {"type": "text", "data": "GROK_"},
        {"type": "text", "data": "NATIVE_OK"},
        {
            "type": "end",
            "sessionId": "grok-session",
            "total_cost_usd": 0.018,
            "modelUsage": {
                "grok-4.5-build": {
                    "inputTokens": 100,
                    "outputTokens": 3,
                    "totalTokens": 103,
                }
            },
        },
    ]
    for line in lines:
        adapter.consume_stdout_line(json.dumps(line), parsed)
    adapter.finalize_output(WorkerRequest(run_id="grok", prompt="x"), parsed, "", "")

    assert parsed.final_text == "GROK_NATIVE_OK"
    assert parsed.session_id == "grok-session"
    assert parsed.model == "grok-4.5-build"
    assert parsed.usage.total_tokens == 103
    assert parsed.usage.cost_usd == pytest.approx(0.018)


def test_agy_is_permanently_read_only_and_parses_plain_text_diagnostics() -> None:
    adapter = AgyAdapter(executable=sys.executable)
    with pytest.raises(UnsafeWorkerRequest, match="code-write"):
        adapter.command_arguments(
            WorkerRequest(run_id="agy-write", prompt="write", code_write_required=True)
        )
    with pytest.raises(UnsafeWorkerRequest, match="model changes"):
        asyncio.run(
            adapter.execute(WorkerRequest(run_id="agy-model", prompt="read", model="other"))
        )

    descriptor, diagnostic_path = tempfile.mkstemp()
    os.close(descriptor)
    adapter._diagnostic_paths["agy-read"] = diagnostic_path
    Path(diagnostic_path).write_text(
        "Created conversation 5fa0d74f-b7fd-4c81-99e2-3fc6b60be5b0\n"
        'selected model override to backend: label="Gemini 3.5 Flash (High)"\n'
    )
    parsed = ParsedOutput()
    adapter.consume_stdout_line("AGY_NATIVE_OK", parsed)
    adapter.finalize_output(
        WorkerRequest(run_id="agy-read", prompt="read"),
        parsed,
        "AGY_NATIVE_OK\n",
        "",
    )
    Path(diagnostic_path).unlink()
    assert parsed.final_text == "AGY_NATIVE_OK"
    assert parsed.session_id == "5fa0d74f-b7fd-4c81-99e2-3fc6b60be5b0"
    assert parsed.model == "Gemini 3.5 Flash (High)"


async def test_mock_adapter_is_deterministic() -> None:
    adapter = MockAdapter(MockBehavior(text="FIXED", session_id="session"))
    request = WorkerRequest(run_id="mock-1", prompt="anything")
    first = await adapter.execute(request)
    second = await adapter.execute(request)
    assert first.final_text == second.final_text == "FIXED"
    assert first.session_id == second.session_id == "session:mock-1"
