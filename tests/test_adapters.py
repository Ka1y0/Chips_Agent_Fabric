from __future__ import annotations

import asyncio
import json
import os
import sys
import tempfile
import urllib.request
from collections.abc import Mapping, Sequence
from pathlib import Path

import pytest

from project_supervisor.adapters import (
    AgyAdapter,
    ClaudeAdapter,
    CodexAdapter,
    GrokAdapter,
    MockAdapter,
    MockBehavior,
    NativeSubprocessAdapter,
    ParsedOutput,
    UnsafeWorkerRequest,
    WorkerProtocolError,
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


class PythonClaudeAdapter(ClaudeAdapter):
    def __init__(self, code: str, **kwargs):  # type: ignore[no-untyped-def]
        super().__init__(sys.executable, **kwargs)
        self.code = code

    def command_arguments(self, request: WorkerRequest) -> Sequence[str]:
        return ("-u", "-c", self.code)


class PythonGrokAdapter(GrokAdapter):
    def __init__(self, code: str, **kwargs):  # type: ignore[no-untyped-def]
        super().__init__(sys.executable, **kwargs)
        self.code = code

    def command_arguments(self, request: WorkerRequest) -> Sequence[str]:
        return ("-u", "-c", self.code)


class PythonAgyAdapter(AgyAdapter):
    def __init__(self, code: str, **kwargs):  # type: ignore[no-untyped-def]
        super().__init__(sys.executable, **kwargs)
        self.code = code

    def command_arguments(self, request: WorkerRequest) -> Sequence[str]:
        return ("-u", "-c", self.code)


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
    assert CodexAdapter().executable == "codex"
    assert ClaudeAdapter().executable == "claude"
    assert AgyAdapter().executable == "agy"
    assert ClaudeAdapter(executable="/opt/workers/claude").executable == "/opt/workers/claude"
    assert AgyAdapter(executable="/opt/workers/agy").executable == "/opt/workers/agy"
    assert CodexAdapter(executable="/opt/workers/codex").executable == "/opt/workers/codex"


def test_local_worker_read_only_native_profiles_disable_provider_tool_surfaces(
    tmp_path: Path,
) -> None:
    request = WorkerRequest(run_id="read-only", prompt="summarize supplied context")
    claude = ClaudeAdapter(read_only=True)
    claude_arguments = list(claude.command_arguments(request))
    assert claude_arguments[claude_arguments.index("--permission-mode") + 1] == "plan"
    assert claude_arguments[claude_arguments.index("--tools") + 1] == ""
    assert {"--safe-mode", "--disable-slash-commands", "--strict-mcp-config", "--no-chrome"} <= set(
        claude_arguments
    )

    grok = GrokAdapter(read_only=True)
    grok_arguments = list(grok.command_arguments(request))
    assert grok_arguments[grok_arguments.index("--permission-mode") + 1] == "plan"
    assert grok_arguments[grok_arguments.index("--tools") + 1] == ""
    assert {"--disable-web-search", "--no-subagents", "--no-memory"} <= set(grok_arguments)

    code_write = WorkerRequest(run_id="write", prompt="edit source", code_write_required=True)
    codex = CodexAdapter(read_only=True)
    codex._result_paths["write"] = tmp_path / "codex-result"
    with pytest.raises(UnsafeWorkerRequest, match="read-only Codex"):
        codex.command_arguments(code_write)
    with pytest.raises(UnsafeWorkerRequest, match="read-only Claude"):
        claude.command_arguments(code_write)
    with pytest.raises(UnsafeWorkerRequest, match="read-only Grok"):
        grok.command_arguments(code_write)


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
    assert result.exit_code is not None
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


@pytest.mark.skipif(os.name == "nt", reason="POSIX process liveness assertion")
async def test_native_event_sink_failure_cannot_orphan_the_spawned_process() -> None:
    adapter = PythonProcessAdapter("import time;time.sleep(10)", termination_grace_seconds=0.05)
    started_pid: int | None = None

    async def broken_sink(event):  # type: ignore[no-untyped-def]
        nonlocal started_pid
        if event.kind == "processStarted":
            started_pid = int(event.payload["pid"])
            raise RuntimeError("event persistence unavailable")

    with pytest.raises(RuntimeError, match="event persistence unavailable"):
        await adapter.execute(
            WorkerRequest(run_id="sink-failure", prompt="unused"), event_sink=broken_sink
        )

    assert started_pid is not None
    with pytest.raises(ProcessLookupError):
        os.kill(started_pid, 0)


async def test_native_duplicate_run_is_rejected_before_second_process_spawn(tmp_path: Path) -> None:
    invocation_log = tmp_path / "invocations"
    code = (
        "import pathlib,time;"
        f"p=pathlib.Path({str(invocation_log)!r});"
        "p.write_text((p.read_text() if p.exists() else '')+'started\\n');"
        "print('ready',flush=True);time.sleep(10)"
    )
    adapter = PythonProcessAdapter(code, heartbeat_seconds=0.01)
    ready = asyncio.Event()
    starts = 0

    async def sink(event):  # type: ignore[no-untyped-def]
        nonlocal starts
        if event.kind == "processStarted":
            starts += 1
        if event.kind == "workerOutput":
            ready.set()

    first = asyncio.create_task(
        adapter.execute(WorkerRequest(run_id="same-run", prompt="unused"), event_sink=sink)
    )
    await asyncio.wait_for(ready.wait(), timeout=1)
    with pytest.raises(ValueError, match="run already active"):
        await adapter.execute(WorkerRequest(run_id="same-run", prompt="unused"), event_sink=sink)

    assert starts == 1
    assert invocation_log.read_text().splitlines() == ["started"]
    assert await adapter.cancel("same-run")
    assert (await first).state is RunState.CANCELLED


@pytest.mark.parametrize(
    ("adapter", "run_id"),
    [
        (PythonClaudeAdapter("print('not-json')"), "claude-malformed"),
        (
            PythonClaudeAdapter(
                "import json; print(json.dumps({'type':'assistant','message':"
                "{'content':[{'type':'text','text':'missing terminal'}]}}))"
            ),
            "claude-missing-terminal",
        ),
        (
            PythonGrokAdapter(
                "import json; print(json.dumps({'type':'text','data':'missing terminal'}))"
            ),
            "grok-missing-terminal",
        ),
        (PythonGrokAdapter("print('not-json')"), "grok-malformed"),
        (
            PythonClaudeAdapter(
                "import json;"
                "print(json.dumps({'type':'result','result':'one'}));"
                "print(json.dumps({'type':'result','result':'two'}))"
            ),
            "claude-duplicate-terminal",
        ),
        (
            PythonClaudeAdapter(
                "import json;print(json.dumps({'type':'result','result':'failed','is_error':True}))"
            ),
            "claude-error-terminal",
        ),
        (PythonAgyAdapter("pass"), "agy-empty"),
    ],
)
async def test_native_structured_protocol_failure_cannot_become_exit_zero_success(
    adapter: NativeSubprocessAdapter, run_id: str
) -> None:
    result = await adapter.execute(WorkerRequest(run_id=run_id, prompt="unused"))

    assert result.exit_code is not None
    assert result.state is RunState.FAILED
    assert result.error is not None and "RESULT_INVALID" in result.error
    assert "workerOutputRejected" in {event.kind for event in result.events}


async def test_native_output_flood_is_bounded_and_terminally_failed() -> None:
    adapter = PythonProcessAdapter(
        "print('x' * 70000)",
        max_line_bytes=1024,
        max_stdout_bytes=2048,
        max_output_lines=8,
        max_output_events=8,
        termination_grace_seconds=0.1,
    )
    result = await adapter.execute(WorkerRequest(run_id="flood", prompt="unused"))

    assert result.state is RunState.FAILED
    assert result.error is not None and "OUTPUT_LIMIT" in result.error
    assert len(result.stdout.encode()) <= 2048
    assert len([event for event in result.events if event.kind == "workerOutput"]) <= 8


@pytest.mark.parametrize(
    ("code", "limits"),
    [
        (
            "for _ in range(10): print('x' * 50)",
            {"max_stdout_bytes": 120, "max_output_lines": 100, "max_output_events": 100},
        ),
        (
            "import sys\nfor _ in range(10): print('x' * 50, file=sys.stderr)",
            {"max_stderr_bytes": 120, "max_output_lines": 100, "max_output_events": 100},
        ),
        (
            "for _ in range(5): print('x')",
            {"max_output_lines": 2, "max_output_events": 100},
        ),
        (
            "for _ in range(5): print('x')",
            {"max_output_lines": 100, "max_output_events": 2},
        ),
    ],
)
async def test_native_independent_output_fences_fail_closed(
    code: str, limits: dict[str, int]
) -> None:
    adapter = PythonProcessAdapter(
        code,
        max_line_bytes=1024,
        max_stdout_bytes=limits.get("max_stdout_bytes", 4096),
        max_stderr_bytes=limits.get("max_stderr_bytes", 4096),
        max_output_lines=limits["max_output_lines"],
        max_output_events=limits["max_output_events"],
        termination_grace_seconds=0.1,
    )
    result = await adapter.execute(
        WorkerRequest(run_id=f"bounded-{hash(code + str(limits))}", prompt="unused")
    )

    assert result.state is RunState.FAILED
    assert result.error is not None and "OUTPUT_LIMIT" in result.error


async def test_native_nonzero_exit_preserves_process_failure_instead_of_protocol_noise() -> None:
    adapter = PythonClaudeAdapter(
        "import sys;print('FAKE_NATIVE_CLI_NONZERO',file=sys.stderr);sys.exit(17)"
    )
    result = await adapter.execute(WorkerRequest(run_id="nonzero", prompt="unused"))

    assert result.state is RunState.FAILED
    assert result.exit_code == 17
    assert result.error == "FAKE_NATIVE_CLI_NONZERO"


@pytest.mark.skipif(os.name == "nt", reason="Windows process trees require Job Object acceptance")
async def test_native_cancel_terminates_the_owned_process_group(tmp_path: Path) -> None:
    orphan_marker = tmp_path / "orphan-marker"
    child = (
        "import signal,time,pathlib;"
        "signal.signal(signal.SIGTERM, signal.SIG_IGN);"
        "time.sleep(.4);"
        f"pathlib.Path({str(orphan_marker)!r}).write_text('orphan')"
    )
    parent = (
        "import subprocess,sys,time;"
        f"subprocess.Popen([sys.executable,'-c',{child!r}]);"
        "print('started', flush=True);time.sleep(10)"
    )
    adapter = PythonProcessAdapter(
        parent,
        heartbeat_seconds=0.01,
        termination_grace_seconds=0.05,
    )
    started = asyncio.Event()

    async def sink(event):  # type: ignore[no-untyped-def]
        if event.kind == "workerOutput":
            started.set()

    execution = asyncio.create_task(
        adapter.execute(WorkerRequest(run_id="process-tree", prompt="unused"), event_sink=sink)
    )
    await asyncio.wait_for(started.wait(), timeout=1)
    assert await adapter.cancel("process-tree")
    result = await asyncio.wait_for(execution, timeout=2)
    await asyncio.sleep(0.5)

    assert result.state is RunState.CANCELLED
    assert not orphan_marker.exists()


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


def test_agy_diagnostic_capture_is_bounded(tmp_path: Path) -> None:
    adapter = AgyAdapter(executable=sys.executable, max_diagnostic_bytes=32)
    diagnostic = tmp_path / "agy.log"
    diagnostic.write_bytes(b"x" * 33)
    adapter._diagnostic_paths["agy-overflow"] = str(diagnostic)

    with pytest.raises(WorkerProtocolError, match="capture limit"):
        adapter.finalize_output(
            WorkerRequest(run_id="agy-overflow", prompt="read"),
            ParsedOutput(),
            "result",
            "",
        )


def _codex_transcript(result: str) -> list[dict[str, object]]:
    return [
        {"type": "thread.started", "thread_id": "codex-thread"},
        {"type": "turn.started"},
        {
            "type": "item.completed",
            "item": {"id": "message-1", "type": "agent_message", "text": result},
        },
        {
            "type": "turn.completed",
            "usage": {
                "input_tokens": 13,
                "cached_input_tokens": 2,
                "cache_write_input_tokens": 1,
                "output_tokens": 7,
                "reasoning_output_tokens": 3,
            },
        },
    ]


def test_codex_parser_requires_reviewed_jsonl_and_private_terminal_file(tmp_path: Path) -> None:
    adapter = CodexAdapter(executable=sys.executable)
    request = WorkerRequest(run_id="codex-parser", prompt="read")
    result_path = tmp_path / "last-message.txt"
    result_path.write_text("CODEX_NATIVE_OK", encoding="utf-8")
    result_path.chmod(0o600)
    adapter._result_paths[request.run_id] = result_path
    parsed = ParsedOutput()

    for event in _codex_transcript("CODEX_NATIVE_OK"):
        adapter.consume_stdout_line(json.dumps(event), parsed)
    adapter.finalize_output(request, parsed, "", "")
    adapter.validate_output(request, parsed, "", "")

    assert parsed.final_text == "CODEX_NATIVE_OK"
    assert parsed.session_id == "codex-thread"
    assert parsed.model is None
    assert parsed.usage.input_tokens == 13
    assert parsed.usage.cache_read_tokens == 2
    assert parsed.usage.cache_creation_tokens == 1
    assert parsed.usage.reasoning_tokens == 3
    assert parsed.usage.total_tokens == 20
    assert parsed.usage.raw["accountSource"] == "unknown"


@pytest.mark.parametrize(
    "mutation",
    ["missing-thread", "missing-turn-started", "missing-terminal", "duplicate-message", "mismatch"],
)
def test_codex_parser_fails_closed_on_incomplete_or_disagreeing_transcript(
    tmp_path: Path, mutation: str
) -> None:
    adapter = CodexAdapter(executable=sys.executable)
    request = WorkerRequest(run_id=f"codex-{mutation}", prompt="read")
    result_path = tmp_path / f"{mutation}.txt"
    result_path.write_text("CODEX_NATIVE_OK", encoding="utf-8")
    result_path.chmod(0o600)
    adapter._result_paths[request.run_id] = result_path
    transcript = _codex_transcript("DIFFERENT" if mutation == "mismatch" else "CODEX_NATIVE_OK")
    if mutation == "missing-thread":
        transcript.pop(0)
    elif mutation == "missing-turn-started":
        transcript.pop(1)
    elif mutation == "missing-terminal":
        transcript.pop()
    elif mutation == "duplicate-message":
        transcript.insert(3, transcript[2])
    parsed = ParsedOutput()
    for event in transcript:
        adapter.consume_stdout_line(json.dumps(event), parsed)
    adapter.finalize_output(request, parsed, "", "")

    with pytest.raises(WorkerProtocolError, match="Codex"):
        adapter.validate_output(request, parsed, "", "")


def test_codex_structured_result_is_schema_validated(tmp_path: Path) -> None:
    adapter = CodexAdapter(executable=sys.executable)
    request = WorkerRequest(run_id="codex-schema", prompt="read")
    result_path = tmp_path / "structured.txt"
    result_path.write_text('{"answer":"yes"}', encoding="utf-8")
    result_path.chmod(0o600)
    adapter._result_paths[request.run_id] = result_path
    adapter._schemas[request.run_id] = {
        "type": "object",
        "properties": {"answer": {"const": "no"}},
        "required": ["answer"],
        "additionalProperties": False,
    }

    with pytest.raises(WorkerProtocolError, match="structured result validation"):
        adapter.finalize_output(request, ParsedOutput(), "", "")


@pytest.mark.parametrize(
    "schema",
    [
        {"$dynamicRef": "https://schema.invalid/remote"},
        {"$recursiveRef": "#"},
        {"type": "string", "pattern": "^(a+)+$"},
        {"type": "object", "patternProperties": {"^x-": {"type": "string"}}},
    ],
    ids=("dynamic-ref", "recursive-ref", "pattern", "pattern-properties"),
)
async def test_codex_rejects_unsafe_schema_keywords_before_process_launch(
    monkeypatch: pytest.MonkeyPatch,
    schema: Mapping[str, object],
) -> None:
    process_started = False

    async def unexpected_process(*args, **kwargs):  # type: ignore[no-untyped-def]
        nonlocal process_started
        process_started = True
        raise AssertionError("unsafe schema reached the process boundary")

    monkeypatch.setattr(asyncio, "create_subprocess_exec", unexpected_process)
    adapter = CodexAdapter(executable=sys.executable)
    request = WorkerRequest(
        run_id="codex-unsafe-schema",
        prompt="read",
        metadata={"response_schema": schema},
    )

    with pytest.raises(WorkerProtocolError, match="Codex response_schema"):
        await adapter.execute(request)

    assert not process_started


def test_codex_structured_validation_preserves_bounded_local_refs(tmp_path: Path) -> None:
    adapter = CodexAdapter(executable=sys.executable)
    request = WorkerRequest(run_id="codex-local-ref", prompt="read")
    result_path = tmp_path / "local-ref.txt"
    result_path.write_text('{"answer":"yes"}', encoding="utf-8")
    result_path.chmod(0o600)
    adapter._result_paths[request.run_id] = result_path
    adapter._schemas[request.run_id] = {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "$defs": {"answer": {"type": "string", "const": "yes"}},
        "type": "object",
        "properties": {"answer": {"$ref": "#/$defs/answer"}},
        "required": ["answer"],
        "additionalProperties": False,
    }

    parsed = ParsedOutput()
    adapter.finalize_output(request, parsed, "", "")

    assert parsed.final_text == '{"answer":"yes"}'


def test_codex_structured_validation_never_fetches_remote_references(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    fetches: list[object] = []

    def unexpected_fetch(*args, **kwargs):  # type: ignore[no-untyped-def]
        fetches.append(args[0] if args else None)
        raise AssertionError("schema validation attempted network access")

    monkeypatch.setattr(urllib.request, "urlopen", unexpected_fetch)
    adapter = CodexAdapter(executable=sys.executable)
    request = WorkerRequest(run_id="codex-offline-schema", prompt="read")
    result_path = tmp_path / "offline-schema.txt"
    result_path.write_text("{}", encoding="utf-8")
    result_path.chmod(0o600)
    adapter._result_paths[request.run_id] = result_path
    # Install directly to exercise the validator's independent no-network backstop even if a
    # future admission-path regression allows a remote reference through.
    adapter._schemas[request.run_id] = {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "$dynamicRef": "https://schema.invalid/remote",
    }

    with pytest.raises(WorkerProtocolError, match="structured result validation"):
        adapter.finalize_output(request, ParsedOutput(), "", "")

    assert fetches == []


async def test_mock_adapter_is_deterministic() -> None:
    adapter = MockAdapter(MockBehavior(text="FIXED", session_id="session"))
    request = WorkerRequest(run_id="mock-1", prompt="anything")
    first = await adapter.execute(request)
    second = await adapter.execute(request)
    assert first.final_text == second.final_text == "FIXED"
    assert first.session_id == second.session_id == "session:mock-1"
