#!/usr/bin/env python3
"""Deterministic offline executable for native Worker adapter acceptance.

The fixture recognizes the exact command-line dialects emitted by the Claude,
Grok, AGY, and Codex adapters.  It never opens a network connection and its behavior
is controlled only by explicitly injected ``CHIPS_FAKE_CLI_*`` environment
variables.  Production requests cannot select a scenario or executable.
"""

from __future__ import annotations

import hashlib
import json
import os
import signal
import sys
import time
import uuid
from contextlib import suppress
from pathlib import Path
from typing import Any

_CONTROL_ENV = "CHIPS_FAKE_CLI_CONTROL_DIR"
_SCENARIO_ENV = "CHIPS_FAKE_CLI_SCENARIO"
_RESULT_ENV = "CHIPS_FAKE_CLI_RESULT"
_DELAY_ENV = "CHIPS_FAKE_CLI_DELAY_SECONDS"
_FLOOD_BYTES_ENV = "CHIPS_FAKE_CLI_FLOOD_BYTES"
_CREDENTIAL_MARKERS = (
    "API_KEY",
    "AUTHORIZATION",
    "BEARER",
    "CREDENTIAL",
    "PASSWORD",
    "SECRET",
    "TOKEN",
)


def _atomic_json(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    temporary = path.with_name(f".{path.name}.{uuid.uuid4()}.tmp")
    descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8", closefd=True) as stream:
            json.dump(value, stream, ensure_ascii=False, separators=(",", ":"), sort_keys=True)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
    finally:
        with suppress(FileNotFoundError):
            temporary.unlink()


def _append_json(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o600)
    try:
        encoded = (json.dumps(value, separators=(",", ":"), sort_keys=True) + "\n").encode()
        os.write(descriptor, encoded)
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _argument(args: list[str], name: str) -> str | None:
    try:
        index = args.index(name)
    except ValueError:
        return None
    return args[index + 1] if index + 1 < len(args) else None


def _detect_profile(args: list[str]) -> str:
    if args[:1] == ["exec"] and "--json" in args:
        return "codex"
    output_format = _argument(args, "--output-format")
    if output_format == "stream-json" and "--verbose" in args:
        return "claude"
    if output_format == "streaming-json":
        return "grok"
    if (
        _argument(args, "--mode") == "plan"
        and "--sandbox" in args
        and _argument(args, "--log-file") is not None
    ):
        return "agy"
    raise ValueError("unrecognized native Worker CLI dialect")


def _validate_arguments(profile: str, args: list[str]) -> tuple[str, str | None, str | None]:
    if profile == "codex":
        required = {
            "--json",
            "--skip-git-repo-check",
            "--ephemeral",
            "--ignore-user-config",
            "--strict-config",
        }
        if args[:1] != ["exec"] or args[-1:] != ["-"] or not required <= set(args):
            raise ValueError("invalid Codex non-interactive arguments")
        if _argument(args, "--color") != "never":
            raise ValueError("Codex acceptance requires color-free JSONL")
        if _argument(args, "--sandbox") not in {"read-only", "workspace-write"}:
            raise ValueError("Codex acceptance requires an explicit bounded sandbox")
        if _argument(args, "--ask-for-approval") != "never":
            raise ValueError("Codex acceptance requires non-interactive approval policy")
        if {
            "--dangerously-bypass-approvals-and-sandbox",
            "--full-auto",
            "--search",
            "--add-dir",
            "--cd",
            "-C",
        } & set(args):
            raise ValueError("Codex acceptance received an unsafe capability-expanding flag")
        result_path = _argument(args, "--output-last-message")
        if result_path is None:
            raise ValueError("Codex acceptance requires a terminal result file")
        raw_prompt = sys.stdin.buffer.read(1024 * 1024 + 1)
        if not raw_prompt or len(raw_prompt) > 1024 * 1024:
            raise ValueError("missing or oversized Codex stdin prompt")
        try:
            prompt = raw_prompt.decode("utf-8")
        except UnicodeError as error:
            raise ValueError("Codex stdin prompt must be UTF-8") from error
        if not prompt:
            raise ValueError("missing Codex stdin prompt")
        return prompt, None, result_path

    prompt = _argument(args, "-p")
    if prompt is None or not prompt:
        raise ValueError("missing non-empty -p prompt")
    diagnostic_path: str | None = None
    if profile == "claude":
        if _argument(args, "--output-format") != "stream-json" or "--verbose" not in args:
            raise ValueError("invalid Claude stream-json arguments")
    elif profile == "grok":
        if _argument(args, "--output-format") != "streaming-json":
            raise ValueError("invalid Grok streaming-json arguments")
        if "--resume" in args and _argument(args, "--session-id") is None:
            raise ValueError("Grok --resume requires --session-id")
    else:
        diagnostic_path = _argument(args, "--log-file")
        if diagnostic_path is None or _argument(args, "--mode") != "plan":
            raise ValueError("invalid AGY plan-mode arguments")
        if "--model" in args:
            raise ValueError("AGY acceptance must not receive a model override")
    return prompt, diagnostic_path, None


def _credential_environment_keys() -> list[str]:
    return sorted(
        key
        for key in os.environ
        if not key.startswith("CHIPS_FAKE_CLI_")
        and any(marker in key.upper() for marker in _CREDENTIAL_MARKERS)
    )


def _write_started(
    control: Path,
    *,
    profile: str,
    scenario: str,
    prompt: str,
    args: list[str],
) -> None:
    record = {
        "pid": os.getpid(),
        "profile": profile,
        "scenario": scenario,
        "argv": args,
        "prompt_sha256": hashlib.sha256(prompt.encode()).hexdigest(),
        "credential_environment_keys": _credential_environment_keys(),
    }
    _append_json(control / "invocations.jsonl", record)
    _atomic_json(control / "started.json", record)


def _write_cancelled(control: Path, *, profile: str, scenario: str) -> None:
    _atomic_json(
        control / "cancelled.json",
        {"pid": os.getpid(), "profile": profile, "scenario": scenario},
    )


def _wait(control: Path, *, scenario: str, cancelled: list[bool]) -> bool:
    try:
        delay = float(os.environ.get(_DELAY_ENV, "0.05"))
    except ValueError:
        delay = 0.05
    deadline = time.monotonic() + max(0.0, delay)
    release = control / "release"
    while not cancelled[0]:
        if scenario in {"block", "cancel"}:
            if release.is_file():
                return True
        elif time.monotonic() >= deadline:
            return True
        time.sleep(0.01)
    return False


def _emit_claude(result: str) -> None:
    session = f"fake-claude-session-{os.getpid()}"
    usage = {"input_tokens": 7, "output_tokens": 11, "cache_read_input_tokens": 0}
    messages = (
        {"type": "system", "subtype": "init", "session_id": session},
        {
            "type": "assistant",
            "session_id": session,
            "message": {
                "model": "claude-offline-fixture",
                "content": [{"type": "text", "text": result}],
                "usage": usage,
            },
        },
        {
            "type": "result",
            "subtype": "success",
            "session_id": session,
            "result": result,
            "total_cost_usd": 0.0,
            "usage": usage,
        },
    )
    for message in messages:
        print(json.dumps(message, separators=(",", ":")), flush=True)


def _emit_grok(result: str) -> None:
    session = f"fake-grok-session-{os.getpid()}"
    split = max(1, len(result) // 2)
    messages = (
        {"type": "text", "data": result[:split]},
        {"type": "text", "data": result[split:]},
        {
            "type": "end",
            "sessionId": session,
            "total_cost_usd": 0.0,
            "modelUsage": {
                "grok-offline-fixture": {
                    "inputTokens": 5,
                    "outputTokens": 9,
                    "totalTokens": 14,
                }
            },
        },
    )
    for message in messages:
        print(json.dumps(message, separators=(",", ":")), flush=True)


def _emit_agy(result: str, diagnostic_path: str) -> None:
    diagnostic = Path(diagnostic_path)
    diagnostic.parent.mkdir(parents=True, exist_ok=True)
    diagnostic.write_text(
        "Created conversation 11111111-2222-3333-4444-555555555555\n"
        'selected model override to backend: label="AGY Offline Fixture"\n',
        encoding="utf-8",
    )
    print(result, flush=True)


def _write_codex_result(result: str, result_path: str) -> None:
    path = Path(result_path)
    # The production adapter pre-creates this private file. Refuse to create an arbitrary target
    # so tests exercise that exact ownership boundary.
    if not path.is_file() or path.is_symlink():
        raise ValueError("Codex terminal result target was not pre-created")
    path.write_text(result, encoding="utf-8")


def _emit_codex(result: str, result_path: str) -> None:
    _write_codex_result(result, result_path)
    session = f"fake-codex-thread-{os.getpid()}"
    messages = (
        {"type": "thread.started", "thread_id": session},
        {"type": "turn.started"},
        {
            "type": "item.completed",
            "item": {
                "id": "fake-codex-agent-message",
                "type": "agent_message",
                "text": result,
            },
        },
        {
            "type": "turn.completed",
            "usage": {
                "input_tokens": 13,
                "cached_input_tokens": 2,
                "cache_write_input_tokens": 0,
                "output_tokens": 7,
                "reasoning_output_tokens": 3,
            },
        },
    )
    for message in messages:
        print(json.dumps(message, separators=(",", ":")), flush=True)


def _emit_flood(*, stderr: bool) -> None:
    try:
        count = int(os.environ.get(_FLOOD_BYTES_ENV, "2097152"))
    except ValueError:
        count = 2_097_152
    stream = sys.stderr.buffer if stderr else sys.stdout.buffer
    stream.write(b"X" * max(1, min(count, 16_777_216)))
    stream.flush()


def main() -> int:
    executable = Path(sys.argv[0]).resolve()
    control = Path(os.environ.get(_CONTROL_ENV, executable.parent)).resolve()
    control.mkdir(parents=True, exist_ok=True, mode=0o700)
    args = sys.argv[1:]
    inferred_scenario = (
        executable.name.rsplit("--", maxsplit=1)[1] if "--" in executable.name else "normal"
    )
    scenario = os.environ.get(_SCENARIO_ENV, inferred_scenario).strip().lower()
    result = os.environ.get(_RESULT_ENV, "NATIVE_CLI_OFFLINE_OK")
    try:
        profile = _detect_profile(args)
        prompt, diagnostic_path, result_path = _validate_arguments(profile, args)
    except ValueError as error:
        print(str(error), file=sys.stderr, flush=True)
        return 64

    cancelled = [False]

    def handle_cancel(_signal: int, _frame: object) -> None:
        cancelled[0] = True

    signal.signal(signal.SIGTERM, handle_cancel)
    if hasattr(signal, "SIGINT"):
        signal.signal(signal.SIGINT, handle_cancel)
    _write_started(control, profile=profile, scenario=scenario, prompt=prompt, args=args)

    if not _wait(control, scenario=scenario, cancelled=cancelled):
        _write_cancelled(control, profile=profile, scenario=scenario)
        return 143
    if scenario == "nonzero":
        print("FAKE_NATIVE_CLI_NONZERO", file=sys.stderr, flush=True)
        return 17
    if scenario == "auth-required":
        print(
            f"Not logged in. Run codex login. token={result}",
            file=sys.stderr,
            flush=True,
        )
        return 1
    if scenario == "malformed":
        if profile == "agy":
            return 0
        if profile == "codex":
            assert result_path is not None
            _write_codex_result(result, result_path)
        print('{"type":"result","unterminated":', flush=True)
        return 0
    if scenario == "wrong-dialect":
        if profile == "codex":
            assert result_path is not None
            _write_codex_result(result, result_path)
            _emit_claude(result)
        else:
            _emit_grok(result) if profile == "claude" else _emit_claude(result)
        return 0
    if scenario in {"flood", "flood-stdout"}:
        _emit_flood(stderr=False)
        return 0
    if scenario == "flood-stderr":
        _emit_flood(stderr=True)
        return 0
    if scenario == "control-flood":
        control_result = "\x00" * 40_000
        if profile == "claude":
            _emit_claude(control_result)
        elif profile == "grok":
            _emit_grok(control_result)
        elif profile == "codex":
            assert result_path is not None
            _emit_codex(control_result, result_path)
        else:
            assert diagnostic_path is not None
            _emit_agy(control_result, diagnostic_path)
        return 0
    if scenario not in {"normal", "block", "cancel"}:
        print(f"unsupported fixture scenario: {scenario}", file=sys.stderr, flush=True)
        return 64

    if profile == "claude":
        _emit_claude(result)
    elif profile == "grok":
        _emit_grok(result)
    elif profile == "codex":
        assert result_path is not None
        _emit_codex(result, result_path)
    else:
        assert diagnostic_path is not None
        _emit_agy(result, diagnostic_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
