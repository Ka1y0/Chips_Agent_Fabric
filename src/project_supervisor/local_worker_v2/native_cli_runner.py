from __future__ import annotations

import argparse
import asyncio
import json
import math
import os
import signal
import stat
import sys
from collections.abc import Mapping
from contextlib import suppress
from pathlib import Path
from typing import Any

from project_supervisor.adapters import AgyAdapter, ClaudeAdapter, CodexAdapter, GrokAdapter
from project_supervisor.adapters.base import WorkerAdapter, WorkerRequest, WorkerResult
from project_supervisor.adapters.native import redact
from project_supervisor.domain import RunState

from .drivers import DriverProfile, DriverProfileError, DriverType
from .model import PROTOCOL_VERSION, LaunchState
from .process import (
    acquire_job_lock,
    atomic_write_json,
    process_birth_identity,
    read_json_receipt,
    release_job_lock,
)

_SPEC_SCHEMA = "local-worker-native-execution/v1"
_MAX_SPEC_BYTES = 256 * 1024
_MAX_PROMPT_BYTES = 128 * 1024
# Keep the complete terminal receipt comfortably below process.read_json_receipt's 64 KiB cap.
_MAX_RESULT_BYTES = 40 * 1024
_MAX_TERMINAL_BYTES = 60 * 1024
_SPEC_FIELDS = frozenset({"schema_version", "launch", "driver", "request"})
_LAUNCH_FIELDS = frozenset(
    {
        "protocol_version",
        "job_id",
        "idempotency_key",
        "launch_nonce",
        "request_digest",
        "process_host_identity",
        "launch_runtime_instance_id",
    }
)
_DRIVER_FIELDS = frozenset(
    {
        "driver_id",
        "driver_type",
        "profile_revision",
        "profile_fingerprint",
        "executable",
        "executable_sha256",
        "max_execution_seconds",
    }
)
_REQUEST_FIELDS = frozenset(
    {"run_id", "task_id", "job_type", "role", "prompt", "schema", "timeout_seconds"}
)


class InvalidExecutionSpec(ValueError):
    pass


def _bounded_text(value: object, maximum_bytes: int) -> str:
    text = str(value or "")
    encoded = text.encode("utf-8", errors="replace")
    if len(encoded) <= maximum_bytes:
        return text
    suffix = b"\n[TRUNCATED]"
    return (encoded[: maximum_bytes - len(suffix)] + suffix).decode("utf-8", errors="ignore")


def _object(value: object, fields: frozenset[str], name: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping) or set(value) != fields:
        raise InvalidExecutionSpec(f"{name} has unknown or missing fields")
    return value


def _identity(value: object, name: str, *, maximum: int = 256) -> str:
    if not isinstance(value, str) or not value.strip() or len(value) > maximum:
        raise InvalidExecutionSpec(f"{name} must be a bounded non-empty string")
    return value


def _read_spec() -> tuple[Mapping[str, Any], Mapping[str, Any], Mapping[str, Any]]:
    raw = sys.stdin.buffer.read(_MAX_SPEC_BYTES + 1)
    if not raw or len(raw) > _MAX_SPEC_BYTES:
        raise InvalidExecutionSpec("execution spec is empty or exceeds 256 KiB")
    try:
        value = json.loads(raw.decode("utf-8"))
    except (UnicodeError, ValueError) as error:
        raise InvalidExecutionSpec("execution spec must be UTF-8 JSON") from error
    root = _object(value, _SPEC_FIELDS, "execution spec")
    if root["schema_version"] != _SPEC_SCHEMA:
        raise InvalidExecutionSpec("execution spec schema is unsupported")
    launch = _object(root["launch"], _LAUNCH_FIELDS, "launch identity")
    driver = _object(root["driver"], _DRIVER_FIELDS, "driver profile")
    request = _object(root["request"], _REQUEST_FIELDS, "worker request")
    if launch["protocol_version"] != PROTOCOL_VERSION:
        raise InvalidExecutionSpec("launch protocol version is unsupported")
    for field in _LAUNCH_FIELDS - {"protocol_version"}:
        _identity(launch[field], f"launch.{field}")

    try:
        profile = DriverProfile.from_mapping(
            {
                "driver_id": driver["driver_id"],
                "driver_type": driver["driver_type"],
                "profile_revision": driver["profile_revision"],
                "executable": driver["executable"],
                "max_execution_seconds": driver["max_execution_seconds"],
            }
        )
    except (DriverProfileError, TypeError, ValueError) as error:
        raise InvalidExecutionSpec("native driver profile is invalid") from error
    if profile.driver_type not in {
        DriverType.CLAUDE,
        DriverType.GROK,
        DriverType.AGY,
        DriverType.CODEX,
    }:
        raise InvalidExecutionSpec("native runner received a non-native driver")
    if (
        profile.profile_fingerprint != driver["profile_fingerprint"]
        or profile.executable_sha256 != driver["executable_sha256"]
    ):
        raise InvalidExecutionSpec("native driver executable/profile identity changed")
    assert profile.executable is not None
    try:
        metadata = profile.executable.stat()
    except OSError as error:
        raise InvalidExecutionSpec("native driver executable is unavailable") from error
    if not stat.S_ISREG(metadata.st_mode) or not os.access(profile.executable, os.X_OK):
        raise InvalidExecutionSpec("native driver executable is not executable")

    maximum = driver["max_execution_seconds"]
    timeout = request["timeout_seconds"]
    if (
        not isinstance(maximum, (int, float))
        or isinstance(maximum, bool)
        or not 1 <= float(maximum) <= 3600
        or not isinstance(timeout, (int, float))
        or isinstance(timeout, bool)
        or not 0 < float(timeout) <= float(maximum)
    ):
        raise InvalidExecutionSpec("worker timeout exceeds its driver profile")
    _identity(request["run_id"], "request.run_id")
    if request["task_id"] is not None:
        _identity(request["task_id"], "request.task_id")
    if request["job_type"] not in {"inference.chat", "inference.structured"}:
        raise InvalidExecutionSpec("worker job_type is unsupported")
    prompt = _identity(request["prompt"], "request.prompt", maximum=_MAX_PROMPT_BYTES)
    if len(prompt.encode("utf-8")) > _MAX_PROMPT_BYTES:
        raise InvalidExecutionSpec("worker prompt exceeds 128 KiB")
    _identity(request["role"], "request.role", maximum=64)
    schema = request["schema"]
    if request["job_type"] == "inference.structured":
        if not isinstance(schema, Mapping):
            raise InvalidExecutionSpec("structured worker request requires an object schema")
    elif schema is not None:
        raise InvalidExecutionSpec("chat worker request must not include a schema")
    return launch, driver, request


def _launch_matches_spool(
    launch: Mapping[str, Any], driver: Mapping[str, Any], spool: Mapping[str, Any]
) -> bool:
    launch_mapping = {
        key: launch[key]
        for key in (
            "protocol_version",
            "job_id",
            "idempotency_key",
            "launch_nonce",
            "request_digest",
            "process_host_identity",
            "launch_runtime_instance_id",
        )
    }
    driver_mapping = {
        "driver_id": driver["driver_id"],
        "driver_type": driver["driver_type"],
        "driver_profile_revision": driver["profile_revision"],
        "driver_profile_fingerprint": driver["profile_fingerprint"],
        "driver_executable_sha256": driver["executable_sha256"],
    }
    expected = {**launch_mapping, **driver_mapping}
    return all(spool.get(key) == value for key, value in expected.items())


def _adapter(driver_type: DriverType, executable: str) -> WorkerAdapter:
    if driver_type is DriverType.CLAUDE:
        return ClaudeAdapter(executable, read_only=True)
    if driver_type is DriverType.GROK:
        return GrokAdapter(executable, read_only=True)
    if driver_type is DriverType.AGY:
        return AgyAdapter(executable)
    if driver_type is DriverType.CODEX:
        return CodexAdapter(executable, read_only=True)
    raise InvalidExecutionSpec("native driver type has no adapter")


def _usage(result: WorkerResult) -> dict[str, int | float]:
    values = {
        "input_tokens": result.usage.input_tokens,
        "output_tokens": result.usage.output_tokens,
        "cache_creation_tokens": result.usage.cache_creation_tokens,
        "cache_read_tokens": result.usage.cache_read_tokens,
        "reasoning_tokens": result.usage.reasoning_tokens,
        "total_tokens": result.usage.total_tokens,
        "cost_usd": result.usage.cost_usd,
    }
    projected: dict[str, int | float] = {}
    for key, value in values.items():
        if value is None or isinstance(value, bool):
            continue
        if isinstance(value, float) and not math.isfinite(value):
            continue
        projected[key] = value
    return projected


def _validate_provider_result(driver_type: DriverType, result: WorkerResult) -> None:
    if result.state is not RunState.COMPLETED or result.exit_code != 0:
        return
    provider_types: set[str] = set()
    malformed = False
    for event in result.events:
        if event.kind != "workerOutput":
            continue
        malformed = malformed or event.payload.get("malformed") is True
        event_type = event.payload.get("providerEventType")
        if isinstance(event_type, str) and event_type != "blank":
            provider_types.add(event_type)
    if malformed:
        raise RuntimeError("native provider emitted malformed structured output")
    if driver_type is DriverType.CLAUDE:
        if not provider_types or not provider_types <= {"system", "assistant", "result"}:
            raise RuntimeError("native Claude output dialect is incompatible")
        if "result" not in provider_types:
            raise RuntimeError("native Claude output omitted its terminal result")
    elif driver_type is DriverType.GROK:
        if not provider_types or not provider_types <= {"text", "result", "end", "completed"}:
            raise RuntimeError("native Grok output dialect is incompatible")
        if not provider_types.intersection({"result", "end", "completed"}):
            raise RuntimeError("native Grok output omitted its terminal result")
    elif driver_type is DriverType.CODEX:
        if not result.final_text.strip():
            raise RuntimeError("native Codex output was empty")
        if any(
            event.payload.get("providerEventType") in {"turn.failed", "error"}
            for event in result.events
            if event.kind == "workerOutput"
        ):
            raise RuntimeError("native Codex output reported a terminal error")
    elif not result.final_text.strip():
        raise RuntimeError("native AGY output was empty")


def _result_projection(result: WorkerResult) -> tuple[LaunchState, dict[str, Any], int]:
    if result.state is RunState.COMPLETED and result.exit_code == 0:
        launch_state = LaunchState.COMPLETED
        provider_state = "SUCCEEDED"
        runner_exit = 0
    elif result.state is RunState.CANCELLED:
        launch_state = LaunchState.CANCELLED
        provider_state = "CANCELLED"
        runner_exit = 143
    else:
        launch_state = LaunchState.FAILED
        provider_state = "FAILED"
        runner_exit = result.exit_code if result.exit_code not in {None, 0} else 1
    content = _bounded_text(redact(result.final_text), _MAX_RESULT_BYTES)
    projected: dict[str, Any] = {
        "state": provider_state,
        "result": {"content": content},
        "metrics": _usage(result),
    }
    if result.model:
        projected["selected_model"] = _bounded_text(redact(result.model), 512)
        projected["result"]["model"] = projected["selected_model"]
    if result.session_id:
        projected["session_id"] = _bounded_text(redact(result.session_id), 1024)
    if launch_state is not LaunchState.COMPLETED:
        if result.state is RunState.CANCELLED:
            failure_code = "PROVIDER_CANCELLED"
        elif result.state is RunState.TIMED_OUT:
            failure_code = "PROVIDER_TIMEOUT"
        elif result.state is RunState.AUTH_REQUIRED:
            failure_code = "PROVIDER_AUTH_REQUIRED"
        elif result.state is RunState.RATE_LIMITED:
            failure_code = "PROVIDER_RATE_LIMITED"
        elif isinstance(result.error, str) and result.error.startswith("OUTPUT_LIMIT:"):
            failure_code = "PROVIDER_OUTPUT_LIMIT"
        elif isinstance(result.error, str) and result.error.startswith("RESULT_INVALID:"):
            failure_code = "PROVIDER_RESULT_INVALID"
        elif result.exit_code not in {None, 0}:
            failure_code = "PROVIDER_EXIT_NONZERO"
        else:
            failure_code = "PROVIDER_EXECUTION_FAILED"
        projected["error"] = failure_code
    return launch_state, projected, runner_exit


async def _force_cleanup(
    adapter: WorkerAdapter,
    execution: asyncio.Task[WorkerResult],
    run_id: str,
) -> None:
    if not execution.done():
        with suppress(BaseException):
            await asyncio.wait_for(adapter.cancel(run_id), timeout=2.0)
        execution.cancel()
    await asyncio.gather(execution, return_exceptions=True)


async def _execute(
    adapter: WorkerAdapter,
    request: WorkerRequest,
    cancel_requested: asyncio.Event,
    force_requested: asyncio.Event,
) -> WorkerResult:
    if cancel_requested.is_set():
        raise RuntimeError("native job was cancelled before provider launch")
    execution = asyncio.create_task(adapter.execute(request))
    cancellation = asyncio.create_task(cancel_requested.wait())
    force = asyncio.create_task(force_requested.wait())
    try:
        done, _pending = await asyncio.wait(
            {execution, cancellation}, return_when=asyncio.FIRST_COMPLETED
        )
        if execution in done:
            return await execution

        adapter_cancel = asyncio.create_task(adapter.cancel(request.run_id))
        cancel_done, _pending = await asyncio.wait(
            {adapter_cancel, force}, return_when=asyncio.FIRST_COMPLETED, timeout=5.0
        )
        if force in cancel_done or adapter_cancel not in cancel_done:
            adapter_cancel.cancel()
            await asyncio.gather(adapter_cancel, return_exceptions=True)
            await _force_cleanup(adapter, execution, request.run_id)
            raise RuntimeError("native adapter cancellation was forcibly terminated")
        try:
            await adapter_cancel
        except BaseException as error:
            await _force_cleanup(adapter, execution, request.run_id)
            raise RuntimeError("native adapter cancellation failed") from error

        completed, _pending = await asyncio.wait(
            {execution, force}, return_when=asyncio.FIRST_COMPLETED, timeout=10.0
        )
        if execution in completed:
            return await execution
        await _force_cleanup(adapter, execution, request.run_id)
        raise RuntimeError("native adapter did not stop after cancellation")
    except BaseException:
        await _force_cleanup(adapter, execution, request.run_id)
        raise
    finally:
        cancellation.cancel()
        force.cancel()
        await asyncio.gather(cancellation, force, return_exceptions=True)


async def run(job_dir: Path) -> int:
    launch, driver, request_value = _read_spec()
    launch_spool = read_json_receipt(job_dir / "launch.json")
    if launch_spool is None or not _launch_matches_spool(launch, driver, launch_spool):
        raise InvalidExecutionSpec("anonymous execution spec does not match durable identity")

    nonce = str(launch["launch_nonce"])
    lock_descriptor = acquire_job_lock(job_dir / f"alive-{nonce}.lock")
    pid = os.getpid()
    birth = process_birth_identity(pid)
    if birth is None:
        release_job_lock(lock_descriptor)
        raise RuntimeError("process birth identity is unavailable")
    base = {
        "protocol_version": PROTOCOL_VERSION,
        "job_id": launch["job_id"],
        "idempotency_key": launch["idempotency_key"],
        "launch_nonce": nonce,
        "request_digest": launch["request_digest"],
        "process_host_identity": launch["process_host_identity"],
        "launch_runtime_instance_id": launch["launch_runtime_instance_id"],
        "driver_id": driver["driver_id"],
        "driver_type": driver["driver_type"],
        "driver_profile_revision": driver["profile_revision"],
        "driver_profile_fingerprint": driver["profile_fingerprint"],
        "driver_executable_sha256": driver["executable_sha256"],
        "pid": pid,
        "process_birth_identity": birth,
    }
    try:
        loop = asyncio.get_running_loop()
        cancelled = asyncio.Event()
        force_cancelled = asyncio.Event()
        signals_seen = 0

        def request_cancel(_signum: int, _frame: object) -> None:
            nonlocal signals_seen
            signals_seen += 1
            loop.call_soon_threadsafe(cancelled.set)
            if signals_seen > 1:
                loop.call_soon_threadsafe(force_cancelled.set)

        signal.signal(signal.SIGTERM, request_cancel)
        if hasattr(signal, "SIGINT"):
            signal.signal(signal.SIGINT, request_cancel)
        # Publish RUNNING only after cancellation can be handled cooperatively.
        atomic_write_json(job_dir / "started.json", base)

        driver_type = DriverType(str(driver["driver_type"]))
        adapter = _adapter(driver_type, str(driver["executable"]))
        schema = request_value["schema"]
        metadata: dict[str, Any] = {"worker_role": str(request_value["role"])}
        if isinstance(schema, Mapping):
            metadata["response_schema"] = dict(schema)
        request = WorkerRequest(
            run_id=str(request_value["run_id"]),
            task_id=(str(request_value["task_id"]) if request_value["task_id"] else None),
            prompt=str(request_value["prompt"]),
            timeout_seconds=float(request_value["timeout_seconds"]),
            code_write_required=False,
            metadata=metadata,
        )
        try:
            worker_result = await _execute(adapter, request, cancelled, force_cancelled)
            _validate_provider_result(driver_type, worker_result)
            terminal_state, result, exit_code = _result_projection(worker_result)
        except BaseException:
            terminal_state = LaunchState.CANCELLED if cancelled.is_set() else LaunchState.FAILED
            result = {
                "state": "CANCELLED" if cancelled.is_set() else "FAILED",
                "result": {"content": ""},
                "metrics": {},
                "error": (
                    "PROVIDER_CANCELLED" if cancelled.is_set() else "PROVIDER_RESULT_INVALID"
                ),
            }
            exit_code = 143 if cancelled.is_set() else 1
        terminal = {
            **base,
            "launch_state": terminal_state.value,
            "exit_code": exit_code,
            "result": result,
        }
        encoded = json.dumps(
            terminal, ensure_ascii=False, separators=(",", ":"), sort_keys=True
        ).encode("utf-8")
        if len(encoded) > _MAX_TERMINAL_BYTES:
            terminal["launch_state"] = LaunchState.FAILED.value
            terminal["exit_code"] = 1
            terminal["result"] = {
                "state": "FAILED",
                "result": {"content": ""},
                "metrics": {},
                "error": "native result projection exceeded the durable receipt limit",
            }
            exit_code = 1
        atomic_write_json(job_dir / "terminal.json", terminal)
        return exit_code
    finally:
        release_job_lock(lock_descriptor)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="bounded CHIPS native CLI driver runner")
    parser.add_argument("--job-dir", required=True)
    return parser


def main() -> int:
    args = build_parser().parse_args()
    try:
        return asyncio.run(run(Path(args.job_dir)))
    except (InvalidExecutionSpec, OSError, RuntimeError, ValueError):
        return 70


if __name__ == "__main__":
    raise SystemExit(main())
