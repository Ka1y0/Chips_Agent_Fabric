from __future__ import annotations

import asyncio
import json
import os
import re
import shutil
import signal
import tempfile
import time
from abc import abstractmethod
from collections.abc import Mapping, Sequence
from contextlib import suppress
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from project_supervisor.domain import RunState

from .base import (
    EventSink,
    Usage,
    WorkerAdapter,
    WorkerEvent,
    WorkerRequest,
    WorkerResult,
    WorkerUnavailable,
    event_time,
    publish_event,
)

_SENSITIVE_KEY = re.compile(
    r"(?:token|api[_-]?key|access[_-]?token|refresh[_-]?token|oauth|authorization|cookie|"
    r"credential|password|secret|signature)$",
    re.IGNORECASE,
)
_TEXT_REDACTIONS = (
    re.compile(r"(?i)(authorization\s*[:=]\s*bearer\s+)[^\s,;]+"),
    re.compile(
        r"(?i)((?:token|api[_-]?key|access[_-]?token|refresh[_-]?token|password|secret)"
        r"\s*[:=]\s*)[^\s,;]+"
    ),
    re.compile(r"\bsk-[A-Za-z0-9_-]{12,}\b"),
)
_SAFE_CHILD_ENVIRONMENT = frozenset(
    {
        "APPDATA",
        "COLORTERM",
        "COMSPEC",
        "HOME",
        "LANG",
        "LC_ALL",
        "LC_CTYPE",
        "LOCALAPPDATA",
        "LOGNAME",
        "NO_COLOR",
        "PATH",
        "PATHEXT",
        "PROGRAMDATA",
        "SHELL",
        "SystemRoot",
        "TEMP",
        "TERM",
        "TMP",
        "TMPDIR",
        "USER",
        "USERPROFILE",
        "WINDIR",
        "XDG_CACHE_HOME",
        "XDG_CONFIG_HOME",
        "XDG_DATA_HOME",
        "XDG_STATE_HOME",
        "__CF_USER_TEXT_ENCODING",
    }
)


def redact(value: Any) -> Any:
    """Return a recursively redacted copy safe for events and persisted evidence."""

    if isinstance(value, Mapping):
        return {
            str(key): "[REDACTED]" if _SENSITIVE_KEY.search(str(key)) else redact(item)
            for key, item in value.items()
        }
    if isinstance(value, list):
        return [redact(item) for item in value]
    if isinstance(value, tuple):
        return tuple(redact(item) for item in value)
    if not isinstance(value, str):
        return value
    result = value
    for pattern in _TEXT_REDACTIONS:
        result = pattern.sub(
            lambda match: f"{match.group(1)}[REDACTED]" if match.groups() else "[REDACTED]",
            result,
        )
    return result


def redact_output_line(value: str) -> str:
    """Redact structured fields before retaining a provider JSON line."""

    ending = "\n" if value.endswith("\n") else ""
    candidate = value.rstrip("\r\n")
    try:
        parsed = json.loads(candidate)
    except (json.JSONDecodeError, TypeError):
        return f"{redact(candidate)}{ending}"
    return f"{json.dumps(redact(parsed), separators=(',', ':'), ensure_ascii=False)}{ending}"


def child_environment(
    ambient: Mapping[str, str], overrides: Mapping[str, str] | None = None
) -> dict[str, str]:
    """Build a minimal CLI environment without ambient credentials or proxies.

    A trusted caller may provide explicit provider-required additions through
    ``overrides``. Those values are process-only and are never emitted.
    """

    selected = {
        key: value
        for key, value in ambient.items()
        if key in _SAFE_CHILD_ENVIRONMENT or key.startswith("LC_")
    }
    selected.update(overrides or {})
    return selected


@dataclass(slots=True)
class ParsedOutput:
    text_fragments: list[str] = field(default_factory=list)
    final_text: str | None = None
    session_id: str | None = None
    model: str | None = None
    context_variant: str | None = None
    usage: Usage = field(default_factory=Usage)
    provider_event_types: set[str] = field(default_factory=set)
    provider_event_counts: dict[str, int] = field(default_factory=dict)
    terminal_event_count: int = 0
    terminal_error: str | None = None


class NativeSubprocessAdapter(WorkerAdapter):
    """Supervise a native CLI without a shell and normalize its lifecycle."""

    def __init__(
        self,
        executable: str,
        *,
        heartbeat_seconds: float = 1.0,
        termination_grace_seconds: float = 2.0,
        environment: Mapping[str, str] | None = None,
        max_stdout_bytes: int = 4 * 1024 * 1024,
        max_stderr_bytes: int = 1024 * 1024,
        max_line_bytes: int = 64 * 1024,
        max_output_lines: int = 20_000,
        max_output_events: int = 20_000,
    ) -> None:
        limits = {
            "max_stdout_bytes": max_stdout_bytes,
            "max_stderr_bytes": max_stderr_bytes,
            "max_line_bytes": max_line_bytes,
            "max_output_lines": max_output_lines,
            "max_output_events": max_output_events,
        }
        if any(
            not isinstance(value, int) or isinstance(value, bool) or value <= 0
            for value in limits.values()
        ):
            raise ValueError("native Worker output limits must be positive integers")
        self.executable = executable
        self.heartbeat_seconds = heartbeat_seconds
        self.termination_grace_seconds = termination_grace_seconds
        self.environment = dict(environment or {})
        self.max_stdout_bytes = max_stdout_bytes
        self.max_stderr_bytes = max_stderr_bytes
        self.max_line_bytes = max_line_bytes
        self.max_output_lines = max_output_lines
        self.max_output_events = max_output_events
        self._processes: dict[str, asyncio.subprocess.Process] = {}
        self._starting_runs: set[str] = set()
        self._cancelled_runs: set[str] = set()
        self._lock = asyncio.Lock()

    @abstractmethod
    def command_arguments(self, request: WorkerRequest) -> Sequence[str]:
        """Return arguments only; the executable is inserted by the supervisor."""

    @abstractmethod
    def consume_stdout_line(self, line: str, parsed: ParsedOutput) -> Mapping[str, Any]:
        """Consume one redacted stdout line and return its normalized event payload."""

    def finalize_output(
        self,
        request: WorkerRequest,
        parsed: ParsedOutput,
        stdout: str,
        stderr: str,
    ) -> None:
        if parsed.final_text is None:
            parsed.final_text = "".join(parsed.text_fragments).strip() or stdout.strip()

    def validate_output(
        self,
        request: WorkerRequest,
        parsed: ParsedOutput,
        stdout: str,
        stderr: str,
    ) -> None:
        """Validate a provider-specific terminal transcript after bounded capture."""

    def stdin_payload(self, request: WorkerRequest) -> bytes | None:
        """Return bounded provider input for stdin, or ``None`` to close stdin.

        Native adapters default to no stdin.  Providers whose reviewed CLI contract accepts a
        prompt on stdin can opt in without placing user content in argv or invoking a shell.
        """

        return None

    async def execute(
        self,
        request: WorkerRequest,
        *,
        event_sink: EventSink | None = None,
    ) -> WorkerResult:
        started_at = event_time()
        events: list[WorkerEvent] = []
        stdout_lines: list[str] = []
        stderr_lines: list[str] = []
        parsed = ParsedOutput()
        temporary_directory: tempfile.TemporaryDirectory[str] | None = None
        process: asyncio.subprocess.Process | None = None
        timed_out = False
        cancelled = False
        output_failure: str | None = None
        output_failed = asyncio.Event()
        stream_bytes = {"stdout": 0, "stderr": 0}
        output_line_count = 0
        output_event_count = 0
        rejection_emitted = False
        owns_start_reservation = False

        def reject_output(code: str, detail: str) -> None:
            nonlocal output_failure
            if output_failure is None:
                output_failure = f"{code}: {detail}"
                output_failed.set()

        async def emit(kind: str, payload: Mapping[str, Any] | None = None) -> None:
            event = WorkerEvent(request.run_id, kind, event_time(), redact(payload or {}))
            events.append(event)
            await publish_event(event_sink, event)

        executable = self._resolve_executable()
        if request.working_directory is None:
            temporary_directory = tempfile.TemporaryDirectory(
                prefix=f"project-supervisor-{self.__class__.__name__.lower()}-"
            )
            cwd = Path(temporary_directory.name)
        else:
            cwd = request.working_directory.resolve()
            if not cwd.is_dir():
                raise WorkerUnavailable(f"working directory is unavailable: {cwd}")

        args = [executable, *self.command_arguments(request)]
        stdin_payload = self.stdin_payload(request)
        if stdin_payload is not None and not isinstance(stdin_payload, bytes):
            raise TypeError("native Worker stdin payload must be bytes")
        env = child_environment(os.environ, self.environment)
        try:
            async with self._lock:
                if request.run_id in self._starting_runs or request.run_id in self._processes:
                    raise ValueError(f"run already active: {request.run_id}")
                self._starting_runs.add(request.run_id)
                owns_start_reservation = True
            process = await asyncio.create_subprocess_exec(
                *args,
                cwd=cwd,
                env=env,
                stdin=(
                    asyncio.subprocess.PIPE
                    if stdin_payload is not None
                    else asyncio.subprocess.DEVNULL
                ),
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                start_new_session=True,
                limit=self.max_line_bytes + 1,
            )
            async with self._lock:
                self._starting_runs.discard(request.run_id)
                owns_start_reservation = False
                self._processes[request.run_id] = process
            await emit("processStarted", {"pid": process.pid, "executable": executable})

            async def read_stream(
                stream: asyncio.StreamReader | None,
                destination: list[str],
                stream_name: str,
            ) -> None:
                nonlocal output_line_count, output_event_count
                if stream is None:
                    return

                async def discard_remainder() -> None:
                    """Drain a rejected pipe so asyncio can reap a blocked child."""

                    with suppress(Exception):
                        while await stream.read(64 * 1024):
                            pass

                try:
                    while True:
                        try:
                            raw_line = await stream.readline()
                        except (ValueError, asyncio.LimitOverrunError):
                            reject_output(
                                "OUTPUT_LIMIT",
                                f"{stream_name} line exceeded {self.max_line_bytes} bytes",
                            )
                            await discard_remainder()
                            return
                        if not raw_line:
                            return
                        if len(raw_line) > self.max_line_bytes:
                            reject_output(
                                "OUTPUT_LIMIT",
                                f"{stream_name} line exceeded {self.max_line_bytes} bytes",
                            )
                            await discard_remainder()
                            return
                        stream_bytes[stream_name] += len(raw_line)
                        stream_limit = (
                            self.max_stdout_bytes
                            if stream_name == "stdout"
                            else self.max_stderr_bytes
                        )
                        if stream_bytes[stream_name] > stream_limit:
                            reject_output(
                                "OUTPUT_LIMIT",
                                f"{stream_name} exceeded {stream_limit} captured bytes",
                            )
                            await discard_remainder()
                            return
                        output_line_count += 1
                        if output_line_count > self.max_output_lines:
                            reject_output(
                                "OUTPUT_LIMIT",
                                f"Worker emitted more than {self.max_output_lines} lines",
                            )
                            await discard_remainder()
                            return
                        line = redact_output_line(raw_line.decode("utf-8", errors="replace"))
                        destination.append(line)
                        payload: Mapping[str, Any] = {
                            "stream": stream_name,
                            "bytes": len(raw_line),
                        }
                        if stream_name == "stdout":
                            try:
                                payload = self.consume_stdout_line(line.rstrip("\r\n"), parsed)
                            except (json.JSONDecodeError, TypeError, ValueError) as error:
                                reject_output(
                                    "RESULT_INVALID",
                                    "stdout did not match the registered adapter protocol "
                                    f"({type(error).__name__})",
                                )
                                await discard_remainder()
                                return
                        output_event_count += 1
                        if output_event_count > self.max_output_events:
                            reject_output(
                                "OUTPUT_LIMIT",
                                f"Worker emitted more than {self.max_output_events} output events",
                            )
                            await discard_remainder()
                            return
                        await emit("workerOutput", payload)
                except asyncio.CancelledError:
                    raise
                except Exception as error:  # pragma: no cover - defensive stream boundary
                    reject_output(
                        "RESULT_INVALID",
                        f"{stream_name} capture failed ({type(error).__name__})",
                    )
                    await discard_remainder()

            async def heartbeat() -> None:
                while process.returncode is None:
                    await asyncio.sleep(self.heartbeat_seconds)
                    if process.returncode is None:
                        await emit("heartbeat", {"pid": process.pid})

            readers = [
                asyncio.create_task(read_stream(process.stdout, stdout_lines, "stdout")),
                asyncio.create_task(read_stream(process.stderr, stderr_lines, "stderr")),
            ]
            heartbeat_task = asyncio.create_task(heartbeat())
            process_wait = asyncio.create_task(process.wait())
            output_failure_wait = asyncio.create_task(output_failed.wait())
            try:
                if stdin_payload is not None:
                    if process.stdin is None:  # pragma: no cover - asyncio contract guard
                        raise RuntimeError("native Worker stdin pipe is unavailable")
                    try:
                        process.stdin.write(stdin_payload)
                        await asyncio.wait_for(
                            process.stdin.drain(), timeout=request.timeout_seconds
                        )
                    except (BrokenPipeError, ConnectionResetError):
                        # The terminal process status and strict output validator remain
                        # authoritative when a CLI exits before consuming its full input.
                        pass
                    finally:
                        process.stdin.close()
                        with suppress(BrokenPipeError, ConnectionResetError, TimeoutError):
                            await asyncio.wait_for(
                                process.stdin.wait_closed(),
                                timeout=self.termination_grace_seconds,
                            )
                done, _pending = await asyncio.wait(
                    {process_wait, output_failure_wait},
                    timeout=request.timeout_seconds,
                    return_when=asyncio.FIRST_COMPLETED,
                )
                if not done:
                    timed_out = True
                    await emit("processTimedOut", {"timeoutSeconds": request.timeout_seconds})
                    await self._stop_process(process)
                elif output_failure_wait in done and output_failure is not None:
                    await emit("workerOutputRejected", {"reason": output_failure})
                    rejection_emitted = True
                    await self._stop_process(process)
                await process_wait
            except TimeoutError:  # pragma: no cover - _stop_process owns its bounded escalation
                timed_out = True
                await self._stop_process(process)
            except asyncio.CancelledError:
                cancelled = True
                await self._stop_process(process)
                raise
            finally:
                output_failure_wait.cancel()
                await asyncio.gather(output_failure_wait, return_exceptions=True)
                try:
                    await asyncio.wait_for(
                        asyncio.gather(*readers), timeout=self.termination_grace_seconds
                    )
                except TimeoutError:
                    reject_output(
                        "PROCESS_LOST",
                        "Worker process tree kept output pipes open after leader exit",
                    )
                    await self._stop_process(process)
                    for reader in readers:
                        reader.cancel()
                    await asyncio.gather(*readers, return_exceptions=True)
                heartbeat_task.cancel()
                await asyncio.gather(heartbeat_task, return_exceptions=True)

            if os.name != "nt" and self._process_group_alive(process.pid):
                reject_output(
                    "PROCESS_LOST",
                    "Worker left a live process in its execution group",
                )
                await self._stop_process(process)

            stdout = "".join(stdout_lines)
            stderr = "".join(stderr_lines)
            if (
                output_failure is None
                and process.returncode == 0
                and not timed_out
                and not cancelled
                and request.run_id not in self._cancelled_runs
            ):
                try:
                    self.finalize_output(request, parsed, stdout, stderr)
                    self.validate_output(request, parsed, stdout, stderr)
                except Exception as error:
                    reject_output(
                        "RESULT_INVALID",
                        f"terminal output validation failed ({type(error).__name__})",
                    )
            if output_failure is not None and not rejection_emitted:
                await emit("workerOutputRejected", {"reason": output_failure})
            exit_code = process.returncode
            if timed_out:
                state = RunState.TIMED_OUT
                error = f"worker exceeded {request.timeout_seconds:g} second timeout"
            elif cancelled or request.run_id in self._cancelled_runs:
                state = RunState.CANCELLED
                error = "worker was cancelled"
            elif output_failure is not None:
                state = RunState.FAILED
                error = output_failure
            elif exit_code == 0:
                state = RunState.COMPLETED
                error = None
            else:
                state = RunState.FAILED
                error = stderr.strip() or f"worker exited with status {exit_code}"
            await emit(
                "processExited",
                {"pid": process.pid, "exitCode": exit_code, "state": state.value},
            )
            return WorkerResult(
                run_id=request.run_id,
                state=state,
                pid=process.pid,
                exit_code=exit_code,
                started_at=started_at,
                ended_at=event_time(),
                stdout=stdout,
                stderr=stderr,
                final_text=parsed.final_text or "",
                events=tuple(events),
                session_id=parsed.session_id,
                model=parsed.model,
                context_variant=parsed.context_variant,
                usage=parsed.usage,
                error=str(redact(error)) if error else None,
            )
        except BaseException:
            if process is not None and self._process_tree_active(process):
                await self._stop_process(process)
            raise
        finally:
            async with self._lock:
                if owns_start_reservation:
                    self._starting_runs.discard(request.run_id)
                if process is not None and self._processes.get(request.run_id) is process:
                    self._processes.pop(request.run_id, None)
                    self._cancelled_runs.discard(request.run_id)
            if temporary_directory is not None:
                temporary_directory.cleanup()

    async def cancel(self, run_id: str) -> bool:
        async with self._lock:
            process = self._processes.get(run_id)
        if process is None or not self._process_tree_active(process):
            return False
        async with self._lock:
            self._cancelled_runs.add(run_id)
        await self._stop_process(process)
        return True

    def _resolve_executable(self) -> str:
        if os.path.sep in self.executable:
            path = Path(self.executable).expanduser()
            if path.is_file() and os.access(path, os.X_OK):
                return str(path)
            raise WorkerUnavailable(f"worker executable unavailable: {path}")
        resolved = shutil.which(self.executable)
        if resolved is None:
            raise WorkerUnavailable(f"worker executable unavailable: {self.executable}")
        return resolved

    async def _stop_process(self, process: asyncio.subprocess.Process) -> None:
        if os.name == "nt":
            if process.returncode is not None:
                return
            process.terminate()
            try:
                await asyncio.wait_for(process.wait(), timeout=self.termination_grace_seconds)
            except TimeoutError:
                process.kill()
                await process.wait()
            return

        if not self._process_group_alive(process.pid):
            if process.returncode is None:
                await process.wait()
            return
        with suppress(ProcessLookupError):
            os.killpg(process.pid, signal.SIGTERM)
        deadline = time.monotonic() + self.termination_grace_seconds
        while self._process_group_alive(process.pid) and time.monotonic() < deadline:
            await asyncio.sleep(0.02)
        if self._process_group_alive(process.pid):
            with suppress(ProcessLookupError):
                os.killpg(process.pid, signal.SIGKILL)
        if process.returncode is None:
            await process.wait()

    @staticmethod
    def _process_group_alive(process_group_id: int) -> bool:
        if os.name == "nt":
            return False
        try:
            os.killpg(process_group_id, 0)
        except ProcessLookupError:
            return False
        except PermissionError:
            return True
        return True

    @classmethod
    def _process_tree_active(cls, process: asyncio.subprocess.Process) -> bool:
        if os.name == "nt":
            return process.returncode is None
        return cls._process_group_alive(process.pid)
