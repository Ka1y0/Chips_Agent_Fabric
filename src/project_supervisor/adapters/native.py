from __future__ import annotations

import asyncio
import json
import os
import re
import shutil
import tempfile
from abc import abstractmethod
from collections.abc import Mapping, Sequence
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


class NativeSubprocessAdapter(WorkerAdapter):
    """Supervise a native CLI without a shell and normalize its lifecycle."""

    def __init__(
        self,
        executable: str,
        *,
        heartbeat_seconds: float = 1.0,
        termination_grace_seconds: float = 2.0,
        environment: Mapping[str, str] | None = None,
    ) -> None:
        self.executable = executable
        self.heartbeat_seconds = heartbeat_seconds
        self.termination_grace_seconds = termination_grace_seconds
        self.environment = dict(environment or {})
        self._processes: dict[str, asyncio.subprocess.Process] = {}
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
        env = child_environment(os.environ, self.environment)
        try:
            process = await asyncio.create_subprocess_exec(
                *args,
                cwd=cwd,
                env=env,
                stdin=asyncio.subprocess.DEVNULL,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                start_new_session=True,
            )
            async with self._lock:
                if request.run_id in self._processes:
                    await self._stop_process(process)
                    raise ValueError(f"run already active: {request.run_id}")
                self._processes[request.run_id] = process
            await emit("processStarted", {"pid": process.pid, "executable": executable})

            async def read_stream(
                stream: asyncio.StreamReader | None,
                destination: list[str],
                stream_name: str,
            ) -> None:
                if stream is None:
                    return
                while raw_line := await stream.readline():
                    line = redact_output_line(raw_line.decode("utf-8", errors="replace"))
                    destination.append(line)
                    payload: Mapping[str, Any] = {"stream": stream_name, "bytes": len(raw_line)}
                    if stream_name == "stdout":
                        try:
                            payload = self.consume_stdout_line(line.rstrip("\r\n"), parsed)
                        except (json.JSONDecodeError, TypeError, ValueError) as error:
                            payload = {
                                "stream": stream_name,
                                "malformed": True,
                                "error": str(redact(str(error))),
                            }
                    await emit("workerOutput", payload)

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
            try:
                await asyncio.wait_for(process.wait(), timeout=request.timeout_seconds)
            except TimeoutError:
                timed_out = True
                await emit("processTimedOut", {"timeoutSeconds": request.timeout_seconds})
                await self._stop_process(process)
            except asyncio.CancelledError:
                cancelled = True
                await self._stop_process(process)
                raise
            finally:
                await asyncio.gather(*readers, return_exceptions=True)
                heartbeat_task.cancel()
                await asyncio.gather(heartbeat_task, return_exceptions=True)

            stdout = "".join(stdout_lines)
            stderr = "".join(stderr_lines)
            self.finalize_output(request, parsed, stdout, stderr)
            exit_code = process.returncode
            if timed_out:
                state = RunState.TIMED_OUT
                error = f"worker exceeded {request.timeout_seconds:g} second timeout"
            elif cancelled or request.run_id in self._cancelled_runs:
                state = RunState.CANCELLED
                error = "worker was cancelled"
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
        finally:
            async with self._lock:
                self._processes.pop(request.run_id, None)
                self._cancelled_runs.discard(request.run_id)
            if temporary_directory is not None:
                temporary_directory.cleanup()

    async def cancel(self, run_id: str) -> bool:
        async with self._lock:
            process = self._processes.get(run_id)
        if process is None or process.returncode is not None:
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
        if process.returncode is not None:
            return
        process.terminate()
        try:
            await asyncio.wait_for(process.wait(), timeout=self.termination_grace_seconds)
        except TimeoutError:
            process.kill()
            await process.wait()
