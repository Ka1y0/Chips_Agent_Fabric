from __future__ import annotations

import os
import re
import tempfile
from collections.abc import Mapping, Sequence
from contextlib import suppress
from typing import Any

from .base import UnsafeWorkerRequest, WorkerRequest, request_requires_code_write
from .native import NativeSubprocessAdapter, ParsedOutput

_CONVERSATION_PATTERNS = (
    re.compile(r"(?i)conversation(?:[_ -]?id)?\s*[:=]\s*([0-9a-f-]{16,})"),
    re.compile(r"(?i)created\s+conversation\s+([0-9a-f-]{16,})"),
    re.compile(r"(?i)conversation/([0-9a-f-]{16,})"),
)
_MODEL_PATTERNS = (
    re.compile(r"(?im)(?:actual\s+)?model\s*[:=]\s*([^\r\n]+)"),
    re.compile(r'(?im)selected model override[^\r\n]*label="([^"]+)"'),
)
_ANSI_ESCAPE = re.compile(r"\x1b\[[0-?]*[ -/]*[@-~]")


class AgyAdapter(NativeSubprocessAdapter):
    """Read-only AGY adapter using its plain-text print interface.

    AGY has no structured event output in the validated 1.1.3 build. stdout is
    therefore streamed as plain text, while non-sensitive session/model hints
    are parsed from the captured diagnostic stream when present.
    """

    def __init__(self, executable: str = "agy", **kwargs: Any) -> None:
        super().__init__(executable, **kwargs)
        self._diagnostic_paths: dict[str, str] = {}

    def command_arguments(self, request: WorkerRequest) -> Sequence[str]:
        self._reject_code_write(request)
        diagnostic_path = self._diagnostic_paths.get(request.run_id)
        if diagnostic_path is None:
            raise RuntimeError("AGY diagnostic capture was not initialized")
        arguments = [
            "--log-file",
            diagnostic_path,
            "--sandbox",
            "--mode",
            "plan",
            "-p",
            request.prompt,
        ]
        if request.session_id:
            arguments.extend(("--conversation", request.session_id))
        # Deliberately do not pass --model: AGY uses its already-selected default.
        return arguments

    async def execute(self, request: WorkerRequest, *, event_sink=None):  # type: ignore[no-untyped-def]
        self._reject_code_write(request)
        if request.model is not None:
            raise UnsafeWorkerRequest("AGY model changes are not permitted through this adapter")
        descriptor, path = tempfile.mkstemp(prefix="project-supervisor-agy-", suffix=".log")
        os.close(descriptor)
        self._diagnostic_paths[request.run_id] = path
        try:
            return await super().execute(request, event_sink=event_sink)
        finally:
            self._diagnostic_paths.pop(request.run_id, None)
            with suppress(FileNotFoundError):
                os.unlink(path)

    def consume_stdout_line(self, line: str, parsed: ParsedOutput) -> Mapping[str, Any]:
        parsed.provider_event_types.add("text")
        parsed.text_fragments.append(f"{line}\n")
        return {"providerEventType": "text", "bytes": len(line.encode())}

    def finalize_output(
        self,
        request: WorkerRequest,
        parsed: ParsedOutput,
        stdout: str,
        stderr: str,
    ) -> None:
        parsed.final_text = _ANSI_ESCAPE.sub("", stdout).strip()
        diagnostic = stderr
        path = self._diagnostic_paths.get(request.run_id)
        if path:
            try:
                with open(path, encoding="utf-8", errors="replace") as log_file:
                    diagnostic = f"{diagnostic}\n{log_file.read()}"
            except FileNotFoundError:
                pass
        for pattern in _CONVERSATION_PATTERNS:
            match = pattern.search(diagnostic)
            if match:
                parsed.session_id = match.group(1)
                break
        for pattern in _MODEL_PATTERNS:
            model_match = pattern.search(diagnostic)
            if model_match:
                parsed.model = model_match.group(1).strip()
                break

    @staticmethod
    def _reject_code_write(request: WorkerRequest) -> None:
        if request_requires_code_write(request):
            raise UnsafeWorkerRequest("AGY is configured as a read-only worker; code-write denied")
