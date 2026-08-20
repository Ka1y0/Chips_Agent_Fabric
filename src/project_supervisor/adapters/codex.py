from __future__ import annotations

import json
import os
import re
import stat
import tempfile
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

from jsonschema.validators import validator_for
from referencing import Registry

from .base import (
    UnsafeWorkerRequest,
    Usage,
    WorkerProtocolError,
    WorkerRequest,
    request_requires_code_write,
)
from .native import NativeSubprocessAdapter, ParsedOutput, redact

_MAX_PROMPT_BYTES = 128 * 1024
_MAX_FINAL_MESSAGE_BYTES = 256 * 1024
_MAX_SCHEMA_BYTES = 128 * 1024
_MODEL = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:/-]{0,127}$")
# An empty referencing registry resolves fragments against the root schema but fails every
# external retrieval instead of jsonschema's compatibility behavior of fetching remote resources.
_OFFLINE_SCHEMA_REGISTRY: Registry[Any] = Registry()
_UNSAFE_REFERENCE_KEYWORDS = frozenset({"$dynamicRef", "$recursiveRef"})
_UNSAFE_REGEX_KEYWORDS = frozenset({"pattern", "patternProperties"})


def _integer(mapping: Mapping[str, Any], key: str) -> int | None:
    value = mapping.get(key)
    return value if isinstance(value, int) and not isinstance(value, bool) and value >= 0 else None


def _reject_external_schema_references(value: object, seen: set[int] | None = None) -> None:
    seen = seen if seen is not None else set()
    if isinstance(value, Mapping):
        if id(value) in seen:
            raise WorkerProtocolError("Codex response_schema must not contain cycles")
        seen.add(id(value))
        if _UNSAFE_REFERENCE_KEYWORDS.intersection(value):
            raise WorkerProtocolError(
                "Codex response_schema dynamic/recursive references are forbidden"
            )
        if _UNSAFE_REGEX_KEYWORDS.intersection(value):
            raise WorkerProtocolError("Codex response_schema regex keywords are forbidden")
        reference = value.get("$ref")
        if isinstance(reference, str) and not reference.startswith("#"):
            raise WorkerProtocolError("Codex response_schema external references are forbidden")
        for item in value.values():
            _reject_external_schema_references(item, seen)
        seen.remove(id(value))
    elif isinstance(value, list):
        if id(value) in seen:
            raise WorkerProtocolError("Codex response_schema must not contain cycles")
        seen.add(id(value))
        for item in value:
            _reject_external_schema_references(item, seen)
        seen.remove(id(value))


class CodexAdapter(NativeSubprocessAdapter):
    """Adapter for the reviewed ``codex exec --json`` non-interactive interface.

    The prompt travels over stdin.  A bounded, private ``--output-last-message`` file is the
    authoritative terminal result; JSONL stdout is validated and used only for progress and
    optional session/usage metadata.  This deliberately avoids treating arbitrary stdout as
    successful completion.
    """

    def __init__(
        self,
        executable: str = "codex",
        *,
        read_only: bool = False,
        max_prompt_bytes: int = _MAX_PROMPT_BYTES,
        max_final_message_bytes: int = _MAX_FINAL_MESSAGE_BYTES,
        **kwargs: Any,
    ) -> None:
        if max_prompt_bytes <= 0 or max_final_message_bytes <= 0:
            raise ValueError("Codex prompt/result limits must be positive")
        super().__init__(executable, **kwargs)
        self.read_only = read_only
        self.max_prompt_bytes = max_prompt_bytes
        self.max_final_message_bytes = max_final_message_bytes
        self._result_paths: dict[str, Path] = {}
        self._schema_paths: dict[str, Path] = {}
        self._schemas: dict[str, Mapping[str, Any]] = {}
        self._contexts: dict[str, tempfile.TemporaryDirectory[str]] = {}

    async def execute(self, request: WorkerRequest, *, event_sink=None):  # type: ignore[no-untyped-def]
        if request.run_id in self._contexts:
            raise ValueError(f"run already active: {request.run_id}")
        directory = tempfile.TemporaryDirectory(prefix="project-supervisor-codex-result-")
        os.chmod(directory.name, 0o700)
        root = Path(directory.name)
        self._contexts[request.run_id] = directory
        result_path = root / "last-message.txt"
        result_descriptor = os.open(result_path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        os.close(result_descriptor)
        self._result_paths[request.run_id] = result_path
        schema = request.metadata.get("response_schema")
        try:
            if schema is not None:
                if not isinstance(schema, Mapping):
                    raise WorkerProtocolError("Codex response_schema must be an object")
                _reject_external_schema_references(schema)
                encoded = json.dumps(
                    dict(schema), ensure_ascii=False, separators=(",", ":"), sort_keys=True
                ).encode("utf-8")
                if len(encoded) > _MAX_SCHEMA_BYTES:
                    raise WorkerProtocolError("Codex response_schema exceeds 128 KiB")
                schema_path = root / "response-schema.json"
                descriptor = os.open(schema_path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
                try:
                    os.write(descriptor, encoded)
                    os.fsync(descriptor)
                finally:
                    os.close(descriptor)
                self._schema_paths[request.run_id] = schema_path
                self._schemas[request.run_id] = dict(schema)
            return await super().execute(request, event_sink=event_sink)
        finally:
            self._result_paths.pop(request.run_id, None)
            self._schema_paths.pop(request.run_id, None)
            self._schemas.pop(request.run_id, None)
            self._contexts.pop(request.run_id, None)
            directory.cleanup()

    def command_arguments(self, request: WorkerRequest) -> Sequence[str]:
        if self.read_only and request_requires_code_write(request):
            raise UnsafeWorkerRequest("read-only Codex profile denied code-write authority")
        if request.session_id is not None:
            raise WorkerProtocolError("Codex provider conversation resume is not supported")
        result_path = self._result_paths.get(request.run_id)
        if result_path is None:
            raise RuntimeError("Codex terminal result capture was not initialized")
        sandbox = "workspace-write" if request_requires_code_write(request) else "read-only"
        if self.read_only:
            sandbox = "read-only"
        arguments = [
            "exec",
            "--json",
            "--color",
            "never",
            "--sandbox",
            sandbox,
            "--ask-for-approval",
            "never",
            "--skip-git-repo-check",
            "--ephemeral",
            "--ignore-user-config",
            "--strict-config",
            "--output-last-message",
            str(result_path),
        ]
        schema_path = self._schema_paths.get(request.run_id)
        if schema_path is not None:
            arguments.extend(("--output-schema", str(schema_path)))
        if request.model is not None:
            if not _MODEL.fullmatch(request.model):
                raise UnsafeWorkerRequest("Codex model identifier is unsafe")
            arguments.extend(("--model", request.model))
        arguments.append("-")
        return arguments

    def stdin_payload(self, request: WorkerRequest) -> bytes:
        encoded = request.prompt.encode("utf-8")
        if len(encoded) > self.max_prompt_bytes:
            raise UnsafeWorkerRequest("Codex prompt exceeds its configured stdin limit")
        return encoded

    def consume_stdout_line(self, line: str, parsed: ParsedOutput) -> Mapping[str, Any]:
        if not line.strip():
            return {"providerEventType": "blank"}
        event = json.loads(line)
        if not isinstance(event, Mapping):
            raise TypeError("Codex event must be a JSON object")
        event_type = event.get("type")
        if not isinstance(event_type, str) or not event_type.strip():
            raise TypeError("Codex event type must be a non-empty string")
        parsed.provider_event_types.add(event_type)
        parsed.provider_event_counts[event_type] = (
            parsed.provider_event_counts.get(event_type, 0) + 1
        )

        if event_type == "thread.started":
            thread_id = event.get("thread_id")
            if not isinstance(thread_id, str) or not thread_id.strip():
                raise TypeError("Codex thread.started requires a thread_id")
            parsed.session_id = thread_id
        elif event_type in {"item.started", "item.updated", "item.completed"}:
            item = event.get("item")
            if not isinstance(item, Mapping):
                raise TypeError("Codex item event requires an item object")
            item_id = item.get("id")
            item_type = item.get("type")
            if not isinstance(item_id, str) or not item_id.strip():
                raise TypeError("Codex item event requires an item id")
            if not isinstance(item_type, str) or not item_type.strip():
                raise TypeError("Codex item event requires an item type")
            if event_type == "item.completed" and item_type == "agent_message":
                text = item.get("text")
                if not isinstance(text, str) or not text.strip():
                    raise TypeError("Codex agent_message requires non-empty text")
                parsed.text_fragments.append(text)
            if event_type == "item.completed" and item_type == "error":
                parsed.terminal_error = "Codex reported an error item"
        elif event_type == "turn.completed":
            parsed.terminal_event_count += 1
            usage = event.get("usage")
            if usage is not None and not isinstance(usage, Mapping):
                raise TypeError("Codex turn.completed usage must be an object")
            if isinstance(usage, Mapping):
                input_tokens = _integer(usage, "input_tokens")
                output_tokens = _integer(usage, "output_tokens")
                parsed.usage = Usage(
                    input_tokens=input_tokens,
                    output_tokens=output_tokens,
                    cache_creation_tokens=_integer(usage, "cache_write_input_tokens"),
                    cache_read_tokens=_integer(usage, "cached_input_tokens"),
                    reasoning_tokens=_integer(usage, "reasoning_output_tokens"),
                    total_tokens=(
                        input_tokens + output_tokens
                        if input_tokens is not None and output_tokens is not None
                        else None
                    ),
                    raw={
                        "executionSource": "codex-cli",
                        "accountSource": "unknown",
                        "usage": redact(dict(usage)),
                    },
                )
        elif event_type in {"turn.failed", "error"}:
            parsed.terminal_error = "Codex reported a terminal execution error"

        item = event.get("item")
        item_type = item.get("type") if isinstance(item, Mapping) else None
        return {
            "providerEventType": event_type,
            "itemType": item_type if isinstance(item_type, str) else None,
            "sessionId": parsed.session_id,
        }

    def finalize_output(
        self,
        request: WorkerRequest,
        parsed: ParsedOutput,
        stdout: str,
        stderr: str,
    ) -> None:
        result_path = self._result_paths.get(request.run_id)
        if result_path is None:
            raise WorkerProtocolError("Codex terminal result path is unavailable")
        try:
            metadata = result_path.lstat()
            if (
                not stat.S_ISREG(metadata.st_mode)
                or result_path.is_symlink()
                or metadata.st_nlink != 1
            ):
                raise WorkerProtocolError("Codex terminal result is not a regular file")
            if metadata.st_uid != os.getuid() or stat.S_IMODE(metadata.st_mode) & 0o077:
                raise WorkerProtocolError("Codex terminal result has unsafe ownership or mode")
            with result_path.open("rb") as stream:
                raw = stream.read(self.max_final_message_bytes + 1)
        except FileNotFoundError as error:
            raise WorkerProtocolError("Codex terminal result is missing") from error
        except OSError as error:
            raise WorkerProtocolError(
                f"Codex terminal result cannot be read ({type(error).__name__})"
            ) from error
        if len(raw) > self.max_final_message_bytes:
            raise WorkerProtocolError("Codex terminal result exceeds its capture limit")
        try:
            final_text = raw.decode("utf-8")
        except UnicodeError as error:
            raise WorkerProtocolError("Codex terminal result is not UTF-8") from error
        if not final_text.strip():
            raise WorkerProtocolError("Codex terminal result is empty")
        schema = self._schemas.get(request.run_id)
        if schema is not None:
            try:
                value = json.loads(final_text)
                validator_type = validator_for(schema)
                validator_type.check_schema(schema)
                validator_type(schema, registry=_OFFLINE_SCHEMA_REGISTRY).validate(value)
            except Exception as error:
                raise WorkerProtocolError(
                    f"Codex structured result validation failed ({type(error).__name__})"
                ) from error
        parsed.final_text = final_text.strip()

    def validate_output(
        self,
        request: WorkerRequest,
        parsed: ParsedOutput,
        stdout: str,
        stderr: str,
    ) -> None:
        if parsed.terminal_error is not None:
            raise WorkerProtocolError(parsed.terminal_error)
        if parsed.provider_event_counts.get("thread.started") != 1:
            raise WorkerProtocolError("Codex output requires exactly one thread.started event")
        if parsed.provider_event_counts.get("turn.started") != 1:
            raise WorkerProtocolError("Codex output requires exactly one turn.started event")
        if parsed.terminal_event_count != 1:
            raise WorkerProtocolError("Codex output requires exactly one turn.completed event")
        if len(parsed.text_fragments) != 1:
            raise WorkerProtocolError(
                "Codex output requires exactly one completed agent_message item"
            )
        if parsed.text_fragments[0].strip() != parsed.final_text.strip():
            raise WorkerProtocolError("Codex JSONL and terminal result disagree")
        if not parsed.final_text or not parsed.final_text.strip():
            raise WorkerProtocolError("Codex terminal result is empty")
