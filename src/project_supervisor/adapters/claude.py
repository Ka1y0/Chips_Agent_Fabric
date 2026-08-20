from __future__ import annotations

import json
import re
from collections.abc import Mapping, Sequence
from typing import Any

from .base import (
    UnsafeWorkerRequest,
    Usage,
    WorkerProtocolError,
    WorkerRequest,
    request_requires_code_write,
)
from .native import NativeSubprocessAdapter, ParsedOutput, redact


def _int_value(mapping: Mapping[str, Any], *keys: str) -> int | None:
    for key in keys:
        value = mapping.get(key)
        if isinstance(value, int) and not isinstance(value, bool):
            return value
    return None


def _float_value(mapping: Mapping[str, Any], *keys: str) -> float | None:
    for key in keys:
        value = mapping.get(key)
        if isinstance(value, (int, float)) and not isinstance(value, bool):
            return float(value)
    return None


def _usage(mapping: Mapping[str, Any], *, cost: float | None = None) -> Usage:
    return Usage(
        input_tokens=_int_value(mapping, "input_tokens", "inputTokens"),
        output_tokens=_int_value(mapping, "output_tokens", "outputTokens"),
        cache_creation_tokens=_int_value(
            mapping, "cache_creation_input_tokens", "cache_creation_tokens", "cacheCreationTokens"
        ),
        cache_read_tokens=_int_value(
            mapping, "cache_read_input_tokens", "cache_read_tokens", "cacheReadTokens"
        ),
        reasoning_tokens=_int_value(mapping, "reasoning_tokens", "reasoningTokens"),
        total_tokens=_int_value(mapping, "total_tokens", "totalTokens"),
        cost_usd=cost,
        raw=redact(dict(mapping)),
    )


class ClaudeAdapter(NativeSubprocessAdapter):
    """Adapter for Claude Code's verified ``stream-json`` print interface."""

    def __init__(
        self, executable: str = "claude", *, read_only: bool = False, **kwargs: Any
    ) -> None:
        super().__init__(executable, **kwargs)
        self.read_only = read_only

    def command_arguments(self, request: WorkerRequest) -> Sequence[str]:
        if self.read_only and request_requires_code_write(request):
            raise UnsafeWorkerRequest("read-only Claude profile denied code-write authority")
        arguments = [
            "-p",
            request.prompt,
            "--output-format",
            "stream-json",
            "--verbose",
        ]
        if self.read_only:
            # These are fixed, operator-owned flags from the pinned Claude CLI contract.  Empty
            # --tools is the provider's documented no-tool mode; safe-mode also disables hooks,
            # plugins, MCP discovery, skills, and project instructions.
            arguments.extend(
                (
                    "--permission-mode",
                    "plan",
                    "--tools",
                    "",
                    "--disable-slash-commands",
                    "--strict-mcp-config",
                    "--no-chrome",
                    "--safe-mode",
                )
            )
        if request.session_id:
            arguments.extend(("--resume", request.session_id))
        if request.model:
            arguments.extend(("--model", request.model))
        return arguments

    def consume_stdout_line(self, line: str, parsed: ParsedOutput) -> Mapping[str, Any]:
        if not line.strip():
            return {"providerEventType": "blank"}
        event = json.loads(line)
        if not isinstance(event, dict):
            raise TypeError("Claude event must be a JSON object")
        event_type = event.get("type")
        if not isinstance(event_type, str) or not event_type:
            raise TypeError("Claude event type must be a non-empty string")
        parsed.provider_event_types.add(event_type)
        session_id = event.get("session_id") or event.get("sessionId")
        if isinstance(session_id, str):
            parsed.session_id = session_id

        if event_type == "assistant":
            message = event.get("message")
            if not isinstance(message, Mapping):
                raise TypeError("Claude assistant event must contain a message object")
            model = message.get("model")
            if isinstance(model, str) and model != "<synthetic>":
                parsed.model = model
            content = message.get("content", ())
            if not isinstance(content, list):
                raise TypeError("Claude assistant content must be a list")
            for block in content:
                if isinstance(block, Mapping) and block.get("type") == "text":
                    text = block.get("text")
                    if not isinstance(text, str):
                        raise TypeError("Claude text content must contain a string")
                    parsed.text_fragments.append(text)
            if isinstance(message.get("usage"), Mapping):
                parsed.usage = _usage(message["usage"], cost=parsed.usage.cost_usd)

        if event_type == "result":
            parsed.terminal_event_count += 1
            result = event.get("result")
            if not isinstance(result, str) or not result.strip():
                raise TypeError("Claude result event must contain non-empty text")
            parsed.final_text = result.strip()
            if event.get("is_error") is True or event.get("isError") is True:
                parsed.terminal_error = "Claude reported an error result"
            cost = _float_value(event, "total_cost_usd", "cost_usd", "costUsd")
            usage = event.get("usage")
            if isinstance(usage, Mapping):
                parsed.usage = _usage(usage, cost=cost)
            elif cost is not None:
                parsed.usage = Usage(cost_usd=cost)
            model_usage = event.get("modelUsage") or event.get("model_usage")
            if isinstance(model_usage, Mapping) and model_usage:
                model_key = next((key for key in model_usage if isinstance(key, str)), None)
                if model_key:
                    if parsed.model is None:
                        parsed.model = re.sub(r"\[[^]]+]$", "", model_key)
                    match = re.search(r"(\[[^]]+])$", model_key)
                    if match:
                        parsed.context_variant = match.group(1)[1:-1]
                    details = model_usage.get(model_key)
                    if isinstance(details, Mapping):
                        context = details.get("contextWindow") or details.get("context_window")
                        if parsed.context_variant is None and isinstance(context, int):
                            parsed.context_variant = str(context)

        return {
            "providerEventType": event_type,
            "sessionId": parsed.session_id,
            "model": parsed.model,
        }

    def validate_output(
        self,
        request: WorkerRequest,
        parsed: ParsedOutput,
        stdout: str,
        stderr: str,
    ) -> None:
        if parsed.terminal_event_count != 1:
            raise WorkerProtocolError("Claude output requires exactly one result event")
        if parsed.terminal_error is not None:
            raise WorkerProtocolError(parsed.terminal_error)
        if not parsed.final_text or not parsed.final_text.strip():
            raise WorkerProtocolError("Claude result text is empty")
