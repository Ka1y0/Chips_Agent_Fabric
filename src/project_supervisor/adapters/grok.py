from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from typing import Any

from .base import Usage, WorkerRequest
from .claude import _float_value, _int_value
from .native import NativeSubprocessAdapter, ParsedOutput, redact


def _grok_usage(mapping: Mapping[str, Any], *, cost: float | None = None) -> Usage:
    return Usage(
        input_tokens=_int_value(mapping, "input_tokens", "inputTokens", "promptTokens"),
        output_tokens=_int_value(mapping, "output_tokens", "outputTokens", "completionTokens"),
        cache_creation_tokens=_int_value(mapping, "cache_creation_tokens", "cacheCreationTokens"),
        cache_read_tokens=_int_value(
            mapping, "cache_read_tokens", "cacheReadTokens", "cachedTokens"
        ),
        reasoning_tokens=_int_value(mapping, "reasoning_tokens", "reasoningTokens"),
        total_tokens=_int_value(mapping, "total_tokens", "totalTokens"),
        cost_usd=cost,
        raw=redact(dict(mapping)),
    )


class GrokAdapter(NativeSubprocessAdapter):
    """Adapter for Grok Build's verified ``streaming-json`` interface."""

    def __init__(self, executable: str = "grok", **kwargs: Any) -> None:
        super().__init__(executable, **kwargs)

    def command_arguments(self, request: WorkerRequest) -> Sequence[str]:
        arguments = ["-p", request.prompt, "--output-format", "streaming-json"]
        if request.session_id:
            arguments.extend(("--session-id", request.session_id, "--resume"))
        if request.model:
            arguments.extend(("--model", request.model))
        return arguments

    def consume_stdout_line(self, line: str, parsed: ParsedOutput) -> Mapping[str, Any]:
        if not line.strip():
            return {"providerEventType": "blank"}
        event = json.loads(line)
        if not isinstance(event, dict):
            raise TypeError("Grok event must be a JSON object")
        event_type = str(event.get("type", "unknown"))
        parsed.provider_event_types.add(event_type)
        session_id = event.get("sessionId") or event.get("session_id")
        if isinstance(session_id, str):
            parsed.session_id = session_id

        if event_type == "text":
            fragment = event.get("data")
            if isinstance(fragment, str):
                parsed.text_fragments.append(fragment)

        if event_type in {"result", "end", "completed"}:
            for key in ("result", "text", "output", "response"):
                value = event.get(key)
                if isinstance(value, str):
                    parsed.final_text = value.strip()
                    break
            cost = _float_value(event, "total_cost_usd", "cost_usd", "costUsd")
            usage = event.get("usage")
            if isinstance(usage, Mapping):
                parsed.usage = _grok_usage(usage, cost=cost)
            elif cost is not None:
                parsed.usage = Usage(cost_usd=cost)
            model_usage = event.get("modelUsage") or event.get("model_usage")
            if isinstance(model_usage, Mapping) and model_usage:
                model = next((key for key in model_usage if isinstance(key, str)), None)
                if model:
                    parsed.model = model
                    details = model_usage.get(model)
                    if isinstance(details, Mapping):
                        model_usage_value = _grok_usage(details, cost=cost)
                        previous = parsed.usage
                        parsed.usage = Usage(
                            input_tokens=model_usage_value.input_tokens or previous.input_tokens,
                            output_tokens=model_usage_value.output_tokens or previous.output_tokens,
                            cache_creation_tokens=(
                                model_usage_value.cache_creation_tokens
                                if model_usage_value.cache_creation_tokens is not None
                                else previous.cache_creation_tokens
                            ),
                            cache_read_tokens=(
                                model_usage_value.cache_read_tokens
                                if model_usage_value.cache_read_tokens is not None
                                else previous.cache_read_tokens
                            ),
                            reasoning_tokens=(
                                model_usage_value.reasoning_tokens or previous.reasoning_tokens
                            ),
                            total_tokens=model_usage_value.total_tokens or previous.total_tokens,
                            cost_usd=model_usage_value.cost_usd or previous.cost_usd,
                            raw={"usage": previous.raw, "modelUsage": model_usage_value.raw},
                        )
        model = event.get("model")
        if isinstance(model, str):
            parsed.model = model

        return {
            "providerEventType": event_type,
            "sessionId": parsed.session_id,
            "model": parsed.model,
        }
