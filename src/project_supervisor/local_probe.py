"""Opt-in, bounded local model HTTP probes; discovery itself remains socket-free.

Only a fixed synthetic inference request is allowed. This is not a generic HTTP
client, a benchmark, a credential reader, or a runtime configuration executor.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import math
import time
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any
from urllib.parse import urlsplit, urlunsplit

import httpx

RUNTIMES = ("lmStudio", "ollama", "llamaCpp", "openAICompatible")
SMOKE_PROMPT = "Reply with exactly the word OK."


class ProbeError(ValueError):
    """A fixed, public-safe classification, never a raw server error body."""


def _model_id(value: object) -> str:
    if (
        not isinstance(value, str)
        or not value.strip()
        or len(value) > 512
        or any(ord(char) < 32 or ord(char) == 127 for char in value)
    ):
        raise ProbeError("invalidModelID")
    return value


def _endpoint(value: str, runtime: str) -> str:
    # The live transport is deliberately narrower than discovery candidates.
    # Pin localhost to IPv4 loopback without DNS or a hosts-file lookup.
    if (
        not isinstance(value, str)
        or not value
        or len(value) > 2048
        or not value.isascii()
        or any(char.isspace() or ord(char) < 32 or ord(char) == 127 for char in value)
        or any(char in value for char in "\\?#")
    ):
        raise ProbeError("invalidEndpoint")
    try:
        parsed = urlsplit(value)
        host, port = parsed.hostname, parsed.port
    except ValueError:
        raise ProbeError("invalidEndpoint") from None
    if (
        parsed.scheme not in {"http", "https"}
        or host not in {"localhost", "127.0.0.1", "::1"}
        or parsed.username is not None
        or parsed.password is not None
        or port == 0
        or parsed.netloc.endswith(":")
    ):
        raise ProbeError("invalidEndpoint")
    path = parsed.path.rstrip("/")
    if path not in ({""} if runtime == "ollama" else {"", "/v1"}):
        raise ProbeError("invalidEndpointPath")
    authority = "[::1]" if host == "::1" else "127.0.0.1"
    if port is not None:
        authority += f":{port}"
    return urlunsplit((parsed.scheme, authority, path, "", ""))


@dataclass(frozen=True, slots=True)
class ProbeOptions:
    runtime: str
    endpoint: str
    model_id: str | None = None
    allow_network: bool = False
    allow_inference: bool = False
    timeout_seconds: float = 30.0
    max_response_bytes: int = 131_072
    max_models: int = 128
    output_token_limit: int = 16
    freshness_seconds: int = 60

    def __post_init__(self) -> None:
        if self.runtime not in RUNTIMES:
            raise ProbeError("unsupportedRuntime")
        for name in ("allow_network", "allow_inference"):
            if type(getattr(self, name)) is not bool:
                raise ProbeError("invalidApprovalFlag")
        for name, maximum in (
            ("max_response_bytes", 1_048_576),
            ("max_models", 1024),
            ("output_token_limit", 32),
            ("freshness_seconds", 3600),
        ):
            value = getattr(self, name)
            if type(value) is not int or not 1 <= value <= maximum:
                raise ProbeError("invalidLimit")
        if (
            type(self.timeout_seconds) not in (int, float)
            or not 0 < self.timeout_seconds <= 120
            or not math.isfinite(self.timeout_seconds)
        ):
            raise ProbeError("invalidTimeout")
        if self.model_id is not None:
            _model_id(self.model_id)
        if self.allow_inference and self.model_id is None:
            raise ProbeError("explicitModelRequired")
        object.__setattr__(self, "endpoint", _endpoint(self.endpoint, self.runtime))

    def routes(self) -> tuple[str, str]:
        if self.runtime == "ollama":
            return self.endpoint + "/api/tags", self.endpoint + "/api/chat"
        base = self.endpoint if self.endpoint.endswith("/v1") else self.endpoint + "/v1"
        return base + "/models", base + "/chat/completions"


@dataclass(frozen=True, slots=True)
class ProbeReport:
    options: ProbeOptions
    status: str
    model_ids: tuple[str, ...] = ()
    error_code: str | None = None
    observed_at: datetime | None = None
    response_seconds: float | None = None
    output_tokens: int | None = None
    provider_tokens_per_second: float | None = None
    inference_requested: bool = False

    def to_protocol(self) -> dict[str, Any]:
        expires = (
            self.observed_at + timedelta(seconds=self.options.freshness_seconds)
            if self.observed_at is not None else None
        )
        return {
            "schemaVersion": "local-model-probe/v1",
            "runtime": self.options.runtime,
            "endpoint": self.options.endpoint,
            "status": self.status,
            "errorCode": self.error_code,
            "modelIDs": list(self.model_ids),
            "selectedModelID": self.options.model_id,
            "observedAt": self.observed_at.isoformat() if self.observed_at else None,
            "expiresAt": expires.isoformat() if expires else None,
            "inferenceVerified": self.status == "inferenceVerified",
            "inferenceRequested": self.inference_requested,
            "modelLoadMayHaveOccurred": self.inference_requested,
            "providerCancellationConfirmed": False,
            "responseSeconds": self.response_seconds,
            "outputTokens": self.output_tokens,
            "providerReportedTokensPerSecond": self.provider_tokens_per_second,
            # Non-streaming response time is not time to first token.
            "measuredTTFTSeconds": None,
            "executionLocality": "unknown",
            "credentialsInspected": False,
            "runtimeSettingsChanged": False,
            "workerRegistered": False,
            "rawOutputRetained": False,
        }

    def control_profile(self, *, now: datetime | None = None) -> dict[str, Any] | None:
        """Project into the existing profiler without inventing model capabilities."""
        if self.status not in {"catalogAvailable", "inferenceVerified"}:
            return None
        if self.options.model_id not in self.model_ids or self.observed_at is None:
            return None
        from .fabric.capabilities import ObservationFreshness, WorkerHealth
        from .local_models import LocalModelObservation, LocalModelRuntime, adapt_local_model

        current = now or datetime.now(UTC)
        if current.tzinfo is None or current.utcoffset() is None:
            raise ProbeError("naiveClock")
        age = (current - self.observed_at).total_seconds()
        fresh = 0 <= age < self.options.freshness_seconds
        observation = LocalModelObservation(
            LocalModelRuntime(self.options.runtime),
            self.options.model_id,
            observed_at=self.observed_at,
            freshness=ObservationFreshness.FRESH if fresh else ObservationFreshness.STALE,
            health=(
                WorkerHealth.HEALTHY
                if fresh and self.status == "inferenceVerified" else WorkerHealth.UNKNOWN
            ),
        )
        # Model-list visibility and a text smoke test do not prove context length,
        # tools, vision, reasoning, memory usage, or parallel capacity.
        return adapt_local_model(observation).to_protocol()


def _strict_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    value: dict[str, Any] = {}
    for key, item in pairs:
        if key in value:
            raise ProbeError("invalidResponse")
        value[key] = item
    return value


def _reject_constant(_value: str) -> None:
    raise ProbeError("invalidResponse")


async def _read_json(
    client: httpx.AsyncClient, options: ProbeOptions, method: str, url: str,
    body: dict[str, Any] | None = None,
) -> dict[str, Any]:
    async with client.stream(method, url, json=body) as response:
        if response.status_code in {401, 403}:
            raise ProbeError("authRequired")
        if 300 <= response.status_code < 400:
            raise ProbeError("redirectRefused")
        if response.status_code == 429:
            raise ProbeError("rateLimited")
        if response.status_code != 200:
            raise ProbeError("httpError")
        if response.headers.get("content-encoding", "identity").lower() != "identity":
            raise ProbeError("compressedResponseRefused")
        if response.headers.get("content-type", "").split(";", 1)[0].strip() != "application/json":
            raise ProbeError("invalidContentType")
        length = response.headers.get("content-length")
        if length is not None:
            if not length.isascii() or not length.isdigit() or len(length) > 12:
                raise ProbeError("invalidResponse")
            if int(length) > options.max_response_bytes:
                raise ProbeError("responseTooLarge")
        raw = bytearray()
        async for chunk in response.aiter_raw():
            if len(raw) + len(chunk) > options.max_response_bytes:
                raise ProbeError("responseTooLarge")
            raw.extend(chunk)
        try:
            value = json.loads(
                raw.decode("utf-8"), object_pairs_hook=_strict_object,
                parse_constant=_reject_constant,
            )
        except (ValueError, UnicodeError, RecursionError):
            raise ProbeError("invalidResponse") from None
        if not isinstance(value, dict):
            raise ProbeError("invalidResponse")
        return value


def _catalog(value: dict[str, Any], options: ProbeOptions) -> tuple[str, ...]:
    rows = value.get("models" if options.runtime == "ollama" else "data")
    if not isinstance(rows, list) or len(rows) > options.max_models:
        raise ProbeError("invalidCatalog")
    ids: list[str] = []
    for row in rows:
        if not isinstance(row, dict):
            raise ProbeError("invalidCatalog")
        ids.append(_model_id(row.get("name" if options.runtime == "ollama" else "id")))
    if len(ids) != len(set(ids)):
        raise ProbeError("ambiguousCatalog")
    return tuple(sorted(ids))


def _smoke_body(options: ProbeOptions) -> dict[str, Any]:
    value = {
        "model": options.model_id,
        "messages": [{"role": "user", "content": SMOKE_PROMPT}],
        "stream": False,
    }
    if options.runtime == "ollama":
        value["options"] = {"num_predict": options.output_token_limit, "temperature": 0}
    else:
        value.update(max_tokens=options.output_token_limit, temperature=0)
    return value


def _smoke_metrics(value: dict[str, Any], options: ProbeOptions) -> tuple[int | None, float | None]:
    # Do not accept a different model, partial response, tool request, or mere 200.
    if value.get("model") != options.model_id:
        raise ProbeError("modelMismatch")
    speed = None
    if options.runtime == "ollama":
        message = value.get("message")
        if value.get("done") is not True:
            raise ProbeError("incompleteInference")
        count = value.get("eval_count")
        duration = value.get("eval_duration")
        if (
            type(duration) is int and 0 < duration <= 10**15
            and type(count) is int and 0 <= count <= 10**9
        ):
            speed = count / duration * 1_000_000_000
    else:
        choices = value.get("choices")
        if not isinstance(choices, list) or len(choices) != 1:
            raise ProbeError("invalidResponse")
        choice = choices[0]
        if not isinstance(choice, dict) or choice.get("finish_reason") not in ("stop", "length"):
            raise ProbeError("incompleteInference")
        message = choice.get("message")
        usage = value.get("usage")
        count = usage.get("completion_tokens") if isinstance(usage, dict) else None
    if (
        not isinstance(message, dict)
        or message.get("role") != "assistant"
        or message.get("tool_calls")
        or not isinstance(message.get("content"), str)
        or message["content"].strip() != "OK"
    ):
        raise ProbeError("smokeMismatch")
    if count is not None and (type(count) is not int or not 0 <= count <= 10**9):
        raise ProbeError("invalidUsage")
    return count, speed


async def probe_local_model(
    options: ProbeOptions, *, transport: httpx.AsyncBaseTransport | None = None,
) -> ProbeReport:
    """Probe one explicitly selected endpoint. A caller may cancel the async task.

    Cancellation closes our connection, but does not prove server-side inference
    stopped. The optional transport is a trusted test seam, never CLI input.
    """
    if not options.allow_network:
        return ProbeReport(options, "dryRun")
    ids: tuple[str, ...] = ()
    inference_requested = False
    try:
        # The deadline covers connect, headers, all chunks, and both requests.
        async with asyncio.timeout(options.timeout_seconds):
            async with httpx.AsyncClient(
                trust_env=False, follow_redirects=False, transport=transport,
                timeout=options.timeout_seconds,
                headers={"Accept": "application/json", "Accept-Encoding": "identity"},
            ) as client:
                catalog_url, chat_url = options.routes()
                ids = _catalog(await _read_json(client, options, "GET", catalog_url), options)
                if options.model_id is not None and options.model_id not in ids:
                    raise ProbeError("modelNotFound")
                elapsed = count = speed = None
                if options.allow_inference:
                    inference_requested = True
                    started = time.monotonic()
                    reply = await _read_json(
                        client, options, "POST", chat_url, _smoke_body(options)
                    )
                    elapsed = time.monotonic() - started
                    count, speed = _smoke_metrics(reply, options)
                return ProbeReport(
                    options, "inferenceVerified" if inference_requested else "catalogAvailable",
                    ids, observed_at=datetime.now(UTC), response_seconds=elapsed,
                    output_tokens=count, provider_tokens_per_second=speed,
                    inference_requested=inference_requested,
                )
    except (TimeoutError, httpx.TimeoutException):
        code = "timeout"
    except httpx.RequestError:
        code = "transportError"
    except ProbeError as error:
        code = str(error)
    return ProbeReport(options, "failed", ids, code, inference_requested=inference_requested)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="chips-model-probe",
        description="Dry-run by default. Probe one local endpoint; never read credentials.",
    )
    parser.add_argument("--runtime", choices=RUNTIMES, required=True)
    parser.add_argument("--endpoint", required=True)
    parser.add_argument("--model")
    parser.add_argument("--allow-network", action="store_true", help="allow a model-list request")
    parser.add_argument(
        "--allow-inference", action="store_true",
        help="also send a fixed smoke prompt; may load a model and consume compute",
    )
    parser.add_argument("--timeout", type=float, default=30)
    args = parser.parse_args(argv)
    try:
        options = ProbeOptions(
            args.runtime, args.endpoint, args.model, args.allow_network,
            args.allow_inference, timeout_seconds=args.timeout,
        )
        report = asyncio.run(probe_local_model(options))
        output = report.to_protocol()
        output["controlProfile"] = report.control_profile()
        print(json.dumps(output, ensure_ascii=True, allow_nan=False, sort_keys=True, indent=2))
        return 1 if report.status == "failed" else 0
    except ProbeError as error:
        print(json.dumps({"status": "rejected", "errorCode": str(error)}))
        return 2
    except KeyboardInterrupt:
        print(json.dumps({"status": "cancelled", "providerCancellationConfirmed": False}))
        return 130


if __name__ == "__main__":
    raise SystemExit(main())
