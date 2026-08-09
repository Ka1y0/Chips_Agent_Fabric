from __future__ import annotations

import asyncio
import ipaddress
import json
import time
from collections.abc import Mapping
from contextlib import suppress
from typing import Any
from urllib.parse import urlparse

import httpx

from project_supervisor.domain import RunState

from .base import (
    EventSink,
    UnsafeWorkerRequest,
    Usage,
    WorkerAdapter,
    WorkerEvent,
    WorkerProtocolError,
    WorkerRequest,
    WorkerResult,
    event_time,
    publish_event,
    request_requires_code_write,
)
from .native import redact

_ALLOWED_ROLES = {
    "FAST_ROUTER",
    "GENERAL_REASONING",
    "RAG",
    "UNCENSORED_REVIEWER",
}


def _is_loopback(hostname: str) -> bool:
    if hostname.strip().lower() == "localhost":
        return True
    try:
        return ipaddress.ip_address(hostname).is_loopback
    except ValueError:
        return False


class LocalWorkerAdapter(WorkerAdapter):
    """Protocol-v1 HTTP boundary for the loopback/private Local Worker daemon.

    This adapter never talks directly to LM Studio and has an immutable
    read-only/non-code policy. Redirects are disabled so a worker cannot turn
    the adapter into an arbitrary URL proxy.
    """

    def __init__(
        self,
        base_url: str,
        *,
        token: str | None = None,
        poll_interval_seconds: float = 0.25,
        client: httpx.AsyncClient | None = None,
    ) -> None:
        parsed = urlparse(base_url)
        if parsed.scheme not in {"http", "https"} or not parsed.hostname:
            raise ValueError("Local Worker base_url must be an absolute HTTP(S) URL")
        if parsed.username or parsed.password or parsed.query or parsed.fragment:
            raise ValueError("Local Worker base_url must not contain credentials or query data")
        if token is not None and not token.strip():
            raise ValueError("Local Worker bearer token must not be empty")
        if not _is_loopback(parsed.hostname):
            if parsed.scheme != "https":
                raise ValueError("non-loopback Local Worker access requires HTTPS")
            if token is None:
                raise ValueError("non-loopback Local Worker access requires a bearer token")
        self.base_url = base_url.rstrip("/")
        self.token = token
        self.poll_interval_seconds = poll_interval_seconds
        self._injected_client = client
        self._job_ids: dict[str, str] = {}
        self._cancelled_runs: set[str] = set()
        self._lock = asyncio.Lock()

    @property
    def _headers(self) -> dict[str, str]:
        headers = {"accept": "application/json", "content-type": "application/json"}
        if self.token:
            headers["authorization"] = f"Bearer {self.token}"
        return headers

    async def execute(
        self,
        request: WorkerRequest,
        *,
        event_sink: EventSink | None = None,
    ) -> WorkerResult:
        self._validate_read_only(request)  # Refusal 1: public execution boundary.
        payload = self._build_job_payload(request)  # Refusal 2: protocol serialization boundary.
        started_at = event_time()
        events: list[WorkerEvent] = []
        last_response: Mapping[str, Any] = {}

        async def emit(kind: str, data: Mapping[str, Any] | None = None) -> None:
            event = WorkerEvent(request.run_id, kind, event_time(), redact(data or {}))
            events.append(event)
            await publish_event(event_sink, event)

        owns_client = self._injected_client is None
        client = self._injected_client or httpx.AsyncClient(
            base_url=self.base_url,
            headers=self._headers,
            follow_redirects=False,
            trust_env=False,
            timeout=httpx.Timeout(10.0),
        )
        job_id: str | None = None
        state = RunState.FAILED
        error: str | None = None
        try:
            health = await client.get("/v1/health")
            health.raise_for_status()
            health_envelope = self._json_object(health, "health")
            health_data = self._unwrap_data(health_envelope)
            protocol = (
                health_envelope.get("protocol")
                or health_envelope.get("protocol_version")
                or health_data.get("protocol")
                or health_data.get("protocol_version")
            )
            if protocol not in {1, "1"}:
                raise WorkerProtocolError("Local Worker does not advertise protocol v1")
            await emit("workerConnected", {"protocol": 1})

            response = await client.post("/v1/jobs", json=payload)
            response.raise_for_status()
            created = self._unwrap_data(self._json_object(response, "job creation"))
            raw_job_id = created.get("id") or created.get("jobId") or created.get("job_id")
            if not isinstance(raw_job_id, str) or not raw_job_id:
                raise WorkerProtocolError("Local Worker did not return a job id")
            job_id = raw_job_id
            async with self._lock:
                self._job_ids[request.run_id] = job_id
            await emit("remoteJobStarted", {"jobId": job_id})

            deadline = time.monotonic() + request.timeout_seconds
            while True:
                if request.run_id in self._cancelled_runs:
                    state = RunState.CANCELLED
                    error = "local worker job was cancelled"
                    break
                if time.monotonic() >= deadline:
                    state = RunState.TIMED_OUT
                    error = f"local worker exceeded {request.timeout_seconds:g} second timeout"
                    # Releasing the remote slot is best effort; it must never
                    # downgrade an observed timeout into a transport failure.
                    with suppress(httpx.HTTPError):
                        await client.post(f"/v1/jobs/{job_id}/cancel")
                    await emit("remoteJobTimedOut", {"jobId": job_id})
                    break
                status_response = await client.get(f"/v1/jobs/{job_id}")
                status_response.raise_for_status()
                last_response = self._unwrap_data(self._json_object(status_response, "job status"))
                raw_state = str(last_response.get("state") or last_response.get("status") or "")
                normalized_state = raw_state.lower()
                await emit("remoteJobStatus", {"jobId": job_id, "state": raw_state})
                if normalized_state in {"completed", "succeeded", "success"}:
                    state = RunState.COMPLETED
                    break
                if normalized_state in {"failed", "error"}:
                    state = RunState.FAILED
                    error = self._safe_error(last_response) or "local worker job failed"
                    break
                if normalized_state in {"cancelled", "canceled"}:
                    state = RunState.CANCELLED
                    error = "local worker job was cancelled"
                    break
                await asyncio.sleep(self.poll_interval_seconds)
        except httpx.HTTPStatusError as exc:
            status_code = exc.response.status_code
            state = self._status_run_state(status_code)
            error = str(redact(str(exc)))
            kind = "workerAuthRequired" if state is RunState.AUTH_REQUIRED else "workerError"
            await emit(kind, {"error": error, "status": status_code})
        except (httpx.HTTPError, WorkerProtocolError) as exc:
            error = str(redact(str(exc)))
            await emit("workerError", {"error": error})
        finally:
            async with self._lock:
                self._job_ids.pop(request.run_id, None)
                self._cancelled_runs.discard(request.run_id)
            if owns_client:
                await client.aclose()

        final_text = self._final_text(last_response)
        usage = self._usage(last_response)
        ended_at = event_time()
        await emit("remoteJobExited", {"jobId": job_id, "state": state.value})
        return WorkerResult(
            run_id=request.run_id,
            state=state,
            pid=None,
            exit_code=0 if state is RunState.COMPLETED else None,
            started_at=started_at,
            ended_at=ended_at,
            stdout=final_text,
            stderr="",
            final_text=final_text,
            events=tuple(events),
            session_id=job_id,
            model=self._model(last_response),
            usage=usage,
            error=error,
        )

    async def cancel(self, run_id: str) -> bool:
        async with self._lock:
            job_id = self._job_ids.get(run_id)
            if job_id is None:
                return False
            self._cancelled_runs.add(run_id)
        owns_client = self._injected_client is None
        client = self._injected_client or httpx.AsyncClient(
            base_url=self.base_url,
            headers=self._headers,
            follow_redirects=False,
            trust_env=False,
            timeout=httpx.Timeout(10.0),
        )
        try:
            response = await client.post(f"/v1/jobs/{job_id}/cancel")
            return response.status_code < 400
        finally:
            if owns_client:
                await client.aclose()

    @staticmethod
    def _status_run_state(status_code: int) -> RunState:
        """Map a Worker HTTP rejection onto the supervisor's run vocabulary.

        Authentication failures are not transient: the runtime marks the worker
        offline instead of retrying against an endpoint that will keep refusing.
        """

        if status_code in {401, 403}:
            return RunState.AUTH_REQUIRED
        if status_code == 429:
            return RunState.RATE_LIMITED
        return RunState.FAILED

    @staticmethod
    def _validate_read_only(request: WorkerRequest) -> None:
        if request_requires_code_write(request):
            raise UnsafeWorkerRequest("Local AI is not authorized for code-write tasks")

    @classmethod
    def _build_job_payload(cls, request: WorkerRequest) -> dict[str, Any]:
        cls._validate_read_only(request)
        if request.model is not None:
            raise UnsafeWorkerRequest("Local model selection must remain server-side")
        metadata = dict(request.metadata)
        role = metadata.get("worker_role")
        if role is None:
            labels = {str(value) for value in metadata.get("labels", [])}
            role = "FAST_ROUTER" if "fastRouting" in labels else "GENERAL_REASONING"
        if role not in _ALLOWED_ROLES:
            raise UnsafeWorkerRequest("Local Worker role is not authorized")

        schema = metadata.get("response_schema")
        if schema is not None and not isinstance(schema, Mapping):
            raise WorkerProtocolError("Local Worker response_schema must be an object")
        payload: dict[str, Any] = {
            "job_type": "inference.structured" if schema is not None else "inference.chat",
            "role": role,
            "prompt": request.prompt,
        }
        if schema is not None:
            payload["schema"] = redact(dict(schema))
        return payload

    @staticmethod
    def _json_object(response: httpx.Response, context: str) -> Mapping[str, Any]:
        try:
            value = response.json()
        except (ValueError, json.JSONDecodeError) as exc:
            raise WorkerProtocolError(f"Local Worker {context} response is not JSON") from exc
        if not isinstance(value, Mapping):
            raise WorkerProtocolError(f"Local Worker {context} response must be an object")
        return value

    @staticmethod
    def _unwrap_data(mapping: Mapping[str, Any]) -> Mapping[str, Any]:
        data = mapping.get("data")
        return data if isinstance(data, Mapping) else mapping

    @staticmethod
    def _string(mapping: Mapping[str, Any], *keys: str) -> str | None:
        for key in keys:
            value = mapping.get(key)
            if isinstance(value, str):
                return value
        return None

    @classmethod
    def _final_text(cls, data: Mapping[str, Any]) -> str:
        result = data.get("result")
        if isinstance(result, Mapping):
            structured = result.get("json")
            if isinstance(structured, (Mapping, list)):
                return json.dumps(structured, separators=(",", ":"), sort_keys=True)
            text = cls._string(result, "content", "text", "output", "response")
            if text is not None:
                return text
            content = result.get("content")
            if isinstance(content, (Mapping, list)):
                return json.dumps(content, separators=(",", ":"), sort_keys=True)
        return cls._string(data, "result", "text", "output", "response") or ""

    @classmethod
    def _model(cls, data: Mapping[str, Any]) -> str | None:
        result = data.get("result")
        if isinstance(result, Mapping):
            model = cls._string(result, "model", "modelId", "model_id")
            if model:
                return model
        return cls._string(data, "selected_model", "model", "modelId", "model_id")

    @classmethod
    def _safe_error(cls, data: Mapping[str, Any]) -> str | None:
        value = cls._string(data, "error", "message", "detail")
        return str(redact(value)) if value else None

    @staticmethod
    def _usage(data: Mapping[str, Any]) -> Usage:
        raw = data.get("usage") or data.get("metrics")
        if not isinstance(raw, Mapping):
            return Usage()

        def integer(*keys: str) -> int | None:
            return next(
                (
                    value
                    for key in keys
                    if isinstance((value := raw.get(key)), int) and not isinstance(value, bool)
                ),
                None,
            )

        cost = raw.get("costUsd") or raw.get("cost_usd")
        return Usage(
            input_tokens=integer("inputTokens", "input_tokens", "prompt_tokens"),
            output_tokens=integer("outputTokens", "output_tokens", "completion_tokens"),
            total_tokens=integer("totalTokens", "total_tokens"),
            cost_usd=float(cost) if isinstance(cost, (int, float)) else None,
            raw=redact(dict(raw)),
        )
