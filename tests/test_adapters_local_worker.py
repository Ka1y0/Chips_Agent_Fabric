from __future__ import annotations

import asyncio
import json

import httpx
import pytest

from project_supervisor.adapters import LocalWorkerAdapter, UnsafeWorkerRequest, WorkerRequest
from project_supervisor.domain import RunState


async def test_local_worker_protocol_v1_boundary_and_normalization() -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        if request.url.path == "/v1/health":
            return httpx.Response(
                200,
                json={"protocol_version": 1, "data": {"protocol_version": 1, "status": "ok"}},
            )
        if request.method == "POST" and request.url.path == "/v1/jobs":
            body = json.loads(request.content)
            assert body == {
                "job_type": "inference.chat",
                "role": "GENERAL_REASONING",
                "prompt": "classify this",
            }
            return httpx.Response(202, json={"protocol_version": 1, "data": {"id": "job-1"}})
        if request.url.path == "/v1/jobs/job-1":
            return httpx.Response(
                200,
                json={
                    "protocol_version": 1,
                    "data": {
                        "state": "SUCCEEDED",
                        "selected_model": "gemma4-general",
                        "result": {"content": "LOCAL_OK", "model": "gemma4-general"},
                        "metrics": {
                            "prompt_tokens": 8,
                            "completion_tokens": 2,
                            "total_tokens": 10,
                        },
                    },
                },
            )
        raise AssertionError(f"unexpected request: {request.method} {request.url}")

    async with httpx.AsyncClient(
        base_url="http://127.0.0.1:7331", transport=httpx.MockTransport(handler)
    ) as client:
        adapter = LocalWorkerAdapter("http://127.0.0.1:7331", client=client)
        result = await adapter.execute(
            WorkerRequest(run_id="local-1", task_id="task-1", prompt="classify this")
        )

    assert result.succeeded
    assert result.session_id == "job-1"
    assert result.final_text == "LOCAL_OK"
    assert result.model == "gemma4-general"
    assert result.usage.total_tokens == 10
    assert [request.url.path for request in requests] == [
        "/v1/health",
        "/v1/jobs",
        "/v1/jobs/job-1",
    ]


@pytest.mark.parametrize(
    "worker_request",
    [
        WorkerRequest(run_id="direct", prompt="write", code_write_required=True),
        WorkerRequest(
            run_id="metadata",
            prompt="write",
            metadata={"requested_capabilities": ["file_write"]},
        ),
    ],
)
async def test_local_worker_refuses_code_write_before_any_http(
    worker_request: WorkerRequest,
) -> None:
    called = False

    def handler(_: httpx.Request) -> httpx.Response:
        nonlocal called
        called = True
        return httpx.Response(500)

    async with httpx.AsyncClient(
        base_url="http://127.0.0.1:7331", transport=httpx.MockTransport(handler)
    ) as client:
        adapter = LocalWorkerAdapter("http://127.0.0.1:7331", client=client)
        with pytest.raises(UnsafeWorkerRequest, match="not authorized"):
            await adapter.execute(worker_request)
    assert not called


def test_local_worker_payload_boundary_rechecks_code_write() -> None:
    request = WorkerRequest(
        run_id="payload",
        prompt="write",
        metadata={"codeWriteRequired": True},
    )
    with pytest.raises(UnsafeWorkerRequest):
        LocalWorkerAdapter._build_job_payload(request)


def test_local_worker_builds_authoritative_structured_job_without_model_override() -> None:
    schema = {
        "type": "object",
        "properties": {"summary": {"type": "string"}},
        "required": ["summary"],
        "additionalProperties": False,
    }
    payload = LocalWorkerAdapter._build_job_payload(
        WorkerRequest(
            run_id="structured",
            prompt="summarize",
            metadata={"worker_role": "GENERAL_REASONING", "response_schema": schema},
        )
    )
    assert payload == {
        "job_type": "inference.structured",
        "role": "GENERAL_REASONING",
        "prompt": "summarize",
        "schema": schema,
    }
    assert "model" not in payload


def test_local_worker_normalizes_schema_enforced_json_result() -> None:
    assert (
        LocalWorkerAdapter._final_text(
            {"result": {"enforcement": "SCHEMA_ENFORCED", "json": {"summary": "ok"}}}
        )
        == '{"summary":"ok"}'
    )


def test_local_worker_rejects_unauthorized_role() -> None:
    with pytest.raises(UnsafeWorkerRequest, match="role is not authorized"):
        LocalWorkerAdapter._build_job_payload(
            WorkerRequest(
                run_id="coding-role",
                prompt="write code",
                metadata={"worker_role": "CODING"},
            )
        )


def test_local_worker_rejects_credential_bearing_or_non_http_base_url() -> None:
    with pytest.raises(ValueError):
        LocalWorkerAdapter("file:///tmp/socket")
    with pytest.raises(ValueError):
        LocalWorkerAdapter("https://user:password@example.test/v1")


def test_non_loopback_local_worker_requires_https_and_bearer() -> None:
    with pytest.raises(ValueError, match="requires HTTPS"):
        LocalWorkerAdapter("http://worker-node.example.ts.net")
    with pytest.raises(ValueError, match="requires a bearer token"):
        LocalWorkerAdapter("https://worker-node.example.ts.net")
    with pytest.raises(ValueError, match="must not be empty"):
        LocalWorkerAdapter("https://worker-node.example.ts.net", token="   ")

    adapter = LocalWorkerAdapter(
        "https://worker-node.example.ts.net", token="test-only-placeholder"
    )
    assert adapter.base_url == "https://worker-node.example.ts.net"


async def test_owned_local_worker_clients_ignore_inherited_proxy_environment(monkeypatch) -> None:
    observed: list[bool | None] = []
    real_client = httpx.AsyncClient

    def client_factory(*args, **kwargs):
        observed.append(kwargs.get("trust_env"))
        return real_client(
            *args, transport=httpx.MockTransport(lambda _: httpx.Response(503)), **kwargs
        )

    monkeypatch.setattr(httpx, "AsyncClient", client_factory)
    adapter = LocalWorkerAdapter("http://127.0.0.1:7331")
    await adapter.execute(WorkerRequest(run_id="proxy-policy", prompt="classify"))

    assert observed == [False]


@pytest.mark.parametrize(
    ("status_code", "expected_state", "expected_kind"),
    [
        (401, RunState.AUTH_REQUIRED, "workerAuthRequired"),
        (403, RunState.AUTH_REQUIRED, "workerAuthRequired"),
        (429, RunState.RATE_LIMITED, "workerError"),
        (500, RunState.FAILED, "workerError"),
    ],
)
async def test_local_worker_classifies_http_rejections_without_leaking_the_bearer(
    status_code: int, expected_state: RunState, expected_kind: str
) -> None:
    token = "test-only-placeholder-bearer"

    def handler(_: httpx.Request) -> httpx.Response:
        return httpx.Response(status_code, json={"error": "rejected"})

    async with httpx.AsyncClient(
        base_url="http://127.0.0.1:7331", transport=httpx.MockTransport(handler)
    ) as client:
        adapter = LocalWorkerAdapter("http://127.0.0.1:7331", token=token, client=client)
        result = await adapter.execute(WorkerRequest(run_id="reject-1", prompt="classify this"))

    assert result.state is expected_state
    assert not result.succeeded
    assert expected_kind in {event.kind for event in result.events}
    serialized = json.dumps(
        [{"kind": event.kind, "payload": dict(event.payload)} for event in result.events]
    )
    assert token not in serialized
    assert token not in (result.error or "")


async def test_local_worker_cancellation_requests_remote_cancel_and_stops_polling() -> None:
    cancelled_paths: list[str] = []
    polled = asyncio.Event()

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/v1/health":
            return httpx.Response(200, json={"protocol": 1, "status": "ok"})
        if request.method == "POST" and request.url.path == "/v1/jobs":
            return httpx.Response(201, json={"jobId": "job-cancel"})
        if request.url.path == "/v1/jobs/job-cancel/cancel":
            cancelled_paths.append(request.url.path)
            return httpx.Response(202, json={"status": "cancelling"})
        if request.url.path == "/v1/jobs/job-cancel":
            polled.set()
            return httpx.Response(200, json={"status": "running"})
        raise AssertionError(f"unexpected request: {request.method} {request.url}")

    async with httpx.AsyncClient(
        base_url="http://127.0.0.1:7331", transport=httpx.MockTransport(handler)
    ) as client:
        adapter = LocalWorkerAdapter(
            "http://127.0.0.1:7331", client=client, poll_interval_seconds=0.01
        )
        execution = asyncio.create_task(
            adapter.execute(
                WorkerRequest(run_id="cancel-1", prompt="classify this", timeout_seconds=30)
            )
        )
        await asyncio.wait_for(polled.wait(), timeout=5)
        assert await adapter.cancel("cancel-1") is True
        result = await asyncio.wait_for(execution, timeout=5)

    assert result.state is RunState.CANCELLED
    assert not result.succeeded
    assert cancelled_paths == ["/v1/jobs/job-cancel/cancel"]
    assert await adapter.cancel("cancel-1") is False


async def test_local_worker_timeout_cancels_remote_job_and_reports_timed_out() -> None:
    cancelled_paths: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/v1/health":
            return httpx.Response(200, json={"protocol": 1, "status": "ok"})
        if request.method == "POST" and request.url.path == "/v1/jobs":
            return httpx.Response(201, json={"jobId": "job-timeout"})
        if request.url.path == "/v1/jobs/job-timeout/cancel":
            cancelled_paths.append(request.url.path)
            return httpx.Response(202, json={"status": "cancelling"})
        if request.url.path == "/v1/jobs/job-timeout":
            return httpx.Response(200, json={"status": "running"})
        raise AssertionError(f"unexpected request: {request.method} {request.url}")

    async with httpx.AsyncClient(
        base_url="http://127.0.0.1:7331", transport=httpx.MockTransport(handler)
    ) as client:
        adapter = LocalWorkerAdapter(
            "http://127.0.0.1:7331", client=client, poll_interval_seconds=0.01
        )
        result = await adapter.execute(
            WorkerRequest(run_id="timeout-1", prompt="classify this", timeout_seconds=0.05)
        )

    assert result.state is RunState.TIMED_OUT
    assert not result.succeeded
    assert cancelled_paths == ["/v1/jobs/job-timeout/cancel"]
    assert result.error is not None and "0.05 second timeout" in result.error
    assert "remoteJobTimedOut" in {event.kind for event in result.events}


async def test_local_worker_timeout_survives_an_unreachable_cancel_endpoint() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/v1/health":
            return httpx.Response(200, json={"protocol": 1, "status": "ok"})
        if request.method == "POST" and request.url.path == "/v1/jobs":
            return httpx.Response(201, json={"jobId": "job-gone"})
        if request.url.path == "/v1/jobs/job-gone/cancel":
            raise httpx.ConnectError("worker went away", request=request)
        if request.url.path == "/v1/jobs/job-gone":
            return httpx.Response(200, json={"status": "running"})
        raise AssertionError(f"unexpected request: {request.method} {request.url}")

    async with httpx.AsyncClient(
        base_url="http://127.0.0.1:7331", transport=httpx.MockTransport(handler)
    ) as client:
        adapter = LocalWorkerAdapter(
            "http://127.0.0.1:7331", client=client, poll_interval_seconds=0.01
        )
        result = await adapter.execute(
            WorkerRequest(run_id="timeout-2", prompt="classify this", timeout_seconds=0.05)
        )

    assert result.state is RunState.TIMED_OUT
    assert result.error is not None and "0.05 second timeout" in result.error
