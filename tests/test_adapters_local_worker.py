from __future__ import annotations

import asyncio
import gzip
import json
from collections.abc import AsyncIterator
from datetime import UTC, datetime

import httpx
import pytest

from project_supervisor.adapters import (
    DurableWorkerAdapter,
    IdempotentLaunchWorkerAdapter,
    LocalWorkerAdapter,
    NegotiatingWorkerAdapter,
    UnsafeWorkerRequest,
    WorkerJobHandle,
    WorkerJobIdempotencyConflict,
    WorkerJobLaunchRejected,
    WorkerJobOutcomeUncertain,
    WorkerJobState,
    WorkerProtocolError,
    WorkerRequest,
)
from project_supervisor.domain import RunState


class CountingAsyncByteStream(httpx.AsyncByteStream):
    def __init__(self, chunks: list[bytes], *, delay_seconds: float = 0.0) -> None:
        self.chunks = chunks
        self.delay_seconds = delay_seconds
        self.consumed = 0
        self.closed = False

    async def __aiter__(self) -> AsyncIterator[bytes]:
        for chunk in self.chunks:
            if self.delay_seconds:
                await asyncio.sleep(self.delay_seconds)
            self.consumed += 1
            yield chunk

    async def aclose(self) -> None:
        self.closed = True


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


def test_local_worker_rejects_oversized_http_documents_before_json_projection() -> None:
    response = httpx.Response(
        200,
        content=b"{" + b"x" * (2 * 1024 * 1024) + b"}",
        headers={"content-type": "application/json"},
    )
    with pytest.raises(WorkerProtocolError, match="capture limit"):
        LocalWorkerAdapter._json_object(response, "test")


async def test_local_worker_stops_streaming_before_consuming_all_oversized_chunks() -> None:
    stream = CountingAsyncByteStream([b"x" * (1024 * 1024) for _ in range(5)])

    def handler(_: httpx.Request) -> httpx.Response:
        return httpx.Response(200, stream=stream)

    async with httpx.AsyncClient(
        base_url="http://127.0.0.1:7331", transport=httpx.MockTransport(handler)
    ) as client:
        adapter = LocalWorkerAdapter("http://127.0.0.1:7331", client=client)
        with pytest.raises(WorkerProtocolError, match="capture limit"):
            await adapter.negotiate_job_contract()

    assert stream.consumed == 3
    assert stream.closed


async def test_local_worker_caps_decoded_compressed_response_bytes() -> None:
    encoded = gzip.compress(b"x" * (2 * 1024 * 1024 + 1))
    assert len(encoded) < 2 * 1024 * 1024
    stream = CountingAsyncByteStream([encoded])

    def handler(_: httpx.Request) -> httpx.Response:
        return httpx.Response(200, headers={"content-encoding": "gzip"}, stream=stream)

    async with httpx.AsyncClient(
        base_url="http://127.0.0.1:7331", transport=httpx.MockTransport(handler)
    ) as client:
        adapter = LocalWorkerAdapter("http://127.0.0.1:7331", client=client)
        with pytest.raises(WorkerProtocolError, match="capture limit"):
            await adapter.negotiate_job_contract()

    assert stream.consumed == 1
    assert stream.closed


async def test_local_worker_enforces_whole_response_deadline_for_periodic_chunks() -> None:
    stream = CountingAsyncByteStream([b" " for _ in range(100)], delay_seconds=0.01)

    def handler(_: httpx.Request) -> httpx.Response:
        return httpx.Response(200, stream=stream)

    async with httpx.AsyncClient(
        base_url="http://127.0.0.1:7331", transport=httpx.MockTransport(handler)
    ) as client:
        adapter = LocalWorkerAdapter(
            "http://127.0.0.1:7331",
            client=client,
            response_timeout_seconds=0.035,
        )
        with pytest.raises(httpx.ReadTimeout, match="overall deadline"):
            await adapter.negotiate_job_contract()

    assert stream.consumed < len(stream.chunks)
    assert stream.closed


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


async def test_local_worker_timeout_holds_job_when_cancel_is_not_terminal() -> None:
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
        with pytest.raises(WorkerJobOutcomeUncertain) as raised:
            await adapter.execute(
                WorkerRequest(run_id="timeout-1", prompt="classify this", timeout_seconds=0.05)
            )

    assert raised.value.state is WorkerJobState.KNOWN_RUNNING
    assert cancelled_paths == ["/v1/jobs/job-timeout/cancel"]
    assert "0.05 second timeout" in str(raised.value)


async def test_local_worker_timeout_keeps_unreachable_cancel_reconcilable() -> None:
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
        with pytest.raises(WorkerJobOutcomeUncertain) as raised:
            await adapter.execute(
                WorkerRequest(run_id="timeout-2", prompt="classify this", timeout_seconds=0.05)
            )

    assert raised.value.state is WorkerJobState.PROVIDER_UNREACHABLE
    assert "cancellation could not be confirmed" in str(raised.value)


async def test_local_worker_timeout_accepts_confirmed_terminal_cancellation() -> None:
    cancellation_requested = False

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal cancellation_requested
        if request.url.path == "/v1/health":
            return httpx.Response(200, json={"protocol": 1, "status": "ok"})
        if request.method == "POST" and request.url.path == "/v1/jobs":
            return httpx.Response(201, json={"jobId": "job-confirmed-cancel"})
        if request.url.path == "/v1/jobs/job-confirmed-cancel/cancel":
            cancellation_requested = True
            return httpx.Response(202, json={"status": "cancelling"})
        if request.url.path == "/v1/jobs/job-confirmed-cancel":
            return httpx.Response(
                200,
                json={"status": "cancelled" if cancellation_requested else "running"},
            )
        raise AssertionError(f"unexpected request: {request.method} {request.url}")

    async with httpx.AsyncClient(
        base_url="http://127.0.0.1:7331", transport=httpx.MockTransport(handler)
    ) as client:
        adapter = LocalWorkerAdapter(
            "http://127.0.0.1:7331", client=client, poll_interval_seconds=0.01
        )
        result = await adapter.execute(
            WorkerRequest(run_id="timeout-3", prompt="classify this", timeout_seconds=0.05)
        )

    assert cancellation_requested
    assert result.state is RunState.CANCELLED


def durable_handle(
    adapter: LocalWorkerAdapter,
    *,
    run_id: str = "durable-run",
    job_id: str = "durable-job",
) -> WorkerJobHandle:
    return WorkerJobHandle(
        run_id=run_id,
        adapter_type=adapter.adapter_type,
        adapter_instance_id=adapter.adapter_instance_id,
        provider_job_id=job_id,
        provider_session_id=job_id,
        created_at=datetime.now(UTC),
    )


def test_local_worker_truthfully_advertises_durable_job_capabilities_without_endpoint() -> None:
    endpoint = "https://worker-node.example.ts.net"
    token = "test-only-placeholder"
    adapter = LocalWorkerAdapter(endpoint, token=token)

    assert isinstance(adapter, DurableWorkerAdapter)
    assert adapter.adapter_type == "local-worker-http-v1"
    assert adapter.adapter_instance_id.startswith("sha256:")
    assert endpoint not in adapter.adapter_instance_id
    assert token not in adapter.adapter_instance_id
    assert adapter.job_capabilities.to_mapping() == {
        "supportsReconcile": True,
        "supportsResume": True,
        "supportsCancel": True,
        "supportsProviderIdempotency": False,
        "supportsStreamReconnect": False,
        "supportsRepeatableCollect": True,
        "supportsIdempotentLaunchLookup": False,
        "supportsDurableLaunchRegistry": False,
    }


def v2_health() -> dict:
    return {
        "data": {
            "protocol_version": 2,
            "protocol_versions": [1, 2],
            "authority_id": "authority-test-1",
            "registry_id": "registry-test-1",
            "capabilities": {
                "supports_reconcile": True,
                "supports_resume": True,
                "supports_cancel": True,
                "supports_repeatable_collect": True,
                "supports_provider_idempotency": True,
                "supports_idempotent_launch_lookup": True,
                "supports_durable_launch_registry": True,
            },
        }
    }


def v2_receipt(
    *,
    key: str,
    digest: str | None,
    state: str = "RUNNING",
    disposition: str = "DEFINITELY_LAUNCHED",
    job_id: str | None = "job-v2-1",
    accepted: bool = True,
    receipt_id: str = "receipt-v2-1",
    replayed: bool = False,
    authority_id: str = "authority-test-1",
    registry_id: str = "registry-test-1",
) -> dict:
    return {
        "data": {
            "protocol_version": 2,
            "authority_id": authority_id,
            "registry_id": registry_id,
            "idempotency_key": key,
            "request_digest": digest,
            "accepted": accepted,
            "launch_state": state,
            "disposition": disposition,
            "launch_record_id": "launch-v2-1",
            "job_id": job_id,
            "receipt_id": receipt_id,
            "replayed": replayed,
        }
    }


def v2_driver_health(
    *,
    runtime_instance_id: str = "runtime-test-1",
    profile_revision: int = 7,
    profile_fingerprint: str = "sha256:" + "a" * 64,
    supports_cancel: bool = True,
) -> dict:
    value = v2_health()
    value["data"].update(
        {
            "node_id": "node-local-test-1",
            "runtime_instance_id": runtime_instance_id,
            "default_driver_id": "claude-reviewed",
            "drivers": [
                {
                    "driver_id": "claude-reviewed",
                    "driver_type": "claude",
                    "profile_revision": profile_revision,
                    "profile_fingerprint": profile_fingerprint,
                    "available": True,
                    "supports_execution": True,
                    "supports_cancel": supports_cancel,
                },
                {
                    "driver_id": "codex-unavailable",
                    "driver_type": "codex",
                    "profile_revision": 1,
                    "profile_fingerprint": "sha256:" + "b" * 64,
                    "available": False,
                    "supports_execution": False,
                    "supports_cancel": False,
                },
            ],
        }
    )
    value["data"]["capabilities"]["supports_server_driver_profiles"] = True
    value["data"]["capabilities"]["supports_cancel"] = supports_cancel
    return value


def v2_driver_receipt(*, key: str, digest: str, state: str = "RUNNING") -> dict:
    value = v2_receipt(
        key=key,
        digest=digest,
        state=state,
        disposition=("DEFINITELY_NOT_LAUNCHED" if state == "NOT_SEEN" else "DEFINITELY_LAUNCHED"),
        job_id=None if state == "NOT_SEEN" else "job-v2-driver-1",
        accepted=state != "NOT_SEEN",
        receipt_id="receipt-not-seen" if state == "NOT_SEEN" else "receipt-driver-1",
    )
    value["data"].update(
        {
            "driver_id": "claude-reviewed",
            "driver_type": "claude",
            "driver_profile_revision": 7,
            "driver_profile_fingerprint": "sha256:" + "a" * 64,
            "launch_runtime_instance_id": "runtime-test-1",
        }
    )
    return value


async def test_v2_negotiation_truthfully_upgrades_contract_and_binds_registry_authority() -> None:
    endpoint = "http://127.0.0.1:7331"

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/v1/health"
        return httpx.Response(200, json=v2_health())

    async with httpx.AsyncClient(
        base_url=endpoint, transport=httpx.MockTransport(handler)
    ) as client:
        adapter = LocalWorkerAdapter(endpoint, client=client)
        v1_instance = adapter.adapter_instance_id
        capabilities = await adapter.negotiate_job_contract()

    assert isinstance(adapter, NegotiatingWorkerAdapter)
    assert isinstance(adapter, IdempotentLaunchWorkerAdapter)
    assert adapter.protocol_version == 2
    assert adapter.adapter_type == "local-worker-http-v2"
    assert adapter.adapter_instance_id != v1_instance
    assert "authority-test-1" not in adapter.adapter_instance_id
    assert "registry-test-1" not in adapter.adapter_instance_id
    assert capabilities.supports_provider_idempotency
    assert capabilities.supports_idempotent_launch_lookup
    assert capabilities.supports_durable_launch_registry


async def test_v2_registered_driver_is_operator_selected_and_identity_is_restart_stable() -> None:
    endpoint = "http://127.0.0.1:7331"

    def client_for(runtime_instance_id: str) -> httpx.AsyncClient:
        return httpx.AsyncClient(
            base_url=endpoint,
            transport=httpx.MockTransport(
                lambda _: httpx.Response(
                    200,
                    json=v2_driver_health(runtime_instance_id=runtime_instance_id),
                )
            ),
        )

    async with client_for("runtime-a") as first_client, client_for("runtime-b") as second_client:
        first = LocalWorkerAdapter(endpoint, client=first_client, driver_id="claude-reviewed")
        second = LocalWorkerAdapter(endpoint, client=second_client, driver_id="claude-reviewed")
        await first.negotiate_job_contract()
        await second.negotiate_job_contract()

    assert first.adapter_instance_id == second.adapter_instance_id
    assert first.node_id == second.node_id == "node-local-test-1"
    assert first.runtime_instance_id == "runtime-a"
    assert second.runtime_instance_id == "runtime-b"
    assert first.driver_profile == {
        "driver_id": "claude-reviewed",
        "driver_type": "claude",
        "profile_revision": 7,
        "profile_fingerprint": "sha256:" + "a" * 64,
        "available": True,
        "supports_execution": True,
        "supports_cancel": True,
    }
    assert "claude-reviewed" not in first.adapter_instance_id


async def test_v2_driver_launch_digest_and_wire_shape_exclude_generic_process_control() -> None:
    key = "supervisor-execution:driver-run"
    request = WorkerRequest(
        run_id="driver-run",
        task_id="driver-task",
        prompt="summarize",
        metadata={
            "worker_role": "GENERAL_REASONING",
            "executable": "/tmp/forbidden",
            "argv": ["--forbidden"],
            "env": {"TOKEN": "secret"},
            "cwd": "/tmp",
        },
    )
    digest = LocalWorkerAdapter.request_digest(request, driver_id="claude-reviewed")
    posts: list[dict] = []

    def handler(http_request: httpx.Request) -> httpx.Response:
        if http_request.url.path == "/v1/health":
            return httpx.Response(200, json=v2_driver_health())
        if http_request.method == "GET":
            assert http_request.url.params["driver_id"] == "claude-reviewed"
            return httpx.Response(
                404,
                json=v2_driver_receipt(key=key, digest=digest, state="NOT_SEEN"),
            )
        if http_request.method == "POST" and http_request.url.path == "/v2/launches":
            body = json.loads(http_request.content)
            posts.append(body)
            assert body["request_digest"] == digest
            assert body["job"] == {
                "driver_id": "claude-reviewed",
                "job_type": "inference.chat",
                "role": "GENERAL_REASONING",
                "prompt": "summarize",
            }
            return httpx.Response(201, json=v2_driver_receipt(key=key, digest=digest))
        raise AssertionError(f"unexpected request: {http_request.method} {http_request.url}")

    async with httpx.AsyncClient(
        base_url="http://127.0.0.1:7331", transport=httpx.MockTransport(handler)
    ) as client:
        adapter = LocalWorkerAdapter(
            "http://127.0.0.1:7331", client=client, driver_id="claude-reviewed"
        )
        handle = await adapter.start_job(request, idempotency_key=key)

    assert handle.provider_job_id == "job-v2-driver-1"
    assert handle.metadata["driverID"] == "claude-reviewed"
    assert handle.metadata["nodeID"] == "node-local-test-1"
    assert len(posts) == 1


async def test_configured_driver_never_falls_back_to_protocol_v1() -> None:
    calls: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request.url.path)
        return httpx.Response(200, json={"protocol_version": 1})

    async with httpx.AsyncClient(
        base_url="http://127.0.0.1:7331", transport=httpx.MockTransport(handler)
    ) as client:
        adapter = LocalWorkerAdapter(
            "http://127.0.0.1:7331", client=client, driver_id="claude-reviewed"
        )
        with pytest.raises(WorkerProtocolError, match="registered-driver contract"):
            await adapter.start_job(
                WorkerRequest(run_id="driver-v1-refused", prompt="summarize"),
                idempotency_key="driver-v1-refused",
            )

    assert calls == ["/v1/health"]


async def test_v2_registered_driver_cancel_capability_is_not_overstated() -> None:
    async with httpx.AsyncClient(
        base_url="http://127.0.0.1:7331",
        transport=httpx.MockTransport(
            lambda _: httpx.Response(200, json=v2_driver_health(supports_cancel=False))
        ),
    ) as client:
        adapter = LocalWorkerAdapter(
            "http://127.0.0.1:7331", client=client, driver_id="claude-reviewed"
        )
        capabilities = await adapter.negotiate_job_contract()

    assert adapter.protocol_version == 2
    assert not capabilities.supports_cancel
    assert capabilities.supports_provider_idempotency


async def test_partial_v2_capabilities_do_not_overstate_resume_or_cancel_support() -> None:
    advertised = v2_health()
    advertised["data"]["capabilities"].pop("supports_resume")

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/v1/health"
        return httpx.Response(200, json=advertised)

    async with httpx.AsyncClient(
        base_url="http://127.0.0.1:7331", transport=httpx.MockTransport(handler)
    ) as client:
        adapter = LocalWorkerAdapter("http://127.0.0.1:7331", client=client)
        capabilities = await adapter.negotiate_job_contract()

    assert adapter.protocol_version == 1
    assert not capabilities.supports_provider_idempotency
    assert not capabilities.supports_idempotent_launch_lookup
    assert not capabilities.supports_durable_launch_registry


async def test_failed_contract_negotiation_freezes_safe_v1_failure_until_next_probe() -> None:
    requests = 0

    def handler(_: httpx.Request) -> httpx.Response:
        nonlocal requests
        requests += 1
        return httpx.Response(401, json={"error": "unauthorized"})

    async with httpx.AsyncClient(
        base_url="http://127.0.0.1:7331", transport=httpx.MockTransport(handler)
    ) as client:
        adapter = LocalWorkerAdapter("http://127.0.0.1:7331", client=client)
        with pytest.raises(WorkerJobLaunchRejected) as negotiated:
            await adapter.negotiate_job_contract()
        with pytest.raises(WorkerJobLaunchRejected) as started:
            await adapter.start_job(
                WorkerRequest(run_id="frozen-auth", prompt="classify"),
                idempotency_key="frozen-auth",
            )

    assert negotiated.value.status_code == started.value.status_code == 401
    assert adapter.protocol_version == 1
    assert not adapter.job_capabilities.supports_provider_idempotency
    assert requests == 1


async def test_v2_duplicate_launch_recovers_existing_handle_without_second_post() -> None:
    key = "supervisor-execution:run-v2-1"
    request = WorkerRequest(run_id="run-v2-1", task_id="task-v2-1", prompt="classify")
    digest = LocalWorkerAdapter.request_digest(request)
    launch_exists = False
    posts = 0

    def handler(http_request: httpx.Request) -> httpx.Response:
        nonlocal launch_exists, posts
        if http_request.url.path == "/v1/health":
            return httpx.Response(200, json=v2_health())
        if http_request.method == "GET" and http_request.url.path.startswith("/v2/launches/"):
            if not launch_exists:
                return httpx.Response(
                    404,
                    json=v2_receipt(
                        key=key,
                        digest=None,
                        state="NOT_SEEN",
                        disposition="DEFINITELY_NOT_LAUNCHED",
                        job_id=None,
                        accepted=False,
                    ),
                )
            return httpx.Response(
                200,
                json=v2_receipt(key=key, digest=digest, replayed=True),
            )
        if http_request.method == "POST" and http_request.url.path == "/v2/launches":
            posts += 1
            body = json.loads(http_request.content)
            assert body["authority_id"] == "authority-test-1"
            assert body["registry_id"] == "registry-test-1"
            assert body["idempotency_key"] == key
            assert body["request_digest"] == digest
            assert body["job"]["prompt"] == "classify"
            launch_exists = True
            return httpx.Response(201, json=v2_receipt(key=key, digest=digest))
        raise AssertionError(f"unexpected request: {http_request.method} {http_request.url}")

    async with httpx.AsyncClient(
        base_url="http://127.0.0.1:7331", transport=httpx.MockTransport(handler)
    ) as client:
        adapter = LocalWorkerAdapter("http://127.0.0.1:7331", client=client)
        first = await adapter.start_job(request, idempotency_key=key)
        second = await adapter.start_job(request, idempotency_key=key)

    assert first.provider_job_id == second.provider_job_id == "job-v2-1"
    assert first.schema_version == second.schema_version == 2
    assert posts == 1


async def test_v2_registry_rotation_before_lookup_cannot_authorize_launch() -> None:
    key = "supervisor-execution:run-v2-rotated-before-lookup"
    request = WorkerRequest(run_id="run-v2-rotated-before-lookup", prompt="classify")
    posts = 0

    def handler(http_request: httpx.Request) -> httpx.Response:
        nonlocal posts
        if http_request.url.path == "/v1/health":
            return httpx.Response(200, json=v2_health())
        if http_request.method == "GET":
            return httpx.Response(
                404,
                json=v2_receipt(
                    key=key,
                    digest=None,
                    state="NOT_SEEN",
                    disposition="DEFINITELY_NOT_LAUNCHED",
                    job_id=None,
                    accepted=False,
                    authority_id="authority-replacement",
                    registry_id="registry-replacement",
                ),
            )
        posts += 1
        return httpx.Response(500)

    async with httpx.AsyncClient(
        base_url="http://127.0.0.1:7331", transport=httpx.MockTransport(handler)
    ) as client:
        adapter = LocalWorkerAdapter("http://127.0.0.1:7331", client=client)
        with pytest.raises(WorkerJobOutcomeUncertain):
            await adapter.start_job(request, idempotency_key=key)

    assert posts == 0


async def test_v2_registry_rotation_between_lookup_and_post_is_fenced() -> None:
    key = "supervisor-execution:run-v2-rotated-before-post"
    request = WorkerRequest(run_id="run-v2-rotated-before-post", prompt="classify")
    lookups = 0
    posts = 0
    replacement_launches = 0

    def handler(http_request: httpx.Request) -> httpx.Response:
        nonlocal lookups, posts, replacement_launches
        if http_request.url.path == "/v1/health":
            return httpx.Response(200, json=v2_health())
        if http_request.method == "GET":
            lookups += 1
            rotated = lookups > 1
            return httpx.Response(
                404,
                json=v2_receipt(
                    key=key,
                    digest=None,
                    state="NOT_SEEN",
                    disposition="DEFINITELY_NOT_LAUNCHED",
                    job_id=None,
                    accepted=False,
                    authority_id=("authority-replacement" if rotated else "authority-test-1"),
                    registry_id=("registry-replacement" if rotated else "registry-test-1"),
                ),
            )
        posts += 1
        body = json.loads(http_request.content)
        assert body["authority_id"] == "authority-test-1"
        assert body["registry_id"] == "registry-test-1"
        # A replacement daemon rejects the stale expected authority before reserving the key.
        return httpx.Response(
            409,
            json={
                "protocol_version": 2,
                "error": {
                    "code": "LAUNCH_AUTHORITY_MISMATCH",
                    "message": "launch authority changed before reservation",
                },
                "data": {
                    "protocol_version": 2,
                    "authority_id": "authority-replacement",
                    "registry_id": "registry-replacement",
                    "idempotency_key": key,
                    "launch_state": "UNKNOWN",
                    "disposition": "LAUNCH_OUTCOME_UNKNOWN",
                },
            },
        )

    async with httpx.AsyncClient(
        base_url="http://127.0.0.1:7331", transport=httpx.MockTransport(handler)
    ) as client:
        adapter = LocalWorkerAdapter("http://127.0.0.1:7331", client=client)
        with pytest.raises(WorkerJobOutcomeUncertain):
            await adapter.start_job(request, idempotency_key=key)

    assert lookups == 2
    assert posts == 1
    assert replacement_launches == 0


async def test_v2_handle_uses_v2_job_route_and_omits_secret_receipt_metadata() -> None:
    key = "supervisor-execution:run-v2-collect"
    request = WorkerRequest(run_id="run-v2-collect", prompt="classify")
    digest = LocalWorkerAdapter.request_digest(request)
    paths: list[str] = []

    def handler(http_request: httpx.Request) -> httpx.Response:
        paths.append(http_request.url.path)
        if http_request.url.path == "/v1/health":
            return httpx.Response(200, json=v2_health())
        if http_request.url.path.startswith("/v2/launches/"):
            receipt = v2_receipt(
                key=key,
                digest=digest,
                state="COMPLETED",
            )
            receipt["data"]["access_token"] = "must-not-enter-handle"
            return httpx.Response(200, json=receipt)
        if http_request.url.path == "/v2/jobs/job-v2-1":
            return httpx.Response(
                200,
                json={"data": {"state": "SUCCEEDED", "result": {"text": "V2_OK"}}},
            )
        raise AssertionError(f"unexpected request: {http_request.method} {http_request.url}")

    async with httpx.AsyncClient(
        base_url="http://127.0.0.1:7331", transport=httpx.MockTransport(handler)
    ) as client:
        adapter = LocalWorkerAdapter("http://127.0.0.1:7331", client=client)
        handle = await adapter.start_job(request, idempotency_key=key)
        result = await adapter.collect_job(request, handle)

    assert result.final_text == "V2_OK"
    assert "must-not-enter-handle" not in json.dumps(dict(handle.metadata))
    assert paths.count("/v2/jobs/job-v2-1") == 1
    assert "/v2/launches" not in paths


async def test_v2_conflicting_replay_fails_closed_without_post() -> None:
    key = "supervisor-execution:run-v2-conflict"
    request = WorkerRequest(run_id="run-v2-conflict", prompt="new payload")
    posts = 0

    def handler(http_request: httpx.Request) -> httpx.Response:
        nonlocal posts
        if http_request.url.path == "/v1/health":
            return httpx.Response(200, json=v2_health())
        if http_request.method == "GET":
            return httpx.Response(
                200,
                json=v2_receipt(key=key, digest="sha256:" + "0" * 64),
            )
        posts += 1
        return httpx.Response(500)

    async with httpx.AsyncClient(
        base_url="http://127.0.0.1:7331", transport=httpx.MockTransport(handler)
    ) as client:
        adapter = LocalWorkerAdapter("http://127.0.0.1:7331", client=client)
        with pytest.raises(WorkerJobIdempotencyConflict):
            await adapter.start_job(request, idempotency_key=key)

    assert posts == 0


async def test_v2_direct_409_conflict_envelope_is_not_retried() -> None:
    key = "supervisor-execution:run-v2-direct-conflict"
    request = WorkerRequest(run_id="run-v2-direct-conflict", prompt="new payload")
    posts = 0

    def handler(http_request: httpx.Request) -> httpx.Response:
        nonlocal posts
        if http_request.url.path == "/v1/health":
            return httpx.Response(200, json=v2_health())
        if http_request.method == "GET":
            return httpx.Response(
                404,
                json=v2_receipt(
                    key=key,
                    digest=None,
                    state="NOT_SEEN",
                    disposition="DEFINITELY_NOT_LAUNCHED",
                    job_id=None,
                    accepted=False,
                ),
            )
        posts += 1
        return httpx.Response(
            409,
            json={
                "protocol_version": 2,
                "error": {
                    "code": "IDEMPOTENCY_CONFLICT",
                    "message": "key belongs to a different digest",
                },
            },
        )

    async with httpx.AsyncClient(
        base_url="http://127.0.0.1:7331", transport=httpx.MockTransport(handler)
    ) as client:
        adapter = LocalWorkerAdapter("http://127.0.0.1:7331", client=client)
        with pytest.raises(WorkerJobIdempotencyConflict):
            await adapter.start_job(request, idempotency_key=key)

    assert posts == 1


async def test_v2_lost_launch_response_recovers_by_key_without_redispatch() -> None:
    key = "supervisor-execution:run-v2-lost"
    request = WorkerRequest(run_id="run-v2-lost", prompt="classify")
    digest = LocalWorkerAdapter.request_digest(request)
    accepted = False
    posts = 0

    def handler(http_request: httpx.Request) -> httpx.Response:
        nonlocal accepted, posts
        if http_request.url.path == "/v1/health":
            return httpx.Response(200, json=v2_health())
        if http_request.method == "GET":
            if accepted:
                return httpx.Response(200, json=v2_receipt(key=key, digest=digest))
            return httpx.Response(
                404,
                json=v2_receipt(
                    key=key,
                    digest=None,
                    state="NOT_SEEN",
                    disposition="DEFINITELY_NOT_LAUNCHED",
                    job_id=None,
                    accepted=False,
                ),
            )
        if http_request.method == "POST":
            posts += 1
            accepted = True
            raise httpx.ReadError("response lost", request=http_request)
        raise AssertionError("unexpected request")

    async with httpx.AsyncClient(
        base_url="http://127.0.0.1:7331", transport=httpx.MockTransport(handler)
    ) as client:
        adapter = LocalWorkerAdapter("http://127.0.0.1:7331", client=client)
        handle = await adapter.start_job(request, idempotency_key=key)

    assert handle.provider_job_id == "job-v2-1"
    assert posts == 1


async def test_v2_reserved_definitely_not_launched_safely_replays_same_request() -> None:
    key = "supervisor-execution:run-v2-reserved"
    request = WorkerRequest(run_id="run-v2-reserved", prompt="classify")
    digest = LocalWorkerAdapter.request_digest(request)
    posts: list[dict] = []

    def handler(http_request: httpx.Request) -> httpx.Response:
        if http_request.url.path == "/v1/health":
            return httpx.Response(200, json=v2_health())
        if http_request.method == "GET":
            return httpx.Response(
                200,
                json=v2_receipt(
                    key=key,
                    digest=digest,
                    state="RESERVED",
                    disposition="DEFINITELY_NOT_LAUNCHED",
                ),
            )
        body = json.loads(http_request.content)
        posts.append(body)
        return httpx.Response(200, json=v2_receipt(key=key, digest=digest, replayed=True))

    async with httpx.AsyncClient(
        base_url="http://127.0.0.1:7331", transport=httpx.MockTransport(handler)
    ) as client:
        adapter = LocalWorkerAdapter("http://127.0.0.1:7331", client=client)
        handle = await adapter.start_job(request, idempotency_key=key)

    assert handle.provider_job_id == "job-v2-1"
    assert len(posts) == 1
    assert posts[0]["idempotency_key"] == key
    assert posts[0]["request_digest"] == digest


async def test_v2_launching_unknown_is_not_mistaken_for_reattachable_handle() -> None:
    key = "supervisor-execution:run-v2-unknown"
    request = WorkerRequest(run_id="run-v2-unknown", prompt="classify")
    digest = LocalWorkerAdapter.request_digest(request)
    posts = 0

    def handler(http_request: httpx.Request) -> httpx.Response:
        nonlocal posts
        if http_request.url.path == "/v1/health":
            return httpx.Response(200, json=v2_health())
        if http_request.method == "GET":
            return httpx.Response(
                200,
                json=v2_receipt(
                    key=key,
                    digest=digest,
                    state="LAUNCHING",
                    disposition="LAUNCH_OUTCOME_UNKNOWN",
                ),
            )
        posts += 1
        return httpx.Response(500)

    async with httpx.AsyncClient(
        base_url="http://127.0.0.1:7331", transport=httpx.MockTransport(handler)
    ) as client:
        adapter = LocalWorkerAdapter("http://127.0.0.1:7331", client=client)
        with pytest.raises(WorkerJobOutcomeUncertain):
            await adapter.start_job(request, idempotency_key=key)

    assert posts == 0


async def test_v2_launching_with_start_receipt_is_reattachable_without_post() -> None:
    key = "supervisor-execution:run-v2-launching"
    request = WorkerRequest(run_id="run-v2-launching", prompt="classify")
    digest = LocalWorkerAdapter.request_digest(request)
    posts = 0

    def handler(http_request: httpx.Request) -> httpx.Response:
        nonlocal posts
        if http_request.url.path == "/v1/health":
            return httpx.Response(200, json=v2_health())
        if http_request.method == "GET":
            return httpx.Response(
                200,
                json=v2_receipt(
                    key=key,
                    digest=digest,
                    state="LAUNCHING",
                    disposition="LAUNCH_IN_PROGRESS",
                ),
            )
        posts += 1
        return httpx.Response(500)

    async with httpx.AsyncClient(
        base_url="http://127.0.0.1:7331", transport=httpx.MockTransport(handler)
    ) as client:
        adapter = LocalWorkerAdapter("http://127.0.0.1:7331", client=client)
        handle = await adapter.start_job(request, idempotency_key=key)

    assert handle.provider_job_id == "job-v2-1"
    assert posts == 0


async def test_v2_prelaunch_rejection_requires_authoritative_receipt() -> None:
    key = "supervisor-execution:run-v2-reject"
    request = WorkerRequest(run_id="run-v2-reject", prompt="classify")
    digest = LocalWorkerAdapter.request_digest(request)

    def handler(http_request: httpx.Request) -> httpx.Response:
        if http_request.url.path == "/v1/health":
            return httpx.Response(200, json=v2_health())
        if http_request.method == "GET":
            return httpx.Response(
                404,
                json=v2_receipt(
                    key=key,
                    digest=None,
                    state="NOT_SEEN",
                    disposition="DEFINITELY_NOT_LAUNCHED",
                    job_id=None,
                    accepted=False,
                ),
            )
        receipt = v2_receipt(
            key=key,
            digest=digest,
            state="REJECTED_PRE_LAUNCH",
            disposition="DEFINITELY_NOT_LAUNCHED",
            job_id=None,
            accepted=False,
            receipt_id="reject-receipt-v2",
        )
        receipt["data"]["launch_error"] = "UNSUPPORTED_WORKER_ROLE"
        receipt["error"] = {
            "code": "PRE_LAUNCH_REJECTED",
            "message": "rejected before external execution",
        }
        return httpx.Response(422, json=receipt)

    async with httpx.AsyncClient(
        base_url="http://127.0.0.1:7331", transport=httpx.MockTransport(handler)
    ) as client:
        adapter = LocalWorkerAdapter("http://127.0.0.1:7331", client=client)
        with pytest.raises(WorkerJobLaunchRejected) as raised:
            await adapter.start_job(request, idempotency_key=key)

    assert raised.value.status_code == 422
    assert raised.value.receipt_id == "reject-receipt-v2"
    assert raised.value.request_digest == digest
    assert raised.value.reason_code == "PRE_LAUNCH_REJECTED"
    assert "UNSUPPORTED_WORKER_ROLE" in str(raised.value)


async def test_v2_unavailable_registry_is_unknown_and_never_launches() -> None:
    key = "supervisor-execution:run-v2-unavailable"
    posts = 0

    def handler(http_request: httpx.Request) -> httpx.Response:
        nonlocal posts
        if http_request.url.path == "/v1/health":
            return httpx.Response(200, json=v2_health())
        if http_request.method == "GET":
            return httpx.Response(
                503,
                json=v2_receipt(
                    key=key,
                    digest=None,
                    state="UNKNOWN",
                    disposition="LAUNCH_OUTCOME_UNKNOWN",
                    job_id=None,
                    accepted=False,
                ),
            )
        posts += 1
        return httpx.Response(500)

    async with httpx.AsyncClient(
        base_url="http://127.0.0.1:7331", transport=httpx.MockTransport(handler)
    ) as client:
        adapter = LocalWorkerAdapter("http://127.0.0.1:7331", client=client)
        with pytest.raises(WorkerJobOutcomeUncertain):
            await adapter.start_job(
                WorkerRequest(run_id="run-v2-unavailable", prompt="classify"),
                idempotency_key=key,
            )

    assert posts == 0


def test_v2_request_digest_is_stable_and_excludes_credentials() -> None:
    request = WorkerRequest(
        run_id="digest-run",
        task_id="digest-task",
        prompt="classify",
        metadata={"worker_role": "GENERAL_REASONING"},
    )
    first = LocalWorkerAdapter.request_digest(request)
    second = LocalWorkerAdapter.request_digest(request)
    changed = LocalWorkerAdapter.request_digest(
        WorkerRequest(run_id="digest-run", task_id="digest-task", prompt="different")
    )

    assert first == second
    assert first.startswith("sha256:")
    assert len(first) == 71
    assert changed != first


async def test_start_job_returns_durable_identity_without_inventing_provider_idempotency() -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        if request.url.path == "/v1/health":
            return httpx.Response(200, json={"protocol": 1})
        if request.url.path == "/v1/jobs":
            body = json.loads(request.content)
            assert set(body) == {"job_type", "role", "prompt"}
            assert "idempotency-key" not in request.headers
            return httpx.Response(
                201,
                json={
                    "jobId": "provider-job-1",
                    "access_token": "must-not-enter-handle",
                },
            )
        raise AssertionError(f"unexpected request: {request.method} {request.url}")

    async with httpx.AsyncClient(
        base_url="http://127.0.0.1:7331", transport=httpx.MockTransport(handler)
    ) as client:
        adapter = LocalWorkerAdapter("http://127.0.0.1:7331", client=client)
        handle = await adapter.start_job(
            WorkerRequest(run_id="durable-run", prompt="classify"),
            idempotency_key="execution-attempt-1",
        )

    serialized = json.dumps(
        {
            "adapterType": handle.adapter_type,
            "adapterInstanceID": handle.adapter_instance_id,
            "providerJobID": handle.provider_job_id,
            "metadata": dict(handle.metadata),
        }
    )
    assert handle.run_id == "durable-run"
    assert handle.provider_job_id == "provider-job-1"
    assert "must-not-enter-handle" not in serialized
    assert "127.0.0.1" not in serialized
    assert [request.url.path for request in requests] == ["/v1/health", "/v1/jobs"]


async def test_start_job_post_rejection_remains_ambiguous_without_a_rejection_receipt() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/v1/health":
            return httpx.Response(200, json={"protocol": 1})
        if request.url.path == "/v1/jobs":
            return httpx.Response(404, json={"error": "late rejection"})
        raise AssertionError(f"unexpected request: {request.method} {request.url}")

    async with httpx.AsyncClient(
        base_url="http://127.0.0.1:7331", transport=httpx.MockTransport(handler)
    ) as client:
        adapter = LocalWorkerAdapter("http://127.0.0.1:7331", client=client)
        with pytest.raises(httpx.HTTPStatusError):
            await adapter.start_job(
                WorkerRequest(run_id="ambiguous-post", prompt="classify"),
                idempotency_key="execution-attempt-ambiguous",
            )


async def test_new_adapter_instance_resumes_existing_job_without_duplicate_launch() -> None:
    launch_count = 0
    poll_count = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal launch_count, poll_count
        if request.url.path == "/v1/health":
            return httpx.Response(200, json={"protocol": 1})
        if request.method == "POST" and request.url.path == "/v1/jobs":
            launch_count += 1
            return httpx.Response(201, json={"jobId": "restart-job"})
        if request.url.path == "/v1/jobs/restart-job":
            poll_count += 1
            if poll_count == 1:
                return httpx.Response(200, json={"status": "running"})
            return httpx.Response(
                200,
                json={"status": "succeeded", "result": {"text": "REATTACHED_OK"}},
            )
        raise AssertionError(f"unexpected request: {request.method} {request.url}")

    async with httpx.AsyncClient(
        base_url="http://127.0.0.1:7331", transport=httpx.MockTransport(handler)
    ) as client:
        first_process = LocalWorkerAdapter("http://127.0.0.1:7331", client=client)
        request = WorkerRequest(run_id="restart-run", prompt="classify", timeout_seconds=5)
        handle = await first_process.start_job(request, idempotency_key="restart-run")

        restarted_process = LocalWorkerAdapter(
            "http://127.0.0.1:7331", client=client, poll_interval_seconds=0.01
        )
        result = await restarted_process.resume_job(request, handle)

    assert result.succeeded
    assert result.final_text == "REATTACHED_OK"
    assert launch_count == 1
    assert poll_count == 2


@pytest.mark.parametrize(
    ("status_code", "payload", "expected"),
    [
        (200, {"status": "running"}, WorkerJobState.KNOWN_RUNNING),
        (200, {"status": "succeeded"}, WorkerJobState.KNOWN_COMPLETED),
        (200, {"status": "failed"}, WorkerJobState.KNOWN_FAILED),
        (200, {"status": "cancelled"}, WorkerJobState.KNOWN_CANCELLED),
        (404, {"error": "missing"}, WorkerJobState.PROVIDER_NOT_FOUND),
        (503, {"error": "offline"}, WorkerJobState.PROVIDER_UNREACHABLE),
        (401, {"error": "unauthorized"}, WorkerJobState.UNKNOWN),
        (200, {"status": "unexpected"}, WorkerJobState.UNKNOWN),
    ],
)
async def test_reconcile_job_distinguishes_provider_states(
    status_code: int,
    payload: dict[str, str],
    expected: WorkerJobState,
) -> None:
    def handler(_: httpx.Request) -> httpx.Response:
        return httpx.Response(status_code, json=payload)

    async with httpx.AsyncClient(
        base_url="http://127.0.0.1:7331", transport=httpx.MockTransport(handler)
    ) as client:
        adapter = LocalWorkerAdapter("http://127.0.0.1:7331", client=client)
        observation = await adapter.reconcile_job(durable_handle(adapter))

    assert observation.state is expected


async def test_reconcile_transport_failure_is_unreachable_not_missing() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("offline", request=request)

    async with httpx.AsyncClient(
        base_url="http://127.0.0.1:7331", transport=httpx.MockTransport(handler)
    ) as client:
        adapter = LocalWorkerAdapter("http://127.0.0.1:7331", client=client)
        observation = await adapter.reconcile_job(durable_handle(adapter))

    assert observation.state is WorkerJobState.PROVIDER_UNREACHABLE
    assert "ConnectError" in (observation.detail or "")


@pytest.mark.parametrize(
    ("provider_code", "expected"),
    [
        ("PROVIDER_AUTH_REQUIRED", RunState.AUTH_REQUIRED),
        ("PROVIDER_RATE_LIMITED", RunState.RATE_LIMITED),
        ("PROVIDER_TIMEOUT", RunState.TIMED_OUT),
        ("PROVIDER_EXIT_NONZERO", RunState.FAILED),
    ],
)
def test_terminal_provider_failure_codes_preserve_exact_run_classification(
    provider_code: str, expected: RunState
) -> None:
    state, error = LocalWorkerAdapter._terminal_run_state(
        WorkerJobState.KNOWN_FAILED,
        {"error": provider_code},
    )

    assert state is expected
    assert error == provider_code


async def test_completed_job_collection_is_repeatable_and_does_not_relaunch() -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(
            200,
            json={
                "status": "succeeded",
                "result": {"text": "COLLECTED_ONCE"},
                "usage": {"totalTokens": 7},
                "authorization": "must-not-enter-handle-metadata",
            },
        )

    async with httpx.AsyncClient(
        base_url="http://127.0.0.1:7331", transport=httpx.MockTransport(handler)
    ) as client:
        adapter = LocalWorkerAdapter("http://127.0.0.1:7331", client=client)
        handle = durable_handle(adapter)
        first = await adapter.collect_job(
            WorkerRequest(run_id=handle.run_id, prompt="collect"), handle
        )
        second = await adapter.collect_job(
            WorkerRequest(run_id=handle.run_id, prompt="collect"), handle
        )

    assert first.final_text == second.final_text == "COLLECTED_ONCE"
    assert first.usage.total_tokens == second.usage.total_tokens == 7
    assert [request.method for request in requests] == ["GET", "GET"]


async def test_cancel_job_works_from_persisted_handle_after_adapter_restart() -> None:
    cancelled: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        cancelled.append(request.url.path)
        return httpx.Response(202, json={"status": "cancelling"})

    async with httpx.AsyncClient(
        base_url="http://127.0.0.1:7331", transport=httpx.MockTransport(handler)
    ) as client:
        original = LocalWorkerAdapter("http://127.0.0.1:7331", client=client)
        handle = durable_handle(original, job_id="cancel-after-restart")
        restarted = LocalWorkerAdapter("http://127.0.0.1:7331", client=client)
        assert await restarted.cancel_job(handle)

    assert cancelled == ["/v1/jobs/cancel-after-restart/cancel"]


@pytest.mark.parametrize("state", ["COMPLETED", "UNKNOWN"])
async def test_v2_cancel_job_honors_explicit_not_accepted_receipt(state: str) -> None:
    paths: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        paths.append(request.url.path)
        if request.url.path == "/v1/health":
            return httpx.Response(200, json=v2_health())
        return httpx.Response(
            200,
            json={
                "protocol_version": 2,
                "data": {
                    "protocol_version": 2,
                    "authority_id": "authority-test-1",
                    "registry_id": "registry-test-1",
                    "state": state,
                    "cancel_accepted": False,
                },
            },
        )

    async with httpx.AsyncClient(
        base_url="http://127.0.0.1:7331", transport=httpx.MockTransport(handler)
    ) as client:
        adapter = LocalWorkerAdapter("http://127.0.0.1:7331", client=client)
        await adapter.negotiate_job_contract()
        handle = durable_handle(adapter, job_id="cancel-v2-not-accepted")
        assert await adapter.cancel_job(handle) is False

    assert paths == ["/v1/health", "/v1/health", "/v2/jobs/cancel-v2-not-accepted/cancel"]


@pytest.mark.parametrize(
    "data",
    [
        {
            "protocol_version": 2,
            "authority_id": "authority-replacement",
            "registry_id": "registry-replacement",
            "cancel_accepted": True,
        },
        {
            "protocol_version": 2,
            "authority_id": "authority-test-1",
            "registry_id": "registry-test-1",
        },
    ],
)
async def test_v2_cancel_job_rejects_wrong_authority_or_malformed_receipt(data: dict) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/v1/health":
            return httpx.Response(200, json=v2_health())
        return httpx.Response(200, json={"protocol_version": 2, "data": data})

    async with httpx.AsyncClient(
        base_url="http://127.0.0.1:7331", transport=httpx.MockTransport(handler)
    ) as client:
        adapter = LocalWorkerAdapter("http://127.0.0.1:7331", client=client)
        await adapter.negotiate_job_contract()
        handle = durable_handle(adapter, job_id="cancel-v2-invalid-receipt")
        with pytest.raises(WorkerProtocolError):
            await adapter.cancel_job(handle)


async def test_v2_cancel_acknowledgement_does_not_override_completed_terminal_state() -> None:
    paths: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        paths.append(request.url.path)
        if request.url.path == "/v1/health":
            return httpx.Response(200, json=v2_health())
        if request.method == "POST":
            return httpx.Response(
                202,
                json={
                    "protocol_version": 2,
                    "data": {
                        "protocol_version": 2,
                        "authority_id": "authority-test-1",
                        "registry_id": "registry-test-1",
                        "state": "CANCEL_REQUESTED",
                        "cancel_accepted": True,
                    },
                },
            )
        return httpx.Response(
            200,
            json={
                "protocol_version": 2,
                "data": {
                    "authority_id": "authority-test-1",
                    "registry_id": "registry-test-1",
                    "state": "COMPLETED",
                    "result": {"content": "completion won cancellation race"},
                },
            },
        )

    async with httpx.AsyncClient(
        base_url="http://127.0.0.1:7331", transport=httpx.MockTransport(handler)
    ) as client:
        adapter = LocalWorkerAdapter(
            "http://127.0.0.1:7331", client=client, poll_interval_seconds=0.001
        )
        await adapter.negotiate_job_contract()
        handle = durable_handle(adapter, run_id="cancel-complete-race", job_id="race-v2")
        assert await adapter.cancel_job(handle) is True
        result = await adapter.resume_job(
            WorkerRequest(run_id=handle.run_id, prompt="observe authoritative terminal"), handle
        )

    assert result.state is RunState.COMPLETED
    assert result.final_text == "completion won cancellation race"
    assert paths.count("/v2/jobs/race-v2/cancel") == 1
    assert paths.count("/v2/jobs/race-v2") == 1


async def test_v1_cancel_job_honors_explicit_accepted_false() -> None:
    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"protocol_version": 1, "data": {"accepted": False}})

    async with httpx.AsyncClient(
        base_url="http://127.0.0.1:7331", transport=httpx.MockTransport(handler)
    ) as client:
        adapter = LocalWorkerAdapter("http://127.0.0.1:7331", client=client)
        handle = durable_handle(adapter, job_id="cancel-v1-not-accepted")
        assert await adapter.cancel_job(handle) is False


async def test_reconcile_wrong_adapter_instance_fails_closed_without_http() -> None:
    contacted = False

    def handler(_: httpx.Request) -> httpx.Response:
        nonlocal contacted
        contacted = True
        return httpx.Response(200, json={"status": "succeeded"})

    async with httpx.AsyncClient(
        base_url="http://127.0.0.1:7332", transport=httpx.MockTransport(handler)
    ) as client:
        original = LocalWorkerAdapter("http://127.0.0.1:7331")
        handle = durable_handle(original)
        replacement = LocalWorkerAdapter("http://127.0.0.1:7332", client=client)
        observation = await replacement.reconcile_job(handle)

    assert observation.state is WorkerJobState.UNKNOWN
    assert "instance" in (observation.detail or "")
    assert not contacted
