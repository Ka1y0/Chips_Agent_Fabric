from __future__ import annotations

import asyncio
import json
from datetime import timedelta

import httpx
import pytest

from project_supervisor.local_probe import (
    ProbeError,
    ProbeOptions,
    main,
    probe_local_model,
)

pytestmark = pytest.mark.asyncio


def response(value: object, **kwargs: object) -> httpx.Response:
    raw = json.dumps(value).encode()
    return httpx.Response(
        200, headers={"content-type": "application/json"}, stream=httpx.ByteStream(raw), **kwargs
    )


def config(**changes: object) -> ProbeOptions:
    return ProbeOptions(**{
        "runtime": "ollama", "endpoint": "http://localhost:11434", "allow_network": True,
        **changes,
    })


async def test_dry_run_never_opens_a_connection() -> None:
    def forbidden(_request: httpx.Request) -> httpx.Response:
        raise AssertionError("dry-run contacted a server")

    report = await probe_local_model(
        config(allow_network=False), transport=httpx.MockTransport(forbidden)
    )
    assert report.status == "dryRun"
    assert report.observed_at is None
    assert report.control_profile() is None


@pytest.mark.parametrize("runtime", ["lmStudio", "llamaCpp", "openAICompatible", "ollama"])
async def test_catalog_reads_fixed_route_and_does_not_infer_capabilities(runtime: str) -> None:
    calls = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request)
        assert request.url.host == "127.0.0.1"
        assert request.method == "GET"
        assert "authorization" not in request.headers
        assert request.headers["accept-encoding"] == "identity"
        assert request.url.path == ("/api/tags" if runtime == "ollama" else "/v1/models")
        return response({"models": [{"name": "fixture"}]} if runtime == "ollama" else {
            "data": [{"id": "fixture"}],
        })

    report = await probe_local_model(
        config(runtime=runtime), transport=httpx.MockTransport(handler)
    )
    assert report.status == "catalogAvailable"
    assert report.model_ids == ("fixture",)
    assert len(calls) == 1
    assert report.to_protocol()["executionLocality"] == "unknown"
    assert not report.inference_requested
    assert report.control_profile() is None


@pytest.mark.parametrize("endpoint", [
    "http://example.invalid/v1", "http://192.168.1.2/v1", "http://0.0.0.0/v1",
    "http://localhost@evil.invalid/v1", "http://user:password@localhost/v1",
    "http://localhost:0", "http://localhost:65536", "http://localhost:",
    "http://localhost/v1?token=redacted", "http://localhost/v1#x",
    "\nhttp://localhost", "http://local\nhost", "http://localhost\\evil",
    "http://127.1", "file:///tmp/file", "http://[::ffff:127.0.0.1]", "http://localhost/admin",
])
async def test_endpoint_rejections_are_static_and_do_not_echo_input(endpoint: str) -> None:
    with pytest.raises(ProbeError) as caught:
        config(endpoint=endpoint)
    assert endpoint not in str(caught.value)
    assert "password" not in str(caught.value)


@pytest.mark.parametrize("changes", [
    {"allow_network": "true"}, {"allow_inference": 1}, {"max_models": True},
    {"max_response_bytes": 0}, {"output_token_limit": 33}, {"freshness_seconds": -1},
    {"timeout_seconds": float("inf")}, {"timeout_seconds": float("nan")},
    {"timeout_seconds": 10**400}, {"model_id": "\x1bmodel"}, {"allow_inference": True},
])
async def test_invalid_config_fails_before_request(changes: dict[str, object]) -> None:
    with pytest.raises(ProbeError):
        config(**changes)


async def test_ipv6_and_openai_base_paths() -> None:
    options = config(runtime="openAICompatible", endpoint="http://[::1]:8080/v1/")
    assert options.routes() == (
        "http://[::1]:8080/v1/models", "http://[::1]:8080/v1/chat/completions"
    )


@pytest.mark.parametrize("status,code", [
    (301, "redirectRefused"), (401, "authRequired"), (403, "authRequired"),
    (429, "rateLimited"), (500, "httpError"),
])
async def test_http_errors_never_follow_redirect_or_retain_body(status: int, code: str) -> None:
    calls = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request)
        return httpx.Response(status, headers={"location": "https://example.invalid"},
                              content=b"PRIVATE_SERVER_ERROR")

    report = await probe_local_model(config(), transport=httpx.MockTransport(handler))
    assert report.error_code == code
    assert "PRIVATE_SERVER_ERROR" not in json.dumps(report.to_protocol())
    assert len(calls) == 1


@pytest.mark.parametrize("body,headers,code", [
    (b"x", {"content-type": "text/html"}, "invalidContentType"),
    (b"{}", {"content-type": "application/json", "content-encoding": "gzip"},
     "compressedResponseRefused"),
    (b"{}", {"content-type": "application/json", "content-length": "9999"}, "responseTooLarge"),
    (b"{}", {"content-type": "application/json", "content-length": "-1"}, "invalidResponse"),
    (b'{"models":[],"models":[]}', {"content-type": "application/json"}, "invalidResponse"),
    (b'{"models":NaN}', {"content-type": "application/json"}, "invalidResponse"),
    (b"[]", {"content-type": "application/json"}, "invalidResponse"),
])
async def test_response_boundaries(body: bytes, headers: dict[str, str], code: str) -> None:
    report = await probe_local_model(
        config(max_response_bytes=100),
        transport=httpx.MockTransport(lambda _req: httpx.Response(
            200, headers=headers, stream=httpx.ByteStream(body)
        )),
    )
    assert report.error_code == code


async def test_chunked_body_limit_stops_before_end_and_closes_stream() -> None:
    seen = []

    class Stream(httpx.AsyncByteStream):
        async def __aiter__(self):
            for index in range(10):
                seen.append(index)
                yield b"x" * 60

        async def aclose(self):
            seen.append("closed")

    report = await probe_local_model(config(max_response_bytes=100), transport=httpx.MockTransport(
        lambda _req: httpx.Response(
            200, headers={"content-type": "application/json"}, stream=Stream()
        )
    ))
    assert report.error_code == "responseTooLarge"
    assert seen == [0, 1, "closed"]


async def test_whole_operation_deadline_stops_trickling_response() -> None:
    closed = asyncio.Event()

    class Stream(httpx.AsyncByteStream):
        async def __aiter__(self):
            while True:
                await asyncio.sleep(0.01)
                yield b" "

        async def aclose(self):
            closed.set()

    report = await probe_local_model(config(timeout_seconds=0.05), transport=httpx.MockTransport(
        lambda _req: httpx.Response(
            200, headers={"content-type": "application/json"}, stream=Stream()
        )
    ))
    assert report.error_code == "timeout"
    assert closed.is_set()


async def test_caller_cancellation_propagates_and_closes_connection() -> None:
    started, closed = asyncio.Event(), asyncio.Event()

    class Stream(httpx.AsyncByteStream):
        async def __aiter__(self):
            started.set()
            await asyncio.Event().wait()
            yield b"{}"

        async def aclose(self):
            closed.set()

    running = asyncio.create_task(probe_local_model(config(), transport=httpx.MockTransport(
        lambda _req: httpx.Response(
            200, headers={"content-type": "application/json"}, stream=Stream()
        )
    )))
    await asyncio.wait_for(started.wait(), 1)
    running.cancel()
    with pytest.raises(asyncio.CancelledError):
        await running
    assert closed.is_set()


async def test_model_must_be_visible_before_inference_is_attempted() -> None:
    calls = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request.method)
        return response({"models": []})

    report = await probe_local_model(config(model_id="absent", allow_inference=True),
                                     transport=httpx.MockTransport(handler))
    assert report.error_code == "modelNotFound"
    assert not report.inference_requested
    assert calls == ["GET"]


@pytest.mark.parametrize("runtime", ["ollama", "lmStudio", "llamaCpp", "openAICompatible"])
async def test_explicit_smoke_uses_fixed_input_and_separate_metrics(runtime: str) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if request.method == "GET":
            return response({"models": [{"name": "fixture"}]} if runtime == "ollama" else {
                "data": [{"id": "fixture"}],
            })
        body = json.loads(request.content)
        assert body["messages"] == [{"role": "user", "content": "Reply with exactly the word OK."}]
        assert body["stream"] is False
        message = {"role": "assistant", "content": "OK"}
        if runtime == "ollama":
            assert body["options"]["num_predict"] == 16
            return response({"model": "fixture", "message": message, "done": True,
                             "eval_count": 2, "eval_duration": 100_000_000})
        assert body["max_tokens"] == 16
        return response({
            "model": "fixture", "choices": [{"message": message, "finish_reason": "stop"}],
            "usage": {"completion_tokens": 2},
        })

    report = await probe_local_model(
        config(runtime=runtime, model_id="fixture", allow_inference=True),
        transport=httpx.MockTransport(handler),
    )
    assert report.status == "inferenceVerified"
    assert report.output_tokens == 2
    assert report.provider_tokens_per_second == (20.0 if runtime == "ollama" else None)
    assert report.to_protocol()["measuredTTFTSeconds"] is None
    assert not report.to_protocol()["rawOutputRetained"]
    assert report.to_protocol()["modelLoadMayHaveOccurred"]


async def test_profile_projection_is_freshness_aware_and_preserves_unknown_capacity() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if request.method == "GET":
            return response({"models": [{"name": "fixture"}]})
        return response({"model": "fixture", "done": True,
                         "message": {"role": "assistant", "content": "OK"}})

    report = await probe_local_model(config(model_id="fixture", allow_inference=True),
                                     transport=httpx.MockTransport(handler))
    profile = report.control_profile(now=report.observed_at)
    assert profile["health"] == "healthy"
    assert profile["observedParallelCapacity"] is None
    assert profile["observedContextWindowTokens"] is None
    assert profile["reasoningMode"] == "unknown"
    assert profile["recommendedParallelism"] == 1
    expired = report.control_profile(now=report.observed_at + timedelta(seconds=60))
    assert expired["freshness"] == "stale"
    assert expired["health"] == "unknown"


async def test_real_loopback_http_catalog_ignores_inherited_proxy(monkeypatch) -> None:
    seen = []

    async def serve(reader, writer):
        try:
            seen.append(await reader.readuntil(b"\r\n\r\n"))
            body = b'{"models":[{"name":"socket-fixture"}]}'
            headers = f"Content-Length: {len(body)}\r\nConnection: close\r\n\r\n".encode()
            writer.write(
                b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n" + headers + body
            )
            await writer.drain()
        finally:
            writer.close()
            await writer.wait_closed()

    monkeypatch.setenv("HTTP_PROXY", "http://127.0.0.1:1")
    monkeypatch.setenv("ALL_PROXY", "http://127.0.0.1:1")
    server = await asyncio.start_server(serve, "127.0.0.1", 0)
    async with server:
        port = server.sockets[0].getsockname()[1]
        report = await probe_local_model(config(endpoint=f"http://localhost:{port}"))
    assert report.model_ids == ("socket-fixture",)
    assert len(seen) == 1
    assert seen[0].startswith(b"GET /api/tags HTTP/1.1")


async def test_cli_dry_run_and_invalid_input_are_machine_readable(capsys) -> None:
    # CLI owns an event loop, so invoke outside this test's running event loop.
    code = await asyncio.to_thread(
        main, ["--runtime", "ollama", "--endpoint", "http://localhost:11434"]
    )
    assert code == 0
    assert json.loads(capsys.readouterr().out)["status"] == "dryRun"
    code = await asyncio.to_thread(
        main, ["--runtime", "ollama", "--endpoint", "http://user:secret@localhost"]
    )
    assert code == 2
    output = capsys.readouterr().out
    assert "secret" not in output
    assert json.loads(output)["status"] == "rejected"


@pytest.mark.parametrize("reply,code", [
    ({"model": "different"}, "modelMismatch"),
    ({"model": "fixture", "done": False}, "incompleteInference"),
    ({"model": "fixture", "done": True, "message": []}, "smokeMismatch"),
    ({"model": "fixture", "done": True,
      "message": {"role": "assistant", "content": []}}, "smokeMismatch"),
    ({"model": "fixture", "done": True,
      "message": {"role": "assistant", "content": "NOT OK"}}, "smokeMismatch"),
    ({"model": "fixture", "done": True,
      "message": {"role": "assistant", "content": "OK", "tool_calls": [{}]}}, "smokeMismatch"),
    ({"model": "fixture", "done": True, "eval_count": True,
      "message": {"role": "assistant", "content": "OK"}}, "invalidUsage"),
])
async def test_smoke_requires_a_verified_terminal_text_reply(reply: dict, code: str) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if request.method == "GET":
            return response({"models": [{"name": "fixture"}]})
        return response(reply)

    report = await probe_local_model(
        config(model_id="fixture", allow_inference=True), transport=httpx.MockTransport(handler)
    )
    assert report.error_code == code
    assert report.status == "failed"
    assert report.inference_requested
    assert not report.to_protocol()["providerCancellationConfirmed"]
    assert report.control_profile() is None


@pytest.mark.parametrize("catalog", [
    {"models": [{"name": "same"}, {"name": "same"}]},
    {"models": [{"name": "first"}, {"name": "second"}]},
    {"models": "not-a-list"}, {"models": [{"name": "\x1bmalicious"}]},
])
async def test_catalog_limits_and_ambiguous_ids_fail_closed(catalog: dict) -> None:
    report = await probe_local_model(config(max_models=1), transport=httpx.MockTransport(
        lambda _req: response(catalog)
    ))
    assert report.status == "failed"


async def test_transport_errors_do_not_echo_sensitive_exception_text() -> None:
    def handler(_request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("PRIVATE TRANSPORT DETAIL")

    report = await probe_local_model(config(), transport=httpx.MockTransport(handler))
    assert report.error_code == "transportError"
    assert "PRIVATE" not in json.dumps(report.to_protocol())
