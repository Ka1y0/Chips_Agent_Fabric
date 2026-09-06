from __future__ import annotations

import http.client
import json
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from typing import Any

import pytest

from scripts import acceptance_readiness as readiness

IDENTITY = {
    "authority_id": "fixture-authority",
    "registry_id": "fixture-registry",
    "node_id": "fixture-node",
    "runtime_instance_id": "fixture-runtime",
}
DRIVER = "fixture-driver"


@pytest.fixture
def marker(tmp_path: Path) -> Path:
    path = tmp_path / "ready.json"
    path.write_text(json.dumps({"ready": True, "protocol_version": 2, **IDENTITY}))
    return path


def health() -> dict[str, Any]:
    return {"protocol_version": 2, "data": {**IDENTITY, "default_driver_id": DRIVER}}


class Connection:
    def __init__(
        self, raw: bytes, *, status: int = 200, headers: dict[str, str] | None = None
    ) -> None:
        self.raw = raw
        self.status = status
        self.headers = headers or {"Content-Type": "application/json"}
        self.closed = False
        self.requests: list[tuple[str, str]] = []
        self.read_sizes: list[int] = []

    def request(self, method: str, path: str, **kwargs: Any) -> None:
        assert kwargs["headers"]["Connection"] == "close"
        self.requests.append((method, path))

    def getresponse(self) -> Connection:
        return self

    def getheader(self, name: str, default: str = "") -> str:
        return self.headers.get(name, default)

    def read(self, limit: int) -> bytes:
        self.read_sizes.append(limit)
        return self.raw[:limit]

    def close(self) -> None:
        self.closed = True


def install_connection(monkeypatch: pytest.MonkeyPatch, connection: Connection) -> None:
    def create(host: str, port: int, *, timeout: float) -> Connection:
        assert host == "127.0.0.1" and port == 12345 and timeout == 0.25
        return connection

    monkeypatch.setattr(readiness.http.client, "HTTPConnection", create)


def test_missing_marker_never_opens_socket(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    def forbidden(*args: Any, **kwargs: Any) -> None:
        raise AssertionError("no socket may be opened without a marker")

    monkeypatch.setattr(readiness.http.client, "HTTPConnection", forbidden)
    assert readiness.loopback_health_ready(tmp_path / "missing", 12345, DRIVER) is False


def test_marker_precedes_listener_and_only_matching_http_identity_is_ready(
    marker: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    requests: list[str] = []
    payload = json.dumps(health()).encode()

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:
            requests.append(self.path)
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

        def log_message(self, *args: Any) -> None:
            pass

    server = HTTPServer(("127.0.0.1", 0), Handler, bind_and_activate=False)
    thread: threading.Thread | None = None
    try:
        server.server_bind()
        port = server.server_address[1]
        # The marker exists, but there is deliberately no listening socket yet.
        assert readiness.loopback_health_ready(marker, port, DRIVER) is False
        assert requests == []
        server.server_activate()
        thread = threading.Thread(target=server.serve_forever, kwargs={"poll_interval": 0.01})
        thread.start()
        monkeypatch.setenv("HTTP_PROXY", "http://127.0.0.1:1")
        monkeypatch.setenv("ALL_PROXY", "http://127.0.0.1:1")
        assert readiness.loopback_health_ready(marker, port, DRIVER) is True
        assert requests == ["/v2/health"]
    finally:
        if thread is not None:
            server.shutdown()
            thread.join(timeout=2)
            assert not thread.is_alive()
        server.server_close()


@pytest.mark.parametrize("field", [*IDENTITY, "default_driver_id"])
def test_wrong_identity_fails_without_echoing_peer_data(
    marker: Path, monkeypatch: pytest.MonkeyPatch, field: str
) -> None:
    document = health()
    document["data"][field] = "private-fixture-sentinel"
    connection = Connection(json.dumps(document).encode())
    install_connection(monkeypatch, connection)
    with pytest.raises(AssertionError, match="identity") as error:
        readiness.loopback_health_ready(marker, 12345, DRIVER)
    assert "private-fixture-sentinel" not in str(error.value)
    assert connection.closed
    assert connection.requests == [("GET", "/v2/health")]


@pytest.mark.parametrize("status", [301, 307, 401, 403, 500, 503])
def test_http_rejection_is_not_readiness_or_a_redirect(
    marker: Path, monkeypatch: pytest.MonkeyPatch, status: int
) -> None:
    connection = Connection(b"private-fixture-sentinel", status=status)
    install_connection(monkeypatch, connection)
    with pytest.raises(AssertionError, match="rejected"):
        readiness.loopback_health_ready(marker, 12345, DRIVER)
    assert connection.closed and not connection.read_sizes
    assert len(connection.requests) == 1


@pytest.mark.parametrize(
    "raw",
    [b"not json", b"[]", b'{"protocol_version":2,"protocol_version":1}', b"x" * 32769],
    ids=["invalid-json", "not-object", "duplicate-key", "too-large"],
)
def test_response_is_bounded_and_strict(
    marker: Path, monkeypatch: pytest.MonkeyPatch, raw: bytes
) -> None:
    connection = Connection(raw)
    install_connection(monkeypatch, connection)
    with pytest.raises(AssertionError):
        readiness.loopback_health_ready(marker, 12345, DRIVER)
    assert connection.read_sizes == [32769]
    assert connection.closed


@pytest.mark.parametrize("version", [True, "2", 1, None])
def test_protocol_version_is_exact(
    marker: Path, monkeypatch: pytest.MonkeyPatch, version: Any
) -> None:
    document = health()
    document["protocol_version"] = version
    connection = Connection(json.dumps(document).encode())
    install_connection(monkeypatch, connection)
    with pytest.raises(AssertionError, match="identity"):
        readiness.loopback_health_ready(marker, 12345, DRIVER)
    assert connection.closed


@pytest.mark.parametrize(
    "headers",
    [
        {"Content-Type": "text/plain"},
        {"Content-Type": "application/json", "Content-Encoding": "gzip"},
    ],
)
def test_encoded_or_wrong_media_reply_is_rejected_before_body_read(
    marker: Path, monkeypatch: pytest.MonkeyPatch, headers: dict[str, str]
) -> None:
    connection = Connection(b"{}", headers=headers)
    install_connection(monkeypatch, connection)
    with pytest.raises(AssertionError):
        readiness.loopback_health_ready(marker, 12345, DRIVER)
    assert connection.closed and not connection.read_sizes


@pytest.mark.parametrize(
    "error_type", [ConnectionRefusedError, TimeoutError, http.client.RemoteDisconnected]
)
def test_startup_transport_failure_is_retryable_but_connection_is_closed(
    marker: Path, monkeypatch: pytest.MonkeyPatch, error_type: type[Exception]
) -> None:
    class Unavailable(Connection):
        def getresponse(self) -> Connection:
            raise error_type("private-fixture-sentinel")

    connection = Unavailable(b"")
    install_connection(monkeypatch, connection)
    assert readiness.loopback_health_ready(marker, 12345, DRIVER) is False
    assert connection.closed


@pytest.mark.parametrize("raw", [b"[", b"x" * 8193, b'{"ready":false}', b'{"ready":true}'])
def test_invalid_marker_fails_before_socket(
    marker: Path, monkeypatch: pytest.MonkeyPatch, raw: bytes
) -> None:
    marker.write_bytes(raw)

    def forbidden(*args: Any, **kwargs: Any) -> None:
        raise AssertionError("socket should not be used")

    monkeypatch.setattr(readiness.http.client, "HTTPConnection", forbidden)
    with pytest.raises(AssertionError) as error:
        readiness.loopback_health_ready(marker, 12345, DRIVER)
    assert "socket should not be used" not in str(error.value)


@pytest.mark.parametrize("port", [0, 65536, True, "12345"])
def test_invalid_port_is_rejected_before_marker_access(tmp_path: Path, port: Any) -> None:
    with pytest.raises(ValueError, match="port"):
        readiness.loopback_health_ready(tmp_path / "missing", port, DRIVER)
