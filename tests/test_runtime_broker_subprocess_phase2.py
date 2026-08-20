from __future__ import annotations

import asyncio
import json
import os
import socket
import sqlite3
import subprocess
import sys
import time
from contextlib import suppress
from datetime import UTC, datetime, timedelta
from pathlib import Path

import httpx
import pytest

from project_supervisor.fabric.execution_plane import (
    FabricRuntimeStartRequest,
    HTTPSFabricRuntimeBootstrap,
)
from project_supervisor.local_worker_v2.runtime_broker import (
    RuntimeBrokerProfile,
    RuntimeBrokerRegistry,
)

pytestmark = pytest.mark.asyncio


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as listener:
        listener.bind(("127.0.0.1", 0))
        return int(listener.getsockname()[1])


def _start_daemon(
    root: Path,
    profile: RuntimeBrokerProfile,
    *,
    port: int,
    barrier: Path | None = None,
) -> subprocess.Popen[bytes]:
    command = [
        sys.executable,
        "-m",
        "project_supervisor.local_worker_v2.runtime_broker",
        "--data-dir",
        str(root / "broker"),
        "--host",
        "127.0.0.1",
        "--port",
        str(port),
        "--auth-token-file",
        str(root / "broker-token"),
        "--broker-authority-id",
        profile.broker_authority_id,
        "--node-id",
        profile.node_id,
        "--binding-id",
        profile.binding_id,
        "--service-profile-id",
        profile.service_profile_id,
        "--service-profile-revision",
        str(profile.service_profile_revision),
        "--target-service-name",
        profile.target_service_name,
        "--test-service-marker",
        str(root / "service-running"),
        "--test-invocation-log",
        str(root / "service-invocations"),
        "--test-host-identity",
        "subprocess-fixture-host",
    ]
    if barrier is not None:
        command.extend(("--test-after-start-barrier", str(barrier)))
    environment = {
        "PATH": os.environ.get("PATH", ""),
        "PYTHONDONTWRITEBYTECODE": "1",
    }
    return subprocess.Popen(
        command,
        cwd=Path(__file__).parents[1],
        env=environment,
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        start_new_session=True,
    )


def _stop(process: subprocess.Popen[bytes]) -> None:
    if process.poll() is None:
        process.kill()
    with suppress(subprocess.TimeoutExpired):
        process.wait(timeout=5)


def _wait_health(port: int, token: str, *, timeout: float = 10) -> dict[str, object]:
    deadline = time.monotonic() + timeout
    last_error: BaseException | None = None
    while time.monotonic() < deadline:
        try:
            response = httpx.get(
                f"http://127.0.0.1:{port}/v1/health",
                headers={"authorization": f"Bearer {token}"},
                timeout=0.25,
                trust_env=False,
            )
            if response.status_code == 200:
                return response.json()
        except (httpx.HTTPError, OSError) as error:
            last_error = error
        time.sleep(0.025)
    raise AssertionError(f"runtime broker did not become healthy: {last_error!r}")


async def test_real_broker_restart_recovers_post_start_crash_without_duplicate(tmp_path) -> None:
    token = "runtime-broker-subprocess-token-" + "0123456789abcdef"
    token_file = tmp_path / "broker-token"
    token_file.write_text(token, encoding="utf-8")
    token_file.chmod(0o600)
    profile = RuntimeBrokerProfile(
        broker_authority_id="broker-authority-subprocess",
        node_id="node-windows-subprocess",
        binding_id="binding-windows-subprocess",
        service_profile_id="local-worker",
        service_profile_revision=1,
        target_service_name="ChipsFabricLocalWorkerFixture",
    )
    database_path = tmp_path / "broker" / "runtime-broker.db"
    registry_id = RuntimeBrokerRegistry.initialize(
        database_path,
        profile,
        test_host_identity="subprocess-fixture-host",
    )
    now = datetime.now(UTC)
    request = FabricRuntimeStartRequest(
        attempt_id="runtime-attempt-subprocess",
        binding_id=profile.binding_id,
        binding_generation=1,
        node_id=profile.node_id,
        service_name=profile.service_profile_id,
        task_id="task-runtime-subprocess",
        requirements_sha256="a" * 64,
        authorization_id="authorization-runtime-subprocess",
        authorization_version=1,
        authorization_sha256="b" * 64,
        broker_authority_id=profile.broker_authority_id,
        broker_registry_id=registry_id,
        service_profile_revision=profile.service_profile_revision,
        service_profile_sha256=profile.digest,
        lease_owner_id="supervisor-runtime-subprocess",
        lease_generation=1,
        idempotency_key="runtime-key-subprocess",
        requested_at=now,
        deadline_at=now + timedelta(seconds=60),
    )
    port = _free_port()
    barrier = tmp_path / "never-release-first-daemon"
    first = _start_daemon(tmp_path, profile, port=port, barrier=barrier)
    second: subprocess.Popen[bytes] | None = None
    third: subprocess.Popen[bytes] | None = None
    try:
        health = await asyncio.to_thread(_wait_health, port, token)
        assert health["brokerRegistryID"] == registry_id
        client = HTTPSFabricRuntimeBootstrap(
            f"http://127.0.0.1:{port}",
            bearer_token=token,
            broker_authority_id=profile.broker_authority_id,
            broker_registry_id=registry_id,
            service_profile_revision=profile.service_profile_revision,
            service_profile_sha256=profile.digest,
            timeout_seconds=30,
        )
        lost_response = asyncio.create_task(client.start_runtime(request))
        for _ in range(400):
            if (tmp_path / "service-running").is_file():
                break
            await asyncio.sleep(0.01)
        assert (tmp_path / "service-running").is_file()
        _stop(first)
        with suppress(Exception):
            await asyncio.wait_for(lost_response, timeout=2)

        with sqlite3.connect(database_path) as connection:
            state = connection.execute(
                "SELECT state FROM runtime_start_operations WHERE attempt_id=?",
                (request.attempt_id,),
            ).fetchone()[0]
        assert state == "dispatching"

        second = _start_daemon(tmp_path, profile, port=port)
        await asyncio.to_thread(_wait_health, port, token)
        recovered = await client.start_runtime(request)
        assert recovered.state == "running"
        assert recovered.external_start_boundary_crossed == "unknown"
        assert recovered.idempotent_replay is True
        receipt_id = recovered.receipt_id
        _stop(second)

        third = _start_daemon(tmp_path, profile, port=port)
        await asyncio.to_thread(_wait_health, port, token)
        replay = await client.start_runtime(request)
        assert replay.receipt_id == receipt_id
        assert replay.idempotent_replay is True
        assert (tmp_path / "service-invocations").read_text(encoding="utf-8").splitlines() == [
            "start"
        ]
        with sqlite3.connect(database_path) as connection:
            counts = connection.execute(
                "SELECT (SELECT COUNT(*) FROM runtime_start_operations),"
                "(SELECT COUNT(*) FROM runtime_start_observations)"
            ).fetchone()
        assert counts[0] == 1
        assert counts[1] == 3
        assert json.loads(json.dumps(replay.to_protocol()))["requestSHA256"] == request.digest
    finally:
        _stop(first)
        if second is not None:
            _stop(second)
        if third is not None:
            _stop(third)
