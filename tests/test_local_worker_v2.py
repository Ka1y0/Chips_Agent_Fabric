from __future__ import annotations

import asyncio
import json
import os
import shutil
import sqlite3
import sys
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import httpx
import pytest

import project_supervisor.local_worker_v2.registry as registry_module
from project_supervisor.adapters.base import WorkerAdapter, WorkerRequest
from project_supervisor.local_worker_v2 import LaunchRegistry, LaunchState, request_digest
from project_supervisor.local_worker_v2.drivers import (
    DriverCatalog,
    DriverProfile,
    DriverProfileError,
    DriverType,
)
from project_supervisor.local_worker_v2.native_cli_runner import _execute
from project_supervisor.local_worker_v2.process import (
    atomic_write_json,
    process_birth_identity,
)
from project_supervisor.local_worker_v2.registry import (
    IdempotencyConflict,
    RegistryUnavailable,
)
from project_supervisor.local_worker_v2.server import (
    DeterministicChildConfig,
    _catalog_from_args,
    _load_auth_token,
    build_parser,
    create_app,
)

TEST_HOST = "offline-test-host"
PROJECT_ROOT = Path(__file__).resolve().parents[1]


def launch_body(
    key: str = "supervisor-execution:run-v2",
    *,
    prompt: str = "classify this",
    role: str = "GENERAL_REASONING",
    driver_id: str = "test",
) -> dict[str, object]:
    job = {
        "driver_id": driver_id,
        "job_type": "inference.chat",
        "role": role,
        "prompt": prompt,
    }
    digest = request_digest(run_id="run-v2", task_id="task-v2", job=job)
    return {
        "protocol_version": 2,
        "idempotency_key": key,
        "request_digest": digest,
        "execution": {"run_id": "run-v2", "task_id": "task-v2"},
        "job": job,
    }


def registry(tmp_path: Path) -> LaunchRegistry:
    return LaunchRegistry(
        tmp_path / "launch-registry.sqlite3",
        test_host_identity=TEST_HOST,
    )


def target_registry(body: dict[str, object], store: LaunchRegistry) -> dict[str, object]:
    return {
        **body,
        "authority_id": store.authority_id,
        "registry_id": store.registry_id,
    }


def reserve(store: LaunchRegistry, body: dict[str, object]):
    return store.reserve(
        idempotency_key=str(body["idempotency_key"]),
        request_digest=str(body["request_digest"]),
    )


def native_profile(
    tmp_path: Path,
    *,
    driver_type: str = "claude",
    scenario: str = "normal",
    driver_id: str | None = None,
) -> tuple[DriverProfile, Path]:
    control = tmp_path / f"native-{driver_type}-{scenario}"
    control.mkdir()
    executable = control / f"fake-native-worker-cli--{scenario}"
    shutil.copy2(PROJECT_ROOT / "scripts" / "fake_native_worker_cli.py", executable)
    executable.chmod(0o700)
    profile = DriverProfile.from_mapping(
        {
            "driver_id": driver_id or f"{driver_type}-{scenario}",
            "driver_type": driver_type,
            "profile_revision": 1,
            "executable": str(executable),
            "max_execution_seconds": 5,
        }
    )
    return profile, executable


async def wait_for_job(
    client: httpx.AsyncClient,
    job_id: str,
    *,
    states: frozenset[str] = frozenset({"COMPLETED", "FAILED", "CANCELLED"}),
) -> dict[str, object]:
    data: dict[str, object] = {}
    for _ in range(500):
        response = await client.get(f"/v2/jobs/{job_id}")
        assert response.status_code == 200
        data = response.json()["data"]
        if data["state"] in states:
            return data
        await asyncio.sleep(0.01)
    raise AssertionError(f"job {job_id} did not reach {sorted(states)}: {data}")


def test_registry_duplicate_and_concurrent_launch_reservation_is_one_job(tmp_path: Path) -> None:
    store = registry(tmp_path)
    body = launch_body()

    first, created = reserve(store, body)
    replay, replay_created = reserve(store, body)

    assert created is True
    assert replay_created is False
    assert replay.launch_record_id == first.launch_record_id
    assert replay.job_id == first.job_id

    concurrent_key = "supervisor-execution:concurrent"
    concurrent = launch_body(concurrent_key)
    with ThreadPoolExecutor(max_workers=8) as pool:
        rows = list(pool.map(lambda _: reserve(store, concurrent), range(16)))

    assert sum(created for _record, created in rows) == 1
    assert len({record.job_id for record, _created in rows}) == 1
    assert store.count_launches() == 2


def test_registry_rejects_conflicting_replay_without_second_job(tmp_path: Path) -> None:
    store = registry(tmp_path)
    first = launch_body(prompt="payload one")
    conflict = launch_body(prompt="payload two")
    reserve(store, first)

    with pytest.raises(IdempotencyConflict):
        reserve(store, conflict)

    assert store.count_launches() == 1
    assert store.get_launch(str(first["idempotency_key"])).request_digest == first["request_digest"]


def test_concurrent_registry_initialization_is_atomic_and_identity_is_stable(
    tmp_path: Path,
) -> None:
    database = tmp_path / "launch-registry.sqlite3"

    def initialize(_: int) -> tuple[str, str]:
        store = LaunchRegistry(database, test_host_identity=TEST_HOST)
        return store.authority_id, store.registry_id

    with ThreadPoolExecutor(max_workers=8) as pool:
        identities = list(pool.map(initialize, range(16)))

    assert len(set(identities)) == 1
    authority_id, registry_id = identities[0]
    assert authority_id != registry_id


def test_registry_copy_to_another_host_fails_closed(tmp_path: Path) -> None:
    database = tmp_path / "launch-registry.sqlite3"
    LaunchRegistry(database, test_host_identity="host-a")

    with pytest.raises(RegistryUnavailable, match="different host"):
        LaunchRegistry(database, test_host_identity="host-b")


async def test_v2_health_receipts_lookup_conflict_and_secret_projection(tmp_path: Path) -> None:
    store = registry(tmp_path)
    app = create_app(store, spool_root=tmp_path / "jobs")
    transport = httpx.ASGITransport(app=app)
    secret = "PRIVATE_PROMPT_SECRET_918277"
    body = target_registry(launch_body(prompt=secret), store)

    async with (
        app.router.lifespan_context(app),
        httpx.AsyncClient(transport=transport, base_url="http://test") as client,
    ):
        health = (await client.get("/v1/health")).json()
        assert health["protocol_version"] == 1
        assert health["data"]["protocol_versions"] == [1, 2]
        assert health["data"]["authority_id"] != health["data"]["registry_id"]
        assert health["data"]["capabilities"]["supports_provider_idempotency"] is True

        missing = await client.get("/v2/launches/supervisor-execution:not-seen")
        assert missing.status_code == 404
        assert missing.json()["data"] == {
            "accepted": False,
            "idempotency_key": "supervisor-execution:not-seen",
            "launch_state": "NOT_SEEN",
            "disposition": "DEFINITELY_NOT_LAUNCHED",
            "protocol_version": 2,
            "driver_id": "test",
            "driver_type": "test",
            "driver_profile_revision": 1,
            "driver_profile_fingerprint": health["data"]["drivers"][0]["profile_fingerprint"],
            "launch_runtime_instance_id": None,
            "authority_id": store.authority_id,
            "registry_id": store.registry_id,
        }

        accepted = await client.post("/v2/launches", json=body)
        assert accepted.status_code == 202
        receipt = accepted.json()["data"]
        assert receipt["accepted"] is True
        assert receipt["launch_record_id"].startswith("launch-")
        assert receipt["job_id"].startswith("local-job-")
        assert receipt["receipt_id"].startswith("receipt-")
        assert receipt["authority_id"] == store.authority_id
        assert receipt["registry_id"] == store.registry_id

        replay = await client.post("/v2/launches", json=body)
        assert replay.status_code == 200
        assert replay.json()["data"]["job_id"] == receipt["job_id"]
        assert store.count_launches() == 1

        conflict_body = target_registry(launch_body(prompt="different payload"), store)
        conflict = await client.post("/v2/launches", json=conflict_body)
        assert conflict.status_code == 409
        assert conflict.json()["error"]["code"] == "IDEMPOTENCY_CONFLICT"
        assert store.count_launches() == 1

        projected = json.dumps((await client.get(f"/v2/jobs/{receipt['job_id']}")).json())
        assert secret not in projected

    database_bytes = b"".join(
        path.read_bytes() for path in tmp_path.glob("launch-registry.sqlite3*") if path.is_file()
    )
    assert secret.encode() not in database_bytes


async def test_prelaunch_rejection_is_durable_and_proves_no_job(tmp_path: Path) -> None:
    store = registry(tmp_path)
    app = create_app(store, spool_root=tmp_path / "jobs")
    body = target_registry(launch_body(role="CODING"), store)

    async with (
        app.router.lifespan_context(app),
        httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client,
    ):
        response = await client.post("/v2/launches", json=body)
        replay = await client.post("/v2/launches", json=body)
        lookup = await client.get(f"/v2/launches/{body['idempotency_key']}")

    assert response.status_code == 422
    receipt = response.json()["data"]
    assert receipt["accepted"] is False
    assert receipt["launch_state"] == "REJECTED_PRE_LAUNCH"
    assert receipt["disposition"] == "DEFINITELY_NOT_LAUNCHED"
    assert receipt["job_id"] is None
    assert receipt["authority_id"] == store.authority_id
    assert receipt["registry_id"] == store.registry_id
    assert replay.json()["data"]["receipt_id"] == receipt["receipt_id"]
    assert lookup.json()["data"]["receipt_id"] == receipt["receipt_id"]
    assert list((tmp_path / "jobs").iterdir()) == []


async def test_protocol_v1_launch_compatibility_remains_non_idempotent(tmp_path: Path) -> None:
    store = registry(tmp_path)
    app = create_app(store, spool_root=tmp_path / "jobs")
    body = {
        "job_type": "inference.chat",
        "role": "GENERAL_REASONING",
        "prompt": "read-only compatibility check",
    }

    async with (
        app.router.lifespan_context(app),
        httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client,
    ):
        first = await client.post("/v1/jobs", json=body)
        second = await client.post("/v1/jobs", json=body)
        assert first.status_code == second.status_code == 201
        assert first.json()["data"]["id"] != second.json()["data"]["id"]
        for job_id in (first.json()["data"]["id"], second.json()["data"]["id"]):
            for _ in range(200):
                terminal = await client.get(f"/v1/jobs/{job_id}")
                if terminal.json()["data"]["state"] == "COMPLETED":
                    break
                await asyncio.sleep(0.01)
            assert terminal.json()["data"]["state"] == "COMPLETED"

    assert store.count_launches() == 2


async def test_registry_unavailable_is_unknown_not_not_seen(tmp_path: Path, monkeypatch) -> None:
    store = registry(tmp_path)
    app = create_app(store, spool_root=tmp_path / "jobs")

    def unavailable(_key: str):
        raise RegistryUnavailable("injected unavailable registry")

    async with app.router.lifespan_context(app):
        monkeypatch.setattr(store, "get_launch", unavailable)
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://test"
        ) as client:
            response = await client.get("/v2/launches/supervisor-execution:unknown")

    assert response.status_code == 503
    assert response.json()["data"]["launch_state"] == "UNKNOWN"
    assert response.json()["data"]["disposition"] == "LAUNCH_OUTCOME_UNKNOWN"
    assert response.json()["data"]["authority_id"] == store.authority_id
    assert response.json()["data"]["registry_id"] == store.registry_id


async def test_launch_targeting_another_registry_is_rejected_before_reservation(
    tmp_path: Path,
) -> None:
    store = registry(tmp_path)
    app = create_app(store, spool_root=tmp_path / "jobs")
    body = {
        **launch_body("supervisor-execution:rotated-registry"),
        "authority_id": "local-worker-authority-old",
        "registry_id": "local-worker-registry-old",
    }

    async with (
        app.router.lifespan_context(app),
        httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client,
    ):
        response = await client.post("/v2/launches", json=body)

    assert response.status_code == 409
    assert response.json()["error"]["code"] == "LAUNCH_AUTHORITY_MISMATCH"
    assert response.json()["data"]["authority_id"] == store.authority_id
    assert response.json()["data"]["registry_id"] == store.registry_id
    assert store.count_launches() == 0


async def test_registry_rotation_between_lookup_and_post_cannot_create_a_job(
    tmp_path: Path,
) -> None:
    store = registry(tmp_path)
    app = create_app(store, spool_root=tmp_path / "jobs")
    body = target_registry(launch_body("supervisor-execution:rotation-race"), store)
    original_authority = store.authority_id
    original_registry = store.registry_id

    async with (
        app.router.lifespan_context(app),
        httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client,
    ):
        lookup = await client.get(f"/v2/launches/{body['idempotency_key']}")
        assert lookup.status_code == 404
        assert lookup.json()["data"]["authority_id"] == original_authority
        assert lookup.json()["data"]["registry_id"] == original_registry
        with sqlite3.connect(store.database_path) as connection:
            connection.execute(
                "UPDATE registry_meta SET value='local-worker-registry-rotated' "
                "WHERE key='registry_id'"
            )
        response = await client.post("/v2/launches", json=body)

    assert response.status_code == 503
    assert response.json()["error"]["code"] == "REGISTRY_UNAVAILABLE"
    assert response.json()["data"]["authority_id"] == original_authority
    assert response.json()["data"]["registry_id"] == original_registry
    with sqlite3.connect(store.database_path) as connection:
        assert connection.execute("SELECT COUNT(*) FROM launch_records").fetchone()[0] == 0


async def test_reserved_restart_does_not_spawn_until_same_key_post_replay(tmp_path: Path) -> None:
    original = registry(tmp_path)
    body = target_registry(launch_body("supervisor-execution:reserved-restart"), original)
    record, _created = reserve(original, body)
    assert record.launch_state is LaunchState.RESERVED

    restarted = registry(tmp_path)
    invocation_log = tmp_path / "invocations.log"
    app = create_app(
        restarted,
        spool_root=tmp_path / "jobs",
        child=DeterministicChildConfig(invocation_log=invocation_log),
    )
    async with (
        app.router.lifespan_context(app),
        httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client,
    ):
        reserved = restarted.get_launch(str(body["idempotency_key"]))
        assert reserved is not None and reserved.launch_state is LaunchState.RESERVED
        assert not invocation_log.exists()
        response = await client.post("/v2/launches", json=body)
        for _ in range(200):
            completed = await client.get(f"/v2/jobs/{record.job_id}")
            if completed.json()["data"]["state"] == "COMPLETED":
                break
            await asyncio.sleep(0.01)

    assert response.status_code == 200
    assert completed.json()["data"]["state"] == "COMPLETED"
    assert response.json()["data"]["job_id"] == record.job_id
    assert invocation_log.read_text(encoding="utf-8").splitlines() == [record.job_id]
    assert restarted.count_launches() == 1


async def test_concurrent_duplicate_post_spawns_one_real_child(tmp_path: Path) -> None:
    store = registry(tmp_path)
    release = tmp_path / "release"
    invocation_log = tmp_path / "invocations.log"
    app = create_app(
        store,
        spool_root=tmp_path / "jobs",
        child=DeterministicChildConfig(
            release_file=release,
            invocation_log=invocation_log,
        ),
    )
    body = target_registry(launch_body("supervisor-execution:concurrent-post"), store)

    async with (
        app.router.lifespan_context(app),
        httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client,
    ):
        first, second = await asyncio.gather(
            client.post("/v2/launches", json=body),
            client.post("/v2/launches", json=body),
        )
        release.touch()
        for _ in range(200):
            job = await client.get(f"/v2/jobs/{first.json()['data']['job_id']}")
            if job.json()["data"]["state"] == "COMPLETED":
                break
            await asyncio.sleep(0.01)

    assert {first.status_code, second.status_code} == {200, 202}
    assert first.json()["data"]["job_id"] == second.json()["data"]["job_id"]
    assert invocation_log.read_text(encoding="utf-8").splitlines() == [
        first.json()["data"]["job_id"]
    ]
    assert store.count_launches() == 1
    assert job.json()["data"]["state"] == "COMPLETED"


async def test_launching_restart_recovers_child_receipt_without_second_spawn(
    tmp_path: Path,
) -> None:
    store = registry(tmp_path)
    body = launch_body("supervisor-execution:spawn-checkpoint-crash")
    record, _created = reserve(store, body)
    record, claimed = store.claim_spawn(record.idempotency_key)
    assert claimed and record.job_id and record.launch_nonce
    job_dir = tmp_path / "jobs" / record.job_id
    release = tmp_path / "release"
    invocation_log = tmp_path / "invocations.log"
    atomic_write_json(
        job_dir / "launch.json",
        {
            "protocol_version": 2,
            "job_id": record.job_id,
            "idempotency_key": record.idempotency_key,
            "launch_nonce": record.launch_nonce,
            "request_digest": record.request_digest,
            "process_host_identity": store.process_host_identity,
        },
    )
    process_options = (
        {"creationflags": 0x00000200} if os.name == "nt" else {"start_new_session": True}
    )
    child = await asyncio.create_subprocess_exec(
        sys.executable,
        "-m",
        "project_supervisor.local_worker_v2.runner",
        "--job-dir",
        str(job_dir),
        "--release-file",
        str(release),
        "--invocation-log",
        str(invocation_log),
        stdin=asyncio.subprocess.DEVNULL,
        stdout=asyncio.subprocess.DEVNULL,
        stderr=asyncio.subprocess.DEVNULL,
        **process_options,
    )
    for _ in range(500):
        if (job_dir / "started.json").is_file():
            break
        await asyncio.sleep(0.01)
    assert (job_dir / "started.json").is_file()
    # This is the daemon crash seam: the process exists, but SQLite still has no process handle.
    before_restart = store.get_launch(record.idempotency_key)
    assert before_restart is not None
    assert before_restart.launch_state is LaunchState.LAUNCHING
    assert before_restart.process_pid is None

    restarted = registry(tmp_path)
    app = create_app(
        restarted,
        spool_root=tmp_path / "jobs",
        child=DeterministicChildConfig(invocation_log=invocation_log),
    )
    async with (
        app.router.lifespan_context(app),
        httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client,
    ):
        recovered = restarted.get_launch(record.idempotency_key)
        assert recovered is not None and recovered.launch_state is LaunchState.RUNNING
        assert recovered.process_pid == child.pid
        running = await client.get(f"/v2/jobs/{record.job_id}")
        assert running.json()["data"]["state"] == "RUNNING"
        release.touch()
        await asyncio.wait_for(child.wait(), timeout=5)
        for _ in range(200):
            terminal = await client.get(f"/v2/jobs/{record.job_id}")
            if terminal.json()["data"]["state"] == "COMPLETED":
                break
            await asyncio.sleep(0.01)

    assert terminal.json()["data"]["state"] == "COMPLETED"
    assert invocation_log.read_text(encoding="utf-8").splitlines() == [record.job_id]
    assert restarted.count_launches() == 1


async def test_pid_birth_without_job_nonce_lock_is_not_treated_as_live(tmp_path: Path) -> None:
    birth = process_birth_identity(os.getpid())
    if birth is None:
        pytest.skip("host cannot inspect process birth identity")
    store = registry(tmp_path)
    body = launch_body("supervisor-execution:pid-reuse")
    record, _created = reserve(store, body)
    record, claimed = store.claim_spawn(record.idempotency_key)
    assert claimed and record.job_id and record.launch_nonce
    job_dir = tmp_path / "jobs" / record.job_id
    atomic_write_json(
        job_dir / "started.json",
        {
            "protocol_version": 2,
            "job_id": record.job_id,
            "idempotency_key": record.idempotency_key,
            "launch_nonce": record.launch_nonce,
            "request_digest": record.request_digest,
            "process_host_identity": store.process_host_identity,
            "pid": os.getpid(),
            "process_birth_identity": birth,
        },
    )
    app = create_app(store, spool_root=tmp_path / "jobs")

    async with (
        app.router.lifespan_context(app),
        httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client,
    ):
        response = await client.get(f"/v2/jobs/{record.job_id}")
        cancellation = await client.post(f"/v2/jobs/{record.job_id}/cancel")

    assert response.status_code == 200
    assert response.json()["data"]["state"] == "UNKNOWN"
    assert cancellation.status_code == 200
    assert cancellation.json()["data"]["cancel_accepted"] is False
    observed = store.get_launch(record.idempotency_key)
    assert observed.launch_error == "PROCESS_IDENTITY_OR_NONCE_LOCK_MISMATCH"
    assert observed.launch_state is LaunchState.RUNNING


def test_registry_schema_one_migrates_additively_and_concurrently(tmp_path: Path) -> None:
    database = tmp_path / "launch-registry.sqlite3"
    now = "2026-08-11T00:00:00Z"
    with sqlite3.connect(database) as connection:
        connection.executescript(registry_module._BASE_SCHEMA)
        connection.execute("INSERT INTO registry_meta(key,value) VALUES ('schema_version','1')")
        connection.execute(
            "INSERT INTO launch_records("
            "launch_record_id,authority_scope,idempotency_key,request_digest,protocol_version,"
            "launch_state,job_id,receipt_id,request_received_at,created_at,updated_at"
            ") VALUES (?,?,?,?,?,?,?,?,?,?,?)",
            (
                "launch-v1-preserved",
                "local-loopback",
                "schema-one:preserved",
                "sha256:" + "1" * 64,
                2,
                "RESERVED",
                "local-job-v1-preserved",
                "receipt-v1-preserved",
                now,
                now,
                now,
            ),
        )

    def open_registry(_: int) -> tuple[str, str, str]:
        opened = LaunchRegistry(database, test_host_identity=TEST_HOST)
        return opened.authority_id, opened.registry_id, opened.node_id

    with ThreadPoolExecutor(max_workers=8) as pool:
        identities = list(pool.map(open_registry, range(16)))

    assert len(set(identities)) == 1
    migrated = LaunchRegistry(database, test_host_identity=TEST_HOST)
    preserved = migrated.get_launch("schema-one:preserved")
    assert preserved is not None
    assert preserved.launch_record_id == "launch-v1-preserved"
    assert preserved.driver_id is None
    with sqlite3.connect(database) as connection:
        assert (
            connection.execute(
                "SELECT value FROM registry_meta WHERE key='schema_version'"
            ).fetchone()[0]
            == "2"
        )
        columns = {
            row[1] for row in connection.execute("PRAGMA table_info(launch_records)").fetchall()
        }
    assert {
        "driver_id",
        "driver_type",
        "driver_profile_revision",
        "driver_profile_fingerprint",
        "launch_runtime_instance_id",
    } <= columns


async def test_health_has_stable_node_and_ephemeral_runtime_driver_catalog(tmp_path: Path) -> None:
    store = registry(tmp_path)
    first = create_app(store, spool_root=tmp_path / "jobs")
    second = create_app(store, spool_root=tmp_path / "jobs")
    first_client = httpx.AsyncClient(
        transport=httpx.ASGITransport(app=first), base_url="http://test"
    )
    second_client = httpx.AsyncClient(
        transport=httpx.ASGITransport(app=second), base_url="http://test"
    )
    async with first_client, second_client:
        first_health = (await first_client.get("/v2/health")).json()["data"]
        second_health = (await second_client.get("/v2/health")).json()["data"]

    assert first_health["node_id"] == second_health["node_id"] == store.node_id
    assert first_health["runtime_instance_id"] != second_health["runtime_instance_id"]
    assert first_health["default_driver_id"] == "test"
    assert first_health["drivers"][0]["driver_type"] == "test"
    assert first_health["capabilities"]["supports_server_driver_profiles"] is True


async def test_strict_v2_job_rejects_client_process_controls_before_launch(
    tmp_path: Path,
) -> None:
    store = registry(tmp_path)
    app = create_app(store, spool_root=tmp_path / "jobs")
    body = launch_body("strict-fields:argv")
    job = dict(body["job"])
    job.update(
        {
            "argv": ["sh", "-c", "unsafe"],
            "env": {"PRIVATE_TOKEN": "do-not-persist"},
            "cwd": "/tmp",
            "executable": "/bin/sh",
        }
    )
    body["job"] = job
    body["request_digest"] = request_digest(run_id="run-v2", task_id="task-v2", job=job)
    body = target_registry(body, store)

    async with (
        app.router.lifespan_context(app),
        httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client,
    ):
        rejected = await client.post("/v2/launches", json=body)
        oversized = await client.post(
            "/v2/launches",
            content=b"{" + b"x" * (256 * 1024),
            headers={"content-type": "application/json"},
        )

    assert rejected.status_code == 422
    assert rejected.json()["data"]["launch_state"] == "REJECTED_PRE_LAUNCH"
    assert rejected.json()["data"]["job_id"] is None
    assert oversized.status_code == 413
    assert store.count_launches() == 1
    assert not list((tmp_path / "jobs").iterdir())
    database_bytes = b"".join(
        path.read_bytes() for path in tmp_path.glob("launch-registry.sqlite3*") if path.is_file()
    )
    assert b"do-not-persist" not in database_bytes


@pytest.mark.parametrize("driver_type", ["claude", "codex"])
async def test_native_driver_full_stack_is_identity_bound_and_secret_free(
    tmp_path: Path, driver_type: str
) -> None:
    profile, executable = native_profile(tmp_path, driver_type=driver_type)
    catalog = DriverCatalog([profile], default_driver_id=profile.driver_id)
    store = registry(tmp_path)
    app = create_app(store, spool_root=tmp_path / "jobs", catalog=catalog)
    secret = "NATIVE_PROMPT_SECRET_315902"
    body = target_registry(
        launch_body(
            "native-driver:normal",
            prompt=secret,
            driver_id=profile.driver_id,
        ),
        store,
    )

    async with (
        app.router.lifespan_context(app),
        httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client,
    ):
        health = (await client.get("/v2/health")).json()["data"]
        launched = await client.post("/v2/launches", json=body)
        assert launched.status_code == 202
        receipt = launched.json()["data"]
        terminal = await wait_for_job(client, receipt["job_id"])

    assert terminal["state"] == "COMPLETED"
    assert terminal["result"]["content"] == "NATIVE_CLI_OFFLINE_OK"
    assert terminal["driver_id"] == profile.driver_id
    assert terminal["driver_type"] == DriverType(driver_type).value
    assert terminal["driver_profile_revision"] == profile.profile_revision
    assert terminal["driver_profile_fingerprint"] == profile.profile_fingerprint
    assert terminal["launch_runtime_instance_id"] == receipt["launch_runtime_instance_id"]
    assert terminal["authority_id"] == store.authority_id
    assert terminal["registry_id"] == store.registry_id
    assert str(executable) not in json.dumps(health)
    assert health["drivers"][0]["supports_execution"] is True
    invocation = json.loads(
        (executable.parent / "invocations.jsonl").read_text(encoding="utf-8").strip()
    )
    if driver_type == "claude":
        assert {
            "--permission-mode",
            "plan",
            "--tools",
            "--disable-slash-commands",
            "--strict-mcp-config",
            "--no-chrome",
            "--safe-mode",
        } <= set(invocation["argv"])
    else:
        assert {
            "exec",
            "--json",
            "--output-last-message",
            "--skip-git-repo-check",
            "--ephemeral",
            "--ignore-user-config",
            "--strict-config",
        } <= set(invocation["argv"])
        assert invocation["argv"][-1] == "-"
        assert invocation["argv"][invocation["argv"].index("--sandbox") + 1] == "read-only"
        assert secret not in invocation["argv"]

    launch_spool = next((tmp_path / "jobs").glob("*/launch.json")).read_text(encoding="utf-8")
    assert secret not in launch_spool
    assert str(executable) not in launch_spool
    database_bytes = b"".join(
        path.read_bytes() for path in tmp_path.glob("launch-registry.sqlite3*") if path.is_file()
    )
    assert secret.encode() not in database_bytes


@pytest.mark.parametrize("scenario", ["malformed", "wrong-dialect", "flood"])
async def test_native_driver_invalid_provider_output_fails_boundedly(
    tmp_path: Path,
    scenario: str,
) -> None:
    profile, _executable = native_profile(tmp_path, scenario=scenario)
    catalog = DriverCatalog([profile], default_driver_id=profile.driver_id)
    store = registry(tmp_path)
    app = create_app(store, spool_root=tmp_path / "jobs", catalog=catalog)
    body = target_registry(
        launch_body(f"native-driver:{scenario}", driver_id=profile.driver_id), store
    )

    async with (
        app.router.lifespan_context(app),
        httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client,
    ):
        launched = await client.post("/v2/launches", json=body)
        terminal = await wait_for_job(client, launched.json()["data"]["job_id"])

    assert terminal["state"] == "FAILED"
    terminal_path = next((tmp_path / "jobs").glob("*/terminal.json"))
    assert terminal_path.stat().st_size <= 65_536


async def test_native_terminal_receipt_is_capped_after_json_escape_expansion(
    tmp_path: Path,
) -> None:
    control = tmp_path / "native-control-output"
    control.mkdir()
    executable = control / "control-output-grok"
    executable.write_text(
        "#!/usr/bin/env python3\n"
        "import json\n"
        "fragment = chr(0) * 4000\n"
        "for _ in range(10):\n"
        "    event = {'type': 'text', 'data': fragment}\n"
        "    print(json.dumps(event), flush=True)\n"
        "print(json.dumps({'type': 'end'}), flush=True)\n",
        encoding="utf-8",
    )
    executable.chmod(0o700)
    profile = DriverProfile.from_mapping(
        {
            "driver_id": "grok-control-output",
            "driver_type": "grok",
            "profile_revision": 1,
            "executable": str(executable),
            "max_execution_seconds": 5,
        }
    )
    store = registry(tmp_path)
    app = create_app(
        store,
        spool_root=tmp_path / "jobs",
        catalog=DriverCatalog([profile], default_driver_id=profile.driver_id),
    )
    body = target_registry(
        launch_body("native-driver:control-output", driver_id=profile.driver_id), store
    )

    async with (
        app.router.lifespan_context(app),
        httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client,
    ):
        launched = await client.post("/v2/launches", json=body)
        terminal = await wait_for_job(client, launched.json()["data"]["job_id"])

    terminal_path = next((tmp_path / "jobs").glob("*/terminal.json"))
    assert terminal["state"] == "FAILED"
    assert terminal_path.stat().st_size <= 60 * 1024
    assert terminal["error"] == "native result projection exceeded the durable receipt limit"


async def test_native_failure_projection_does_not_expose_path_or_secret_detail(
    tmp_path: Path,
) -> None:
    control = tmp_path / "native-sensitive-error"
    control.mkdir()
    executable = control / "sensitive-error-claude"
    sensitive_path = "/private/operator/secrets/provider-token.json"
    sensitive_value = "SUPER_SECRET_PROVIDER_TOKEN_4411"
    executable.write_text(
        "#!/usr/bin/env python3\n"
        "import sys\n"
        f"sys.stderr.write({(sensitive_path + ':' + sensitive_value)!r})\n"
        "raise SystemExit(17)\n",
        encoding="utf-8",
    )
    executable.chmod(0o700)
    profile = DriverProfile.from_mapping(
        {
            "driver_id": "claude-sensitive-error",
            "driver_type": "claude",
            "profile_revision": 1,
            "executable": str(executable),
            "max_execution_seconds": 5,
        }
    )
    store = registry(tmp_path)
    app = create_app(
        store,
        spool_root=tmp_path / "jobs",
        catalog=DriverCatalog([profile], default_driver_id=profile.driver_id),
    )
    body = target_registry(
        launch_body("native-driver:sensitive-error", driver_id=profile.driver_id), store
    )

    async with (
        app.router.lifespan_context(app),
        httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client,
    ):
        launched = await client.post("/v2/launches", json=body)
        terminal = await wait_for_job(client, launched.json()["data"]["job_id"])

    projection = json.dumps(terminal)
    assert terminal["state"] == "FAILED"
    assert terminal["error"] == "PROVIDER_EXIT_NONZERO"
    assert sensitive_path not in projection
    assert sensitive_value not in projection


async def test_native_runner_pre_set_cancel_never_calls_adapter_execute() -> None:
    class RecordingAdapter(WorkerAdapter):
        def __init__(self) -> None:
            self.executions = 0

        async def execute(self, request: WorkerRequest, *, event_sink=None):  # type: ignore[no-untyped-def]
            self.executions += 1
            raise AssertionError("pre-cancelled runner must not execute a provider")

        async def cancel(self, run_id: str) -> bool:
            return False

    adapter = RecordingAdapter()
    cancelled = asyncio.Event()
    cancelled.set()
    with pytest.raises(RuntimeError, match="before provider launch"):
        await _execute(
            adapter,
            WorkerRequest(run_id="pre-cancelled", prompt="never launch"),
            cancelled,
            asyncio.Event(),
        )
    assert adapter.executions == 0


async def test_native_driver_cancel_reaps_provider_before_terminal_receipt(tmp_path: Path) -> None:
    if os.name == "nt":
        pytest.skip("native cancellation is not advertised until Windows Job Objects exist")
    profile, executable = native_profile(tmp_path, scenario="cancel")
    catalog = DriverCatalog([profile], default_driver_id=profile.driver_id)
    store = registry(tmp_path)
    app = create_app(store, spool_root=tmp_path / "jobs", catalog=catalog)
    body = target_registry(launch_body("native-driver:cancel", driver_id=profile.driver_id), store)

    async with (
        app.router.lifespan_context(app),
        httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client,
    ):
        launched = await client.post("/v2/launches", json=body)
        job_id = launched.json()["data"]["job_id"]
        for _ in range(500):
            if (executable.parent / "started.json").is_file():
                break
            await asyncio.sleep(0.01)
        assert (executable.parent / "started.json").is_file()
        cancellation = await client.post(f"/v2/jobs/{job_id}/cancel")
        terminal = await wait_for_job(client, job_id)

    assert cancellation.status_code == 202
    assert cancellation.json()["data"]["cancel_accepted"] is True
    assert terminal["state"] == "CANCELLED"
    assert (executable.parent / "cancelled.json").is_file()


async def test_profile_change_is_unknown_but_binary_replacement_after_spawn_is_collectable(
    tmp_path: Path,
) -> None:
    profile, executable = native_profile(tmp_path, scenario="normal", driver_id="native-pinned")
    catalog = DriverCatalog([profile], default_driver_id=profile.driver_id)
    store = registry(tmp_path)
    app = create_app(store, spool_root=tmp_path / "jobs", catalog=catalog)
    body = target_registry(launch_body("native-driver:pinned", driver_id=profile.driver_id), store)

    async with (
        app.router.lifespan_context(app),
        httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client,
    ):
        launched = await client.post("/v2/launches", json=body)
        job_id = launched.json()["data"]["job_id"]
        terminal = await wait_for_job(client, job_id)
        executable.write_text("#!/bin/sh\nexit 99\n", encoding="utf-8")
        executable.chmod(0o700)
        still_collectable = await client.get(f"/v2/jobs/{job_id}")

    assert terminal["state"] == "COMPLETED"
    assert still_collectable.json()["data"]["state"] == "COMPLETED"

    replacement = DriverProfile.from_mapping(
        {
            "driver_id": profile.driver_id,
            "driver_type": "claude",
            "profile_revision": 2,
            "executable": str(executable),
            "max_execution_seconds": 5,
        }
    )
    restarted = create_app(
        store,
        spool_root=tmp_path / "jobs",
        catalog=DriverCatalog([replacement], default_driver_id=replacement.driver_id),
    )
    async with (
        restarted.router.lifespan_context(restarted),
        httpx.AsyncClient(
            transport=httpx.ASGITransport(app=restarted), base_url="http://test"
        ) as client,
    ):
        mismatch = await client.get(f"/v2/jobs/{job_id}")
    assert mismatch.json()["data"]["state"] == "UNKNOWN"
    assert mismatch.json()["data"]["launch_error"] == "DRIVER_PROFILE_MISMATCH"


def test_production_mode_requires_native_profiles_and_rejects_test_seams(
    tmp_path: Path,
) -> None:
    profile, _executable = native_profile(tmp_path)
    profile_file = tmp_path / "profile.json"
    profile_file.write_text(
        json.dumps(
            {
                "driver_id": profile.driver_id,
                "driver_type": profile.driver_type.value,
                "profile_revision": profile.profile_revision,
                "executable": str(profile.executable),
                "max_execution_seconds": profile.max_execution_seconds,
            }
        ),
        encoding="utf-8",
    )
    token_file = tmp_path / "worker.token"
    token_file.write_text("A" * 48 + "\n", encoding="ascii")
    token_file.chmod(0o600)
    parser = build_parser()
    valid = parser.parse_args(
        [
            "--data-dir",
            str(tmp_path / "state"),
            "--port",
            "8000",
            "--production",
            "--driver-profile",
            str(profile_file),
            "--default-driver-id",
            profile.driver_id,
            "--auth-token-file",
            str(token_file),
        ]
    )
    assert _catalog_from_args(valid).default_driver_id == profile.driver_id

    unsafe = parser.parse_args(
        [
            "--data-dir",
            str(tmp_path / "state"),
            "--port",
            "8000",
            "--production",
            "--driver-profile",
            str(profile_file),
            "--default-driver-id",
            profile.driver_id,
            "--auth-token-file",
            str(token_file),
            "--test-response-barrier",
            str(tmp_path / "barrier"),
        ]
    )
    with pytest.raises(SystemExit, match="rejects every --test"):
        _catalog_from_args(unsafe)

    assert _load_auth_token(token_file) == "A" * 48
    token_file.chmod(0o644)
    with pytest.raises(ValueError, match="0600"):
        _load_auth_token(token_file)


async def test_authenticated_daemon_rejects_every_unauthenticated_route_without_mutation(
    tmp_path: Path,
) -> None:
    token = "local-worker-owner-token-" + "A" * 32
    store = registry(tmp_path)
    app = create_app(store, spool_root=tmp_path / "jobs", auth_token=token)
    body = target_registry(launch_body("authenticated:launch"), store)

    async with (
        app.router.lifespan_context(app),
        httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client,
    ):
        unauthenticated_health = await client.get("/v2/health")
        wrong_launch = await client.post(
            "/v2/launches",
            json=body,
            headers={"authorization": "Bearer " + "B" * len(token)},
        )
        authenticated_health = await client.get(
            "/v2/health", headers={"authorization": f"Bearer {token}"}
        )
        launched = await client.post(
            "/v2/launches",
            json=body,
            headers={"authorization": f"Bearer {token}"},
        )
        job_id = launched.json()["data"]["job_id"]
        unauthenticated_job = await client.get(f"/v2/jobs/{job_id}")
        authenticated_job = await client.get(
            f"/v2/jobs/{job_id}", headers={"authorization": f"Bearer {token}"}
        )

    assert unauthenticated_health.status_code == 401
    assert wrong_launch.status_code == 401
    assert wrong_launch.json()["error"]["code"] == "AUTHENTICATION_REQUIRED"
    assert authenticated_health.status_code == 200
    assert launched.status_code == 202
    assert unauthenticated_job.status_code == 401
    assert authenticated_job.status_code == 200
    assert store.count_launches() == 1
    assert token not in json.dumps(authenticated_health.json())


def test_codex_native_profile_is_truthfully_available_and_catalog_hides_paths(
    tmp_path: Path,
) -> None:
    native, executable = native_profile(tmp_path)
    codex, codex_executable = native_profile(
        tmp_path,
        driver_type="codex",
        driver_id="codex-reviewed",
    )
    catalog = DriverCatalog([native, codex], default_driver_id=native.driver_id)
    projected = catalog.public_data()
    encoded = json.dumps(projected)

    assert str(executable) not in encoded
    assert str(codex_executable) not in encoded
    codex_data = next(item for item in projected if item["driver_id"] == codex.driver_id)
    assert codex_data["driver_type"] == DriverType.CODEX.value
    assert codex_data["available"] is True
    assert codex_data["supports_execution"] is True
    assert codex_data["supports_cancel"] is (os.name != "nt")
    native_data = next(item for item in projected if item["driver_id"] == native.driver_id)
    assert native_data["supports_cancel"] is (os.name != "nt")


def test_native_profile_fences_same_bytes_replacement_and_writable_executable(
    tmp_path: Path,
) -> None:
    profile, executable = native_profile(tmp_path)
    replacement = executable.with_name("replacement")
    shutil.copy2(executable, replacement)
    replacement.chmod(0o700)
    os.replace(replacement, executable)

    assert profile.is_current() is False

    if os.name != "nt":
        executable.chmod(0o722)
        with pytest.raises(DriverProfileError, match="group/world writable"):
            DriverProfile.from_mapping(
                {
                    "driver_id": "unsafe-writable",
                    "driver_type": "claude",
                    "profile_revision": 1,
                    "executable": str(executable),
                    "max_execution_seconds": 5,
                }
            )
