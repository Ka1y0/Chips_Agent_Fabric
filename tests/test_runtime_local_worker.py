"""End-to-end runtime coverage for the Local Worker HTTP harness.

These tests drive the real scheduler, runtime, SQLite journal and adapter with a
protocol-v1/v2 HTTP doubles. They prove the supervisor half of the remote contract;
they do not stand in for a real private GPU-node acceptance run.
"""

from __future__ import annotations

import json

import httpx

from project_supervisor.adapters import LocalWorkerAdapter
from project_supervisor.domain import (
    ExecutionTopology,
    FailureClass,
    Harness,
    ModelDescriptor,
    NodeState,
    Provider,
    ResourceState,
    RunState,
    TaskLabel,
    TaskRequirements,
    TaskState,
    WorkerSnapshot,
    WorkerState,
)
from project_supervisor.runtime import AdapterRegistry, SupervisorRuntime
from project_supervisor.scheduler import DeterministicScheduler
from project_supervisor.store import StateStore

STRUCTURED_RESULT = {
    "summary": "Event-sourced supervisor with a loopback inference worker.",
    "risks": ["Single control node", "Unversioned worker protocol"],
    "recommendedChecks": ["Replay the journal", "Probe the worker without a bearer"],
}


def fabric_fixture(tmp_path, adapter: LocalWorkerAdapter):
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    store = StateStore(tmp_path / "state.db")
    store.create_project(
        project_id="fabric",
        name="Local Worker fabric",
        root_path=str(workspace),
        goal="Route a read-only analysis to the private Windows worker",
    )
    store.upsert_node(
        node_id="worker-node-01",
        hostname="worker-node-01",
        display_name="Windows Local Worker",
        role="worker",
        state=NodeState.ONLINE,
        operating_system="Windows",
        capabilities={"local-inference"},
    )
    store.upsert_worker(
        WorkerSnapshot(
            id="local-worker-node-01",
            node_id="worker-node-01",
            harness=Harness.LOCAL_WORKER,
            provider=Provider.LOCAL,
            model=ModelDescriptor("server-selected", "Server selected", Provider.LOCAL),
            state=WorkerState.IDLE,
            node_state=NodeState.ONLINE,
            resource_state=ResourceState.AVAILABLE,
            capabilities=frozenset({"local-classification", "read-only-analysis"}),
            code_write_allowed=False,
            privacy_allowed=True,
            quality_score=0.6,
            reliability_score=0.7,
            expected_latency_seconds=0.01,
            monetary_cost_score=1.0,
        )
    )
    registry = AdapterRegistry()
    registry.register("local-worker-node-01", adapter)
    runtime = SupervisorRuntime(
        store=store,
        scheduler=DeterministicScheduler(),
        adapters=registry,
        evidence_root=tmp_path / "evidence",
        max_attempts=1,
    )
    return store, runtime


async def submit_review(runtime: SupervisorRuntime) -> str:
    return await runtime.submit_task(
        project_id="fabric",
        title="Local Worker structured review",
        description="Return one raw JSON object describing the architecture.",
        topology=ExecutionTopology.SINGLE,
        priority=90,
        requirements=TaskRequirements(
            labels=frozenset({TaskLabel.ARCHITECTURE, TaskLabel.REVIEW}),
            required_capabilities=frozenset({"local-classification", "read-only-analysis"}),
            code_write_required=False,
        ),
    )


def worker_double(payload: dict, *, model: str = "gemma4-general"):
    submitted: list[dict] = []

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/v1/health":
            return httpx.Response(200, json={"protocol": 1, "version": "1.0.0-phase1"})
        if request.method == "POST" and request.url.path == "/v1/jobs":
            submitted.append(json.loads(request.content))
            return httpx.Response(201, json={"jobId": "job-fabric-1"})
        if request.url.path == "/v1/jobs/job-fabric-1":
            return httpx.Response(
                200,
                json={
                    "status": "succeeded",
                    "model": model,
                    "result": {"text": json.dumps(payload)},
                    "usage": {"inputTokens": 120, "outputTokens": 64, "totalTokens": 184},
                },
            )
        raise AssertionError(f"unexpected request: {request.method} {request.url}")

    return handler, submitted


async def test_local_worker_task_persists_full_lifecycle_through_the_runtime(tmp_path) -> None:
    handler, submitted = worker_double(STRUCTURED_RESULT)
    async with httpx.AsyncClient(
        base_url="http://127.0.0.1:7331", transport=httpx.MockTransport(handler)
    ) as client:
        adapter = LocalWorkerAdapter("http://127.0.0.1:7331", client=client)
        store, runtime = fabric_fixture(tmp_path, adapter)
        task_id = await submit_review(runtime)
        await runtime.run_until_idle()

    task = store.get_task(task_id)
    runs = store.list_worker_runs(task_id)
    assert task["state"] == TaskState.REVIEWING.value
    assert len(runs) == 1
    assert runs[0]["state"] == RunState.COMPLETED.value
    assert runs[0]["worker_id"] == "local-worker-node-01"

    run = store.get_worker_run(runs[0]["id"])
    assert run["session_id"] is not None
    with store.connect() as connection:
        summary = connection.execute(
            "SELECT summary FROM worker_results WHERE run_id=?", (runs[0]["id"],)
        ).fetchone()["summary"]
    assert json.loads(summary) == STRUCTURED_RESULT

    worker_row = next(row for row in store.list_workers() if row["id"] == "local-worker-node-01")
    assert worker_row["model_identifier"] == "gemma4-general"
    assert store.highest_event_sequence() > 0

    # The immutable read-only policy must survive the runtime's own metadata.
    assert submitted[0]["job_type"] == "inference.chat"
    assert submitted[0]["role"] == "GENERAL_REASONING"
    assert "model" not in submitted[0]
    assert "codeWriteAllowed" not in submitted[0]


async def test_runtime_negotiates_v2_before_persisting_provider_launch_intent(tmp_path) -> None:
    launch_posts: list[dict] = []

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/v1/health":
            return httpx.Response(
                200,
                json={
                    "protocol_version": 1,
                    "data": {
                        "protocol_versions": [1, 2],
                        "authority_id": "authority-runtime-v2",
                        "registry_id": "registry-runtime-v2",
                        "capabilities": {
                            "supports_reconcile": True,
                            "supports_resume": True,
                            "supports_cancel": True,
                            "supports_repeatable_collect": True,
                            "supports_provider_idempotency": True,
                            "supports_idempotent_launch_lookup": True,
                            "supports_durable_launch_registry": True,
                        },
                    },
                },
            )
        if request.method == "GET" and request.url.path.startswith("/v2/launches/"):
            key = request.url.path.removeprefix("/v2/launches/")
            return httpx.Response(
                404,
                json={
                    "protocol_version": 2,
                    "data": {
                        "accepted": False,
                        "protocol_version": 2,
                        "authority_id": "authority-runtime-v2",
                        "registry_id": "registry-runtime-v2",
                        "idempotency_key": key,
                        "launch_state": "NOT_SEEN",
                        "disposition": "DEFINITELY_NOT_LAUNCHED",
                    },
                },
            )
        if request.method == "POST" and request.url.path == "/v2/launches":
            body = json.loads(request.content)
            assert body["authority_id"] == "authority-runtime-v2"
            assert body["registry_id"] == "registry-runtime-v2"
            launch_posts.append(body)
            return httpx.Response(
                202,
                json={
                    "protocol_version": 2,
                    "data": {
                        "accepted": True,
                        "protocol_version": 2,
                        "authority_id": "authority-runtime-v2",
                        "registry_id": "registry-runtime-v2",
                        "idempotency_key": body["idempotency_key"],
                        "request_digest": body["request_digest"],
                        "launch_state": "RUNNING",
                        "disposition": "DEFINITELY_LAUNCHED",
                        "launch_record_id": "launch-runtime-v2",
                        "job_id": "job-runtime-v2",
                        "receipt_id": "receipt-runtime-v2",
                    },
                },
            )
        if request.method == "GET" and request.url.path == "/v2/jobs/job-runtime-v2":
            return httpx.Response(
                200,
                json={
                    "protocol_version": 2,
                    "data": {
                        "state": "SUCCEEDED",
                        "selected_model": "local-v2-fixture",
                        "result": {"content": json.dumps(STRUCTURED_RESULT)},
                        "metrics": {"total_tokens": 3},
                    },
                },
            )
        raise AssertionError(f"unexpected request: {request.method} {request.url}")

    async with httpx.AsyncClient(
        base_url="http://127.0.0.1:7331", transport=httpx.MockTransport(handler)
    ) as client:
        adapter = LocalWorkerAdapter("http://127.0.0.1:7331", client=client)
        store, runtime = fabric_fixture(tmp_path, adapter)
        task_id = await submit_review(runtime)
        await runtime.run_until_idle()

    runs = store.list_worker_runs(task_id)
    assert len(runs) == 1
    provider_job = store.get_provider_job(runs[0]["id"])
    assert provider_job["adapter_type"] == "local-worker-http-v2"
    assert provider_job["protocol_version"] == 2
    assert provider_job["supports_provider_idempotency"] == 1
    assert provider_job["supports_idempotent_launch_lookup"] == 1
    assert provider_job["supports_durable_launch_registry"] == 1
    assert provider_job["provider_job_id"] == "job-runtime-v2"
    assert len(launch_posts) == 1
    assert launch_posts[0]["idempotency_key"] == provider_job["idempotency_key"]
    assert launch_posts[0]["request_digest"].startswith("sha256:")
    assert store.get_task(task_id)["state"] == TaskState.REVIEWING.value


async def test_runtime_marks_the_worker_offline_when_the_bearer_is_rejected(tmp_path) -> None:
    def handler(_: httpx.Request) -> httpx.Response:
        return httpx.Response(401, json={"error": "unauthorized"})

    async with httpx.AsyncClient(
        base_url="http://127.0.0.1:7331", transport=httpx.MockTransport(handler)
    ) as client:
        adapter = LocalWorkerAdapter("http://127.0.0.1:7331", client=client)
        store, runtime = fabric_fixture(tmp_path, adapter)
        task_id = await submit_review(runtime)
        await runtime.run_until_idle()

    runs = store.list_worker_runs(task_id)
    assert len(runs) == 1
    run = store.get_worker_run(runs[0]["id"])
    assert run["state"] == RunState.AUTH_REQUIRED.value
    assert run["failure_class"] == FailureClass.AUTH.value

    worker_row = next(row for row in store.list_workers() if row["id"] == "local-worker-node-01")
    assert worker_row["state"] == WorkerState.OFFLINE.value
    assert store.get_task(task_id)["state"] != TaskState.SUCCEEDED.value


async def test_code_write_task_is_refused_before_any_local_worker_request(tmp_path) -> None:
    contacted = False

    def handler(_: httpx.Request) -> httpx.Response:
        nonlocal contacted
        contacted = True
        return httpx.Response(200, json={"protocol": 1})

    async with httpx.AsyncClient(
        base_url="http://127.0.0.1:7331", transport=httpx.MockTransport(handler)
    ) as client:
        adapter = LocalWorkerAdapter("http://127.0.0.1:7331", client=client)
        store, runtime = fabric_fixture(tmp_path, adapter)
        task_id = await runtime.submit_task(
            project_id="fabric",
            title="Attempted local code write",
            description="Edit the repository.",
            topology=ExecutionTopology.SINGLE,
            requirements=TaskRequirements(
                labels=frozenset({TaskLabel.CODING}),
                required_capabilities=frozenset({"local-classification"}),
                code_write_required=True,
            ),
        )
        await runtime.run_until_idle()

    # The scheduler refuses first because the worker declares code_write_allowed=False,
    # so no run is ever created. The adapter's own two refusals remain the backstop and
    # are covered in tests/test_adapters_local_worker.py.
    assert not contacted
    assert store.list_worker_runs(task_id) == []
    assert store.get_task(task_id)["state"] == TaskState.BLOCKED.value
