import json

import pytest
from fastapi.testclient import TestClient

from project_supervisor.api import APISettings, create_app
from project_supervisor.domain import (
    ExecutionTopology,
    TaskLabel,
    TaskRecord,
    TaskRequirements,
    TaskState,
)
from project_supervisor.store import StateStore, timestamp


@pytest.fixture
def api_store(tmp_path) -> StateStore:
    store = StateStore(tmp_path / "api-state.db")
    store.create_project(
        project_id="project-1",
        name="API fixture",
        root_path=str(tmp_path / "fixture"),
        goal="Exercise read-only projections",
    )
    task = TaskRecord(
        id="task-1",
        project_id="project-1",
        title="Observe worker",
        description="A deterministic API fixture",
        state=TaskState.DRAFT,
        topology=ExecutionTopology.SINGLE,
        requirements=TaskRequirements(labels=frozenset({TaskLabel.REVIEW})),
        priority=80,
    )
    store.create_task(task, "#001")
    store.transition_task("task-1", TaskState.QUEUED)
    store.transition_task("task-1", TaskState.READY)
    store.transition_task("task-1", TaskState.RUNNING)
    now = timestamp()
    with store.transaction() as connection:
        connection.execute(
            "INSERT INTO nodes(id,hostname,display_name,role,state,operating_system,"
            "last_heartbeat_at,created_at,updated_at) VALUES (?,?,?,?,?,?,?,?,?)",
            (
                "node-1",
                "control-node-01",
                "Mac Control",
                "control",
                "online",
                "macOS",
                now,
                now,
                now,
            ),
        )
        connection.execute(
            "INSERT INTO models(id,provider,identifier,display_name,context_variant,"
            "context_window_tokens) VALUES (?,?,?,?,?,?)",
            ("model-1", "anthropic", "claude-opus-5", "Claude Opus 5", "1M", 1_000_000),
        )
        connection.execute(
            "INSERT INTO workers(id,node_id,harness,provider,model_id,state,resource_state,"
            "capabilities_json,code_write_allowed,privacy_allowed,harness_version,"
            "last_heartbeat_at,created_at,updated_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                "worker-1",
                "node-1",
                "claudeCode",
                "anthropic",
                "model-1",
                "running",
                "available",
                '["review"]',
                1,
                1,
                "claude-code fixture",
                now,
                now,
                now,
            ),
        )
        connection.execute(
            "INSERT INTO sessions(id,worker_id,provider_session_id,model_id,state,created_at,"
            "updated_at) VALUES (?,?,?,?,?,?,?)",
            ("session-1", "worker-1", "provider-conversation-1", "model-1", "active", now, now),
        )
        connection.execute(
            "INSERT INTO worker_runs(id,task_id,worker_id,session_id,state,process_id,attempt,"
            "started_at,last_event_at,created_at,updated_at) VALUES (?,?,?,?,?,?,?,?,?,?,?)",
            (
                "run-1",
                "task-1",
                "worker-1",
                "session-1",
                "running",
                421,
                1,
                now,
                now,
                now,
                now,
            ),
        )
        for metric, value, unit in (
            ("inputTokens", 12, "tokens"),
            ("outputTokens", 3, "tokens"),
            ("estimatedCostUSD", 0.01, "usd"),
        ):
            connection.execute(
                "INSERT INTO usage_records(id,task_id,run_id,worker_id,model_id,metric,value,"
                "unit,confidence,recorded_at) VALUES (?,?,?,?,?,?,?,?,?,?)",
                (
                    f"usage-{metric}",
                    "task-1",
                    "run-1",
                    "worker-1",
                    "model-1",
                    metric,
                    value,
                    unit,
                    "providerReported",
                    now,
                ),
            )
        connection.execute(
            "INSERT INTO events(event_id,kind,severity,entity_type,entity_id,project_id,task_id,"
            "worker_id,run_id,summary,payload_json,actor,created_at) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                "evt-worker-output",
                "output",
                "info",
                "workerRun",
                "run-1",
                "project-1",
                "task-1",
                "worker-1",
                "run-1",
                "Worker emitted output",
                json.dumps(
                    {
                        "harness": "claude",
                        "detail": {"subtype": "assistant", "text": "fixture"},
                    }
                ),
                "claude-adapter",
                now,
            ),
        )
    return store


@pytest.fixture
def client(api_store: StateStore) -> TestClient:
    app = create_app(api_store, allow_unauthenticated_loopback=True)
    return TestClient(app, client=("127.0.0.1", 50000))


def test_rest_envelopes_match_cyber_office_contract(client: TestClient) -> None:
    for path in ("/v1/status", "/v1/nodes", "/v1/workers", "/v1/tasks", "/v1/tasks/task-1"):
        response = client.get(path)
        assert response.status_code == 200
        body = response.json()
        assert body["apiVersion"] == "v1"
        assert body["generatedAt"].endswith("Z")
        assert "data" in body

    worker = client.get("/v1/workers").json()["data"][0]
    assert worker["harness"] == "claudeCode"
    assert worker["provider"] == "anthropic"
    assert worker["model"]["identifier"] == "claude-opus-5"
    assert worker["usage"]["inputTokens"] == {"state": "known", "value": 12}
    assert worker["usage"]["cacheReadTokens"] == {
        "state": "unavailable",
        "reason": "notReported",
    }


def test_agent_native_discovery_is_machine_readable(client: TestClient) -> None:
    health = client.get("/v1/health").json()["data"]
    assert health == {"status": "ok", "apiVersion": "v1", "readOnly": True}
    capabilities = client.get("/v1/capabilities").json()["data"]
    assert capabilities["observation"]["webSocket"] is True
    assert capabilities["mutations"] == []
    assert capabilities["safety"]["arbitraryShell"] is False
    schemas = client.get("/v1/schemas").json()["data"]
    assert schemas["openAPI"] == "/openapi.json"
    assert schemas["streamFrames"] == ["snapshot", "events", "keepalive"]


def test_unknown_node_telemetry_is_never_fabricated_as_zero(client: TestClient) -> None:
    node = client.get("/v1/nodes").json()["data"][0]
    assert node["metrics"]["cpuLoadPercent"] == {
        "state": "unavailable",
        "reason": "notReported",
    }
    assert node["queuedJobCount"]["state"] == "unavailable"
    assert node["runningJobCount"] == {"state": "known", "value": 1}


def test_worker_node_role_uses_cyber_office_contract(api_store: StateStore) -> None:
    now = timestamp()
    with api_store.transaction() as connection:
        connection.execute(
            "INSERT INTO nodes(id,hostname,display_name,role,state,operating_system,"
            "last_heartbeat_at,created_at,updated_at) VALUES (?,?,?,?,?,?,?,?,?)",
            (
                "gpu-node-1",
                "worker-node-01",
                "Windows GPU Worker",
                "worker",
                "online",
                "Windows",
                now,
                now,
                now,
            ),
        )

    response = TestClient(
        create_app(api_store, allow_unauthenticated_loopback=True),
        client=("127.0.0.1", 50000),
    ).get("/v1/nodes")
    nodes = {node["id"]: node for node in response.json()["data"]}
    assert nodes["node-1"]["role"] == "control"
    assert nodes["gpu-node-1"]["role"] == "gpuCompute"


def test_task_detail_populates_session_and_not_found_is_stable(client: TestClient) -> None:
    task = client.get("/v1/tasks/task-1").json()["data"]
    assert task["state"] == "running"
    assert task["priority"] == "high"
    assert task["sessions"][0]["conversationID"] == "provider-conversation-1"
    assert task["progressFraction"]["state"] == "unavailable"

    missing = client.get("/v1/tasks/missing")
    assert missing.status_code == 404
    assert missing.json()["code"] == "task_not_found"


def test_event_filters_cursor_and_normalized_payload(client: TestClient) -> None:
    first = client.get("/v1/events", params={"limit": 2, "task": "task-1"}).json()
    assert len(first["data"]) == 2
    assert first["meta"]["hasMore"] is True
    assert first["meta"]["nextCursor"]
    assert first["data"][0]["sequence"] > first["data"][1]["sequence"]

    second = client.get(
        "/v1/events", params={"limit": 2, "task": "task-1", "cursor": first["meta"]["nextCursor"]}
    ).json()
    assert {event["id"] for event in first["data"]}.isdisjoint(
        event["id"] for event in second["data"]
    )

    output = client.get("/v1/events", params={"kind": "output"}).json()["data"][0]
    assert output["payload"]["harness"] == "claude"
    assert output["payload"]["detail"]["text"] == "fixture"

    invalid = client.get("/v1/events", params={"cursor": "not-a-cursor"})
    assert invalid.status_code == 400


def test_remote_configuration_requires_scoped_bearer(api_store: StateStore) -> None:
    app = create_app(
        api_store,
        APISettings(bind_host="0.0.0.0", allow_unauthenticated_loopback=True),
    )
    client = TestClient(app, client=("10.20.30.40", 50000))
    assert client.get("/v1/status").status_code == 401

    _, wrong = api_store.issue_api_token(label="wrong", scopes={"tasks:write"})
    assert client.get("/v1/status", headers={"Authorization": f"Bearer {wrong}"}).status_code == 403

    _, observe = api_store.issue_api_token(label="monitor", scopes={"observe:read"})
    response = client.get("/v1/status", headers={"Authorization": f"Bearer {observe}"})
    assert response.status_code == 200


def test_websocket_snapshot_replay_and_keepalive(api_store: StateStore) -> None:
    app = create_app(
        api_store,
        APISettings(
            bind_host="127.0.0.1",
            allow_unauthenticated_loopback=True,
            keepalive_seconds=0.05,
            poll_interval_seconds=0.01,
        ),
    )
    client = TestClient(app, client=("127.0.0.1", 50000))
    after = api_store.highest_event_sequence() - 1
    with client.websocket_connect(f"/v1/stream?afterSequence={after}") as socket:
        snapshot = socket.receive_json()
        assert snapshot["type"] == "snapshot"
        assert snapshot["snapshot"]["generatedAt"].endswith("Z")
        replay = socket.receive_json()
        assert replay["type"] == "events"
        assert all(event["sequence"] > after for event in replay["events"])
        keepalive = socket.receive_json()
        assert keepalive["type"] == "keepalive"
        assert keepalive["highestSequence"] == api_store.highest_event_sequence()
