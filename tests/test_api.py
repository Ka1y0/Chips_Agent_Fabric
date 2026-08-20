import hashlib
import json

import pytest
from fastapi.testclient import TestClient

from project_supervisor.api import APISettings, create_app
from project_supervisor.domain import (
    EventSeverity,
    ExecutionTopology,
    FailureClass,
    RunState,
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
            ("costUSD", 0.01, "usd"),
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


def seed_fabric_semantic_api(api_store: StateStore, *, private_marker: str) -> None:
    now = timestamp()
    digest = "a" * 64
    with api_store.transaction() as connection:
        connection.execute(
            "INSERT INTO autonomous_goals(id,project_id,intent,effective_intent,state,"
            "budgets_json,created_at,updated_at) VALUES (?,?,?,?,?,?,?,?)",
            (
                "goal-fabric-1",
                "project-1",
                "Exercise Fabric projections",
                "Exercise Fabric projections",
                "running",
                "{}",
                now,
                now,
            ),
        )
        manifest = {
            "capabilities": [{"name": "review-code", "parameters": {"apiToken": private_marker}}],
            "models": ["model-1"],
            "locality": "remote",
            "privacy": "internal",
            "costMode": "subscription",
            "incrementalCostUSD": 0.0,
            "maxConcurrency": 2,
            "prompt": private_marker,
        }
        connection.execute(
            "INSERT INTO worker_capability_manifests(id,worker_id,revision,schema_version,"
            "catalog_version,provider_id,adapter_kind,definition_sha256,source,observed_at,"
            "valid_until,manifest_json,created_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                "manifest-1",
                "worker-1",
                1,
                "worker-capability-manifest/v1",
                "fabric-capabilities/v1",
                "anthropic",
                "claudeCode",
                f"sha256:{digest}",
                private_marker,
                now,
                None,
                json.dumps(manifest),
                now,
            ),
        )
        connection.execute(
            "INSERT INTO worker_capability_manifest_heads(worker_id,manifest_id,generation,"
            "updated_at) VALUES (?,?,?,?)",
            ("worker-1", "manifest-1", 1, now),
        )
        connection.execute(
            "INSERT INTO worker_capability_observations(id,worker_id,manifest_id,version,"
            "health,active_jobs,subscription_state,quota_state,quota_freshness,state_json,"
            "observed_at,created_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                "observation-1",
                "worker-1",
                "manifest-1",
                1,
                "healthy",
                1,
                "available",
                "scarce",
                "fresh",
                json.dumps({"authorizationToken": private_marker}),
                now,
                now,
            ),
        )
        connection.execute(
            "INSERT INTO routing_decisions(id,task_id,topology,policy_version,"
            "selected_workers_json,explanation_json,created_at) VALUES (?,?,?,?,?,?,?)",
            (
                "routing-fabric-1",
                "task-1",
                "single",
                "fabric-routing/v1",
                '["worker-1"]',
                json.dumps({"prompt": private_marker}),
                now,
            ),
        )
        connection.execute(
            "INSERT INTO routing_candidates(decision_id,worker_id,selected,score,"
            "components_json,rejection_code,rejection_detail) VALUES (?,?,?,?,?,?,?)",
            (
                "routing-fabric-1",
                "worker-1",
                1,
                0.95,
                json.dumps({"privatePath": private_marker}),
                "",
                private_marker,
            ),
        )
        connection.execute(
            "UPDATE tasks SET execution_spec_json=? WHERE id='task-1'",
            (json.dumps({"prompt": private_marker, "path": private_marker}),),
        )
        connection.execute(
            "INSERT INTO child_work_proposals(id,goal_id,parent_task_id,source_run_id,"
            "proposal_key,semantic_digest,source_result_sha256,depth,plan_version,steer_version,"
            "task_definition_revision,specification_json,created_at) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                "proposal-fabric-1",
                "goal-fabric-1",
                "task-1",
                "run-1",
                "review-follow-up",
                digest,
                "b" * 64,
                2,
                3,
                4,
                5,
                json.dumps({"prompt": private_marker, "token": private_marker}),
                now,
            ),
        )
        connection.execute(
            "INSERT INTO child_work_proposal_decisions(id,proposal_id,revision,outcome,"
            "policy_version,reason_code,child_task_id,budget_json,created_at) "
            "VALUES (?,?,?,?,?,?,?,?,?)",
            (
                "proposal-decision-1",
                "proposal-fabric-1",
                1,
                "deferred",
                "spawn-policy/v1",
                "TASK_CONCURRENCY_LIMIT",
                None,
                json.dumps({"secret": private_marker}),
                now,
            ),
        )
        connection.execute(
            "INSERT INTO result_fusion_decisions(id,task_id,source_attempt,policy_version,"
            "input_set_sha256,classification,fused_json,conflicts_json,provenance_json,"
            "confidence,verification_required,created_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                "fusion-1",
                "task-1",
                2,
                "deterministic-fusion/v1",
                "c" * 64,
                "compatible",
                json.dumps({"prompt": private_marker}),
                json.dumps({"value": private_marker}),
                json.dumps({"path": private_marker}),
                0.75,
                1,
                now,
            ),
        )
        connection.execute(
            "INSERT INTO fusion_verification_handoffs(id,fusion_id,verification_scope_id,"
            "task_definition_revision,source_attempt,steer_version,token_sha256,created_at) "
            "VALUES (?,?,?,?,?,?,?,?)",
            ("handoff-1", "fusion-1", None, 5, 2, 4, "d" * 64, now),
        )
        connection.execute(
            "INSERT INTO interaction_resources(resource_key,resource_type,scope_id,created_at,"
            "updated_at) VALUES (?,?,?,?,?)",
            ("browser.context-1", "browserContext", "scope-1", now, now),
        )
        connection.execute(
            "INSERT INTO interaction_resource_leases(lease_id,resource_key,lease_group_id,"
            "owner_id,task_id,run_id,generation,state,acquired_at,heartbeat_at,expires_at,"
            "released_at) VALUES (?,?,?,?,?,?,?,'active',?,?,?,NULL)",
            (
                "lease-1",
                "browser.context-1",
                "lease-group-1",
                f"owner-{private_marker}",
                "task-1",
                "run-1",
                1,
                now,
                now,
                "2999-01-01T00:00:00Z",
            ),
        )
        connection.execute(
            "INSERT INTO interaction_executions(id,project_id,task_id,run_id,worker_id,"
            "adapter_kind,channel,app_id,app_version,plan_schema,plan_sha256,state,"
            "observation_count,grounding_count,started_at,updated_at,finished_at) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                "interaction-1",
                "project-1",
                "task-1",
                "run-1",
                "worker-1",
                "native",
                "accessibility",
                "com.example.app",
                "1.2.3",
                "ui-plan/v1",
                "e" * 64,
                "succeeded",
                2,
                1,
                now,
                now,
                now,
            ),
        )
        connection.execute(
            "INSERT INTO ui_snapshots(id,execution_id,previous_snapshot_id,ordinal,app_id,"
            "window_id,source,state_sha256,safe_tree_json,focus_element_id,observed_at) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?)",
            (
                "snapshot-1",
                "interaction-1",
                None,
                0,
                "com.example.app",
                "window-1",
                "accessibility",
                "f" * 64,
                json.dumps({"name": private_marker, "value": private_marker}),
                private_marker,
                now,
            ),
        )
        connection.execute(
            "INSERT INTO interaction_trajectories(id,execution_id,schema_version,verified,"
            "trajectory_json,created_at) VALUES (?,?,?,?,?,?)",
            (
                "trajectory-1",
                "interaction-1",
                "interaction-trajectory/v1",
                1,
                json.dumps({"typedValue": private_marker}),
                now,
            ),
        )
        connection.execute(
            "INSERT INTO semantic_skills(id,app_id,semantic_action,app_version_constraint,"
            "lifecycle,revision,template_json,success_count,failure_count,confidence,"
            "source_trajectory_id,last_verified_at,created_at,updated_at) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                "skill-1",
                "com.example.app",
                "document.review",
                "1.2.3",
                "active",
                1,
                json.dumps({"locator": private_marker, "typedValue": private_marker}),
                2,
                0,
                0.9,
                "trajectory-1",
                now,
                now,
                now,
            ),
        )
        connection.execute(
            "INSERT INTO semantic_skill_evidence(skill_id,trajectory_id,outcome,verified,"
            "app_version,observed_at) VALUES (?,?,?,?,?,?)",
            ("skill-1", "trajectory-1", "success", 1, "1.2.3", now),
        )
        connection.execute(
            "INSERT INTO ui_graph_edges(app_id,app_version,from_state_sha256,semantic_action,"
            "to_state_sha256,success_count,failure_count,last_verified_at,confidence,created_at,"
            "updated_at) VALUES (?,?,?,?,?,?,?,?,?,?,?)",
            (
                "com.example.app",
                "1.2.3",
                "1" * 64,
                "document.review",
                "2" * 64,
                3,
                1,
                now,
                0.8,
                now,
                now,
            ),
        )


def test_rest_envelopes_match_cyber_office_contract(client: TestClient) -> None:
    for path in (
        "/v1/status",
        "/v1/nodes",
        "/v1/workers",
        "/v1/runs",
        "/v1/runs/run-1",
        "/v1/escalations",
        "/v1/tasks",
        "/v1/tasks/task-1",
    ):
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
    assert "runs" in capabilities["resources"]
    assert "executionEscalations" in capabilities["resources"]
    assert capabilities["mutations"] == []
    assert capabilities["safety"]["arbitraryShell"] is False
    schemas = client.get("/v1/schemas").json()["data"]
    assert schemas["openAPI"] == "/openapi.json"
    assert schemas["streamFrames"] == ["snapshot", "events", "keepalive"]


def test_fabric_read_routes_are_semantic_allowlists(
    api_store: StateStore,
    client: TestClient,
) -> None:
    marker = "private-fabric-marker-987654"
    seed_fabric_semantic_api(api_store, private_marker=marker)

    responses = {
        "capabilities": client.get("/v1/fabric/capabilities"),
        "routing": client.get("/v1/fabric/routing", params={"taskID": "task-1"}),
        "spawn": client.get("/v1/fabric/spawn-proposals", params={"goalID": "goal-fabric-1"}),
        "fusion": client.get("/v1/fabric/fusions", params={"taskID": "task-1"}),
        "interactions": client.get(
            "/v1/interactions", params={"taskID": "task-1", "state": "succeeded"}
        ),
        "resources": client.get(
            "/v1/interaction-resources",
            params={"resourceType": "browserContext", "activeOnly": True},
        ),
        "skills": client.get(
            "/v1/skills", params={"appID": "com.example.app", "lifecycle": "active"}
        ),
        "graph": client.get("/v1/ui-graph", params={"appID": "com.example.app"}),
    }
    assert all(response.status_code == 200 for response in responses.values())
    serialized = json.dumps(
        {name: response.json() for name, response in responses.items()}, sort_keys=True
    )
    assert marker not in serialized

    capability_data = responses["capabilities"].json()["data"]
    assert capability_data["protocolVersion"] == "fabric-v0.3-dev"
    worker = capability_data["workers"][0]
    assert set(worker) == {"workerID", "nodeID", "manifest", "observation"}
    assert set(worker["manifest"]) == {
        "id",
        "revision",
        "schemaVersion",
        "catalogVersion",
        "providerID",
        "adapterKind",
        "definitionSHA256",
        "capabilityNames",
        "models",
        "locality",
        "privacy",
        "costMode",
        "incrementalCostUSD",
        "maxConcurrency",
        "observedAt",
        "validUntil",
        "headGeneration",
        "headUpdatedAt",
    }
    assert worker["manifest"]["capabilityNames"] == ["review-code"]
    assert worker["manifest"]["models"] == ["model-1"]
    assert set(worker["observation"]) == {
        "id",
        "version",
        "health",
        "activeJobs",
        "subscriptionState",
        "quotaState",
        "quotaFreshness",
        "observedAt",
    }

    routing = responses["routing"].json()["data"][0]
    assert set(routing) == {
        "id",
        "taskID",
        "topology",
        "policyVersion",
        "selectedWorkerIDs",
        "candidates",
        "createdAt",
    }
    assert routing["selectedWorkerIDs"] == ["worker-1"]
    assert set(routing["candidates"][0]) == {
        "workerID",
        "selected",
        "score",
        "rejectionCode",
    }

    spawn = responses["spawn"].json()["data"][0]
    assert set(spawn) == {
        "id",
        "goalID",
        "parentTaskID",
        "sourceRunID",
        "proposalKey",
        "semanticDigest",
        "sourceResultSHA256",
        "depth",
        "planVersion",
        "steerVersion",
        "taskDefinitionRevision",
        "createdAt",
        "latestDecision",
    }
    assert set(spawn["latestDecision"]) == {
        "id",
        "revision",
        "outcome",
        "policyVersion",
        "reasonCode",
        "childTaskID",
        "createdAt",
    }

    fusion = responses["fusion"].json()["data"][0]
    assert set(fusion) == {
        "id",
        "taskID",
        "sourceAttempt",
        "policyVersion",
        "inputSetSHA256",
        "classification",
        "confidence",
        "verificationRequired",
        "createdAt",
        "verificationHandoffs",
    }
    assert set(fusion["verificationHandoffs"][0]) == {
        "id",
        "verificationScopeID",
        "taskDefinitionRevision",
        "sourceAttempt",
        "steerVersion",
        "createdAt",
    }

    interaction = responses["interactions"].json()["data"][0]
    assert set(interaction) == {
        "id",
        "projectID",
        "taskID",
        "runID",
        "workerID",
        "adapterKind",
        "channel",
        "appID",
        "appVersion",
        "planSHA256",
        "state",
        "observationCount",
        "groundingCount",
        "startedAt",
        "updatedAt",
        "finishedAt",
        "trajectory",
    }
    assert set(interaction["trajectory"]) == {"id", "verified", "createdAt"}

    resource = responses["resources"].json()["data"][0]
    assert set(resource) == {
        "resourceKey",
        "resourceType",
        "scopeID",
        "createdAt",
        "updatedAt",
        "activeLease",
    }
    assert set(resource["activeLease"]) == {
        "leaseID",
        "leaseGroupID",
        "taskID",
        "runID",
        "generation",
        "state",
        "acquiredAt",
        "heartbeatAt",
        "expiresAt",
    }

    skill = responses["skills"].json()["data"][0]
    assert set(skill) == {
        "id",
        "appID",
        "semanticAction",
        "appVersionConstraint",
        "lifecycle",
        "revision",
        "successCount",
        "failureCount",
        "confidence",
        "sourceTrajectoryID",
        "evidenceCount",
        "verifiedEvidenceCount",
        "lastVerifiedAt",
        "createdAt",
        "updatedAt",
    }
    graph = responses["graph"].json()["data"][0]
    assert set(graph) == {
        "appID",
        "appVersion",
        "fromStateSHA256",
        "semanticAction",
        "toStateSHA256",
        "successCount",
        "failureCount",
        "lastVerifiedAt",
        "confidence",
        "createdAt",
        "updatedAt",
    }


def test_fabric_capability_projection_does_not_mix_manifest_generations(
    api_store: StateStore,
    client: TestClient,
) -> None:
    seed_fabric_semantic_api(api_store, private_marker="private-generation-marker")
    now = timestamp()
    with api_store.transaction() as connection:
        connection.execute(
            "INSERT INTO worker_capability_manifests(id,worker_id,revision,schema_version,"
            "catalog_version,provider_id,adapter_kind,definition_sha256,source,observed_at,"
            "valid_until,manifest_json,created_at) SELECT ?,worker_id,2,schema_version,"
            "catalog_version,provider_id,adapter_kind,?,source,?,valid_until,manifest_json,? "
            "FROM worker_capability_manifests WHERE id=?",
            ("manifest-2", f"sha256:{'b' * 64}", now, now, "manifest-1"),
        )
        connection.execute(
            "UPDATE worker_capability_manifest_heads SET manifest_id=?,generation=2,updated_at=? "
            "WHERE worker_id=?",
            ("manifest-2", now, "worker-1"),
        )

    worker = client.get("/v1/fabric/capabilities").json()["data"]["workers"][0]
    assert worker["manifest"]["id"] == "manifest-2"
    assert worker["observation"] is None


@pytest.mark.parametrize(
    ("path", "parameters"),
    (
        ("/v1/interactions", {"state": "unknown"}),
        ("/v1/interaction-resources", {"resourceType": "screenCoordinates"}),
        ("/v1/skills", {"lifecycle": "unverified"}),
    ),
)
def test_fabric_read_route_filters_fail_closed(
    client: TestClient,
    path: str,
    parameters: dict[str, str],
) -> None:
    assert client.get(path, params=parameters).status_code == 400


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
    assert task["definitionRevision"] == 1
    assert task["verificationScope"] is None
    assert task["sessions"][0]["conversationID"] == "provider-conversation-1"
    assert task["progressFraction"]["state"] == "unavailable"
    assert task["dependencies"] == []
    assert task["blockedByDependencies"] == []

    missing = client.get("/v1/tasks/missing")
    assert missing.status_code == 404
    assert missing.json()["code"] == "task_not_found"
    assert client.get("/v1/tasks/run-shaped-id").json()["code"] == "task_not_found"


def test_task_verification_scope_projection_is_allowlisted_and_semantic(
    api_store: StateStore,
    client: TestClient,
) -> None:
    private_criterion_value = "PRIVATE_CRITERION_DEFINITION_7719"
    criterion_id = api_store.add_acceptance_criterion(
        project_id="project-1",
        criterion_id="criterion-task-v1",
        kind="fileExists",
        description=f"Do not project {private_criterion_value}",
        command=["fixture-check", private_criterion_value],
        expected={"private": private_criterion_value},
    )
    scope = api_store.bind_task_verification_scope(
        "task-1",
        criterion_ids=[criterion_id],
    )

    detail_response = client.get("/v1/tasks/task-1")
    list_response = client.get("/v1/tasks")
    assert detail_response.status_code == list_response.status_code == 200
    detail = detail_response.json()["data"]
    listed = next(task for task in list_response.json()["data"] if task["id"] == "task-1")
    expected = {
        "id": scope["id"],
        "criteriaVersion": 1,
        "taskDefinitionRevision": 1,
        "goalID": None,
        "iterationID": None,
        "planVersion": None,
        "steerVersion": None,
        "schemaVersion": "task-verification-scope/v1",
        "definitionSHA256": scope["definition_sha256"],
        "criterionCount": 1,
        "createdAt": scope["created_at"],
    }
    assert detail["definitionRevision"] == listed["definitionRevision"] == 1
    assert detail["verificationScope"] == listed["verificationScope"] == expected
    assert private_criterion_value not in detail_response.text
    assert private_criterion_value not in list_response.text
    for private_field in ("description", "command", "expected", "items"):
        assert private_field not in detail["verificationScope"]

    events = client.get(
        "/v1/events",
        params={"kind": "taskVerificationScopeBound", "task": "task-1"},
    ).json()["data"]
    assert len(events) == 1
    assert events[0]["kind"] == "taskVerificationScopeBound"
    assert events[0]["payload"]["detail"]["fields"]["verificationScopeID"] == scope["id"]


def test_run_inspection_is_bounded_and_filterable(
    api_store: StateStore, client: TestClient
) -> None:
    second_run_id = api_store.create_worker_run(task_id="task-1", worker_id="worker-1", attempt=2)

    runs = client.get("/v1/runs", params={"limit": 1}).json()["data"]
    assert len(runs) == 1

    running = client.get("/v1/runs", params={"state": "running"}).json()["data"]
    assert [run["id"] for run in running] == ["run-1"]
    assert running[0]["exitCode"] is None
    assert running[0]["failure"] is None
    assert running[0]["result"] is None
    assert running[0]["evidenceReference"] is None
    assert running[0]["usage"]["cacheReadTokens"] == {
        "state": "unavailable",
        "reason": "notReported",
    }
    starting = client.get("/v1/runs", params={"state": "starting"}).json()["data"]
    assert [run["id"] for run in starting] == [second_run_id]
    assert len(client.get("/v1/runs", params={"taskID": "task-1"}).json()["data"]) == 2
    assert len(client.get("/v1/runs", params={"workerID": "worker-1"}).json()["data"]) == 2
    assert client.get("/v1/runs", params={"taskID": "missing"}).json()["data"] == []
    assert client.get("/v1/runs", params={"workerID": "missing"}).json()["data"] == []

    assert client.get("/v1/runs", params={"state": "not-a-state"}).status_code == 400
    assert client.get("/v1/runs", params={"limit": 0}).status_code == 422
    assert client.get("/v1/runs", params={"limit": 501}).status_code == 422


def test_run_detail_exposes_normalized_result_usage_and_references(
    api_store: StateStore, client: TestClient
) -> None:
    api_store.transition_worker_run(
        "run-1",
        RunState.FAILED,
        exit_code=7,
        raw_output_reference="evidence/token=raw-reference-secret",
        failure_class=FailureClass.TRANSIENT,
        failure_detail="token=failure-secret",
    )
    api_store.save_worker_result(
        run_id="run-1",
        summary="Reviewed token=result-secret",
        changed_files=["src/fixture.py"],
        commands_run=["pytest tests/test_fixture.py"],
        tests=[{"name": "fixture", "passed": False}],
        artifacts=["artifacts/result.json"],
        commit_hash="abc123",
        blockers=["Needs deterministic repair"],
        confidence=0.75,
        recommended_next_actions=["Repair fixture"],
    )

    response = client.get("/v1/runs/run-1")
    assert response.status_code == 200
    run = response.json()["data"]
    assert run["id"] == "run-1"
    assert run["projectID"] == "project-1"
    assert run["taskID"] == "task-1"
    assert run["workerID"] == "worker-1"
    assert run["nodeID"] == "node-1"
    assert run["provider"] == "anthropic"
    assert run["harness"] == "claudeCode"
    assert run["model"]["identifier"] == "claude-opus-5"
    assert run["sessionID"] == "session-1"
    assert run["providerSessionID"] == "provider-conversation-1"
    assert run["state"] == "failed"
    assert run["attempt"] == 1
    assert run["processID"] == 421
    assert run["startedAt"] is not None
    assert run["endedAt"] is not None
    assert run["exitCode"] == 7
    assert run["failure"] == {"class": "transient", "detail": "token=[REDACTED]"}
    assert run["usage"]["inputTokens"] == {"state": "known", "value": 12}
    assert run["usage"]["estimatedCostUSD"] == {"state": "known", "value": 0.01}
    assert run["usage"]["cacheReadTokens"] == {
        "state": "unavailable",
        "reason": "notReported",
    }
    assert run["result"] == {
        "summary": "Reviewed token=[REDACTED]",
        "changedFiles": ["src/fixture.py"],
        "commandsRun": ["pytest tests/test_fixture.py"],
        "tests": [{"name": "fixture", "passed": False}],
        "artifacts": ["artifacts/result.json"],
        "commitHash": "abc123",
        "blockers": ["Needs deterministic repair"],
        "confidence": 0.75,
        "recommendedNextActions": ["Repair fixture"],
        "createdAt": run["result"]["createdAt"],
    }
    assert run["evidenceReference"] == "evidence/token=[REDACTED]"
    assert "raw-reference-secret" not in response.text
    assert "result-secret" not in response.text

    missing = client.get("/v1/runs/missing")
    assert missing.status_code == 404
    assert missing.json()["code"] == "run_not_found"


def test_provider_job_and_escalation_projection_is_allowlisted_and_secret_safe(
    api_store: StateStore, client: TestClient
) -> None:
    metadata_secret = "COMPOSITE_METADATA_SECRET_8917"
    session_secret = "PROVIDER_SESSION_SECRET_8917"
    runtime_host_secret = "PRIVATE_RUNTIME_HOST_8917"
    runtime_identity_secret = "PRIVATE_PROCESS_IDENTITY_8917"
    idempotency_key = "supervisor-execution:PRIVATE_IDEMPOTENCY_SECRET_8917"
    now = timestamp()
    with api_store.transaction() as connection:
        connection.execute(
            "INSERT INTO provider_jobs("
            "id,run_id,task_id,worker_id,adapter_type,adapter_instance_id,provider,handle_version,"
            "provider_job_id,"
            "provider_session_id,runtime_pid,runtime_host,runtime_identity,launch_generation,"
            "idempotency_key,launch_state,reconciliation_state,result_collection_state,"
            "supports_reconcile,supports_resume,supports_cancel,supports_durable_cancel,"
            "supports_provider_idempotency,supports_stream_reconnect,supports_repeatable_collect,"
            "supports_idempotent_launch_lookup,supports_durable_launch_registry,protocol_version,"
            "adapter_metadata_json,"
            "created_at,launched_at,last_reconciled_at,result_collected_at,updated_at"
            ") VALUES ("
            ":id,:run_id,:task_id,:worker_id,:adapter_type,:adapter_instance_id,:provider,"
            ":handle_version,"
            ":provider_job_id,:provider_session_id,:runtime_pid,:runtime_host,:runtime_identity,"
            ":launch_generation,:idempotency_key,:launch_state,:reconciliation_state,"
            ":result_collection_state,:supports_reconcile,:supports_resume,:supports_cancel,"
            ":supports_durable_cancel,:supports_provider_idempotency,:supports_stream_reconnect,"
            ":supports_repeatable_collect,:supports_idempotent_launch_lookup,"
            ":supports_durable_launch_registry,:protocol_version,:adapter_metadata_json,"
            ":created_at,:launched_at,"
            ":last_reconciled_at,"
            ":result_collected_at,:updated_at)",
            {
                "id": "provider-job-internal-1",
                "run_id": "run-1",
                "task_id": "task-1",
                "worker_id": "worker-1",
                "adapter_type": "localWorker",
                "adapter_instance_id": "local-worker-instance-1",
                "provider": "anthropic",
                "handle_version": 1,
                "provider_job_id": "external-job-token=provider-id-secret",
                "provider_session_id": session_secret,
                "runtime_pid": 98_765,
                "runtime_host": runtime_host_secret,
                "runtime_identity": runtime_identity_secret,
                "launch_generation": 4,
                "idempotency_key": idempotency_key,
                "launch_state": "bound",
                "reconciliation_state": "providerUnreachable",
                "result_collection_state": "pending",
                "supports_reconcile": 1,
                "supports_resume": 1,
                "supports_cancel": 1,
                "supports_durable_cancel": 0,
                "supports_provider_idempotency": 1,
                "supports_stream_reconnect": 0,
                "supports_repeatable_collect": 1,
                "supports_idempotent_launch_lookup": 1,
                "supports_durable_launch_registry": 1,
                "protocol_version": 2,
                "adapter_metadata_json": json.dumps(
                    {
                        "authHeaders": {"X-Api-Key": metadata_secret},
                        "endpoint": f"https://private.invalid/jobs?accessToken={metadata_secret}",
                        "runtimePath": f"/private/{metadata_secret}/worker.sock",
                        "nodeID": "node-local-1",
                        "driverID": "claude-reviewed",
                        "driverType": "claude",
                        "driverProfileRevision": 7,
                        "driverProfileFingerprint": "sha256:" + "a" * 64,
                        "launchRuntimeInstanceID": "runtime-launch-1",
                    }
                ),
                "created_at": now,
                "launched_at": now,
                "last_reconciled_at": now,
                "result_collected_at": None,
                "updated_at": now,
            },
        )
        connection.execute(
            "INSERT INTO execution_escalations("
            "id,project_id,task_id,run_id,provider_job_id,code,state,summary,detail,created_by,"
            "created_at,updated_at) VALUES (?,?,?,?,?,?,'open',?,?,?,?,?)",
            (
                "escalation-1",
                "project-1",
                "task-1",
                "run-1",
                "provider-job-internal-1",
                "EXTERNAL_JOB_UNREACHABLE",
                "Provider job is temporarily unreachable",
                f"authHeaders.X-Api-Key={metadata_secret}",
                "recovery",
                now,
                now,
            ),
        )

    response = client.get("/v1/runs/run-1")
    assert response.status_code == 200
    job = response.json()["data"]["providerJob"]
    assert job == {
        "id": "provider-job-internal-1",
        "adapterType": "localWorker",
        "protocolVersion": 2,
        "provider": "anthropic",
        "externalID": "external-job-token=[REDACTED]",
        "launchGeneration": 4,
        "state": "bound",
        "reconciliation": {
            "state": "providerUnreachable",
            "lastReconciledAt": now,
        },
        "capabilities": {
            "supportsReconcile": True,
            "supportsResume": True,
            "supportsCancel": True,
            "supportsDurableCancel": False,
            "supportsProviderIdempotency": True,
            "supportsStreamReconnect": False,
            "supportsRepeatableCollect": True,
            "supportsIdempotentLaunchLookup": True,
            "supportsDurableLaunchRegistry": True,
        },
        "resultCollection": {"state": "pending", "collectedAt": None},
        "idempotencyKeyFingerprint": hashlib.sha256(idempotency_key.encode()).hexdigest()[:16],
        "createdAt": now,
        "launchedAt": now,
        "updatedAt": now,
        "runtime": {
            "nodeID": "node-local-1",
            "launchRuntimeInstanceID": "runtime-launch-1",
            "driver": {
                "id": "claude-reviewed",
                "type": "claude",
                "profileRevision": 7,
                "profileFingerprint": "sha256:" + "a" * 64,
            },
        },
        "openEscalations": [
            {
                "id": "escalation-1",
                "projectID": "project-1",
                "goalID": None,
                "taskID": "task-1",
                "runID": "run-1",
                "providerJobID": "provider-job-internal-1",
                "code": "EXTERNAL_JOB_UNREACHABLE",
                "state": "open",
                "summary": "Provider job is temporarily unreachable",
                "createdAt": now,
                "updatedAt": now,
                "resolvedAt": None,
            }
        ],
    }
    for private_value in (
        "provider-id-secret",
        metadata_secret,
        session_secret,
        runtime_host_secret,
        runtime_identity_secret,
        idempotency_key,
        "authHeaders",
        "X-Api-Key",
        "accessToken",
        "runtimePath",
    ):
        assert private_value not in response.text
    for private_field in (
        "adapterMetadata",
        "adapter_metadata_json",
        "runtimePID",
        "runtimeHost",
        "runtimeIdentity",
        "idempotencyKey",
        "endpoint",
    ):
        assert private_field not in job

    escalations = client.get(
        "/v1/escalations",
        params={
            "state": "open",
            "runID": "run-1",
            "taskID": "task-1",
            "code": "EXTERNAL_JOB_UNREACHABLE",
        },
    )
    assert escalations.status_code == 200
    assert escalations.json()["data"] == job["openEscalations"]
    assert metadata_secret not in escalations.text
    assert "detail" not in escalations.text

    detail = client.get("/v1/escalations/escalation-1")
    assert detail.status_code == 200
    assert detail.json()["data"] == job["openEscalations"][0]
    missing = client.get("/v1/escalations/missing")
    assert missing.status_code == 404
    assert missing.json()["code"] == "escalation_not_found"

    assert client.get("/v1/escalations", params={"state": "invalid"}).status_code == 400
    assert client.get("/v1/escalations", params={"code": "UNKNOWN"}).status_code == 400
    assert client.get("/v1/escalations", params={"limit": 0}).status_code == 422


def test_run_detail_does_not_expose_absolute_host_evidence_path(
    api_store: StateStore, client: TestClient, tmp_path
) -> None:
    absolute_reference = str(tmp_path / "private" / "token=host-path-secret" / "output.log")
    api_store.transition_worker_run(
        "run-1",
        RunState.FAILED,
        exit_code=1,
        raw_output_reference=absolute_reference,
        failure_class=FailureClass.TRANSIENT,
        failure_detail="fixture failure",
    )

    response = client.get("/v1/runs/run-1")

    assert response.status_code == 200
    assert response.json()["data"]["evidenceReference"] is None
    assert absolute_reference not in response.text
    assert "host-path-secret" not in response.text


def test_task_session_projection_redacts_secret_bearing_provider_identity(
    api_store: StateStore, client: TestClient
) -> None:
    with api_store.transaction() as connection:
        connection.execute(
            "UPDATE sessions SET provider_session_id=? WHERE id='session-1'",
            ("token=provider-session-secret",),
        )

    response = client.get("/v1/tasks/task-1")

    assert response.status_code == 200
    session = response.json()["data"]["sessions"][0]
    assert session["conversationID"] == "token=[REDACTED]"
    assert "provider-session-secret" not in response.text


def test_usage_cost_aliases_are_selected_per_run_before_aggregation(
    api_store: StateStore, client: TestClient
) -> None:
    second_run_id = api_store.create_worker_run(task_id="task-1", worker_id="worker-1", attempt=2)
    now = timestamp()
    with api_store.transaction() as connection:
        for usage_id, metric, value in (
            ("usage-run-2-canonical-cost", "estimatedCostUSD", 0.02),
            ("usage-run-2-legacy-alias", "costUSD", 9.99),
        ):
            connection.execute(
                "INSERT INTO usage_records(id,task_id,run_id,worker_id,model_id,metric,value,"
                "unit,confidence,recorded_at) VALUES (?,?,?,?,?,?,?,?,?,?)",
                (
                    usage_id,
                    "task-1",
                    second_run_id,
                    "worker-1",
                    "model-1",
                    metric,
                    value,
                    "usd",
                    "providerReported",
                    now,
                ),
            )

    worker = client.get("/v1/workers").json()["data"][0]
    task = client.get("/v1/tasks/task-1").json()["data"]
    second_run = client.get(f"/v1/runs/{second_run_id}").json()["data"]

    expected = {"state": "known", "value": 0.03}
    assert worker["usage"]["estimatedCostUSD"] == expected
    assert task["usage"]["estimatedCostUSD"] == expected
    assert second_run["usage"]["estimatedCostUSD"] == {
        "state": "known",
        "value": 0.02,
    }


@pytest.mark.parametrize("run_id", ["goal-shaped-id", "project-shaped-id"])
def test_run_not_found_classification_uses_resource_type(client: TestClient, run_id: str) -> None:
    response = client.get(f"/v1/runs/{run_id}")

    assert response.status_code == 404
    assert response.json()["code"] == "run_not_found"


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


def test_adapter_event_credentials_never_reach_sqlite_wal_or_public_api(
    api_store: StateStore,
    client: TestClient,
) -> None:
    marker = "CREDENTIAL-MARKER-MUST-NOT-PERSIST"
    api_store.record_adapter_event(
        run_id="run-1",
        kind="credentialFixture",
        payload={
            "sessionCookie": marker,
            "githubToken": marker,
            "input_tokens": 41,
            "remaining_tokens": 12,
            "nested": [
                {
                    "bearer-token": marker,
                    "oauth token": marker,
                    "client secret": marker,
                }
            ],
        },
    )

    with api_store.connect() as connection:
        row = connection.execute(
            "SELECT payload_json FROM events WHERE kind='workerAdapterEvent' "
            "ORDER BY sequence DESC LIMIT 1"
        ).fetchone()
    assert row is not None
    persisted = json.loads(row["payload_json"])
    assert persisted["sessionCookie"] == "[REDACTED]"
    assert persisted["githubToken"] == "[REDACTED]"
    assert set(persisted["nested"][0].values()) == {"[REDACTED]"}
    assert persisted["input_tokens"] == 41
    assert persisted["remaining_tokens"] == 12

    response = client.get("/v1/events", params={"task": "task-1"})
    assert response.status_code == 200
    serialized = json.dumps(response.json(), sort_keys=True)
    assert marker not in serialized
    projected = next(
        event
        for event in response.json()["data"]
        if event["payload"]["detail"]["fields"].get("adapterEventKind") == "credentialFixture"
    )
    fields = projected["payload"]["detail"]["fields"]
    assert fields["sessionCookie"] == "[REDACTED]"
    assert fields["githubToken"] == "[REDACTED]"
    assert fields["input_tokens"] == "41"
    assert fields["remaining_tokens"] == "12"

    for path in (api_store.path, api_store.path.with_name(f"{api_store.path.name}-wal")):
        if path.exists():
            assert marker.encode() not in path.read_bytes()


def test_semantic_orchestration_event_kind_is_preserved(
    api_store: StateStore,
    client: TestClient,
) -> None:
    with api_store.transaction() as connection:
        api_store._append_event(
            connection,
            kind="taskDependencyAdded",
            severity=EventSeverity.INFO,
            entity_type="task",
            entity_id="task-1",
            project_id="project-1",
            task_id="task-1",
            summary="Dependency committed",
            payload={"dependsOnTaskID": "task-prerequisite"},
            actor="scheduler",
        )

    events = client.get(
        "/v1/events", params={"kind": "taskDependencyAdded", "task": "task-1"}
    ).json()["data"]

    assert len(events) == 1
    assert events[0]["kind"] == "taskDependencyAdded"


@pytest.mark.parametrize(
    "kind",
    (
        "workerCapabilityManifestRecorded",
        "workerCapabilityObservationRecorded",
        "capabilityRegistered",
        "capabilityUpdated",
        "capabilityDynamicStateUpdated",
        "nodeExecutionRecoveryLeaseAcquired",
        "nodeRecoveryStarted",
        "nodeRecoveryAccepted",
        "nodeRecoveryRejected",
        "nodeRecoveryFailed",
        "nodeRecoveryOutcomeUnknown",
        "nodeRecovered",
        "providerProcessStarted",
        "providerInferenceStarted",
        "providerInferenceCompleted",
        "providerInvocationRejected",
        "childWorkProposed",
        "childWorkProposalDecided",
        "subagentRootBound",
        "subagentProposed",
        "subagentAccepted",
        "subagentRejected",
        "resultFusionRecorded",
        "fusionVerificationHandoffCreated",
        "fusionStarted",
        "fusionConflictDetected",
        "fusionCompleted",
        "uiResourceRegistered",
        "uiResourceAcquired",
        "uiResourceReleased",
        "uiExecutionStarted",
        "uiObserved",
        "uiActionVerified",
        "uiActionFailed",
        "trajectoryCompleted",
        "skillCandidateCreated",
        "skillValidated",
        "skillActivated",
        "skillInvalidated",
        "uiGraphTransitionRecorded",
    ),
)
def test_fabric_event_kinds_are_preserved(
    api_store: StateStore,
    client: TestClient,
    kind: str,
) -> None:
    with api_store.transaction() as connection:
        api_store._append_event(
            connection,
            kind=kind,
            severity=EventSeverity.INFO,
            entity_type="task",
            entity_id="task-1",
            project_id="project-1",
            task_id="task-1",
            summary="Fabric semantic lifecycle event",
            payload={"state": "observed"},
            actor="fabric",
        )

    events = client.get("/v1/events", params={"kind": kind, "task": "task-1"}).json()["data"]

    assert len(events) == 1
    assert events[0]["kind"] == kind


@pytest.mark.parametrize(
    "kind",
    (
        "providerJobPrepared",
        "providerJobLaunchStarted",
        "providerJobHandleBound",
        "providerJobReconciled",
        "providerJobResultCollected",
        "humanEscalationRequested",
        "humanEscalationResolved",
    ),
)
def test_provider_job_and_escalation_event_kinds_are_preserved(
    api_store: StateStore,
    client: TestClient,
    kind: str,
) -> None:
    with api_store.transaction() as connection:
        api_store._append_event(
            connection,
            kind=kind,
            severity=EventSeverity.INFO,
            entity_type="workerRun",
            entity_id="run-1",
            project_id="project-1",
            task_id="task-1",
            worker_id="worker-1",
            run_id="run-1",
            summary="Durable provider lifecycle event",
            payload={"state": "knownRunning"},
            actor="recovery",
        )

    events = client.get("/v1/events", params={"kind": kind, "task": "task-1"}).json()["data"]

    assert len(events) == 1
    assert events[0]["kind"] == kind


def test_remote_configuration_requires_scoped_bearer(api_store: StateStore) -> None:
    app = create_app(
        api_store,
        APISettings(bind_host="0.0.0.0", allow_unauthenticated_loopback=True),
    )
    client = TestClient(app, client=("10.20.30.40", 50000))
    assert client.get("/v1/status").status_code == 401
    fabric_paths = (
        "/v1/fabric/capabilities",
        "/v1/fabric/execution-plane",
        "/v1/fabric/authorizations",
        "/v1/fabric/data-provenance",
        "/v1/fabric/provider-invocations",
        "/v1/fabric/hypotheses",
        "/v1/fabric/write-authorities",
    )
    assert all(client.get(path).status_code == 401 for path in fabric_paths)

    _, wrong = api_store.issue_api_token(label="wrong", scopes={"tasks:write"})
    assert client.get("/v1/status", headers={"Authorization": f"Bearer {wrong}"}).status_code == 403

    _, observe = api_store.issue_api_token(label="monitor", scopes={"observe:read"})
    response = client.get("/v1/status", headers={"Authorization": f"Bearer {observe}"})
    assert response.status_code == 200
    assert all(
        client.get(path, headers={"Authorization": f"Bearer {observe}"}).status_code == 200
        for path in fabric_paths
    )


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
