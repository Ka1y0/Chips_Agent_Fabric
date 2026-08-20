from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

import jsonschema
from fastapi.testclient import TestClient

from project_supervisor.api import create_app
from project_supervisor.domain import (
    EvidenceConfidence,
    ExecutionTopology,
    Harness,
    ModelDescriptor,
    NodeState,
    Provider,
    ResourceState,
    RunState,
    TaskRecord,
    TaskRequirements,
    TaskState,
    WorkerSnapshot,
    WorkerState,
)
from project_supervisor.resource_economics import ResourceEconomicsRepository
from project_supervisor.store import StateStore, timestamp
from project_supervisor.telemetry import (
    ExecutorKind,
    InvocationOutcome,
    InvocationTelemetry,
    InvocationTelemetryRepository,
    ObservedDimension,
    ObservedMetric,
    TaskRole,
)


def setup_store(tmp_path: Path) -> StateStore:
    store = StateStore(tmp_path / "economics.db")
    store.create_project(
        project_id="project-1", name="Economics", root_path=str(tmp_path), goal="Observe usage"
    )
    store.upsert_node(
        node_id="node-1",
        hostname="fixture",
        display_name="Fixture",
        role="control",
        state=NodeState.ONLINE,
    )
    for worker_id, harness in (("codex", Harness.CODEX), ("peer", Harness.MOCK)):
        store.upsert_worker(
            WorkerSnapshot(
                id=worker_id,
                node_id="node-1",
                harness=harness,
                provider=Provider.MOCK,
                model=ModelDescriptor("model", "Model", Provider.MOCK),
                state=WorkerState.IDLE,
                node_state=NodeState.ONLINE,
                resource_state=ResourceState.AVAILABLE,
                capabilities=frozenset({"analysis"}),
                code_write_allowed=False,
                privacy_allowed=True,
            )
        )
    for index in (1, 2):
        task = TaskRecord(
            id=f"task-{index}",
            project_id="project-1",
            title=f"Task {index}",
            description="Accepted task",
            state=TaskState.DRAFT,
            topology=ExecutionTopology.SINGLE,
            requirements=TaskRequirements(labels=frozenset()),
        )
        store.create_task(task, f"#000{index}")
        store.transition_task(task.id, TaskState.QUEUED)
        store.transition_task(task.id, TaskState.READY)
        store.transition_task(task.id, TaskState.RUNNING)
        store.transition_task(task.id, TaskState.REVIEWING)
        store.transition_task(task.id, TaskState.SUCCEEDED)
    now = timestamp()
    with store.transaction() as connection:
        connection.execute(
            "INSERT INTO autonomous_goals("
            "id,project_id,intent,effective_intent,state,termination_reason,termination_detail,"
            "budgets_json,created_at,updated_at,finished_at) VALUES (?,?,?,?,?,?,?,?,?,?,?)",
            (
                "goal-1",
                "project-1",
                "intent",
                "intent",
                "terminated",
                "SUCCESS",
                "verified",
                "{}",
                now,
                now,
                now,
            ),
        )
        connection.execute(
            "INSERT INTO autonomous_iterations("
            "id,goal_id,sequence,state,started_at,updated_at,completed_at) VALUES (?,?,?,?,?,?,?)",
            ("iteration-1", "goal-1", 1, "completed", now, now, now),
        )
        for index in (1, 2):
            connection.execute(
                "INSERT INTO autonomous_actions("
                "id,goal_id,iteration_id,ordinal,action_key,title,description,role,payload_json,"
                "state,task_id,created_at,updated_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    f"action-{index}",
                    "goal-1",
                    "iteration-1",
                    index - 1,
                    f"key-{index}",
                    f"Action {index}",
                    "Run",
                    "primary",
                    "{}",
                    "completed",
                    f"task-{index}",
                    now,
                    now,
                ),
            )
    return store


def record(
    store: StateStore,
    *,
    index: int,
    task_id: str,
    worker_id: str,
    executor: ExecutorKind,
    input_tokens: int | None,
    output_tokens: int | None,
    quality: float | None,
) -> None:
    run_id = store.create_worker_run(task_id=task_id, worker_id=worker_id, attempt=1)
    store.transition_worker_run(run_id, RunState.RUNNING)
    store.transition_worker_run(run_id, RunState.COMPLETED)
    unavailable = ObservedMetric.unavailable()
    InvocationTelemetryRepository(store).record(
        InvocationTelemetry(
            id=f"telemetry-{index}",
            goal_id="goal-1",
            task_id=task_id,
            run_id=run_id,
            worker_id=worker_id,
            provider="mock",
            node_id="node-1",
            task_role=TaskRole.PRIMARY,
            outcome=InvocationOutcome.SUCCESS,
            duration=ObservedMetric.known(1, "seconds", EvidenceConfidence.EXACT),
            model=ObservedDimension.known("model"),
            input_tokens=(
                ObservedMetric.known(input_tokens, "tokens")
                if input_tokens is not None
                else unavailable
            ),
            output_tokens=(
                ObservedMetric.known(output_tokens, "tokens")
                if output_tokens is not None
                else unavailable
            ),
            executor_kind=executor,
            quality_score=(
                ObservedMetric.known(quality, "ratio", EvidenceConfidence.VERIFIED)
                if quality is not None
                else unavailable
            ),
            recorded_at=datetime(2026, 8, 9, tzinfo=UTC) + timedelta(seconds=index),
        )
    )


def validate(payload: dict) -> None:
    schema = json.loads(
        (Path(__file__).parents[1] / "schemas/resource-economics-v1.schema.json").read_text()
    )
    jsonschema.Draft202012Validator(schema, format_checker=jsonschema.FormatChecker()).validate(
        payload
    )


def test_resource_economics_are_complete_and_provenance_is_explicit(tmp_path: Path) -> None:
    store = setup_store(tmp_path)
    record(
        store,
        index=1,
        task_id="task-1",
        worker_id="codex",
        executor=ExecutorKind.CODEX,
        input_tokens=10,
        output_tokens=5,
        quality=0.5,
    )
    record(
        store,
        index=2,
        task_id="task-2",
        worker_id="peer",
        executor=ExecutorKind.OFFLOADED,
        input_tokens=20,
        output_tokens=5,
        quality=1.0,
    )
    payload = ResourceEconomicsRepository(store).aggregate(goal_id="goal-1").to_protocol()
    validate(payload)
    assert payload["codexOffloadRatio"] == {
        "state": "known",
        "value": 0.5,
        "unit": "ratio",
        "provenance": "LOCALLY_MEASURED",
    }
    assert payload["qualityAdjustedOffload"]["value"] == 2 / 3
    assert payload["qualityAdjustedOffload"]["provenance"] == "INFERRED"
    assert payload["tokensPerSuccessfulGoal"]["value"] == 40
    assert payload["tokensPerAcceptedTask"]["value"] == 20
    assert payload["coverage"]["unknownInvocationCount"] == 0

    client = TestClient(create_app(store, allow_unauthenticated_loopback=True))
    response = client.get("/v1/resources/economics", params={"goalID": "goal-1"})
    assert response.status_code == 200
    assert response.json()["data"] == payload


def test_unknown_token_dimension_makes_outcome_rates_unknown(tmp_path: Path) -> None:
    store = setup_store(tmp_path)
    record(
        store,
        index=1,
        task_id="task-1",
        worker_id="peer",
        executor=ExecutorKind.UNKNOWN,
        input_tokens=10,
        output_tokens=None,
        quality=None,
    )
    payload = ResourceEconomicsRepository(store).aggregate(goal_id="goal-1").to_protocol()
    validate(payload)
    assert payload["codexOffloadRatio"]["state"] == "unavailable"
    assert payload["qualityAdjustedOffload"]["provenance"] == "UNKNOWN"
    assert payload["tokensPerSuccessfulGoal"]["state"] == "unavailable"
    assert payload["tokensPerAcceptedTask"]["state"] == "unavailable"
    assert payload["coverage"]["invocationCount"] == 1
    assert payload["coverage"]["completeInvocationCount"] == 0
    assert payload["coverage"]["unknownInvocationCount"] == 1
    assert payload["coverage"]["successfulGoalsWithCompleteTelemetry"] == 0
    assert payload["coverage"]["acceptedTasksWithCompleteTelemetry"] == 0
    assert payload["coverage"]["unknownExecutorCallCount"] == 1
    assert payload["coverage"]["unscoredCallCount"] == 1
