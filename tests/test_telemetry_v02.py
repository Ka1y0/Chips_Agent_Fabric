from __future__ import annotations

import asyncio
import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

import jsonschema
import pytest
from fastapi.testclient import TestClient

from project_supervisor.adapters import MockAdapter, MockBehavior
from project_supervisor.adapters.base import Usage, WorkerResult
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
    TaskLabel,
    TaskRecord,
    TaskRequirements,
    TaskState,
    WorkerSnapshot,
    WorkerState,
)
from project_supervisor.runtime import AdapterRegistry, SupervisorRuntime
from project_supervisor.scheduler import DeterministicScheduler
from project_supervisor.store import StateStore, timestamp
from project_supervisor.telemetry import (
    ExecutorKind,
    InvocationTelemetry,
    InvocationTelemetryRepository,
    ObservedMetric,
    TaskRole,
)


def validate_schema(name: str, payload: dict[str, object]) -> None:
    schema = json.loads((Path(__file__).parents[1] / "schemas" / name).read_text())
    jsonschema.Draft202012Validator(schema, format_checker=jsonschema.FormatChecker()).validate(
        payload
    )


@pytest.fixture
def telemetry_store(tmp_path: Path) -> tuple[StateStore, str, str]:
    store = StateStore(tmp_path / "state.db")
    store.create_project(
        project_id="project-1",
        name="Telemetry fixture",
        root_path=str(tmp_path),
        goal="Observe without fabrication",
    )
    store.upsert_node(
        node_id="node-1",
        hostname="fixture",
        display_name="Fixture",
        role="control",
        state=NodeState.ONLINE,
    )
    for worker_id, harness in (("codex-worker", Harness.CODEX), ("peer-worker", Harness.MOCK)):
        store.upsert_worker(
            WorkerSnapshot(
                id=worker_id,
                node_id="node-1",
                harness=harness,
                provider=Provider.MOCK,
                model=ModelDescriptor("declared", "Declared", Provider.MOCK),
                state=WorkerState.IDLE,
                node_state=NodeState.ONLINE,
                resource_state=ResourceState.AVAILABLE,
                capabilities=frozenset({"analysis"}),
                code_write_allowed=False,
                privacy_allowed=True,
            )
        )
    store.create_task(
        TaskRecord(
            id="task-1",
            project_id="project-1",
            title="Observe",
            description="Observe invocation telemetry",
            state=TaskState.DRAFT,
            topology=ExecutionTopology.FALLBACK,
            requirements=TaskRequirements(labels=frozenset({TaskLabel.RESEARCH})),
        ),
        "#0001",
    )
    store.transition_task("task-1", TaskState.QUEUED)
    store.transition_task("task-1", TaskState.READY)
    store.transition_task("task-1", TaskState.RUNNING)
    with store.transaction() as connection:
        now = timestamp()
        connection.execute(
            "INSERT INTO autonomous_goals("
            "id,project_id,intent,effective_intent,state,budgets_json,created_at,updated_at"
            ") VALUES (?,?,?,?,?,?,?,?)",
            ("goal-1", "project-1", "observe", "observe", "running", "{}", now, now),
        )
        connection.execute(
            "INSERT INTO autonomous_iterations("
            "id,goal_id,sequence,state,started_at,updated_at"
            ") VALUES (?,?,?,?,?,?)",
            ("iteration-1", "goal-1", 1, "dispatching", now, now),
        )
        connection.execute(
            "INSERT INTO autonomous_actions("
            "id,goal_id,iteration_id,ordinal,action_key,title,description,role,payload_json,state,"
            "task_id,created_at,updated_at"
            ") VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                "action-1",
                "goal-1",
                "iteration-1",
                0,
                "observe",
                "Observe",
                "Observe",
                "verifier",
                "{}",
                "running",
                "task-1",
                now,
                now,
            ),
        )
    first_run = store.create_worker_run(task_id="task-1", worker_id="codex-worker", attempt=1)
    store.transition_worker_run(first_run, RunState.RUNNING)
    second_run = store.create_worker_run(task_id="task-1", worker_id="peer-worker", attempt=2)
    store.transition_worker_run(second_run, RunState.RUNNING)
    return store, first_run, second_run


def result(
    run_id: str,
    *,
    state: RunState,
    model: str | None,
    usage: Usage,
    offset: int,
) -> WorkerResult:
    started = datetime(2026, 8, 9, 1, 0, tzinfo=UTC) + timedelta(seconds=offset)
    return WorkerResult(
        run_id=run_id,
        state=state,
        pid=123,
        exit_code=0 if state is RunState.COMPLETED else 1,
        started_at=started,
        ended_at=started + timedelta(seconds=2),
        stdout="",
        stderr="",
        final_text="done" if state is RunState.COMPLETED else "",
        events=(),
        model=model,
        usage=usage,
    )


def test_invocation_round_trip_schema_and_unknown_are_explicit(
    telemetry_store: tuple[StateStore, str, str],
) -> None:
    store, first_run, _ = telemetry_store
    repository = InvocationTelemetryRepository(store)
    assert repository.goal_context_for_task("task-1") == ("goal-1", TaskRole.VERIFIER)
    record = InvocationTelemetry.from_worker_result(
        telemetry_id="telemetry-1",
        goal_id=repository.goal_id_for_task("task-1"),
        task_id="task-1",
        worker_id="codex-worker",
        provider="mock",
        node_id="node-1",
        task_role=TaskRole.PRIMARY,
        executor_kind=ExecutorKind.CODEX,
        result=result(
            first_run,
            state=RunState.COMPLETED,
            model=None,
            usage=Usage(input_tokens=0, output_tokens=4, cost_usd=None),
            offset=0,
        ),
    )
    repository.record(record)

    restored = repository.list(task_id="task-1")[0]
    payload = restored.to_protocol()
    validate_schema("invocation-telemetry-v1.schema.json", payload)
    assert payload["goalID"] == "goal-1"
    assert payload["inputTokens"] == {
        "state": "known",
        "value": 0,
        "confidence": "providerReported",
        "unit": "tokens",
    }
    assert payload["cost"]["state"] == "unavailable"
    assert payload["cost"]["reason"] == "notReported"
    assert payload["model"] == {"state": "unavailable", "reason": "notReported"}


def test_task_and_goal_aggregation_preserve_partial_observability(
    telemetry_store: tuple[StateStore, str, str],
) -> None:
    store, first_run, second_run = telemetry_store
    repository = InvocationTelemetryRepository(store)
    repository.record(
        InvocationTelemetry.from_worker_result(
            telemetry_id="telemetry-1",
            goal_id="goal-1",
            task_id="task-1",
            worker_id="codex-worker",
            provider="mock",
            node_id="node-1",
            task_role=TaskRole.PRIMARY,
            executor_kind=ExecutorKind.CODEX,
            quality_score=ObservedMetric.known(0.5, "ratio", EvidenceConfidence.VERIFIED),
            result=result(
                first_run,
                state=RunState.FAILED,
                model="model-a",
                usage=Usage(input_tokens=None, output_tokens=0),
                offset=0,
            ),
        )
    )
    repository.record(
        InvocationTelemetry.from_worker_result(
            telemetry_id="telemetry-2",
            goal_id="goal-1",
            task_id="task-1",
            worker_id="peer-worker",
            provider="mock",
            node_id="node-1",
            task_role=TaskRole.FALLBACK,
            executor_kind=ExecutorKind.OFFLOADED,
            retry_count=1,
            fallback=True,
            remaining_quota=ObservedMetric.known(
                7, "requests", EvidenceConfidence.PROVIDER_REPORTED
            ),
            quality_score=ObservedMetric.known(1.0, "ratio", EvidenceConfidence.VERIFIED),
            result=result(
                second_run,
                state=RunState.COMPLETED,
                model="model-b",
                usage=Usage(input_tokens=10, output_tokens=3, cache_read_tokens=2, cost_usd=0),
                offset=5,
            ),
        )
    )

    aggregate = repository.aggregate(goal_id="goal-1").to_protocol()
    validate_schema("telemetry-aggregate-v1.schema.json", aggregate)
    assert aggregate["callCount"] == 2
    assert aggregate["callsByWorker"] == {"codex-worker": 1, "peer-worker": 1}
    assert aggregate["callsByOutcome"] == {"failure": 1, "success": 1}
    assert aggregate["callsByTaskRole"] == {"fallback": 1, "primary": 1}
    assert aggregate["retryCallCount"] == 1
    assert aggregate["retryAttemptCount"] == 1
    assert aggregate["fallbackCount"] == 1
    assert aggregate["inputTokens"]["knownCount"] == 1
    assert aggregate["inputTokens"]["unavailableCount"] == 1
    assert aggregate["inputTokens"]["observedSum"]["value"] == 10
    assert aggregate["cost"]["observedSum"]["value"] == 0
    assert aggregate["remainingQuota"]["latest"]["value"] == 7
    assert aggregate["codexOffload"]["ratio"]["value"] == 0.5
    assert aggregate["qualityAdjustedOffload"]["ratio"]["value"] == pytest.approx(2 / 3)
    assert repository.aggregate(task_id="missing").to_protocol()["callCount"] == 0


def test_unknown_executor_does_not_inflate_offload_ratio(
    telemetry_store: tuple[StateStore, str, str],
) -> None:
    store, first_run, _ = telemetry_store
    repository = InvocationTelemetryRepository(store)
    repository.record(
        InvocationTelemetry.from_worker_result(
            telemetry_id="telemetry-unknown",
            task_id="task-1",
            worker_id="codex-worker",
            provider="mock",
            node_id="node-1",
            task_role=TaskRole.OTHER,
            executor_kind=ExecutorKind.UNKNOWN,
            result=result(
                first_run,
                state=RunState.CANCELLED,
                model=None,
                usage=Usage(),
                offset=0,
            ),
        )
    )
    aggregate = repository.aggregate(task_id="task-1").to_protocol()
    assert aggregate["codexOffload"]["unknownCalls"] == 1
    assert aggregate["codexOffload"]["ratio"]["state"] == "unavailable"
    assert aggregate["qualityAdjustedOffload"]["ratio"]["state"] == "unavailable"


async def test_runtime_automatically_records_real_attempt_and_fallback_telemetry(
    tmp_path: Path,
) -> None:
    store = StateStore(tmp_path / "runtime.db")
    store.create_project(
        project_id="project-runtime",
        name="Runtime telemetry",
        root_path=str(tmp_path),
        goal="Exercise automatic invocation recording",
    )
    store.upsert_node(
        node_id="node-runtime",
        hostname="fixture",
        display_name="Fixture",
        role="control",
        state=NodeState.ONLINE,
    )
    registry = AdapterRegistry()
    for worker_id, behavior in (
        ("primary", MockBehavior(text="", exit_code=7, model="failed-model")),
        ("fallback", MockBehavior(text="OK", model="fallback-model")),
    ):
        store.upsert_worker(
            WorkerSnapshot(
                id=worker_id,
                node_id="node-runtime",
                harness=Harness.MOCK,
                provider=Provider.MOCK,
                model=ModelDescriptor("declared", "Declared", Provider.MOCK),
                state=WorkerState.IDLE,
                node_state=NodeState.ONLINE,
                resource_state=ResourceState.AVAILABLE,
                capabilities=frozenset({"analysis"}),
                code_write_allowed=False,
                privacy_allowed=True,
                quality_score=0.9 if worker_id == "primary" else 0.7,
            )
        )
        registry.register(worker_id, MockAdapter(behavior))
    runtime = SupervisorRuntime(
        store=store,
        scheduler=DeterministicScheduler(),
        adapters=registry,
        evidence_root=tmp_path / "evidence",
    )
    task_id = await runtime.submit_task(
        project_id="project-runtime",
        title="Fallback",
        description="Use fallback after failure",
        topology=ExecutionTopology.FALLBACK,
        requirements=TaskRequirements(
            labels=frozenset({TaskLabel.RESEARCH}),
            required_capabilities=frozenset({"analysis"}),
            preferred_workers=("primary", "fallback"),
        ),
    )
    await runtime.run_until_idle()

    records = runtime.telemetry.list(task_id=task_id)
    assert [(record.worker_id, record.outcome.value) for record in records] == [
        ("primary", "failure"),
        ("fallback", "success"),
    ]
    assert records[0].task_role is TaskRole.PRIMARY
    assert records[0].retry_count == 0
    assert records[0].fallback is False
    assert records[1].task_role is TaskRole.FALLBACK
    assert records[1].retry_count == 1
    assert records[1].fallback is True
    assert records[1].model.value == "fallback-model"
    assert records[1].cost.telemetry.value == 0
    assert records[1].remaining_quota.telemetry.known is False
    aggregate = runtime.telemetry.aggregate(task_id=task_id).to_protocol()
    assert aggregate["callCount"] == 2
    assert aggregate["codexOffload"]["ratio"]["value"] == 1
    assert aggregate["qualityAdjustedOffload"]["ratio"]["state"] == "unavailable"

    store.upsert_worker(
        WorkerSnapshot(
            id="cancellable",
            node_id="node-runtime",
            harness=Harness.MOCK,
            provider=Provider.MOCK,
            model=ModelDescriptor("declared", "Declared", Provider.MOCK),
            state=WorkerState.IDLE,
            node_state=NodeState.ONLINE,
            resource_state=ResourceState.AVAILABLE,
            capabilities=frozenset({"slow"}),
            code_write_allowed=False,
            privacy_allowed=True,
        )
    )
    registry.register(
        "cancellable", MockAdapter(MockBehavior(delay_seconds=0.05, model="cancelled-model"))
    )
    cancelled_task = await runtime.submit_task(
        project_id="project-runtime",
        title="Cancel",
        description="Wait for cancellation",
        requirements=TaskRequirements(
            labels=frozenset({TaskLabel.RESEARCH}),
            required_capabilities=frozenset({"slow"}),
        ),
    )
    await runtime.dispatch_ready()
    await asyncio.sleep(0.01)
    assert await runtime.cancel_task(cancelled_task) is True
    await runtime.wait_for_active()
    assert store.get_task(cancelled_task)["state"] == TaskState.CANCELLED.value
    cancelled = runtime.telemetry.list(task_id=cancelled_task)
    assert len(cancelled) == 1
    assert cancelled[0].outcome.value == "cancelled"


async def test_restart_recovery_records_interrupted_invocation_without_fabricating_usage(
    telemetry_store: tuple[StateStore, str, str], tmp_path: Path
) -> None:
    store, first_run, second_run = telemetry_store
    store.transition_worker_run(second_run, RunState.FAILED)
    runtime = SupervisorRuntime(
        store=store,
        scheduler=DeterministicScheduler(),
        adapters=AdapterRegistry(),
        evidence_root=tmp_path / "evidence",
        max_attempts=3,
    )

    result_summary = await runtime.recover()

    assert result_summary["runsInterrupted"] == 1
    assert store.get_task("task-1")["state"] == TaskState.READY.value
    records = runtime.telemetry.list(task_id="task-1")
    assert len(records) == 1
    assert records[0].run_id == first_run
    assert records[0].outcome.value == "interrupted"
    assert records[0].task_role is TaskRole.VERIFIER
    assert records[0].model.value is None
    assert records[0].input_tokens.telemetry.known is False
    assert records[0].cost.telemetry.known is False


def test_read_only_api_exposes_safe_goal_telemetry_aggregate(
    telemetry_store: tuple[StateStore, str, str],
) -> None:
    store, first_run, _ = telemetry_store
    repository = InvocationTelemetryRepository(store)
    repository.record(
        InvocationTelemetry.from_worker_result(
            telemetry_id="telemetry-api",
            goal_id="goal-1",
            task_id="task-1",
            worker_id="codex-worker",
            provider="mock",
            node_id="node-1",
            task_role=TaskRole.VERIFIER,
            executor_kind=ExecutorKind.CODEX,
            result=result(
                first_run,
                state=RunState.COMPLETED,
                model="model-api",
                usage=Usage(input_tokens=2, output_tokens=1, cost_usd=None),
                offset=0,
            ),
        )
    )
    client = TestClient(
        create_app(store, allow_unauthenticated_loopback=True),
        client=("127.0.0.1", 50000),
    )

    response = client.get("/v1/telemetry", params={"goalID": "goal-1"})

    assert response.status_code == 200
    body = response.json()["data"]
    assert body["aggregate"]["callCount"] == 1
    assert body["aggregate"]["cost"]["observedSum"]["state"] == "unavailable"
    assert body["invocations"][0]["taskRole"] == "verifier"
    assert "stdout" not in body["invocations"][0]
