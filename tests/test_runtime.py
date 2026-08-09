from __future__ import annotations

import json

from project_supervisor.adapters import MockAdapter, MockBehavior
from project_supervisor.domain import (
    ApprovalState,
    ExecutionTopology,
    Harness,
    ModelDescriptor,
    NodeState,
    PermissionClass,
    Provider,
    ResourceState,
    TaskLabel,
    TaskRequirements,
    TaskState,
    WorkerSnapshot,
    WorkerState,
)
from project_supervisor.runtime import AdapterRegistry, SupervisorRuntime
from project_supervisor.scheduler import DeterministicScheduler
from project_supervisor.store import StateStore
from project_supervisor.verification import DefinitionOfDoneResult, VerificationResult


def register_worker(
    store: StateStore,
    registry: AdapterRegistry,
    worker_id: str,
    adapter: MockAdapter,
    *,
    quality: float = 0.8,
) -> None:
    store.upsert_worker(
        WorkerSnapshot(
            id=worker_id,
            node_id="node-1",
            harness=Harness.MOCK,
            provider=Provider.MOCK,
            model=ModelDescriptor("declared-model", "Declared Model", Provider.MOCK),
            state=WorkerState.IDLE,
            node_state=NodeState.ONLINE,
            resource_state=ResourceState.AVAILABLE,
            capabilities=frozenset({"analysis", "review"}),
            code_write_allowed=False,
            privacy_allowed=True,
            quality_score=quality,
            reliability_score=0.9,
            expected_latency_seconds=0.01,
            monetary_cost_score=1.0,
        )
    )
    registry.register(worker_id, adapter)


def runtime_fixture(tmp_path, *, max_attempts: int = 2):
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    store = StateStore(tmp_path / "state.db")
    store.create_project(
        project_id="project-1",
        name="Runtime fixture",
        root_path=str(workspace),
        goal="Exercise the event-driven runtime",
    )
    store.upsert_node(
        node_id="node-1",
        hostname="fixture",
        display_name="Fixture Node",
        role="control",
        state=NodeState.ONLINE,
    )
    registry = AdapterRegistry()
    runtime = SupervisorRuntime(
        store=store,
        scheduler=DeterministicScheduler(),
        adapters=registry,
        evidence_root=tmp_path / "evidence",
        max_attempts=max_attempts,
    )
    return store, registry, runtime


async def test_runtime_persists_route_run_session_usage_events_and_evidence(tmp_path) -> None:
    store, registry, runtime = runtime_fixture(tmp_path)
    register_worker(
        store,
        registry,
        "worker-1",
        MockAdapter(MockBehavior(text="ANALYSIS_COMPLETE", model="observed-model")),
    )
    task_id = await runtime.submit_task(
        project_id="project-1",
        title="Analyze fixture",
        description="Return a concise architecture analysis",
        requirements=TaskRequirements(
            labels=frozenset({TaskLabel.RESEARCH}),
            required_capabilities=frozenset({"analysis"}),
        ),
    )

    await runtime.run_until_idle()

    task = store.get_task(task_id)
    assert task["state"] == TaskState.REVIEWING.value
    assert len(store.list_routing_decisions(task_id)) == 1
    run = store.list_worker_runs(task_id)[0]
    assert run["state"] == "completed"
    assert run["session_id"] is not None
    assert (tmp_path / "evidence" / f"{run['id']}.json").is_file()
    with store.connect() as connection:
        usage = connection.execute(
            "SELECT metric,value,unavailable_reason FROM usage_records WHERE run_id=?",
            (run["id"],),
        ).fetchall()
        result = connection.execute(
            "SELECT summary FROM worker_results WHERE run_id=?", (run["id"],)
        ).fetchone()
    assert result["summary"] == "ANALYSIS_COMPLETE"
    assert {row["metric"] for row in usage} >= {"inputTokens", "outputTokens", "costUSD"}
    assert next(row for row in usage if row["metric"] == "costUSD")["value"] == 0
    assert (
        next(row for row in usage if row["metric"] == "reasoningTokens")["unavailable_reason"]
        == "notReported"
    )
    assert store.list_workers()[0]["model_identifier"] == "observed-model"
    event_kinds = {event["kind"] for event in store.list_events(limit=200)}
    assert {
        "routingDecisionRecorded",
        "workerAdapterEvent",
        "workerCompleted",
        "taskStateChanged",
    } <= event_kinds


async def test_fallback_replans_away_from_failed_worker(tmp_path) -> None:
    store, registry, runtime = runtime_fixture(tmp_path, max_attempts=2)
    register_worker(
        store,
        registry,
        "primary",
        MockAdapter(MockBehavior(text="", exit_code=9)),
        quality=0.9,
    )
    register_worker(
        store,
        registry,
        "fallback",
        MockAdapter(MockBehavior(text="FALLBACK_OK")),
        quality=0.7,
    )
    task_id = await runtime.submit_task(
        project_id="project-1",
        title="Fallback analysis",
        description="Analyze using a fallback when needed",
        topology=ExecutionTopology.FALLBACK,
        requirements=TaskRequirements(
            labels=frozenset({TaskLabel.RESEARCH}),
            required_capabilities=frozenset({"analysis"}),
            preferred_workers=("primary", "fallback"),
        ),
    )

    await runtime.run_until_idle()

    assert store.get_task(task_id)["state"] == TaskState.REVIEWING.value
    runs = store.list_worker_runs(task_id)
    assert [(run["worker_id"], run["state"]) for run in runs] == [
        ("primary", "failed"),
        ("fallback", "completed"),
    ]
    assert len(store.list_routing_decisions(task_id)) == 2
    with store.connect() as connection:
        failures = connection.execute(
            "SELECT classification,retryable FROM failures WHERE task_id=?", (task_id,)
        ).fetchall()
    assert [(row["classification"], row["retryable"]) for row in failures] == [("transient", 1)]


async def test_definition_of_done_is_persisted_before_success(tmp_path) -> None:
    store, registry, runtime = runtime_fixture(tmp_path)
    register_worker(store, registry, "worker-1", MockAdapter())
    criterion_id = store.add_acceptance_criterion(
        project_id="project-1",
        criterion_id="criterion-1",
        kind="fileExists",
        description="Required artifact exists",
    )
    task_id = await runtime.submit_task(
        project_id="project-1",
        title="Verified analysis",
        description="Produce a verifiable result",
        requirements=TaskRequirements(
            labels=frozenset({TaskLabel.RESEARCH}),
            required_capabilities=frozenset({"analysis"}),
        ),
    )
    await runtime.run_until_idle()

    state = await runtime.apply_verification(
        task_id,
        DefinitionOfDoneResult(
            complete=True,
            results=(
                VerificationResult(
                    criterion_id,
                    True,
                    "artifact exists",
                    evidence={"path": "artifact.json", "secret": "token=do-not-store"},
                ),
            ),
            required_failures=(),
        ),
    )

    assert state is TaskState.SUCCEEDED
    verification = store.list_verifications(task_id)[0]
    assert "do-not-store" not in verification["evidence_json"]
    assert json.loads(verification["evidence_json"])["secret"] == "token=[REDACTED]"
    assert store.list_acceptance_criteria("project-1")[0]["state"] == "passed"


async def test_dependencies_wait_and_red_tasks_create_durable_approval(tmp_path) -> None:
    store, registry, runtime = runtime_fixture(tmp_path)
    register_worker(store, registry, "worker-1", MockAdapter())
    requirements = TaskRequirements(
        labels=frozenset({TaskLabel.RESEARCH}),
        required_capabilities=frozenset({"analysis"}),
    )
    prerequisite = await runtime.submit_task(
        project_id="project-1",
        task_id="prerequisite",
        title="Prerequisite",
        description="First task",
        requirements=requirements,
    )
    dependent = await runtime.submit_task(
        project_id="project-1",
        task_id="dependent",
        title="Dependent",
        description="Second task",
        requirements=requirements,
    )
    store.add_task_dependency(dependent, prerequisite)
    summary = await runtime.dispatch_ready()
    assert prerequisite in summary.launched_task_ids
    assert dependent not in summary.launched_task_ids
    await runtime.wait_for_active()

    red = await runtime.submit_task(
        project_id="project-1",
        title="Needs approval",
        description="A guarded operation",
        requirements=TaskRequirements(
            labels=frozenset({TaskLabel.RESEARCH}),
            permission_class=PermissionClass.RED,
            approval_state=ApprovalState.PENDING,
        ),
    )
    assert store.get_task(red)["state"] == TaskState.BLOCKED.value
    with store.connect() as connection:
        approval = connection.execute(
            "SELECT state,task_id FROM approvals WHERE task_id=?", (red,)
        ).fetchone()
    assert (approval["state"], approval["task_id"]) == ("pending", red)
