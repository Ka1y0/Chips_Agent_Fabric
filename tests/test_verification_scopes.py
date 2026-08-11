from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from project_supervisor.autonomy import (
    AutonomousIterationEngine,
    DispatchHandle,
    GoalService,
    SupervisorRuntimeDispatcher,
)
from project_supervisor.domain import (
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
from project_supervisor.store import StateStore
from project_supervisor.verification import (
    DefinitionOfDoneResult,
    VerificationPolicyError,
    VerificationResult,
)


@pytest.fixture
def store(tmp_path: Path) -> StateStore:
    value = StateStore(tmp_path / "state.db")
    value.create_project(
        project_id="project-1",
        name="Verification scope fixture",
        root_path=str(tmp_path),
        goal="Verify the correct Task definition",
    )
    value.upsert_node(
        node_id="node-1",
        hostname="fixture",
        display_name="Fixture Node",
        role="control",
        state=NodeState.ONLINE,
    )
    value.upsert_worker(
        WorkerSnapshot(
            id="worker-1",
            node_id="node-1",
            harness=Harness.MOCK,
            provider=Provider.MOCK,
            model=ModelDescriptor("mock-model", "Mock Model", Provider.MOCK),
            state=WorkerState.IDLE,
            node_state=NodeState.ONLINE,
            resource_state=ResourceState.AVAILABLE,
            capabilities=frozenset({"review"}),
            code_write_allowed=False,
            privacy_allowed=True,
        )
    )
    value.create_task(
        TaskRecord(
            id="task-1",
            project_id="project-1",
            title="Produce a durable artifact",
            description="Create the artifact required by the current Task definition",
            state=TaskState.DRAFT,
            topology=ExecutionTopology.SINGLE,
            requirements=TaskRequirements(labels=frozenset({TaskLabel.REVIEW})),
        ),
        "#001",
    )
    value.transition_task("task-1", TaskState.QUEUED)
    value.transition_task("task-1", TaskState.READY)
    return value


def add_criterion(store: StateStore, criterion_id: str, description: str) -> str:
    return store.add_acceptance_criterion(
        project_id="project-1",
        criterion_id=criterion_id,
        kind="fileExists",
        description=description,
    )


def dispatch_and_finish(store: StateStore) -> dict[str, object]:
    task = store.get_task("task-1")
    claim = store.claim_task_dispatch("task-1", ["worker-1"], expected_version=int(task["version"]))
    assert claim is not None
    run_id = claim["runIDs"]["worker-1"]
    assert store.activate_worker_run(run_id)
    store.transition_worker_run(run_id, RunState.COMPLETED, exit_code=0)
    store.transition_task("task-1", TaskState.REVIEWING)
    return {**claim, "runID": run_id}


def passing_result(*criterion_ids: str) -> DefinitionOfDoneResult:
    return DefinitionOfDoneResult(
        complete=True,
        results=tuple(
            VerificationResult(criterion_id, True, f"{criterion_id} passed")
            for criterion_id in criterion_ids
        ),
        required_failures=(),
    )


def failing_result(criterion_id: str) -> DefinitionOfDoneResult:
    return DefinitionOfDoneResult(
        complete=False,
        results=(VerificationResult(criterion_id, False, f"{criterion_id} failed"),),
        required_failures=(criterion_id,),
    )


def verification_provenance(context: dict[str, object]) -> dict[str, str | int]:
    scope_id = context["verification_scope_id"]
    task_revision = context["task_definition_revision"]
    source_attempt = context["source_attempt"]
    assert isinstance(scope_id, str)
    assert isinstance(task_revision, int)
    assert isinstance(source_attempt, int)
    return {
        "expected_verification_scope_id": scope_id,
        "expected_task_definition_revision": task_revision,
        "expected_source_attempt": source_attempt,
    }


def test_scoped_verification_uses_dispatch_snapshot_without_mutating_project_templates(
    store: StateStore,
) -> None:
    criterion_id = add_criterion(store, "criterion-v1", "Version one artifact exists")
    scope = store.bind_task_verification_scope("task-1", criterion_ids=[criterion_id])

    claim = dispatch_and_finish(store)
    run = store.get_worker_run(str(claim["runID"]))

    assert claim["verificationScopeID"] == scope["id"]
    assert claim["taskDefinitionRevision"] == scope["task_definition_revision"] == 1
    assert run["verification_scope_id"] == scope["id"]
    assert run["task_definition_revision"] == 1
    context = store.get_task_verification_context("task-1")

    state = store.apply_task_verification(
        "task-1", passing_result(criterion_id), **verification_provenance(context)
    )

    assert state is TaskState.SUCCEEDED
    verification = store.list_verifications("task-1")[0]
    items = store.list_task_verification_scope_items(scope["id"])
    assert verification["verification_scope_id"] == scope["id"]
    assert verification["scope_item_id"] == items[0]["id"]
    assert verification["source_attempt"] == 1
    assert scope["sealed_at"] is not None
    assert len(scope["definition_sha256"]) == 64
    assert len(items[0]["definition_sha256"]) == 64
    template = store.list_acceptance_criteria("project-1")[0]
    assert template["state"] == "pending"
    assert template["evidence_json"] is None


def test_rebinding_rejects_old_execution_without_writes_and_preserves_history(
    store: StateStore,
) -> None:
    first_id = add_criterion(store, "criterion-v1", "Version one artifact exists")
    first_scope = store.bind_task_verification_scope("task-1", criterion_ids=[first_id])
    claim = dispatch_and_finish(store)
    first_context = store.get_task_verification_context("task-1")
    first_items = store.list_task_verification_scope_items(first_scope["id"])

    second_id = add_criterion(store, "criterion-v2", "Steered artifact also exists")
    second_scope = store.bind_task_verification_scope(
        "task-1",
        criterion_ids=[first_id, second_id],
    )
    before_sequence = store.highest_event_sequence()

    assert first_scope["criteria_version"] == 1
    assert first_scope["task_definition_revision"] == 1
    assert second_scope["criteria_version"] == 2
    assert second_scope["task_definition_revision"] == 2
    assert claim["verificationScopeID"] == first_scope["id"]

    with pytest.raises(VerificationPolicyError, match="stale verification provenance"):
        store.apply_task_verification(
            "task-1", passing_result(first_id), **verification_provenance(first_context)
        )

    assert store.get_task("task-1")["state"] == TaskState.REVIEWING.value
    assert store.list_verifications("task-1") == []
    assert store.highest_event_sequence() == before_sequence
    assert [scope["id"] for scope in store.list_task_verification_scopes("task-1")] == [
        first_scope["id"],
        second_scope["id"],
    ]
    assert store.list_task_verification_scope_items(first_scope["id"]) == first_items
    assert len(store.list_task_verification_scope_items(second_scope["id"])) == 2
    assert {item["state"] for item in store.list_acceptance_criteria("project-1")} == {"pending"}

    with store.connect() as connection, pytest.raises(sqlite3.IntegrityError, match="append-only"):
        connection.execute(
            "UPDATE task_verification_scopes SET criteria_version=99 WHERE id=?",
            (first_scope["id"],),
        )
    with store.connect() as connection, pytest.raises(sqlite3.IntegrityError, match="append-only"):
        connection.execute(
            "DELETE FROM task_verification_scope_items WHERE id=?", (first_items[0]["id"],)
        )


def test_bound_scope_rejects_late_criteria_and_parent_deletion(store: StateStore) -> None:
    criterion_id = add_criterion(store, "criterion-v1", "Version one artifact exists")
    scope = store.bind_task_verification_scope("task-1", criterion_ids=[criterion_id])
    item = store.list_task_verification_scope_items(scope["id"])[0]

    with (
        store.connect() as connection,
        pytest.raises(sqlite3.IntegrityError, match="sealed.*criteria are append-only"),
    ):
        connection.execute(
            "INSERT INTO task_verification_scope_items("
            "id,scope_id,criterion_id,source_criterion_id,ordinal,required,kind,description,"
            "command_json,expected_json,definition_sha256,created_at) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                "late-scope-item",
                scope["id"],
                "late-criterion",
                None,
                1,
                1,
                item["kind"],
                "A criterion appended after the definition was sealed",
                None,
                None,
                "0" * 64,
                item["created_at"],
            ),
        )

    with store.connect() as connection, pytest.raises(sqlite3.IntegrityError):
        connection.execute("DELETE FROM tasks WHERE id='task-1'")

    assert len(store.list_task_verification_scope_items(scope["id"])) == 1
    assert store.get_task("task-1")["current_verification_scope_id"] == scope["id"]


def test_scope_reads_fail_closed_when_sealed_definition_hash_is_invalid(
    store: StateStore,
) -> None:
    criterion_id = add_criterion(store, "criterion-v1", "Version one artifact exists")
    valid_scope = store.bind_task_verification_scope("task-1", criterion_ids=[criterion_id])
    valid_item = store.list_task_verification_scope_items(valid_scope["id"])[0]

    with store.transaction() as connection:
        connection.execute(
            "INSERT INTO task_verification_scopes("
            "id,project_id,task_id,criteria_version,task_definition_revision,goal_id,"
            "iteration_id,plan_version,steer_version,schema_version,definition_sha256,created_at) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                "verification-scope-invalid-hash",
                "project-1",
                "task-1",
                2,
                2,
                None,
                None,
                None,
                None,
                "task-verification-scope/v1",
                valid_scope["definition_sha256"],
                valid_scope["created_at"],
            ),
        )
        connection.execute(
            "INSERT INTO task_verification_scope_items("
            "id,scope_id,criterion_id,source_criterion_id,ordinal,required,kind,description,"
            "command_json,expected_json,definition_sha256,created_at) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                "verification-scope-item-invalid-parent-hash",
                "verification-scope-invalid-hash",
                valid_item["criterion_id"],
                valid_item["source_criterion_id"],
                0,
                valid_item["required"],
                valid_item["kind"],
                valid_item["description"],
                valid_item["command_json"],
                valid_item["expected_json"],
                valid_item["definition_sha256"],
                valid_item["created_at"],
            ),
        )
        connection.execute(
            "UPDATE task_verification_scopes SET sealed_at=? WHERE id=?",
            (valid_scope["sealed_at"], "verification-scope-invalid-hash"),
        )

    with pytest.raises(VerificationPolicyError, match="scope definition hash mismatch"):
        store.get_task_verification_scope("verification-scope-invalid-hash")


def test_unscoped_task_retains_legacy_project_acceptance_criteria_behavior(
    store: StateStore,
) -> None:
    criterion_id = add_criterion(store, "legacy-criterion", "Legacy artifact exists")

    claim = dispatch_and_finish(store)

    assert claim["verificationScopeID"] is None
    run = store.get_worker_run(str(claim["runID"]))
    assert run["verification_scope_id"] is None
    assert run["task_definition_revision"] == 1

    state = store.apply_task_verification("task-1", passing_result(criterion_id))

    assert state is TaskState.SUCCEEDED
    verification = store.list_verifications("task-1")[0]
    assert verification["verification_scope_id"] is None
    assert verification["scope_item_id"] is None
    assert verification["source_attempt"] is None
    assert store.list_acceptance_criteria("project-1")[0]["state"] == "passed"


def test_legacy_criterion_verification_cannot_cross_project_or_bypass_scope(
    store: StateStore, tmp_path: Path
) -> None:
    store.create_project(
        project_id="project-2",
        name="Unrelated project",
        root_path=str(tmp_path / "other"),
        goal="Keep criterion ownership isolated",
    )
    foreign_id = store.add_acceptance_criterion(
        project_id="project-2",
        criterion_id="foreign-criterion",
        kind="fileExists",
        description="Other project artifact exists",
    )

    with pytest.raises(ValueError, match="does not belong to Task project"):
        store.record_verification(
            task_id="task-1",
            criterion_id=foreign_id,
            kind="fileExists",
            passed=True,
            evidence={},
            verifier="legacy-verifier",
        )

    local_id = add_criterion(store, "local-criterion", "Local artifact exists")
    store.bind_task_verification_scope("task-1", criterion_ids=[local_id])
    with pytest.raises(VerificationPolicyError, match="requires provenance"):
        store.record_verification(
            task_id="task-1",
            criterion_id=local_id,
            kind="fileExists",
            passed=True,
            evidence={},
            verifier="legacy-verifier",
        )

    assert store.list_verifications("task-1") == []
    assert store.list_acceptance_criteria("project-2")[0]["state"] == "pending"


def test_delayed_result_from_failed_attempt_cannot_satisfy_rebound_scope(
    store: StateStore,
) -> None:
    criterion_id = add_criterion(store, "stable-criterion-id", "Artifact version one exists")
    first_scope = store.bind_task_verification_scope("task-1", criterion_ids=[criterion_id])
    first_claim = dispatch_and_finish(store)
    first_context = store.get_task_verification_context("task-1")

    first_state = store.apply_task_verification(
        "task-1", failing_result(criterion_id), **verification_provenance(first_context)
    )

    assert first_state is TaskState.READY
    store.set_worker_state("worker-1", WorkerState.IDLE)
    second_scope = store.bind_task_verification_scope("task-1", criterion_ids=[criterion_id])
    second_claim = dispatch_and_finish(store)
    second_context = store.get_task_verification_context("task-1")
    before_sequence = store.highest_event_sequence()
    before_verifications = store.list_verifications("task-1")

    assert first_claim["attempt"] == first_context["source_attempt"] == 1
    assert second_claim["attempt"] == second_context["source_attempt"] == 2
    assert first_scope["id"] != second_scope["id"]
    assert first_scope["task_definition_revision"] == 1
    assert second_scope["task_definition_revision"] == 2

    with pytest.raises(VerificationPolicyError, match="stale verification provenance"):
        store.apply_task_verification(
            "task-1", passing_result(criterion_id), **verification_provenance(first_context)
        )

    assert store.get_task("task-1")["state"] == TaskState.REVIEWING.value
    assert store.list_verifications("task-1") == before_verifications
    assert store.highest_event_sequence() == before_sequence

    final_state = store.apply_task_verification(
        "task-1", passing_result(criterion_id), **verification_provenance(second_context)
    )

    assert final_state is TaskState.SUCCEEDED
    observed = sorted(
        (
            row["source_attempt"],
            row["verification_scope_id"],
            row["passed"],
        )
        for row in store.list_verifications("task-1")
    )
    assert observed == [
        (1, first_scope["id"], 0),
        (2, second_scope["id"], 1),
    ]


def test_scope_binding_rejects_stale_plan_and_steer_metadata(store: StateStore) -> None:
    criterion_id = add_criterion(store, "criterion-v1", "Scoped artifact exists")
    goals = GoalService(store)
    goal = goals.create_goal(project_id="project-1", intent="Produce the scoped artifact")
    engine = AutonomousIterationEngine(
        store=store,
        evaluator=object(),
        planner=object(),
        dispatcher=object(),
        verifier=object(),
    )
    engine._start(goal["id"])
    iteration = engine._begin_iteration(goal["id"])

    with pytest.raises(ValueError, match="plan_version does not match"):
        store.bind_task_verification_scope(
            "task-1",
            criterion_ids=[criterion_id],
            goal_id=goal["id"],
            iteration_id=iteration["id"],
            plan_version=2,
        )

    steered = goals.steer(goal["id"], "Require the revised artifact contract")
    with pytest.raises(ValueError, match="steer_version does not match"):
        store.bind_task_verification_scope(
            "task-1",
            criterion_ids=[criterion_id],
            goal_id=goal["id"],
            iteration_id=iteration["id"],
            steer_version=0,
        )

    with pytest.raises(ValueError, match="iteration predates"):
        store.bind_task_verification_scope(
            "task-1",
            criterion_ids=[criterion_id],
            goal_id=goal["id"],
            iteration_id=iteration["id"],
            plan_version=1,
            steer_version=steered["steer_version"],
        )

    current_iteration = engine._begin_iteration(goal["id"])
    scope = store.bind_task_verification_scope(
        "task-1",
        criterion_ids=[criterion_id],
        goal_id=goal["id"],
        iteration_id=current_iteration["id"],
        plan_version=2,
        steer_version=steered["steer_version"],
    )

    assert scope["goal_id"] == goal["id"]
    assert scope["iteration_id"] == current_iteration["id"]
    assert scope["plan_version"] == 2
    assert scope["steer_version"] == 1


async def test_runtime_exit_policy_cannot_bypass_current_scoped_criteria(
    store: StateStore, tmp_path: Path
) -> None:
    criterion_id = add_criterion(store, "criterion-v1", "Scoped artifact exists")
    scope = store.bind_task_verification_scope("task-1", criterion_ids=[criterion_id])
    dispatch_and_finish(store)
    runtime = SupervisorRuntime(
        store=store,
        scheduler=DeterministicScheduler(),
        adapters=AdapterRegistry(),
        evidence_root=tmp_path / "evidence",
    )

    result = await SupervisorRuntimeDispatcher(runtime).collect(
        DispatchHandle(reference="task-1", task_id="task-1")
    )

    assert not result.succeeded
    assert result.payload["verificationScopeID"] == scope["id"]
    assert store.get_task("task-1")["state"] == TaskState.REVIEWING.value
