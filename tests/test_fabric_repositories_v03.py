from __future__ import annotations

import json
from datetime import UTC, datetime

import pytest

from project_supervisor.autonomy import GoalBudget, GoalService
from project_supervisor.domain import (
    ExecutionTopology,
    Harness,
    ModelDescriptor,
    NodeState,
    Provider,
    ResourceState,
    TaskLabel,
    TaskRecord,
    TaskRequirements,
    TaskState,
    WorkerSnapshot,
    WorkerState,
)
from project_supervisor.fabric.capabilities import (
    INITIAL_CAPABILITY_CATALOG,
    WORKER_MANIFEST_SCHEMA_VERSION,
    CostMode,
    ObservationFreshness,
    QuotaAvailability,
    SubscriptionState,
    WorkerDynamicState,
    WorkerHealth,
    WorkerLocality,
    WorkerManifest,
    WorkerPrivacy,
)
from project_supervisor.fabric.execution import ChildWorkProposal, SpawnPolicy
from project_supervisor.fabric.fusion import FusionEngine, FusionStatus, ResultContribution
from project_supervisor.fabric.persistence import (
    CapabilityRegistryRepository,
    FusionRepository,
    SpawnRepository,
)
from project_supervisor.store import StateStore, timestamp
from project_supervisor.verification import (
    DefinitionOfDoneResult,
    VerificationPolicyError,
    VerificationResult,
)


def seeded_store(tmp_path) -> StateStore:
    store = StateStore(tmp_path / "state.db")
    store.create_project(
        project_id="project-fabric",
        name="Fabric fixture",
        root_path=str(tmp_path / "fixture"),
        goal="Exercise the local fabric",
    )
    store.upsert_node(
        node_id="node-local",
        hostname="fixture",
        display_name="Fixture",
        role="worker",
        state=NodeState.ONLINE,
    )
    store.upsert_worker(
        WorkerSnapshot(
            id="worker-local",
            node_id="node-local",
            harness=Harness.MOCK,
            provider=Provider.MOCK,
            model=ModelDescriptor("fixture-model", "Fixture", Provider.MOCK),
            state=WorkerState.IDLE,
            node_state=NodeState.ONLINE,
            resource_state=ResourceState.AVAILABLE,
            capabilities=frozenset({"analysis", "control-gui"}),
            code_write_allowed=False,
            privacy_allowed=True,
        )
    )
    return store


def task(task_id: str, *, capabilities: frozenset[str] = frozenset({"analysis"})) -> TaskRecord:
    return TaskRecord(
        id=task_id,
        project_id="project-fabric",
        title="Fabric task",
        description="Execute bounded deterministic work",
        state=TaskState.DRAFT,
        topology=ExecutionTopology.SINGLE,
        requirements=TaskRequirements(
            labels=frozenset({TaskLabel.RESEARCH}),
            required_capabilities=capabilities,
        ),
    )


def test_capability_registry_versions_static_truth_and_dynamic_unknowns(tmp_path) -> None:
    store = seeded_store(tmp_path)
    registry = CapabilityRegistryRepository(store)
    manifest = WorkerManifest(
        worker_id="worker-local",
        node_id="node-local",
        provider_id="mock",
        adapter_kind="fixture-semantic-ui-v1",
        capabilities=("CONTROL_GUI", "analysis"),
        manifest_revision=1,
        locality=WorkerLocality.LOCAL,
        privacy=WorkerPrivacy.SENSITIVE,
        cost_mode=CostMode.LOCAL_FREE,
        max_concurrency=2,
    )

    registered = registry.register_manifest(manifest, expected_head_generation=0)
    replay = registry.register_manifest(manifest, expected_head_generation=1)
    observation = registry.record_observation(
        WorkerDynamicState(
            worker_id="worker-local",
            health=WorkerHealth.HEALTHY,
            health_freshness=ObservationFreshness.FRESH,
            quota=QuotaAvailability.UNKNOWN,
            quota_freshness=ObservationFreshness.UNKNOWN,
            subscription_state=SubscriptionState.UNKNOWN,
            load=0.25,
            running_tasks=0,
            observed_at=datetime(2026, 8, 11, 15, 0, tzinfo=UTC),
        )
    )

    assert registered["digest"] == manifest.digest
    assert replay["manifestID"] == registered["manifestID"]
    assert observation["quota_state"] == "unknown"
    snapshot = store.worker_snapshots()[0]
    assert snapshot.capabilities == frozenset({"analysis", "control-gui"})
    assert snapshot.manifest_digest == manifest.digest
    assert snapshot.health is WorkerHealth.HEALTHY
    assert snapshot.quota_state is QuotaAvailability.UNKNOWN
    assert snapshot.worker_load == 0.25
    assert snapshot.cost_mode is CostMode.LOCAL_FREE

    replacement = WorkerManifest(
        worker_id="worker-local",
        node_id="node-local",
        provider_id="mock",
        adapter_kind="fixture-semantic-ui-v1",
        capabilities=("CONTROL_GUI", "analysis", "VERIFY_RESULT"),
        manifest_revision=2,
        locality=WorkerLocality.LOCAL,
        privacy=WorkerPrivacy.SENSITIVE,
        cost_mode=CostMode.LOCAL_FREE,
        max_concurrency=2,
    )
    registry.register_manifest(replacement, expected_head_generation=1)
    current = registry.get_worker("worker-local")
    assert current["revision"] == 2
    assert current["dynamic"] is None


def test_spawn_repository_is_atomic_idempotent_and_pause_depth_governed(tmp_path) -> None:
    store = seeded_store(tmp_path)
    store.create_task(task("task-root"), "#0001")
    goals = GoalService(store)
    goal = goals.create_goal(
        project_id="project-fabric",
        intent="Govern child work",
        budgets=GoalBudget(max_tasks=5, max_total_tokens=150),
        goal_id="goal-fabric",
    )
    with store.transaction() as connection:
        connection.execute(
            "UPDATE autonomous_goals SET state='running',started_at=?,updated_at=? WHERE id=?",
            (timestamp(), timestamp(), goal["id"]),
        )
    repository = SpawnRepository(
        store,
        SpawnPolicy(
            max_depth=2,
            max_total_children=5,
            max_total_tokens=150,
            circuit_failure_limit=1,
        ),
    )
    repository.bind_root_task(
        task_id="task-root",
        goal_id="goal-fabric",
        plan_version=1,
        expected_steer_version=0,
    )
    proposal = ChildWorkProposal(
        proposal_id="proposal-child-a",
        goal_id="goal-fabric",
        parent_task_id="task-root",
        proposal_key="child-a",
        title="Child A",
        description="Run one bounded child",
        steer_version=0,
        depth=1,
        task_definition_revision=1,
        estimated_tokens=100,
        estimated_seconds=3,
        estimated_cost_usd=0,
        payload={
            "requirements": {
                "requiredCapabilities": ["READ_VIDEO"],
                "requiredCapabilityParameters": [
                    {
                        "name": "READ_VIDEO",
                        "parameters": {"maxDuration": 120, "localOnly": True},
                    }
                ],
                "requiredManifestSchemaVersion": WORKER_MANIFEST_SCHEMA_VERSION,
                "requiredCatalogVersion": INITIAL_CAPABILITY_CATALOG.version,
                "localOnly": True,
            }
        },
    )

    accepted = repository.admit(proposal)
    replay = repository.admit(proposal)
    conflicting = repository.admit(
        ChildWorkProposal(
            proposal_id="proposal-child-conflict",
            goal_id="goal-fabric",
            parent_task_id="task-root",
            proposal_key="child-a",
            title="Changed child",
            description="A conflicting semantic replay",
            steer_version=0,
            depth=1,
            task_definition_revision=1,
            estimated_tokens=100,
            estimated_seconds=3,
            estimated_cost_usd=0,
        )
    )

    assert accepted["canonicalTaskCreated"]
    child = store.get_task(accepted["childTaskID"])
    assert child["state"] == "ready"
    assert json.loads(child["required_capabilities_json"]) == ["read-video"]
    capability_contract = json.loads(child["capability_constraints_json"])
    assert capability_contract == {
        "required": [
            {
                "name": "read-video",
                "parameters": {"local-only": True, "max-duration-seconds": 120},
            }
        ],
        "requiredCatalogVersion": INITIAL_CAPABILITY_CATALOG.version,
        "requiredManifestSchemaVersion": WORKER_MANIFEST_SCHEMA_VERSION,
        "schemaVersion": "capability-request/v1",
    }
    assert replay["disposition"] == "replay"
    assert replay["childTaskID"] == accepted["childTaskID"]
    assert conflicting["reasonCode"] == "IDEMPOTENCY_CONFLICT"
    with store.connect() as connection:
        assert connection.execute("SELECT COUNT(*) FROM child_work_proposals").fetchone()[0] == 1
        assert connection.execute("SELECT COUNT(*) FROM tasks").fetchone()[0] == 2

    reserved_budget = repository.admit(
        ChildWorkProposal(
            proposal_id="proposal-budget-reserved",
            goal_id="goal-fabric",
            parent_task_id="task-root",
            proposal_key="budget-reserved",
            title="Reserved budget",
            description="Must account for the active child reservation",
            steer_version=0,
            depth=1,
            task_definition_revision=1,
            estimated_tokens=60,
            estimated_seconds=1,
            estimated_cost_usd=0,
        )
    )
    assert reserved_budget["reasonCode"] == "TOKEN_BUDGET_EXCEEDED"

    with store.transaction() as connection:
        connection.execute(
            "UPDATE tasks SET state='failed',finished_at=?,updated_at=? WHERE id=?",
            (timestamp(), timestamp(), accepted["childTaskID"]),
        )
    open_circuit = repository.admit(
        ChildWorkProposal(
            proposal_id="proposal-open-circuit",
            goal_id="goal-fabric",
            parent_task_id="task-root",
            proposal_key="open-circuit",
            title="Circuit is open",
            description="Must not expand a repeatedly failing branch",
            steer_version=0,
            depth=1,
            task_definition_revision=1,
            estimated_tokens=1,
            estimated_seconds=1,
            estimated_cost_usd=0,
        )
    )
    assert open_circuit["reasonCode"] == "CIRCUIT_OPEN"

    goals.pause("goal-fabric", "soft", reason="operator pause")
    paused = repository.admit(
        ChildWorkProposal(
            proposal_id="proposal-paused",
            goal_id="goal-fabric",
            parent_task_id="task-root",
            proposal_key="paused-child",
            title="Paused child",
            description="Must remain only a rejected proposal",
            steer_version=0,
            depth=1,
            task_definition_revision=1,
            estimated_tokens=1,
            estimated_seconds=1,
            estimated_cost_usd=0,
        )
    )
    assert paused["reasonCode"] == "GOAL_NOT_RUNNING"
    assert paused["childTaskID"] is None


def test_fusion_repository_preserves_conflict_and_versioned_verification_handoff(tmp_path) -> None:
    store = seeded_store(tmp_path)
    store.upsert_worker(
        WorkerSnapshot(
            id="worker-reviewer",
            node_id="node-local",
            harness=Harness.MOCK,
            provider=Provider.MOCK,
            model=ModelDescriptor("review-model", "Review", Provider.MOCK),
            state=WorkerState.IDLE,
            node_state=NodeState.ONLINE,
            resource_state=ResourceState.AVAILABLE,
            capabilities=frozenset({"analysis", "review-code"}),
            code_write_allowed=False,
            privacy_allowed=True,
        )
    )
    store.create_task(task("task-fusion"), "#0001")
    criterion = store.add_acceptance_criterion(
        project_id="project-fabric",
        kind="assertion",
        description="Fused result is independently verified",
    )
    scope = store.bind_task_verification_scope("task-fusion", criterion_ids=(criterion,))
    run_a = store.create_worker_run(task_id="task-fusion", worker_id="worker-local", attempt=1)
    run_b = store.create_worker_run(task_id="task-fusion", worker_id="worker-reviewer", attempt=1)
    store.save_worker_result(run_id=run_a, summary="worker A asserted X")
    store.save_worker_result(run_id=run_b, summary="worker B asserted X")
    with store.transaction() as connection:
        connection.execute(
            "UPDATE worker_runs SET state='completed',exit_code=0,ended_at=?,updated_at=? "
            "WHERE task_id='task-fusion'",
            (timestamp(), timestamp()),
        )

    repository = FusionRepository(store)

    def contribution(run_id: str, identity: str, value: bool, worker_id: str) -> ResultContribution:
        return ResultContribution(
            contribution_id=identity,
            task_id="task-fusion",
            run_id=run_id,
            worker_id=worker_id,
            verification_scope_id=scope["id"],
            task_definition_revision=1,
            source_attempt=1,
            source_result_sha256=repository.source_result_sha256(run_id),
            steer_version=0,
            claims={"X": value},
        )

    conflict = FusionEngine().fuse(
        (
            contribution(run_a, "contribution-a", True, "worker-local"),
            contribution(run_b, "contribution-b", False, "worker-reviewer"),
        )
    )
    conflict_row = repository.persist(conflict)
    compatible = FusionEngine().fuse(
        (
            contribution(run_a, "contribution-c", True, "worker-local"),
            contribution(run_b, "contribution-d", True, "worker-reviewer"),
        )
    )
    compatible_row = repository.persist(compatible)
    replay = repository.persist(compatible)

    assert conflict.status is FusionStatus.CONTRADICTORY
    assert conflict_row["conflicts"][0]["claimKey"] == "X"
    assert conflict_row["verificationHandoff"] is None
    assert compatible_row["classification"] == "compatible"
    assert compatible_row["verificationHandoff"]["verification_scope_id"] == scope["id"]
    assert replay["fusionID"] == compatible_row["fusionID"]
    with store.transaction() as connection:
        connection.execute(
            "UPDATE tasks SET state='reviewing',updated_at=? WHERE id='task-fusion'",
            (timestamp(),),
        )
    terminal = repository.apply_independent_verification(
        compatible_row["fusionID"],
        DefinitionOfDoneResult(
            complete=True,
            results=(VerificationResult(criterion, True, "independent pass"),),
            required_failures=(),
        ),
        max_attempts=2,
    )
    assert terminal is TaskState.SUCCEEDED
    assert {event["kind"] for event in store.list_events(limit=1000)} >= {
        "fusionStarted",
        "fusionConflictDetected",
        "fusionCompleted",
    }


def test_fusion_handoff_is_fenced_by_worker_result_and_goal_steer(tmp_path) -> None:
    store = seeded_store(tmp_path)
    store.create_task(task("task-steered-fusion"), "#0001")
    criterion = store.add_acceptance_criterion(
        project_id="project-fabric",
        kind="assertion",
        description="Current steering version is independently verified",
    )
    store.bind_task_verification_scope("task-steered-fusion", criterion_ids=(criterion,))
    goals = GoalService(store)
    goals.create_goal(
        project_id="project-fabric",
        intent="Fence stale fusion",
        budgets=GoalBudget(max_tasks=3),
        goal_id="goal-steered-fusion",
    )
    with store.transaction() as connection:
        connection.execute(
            "UPDATE autonomous_goals SET state='running',started_at=?,updated_at=? WHERE id=?",
            (timestamp(), timestamp(), "goal-steered-fusion"),
        )
    SpawnRepository(store).bind_root_task(
        task_id="task-steered-fusion",
        goal_id="goal-steered-fusion",
        expected_steer_version=0,
    )
    run_id = store.create_worker_run(
        task_id="task-steered-fusion", worker_id="worker-local", attempt=1
    )
    repository = FusionRepository(store)
    with pytest.raises(ValueError, match="immutable Worker result"):
        repository.contribution_from_worker_result(
            run_id,
            contribution_id="missing-result",
            claims={"current": True},
        )
    store.save_worker_result(run_id=run_id, summary="current result")
    with store.transaction() as connection:
        connection.execute(
            "UPDATE worker_runs SET state='completed',exit_code=0,ended_at=?,updated_at=? "
            "WHERE id=?",
            (timestamp(), timestamp(), run_id),
        )
        connection.execute(
            "UPDATE tasks SET state='reviewing',attempt_count=1,updated_at=? WHERE id=?",
            (timestamp(), "task-steered-fusion"),
        )
    persisted = repository.fuse_attempt("task-steered-fusion", {run_id: {"current": True}})

    goals.steer("goal-steered-fusion", "Use the new acceptance intent")

    with pytest.raises(VerificationPolicyError, match="steering version changed"):
        repository.apply_independent_verification(
            persisted["fusionID"],
            DefinitionOfDoneResult(
                complete=True,
                results=(VerificationResult(criterion, True, "stale pass"),),
                required_failures=(),
            ),
        )
    assert store.get_task("task-steered-fusion")["state"] == TaskState.REVIEWING.value
