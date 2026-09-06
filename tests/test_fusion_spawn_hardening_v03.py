from __future__ import annotations

from dataclasses import replace

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
from project_supervisor.fabric.execution import ChildWorkProposal, SpawnPolicy
from project_supervisor.fabric.fusion import FusionEngine, FusionPolicy
from project_supervisor.fabric.persistence import FusionRepository, SpawnRepository
from project_supervisor.store import StateStore, timestamp


def _store(tmp_path) -> StateStore:
    store = StateStore(tmp_path / "fusion-spawn-audit.db")
    store.create_project(
        project_id="project-audit",
        name="Fusion/spawn audit",
        root_path=str(tmp_path / "fixture"),
        goal="Exercise canonical provenance",
    )
    store.upsert_node(
        node_id="node-audit",
        hostname="audit.local",
        display_name="Audit node",
        role="worker",
        state=NodeState.ONLINE,
    )
    for worker_id in ("worker-y", "worker-z"):
        store.upsert_worker(
            WorkerSnapshot(
                id=worker_id,
                node_id="node-audit",
                harness=Harness.MOCK,
                provider=Provider.MOCK,
                model=ModelDescriptor("fixture", "Fixture", Provider.MOCK),
                state=WorkerState.IDLE,
                node_state=NodeState.ONLINE,
                resource_state=ResourceState.AVAILABLE,
                capabilities=frozenset({"analysis"}),
                code_write_allowed=False,
                privacy_allowed=True,
            )
        )
    return store


def _task(task_id: str) -> TaskRecord:
    return TaskRecord(
        id=task_id,
        project_id="project-audit",
        title="Audit task",
        description="Exercise one persisted invariant",
        state=TaskState.DRAFT,
        topology=ExecutionTopology.SINGLE,
        requirements=TaskRequirements(
            labels=frozenset({TaskLabel.RESEARCH}),
            required_capabilities=frozenset({"analysis"}),
        ),
    )


def _running_goal(
    store: StateStore,
    *,
    task_id: str,
    goal_id: str,
    budget: GoalBudget | None = None,
    policy: SpawnPolicy | None = None,
) -> SpawnRepository:
    store.create_task(_task(task_id), "#0001")
    GoalService(store).create_goal(
        project_id="project-audit",
        intent="Govern recursive work",
        budgets=budget or GoalBudget(max_tasks=16),
        goal_id=goal_id,
    )
    with store.transaction() as connection:
        connection.execute(
            "UPDATE autonomous_goals SET state='running',started_at=?,updated_at=? WHERE id=?",
            (timestamp(), timestamp(), goal_id),
        )
    repository = SpawnRepository(store, policy)
    repository.bind_root_task(
        task_id=task_id,
        goal_id=goal_id,
        expected_steer_version=0,
    )
    return repository


def _proposal(
    *,
    identity: str,
    goal_id: str,
    parent_task_id: str,
    slots: int = 1,
    tokens: int | None = 0,
    cost: float | None = 0,
    circuit: str | None = None,
) -> ChildWorkProposal:
    return ChildWorkProposal(
        proposal_id=f"proposal-{identity}",
        goal_id=goal_id,
        parent_task_id=parent_task_id,
        proposal_key=identity,
        title=f"Child {identity}",
        description="A bounded child proposal",
        steer_version=0,
        depth=1,
        task_definition_revision=1,
        requested_worker_slots=slots,
        estimated_tokens=tokens,
        estimated_seconds=1,
        estimated_cost_usd=cost,
        circuit_key=circuit,
    )


def _envelope(identity: str) -> dict[str, object]:
    return {
        "schemaVersion": "child-work-proposal/v1",
        "proposalID": f"proposal-{identity}",
        "proposalKey": identity,
        "title": f"Child {identity}",
        "description": "Derived from an immutable terminal Worker result",
        "estimatedTokens": 0,
        "estimatedSeconds": 1,
        "estimatedCostUSD": 0,
        "payload": {},
    }


def test_fusion_rejects_multiple_contributions_claiming_one_canonical_run(tmp_path) -> None:
    """One immutable WorkerResult must not be amplified into multiple contributors."""

    store = _store(tmp_path)
    store.create_task(_task("task-fusion"), "#0001")
    criterion_id = store.add_acceptance_criterion(
        project_id="project-audit",
        kind="assertion",
        description="The fused claim is independently verified",
    )
    store.bind_task_verification_scope("task-fusion", criterion_ids=(criterion_id,))
    run_id = store.create_worker_run(task_id="task-fusion", worker_id="worker-z", attempt=1)
    store.save_worker_result(run_id=run_id, summary="one immutable result")
    with store.transaction() as connection:
        connection.execute(
            "UPDATE worker_runs SET state='completed',exit_code=0,ended_at=?,updated_at=? "
            "WHERE id=?",
            (timestamp(), timestamp(), run_id),
        )

    repository = FusionRepository(store)
    canonical = repository.contribution_from_worker_result(
        run_id,
        contribution_id="contribution-z-canonical",
        claims={"answer": True},
    )
    forged = replace(
        canonical,
        contribution_id="contribution-a-forged",
        worker_id="aaa-forged-worker",
        source_result_sha256="f" * 64,
    )
    fused = FusionEngine(FusionPolicy(minimum_contributions=2)).fuse((forged, canonical))

    with pytest.raises(ValueError, match="canonical|one contribution|duplicate run"):
        repository.persist(fused)


def test_fusion_attempt_requires_every_canonical_run_and_immutable_result(tmp_path) -> None:
    store = _store(tmp_path)
    store.create_task(_task("task-panel"), "#0001")
    criterion_id = store.add_acceptance_criterion(
        project_id="project-audit",
        kind="assertion",
        description="Every panel run contributes",
    )
    store.bind_task_verification_scope("task-panel", criterion_ids=(criterion_id,))
    run_y = store.create_worker_run(task_id="task-panel", worker_id="worker-y", attempt=1)
    run_z = store.create_worker_run(task_id="task-panel", worker_id="worker-z", attempt=1)
    store.save_worker_result(run_id=run_y, summary="Y")
    store.save_worker_result(run_id=run_z, summary="Z")
    with store.transaction() as connection:
        connection.execute(
            "UPDATE worker_runs SET state='completed',exit_code=0,ended_at=?,updated_at=? "
            "WHERE task_id='task-panel'",
            (timestamp(), timestamp()),
        )

    repository = FusionRepository(store)
    with pytest.raises(ValueError, match="every run"):
        repository.fuse_attempt("task-panel", {run_y: {"answer": True}})
    persisted = repository.fuse_attempt(
        "task-panel", {run_y: {"answer": True}, run_z: {"answer": True}}
    )
    assert {item["runID"] for item in persisted["provenance"]} == {run_y, run_z}


def test_spawn_intake_requires_terminal_result_and_persists_derived_provenance(tmp_path) -> None:
    store = _store(tmp_path)
    repository = _running_goal(
        store,
        task_id="task-root",
        goal_id="goal-root",
    )
    run_id = store.create_worker_run(task_id="task-root", worker_id="worker-z", attempt=1)
    store.save_worker_result(run_id=run_id, summary="canonical proposal source")

    with pytest.raises(ValueError, match="terminal"):
        repository.admit_from_worker_result(run_id, _envelope("non-terminal"))

    with store.transaction() as connection:
        connection.execute(
            "UPDATE worker_runs SET state='completed',exit_code=0,ended_at=?,updated_at=? "
            "WHERE id=?",
            (timestamp(), timestamp(), run_id),
        )
    admitted = repository.admit_from_worker_result(run_id, _envelope("terminal"))
    assert admitted["canonicalTaskCreated"]
    with store.connect() as connection:
        proposal = connection.execute(
            "SELECT * FROM child_work_proposals WHERE id='proposal-terminal'"
        ).fetchone()
    assert proposal["source_run_id"] == run_id
    assert proposal["source_result_sha256"] is not None
    assert len(proposal["source_result_sha256"]) == 64
    assert proposal["depth"] == 1
    assert proposal["task_definition_revision"] == 1


def test_spawn_intake_rejects_terminal_result_from_stale_task_revision(tmp_path) -> None:
    """A replan must not let an old run borrow the Task's new revision provenance."""

    store = _store(tmp_path)
    repository = _running_goal(
        store,
        task_id="task-replanned",
        goal_id="goal-replanned",
    )
    criterion_id = store.add_acceptance_criterion(
        project_id="project-audit",
        kind="assertion",
        description="Version one",
    )
    first_scope = store.bind_task_verification_scope(
        "task-replanned", criterion_ids=(criterion_id,)
    )
    run_id = store.create_worker_run(task_id="task-replanned", worker_id="worker-z", attempt=1)
    store.save_worker_result(run_id=run_id, summary="old revision result")
    with store.transaction() as connection:
        connection.execute(
            "UPDATE worker_runs SET state='completed',exit_code=0,ended_at=?,updated_at=? "
            "WHERE id=?",
            (timestamp(), timestamp(), run_id),
        )
    second_scope = store.bind_task_verification_scope(
        "task-replanned", criterion_ids=(criterion_id,)
    )
    assert first_scope["task_definition_revision"] == 1
    assert second_scope["task_definition_revision"] == 2

    with pytest.raises((ValueError, RuntimeError), match="stale|revision|current"):
        repository.admit_from_worker_result(run_id, _envelope("stale-revision"))


def test_spawn_intake_rejects_terminal_result_from_stale_attempt(tmp_path) -> None:
    store = _store(tmp_path)
    repository = _running_goal(
        store,
        task_id="task-retried",
        goal_id="goal-retried",
    )
    run_one = store.create_worker_run(task_id="task-retried", worker_id="worker-z", attempt=1)
    store.save_worker_result(run_id=run_one, summary="attempt one result")
    with store.transaction() as connection:
        connection.execute(
            "UPDATE worker_runs SET state='completed',exit_code=0,ended_at=?,updated_at=? "
            "WHERE id=?",
            (timestamp(), timestamp(), run_one),
        )
    store.create_worker_run(task_id="task-retried", worker_id="worker-z", attempt=2)
    assert store.get_task("task-retried")["attempt_count"] == 2

    with pytest.raises((ValueError, RuntimeError), match="stale|attempt|current"):
        repository.admit_from_worker_result(run_one, _envelope("stale-attempt"))


@pytest.mark.parametrize(
    ("budget", "policy", "first", "second", "reason"),
    (
        (
            GoalBudget(max_tasks=16, max_total_tokens=100),
            SpawnPolicy(max_total_tokens=100),
            {"slots": 1, "tokens": 60, "cost": 0},
            {"slots": 1, "tokens": 50, "cost": 0},
            "TOKEN_BUDGET_EXCEEDED",
        ),
        (
            GoalBudget(max_tasks=16, max_cost_usd=10),
            SpawnPolicy(max_cost_usd=10),
            {"slots": 1, "tokens": 0, "cost": 6},
            {"slots": 1, "tokens": 0, "cost": 5},
            "COST_BUDGET_EXCEEDED",
        ),
        (
            GoalBudget(max_tasks=16),
            SpawnPolicy(max_parallel_worker_slots=2),
            {"slots": 2, "tokens": 0, "cost": 0},
            {"slots": 1, "tokens": 0, "cost": 0},
            "WORKER_CONCURRENCY_LIMIT",
        ),
    ),
)
def test_accepted_child_reservations_fence_later_admission(
    tmp_path, budget, policy, first, second, reason
) -> None:
    store = _store(tmp_path)
    repository = _running_goal(
        store,
        task_id="task-budget-root",
        goal_id="goal-budget",
        budget=budget,
        policy=policy,
    )
    accepted = repository.admit(
        _proposal(
            identity="reserved",
            goal_id="goal-budget",
            parent_task_id="task-budget-root",
            **first,
        )
    )
    rejected = repository.admit(
        _proposal(
            identity="next",
            goal_id="goal-budget",
            parent_task_id="task-budget-root",
            **second,
        )
    )
    assert accepted["canonicalTaskCreated"]
    assert rejected["reasonCode"] == reason


def test_circuit_failure_context_survives_repository_and_store_restart(tmp_path) -> None:
    store = _store(tmp_path)
    repository = _running_goal(
        store,
        task_id="task-circuit-root",
        goal_id="goal-circuit",
        policy=SpawnPolicy(circuit_failure_limit=1),
    )
    first = repository.admit(
        _proposal(
            identity="branch-first",
            goal_id="goal-circuit",
            parent_task_id="task-circuit-root",
            circuit="branch:stable",
        )
    )
    assert first["canonicalTaskCreated"]
    with store.transaction() as connection:
        connection.execute(
            "UPDATE tasks SET state='failed',finished_at=?,updated_at=? WHERE id=?",
            (timestamp(), timestamp(), first["childTaskID"]),
        )

    reopened = StateStore(store.path)
    after_restart = SpawnRepository(reopened, SpawnPolicy(circuit_failure_limit=1)).admit(
        _proposal(
            identity="branch-second",
            goal_id="goal-circuit",
            parent_task_id="task-circuit-root",
            circuit="branch:stable",
        )
    )
    assert after_restart["reasonCode"] == "CIRCUIT_OPEN"
    assert after_restart["childTaskID"] is None
