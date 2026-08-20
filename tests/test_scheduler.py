import pytest

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
    WorkerSnapshot,
    WorkerState,
)
from project_supervisor.hybrid import ResourceRoutingEvidence
from project_supervisor.scheduler import DeterministicScheduler


def worker(
    worker_id: str,
    *,
    provider: Provider = Provider.ANTHROPIC,
    capabilities: frozenset[str] = frozenset({"coding", "review"}),
    code_write_allowed: bool = True,
    quality: float = 0.8,
    cost: float = 0.5,
    state: WorkerState = WorkerState.IDLE,
    resource: ResourceState = ResourceState.AVAILABLE,
) -> WorkerSnapshot:
    harness = Harness.LOCAL_WORKER if provider is Provider.LOCAL else Harness.CLAUDE_CODE
    return WorkerSnapshot(
        id=worker_id,
        node_id="node-1",
        harness=harness,
        provider=provider,
        model=ModelDescriptor("model-1", "Model 1", provider, context_window_tokens=200_000),
        state=state,
        node_state=NodeState.ONLINE,
        resource_state=resource,
        capabilities=capabilities,
        code_write_allowed=code_write_allowed,
        privacy_allowed=True,
        quality_score=quality,
        reliability_score=0.9,
        expected_latency_seconds=10,
        monetary_cost_score=cost,
    )


def coding_requirements(**changes: object) -> TaskRequirements:
    values = {
        "labels": frozenset({TaskLabel.CODING}),
        "required_capabilities": frozenset({"coding"}),
        "code_write_required": True,
    }
    values.update(changes)
    return TaskRequirements(**values)


def test_scheduler_is_deterministic_and_uses_id_tie_break() -> None:
    scheduler = DeterministicScheduler()
    workers = [worker("worker-b"), worker("worker-a")]
    first = scheduler.schedule(
        task_id="task-1",
        requirements=coding_requirements(),
        topology=ExecutionTopology.SINGLE,
        workers=workers,
    )
    second = scheduler.schedule(
        task_id="task-1",
        requirements=coding_requirements(),
        topology=ExecutionTopology.SINGLE,
        workers=reversed(workers),
    )
    assert first == second
    assert first.selected_worker_ids == ("worker-a",)


def test_scheduler_rejects_duplicate_worker_ids_before_scoring() -> None:
    duplicate = worker("duplicate")
    with pytest.raises(ValueError, match="workers must be unique by ID: duplicate"):
        DeterministicScheduler().schedule(
            task_id="duplicate-workers",
            requirements=coding_requirements(),
            topology=ExecutionTopology.PARALLEL_PANEL,
            workers=(duplicate, duplicate),
        )


def test_scheduler_preserves_evidence_for_workers_prefiltered_upstream() -> None:
    evidence = ResourceRoutingEvidence(
        worker_id="prefiltered",
        quota_pool_id="test-pool",
        provider="anthropic",
        quota_state="available",
        freshness="fresh",
        provenance="PROVIDER_REPORTED",
        source="test",
        confidence="verified",
        health_score=1.0,
    )
    decision = DeterministicScheduler().schedule(
        task_id="prefiltered-evidence",
        requirements=coding_requirements(),
        topology=ExecutionTopology.SINGLE,
        workers=(worker("known"),),
        resource_evidence=(evidence,),
    )

    assert decision.selected_worker_ids == ("known",)
    assert [item.worker_id for item in decision.candidates] == ["known"]
    assert [item["workerID"] for item in decision.explanation["resourceEvidence"]] == [
        "prefiltered"
    ]


def test_local_worker_cannot_write_code_even_if_capability_claims_it() -> None:
    decision = DeterministicScheduler().schedule(
        task_id="task-code",
        requirements=coding_requirements(),
        topology=ExecutionTopology.SINGLE,
        workers=[worker("local", provider=Provider.LOCAL, code_write_allowed=False)],
    )
    assert decision.selected_worker_ids == ()
    assert [item.reason_code for item in decision.rejected] == ["CODE_WRITE_FORBIDDEN"]


def test_red_task_requires_durable_approval() -> None:
    requirements = coding_requirements(
        permission_class=PermissionClass.RED,
        approval_state=ApprovalState.PENDING,
    )
    decision = DeterministicScheduler().schedule(
        task_id="task-red",
        requirements=requirements,
        topology=ExecutionTopology.SINGLE,
        workers=[worker("cloud")],
    )
    assert decision.selected_worker_ids == ()
    assert any(item.reason_code == "HUMAN_APPROVAL_REQUIRED" for item in decision.rejected)


def test_all_required_topologies_have_explicit_selection_behavior() -> None:
    workers = [
        worker("expensive", quality=0.95, cost=0.1),
        worker("cheap", quality=0.7, cost=1.0),
        worker("reviewer", capabilities=frozenset({"coding", "review"}), quality=0.6),
    ]
    scheduler = DeterministicScheduler()
    requirements = coding_requirements(panel_size=2, preferred_workers=("reviewer", "cheap"))

    fallback = scheduler.schedule(
        task_id="fallback",
        requirements=requirements,
        topology=ExecutionTopology.FALLBACK,
        workers=workers,
    )
    cheap_first = scheduler.schedule(
        task_id="cheap",
        requirements=requirements,
        topology=ExecutionTopology.CHEAP_FIRST_ESCALATION,
        workers=workers,
    )
    primary_review = scheduler.schedule(
        task_id="review",
        requirements=requirements,
        topology=ExecutionTopology.PRIMARY_REVIEWER,
        workers=workers,
    )
    panel = scheduler.schedule(
        task_id="panel",
        requirements=requirements,
        topology=ExecutionTopology.PARALLEL_PANEL,
        workers=workers,
    )

    assert fallback.selected_worker_ids == ("reviewer",)
    assert cheap_first.selected_worker_ids == ("cheap",)
    assert len(primary_review.selected_worker_ids) == 2
    assert len(set(primary_review.selected_worker_ids)) == 2
    assert len(panel.selected_worker_ids) == 2


def test_unavailable_worker_rejections_are_machine_readable() -> None:
    decision = DeterministicScheduler().schedule(
        task_id="unavailable",
        requirements=coding_requirements(),
        topology=ExecutionTopology.SINGLE,
        workers=[
            worker("busy", state=WorkerState.RUNNING),
            worker("limited", resource=ResourceState.RATE_LIMITED),
        ],
    )
    assert decision.selected_worker_ids == ()
    assert {item.reason_code for item in decision.rejected} == {
        "WORKER_UNAVAILABLE",
        "RESOURCE_UNAVAILABLE",
    }
