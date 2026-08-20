from __future__ import annotations

import pytest

from project_supervisor.cluster import (
    AgentRole,
    ClusterPlanner,
    ClusterWorker,
    computer_use_blueprint,
    parallel_work_blueprint,
)


def worker(
    worker_id: str,
    capabilities: set[str],
    *,
    quality: float = 0.7,
    latency: float = 0.7,
    concurrency: int = 1,
) -> ClusterWorker:
    return ClusterWorker(
        worker_id,
        "node-1",
        frozenset(capabilities),
        quality,
        latency,
        max_concurrency=concurrency,
    )


def test_computer_use_delegates_perception_to_fast_visual_worker() -> None:
    blueprint = computer_use_blueprint("computer-task")
    workers = (
        worker("vision-fast", {"vision.grounding", "latency.fast"}, latency=1.0),
        worker("planner", {"reasoning"}, quality=0.95),
        worker("actor", {"computer.use", "ui.atomicActions"}, latency=0.9),
        worker("reviewer", {"vision.grounding", "review", "reasoning"}, quality=0.98),
        worker("synth", {"reasoning"}, quality=0.9),
    )

    plan = ClusterPlanner().assign(blueprint, workers)
    by_stage = {assignment.stage_id: assignment for assignment in plan.assignments}

    assert plan.complete
    assert by_stage["observe-ground"].worker_id == "vision-fast"
    assert by_stage["act"].worker_id == "actor"
    assert by_stage["verify"].worker_id == "reviewer"
    assert by_stage["verify"].worker_id != by_stage["act"].worker_id


def test_sequential_stages_can_reuse_capacity_while_same_wave_respects_limits() -> None:
    blueprint = parallel_work_blueprint(
        "research",
        capability="research",
        fanout=2,
        independent_review=False,
    )
    workers = (
        worker("general", {"reasoning", "research"}, concurrency=1),
        worker("research-2", {"research"}, concurrency=1),
    )

    plan = ClusterPlanner().assign(blueprint, workers)

    assert plan.complete
    parallel = [item for item in plan.assignments if item.stage_id == "parallel-work"]
    assert {item.worker_id for item in parallel} == {"general", "research-2"}
    assert any(
        item.stage_id == "synthesize" and item.worker_id == "general"
        for item in plan.assignments
    )


def test_missing_specialist_is_machine_readable() -> None:
    plan = ClusterPlanner().assign(
        computer_use_blueprint("missing-vision"),
        (worker("reasoner", {"reasoning"}),),
    )

    assert not plan.complete
    assert "observe-ground:1" in plan.unassigned_stages


def test_duplicate_worker_identity_is_rejected() -> None:
    duplicate = worker("same", {"reasoning", "research"})
    with pytest.raises(ValueError, match="duplicate cluster worker"):
        ClusterPlanner().assign(
            parallel_work_blueprint("duplicate", capability="research", fanout=1),
            (duplicate, duplicate),
        )


def test_parallel_fanout_is_bounded() -> None:
    with pytest.raises(ValueError, match="between one and 32"):
        parallel_work_blueprint("too-wide", capability="research", fanout=33)

    assert computer_use_blueprint("roles").stages[0].role is AgentRole.FAST_VISION
