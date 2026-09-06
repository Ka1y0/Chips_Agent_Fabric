from __future__ import annotations

import pytest

from project_supervisor.cluster import (
    AgentRole,
    ClusterBlueprint,
    ClusterPlanner,
    ClusterStage,
    computer_use_blueprint,
    parallel_work_blueprint,
)
from project_supervisor.fabric.execution import SpawnPolicy


def test_computer_use_expands_role_aware_dag_without_selecting_workers() -> None:
    blueprint = computer_use_blueprint("computer-task", visual_replicas=2)
    plan = ClusterPlanner().expand(blueprint)
    stages = {stage.stage_id: stage for stage in blueprint.stages}

    assert [stage.stage_id for stage in blueprint.stages] == [
        "observe-ground",
        "plan",
        "act",
        "verify",
        "synthesize",
    ]
    assert stages["observe-ground"].role is AgentRole.FAST_VISION
    assert stages["observe-ground"].required_capabilities == frozenset({"read-image"})
    assert stages["act"].required_capabilities == frozenset({"control-gui"})
    assert stages["act"].resource_kinds == frozenset({"display", "keyboard", "mouse"})
    assert stages["verify"].independent_from_roles == frozenset({AgentRole.COMPUTER_ACTOR})
    assert all(instance.to_protocol()["workerID"] is None for instance in plan.instances)
    assert plan.to_protocol()["workerSelectionPending"] is True


def test_cluster_expansion_is_deterministic_and_preserves_waves() -> None:
    blueprint = parallel_work_blueprint(
        "research",
        capability="research",
        fanout=2,
        independent_review=True,
    )

    first = ClusterPlanner().expand(blueprint)
    second = ClusterPlanner().expand(blueprint)

    assert first == second
    parallel = [value for value in first.instances if value.stage_id == "parallel-work"]
    assert [value.instance_id for value in parallel] == ["parallel-work:1", "parallel-work:2"]
    assert {value.wave for value in parallel} == {1}
    assert next(value for value in first.instances if value.stage_id == "review").wave == 3


def test_cluster_blueprint_rejects_cycles() -> None:
    with pytest.raises(ValueError, match="must form a DAG"):
        ClusterBlueprint(
            "cycle",
            (
                ClusterStage("a", AgentRole.PLANNER, frozenset({"reasoning"}), depends_on=("b",)),
                ClusterStage(
                    "b",
                    AgentRole.REVIEWER,
                    frozenset({"verify-result"}),
                    depends_on=("a",),
                ),
            ),
            max_parallelism=1,
        )


def test_parallel_fanout_is_bounded_by_canonical_spawn_policy() -> None:
    with pytest.raises(ValueError, match="between one and 32"):
        parallel_work_blueprint("too-wide", capability="research", fanout=33)
    with pytest.raises(ValueError, match="canonical active-task limit"):
        parallel_work_blueprint("default-limit", capability="research", fanout=9)

    widened = parallel_work_blueprint(
        "reviewed-limit",
        capability="research",
        fanout=12,
        spawn_policy=SpawnPolicy(
            max_children_per_parent=16,
            max_parallel_tasks=16,
            max_parallel_worker_slots=16,
        ),
    )
    assert widened.max_parallelism == 12
    assert widened.to_protocol()["workerSelectionAuthority"] == "deterministicScheduler"
