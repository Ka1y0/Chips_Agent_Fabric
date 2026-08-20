from __future__ import annotations

from project_supervisor.cluster import AgentRole
from project_supervisor.hybrid_engine import (
    HybridEngine,
    HybridExecutionMode,
    HybridRequest,
    HybridWorkloadKind,
)


def test_computer_use_becomes_a_bridge_enabled_specialist_dag() -> None:
    plan = HybridEngine().plan(
        HybridRequest(
            "ui-1",
            HybridWorkloadKind.COMPUTER_USE,
            requested_parallelism=2,
            visual_interaction_steps=25,
        )
    )

    assert plan.mode is HybridExecutionMode.DAG_CLUSTER
    assert plan.bridge_enabled
    assert plan.blueprint.stages[0].role is AgentRole.FAST_VISION
    assert plan.blueprint.stages[0].replicas == 2
    assert any("low-latency perception lane" in fact for fact in plan.facts)


def test_privacy_sensitive_work_is_local_first_only_when_capabilities_fit() -> None:
    request = HybridRequest(
        "private",
        HybridWorkloadKind.RESEARCH,
        required_capabilities=frozenset({"research"}),
        privacy_sensitive=True,
    )

    local = HybridEngine().plan(request, local_capabilities=frozenset({"research"}))
    insufficient = HybridEngine().plan(request, local_capabilities=frozenset({"chat"}))

    assert local.local_first
    assert not insufficient.local_first
    assert any("no silent privacy downgrade" in fact for fact in insufficient.facts)


def test_authority_bearing_multi_stage_work_never_uses_bridge() -> None:
    plan = HybridEngine().plan(
        HybridRequest(
            "authority",
            HybridWorkloadKind.CODING,
            requested_parallelism=2,
            authority_bearing=True,
        )
    )

    assert not plan.bridge_enabled
    assert any("authority-bearing" in fact for fact in plan.facts)


def test_high_uncertainty_expands_single_request_to_bounded_panel() -> None:
    plan = HybridEngine().plan(
        HybridRequest(
            "uncertain",
            HybridWorkloadKind.RESEARCH,
            high_uncertainty=True,
        )
    )

    assert plan.mode is HybridExecutionMode.PARALLEL_PANEL
    parallel_stage = next(
        stage for stage in plan.blueprint.stages if stage.stage_id == "parallel-work"
    )
    assert parallel_stage.replicas == 2


def test_simple_latency_sensitive_request_uses_cheap_first_escalation() -> None:
    plan = HybridEngine().plan(
        HybridRequest(
            "quick",
            HybridWorkloadKind.GENERAL,
            latency_sensitive=True,
        )
    )

    assert plan.mode is HybridExecutionMode.CHEAP_FIRST_ESCALATION
    assert len(plan.blueprint.stages) == 1
