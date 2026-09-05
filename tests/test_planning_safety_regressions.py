"""Regression tests for the planning/profile audit; no live providers or credentials."""

from __future__ import annotations

import math

import pytest

from project_supervisor.cluster import (
    AgentRole,
    ClusterBlueprint,
    ClusterPlanner,
    ClusterStage,
)
from project_supervisor.hybrid_engine import HybridEngine, HybridRequest, HybridWorkloadKind
from project_supervisor.local_models import (
    LocalModelObservation,
    LocalModelRole,
    LocalModelRuntime,
    LocalRuntimeCandidate,
    MemoryPressure,
    adapt_local_model,
    detect_runtime_candidates,
)


@pytest.mark.parametrize(
    "options",
    [
        {"requested_parallelism": 2},
        {"high_uncertainty": True},
        {"require_independent_review": True},
    ],
)
def test_private_plan_checks_all_generated_stage_capabilities(options: dict[str, object]) -> None:
    request = HybridRequest(
        "private-research",
        HybridWorkloadKind.RESEARCH,
        privacy_sensitive=True,
        **options,
    )
    plan = HybridEngine().plan(request, local_capabilities=frozenset({"research"}))

    assert plan.dispatch_blocked
    assert not plan.local_first
    assert not plan.remote_fallback_allowed
    assert not plan.bridge_enabled
    assert any("reasoning" in fact for fact in plan.facts)


def test_private_panel_can_proceed_when_every_stage_is_supported_locally() -> None:
    plan = HybridEngine().plan(
        HybridRequest(
            "private-panel",
            HybridWorkloadKind.RESEARCH,
            privacy_sensitive=True,
            requested_parallelism=2,
            require_independent_review=True,
        ),
        local_capabilities=frozenset({"research", "reasoning", "verify-result"}),
    )

    assert plan.local_first
    assert not plan.dispatch_blocked
    assert not plan.remote_fallback_allowed
    assert not plan.bridge_enabled


def test_computer_use_preserves_explicit_requirements_on_the_action_stage() -> None:
    plan = HybridEngine().plan(
        HybridRequest(
            "browser-task",
            HybridWorkloadKind.COMPUTER_USE,
            required_capabilities=frozenset({"control-browser"}),
        )
    )
    actor = next(stage for stage in plan.blueprint.stages if stage.role is AgentRole.COMPUTER_ACTOR)
    observer = next(stage for stage in plan.blueprint.stages if stage.role is AgentRole.FAST_VISION)

    assert {"control-gui", "control-browser"} <= actor.required_capabilities
    assert observer.required_capabilities == frozenset({"read-image"})
    assert any("control-browser" in item.required_capabilities for item in ClusterPlanner().expand(
        plan.blueprint
    ).instances)


def test_custom_computer_use_requirements_cannot_bypass_private_stage_preflight() -> None:
    plan = HybridEngine().plan(
        HybridRequest(
            "private-browser",
            HybridWorkloadKind.COMPUTER_USE,
            required_capabilities=frozenset({"control-browser"}),
            privacy_sensitive=True,
        ),
        local_capabilities=frozenset({"control-browser"}),
    )

    assert plan.dispatch_blocked
    assert not plan.remote_fallback_allowed


def test_computer_use_rejects_blank_explicit_capability_ids() -> None:
    with pytest.raises(ValueError, match="required_capabilities"):
        HybridRequest(
            "invalid-capability",
            HybridWorkloadKind.COMPUTER_USE,
            required_capabilities=frozenset({" "}),
        )


def test_cluster_expansion_and_protocol_preserve_stage_routing_weights() -> None:
    blueprint = ClusterBlueprint(
        "weighted-stage",
        stages=(ClusterStage(
            "observe",
            AgentRole.FAST_VISION,
            frozenset({"read-image"}),
            latency_weight=0.8,
            quality_weight=0.15,
            cost_weight=0.05,
        ),),
        max_parallelism=1,
    )
    expanded = ClusterPlanner().expand(blueprint)
    expected = {"latency": 0.8, "quality": 0.15, "cost": 0.05}

    assert blueprint.to_protocol()["stages"][0]["routingWeights"] == expected
    assert expanded.instances[0].to_protocol()["routingWeights"] == expected
    assert expanded.instances[0].latency_weight == 0.8
    assert expanded.to_protocol()["workerSelectionPending"] is True


@pytest.mark.parametrize("name", ["latency_weight", "quality_weight", "cost_weight"])
@pytest.mark.parametrize("value", [math.nan, math.inf, -math.inf])
def test_cluster_rejects_nonfinite_routing_weights(name: str, value: float) -> None:
    with pytest.raises(ValueError, match="weights"):
        ClusterStage("invalid", AgentRole.PLANNER, frozenset({"reasoning"}), **{name: value})


@pytest.mark.parametrize(
    ("supported", "mode"), [(None, "unknown"), (False, "unsupported"), (True, "onDemand")]
)
def test_reasoning_profile_preserves_unknown_false_and_true(
    supported: bool | None, mode: str,
) -> None:
    profile = adapt_local_model(LocalModelObservation(
        LocalModelRuntime.OLLAMA, "fixture-model", supports_reasoning=supported,
    ))

    assert profile.reasoning_mode == mode
    assert profile.to_protocol()["reasoningMode"] == mode
    assert (LocalModelRole.DEEP_REASONING in profile.roles) is (supported is True)


@pytest.mark.parametrize("name", ["supports_vision", "supports_tools", "supports_reasoning"])
@pytest.mark.parametrize("value", ["false", 0, 1])
def test_model_capability_flags_do_not_accept_truthy_untyped_values(name: str, value: object) -> None:
    with pytest.raises(ValueError, match=name):
        LocalModelObservation(LocalModelRuntime.OLLAMA, "fixture-model", **{name: value})


def test_unknown_memory_headroom_does_not_recommend_parallel_or_full_gpu_load() -> None:
    profile = adapt_local_model(
        LocalModelObservation(
            LocalModelRuntime.LM_STUDIO,
            "unmeasured-memory",
            measured_tokens_per_second=100.0,
            max_parallel=4,
        ),
        host_vram_bytes=16 * 1024**3,
    )

    assert profile.memory_pressure is MemoryPressure.UNKNOWN
    assert profile.observed_parallel_capacity == 4
    assert profile.parallelism == 1
    assert profile.gpu_offload_policy == "unknown"
    assert profile.to_protocol()["adaptiveMutation"] is False


@pytest.mark.parametrize("entrypoint", ["configured", "direct"])
@pytest.mark.parametrize("endpoint", [
    "http://localhost:9000/v1?token=fixture-only",
    "http://localhost:9000/v1#fragment",
    "http://@localhost:9000/v1",
    "http://fixture-only@localhost:9000/v1",
    "http://localhost:not-a-port/v1",
    "http://localhost:65536/v1",
    "http://localhost:0/v1",
    "http://localhost:/v1",
    "http://[::1]:0/v1",
    "\nhttp://localhost:9000/v1",
    "http://local\thost:9000/v1",
    "http://localhost:9000/v1 ",
    "https://models.example.invalid/v1",
])
def test_local_endpoint_rejects_unsafe_or_malformed_urls_without_echoing_them(
    entrypoint: str, endpoint: str,
) -> None:
    with pytest.raises(ValueError) as caught:
        if entrypoint == "configured":
            detect_runtime_candidates({}, configured_openai_endpoints=(endpoint,))
        else:
            LocalRuntimeCandidate(LocalModelRuntime.OPENAI_COMPATIBLE, endpoint, "fixture")
    assert "fixture-only" not in str(caught.value)


@pytest.mark.parametrize("endpoint", [
    "http://localhost:9000/v1",
    "https://127.0.0.1:1234/v1",
    "http://[::1]:8080/v1",
])
def test_explicit_loopback_candidates_remain_usable_without_a_probe(endpoint: str) -> None:
    candidate = LocalRuntimeCandidate(LocalModelRuntime.OPENAI_COMPATIBLE, endpoint, "fixture")
    assert candidate.to_protocol()["endpoint"] == endpoint
    assert candidate.probe_required
