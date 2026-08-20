from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime

import pytest

from project_supervisor.domain import (
    ExecutionTopology,
    Harness,
    ModelDescriptor,
    NodeState,
    Provider,
    ResourceState,
    TaskLabel,
    TaskRequirements,
    WorkerSnapshot,
    WorkerState,
)
from project_supervisor.fabric.capabilities import (
    INITIAL_CAPABILITY_CATALOG,
    WORKER_MANIFEST_SCHEMA_VERSION,
    CapabilityCatalog,
    CapabilityClaim,
    CapabilityDefinition,
    CapabilityError,
    CapabilityParameterError,
    CostMode,
    ObservationFreshness,
    QuotaAvailability,
    SubscriptionState,
    UnknownCapabilityError,
    WorkerDynamicState,
    WorkerHealth,
    WorkerLocality,
    WorkerManifest,
    WorkerPrivacy,
)
from project_supervisor.scheduler import DeterministicScheduler


def versioned_worker(
    worker_id: str,
    *,
    claims: tuple[str | CapabilityClaim, ...] = ("analysis",),
    cost_mode: CostMode = CostMode.UNKNOWN,
    subscription: SubscriptionState = SubscriptionState.UNKNOWN,
    locality: WorkerLocality = WorkerLocality.REMOTE,
    privacy: WorkerPrivacy = WorkerPrivacy.INTERNAL,
    health: WorkerHealth = WorkerHealth.HEALTHY,
    health_freshness: ObservationFreshness = ObservationFreshness.FRESH,
    quota_freshness: ObservationFreshness = ObservationFreshness.FRESH,
    quota: QuotaAvailability = QuotaAvailability.AVAILABLE,
    resource_state: ResourceState = ResourceState.AVAILABLE,
    load: float = 0.0,
    quality: float = 0.8,
    incremental_cost_usd: float | None = None,
    running_tasks: int = 0,
    max_concurrency: int = 1,
    state: WorkerState = WorkerState.IDLE,
    manifest_valid_until: datetime | None = None,
) -> WorkerSnapshot:
    manifest = WorkerManifest(
        worker_id=worker_id,
        node_id="node-1",
        provider_id="mock",
        adapter_kind="fixture",
        capabilities=claims,
        models=("model-1",),
        locality=locality,
        privacy=privacy,
        cost_mode=cost_mode,
        incremental_cost_usd=incremental_cost_usd,
        max_concurrency=max_concurrency,
    )
    return WorkerSnapshot(
        id=worker_id,
        node_id="node-1",
        harness=Harness.MOCK,
        provider=Provider.MOCK,
        model=ModelDescriptor(
            "model-1",
            "Model 1",
            Provider.MOCK,
            context_window_tokens=128_000,
        ),
        state=state,
        node_state=NodeState.ONLINE,
        resource_state=resource_state,
        capabilities=manifest.capability_names,
        capability_claims=tuple(manifest.capabilities),
        code_write_allowed=False,
        privacy_allowed=True,
        quality_score=quality,
        reliability_score=0.8,
        expected_latency_seconds=5,
        monetary_cost_score=0.5,
        manifest_schema_version=manifest.schema_version,
        capability_catalog_version=manifest.catalog_version,
        manifest_revision=manifest.manifest_revision,
        manifest_digest=manifest.digest,
        manifest_valid_until=manifest_valid_until,
        cost_mode=cost_mode,
        subscription_state=subscription,
        incremental_cost_usd=incremental_cost_usd,
        quota_state=quota,
        quota_freshness=quota_freshness,
        locality=locality,
        privacy=privacy,
        health=health,
        health_freshness=health_freshness,
        worker_load=load,
        running_tasks=running_tasks,
        max_concurrency=max_concurrency,
    )


def analysis_requirements(**changes: object) -> TaskRequirements:
    values = {
        "labels": frozenset({TaskLabel.RESEARCH}),
        "required_capabilities": frozenset({"analysis"}),
    }
    values.update(changes)
    return TaskRequirements(**values)


def schedule(
    workers: tuple[WorkerSnapshot, ...],
    requirements: TaskRequirements | None = None,
):
    return DeterministicScheduler().schedule(
        task_id="task-capability-pilot",
        requirements=requirements or analysis_requirements(),
        topology=ExecutionTopology.SINGLE,
        workers=workers,
    )


def test_catalog_is_versioned_extensible_and_unknown_manifest_claims_fail_closed() -> None:
    extension = CapabilityDefinition(
        "fixture-specialist",
        aliases=frozenset({"FIXTURE_SPECIALIST"}),
    )
    catalog = INITIAL_CAPABILITY_CATALOG.extend(
        version="fabric-capabilities/v1.fixture",
        definitions=(extension,),
    )
    assert catalog.resolve("FIXTURE_SPECIALIST").name == "fixture-specialist"
    assert catalog.to_protocol()["version"] == "fabric-capabilities/v1.fixture"

    manifest = WorkerManifest(
        worker_id="worker-extension",
        node_id="node-1",
        provider_id="fixture",
        adapter_kind="fixture",
        capabilities=("FIXTURE_SPECIALIST",),
        catalog_version=catalog.version,
        catalog=catalog,
    )
    assert manifest.capability_names == frozenset({"fixture-specialist"})
    with pytest.raises(UnknownCapabilityError, match="unknown capability"):
        WorkerManifest(
            worker_id="worker-unknown",
            node_id="node-1",
            provider_id="fixture",
            adapter_kind="fixture",
            capabilities=("provider-magic",),
        )


def test_initial_catalog_contains_the_provider_independent_core_vocabulary() -> None:
    expected = {
        "edit-code",
        "read-text",
        "read-image",
        "read-video",
        "read-youtube",
        "web-search",
        "search-x",
        "generate-image",
        "generate-video",
        "generate-3d",
        "use-local-gpu",
        "use-local-model",
        "control-browser",
        "control-gui",
        "run-tests",
        "review-code",
        "verify-result",
    }
    assert expected.issubset(
        {definition.name for definition in INITIAL_CAPABILITY_CATALOG.definitions}
    )
    assert INITIAL_CAPABILITY_CATALOG.resolve("coding").name == "edit-code"
    assert INITIAL_CAPABILITY_CATALOG.resolve("review").name == "review-code"


def test_manifest_digest_is_canonical_and_dynamic_observations_are_separate() -> None:
    first = WorkerManifest(
        worker_id="worker-digest",
        node_id="node-1",
        provider_id="fixture",
        adapter_kind="fixture",
        capabilities=("nativeWorkers", "analysis"),
        models=("model-b", "model-a"),
        cost_mode=CostMode.SUBSCRIPTION,
    )
    second = WorkerManifest(
        worker_id="worker-digest",
        node_id="node-1",
        provider_id="fixture",
        adapter_kind="fixture",
        capabilities=("analysis", "native-workers"),
        models=("model-b", "model-a"),
        cost_mode=CostMode.SUBSCRIPTION,
    )
    observed = WorkerDynamicState(
        worker_id="worker-digest",
        health=WorkerHealth.DEGRADED,
        health_freshness=ObservationFreshness.FRESH,
        quota=QuotaAvailability.WARNING,
        quota_freshness=ObservationFreshness.FRESH,
        subscription_state=SubscriptionState.AVAILABLE,
        load=0.7,
        running_tasks=2,
        observed_at=datetime(2026, 8, 11, tzinfo=UTC),
    )

    assert first.digest == second.digest
    assert first.schema_version == WORKER_MANIFEST_SCHEMA_VERSION
    assert "subscription" not in first.canonical_data()
    assert observed.to_protocol()["subscriptionState"] == "available"
    assert first.digest.startswith("sha256:")


def test_parameter_aliases_and_worker_upper_bounds_satisfy_read_video_request() -> None:
    worker = versioned_worker(
        "video-worker",
        claims=(
            CapabilityClaim(
                "READ_VIDEO",
                {"maxDuration": 600, "localOnly": True},
            ),
        ),
        locality=WorkerLocality.LOCAL,
    )
    supported = TaskRequirements(
        labels=frozenset({TaskLabel.RESEARCH}),
        required_capabilities=frozenset({"read_video"}),
        required_capability_parameters=(
            CapabilityClaim(
                "read-video",
                {"max_duration_seconds": 300, "local_only": True},
            ),
        ),
        local_only=True,
    )
    too_long = replace(
        supported,
        required_capability_parameters=(
            CapabilityClaim(
                "read-video",
                {"max-duration-seconds": 601, "local-only": True},
            ),
        ),
    )

    assert schedule((worker,), supported).selected_worker_ids == ("video-worker",)
    rejected = schedule((worker,), too_long)
    assert rejected.selected_worker_ids == ()
    assert "CAPABILITY_PARAMETER_MISMATCH" in {item.reason_code for item in rejected.rejected}


def test_capability_parameter_schema_rejects_bad_types_and_unknown_parameters() -> None:
    with pytest.raises(CapabilityParameterError, match="must be number"):
        WorkerManifest(
            worker_id="worker-bad-type",
            node_id="node-1",
            provider_id="fixture",
            adapter_kind="fixture",
            capabilities=(CapabilityClaim("read-video", {"maxDuration": "many"}),),
        )
    with pytest.raises(CapabilityParameterError, match="unknown parameters"):
        WorkerManifest(
            worker_id="worker-bad-key",
            node_id="node-1",
            provider_id="fixture",
            adapter_kind="fixture",
            capabilities=(CapabilityClaim("read-video", {"codecSecret": "x"}),),
        )


def test_legacy_snapshots_remain_compatible_but_cannot_claim_parameter_proof() -> None:
    legacy = WorkerSnapshot(
        id="legacy",
        node_id="node-1",
        harness=Harness.MOCK,
        provider=Provider.MOCK,
        model=ModelDescriptor("legacy", "Legacy", Provider.MOCK),
        state=WorkerState.IDLE,
        node_state=NodeState.ONLINE,
        resource_state=ResourceState.AVAILABLE,
        capabilities=frozenset({"analysis"}),
        code_write_allowed=False,
        privacy_allowed=True,
    )
    assert schedule((legacy,)).selected_worker_ids == ("legacy",)

    parameterized = analysis_requirements(
        required_capability_parameters=(CapabilityClaim("analysis"),)
    )
    decision = schedule((legacy,), parameterized)
    assert decision.selected_worker_ids == ()
    assert [item.reason_code for item in decision.rejected] == ["CAPABILITY_PARAMETERS_UNVERIFIED"]


def test_default_cost_order_never_treats_unknown_as_free_or_unlimited() -> None:
    workers = (
        versioned_worker(
            "subscription",
            cost_mode=CostMode.SUBSCRIPTION,
            subscription=SubscriptionState.AVAILABLE,
        ),
        versioned_worker("local-free", cost_mode=CostMode.LOCAL_FREE),
        versioned_worker("paid", cost_mode=CostMode.PAID, incremental_cost_usd=0.01),
        versioned_worker("metered", cost_mode=CostMode.METERED, incremental_cost_usd=0.01),
        versioned_worker("unknown"),
    )
    decision = schedule(workers)
    assert [candidate.worker_id for candidate in decision.candidates] == [
        "subscription",
        "local-free",
        "paid",
        "metered",
        "unknown",
    ]

    zero_cost_only = schedule(
        (workers[1], workers[-1]),
        analysis_requirements(max_incremental_cost_usd=0),
    )
    assert zero_cost_only.selected_worker_ids == ("local-free",)
    assert [item.reason_code for item in zero_cost_only.rejected] == ["INCREMENTAL_COST_UNKNOWN"]


def test_routing_pilot_uses_load_and_exposes_score_components_and_rejections() -> None:
    subscription_high_load = versioned_worker(
        "worker-a",
        cost_mode=CostMode.SUBSCRIPTION,
        subscription=SubscriptionState.AVAILABLE,
        locality=WorkerLocality.REMOTE,
        privacy=WorkerPrivacy.SENSITIVE,
        load=0.95,
    )
    local_free_low_load = versioned_worker(
        "worker-b",
        cost_mode=CostMode.LOCAL_FREE,
        locality=WorkerLocality.LOCAL,
        privacy=WorkerPrivacy.INTERNAL,
        load=0.0,
    )
    wrong_capability = versioned_worker(
        "worker-c",
        claims=("review",),
        cost_mode=CostMode.LOCAL_FREE,
        locality=WorkerLocality.LOCAL,
    )
    decision = schedule(
        (subscription_high_load, local_free_low_load, wrong_capability),
        analysis_requirements(preferred_capabilities=frozenset({"reasoning"})),
    )

    assert decision.selected_worker_ids == ("worker-b",)
    assert {item.worker_id: item.reason_code for item in decision.rejected} == {
        "worker-c": "CAPABILITY_MISMATCH"
    }
    components = decision.explanation["candidateScores"][0]["components"]
    assert {
        "preferredCapabilityFit",
        "costModePreference",
        "workerHealth",
        "workerLoad",
        "quotaFreshness",
    }.issubset(components)
    assert decision.explanation["costPreferenceOrder"][0] == "activeSubscription"


def test_health_quota_and_privacy_changes_reroute_without_manifest_mutation() -> None:
    remote_sensitive = versioned_worker(
        "worker-a",
        cost_mode=CostMode.SUBSCRIPTION,
        subscription=SubscriptionState.AVAILABLE,
        privacy=WorkerPrivacy.SENSITIVE,
        load=0.9,
    )
    local_internal = versioned_worker(
        "worker-b",
        cost_mode=CostMode.LOCAL_FREE,
        locality=WorkerLocality.LOCAL,
        privacy=WorkerPrivacy.INTERNAL,
        load=0.0,
    )
    assert schedule((remote_sensitive, local_internal)).selected_worker_ids == ("worker-b",)

    unhealthy = replace(local_internal, health=WorkerHealth.UNHEALTHY)
    assert schedule((remote_sensitive, unhealthy)).selected_worker_ids == ("worker-a",)
    exhausted = replace(local_internal, resource_state=ResourceState.BUDGET_EXHAUSTED)
    assert schedule((remote_sensitive, exhausted)).selected_worker_ids == ("worker-a",)
    registry_exhausted = replace(
        local_internal,
        quota_state=QuotaAvailability.EXHAUSTED,
    )
    quota_reroute = schedule((remote_sensitive, registry_exhausted))
    assert quota_reroute.selected_worker_ids == ("worker-a",)
    assert any(item.reason_code == "CAPABILITY_QUOTA_EXHAUSTED" for item in quota_reroute.rejected)
    private = schedule(
        (remote_sensitive, local_internal),
        analysis_requirements(privacy_sensitive=True),
    )
    assert private.selected_worker_ids == ("worker-a",)
    assert any(item.reason_code == "PRIVACY_CLASS_MISMATCH" for item in private.rejected)


def test_quota_freshness_and_load_observations_deterministically_reroute() -> None:
    worker_a = versioned_worker("worker-a", load=0.0)
    worker_b = versioned_worker("worker-b", load=0.0)
    assert schedule((worker_a, worker_b)).selected_worker_ids == ("worker-a",)

    stale_a = replace(worker_a, quota_freshness=ObservationFreshness.STALE)
    assert schedule((stale_a, worker_b)).selected_worker_ids == ("worker-b",)
    loaded_b = replace(worker_b, worker_load=1.0)
    assert schedule((stale_a, loaded_b)).selected_worker_ids == ("worker-a",)


def test_explicit_override_never_bypasses_capability_or_health_safety() -> None:
    capable = versioned_worker("capable")
    wrong = versioned_worker("wrong", claims=("review",))
    wrong_override = schedule(
        (capable, wrong),
        analysis_requirements(explicit_worker_override="wrong"),
    )
    assert wrong_override.selected_worker_ids == ()
    assert {item.reason_code for item in wrong_override.rejected} >= {
        "EXPLICIT_OVERRIDE_MISMATCH",
        "CAPABILITY_MISMATCH",
    }

    unhealthy = replace(capable, health=WorkerHealth.UNHEALTHY)
    unsafe_override = schedule(
        (unhealthy,),
        analysis_requirements(explicit_worker_override="capable"),
    )
    assert unsafe_override.selected_worker_ids == ()
    assert [item.reason_code for item in unsafe_override.rejected] == ["WORKER_UNHEALTHY"]


def test_manifest_identity_and_catalog_mismatches_fail_closed() -> None:
    worker = versioned_worker("versioned")
    unsupported_schema = replace(
        worker,
        manifest_schema_version="worker-capability-manifest/v999",
    )
    decision = schedule((unsupported_schema,))
    assert decision.selected_worker_ids == ()
    assert any(item.reason_code == "MANIFEST_SCHEMA_UNSUPPORTED" for item in decision.rejected)

    with pytest.raises(CapabilityError, match="unsupported"):
        WorkerManifest(
            worker_id="bad-schema",
            node_id="node-1",
            provider_id="fixture",
            adapter_kind="fixture",
            capabilities=("analysis",),
            schema_version="worker-capability-manifest/v999",
        )


def test_minimum_quality_and_required_manifest_versions_are_hard_constraints() -> None:
    worker = versioned_worker("low-quality", quality=0.4)
    decision = schedule(
        (worker,),
        analysis_requirements(
            minimum_quality_score=0.8,
            required_manifest_schema_version=WORKER_MANIFEST_SCHEMA_VERSION,
            required_capability_catalog_version=INITIAL_CAPABILITY_CATALOG.version,
        ),
    )
    assert decision.selected_worker_ids == ()
    assert [item.reason_code for item in decision.rejected] == ["QUALITY_BELOW_MINIMUM"]

    mismatch = schedule(
        (worker,),
        analysis_requirements(required_capability_catalog_version="future-catalog/v2"),
    )
    assert mismatch.selected_worker_ids == ()
    assert [item.reason_code for item in mismatch.rejected] == [
        "CAPABILITY_CATALOG_VERSION_MISMATCH"
    ]


def test_capacity_and_task_locality_are_hard_constraints() -> None:
    full = versioned_worker("full", running_tasks=2, max_concurrency=2)
    remote = versioned_worker("remote")
    decision = schedule(
        (full, remote),
        analysis_requirements(local_only=True),
    )
    assert decision.selected_worker_ids == ()
    assert {item.reason_code for item in decision.rejected} == {
        "WORKER_CAPACITY_EXHAUSTED",
        "LOCALITY_MISMATCH",
    }


def test_manifest_expiry_and_partial_versioned_capacity_are_fail_closed() -> None:
    expired = versioned_worker(
        "expired",
        manifest_valid_until=datetime(2000, 1, 1, tzinfo=UTC),
    )
    partial = versioned_worker(
        "partial",
        state=WorkerState.RUNNING,
        running_tasks=1,
        max_concurrency=2,
        manifest_valid_until=datetime(2100, 1, 1, tzinfo=UTC),
    )

    decision = schedule((expired, partial))

    assert decision.selected_worker_ids == ("partial",)
    assert [item.reason_code for item in decision.rejected] == ["MANIFEST_EXPIRED"]
    assert decision.candidates[0].components["availability"] == 0.5

    inconsistent = replace(partial, state=WorkerState.IDLE)
    rejected = schedule((inconsistent,))
    assert rejected.selected_worker_ids == ()
    assert [item.reason_code for item in rejected.rejected] == ["WORKER_UNAVAILABLE"]

    with pytest.raises(ValueError, match="timezone-aware"):
        replace(partial, manifest_valid_until=datetime(2100, 1, 1))


def test_catalog_constructor_rejects_alias_collisions() -> None:
    with pytest.raises(CapabilityError, match="duplicate capability alias"):
        CapabilityCatalog(
            "fixture/v1",
            (
                CapabilityDefinition("first", aliases=frozenset({"shared"})),
                CapabilityDefinition("second", aliases=frozenset({"shared"})),
            ),
        )
