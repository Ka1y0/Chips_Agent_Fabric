from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

from project_supervisor.adapters import MockAdapter, MockBehavior
from project_supervisor.domain import (
    EvidenceConfidence,
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
from project_supervisor.hybrid import ResourceRoutingEvidence
from project_supervisor.resource_usage import (
    PreDispatchQuotaGuard,
    QuotaGuardCandidate,
    QuotaState,
    ResourceObservation,
    ResourceUsageRepository,
    ResourceUsageService,
    UsageMetric,
    UsageProvenance,
)
from project_supervisor.runtime import AdapterRegistry, SupervisorRuntime
from project_supervisor.scheduler import DeterministicScheduler
from project_supervisor.store import StateStore


def worker(worker_id: str, *, state: WorkerState = WorkerState.IDLE) -> WorkerSnapshot:
    return WorkerSnapshot(
        id=worker_id,
        node_id="node-1",
        harness=Harness.MOCK,
        provider=Provider.MOCK,
        model=ModelDescriptor("mock", "Mock", Provider.MOCK),
        state=state,
        node_state=NodeState.ONLINE,
        resource_state=ResourceState.AVAILABLE,
        capabilities=frozenset({"analysis"}),
        code_write_allowed=False,
        privacy_allowed=True,
        quality_score=0.5,
        reliability_score=0.5,
        expected_latency_seconds=1,
        monetary_cost_score=1.0,
    )


def evidence(worker_id: str, state: str, health: float) -> ResourceRoutingEvidence:
    return ResourceRoutingEvidence(
        worker_id=worker_id,
        quota_pool_id=f"pool-{worker_id}",
        provider="mock",
        quota_state=state,
        freshness="fresh" if state != "unknown" else "unknown",
        provenance="PROVIDER_REPORTED" if state != "unknown" else "UNKNOWN",
        source="fixtureStatus",
        confidence="providerReported" if state != "unknown" else "unknown",
        health_score=health,
        observed_at=datetime(2026, 8, 10, tzinfo=UTC) if state != "unknown" else None,
        reason="notReported" if state == "unknown" else None,
    )


def observation(
    worker_id: str,
    state: QuotaState,
    now: datetime,
    *,
    provider: str = "mock",
) -> ResourceObservation:
    remaining = 0 if state is QuotaState.EXHAUSTED else 50
    return ResourceObservation(
        provider=provider,
        quota_pool_id=f"pool-{worker_id}",
        worker_id=worker_id,
        remaining=UsageMetric.known(remaining, "requests", UsageProvenance.PROVIDER_REPORTED),
        quota_state=state,
        quota_state_provenance=UsageProvenance.PROVIDER_REPORTED,
        source="fixtureStatus",
        confidence=EvidenceConfidence.PROVIDER_REPORTED,
        observed_at=now,
        fresh_until=now + timedelta(seconds=60),
    )


def setup_store(tmp_path: Path) -> StateStore:
    store = StateStore(tmp_path / "resource-routing.db")
    store.create_project(
        project_id="project-1",
        name="Resource routing",
        root_path=str(tmp_path),
        goal="route from real resource ledger evidence",
    )
    store.upsert_node(
        node_id="node-1",
        hostname="fixture",
        display_name="Fixture",
        role="control",
        state=NodeState.ONLINE,
    )
    for worker_id in ("worker-a", "worker-b"):
        store.upsert_worker(worker(worker_id))
    seed = TaskRecord(
        id="seed-task",
        project_id="project-1",
        title="Seed resource evidence",
        description="Use safe provider status",
        state=TaskState.DRAFT,
        topology=ExecutionTopology.SINGLE,
        requirements=TaskRequirements(labels=frozenset({TaskLabel.RESEARCH})),
    )
    store.create_task(seed, "#0001")
    for state in (
        TaskState.QUEUED,
        TaskState.READY,
        TaskState.RUNNING,
        TaskState.REVIEWING,
        TaskState.SUCCEEDED,
    ):
        store.transition_task(seed.id, state)
    return store


def test_scheduler_uses_frozen_resource_score_without_weakening_hard_constraints() -> None:
    requirements = TaskRequirements(
        labels=frozenset({TaskLabel.RESEARCH}),
        required_capabilities=frozenset({"analysis"}),
    )
    decision = DeterministicScheduler().schedule(
        task_id="task-resource",
        requirements=requirements,
        topology=ExecutionTopology.SINGLE,
        workers=(worker("worker-a"), worker("worker-b")),
        resource_evidence=(
            evidence("worker-a", "unknown", 0.25),
            evidence("worker-b", "available", 1.0),
        ),
    )
    assert decision.selected_worker_ids == ("worker-b",)
    assert {item.worker_id: item.components["quotaHealth"] for item in decision.candidates} == {
        "worker-b": 1.0,
        "worker-a": 0.25,
    }
    assert decision.explanation["resourceEvidence"][0]["quotaState"] == "unknown"

    busy = DeterministicScheduler().schedule(
        task_id="task-busy",
        requirements=requirements,
        topology=ExecutionTopology.SINGLE,
        workers=(worker("worker-a", state=WorkerState.RUNNING),),
        resource_evidence=(evidence("worker-a", "available", 1.0),),
    )
    assert busy.selected_worker_ids == ()
    assert [item.reason_code for item in busy.rejected] == ["WORKER_UNAVAILABLE"]


def test_stale_or_provider_mismatched_quota_becomes_explicit_unknown(tmp_path: Path) -> None:
    store = setup_store(tmp_path)
    repository = ResourceUsageRepository(store)
    observed = datetime(2026, 8, 10, tzinfo=UTC)
    ResourceUsageService(repository).audit_task_terminal(
        task_id="seed-task",
        run_id=None,
        terminal_state="succeeded",
        observations=(observation("worker-a", QuotaState.CRITICAL, observed, provider="other"),),
        observed_at=observed,
    )
    guard = PreDispatchQuotaGuard(repository)
    mismatch = guard.evaluate(
        (QuotaGuardCandidate("worker-a", "pool-worker-a", premium=True, provider="mock"),),
        as_of=observed,
    )
    assert mismatch.ordered_worker_ids == ("worker-a",)
    assert mismatch.pool_states == {"worker-a": "unknown"}
    assert mismatch.evidence_by_worker["worker-a"].reason == "providerMismatch"

    stale = guard.evaluate(
        (QuotaGuardCandidate("worker-a", "pool-worker-a", premium=True, provider="other"),),
        as_of=observed + timedelta(minutes=2),
    )
    assert stale.pool_states == {"worker-a": "unknown"}
    assert stale.evidence_by_worker["worker-a"].reason == "staleSnapshot"
    assert stale.evidence_by_worker["worker-a"].health_score() == 0.25


async def test_runtime_routes_around_fresh_exhausted_pool_and_persists_evidence(
    tmp_path: Path,
) -> None:
    store = setup_store(tmp_path)
    observed = datetime.now(UTC)
    ResourceUsageService(ResourceUsageRepository(store)).audit_task_terminal(
        task_id="seed-task",
        run_id=None,
        terminal_state="succeeded",
        observations=(
            observation("worker-a", QuotaState.EXHAUSTED, observed),
            observation("worker-b", QuotaState.AVAILABLE, observed),
        ),
        observed_at=observed,
    )
    registry = AdapterRegistry()
    registry.register("worker-a", MockAdapter(MockBehavior(text="A")))
    registry.register("worker-b", MockAdapter(MockBehavior(text="B")))
    runtime = SupervisorRuntime(
        store=store,
        scheduler=DeterministicScheduler(),
        adapters=registry,
        evidence_root=tmp_path / "evidence",
    )
    target = await runtime.submit_task(
        project_id="project-1",
        title="Resource-aware dispatch",
        description="Choose a capable non-exhausted resource",
        requirements=TaskRequirements(
            labels=frozenset({TaskLabel.RESEARCH}),
            required_capabilities=frozenset({"analysis"}),
        ),
        reference="#0002",
    )

    await runtime.run_until_idle()

    assert store.list_worker_runs(target)[0]["worker_id"] == "worker-b"
    routing = store.list_routing_decisions(target)[0]
    explanation = json.loads(routing["explanation_json"])
    assert explanation["selected"] == ["worker-b"]
    assert explanation["quotaGuard"]["avoided"] == {"worker-a": "quotaExhaustedAlternativeExists"}
    assert explanation["quotaGuard"]["evidenceByWorker"]["worker-a"] == {
        "confidence": "providerReported",
        "freshness": "fresh",
        "healthScore": 0.0,
        "observedAt": observed.replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        "provider": "mock",
        "provenance": "PROVIDER_REPORTED",
        "quotaPoolID": "pool-worker-a",
        "quotaState": "exhausted",
        "reason": None,
        "source": "fixtureStatus",
        "workerID": "worker-a",
    }
    assert any(
        rejection["workerID"] == "worker-a" and rejection["reasonCode"] == "QUOTA_GUARD"
        for rejection in explanation["rejected"]
    )
