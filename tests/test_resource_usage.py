from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

import jsonschema
import pytest
from referencing import Registry, Resource

from project_supervisor.adapters.base import Usage, WorkerResult
from project_supervisor.domain import (
    EvidenceConfidence,
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
from project_supervisor.resource_usage import (
    Freshness,
    PreDispatchQuotaGuard,
    QuotaGuardCandidate,
    QuotaState,
    ResourceObservation,
    ResourceUsageRepository,
    ResourceUsageService,
    UsageDimension,
    UsageInstant,
    UsageMetric,
    UsageProvenance,
    infer_quota_state,
)
from project_supervisor.store import StateStore, timestamp
from project_supervisor.telemetry import (
    ExecutorKind,
    InvocationTelemetry,
    InvocationTelemetryRepository,
    TaskRole,
)


def validate_schema(name: str, payload: dict[str, object]) -> None:
    schema_dir = Path(__file__).parents[1] / "schemas"
    schemas = {
        path.name: json.loads(path.read_text(encoding="utf-8"))
        for path in schema_dir.glob("resource-usage-*.schema.json")
    }
    schemas["quota-guard-decision-v1.schema.json"] = json.loads(
        (schema_dir / "quota-guard-decision-v1.schema.json").read_text(encoding="utf-8")
    )
    registry = Registry()
    for schema_name, schema in schemas.items():
        registry = registry.with_resource(schema_name, Resource.from_contents(schema))
        registry = registry.with_resource(schema["$id"], Resource.from_contents(schema))
    jsonschema.Draft202012Validator(
        schemas[name], registry=registry, format_checker=jsonschema.FormatChecker()
    ).validate(payload)


@pytest.fixture
def resource_store(tmp_path: Path) -> tuple[StateStore, str]:
    store = StateStore(tmp_path / "state.db")
    store.create_project(
        project_id="project-1",
        name="Resource ledger",
        root_path=str(tmp_path),
        goal="Observe subscriptions without fabrication",
    )
    store.upsert_node(
        node_id="node-1",
        hostname="fixture",
        display_name="Fixture",
        role="control",
        state=NodeState.ONLINE,
    )
    for worker_id in ("premium-worker", "local-worker", "unknown-worker"):
        store.upsert_worker(
            WorkerSnapshot(
                id=worker_id,
                node_id="node-1",
                harness=Harness.MOCK,
                provider=Provider.MOCK,
                model=ModelDescriptor("fixture-model", "Fixture", Provider.MOCK),
                state=WorkerState.IDLE,
                node_state=NodeState.ONLINE,
                resource_state=ResourceState.AVAILABLE,
                capabilities=frozenset({"analysis"}),
                code_write_allowed=False,
                privacy_allowed=True,
            )
        )
    task = TaskRecord(
        id="task-1",
        project_id="project-1",
        title="Audit",
        description="Audit resources",
        state=TaskState.DRAFT,
        topology=ExecutionTopology.FALLBACK,
        requirements=TaskRequirements(labels=frozenset({TaskLabel.RESEARCH})),
    )
    store.create_task(task, "#0001")
    for state in (TaskState.QUEUED, TaskState.READY, TaskState.RUNNING):
        store.transition_task(task.id, state)
    now = timestamp(datetime(2026, 8, 9, 10, 0, tzinfo=UTC))
    with store.transaction() as connection:
        connection.execute(
            "INSERT INTO autonomous_goals("
            "id,project_id,intent,effective_intent,state,budgets_json,created_at,updated_at"
            ") VALUES (?,?,?,?,?,?,?,?)",
            ("goal-1", "project-1", "audit", "audit", "running", "{}", now, now),
        )
        connection.execute(
            "INSERT INTO autonomous_iterations("
            "id,goal_id,sequence,state,started_at,updated_at"
            ") VALUES (?,?,?,?,?,?)",
            ("iteration-1", "goal-1", 1, "collecting", now, now),
        )
        connection.execute(
            "INSERT INTO autonomous_actions("
            "id,goal_id,iteration_id,ordinal,action_key,title,description,role,payload_json,state,"
            "task_id,created_at,updated_at"
            ") VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                "action-1",
                "goal-1",
                "iteration-1",
                0,
                "audit",
                "Audit",
                "Audit",
                "primary",
                "{}",
                "running",
                "task-1",
                now,
                now,
            ),
        )
    run_id = store.create_worker_run(task_id="task-1", worker_id="premium-worker", attempt=1)
    store.transition_worker_run(run_id, RunState.RUNNING)
    return store, run_id


def observation(
    *,
    pool: str,
    worker: str,
    state: QuotaState,
    remaining: float | None,
    observed_at: datetime,
    fresh_for: int = 60,
    reset_at: datetime | None = None,
    reset_observed: bool = False,
) -> ResourceObservation:
    remaining_metric = (
        UsageMetric.known(remaining, "requests", UsageProvenance.PROVIDER_REPORTED)
        if remaining is not None
        else UsageMetric.unknown()
    )
    return ResourceObservation(
        provider="mock",
        quota_pool_id=pool,
        account_scope=UsageDimension.known("subscription", UsageProvenance.PROVIDER_REPORTED),
        plan_tier=UsageDimension.known("fixture", UsageProvenance.PROVIDER_REPORTED),
        worker_id=worker,
        model=UsageDimension.known("fixture-model", UsageProvenance.PROVIDER_REPORTED),
        quota_window=UsageDimension.known("daily", UsageProvenance.PROVIDER_REPORTED),
        used=UsageMetric.known(
            100 - (remaining or 0), "requests", UsageProvenance.PROVIDER_REPORTED
        ),
        remaining=remaining_metric,
        reset_at=(
            UsageInstant.known(reset_at, UsageProvenance.PROVIDER_REPORTED)
            if reset_at is not None
            else UsageInstant.unknown()
        ),
        task_calls=UsageMetric.known(1, "calls", UsageProvenance.LOCALLY_MEASURED),
        input_tokens=UsageMetric.known(3, "tokens", UsageProvenance.PROVIDER_REPORTED),
        output_tokens=UsageMetric.known(2, "tokens", UsageProvenance.PROVIDER_REPORTED),
        cost=UsageMetric.unknown(),
        quota_state=state,
        quota_state_provenance=UsageProvenance.PROVIDER_REPORTED,
        source="safeProviderStatus",
        confidence=EvidenceConfidence.PROVIDER_REPORTED,
        observed_at=observed_at,
        fresh_until=observed_at + timedelta(seconds=fresh_for),
        reset_observed=reset_observed,
    )


def test_values_require_explicit_provenance_and_unknown_is_not_zero() -> None:
    unknown = UsageMetric.unknown()
    assert unknown.value is None
    assert unknown.to_protocol() == {
        "state": "unavailable",
        "reason": "notReported",
        "provenance": "UNKNOWN",
        "unit": None,
    }
    with pytest.raises(ValueError, match="non-UNKNOWN"):
        UsageMetric.known(0, "tokens", UsageProvenance.UNKNOWN)
    with pytest.raises(ValueError, match="UNKNOWN provenance"):
        UsageMetric(None, None, UsageProvenance.INFERRED, None)

    assert infer_quota_state(used=unknown, remaining=unknown) is QuotaState.UNKNOWN
    assert (
        infer_quota_state(
            used=UsageMetric.known(100, "requests", UsageProvenance.PROVIDER_REPORTED),
            remaining=UsageMetric.known(0, "requests", UsageProvenance.PROVIDER_REPORTED),
        )
        is QuotaState.EXHAUSTED
    )


def test_post_task_audit_persists_schema_events_reset_and_restart(
    resource_store: tuple[StateStore, str],
) -> None:
    store, run_id = resource_store
    repository = ResourceUsageRepository(store)
    service = ResourceUsageService(repository)
    first_time = datetime(2026, 8, 9, 10, 0, tzinfo=UTC)
    first_reset = first_time + timedelta(minutes=5)
    first = service.audit_task_terminal(
        task_id="task-1",
        run_id=run_id,
        terminal_state="succeeded",
        audit_key="terminal-1",
        observations=(
            observation(
                pool="premium-pool",
                worker="premium-worker",
                state=QuotaState.WARNING,
                remaining=15,
                observed_at=first_time,
                reset_at=first_reset,
            ),
        ),
        observed_at=first_time,
    )
    assert first.goal_id == "goal-1"
    assert first.status == "completed"
    assert len(first.snapshots) == 1
    payload = first.snapshots[0].to_protocol(as_of=first_time)
    validate_schema("resource-usage-snapshot-v1.schema.json", payload)
    assert payload["freshness"] == "fresh"
    assert payload["remaining"]["provenance"] == "PROVIDER_REPORTED"
    assert payload["taskLocal"]["cost"]["state"] == "unavailable"

    second_time = first_reset + timedelta(seconds=1)
    second_reset = first_reset + timedelta(days=1)
    service.audit_task_terminal(
        task_id="task-1",
        run_id=run_id,
        terminal_state="succeeded",
        audit_key="terminal-2",
        observations=(
            observation(
                pool="premium-pool",
                worker="premium-worker",
                state=QuotaState.AVAILABLE,
                remaining=100,
                observed_at=second_time,
                reset_at=second_reset,
            ),
        ),
        observed_at=second_time,
    )
    event_kinds = [event["kind"] for event in store.list_events(limit=1000)]
    assert "quota_snapshot" in event_kinds
    assert "usage_delta" in event_kinds
    assert "quota_warning" in event_kinds
    assert "usage_unknown" in event_kinds
    assert "quota_reset_observed" in event_kinds

    reopened = ResourceUsageRepository(StateStore(store.path))
    restored = reopened.audit_by_key("terminal-2")
    assert restored is not None
    assert restored.snapshots[0].observation.remaining.value == 100
    assert reopened.audit_by_key("terminal-1") is not None
    assert (
        service.audit_task_terminal(
            task_id="task-1",
            run_id=run_id,
            terminal_state="succeeded",
            audit_key="terminal-1",
            observations=(),
        ).id
        == first.id
    )


def test_local_invocation_is_reused_without_new_inference_and_unsafe_observer_is_skipped(
    resource_store: tuple[StateStore, str],
) -> None:
    store, run_id = resource_store
    ended = datetime(2026, 8, 9, 10, 0, 3, tzinfo=UTC)
    result = WorkerResult(
        run_id=run_id,
        state=RunState.COMPLETED,
        pid=123,
        exit_code=0,
        started_at=ended - timedelta(seconds=3),
        ended_at=ended,
        stdout="",
        stderr="",
        final_text="done",
        events=(),
        model="actual-model",
        usage=Usage(input_tokens=5, output_tokens=7, cost_usd=None),
    )
    InvocationTelemetryRepository(store).record(
        InvocationTelemetry.from_worker_result(
            telemetry_id="invocation-1",
            goal_id="goal-1",
            task_id="task-1",
            worker_id="premium-worker",
            provider="mock",
            node_id="node-1",
            task_role=TaskRole.PRIMARY,
            executor_kind=ExecutorKind.OFFLOADED,
            result=result,
        )
    )

    class UnsafeObserver:
        name = "wouldSpendQuota"
        requires_inference = True
        called = False

        def observe(self, context: object) -> tuple[ResourceObservation, ...]:
            self.called = True
            raise AssertionError("must not be called")

    unsafe = UnsafeObserver()
    audit = ResourceUsageService(
        ResourceUsageRepository(store), observers=(unsafe,)
    ).audit_task_terminal(task_id="task-1", run_id=run_id, terminal_state="succeeded")
    assert unsafe.called is False
    assert audit.status == "completedWithErrors"
    assert audit.errors == ("wouldSpendQuota:skippedInferenceRequired",)
    local = audit.snapshots[0].observation
    assert local.source == "invocationTelemetry"
    assert local.task_calls.provenance is UsageProvenance.LOCALLY_MEASURED
    assert local.input_tokens.value == 5
    assert local.input_tokens.provenance is UsageProvenance.PROVIDER_REPORTED
    assert local.remaining.value is None
    assert local.quota_state is QuotaState.UNKNOWN


def test_aggregation_supports_resource_dimensions_and_preserves_unknown(
    resource_store: tuple[StateStore, str],
) -> None:
    store, run_id = resource_store
    repository = ResourceUsageRepository(store)
    now = datetime(2026, 8, 9, 10, 0, tzinfo=UTC)
    ResourceUsageService(repository).audit_task_terminal(
        task_id="task-1",
        run_id=run_id,
        terminal_state="failed",
        observations=(
            observation(
                pool="premium-pool",
                worker="premium-worker",
                state=QuotaState.AVAILABLE,
                remaining=80,
                observed_at=now,
            ),
            ResourceObservation(
                provider="mock",
                quota_pool_id="unknown-pool",
                worker_id="unknown-worker",
                source="cachedStatus",
                observed_at=now,
            ),
        ),
        observed_at=now,
    )
    aggregate = repository.aggregate(goal_id="goal-1", provider="mock")
    payload = aggregate.to_protocol(as_of=now)
    validate_schema("resource-usage-aggregate-v1.schema.json", payload)
    assert payload["auditCount"] == 1
    assert payload["snapshotCount"] == 2
    assert payload["snapshotsByQuotaPool"] == {"premium-pool": 1, "unknown-pool": 1}
    assert payload["inputTokens"] == {
        "valuesByUnit": {"tokens": 3},
        "knownCount": 1,
        "unknownCount": 1,
    }
    assert payload["cost"]["knownCount"] == 0
    assert payload["cost"]["unknownCount"] == 2


def test_quota_guard_avoids_exhausted_and_scarce_premium_but_not_unknown(
    resource_store: tuple[StateStore, str],
) -> None:
    store, run_id = resource_store
    repository = ResourceUsageRepository(store)
    now = datetime(2026, 8, 9, 10, 0, tzinfo=UTC)
    ResourceUsageService(repository).audit_task_terminal(
        task_id="task-1",
        run_id=run_id,
        terminal_state="succeeded",
        observations=(
            observation(
                pool="premium-pool",
                worker="premium-worker",
                state=QuotaState.CRITICAL,
                remaining=4,
                observed_at=now,
            ),
            observation(
                pool="local-pool",
                worker="local-worker",
                state=QuotaState.AVAILABLE,
                remaining=90,
                observed_at=now,
            ),
        ),
        observed_at=now,
    )
    decision = PreDispatchQuotaGuard(repository).evaluate(
        (
            QuotaGuardCandidate("premium-worker", "premium-pool", premium=True),
            QuotaGuardCandidate("local-worker", "local-pool"),
            QuotaGuardCandidate("unknown-worker", "missing-pool"),
        ),
        as_of=now,
    )
    validate_schema("quota-guard-decision-v1.schema.json", decision.to_protocol())
    assert decision.ordered_worker_ids == ("local-worker", "unknown-worker")
    assert decision.avoided == {"premium-worker": "premiumQuotaCriticalAlternativeExists"}
    assert decision.pool_states["unknown-worker"] == "unknown"

    stale = PreDispatchQuotaGuard(repository).evaluate(
        (QuotaGuardCandidate("local-worker", "local-pool"),),
        as_of=now + timedelta(minutes=2),
    )
    assert stale.pool_states == {"local-worker": "unknown"}
    assert stale.dispatch_allowed


def test_quota_guard_blocks_when_all_capable_pools_are_observably_exhausted(
    resource_store: tuple[StateStore, str],
) -> None:
    store, run_id = resource_store
    repository = ResourceUsageRepository(store)
    now = datetime(2026, 8, 9, 10, 0, tzinfo=UTC)
    ResourceUsageService(repository).audit_task_terminal(
        task_id="task-1",
        run_id=run_id,
        terminal_state="failed",
        observations=(
            observation(
                pool="premium-pool",
                worker="premium-worker",
                state=QuotaState.EXHAUSTED,
                remaining=0,
                observed_at=now,
            ),
            observation(
                pool="local-pool",
                worker="local-worker",
                state=QuotaState.EXHAUSTED,
                remaining=0,
                observed_at=now,
            ),
        ),
        observed_at=now,
    )
    decision = PreDispatchQuotaGuard(repository).evaluate(
        (
            QuotaGuardCandidate("premium-worker", "premium-pool", premium=True),
            QuotaGuardCandidate("local-worker", "local-pool"),
        ),
        as_of=now,
    )
    assert not decision.dispatch_allowed
    assert set(decision.avoided.values()) == {"allCapableQuotaPoolsExhausted"}


def test_unknown_freshness_is_not_treated_as_fresh(
    resource_store: tuple[StateStore, str],
) -> None:
    store, run_id = resource_store
    repository = ResourceUsageRepository(store)
    now = datetime(2026, 8, 9, 10, 0, tzinfo=UTC)
    audit = ResourceUsageService(repository).audit_task_terminal(
        task_id="task-1", run_id=run_id, terminal_state="cancelled", observed_at=now
    )
    assert audit.snapshots[0].freshness(now) is Freshness.UNKNOWN
