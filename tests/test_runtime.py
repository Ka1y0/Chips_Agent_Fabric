from __future__ import annotations

import asyncio
import json
import threading
from datetime import UTC, datetime

import pytest

from project_supervisor.adapters import (
    EventSink,
    MockAdapter,
    MockBehavior,
    WorkerRequest,
    WorkerResult,
)
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
    TaskState,
    WorkerSnapshot,
    WorkerState,
)
from project_supervisor.fabric.capabilities import WorkerManifest
from project_supervisor.fabric.persistence import CapabilityRegistryRepository
from project_supervisor.runtime import AdapterRegistry, SupervisorRuntime
from project_supervisor.scheduler import DeterministicScheduler
from project_supervisor.store import StateStore
from project_supervisor.verification import (
    DefinitionOfDoneResult,
    VerificationPolicyError,
    VerificationResult,
)


class CountingMockAdapter(MockAdapter):
    def __init__(self, behavior: MockBehavior | None = None) -> None:
        super().__init__(behavior)
        self.execute_calls = 0

    async def execute(
        self,
        request: WorkerRequest,
        *,
        event_sink: EventSink | None = None,
    ) -> WorkerResult:
        self.execute_calls += 1
        return await super().execute(request, event_sink=event_sink)


class GatedMockAdapter(CountingMockAdapter):
    def __init__(self) -> None:
        super().__init__()
        self.started = asyncio.Event()
        self.release = asyncio.Event()

    async def execute(
        self,
        request: WorkerRequest,
        *,
        event_sink: EventSink | None = None,
    ) -> WorkerResult:
        self.started.set()
        await self.release.wait()
        return await super().execute(request, event_sink=event_sink)


class ConcurrentGatedMockAdapter(CountingMockAdapter):
    def __init__(self, expected: int) -> None:
        super().__init__()
        self.expected = expected
        self.started_count = 0
        self.all_started = asyncio.Event()
        self.release = asyncio.Event()

    async def execute(
        self,
        request: WorkerRequest,
        *,
        event_sink: EventSink | None = None,
    ) -> WorkerResult:
        self.started_count += 1
        if self.started_count >= self.expected:
            self.all_started.set()
        await self.release.wait()
        return await super().execute(request, event_sink=event_sink)


def register_worker(
    store: StateStore,
    registry: AdapterRegistry,
    worker_id: str,
    adapter: MockAdapter,
    *,
    quality: float = 0.8,
) -> None:
    store.upsert_worker(
        WorkerSnapshot(
            id=worker_id,
            node_id="node-1",
            harness=Harness.MOCK,
            provider=Provider.MOCK,
            model=ModelDescriptor("declared-model", "Declared Model", Provider.MOCK),
            state=WorkerState.IDLE,
            node_state=NodeState.ONLINE,
            resource_state=ResourceState.AVAILABLE,
            capabilities=frozenset({"analysis", "review"}),
            code_write_allowed=False,
            privacy_allowed=True,
            quality_score=quality,
            reliability_score=0.9,
            expected_latency_seconds=0.01,
            monetary_cost_score=1.0,
        )
    )
    registry.register(worker_id, adapter)


def runtime_fixture(tmp_path, *, max_attempts: int = 2):
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    store = StateStore(tmp_path / "state.db")
    store.create_project(
        project_id="project-1",
        name="Runtime fixture",
        root_path=str(workspace),
        goal="Exercise the event-driven runtime",
    )
    store.upsert_node(
        node_id="node-1",
        hostname="fixture",
        display_name="Fixture Node",
        role="control",
        state=NodeState.ONLINE,
    )
    registry = AdapterRegistry()
    runtime = SupervisorRuntime(
        store=store,
        scheduler=DeterministicScheduler(),
        adapters=registry,
        evidence_root=tmp_path / "evidence",
        max_attempts=max_attempts,
    )
    return store, registry, runtime


async def test_runtime_persists_route_run_session_usage_events_and_evidence(tmp_path) -> None:
    store, registry, runtime = runtime_fixture(tmp_path)
    register_worker(
        store,
        registry,
        "worker-1",
        MockAdapter(MockBehavior(text="ANALYSIS_COMPLETE", model="observed-model")),
    )
    task_id = await runtime.submit_task(
        project_id="project-1",
        title="Analyze fixture",
        description="Return a concise architecture analysis",
        requirements=TaskRequirements(
            labels=frozenset({TaskLabel.RESEARCH}),
            required_capabilities=frozenset({"analysis"}),
        ),
    )

    await runtime.run_until_idle()

    task = store.get_task(task_id)
    assert task["state"] == TaskState.REVIEWING.value
    assert len(store.list_routing_decisions(task_id)) == 1
    run = store.list_worker_runs(task_id)[0]
    assert run["state"] == "completed"
    assert run["session_id"] is not None
    assert (tmp_path / "evidence" / f"{run['id']}.json").is_file()
    with store.connect() as connection:
        usage = connection.execute(
            "SELECT metric,value,unavailable_reason FROM usage_records WHERE run_id=?",
            (run["id"],),
        ).fetchall()
        result = connection.execute(
            "SELECT summary FROM worker_results WHERE run_id=?", (run["id"],)
        ).fetchone()
    assert result["summary"] == "ANALYSIS_COMPLETE"
    assert {row["metric"] for row in usage} >= {"inputTokens", "outputTokens", "costUSD"}
    assert next(row for row in usage if row["metric"] == "costUSD")["value"] == 0
    assert (
        next(row for row in usage if row["metric"] == "reasoningTokens")["unavailable_reason"]
        == "notReported"
    )
    assert store.list_workers()[0]["model_identifier"] == "observed-model"
    event_kinds = {event["kind"] for event in store.list_events(limit=200)}
    assert {
        "routingDecisionRecorded",
        "workerAdapterEvent",
        "workerCompleted",
        "taskStateChanged",
    } <= event_kinds


async def test_fallback_replans_away_from_failed_worker(tmp_path) -> None:
    store, registry, runtime = runtime_fixture(tmp_path, max_attempts=2)
    register_worker(
        store,
        registry,
        "primary",
        MockAdapter(MockBehavior(text="", exit_code=9)),
        quality=0.9,
    )
    register_worker(
        store,
        registry,
        "fallback",
        MockAdapter(MockBehavior(text="FALLBACK_OK")),
        quality=0.7,
    )
    task_id = await runtime.submit_task(
        project_id="project-1",
        title="Fallback analysis",
        description="Analyze using a fallback when needed",
        topology=ExecutionTopology.FALLBACK,
        requirements=TaskRequirements(
            labels=frozenset({TaskLabel.RESEARCH}),
            required_capabilities=frozenset({"analysis"}),
            preferred_workers=("primary", "fallback"),
        ),
    )

    await runtime.run_until_idle()

    assert store.get_task(task_id)["state"] == TaskState.REVIEWING.value
    runs = store.list_worker_runs(task_id)
    assert [(run["worker_id"], run["state"]) for run in runs] == [
        ("primary", "failed"),
        ("fallback", "completed"),
    ]
    assert len(store.list_routing_decisions(task_id)) == 2
    with store.connect() as connection:
        failures = connection.execute(
            "SELECT classification,retryable FROM failures WHERE task_id=?", (task_id,)
        ).fetchall()
    assert [(row["classification"], row["retryable"]) for row in failures] == [("transient", 1)]


async def test_definition_of_done_is_persisted_before_success(tmp_path) -> None:
    store, registry, runtime = runtime_fixture(tmp_path)
    register_worker(store, registry, "worker-1", MockAdapter())
    criterion_id = store.add_acceptance_criterion(
        project_id="project-1",
        criterion_id="criterion-1",
        kind="fileExists",
        description="Required artifact exists",
    )
    task_id = await runtime.submit_task(
        project_id="project-1",
        title="Verified analysis",
        description="Produce a verifiable result",
        requirements=TaskRequirements(
            labels=frozenset({TaskLabel.RESEARCH}),
            required_capabilities=frozenset({"analysis"}),
        ),
    )
    await runtime.run_until_idle()

    state = await runtime.apply_verification(
        task_id,
        DefinitionOfDoneResult(
            complete=True,
            results=(
                VerificationResult(
                    criterion_id,
                    True,
                    "artifact exists",
                    evidence={"path": "artifact.json", "secret": "token=do-not-store"},
                ),
            ),
            required_failures=(),
        ),
    )

    assert state is TaskState.SUCCEEDED
    verification = store.list_verifications(task_id)[0]
    assert "do-not-store" not in verification["evidence_json"]
    assert json.loads(verification["evidence_json"])["secret"] == "[REDACTED]"
    assert store.list_acceptance_criteria("project-1")[0]["state"] == "passed"


async def test_verification_missing_stored_criterion_cannot_succeed(tmp_path) -> None:
    store, registry, runtime = runtime_fixture(tmp_path)
    register_worker(store, registry, "worker-1", MockAdapter())
    store.add_acceptance_criterion(
        project_id="project-1",
        criterion_id="criterion-1",
        kind="fileExists",
        description="First required artifact exists",
    )
    store.add_acceptance_criterion(
        project_id="project-1",
        criterion_id="criterion-2",
        kind="fileExists",
        description="Second required artifact exists",
    )
    task_id = await runtime.submit_task(
        project_id="project-1",
        title="Incomplete verification",
        description="Report only one of two required checks",
        requirements=TaskRequirements(
            labels=frozenset({TaskLabel.RESEARCH}),
            required_capabilities=frozenset({"analysis"}),
        ),
    )
    await runtime.run_until_idle()

    state = await runtime.apply_verification(
        task_id,
        DefinitionOfDoneResult(
            complete=True,
            results=(VerificationResult("criterion-1", True, "first artifact exists"),),
            required_failures=(),
        ),
    )

    assert state is TaskState.READY
    assert store.get_task(task_id)["state"] == TaskState.READY.value
    verifications = {
        verification["criterion_id"]: verification
        for verification in store.list_verifications(task_id)
    }
    assert verifications["criterion-1"]["passed"] == 1
    assert verifications["criterion-2"]["passed"] == 0
    assert json.loads(verifications["criterion-2"]["evidence_json"])["reasonCode"] == (
        "VERIFICATION_RESULT_MISSING"
    )


async def test_verification_rejects_unknown_and_duplicate_ids_and_incomplete_result(
    tmp_path,
) -> None:
    store, registry, runtime = runtime_fixture(tmp_path)
    register_worker(store, registry, "worker-1", MockAdapter())
    store.add_acceptance_criterion(
        project_id="project-1",
        criterion_id="criterion-1",
        kind="fileExists",
        description="Required artifact exists",
    )
    task_id = await runtime.submit_task(
        project_id="project-1",
        title="Fail-closed verification",
        description="Reject malformed or incomplete verification",
        requirements=TaskRequirements(
            labels=frozenset({TaskLabel.RESEARCH}),
            required_capabilities=frozenset({"analysis"}),
        ),
    )
    await runtime.run_until_idle()

    with pytest.raises(VerificationPolicyError, match="unknown criteria"):
        await runtime.apply_verification(
            task_id,
            DefinitionOfDoneResult(
                complete=True,
                results=(VerificationResult("unknown", True, "not canonical"),),
                required_failures=(),
            ),
        )
    with pytest.raises(VerificationPolicyError, match="duplicate criterion IDs"):
        await runtime.apply_verification(
            task_id,
            DefinitionOfDoneResult(
                complete=True,
                results=(
                    VerificationResult("criterion-1", True, "first copy"),
                    VerificationResult("criterion-1", True, "duplicate copy"),
                ),
                required_failures=(),
            ),
        )
    assert store.get_task(task_id)["state"] == TaskState.REVIEWING.value
    assert store.list_verifications(task_id) == []

    state = await runtime.apply_verification(
        task_id,
        DefinitionOfDoneResult(
            complete=False,
            results=(VerificationResult("criterion-1", True, "artifact exists"),),
            required_failures=(),
        ),
    )

    assert state is TaskState.READY
    assert store.get_task(task_id)["state"] == TaskState.READY.value


async def test_verification_failure_cannot_exceed_execution_attempt_limit(tmp_path) -> None:
    store, registry, runtime = runtime_fixture(tmp_path, max_attempts=1)
    register_worker(store, registry, "worker-1", MockAdapter())
    store.add_acceptance_criterion(
        project_id="project-1",
        criterion_id="criterion-1",
        kind="fileExists",
        description="Required artifact exists",
    )
    task_id = await runtime.submit_task(
        project_id="project-1",
        title="Bounded verification retry",
        description="A failed verifier must not create attempt two",
        requirements=TaskRequirements(
            labels=frozenset({TaskLabel.RESEARCH}),
            required_capabilities=frozenset({"analysis"}),
        ),
    )
    await runtime.run_until_idle()

    state = await runtime.apply_verification(
        task_id,
        DefinitionOfDoneResult(
            complete=False,
            results=(VerificationResult("criterion-1", False, "artifact missing"),),
            required_failures=("criterion-1",),
        ),
    )
    await runtime.run_until_idle()

    assert state is TaskState.FAILED
    task = store.get_task(task_id)
    assert task["state"] == TaskState.FAILED.value
    assert task["attempt_count"] == 1
    assert [run["attempt"] for run in store.list_worker_runs(task_id)] == [1]


async def test_concurrent_pass_and_fail_verification_persist_exactly_one_decision(
    tmp_path,
) -> None:
    store, registry, runtime = runtime_fixture(tmp_path)
    register_worker(store, registry, "worker-1", MockAdapter())
    criterion_id = store.add_acceptance_criterion(
        project_id="project-1",
        criterion_id="criterion-1",
        kind="fileExists",
        description="Required artifact exists",
    )
    task_id = await runtime.submit_task(
        project_id="project-1",
        title="Race two verification decisions",
        description="Only one canonical verifier decision may commit",
        requirements=TaskRequirements(
            labels=frozenset({TaskLabel.RESEARCH}),
            required_capabilities=frozenset({"analysis"}),
        ),
    )
    await runtime.run_until_idle()
    peer_store = StateStore(store.path)
    peer_runtime = SupervisorRuntime(
        store=peer_store,
        scheduler=DeterministicScheduler(),
        adapters=AdapterRegistry(),
        evidence_root=tmp_path / "peer-evidence",
        runtime_id="peer-verifier",
    )
    passing = DefinitionOfDoneResult(
        complete=True,
        results=(VerificationResult(criterion_id, True, "artifact exists"),),
        required_failures=(),
    )
    failing = DefinitionOfDoneResult(
        complete=True,
        results=(VerificationResult(criterion_id, False, "artifact missing"),),
        required_failures=(criterion_id,),
    )

    outcomes = await asyncio.gather(
        runtime.apply_verification(task_id, passing),
        peer_runtime.apply_verification(task_id, failing),
        return_exceptions=True,
    )

    committed = [outcome for outcome in outcomes if isinstance(outcome, TaskState)]
    rejected = [outcome for outcome in outcomes if isinstance(outcome, RuntimeError)]
    assert len(committed) == 1
    assert len(rejected) == 1
    verifications = store.list_verifications(task_id)
    assert len(verifications) == 1
    task_state = TaskState(store.get_task(task_id)["state"])
    assert task_state is committed[0]
    assert bool(verifications[0]["passed"]) is (task_state is TaskState.SUCCEEDED)
    criterion = store.list_acceptance_criteria("project-1")[0]
    assert criterion["state"] == ("passed" if task_state is TaskState.SUCCEEDED else "failed")


async def test_dependencies_wait_and_red_tasks_create_durable_approval(tmp_path) -> None:
    store, registry, runtime = runtime_fixture(tmp_path)
    register_worker(store, registry, "worker-1", MockAdapter())
    requirements = TaskRequirements(
        labels=frozenset({TaskLabel.RESEARCH}),
        required_capabilities=frozenset({"analysis"}),
    )
    prerequisite = await runtime.submit_task(
        project_id="project-1",
        task_id="prerequisite",
        title="Prerequisite",
        description="First task",
        requirements=requirements,
    )
    dependent = await runtime.submit_task(
        project_id="project-1",
        task_id="dependent",
        title="Dependent",
        description="Second task",
        requirements=requirements,
    )
    store.add_task_dependency(dependent, prerequisite)
    summary = await runtime.dispatch_ready()
    assert prerequisite in summary.launched_task_ids
    assert dependent not in summary.launched_task_ids
    await runtime.wait_for_active()
    store.transition_task(
        prerequisite,
        TaskState.SUCCEEDED,
        actor="test-verifier",
        summary="Prerequisite verification passed",
    )
    released = await runtime.dispatch_ready()
    assert released.launched_task_ids == (dependent,)
    await runtime.wait_for_active()

    red = await runtime.submit_task(
        project_id="project-1",
        title="Needs approval",
        description="A guarded operation",
        requirements=TaskRequirements(
            labels=frozenset({TaskLabel.RESEARCH}),
            permission_class=PermissionClass.RED,
            approval_state=ApprovalState.PENDING,
        ),
    )
    assert store.get_task(red)["state"] == TaskState.BLOCKED.value
    with store.connect() as connection:
        approval = connection.execute(
            "SELECT state,task_id FROM approvals WHERE task_id=?", (red,)
        ).fetchone()
    assert (approval["state"], approval["task_id"]) == ("pending", red)


async def test_failed_prerequisite_blocks_dependent_without_dispatch(tmp_path) -> None:
    store, _registry, runtime = runtime_fixture(tmp_path)
    requirements = TaskRequirements(
        labels=frozenset({TaskLabel.RESEARCH}),
        required_capabilities=frozenset({"analysis"}),
    )
    prerequisite = await runtime.submit_task(
        project_id="project-1",
        task_id="prerequisite",
        title="Failed prerequisite",
        description="This task fails before its dependent can start",
        requirements=requirements,
    )
    dependent = await runtime.submit_task(
        project_id="project-1",
        task_id="dependent",
        title="Dependent",
        description="Must not run after prerequisite failure",
        requirements=requirements,
    )
    store.add_task_dependency(dependent, prerequisite)
    store.transition_task(prerequisite, TaskState.FAILED)

    summary = await runtime.dispatch_ready()

    assert summary.launched_task_ids == ()
    assert summary.blocked_task_ids == (dependent,)
    assert store.get_task(dependent)["state"] == TaskState.BLOCKED.value
    assert store.list_worker_runs(dependent) == []
    event = store.list_events(task_id=dependent)[-1]
    assert event["kind"] == "taskStateChanged"
    assert event["payload"]["reasonCode"] == "DEPENDENCY_TERMINAL_FAILURE"
    assert event["payload"]["dependencies"] == [
        {"taskID": prerequisite, "state": TaskState.FAILED.value}
    ]


async def test_independent_tasks_launch_in_parallel_when_workers_are_available(tmp_path) -> None:
    store, registry, runtime = runtime_fixture(tmp_path)
    register_worker(
        store,
        registry,
        "worker-1",
        MockAdapter(MockBehavior(delay_seconds=0.05)),
    )
    register_worker(
        store,
        registry,
        "worker-2",
        MockAdapter(MockBehavior(delay_seconds=0.05)),
    )
    requirements = TaskRequirements(
        labels=frozenset({TaskLabel.RESEARCH}),
        required_capabilities=frozenset({"analysis"}),
    )
    first = await runtime.submit_task(
        project_id="project-1",
        title="Independent one",
        description="Run independently",
        requirements=requirements,
    )
    second = await runtime.submit_task(
        project_id="project-1",
        title="Independent two",
        description="Run independently",
        requirements=requirements,
    )

    summary = await runtime.dispatch_ready()

    assert set(summary.launched_task_ids) == {first, second}
    assert len(runtime._active) == 2
    await runtime.wait_for_active()
    assert {store.get_task(first)["state"], store.get_task(second)["state"]} == {
        TaskState.REVIEWING.value
    }


async def test_dispatch_prefers_higher_priority_when_capacity_is_bounded(tmp_path) -> None:
    store, registry, runtime = runtime_fixture(tmp_path)
    register_worker(
        store,
        registry,
        "worker-1",
        MockAdapter(MockBehavior(delay_seconds=0.05)),
    )
    requirements = TaskRequirements(
        labels=frozenset({TaskLabel.RESEARCH}),
        required_capabilities=frozenset({"analysis"}),
    )
    low = await runtime.submit_task(
        project_id="project-1",
        title="Low priority",
        description="Wait for higher priority work",
        requirements=requirements,
        priority=10,
    )
    high = await runtime.submit_task(
        project_id="project-1",
        title="High priority",
        description="Use the bounded Worker first",
        requirements=requirements,
        priority=90,
    )

    summary = await runtime.dispatch_ready()

    assert summary.launched_task_ids == (high,)
    assert low in summary.deferred_task_ids
    await runtime.wait_for_active()


async def test_versioned_worker_claims_declared_capacity_and_expiry_is_fenced(tmp_path) -> None:
    store, registry, runtime = runtime_fixture(tmp_path)
    adapter = ConcurrentGatedMockAdapter(expected=2)
    register_worker(store, registry, "worker-versioned", adapter)
    capability_registry = CapabilityRegistryRepository(store)
    expired_manifest = WorkerManifest(
        worker_id="worker-versioned",
        node_id="node-1",
        provider_id="mock",
        adapter_kind="mock",
        capabilities=("analysis", "review"),
        max_concurrency=2,
        manifest_revision=1,
    )
    capability_registry.register_manifest(
        expired_manifest,
        observed_at=datetime(1999, 1, 1, tzinfo=UTC),
        valid_until=datetime(2000, 1, 1, tzinfo=UTC),
        expected_head_generation=0,
    )
    requirements = TaskRequirements(
        labels=frozenset({TaskLabel.RESEARCH}),
        required_capabilities=frozenset({"analysis"}),
    )
    first = await runtime.submit_task(
        project_id="project-1",
        task_id="capacity-task-1",
        title="Capacity one",
        description="Use the first declared slot",
        requirements=requirements,
        priority=90,
    )
    expired_snapshot = store.worker_snapshots()[0]
    assert expired_snapshot.manifest_valid_until == datetime(2000, 1, 1, tzinfo=UTC)
    assert (
        store.claim_task_dispatch(
            first,
            ["worker-versioned"],
            expected_version=store.get_task(first)["version"],
            expected_manifest_digests={"worker-versioned": expired_manifest.digest},
        )
        is None
    )
    assert store.list_worker_runs(first) == []

    current_manifest = WorkerManifest(
        worker_id="worker-versioned",
        node_id="node-1",
        provider_id="mock",
        adapter_kind="mock",
        capabilities=("analysis", "review"),
        max_concurrency=2,
        manifest_revision=2,
    )
    capability_registry.register_manifest(
        current_manifest,
        observed_at=datetime(2026, 1, 1, tzinfo=UTC),
        valid_until=datetime(2100, 1, 1, tzinfo=UTC),
        expected_head_generation=1,
    )
    second = await runtime.submit_task(
        project_id="project-1",
        task_id="capacity-task-2",
        title="Capacity two",
        description="Use the second declared slot",
        requirements=requirements,
        priority=80,
    )
    third = await runtime.submit_task(
        project_id="project-1",
        task_id="capacity-task-3",
        title="Capacity three",
        description="Wait until a declared slot is free",
        requirements=requirements,
        priority=70,
    )

    summary = await runtime.dispatch_ready(task_ids={first, second, third})
    await asyncio.wait_for(adapter.all_started.wait(), timeout=1)

    assert summary.launched_task_ids == (first, second)
    assert summary.deferred_task_ids == (third,)
    assert store.list_worker_runs(third) == []
    snapshot = store.worker_snapshots()[0]
    assert snapshot.state is WorkerState.RUNNING
    assert snapshot.running_tasks == 2
    assert snapshot.max_concurrency == 2
    assert snapshot.manifest_valid_until == datetime(2100, 1, 1, tzinfo=UTC)
    duplicate = await runtime.dispatch_ready(task_ids={first})
    assert duplicate.launched_task_ids == ()
    assert len(store.list_worker_runs(first)) == 1

    adapter.release.set()
    await runtime.wait_for_active(task_ids={first, second})
    assert store.list_workers()[0]["state"] == WorkerState.IDLE.value

    final = await runtime.dispatch_ready(task_ids={third})
    assert final.launched_task_ids == (third,)
    await runtime.wait_for_active(task_ids={third})
    assert adapter.execute_calls == 3
    assert {store.get_task(task_id)["state"] for task_id in (first, second, third)} == {
        TaskState.REVIEWING.value
    }


async def test_versioned_worker_capacity_is_atomic_across_store_instances(tmp_path) -> None:
    store, registry, runtime = runtime_fixture(tmp_path)
    register_worker(store, registry, "worker-versioned", MockAdapter())
    manifest = WorkerManifest(
        worker_id="worker-versioned",
        node_id="node-1",
        provider_id="mock",
        adapter_kind="mock",
        capabilities=("analysis", "review"),
        max_concurrency=2,
    )
    CapabilityRegistryRepository(store).register_manifest(
        manifest,
        valid_until=datetime(2100, 1, 1, tzinfo=UTC),
    )
    requirements = TaskRequirements(
        labels=frozenset({TaskLabel.RESEARCH}),
        required_capabilities=frozenset({"analysis"}),
    )
    task_ids = tuple(
        [
            await runtime.submit_task(
                project_id="project-1",
                task_id=f"atomic-capacity-{index}",
                title=f"Atomic capacity {index}",
                description="Compete for one durable versioned Worker",
                requirements=requirements,
            )
            for index in range(3)
        ]
    )
    peers = tuple(StateStore(store.path) for _ in task_ids)
    barrier = threading.Barrier(len(task_ids))

    def claim(peer: StateStore, task_id: str):
        barrier.wait(timeout=2)
        return peer.claim_task_dispatch(
            task_id,
            ["worker-versioned"],
            expected_version=peer.get_task(task_id)["version"],
            expected_manifest_digests={"worker-versioned": manifest.digest},
        )

    claims = await asyncio.gather(
        *(
            asyncio.to_thread(claim, peer, task_id)
            for peer, task_id in zip(peers, task_ids, strict=True)
        )
    )

    successful = [claim for claim in claims if claim is not None]
    assert len(successful) == 2
    assert len(store.list_worker_runs()) == 2
    assert store.worker_snapshots()[0].running_tasks == 2
    claimed_task = next(
        task_id
        for task_id in task_ids
        if store.get_task(task_id)["state"] == TaskState.RUNNING.value
    )
    assert (
        StateStore(store.path).claim_task_dispatch(
            claimed_task,
            ["worker-versioned"],
            expected_version=store.get_task(claimed_task)["version"],
            expected_manifest_digests={"worker-versioned": manifest.digest},
        )
        is None
    )


async def test_peer_recovery_preserves_live_unexpired_execution_lease(tmp_path) -> None:
    store, registry, runtime = runtime_fixture(tmp_path)
    adapter = GatedMockAdapter()
    register_worker(store, registry, "worker-1", adapter)
    task_id = await runtime.submit_task(
        project_id="project-1",
        title="Live leased execution",
        description="A peer restart must not interrupt the live owner",
        requirements=TaskRequirements(
            labels=frozenset({TaskLabel.RESEARCH}),
            required_capabilities=frozenset({"analysis"}),
        ),
    )
    peer_store = StateStore(store.path)
    peer_runtime = SupervisorRuntime(
        store=peer_store,
        scheduler=DeterministicScheduler(),
        adapters=AdapterRegistry(),
        evidence_root=tmp_path / "peer-evidence",
        runtime_id="peer-runtime",
    )

    dispatched = await runtime.dispatch_ready()
    await asyncio.wait_for(adapter.started.wait(), timeout=1)
    recovered = await peer_runtime.recover(task_ids={task_id})

    assert dispatched.launched_task_ids == (task_id,)
    assert recovered == {"runsInterrupted": 0, "tasksInterrupted": 0}
    assert store.get_task(task_id)["state"] == TaskState.RUNNING.value
    assert len(store.list_worker_runs(task_id)) == 1

    adapter.release.set()
    await runtime.wait_for_active()

    assert adapter.execute_calls == 1
    assert store.get_task(task_id)["state"] == TaskState.REVIEWING.value
    runs = store.list_worker_runs(task_id)
    assert len(runs) == 1
    assert runs[0]["state"] == "completed"


async def test_expired_execution_lease_is_recovered_and_requeued_exactly_once(
    tmp_path,
) -> None:
    store, registry, _runtime = runtime_fixture(tmp_path, max_attempts=2)
    register_worker(store, registry, "worker-1", MockAdapter())
    task_id = await _runtime.submit_task(
        project_id="project-1",
        title="Crashed leased execution",
        description="An expired owner can be reconciled once",
        requirements=TaskRequirements(
            labels=frozenset({TaskLabel.RESEARCH}),
            required_capabilities=frozenset({"analysis"}),
        ),
    )
    ready = store.get_task(task_id)
    claim = store.claim_task_dispatch(
        task_id,
        ["worker-1"],
        expected_version=ready["version"],
        lease_owner_id="crashed-runtime",
        lease_ttl_seconds=30,
    )
    assert claim is not None
    with store.transaction() as connection:
        connection.execute(
            "UPDATE task_execution_leases SET expires_at=? WHERE task_id=?",
            ("1970-01-01T00:00:00Z", task_id),
        )
    peer_store = StateStore(store.path)
    peer_runtime = SupervisorRuntime(
        store=peer_store,
        scheduler=DeterministicScheduler(),
        adapters=AdapterRegistry(),
        evidence_root=tmp_path / "peer-evidence",
        max_attempts=2,
        runtime_id="recovery-runtime",
    )

    first = await peer_runtime.recover(task_ids={task_id})
    second = await peer_runtime.recover(task_ids={task_id})

    assert first == {"runsInterrupted": 1, "tasksInterrupted": 1}
    assert second == {"runsInterrupted": 0, "tasksInterrupted": 0}
    task = store.get_task(task_id)
    assert task["state"] == TaskState.READY.value
    assert task["attempt_count"] == 1
    runs = store.list_worker_runs(task_id)
    assert len(runs) == 1
    assert runs[0]["id"] == claim["runIDs"]["worker-1"]
    assert runs[0]["state"] == "interrupted"
    event_kinds = [event["kind"] for event in store.list_events(task_id=task_id)]
    assert event_kinds.count("workerRunInterrupted") == 1
    assert event_kinds.count("taskRecovered") == 1
    with store.connect() as connection:
        lease = connection.execute(
            "SELECT state FROM task_execution_leases WHERE task_id=?", (task_id,)
        ).fetchone()
    assert lease["state"] == "released"


async def test_cancel_before_adapter_activation_never_invokes_adapter(
    tmp_path,
    monkeypatch,
) -> None:
    store, registry, runtime = runtime_fixture(tmp_path)
    adapter = CountingMockAdapter()
    register_worker(store, registry, "worker-1", adapter)
    task_id = await runtime.submit_task(
        project_id="project-1",
        title="Cancel STARTING execution",
        description="Cancellation must fence the adapter invocation",
        requirements=TaskRequirements(
            labels=frozenset({TaskLabel.RESEARCH}),
            required_capabilities=frozenset({"analysis"}),
        ),
    )
    loop = asyncio.get_running_loop()
    activation_entered = asyncio.Event()
    allow_activation = threading.Event()
    cancellation_persisted = asyncio.Event()
    original_activate = store.activate_worker_run
    original_cancel = store.cancel_task_execution

    def delayed_activation(
        run_id: str,
        *,
        lease_owner_id: str | None = None,
        lease_generation: int | None = None,
        actor: str = "runtime",
    ) -> bool:
        loop.call_soon_threadsafe(activation_entered.set)
        if not allow_activation.wait(timeout=5):
            raise TimeoutError("test did not release adapter activation fence")
        return original_activate(
            run_id,
            lease_owner_id=lease_owner_id,
            lease_generation=lease_generation,
            actor=actor,
        )

    def tracked_cancel(task_identity: str, *, actor: str = "runtime") -> bool:
        result = original_cancel(task_identity, actor=actor)
        loop.call_soon_threadsafe(cancellation_persisted.set)
        return result

    monkeypatch.setattr(store, "activate_worker_run", delayed_activation)
    monkeypatch.setattr(store, "cancel_task_execution", tracked_cancel)

    dispatched = await runtime.dispatch_ready()
    await asyncio.wait_for(activation_entered.wait(), timeout=1)
    cancellation = asyncio.create_task(runtime.cancel_task(task_id))
    await asyncio.wait_for(cancellation_persisted.wait(), timeout=1)
    allow_activation.set()

    assert dispatched.launched_task_ids == (task_id,)
    assert await cancellation
    assert adapter.execute_calls == 0
    assert store.get_task(task_id)["state"] == TaskState.CANCELLED.value
    runs = store.list_worker_runs(task_id)
    assert len(runs) == 1
    assert runs[0]["state"] == "cancelled"
    assert store.list_workers()[0]["state"] == WorkerState.IDLE.value


async def test_recovery_requeues_atomic_pre_run_claim_once(tmp_path) -> None:
    store, registry, runtime = runtime_fixture(tmp_path, max_attempts=2)
    register_worker(store, registry, "worker-1", MockAdapter())
    task_id = await runtime.submit_task(
        project_id="project-1",
        title="Crash before adapter start",
        description="Recover a durable dispatch claim",
        requirements=TaskRequirements(
            labels=frozenset({TaskLabel.RESEARCH}),
            required_capabilities=frozenset({"analysis"}),
        ),
    )
    ready = store.get_task(task_id)
    claim = store.claim_task_dispatch(task_id, ["worker-1"], expected_version=ready["version"])
    assert claim is not None

    first = await runtime.recover()
    second = await runtime.recover()

    assert first == {"runsInterrupted": 1, "tasksInterrupted": 1}
    assert second == {"runsInterrupted": 0, "tasksInterrupted": 0}
    assert store.get_task(task_id)["state"] == TaskState.READY.value
    assert store.list_workers()[0]["state"] == WorkerState.IDLE.value
    runs = store.list_worker_runs(task_id)
    assert len(runs) == 1
    assert runs[0]["id"] == claim["runIDs"]["worker-1"]
    assert runs[0]["state"] == "interrupted"


async def test_recovery_at_attempt_limit_fails_without_redispatch(tmp_path) -> None:
    store, registry, runtime = runtime_fixture(tmp_path, max_attempts=1)
    register_worker(store, registry, "worker-1", MockAdapter())
    task_id = await runtime.submit_task(
        project_id="project-1",
        title="Exhausted pre-run claim",
        description="Do not dispatch after recovery exhausts the attempt budget",
        requirements=TaskRequirements(
            labels=frozenset({TaskLabel.RESEARCH}),
            required_capabilities=frozenset({"analysis"}),
        ),
    )
    ready = store.get_task(task_id)
    claim = store.claim_task_dispatch(task_id, ["worker-1"], expected_version=ready["version"])
    assert claim is not None

    recovered = await runtime.recover()
    dispatch = await runtime.dispatch_ready()

    assert recovered == {"runsInterrupted": 1, "tasksInterrupted": 1}
    assert dispatch.launched_task_ids == ()
    assert store.get_task(task_id)["state"] == TaskState.FAILED.value
    assert store.list_workers()[0]["state"] == WorkerState.IDLE.value
    runs = store.list_worker_runs(task_id)
    assert len(runs) == 1
    assert runs[0]["id"] == claim["runIDs"]["worker-1"]
    assert runs[0]["state"] == "interrupted"
