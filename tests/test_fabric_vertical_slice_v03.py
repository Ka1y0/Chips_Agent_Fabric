from __future__ import annotations

import asyncio
from datetime import UTC, datetime

from project_supervisor.adapters import EventSink, MockAdapter, WorkerRequest, WorkerResult
from project_supervisor.autonomy import GoalBudget, GoalService
from project_supervisor.domain import (
    ExecutionTopology,
    Harness,
    ModelDescriptor,
    NodeState,
    Provider,
    ResourceState,
    TaskLabel,
    TaskRequirements,
    TaskState,
    WorkerSnapshot,
    WorkerState,
)
from project_supervisor.fabric.capabilities import (
    INITIAL_CAPABILITY_CATALOG,
    WORKER_MANIFEST_SCHEMA_VERSION,
    CostMode,
    ObservationFreshness,
    QuotaAvailability,
    SubscriptionState,
    WorkerDynamicState,
    WorkerHealth,
    WorkerLocality,
    WorkerManifest,
    WorkerPrivacy,
)
from project_supervisor.fabric.execution import ChildWorkProposal, SpawnPolicy, SpawnReason
from project_supervisor.fabric.persistence import CapabilityRegistryRepository, SpawnRepository
from project_supervisor.runtime import AdapterRegistry, SupervisorRuntime
from project_supervisor.scheduler import DeterministicScheduler
from project_supervisor.store import StateStore, timestamp
from project_supervisor.verification import DefinitionOfDoneResult, VerificationResult


class ParallelGate:
    def __init__(self, expected: int) -> None:
        self.expected = expected
        self.started: set[str] = set()
        self.all_started = asyncio.Event()

    async def arrive(self, worker_id: str) -> None:
        self.started.add(worker_id)
        if len(self.started) == self.expected:
            self.all_started.set()
        await self.all_started.wait()


class GatedFabricAdapter(MockAdapter):
    def __init__(self, worker_id: str, gate: ParallelGate) -> None:
        super().__init__()
        self.worker_id = worker_id
        self.gate = gate

    async def execute(
        self,
        request: WorkerRequest,
        *,
        event_sink: EventSink | None = None,
    ) -> WorkerResult:
        await self.gate.arrive(self.worker_id)
        return await super().execute(request, event_sink=event_sink)


def register_worker(
    store: StateStore,
    adapters: AdapterRegistry,
    capabilities: CapabilityRegistryRepository,
    *,
    worker_id: str,
    adapter: MockAdapter,
) -> None:
    store.upsert_worker(
        WorkerSnapshot(
            id=worker_id,
            node_id="node-fabric",
            harness=Harness.MOCK,
            provider=Provider.MOCK,
            model=ModelDescriptor("fabric-fixture", "Fabric fixture", Provider.MOCK),
            state=WorkerState.IDLE,
            node_state=NodeState.ONLINE,
            resource_state=ResourceState.AVAILABLE,
            capabilities=frozenset({"analysis"}),
            code_write_allowed=False,
            privacy_allowed=True,
            quality_score=0.8,
            reliability_score=1,
            expected_latency_seconds=0.01,
            monetary_cost_score=0,
        )
    )
    capabilities.register_manifest(
        WorkerManifest(
            worker_id=worker_id,
            node_id="node-fabric",
            provider_id=Provider.MOCK.value,
            adapter_kind="parallel-fixture-v1",
            capabilities=("analysis",),
            models=("fabric-fixture",),
            locality=WorkerLocality.LOCAL,
            privacy=WorkerPrivacy.SENSITIVE,
            cost_mode=CostMode.LOCAL_FREE,
            max_concurrency=1,
        ),
        expected_head_generation=0,
    )
    capabilities.record_observation(
        WorkerDynamicState(
            worker_id=worker_id,
            health=WorkerHealth.HEALTHY,
            health_freshness=ObservationFreshness.FRESH,
            quota=QuotaAvailability.AVAILABLE,
            quota_freshness=ObservationFreshness.FRESH,
            subscription_state=SubscriptionState.UNKNOWN,
            load=0,
            running_tasks=0,
            observed_at=datetime.now(UTC),
        )
    )
    adapters.register(worker_id, adapter)


def requirements() -> TaskRequirements:
    return TaskRequirements(
        labels=frozenset({TaskLabel.RESEARCH}),
        required_capabilities=frozenset({"analysis"}),
        local_only=True,
        required_manifest_schema_version=WORKER_MANIFEST_SCHEMA_VERSION,
        required_capability_catalog_version=INITIAL_CAPABILITY_CATALOG.version,
    )


async def verify_task(
    runtime: SupervisorRuntime,
    store: StateStore,
    task_id: str,
    criterion_id: str,
) -> None:
    context = store.get_task_verification_context(task_id)
    state = await runtime.apply_verification(
        task_id,
        DefinitionOfDoneResult(
            complete=True,
            results=(VerificationResult(criterion_id, True, "fixture result verified"),),
            required_failures=(),
        ),
        expected_verification_scope_id=context["verification_scope_id"],
        expected_task_definition_revision=context["task_definition_revision"],
        expected_source_attempt=context["source_attempt"],
    )
    assert state is TaskState.SUCCEEDED


async def test_parallel_dag_and_worker_proposed_child_are_canonically_governed(tmp_path) -> None:
    store = StateStore(tmp_path / "state.db")
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    store.create_project(
        project_id="project-parallel",
        name="Parallel Fabric fixture",
        root_path=str(workspace),
        goal="Prove parallel DAG and child-work governance",
    )
    store.upsert_node(
        node_id="node-fabric",
        hostname="fixture",
        display_name="Fabric node",
        role="worker",
        state=NodeState.ONLINE,
    )
    gate = ParallelGate(expected=2)
    adapters = AdapterRegistry()
    capability_registry = CapabilityRegistryRepository(store)
    register_worker(
        store,
        adapters,
        capability_registry,
        worker_id="worker-a",
        adapter=GatedFabricAdapter("worker-a", gate),
    )
    register_worker(
        store,
        adapters,
        capability_registry,
        worker_id="worker-b",
        adapter=GatedFabricAdapter("worker-b", gate),
    )
    runtime = SupervisorRuntime(
        store=store,
        scheduler=DeterministicScheduler(),
        adapters=adapters,
        evidence_root=tmp_path / "evidence",
        max_attempts=1,
    )

    task_a = await runtime.submit_task(
        project_id="project-parallel",
        task_id="task-a",
        title="Independent A",
        description="Produce bounded result A",
        requirements=requirements(),
    )
    task_b = await runtime.submit_task(
        project_id="project-parallel",
        task_id="task-b",
        title="Independent B",
        description="Produce bounded result B",
        requirements=requirements(),
    )
    task_c = await runtime.submit_task(
        project_id="project-parallel",
        task_id="task-c",
        title="Join C",
        description="Wait for both verified prerequisites",
        requirements=requirements(),
        topology=ExecutionTopology.SINGLE,
    )
    store.add_task_dependency(task_c, task_a)
    store.add_task_dependency(task_c, task_b)

    criterion_id = store.add_acceptance_criterion(
        project_id="project-parallel",
        kind="assertion",
        description="The deterministic Worker result is verified",
    )
    store.bind_task_verification_scope(task_a, criterion_ids=(criterion_id,))
    store.bind_task_verification_scope(task_b, criterion_ids=(criterion_id,))

    goals = GoalService(store)
    goals.create_goal(
        project_id="project-parallel",
        intent="Permit one bounded child proposal",
        budgets=GoalBudget(max_tasks=1),
        goal_id="goal-parallel",
    )
    with store.transaction() as connection:
        connection.execute(
            "UPDATE autonomous_goals SET state='running',started_at=?,updated_at=? WHERE id=?",
            (timestamp(), timestamp(), "goal-parallel"),
        )
    spawn_policy = SpawnPolicy(
        max_depth=1,
        max_children_per_parent=3,
        max_total_children=5,
    )
    spawn = SpawnRepository(store, spawn_policy)
    spawn.bind_root_task(
        task_id=task_a,
        goal_id="goal-parallel",
        expected_steer_version=0,
    )

    first_wave = await runtime.dispatch_ready(task_ids={task_a, task_b, task_c})
    await asyncio.wait_for(gate.all_started.wait(), timeout=2)
    assert set(first_wave.launched_task_ids) == {task_a, task_b}
    assert task_c not in first_wave.launched_task_ids
    assert gate.started == {"worker-a", "worker-b"}
    await runtime.wait_for_active(task_ids={task_a, task_b})
    runs_a = store.list_worker_runs(task_a)
    runs_b = store.list_worker_runs(task_b)
    assert runs_a[0]["worker_id"] != runs_b[0]["worker_id"]

    still_waiting = await runtime.dispatch_ready(task_ids={task_c})
    assert still_waiting.launched_task_ids == ()
    assert store.get_task(task_c)["state"] == TaskState.READY.value

    await verify_task(runtime, store, task_a, criterion_id)
    await verify_task(runtime, store, task_b, criterion_id)
    joined = await runtime.dispatch_ready(task_ids={task_c})
    assert joined.launched_task_ids == (task_c,)
    await runtime.wait_for_active(task_ids={task_c})
    assert store.get_task(task_c)["state"] == TaskState.REVIEWING.value

    proposal_envelope = {
        "schemaVersion": "child-work-proposal/v1",
        "proposalID": "proposal-a1",
        "proposalKey": "child-a1",
        "title": "A1",
        "description": "Bounded follow-up proposed by A",
        "provider": "mock",
        "estimatedTokens": 1,
        "estimatedSeconds": 1,
        "estimatedCostUSD": 0,
        "payload": {"requirements": {"requiredCapabilities": ["analysis"], "localOnly": True}},
    }
    (accepted,) = await runtime.admit_child_work_proposals(
        runs_a[0]["id"],
        (proposal_envelope,),
        policy=spawn_policy,
    )
    (replay,) = await runtime.admit_child_work_proposals(
        runs_a[0]["id"],
        (proposal_envelope,),
        policy=spawn_policy,
    )
    assert accepted["canonicalTaskCreated"]
    assert replay["disposition"] == "replay"
    assert replay["childTaskID"] == accepted["childTaskID"]
    child_id = accepted["childTaskID"]
    assert store.get_task(child_id)["state"] == TaskState.READY.value
    with store.connect() as connection:
        assert (
            connection.execute(
                "SELECT COUNT(*) FROM child_work_proposals "
                "WHERE goal_id='goal-parallel' AND proposal_key='child-a1'"
            ).fetchone()[0]
            == 1
        )
        assert (
            connection.execute(
                "SELECT COUNT(*) FROM autonomous_task_bindings "
                "WHERE parent_task_id=? AND proposal_id='proposal-a1'",
                (task_a,),
            ).fetchone()[0]
            == 1
        )

    depth_overflow = spawn.admit(
        ChildWorkProposal(
            proposal_id="proposal-too-deep",
            goal_id="goal-parallel",
            parent_task_id=child_id,
            proposal_key="grandchild",
            title="Too deep",
            description="Must be rejected",
            steer_version=0,
            depth=2,
            task_definition_revision=1,
            estimated_tokens=1,
            estimated_seconds=1,
            estimated_cost_usd=0,
        )
    )
    budget_exhausted = spawn.admit(
        ChildWorkProposal(
            proposal_id="proposal-over-budget",
            goal_id="goal-parallel",
            parent_task_id=task_a,
            proposal_key="second-child",
            title="Over budget",
            description="Must remain a rejected proposal",
            steer_version=0,
            depth=1,
            task_definition_revision=int(store.get_task(task_a)["definition_revision"]),
            estimated_tokens=1,
            estimated_seconds=1,
            estimated_cost_usd=0,
        )
    )
    assert depth_overflow["reasonCode"] == SpawnReason.DEPTH_LIMIT.value
    assert budget_exhausted["reasonCode"] == SpawnReason.TOTAL_CHILDREN_LIMIT.value

    goals.pause("goal-parallel", "soft", reason="operator pause")
    paused = spawn.admit(
        ChildWorkProposal(
            proposal_id="proposal-paused",
            goal_id="goal-parallel",
            parent_task_id=task_a,
            proposal_key="paused-child",
            title="Paused",
            description="Pause blocks new child work",
            steer_version=0,
            depth=1,
            task_definition_revision=int(store.get_task(task_a)["definition_revision"]),
            estimated_tokens=1,
            estimated_seconds=1,
            estimated_cost_usd=0,
        )
    )
    assert paused["reasonCode"] == SpawnReason.GOAL_NOT_RUNNING.value
    goals.resume("goal-parallel", reason="continue governance checks")
    goals.steer("goal-parallel", "Use the next planning version")
    stale_parent = spawn.admit(
        ChildWorkProposal(
            proposal_id="proposal-stale-parent",
            goal_id="goal-parallel",
            parent_task_id=task_a,
            proposal_key="stale-parent-child",
            title="Stale parent",
            description="Pre-steer work cannot authorize post-steer expansion",
            steer_version=1,
            depth=1,
            task_definition_revision=int(store.get_task(task_a)["definition_revision"]),
            estimated_tokens=1,
            estimated_seconds=1,
            estimated_cost_usd=0,
        )
    )
    assert stale_parent["reasonCode"] == SpawnReason.STALE_PARENT_STEER_VERSION.value
    goals.stop("goal-parallel", reason="operator halt")
    halted = spawn.admit(
        ChildWorkProposal(
            proposal_id="proposal-halted",
            goal_id="goal-parallel",
            parent_task_id=task_a,
            proposal_key="halted-child",
            title="Halted",
            description="HALT blocks new child work",
            steer_version=1,
            depth=1,
            task_definition_revision=int(store.get_task(task_a)["definition_revision"]),
            estimated_tokens=1,
            estimated_seconds=1,
            estimated_cost_usd=0,
        )
    )
    assert halted["reasonCode"] == SpawnReason.GOAL_NOT_RUNNING.value
    assert store.get_task(child_id)["state"] == TaskState.READY.value
    assert len(store.list_tasks("project-parallel")) == 4


async def test_runtime_child_work_intake_replay_creates_one_canonical_task(tmp_path) -> None:
    store = StateStore(tmp_path / "runtime-child-intake.db")
    workspace = tmp_path / "runtime-child-workspace"
    workspace.mkdir()
    store.create_project(
        project_id="project-runtime-child",
        name="Runtime child intake fixture",
        root_path=str(workspace),
        goal="Prove Runtime replay retains one canonical child",
    )
    store.upsert_node(
        node_id="node-fabric",
        hostname="fixture",
        display_name="Fabric node",
        role="worker",
        state=NodeState.ONLINE,
    )
    adapters = AdapterRegistry()
    register_worker(
        store,
        adapters,
        CapabilityRegistryRepository(store),
        worker_id="worker-runtime-parent",
        adapter=MockAdapter(),
    )
    runtime = SupervisorRuntime(
        store=store,
        scheduler=DeterministicScheduler(),
        adapters=adapters,
        evidence_root=tmp_path / "runtime-child-evidence",
        max_attempts=1,
    )
    task_id = await runtime.submit_task(
        project_id="project-runtime-child",
        task_id="task-runtime-parent",
        title="Runtime parent",
        description="Produce one canonical child proposal",
        requirements=requirements(),
    )
    goals = GoalService(store)
    goals.create_goal(
        project_id="project-runtime-child",
        intent="Admit exactly one logical child",
        budgets=GoalBudget(max_tasks=1),
        goal_id="goal-runtime-child",
    )
    with store.transaction() as connection:
        connection.execute(
            "UPDATE autonomous_goals SET state='running',started_at=?,updated_at=? WHERE id=?",
            (timestamp(), timestamp(), "goal-runtime-child"),
        )
    policy = SpawnPolicy(max_depth=1, max_total_children=1)
    SpawnRepository(store, policy).bind_root_task(
        task_id=task_id,
        goal_id="goal-runtime-child",
        expected_steer_version=0,
    )
    run_id = store.create_worker_run(
        task_id=task_id,
        worker_id="worker-runtime-parent",
        attempt=1,
    )
    store.save_worker_result(run_id=run_id, summary="proposes one child")
    with store.transaction() as connection:
        connection.execute(
            "UPDATE worker_runs SET state='completed',exit_code=0,ended_at=?,updated_at=? "
            "WHERE id=?",
            (timestamp(), timestamp(), run_id),
        )
    envelope = {
        "schemaVersion": "child-work-proposal/v1",
        "proposalID": "proposal-runtime-child",
        "proposalKey": "runtime-child",
        "title": "Runtime child",
        "description": "Canonical replay fixture",
        "estimatedTokens": 1,
        "estimatedSeconds": 1,
        "estimatedCostUSD": 0,
        "payload": {},
    }

    accepted, replay = await runtime.admit_child_work_proposals(
        run_id,
        (envelope, envelope),
        policy=policy,
    )
    restarted_runtime = SupervisorRuntime(
        store=StateStore(store.path),
        scheduler=DeterministicScheduler(),
        adapters=AdapterRegistry(),
        evidence_root=tmp_path / "runtime-child-evidence-restarted",
        max_attempts=1,
    )
    (restart_replay,) = await restarted_runtime.admit_child_work_proposals(
        run_id,
        (envelope,),
        policy=policy,
    )

    assert accepted["canonicalTaskCreated"]
    assert replay["disposition"] == restart_replay["disposition"] == "replay"
    assert accepted["childTaskID"] == replay["childTaskID"] == restart_replay["childTaskID"]
    with store.connect() as connection:
        assert connection.execute("SELECT COUNT(*) FROM child_work_proposals").fetchone()[0] == 1
        assert (
            connection.execute(
                "SELECT COUNT(*) FROM child_work_proposal_decisions WHERE outcome='accepted'"
            ).fetchone()[0]
            == 1
        )
        assert (
            connection.execute(
                "SELECT COUNT(*) FROM autonomous_task_bindings WHERE parent_task_id=?",
                (task_id,),
            ).fetchone()[0]
            == 1
        )
        assert (
            connection.execute(
                "SELECT COUNT(*) FROM tasks WHERE project_id='project-runtime-child'"
            ).fetchone()[0]
            == 2
        )
