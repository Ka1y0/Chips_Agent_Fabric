from __future__ import annotations

import asyncio
import json
import sqlite3
from collections import deque
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from project_supervisor.adapters import MockAdapter, MockBehavior
from project_supervisor.autonomy import (
    ActionResult,
    AutonomousIterationEngine,
    DispatchHandle,
    EvaluationDisposition,
    GoalBudget,
    GoalEvaluation,
    GoalPlan,
    GoalService,
    GoalState,
    GoalVerification,
    PauseMode,
    PlannedAction,
    SupervisorRuntimeDispatcher,
    TerminationReason,
)
from project_supervisor.domain import (
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
from project_supervisor.runtime import AdapterRegistry, SupervisorRuntime
from project_supervisor.scheduler import DeterministicScheduler
from project_supervisor.store import StateStore


@pytest.fixture
def store(tmp_path: Path) -> StateStore:
    value = StateStore(tmp_path / "state.db")
    value.create_project(
        project_id="project-1",
        name="Autonomy fixture",
        root_path=str(tmp_path),
        goal="exercise autonomous iteration",
    )
    return value


class SequenceEvaluator:
    def __init__(self, values: list[GoalEvaluation]) -> None:
        self.values = deque(values)
        self.contexts: list[Any] = []

    async def evaluate(self, context: Any) -> GoalEvaluation:
        self.contexts.append(context)
        return self.values.popleft()


class IterationPlanner:
    def __init__(self) -> None:
        self.contexts: list[Any] = []

    async def plan(self, context: Any, evaluation: GoalEvaluation) -> GoalPlan:
        del evaluation
        self.contexts.append(context)
        sequence = context.goal["iteration_count"]
        return GoalPlan(
            summary=f"plan iteration {sequence}",
            actions=(
                PlannedAction(
                    key=f"iteration-{sequence}",
                    title=f"Work {sequence}",
                    description=f"perform work {sequence}",
                ),
            ),
        )


class ControlledPlanner(IterationPlanner):
    def __init__(self) -> None:
        super().__init__()
        self.started = asyncio.Event()
        self.release = asyncio.Event()

    async def plan(self, context: Any, evaluation: GoalEvaluation) -> GoalPlan:
        self.started.set()
        await self.release.wait()
        return await super().plan(context, evaluation)


class SequenceVerifier:
    def __init__(self, values: list[GoalVerification]) -> None:
        self.values = deque(values)
        self.calls = 0

    async def verify(self, context: Any, results: tuple[ActionResult, ...]) -> GoalVerification:
        del context
        assert results
        self.calls += 1
        return self.values.popleft()


class RecordingVerifier:
    def __init__(self, value: GoalVerification) -> None:
        self.value = value
        self.results: list[tuple[ActionResult, ...]] = []

    async def verify(self, context: Any, results: tuple[ActionResult, ...]) -> GoalVerification:
        del context
        self.results.append(results)
        return self.value


class ImmediateDispatcher:
    def __init__(self) -> None:
        self.dispatched: list[str] = []
        self.recovered: list[str] = []
        self.cancelled: list[str] = []

    async def dispatch(self, context: Any, action: PlannedAction, action_id: str) -> DispatchHandle:
        del context, action
        self.dispatched.append(action_id)
        return DispatchHandle(reference=action_id)

    async def collect(self, handle: DispatchHandle) -> ActionResult:
        return ActionResult(True, f"completed {handle.reference}")

    async def cancel(self, handle: DispatchHandle) -> bool:
        self.cancelled.append(handle.reference)
        return True

    async def recover(
        self, context: Any, action: PlannedAction, handle: DispatchHandle
    ) -> DispatchHandle:
        del context, action
        self.recovered.append(handle.reference)
        return handle


class ControlledDispatcher(ImmediateDispatcher):
    def __init__(self) -> None:
        super().__init__()
        self.started = asyncio.Event()
        self.release = asyncio.Event()
        self.cancel_signal = asyncio.Event()

    async def collect(self, handle: DispatchHandle) -> ActionResult:
        self.started.set()
        release = asyncio.create_task(self.release.wait())
        cancelled = asyncio.create_task(self.cancel_signal.wait())
        done, pending = await asyncio.wait(
            {release, cancelled}, return_when=asyncio.FIRST_COMPLETED
        )
        for item in pending:
            item.cancel()
        was_cancelled = cancelled in done
        return ActionResult(
            not was_cancelled,
            "cancelled" if was_cancelled else f"completed {handle.reference}",
            cancelled=was_cancelled,
        )

    async def cancel(self, handle: DispatchHandle) -> bool:
        await super().cancel(handle)
        self.cancel_signal.set()
        return True


class NonCooperativeDispatcher(ImmediateDispatcher):
    def __init__(self) -> None:
        super().__init__()
        self.started = asyncio.Event()
        self.never = asyncio.Event()

    async def collect(self, handle: DispatchHandle) -> ActionResult:
        del handle
        self.started.set()
        await self.never.wait()
        raise AssertionError("non-cooperative collection should be cancelled locally")

    async def cancel(self, handle: DispatchHandle) -> bool:
        self.cancelled.append(handle.reference)
        return True


class SlowCancellationDispatcher(NonCooperativeDispatcher):
    def __init__(self) -> None:
        super().__init__()
        self.cancel_started = asyncio.Event()
        self.allow_cancel = asyncio.Event()
        self.cancel_completed = asyncio.Event()
        self.cancel_aborted = False

    async def cancel(self, handle: DispatchHandle) -> bool:
        self.cancel_started.set()
        try:
            await self.allow_cancel.wait()
        except asyncio.CancelledError:
            self.cancel_aborted = True
            raise
        self.cancelled.append(handle.reference)
        self.cancel_completed.set()
        return True


def incomplete(fingerprint: str) -> GoalEvaluation:
    return GoalEvaluation(
        EvaluationDisposition.INCOMPLETE,
        "goal remains incomplete",
        fingerprint,
    )


def verify_incomplete(fingerprint: str) -> GoalVerification:
    return GoalVerification(False, "more work is required", fingerprint)


def verify_success(fingerprint: str = "done") -> GoalVerification:
    return GoalVerification(True, "goal acceptance is satisfied", fingerprint)


def evaluator_satisfied(fingerprint: str = "evaluation-done") -> GoalEvaluation:
    return GoalEvaluation(
        EvaluationDisposition.SATISFIED,
        "evaluator proposes completion",
        fingerprint,
    )


def engine(
    store: StateStore,
    evaluator: SequenceEvaluator,
    verifier: SequenceVerifier,
    dispatcher: ImmediateDispatcher,
    *,
    cancellation_grace_seconds: float = 0.5,
) -> AutonomousIterationEngine:
    return AutonomousIterationEngine(
        store=store,
        evaluator=evaluator,
        planner=IterationPlanner(),
        dispatcher=dispatcher,
        verifier=verifier,
        control_poll_seconds=0.005,
        cancellation_grace_seconds=cancellation_grace_seconds,
    )


def persist_satisfied_evaluation(
    iteration_engine: AutonomousIterationEngine,
    goal_id: str,
    *,
    steer_version: int = 0,
) -> dict[str, Any]:
    iteration_engine._start(goal_id)
    iteration = iteration_engine._begin_iteration(goal_id)
    assert iteration_engine._record_evaluation(
        iteration["id"],
        evaluator_satisfied(),
        expected_steer_version=steer_version,
    )
    return iteration


def test_goal_controls_are_durable_and_journaled(store: StateStore) -> None:
    service = GoalService(store)
    goal = service.create_goal(project_id="project-1", intent="Ship a verified subsystem")
    assert goal["state"] == GoalState.CREATED.value
    assert goal["event_cursor"] > 0

    paused = service.pause(goal["id"], PauseMode.SOFT, reason="inspect")
    assert paused["state"] == GoalState.SOFT_PAUSED.value
    resumed = service.resume(goal["id"], reason="inspection complete")
    assert resumed["state"] == GoalState.RUNNING.value
    steered = service.steer(goal["id"], "Prefer the portable solution", priority=90)
    assert steered["steer_version"] == 1
    assert "Prefer the portable solution" in steered["effective_intent"]
    stopped = service.stop(goal["id"], reason="operator decision")
    assert stopped["state"] == GoalState.STOPPED.value
    assert stopped["termination_reason"] == TerminationReason.USER_STOPPED.value
    assert GoalService(store).get_goal(goal["id"])["state"] == GoalState.STOPPED.value
    with pytest.raises(ValueError):
        service.resume(goal["id"])

    kinds = {event["kind"] for event in store.list_events(limit=1000)}
    assert {
        "goalCreated",
        "goalPaused",
        "goalResumed",
        "goalSteered",
        "goalStopped",
    } <= kinds

    sanitized = service.create_goal(
        project_id="project-1", intent="Inspect safely with token=do-not-persist"
    )
    assert "do-not-persist" not in sanitized["intent"]
    assert "[REDACTED]" in sanitized["intent"]


def test_existing_v01_database_upgrades_without_rewriting_prior_migrations(tmp_path: Path) -> None:
    database = tmp_path / "upgrade.db"
    migrations = Path(__file__).parents[1] / "src/project_supervisor/migrations"
    with sqlite3.connect(database) as connection:
        connection.execute(
            "CREATE TABLE schema_migrations (version TEXT PRIMARY KEY, applied_at TEXT NOT NULL)"
        )
        for version in ("0001_initial", "0002_v01_foundations"):
            connection.executescript((migrations / f"{version}.sql").read_text(encoding="utf-8"))
            connection.execute(
                "INSERT INTO schema_migrations(version,applied_at) VALUES (?,?)",
                (version, "2026-08-09T00:00:00Z"),
            )

    upgraded = StateStore(database)

    with upgraded.connect() as connection:
        versions = [
            row["version"]
            for row in connection.execute(
                "SELECT version FROM schema_migrations ORDER BY version"
            ).fetchall()
        ]
        tables = {
            row["name"]
            for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()
        }
    assert versions == [
        "0001_initial",
        "0002_v01_foundations",
        "0003_autonomous_iteration",
        "0004_invocation_telemetry",
        "0005_autonomous_host",
        "0006_resource_usage_ledger",
        "0007_autonomy_decision_tasks",
        "0008_node_runtime_recovery",
        "0009_node_recovery_host",
        "0010_task_execution_leases",
        "0011_durable_provider_jobs",
        "0012_task_verification_scopes",
        "0013_local_worker_protocol_v2",
    ]
    assert {
        "autonomous_goals",
        "autonomous_actions",
        "invocation_telemetry",
        "autonomous_hosts",
        "post_task_usage_audits",
        "autonomy_decision_tasks",
        "node_runtime_recovery_policies",
        "node_runtime_recovery_attempts",
        "node_runtime_recovery_leases",
        "node_runtime_recovery_monitors",
        "node_runtime_recovery_checkpoints",
        "task_execution_leases",
        "provider_jobs",
        "execution_escalations",
        "task_verification_scopes",
        "task_verification_scope_items",
    } <= tables


async def test_successful_task_does_not_finish_goal_and_replans_automatically(
    store: StateStore,
) -> None:
    service = GoalService(store)
    goal = service.create_goal(project_id="project-1", intent="Reach verified success")
    evaluator = SequenceEvaluator([incomplete("start"), incomplete("after-one")])
    verifier = SequenceVerifier([verify_incomplete("after-one"), verify_success()])
    dispatcher = ImmediateDispatcher()

    result = await engine(store, evaluator, verifier, dispatcher).run(goal["id"])

    assert result["state"] == GoalState.TERMINATED.value
    assert result["termination_reason"] == TerminationReason.SUCCESS.value
    assert result["iteration_count"] == 2
    assert result["task_count"] == 2
    assert len(dispatcher.dispatched) == 2
    assert verifier.calls == 2
    kinds = [event["kind"] for event in store.list_events(limit=1000)]
    assert kinds.count("goalReplanRequired") == 1
    assert kinds.count("goalPlanCreated") == 2


async def test_goal_context_is_isolated_and_includes_iteration_events(
    store: StateStore,
) -> None:
    service = GoalService(store)
    first = service.create_goal(project_id="project-1", intent="First isolated goal")
    second = service.create_goal(project_id="project-1", intent="Second isolated goal")
    first_engine = engine(
        store,
        SequenceEvaluator([incomplete("first")]),
        SequenceVerifier([verify_success()]),
        ImmediateDispatcher(),
    )

    await first_engine.run(first["id"])

    first_context = first_engine._context(first["id"])
    second_context = first_engine._context(second["id"])
    assert any(event["kind"] == "goalPlanCreated" for event in first_context.events)
    assert all(event["entity_id"] != first["id"] for event in second_context.events)
    assert second_context.tasks == ()


async def test_soft_pause_allows_active_work_then_resume_continues(store: StateStore) -> None:
    service = GoalService(store)
    goal = service.create_goal(project_id="project-1", intent="pause safely")
    dispatcher = ControlledDispatcher()
    evaluator = SequenceEvaluator([incomplete("before"), incomplete("after")])
    verifier = SequenceVerifier([verify_incomplete("after"), verify_success()])
    iteration_engine = engine(store, evaluator, verifier, dispatcher)

    running = asyncio.create_task(iteration_engine.run(goal["id"]))
    await dispatcher.started.wait()
    service.pause(goal["id"], PauseMode.SOFT, reason="operator review")
    dispatcher.release.set()
    paused = await running

    assert paused["state"] == GoalState.SOFT_PAUSED.value
    assert not dispatcher.cancelled
    assert verifier.calls == 0
    service.resume(goal["id"], reason="continue")
    completed = await iteration_engine.run(goal["id"])
    assert completed["termination_reason"] == TerminationReason.SUCCESS.value


@pytest.mark.parametrize("control", ["hard", "stop"])
async def test_hard_pause_and_stop_propagate_cancellation(store: StateStore, control: str) -> None:
    service = GoalService(store)
    goal = service.create_goal(project_id="project-1", intent=f"test {control}")
    dispatcher = ControlledDispatcher()
    iteration_engine = engine(
        store,
        SequenceEvaluator([incomplete("start")]),
        SequenceVerifier([verify_incomplete("cancelled")]),
        dispatcher,
    )

    running = asyncio.create_task(iteration_engine.run(goal["id"]))
    await dispatcher.started.wait()
    if control == "hard":
        service.pause(goal["id"], PauseMode.HARD, reason="freeze")
    else:
        service.stop(goal["id"], reason="stop now")
    result = await asyncio.wait_for(running, timeout=1)

    assert dispatcher.cancelled
    assert result["state"] == (
        GoalState.HARD_PAUSED.value if control == "hard" else GoalState.STOPPED.value
    )


async def test_cancellation_grace_bounds_loop_without_abandoning_request(
    store: StateStore,
) -> None:
    service = GoalService(store)
    goal = service.create_goal(project_id="project-1", intent="cancel durably")
    dispatcher = SlowCancellationDispatcher()
    iteration_engine = engine(
        store,
        SequenceEvaluator([incomplete("start")]),
        SequenceVerifier([verify_incomplete("cancelled")]),
        dispatcher,
        cancellation_grace_seconds=0.01,
    )

    running = asyncio.create_task(iteration_engine.run(goal["id"]))
    await dispatcher.started.wait()
    service.stop(goal["id"], reason="stop without abandoning provider cancellation")
    result = await asyncio.wait_for(running, timeout=1)

    assert result["state"] == GoalState.STOPPED.value
    assert dispatcher.cancel_started.is_set()
    assert not dispatcher.cancel_completed.is_set()
    assert not dispatcher.cancel_aborted
    cancellation_events = [
        event
        for event in store.list_events(limit=1000)
        if event["kind"] == "goalActionCancellationRequested"
    ]
    assert cancellation_events[-1]["payload"]["requestState"] == "pending"

    dispatcher.allow_cancel.set()
    await asyncio.wait_for(dispatcher.cancel_completed.wait(), timeout=1)
    assert not dispatcher.cancel_aborted
    assert dispatcher.cancelled


async def test_steer_is_visible_to_next_evaluation_without_losing_completed_work(
    store: StateStore,
) -> None:
    service = GoalService(store)
    goal = service.create_goal(project_id="project-1", intent="initial intent")
    dispatcher = ControlledDispatcher()
    evaluator = SequenceEvaluator([incomplete("start"), incomplete("steered")])
    verifier = SequenceVerifier([verify_incomplete("progress"), verify_success()])
    iteration_engine = engine(store, evaluator, verifier, dispatcher)

    running = asyncio.create_task(iteration_engine.run(goal["id"]))
    await dispatcher.started.wait()
    service.steer(goal["id"], "Add the corrected constraint", preserve_valid_work=True)
    dispatcher.release.set()
    result = await running

    assert result["termination_reason"] == TerminationReason.SUCCESS.value
    assert len(evaluator.contexts) == 2
    assert "corrected constraint" in evaluator.contexts[1].goal["effective_intent"]
    assert result["task_count"] == 2


async def test_steer_during_planning_discards_stale_decision_and_replans(
    store: StateStore,
) -> None:
    service = GoalService(store)
    goal = service.create_goal(project_id="project-1", intent="initial intent")
    planner = ControlledPlanner()
    evaluator = SequenceEvaluator([incomplete("before"), incomplete("after")])
    verifier = SequenceVerifier([verify_success()])
    iteration_engine = AutonomousIterationEngine(
        store=store,
        evaluator=evaluator,
        planner=planner,
        dispatcher=ImmediateDispatcher(),
        verifier=verifier,
        control_poll_seconds=0.005,
    )

    running = asyncio.create_task(iteration_engine.run(goal["id"]))
    await planner.started.wait()
    service.steer(goal["id"], "Replan with the corrected constraint")
    planner.release.set()
    result = await running

    assert result["termination_reason"] == TerminationReason.SUCCESS.value
    assert result["iteration_count"] == 2
    assert result["task_count"] == 1
    assert len(evaluator.contexts) == 2
    assert "corrected constraint" in evaluator.contexts[1].goal["effective_intent"]
    assert any(
        event["kind"] == "goalSteerReevaluationScheduled"
        for event in iteration_engine._context(goal["id"]).events
    )


async def test_no_progress_guard_terminates_without_infinite_retry(store: StateStore) -> None:
    service = GoalService(store)
    goal = service.create_goal(
        project_id="project-1",
        intent="detect a stuck loop",
        budgets=GoalBudget(no_progress_limit=2, max_iterations=10),
    )
    evaluator = SequenceEvaluator([incomplete("same") for _ in range(3)])
    verifier = SequenceVerifier([verify_incomplete("same") for _ in range(2)])

    result = await engine(store, evaluator, verifier, ImmediateDispatcher()).run(goal["id"])

    assert result["termination_reason"] == TerminationReason.NO_PROGRESS.value
    assert result["iteration_count"] == 3
    assert result["task_count"] == 2


async def test_running_action_is_recovered_from_persisted_checkpoint(store: StateStore) -> None:
    service = GoalService(store)
    goal = service.create_goal(project_id="project-1", intent="survive restart")
    first_dispatcher = ControlledDispatcher()
    first = engine(
        store,
        SequenceEvaluator([incomplete("start")]),
        SequenceVerifier([verify_success()]),
        first_dispatcher,
    )
    crashed = asyncio.create_task(first.run(goal["id"]))
    await first_dispatcher.started.wait()
    crashed.cancel()
    with pytest.raises(asyncio.CancelledError):
        await crashed

    recovered_dispatcher = ImmediateDispatcher()
    recovered = engine(
        store,
        SequenceEvaluator([]),
        SequenceVerifier([verify_success()]),
        recovered_dispatcher,
    )
    result = await recovered.run(goal["id"])

    assert result["termination_reason"] == TerminationReason.SUCCESS.value
    assert recovered_dispatcher.recovered
    assert result["iteration_count"] == 1


def test_budget_validation_and_unknown_observed_budget_fail_closed(store: StateStore) -> None:
    with pytest.raises(ValueError):
        GoalBudget(max_iterations=0)


async def test_iteration_limit_is_a_durable_terminal_reason(store: StateStore) -> None:
    service = GoalService(store)
    goal = service.create_goal(
        project_id="project-1",
        intent="bounded loop",
        budgets=GoalBudget(max_iterations=1),
    )
    result = await engine(
        store,
        SequenceEvaluator([incomplete("one")]),
        SequenceVerifier([verify_incomplete("two")]),
        ImmediateDispatcher(),
    ).run(goal["id"])
    assert result["termination_reason"] == TerminationReason.ITERATION_LIMIT.value


async def test_elapsed_budget_stops_and_cancels_blocked_inflight_collection(
    store: StateStore,
) -> None:
    service = GoalService(store)
    goal = service.create_goal(
        project_id="project-1",
        intent="do not wait forever for an action",
        budgets=GoalBudget(max_elapsed_seconds=1.1),
    )
    dispatcher = NonCooperativeDispatcher()

    result = await engine(
        store,
        SequenceEvaluator([incomplete("waiting")]),
        SequenceVerifier([verify_success()]),
        dispatcher,
    ).run(goal["id"])

    assert result["termination_reason"] == TerminationReason.BUDGET_LIMIT.value
    assert "during work" in result["termination_detail"]
    assert dispatcher.cancelled


async def test_configured_token_budget_fails_closed_for_unmetered_actions(
    store: StateStore,
) -> None:
    service = GoalService(store)
    goal = service.create_goal(
        project_id="project-1",
        intent="never treat missing usage as zero",
        budgets=GoalBudget(max_total_tokens=100, max_iterations=5),
    )
    result = await engine(
        store,
        SequenceEvaluator([incomplete("before"), incomplete("after")]),
        SequenceVerifier([verify_incomplete("after")]),
        ImmediateDispatcher(),
    ).run(goal["id"])

    assert result["termination_reason"] == TerminationReason.BUDGET_LIMIT.value
    assert "cannot be safely observed" in result["termination_detail"]
    assert result["task_count"] == 1


async def test_runtime_dispatcher_routes_generated_task_through_hybrid_engine(
    tmp_path: Path,
) -> None:
    state = StateStore(tmp_path / "runtime.db")
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    state.create_project(
        project_id="project-1",
        name="Runtime autonomy fixture",
        root_path=str(workspace),
        goal="route an autonomous action",
    )
    state.upsert_node(
        node_id="node-1",
        hostname="fixture",
        display_name="Fixture",
        role="control",
        state=NodeState.ONLINE,
    )
    registry = AdapterRegistry()
    state.upsert_worker(
        WorkerSnapshot(
            id="worker-1",
            node_id="node-1",
            harness=Harness.MOCK,
            provider=Provider.MOCK,
            model=ModelDescriptor("mock", "Mock", Provider.MOCK),
            state=WorkerState.IDLE,
            node_state=NodeState.ONLINE,
            resource_state=ResourceState.AVAILABLE,
            capabilities=frozenset({"analysis"}),
            code_write_allowed=False,
            privacy_allowed=True,
        )
    )
    registry.register("worker-1", MockAdapter(MockBehavior(text="WORKER_RESULT")))
    runtime = SupervisorRuntime(
        store=state,
        scheduler=DeterministicScheduler(),
        adapters=registry,
        evidence_root=tmp_path / "evidence",
    )
    service = GoalService(state)
    goal = service.create_goal(project_id="project-1", intent="complete real runtime path")
    iteration_engine = AutonomousIterationEngine(
        store=state,
        evaluator=SequenceEvaluator([incomplete("start")]),
        planner=IterationPlanner(),
        dispatcher=SupervisorRuntimeDispatcher(runtime),
        verifier=SequenceVerifier([verify_success()]),
    )

    result = await iteration_engine.run(goal["id"])

    assert result["termination_reason"] == TerminationReason.SUCCESS.value
    tasks = state.list_tasks("project-1")
    assert len(tasks) == 1
    assert tasks[0]["state"] == "succeeded"
    assert len(state.list_routing_decisions(tasks[0]["id"])) == 1
    assert state.list_worker_runs(tasks[0]["id"])[0]["state"] == "completed"
    assert state.list_verifications(tasks[0]["id"])[0]["kind"] == "workerExitPolicy"


async def test_evaluator_satisfaction_requires_independent_verification_and_terminates_once(
    store: StateStore,
) -> None:
    service = GoalService(store)
    goal = service.create_goal(project_id="project-1", intent="verify evaluator completion")
    verifier = RecordingVerifier(verify_success("independently-verified"))
    iteration_engine = AutonomousIterationEngine(
        store=store,
        evaluator=SequenceEvaluator([evaluator_satisfied()]),
        planner=IterationPlanner(),
        dispatcher=ImmediateDispatcher(),
        verifier=verifier,
    )

    first = await iteration_engine.run_once(goal["id"])
    second = await iteration_engine.run_once(goal["id"])

    assert first["termination_reason"] == TerminationReason.SUCCESS.value
    assert second["termination_reason"] == TerminationReason.SUCCESS.value
    assert verifier.results == [()]
    termination_events = [
        event
        for event in store.list_events(limit=200)
        if event["kind"] == "goalTerminated" and event["entity_id"] == goal["id"]
    ]
    assert len(termination_events) == 1


async def test_persisted_evaluator_satisfaction_resumes_through_verifier_once(
    store: StateStore,
) -> None:
    service = GoalService(store)
    goal = service.create_goal(project_id="project-1", intent="resume evaluator checkpoint")
    seed = AutonomousIterationEngine(
        store=store,
        evaluator=SequenceEvaluator([]),
        planner=IterationPlanner(),
        dispatcher=ImmediateDispatcher(),
        verifier=RecordingVerifier(verify_success()),
    )
    iteration = persist_satisfied_evaluation(seed, goal["id"])

    restarted_store = StateStore(store.path)
    evaluator = SequenceEvaluator([])
    verifier = RecordingVerifier(verify_success("verified-after-restart"))
    recovered = AutonomousIterationEngine(
        store=restarted_store,
        evaluator=evaluator,
        planner=IterationPlanner(),
        dispatcher=ImmediateDispatcher(),
        verifier=verifier,
    )

    first = await recovered.run_once(goal["id"])
    second = await recovered.run_once(goal["id"])

    assert first["termination_reason"] == TerminationReason.SUCCESS.value
    assert second["termination_reason"] == TerminationReason.SUCCESS.value
    assert evaluator.contexts == []
    assert verifier.results == [()]
    with restarted_store.connect() as connection:
        restored = connection.execute(
            "SELECT state,verification_json FROM autonomous_iterations WHERE id=?",
            (iteration["id"],),
        ).fetchone()
    assert restored["state"] == "completed"
    assert json.loads(restored["verification_json"])["steerVersion"] == 0
    assert (
        len(
            [
                event
                for event in restarted_store.list_events(limit=200)
                if event["kind"] == "goalTerminated" and event["entity_id"] == goal["id"]
            ]
        )
        == 1
    )


async def test_persisted_verification_resumes_outcome_without_second_verifier_call(
    store: StateStore,
) -> None:
    service = GoalService(store)
    goal = service.create_goal(project_id="project-1", intent="resume verifier checkpoint")
    seed_verifier = RecordingVerifier(verify_success())
    seed = AutonomousIterationEngine(
        store=store,
        evaluator=SequenceEvaluator([]),
        planner=IterationPlanner(),
        dispatcher=ImmediateDispatcher(),
        verifier=seed_verifier,
    )
    iteration = persist_satisfied_evaluation(seed, goal["id"])
    assert seed._record_verification(
        iteration["id"],
        verify_success("persisted-verification"),
        expected_steer_version=0,
    )

    restarted_store = StateStore(store.path)
    recovered_verifier = RecordingVerifier(verify_success("must-not-run"))
    recovered = AutonomousIterationEngine(
        store=restarted_store,
        evaluator=SequenceEvaluator([]),
        planner=IterationPlanner(),
        dispatcher=ImmediateDispatcher(),
        verifier=recovered_verifier,
    )

    first = await recovered.run_once(goal["id"])
    second = await recovered.run_once(goal["id"])

    assert first["termination_reason"] == TerminationReason.SUCCESS.value
    assert second["termination_reason"] == TerminationReason.SUCCESS.value
    assert seed_verifier.results == []
    assert recovered_verifier.results == []
    assert (
        len(
            [
                event
                for event in restarted_store.list_events(limit=200)
                if event["kind"] == "goalVerified" and event["entity_id"] == iteration["id"]
            ]
        )
        == 1
    )
    assert (
        len(
            [
                event
                for event in restarted_store.list_events(limit=200)
                if event["kind"] == "goalTerminated" and event["entity_id"] == goal["id"]
            ]
        )
        == 1
    )


async def test_persisted_evaluation_is_fenced_when_human_steers_before_resume(
    store: StateStore,
) -> None:
    service = GoalService(store)
    goal = service.create_goal(project_id="project-1", intent="initial checkpoint intent")
    seed = AutonomousIterationEngine(
        store=store,
        evaluator=SequenceEvaluator([]),
        planner=IterationPlanner(),
        dispatcher=ImmediateDispatcher(),
        verifier=RecordingVerifier(verify_success()),
    )
    stale_iteration = persist_satisfied_evaluation(seed, goal["id"])
    service.steer(goal["id"], "Use the newly authorized recovery boundary")

    restarted_store = StateStore(store.path)
    evaluator = SequenceEvaluator([evaluator_satisfied("fresh-evaluation")])
    verifier = RecordingVerifier(verify_success("fresh-verification"))
    recovered = AutonomousIterationEngine(
        store=restarted_store,
        evaluator=evaluator,
        planner=IterationPlanner(),
        dispatcher=ImmediateDispatcher(),
        verifier=verifier,
    )

    after_stale_checkpoint = await recovered.run_once(goal["id"])

    assert after_stale_checkpoint["state"] == GoalState.RUNNING.value
    assert verifier.results == []
    assert evaluator.contexts == []
    with restarted_store.connect() as connection:
        stale = connection.execute(
            "SELECT state FROM autonomous_iterations WHERE id=?", (stale_iteration["id"],)
        ).fetchone()
    assert stale["state"] == "interrupted"

    completed = await recovered.run_once(goal["id"])

    assert completed["termination_reason"] == TerminationReason.SUCCESS.value
    assert verifier.results == [()]
    assert len(evaluator.contexts) == 1
    assert "newly authorized recovery boundary" in evaluator.contexts[0].goal["effective_intent"]
    assert any(
        event["kind"] == "goalSteerReevaluationScheduled"
        and event["entity_id"] == stale_iteration["id"]
        for event in restarted_store.list_events(limit=200)
    )


async def test_legacy_evaluation_checkpoint_without_steer_version_fails_closed(
    store: StateStore,
) -> None:
    service = GoalService(store)
    goal = service.create_goal(project_id="project-1", intent="legacy checkpoint")
    seed = AutonomousIterationEngine(
        store=store,
        evaluator=SequenceEvaluator([]),
        planner=IterationPlanner(),
        dispatcher=ImmediateDispatcher(),
        verifier=RecordingVerifier(verify_success()),
    )
    seed._start(goal["id"])
    service.steer(goal["id"], "Guidance already exists before this legacy checkpoint")
    iteration = seed._begin_iteration(goal["id"])
    assert seed._record_evaluation(
        iteration["id"],
        evaluator_satisfied("legacy-ambiguous"),
        expected_steer_version=1,
    )
    with store.transaction() as connection:
        row = connection.execute(
            "SELECT evaluation_json FROM autonomous_iterations WHERE id=?", (iteration["id"],)
        ).fetchone()
        legacy_checkpoint = json.loads(row["evaluation_json"])
        legacy_checkpoint.pop("steerVersion")
        connection.execute(
            "UPDATE autonomous_iterations SET evaluation_json=? WHERE id=?",
            (json.dumps(legacy_checkpoint), iteration["id"]),
        )

    verifier = RecordingVerifier(verify_success("must-not-approve-legacy"))
    recovered = AutonomousIterationEngine(
        store=StateStore(store.path),
        evaluator=SequenceEvaluator([]),
        planner=IterationPlanner(),
        dispatcher=ImmediateDispatcher(),
        verifier=verifier,
    )
    result = await recovered.run_once(goal["id"])

    assert result["state"] == GoalState.RUNNING.value
    assert verifier.results == []
    assert recovered._get_iteration(iteration["id"])["state"] == "interrupted"
    assert any(
        event["kind"] == "goalSteerReevaluationScheduled"
        and event["payload"]["observedSteerVersion"] == -1
        for event in recovered._context(goal["id"]).events
    )


@pytest.mark.parametrize("active_kind", ["task", "run"])
async def test_satisfied_verification_cannot_terminate_with_active_canonical_work(
    store: StateStore,
    active_kind: str,
) -> None:
    service = GoalService(store)
    goal = service.create_goal(project_id="project-1", intent="wait for canonical work")
    seed = AutonomousIterationEngine(
        store=store,
        evaluator=SequenceEvaluator([]),
        planner=IterationPlanner(),
        dispatcher=ImmediateDispatcher(),
        verifier=RecordingVerifier(verify_success()),
    )
    iteration = persist_satisfied_evaluation(seed, goal["id"])
    assert seed._record_plan(
        iteration["id"],
        goal["id"],
        GoalPlan(
            summary="associate canonical work",
            actions=(
                PlannedAction(
                    key="canonical-work",
                    title="Canonical work",
                    description="must reach a terminal state before goal completion",
                ),
            ),
        ),
        expected_steer_version=0,
    )
    task_state = TaskState.READY if active_kind == "task" else TaskState.SUCCEEDED
    store.create_task(
        TaskRecord(
            id=f"active-{active_kind}",
            project_id="project-1",
            title="Associated canonical work",
            description="completion fence fixture",
            state=task_state,
            topology=ExecutionTopology.SINGLE,
            requirements=TaskRequirements(labels=frozenset({TaskLabel.REVIEW})),
        ),
        f"#{active_kind}",
    )
    with store.transaction() as connection:
        connection.execute(
            "UPDATE autonomous_actions SET task_id=?,state='completed' WHERE iteration_id=?",
            (f"active-{active_kind}", iteration["id"]),
        )
    if active_kind == "run":
        store.upsert_node(
            node_id="active-node",
            hostname="fixture",
            display_name="Active fixture",
            role="worker",
            state=NodeState.ONLINE,
        )
        store.upsert_worker(
            WorkerSnapshot(
                id="active-worker",
                node_id="active-node",
                harness=Harness.MOCK,
                provider=Provider.MOCK,
                model=ModelDescriptor("mock", "Mock", Provider.MOCK),
                state=WorkerState.IDLE,
                node_state=NodeState.ONLINE,
                resource_state=ResourceState.AVAILABLE,
                capabilities=frozenset({"review"}),
                code_write_allowed=False,
                privacy_allowed=True,
            )
        )
        store.create_worker_run(task_id="active-run", worker_id="active-worker", attempt=1)
    assert seed._record_verification(
        iteration["id"],
        verify_success("canonical-work-still-active"),
        expected_steer_version=0,
    )

    verifier = RecordingVerifier(verify_success("must-not-run"))
    recovered_store = StateStore(store.path)
    recovered = AutonomousIterationEngine(
        store=recovered_store,
        evaluator=SequenceEvaluator([]),
        planner=IterationPlanner(),
        dispatcher=ImmediateDispatcher(),
        verifier=verifier,
        control_poll_seconds=0.001,
    )
    result = await recovered.run_once(goal["id"])

    assert result["state"] == GoalState.RUNNING.value
    assert result["termination_reason"] is None
    assert verifier.results == []
    assert not any(
        event["kind"] == "goalTerminated" and event["entity_id"] == goal["id"]
        for event in recovered_store.list_events(limit=200)
    )


async def test_evaluator_satisfaction_rejected_by_verifier_remains_nonterminal(
    store: StateStore,
) -> None:
    service = GoalService(store)
    goal = service.create_goal(project_id="project-1", intent="reject premature completion")
    verifier = RecordingVerifier(verify_incomplete("verification-rejected"))
    iteration_engine = AutonomousIterationEngine(
        store=store,
        evaluator=SequenceEvaluator([evaluator_satisfied()]),
        planner=IterationPlanner(),
        dispatcher=ImmediateDispatcher(),
        verifier=verifier,
    )

    result = await iteration_engine.run_once(goal["id"])

    assert result["state"] == GoalState.RUNNING.value
    assert result["termination_reason"] is None
    assert verifier.results == [()]
    with store.connect() as connection:
        iteration = connection.execute(
            "SELECT state,verification_json FROM autonomous_iterations WHERE goal_id=?",
            (goal["id"],),
        ).fetchone()
    assert iteration["state"] == "completed"
    assert iteration["verification_json"] is not None


async def test_runtime_dispatcher_serializes_sibling_actions_on_one_worker_without_false_block(
    tmp_path: Path,
) -> None:
    state = StateStore(tmp_path / "capacity.db")
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    state.create_project(
        project_id="project-1",
        name="Capacity fixture",
        root_path=str(workspace),
        goal="serialize autonomous actions on bounded Worker capacity",
    )
    state.upsert_node(
        node_id="node-1",
        hostname="fixture",
        display_name="Fixture",
        role="control",
        state=NodeState.ONLINE,
    )
    state.upsert_worker(
        WorkerSnapshot(
            id="worker-1",
            node_id="node-1",
            harness=Harness.MOCK,
            provider=Provider.MOCK,
            model=ModelDescriptor("mock", "Mock", Provider.MOCK),
            state=WorkerState.IDLE,
            node_state=NodeState.ONLINE,
            resource_state=ResourceState.AVAILABLE,
            capabilities=frozenset({"analysis"}),
            code_write_allowed=False,
            privacy_allowed=True,
        )
    )
    registry = AdapterRegistry()
    registry.register("worker-1", MockAdapter(MockBehavior(text="SERIAL_OK", delay_seconds=0.05)))
    runtime = SupervisorRuntime(
        store=state,
        scheduler=DeterministicScheduler(),
        adapters=registry,
        evidence_root=tmp_path / "evidence",
    )
    dispatcher = SupervisorRuntimeDispatcher(runtime)
    context = SimpleNamespace(goal={"project_id": "project-1"})
    action = PlannedAction(
        key="capacity-action",
        title="Bounded action",
        description="use the single analysis Worker",
        payload={"requiredCapabilities": ["analysis"]},
    )

    first = await dispatcher.dispatch(context, action, "capacity-action-1")
    second = await dispatcher.dispatch(context, action, "capacity-action-2")
    assert second.task_id is not None
    assert state.get_task(second.task_id)["state"] == "ready"
    assert any(
        event["kind"] == "dispatchDeferred" and event["task_id"] == second.task_id
        for event in state.list_events(limit=200)
    )

    first_result, second_result = await asyncio.gather(
        dispatcher.collect(first), dispatcher.collect(second)
    )

    assert first_result.succeeded and second_result.succeeded
    assert [state.get_task(handle.task_id)["state"] for handle in (first, second)] == [
        "succeeded",
        "succeeded",
    ]
    runs = state.list_worker_runs()
    assert {run["task_id"]: run["state"] for run in runs} == {
        first.task_id: "completed",
        second.task_id: "completed",
    }
    assert not any(task["state"] == "blocked" for task in state.list_tasks("project-1"))


async def test_planner_cannot_self_approve_a_red_action(tmp_path: Path) -> None:
    state = StateStore(tmp_path / "red.db")
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    state.create_project(
        project_id="project-1",
        name="RED autonomy fixture",
        root_path=str(workspace),
        goal="keep approval human-owned",
    )
    runtime = SupervisorRuntime(
        store=state,
        scheduler=DeterministicScheduler(),
        adapters=AdapterRegistry(),
        evidence_root=tmp_path / "evidence",
    )
    dispatcher = SupervisorRuntimeDispatcher(runtime)
    action = PlannedAction(
        key="red-action",
        title="Guarded work",
        description="Do not run without a human approval record",
        payload={"permissionClass": "red", "approvalState": "approved"},
    )

    handle = await dispatcher.dispatch(
        SimpleNamespace(goal={"project_id": "project-1"}), action, "action-red"
    )

    assert handle.task_id is not None
    assert state.get_task(handle.task_id)["state"] == "blocked"
    with state.connect() as connection:
        approval = connection.execute(
            "SELECT state,requested_by FROM approvals WHERE task_id=?", (handle.task_id,)
        ).fetchone()
    assert dict(approval) == {"state": "pending", "requested_by": "runtime"}
    assert state.list_worker_runs(handle.task_id) == []
