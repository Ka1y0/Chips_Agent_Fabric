from __future__ import annotations

import asyncio
from collections import deque
from pathlib import Path
from typing import Any

import pytest

from project_supervisor.autonomous_host import (
    AutonomousHost,
    AutonomousHostConfig,
    AutonomousHostRepository,
    GoalLeaseUnavailable,
    HostIdentityInUse,
)
from project_supervisor.autonomy import (
    ActionResult,
    AutonomousIterationEngine,
    DispatchHandle,
    EvaluationDisposition,
    GoalEvaluation,
    GoalPlan,
    GoalService,
    GoalState,
    GoalVerification,
    PauseMode,
    PlannedAction,
    TerminationReason,
)
from project_supervisor.store import StateStore


@pytest.fixture
def store(tmp_path: Path) -> StateStore:
    value = StateStore(tmp_path / "state.db")
    value.create_project(
        project_id="project-1",
        name="Host fixture",
        root_path=str(tmp_path),
        goal="exercise the autonomous host",
    )
    return value


class SequenceEvaluator:
    def __init__(self, *values: GoalEvaluation) -> None:
        self.values = deque(values)

    async def evaluate(self, context: Any) -> GoalEvaluation:
        del context
        return self.values.popleft()


class OneActionPlanner:
    async def plan(self, context: Any, evaluation: GoalEvaluation) -> GoalPlan:
        del evaluation
        sequence = context.goal["iteration_count"]
        return GoalPlan(
            summary=f"iteration {sequence}",
            actions=(
                PlannedAction(
                    key=f"work-{sequence}",
                    title="Work",
                    description="Perform deterministic work",
                ),
            ),
        )


class SequenceVerifier:
    def __init__(self, *values: GoalVerification) -> None:
        self.values = deque(values)

    async def verify(self, context: Any, results: tuple[ActionResult, ...]) -> GoalVerification:
        del context
        assert results
        return self.values.popleft()


class ImmediateDispatcher:
    def __init__(self) -> None:
        self.recovered: list[str] = []

    async def dispatch(self, context: Any, action: PlannedAction, action_id: str) -> DispatchHandle:
        del context, action
        return DispatchHandle(reference=action_id)

    async def collect(self, handle: DispatchHandle) -> ActionResult:
        return ActionResult(True, f"completed {handle.reference}")

    async def cancel(self, handle: DispatchHandle) -> bool:
        del handle
        return True

    async def recover(
        self, context: Any, action: PlannedAction, handle: DispatchHandle
    ) -> DispatchHandle:
        del context, action
        self.recovered.append(handle.reference)
        return handle


class BlockingDispatcher(ImmediateDispatcher):
    def __init__(self) -> None:
        super().__init__()
        self.started = asyncio.Event()
        self.release = asyncio.Event()
        self.cancelled = asyncio.Event()

    async def collect(self, handle: DispatchHandle) -> ActionResult:
        self.started.set()
        released = asyncio.create_task(self.release.wait())
        cancelled = asyncio.create_task(self.cancelled.wait())
        try:
            done, pending = await asyncio.wait(
                {released, cancelled}, return_when=asyncio.FIRST_COMPLETED
            )
            for task in pending:
                task.cancel()
            was_cancelled = cancelled in done
            return ActionResult(
                not was_cancelled,
                "cancelled" if was_cancelled else f"completed {handle.reference}",
                cancelled=was_cancelled,
            )
        finally:
            for task in (released, cancelled):
                if not task.done():
                    task.cancel()

    async def cancel(self, handle: DispatchHandle) -> bool:
        del handle
        self.cancelled.set()
        return True


def incomplete(fingerprint: str = "incomplete") -> GoalEvaluation:
    return GoalEvaluation(
        EvaluationDisposition.INCOMPLETE,
        "goal remains incomplete",
        fingerprint,
    )


def success(fingerprint: str = "complete") -> GoalVerification:
    return GoalVerification(True, "goal is verified", fingerprint)


def make_engine(
    store: StateStore,
    *,
    dispatcher: ImmediateDispatcher | None = None,
    verifier: SequenceVerifier | None = None,
) -> AutonomousIterationEngine:
    return AutonomousIterationEngine(
        store=store,
        evaluator=SequenceEvaluator(incomplete()),
        planner=OneActionPlanner(),
        dispatcher=dispatcher or ImmediateDispatcher(),
        verifier=verifier or SequenceVerifier(success()),
        control_poll_seconds=0.005,
    )


def make_custom_engine(
    store: StateStore,
    *,
    evaluator: SequenceEvaluator,
    dispatcher: ImmediateDispatcher,
    verifier: SequenceVerifier,
) -> AutonomousIterationEngine:
    return AutonomousIterationEngine(
        store=store,
        evaluator=evaluator,
        planner=OneActionPlanner(),
        dispatcher=dispatcher,
        verifier=verifier,
        control_poll_seconds=0.005,
    )


def host_config(**overrides: Any) -> AutonomousHostConfig:
    values = {
        "max_concurrent_goals": 2,
        "poll_interval_seconds": 0.01,
        "heartbeat_interval_seconds": 0.02,
        "lease_ttl_seconds": 0.2,
        "shutdown_grace_seconds": 0.03,
    }
    values.update(overrides)
    return AutonomousHostConfig(**values)


async def wait_until(predicate: Any, timeout: float = 1.0) -> None:
    async with asyncio.timeout(timeout):
        while not predicate():
            await asyncio.sleep(0.005)


async def test_host_auto_advances_created_goal_and_persists_observability(
    store: StateStore,
) -> None:
    goal = GoalService(store).create_goal(project_id="project-1", intent="finish unattended")
    host = AutonomousHost(
        store=store,
        engine_factory=lambda _goal_id: make_engine(store),
        config=host_config(),
        host_id="host-primary",
        process_id=1001,
    )
    serving = asyncio.create_task(host.serve())

    await wait_until(
        lambda: GoalService(store).get_goal(goal["id"])["state"] == GoalState.TERMINATED.value
    )
    host.request_shutdown()
    await serving

    completed = GoalService(store).get_goal(goal["id"])
    assert completed["termination_reason"] == TerminationReason.SUCCESS.value
    assert completed["iteration_count"] == 1
    assert host.repository.get_host("host-primary")["state"] == "stopped"
    lease = host.repository.get_goal_lease(goal["id"])
    assert lease is not None
    assert lease["state"] == "released"
    assert lease["recovery_state"] == "complete"


async def test_live_lease_rejects_second_host_and_stale_lease_is_recoverable(
    store: StateStore,
) -> None:
    goal = GoalService(store).create_goal(project_id="project-1", intent="exclusive work")
    blocking = BlockingDispatcher()
    first = AutonomousHost(
        store=store,
        engine_factory=lambda _goal_id: make_engine(store, dispatcher=blocking),
        config=host_config(shutdown_grace_seconds=0.01),
        host_id="host-one",
        process_id=1001,
    )
    second = AutonomousHost(
        store=store,
        engine_factory=lambda _goal_id: make_engine(store),
        config=host_config(),
        host_id="host-two",
        process_id=1002,
    )
    first_run = asyncio.create_task(first.run_goal(goal["id"]))
    await blocking.started.wait()

    with pytest.raises(GoalLeaseUnavailable):
        await second.run_goal(goal["id"])

    first_run.cancel()
    await asyncio.gather(first_run, return_exceptions=True)
    repository = AutonomousHostRepository(store)
    first_claim = repository.try_acquire_goal(goal["id"], "host-one", lease_ttl_seconds=1.0)
    assert first_claim is not None
    with store.transaction() as connection:
        connection.execute(
            "UPDATE autonomous_goal_leases SET state='owned',expires_at=? WHERE goal_id=?",
            ("2000-01-01T00:00:00.000000Z", goal["id"]),
        )
    recovered = repository.try_acquire_goal(goal["id"], "host-two", lease_ttl_seconds=1.0)
    assert recovered is not None
    assert recovered.stale_owner_recovered
    assert recovered.generation > first_claim.generation
    assert recovered.recovery_state == "staleOwnerRecovered"
    kinds = [event["kind"] for event in store.list_events(limit=1000)]
    assert "autonomousGoalLeaseRecovered" in kinds


def test_live_host_identity_cannot_be_reused_by_another_process(store: StateStore) -> None:
    repository = AutonomousHostRepository(store)
    repository.register_host("host-shared", process_id=1001, stale_after_seconds=1.0)
    repository.heartbeat_host("host-shared", active_goal_count=0)

    with pytest.raises(HostIdentityInUse, match="live heartbeat"):
        repository.register_host("host-shared", process_id=2002, stale_after_seconds=1.0)


async def test_serve_ignores_paused_and_stopped_then_observes_resume(store: StateStore) -> None:
    service = GoalService(store)
    paused = service.create_goal(project_id="project-1", intent="wait for resume")
    stopped = service.create_goal(project_id="project-1", intent="never run")
    service.pause(paused["id"], PauseMode.SOFT, reason="operator checkpoint")
    service.stop(stopped["id"], reason="operator stopped")
    factory_calls: list[str] = []

    def factory(goal_id: str) -> AutonomousIterationEngine:
        factory_calls.append(goal_id)
        return make_engine(store)

    host = AutonomousHost(
        store=store,
        engine_factory=factory,
        config=host_config(),
        host_id="host-controls",
        process_id=1003,
    )
    serving = asyncio.create_task(host.serve())
    await asyncio.sleep(0.05)
    assert factory_calls == []

    service.resume(paused["id"], reason="continue")
    await wait_until(lambda: service.get_goal(paused["id"])["state"] == GoalState.TERMINATED.value)
    host.request_shutdown()
    await serving

    assert factory_calls == [paused["id"]]
    assert service.get_goal(stopped["id"])["state"] == GoalState.STOPPED.value


async def test_hard_pause_cancels_and_resume_is_rediscovered(store: StateStore) -> None:
    service = GoalService(store)
    goal = service.create_goal(project_id="project-1", intent="pause cancellable work")
    blocking = BlockingDispatcher()
    verifier = SequenceVerifier(success("resumed"))
    calls = 0

    def factory(_goal_id: str) -> AutonomousIterationEngine:
        nonlocal calls
        calls += 1
        dispatcher = blocking if calls == 1 else ImmediateDispatcher()
        return make_engine(store, dispatcher=dispatcher, verifier=verifier)

    host = AutonomousHost(
        store=store,
        engine_factory=factory,
        config=host_config(),
        host_id="host-hard-pause",
        process_id=1004,
    )
    serving = asyncio.create_task(host.serve())
    await blocking.started.wait()
    service.pause(goal["id"], PauseMode.HARD, reason="freeze")
    await wait_until(
        lambda: (
            service.get_goal(goal["id"])["state"] == GoalState.HARD_PAUSED.value
            and blocking.cancelled.is_set()
        )
    )
    await wait_until(
        lambda: (host.repository.get_goal_lease(goal["id"]) or {}).get("state") == "released"
    )

    service.resume(goal["id"], reason="continue from checkpoint")
    await wait_until(lambda: service.get_goal(goal["id"])["state"] == GoalState.TERMINATED.value)
    host.request_shutdown()
    await serving

    assert calls == 2
    assert service.get_goal(goal["id"])["termination_reason"] == "SUCCESS"


async def test_soft_pause_finishes_work_without_cancel_then_resume_replans(
    store: StateStore,
) -> None:
    service = GoalService(store)
    goal = service.create_goal(project_id="project-1", intent="pause after safe work")
    blocking = BlockingDispatcher()
    evaluator = SequenceEvaluator(incomplete("first"), incomplete("second"))
    verifier = SequenceVerifier(
        GoalVerification(False, "more work", "after-first"), success("done")
    )
    calls = 0

    def factory(_goal_id: str) -> AutonomousIterationEngine:
        nonlocal calls
        calls += 1
        return make_custom_engine(
            store,
            evaluator=evaluator,
            dispatcher=blocking if calls == 1 else ImmediateDispatcher(),
            verifier=verifier,
        )

    host = AutonomousHost(
        store=store,
        engine_factory=factory,
        config=host_config(),
        host_id="host-soft-pause",
        process_id=1007,
    )
    serving = asyncio.create_task(host.serve())
    await blocking.started.wait()
    service.pause(goal["id"], PauseMode.SOFT, reason="checkpoint")
    blocking.release.set()
    await wait_until(
        lambda: (host.repository.get_goal_lease(goal["id"]) or {}).get("state") == "released"
    )
    assert service.get_goal(goal["id"])["state"] == GoalState.SOFT_PAUSED.value
    assert not blocking.cancelled.is_set()

    service.resume(goal["id"], reason="continue")
    await wait_until(lambda: service.get_goal(goal["id"])["state"] == GoalState.TERMINATED.value)
    host.request_shutdown()
    await serving

    assert calls == 2
    assert service.get_goal(goal["id"])["termination_reason"] == "SUCCESS"


async def test_live_stop_requests_cancellation_and_is_durable(store: StateStore) -> None:
    service = GoalService(store)
    goal = service.create_goal(project_id="project-1", intent="stop running work")
    blocking = BlockingDispatcher()
    host = AutonomousHost(
        store=store,
        engine_factory=lambda _goal_id: make_engine(store, dispatcher=blocking),
        config=host_config(),
        host_id="host-stop",
        process_id=1008,
    )
    serving = asyncio.create_task(host.serve())
    await blocking.started.wait()
    service.stop(goal["id"], reason="operator stop")
    await wait_until(blocking.cancelled.is_set)
    await wait_until(
        lambda: (host.repository.get_goal_lease(goal["id"]) or {}).get("state") == "released"
    )
    host.request_shutdown()
    await serving

    stopped = service.get_goal(goal["id"])
    assert stopped["state"] == GoalState.STOPPED.value
    assert stopped["termination_reason"] == TerminationReason.USER_STOPPED.value


async def test_shutdown_releases_lease_and_new_host_recovers_checkpoint(store: StateStore) -> None:
    service = GoalService(store)
    goal = service.create_goal(project_id="project-1", intent="survive host restart")
    blocking = BlockingDispatcher()
    first = AutonomousHost(
        store=store,
        engine_factory=lambda _goal_id: make_engine(store, dispatcher=blocking),
        config=host_config(shutdown_grace_seconds=0.01),
        host_id="host-before-restart",
        process_id=1005,
    )
    first_serving = asyncio.create_task(first.serve())
    await blocking.started.wait()
    first.request_shutdown()
    await first_serving
    lease = first.repository.get_goal_lease(goal["id"])
    assert lease is not None and lease["state"] == "released"
    assert service.get_goal(goal["id"])["state"] == GoalState.RUNNING.value

    recovered_dispatcher = ImmediateDispatcher()
    second = AutonomousHost(
        store=store,
        engine_factory=lambda _goal_id: make_engine(
            store, dispatcher=recovered_dispatcher, verifier=SequenceVerifier(success())
        ),
        config=host_config(),
        host_id="host-after-restart",
        process_id=1006,
    )
    second_serving = asyncio.create_task(second.serve())
    await wait_until(lambda: service.get_goal(goal["id"])["state"] == GoalState.TERMINATED.value)
    second.request_shutdown()
    await second_serving

    assert recovered_dispatcher.recovered
    assert service.get_goal(goal["id"])["termination_reason"] == "SUCCESS"


async def test_bounded_concurrency_runs_only_configured_number_of_goals(
    store: StateStore,
) -> None:
    service = GoalService(store)
    first_goal = service.create_goal(project_id="project-1", intent="first", goal_id="goal-a")
    second_goal = service.create_goal(project_id="project-1", intent="second", goal_id="goal-b")
    dispatchers = {
        first_goal["id"]: BlockingDispatcher(),
        second_goal["id"]: BlockingDispatcher(),
    }
    factory_calls: list[str] = []

    def factory(goal_id: str) -> AutonomousIterationEngine:
        factory_calls.append(goal_id)
        return make_engine(store, dispatcher=dispatchers[goal_id])

    host = AutonomousHost(
        store=store,
        engine_factory=factory,
        config=host_config(max_concurrent_goals=1),
        host_id="host-bounded",
        process_id=1009,
    )
    serving = asyncio.create_task(host.serve())
    await dispatchers[first_goal["id"]].started.wait()
    await wait_until(
        lambda: (
            (host.repository.get_goal_lease(first_goal["id"]) or {}).get("current_action_id")
            is not None
        )
    )
    assert factory_calls == [first_goal["id"]]
    status = host.status()
    assert status["active_goal_count"] == 1
    assert status["active_goals"][0]["current_iteration_id"] is not None
    assert status["active_goals"][0]["current_action_id"] is not None
    assert status["active_goals"][0]["in_flight_state"] == "running"

    dispatchers[first_goal["id"]].release.set()
    await dispatchers[second_goal["id"]].started.wait()
    assert factory_calls == [first_goal["id"], second_goal["id"]]
    dispatchers[second_goal["id"]].release.set()
    await wait_until(
        lambda: service.get_goal(second_goal["id"])["state"] == GoalState.TERMINATED.value
    )
    host.request_shutdown()
    await serving


async def test_factory_failure_releases_lease_for_safe_retry(store: StateStore) -> None:
    goal = GoalService(store).create_goal(project_id="project-1", intent="factory failure")

    def broken_factory(_goal_id: str) -> AutonomousIterationEngine:
        raise RuntimeError("driver unavailable")

    host = AutonomousHost(
        store=store,
        engine_factory=broken_factory,
        config=host_config(),
        host_id="host-broken-factory",
        process_id=1010,
    )
    with pytest.raises(RuntimeError, match="driver unavailable"):
        await host.run_goal(goal["id"])

    lease = host.repository.get_goal_lease(goal["id"])
    assert lease is not None
    assert lease["state"] == "released"
    assert "driver unavailable" in lease["last_error"]


def test_migration_exposes_host_and_goal_lease_tables(store: StateStore) -> None:
    with store.connect() as connection:
        tables = {
            row["name"]
            for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()
        }
        version = connection.execute(
            "SELECT 1 FROM schema_migrations WHERE version='0005_autonomous_host'"
        ).fetchone()
    assert {"autonomous_hosts", "autonomous_goal_leases"} <= tables
    assert version is not None
