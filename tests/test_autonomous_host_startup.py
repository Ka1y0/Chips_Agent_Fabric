from __future__ import annotations

import asyncio
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest

from project_supervisor import autonomous_host as host_module
from project_supervisor.autonomous_host import (
    AutonomousHost,
    AutonomousHostConfig,
    GoalLeaseLost,
)
from project_supervisor.autonomy import AutonomousIterationEngine, GoalService
from project_supervisor.store import StateStore


@dataclass
class Clock:
    value: datetime

    def advance(self, seconds: float) -> None:
        self.value += timedelta(seconds=seconds)


@pytest.fixture
def clock(monkeypatch: pytest.MonkeyPatch) -> Clock:
    clock = Clock(datetime(2030, 1, 1, tzinfo=UTC))

    class ControlledDateTime(datetime):
        @classmethod
        def now(cls, tz: Any = None) -> datetime:
            return clock.value.astimezone(tz) if tz else clock.value.replace(tzinfo=None)

    monkeypatch.setattr(host_module, "datetime", ControlledDateTime)
    return clock


@pytest.fixture
def state(tmp_path: Path, clock: Clock) -> tuple[StateStore, str]:
    del clock
    store = StateStore(tmp_path / "state.db")
    store.create_project(
        project_id="startup-project",
        name="Startup fixture",
        root_path=str(tmp_path),
        goal="Exercise the leased initialization boundary",
    )
    goal = GoalService(store).create_goal(project_id="startup-project", intent="bounded work")
    return store, str(goal["id"])


class RecordingEngine(AutonomousIterationEngine):
    """Host/engine seam only; this fixture never pretends to execute a provider."""

    def __init__(self) -> None:
        self.guard: Callable[[], None] | None = None
        self.calls: list[str] = []

    def set_ownership_guard(self, guard: Callable[[], None]) -> None:
        self.guard = guard

    async def run(self, goal_id: str) -> dict[str, Any]:
        self.calls.append(goal_id)
        assert self.guard is not None
        self.guard()
        return {"id": goal_id}


def make_host(store: StateStore, factory: Any) -> AutonomousHost:
    return AutonomousHost(
        store=store,
        engine_factory=factory,
        host_id="startup-host",
        process_id=103,
        config=AutonomousHostConfig(
            poll_interval_seconds=0.01,
            heartbeat_interval_seconds=0.005,
            lease_ttl_seconds=10,
            shutdown_grace_seconds=0.01,
        ),
    )


@pytest.mark.parametrize(
    "field",
    [
        "poll_interval_seconds",
        "heartbeat_interval_seconds",
        "lease_ttl_seconds",
        "shutdown_grace_seconds",
    ],
)
@pytest.mark.parametrize(
    "value",
    [float("nan"), float("inf"), -float("inf"), 0, -1, True, "1", None, 10**400],
    ids=["nan", "inf", "negative-inf", "zero", "negative", "bool", "text", "none", "huge-int"],
)
def test_invalid_host_timing_fails_before_startup(field: str, value: Any) -> None:
    with pytest.raises(ValueError, match=field):
        AutonomousHostConfig(**{field: value})


@pytest.mark.parametrize("value", [0, -1, True, 1.5, "2", None])
def test_concurrency_requires_a_positive_integer(value: Any) -> None:
    with pytest.raises(ValueError, match="max_concurrent_goals"):
        AutonomousHostConfig(max_concurrent_goals=value)


def test_finite_integer_and_fractional_durations_remain_compatible() -> None:
    config = AutonomousHostConfig(
        max_concurrent_goals=2,
        poll_interval_seconds=1,
        heartbeat_interval_seconds=0.5,
        lease_ttl_seconds=10,
        shutdown_grace_seconds=1.5,
    )
    assert config.lease_ttl_seconds == 10


async def test_async_initialization_is_renewed_across_multiple_lease_windows(
    state: tuple[StateStore, str], clock: Clock, monkeypatch: pytest.MonkeyPatch
) -> None:
    store, goal_id = state
    engine = RecordingEngine()
    third_renewal = asyncio.Event()
    renewals = 0

    async def factory(_: str) -> RecordingEngine:
        await third_renewal.wait()
        return engine

    host = make_host(store, factory)
    original_heartbeat = host.repository.heartbeat_goal

    def renew(claim: Any, *, lease_ttl_seconds: float) -> bool:
        nonlocal renewals
        # Virtual time crosses the initial 10-second lease without a sleep race.
        clock.advance(4)
        accepted = original_heartbeat(claim, lease_ttl_seconds=lease_ttl_seconds)
        assert accepted
        renewals += 1
        if renewals == 3:
            third_renewal.set()
        return accepted

    monkeypatch.setattr(host.repository, "heartbeat_goal", renew)
    async with asyncio.timeout(3):
        result = await host.run_goal(goal_id)
    assert result["id"] == goal_id
    assert renewals >= 3
    assert engine.calls == [goal_id]
    lease = host.repository.get_goal_lease(goal_id)
    assert lease is not None
    assert lease["generation"] == 1
    assert lease["state"] == "released"
    kinds = [event["kind"] for event in store.list_events(limit=1000)]
    assert kinds.count("autonomousGoalLeaseAcquired") == 1
    assert "autonomousGoalLeaseLost" not in kinds


async def test_expired_claim_does_not_even_invoke_factory(
    state: tuple[StateStore, str], clock: Clock
) -> None:
    store, goal_id = state
    calls: list[str] = []

    def factory(goal: str) -> RecordingEngine:
        calls.append(goal)
        return RecordingEngine()

    host = make_host(store, factory)
    host._ensure_registered()
    claim = host.repository.try_acquire_goal(goal_id, host.host_id, lease_ttl_seconds=10)
    assert claim is not None
    clock.advance(11)
    with pytest.raises(GoalLeaseLost):
        await host._run_claimed(claim)
    assert calls == []
    assert host.repository.get_goal_lease(goal_id)["state"] == "lost"


async def test_factory_return_after_expiry_never_enters_engine(
    state: tuple[StateStore, str], clock: Clock
) -> None:
    store, goal_id = state
    engine = RecordingEngine()

    def factory(_: str) -> RecordingEngine:
        # Models a synchronous factory that blocked past its lease, without sleeping.
        clock.advance(11)
        return engine

    host = make_host(store, factory)
    with pytest.raises(GoalLeaseLost):
        await host.run_goal(goal_id)
    assert engine.calls == []
    assert host.repository.get_goal_lease(goal_id)["state"] == "lost"


async def test_factory_return_after_takeover_cannot_mutate_new_owner(
    state: tuple[StateStore, str], clock: Clock
) -> None:
    store, goal_id = state
    engine = RecordingEngine()
    successor_row: dict[str, Any] | None = None

    async def factory(_: str) -> RecordingEngine:
        nonlocal successor_row
        clock.advance(11)
        host.repository.register_host("successor-host", process_id=104)
        claim = host.repository.try_acquire_goal(
            goal_id, "successor-host", lease_ttl_seconds=10
        )
        assert claim is not None and claim.generation == 2
        successor_row = host.repository.get_goal_lease(goal_id)
        return engine

    host = make_host(store, factory)
    with pytest.raises(GoalLeaseLost):
        await host.run_goal(goal_id)
    assert engine.calls == []
    assert host.repository.get_goal_lease(goal_id) == successor_row


@pytest.mark.parametrize("suppress_cancellation", [False, True])
async def test_ownership_loss_cancels_pending_factory_without_late_execution(
    state: tuple[StateStore, str], clock: Clock, suppress_cancellation: bool
) -> None:
    store, goal_id = state
    engine = RecordingEngine()
    started = asyncio.Event()
    finalized = asyncio.Event()
    never = asyncio.Event()

    async def factory(_: str) -> RecordingEngine:
        started.set()
        try:
            await never.wait()
        except asyncio.CancelledError:
            if not suppress_cancellation:
                raise
        finally:
            finalized.set()
        return engine

    host = make_host(store, factory)
    task = asyncio.create_task(host.run_goal(goal_id))
    try:
        async with asyncio.timeout(3):
            await started.wait()
            clock.advance(11)
            with pytest.raises(GoalLeaseLost):
                await task
        assert finalized.is_set()
        assert engine.calls == []
        assert host.repository.get_goal_lease(goal_id)["state"] == "lost"
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)


@pytest.mark.parametrize("suppress_cancellation", [False, True])
@pytest.mark.parametrize("shutdown", [False, True])
async def test_shutdown_or_caller_cancellation_cleans_up_initialization(
    state: tuple[StateStore, str], shutdown: bool, suppress_cancellation: bool
) -> None:
    store, goal_id = state
    started = asyncio.Event()
    finalized = asyncio.Event()
    engine = RecordingEngine()

    async def factory(_: str) -> RecordingEngine:
        started.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            if not suppress_cancellation:
                raise
        finally:
            finalized.set()
        return engine

    host = make_host(store, factory)
    task = asyncio.create_task(host.run_goal(goal_id))
    try:
        async with asyncio.timeout(3):
            await started.wait()
            if shutdown:
                host.request_shutdown()
                result = await task
                assert result["id"] == goal_id
            else:
                task.cancel()
                with pytest.raises(asyncio.CancelledError):
                    await task
        assert finalized.is_set()
        assert engine.calls == []
        assert host.repository.get_goal_lease(goal_id)["state"] == "released"
        assert GoalService(store).get_goal(goal_id)["termination_reason"] is None
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)


@pytest.mark.parametrize("wrong_type", [False, True])
async def test_bad_factory_releases_claim_without_starting_work(
    state: tuple[StateStore, str], wrong_type: bool
) -> None:
    store, goal_id = state

    async def factory(_: str) -> Any:
        if wrong_type:
            return object()
        raise RuntimeError("fixture initialization failure")

    host = make_host(store, factory)
    expected_error = TypeError if wrong_type else RuntimeError
    with pytest.raises(expected_error):
        await host.run_goal(goal_id)
    lease = host.repository.get_goal_lease(goal_id)
    assert lease is not None and lease["state"] == "released"
    assert GoalService(store).get_goal(goal_id)["task_count"] == 0
