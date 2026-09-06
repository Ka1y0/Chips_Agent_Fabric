from __future__ import annotations

import asyncio
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest

from project_supervisor import autonomous_host as host_module
from project_supervisor.autonomous_host import (
    AutonomousHost,
    AutonomousHostRepository,
    GoalLeaseLost,
)
from project_supervisor.autonomy import AutonomousIterationEngine, GoalService
from project_supervisor.store import StateStore


@dataclass
class LeaseClock:
    value: datetime

    def advance(self, seconds: float) -> None:
        self.value += timedelta(seconds=seconds)

    def text(self, offset: float = 0) -> str:
        value = self.value + timedelta(seconds=offset)
        return value.isoformat(timespec="microseconds").replace("+00:00", "Z")


@pytest.fixture
def clock(monkeypatch: pytest.MonkeyPatch) -> LeaseClock:
    clock = LeaseClock(datetime(2030, 1, 1, tzinfo=UTC))

    class ControlledDateTime(datetime):
        @classmethod
        def now(cls, tz: Any = None) -> datetime:
            return clock.value.astimezone(tz) if tz else clock.value.replace(tzinfo=None)

    monkeypatch.setattr(host_module, "datetime", ControlledDateTime)
    return clock


@pytest.fixture
def lease_state(
    tmp_path: Path, clock: LeaseClock
) -> tuple[StateStore, AutonomousHostRepository, str]:
    del clock
    store = StateStore(tmp_path / "state.db")
    store.create_project(
        project_id="lease-project",
        name="Lease boundary fixture",
        root_path=str(tmp_path),
        goal="Verify durable lease boundaries",
    )
    goal = GoalService(store).create_goal(project_id="lease-project", intent="bounded work")
    repository = AutonomousHostRepository(store)
    repository.register_host("owner-one", process_id=101)
    repository.register_host("owner-two", process_id=102)
    return store, repository, str(goal["id"])


@pytest.mark.parametrize("elapsed", [10, 10.000001, 100])
def test_expired_heartbeat_cannot_resurrect_generation(
    lease_state: tuple[StateStore, AutonomousHostRepository, str],
    clock: LeaseClock,
    elapsed: float,
) -> None:
    store, repository, goal_id = lease_state
    claim = repository.try_acquire_goal(goal_id, "owner-one", lease_ttl_seconds=10)
    assert claim is not None
    before = repository.get_goal_lease(goal_id)
    events_before = store.list_events(limit=1000)
    clock.advance(elapsed)

    assert repository.heartbeat_goal(claim, lease_ttl_seconds=10) is False
    assert repository.get_goal_lease(goal_id) == before
    assert store.list_events(limit=1000) == events_before
    with pytest.raises(GoalLeaseLost):
        repository.assert_goal_lease(claim)


@pytest.mark.parametrize("next_owner", ["owner-one", "owner-two"])
def test_live_renewal_then_expiry_requires_a_new_generation(
    lease_state: tuple[StateStore, AutonomousHostRepository, str],
    clock: LeaseClock,
    next_owner: str,
) -> None:
    _, repository, goal_id = lease_state
    first = repository.try_acquire_goal(goal_id, "owner-one", lease_ttl_seconds=10)
    assert first is not None
    clock.advance(9)
    assert repository.heartbeat_goal(first, lease_ttl_seconds=10) is True
    renewed = repository.get_goal_lease(goal_id)
    assert renewed is not None
    assert renewed["expires_at"] == clock.text(10)
    assert renewed["generation"] == first.generation

    clock.advance(10)
    second = repository.try_acquire_goal(goal_id, next_owner, lease_ttl_seconds=10)
    assert second is not None
    assert second.generation == first.generation + 1
    assert second.stale_owner_recovered
    current = repository.get_goal_lease(goal_id)
    assert repository.heartbeat_goal(first, lease_ttl_seconds=10) is False
    assert repository.release_goal(first) is False
    assert repository.get_goal_lease(goal_id) == current
    with pytest.raises(GoalLeaseLost):
        repository.assert_goal_lease(first)
    repository.assert_goal_lease(second)


def test_new_lease_clock_starts_after_write_lock_acquisition(
    lease_state: tuple[StateStore, AutonomousHostRepository, str],
    clock: LeaseClock,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store, repository, goal_id = lease_state
    original_transaction = store.transaction

    @contextmanager
    def delayed_transaction() -> Iterator[Any]:
        with original_transaction() as connection:
            # Deterministically model a write-lock wait, without sleeping.
            clock.advance(30)
            yield connection

    with monkeypatch.context() as patch:
        patch.setattr(store, "transaction", delayed_transaction)
        claim = repository.try_acquire_goal(goal_id, "owner-one", lease_ttl_seconds=10)
    assert claim is not None
    lease = repository.get_goal_lease(goal_id)
    assert lease is not None
    assert lease["acquired_at"] == clock.text()
    assert lease["expires_at"] == clock.text(10)
    repository.assert_goal_lease(claim)


@pytest.mark.parametrize("delay_at", ["observation", "transaction"])
def test_heartbeat_rechecks_expiry_after_waits(
    lease_state: tuple[StateStore, AutonomousHostRepository, str],
    clock: LeaseClock,
    monkeypatch: pytest.MonkeyPatch,
    delay_at: str,
) -> None:
    store, repository, goal_id = lease_state
    claim = repository.try_acquire_goal(goal_id, "owner-one", lease_ttl_seconds=10)
    assert claim is not None
    before = repository.get_goal_lease(goal_id)
    original_transaction = store.transaction
    original_position = repository._goal_position

    @contextmanager
    def delayed_transaction() -> Iterator[Any]:
        with original_transaction() as connection:
            clock.advance(11)
            yield connection

    def delayed_position(goal: str) -> tuple[str | None, str | None, str | None]:
        position = original_position(goal)
        clock.advance(11)
        return position

    with monkeypatch.context() as patch:
        if delay_at == "transaction":
            patch.setattr(store, "transaction", delayed_transaction)
        else:
            patch.setattr(repository, "_goal_position", delayed_position)
        assert repository.heartbeat_goal(claim, lease_ttl_seconds=10) is False
    assert repository.get_goal_lease(goal_id) == before


def test_contender_rechecks_expiry_after_write_lock_wait(
    lease_state: tuple[StateStore, AutonomousHostRepository, str],
    clock: LeaseClock,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store, repository, goal_id = lease_state
    first = repository.try_acquire_goal(goal_id, "owner-one", lease_ttl_seconds=10)
    assert first is not None
    original_transaction = store.transaction

    @contextmanager
    def delayed_transaction() -> Iterator[Any]:
        with original_transaction() as connection:
            clock.advance(11)
            yield connection

    with monkeypatch.context() as patch:
        patch.setattr(store, "transaction", delayed_transaction)
        second = repository.try_acquire_goal(goal_id, "owner-two", lease_ttl_seconds=10)
    assert second is not None
    assert second.stale_owner_recovered
    assert second.generation == first.generation + 1
    repository.assert_goal_lease(second)


@pytest.mark.parametrize("check_in_engine", [True, False])
async def test_ownership_loss_is_never_journaled_as_normal_completion(
    lease_state: tuple[StateStore, AutonomousHostRepository, str],
    clock: LeaseClock,
    check_in_engine: bool,
) -> None:
    store, repository, goal_id = lease_state

    class ExpiringEngine(AutonomousIterationEngine):
        def __init__(self) -> None:
            # Only the host/engine ownership seam is exercised in this fixture.
            self.guard: Callable[[], None] | None = None

        def set_ownership_guard(self, guard: Callable[[], None]) -> None:
            self.guard = guard

        async def run(self, goal: str) -> dict[str, Any]:
            clock.advance(11)
            if check_in_engine:
                assert self.guard is not None
                self.guard()
            return {"id": goal}

    host = AutonomousHost(
        store=store,
        engine_factory=lambda _: ExpiringEngine(),
        host_id="expiring-host",
        process_id=103,
    )
    async with asyncio.timeout(2):
        with pytest.raises(GoalLeaseLost):
            await host.run_goal(goal_id)
    lease = repository.get_goal_lease(goal_id)
    assert lease is not None
    assert lease["state"] == "lost"
    assert lease["recovery_state"] == "lost"
    kinds = [event["kind"] for event in store.list_events(limit=1000)]
    assert kinds.count("autonomousGoalLeaseLost") == 1
    assert "autonomousGoalLeaseReleased" not in kinds
    assert GoalService(store).get_goal(goal_id)["termination_reason"] is None
