from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest
from test_autonomous_host import BlockingDispatcher, host_config, make_engine

from project_supervisor import autonomous_host as host_module
from project_supervisor.autonomous_host import AutonomousHost, GoalLeaseLost
from project_supervisor.autonomy import GoalService, PauseMode
from project_supervisor.goal_inspection import inspect_goal
from project_supervisor.store import StateStore


@pytest.fixture
def store(tmp_path: Path) -> StateStore:
    value = StateStore(tmp_path / "state.db")
    value.create_project(
        project_id="controls", name="Controls", root_path=str(tmp_path), goal="Test"
    )
    return value


@pytest.fixture
def clock(monkeypatch: pytest.MonkeyPatch) -> list[datetime]:
    value = [datetime(2030, 1, 1, tzinfo=UTC)]

    class Clock(datetime):
        @classmethod
        def now(cls, tz: Any = None) -> datetime:
            return value[0].astimezone(tz) if tz else value[0].replace(tzinfo=None)

    monkeypatch.setattr(host_module, "datetime", Clock)
    return value


def control(service: GoalService, goal_id: str, mode: str) -> None:
    if mode == "stopped":
        service.stop(goal_id, reason="fixture stop")
    else:
        pause = PauseMode.SOFT if mode == "softPaused" else PauseMode.HARD
        service.pause(goal_id, pause, reason="fixture pause")


@pytest.mark.parametrize("mode", ["softPaused", "hardPaused", "stopped"])
@pytest.mark.parametrize("expired", [False, True])
async def test_control_release_and_expiry_are_distinct_durable_outcomes(
    store: StateStore, clock: list[datetime], mode: str, expired: bool,
) -> None:
    service = GoalService(store)
    goal = service.create_goal(project_id="controls", intent="Finish only authorized work")
    blocking = BlockingDispatcher()
    host = AutonomousHost(
        store=store, engine_factory=lambda _id: make_engine(store, dispatcher=blocking),
        config=host_config(), host_id="control-host", process_id=101,
    )
    running = asyncio.create_task(host.run_goal(goal["id"]))
    try:
        await asyncio.wait_for(blocking.started.wait(), timeout=1)
        control(service, goal["id"], mode)
        if expired:
            # Exact boundary, not a CPU-speed-dependent sleep or a longer lease TTL.
            clock[0] += timedelta(seconds=0.2)
        if mode == "softPaused" or expired:
            blocking.release.set()
        if expired:
            with pytest.raises(GoalLeaseLost):
                await asyncio.wait_for(running, timeout=1)
        else:
            await asyncio.wait_for(running, timeout=1)
            assert blocking.cancelled.is_set() is (mode != "softPaused")
        observed = inspect_goal(store.path, goal["id"], now=clock[0])
        assert observed["status"] == "observed"
        assert observed["goal"]["state"] == mode
        assert observed["goal"]["taskCount"] == 1
        assert observed["leaseObservation"] == ("lost" if expired else "released")
        assert observed["lease"]["generation"] == 1
        assert observed["executionQuiescence"] == "unknown"
        kinds = {event["kind"] for event in observed["events"]}
        expected = "autonomousGoalLeaseLost" if expired else "autonomousGoalLeaseReleased"
        forbidden = "autonomousGoalLeaseReleased" if expired else "autonomousGoalLeaseLost"
        assert expected in kinds
        assert forbidden not in kinds
        assert host.repository.try_acquire_goal(
            goal["id"], host.host_id, lease_ttl_seconds=0.2,
        ) is None
    finally:
        if not running.done():
            running.cancel()
        await asyncio.gather(running, return_exceptions=True)
