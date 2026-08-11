from __future__ import annotations

import asyncio
import inspect
import json
import os
import uuid
from collections.abc import Awaitable, Callable
from contextlib import suppress
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from enum import StrEnum
from typing import Any, Protocol

from .autonomy import AutonomousIterationEngine, GoalService, GoalState
from .domain import EventSeverity
from .store import StateStore, compact_json, redact_sensitive


class AutonomousHostState(StrEnum):
    STARTING = "starting"
    RUNNING = "running"
    STOPPING = "stopping"
    STOPPED = "stopped"
    FAILED = "failed"


class GoalLeaseUnavailable(RuntimeError):
    """Another live autonomous host owns the Goal lease."""


class GoalLeaseLost(RuntimeError):
    """The host can no longer prove ownership of the Goal lease."""


class HostIdentityInUse(RuntimeError):
    """A different live process already owns the requested autonomous host identity."""


@dataclass(frozen=True, slots=True)
class AutonomousHostConfig:
    max_concurrent_goals: int = 2
    poll_interval_seconds: float = 1.0
    heartbeat_interval_seconds: float = 2.0
    lease_ttl_seconds: float = 10.0
    shutdown_grace_seconds: float = 10.0

    def __post_init__(self) -> None:
        if self.max_concurrent_goals < 1:
            raise ValueError("max_concurrent_goals must be positive")
        for name in (
            "poll_interval_seconds",
            "heartbeat_interval_seconds",
            "lease_ttl_seconds",
            "shutdown_grace_seconds",
        ):
            if getattr(self, name) <= 0:
                raise ValueError(f"{name} must be positive")
        if self.lease_ttl_seconds <= self.heartbeat_interval_seconds:
            raise ValueError("lease_ttl_seconds must exceed heartbeat_interval_seconds")


@dataclass(frozen=True, slots=True)
class GoalLeaseClaim:
    goal_id: str
    host_id: str
    generation: int
    recovery_state: str
    stale_owner_recovered: bool


class EngineFactory(Protocol):
    def __call__(
        self, goal_id: str
    ) -> AutonomousIterationEngine | Awaitable[AutonomousIterationEngine]: ...


def _time(value: datetime | None = None) -> str:
    """Comparable UTC timestamp with enough precision for short production leases."""

    return (value or datetime.now(UTC)).isoformat(timespec="microseconds").replace("+00:00", "Z")


def _expires(now: datetime, seconds: float) -> str:
    return _time(now + timedelta(seconds=seconds))


class AutonomousHostRepository:
    """Transactional persistence for host identity, observability, and fenced Goal leases."""

    def __init__(self, store: StateStore) -> None:
        self.store = store

    def register_host(
        self,
        host_id: str,
        *,
        process_id: int,
        stale_after_seconds: float = 10.0,
        metadata: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        if not host_id.strip():
            raise ValueError("host_id is required")
        if process_id <= 0:
            raise ValueError("process_id must be positive")
        if stale_after_seconds <= 0:
            raise ValueError("stale_after_seconds must be positive")
        now = _time()
        stale_before = _time(datetime.now(UTC) - timedelta(seconds=stale_after_seconds))
        safe_metadata = redact_sensitive(metadata or {})
        with self.store.transaction() as connection:
            existing = connection.execute(
                "SELECT state,heartbeat_at,process_id FROM autonomous_hosts WHERE host_id=?",
                (host_id,),
            ).fetchone()
            if existing is None:
                connection.execute(
                    "INSERT INTO autonomous_hosts("
                    "host_id,process_id,state,started_at,heartbeat_at,metadata_json"
                    ") VALUES (?,?,?,?,?,?)",
                    (
                        host_id,
                        process_id,
                        AutonomousHostState.STARTING.value,
                        now,
                        now,
                        compact_json(safe_metadata),
                    ),
                )
            else:
                if (
                    existing["state"]
                    in {
                        AutonomousHostState.STARTING.value,
                        AutonomousHostState.RUNNING.value,
                        AutonomousHostState.STOPPING.value,
                    }
                    and existing["heartbeat_at"] > stale_before
                ):
                    raise HostIdentityInUse(
                        f"autonomous host identity {host_id} has a live heartbeat"
                    )
                connection.execute(
                    "UPDATE autonomous_hosts SET process_id=?,state=?,started_at=?,"
                    "heartbeat_at=?,stopped_at=NULL,active_goal_count=0,last_error=NULL,"
                    "metadata_json=? WHERE host_id=?",
                    (
                        process_id,
                        AutonomousHostState.STARTING.value,
                        now,
                        now,
                        compact_json(safe_metadata),
                        host_id,
                    ),
                )
            self.store._append_event(
                connection,
                kind="autonomousHostStarted",
                severity=EventSeverity.NOTICE,
                entity_type="autonomousHost",
                entity_id=host_id,
                summary="Production autonomous host registered",
                payload={"processID": process_id},
                actor=f"autonomous-host:{host_id}",
            )
        return self.get_host(host_id)

    def heartbeat_host(
        self,
        host_id: str,
        *,
        active_goal_count: int,
        state: AutonomousHostState = AutonomousHostState.RUNNING,
        last_error: str | None = None,
    ) -> bool:
        if active_goal_count < 0:
            raise ValueError("active_goal_count cannot be negative")
        with self.store.transaction() as connection:
            cursor = connection.execute(
                "UPDATE autonomous_hosts SET state=?,heartbeat_at=?,active_goal_count=?,"
                "last_error=COALESCE(?,last_error) WHERE host_id=?",
                (
                    state.value,
                    _time(),
                    active_goal_count,
                    redact_sensitive(last_error),
                    host_id,
                ),
            )
        return cursor.rowcount == 1

    def stop_host(
        self,
        host_id: str,
        *,
        failed: bool = False,
        last_error: str | None = None,
    ) -> None:
        now = _time()
        state = AutonomousHostState.FAILED if failed else AutonomousHostState.STOPPED
        with self.store.transaction() as connection:
            connection.execute(
                "UPDATE autonomous_hosts SET state=?,heartbeat_at=?,stopped_at=?,"
                "active_goal_count=0,last_error=COALESCE(?,last_error) WHERE host_id=?",
                (state.value, now, now, redact_sensitive(last_error), host_id),
            )
            self.store._append_event(
                connection,
                kind="autonomousHostStopped",
                severity=EventSeverity.ERROR if failed else EventSeverity.NOTICE,
                entity_type="autonomousHost",
                entity_id=host_id,
                summary="Production autonomous host stopped",
                payload={"state": state.value, "error": redact_sensitive(last_error)},
                actor=f"autonomous-host:{host_id}",
            )

    def get_host(self, host_id: str) -> dict[str, Any]:
        with self.store.connect() as connection:
            row = connection.execute(
                "SELECT * FROM autonomous_hosts WHERE host_id=?", (host_id,)
            ).fetchone()
        if row is None:
            raise KeyError(host_id)
        result = dict(row)
        result["metadata"] = json.loads(result.pop("metadata_json"))
        return result

    def list_hosts(self) -> list[dict[str, Any]]:
        with self.store.connect() as connection:
            ids = [
                row["host_id"]
                for row in connection.execute(
                    "SELECT host_id FROM autonomous_hosts ORDER BY started_at,host_id"
                ).fetchall()
            ]
        return [self.get_host(host_id) for host_id in ids]

    def eligible_goal_ids(self, *, limit: int) -> list[str]:
        if limit < 1:
            return []
        now = _time()
        with self.store.connect() as connection:
            rows = connection.execute(
                "SELECT g.id FROM autonomous_goals g "
                "LEFT JOIN autonomous_goal_leases lease ON lease.goal_id=g.id "
                "WHERE g.state IN ('created','running') AND (lease.goal_id IS NULL "
                "OR lease.state<>'owned' OR lease.expires_at<=?) "
                "ORDER BY g.updated_at,g.id LIMIT ?",
                (now, limit),
            ).fetchall()
        return [str(row["id"]) for row in rows]

    def try_acquire_goal(
        self,
        goal_id: str,
        host_id: str,
        *,
        lease_ttl_seconds: float,
    ) -> GoalLeaseClaim | None:
        now_value = datetime.now(UTC)
        now = _time(now_value)
        expires_at = _expires(now_value, lease_ttl_seconds)
        with self.store.transaction() as connection:
            goal = connection.execute(
                "SELECT project_id,state FROM autonomous_goals WHERE id=?", (goal_id,)
            ).fetchone()
            if goal is None:
                raise KeyError(goal_id)
            if goal["state"] not in {GoalState.CREATED.value, GoalState.RUNNING.value}:
                return None
            if (
                connection.execute(
                    "SELECT 1 FROM autonomous_hosts WHERE host_id=?", (host_id,)
                ).fetchone()
                is None
            ):
                raise KeyError(host_id)
            lease = connection.execute(
                "SELECT * FROM autonomous_goal_leases WHERE goal_id=?", (goal_id,)
            ).fetchone()
            stale = bool(
                lease is not None and lease["state"] == "owned" and lease["expires_at"] <= now
            )
            if lease is not None and lease["state"] == "owned" and lease["expires_at"] > now:
                return None
            active_iteration = connection.execute(
                "SELECT id FROM autonomous_iterations WHERE goal_id=? "
                "AND state NOT IN ('completed','interrupted') "
                "ORDER BY sequence DESC LIMIT 1",
                (goal_id,),
            ).fetchone()
            recovery_state = (
                "staleOwnerRecovered"
                if stale
                else "resuming"
                if active_iteration is not None or goal["state"] == GoalState.RUNNING.value
                else "fresh"
            )
            generation = int(lease["generation"]) + 1 if lease is not None else 1
            previous_host_id = lease["host_id"] if lease is not None else None
            if lease is None:
                connection.execute(
                    "INSERT INTO autonomous_goal_leases("
                    "goal_id,host_id,generation,state,acquired_at,heartbeat_at,expires_at,"
                    "recovery_state,previous_host_id"
                    ") VALUES (?,?,?,?,?,?,?,?,?)",
                    (
                        goal_id,
                        host_id,
                        generation,
                        "owned",
                        now,
                        now,
                        expires_at,
                        recovery_state,
                        previous_host_id,
                    ),
                )
            else:
                connection.execute(
                    "UPDATE autonomous_goal_leases SET host_id=?,generation=?,state='owned',"
                    "acquired_at=?,heartbeat_at=?,expires_at=?,released_at=NULL,"
                    "current_iteration_id=NULL,current_action_id=NULL,in_flight_state=NULL,"
                    "recovery_state=?,previous_host_id=?,last_error=NULL WHERE goal_id=?",
                    (
                        host_id,
                        generation,
                        now,
                        now,
                        expires_at,
                        recovery_state,
                        previous_host_id,
                        goal_id,
                    ),
                )
            self.store._append_event(
                connection,
                kind=("autonomousGoalLeaseRecovered" if stale else "autonomousGoalLeaseAcquired"),
                severity=EventSeverity.WARNING if stale else EventSeverity.INFO,
                entity_type="autonomousGoalLease",
                entity_id=goal_id,
                project_id=goal["project_id"],
                summary=(
                    "Expired autonomous Goal lease recovered"
                    if stale
                    else "Autonomous Goal lease acquired"
                ),
                payload={
                    "hostID": host_id,
                    "generation": generation,
                    "recoveryState": recovery_state,
                    "previousHostID": previous_host_id,
                },
                actor=f"autonomous-host:{host_id}",
            )
        return GoalLeaseClaim(goal_id, host_id, generation, recovery_state, stale)

    def heartbeat_goal(
        self,
        claim: GoalLeaseClaim,
        *,
        lease_ttl_seconds: float,
    ) -> bool:
        now_value = datetime.now(UTC)
        iteration_id, action_id, in_flight = self._goal_position(claim.goal_id)
        with self.store.transaction() as connection:
            cursor = connection.execute(
                "UPDATE autonomous_goal_leases SET heartbeat_at=?,expires_at=?,"
                "current_iteration_id=?,current_action_id=?,in_flight_state=? "
                "WHERE goal_id=? AND host_id=? AND generation=? AND state='owned'",
                (
                    _time(now_value),
                    _expires(now_value, lease_ttl_seconds),
                    iteration_id,
                    action_id,
                    in_flight,
                    claim.goal_id,
                    claim.host_id,
                    claim.generation,
                ),
            )
        return cursor.rowcount == 1

    def assert_goal_lease(self, claim: GoalLeaseClaim) -> None:
        """Fence a Goal mutation to the current unexpired lease generation."""

        with self.store.connect() as connection:
            row = connection.execute(
                "SELECT 1 FROM autonomous_goal_leases WHERE goal_id=? AND host_id=? "
                "AND generation=? AND state='owned' AND expires_at>?",
                (claim.goal_id, claim.host_id, claim.generation, _time()),
            ).fetchone()
        if row is None:
            raise GoalLeaseLost(
                f"host {claim.host_id} no longer owns generation {claim.generation} "
                f"for goal {claim.goal_id}"
            )

    def release_goal(
        self,
        claim: GoalLeaseClaim,
        *,
        error: str | None = None,
        lost: bool = False,
    ) -> bool:
        now = _time()
        state = "lost" if lost else "released"
        recovery = "lost" if lost else "complete"
        with self.store.transaction() as connection:
            cursor = connection.execute(
                "UPDATE autonomous_goal_leases SET state=?,heartbeat_at=?,expires_at=?,"
                "released_at=?,in_flight_state=NULL,recovery_state=?,last_error=? "
                "WHERE goal_id=? AND host_id=? AND generation=? AND state='owned'",
                (
                    state,
                    now,
                    now,
                    now,
                    recovery,
                    redact_sensitive(error),
                    claim.goal_id,
                    claim.host_id,
                    claim.generation,
                ),
            )
            if cursor.rowcount:
                goal = connection.execute(
                    "SELECT project_id FROM autonomous_goals WHERE id=?", (claim.goal_id,)
                ).fetchone()
                self.store._append_event(
                    connection,
                    kind="autonomousGoalLeaseLost" if lost else "autonomousGoalLeaseReleased",
                    severity=EventSeverity.ERROR if lost else EventSeverity.INFO,
                    entity_type="autonomousGoalLease",
                    entity_id=claim.goal_id,
                    project_id=goal["project_id"] if goal else None,
                    summary=(
                        "Autonomous Goal lease ownership lost"
                        if lost
                        else "Autonomous Goal lease released"
                    ),
                    payload={
                        "hostID": claim.host_id,
                        "generation": claim.generation,
                        "error": redact_sensitive(error),
                    },
                    actor=f"autonomous-host:{claim.host_id}",
                )
        return cursor.rowcount == 1

    def get_goal_lease(self, goal_id: str) -> dict[str, Any] | None:
        with self.store.connect() as connection:
            row = connection.execute(
                "SELECT * FROM autonomous_goal_leases WHERE goal_id=?", (goal_id,)
            ).fetchone()
        return dict(row) if row else None

    def list_goal_leases(
        self, *, host_id: str | None = None, owned_only: bool = False
    ) -> list[dict[str, Any]]:
        clauses: list[str] = []
        parameters: list[Any] = []
        if host_id is not None:
            clauses.append("host_id=?")
            parameters.append(host_id)
        if owned_only:
            clauses.append("state='owned'")
        query = "SELECT * FROM autonomous_goal_leases"
        if clauses:
            query += " WHERE " + " AND ".join(clauses)
        query += " ORDER BY acquired_at,goal_id"
        with self.store.connect() as connection:
            return [dict(row) for row in connection.execute(query, parameters).fetchall()]

    def _goal_position(self, goal_id: str) -> tuple[str | None, str | None, str | None]:
        with self.store.connect() as connection:
            iteration = connection.execute(
                "SELECT id,state FROM autonomous_iterations WHERE goal_id=? "
                "AND state NOT IN ('completed','interrupted') "
                "ORDER BY sequence DESC LIMIT 1",
                (goal_id,),
            ).fetchone()
            action = connection.execute(
                "SELECT id,state FROM autonomous_actions WHERE goal_id=? "
                "AND state IN ('dispatching','running') ORDER BY updated_at DESC,id LIMIT 1",
                (goal_id,),
            ).fetchone()
        if action is not None:
            return (
                iteration["id"] if iteration else None,
                action["id"],
                str(action["state"]),
            )
        return (
            iteration["id"] if iteration else None,
            None,
            str(iteration["state"]) if iteration else "idle",
        )


class AutonomousHost:
    """Production scheduler for existing provider-neutral AutonomousIterationEngine instances."""

    def __init__(
        self,
        *,
        store: StateStore,
        engine_factory: EngineFactory,
        config: AutonomousHostConfig | None = None,
        host_id: str | None = None,
        process_id: int | None = None,
    ) -> None:
        self.store = store
        self.goals = GoalService(store)
        self.engine_factory = engine_factory
        self.config = config or AutonomousHostConfig()
        self.host_id = host_id or f"host-{uuid.uuid4()}"
        self.process_id = process_id or os.getpid()
        self.repository = AutonomousHostRepository(store)
        self._shutdown = asyncio.Event()
        self._registered = False
        self._active: dict[str, asyncio.Task[dict[str, Any]]] = {}

    def request_shutdown(self) -> None:
        """Stop acquiring work and checkpoint active Goals at their existing SQLite boundaries."""

        self._shutdown.set()

    async def run_goal(self, goal_id: str) -> dict[str, Any]:
        """Advance one eligible Goal while holding its durable lease."""

        self._ensure_registered()
        failure: BaseException | None = None
        goal_task: asyncio.Task[dict[str, Any]] | None = None
        shutdown_wait: asyncio.Task[bool] | None = None
        try:
            claim = self.repository.try_acquire_goal(
                goal_id,
                self.host_id,
                lease_ttl_seconds=self.config.lease_ttl_seconds,
            )
            if claim is None:
                raise GoalLeaseUnavailable(f"goal {goal_id} is paused, terminal, or leased")
            self.repository.heartbeat_host(self.host_id, active_goal_count=1)
            goal_task = asyncio.create_task(
                self._run_claimed(claim), name=f"autonomous-goal:{goal_id}"
            )
            shutdown_wait = asyncio.create_task(self._shutdown.wait())
            done, _ = await asyncio.wait(
                {goal_task, shutdown_wait}, return_when=asyncio.FIRST_COMPLETED
            )
            if goal_task in done:
                shutdown_wait.cancel()
                await asyncio.gather(shutdown_wait, return_exceptions=True)
                return await goal_task
            try:
                return await asyncio.wait_for(
                    asyncio.shield(goal_task), timeout=self.config.shutdown_grace_seconds
                )
            except TimeoutError:
                goal_task.cancel()
                await asyncio.gather(goal_task, return_exceptions=True)
                return self.goals.get_goal(goal_id)
        except BaseException as error:
            failure = error
            raise
        finally:
            if shutdown_wait is not None and not shutdown_wait.done():
                shutdown_wait.cancel()
                await asyncio.gather(shutdown_wait, return_exceptions=True)
            if goal_task is not None and not goal_task.done():
                goal_task.cancel()
                await asyncio.gather(goal_task, return_exceptions=True)
            self.repository.heartbeat_host(self.host_id, active_goal_count=0)
            self.repository.stop_host(
                self.host_id,
                failed=failure is not None,
                last_error=str(redact_sensitive(str(failure))) if failure else None,
            )

    async def serve(self) -> None:
        """Continuously discover eligible Goals with bounded concurrency and durable ownership."""

        self._ensure_registered()
        failure: BaseException | None = None
        try:
            while not self._shutdown.is_set():
                self._reap_finished()
                capacity = self.config.max_concurrent_goals - len(self._active)
                if capacity > 0:
                    for goal_id in self.repository.eligible_goal_ids(limit=capacity * 2):
                        if (
                            goal_id in self._active
                            or len(self._active) >= self.config.max_concurrent_goals
                        ):
                            continue
                        claim = self.repository.try_acquire_goal(
                            goal_id,
                            self.host_id,
                            lease_ttl_seconds=self.config.lease_ttl_seconds,
                        )
                        if claim is None:
                            continue
                        self._active[goal_id] = asyncio.create_task(
                            self._run_claimed(claim), name=f"autonomous-goal:{goal_id}"
                        )
                self.repository.heartbeat_host(self.host_id, active_goal_count=len(self._active))
                await self._wait_for_poll_or_shutdown()
        except BaseException as error:
            failure = error
            raise
        finally:
            await self._stop_active()
            self.repository.stop_host(
                self.host_id,
                failed=failure is not None,
                last_error=str(redact_sensitive(str(failure))) if failure else None,
            )

    def status(self) -> dict[str, Any]:
        host = self.repository.get_host(self.host_id)
        host["active_goals"] = [
            self.repository.get_goal_lease(goal_id) for goal_id in sorted(self._active)
        ]
        return host

    def _ensure_registered(self) -> None:
        if self._registered:
            return
        self.repository.register_host(
            self.host_id,
            process_id=self.process_id,
            stale_after_seconds=self.config.lease_ttl_seconds,
            metadata={"maxConcurrentGoals": self.config.max_concurrent_goals},
        )
        self.repository.heartbeat_host(self.host_id, active_goal_count=0)
        self._registered = True

    async def _run_claimed(self, claim: GoalLeaseClaim) -> dict[str, Any]:
        engine_task: asyncio.Task[dict[str, Any]] | None = None
        error: BaseException | None = None
        lost = False
        try:
            value = self.engine_factory(claim.goal_id)
            engine = await value if inspect.isawaitable(value) else value
            if not isinstance(engine, AutonomousIterationEngine):
                raise TypeError("engine_factory must return AutonomousIterationEngine")
            engine.set_ownership_guard(lambda: self.repository.assert_goal_lease(claim))
            engine_task = asyncio.create_task(engine.run(claim.goal_id))
            while not engine_task.done():
                done, _ = await asyncio.wait(
                    {engine_task},
                    timeout=self.config.heartbeat_interval_seconds,
                    return_when=asyncio.FIRST_COMPLETED,
                )
                if done:
                    break
                if not self.repository.heartbeat_goal(
                    claim, lease_ttl_seconds=self.config.lease_ttl_seconds
                ):
                    lost = True
                    engine_task.cancel()
                    await asyncio.gather(engine_task, return_exceptions=True)
                    raise GoalLeaseLost(
                        f"host {self.host_id} lost lease generation {claim.generation} "
                        f"for goal {claim.goal_id}"
                    )
            result = await engine_task
            if not self.repository.heartbeat_goal(
                claim, lease_ttl_seconds=self.config.lease_ttl_seconds
            ):
                lost = True
                raise GoalLeaseLost(
                    f"host {self.host_id} lost lease generation {claim.generation} "
                    f"for goal {claim.goal_id} at completion"
                )
            return result
        except BaseException as caught:
            error = caught
            if engine_task is not None and not engine_task.done():
                engine_task.cancel()
                await asyncio.gather(engine_task, return_exceptions=True)
            raise
        finally:
            self.repository.release_goal(
                claim,
                error=str(redact_sensitive(str(error))) if error else None,
                lost=lost,
            )

    def _reap_finished(self) -> None:
        for goal_id, task in list(self._active.items()):
            if not task.done():
                continue
            self._active.pop(goal_id)
            try:
                task.result()
            except asyncio.CancelledError:
                pass
            except Exception as error:
                self.repository.heartbeat_host(
                    self.host_id,
                    active_goal_count=len(self._active),
                    last_error=str(redact_sensitive(str(error))),
                )

    async def _wait_for_poll_or_shutdown(self) -> None:
        with suppress(TimeoutError):
            await asyncio.wait_for(self._shutdown.wait(), timeout=self.config.poll_interval_seconds)

    async def _stop_active(self) -> None:
        if not self._active:
            return
        self.repository.heartbeat_host(
            self.host_id,
            active_goal_count=len(self._active),
            state=AutonomousHostState.STOPPING,
        )
        tasks = tuple(self._active.values())
        done, pending = await asyncio.wait(tasks, timeout=self.config.shutdown_grace_seconds)
        del done
        for task in pending:
            task.cancel()
        if pending:
            await asyncio.gather(*pending, return_exceptions=True)
        self._reap_finished()


AutonomousEngineFactory = Callable[
    [str], AutonomousIterationEngine | Awaitable[AutonomousIterationEngine]
]
