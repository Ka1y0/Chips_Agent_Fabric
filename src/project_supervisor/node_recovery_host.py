from __future__ import annotations

import asyncio
import json
import os
import re
import uuid
from collections.abc import Callable
from contextlib import suppress
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from enum import StrEnum
from functools import partial
from typing import Any, Protocol

from .domain import EventSeverity
from .node_recovery import (
    BoundedRuntimeRecoveryAdapter,
    NodeRuntimeRecoveryService,
    RecoveryAttemptState,
    RecoveryCapabilityAuthorizer,
    RecoveryLeaseUnavailable,
    RecoveryOutcome,
    RuntimeRecoveryPolicy,
    SideEffectFreeRuntimeProbe,
)
from .store import StateStore, compact_json, redact_sensitive

_MONITOR_ID = re.compile(r"^[A-Za-z][A-Za-z0-9._:@/-]{0,119}$")


class RecoveryMonitorState(StrEnum):
    STARTING = "starting"
    RUNNING = "running"
    STOPPING = "stopping"
    STOPPED = "stopped"
    FAILED = "failed"


class RecoveryMonitorIdentityInUse(RuntimeError):
    """A different live process already owns this recovery monitor identity."""


@dataclass(frozen=True, slots=True)
class RuntimeRecoveryBinding:
    """Process-local binding; secrets and executable authority never enter SQLite."""

    probe: SideEffectFreeRuntimeProbe
    adapter: BoundedRuntimeRecoveryAdapter
    authorizer: RecoveryCapabilityAuthorizer | None = None


class RecoveryBindingProvider(Protocol):
    def __call__(self, policy: RuntimeRecoveryPolicy) -> RuntimeRecoveryBinding | None: ...


@dataclass(frozen=True, slots=True)
class RuntimeRecoveryMonitorConfig:
    discovery_poll_seconds: float = 1.0
    host_heartbeat_seconds: float = 2.0
    host_stale_after_seconds: float = 10.0
    max_concurrent_policies: int = 2

    def __post_init__(self) -> None:
        if not 0.05 <= self.discovery_poll_seconds <= 60:
            raise ValueError("discovery_poll_seconds must be between 0.05 and 60")
        if not 0.05 <= self.host_heartbeat_seconds <= 60:
            raise ValueError("host_heartbeat_seconds must be between 0.05 and 60")
        if not 0.1 <= self.host_stale_after_seconds <= 300:
            raise ValueError("host_stale_after_seconds must be between 0.1 and 300")
        if self.host_stale_after_seconds <= self.host_heartbeat_seconds:
            raise ValueError("host_stale_after_seconds must exceed host_heartbeat_seconds")
        if not 1 <= self.max_concurrent_policies <= 32:
            raise ValueError("max_concurrent_policies must be between 1 and 32")


def _time(value: datetime | None = None) -> str:
    return (value or datetime.now(UTC)).isoformat(timespec="microseconds").replace("+00:00", "Z")


class RuntimeRecoveryMonitorRepository:
    """Durable host/checkpoint state for bounded node recovery polling."""

    def __init__(self, store: StateStore) -> None:
        self.store = store

    def register_monitor(
        self,
        monitor_id: str,
        *,
        process_id: int,
        stale_after_seconds: float,
        metadata: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        if not _MONITOR_ID.fullmatch(monitor_id) or process_id <= 0:
            raise ValueError("monitor_id and a positive process_id are required")
        now_value = datetime.now(UTC)
        now = _time(now_value)
        stale_before = _time(now_value - timedelta(seconds=stale_after_seconds))
        safe_metadata = redact_sensitive(metadata or {})
        with self.store.transaction() as connection:
            existing = connection.execute(
                "SELECT state,heartbeat_at FROM node_runtime_recovery_monitors WHERE monitor_id=?",
                (monitor_id,),
            ).fetchone()
            active = bool(
                existing is not None
                and existing["state"]
                in {
                    RecoveryMonitorState.STARTING.value,
                    RecoveryMonitorState.RUNNING.value,
                    RecoveryMonitorState.STOPPING.value,
                }
            )
            stale_recovered = bool(active and existing["heartbeat_at"] <= stale_before)
            if existing is not None and (
                existing["state"]
                in {
                    RecoveryMonitorState.STARTING.value,
                    RecoveryMonitorState.RUNNING.value,
                    RecoveryMonitorState.STOPPING.value,
                }
                and existing["heartbeat_at"] > stale_before
            ):
                raise RecoveryMonitorIdentityInUse(
                    f"runtime recovery monitor identity {monitor_id} has a live heartbeat"
                )
            if existing is None:
                connection.execute(
                    "INSERT INTO node_runtime_recovery_monitors("
                    "monitor_id,process_id,state,started_at,heartbeat_at,metadata_json) "
                    "VALUES (?,?,?,?,?,?)",
                    (
                        monitor_id,
                        process_id,
                        RecoveryMonitorState.STARTING.value,
                        now,
                        now,
                        compact_json(safe_metadata),
                    ),
                )
            else:
                connection.execute(
                    "UPDATE node_runtime_recovery_monitors SET process_id=?,state=?,started_at=?,"
                    "heartbeat_at=?,stopped_at=NULL,active_policy_count=0,last_error=NULL,"
                    "metadata_json=? WHERE monitor_id=?",
                    (
                        process_id,
                        RecoveryMonitorState.STARTING.value,
                        now,
                        now,
                        compact_json(safe_metadata),
                        monitor_id,
                    ),
                )
            self.store._append_event(
                connection,
                kind=(
                    "runtimeRecoveryMonitorRecovered"
                    if stale_recovered
                    else "runtimeRecoveryMonitorStarted"
                ),
                severity=EventSeverity.WARNING if stale_recovered else EventSeverity.NOTICE,
                entity_type="nodeRuntimeRecoveryMonitor",
                entity_id=monitor_id,
                summary=(
                    "Stale runtime recovery monitor identity recovered"
                    if stale_recovered
                    else "Runtime recovery monitor registered"
                ),
                payload={"processID": process_id},
                actor=f"node-recovery-monitor:{monitor_id}",
            )
        return self.get_monitor(monitor_id)

    def heartbeat_monitor(
        self,
        monitor_id: str,
        *,
        active_policy_count: int,
        observations_delta: int = 0,
        recoveries_delta: int = 0,
        stale_takeovers_delta: int = 0,
        state: RecoveryMonitorState = RecoveryMonitorState.RUNNING,
        last_error: str | None = None,
    ) -> bool:
        values = (
            active_policy_count,
            observations_delta,
            recoveries_delta,
            stale_takeovers_delta,
        )
        if any(value < 0 for value in values):
            raise ValueError("monitor counts cannot be negative")
        with self.store.transaction() as connection:
            cursor = connection.execute(
                "UPDATE node_runtime_recovery_monitors SET state=?,heartbeat_at=?,"
                "active_policy_count=?,observation_count=observation_count+?,"
                "recovery_count=recovery_count+?,stale_takeover_count=stale_takeover_count+?,"
                "last_error=COALESCE(?,last_error) WHERE monitor_id=?",
                (
                    state.value,
                    _time(),
                    active_policy_count,
                    observations_delta,
                    recoveries_delta,
                    stale_takeovers_delta,
                    str(redact_sensitive(last_error))[:240] if last_error else None,
                    monitor_id,
                ),
            )
        return cursor.rowcount == 1

    def stop_monitor(
        self, monitor_id: str, *, failed: bool = False, last_error: str | None = None
    ) -> None:
        now = _time()
        state = RecoveryMonitorState.FAILED if failed else RecoveryMonitorState.STOPPED
        with self.store.transaction() as connection:
            connection.execute(
                "UPDATE node_runtime_recovery_monitors SET state=?,heartbeat_at=?,stopped_at=?,"
                "active_policy_count=0,last_error=COALESCE(?,last_error) WHERE monitor_id=?",
                (
                    state.value,
                    now,
                    now,
                    str(redact_sensitive(last_error))[:240] if last_error else None,
                    monitor_id,
                ),
            )
            self.store._append_event(
                connection,
                kind="runtimeRecoveryMonitorStopped",
                severity=EventSeverity.ERROR if failed else EventSeverity.NOTICE,
                entity_type="nodeRuntimeRecoveryMonitor",
                entity_id=monitor_id,
                summary="Runtime recovery monitor stopped",
                payload={"state": state.value, "error": redact_sensitive(last_error)},
                actor=f"node-recovery-monitor:{monitor_id}",
            )

    def get_monitor(self, monitor_id: str) -> dict[str, Any]:
        with self.store.connect() as connection:
            row = connection.execute(
                "SELECT * FROM node_runtime_recovery_monitors WHERE monitor_id=?", (monitor_id,)
            ).fetchone()
        if row is None:
            raise KeyError(monitor_id)
        value = dict(row)
        value["metadata"] = json.loads(value.pop("metadata_json"))
        return value

    def due_policy_ids(self, *, limit: int, observed_at: datetime | None = None) -> list[str]:
        if limit < 1:
            return []
        now = _time(observed_at)
        with self.store.connect() as connection:
            rows = connection.execute(
                "SELECT p.id FROM node_runtime_recovery_policies p "
                "LEFT JOIN node_runtime_recovery_checkpoints c ON c.policy_id=p.id "
                "LEFT JOIN node_runtime_recovery_leases l ON l.policy_id=p.id "
                "WHERE p.enabled=1 AND (c.policy_id IS NULL OR c.next_observation_at<=?) "
                "AND (l.policy_id IS NULL OR l.state<>'owned' OR l.expires_at<=?) "
                "ORDER BY COALESCE(c.next_observation_at,p.created_at),p.id LIMIT ?",
                (now, now, limit),
            ).fetchall()
        return [str(row["id"]) for row in rows]

    def record_checkpoint(
        self,
        *,
        monitor_id: str,
        policy: RuntimeRecoveryPolicy,
        state: str,
        recovery_id: str | None,
        error: str | None,
        observed_at: datetime | None = None,
    ) -> dict[str, Any]:
        now_value = observed_at or datetime.now(UTC)
        now = _time(now_value)
        healthy = state in {
            RecoveryAttemptState.OBSERVED_HEALTHY.value,
            RecoveryAttemptState.READY.value,
        }
        with self.store.transaction() as connection:
            current = connection.execute(
                "SELECT consecutive_failures,last_state FROM node_runtime_recovery_checkpoints "
                "WHERE policy_id=?",
                (policy.id,),
            ).fetchone()
            prior_failures = int(current["consecutive_failures"]) if current else 0
            failures = 0 if healthy else prior_failures + 1
            delay = policy.monitor_interval_seconds
            if failures:
                delay = min(
                    86400.0,
                    policy.failure_backoff_seconds * (2 ** min(failures - 1, 8)),
                )
            next_at = _time(now_value + timedelta(seconds=delay))
            safe_error = str(redact_sensitive(error))[:240] if error else None
            connection.execute(
                "INSERT INTO node_runtime_recovery_checkpoints("
                "policy_id,monitor_id,last_recovery_id,last_state,consecutive_failures,"
                "last_observed_at,next_observation_at,last_error,updated_at) "
                "VALUES (?,?,?,?,?,?,?,?,?) ON CONFLICT(policy_id) DO UPDATE SET "
                "monitor_id=excluded.monitor_id,last_recovery_id=excluded.last_recovery_id,"
                "last_state=excluded.last_state,consecutive_failures=excluded.consecutive_failures,"
                "last_observed_at=excluded.last_observed_at,"
                "next_observation_at=excluded.next_observation_at,last_error=excluded.last_error,"
                "updated_at=excluded.updated_at",
                (
                    policy.id,
                    monitor_id,
                    recovery_id,
                    state,
                    failures,
                    now,
                    next_at,
                    safe_error,
                    now,
                ),
            )
            if current is None or current["last_state"] != state:
                self.store._append_event(
                    connection,
                    kind="runtimeRecoveryMonitorStateChanged",
                    severity=(EventSeverity.INFO if healthy else EventSeverity.WARNING),
                    entity_type="nodeRuntimeRecoveryPolicy",
                    entity_id=policy.id,
                    summary="Runtime recovery monitor state changed",
                    payload={
                        "monitorID": monitor_id,
                        "state": state,
                        "consecutiveFailures": failures,
                        "nextObservationAt": next_at,
                        "error": safe_error,
                    },
                    actor=f"node-recovery-monitor:{monitor_id}",
                )
        return self.get_checkpoint(policy.id) or {}

    def get_checkpoint(self, policy_id: str) -> dict[str, Any] | None:
        with self.store.connect() as connection:
            row = connection.execute(
                "SELECT * FROM node_runtime_recovery_checkpoints WHERE policy_id=?", (policy_id,)
            ).fetchone()
        return dict(row) if row else None


class RuntimeRecoveryMonitor:
    """Production polling host for existing recovery policy/service semantics.

    The monitor never discovers executable commands or credentials. A deployment must inject a
    reviewed binding for each policy; missing bindings fail closed and are durably observable.
    """

    def __init__(
        self,
        *,
        store: StateStore,
        binding_provider: RecoveryBindingProvider,
        config: RuntimeRecoveryMonitorConfig | None = None,
        monitor_id: str | None = None,
        process_id: int | None = None,
    ) -> None:
        self.store = store
        self.service = NodeRuntimeRecoveryService(store)
        self.repository = RuntimeRecoveryMonitorRepository(store)
        self.binding_provider = binding_provider
        self.config = config or RuntimeRecoveryMonitorConfig()
        self.monitor_id = monitor_id or f"runtime-monitor-{uuid.uuid4()}"
        self.process_id = process_id or os.getpid()
        self._shutdown = asyncio.Event()
        self._registered = False

    def request_shutdown(self) -> None:
        self._shutdown.set()

    async def run_once(self) -> list[dict[str, Any]]:
        self._ensure_registered()
        policy_ids = self.repository.due_policy_ids(limit=self.config.max_concurrent_policies)
        if not policy_ids:
            self.repository.heartbeat_monitor(self.monitor_id, active_policy_count=0)
            return []
        self.repository.heartbeat_monitor(self.monitor_id, active_policy_count=len(policy_ids))
        tasks = [asyncio.create_task(self._run_policy(policy_id)) for policy_id in policy_ids]
        pending: set[asyncio.Task[dict[str, Any]]] = set(tasks)
        while pending:
            _, pending = await asyncio.wait(
                pending,
                timeout=self.config.host_heartbeat_seconds,
                return_when=asyncio.FIRST_COMPLETED,
            )
            if pending:
                self.repository.heartbeat_monitor(self.monitor_id, active_policy_count=len(pending))
        results = [task.result() for task in tasks]
        recoveries = sum(
            result.get("state") == RecoveryAttemptState.READY.value for result in results
        )
        stale_takeovers = sum(bool(result.get("staleOwnerRecovered")) for result in results)
        self.repository.heartbeat_monitor(
            self.monitor_id,
            active_policy_count=0,
            observations_delta=len(results),
            recoveries_delta=recoveries,
            stale_takeovers_delta=stale_takeovers,
        )
        return results

    async def serve(self) -> None:
        self._ensure_registered()
        failure: BaseException | None = None
        try:
            while not self._shutdown.is_set():
                await self.run_once()
                with suppress(TimeoutError):
                    await asyncio.wait_for(
                        self._shutdown.wait(), timeout=self.config.discovery_poll_seconds
                    )
        except BaseException as error:
            failure = error
            raise
        finally:
            self.repository.stop_monitor(
                self.monitor_id,
                failed=failure is not None,
                last_error=str(redact_sensitive(str(failure))) if failure else None,
            )

    def status(self) -> dict[str, Any]:
        return self.repository.get_monitor(self.monitor_id)

    def _ensure_registered(self) -> None:
        if self._registered:
            return
        self.repository.register_monitor(
            self.monitor_id,
            process_id=self.process_id,
            stale_after_seconds=self.config.host_stale_after_seconds,
            metadata={"maxConcurrentPolicies": self.config.max_concurrent_policies},
        )
        self.repository.heartbeat_monitor(self.monitor_id, active_policy_count=0)
        self._registered = True

    async def _run_policy(self, policy_id: str) -> dict[str, Any]:
        policy = await asyncio.to_thread(self.service.get_policy, policy_id)
        try:
            binding = await asyncio.to_thread(self.binding_provider, policy)
            if binding is None:
                error = "no reviewed runtime recovery binding is configured for this policy"
                await asyncio.to_thread(
                    self.repository.record_checkpoint,
                    monitor_id=self.monitor_id,
                    policy=policy,
                    state="bindingUnavailable",
                    recovery_id=None,
                    error=error,
                )
                return {"policyID": policy.id, "state": "bindingUnavailable", "error": error}
            outcome: RecoveryOutcome = await asyncio.to_thread(
                partial(
                    self.service.recover_if_needed,
                    policy.id,
                    probe=binding.probe,
                    adapter=binding.adapter,
                    authorizer=binding.authorizer,
                    requested_by=f"monitor:{self.monitor_id}",
                    trigger_reason="bounded periodic runtime health observation",
                    lease_owner_id=f"monitor:{self.monitor_id}",
                )
            )
            await asyncio.to_thread(
                self.repository.record_checkpoint,
                monitor_id=self.monitor_id,
                policy=policy,
                state=outcome.state.value,
                recovery_id=outcome.id,
                error=outcome.failure_detail,
            )
            value = outcome.to_protocol()
            value["policyID"] = policy.id
            return value
        except RecoveryLeaseUnavailable:
            return {"policyID": policy.id, "state": "leased", "skipped": True}
        except Exception as error:
            detail = str(redact_sensitive(str(error)))[:240]
            await asyncio.to_thread(
                self.repository.record_checkpoint,
                monitor_id=self.monitor_id,
                policy=policy,
                state="monitorError",
                recovery_id=None,
                error=detail,
            )
            return {"policyID": policy.id, "state": "monitorError", "error": detail}


RecoveryBindingFactory = Callable[[RuntimeRecoveryPolicy], RuntimeRecoveryBinding | None]
