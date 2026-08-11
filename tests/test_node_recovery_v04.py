from __future__ import annotations

import asyncio
import json
import threading
from collections import deque
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime
from pathlib import Path

import jsonschema
import pytest

from project_supervisor.domain import (
    Harness,
    ModelDescriptor,
    NodeState,
    Provider,
    ResourceState,
    WorkerSnapshot,
    WorkerState,
)
from project_supervisor.node_recovery import (
    NodeRuntimeAdapterDescriptor,
    NodeRuntimeRecoveryService,
    RecoveryAttemptState,
    RecoveryAuthorizationDecision,
    RecoveryLeaseLost,
    RecoveryOperation,
    RuntimeHealthObservation,
    RuntimeRecoveryPolicy,
    RuntimeRecoveryResult,
)
from project_supervisor.node_recovery_host import (
    RecoveryMonitorIdentityInUse,
    RuntimeRecoveryBinding,
    RuntimeRecoveryMonitor,
    RuntimeRecoveryMonitorConfig,
    RuntimeRecoveryMonitorRepository,
)
from project_supervisor.store import StateStore


def health(*, ready: bool, models: tuple[str, ...] | None = None) -> RuntimeHealthObservation:
    return RuntimeHealthObservation(
        reachable=ready,
        runtime_ready=ready,
        models=models,
        observed_at=datetime.now(UTC),
        source="fixture.loopbackGET",
    )


class Probe:
    side_effect_free = True

    def __init__(self, *values: RuntimeHealthObservation) -> None:
        self.values = deque(values)
        self.calls = 0

    def observe(self, _policy: RuntimeRecoveryPolicy) -> RuntimeHealthObservation:
        self.calls += 1
        if len(self.values) > 1:
            return self.values.popleft()
        return self.values[0]


class Authorizer:
    def authorize(self, _context):
        return RecoveryAuthorizationDecision(True, "grant-v04-fixture", "fixture authorization")


class Adapter:
    descriptor = NodeRuntimeAdapterDescriptor(
        adapter_id="windows-node-runtime.fixture",
        node_id="node-gpu",
        protocol_version="1.0",
        max_operation_seconds=5,
        allowed_operations=frozenset({RecoveryOperation.START_RUNTIME}),
    )

    def __init__(self) -> None:
        self.requests = []

    def recover(self, request):
        self.requests.append(request)
        return RuntimeRecoveryResult(
            True,
            "operation-v04-fixture",
            observed_fencing_generation=request.lease_generation,
        )


@pytest.fixture
def recovery_store(tmp_path: Path) -> StateStore:
    store = StateStore(tmp_path / "state.db")
    store.create_project(
        project_id="project-v04",
        name="Node recovery V0.4",
        root_path=str(tmp_path),
        goal="Recover a node runtime safely",
    )
    store.upsert_node(
        node_id="node-gpu",
        hostname="fixture",
        display_name="Fixture GPU",
        role="worker",
        state=NodeState.ONLINE,
    )
    store.upsert_worker(
        WorkerSnapshot(
            id="local-worker",
            node_id="node-gpu",
            harness=Harness.LOCAL_WORKER,
            provider=Provider.LOCAL,
            model=ModelDescriptor("local-model", "Local", Provider.LOCAL),
            state=WorkerState.IDLE,
            node_state=NodeState.ONLINE,
            resource_state=ResourceState.AVAILABLE,
            capabilities=frozenset({"analysis"}),
            code_write_allowed=False,
            privacy_allowed=True,
        )
    )
    return store


def recovery_policy() -> RuntimeRecoveryPolicy:
    return RuntimeRecoveryPolicy(
        id="recovery-policy",
        node_id="node-gpu",
        runtime_id="lm-studio-runtime",
        backend_endpoint="http://127.0.0.1:1234",
        expected_models=("local-model",),
        worker_ids=("local-worker",),
        enabled=True,
        max_attempts=1,
        monitor_interval_seconds=5,
        failure_backoff_seconds=10,
        lease_ttl_seconds=15,
    )


def test_durable_single_flight_stale_takeover_fences_old_owner(
    recovery_store: StateStore,
) -> None:
    service = NodeRuntimeRecoveryService(recovery_store)
    policy = recovery_policy()
    service.upsert_policy(policy, configured_by="operator:test")
    old = service.leases.try_acquire(
        policy_id=policy.id,
        owner_id="monitor:old",
        recovery_id="runtime-recovery-old",
        lease_ttl_seconds=15,
    )
    assert old is not None
    assert (
        service.leases.try_acquire(
            policy_id=policy.id,
            owner_id="monitor:contender",
            recovery_id="runtime-recovery-contender",
            lease_ttl_seconds=15,
        )
        is None
    )
    service._create_attempt(  # noqa: SLF001 - deterministic crash-window fixture
        old.recovery_id,
        policy,
        old,
        RecoveryAttemptState.DEGRADED,
        "crash fixture",
        "monitor:old",
        health(ready=False),
        finished=False,
    )
    with recovery_store.transaction() as connection:
        connection.execute(
            "UPDATE node_runtime_recovery_leases SET expires_at=? WHERE policy_id=?",
            ("2020-01-01T00:00:00.000000Z", policy.id),
        )

    current = service.leases.try_acquire(
        policy_id=policy.id,
        owner_id="monitor:new",
        recovery_id="runtime-recovery-new",
        lease_ttl_seconds=15,
    )
    assert current is not None
    assert current.generation == old.generation + 1
    assert current.stale_owner_recovered
    with pytest.raises(RecoveryLeaseLost):
        service.leases.assert_owned(old)
    with recovery_store.connect() as connection:
        attempt = connection.execute(
            "SELECT state,failure_code,finished_at FROM node_runtime_recovery_attempts WHERE id=?",
            (old.recovery_id,),
        ).fetchone()
    assert dict(attempt) == {
        "state": "failed",
        "failure_code": "staleLeaseRecovered",
        "finished_at": attempt["finished_at"],
    }
    assert attempt["finished_at"] is not None
    assert not service.leases.release(old)
    assert service.leases.release(current)


def test_two_process_style_contenders_produce_exactly_one_live_owner(
    recovery_store: StateStore,
) -> None:
    service = NodeRuntimeRecoveryService(recovery_store)
    service.upsert_policy(recovery_policy(), configured_by="operator:test")
    barrier = threading.Barrier(2)

    def contend(index: int):
        independent = NodeRuntimeRecoveryService(StateStore(recovery_store.path))
        barrier.wait(timeout=1)
        return independent.leases.try_acquire(
            policy_id="recovery-policy",
            owner_id=f"monitor:contender-{index}",
            recovery_id=f"runtime-recovery-contender-{index}",
            lease_ttl_seconds=15,
        )

    with ThreadPoolExecutor(max_workers=2) as executor:
        claims = list(executor.map(contend, (1, 2)))
    winners = [claim for claim in claims if claim is not None]
    assert len(winners) == 1
    assert service.leases.get("recovery-policy")["owner_id"] == winners[0].owner_id
    assert service.leases.release(winners[0])


def test_live_policy_is_immutable_and_orphaned_attempt_is_reconciled(
    recovery_store: StateStore,
) -> None:
    service = NodeRuntimeRecoveryService(recovery_store)
    policy = recovery_policy()
    service.upsert_policy(policy, configured_by="operator:test")
    claim = service.leases.try_acquire(
        policy_id=policy.id,
        owner_id="monitor:orphan",
        recovery_id="runtime-recovery-orphan",
        lease_ttl_seconds=15,
    )
    assert claim is not None
    with pytest.raises(RuntimeError, match="while its lease is live"):
        service.upsert_policy(policy, configured_by="operator:test")
    service._create_attempt(  # noqa: SLF001 - deterministic orphan fixture
        claim.recovery_id,
        policy,
        claim,
        RecoveryAttemptState.DEGRADED,
        "orphan fixture",
        "monitor:orphan",
        health(ready=False),
        finished=False,
    )
    assert service.leases.release(claim)

    resumed = service.leases.try_acquire(
        policy_id=policy.id,
        owner_id="monitor:restart",
        recovery_id="runtime-recovery-restart",
        lease_ttl_seconds=15,
    )
    assert resumed is not None
    assert not resumed.stale_owner_recovered
    lease = service.leases.get(policy.id)
    assert lease is not None and lease["recovery_state"] == "resuming"
    with recovery_store.connect() as connection:
        attempt = connection.execute(
            "SELECT state,failure_code FROM node_runtime_recovery_attempts WHERE id=?",
            (claim.recovery_id,),
        ).fetchone()
    assert tuple(attempt) == ("failed", "orphanedAttemptRecovered")
    assert service.leases.release(resumed)


def test_typed_adapter_requires_current_fencing_acknowledgement(
    recovery_store: StateStore,
) -> None:
    service = NodeRuntimeRecoveryService(recovery_store)
    service.upsert_policy(recovery_policy(), configured_by="operator:test")

    class WrongFenceAdapter(Adapter):
        def recover(self, request):
            return RuntimeRecoveryResult(
                True,
                "operation-wrong-fence",
                observed_fencing_generation=request.lease_generation + 1,
            )

    outcome = service.recover_if_needed(
        "recovery-policy",
        probe=Probe(health(ready=False)),
        adapter=WrongFenceAdapter(),
        authorizer=Authorizer(),
        requested_by="monitor:test",
        trigger_reason="fencing acceptance",
    )
    assert outcome.state is RecoveryAttemptState.FAILED
    assert outcome.failure_code == "adapterFencingMismatch"
    assert recovery_store.list_nodes()[0]["state"] == "degraded"


def test_retries_reuse_one_idempotency_key_and_fencing_generation(
    recovery_store: StateStore,
) -> None:
    service = NodeRuntimeRecoveryService(recovery_store)
    base = recovery_policy()
    policy = RuntimeRecoveryPolicy(
        id=base.id,
        node_id=base.node_id,
        runtime_id=base.runtime_id,
        backend_endpoint=base.backend_endpoint,
        expected_models=base.expected_models,
        worker_ids=base.worker_ids,
        enabled=True,
        max_attempts=2,
        monitor_interval_seconds=base.monitor_interval_seconds,
        failure_backoff_seconds=base.failure_backoff_seconds,
        lease_ttl_seconds=base.lease_ttl_seconds,
    )
    service.upsert_policy(policy, configured_by="operator:test")

    class RetryAdapter(Adapter):
        def recover(self, request):
            self.requests.append(request)
            if len(self.requests) == 1:
                return RuntimeRecoveryResult(False, detail="bounded transient rejection")
            return RuntimeRecoveryResult(
                True,
                "operation-retry-fixture",
                observed_fencing_generation=request.lease_generation,
            )

    adapter = RetryAdapter()
    outcome = service.recover_if_needed(
        policy.id,
        probe=Probe(
            health(ready=False),
            health(ready=True, models=("local-model",)),
        ),
        adapter=adapter,
        authorizer=Authorizer(),
        requested_by="monitor:test",
        trigger_reason="idempotent retry acceptance",
    )
    assert outcome.state is RecoveryAttemptState.READY
    assert len(adapter.requests) == 2
    assert {request.idempotency_key for request in adapter.requests} == {outcome.id}
    assert {request.lease_generation for request in adapter.requests} == {outcome.lease_generation}


@pytest.mark.asyncio
async def test_monitor_polls_once_checkpoints_and_avoids_busy_spin(
    recovery_store: StateStore,
) -> None:
    service = NodeRuntimeRecoveryService(recovery_store)
    service.upsert_policy(recovery_policy(), configured_by="operator:test")
    probe = Probe(health(ready=True, models=("local-model",)))
    adapter = Adapter()
    monitor = RuntimeRecoveryMonitor(
        store=recovery_store,
        binding_provider=lambda _policy: RuntimeRecoveryBinding(probe, adapter),
        config=RuntimeRecoveryMonitorConfig(
            discovery_poll_seconds=0.05,
            host_heartbeat_seconds=0.05,
            host_stale_after_seconds=0.2,
            max_concurrent_policies=1,
        ),
        monitor_id="monitor-v04",
        process_id=4004,
    )
    first = await monitor.run_once()
    second = await monitor.run_once()
    assert first[0]["state"] == "observedHealthy"
    assert second == []
    assert probe.calls == 1
    checkpoint = monitor.repository.get_checkpoint("recovery-policy")
    assert checkpoint is not None
    assert checkpoint["consecutive_failures"] == 0
    assert checkpoint["next_observation_at"] > checkpoint["last_observed_at"]

    status = service.list_status()[0]
    schema = json.loads(
        (Path(__file__).parents[1] / "schemas/node-runtime-recovery-v1.schema.json").read_text()
    )
    jsonschema.Draft202012Validator(schema, format_checker=jsonschema.FormatChecker()).validate(
        status
    )
    assert status["lease"]["state"] == "released"
    assert status["monitoring"]["monitorID"] == "monitor-v04"
    assert status["monitoring"]["host"]["state"] == "running"
    assert status["monitoring"]["host"]["heartbeatAt"] is not None


@pytest.mark.asyncio
async def test_monitor_recovers_runtime_and_persists_restart_schedule(
    recovery_store: StateStore,
) -> None:
    service = NodeRuntimeRecoveryService(recovery_store)
    service.upsert_policy(recovery_policy(), configured_by="operator:test")
    first_probe = Probe(
        health(ready=False),
        health(ready=True, models=("local-model",)),
    )
    adapter = Adapter()
    first = RuntimeRecoveryMonitor(
        store=recovery_store,
        binding_provider=lambda _policy: RuntimeRecoveryBinding(first_probe, adapter, Authorizer()),
        monitor_id="monitor-first",
        process_id=4101,
    )
    result = await first.run_once()
    assert result[0]["state"] == "ready"
    assert len(adapter.requests) == 1
    request = adapter.requests[0]
    assert request.node_id == "node-gpu"
    assert request.backend_endpoint == "http://127.0.0.1:1234"
    assert not hasattr(request, "command")
    assert request.idempotency_key == request.recovery_id
    assert request.deadline_at > request.requested_at
    adapter_exchange = {
        "descriptor": adapter.descriptor.to_protocol(),
        "request": request.to_protocol(),
        "result": RuntimeRecoveryResult(
            True,
            "operation-schema-fixture",
            observed_fencing_generation=request.lease_generation,
        ).to_protocol(),
    }
    adapter_schema = json.loads(
        (Path(__file__).parents[1] / "schemas/node-runtime-adapter-v1.schema.json").read_text()
    )
    adapter_validator = jsonschema.Draft202012Validator(
        adapter_schema, format_checker=jsonschema.FormatChecker()
    )
    adapter_validator.validate(adapter_exchange)
    unsafe_exchange = json.loads(json.dumps(adapter_exchange))
    unsafe_exchange["request"]["command"] = "not-an-allowed-contract-field"
    with pytest.raises(jsonschema.ValidationError):
        adapter_validator.validate(unsafe_exchange)
    assert first.status()["recovery_count"] == 1

    reopened = StateStore(recovery_store.path)
    second_probe = Probe(health(ready=True, models=("local-model",)))
    second = RuntimeRecoveryMonitor(
        store=reopened,
        binding_provider=lambda _policy: RuntimeRecoveryBinding(second_probe, Adapter()),
        monitor_id="monitor-second",
        process_id=4102,
    )
    assert await second.run_once() == []
    assert second_probe.calls == 0
    with reopened.transaction() as connection:
        connection.execute(
            "UPDATE node_runtime_recovery_checkpoints SET next_observation_at=? "
            "WHERE policy_id='recovery-policy'",
            ("2020-01-01T00:00:00.000000Z",),
        )
    assert (await second.run_once())[0]["state"] == "observedHealthy"
    assert second_probe.calls == 1


@pytest.mark.asyncio
async def test_monitor_missing_binding_fails_closed_with_durable_backoff(
    recovery_store: StateStore,
) -> None:
    service = NodeRuntimeRecoveryService(recovery_store)
    service.upsert_policy(recovery_policy(), configured_by="operator:test")
    monitor = RuntimeRecoveryMonitor(
        store=recovery_store,
        binding_provider=lambda _policy: None,
        monitor_id="monitor-no-binding",
        process_id=4201,
    )
    result = await monitor.run_once()
    assert result[0]["state"] == "bindingUnavailable"
    checkpoint = monitor.repository.get_checkpoint("recovery-policy")
    assert checkpoint is not None
    assert checkpoint["consecutive_failures"] == 1
    assert checkpoint["last_error"] == (
        "no reviewed runtime recovery binding is configured for this policy"
    )
    assert recovery_store.list_nodes()[0]["state"] == "online"


@pytest.mark.asyncio
async def test_monitor_serve_stops_cleanly_without_repolling_before_checkpoint(
    recovery_store: StateStore,
) -> None:
    service = NodeRuntimeRecoveryService(recovery_store)
    service.upsert_policy(recovery_policy(), configured_by="operator:test")
    probe = Probe(health(ready=True, models=("local-model",)))
    monitor = RuntimeRecoveryMonitor(
        store=recovery_store,
        binding_provider=lambda _policy: RuntimeRecoveryBinding(probe, Adapter()),
        config=RuntimeRecoveryMonitorConfig(
            discovery_poll_seconds=0.05,
            host_heartbeat_seconds=0.05,
            host_stale_after_seconds=0.2,
            max_concurrent_policies=1,
        ),
        monitor_id="monitor-serve",
        process_id=4301,
    )
    task = asyncio.create_task(monitor.serve())
    await asyncio.sleep(0.14)
    monitor.request_shutdown()
    await asyncio.wait_for(task, timeout=1)
    assert probe.calls == 1
    assert monitor.status()["state"] == "stopped"


def test_monitor_identity_requires_stale_heartbeat_before_recovery(
    recovery_store: StateStore,
) -> None:
    repository = RuntimeRecoveryMonitorRepository(recovery_store)
    repository.register_monitor("monitor-identity", process_id=4401, stale_after_seconds=1)
    repository.heartbeat_monitor("monitor-identity", active_policy_count=0)
    with pytest.raises(RecoveryMonitorIdentityInUse):
        repository.register_monitor("monitor-identity", process_id=4402, stale_after_seconds=1)
    with recovery_store.transaction() as connection:
        connection.execute(
            "UPDATE node_runtime_recovery_monitors SET heartbeat_at=? WHERE monitor_id=?",
            ("2020-01-01T00:00:00.000000Z", "monitor-identity"),
        )
    recovered = repository.register_monitor(
        "monitor-identity", process_id=4402, stale_after_seconds=1
    )
    assert recovered["process_id"] == 4402
    events = recovery_store.list_events(after_sequence=0, limit=200)
    assert "runtimeRecoveryMonitorRecovered" in {event["kind"] for event in events}


def test_v04_migration_exposes_recovery_host_tables(recovery_store: StateStore) -> None:
    with recovery_store.connect() as connection:
        tables = {
            row["name"]
            for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()
        }
        migration = connection.execute(
            "SELECT 1 FROM schema_migrations WHERE version='0009_node_recovery_host'"
        ).fetchone()
        policy_columns = {
            row["name"]
            for row in connection.execute(
                "PRAGMA table_info(node_runtime_recovery_policies)"
            ).fetchall()
        }
    assert {
        "node_runtime_recovery_leases",
        "node_runtime_recovery_monitors",
        "node_runtime_recovery_checkpoints",
    } <= tables
    assert {
        "monitor_interval_seconds",
        "failure_backoff_seconds",
        "lease_ttl_seconds",
    } <= policy_columns
    assert migration is not None
