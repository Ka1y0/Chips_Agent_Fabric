from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta

from project_supervisor.adapters import MockAdapter
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
from project_supervisor.fabric.execution_plane import (
    EvidenceState,
    ExecutabilityDisposition,
    ExecutionPlaneRepository,
    PlatformApprovalState,
    WorkerExecutionObservation,
    parse_tailscale_status,
)
from project_supervisor.runtime import AdapterRegistry, SupervisorRuntime
from project_supervisor.scheduler import DeterministicScheduler
from project_supervisor.store import StateStore


def _seed(tmp_path, *, worker_count: int = 1):
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    store = StateStore(tmp_path / "state.db")
    store.create_project(
        project_id="project-phase2",
        name="Phase 2",
        root_path=str(workspace),
        goal="Exercise execution plane recovery",
    )
    store.upsert_node(
        node_id="node-windows",
        hostname="private-node",
        display_name="Windows Fabric Node",
        role="worker",
        state=NodeState.ONLINE,
    )
    workers: list[str] = []
    for index in range(worker_count):
        worker_id = f"worker-phase2-{index + 1}"
        workers.append(worker_id)
        store.upsert_worker(
            WorkerSnapshot(
                id=worker_id,
                node_id="node-windows",
                harness=Harness.MOCK,
                provider=Provider.MOCK,
                model=ModelDescriptor("fixture-model", "Fixture", Provider.MOCK),
                state=WorkerState.IDLE,
                node_state=NodeState.ONLINE,
                resource_state=ResourceState.AVAILABLE,
                capabilities=frozenset({"analysis"}),
                code_write_allowed=False,
                privacy_allowed=True,
            )
        )
    return store, tuple(workers)


def _observation(
    worker_id: str,
    *,
    reachable: EvidenceState = EvidenceState.YES,
    capacity: EvidenceState = EvidenceState.YES,
    approval: PlatformApprovalState = PlatformApprovalState.NOT_REQUIRED,
    observed_at: datetime | None = None,
    valid_for_seconds: float = 30,
) -> WorkerExecutionObservation:
    observed = observed_at or datetime.now(UTC)
    return WorkerExecutionObservation(
        worker_id=worker_id,
        node_id="node-windows",
        discovered=EvidenceState.YES,
        configured=EvidenceState.YES,
        authenticated=EvidenceState.YES,
        authorized=EvidenceState.YES,
        platform_approval=approval,
        reachable=reachable,
        runtime_available=EvidenceState.YES,
        healthy=EvidenceState.YES,
        capacity_available=capacity,
        observed_at=observed,
        valid_until=observed + timedelta(seconds=valid_for_seconds),
        protocol_version="2.0",
    )


def test_tailscale_discovery_uses_stable_identity_not_address() -> None:
    first = parse_tailscale_status(
        {
            "Peer": {
                "node-key": {
                    "ID": "stable-peer-id",
                    "OS": "windows",
                    "Online": True,
                    "TailscaleIPs": ["100.64.1.2"],
                    "DNSName": "private-name.example",
                }
            }
        }
    )[0]
    second = parse_tailscale_status(
        {
            "Peer": {
                "node-key": {
                    "ID": "stable-peer-id",
                    "OS": "windows",
                    "Online": True,
                    "TailscaleIPs": ["100.90.8.7"],
                    "DNSName": "changed.example",
                }
            }
        }
    )[0]

    assert first.identity_sha256 == second.identity_sha256
    assert first.reachable is EvidenceState.UNKNOWN
    assert "100.64.1.2" not in first.peer_identity
    assert "private-name" not in first.peer_identity


def test_execution_facets_are_hard_constraints_and_expiry_is_supervisor_computed(
    tmp_path,
) -> None:
    store, (worker_id,) = _seed(tmp_path)
    repository = ExecutionPlaneRepository(store)
    repository.record_worker_observation(_observation(worker_id, reachable=EvidenceState.NO))
    snapshot = store.worker_snapshots()[0]
    decision = DeterministicScheduler().schedule(
        task_id="task-route",
        requirements=TaskRequirements(
            labels=frozenset({TaskLabel.RESEARCH}),
            required_capabilities=frozenset({"analysis"}),
        ),
        topology=ExecutionTopology.SINGLE,
        workers=(snapshot,),
    )
    assert decision.selected_worker_ids == ()
    assert {item.reason_code for item in decision.rejected} == {"RUNTIME_UNREACHABLE"}

    old = datetime.now(UTC) - timedelta(seconds=20)
    repository.record_worker_observation(
        _observation(worker_id, observed_at=old, valid_for_seconds=1)
    )
    stale = store.worker_snapshots()[0]
    assert stale.execution_disposition == ExecutabilityDisposition.STALE.value
    assert stale.execution_rejection_code == "RUNTIME_OBSERVATION_STALE"


def test_platform_approval_unknown_cannot_be_outscored(tmp_path) -> None:
    store, (worker_id,) = _seed(tmp_path)
    ExecutionPlaneRepository(store).record_worker_observation(
        _observation(worker_id, approval=PlatformApprovalState.UNKNOWN)
    )
    decision = DeterministicScheduler().schedule(
        task_id="task-platform",
        requirements=TaskRequirements(labels=frozenset({TaskLabel.RESEARCH})),
        topology=ExecutionTopology.SINGLE,
        workers=store.worker_snapshots(),
    )
    assert decision.selected_worker_ids == ()
    assert decision.rejected[0].reason_code == "PLATFORM_APPROVAL_UNKNOWN"


class _RecoverOnce:
    def __init__(self, repository: ExecutionPlaneRepository, worker_id: str) -> None:
        self.repository = repository
        self.worker_id = worker_id
        self.calls = 0

    async def recover_capacity(self, *, task_id, requirements, rejection_codes) -> bool:
        del task_id, requirements
        self.calls += 1
        assert "RUNTIME_UNREACHABLE" in rejection_codes
        self.repository.record_worker_observation(_observation(self.worker_id))
        return True


class _RecoverOnSecondAttempt(_RecoverOnce):
    async def recover_capacity(self, *, task_id, requirements, rejection_codes) -> bool:
        del task_id, requirements
        self.calls += 1
        assert "RUNTIME_UNREACHABLE" in rejection_codes
        if self.calls == 1:
            return False
        self.repository.record_worker_observation(_observation(self.worker_id))
        return True


async def test_runtime_recovers_capacity_then_dispatches_original_attempt_once(tmp_path) -> None:
    store, (worker_id,) = _seed(tmp_path)
    repository = ExecutionPlaneRepository(store)
    repository.record_worker_observation(_observation(worker_id, reachable=EvidenceState.NO))
    registry = AdapterRegistry()
    registry.register(worker_id, MockAdapter())
    recovery = _RecoverOnce(repository, worker_id)
    runtime = SupervisorRuntime(
        store=store,
        scheduler=DeterministicScheduler(),
        adapters=registry,
        evidence_root=tmp_path / "evidence",
        execution_plane_recovery=recovery,
    )
    task_id = await runtime.submit_task(
        project_id="project-phase2",
        title="Recover before escalation",
        description="Read-only analysis",
        requirements=TaskRequirements(
            labels=frozenset({TaskLabel.RESEARCH}),
            required_capabilities=frozenset({"analysis"}),
        ),
    )

    await runtime.run_until_idle()

    assert recovery.calls == 1
    assert store.get_task(task_id)["state"] == TaskState.REVIEWING.value
    runs = store.list_worker_runs(task_id)
    assert len(runs) == 1
    assert int(runs[0]["attempt"]) == 1
    with store.connect() as connection:
        wait = connection.execute(
            "SELECT state,recovery_attempts FROM task_capacity_waits WHERE task_id=?",
            (task_id,),
        ).fetchone()
    assert dict(wait) == {"state": "resolved", "recovery_attempts": 1}


async def test_blocked_capacity_wait_is_recovered_on_a_later_dispatch_tick(tmp_path) -> None:
    store, (worker_id,) = _seed(tmp_path)
    repository = ExecutionPlaneRepository(store)
    repository.record_worker_observation(_observation(worker_id, reachable=EvidenceState.NO))
    registry = AdapterRegistry()
    registry.register(worker_id, MockAdapter())
    recovery = _RecoverOnSecondAttempt(repository, worker_id)
    runtime = SupervisorRuntime(
        store=store,
        scheduler=DeterministicScheduler(),
        adapters=registry,
        evidence_root=tmp_path / "evidence",
        execution_plane_recovery=recovery,
    )
    task_id = await runtime.submit_task(
        project_id="project-phase2",
        title="Recover on a later host tick",
        description="The original Task must resume without a new prompt",
        requirements=TaskRequirements(
            labels=frozenset({TaskLabel.RESEARCH}),
            required_capabilities=frozenset({"analysis"}),
        ),
    )

    first = await runtime.dispatch_ready()
    assert first.blocked_task_ids == (task_id,)
    assert store.get_task(task_id)["state"] == TaskState.BLOCKED.value
    with store.transaction() as connection:
        connection.execute(
            "UPDATE task_capacity_waits SET next_check_at='2000-01-01T00:00:00Z' WHERE task_id=?",
            (task_id,),
        )

    second = await runtime.dispatch_ready()
    assert second.launched_task_ids == (task_id,)
    await runtime.wait_for_active()

    assert recovery.calls == 2
    assert store.get_task(task_id)["state"] == TaskState.REVIEWING.value
    runs = store.list_worker_runs(task_id)
    assert len(runs) == 1
    assert int(runs[0]["attempt"]) == 1
    with store.connect() as connection:
        wait = connection.execute(
            "SELECT state,recovery_attempts FROM task_capacity_waits WHERE task_id=?",
            (task_id,),
        ).fetchone()
    assert dict(wait) == {"state": "resolved", "recovery_attempts": 2}


def test_operator_binding_persists_only_peer_fingerprint(tmp_path) -> None:
    store, _ = _seed(tmp_path)
    row = ExecutionPlaneRepository(store).register_binding(
        node_id="node-windows",
        transport_provider="tailscale",
        peer_identity="private-stable-peer-material",
        service_name="local-worker",
        endpoint_ref="endpoint.windowsWorker",
        configured_by="operator.test",
        expected_platform="windows",
    )
    assert row["peer_identity_sha256"] != "private-stable-peer-material"
    with store.connect() as connection:
        encoded = str(
            dict(
                connection.execute(
                    "SELECT * FROM node_transport_bindings WHERE id=?", (row["id"],)
                ).fetchone()
            )
        )
    assert "private-stable-peer-material" not in encoded


def test_dispatch_claim_rejects_a_newer_non_executable_observation(tmp_path) -> None:
    store, (worker_id,) = _seed(tmp_path)
    repository = ExecutionPlaneRepository(store)
    first = repository.record_worker_observation(_observation(worker_id))
    runtime = SupervisorRuntime(
        store=store,
        scheduler=DeterministicScheduler(),
        adapters=AdapterRegistry(),
        evidence_root=tmp_path / "evidence",
    )
    task_id = asyncio.run(
        runtime.submit_task(
            project_id="project-phase2",
            title="Fence stale route evidence",
            description="No adapter invocation is permitted after health changes",
            requirements=TaskRequirements(
                labels=frozenset({TaskLabel.RESEARCH}),
                required_capabilities=frozenset({"analysis"}),
            ),
        )
    )
    repository.record_worker_observation(_observation(worker_id, reachable=EvidenceState.NO))

    task = store.get_task(task_id)
    claim = store.claim_task_dispatch(
        task_id,
        (worker_id,),
        expected_version=int(task["version"]),
        expected_execution_observation_ids={worker_id: first["id"]},
    )

    assert claim is None
    assert store.list_worker_runs(task_id) == []
