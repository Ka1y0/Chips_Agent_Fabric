from __future__ import annotations

import asyncio
import hashlib
import json
from dataclasses import replace
from datetime import UTC, datetime, timedelta

import httpx
import pytest

from project_supervisor.adapters import LocalWorkerAdapter, MockAdapter
from project_supervisor.domain import (
    ExecutionTopology,
    Harness,
    ModelDescriptor,
    NodeState,
    PermissionClass,
    Provider,
    ResourceState,
    TaskLabel,
    TaskRequirements,
    TaskState,
    WorkerSnapshot,
    WorkerState,
)
from project_supervisor.fabric.authority import (
    AuthorizationEnvelope,
    AuthorizationRepository,
)
from project_supervisor.fabric.execution_plane import (
    EvidenceState,
    ExecutionPlaneRecoveryCoordinator,
    ExecutionPlaneRepository,
    FabricRuntimeBootstrapDescriptor,
    FabricRuntimeStartRequest,
    FabricRuntimeStartResult,
    HTTPSFabricRuntimeBootstrap,
    LocalWorkerFabricRuntimeProbe,
    LocalWorkerProbeBinding,
    NodeRuntimeProbeResult,
    PlatformApprovalState,
    TailscaleCLITransportDiscovery,
    TransportPeerObservation,
    WorkerExecutionObservation,
    requirements_digest,
)
from project_supervisor.runtime import AdapterRegistry, SupervisorRuntime
from project_supervisor.scheduler import DeterministicScheduler
from project_supervisor.store import StateStore

_PEER_IDENTITY = "node-key:stable-peer-id"
_BROKER_AUTHORITY = "broker-authority-fixture"
_BROKER_REGISTRY = "broker-registry-fixture"
_PROFILE_SHA256 = "d" * 64


class _Discovery:
    def __init__(self, *, online: EvidenceState = EvidenceState.YES) -> None:
        self.online = online
        self.calls = 0

    async def inspect(self, binding) -> TransportPeerObservation:
        del binding
        self.calls += 1
        return TransportPeerObservation(
            provider="tailscale",
            peer_identity=_PEER_IDENTITY,
            platform="windows",
            online=self.online,
            reachable=EvidenceState.UNKNOWN,
            observed_at=datetime.now(UTC),
        )


class _Probe:
    def __init__(self, *results: NodeRuntimeProbeResult) -> None:
        self.results = list(results)
        self.calls = 0

    async def observe(self, binding) -> NodeRuntimeProbeResult:
        del binding
        self.calls += 1
        return self.results[min(self.calls - 1, len(self.results) - 1)]


class _Bootstrap:
    descriptor = FabricRuntimeBootstrapDescriptor(
        adapter_type="fixture.fabric-bootstrap",
        adapter_instance_id="fixture.bootstrap.instance",
        broker_authority_id=_BROKER_AUTHORITY,
        broker_registry_id=_BROKER_REGISTRY,
        service_profile_revision=1,
        service_profile_sha256=_PROFILE_SHA256,
        max_operation_seconds=5,
    )

    def __init__(self, *, fail: bool = False, entered: asyncio.Event | None = None) -> None:
        self.fail = fail
        self.entered = entered
        self.release = asyncio.Event()
        if entered is None:
            self.release.set()
        self.requests: list[FabricRuntimeStartRequest] = []

    async def start_runtime(self, request: FabricRuntimeStartRequest) -> FabricRuntimeStartResult:
        self.requests.append(request)
        if self.entered is not None:
            self.entered.set()
        await self.release.wait()
        if self.fail:
            raise OSError("fixture response was lost")
        return FabricRuntimeStartResult(
            attempt_id=request.attempt_id,
            receipt_id=f"receipt-{request.attempt_id}",
            request_sha256=request.digest,
            binding_id=request.binding_id,
            binding_generation=request.binding_generation,
            node_id=request.node_id,
            service_name=request.service_name,
            broker_authority_id=request.broker_authority_id,
            broker_registry_id=request.broker_registry_id,
            service_profile_revision=request.service_profile_revision,
            service_profile_sha256=request.service_profile_sha256,
            idempotency_key=request.idempotency_key,
            lease_generation=request.lease_generation,
            state="running",
            disposition="desiredStateSatisfied",
            external_start_boundary_crossed="yes",
            service_state="running",
            accepted=True,
            reason_code="runtimeStartAccepted",
            observed_at=datetime.now(UTC),
        )


def _unavailable(worker_id: str) -> NodeRuntimeProbeResult:
    return NodeRuntimeProbeResult(
        worker_ids=(worker_id,),
        authenticated=EvidenceState.UNKNOWN,
        authorized=EvidenceState.YES,
        platform_approval=PlatformApprovalState.NOT_REQUIRED,
        reachable=EvidenceState.YES,
        runtime_available=EvidenceState.NO,
        healthy=EvidenceState.NO,
        capacity_available=EvidenceState.UNKNOWN,
        reason_codes=("runtimeOriginUnavailable",),
    )


def _healthy(worker_id: str) -> NodeRuntimeProbeResult:
    return NodeRuntimeProbeResult(
        worker_ids=(worker_id,),
        authenticated=EvidenceState.YES,
        authorized=EvidenceState.YES,
        platform_approval=PlatformApprovalState.NOT_REQUIRED,
        reachable=EvidenceState.YES,
        runtime_available=EvidenceState.YES,
        healthy=EvidenceState.YES,
        capacity_available=EvidenceState.YES,
        protocol_version="2",
        runtime_identity="fixture-runtime",
    )


async def _seed_recovery(
    tmp_path,
    *,
    task_id: str = "task-runtime-recovery",
    authorization: bool = True,
    platform: PlatformApprovalState = PlatformApprovalState.NOT_REQUIRED,
):
    workspace = tmp_path / "workspace"
    workspace.mkdir(exist_ok=True)
    store = StateStore(tmp_path / "state.db")
    store.create_project(
        project_id="project-runtime-recovery",
        name="Runtime recovery",
        root_path=str(workspace),
        goal="Recover one typed Fabric runtime",
    )
    store.upsert_node(
        node_id="node-windows",
        hostname="private-node",
        display_name="Windows Fabric Node",
        role="worker",
        state=NodeState.ONLINE,
    )
    worker_id = "worker-windows"
    store.upsert_worker(
        WorkerSnapshot(
            id=worker_id,
            node_id="node-windows",
            harness=Harness.MOCK,
            provider=Provider.MOCK,
            model=ModelDescriptor("fixture", "Fixture", Provider.MOCK),
            state=WorkerState.IDLE,
            node_state=NodeState.ONLINE,
            resource_state=ResourceState.AVAILABLE,
            capabilities=frozenset({"analysis"}),
            worker_classes=frozenset({"runtimeWorker"}),
            code_write_allowed=False,
            privacy_allowed=True,
        )
    )
    runtime = SupervisorRuntime(
        store=store,
        scheduler=DeterministicScheduler(),
        adapters=AdapterRegistry(),
        evidence_root=tmp_path / "evidence",
    )
    requirements = TaskRequirements(
        labels=frozenset({TaskLabel.RESEARCH}),
        required_capabilities=frozenset({"analysis"}),
    )
    await runtime.submit_task(
        project_id="project-runtime-recovery",
        task_id=task_id,
        title="Recover runtime",
        description="Typed recovery only",
        topology=ExecutionTopology.SINGLE,
        requirements=requirements,
    )
    repository = ExecutionPlaneRepository(store)
    binding = repository.register_binding(
        node_id="node-windows",
        transport_provider="tailscale",
        peer_identity=_PEER_IDENTITY,
        service_name="local-worker",
        endpoint_ref="endpoint.windows-worker",
        configured_by="operator.test",
        expected_platform="windows",
    )
    digest = requirements_digest(requirements)
    repository.wait_for_capacity(
        task_id=task_id,
        requirements_sha256=digest,
        reason_code="RUNTIME_NOT_AVAILABLE",
    )
    if authorization:
        envelope = AuthorizationEnvelope(
            authorization_id=f"authorization-{task_id}",
            project_id="project-runtime-recovery",
            root_task_id=task_id,
            subject="fabric.runtime.recovery",
            permission_ceiling=PermissionClass.YELLOW,
            capabilities=frozenset({"analysis"}),
            actions=frozenset({"analysis.propose", "evidence.read", "fabric.runtime.start"}),
            allowed_providers=frozenset({"mock"}),
            allowed_action_classes=frozenset(
                {"analysis.propose", "evidence.read", "fabric.runtime.start"}
            ),
            allowed_worker_classes=frozenset({"runtimeBroker", "runtimeWorker"}),
            issued_by="operator.test",
            expires_at=datetime.now(UTC) + timedelta(minutes=5),
            platform_approval_required=platform is not PlatformApprovalState.NOT_REQUIRED,
            platform_approval_state=platform,
        )
        authorizations = AuthorizationRepository(store)
        authorizations.issue(envelope)
        if envelope.permits_dispatch():
            authorizations.bind_task(envelope.authorization_id, task_id)
        else:
            # A stale/corrupt binding cannot make a rejected platform decision executable.
            with store.transaction() as connection:
                connection.execute(
                    "INSERT INTO authorization_envelope_bindings(id,envelope_id,task_id,run_id,"
                    "binding_kind,bound_by,created_at) VALUES (?,?,?,NULL,'task',?,?)",
                    (
                        f"fixture-binding-{task_id}",
                        envelope.authorization_id,
                        task_id,
                        "fixture.corrupt-state",
                        datetime.now(UTC).isoformat().replace("+00:00", "Z"),
                    ),
                )
    return store, repository, binding, requirements, worker_id


def test_bootstrap_contract_is_typed_and_rejects_weakened_fencing() -> None:
    with pytest.raises(ValueError, match="generation, deadline and idempotency"):
        FabricRuntimeBootstrapDescriptor(
            adapter_type="fixture.bootstrap",
            adapter_instance_id="fixture.instance",
            broker_authority_id=_BROKER_AUTHORITY,
            broker_registry_id=_BROKER_REGISTRY,
            service_profile_revision=1,
            service_profile_sha256=_PROFILE_SHA256,
            enforces_idempotency=False,
        )

    request = FabricRuntimeStartRequest(
        attempt_id="attempt-one",
        binding_id="binding-one",
        binding_generation=1,
        node_id="node-one",
        service_name="local-worker",
        task_id="task-one",
        requirements_sha256="a" * 64,
        authorization_id="authorization-one",
        authorization_version=1,
        authorization_sha256="c" * 64,
        broker_authority_id=_BROKER_AUTHORITY,
        broker_registry_id=_BROKER_REGISTRY,
        service_profile_revision=1,
        service_profile_sha256=_PROFILE_SHA256,
        lease_owner_id="owner-one",
        lease_generation=1,
        idempotency_key="runtime-start-one",
        requested_at=datetime.now(UTC),
        deadline_at=datetime.now(UTC) + timedelta(seconds=5),
    )
    protocol = request.to_protocol()
    assert protocol["operation"] == "fabric.runtime.start"
    assert not {"command", "argv", "env", "cwd", "shell", "executable"}.intersection(protocol)


async def test_https_bootstrap_uses_fixed_path_bounded_typed_receipt() -> None:
    token = "fixture-token-that-is-long-enough-1234567890"
    observed: list[httpx.Request] = []

    def handler(message: httpx.Request) -> httpx.Response:
        observed.append(message)
        assert message.method == "POST"
        assert message.url.path == "/v1/fabric/runtime/start"
        assert message.headers["authorization"] == f"Bearer {token}"
        payload = json.loads(message.content)
        assert payload["operation"] == "fabric.runtime.start"
        assert not {"command", "argv", "env", "cwd", "shell"}.intersection(payload)
        return httpx.Response(
            202,
            json={
                "schemaVersion": "fabric-runtime-start-result/v1",
                "attemptID": payload["attemptID"],
                "receiptID": f"receipt-{payload['attemptID']}",
                "requestSHA256": request.digest,
                "bindingID": payload["bindingID"],
                "bindingGeneration": payload["bindingGeneration"],
                "nodeID": payload["nodeID"],
                "serviceName": payload["serviceName"],
                "brokerAuthorityID": payload["brokerAuthorityID"],
                "brokerRegistryID": payload["brokerRegistryID"],
                "serviceProfileRevision": payload["serviceProfileRevision"],
                "serviceProfileSHA256": payload["serviceProfileSHA256"],
                "idempotencyKey": payload["idempotencyKey"],
                "leaseGeneration": payload["leaseGeneration"],
                "state": "running",
                "disposition": "desiredStateSatisfied",
                "externalStartBoundaryCrossed": "yes",
                "serviceState": "running",
                "accepted": True,
                "reasonCode": "runtimeStartAccepted",
                "observedAt": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
                "idempotentReplay": False,
            },
        )

    now = datetime.now(UTC)
    request = FabricRuntimeStartRequest(
        attempt_id="attempt-https",
        binding_id="binding-https",
        binding_generation=2,
        node_id="node-windows",
        service_name="local-worker",
        task_id="task-https",
        requirements_sha256="b" * 64,
        authorization_id="authorization-https",
        authorization_version=1,
        authorization_sha256="c" * 64,
        broker_authority_id=_BROKER_AUTHORITY,
        broker_registry_id=_BROKER_REGISTRY,
        service_profile_revision=1,
        service_profile_sha256=_PROFILE_SHA256,
        lease_owner_id="recovery.owner",
        lease_generation=3,
        idempotency_key="runtime-start-https",
        requested_at=now,
        deadline_at=now + timedelta(seconds=5),
    )
    bootstrap = HTTPSFabricRuntimeBootstrap(
        "https://worker.example.test",
        bearer_token=token,
        broker_authority_id=_BROKER_AUTHORITY,
        broker_registry_id=_BROKER_REGISTRY,
        service_profile_revision=1,
        service_profile_sha256=_PROFILE_SHA256,
        transport=httpx.MockTransport(handler),
        timeout_seconds=5,
    )

    result = await bootstrap.start_runtime(request)

    assert result.accepted is True
    assert result.lease_generation == 3
    assert len(observed) == 1
    with pytest.raises(ValueError, match="require HTTPS"):
        HTTPSFabricRuntimeBootstrap(
            "http://worker.example.test",
            bearer_token=token,
            broker_authority_id=_BROKER_AUTHORITY,
            broker_registry_id=_BROKER_REGISTRY,
            service_profile_revision=1,
            service_profile_sha256=_PROFILE_SHA256,
        )


async def test_tailscale_cli_discovery_uses_fixed_read_only_argv(tmp_path) -> None:
    executable = tmp_path / "tailscale-fixture"
    executable.write_text(
        "#!/bin/sh\n"
        'test "$1" = status || exit 11\n'
        'test "$2" = --json || exit 12\n'
        'printf \'%s\' \'{"Peer":{"node-key":{"ID":"stable-peer-id",'
        '"OS":"windows","Online":true}}}\'\n',
        encoding="utf-8",
    )
    executable.chmod(0o700)
    discovery = TailscaleCLITransportDiscovery(str(executable))
    identity = _PEER_IDENTITY

    observation = await discovery.inspect(
        {
            "transport_provider": "tailscale",
            "peer_identity_sha256": hashlib.sha256(identity.encode()).hexdigest(),
        }
    )

    assert observation.identity_sha256 == hashlib.sha256(identity.encode()).hexdigest()
    assert observation.online is EvidenceState.YES
    assert observation.reachable is EvidenceState.UNKNOWN


async def test_local_worker_probe_reports_502_as_origin_unavailable() -> None:
    endpoint = "http://127.0.0.1:7331"
    async with httpx.AsyncClient(
        base_url=endpoint,
        transport=httpx.MockTransport(lambda _: httpx.Response(502)),
    ) as client:
        adapter = LocalWorkerAdapter(endpoint, client=client)
        probe = LocalWorkerFabricRuntimeProbe(
            (
                LocalWorkerProbeBinding(
                    endpoint_ref="endpoint.windows-worker",
                    node_id="node-windows",
                    worker_ids=("worker-windows",),
                    adapter=adapter,
                    authorized=EvidenceState.YES,
                    platform_approval=PlatformApprovalState.NOT_REQUIRED,
                ),
            )
        )
        result = await probe.observe(
            {"endpoint_ref": "endpoint.windows-worker", "node_id": "node-windows"}
        )

    assert result.reachable is EvidenceState.YES
    assert result.runtime_available is EvidenceState.NO
    assert result.capacity_available is EvidenceState.UNKNOWN
    assert result.reason_codes == ("runtimeOriginUnavailable",)


@pytest.mark.parametrize(
    ("authorization", "platform"),
    (
        (False, PlatformApprovalState.NOT_REQUIRED),
        (True, PlatformApprovalState.REJECTED),
        (True, PlatformApprovalState.UNKNOWN),
    ),
)
async def test_bootstrap_requires_current_user_and_platform_authority(
    tmp_path, authorization, platform
) -> None:
    _, repository, _, requirements, worker_id = await _seed_recovery(
        tmp_path,
        authorization=authorization,
        platform=platform,
    )
    bootstrap = _Bootstrap()
    recovered = await ExecutionPlaneRecoveryCoordinator(
        repository=repository,
        discovery=_Discovery(),
        probe=_Probe(_unavailable(worker_id)),
        bootstrap=bootstrap,
        recovery_owner_id="recovery.owner",
    ).recover_capacity(
        task_id="task-runtime-recovery",
        requirements=requirements,
        rejection_codes=("RUNTIME_NOT_AVAILABLE",),
    )

    assert recovered is False
    assert bootstrap.requests == []


async def test_offline_peer_never_bootstraps(tmp_path) -> None:
    _, repository, _, requirements, worker_id = await _seed_recovery(tmp_path)
    bootstrap = _Bootstrap()
    recovered = await ExecutionPlaneRecoveryCoordinator(
        repository=repository,
        discovery=_Discovery(online=EvidenceState.NO),
        probe=_Probe(_unavailable(worker_id)),
        bootstrap=bootstrap,
        recovery_owner_id="recovery.owner",
    ).recover_capacity(
        task_id="task-runtime-recovery",
        requirements=requirements,
        rejection_codes=("RUNTIME_NOT_AVAILABLE",),
    )
    assert recovered is False
    assert bootstrap.requests == []


async def test_typed_bootstrap_requires_matching_receipt_and_verified_probe(tmp_path) -> None:
    store, repository, binding, requirements, worker_id = await _seed_recovery(tmp_path)
    bootstrap = _Bootstrap()
    recovered = await ExecutionPlaneRecoveryCoordinator(
        repository=repository,
        discovery=_Discovery(),
        probe=_Probe(_unavailable(worker_id), _healthy(worker_id)),
        bootstrap=bootstrap,
        recovery_owner_id="recovery.owner",
    ).recover_capacity(
        task_id="task-runtime-recovery",
        requirements=requirements,
        rejection_codes=("RUNTIME_NOT_AVAILABLE",),
    )

    assert recovered is True
    assert len(bootstrap.requests) == 1
    request = bootstrap.requests[0]
    assert request.binding_id == binding["id"]
    with store.connect() as connection:
        stages = [
            row["stage"]
            for row in connection.execute(
                "SELECT stage FROM node_execution_recovery_events WHERE attempt_id=? "
                "ORDER BY ordinal",
                (request.attempt_id,),
            ).fetchall()
        ]
    assert stages == ["requested", "accepted", "verified"]

    mismatch = replace(
        FabricRuntimeStartResult(
            attempt_id=request.attempt_id,
            receipt_id=f"receipt-{request.attempt_id}",
            request_sha256=request.digest,
            binding_id=request.binding_id,
            binding_generation=request.binding_generation,
            node_id=request.node_id,
            service_name=request.service_name,
            broker_authority_id=request.broker_authority_id,
            broker_registry_id=request.broker_registry_id,
            service_profile_revision=request.service_profile_revision,
            service_profile_sha256=request.service_profile_sha256,
            idempotency_key=request.idempotency_key,
            lease_generation=request.lease_generation,
            state="running",
            disposition="desiredStateSatisfied",
            external_start_boundary_crossed="yes",
            service_state="running",
            accepted=True,
            reason_code="runtimeStartAccepted",
            observed_at=datetime.now(UTC),
        ),
        lease_generation=request.lease_generation + 1,
    )
    with pytest.raises(ValueError, match="does not match"):
        repository.record_runtime_start_result(request, mismatch)


async def test_runtime_auto_resumes_original_task_after_verified_bootstrap(tmp_path) -> None:
    store, repository, _, requirements, worker_id = await _seed_recovery(tmp_path)
    observed_at = datetime.now(UTC)
    repository.record_worker_observation(
        WorkerExecutionObservation(
            worker_id=worker_id,
            node_id="node-windows",
            discovered=EvidenceState.YES,
            configured=EvidenceState.YES,
            authenticated=EvidenceState.UNKNOWN,
            authorized=EvidenceState.YES,
            platform_approval=PlatformApprovalState.NOT_REQUIRED,
            reachable=EvidenceState.YES,
            runtime_available=EvidenceState.NO,
            healthy=EvidenceState.NO,
            capacity_available=EvidenceState.UNKNOWN,
            observed_at=observed_at,
            valid_until=observed_at + timedelta(seconds=30),
        )
    )
    registry = AdapterRegistry()
    registry.register(worker_id, MockAdapter())
    bootstrap = _Bootstrap()
    coordinator = ExecutionPlaneRecoveryCoordinator(
        repository=repository,
        discovery=_Discovery(),
        probe=_Probe(_unavailable(worker_id), _healthy(worker_id)),
        bootstrap=bootstrap,
        recovery_owner_id="recovery.owner",
    )
    runtime = SupervisorRuntime(
        store=store,
        scheduler=DeterministicScheduler(),
        adapters=registry,
        evidence_root=tmp_path / "runtime-evidence",
        execution_plane_recovery=coordinator,
    )

    await runtime.run_until_idle()

    assert store.get_task("task-runtime-recovery")["state"] == TaskState.REVIEWING.value
    assert len(store.list_worker_runs("task-runtime-recovery")) == 1
    assert len(bootstrap.requests) == 1


async def test_lost_bootstrap_response_replays_same_identity_and_converges(tmp_path) -> None:
    store, repository, _, requirements, worker_id = await _seed_recovery(tmp_path)
    bootstrap = _Bootstrap(fail=True)
    coordinator = ExecutionPlaneRecoveryCoordinator(
        repository=repository,
        discovery=_Discovery(),
        probe=_Probe(_unavailable(worker_id), _unavailable(worker_id), _healthy(worker_id)),
        bootstrap=bootstrap,
        recovery_owner_id="recovery.owner",
    )
    first = await coordinator.recover_capacity(
        task_id="task-runtime-recovery",
        requirements=requirements,
        rejection_codes=("RUNTIME_NOT_AVAILABLE",),
    )
    bootstrap.fail = False
    second = await coordinator.recover_capacity(
        task_id="task-runtime-recovery",
        requirements=requirements,
        rejection_codes=("RUNTIME_NOT_AVAILABLE",),
    )

    assert first is False and second is True
    assert len(bootstrap.requests) == 2
    assert bootstrap.requests[0].to_protocol() == bootstrap.requests[1].to_protocol()
    assert bootstrap.requests[0].digest == bootstrap.requests[1].digest
    with store.connect() as connection:
        stages = [
            row["stage"]
            for row in connection.execute(
                "SELECT stage FROM node_execution_recovery_events ORDER BY created_at,ordinal"
            ).fetchall()
        ]
    assert stages == ["requested", "outcomeUnknown", "accepted", "verified"]


async def test_concurrent_capacity_waits_share_one_binding_recovery_lease(tmp_path) -> None:
    _, repository, _, requirements, worker_id = await _seed_recovery(tmp_path)
    second_task = "task-runtime-recovery-two"
    runtime = SupervisorRuntime(
        store=repository.store,
        scheduler=DeterministicScheduler(),
        adapters=AdapterRegistry(),
        evidence_root=tmp_path / "evidence-two",
    )
    await runtime.submit_task(
        project_id="project-runtime-recovery",
        task_id=second_task,
        title="Recover same runtime",
        description="A second bounded waiter",
        requirements=requirements,
    )
    repository.wait_for_capacity(
        task_id=second_task,
        requirements_sha256=requirements_digest(requirements),
        reason_code="RUNTIME_NOT_AVAILABLE",
    )
    envelope = AuthorizationEnvelope(
        authorization_id="authorization-second-task",
        project_id="project-runtime-recovery",
        root_task_id=second_task,
        subject="fabric.runtime.recovery",
        permission_ceiling=PermissionClass.YELLOW,
        actions=frozenset({"fabric.runtime.start"}),
        allowed_action_classes=frozenset({"fabric.runtime.start"}),
        issued_by="operator.test",
        expires_at=datetime.now(UTC) + timedelta(minutes=5),
    )
    authorizations = AuthorizationRepository(repository.store)
    authorizations.issue(envelope)
    authorizations.bind_task(envelope.authorization_id, second_task)
    entered = asyncio.Event()
    bootstrap = _Bootstrap(entered=entered)
    coordinator = ExecutionPlaneRecoveryCoordinator(
        repository=repository,
        discovery=_Discovery(),
        probe=_Probe(_unavailable(worker_id)),
        bootstrap=bootstrap,
        recovery_owner_id="recovery.owner",
    )

    first = asyncio.create_task(
        coordinator.recover_capacity(
            task_id="task-runtime-recovery",
            requirements=requirements,
            rejection_codes=("RUNTIME_NOT_AVAILABLE",),
        )
    )
    await entered.wait()
    second = asyncio.create_task(
        coordinator.recover_capacity(
            task_id=second_task,
            requirements=requirements,
            rejection_codes=("RUNTIME_NOT_AVAILABLE",),
        )
    )
    await asyncio.sleep(0)
    bootstrap.release.set()
    await asyncio.gather(first, second)

    assert len(bootstrap.requests) == 1


async def test_runtime_recovery_event_replay_is_idempotent_but_conflicts_fail(tmp_path) -> None:
    _, repository, binding, requirements, _ = await _seed_recovery(tmp_path)
    generation = repository.claim_binding_recovery(
        binding["id"], owner_id="recovery.owner", lease_seconds=30
    )
    assert generation is not None
    request = repository.begin_runtime_start(
        binding_id=binding["id"],
        task_id="task-runtime-recovery",
        requirements_sha256=requirements_digest(requirements),
        bootstrap_descriptor=_Bootstrap.descriptor,
        owner_id="recovery.owner",
        lease_generation=generation,
        deadline_seconds=5,
    )
    result = FabricRuntimeStartResult(
        attempt_id=request.attempt_id,
        receipt_id=f"receipt-{request.attempt_id}",
        request_sha256=request.digest,
        binding_id=request.binding_id,
        binding_generation=request.binding_generation,
        node_id=request.node_id,
        service_name=request.service_name,
        broker_authority_id=request.broker_authority_id,
        broker_registry_id=request.broker_registry_id,
        service_profile_revision=request.service_profile_revision,
        service_profile_sha256=request.service_profile_sha256,
        idempotency_key=request.idempotency_key,
        lease_generation=request.lease_generation,
        state="running",
        disposition="desiredStateSatisfied",
        external_start_boundary_crossed="yes",
        service_state="running",
        accepted=True,
        reason_code="runtimeStartAccepted",
        observed_at=datetime.now(UTC),
    )
    repository.record_runtime_start_result(request, result)
    repository.record_runtime_start_result(request, result)

    with pytest.raises(RuntimeError, match="monotonic history"):
        repository.record_runtime_start_outcome_unknown(
            request,
            reason_code="lateAmbiguity",
        )
    with pytest.raises(RuntimeError, match="conflicting runtime recovery event replay"):
        repository.record_runtime_start_result(
            request,
            replace(result, reason_code="differentAuthoritativeReceipt"),
        )


async def test_runtime_recovery_records_repeated_nonterminal_observations_in_order(
    tmp_path,
) -> None:
    store, repository, binding, requirements, _ = await _seed_recovery(tmp_path)
    generation = repository.claim_binding_recovery(
        binding["id"], owner_id="recovery.owner", lease_seconds=30
    )
    assert generation is not None
    request = repository.begin_runtime_start(
        binding_id=binding["id"],
        task_id="task-runtime-recovery",
        requirements_sha256=requirements_digest(requirements),
        bootstrap_descriptor=_Bootstrap.descriptor,
        owner_id="recovery.owner",
        lease_generation=generation,
        deadline_seconds=5,
    )
    base = FabricRuntimeStartResult(
        attempt_id=request.attempt_id,
        receipt_id=f"receipt-{request.attempt_id}",
        request_sha256=request.digest,
        binding_id=request.binding_id,
        binding_generation=request.binding_generation,
        node_id=request.node_id,
        service_name=request.service_name,
        broker_authority_id=request.broker_authority_id,
        broker_registry_id=request.broker_registry_id,
        service_profile_revision=request.service_profile_revision,
        service_profile_sha256=request.service_profile_sha256,
        idempotency_key=request.idempotency_key,
        lease_generation=request.lease_generation,
        state="startPending",
        disposition="startInProgress",
        external_start_boundary_crossed="unknown",
        service_state="startPending",
        accepted=False,
        reason_code="runtimeStartPending",
        observed_at=datetime.now(UTC),
    )
    unknown = replace(
        base,
        state="outcomeUnknown",
        disposition="startOutcomeUnknown",
        service_state="unknown",
        reason_code="runtimeStartOutcomeUnknown",
        observed_at=base.observed_at + timedelta(milliseconds=1),
    )
    second_pending = replace(
        base,
        reason_code="runtimeStartStillPending",
        observed_at=base.observed_at + timedelta(milliseconds=2),
    )
    running = replace(
        base,
        state="running",
        disposition="desiredStateSatisfied",
        external_start_boundary_crossed="unknown",
        service_state="running",
        accepted=True,
        reason_code="runtimeObservedRunningAfterUncertainStart",
        observed_at=base.observed_at + timedelta(milliseconds=3),
    )

    for observation in (base, unknown, second_pending, running):
        repository.record_runtime_start_result(request, observation)
    repository.record_runtime_start_verified(request)

    with store.connect() as connection:
        rows = connection.execute(
            "SELECT event_key,stage FROM node_execution_recovery_events "
            "WHERE attempt_id=? ORDER BY ordinal",
            (request.attempt_id,),
        ).fetchall()
    assert [(row["event_key"], row["stage"]) for row in rows] == [
        ("request", "requested"),
        ("result:startPending", "outcomeUnknown"),
        ("result:outcomeUnknown", "outcomeUnknown"),
        ("result:startPending:4", "outcomeUnknown"),
        ("result:running", "accepted"),
        ("verified", "verified"),
    ]
