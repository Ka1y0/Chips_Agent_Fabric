from __future__ import annotations

import asyncio
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path

import httpx
import pytest

from project_supervisor.fabric.execution_plane import FabricRuntimeStartRequest
from project_supervisor.local_worker_v2.runtime_broker import (
    BrokerConflict,
    BrokerUnavailable,
    RuntimeBrokerProfile,
    RuntimeBrokerRegistry,
    RuntimeBrokerService,
    ServiceState,
    StartDisposition,
    WindowsSCMServiceController,
    create_runtime_broker_app,
)

pytestmark = pytest.mark.asyncio

_TOKEN = "runtime-broker-fixture-token-0123456789abcdef"


class _Controller:
    def __init__(
        self,
        *,
        state: ServiceState = ServiceState.STOPPED,
        disposition: StartDisposition = StartDisposition.ACCEPTED,
        after_start: ServiceState | None = ServiceState.RUNNING,
        entered: asyncio.Event | None = None,
        release: asyncio.Event | None = None,
        observe_delay: float = 0,
    ) -> None:
        self.state = state
        self.disposition = disposition
        self.after_start = after_start
        self.entered = entered
        self.release = release
        self.observe_delay = observe_delay
        self.start_calls = 0

    async def observe(self) -> ServiceState:
        if self.observe_delay:
            await asyncio.sleep(self.observe_delay)
        return self.state

    async def start(self, *, deadline_at: datetime) -> StartDisposition:
        assert deadline_at.tzinfo is not None
        self.start_calls += 1
        if self.entered is not None:
            self.entered.set()
        if self.release is not None:
            await self.release.wait()
        if self.after_start is not None:
            self.state = self.after_start
        return self.disposition


def _profile() -> RuntimeBrokerProfile:
    return RuntimeBrokerProfile(
        broker_authority_id="broker-authority-fixture",
        node_id="node-windows-fixture",
        binding_id="binding-windows-fixture",
        service_profile_id="local-worker",
        service_profile_revision=3,
        target_service_name="ChipsFabricLocalWorker",
    )


def _registry(tmp_path: Path) -> RuntimeBrokerRegistry:
    path = tmp_path / "broker" / "registry.db"
    RuntimeBrokerRegistry.initialize(
        path,
        _profile(),
        test_host_identity="fixture-host",
    )
    return RuntimeBrokerRegistry(
        path,
        _profile(),
        test_host_identity="fixture-host",
    )


def _request(
    registry: RuntimeBrokerRegistry,
    *,
    attempt: str = "runtime-attempt-1",
    key: str = "runtime-key-1",
    binding_generation: int = 1,
    lease_generation: int = 1,
    requested_at: datetime | None = None,
    deadline_at: datetime | None = None,
) -> FabricRuntimeStartRequest:
    now = requested_at or datetime.now(UTC)
    return FabricRuntimeStartRequest(
        attempt_id=attempt,
        binding_id=registry.profile.binding_id,
        binding_generation=binding_generation,
        node_id=registry.profile.node_id,
        service_name=registry.profile.service_profile_id,
        task_id="task-runtime-recovery",
        requirements_sha256="a" * 64,
        authorization_id="authorization-runtime-recovery",
        authorization_version=1,
        authorization_sha256="b" * 64,
        broker_authority_id=registry.profile.broker_authority_id,
        broker_registry_id=registry.registry_id,
        service_profile_revision=registry.profile.service_profile_revision,
        service_profile_sha256=registry.profile.digest,
        lease_owner_id="supervisor-recovery-owner",
        lease_generation=lease_generation,
        idempotency_key=key,
        requested_at=now,
        deadline_at=deadline_at or now + timedelta(seconds=30),
    )


async def _post(
    app,
    request: FabricRuntimeStartRequest,
    *,
    token: str = _TOKEN,
) -> httpx.Response:
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://broker.test",
    ) as client:
        return await client.post(
            "/v1/fabric/runtime/start",
            headers={"authorization": f"Bearer {token}"},
            json=request.to_protocol(),
        )


async def test_broker_auth_health_and_public_projection_are_fixed_and_safe(tmp_path) -> None:
    registry = _registry(tmp_path)
    app = create_runtime_broker_app(
        RuntimeBrokerService(registry, _Controller()),
        auth_token=_TOKEN,
    )
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://broker.test") as client:
        unauthorized = await client.get("/v1/health")
        assert unauthorized.status_code == 401
        response = await client.get(
            "/v1/health",
            headers={"authorization": f"Bearer {_TOKEN}"},
        )
        docs = await client.get(
            "/docs",
            headers={"authorization": f"Bearer {_TOKEN}"},
        )

    assert response.status_code == 200
    assert response.headers["cache-control"] == "no-store"
    assert response.json() == {
        "schemaVersion": "fabric-runtime-broker-health/v1",
        "brokerAuthorityID": registry.profile.broker_authority_id,
        "brokerRegistryID": registry.registry_id,
        "nodeID": registry.profile.node_id,
        "serviceProfileID": registry.profile.service_profile_id,
        "serviceProfileRevision": registry.profile.service_profile_revision,
        "serviceProfileSHA256": registry.profile.digest,
        "operation": "fabric.runtime.start",
        "durableIdempotency": True,
        "generationFencing": True,
        "deadlineEnforcement": True,
    }
    assert docs.status_code == 404
    assert registry.profile.target_service_name not in response.text


async def test_registry_initialization_is_concurrent_and_rotation_fails_closed(tmp_path) -> None:
    path = tmp_path / "broker" / "registry.db"

    def initialize() -> str:
        return RuntimeBrokerRegistry.initialize(
            path,
            _profile(),
            test_host_identity="fixture-host",
        )

    first, second = await asyncio.gather(
        asyncio.to_thread(initialize),
        asyncio.to_thread(initialize),
    )
    assert first == second
    registry = RuntimeBrokerRegistry(
        path,
        _profile(),
        test_host_identity="fixture-host",
    )
    replacement = tmp_path / "replacement.db"
    replacement_id = RuntimeBrokerRegistry.initialize(
        replacement,
        _profile(),
        test_host_identity="fixture-host",
    )
    assert replacement_id != registry.registry_id
    replacement.replace(path)

    with pytest.raises(BrokerUnavailable, match="identity changed"):
        registry._verify_registry()


async def test_windows_service_profile_digest_includes_every_multisz_dependency() -> None:
    import ctypes

    first_raw = "RpcSs\0Tcpip\0\0".encode("utf-16-le")
    second_raw = "RpcSs\0Dnscache\0\0".encode("utf-16-le")
    first = ctypes.create_string_buffer(first_raw)
    second = ctypes.create_string_buffer(second_raw)
    first_dependencies = WindowsSCMServiceController._read_windows_multisz(
        ctypes.addressof(first),
        buffer_address=ctypes.addressof(first),
        buffer_size=len(first_raw),
    )
    second_dependencies = WindowsSCMServiceController._read_windows_multisz(
        ctypes.addressof(second),
        buffer_address=ctypes.addressof(second),
        buffer_size=len(second_raw),
    )
    base = {
        "serviceType": 16,
        "startType": 3,
        "errorControl": 1,
        "binaryPathName": "C:\\Program Files\\Chips\\worker.exe",
        "loadOrderGroup": "",
        "tagID": 0,
        "serviceStartName": "NT SERVICE\\ChipsWorker",
    }

    assert first_dependencies == ("RpcSs", "Tcpip")
    assert second_dependencies == ("RpcSs", "Dnscache")
    assert WindowsSCMServiceController._service_config_sha256(
        {**base, "dependencies": first_dependencies}
    ) != WindowsSCMServiceController._service_config_sha256(
        {**base, "dependencies": second_dependencies}
    )


async def test_concurrent_duplicate_and_lost_response_replay_start_once(tmp_path) -> None:
    registry = _registry(tmp_path)
    entered = asyncio.Event()
    release = asyncio.Event()
    controller = _Controller(entered=entered, release=release)
    app = create_runtime_broker_app(
        RuntimeBrokerService(registry, controller),
        auth_token=_TOKEN,
    )
    request = _request(registry)

    first = asyncio.create_task(_post(app, request))
    await entered.wait()
    second = asyncio.create_task(_post(app, request))
    release.set()
    first_response, second_response = await asyncio.gather(first, second)
    replay = await _post(app, request)

    assert first_response.status_code == second_response.status_code == replay.status_code == 200
    assert controller.start_calls == 1
    receipts = [response.json() for response in (first_response, second_response, replay)]
    assert len({receipt["receiptID"] for receipt in receipts}) == 1
    assert len({receipt["requestSHA256"] for receipt in receipts}) == 1
    assert replay.json()["idempotentReplay"] is True
    assert replay.json()["state"] == "running"


async def test_cross_process_style_dispatch_claim_has_exactly_one_winner(tmp_path) -> None:
    registry = _registry(tmp_path)
    second_registry = RuntimeBrokerRegistry(
        registry.database_path,
        registry.profile,
        test_host_identity="fixture-host",
    )
    observed = 0
    both_observed = asyncio.Event()
    release = asyncio.Event()

    class RaceController(_Controller):
        async def observe(self) -> ServiceState:
            nonlocal observed
            state = self.state
            observed += 1
            if observed == 2:
                both_observed.set()
            if observed <= 2:
                await release.wait()
            return state

    controller = RaceController()
    request = _request(registry)
    first = asyncio.create_task(RuntimeBrokerService(registry, controller).start(request))
    second = asyncio.create_task(RuntimeBrokerService(second_registry, controller).start(request))
    await both_observed.wait()
    release.set()
    results = await asyncio.gather(first, second)

    assert controller.start_calls == 1
    assert {result.state for result in results} == {"running"}
    assert len({result.receipt_id for result in results}) == 1


async def test_reserved_operation_blocks_new_generation_and_remains_current(tmp_path) -> None:
    registry = _registry(tmp_path)
    first = _request(registry)
    registry.reserve(first)
    second = _request(
        registry,
        attempt="runtime-attempt-2",
        key="runtime-key-2",
        lease_generation=2,
    )

    with pytest.raises(BrokerConflict, match="prior runtime start outcome"):
        registry.reserve(second)
    result = await RuntimeBrokerService(registry, _Controller()).start(first)
    assert result.state == "running"


async def test_slow_observation_cannot_cross_an_expired_deadline(tmp_path) -> None:
    registry = _registry(tmp_path)
    controller = _Controller(observe_delay=0.05)
    now = datetime.now(UTC)
    request = _request(
        registry,
        requested_at=now,
        deadline_at=now + timedelta(seconds=0.02),
    )

    result = await RuntimeBrokerService(registry, controller).start(request)

    assert result.state == "rejectedPreStart"
    assert result.external_start_boundary_crossed == "no"
    assert controller.start_calls == 0


async def test_pending_receipt_reobserves_and_converges_without_second_start(tmp_path) -> None:
    registry = _registry(tmp_path)
    controller = _Controller(after_start=ServiceState.START_PENDING)
    service = RuntimeBrokerService(registry, controller)
    request = _request(registry)

    pending = await service.start(request)
    controller.state = ServiceState.RUNNING
    running = await service.start(request)

    assert pending.state == "startPending"
    assert running.state == "running"
    assert running.idempotent_replay is True
    assert running.external_start_boundary_crossed == "yes"
    assert controller.start_calls == 1


async def test_repeated_pending_observation_is_append_only_and_does_not_restart(tmp_path) -> None:
    registry = _registry(tmp_path)
    controller = _Controller(after_start=ServiceState.START_PENDING)
    service = RuntimeBrokerService(registry, controller)
    request = _request(registry)

    first_pending = await service.start(request)
    controller.state = ServiceState.UNKNOWN
    unknown = await service.start(request)
    controller.state = ServiceState.START_PENDING
    second_pending = await service.start(request)

    assert first_pending.state == second_pending.state == "startPending"
    assert unknown.state == "outcomeUnknown"
    assert second_pending.idempotent_replay is True
    assert controller.start_calls == 1
    with registry._connect() as connection:
        observations = connection.execute(
            "SELECT ordinal,event_key,state FROM runtime_start_observations "
            "WHERE attempt_id=? ORDER BY ordinal",
            (request.attempt_id,),
        ).fetchall()
    assert [row["state"] for row in observations] == [
        "reserved",
        "dispatching",
        "startPending",
        "outcomeUnknown",
        "startPending",
    ]
    assert observations[-1]["event_key"] == "startPending:5"


@pytest.mark.parametrize(
    ("disposition", "observed", "expected_state", "expected_boundary"),
    [
        (StartDisposition.ACCEPTED, ServiceState.RUNNING, "running", "yes"),
        (StartDisposition.ACCEPTED, ServiceState.STOPPED, "outcomeUnknown", "yes"),
        (StartDisposition.ALREADY_RUNNING, ServiceState.RUNNING, "alreadyRunning", "no"),
        (
            StartDisposition.REJECTED_PRE_START,
            ServiceState.RUNNING,
            "alreadyRunning",
            "no",
        ),
        (StartDisposition.UNKNOWN, ServiceState.RUNNING, "alreadyRunning", "unknown"),
    ],
)
async def test_start_disposition_is_authoritative_for_boundary_receipt(
    tmp_path,
    disposition: StartDisposition,
    observed: ServiceState,
    expected_state: str,
    expected_boundary: str,
) -> None:
    registry = _registry(tmp_path)
    controller = _Controller(disposition=disposition, after_start=observed)

    result = await RuntimeBrokerService(registry, controller).start(_request(registry))

    assert result.state == expected_state
    assert result.external_start_boundary_crossed == expected_boundary
    assert controller.start_calls == 1


async def test_conflicting_replay_and_generation_skips_are_rejected_without_start(tmp_path) -> None:
    registry = _registry(tmp_path)
    controller = _Controller()
    app = create_runtime_broker_app(
        RuntimeBrokerService(registry, controller),
        auth_token=_TOKEN,
    )
    request = _request(registry)
    assert (await _post(app, request)).status_code == 200

    conflict = replace(request, authorization_sha256="c" * 64)
    same_generation = _request(
        registry,
        attempt="runtime-attempt-other",
        key="runtime-key-other",
    )
    skipped_generation = _request(
        registry,
        attempt="runtime-attempt-skipped",
        key="runtime-key-skipped",
        binding_generation=3,
        lease_generation=2,
    )

    assert (await _post(app, conflict)).status_code == 409
    assert (await _post(app, same_generation)).status_code == 409
    assert (await _post(app, skipped_generation)).status_code == 409
    assert controller.start_calls == 1


async def test_expired_request_has_pre_start_receipt_and_no_external_boundary(tmp_path) -> None:
    registry = _registry(tmp_path)
    controller = _Controller()
    app = create_runtime_broker_app(
        RuntimeBrokerService(registry, controller),
        auth_token=_TOKEN,
    )
    requested = datetime.now(UTC) - timedelta(seconds=20)
    request = _request(
        registry,
        requested_at=requested,
        deadline_at=requested + timedelta(seconds=5),
    )
    response = await _post(app, request)

    assert response.status_code == 422
    assert response.json()["state"] == "rejectedPreStart"
    assert response.json()["disposition"] == "definitelyNotRequested"
    assert response.json()["externalStartBoundaryCrossed"] == "no"
    assert controller.start_calls == 0


async def test_dispatching_crash_reconciles_without_second_start(tmp_path) -> None:
    registry = _registry(tmp_path)
    request = _request(registry)
    row, replay = registry.reserve(request)
    assert replay is False
    registry.transition(
        request,
        expected_states=frozenset({"reserved"}),
        state="dispatching",
        disposition="definitelyStartRequested",
        boundary_crossed="unknown",
        service_state="stopped",
        reason_code="testCrashAfterDispatching",
    )
    controller = _Controller(state=ServiceState.STOPPED)
    service = RuntimeBrokerService(registry, controller)

    result = await service.start(request)
    replay_result = await service.start(request)

    assert row["state"] == "reserved"
    assert result.state == replay_result.state == "outcomeUnknown"
    assert replay_result.idempotent_replay is True
    assert result.external_start_boundary_crossed == "unknown"
    assert controller.start_calls == 0
    next_request = _request(
        registry,
        attempt="runtime-attempt-2",
        key="runtime-key-2",
        lease_generation=2,
    )
    with pytest.raises(BrokerConflict, match="prior runtime start outcome"):
        await service.start(next_request)


async def test_registry_restart_recovers_running_service_and_preserves_receipt(tmp_path) -> None:
    registry = _registry(tmp_path)
    request = _request(registry)
    first_controller = _Controller()
    first = await RuntimeBrokerService(registry, first_controller).start(request)
    reopened = RuntimeBrokerRegistry(
        registry.database_path,
        registry.profile,
        test_host_identity="fixture-host",
    )
    second_controller = _Controller(state=ServiceState.RUNNING)
    second = await RuntimeBrokerService(reopened, second_controller).start(request)

    assert first.receipt_id == second.receipt_id
    assert second.idempotent_replay is True
    assert first_controller.start_calls == 1
    assert second_controller.start_calls == 0


async def test_missing_registry_is_unavailable_and_never_calls_controller(tmp_path) -> None:
    registry = _registry(tmp_path)
    request = _request(registry)
    registry.database_path.unlink()
    controller = _Controller()

    with pytest.raises(BrokerUnavailable):
        await RuntimeBrokerService(registry, controller).start(request)
    assert controller.start_calls == 0


async def test_profile_and_registry_identity_mismatch_fail_closed(tmp_path) -> None:
    registry = _registry(tmp_path)
    controller = _Controller()
    service = RuntimeBrokerService(registry, controller)
    request = _request(registry)

    with pytest.raises(BrokerConflict):
        await service.start(replace(request, broker_registry_id="broker-registry-wrong"))
    with pytest.raises(BrokerConflict):
        await service.start(replace(request, service_profile_sha256="f" * 64))
    assert controller.start_calls == 0


async def test_registry_and_public_receipts_do_not_persist_bearer_or_internal_target(
    tmp_path,
) -> None:
    registry = _registry(tmp_path)
    app = create_runtime_broker_app(
        RuntimeBrokerService(registry, _Controller()),
        auth_token=_TOKEN,
    )
    response = await _post(app, _request(registry))
    assert response.status_code == 200

    persisted = b"".join(
        path.read_bytes() for path in registry.database_path.parent.iterdir() if path.is_file()
    )
    assert _TOKEN.encode() not in persisted
    assert registry.profile.target_service_name.encode() not in persisted
    assert registry.profile.target_service_name not in response.text
    assert "authorizationSHA256" not in response.text
