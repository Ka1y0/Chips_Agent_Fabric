from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

import jsonschema
import pytest

from project_supervisor.adapters import MockAdapter, WorkerRequest
from project_supervisor.domain import EvidenceConfidence, TelemetryValue
from project_supervisor.protocols.capabilities import CapabilityGrant, GrantState
from project_supervisor.protocols.identity import NodePublicIdentity
from project_supervisor.protocols.transport import TransportHealth, TransportStatus
from project_supervisor.protocols.worker import (
    GenericWorkerEndpoint,
    WorkerAvailability,
    WorkerContract,
    WorkerControl,
    WorkerHealth,
    WorkerModel,
    WorkerPermissions,
    WorkerTelemetry,
)

SCHEMAS = Path(__file__).parents[1] / "schemas"


def validate_schema(name: str, value: object) -> None:
    schema = json.loads((SCHEMAS / name).read_text(encoding="utf-8"))
    validator = jsonschema.Draft202012Validator(schema, format_checker=jsonschema.FormatChecker())
    validator.validate(value)


def worker_contract() -> WorkerContract:
    return WorkerContract(
        worker_id="worker-portable",
        node_id="node-portable",
        provider="example-provider",
        implementation="example-adapter",
        models=(
            WorkerModel(
                "model-general",
                "General Model",
                context_limit_tokens=32_000,
                capabilities=frozenset({"review"}),
            ),
        ),
        default_model_id="model-general",
        capabilities=frozenset({"execute", "review"}),
        permissions=WorkerPermissions(
            allowed=frozenset({"inference.read"}),
            denied=frozenset({"production.write"}),
        ),
        cost_visibility="estimated",
    )


def test_worker_contract_is_provider_neutral_and_schema_valid() -> None:
    contract = worker_contract()
    value = contract.to_protocol()
    validate_schema("worker-contract-v1.schema.json", value)
    assert value["identity"]["provider"] == "example-provider"
    assert value["permissions"]["denied"] == ["production.write"]

    with pytest.raises(ValueError, match="advertised model"):
        WorkerContract(
            worker_id="worker",
            node_id="node",
            provider="provider",
            implementation="adapter",
            models=(),
            default_model_id="missing",
            capabilities=frozenset(),
            permissions=WorkerPermissions(frozenset()),
        )


async def test_generic_worker_endpoint_preserves_v0_execute_stream_and_cancel() -> None:
    endpoint = GenericWorkerEndpoint(
        MockAdapter(),
        worker_contract(),
        health_probe=lambda: WorkerHealth(
            WorkerAvailability.AVAILABLE,
            latency_ms=TelemetryValue(3, EvidenceConfidence.EXACT),
        ),
    )
    assert isinstance(endpoint, WorkerControl)
    assert (await endpoint.discover()).provider == "example-provider"
    assert (await endpoint.health()).availability is WorkerAvailability.AVAILABLE
    validate_schema("worker-health-v1.schema.json", (await endpoint.health()).to_protocol())
    validate_schema("worker-telemetry-v1.schema.json", (await endpoint.telemetry()).to_protocol())

    observed = [item async for item in endpoint.stream(WorkerRequest("run-v01", "read"))]
    assert [item.kind for item in observed[:-1]] == ["processStarted", "processExited"]
    assert observed[-1].final_text == "MOCK_OK"
    assert not await endpoint.cancel("not-active")

    telemetry = WorkerTelemetry(
        remaining_quota=TelemetryValue(0, EvidenceConfidence.PROVIDER_REPORTED),
        attributes={"rateLimited": False},
    )
    validate_schema("worker-telemetry-v1.schema.json", telemetry.to_protocol())
    assert telemetry.to_protocol()["remainingQuota"]["value"] == 0


def test_transport_semantics_are_vendor_neutral_and_fail_closed() -> None:
    status = TransportStatus(
        transport_id="private-overlay-1",
        provider="example-private-transport",
        authenticated=True,
        encrypted=True,
        peer_identity="node:peer-1",
        reachable=True,
        health=TransportHealth.HEALTHY,
        latency_ms=TelemetryValue(8, EvidenceConfidence.VERIFIED),
    )
    assert status.safe_for_fabric
    validate_schema("transport-status-v1.schema.json", status.to_protocol())

    # Reachability and authenticated peer identity are separate observations.
    # This state is diagnostic evidence, never a safe Fabric route.
    unauthenticated = TransportStatus(
        transport_id="reachable-but-unauthenticated",
        provider="example-private-transport",
        authenticated=False,
        encrypted=True,
        peer_identity=None,
        reachable=True,
        health=TransportHealth.DEGRADED,
    )
    assert not unauthenticated.safe_for_fabric
    validate_schema("transport-status-v1.schema.json", unauthenticated.to_protocol())

    with pytest.raises(ValueError, match="peer identity"):
        TransportStatus(
            transport_id="unsafe",
            provider="transport",
            authenticated=True,
            encrypted=True,
            peer_identity=None,
            reachable=True,
            health=TransportHealth.UNKNOWN,
        )


def test_public_node_identity_has_no_private_material_and_is_schema_valid() -> None:
    identity = NodePublicIdentity(
        node_id="node-stable",
        key_id="key-1",
        algorithm="ed25519",
        public_key_fingerprint="sha256:" + "a" * 64,
    )
    value = identity.to_protocol()
    validate_schema("node-public-identity-v1.schema.json", value)
    assert set(value).isdisjoint({"privateKey", "secret", "seed"})

    with pytest.raises(ValueError, match="fingerprint"):
        NodePublicIdentity("node", "key", "ed25519", "not-a-fingerprint")


def test_capability_grant_is_scoped_attributable_expiring_and_revocable() -> None:
    issued = datetime(2026, 8, 9, tzinfo=UTC)
    grant = CapabilityGrant(
        grant_id="grant-1",
        capability="service.start",
        subject="node:worker-1",
        requested_by="agent:bootstrap",
        task_id="task-enroll",
        issued_by="admin:operator",
        issued_at=issued,
        expires_at=issued + timedelta(minutes=5),
        constraints={"service": "chips-worker"},
    )
    validate_schema("capability-grant-v1.schema.json", grant.to_protocol())
    assert grant.authorizes(
        "service.start",
        subject="node:worker-1",
        task_id="task-enroll",
        now=issued + timedelta(minutes=1),
    )
    assert not grant.authorizes(
        "service.stop",
        subject="node:worker-1",
        task_id="task-enroll",
        now=issued + timedelta(minutes=1),
    )
    assert not grant.authorizes(
        "service.start",
        subject="node:worker-1",
        task_id="task-enroll",
        now=issued + timedelta(minutes=6),
    )

    with pytest.raises(ValueError, match="revoked grants"):
        CapabilityGrant(
            grant_id="grant-revoked",
            capability="service.stop",
            subject="node:worker-1",
            requested_by="agent:bootstrap",
            task_id="task-enroll",
            issued_by="admin:operator",
            issued_at=issued,
            expires_at=None,
            state=GrantState.REVOKED,
        )
