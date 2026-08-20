from __future__ import annotations

import json
from collections import deque
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
    LoopbackModelsHTTPProbe,
    NodeRuntimeAdapterDescriptor,
    NodeRuntimeRecoveryService,
    RecoveryAttemptState,
    RecoveryAuthorizationDecision,
    RecoveryOperation,
    RuntimeHealthObservation,
    RuntimeRecoveryPolicy,
    RuntimeRecoveryResult,
)
from project_supervisor.store import StateStore


def observation(*, ready: bool, models: tuple[str, ...] | None = None) -> RuntimeHealthObservation:
    return RuntimeHealthObservation(
        reachable=ready,
        runtime_ready=ready,
        models=models,
        observed_at=datetime(2026, 8, 9, tzinfo=UTC),
        source="fixture-side-effect-free-status",
    )


class Probe:
    side_effect_free = True

    def __init__(self, *values: RuntimeHealthObservation) -> None:
        self.values = deque(values)
        self.calls = 0

    def observe(self, _policy: RuntimeRecoveryPolicy) -> RuntimeHealthObservation:
        self.calls += 1
        return self.values.popleft()


class Authorizer:
    def __init__(self, allowed: bool) -> None:
        self.allowed = allowed
        self.contexts = []

    def authorize(self, context):
        self.contexts.append(context)
        if not self.allowed:
            return RecoveryAuthorizationDecision(False, None, "operator grant unavailable")
        return RecoveryAuthorizationDecision(True, "grant-ref-fixture", "verified test grant")


class Adapter:
    descriptor = NodeRuntimeAdapterDescriptor(
        adapter_id="fixture-node-runtime",
        node_id="node-gpu",
        protocol_version="1.0",
        max_operation_seconds=5,
        allowed_operations=frozenset({RecoveryOperation.START_RUNTIME}),
    )

    def __init__(self, *, accepted: bool = True) -> None:
        self.accepted = accepted
        self.requests = []

    def recover(self, request):
        self.requests.append(request)
        return RuntimeRecoveryResult(
            self.accepted,
            "operation-ref-fixture" if self.accepted else None,
            observed_fencing_generation=request.lease_generation if self.accepted else None,
        )


@pytest.fixture
def recovery_store(tmp_path: Path) -> tuple[StateStore, NodeRuntimeRecoveryService]:
    store = StateStore(tmp_path / "state.db")
    store.create_project(
        project_id="project-1", name="Recovery", root_path=str(tmp_path), goal="Recover safely"
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
    return store, NodeRuntimeRecoveryService(store)


def policy(*, enabled: bool = True, endpoint: str = "http://127.0.0.1:1234"):
    return RuntimeRecoveryPolicy(
        id="recovery-policy",
        node_id="node-gpu",
        runtime_id="local-inference-runtime",
        backend_endpoint=endpoint,
        expected_models=("local-model",),
        worker_ids=("local-worker",),
        enabled=enabled,
        max_attempts=1,
    )


def test_policy_rejects_non_loopback_or_embedded_credentials() -> None:
    with pytest.raises(ValueError, match="loopback-only"):
        policy(endpoint="http://192.0.2.10:1234")
    with pytest.raises(ValueError, match="loopback-only"):
        policy(endpoint="http://127.0.0.2:1234")
    with pytest.raises(ValueError):
        policy(endpoint="HTTP://localhost:1234")
    with pytest.raises(ValueError, match="loopback-only"):
        policy(endpoint="http://LOCALHOST:1234")
    with pytest.raises(ValueError, match="credentials"):
        policy(endpoint="http://identity@127.0.0.1:1234")
    with pytest.raises(ValueError, match="without an application path"):
        policy(endpoint="http://127.0.0.1:1234/proxy")
    with pytest.raises(ValueError, match="opaque grant"):
        RecoveryAuthorizationDecision(True, "unexpected raw value", "unsafe fixture")


@pytest.mark.parametrize("terminator", ("\n", "\r", "\u2028", "\u2029"))
def test_core_rejects_line_terminators_in_backend_endpoint(terminator: str) -> None:
    with pytest.raises(ValueError):
        policy(endpoint=f"http://127.0.0.1:1234{terminator}")


@pytest.mark.parametrize(
    "endpoint",
    (
        "http://127.0.0.1:1234",
        "http://localhost:1234",
        "http://[::1]:1234",
    ),
)
def test_exact_loopback_origins_round_trip_through_authoritative_schema(
    recovery_store: tuple[StateStore, NodeRuntimeRecoveryService], endpoint: str
) -> None:
    store, service = recovery_store
    service.upsert_policy(policy(endpoint=endpoint), configured_by="operator:test")
    adapter = Adapter()
    outcome = service.recover_if_needed(
        "recovery-policy",
        probe=Probe(
            observation(ready=False),
            observation(ready=True, models=("local-model",)),
        ),
        adapter=adapter,
        authorizer=Authorizer(True),
        requested_by="health-monitor",
        trigger_reason="schema round-trip",
    )
    assert outcome.state is RecoveryAttemptState.READY
    request = adapter.requests[0]

    status_payload = service.list_status(node_id="node-gpu")[0]
    status_schema = json.loads(
        (Path(__file__).parents[1] / "schemas/node-runtime-recovery-v1.schema.json").read_text()
    )
    jsonschema.Draft202012Validator(
        status_schema, format_checker=jsonschema.FormatChecker()
    ).validate(status_payload)
    assert status_payload["backendEndpoint"] == endpoint

    exchange = {
        "descriptor": adapter.descriptor.to_protocol(),
        "request": request.to_protocol(),
        "result": RuntimeRecoveryResult(
            True,
            "operation-schema-round-trip",
            observed_fencing_generation=request.lease_generation,
        ).to_protocol(),
    }
    adapter_schema = json.loads(
        (Path(__file__).parents[1] / "schemas/node-runtime-adapter-v1.schema.json").read_text()
    )
    jsonschema.Draft202012Validator(
        adapter_schema, format_checker=jsonschema.FormatChecker()
    ).validate(exchange)


@pytest.mark.parametrize(
    "endpoint",
    (
        "http://127.0.0.2:1234",
        "HTTP://localhost:1234",
        "http://LOCALHOST:1234",
        "http://127.0.0.1:1234\n",
        "http://127.0.0.1:1234\r",
        "http://127.0.0.1:1234\u2028",
        "http://127.0.0.1:1234\u2029",
    ),
)
def test_authoritative_schemas_reject_noncanonical_endpoint(
    endpoint: str,
) -> None:
    adapter_schema = json.loads(
        (Path(__file__).parents[1] / "schemas/node-runtime-adapter-v1.schema.json").read_text()
    )
    request_schema = adapter_schema["$defs"]["request"]
    request = {
        "recoveryID": "runtime-recovery-fixture",
        "policyID": "recovery-policy",
        "nodeID": "node-gpu",
        "runtimeID": "local-inference-runtime",
        "operation": "runtime.start",
        "backendEndpoint": endpoint,
        "authorizationRef": "grant-ref-fixture",
        "leaseOwnerID": "health-monitor",
        "leaseGeneration": 1,
        "idempotencyKey": "runtime-recovery-fixture",
        "requestedAt": "2026-08-10T00:00:00Z",
        "deadlineAt": "2026-08-10T00:00:05Z",
    }
    with pytest.raises(jsonschema.ValidationError):
        jsonschema.Draft202012Validator(
            request_schema, format_checker=jsonschema.FormatChecker()
        ).validate(request)

    recovery_schema = json.loads(
        (Path(__file__).parents[1] / "schemas/node-runtime-recovery-v1.schema.json").read_text()
    )
    recovery_endpoint_schema = recovery_schema["properties"]["backendEndpoint"]
    with pytest.raises(jsonschema.ValidationError):
        jsonschema.Draft202012Validator(recovery_endpoint_schema).validate(endpoint)


def test_healthy_probe_has_no_recovery_side_effect(
    recovery_store: tuple[StateStore, NodeRuntimeRecoveryService],
) -> None:
    _, service = recovery_store
    service.upsert_policy(policy(), configured_by="operator:test")
    probe = Probe(observation(ready=True, models=("local-model",)))
    adapter = Adapter()
    outcome = service.recover_if_needed(
        "recovery-policy",
        probe=probe,
        adapter=adapter,
        requested_by="health-monitor",
        trigger_reason="periodic health observation",
    )
    assert outcome.state is RecoveryAttemptState.OBSERVED_HEALTHY
    assert adapter.requests == []
    assert probe.calls == 1


def test_recovery_requires_explicit_authorization_and_fails_closed(
    recovery_store: tuple[StateStore, NodeRuntimeRecoveryService],
) -> None:
    store, service = recovery_store
    service.upsert_policy(policy(), configured_by="operator:test")
    adapter = Adapter()
    outcome = service.recover_if_needed(
        "recovery-policy",
        probe=Probe(observation(ready=False)),
        adapter=adapter,
        authorizer=Authorizer(False),
        requested_by="health-monitor",
        trigger_reason="runtime unavailable",
    )
    assert outcome.state is RecoveryAttemptState.PERMISSION_REQUIRED
    assert outcome.failure_code == "authorizationDenied"
    assert adapter.requests == []
    assert store.list_nodes()[0]["state"] == "degraded"
    assert store.list_workers()[0]["state"] == "offline"


def test_authorized_recovery_verifies_expected_model_before_ready(
    recovery_store: tuple[StateStore, NodeRuntimeRecoveryService],
) -> None:
    store, service = recovery_store
    service.upsert_policy(policy(), configured_by="operator:test")
    authorizer = Authorizer(True)
    adapter = Adapter()
    outcome = service.recover_if_needed(
        "recovery-policy",
        probe=Probe(
            observation(ready=False),
            observation(ready=True, models=("local-model", "other-model")),
        ),
        adapter=adapter,
        authorizer=authorizer,
        requested_by="health-monitor",
        trigger_reason="runtime unavailable",
    )
    assert outcome.state is RecoveryAttemptState.READY
    assert outcome.attempt_count == 1
    assert len(adapter.requests) == 1
    request = adapter.requests[0]
    assert request.operation is RecoveryOperation.START_RUNTIME
    assert request.backend_endpoint == "http://127.0.0.1:1234"
    assert not hasattr(request, "command")
    assert request.idempotency_key == request.recovery_id
    assert request.lease_generation == outcome.lease_generation
    assert request.deadline_at > request.requested_at
    assert authorizer.contexts[0].capability == "service.start"
    assert store.list_nodes()[0]["state"] == "online"
    assert store.list_workers()[0]["state"] == "idle"

    payload = service.list_status(node_id="node-gpu")[0]
    schema = json.loads(
        (Path(__file__).parents[1] / "schemas/node-runtime-recovery-v1.schema.json").read_text()
    )
    jsonschema.Draft202012Validator(schema, format_checker=jsonschema.FormatChecker()).validate(
        payload
    )
    assert payload["latestRecovery"]["state"] == "ready"
    event_kinds = {row["kind"] for row in store.list_events(after_sequence=0, limit=200)}
    assert {"runtimeRecoveryAuthorized", "runtimeRecoveryReady"} <= event_kinds


def test_wrong_model_stays_degraded_and_broad_adapter_is_rejected(
    recovery_store: tuple[StateStore, NodeRuntimeRecoveryService],
) -> None:
    store, service = recovery_store
    service.upsert_policy(policy(), configured_by="operator:test")
    outcome = service.recover_if_needed(
        "recovery-policy",
        probe=Probe(observation(ready=False), observation(ready=True, models=("wrong-model",))),
        adapter=Adapter(),
        authorizer=Authorizer(True),
        requested_by="health-monitor",
        trigger_reason="runtime unavailable",
    )
    assert outcome.state is RecoveryAttemptState.FAILED
    assert outcome.failure_code == "verificationFailed"
    assert store.list_nodes()[0]["state"] == "degraded"

    class BroadAdapter(Adapter):
        descriptor = NodeRuntimeAdapterDescriptor(
            adapter_id="unsafe-fixture-node-runtime",
            node_id="node-gpu",
            protocol_version="1.0",
            max_operation_seconds=5,
            allowed_operations=frozenset({RecoveryOperation.START_RUNTIME}),
            allows_arbitrary_commands=True,
        )

    with pytest.raises(ValueError, match="fenced, deadline-bounded"):
        service.recover_if_needed(
            "recovery-policy",
            probe=Probe(observation(ready=False)),
            adapter=BroadAdapter(),
            requested_by="health-monitor",
            trigger_reason="runtime unavailable",
        )


def test_adapter_error_is_sanitized_in_durable_evidence(
    recovery_store: tuple[StateStore, NodeRuntimeRecoveryService],
) -> None:
    store, service = recovery_store
    service.upsert_policy(policy(), configured_by="operator:test")
    sensitive_value = "do-not-persist-this"
    diagnostic = f"{'to' + 'ken'}={sensitive_value}"

    class FailingAdapter(Adapter):
        def recover(self, request):
            raise RuntimeError(diagnostic)

    outcome = service.recover_if_needed(
        "recovery-policy",
        probe=Probe(observation(ready=False)),
        adapter=FailingAdapter(),
        authorizer=Authorizer(True),
        requested_by="health-monitor",
        trigger_reason="runtime unavailable",
    )
    assert outcome.state is RecoveryAttemptState.FAILED
    assert sensitive_value not in (outcome.failure_detail or "")
    with store.connect() as connection:
        serialized = json.dumps(
            [
                dict(row)
                for row in connection.execute("SELECT * FROM node_runtime_recovery_attempts")
            ]
        )
    assert sensitive_value not in serialized
    assert "[REDACTED]" in serialized


def test_concrete_loopback_probe_uses_bounded_models_get_without_network() -> None:
    captured = []

    def transport(endpoint: str, timeout: float, limit: int) -> bytes:
        captured.append((endpoint, timeout, limit))
        return b'{"data":[{"id":"local-model"}]}'

    probe = LoopbackModelsHTTPProbe(transport=transport)
    result = probe.observe(policy())
    assert result.ready_for(policy())
    assert result.models == ("local-model",)
    assert captured == [("http://127.0.0.1:1234", 2.0, 1_048_576)]
