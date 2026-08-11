from __future__ import annotations

import http.client
import json
import re
import ssl
import uuid
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from enum import StrEnum
from typing import Any, Protocol
from urllib.parse import urlsplit

from .domain import EventSeverity
from .store import StateStore, compact_json, redact_sensitive, timestamp

_CAPABILITY = re.compile(r"^[a-z][a-z0-9]*(?:[._-][a-z0-9]+)+$")
_AUTHORIZATION_REF = re.compile(r"^(?:grant|approval|capability)-[A-Za-z0-9._:-]{1,140}$")
_OPERATION_REF = re.compile(r"^(?:operation|request|runtime)-[A-Za-z0-9._:-]{1,140}$")
_ATTRIBUTION = re.compile(r"^[A-Za-z][A-Za-z0-9._:@/-]{0,159}$")
_PROTOCOL_VERSION = re.compile(r"^[0-9]+(?:\.[0-9]+){1,2}$")
_CANONICAL_LOOPBACK_ENDPOINT = re.compile(r"^https?://(?:127\.0\.0\.1|localhost|\[::1\]):[0-9]+/?$")


class RecoveryOperation(StrEnum):
    START_RUNTIME = "runtime.start"


class RecoveryAttemptState(StrEnum):
    OBSERVED_HEALTHY = "observedHealthy"
    DEGRADED = "degraded"
    PERMISSION_REQUIRED = "permissionRequired"
    RECOVERING = "recovering"
    VERIFYING = "verifying"
    READY = "ready"
    FAILED = "failed"


@dataclass(frozen=True, slots=True)
class RuntimeRecoveryPolicy:
    id: str
    node_id: str
    runtime_id: str
    backend_endpoint: str
    required_capability: str = "service.start"
    expected_models: tuple[str, ...] = ()
    worker_ids: tuple[str, ...] = ()
    enabled: bool = False
    max_attempts: int = 1
    monitor_interval_seconds: float = 30.0
    failure_backoff_seconds: float = 60.0
    lease_ttl_seconds: float = 60.0

    def __post_init__(self) -> None:
        values = (self.id, self.node_id, self.runtime_id, self.required_capability)
        if not all(value.strip() for value in values):
            raise ValueError("recovery policy identifiers must not be empty")
        if not _CAPABILITY.fullmatch(self.required_capability):
            raise ValueError("required_capability must be a scoped machine-readable name")
        _validate_loopback_endpoint(self.backend_endpoint)
        if not 1 <= self.max_attempts <= 3:
            raise ValueError("max_attempts must be between one and three")
        if not 5 <= self.monitor_interval_seconds <= 3600:
            raise ValueError("monitor_interval_seconds must be between 5 and 3600")
        if not 5 <= self.failure_backoff_seconds <= 86400:
            raise ValueError("failure_backoff_seconds must be between 5 and 86400")
        if not 15 <= self.lease_ttl_seconds <= 300:
            raise ValueError("lease_ttl_seconds must be between 15 and 300")
        for name, items in (
            ("expected_models", self.expected_models),
            ("worker_ids", self.worker_ids),
        ):
            if any(not item.strip() for item in items) or len(set(items)) != len(items):
                raise ValueError(f"{name} must contain unique non-empty values")

    def to_protocol(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "nodeID": self.node_id,
            "runtimeID": self.runtime_id,
            "backendEndpoint": self.backend_endpoint,
            "requiredCapability": self.required_capability,
            "expectedModels": list(self.expected_models),
            "workerIDs": list(self.worker_ids),
            "enabled": self.enabled,
            "maxAttempts": self.max_attempts,
            "monitorIntervalSeconds": self.monitor_interval_seconds,
            "failureBackoffSeconds": self.failure_backoff_seconds,
            "leaseTTLSeconds": self.lease_ttl_seconds,
        }


@dataclass(frozen=True, slots=True)
class RuntimeHealthObservation:
    reachable: bool
    runtime_ready: bool
    models: tuple[str, ...] | None
    observed_at: datetime
    source: str
    detail: str | None = None

    def __post_init__(self) -> None:
        if self.observed_at.tzinfo is None:
            raise ValueError("health observation time must be timezone-aware")
        if not _ATTRIBUTION.fullmatch(self.source):
            raise ValueError("health observation source must be a bounded machine identifier")
        if self.models is not None and (
            any(not model.strip() for model in self.models)
            or len(set(self.models)) != len(self.models)
        ):
            raise ValueError("observed models must contain unique non-empty values")

    def ready_for(self, policy: RuntimeRecoveryPolicy) -> bool:
        if not (self.reachable and self.runtime_ready):
            return False
        if not policy.expected_models:
            return True
        if self.models is None:
            return False
        return set(policy.expected_models).issubset(self.models)

    def to_protocol(self) -> dict[str, Any]:
        return {
            "reachable": self.reachable,
            "runtimeReady": self.runtime_ready,
            "models": list(self.models) if self.models is not None else None,
            "observedAt": timestamp(self.observed_at),
            "source": self.source,
            "detail": redact_sensitive(self.detail),
        }


class SideEffectFreeRuntimeProbe(Protocol):
    side_effect_free: bool

    def observe(self, policy: RuntimeRecoveryPolicy) -> RuntimeHealthObservation: ...


class LoopbackModelsHTTPProbe:
    """Bounded node-local GET probe for an OpenAI-compatible ``/v1/models`` surface."""

    side_effect_free = True

    def __init__(
        self,
        *,
        timeout_seconds: float = 2.0,
        max_response_bytes: int = 1_048_576,
        transport: Callable[[str, float, int], bytes] | None = None,
    ) -> None:
        if not 0 < timeout_seconds <= 5:
            raise ValueError("probe timeout must be > 0 and <= 5 seconds")
        if not 1 <= max_response_bytes <= 1_048_576:
            raise ValueError("probe response limit must be between 1 byte and 1 MiB")
        self.timeout_seconds = timeout_seconds
        self.max_response_bytes = max_response_bytes
        self._transport = transport or _loopback_models_get

    def observe(self, policy: RuntimeRecoveryPolicy) -> RuntimeHealthObservation:
        _validate_loopback_endpoint(policy.backend_endpoint)
        raw = self._transport(
            policy.backend_endpoint, self.timeout_seconds, self.max_response_bytes
        )
        value = json.loads(raw.decode("utf-8"))
        data = value.get("data") if isinstance(value, dict) else None
        if not isinstance(data, list):
            raise ValueError("model status response must contain a data array")
        models: list[str] = []
        for item in data:
            identifier = item.get("id") if isinstance(item, dict) else None
            if not isinstance(identifier, str) or not identifier.strip():
                raise ValueError("every model status item requires a non-empty id")
            models.append(identifier)
        return RuntimeHealthObservation(
            reachable=True,
            runtime_ready=True,
            models=tuple(dict.fromkeys(models)),
            observed_at=datetime.now(UTC),
            source="loopback.modelsGET",
        )


@dataclass(frozen=True, slots=True)
class RecoveryAuthorizationContext:
    recovery_id: str
    node_id: str
    runtime_id: str
    capability: str
    operation: RecoveryOperation
    requested_by: str
    attempt: int


@dataclass(frozen=True, slots=True)
class RecoveryAuthorizationDecision:
    authorized: bool
    authorization_ref: str | None
    reason: str

    def __post_init__(self) -> None:
        if not self.reason.strip():
            raise ValueError("authorization decision requires a reason")
        if self.authorized != bool(self.authorization_ref and self.authorization_ref.strip()):
            raise ValueError("authorized decisions require a non-empty opaque authorization_ref")
        if self.authorization_ref is not None and not _AUTHORIZATION_REF.fullmatch(
            self.authorization_ref
        ):
            raise ValueError("authorization_ref must be an opaque grant/approval/capability ID")


class RecoveryCapabilityAuthorizer(Protocol):
    def authorize(self, context: RecoveryAuthorizationContext) -> RecoveryAuthorizationDecision: ...


@dataclass(frozen=True, slots=True)
class RuntimeRecoveryRequest:
    recovery_id: str
    policy_id: str
    node_id: str
    runtime_id: str
    operation: RecoveryOperation
    backend_endpoint: str
    authorization_ref: str
    lease_owner_id: str
    lease_generation: int
    idempotency_key: str
    requested_at: datetime
    deadline_at: datetime

    def __post_init__(self) -> None:
        identifiers = (
            self.recovery_id,
            self.policy_id,
            self.node_id,
            self.runtime_id,
            self.lease_owner_id,
        )
        if not all(identifier.strip() for identifier in identifiers):
            raise ValueError("runtime recovery request identifiers must not be empty")
        if not _ATTRIBUTION.fullmatch(self.lease_owner_id):
            raise ValueError("runtime recovery request lease owner is invalid")
        if not _AUTHORIZATION_REF.fullmatch(self.authorization_ref):
            raise ValueError("runtime recovery request requires an opaque authorization reference")
        if self.requested_at.tzinfo is None or self.deadline_at.tzinfo is None:
            raise ValueError("runtime recovery request times must be timezone-aware")
        if self.deadline_at <= self.requested_at:
            raise ValueError("runtime recovery deadline must follow requested_at")
        if self.lease_generation < 1:
            raise ValueError("runtime recovery fencing generation must be positive")
        if self.idempotency_key != self.recovery_id:
            raise ValueError("runtime recovery idempotency_key must equal the durable recovery ID")
        _validate_loopback_endpoint(self.backend_endpoint)

    def to_protocol(self) -> dict[str, Any]:
        return {
            "recoveryID": self.recovery_id,
            "policyID": self.policy_id,
            "nodeID": self.node_id,
            "runtimeID": self.runtime_id,
            "operation": self.operation.value,
            "backendEndpoint": self.backend_endpoint,
            "authorizationRef": self.authorization_ref,
            "leaseOwnerID": self.lease_owner_id,
            "leaseGeneration": self.lease_generation,
            "idempotencyKey": self.idempotency_key,
            "requestedAt": timestamp(self.requested_at),
            "deadlineAt": timestamp(self.deadline_at),
        }


@dataclass(frozen=True, slots=True)
class NodeRuntimeAdapterDescriptor:
    """Machine-readable trust boundary for a node-side runtime adapter.

    A production adapter must enforce fencing, deadline, and idempotency at the Node Runtime or
    Privilege Broker boundary. Merely carrying those fields in the Supervisor is not sufficient.
    """

    adapter_id: str
    node_id: str
    protocol_version: str
    max_operation_seconds: float
    allowed_operations: frozenset[RecoveryOperation]
    allows_arbitrary_commands: bool = False
    enforces_fencing: bool = True
    enforces_deadline: bool = True
    enforces_idempotency: bool = True

    def __post_init__(self) -> None:
        if not _ATTRIBUTION.fullmatch(self.adapter_id) or not _ATTRIBUTION.fullmatch(self.node_id):
            raise ValueError("adapter and node identifiers must be bounded machine identities")
        if not _PROTOCOL_VERSION.fullmatch(self.protocol_version):
            raise ValueError("adapter protocol_version must be numeric major.minor[.patch]")
        if not 0 < self.max_operation_seconds <= 120:
            raise ValueError("adapter max_operation_seconds must be > 0 and <= 120")

    def to_protocol(self) -> dict[str, Any]:
        return {
            "adapterID": self.adapter_id,
            "nodeID": self.node_id,
            "protocolVersion": self.protocol_version,
            "maxOperationSeconds": self.max_operation_seconds,
            "allowedOperations": sorted(operation.value for operation in self.allowed_operations),
            "allowsArbitraryCommands": self.allows_arbitrary_commands,
            "enforcesFencing": self.enforces_fencing,
            "enforcesDeadline": self.enforces_deadline,
            "enforcesIdempotency": self.enforces_idempotency,
        }


@dataclass(frozen=True, slots=True)
class RuntimeRecoveryResult:
    accepted: bool
    operation_reference: str | None = None
    detail: str | None = None
    observed_fencing_generation: int | None = None
    idempotent_replay: bool = False

    def __post_init__(self) -> None:
        if self.accepted and (
            self.operation_reference is None
            or not _OPERATION_REF.fullmatch(self.operation_reference)
            or self.observed_fencing_generation is None
            or self.observed_fencing_generation < 1
        ):
            raise ValueError(
                "accepted recovery results require an opaque operation reference "
                "and fencing generation"
            )

    def to_protocol(self) -> dict[str, Any]:
        return {
            "accepted": self.accepted,
            "operationReference": self.operation_reference,
            "detail": redact_sensitive(self.detail),
            "observedFencingGeneration": self.observed_fencing_generation,
            "idempotentReplay": self.idempotent_replay,
        }


class BoundedRuntimeRecoveryAdapter(Protocol):
    descriptor: NodeRuntimeAdapterDescriptor

    def recover(self, request: RuntimeRecoveryRequest) -> RuntimeRecoveryResult: ...


class DenyAllRecoveryAuthorizer:
    """Production-safe default: policy presence is not authorization."""

    def authorize(self, context: RecoveryAuthorizationContext) -> RecoveryAuthorizationDecision:
        return RecoveryAuthorizationDecision(False, None, "no capability authorizer configured")


@dataclass(frozen=True, slots=True)
class RecoveryOutcome:
    id: str
    policy_id: str
    state: RecoveryAttemptState
    attempt_count: int
    authorization_ref: str | None
    before: RuntimeHealthObservation
    after: RuntimeHealthObservation | None
    failure_code: str | None
    failure_detail: str | None
    lease_generation: int | None = None
    stale_owner_recovered: bool = False

    def to_protocol(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "policyID": self.policy_id,
            "state": self.state.value,
            "attemptCount": self.attempt_count,
            "authorizationRef": self.authorization_ref,
            "beforeHealth": self.before.to_protocol(),
            "afterHealth": self.after.to_protocol() if self.after else None,
            "failureCode": self.failure_code,
            "failureDetail": self.failure_detail,
            "leaseGeneration": self.lease_generation,
            "staleOwnerRecovered": self.stale_owner_recovered,
        }


class RecoveryLeaseUnavailable(RuntimeError):
    """A live owner already holds the per-policy recovery lease."""


class RecoveryLeaseLost(RuntimeError):
    """The caller can no longer prove ownership of its fenced recovery generation."""


@dataclass(frozen=True, slots=True)
class RecoveryLeaseClaim:
    policy_id: str
    owner_id: str
    generation: int
    recovery_id: str
    expires_at: datetime
    stale_owner_recovered: bool


def _lease_time(value: datetime | None = None) -> str:
    return (value or datetime.now(UTC)).isoformat(timespec="microseconds").replace("+00:00", "Z")


class RuntimeRecoveryLeaseRepository:
    """Transactional, generation-fenced ownership for one recovery policy at a time."""

    def __init__(self, store: StateStore) -> None:
        self.store = store

    def try_acquire(
        self,
        *,
        policy_id: str,
        owner_id: str,
        recovery_id: str,
        lease_ttl_seconds: float,
    ) -> RecoveryLeaseClaim | None:
        if not _ATTRIBUTION.fullmatch(owner_id):
            raise ValueError("recovery lease owner must be a bounded machine identity")
        if not 15 <= lease_ttl_seconds <= 300:
            raise ValueError("recovery lease TTL must be between 15 and 300 seconds")
        now_value = datetime.now(UTC)
        now = _lease_time(now_value)
        expires_value = now_value + timedelta(seconds=lease_ttl_seconds)
        expires_at = _lease_time(expires_value)
        with self.store.transaction() as connection:
            policy = connection.execute(
                "SELECT node_id,runtime_id FROM node_runtime_recovery_policies WHERE id=?",
                (policy_id,),
            ).fetchone()
            if policy is None:
                raise KeyError(policy_id)
            lease = connection.execute(
                "SELECT * FROM node_runtime_recovery_leases WHERE policy_id=?", (policy_id,)
            ).fetchone()
            stale = bool(
                lease is not None and lease["state"] == "owned" and lease["expires_at"] <= now
            )
            if lease is not None and lease["state"] == "owned" and lease["expires_at"] > now:
                return None
            unfinished = connection.execute(
                "SELECT id FROM node_runtime_recovery_attempts WHERE policy_id=? "
                "AND finished_at IS NULL ORDER BY started_at DESC,id DESC LIMIT 1",
                (policy_id,),
            ).fetchone()
            generation = int(lease["generation"]) + 1 if lease is not None else 1
            previous_owner_id = lease["owner_id"] if lease is not None else None
            recovery_state = (
                "staleOwnerRecovered" if stale else "resuming" if unfinished else "fresh"
            )
            if lease is None:
                connection.execute(
                    "INSERT INTO node_runtime_recovery_leases("
                    "policy_id,owner_id,generation,state,acquired_at,heartbeat_at,expires_at,"
                    "active_recovery_id,recovery_state,previous_owner_id) "
                    "VALUES (?,?,?,'owned',?,?,?,?,?,?)",
                    (
                        policy_id,
                        owner_id,
                        generation,
                        now,
                        now,
                        expires_at,
                        recovery_id,
                        recovery_state,
                        previous_owner_id,
                    ),
                )
            else:
                connection.execute(
                    "UPDATE node_runtime_recovery_leases SET owner_id=?,generation=?,state='owned',"
                    "acquired_at=?,heartbeat_at=?,expires_at=?,released_at=NULL,active_recovery_id=?,"
                    "recovery_state=?,previous_owner_id=?,last_error=NULL WHERE policy_id=?",
                    (
                        owner_id,
                        generation,
                        now,
                        now,
                        expires_at,
                        recovery_id,
                        recovery_state,
                        previous_owner_id,
                        policy_id,
                    ),
                )
            if stale or unfinished is not None:
                detail = (
                    "prior recovery owner lease expired; transient attempt fenced and reconciled"
                    if stale
                    else "orphaned transient recovery attempt reconciled before restart"
                )
                connection.execute(
                    "UPDATE node_runtime_recovery_attempts SET state='failed',"
                    "failure_code=?,failure_detail=?,updated_at=?,"
                    "finished_at=? "
                    "WHERE policy_id=? AND finished_at IS NULL",
                    (
                        "staleLeaseRecovered" if stale else "orphanedAttemptRecovered",
                        detail,
                        now,
                        now,
                        policy_id,
                    ),
                )
            self.store._append_event(
                connection,
                kind=(
                    "runtimeRecoveryLeaseRecovered"
                    if stale
                    else "runtimeRecoveryLeaseResumed"
                    if unfinished is not None
                    else "runtimeRecoveryLeaseAcquired"
                ),
                severity=(
                    EventSeverity.WARNING if stale or unfinished is not None else EventSeverity.INFO
                ),
                entity_type="nodeRuntimeRecoveryLease",
                entity_id=policy_id,
                summary=(
                    "Expired runtime recovery lease recovered"
                    if stale
                    else "Orphaned runtime recovery attempt resumed"
                    if unfinished is not None
                    else "Runtime recovery lease acquired"
                ),
                payload={
                    "nodeID": policy["node_id"],
                    "runtimeID": policy["runtime_id"],
                    "ownerID": owner_id,
                    "generation": generation,
                    "previousOwnerID": previous_owner_id,
                    "recoveryID": recovery_id,
                },
                actor=f"node-recovery:{owner_id}",
            )
        return RecoveryLeaseClaim(
            policy_id,
            owner_id,
            generation,
            recovery_id,
            expires_value,
            stale,
        )

    def heartbeat(self, claim: RecoveryLeaseClaim, *, lease_ttl_seconds: float) -> bool:
        now_value = datetime.now(UTC)
        now = _lease_time(now_value)
        with self.store.transaction() as connection:
            cursor = connection.execute(
                "UPDATE node_runtime_recovery_leases SET heartbeat_at=?,expires_at=? "
                "WHERE policy_id=? AND owner_id=? AND generation=? AND state='owned' "
                "AND expires_at>?",
                (
                    now,
                    _lease_time(now_value + timedelta(seconds=lease_ttl_seconds)),
                    claim.policy_id,
                    claim.owner_id,
                    claim.generation,
                    now,
                ),
            )
        return cursor.rowcount == 1

    def assert_owned(self, claim: RecoveryLeaseClaim) -> None:
        with self.store.connect() as connection:
            self.assert_owned_in(connection, claim)

    @staticmethod
    def assert_owned_in(connection: Any, claim: RecoveryLeaseClaim) -> None:
        row = connection.execute(
            "SELECT 1 FROM node_runtime_recovery_leases WHERE policy_id=? AND owner_id=? "
            "AND generation=? AND state='owned' AND expires_at>?",
            (claim.policy_id, claim.owner_id, claim.generation, _lease_time()),
        ).fetchone()
        if row is None:
            raise RecoveryLeaseLost(
                f"owner {claim.owner_id} no longer holds generation {claim.generation} "
                f"for recovery policy {claim.policy_id}"
            )

    def release(
        self,
        claim: RecoveryLeaseClaim,
        *,
        lost: bool = False,
        error: str | None = None,
    ) -> bool:
        now = _lease_time()
        state = "lost" if lost else "released"
        recovery_state = "lost" if lost else "complete"
        with self.store.transaction() as connection:
            cursor = connection.execute(
                "UPDATE node_runtime_recovery_leases SET state=?,heartbeat_at=?,expires_at=?,"
                "released_at=?,recovery_state=?,last_error=? WHERE policy_id=? AND owner_id=? "
                "AND generation=? AND state='owned'",
                (
                    state,
                    now,
                    now,
                    now,
                    recovery_state,
                    str(redact_sensitive(error))[:240] if error else None,
                    claim.policy_id,
                    claim.owner_id,
                    claim.generation,
                ),
            )
            if cursor.rowcount:
                self.store._append_event(
                    connection,
                    kind="runtimeRecoveryLeaseLost" if lost else "runtimeRecoveryLeaseReleased",
                    severity=EventSeverity.ERROR if lost else EventSeverity.INFO,
                    entity_type="nodeRuntimeRecoveryLease",
                    entity_id=claim.policy_id,
                    summary=(
                        "Runtime recovery lease ownership lost"
                        if lost
                        else "Runtime recovery lease released"
                    ),
                    payload={
                        "ownerID": claim.owner_id,
                        "generation": claim.generation,
                        "recoveryID": claim.recovery_id,
                        "error": redact_sensitive(error),
                    },
                    actor=f"node-recovery:{claim.owner_id}",
                )
        return cursor.rowcount == 1

    def get(self, policy_id: str) -> dict[str, Any] | None:
        with self.store.connect() as connection:
            row = connection.execute(
                "SELECT * FROM node_runtime_recovery_leases WHERE policy_id=?", (policy_id,)
            ).fetchone()
        return dict(row) if row else None


class NodeRuntimeRecoveryService:
    """Fail-closed orchestration around a node-local, narrowly bounded recovery adapter.

    This service never accepts a command, executable, argument vector, credential, or remote shell.
    The actual node-side adapter must be separately trusted and authorized for ``runtime.start``.
    """

    def __init__(self, store: StateStore) -> None:
        self.store = store
        self.leases = RuntimeRecoveryLeaseRepository(store)

    def upsert_policy(self, policy: RuntimeRecoveryPolicy, *, configured_by: str) -> None:
        if not _ATTRIBUTION.fullmatch(configured_by):
            raise ValueError("configured_by must be a bounded operator identity")
        now = timestamp()
        with self.store.transaction() as connection:
            if (
                connection.execute("SELECT 1 FROM nodes WHERE id=?", (policy.node_id,)).fetchone()
                is None
            ):
                raise KeyError(policy.node_id)
            live_lease = connection.execute(
                "SELECT 1 FROM node_runtime_recovery_leases WHERE policy_id=? "
                "AND state='owned' AND expires_at>?",
                (policy.id, _lease_time()),
            ).fetchone()
            if live_lease is not None:
                raise RecoveryLeaseUnavailable(
                    f"cannot reconfigure recovery policy {policy.id} while its lease is live"
                )
            if policy.worker_ids:
                placeholders = ",".join("?" for _ in policy.worker_ids)
                rows = connection.execute(
                    f"SELECT id,node_id FROM workers WHERE id IN ({placeholders})",
                    policy.worker_ids,
                ).fetchall()
                if len(rows) != len(policy.worker_ids) or any(
                    row["node_id"] != policy.node_id for row in rows
                ):
                    raise ValueError("every recovery Worker must exist on the policy node")
            connection.execute(
                "INSERT INTO node_runtime_recovery_policies("
                "id,node_id,runtime_id,backend_endpoint,required_capability,expected_models_json,"
                "worker_ids_json,enabled,max_attempts,monitor_interval_seconds,"
                "failure_backoff_seconds,lease_ttl_seconds,created_at,updated_at) "
                "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?) "
                "ON CONFLICT(id) DO UPDATE SET node_id=excluded.node_id,"
                "runtime_id=excluded.runtime_id,"
                "backend_endpoint=excluded.backend_endpoint,required_capability=excluded.required_capability,"
                "expected_models_json=excluded.expected_models_json,worker_ids_json=excluded.worker_ids_json,"
                "enabled=excluded.enabled,max_attempts=excluded.max_attempts,"
                "monitor_interval_seconds=excluded.monitor_interval_seconds,"
                "failure_backoff_seconds=excluded.failure_backoff_seconds,"
                "lease_ttl_seconds=excluded.lease_ttl_seconds,updated_at=excluded.updated_at",
                (
                    policy.id,
                    policy.node_id,
                    policy.runtime_id,
                    policy.backend_endpoint,
                    policy.required_capability,
                    compact_json(policy.expected_models),
                    compact_json(policy.worker_ids),
                    int(policy.enabled),
                    policy.max_attempts,
                    policy.monitor_interval_seconds,
                    policy.failure_backoff_seconds,
                    policy.lease_ttl_seconds,
                    now,
                    now,
                ),
            )
            self.store._append_event(
                connection,
                kind="runtimeRecoveryPolicyConfigured",
                severity=EventSeverity.NOTICE,
                entity_type="nodeRuntimeRecoveryPolicy",
                entity_id=policy.id,
                summary=f"Runtime recovery policy {policy.id} configured",
                payload={
                    "nodeID": policy.node_id,
                    "runtimeID": policy.runtime_id,
                    "enabled": policy.enabled,
                    "requiredCapability": policy.required_capability,
                },
                actor=configured_by,
            )

    def get_policy(self, policy_id: str) -> RuntimeRecoveryPolicy:
        with self.store.connect() as connection:
            row = connection.execute(
                "SELECT * FROM node_runtime_recovery_policies WHERE id=?", (policy_id,)
            ).fetchone()
        if row is None:
            raise KeyError(policy_id)
        return _policy_from_row(dict(row))

    def list_status(self, *, node_id: str | None = None) -> list[dict[str, Any]]:
        clauses = " WHERE p.node_id=?" if node_id else ""
        parameters = (node_id,) if node_id else ()
        with self.store.connect() as connection:
            rows = connection.execute(
                "SELECT p.*,a.id AS attempt_id,a.state AS attempt_state,a.attempt_count,"
                "a.failure_code,a.failure_detail,a.updated_at AS attempt_updated_at,"
                "l.owner_id AS lease_owner_id,l.generation AS lease_generation,"
                "l.state AS lease_state,l.heartbeat_at AS lease_heartbeat_at,"
                "l.expires_at AS lease_expires_at,l.recovery_state AS lease_recovery_state,"
                "c.monitor_id AS checkpoint_monitor_id,c.last_state AS checkpoint_last_state,"
                "c.consecutive_failures,c.last_observed_at,c.next_observation_at,"
                "c.last_error AS checkpoint_last_error,m.state AS monitor_state,"
                "m.started_at AS monitor_started_at,m.heartbeat_at AS monitor_heartbeat_at,"
                "m.active_policy_count AS monitor_active_policy_count,"
                "m.last_error AS monitor_last_error "
                "FROM node_runtime_recovery_policies p LEFT JOIN node_runtime_recovery_attempts a "
                "ON a.id=(SELECT a2.id FROM node_runtime_recovery_attempts a2 "
                "WHERE a2.policy_id=p.id ORDER BY a2.started_at DESC,a2.id DESC LIMIT 1)"
                " LEFT JOIN node_runtime_recovery_leases l ON l.policy_id=p.id"
                " LEFT JOIN node_runtime_recovery_checkpoints c ON c.policy_id=p.id"
                " LEFT JOIN node_runtime_recovery_monitors m ON m.monitor_id=c.monitor_id"
                f"{clauses} ORDER BY p.node_id,p.runtime_id",
                parameters,
            ).fetchall()
        result: list[dict[str, Any]] = []
        for row in rows:
            value = _policy_from_row(dict(row)).to_protocol()
            value["latestRecovery"] = (
                {
                    "id": row["attempt_id"],
                    "state": row["attempt_state"],
                    "attemptCount": row["attempt_count"],
                    "failureCode": row["failure_code"],
                    "failureDetail": row["failure_detail"],
                    "updatedAt": row["attempt_updated_at"],
                }
                if row["attempt_id"] is not None
                else None
            )
            value["lease"] = (
                {
                    "ownerID": row["lease_owner_id"],
                    "generation": row["lease_generation"],
                    "state": row["lease_state"],
                    "heartbeatAt": row["lease_heartbeat_at"],
                    "expiresAt": row["lease_expires_at"],
                    "recoveryState": row["lease_recovery_state"],
                }
                if row["lease_owner_id"] is not None
                else None
            )
            value["monitoring"] = (
                {
                    "monitorID": row["checkpoint_monitor_id"],
                    "lastState": row["checkpoint_last_state"],
                    "consecutiveFailures": row["consecutive_failures"],
                    "lastObservedAt": row["last_observed_at"],
                    "nextObservationAt": row["next_observation_at"],
                    "lastError": row["checkpoint_last_error"],
                    "host": (
                        {
                            "id": row["checkpoint_monitor_id"],
                            "state": row["monitor_state"],
                            "startedAt": row["monitor_started_at"],
                            "heartbeatAt": row["monitor_heartbeat_at"],
                            "activePolicyCount": row["monitor_active_policy_count"],
                            "lastError": row["monitor_last_error"],
                        }
                        if row["monitor_state"] is not None
                        else None
                    ),
                }
                if row["next_observation_at"] is not None
                else None
            )
            result.append(value)
        return result

    def recover_if_needed(
        self,
        policy_id: str,
        *,
        probe: SideEffectFreeRuntimeProbe,
        adapter: BoundedRuntimeRecoveryAdapter,
        authorizer: RecoveryCapabilityAuthorizer | None = None,
        requested_by: str,
        trigger_reason: str,
        lease_owner_id: str | None = None,
    ) -> RecoveryOutcome:
        policy = self.get_policy(policy_id)
        if getattr(probe, "side_effect_free", False) is not True:
            raise ValueError("runtime probe must explicitly declare side_effect_free=True")
        descriptor = getattr(adapter, "descriptor", None)
        if not isinstance(descriptor, NodeRuntimeAdapterDescriptor):
            raise ValueError("recovery adapter requires a typed NodeRuntimeAdapterDescriptor")
        if (
            descriptor.allows_arbitrary_commands
            or descriptor.allowed_operations != frozenset({RecoveryOperation.START_RUNTIME})
            or not descriptor.enforces_fencing
            or not descriptor.enforces_deadline
            or not descriptor.enforces_idempotency
        ):
            raise ValueError(
                "recovery adapter must enforce a fenced, deadline-bounded, idempotent runtime.start"
            )
        if descriptor.node_id != policy.node_id:
            raise ValueError("recovery adapter node identity does not match the policy node")
        if descriptor.max_operation_seconds > policy.lease_ttl_seconds - 10:
            raise ValueError(
                "adapter operation bound must leave at least ten seconds of lease margin"
            )
        if not _ATTRIBUTION.fullmatch(requested_by) or not trigger_reason.strip():
            raise ValueError("bounded recovery attribution and trigger reason are required")

        recovery_id = f"runtime-recovery-{uuid.uuid4()}"
        owner_id = lease_owner_id or f"recovery-owner-{uuid.uuid4()}"
        claim = self.leases.try_acquire(
            policy_id=policy.id,
            owner_id=owner_id,
            recovery_id=recovery_id,
            lease_ttl_seconds=policy.lease_ttl_seconds,
        )
        if claim is None:
            raise RecoveryLeaseUnavailable(f"runtime recovery policy {policy.id} has a live owner")
        caught: BaseException | None = None
        lost = False
        try:
            return self._recover_claimed(
                policy,
                claim,
                probe=probe,
                adapter=adapter,
                authorizer=authorizer,
                requested_by=requested_by,
                trigger_reason=trigger_reason,
            )
        except BaseException as error:
            caught = error
            lost = isinstance(error, RecoveryLeaseLost)
            raise
        finally:
            self.leases.release(
                claim,
                lost=lost,
                error=str(redact_sensitive(str(caught))) if caught else None,
            )

    def _recover_claimed(
        self,
        policy: RuntimeRecoveryPolicy,
        claim: RecoveryLeaseClaim,
        *,
        probe: SideEffectFreeRuntimeProbe,
        adapter: BoundedRuntimeRecoveryAdapter,
        authorizer: RecoveryCapabilityAuthorizer | None,
        requested_by: str,
        trigger_reason: str,
    ) -> RecoveryOutcome:
        recovery_id = claim.recovery_id
        before = self._observe(probe, policy)
        self.leases.assert_owned(claim)
        if before.ready_for(policy):
            self._create_attempt(
                recovery_id,
                policy,
                claim,
                RecoveryAttemptState.OBSERVED_HEALTHY,
                trigger_reason,
                requested_by,
                before,
                finished=True,
            )
            self._event(
                policy,
                claim,
                recovery_id,
                "runtimeHealthObserved",
                EventSeverity.INFO,
                "Runtime is healthy; recovery was not executed",
                {"state": "ready"},
            )
            return RecoveryOutcome(
                recovery_id,
                policy.id,
                RecoveryAttemptState.OBSERVED_HEALTHY,
                0,
                None,
                before,
                None,
                None,
                None,
                claim.generation,
                claim.stale_owner_recovered,
            )

        self._create_attempt(
            recovery_id,
            policy,
            claim,
            RecoveryAttemptState.DEGRADED,
            trigger_reason,
            requested_by,
            before,
            finished=False,
        )
        self._mark_degraded(policy, claim, recovery_id)
        if not policy.enabled:
            return self._fail(
                recovery_id,
                policy,
                claim,
                before,
                RecoveryAttemptState.PERMISSION_REQUIRED,
                "policyDisabled",
                "runtime recovery policy is disabled",
                "runtimeRecoveryDenied",
            )

        capability_authorizer = authorizer or DenyAllRecoveryAuthorizer()
        last_after: RuntimeHealthObservation | None = None
        last_authorization: str | None = None
        for attempt in range(1, policy.max_attempts + 1):
            decision = capability_authorizer.authorize(
                RecoveryAuthorizationContext(
                    recovery_id=recovery_id,
                    node_id=policy.node_id,
                    runtime_id=policy.runtime_id,
                    capability=policy.required_capability,
                    operation=RecoveryOperation.START_RUNTIME,
                    requested_by=requested_by,
                    attempt=attempt,
                )
            )
            if not decision.authorized:
                return self._fail(
                    recovery_id,
                    policy,
                    claim,
                    before,
                    RecoveryAttemptState.PERMISSION_REQUIRED,
                    "authorizationDenied",
                    decision.reason,
                    "runtimeRecoveryDenied",
                    attempt_count=attempt - 1,
                )
            last_authorization = decision.authorization_ref
            self._update_attempt(
                recovery_id,
                claim,
                state=RecoveryAttemptState.RECOVERING,
                attempt_count=attempt,
                authorization_ref=last_authorization,
            )
            self._event(
                policy,
                claim,
                recovery_id,
                "runtimeRecoveryAuthorized",
                EventSeverity.NOTICE,
                "Runtime recovery capability authorized",
                {
                    "capability": policy.required_capability,
                    "attempt": attempt,
                    "authorizationRef": last_authorization,
                },
            )
            self._event(
                policy,
                claim,
                recovery_id,
                "runtimeRecoveryStarted",
                EventSeverity.NOTICE,
                "Bounded runtime.start recovery operation requested",
                {"attempt": attempt},
            )
            if not self.leases.heartbeat(claim, lease_ttl_seconds=policy.lease_ttl_seconds):
                raise RecoveryLeaseLost(
                    f"runtime recovery lease generation {claim.generation} expired before dispatch"
                )
            requested_at = datetime.now(UTC)
            request = RuntimeRecoveryRequest(
                recovery_id=recovery_id,
                policy_id=policy.id,
                node_id=policy.node_id,
                runtime_id=policy.runtime_id,
                operation=RecoveryOperation.START_RUNTIME,
                backend_endpoint=policy.backend_endpoint,
                authorization_ref=last_authorization or "",
                lease_owner_id=claim.owner_id,
                lease_generation=claim.generation,
                idempotency_key=recovery_id,
                requested_at=requested_at,
                deadline_at=requested_at
                + timedelta(seconds=adapter.descriptor.max_operation_seconds),
            )
            try:
                execution = adapter.recover(request)
            except Exception as error:  # boundary: adapter failures are durable, sanitized evidence
                detail = str(redact_sensitive(str(error)))[:240]
                if attempt == policy.max_attempts:
                    return self._fail(
                        recovery_id,
                        policy,
                        claim,
                        before,
                        RecoveryAttemptState.FAILED,
                        "recoveryAdapterFailed",
                        detail,
                        "runtimeRecoveryFailed",
                        attempt_count=attempt,
                        authorization_ref=last_authorization,
                    )
                continue
            if not self.leases.heartbeat(claim, lease_ttl_seconds=policy.lease_ttl_seconds):
                raise RecoveryLeaseLost(
                    f"runtime recovery lease generation {claim.generation} expired during dispatch"
                )
            if execution.accepted and execution.observed_fencing_generation != claim.generation:
                return self._fail(
                    recovery_id,
                    policy,
                    claim,
                    before,
                    RecoveryAttemptState.FAILED,
                    "adapterFencingMismatch",
                    "node adapter did not acknowledge the current fencing generation",
                    "runtimeRecoveryFailed",
                    attempt_count=attempt,
                    authorization_ref=last_authorization,
                )
            if not execution.accepted:
                detail = str(
                    redact_sensitive(execution.detail or "runtime start was not accepted")
                )[:240]
                if attempt == policy.max_attempts:
                    return self._fail(
                        recovery_id,
                        policy,
                        claim,
                        before,
                        RecoveryAttemptState.FAILED,
                        "runtimeStartRejected",
                        detail,
                        "runtimeRecoveryFailed",
                        attempt_count=attempt,
                        authorization_ref=last_authorization,
                    )
                continue
            self._update_attempt(
                recovery_id,
                claim,
                state=RecoveryAttemptState.VERIFYING,
                attempt_count=attempt,
                authorization_ref=last_authorization,
            )
            last_after = self._observe(probe, policy)
            self.leases.assert_owned(claim)
            if last_after.ready_for(policy):
                self._update_attempt(
                    recovery_id,
                    claim,
                    state=RecoveryAttemptState.READY,
                    attempt_count=attempt,
                    authorization_ref=last_authorization,
                    after=last_after,
                    finished=True,
                )
                self._mark_ready(policy, claim)
                self._event(
                    policy,
                    claim,
                    recovery_id,
                    "runtimeRecoveryReady",
                    EventSeverity.NOTICE,
                    "Runtime recovery passed health and model verification",
                    {"attempt": attempt, "models": list(last_after.models or ())},
                )
                return RecoveryOutcome(
                    recovery_id,
                    policy.id,
                    RecoveryAttemptState.READY,
                    attempt,
                    last_authorization,
                    before,
                    last_after,
                    None,
                    None,
                    claim.generation,
                    claim.stale_owner_recovered,
                )

        return self._fail(
            recovery_id,
            policy,
            claim,
            before,
            RecoveryAttemptState.FAILED,
            "verificationFailed",
            "runtime or expected model did not become ready",
            "runtimeRecoveryFailed",
            attempt_count=policy.max_attempts,
            authorization_ref=last_authorization,
            after=last_after,
        )

    @staticmethod
    def _observe(
        probe: SideEffectFreeRuntimeProbe, policy: RuntimeRecoveryPolicy
    ) -> RuntimeHealthObservation:
        try:
            return probe.observe(policy)
        except Exception as error:
            return RuntimeHealthObservation(
                reachable=False,
                runtime_ready=False,
                models=None,
                observed_at=datetime.now(UTC),
                source="probeError",
                detail=str(redact_sensitive(str(error)))[:240],
            )

    def _create_attempt(
        self,
        recovery_id: str,
        policy: RuntimeRecoveryPolicy,
        claim: RecoveryLeaseClaim,
        state: RecoveryAttemptState,
        trigger_reason: str,
        requested_by: str,
        before: RuntimeHealthObservation,
        *,
        finished: bool,
    ) -> None:
        now = timestamp()
        with self.store.transaction() as connection:
            self.leases.assert_owned_in(connection, claim)
            connection.execute(
                "INSERT INTO node_runtime_recovery_attempts("
                "id,policy_id,node_id,runtime_id,state,trigger_reason,requested_by,attempt_count,"
                "before_health_json,started_at,updated_at,finished_at,lease_owner_id,"
                "lease_generation) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    recovery_id,
                    policy.id,
                    policy.node_id,
                    policy.runtime_id,
                    state.value,
                    str(redact_sensitive(trigger_reason))[:240],
                    requested_by,
                    0,
                    compact_json(before.to_protocol()),
                    now,
                    now,
                    now if finished else None,
                    claim.owner_id,
                    claim.generation,
                ),
            )

    def _update_attempt(
        self,
        recovery_id: str,
        claim: RecoveryLeaseClaim,
        *,
        state: RecoveryAttemptState,
        attempt_count: int,
        authorization_ref: str | None = None,
        after: RuntimeHealthObservation | None = None,
        failure_code: str | None = None,
        failure_detail: str | None = None,
        finished: bool = False,
    ) -> None:
        now = timestamp()
        with self.store.transaction() as connection:
            self.leases.assert_owned_in(connection, claim)
            connection.execute(
                "UPDATE node_runtime_recovery_attempts SET state=?,attempt_count=?,"
                "authorization_ref=COALESCE(?,authorization_ref),after_health_json=COALESCE(?,after_health_json),"
                "failure_code=?,failure_detail=?,updated_at=?,finished_at=? WHERE id=?",
                (
                    state.value,
                    attempt_count,
                    str(redact_sensitive(authorization_ref))[:160] if authorization_ref else None,
                    compact_json(after.to_protocol()) if after else None,
                    failure_code,
                    str(redact_sensitive(failure_detail))[:240] if failure_detail else None,
                    now,
                    now if finished else None,
                    recovery_id,
                ),
            )

    def _fail(
        self,
        recovery_id: str,
        policy: RuntimeRecoveryPolicy,
        claim: RecoveryLeaseClaim,
        before: RuntimeHealthObservation,
        state: RecoveryAttemptState,
        code: str,
        detail: str,
        event_kind: str,
        *,
        attempt_count: int = 0,
        authorization_ref: str | None = None,
        after: RuntimeHealthObservation | None = None,
    ) -> RecoveryOutcome:
        self._update_attempt(
            recovery_id,
            claim,
            state=state,
            attempt_count=attempt_count,
            authorization_ref=authorization_ref,
            after=after,
            failure_code=code,
            failure_detail=detail,
            finished=True,
        )
        self._event(
            policy,
            claim,
            recovery_id,
            event_kind,
            EventSeverity.WARNING,
            detail,
            {"failureCode": code, "attemptCount": attempt_count},
        )
        return RecoveryOutcome(
            recovery_id,
            policy.id,
            state,
            attempt_count,
            authorization_ref,
            before,
            after,
            code,
            str(redact_sensitive(detail))[:240],
            claim.generation,
            claim.stale_owner_recovered,
        )

    def _mark_degraded(
        self, policy: RuntimeRecoveryPolicy, claim: RecoveryLeaseClaim, recovery_id: str
    ) -> None:
        now = timestamp()
        with self.store.transaction() as connection:
            self.leases.assert_owned_in(connection, claim)
            connection.execute(
                "UPDATE nodes SET state='degraded',updated_at=? WHERE id=?", (now, policy.node_id)
            )
            if policy.worker_ids:
                placeholders = ",".join("?" for _ in policy.worker_ids)
                connection.execute(
                    f"UPDATE workers SET state='offline',resource_state='unknown',updated_at=? "
                    f"WHERE id IN ({placeholders}) AND state IN ('idle','offline','failed')",
                    (now, *policy.worker_ids),
                )
            self._append_recovery_event(
                connection,
                policy,
                recovery_id,
                "runtimeHealthObserved",
                EventSeverity.WARNING,
                "Runtime health is degraded",
                {"state": "degraded"},
            )

    def _mark_ready(self, policy: RuntimeRecoveryPolicy, claim: RecoveryLeaseClaim) -> None:
        now = timestamp()
        with self.store.transaction() as connection:
            self.leases.assert_owned_in(connection, claim)
            unhealthy_policy = connection.execute(
                "SELECT 1 FROM node_runtime_recovery_policies p WHERE p.node_id=? "
                "AND p.enabled=1 AND p.id<>? AND NOT EXISTS("
                "SELECT 1 FROM node_runtime_recovery_attempts a WHERE a.id=("
                "SELECT a2.id FROM node_runtime_recovery_attempts a2 WHERE a2.policy_id=p.id "
                "ORDER BY a2.started_at DESC,a2.id DESC LIMIT 1) "
                "AND a.state IN ('observedHealthy','ready')) LIMIT 1",
                (policy.node_id, policy.id),
            ).fetchone()
            if unhealthy_policy is None:
                connection.execute(
                    "UPDATE nodes SET state='online',last_heartbeat_at=?,updated_at=? WHERE id=?",
                    (now, now, policy.node_id),
                )
            if policy.worker_ids:
                placeholders = ",".join("?" for _ in policy.worker_ids)
                connection.execute(
                    f"UPDATE workers SET state='idle',resource_state='available',"
                    f"last_heartbeat_at=?,updated_at=? WHERE id IN ({placeholders}) "
                    "AND state IN ('offline','failed')",
                    (now, now, *policy.worker_ids),
                )

    def _event(
        self,
        policy: RuntimeRecoveryPolicy,
        claim: RecoveryLeaseClaim,
        entity_id: str,
        kind: str,
        severity: EventSeverity,
        summary: str,
        payload: dict[str, Any],
    ) -> None:
        with self.store.transaction() as connection:
            self.leases.assert_owned_in(connection, claim)
            self._append_recovery_event(
                connection, policy, entity_id, kind, severity, summary, payload
            )

    def _append_recovery_event(
        self,
        connection: Any,
        policy: RuntimeRecoveryPolicy,
        entity_id: str,
        kind: str,
        severity: EventSeverity,
        summary: str,
        payload: dict[str, Any],
    ) -> None:
        self.store._append_event(
            connection,
            kind=kind,
            severity=severity,
            entity_type="nodeRuntimeRecovery",
            entity_id=entity_id,
            summary=str(redact_sensitive(summary))[:240],
            payload=redact_sensitive(
                {
                    "policyID": policy.id,
                    "nodeID": policy.node_id,
                    "runtimeID": policy.runtime_id,
                    **payload,
                }
            ),
            actor="node-recovery",
        )


def _validate_loopback_endpoint(endpoint: str) -> None:
    parsed = urlsplit(endpoint)
    if (
        not endpoint.startswith(("http://", "https://"))
        or not parsed.hostname
        or parsed.port is None
    ):
        raise ValueError("backend_endpoint must be an HTTP(S) loopback URL with an explicit port")
    if parsed.username or parsed.password or parsed.query or parsed.fragment:
        raise ValueError("backend_endpoint cannot contain credentials, query, or fragment")
    if parsed.path not in {"", "/"}:
        raise ValueError("backend_endpoint must be a loopback origin without an application path")
    if not _CANONICAL_LOOPBACK_ENDPOINT.fullmatch(endpoint):
        raise ValueError("runtime recovery backend must remain loopback-only")


def _loopback_models_get(endpoint: str, timeout_seconds: float, max_bytes: int) -> bytes:
    parsed = urlsplit(endpoint)
    _validate_loopback_endpoint(endpoint)
    connection_type = (
        http.client.HTTPSConnection if parsed.scheme == "https" else http.client.HTTPConnection
    )
    kwargs: dict[str, Any] = {
        "host": parsed.hostname,
        "port": parsed.port,
        "timeout": timeout_seconds,
    }
    if connection_type is http.client.HTTPSConnection:
        kwargs["context"] = ssl.create_default_context()
    connection = connection_type(**kwargs)
    path = f"{parsed.path.rstrip('/')}/v1/models"
    try:
        connection.request("GET", path, headers={"Accept": "application/json"})
        response = connection.getresponse()
        if response.status != 200:
            raise RuntimeError(f"model status returned HTTP {response.status}")
        body = response.read(max_bytes + 1)
        if len(body) > max_bytes:
            raise RuntimeError("model status response exceeded configured limit")
        return body
    finally:
        connection.close()


def _policy_from_row(row: dict[str, Any]) -> RuntimeRecoveryPolicy:
    return RuntimeRecoveryPolicy(
        id=str(row["id"]),
        node_id=str(row["node_id"]),
        runtime_id=str(row["runtime_id"]),
        backend_endpoint=str(row["backend_endpoint"]),
        required_capability=str(row["required_capability"]),
        expected_models=tuple(json.loads(row["expected_models_json"])),
        worker_ids=tuple(json.loads(row["worker_ids_json"])),
        enabled=bool(row["enabled"]),
        max_attempts=int(row["max_attempts"]),
        monitor_interval_seconds=float(row["monitor_interval_seconds"]),
        failure_backoff_seconds=float(row["failure_backoff_seconds"]),
        lease_ttl_seconds=float(row["lease_ttl_seconds"]),
    )
