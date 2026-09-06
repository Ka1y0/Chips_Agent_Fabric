from __future__ import annotations

import asyncio
import hashlib
import json
import os
import re
import shutil
import uuid
from collections.abc import Awaitable, Callable, Iterable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from enum import StrEnum
from pathlib import Path
from typing import Any, Protocol

import httpx

from project_supervisor.adapters import (
    LocalWorkerAdapter,
    WorkerJobLaunchRejected,
    WorkerProtocolError,
)
from project_supervisor.domain import EventSeverity, TaskRequirements, TaskState
from project_supervisor.store import (
    StateStore,
    compact_json,
    lease_expiry_timestamp,
    timestamp,
)

from .persistence import _event

EXECUTABILITY_SCHEMA_VERSION = "worker-executability/v1"
_SEMANTIC_ID = re.compile(r"^[A-Za-z][A-Za-z0-9._:-]{0,159}$")
_PROVIDER_ID = re.compile(r"^[a-z][a-z0-9._-]{0,63}$")
_SERVICE_NAME = re.compile(r"^[a-z][a-z0-9._-]{0,95}$")
_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_MAX_TAILSCALE_STATUS_BYTES = 2 * 1024 * 1024
_MAX_BOOTSTRAP_RESPONSE_BYTES = 64 * 1024


class EvidenceState(StrEnum):
    YES = "yes"
    NO = "no"
    UNKNOWN = "unknown"


class PlatformApprovalState(StrEnum):
    NOT_REQUIRED = "notRequired"
    APPROVED = "approved"
    PENDING = "pending"
    REJECTED = "rejected"
    UNKNOWN = "unknown"


class ExecutabilityDisposition(StrEnum):
    EXECUTABLE = "executable"
    NOT_EXECUTABLE = "notExecutable"
    UNKNOWN = "unknown"
    STALE = "stale"


@dataclass(frozen=True, slots=True)
class RuntimeExecutability:
    disposition: ExecutabilityDisposition
    rejection_code: str | None
    reasons: tuple[str, ...]

    @property
    def executable(self) -> bool:
        return self.disposition is ExecutabilityDisposition.EXECUTABLE


@dataclass(frozen=True, slots=True)
class WorkerExecutionObservation:
    worker_id: str
    node_id: str
    discovered: EvidenceState
    configured: EvidenceState
    authenticated: EvidenceState
    authorized: EvidenceState
    platform_approval: PlatformApprovalState
    reachable: EvidenceState
    runtime_available: EvidenceState
    healthy: EvidenceState
    capacity_available: EvidenceState
    observed_at: datetime
    valid_until: datetime
    binding_id: str | None = None
    protocol_version: str | None = None
    runtime_identity: str | None = None
    reason_codes: tuple[str, ...] = ()
    schema_version: str = EXECUTABILITY_SCHEMA_VERSION

    def __post_init__(self) -> None:
        for name in (self.worker_id, self.node_id):
            if not _SEMANTIC_ID.fullmatch(name):
                raise ValueError("Worker and Node identities must be bounded semantic IDs")
        if self.binding_id is not None and not _SEMANTIC_ID.fullmatch(self.binding_id):
            raise ValueError("binding_id must be a bounded semantic ID")
        if self.schema_version != EXECUTABILITY_SCHEMA_VERSION:
            raise ValueError("unsupported Worker executability schema")
        if self.observed_at.tzinfo is None or self.valid_until.tzinfo is None:
            raise ValueError("execution observation timestamps must be timezone-aware")
        if self.valid_until <= self.observed_at:
            raise ValueError("execution observation validity must follow observation time")
        if len(self.reason_codes) > 32 or any(
            not _SEMANTIC_ID.fullmatch(reason) for reason in self.reason_codes
        ):
            raise ValueError("execution reason codes must be bounded semantic IDs")
        if self.runtime_identity is not None and len(self.runtime_identity) > 512:
            raise ValueError("runtime identity must be bounded")

    def evaluate(self, *, now: datetime | None = None) -> RuntimeExecutability:
        observed = now or datetime.now(UTC)
        if observed >= self.valid_until:
            return RuntimeExecutability(
                ExecutabilityDisposition.STALE,
                "RUNTIME_OBSERVATION_STALE",
                (*self.reason_codes, "observationExpired"),
            )
        ordered: tuple[tuple[EvidenceState, str], ...] = (
            (self.discovered, "RUNTIME_NOT_DISCOVERED"),
            (self.configured, "RUNTIME_NOT_CONFIGURED"),
            (self.authenticated, "RUNTIME_NOT_AUTHENTICATED"),
            (self.authorized, "RUNTIME_NOT_AUTHORIZED"),
            (self.reachable, "RUNTIME_UNREACHABLE"),
            (self.runtime_available, "RUNTIME_NOT_AVAILABLE"),
            (self.healthy, "RUNTIME_UNHEALTHY"),
            (self.capacity_available, "RUNTIME_CAPACITY_UNAVAILABLE"),
        )
        for state, code in ordered:
            if state is EvidenceState.NO:
                return RuntimeExecutability(
                    ExecutabilityDisposition.NOT_EXECUTABLE,
                    code,
                    self.reason_codes,
                )
            if state is EvidenceState.UNKNOWN:
                return RuntimeExecutability(
                    ExecutabilityDisposition.UNKNOWN,
                    "RUNTIME_EXECUTABILITY_UNKNOWN",
                    (*self.reason_codes, code),
                )
        if self.platform_approval in {
            PlatformApprovalState.PENDING,
            PlatformApprovalState.REJECTED,
        }:
            code = (
                "PLATFORM_APPROVAL_REQUIRED"
                if self.platform_approval is PlatformApprovalState.PENDING
                else "PLATFORM_APPROVAL_REJECTED"
            )
            return RuntimeExecutability(
                ExecutabilityDisposition.NOT_EXECUTABLE,
                code,
                self.reason_codes,
            )
        if self.platform_approval is PlatformApprovalState.UNKNOWN:
            return RuntimeExecutability(
                ExecutabilityDisposition.UNKNOWN,
                "PLATFORM_APPROVAL_UNKNOWN",
                self.reason_codes,
            )
        return RuntimeExecutability(
            ExecutabilityDisposition.EXECUTABLE,
            None,
            self.reason_codes,
        )


@dataclass(frozen=True, slots=True)
class TransportPeerObservation:
    provider: str
    peer_identity: str
    platform: str | None
    online: EvidenceState
    reachable: EvidenceState
    observed_at: datetime

    def __post_init__(self) -> None:
        if not _PROVIDER_ID.fullmatch(self.provider):
            raise ValueError("transport provider must be a bounded semantic ID")
        if not self.peer_identity or len(self.peer_identity) > 512:
            raise ValueError("transport peer identity must be bounded and non-empty")
        if self.observed_at.tzinfo is None:
            raise ValueError("transport observation time must be timezone-aware")

    @property
    def identity_sha256(self) -> str:
        return hashlib.sha256(self.peer_identity.encode("utf-8")).hexdigest()


@dataclass(frozen=True, slots=True)
class FabricRuntimeBootstrapDescriptor:
    adapter_type: str
    adapter_instance_id: str
    broker_authority_id: str
    broker_registry_id: str
    service_profile_revision: int
    service_profile_sha256: str
    operation: str = "fabric.runtime.start"
    enforces_generation: bool = True
    enforces_deadline: bool = True
    enforces_idempotency: bool = True
    max_operation_seconds: float = 30.0

    def __post_init__(self) -> None:
        if any(
            not _SEMANTIC_ID.fullmatch(value)
            for value in (
                self.adapter_type,
                self.adapter_instance_id,
                self.broker_authority_id,
                self.broker_registry_id,
            )
        ):
            raise ValueError("bootstrap adapter identities must be bounded semantic IDs")
        if self.service_profile_revision < 1 or not _SHA256.fullmatch(self.service_profile_sha256):
            raise ValueError("bootstrap service profile identity is invalid")
        if self.operation != "fabric.runtime.start":
            raise ValueError("bootstrap adapter may expose only fabric.runtime.start")
        if not (self.enforces_generation and self.enforces_deadline and self.enforces_idempotency):
            raise ValueError("bootstrap adapter must enforce generation, deadline and idempotency")
        if not 1 <= self.max_operation_seconds <= 300:
            raise ValueError(
                "bootstrap adapter operation bound must be between one and 300 seconds"
            )


@dataclass(frozen=True, slots=True)
class FabricRuntimeStartRequest:
    attempt_id: str
    binding_id: str
    binding_generation: int
    node_id: str
    service_name: str
    task_id: str
    requirements_sha256: str
    authorization_id: str
    authorization_version: int
    authorization_sha256: str
    broker_authority_id: str
    broker_registry_id: str
    service_profile_revision: int
    service_profile_sha256: str
    lease_owner_id: str
    lease_generation: int
    idempotency_key: str
    requested_at: datetime
    deadline_at: datetime
    operation: str = "fabric.runtime.start"
    schema_version: str = "fabric-runtime-start/v1"

    def __post_init__(self) -> None:
        for value in (
            self.attempt_id,
            self.binding_id,
            self.node_id,
            self.service_name,
            self.task_id,
            self.authorization_id,
            self.broker_authority_id,
            self.broker_registry_id,
            self.lease_owner_id,
            self.idempotency_key,
        ):
            if not _SEMANTIC_ID.fullmatch(value):
                raise ValueError("runtime start request identities must be bounded semantic IDs")
        if not _SHA256.fullmatch(self.requirements_sha256) or not _SHA256.fullmatch(
            self.authorization_sha256
        ):
            raise ValueError("runtime start requirements and authorization digests must be SHA-256")
        if (
            self.binding_generation < 1
            or self.authorization_version < 1
            or self.service_profile_revision < 1
        ):
            raise ValueError(
                "runtime start binding, authorization and profile versions must be positive"
            )
        if self.lease_generation < 1:
            raise ValueError("runtime start lease generation must be positive")
        if not _SHA256.fullmatch(self.service_profile_sha256):
            raise ValueError("runtime start service profile digest must be SHA-256")
        if self.operation != "fabric.runtime.start":
            raise ValueError("runtime start operation is fixed")
        if self.schema_version != "fabric-runtime-start/v1":
            raise ValueError("unsupported runtime start schema")
        if self.requested_at.tzinfo is None or self.deadline_at.tzinfo is None:
            raise ValueError("runtime start timestamps must be timezone-aware")
        if not self.requested_at < self.deadline_at:
            raise ValueError("runtime start deadline must follow its request time")

    def to_protocol(self) -> dict[str, Any]:
        return {
            "schemaVersion": self.schema_version,
            "attemptID": self.attempt_id,
            "bindingID": self.binding_id,
            "bindingGeneration": self.binding_generation,
            "nodeID": self.node_id,
            "serviceName": self.service_name,
            "taskID": self.task_id,
            "requirementsSHA256": self.requirements_sha256,
            "authorizationID": self.authorization_id,
            "authorizationVersion": self.authorization_version,
            "authorizationSHA256": self.authorization_sha256,
            "brokerAuthorityID": self.broker_authority_id,
            "brokerRegistryID": self.broker_registry_id,
            "serviceProfileRevision": self.service_profile_revision,
            "serviceProfileSHA256": self.service_profile_sha256,
            "leaseOwnerID": self.lease_owner_id,
            "leaseGeneration": self.lease_generation,
            "idempotencyKey": self.idempotency_key,
            "operation": self.operation,
            "requestedAt": timestamp(self.requested_at),
            "deadlineAt": timestamp(self.deadline_at),
        }

    @property
    def digest(self) -> str:
        return hashlib.sha256(compact_json(self.to_protocol()).encode("utf-8")).hexdigest()

    @classmethod
    def from_protocol(cls, value: Mapping[str, Any]) -> FabricRuntimeStartRequest:
        expected = {
            "schemaVersion",
            "attemptID",
            "bindingID",
            "bindingGeneration",
            "nodeID",
            "serviceName",
            "taskID",
            "requirementsSHA256",
            "authorizationID",
            "authorizationVersion",
            "authorizationSHA256",
            "brokerAuthorityID",
            "brokerRegistryID",
            "serviceProfileRevision",
            "serviceProfileSHA256",
            "leaseOwnerID",
            "leaseGeneration",
            "idempotencyKey",
            "operation",
            "requestedAt",
            "deadlineAt",
        }
        if set(value) != expected:
            raise ValueError("runtime start request has an invalid shape")
        positive_integer_fields = (
            "bindingGeneration",
            "authorizationVersion",
            "serviceProfileRevision",
            "leaseGeneration",
        )
        for field in positive_integer_fields:
            item = value.get(field)
            if isinstance(item, bool) or not isinstance(item, int) or item < 1:
                raise ValueError(
                    "runtime start request generation fields must be positive integers"
                )
        requested_at = value.get("requestedAt")
        deadline_at = value.get("deadlineAt")
        if not isinstance(requested_at, str) or not isinstance(deadline_at, str):
            raise ValueError("runtime start request timestamps are invalid")
        return cls(
            attempt_id=str(value.get("attemptID")),
            binding_id=str(value.get("bindingID")),
            binding_generation=int(value["bindingGeneration"]),
            node_id=str(value.get("nodeID")),
            service_name=str(value.get("serviceName")),
            task_id=str(value.get("taskID")),
            requirements_sha256=str(value.get("requirementsSHA256")),
            authorization_id=str(value.get("authorizationID")),
            authorization_version=int(value["authorizationVersion"]),
            authorization_sha256=str(value.get("authorizationSHA256")),
            broker_authority_id=str(value.get("brokerAuthorityID")),
            broker_registry_id=str(value.get("brokerRegistryID")),
            service_profile_revision=int(value["serviceProfileRevision"]),
            service_profile_sha256=str(value.get("serviceProfileSHA256")),
            lease_owner_id=str(value.get("leaseOwnerID")),
            lease_generation=int(value["leaseGeneration"]),
            idempotency_key=str(value.get("idempotencyKey")),
            requested_at=_aware_datetime(requested_at),
            deadline_at=_aware_datetime(deadline_at),
            operation=str(value.get("operation")),
            schema_version=str(value.get("schemaVersion")),
        )


@dataclass(frozen=True, slots=True)
class FabricRuntimeStartResult:
    attempt_id: str
    receipt_id: str
    request_sha256: str
    binding_id: str
    binding_generation: int
    node_id: str
    service_name: str
    broker_authority_id: str
    broker_registry_id: str
    service_profile_revision: int
    service_profile_sha256: str
    idempotency_key: str
    lease_generation: int
    state: str
    disposition: str
    external_start_boundary_crossed: str
    service_state: str
    accepted: bool
    reason_code: str
    observed_at: datetime
    idempotent_replay: bool = False
    schema_version: str = "fabric-runtime-start-result/v1"

    def __post_init__(self) -> None:
        for value in (
            self.attempt_id,
            self.receipt_id,
            self.binding_id,
            self.node_id,
            self.service_name,
            self.broker_authority_id,
            self.broker_registry_id,
            self.idempotency_key,
            self.reason_code,
        ):
            if not _SEMANTIC_ID.fullmatch(value):
                raise ValueError("runtime start result fields must be bounded semantic IDs")
        if self.binding_generation < 1 or self.lease_generation < 1:
            raise ValueError("runtime start result generation must be positive")
        if not _SHA256.fullmatch(self.request_sha256):
            raise ValueError("runtime start result request digest must be SHA-256")
        if self.service_profile_revision < 1 or not _SHA256.fullmatch(self.service_profile_sha256):
            raise ValueError("runtime start result service profile identity is invalid")
        if self.observed_at.tzinfo is None:
            raise ValueError("runtime start result timestamp must be timezone-aware")
        if self.state not in {
            "running",
            "alreadyRunning",
            "rejectedPreStart",
            "startPending",
            "outcomeUnknown",
            "failed",
        }:
            raise ValueError("runtime start result state is invalid")
        if self.disposition not in {
            "definitelyNotRequested",
            "definitelyStartRequested",
            "startInProgress",
            "desiredStateSatisfied",
            "startOutcomeUnknown",
        }:
            raise ValueError("runtime start result disposition is invalid")
        if self.external_start_boundary_crossed not in {"yes", "no", "unknown"}:
            raise ValueError("runtime start boundary evidence is invalid")
        if self.service_state not in {
            "running",
            "startPending",
            "stopped",
            "unknown",
        }:
            raise ValueError("runtime start service state is invalid")
        if self.accepted != (self.state in {"running", "alreadyRunning"}):
            raise ValueError("runtime start accepted flag conflicts with semantic state")
        if self.schema_version != "fabric-runtime-start-result/v1":
            raise ValueError("unsupported runtime start result schema")

    def to_protocol(self) -> dict[str, Any]:
        return {
            "schemaVersion": self.schema_version,
            "attemptID": self.attempt_id,
            "receiptID": self.receipt_id,
            "requestSHA256": self.request_sha256,
            "bindingID": self.binding_id,
            "bindingGeneration": self.binding_generation,
            "nodeID": self.node_id,
            "serviceName": self.service_name,
            "brokerAuthorityID": self.broker_authority_id,
            "brokerRegistryID": self.broker_registry_id,
            "serviceProfileRevision": self.service_profile_revision,
            "serviceProfileSHA256": self.service_profile_sha256,
            "idempotencyKey": self.idempotency_key,
            "leaseGeneration": self.lease_generation,
            "state": self.state,
            "disposition": self.disposition,
            "externalStartBoundaryCrossed": self.external_start_boundary_crossed,
            "serviceState": self.service_state,
            "accepted": self.accepted,
            "reasonCode": self.reason_code,
            "observedAt": timestamp(self.observed_at),
            "idempotentReplay": self.idempotent_replay,
        }

    @classmethod
    def from_protocol(cls, value: Mapping[str, Any]) -> FabricRuntimeStartResult:
        expected = {
            "schemaVersion",
            "attemptID",
            "receiptID",
            "requestSHA256",
            "bindingID",
            "bindingGeneration",
            "nodeID",
            "serviceName",
            "brokerAuthorityID",
            "brokerRegistryID",
            "serviceProfileRevision",
            "serviceProfileSHA256",
            "idempotencyKey",
            "leaseGeneration",
            "state",
            "disposition",
            "externalStartBoundaryCrossed",
            "serviceState",
            "accepted",
            "reasonCode",
            "observedAt",
            "idempotentReplay",
        }
        if set(value) != expected:
            raise ValueError("runtime start result has an invalid shape")
        accepted = value.get("accepted")
        replay = value.get("idempotentReplay")
        binding_generation = value.get("bindingGeneration")
        generation = value.get("leaseGeneration")
        profile_revision = value.get("serviceProfileRevision")
        if (
            not isinstance(accepted, bool)
            or not isinstance(replay, bool)
            or isinstance(binding_generation, bool)
            or not isinstance(binding_generation, int)
            or isinstance(generation, bool)
            or not isinstance(generation, int)
            or isinstance(profile_revision, bool)
            or not isinstance(profile_revision, int)
        ):
            raise ValueError("runtime start result has invalid typed fields")
        observed_at = value.get("observedAt")
        if not isinstance(observed_at, str):
            raise ValueError("runtime start result timestamp is invalid")
        return cls(
            attempt_id=str(value.get("attemptID")),
            receipt_id=str(value.get("receiptID")),
            request_sha256=str(value.get("requestSHA256")),
            binding_id=str(value.get("bindingID")),
            binding_generation=binding_generation,
            node_id=str(value.get("nodeID")),
            service_name=str(value.get("serviceName")),
            broker_authority_id=str(value.get("brokerAuthorityID")),
            broker_registry_id=str(value.get("brokerRegistryID")),
            service_profile_revision=profile_revision,
            service_profile_sha256=str(value.get("serviceProfileSHA256")),
            idempotency_key=str(value.get("idempotencyKey")),
            lease_generation=generation,
            state=str(value.get("state")),
            disposition=str(value.get("disposition")),
            external_start_boundary_crossed=str(value.get("externalStartBoundaryCrossed")),
            service_state=str(value.get("serviceState")),
            accepted=accepted,
            reason_code=str(value.get("reasonCode")),
            observed_at=_aware_datetime(observed_at),
            idempotent_replay=replay,
            schema_version=str(value.get("schemaVersion")),
        )


def parse_tailscale_status(
    value: Mapping[str, Any], *, observed_at: datetime | None = None
) -> tuple[TransportPeerObservation, ...]:
    """Parse stable peer identities without treating an address or hostname as authority."""

    peers = value.get("Peer")
    if not isinstance(peers, Mapping):
        return ()
    when = observed_at or datetime.now(UTC)
    observations: list[TransportPeerObservation] = []
    for key, raw in peers.items():
        if not isinstance(raw, Mapping):
            continue
        stable_id = raw.get("ID")
        public_key = raw.get("PublicKey")
        identity = stable_id if isinstance(stable_id, str) and stable_id else public_key
        if not isinstance(identity, str) or not identity:
            continue
        online = raw.get("Online")
        if not isinstance(online, bool):
            online_state = EvidenceState.UNKNOWN
        else:
            online_state = EvidenceState.YES if online else EvidenceState.NO
        # A stable JSON membership observation proves online/offline only.  Reachability requires a
        # bounded ping or an authenticated service probe and therefore remains UNKNOWN here.
        observations.append(
            TransportPeerObservation(
                provider="tailscale",
                peer_identity=f"{key}:{identity}",
                platform=(str(raw["OS"]) if isinstance(raw.get("OS"), str) else None),
                online=online_state,
                reachable=EvidenceState.UNKNOWN,
                observed_at=when,
            )
        )
    return tuple(sorted(observations, key=lambda item: item.identity_sha256))


class ExecutionPlaneRepository:
    def __init__(self, store: StateStore) -> None:
        self.store = store

    def register_binding(
        self,
        *,
        node_id: str,
        transport_provider: str,
        peer_identity: str,
        service_name: str,
        endpoint_ref: str,
        configured_by: str,
        expected_platform: str | None = None,
        enabled: bool = True,
    ) -> dict[str, Any]:
        if not _PROVIDER_ID.fullmatch(transport_provider):
            raise ValueError("transport provider must be a bounded semantic ID")
        if not peer_identity or len(peer_identity) > 512:
            raise ValueError("transport peer identity must be bounded and non-empty")
        if not _SERVICE_NAME.fullmatch(service_name):
            raise ValueError("transport service name must be a bounded semantic ID")
        for value in (endpoint_ref, configured_by):
            if not _SEMANTIC_ID.fullmatch(value):
                raise ValueError("endpoint and actor references must be opaque semantic IDs")
        fingerprint = hashlib.sha256(peer_identity.encode("utf-8")).hexdigest()
        now = timestamp()
        with self.store.transaction() as connection:
            if connection.execute("SELECT 1 FROM nodes WHERE id=?", (node_id,)).fetchone() is None:
                raise KeyError(node_id)
            row = connection.execute(
                "SELECT * FROM node_transport_bindings WHERE node_id=? AND transport_provider=?",
                (node_id, transport_provider),
            ).fetchone()
            if row is not None:
                if (
                    row["peer_identity_sha256"] != fingerprint
                    or row["service_name"] != service_name
                    or row["endpoint_ref"] != endpoint_ref
                ):
                    raise RuntimeError(
                        "transport binding identity is immutable; register a new Node"
                    )
                return dict(row)
            identity = f"node-binding-{uuid.uuid4()}"
            connection.execute(
                "INSERT INTO node_transport_bindings(id,node_id,transport_provider,"
                "peer_identity_sha256,service_name,endpoint_ref,expected_platform,enabled,"
                "generation,configured_by,created_at,updated_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    identity,
                    node_id,
                    transport_provider,
                    fingerprint,
                    service_name,
                    endpoint_ref,
                    expected_platform,
                    int(enabled),
                    1,
                    configured_by,
                    now,
                    now,
                ),
            )
            _event(
                self.store,
                connection,
                kind="nodeTransportBound",
                entity_type="nodeTransportBinding",
                entity_id=identity,
                summary="Operator-pinned transport identity bound to Fabric Node",
                payload={
                    "bindingID": identity,
                    "nodeID": node_id,
                    "transportProvider": transport_provider,
                    "serviceName": service_name,
                    "enabled": enabled,
                    "generation": 1,
                },
                actor=configured_by,
            )
            return dict(
                connection.execute(
                    "SELECT * FROM node_transport_bindings WHERE id=?", (identity,)
                ).fetchone()
            )

    def bindings(self, *, enabled_only: bool = True) -> list[dict[str, Any]]:
        where = " WHERE enabled=1" if enabled_only else ""
        with self.store.connect() as connection:
            return [
                dict(row)
                for row in connection.execute(
                    "SELECT * FROM node_transport_bindings" + where + " ORDER BY node_id,id"
                ).fetchall()
            ]

    def claim_binding_recovery(
        self,
        binding_id: str,
        *,
        owner_id: str,
        lease_seconds: float,
    ) -> int | None:
        if not _SEMANTIC_ID.fullmatch(owner_id):
            raise ValueError("binding recovery owner must be a bounded semantic ID")
        if not 1 <= lease_seconds <= 300:
            raise ValueError("binding recovery lease must be between one and 300 seconds")
        now_value = datetime.now(UTC)
        now = timestamp(now_value)
        expires_at = lease_expiry_timestamp(now_value, lease_seconds)
        with self.store.transaction() as connection:
            binding = connection.execute(
                "SELECT node_id FROM node_transport_bindings WHERE id=? AND enabled=1",
                (binding_id,),
            ).fetchone()
            if binding is None:
                return None
            lease = connection.execute(
                "SELECT * FROM node_execution_recovery_leases WHERE binding_id=?",
                (binding_id,),
            ).fetchone()
            if lease is not None and lease["state"] == "active" and lease["expires_at"] > now:
                return None
            generation = int(lease["generation"]) + 1 if lease is not None else 1
            connection.execute(
                "INSERT INTO node_execution_recovery_leases(binding_id,owner_id,generation,state,"
                "acquired_at,heartbeat_at,expires_at,released_at) "
                "VALUES (?,?,?,'active',?,?,?,NULL) ON CONFLICT(binding_id) DO UPDATE SET "
                "owner_id=excluded.owner_id,generation=excluded.generation,state='active',"
                "acquired_at=excluded.acquired_at,heartbeat_at=excluded.heartbeat_at,"
                "expires_at=excluded.expires_at,released_at=NULL",
                (binding_id, owner_id, generation, now, now, expires_at),
            )
            _event(
                self.store,
                connection,
                kind="nodeExecutionRecoveryLeaseAcquired",
                entity_type="nodeTransportBinding",
                entity_id=binding_id,
                summary="Fabric Node recovery single-flight lease acquired",
                payload={
                    "bindingID": binding_id,
                    "nodeID": binding["node_id"],
                    "generation": generation,
                    "expiresAt": expires_at,
                },
                actor=owner_id,
            )
            return generation

    def release_binding_recovery(
        self,
        binding_id: str,
        *,
        owner_id: str,
        generation: int,
    ) -> bool:
        with self.store.transaction() as connection:
            cursor = connection.execute(
                "UPDATE node_execution_recovery_leases SET state='released',released_at=?,"
                "heartbeat_at=? WHERE binding_id=? AND owner_id=? AND generation=? "
                "AND state='active'",
                (timestamp(), timestamp(), binding_id, owner_id, generation),
            )
            return cursor.rowcount == 1

    def has_unresolved_runtime_start(
        self,
        *,
        binding_id: str,
        task_id: str,
        requirements_sha256: str,
    ) -> bool:
        with self.store.connect() as connection:
            return self._runtime_start_unresolved(
                connection,
                binding_id=binding_id,
                task_id=task_id,
                requirements_sha256=requirements_sha256,
            )

    def unresolved_runtime_start(
        self,
        *,
        binding_id: str,
        task_id: str,
        requirements_sha256: str,
    ) -> FabricRuntimeStartRequest | None:
        with self.store.connect() as connection:
            rows = connection.execute(
                "SELECT attempt.*,binding.node_id,binding.service_name "
                "FROM node_execution_recovery_attempts attempt "
                "JOIN node_transport_bindings binding ON binding.id=attempt.binding_id "
                "JOIN node_execution_recovery_events event ON event.attempt_id=attempt.id "
                "WHERE attempt.binding_id=? AND attempt.task_id=? "
                "AND attempt.requirements_sha256=? "
                "AND event.ordinal=(SELECT MAX(latest.ordinal) "
                "FROM node_execution_recovery_events latest "
                "WHERE latest.attempt_id=attempt.id) "
                "AND event.stage IN ('requested','accepted','outcomeUnknown') "
                "ORDER BY attempt.requested_at DESC",
                (binding_id, task_id, requirements_sha256),
            ).fetchall()
            if not rows:
                return None
            if len(rows) != 1:
                raise RuntimeError("multiple runtime start outcomes remain unresolved")
            row = rows[0]
            authorization = connection.execute(
                "SELECT * FROM authorization_envelopes WHERE id=?",
                (row["authorization_id"],),
            ).fetchone()
            if authorization is None or (
                authorization["expires_at"] is not None
                and _aware_datetime(str(authorization["expires_at"])) <= datetime.now(UTC)
            ):
                raise ValueError("runtime start authorization is no longer current")
            key = f"fabric-runtime-start:{row['id']}"
            if hashlib.sha256(key.encode("utf-8")).hexdigest() != row["idempotency_key_sha256"]:
                raise RuntimeError("runtime start idempotency identity cannot be reconstructed")
            request = FabricRuntimeStartRequest(
                attempt_id=str(row["id"]),
                binding_id=str(row["binding_id"]),
                binding_generation=int(row["binding_generation"]),
                node_id=str(row["node_id"]),
                service_name=str(row["service_name"]),
                task_id=str(row["task_id"]),
                requirements_sha256=str(row["requirements_sha256"]),
                authorization_id=str(row["authorization_id"]),
                authorization_version=int(row["authorization_version"]),
                authorization_sha256=str(row["authorization_sha256"]),
                broker_authority_id=str(row["broker_authority_id"]),
                broker_registry_id=str(row["broker_registry_id"]),
                service_profile_revision=int(row["service_profile_revision"]),
                service_profile_sha256=str(row["service_profile_sha256"]),
                lease_owner_id=str(row["owner_id"]),
                lease_generation=int(row["lease_generation"]),
                idempotency_key=key,
                requested_at=_aware_datetime(str(row["requested_at"])),
                deadline_at=_aware_datetime(str(row["deadline_at"])),
            )
            if request.digest != row["request_sha256"]:
                raise RuntimeError("runtime start request reconstruction changed its digest")
            return request

    def begin_runtime_start(
        self,
        *,
        binding_id: str,
        task_id: str,
        requirements_sha256: str,
        bootstrap_descriptor: FabricRuntimeBootstrapDescriptor,
        owner_id: str,
        lease_generation: int,
        deadline_seconds: float,
    ) -> FabricRuntimeStartRequest:
        if not _SHA256.fullmatch(requirements_sha256):
            raise ValueError("runtime start requirements digest must be SHA-256")
        if not 1 <= deadline_seconds <= 300:
            raise ValueError("runtime start deadline must be between one and 300 seconds")
        requested_at = datetime.now(UTC)
        deadline_at = requested_at + timedelta(seconds=deadline_seconds)
        now = timestamp(requested_at)
        with self.store.transaction() as connection:
            self._require_binding_recovery_lease(
                connection,
                binding_id=binding_id,
                owner_id=owner_id,
                generation=lease_generation,
            )
            binding = connection.execute(
                "SELECT * FROM node_transport_bindings WHERE id=? AND enabled=1",
                (binding_id,),
            ).fetchone()
            if binding is None:
                raise ValueError("runtime start binding is unavailable")
            if bootstrap_descriptor.service_profile_sha256 == "0" * 64:
                raise ValueError("runtime start service profile is not pinned")
            wait = connection.execute(
                "SELECT requirements_sha256,state FROM task_capacity_waits WHERE task_id=?",
                (task_id,),
            ).fetchone()
            if (
                wait is None
                or wait["state"] != "waiting"
                or wait["requirements_sha256"] != requirements_sha256
            ):
                raise ValueError("runtime start is not bound to the current capacity wait")
            task = connection.execute(
                "SELECT project_id,state FROM tasks WHERE id=?",
                (task_id,),
            ).fetchone()
            if task is None or task["state"] not in {
                TaskState.READY.value,
                TaskState.BLOCKED.value,
            }:
                raise ValueError("runtime start Task is not waiting for execution capacity")
            authorizations = connection.execute(
                "SELECT envelope.* FROM authorization_envelope_bindings binding "
                "JOIN authorization_envelopes envelope ON envelope.id=binding.envelope_id "
                "WHERE binding.task_id=? AND binding.binding_kind='task' "
                "AND binding.run_id IS NULL ORDER BY binding.created_at DESC",
                (task_id,),
            ).fetchall()
            if len(authorizations) != 1:
                raise ValueError("runtime start requires one unambiguous Task authorization")
            authorization = authorizations[0]
            if authorization["project_id"] != task["project_id"]:
                raise ValueError("runtime start authorization project mismatch")
            if (
                authorization["expires_at"] is not None
                and _aware_datetime(str(authorization["expires_at"])) <= requested_at
            ):
                raise ValueError("runtime start authorization expired")
            if authorization["user_approval_state"] not in {"notRequired", "approved"}:
                raise ValueError("runtime start user authorization is not approved")
            if authorization["platform_approval_state"] not in {"notRequired", "approved"}:
                raise ValueError("runtime start platform approval is not satisfied")
            allowed_actions = set(json.loads(authorization["allowed_action_classes_json"]))
            denied_actions = set(json.loads(authorization["denied_action_classes_json"]))
            if (
                "fabric.runtime.start" not in allowed_actions
                or "fabric.runtime.start" in denied_actions
            ):
                raise ValueError("runtime start action is not authorized")
            if self._runtime_start_unresolved(
                connection,
                binding_id=binding_id,
                task_id=task_id,
                requirements_sha256=requirements_sha256,
            ):
                raise RuntimeError(
                    "runtime start outcome is unresolved; probe instead of replaying"
                )
            attempt_id = f"node-runtime-start-{uuid.uuid4()}"
            idempotency_key = f"fabric-runtime-start:{attempt_id}"
            request = FabricRuntimeStartRequest(
                attempt_id=attempt_id,
                binding_id=binding_id,
                binding_generation=int(binding["generation"]),
                node_id=str(binding["node_id"]),
                service_name=str(binding["service_name"]),
                task_id=task_id,
                requirements_sha256=requirements_sha256,
                authorization_id=str(authorization["id"]),
                authorization_version=int(authorization["version"]),
                authorization_sha256=str(authorization["definition_sha256"]),
                broker_authority_id=bootstrap_descriptor.broker_authority_id,
                broker_registry_id=bootstrap_descriptor.broker_registry_id,
                service_profile_revision=bootstrap_descriptor.service_profile_revision,
                service_profile_sha256=bootstrap_descriptor.service_profile_sha256,
                lease_owner_id=owner_id,
                lease_generation=lease_generation,
                idempotency_key=idempotency_key,
                requested_at=requested_at,
                deadline_at=deadline_at,
            )
            connection.execute(
                "INSERT INTO node_execution_recovery_attempts(id,binding_id,binding_generation,"
                "task_id,requirements_sha256,authorization_id,authorization_version,owner_id,"
                "authorization_sha256,lease_generation,operation,broker_authority_id,"
                "broker_registry_id,service_profile_revision,service_profile_sha256,"
                "idempotency_key_sha256,request_sha256,requested_at,deadline_at,created_at) "
                "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    request.attempt_id,
                    request.binding_id,
                    request.binding_generation,
                    request.task_id,
                    request.requirements_sha256,
                    request.authorization_id,
                    request.authorization_version,
                    request.lease_owner_id,
                    request.authorization_sha256,
                    request.lease_generation,
                    request.operation,
                    request.broker_authority_id,
                    request.broker_registry_id,
                    request.service_profile_revision,
                    request.service_profile_sha256,
                    hashlib.sha256(request.idempotency_key.encode("utf-8")).hexdigest(),
                    request.digest,
                    timestamp(request.requested_at),
                    timestamp(request.deadline_at),
                    now,
                ),
            )
            self._append_runtime_recovery_event(
                connection,
                request=request,
                event_key="request",
                stage="requested",
                reason_code="typedRuntimeStartRequested",
                observed_at=request.requested_at,
            )
            return request

    def record_runtime_start_result(
        self,
        request: FabricRuntimeStartRequest,
        result: FabricRuntimeStartResult,
        *,
        lease_owner_id: str | None = None,
        lease_generation: int | None = None,
    ) -> None:
        if (
            result.attempt_id != request.attempt_id
            or result.request_sha256 != request.digest
            or result.binding_id != request.binding_id
            or result.binding_generation != request.binding_generation
            or result.node_id != request.node_id
            or result.service_name != request.service_name
            or result.broker_authority_id != request.broker_authority_id
            or result.broker_registry_id != request.broker_registry_id
            or result.service_profile_revision != request.service_profile_revision
            or result.service_profile_sha256 != request.service_profile_sha256
            or result.idempotency_key != request.idempotency_key
            or result.lease_generation != request.lease_generation
        ):
            raise ValueError("runtime start result does not match its fenced request")
        with self.store.transaction() as connection:
            self._require_binding_recovery_lease(
                connection,
                binding_id=request.binding_id,
                owner_id=lease_owner_id or request.lease_owner_id,
                generation=lease_generation or request.lease_generation,
            )
            self._require_runtime_start_request(connection, request)
            result_protocol = result.to_protocol()
            result_protocol["idempotentReplay"] = False
            self._append_runtime_recovery_event(
                connection,
                request=request,
                event_key=f"result:{result.state}",
                stage=(
                    "accepted"
                    if result.accepted
                    else (
                        "outcomeUnknown"
                        if result.state in {"startPending", "outcomeUnknown"}
                        else "rejected"
                    )
                ),
                reason_code=result.reason_code,
                observed_at=result.observed_at,
                result_generation=result.lease_generation,
                result_protocol=result_protocol,
            )

    def record_runtime_start_outcome_unknown(
        self,
        request: FabricRuntimeStartRequest,
        *,
        reason_code: str,
        lease_owner_id: str | None = None,
        lease_generation: int | None = None,
    ) -> None:
        if not _SEMANTIC_ID.fullmatch(reason_code):
            raise ValueError("runtime start failure reason must be a bounded semantic ID")
        with self.store.transaction() as connection:
            self._require_binding_recovery_lease(
                connection,
                binding_id=request.binding_id,
                owner_id=lease_owner_id or request.lease_owner_id,
                generation=lease_generation or request.lease_generation,
            )
            self._require_runtime_start_request(connection, request)
            self._append_runtime_recovery_event(
                connection,
                request=request,
                event_key="outcome",
                stage="outcomeUnknown",
                reason_code=reason_code,
                observed_at=datetime.now(UTC),
            )

    def record_runtime_start_verified(
        self,
        request: FabricRuntimeStartRequest,
        *,
        lease_owner_id: str | None = None,
        lease_generation: int | None = None,
    ) -> None:
        with self.store.transaction() as connection:
            self._require_binding_recovery_lease(
                connection,
                binding_id=request.binding_id,
                owner_id=lease_owner_id or request.lease_owner_id,
                generation=lease_generation or request.lease_generation,
            )
            self._require_runtime_start_request(connection, request)
            latest = connection.execute(
                "SELECT stage FROM node_execution_recovery_events WHERE attempt_id=? "
                "ORDER BY ordinal DESC LIMIT 1",
                (request.attempt_id,),
            ).fetchone()
            if latest is None or latest["stage"] not in {"accepted", "outcomeUnknown"}:
                raise ValueError(
                    "runtime start can be verified only after a receipt that crossed or may "
                    "have crossed the start boundary"
                )
            self._append_runtime_recovery_event(
                connection,
                request=request,
                event_key="verified",
                stage="verified",
                reason_code="authenticatedRuntimeVerified",
                observed_at=datetime.now(UTC),
            )

    def _require_binding_recovery_lease(
        self,
        connection: Any,
        *,
        binding_id: str,
        owner_id: str,
        generation: int,
    ) -> None:
        lease = connection.execute(
            "SELECT * FROM node_execution_recovery_leases WHERE binding_id=?",
            (binding_id,),
        ).fetchone()
        if (
            lease is None
            or lease["owner_id"] != owner_id
            or int(lease["generation"]) != generation
            or lease["state"] != "active"
            or lease["expires_at"] <= timestamp()
        ):
            raise RuntimeError("Fabric Node recovery lease is no longer current")

    @staticmethod
    def _require_runtime_start_request(connection: Any, request: FabricRuntimeStartRequest) -> None:
        row = connection.execute(
            "SELECT request_sha256 FROM node_execution_recovery_attempts WHERE id=?",
            (request.attempt_id,),
        ).fetchone()
        if row is None or row["request_sha256"] != request.digest:
            raise RuntimeError("runtime start request no longer matches canonical evidence")

    @staticmethod
    def _runtime_start_unresolved(
        connection: Any,
        *,
        binding_id: str,
        task_id: str,
        requirements_sha256: str,
    ) -> bool:
        row = connection.execute(
            "SELECT 1 FROM node_execution_recovery_attempts attempt "
            "JOIN node_execution_recovery_events event ON event.attempt_id=attempt.id "
            "WHERE attempt.binding_id=? AND attempt.task_id=? "
            "AND attempt.requirements_sha256=? "
            "AND event.ordinal=(SELECT MAX(latest.ordinal) "
            "FROM node_execution_recovery_events latest WHERE latest.attempt_id=attempt.id) "
            "AND event.stage IN ('requested','accepted','outcomeUnknown') LIMIT 1",
            (binding_id, task_id, requirements_sha256),
        ).fetchone()
        return row is not None

    def _append_runtime_recovery_event(
        self,
        connection: Any,
        *,
        request: FabricRuntimeStartRequest,
        event_key: str,
        stage: str,
        reason_code: str,
        observed_at: datetime,
        result_generation: int | None = None,
        result_protocol: Mapping[str, Any] | None = None,
    ) -> None:
        payload = {
            "stage": stage,
            "reasonCode": reason_code,
            "resultGeneration": result_generation,
            "result": dict(result_protocol) if result_protocol is not None else None,
        }
        event_sha256 = hashlib.sha256(compact_json(payload).encode("utf-8")).hexdigest()
        existing = connection.execute(
            "SELECT event_sha256 FROM node_execution_recovery_events "
            "WHERE attempt_id=? AND (event_key=? OR event_key GLOB ?) ORDER BY ordinal",
            (request.attempt_id, event_key, f"{event_key}:*"),
        ).fetchall()
        if any(row["event_sha256"] == event_sha256 for row in existing):
            return
        if existing and stage != "outcomeUnknown":
            raise RuntimeError("conflicting runtime recovery event replay")
        latest = connection.execute(
            "SELECT stage FROM node_execution_recovery_events WHERE attempt_id=? "
            "ORDER BY ordinal DESC LIMIT 1",
            (request.attempt_id,),
        ).fetchone()
        latest_stage = str(latest["stage"]) if latest is not None else None
        allowed_previous = {
            "requested": {None},
            "accepted": {"requested", "outcomeUnknown"},
            "rejected": {"requested", "outcomeUnknown"},
            "failed": {"requested"},
            "verified": {"accepted", "outcomeUnknown"},
            "outcomeUnknown": {"requested", "outcomeUnknown"},
        }
        if stage not in allowed_previous or latest_stage not in allowed_previous[stage]:
            raise RuntimeError("runtime recovery event would violate monotonic history")
        ordinal = int(
            connection.execute(
                "SELECT COALESCE(MAX(ordinal),0)+1 AS ordinal "
                "FROM node_execution_recovery_events WHERE attempt_id=?",
                (request.attempt_id,),
            ).fetchone()["ordinal"]
        )
        if existing:
            event_key = f"{event_key}:{ordinal}"
        connection.execute(
            "INSERT INTO node_execution_recovery_events(id,attempt_id,ordinal,event_key,"
            "event_sha256,stage,result_generation,reason_code,observed_at,created_at) "
            "VALUES (?,?,?,?,?,?,?,?,?,?)",
            (
                f"node-recovery-event-{uuid.uuid4()}",
                request.attempt_id,
                ordinal,
                event_key,
                event_sha256,
                stage,
                result_generation,
                reason_code,
                timestamp(observed_at),
                timestamp(),
            ),
        )
        _event(
            self.store,
            connection,
            kind={
                "requested": "nodeRecoveryStarted",
                "accepted": "nodeRecoveryAccepted",
                "rejected": "nodeRecoveryRejected",
                "failed": "nodeRecoveryFailed",
                "verified": "nodeRecovered",
                "outcomeUnknown": "nodeRecoveryOutcomeUnknown",
            }[stage],
            entity_type="nodeExecutionRecoveryAttempt",
            entity_id=request.attempt_id,
            task_id=request.task_id,
            summary=f"Fabric Node runtime recovery {stage}",
            payload={
                "attemptID": request.attempt_id,
                "bindingID": request.binding_id,
                "nodeID": request.node_id,
                "serviceName": request.service_name,
                "generation": request.lease_generation,
                "stage": stage,
                "reasonCode": reason_code,
            },
            actor=request.lease_owner_id,
            severity=(
                EventSeverity.INFO
                if stage in {"requested", "accepted", "verified"}
                else EventSeverity.WARNING
            ),
        )

    def record_worker_observation(
        self, observation: WorkerExecutionObservation, *, actor: str = "execution-plane"
    ) -> dict[str, Any]:
        disposition = observation.evaluate()
        now = timestamp()
        with self.store.transaction() as connection:
            worker = connection.execute(
                "SELECT node_id FROM workers WHERE id=?", (observation.worker_id,)
            ).fetchone()
            if worker is None:
                raise KeyError(observation.worker_id)
            if worker["node_id"] != observation.node_id:
                raise ValueError("execution observation Node does not own the Worker")
            if observation.binding_id is not None:
                binding = connection.execute(
                    "SELECT node_id FROM node_transport_bindings WHERE id=? AND enabled=1",
                    (observation.binding_id,),
                ).fetchone()
                if binding is None or binding["node_id"] != observation.node_id:
                    raise ValueError("execution observation binding does not match the Worker Node")
            latest = connection.execute(
                "SELECT COALESCE(MAX(version),0)+1 AS version "
                "FROM worker_execution_observations WHERE worker_id=?",
                (observation.worker_id,),
            ).fetchone()
            version = int(latest["version"])
            identity = f"worker-execution-observation-{uuid.uuid4()}"
            runtime_fingerprint = (
                hashlib.sha256(observation.runtime_identity.encode("utf-8")).hexdigest()
                if observation.runtime_identity is not None
                else None
            )
            connection.execute(
                "INSERT INTO worker_execution_observations(id,worker_id,node_id,binding_id,"
                "version,schema_version,discovered,configured,authenticated,authorized,"
                "platform_approval,reachable,runtime_available,healthy,capacity_available,"
                "protocol_version,runtime_identity_sha256,reason_codes_json,observed_at,"
                "valid_until,created_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    identity,
                    observation.worker_id,
                    observation.node_id,
                    observation.binding_id,
                    version,
                    observation.schema_version,
                    observation.discovered.value,
                    observation.configured.value,
                    observation.authenticated.value,
                    observation.authorized.value,
                    observation.platform_approval.value,
                    observation.reachable.value,
                    observation.runtime_available.value,
                    observation.healthy.value,
                    observation.capacity_available.value,
                    observation.protocol_version,
                    runtime_fingerprint,
                    compact_json(list(observation.reason_codes)),
                    timestamp(observation.observed_at),
                    timestamp(observation.valid_until),
                    now,
                ),
            )
            _event(
                self.store,
                connection,
                kind="workerExecutabilityObserved",
                entity_type="workerExecutionObservation",
                entity_id=identity,
                summary="Worker runtime executability observed",
                payload={
                    "observationID": identity,
                    "workerID": observation.worker_id,
                    "nodeID": observation.node_id,
                    "version": version,
                    "disposition": disposition.disposition.value,
                    "reasonCode": disposition.rejection_code,
                    "validUntil": timestamp(observation.valid_until),
                },
                worker_id=observation.worker_id,
                actor=actor,
                severity=(EventSeverity.INFO if disposition.executable else EventSeverity.WARNING),
            )
            return dict(
                connection.execute(
                    "SELECT * FROM worker_execution_observations WHERE id=?", (identity,)
                ).fetchone()
            )

    def wait_for_capacity(
        self,
        *,
        task_id: str,
        requirements_sha256: str,
        reason_code: str,
        max_recovery_attempts: int = 2,
        retry_after_seconds: float = 5.0,
    ) -> dict[str, Any]:
        if len(requirements_sha256) != 64:
            raise ValueError("requirements digest must be a SHA-256 hex digest")
        if not 1 <= max_recovery_attempts <= 8 or not 0 < retry_after_seconds <= 3600:
            raise ValueError("capacity recovery bounds are invalid")
        now_value = datetime.now(UTC)
        now = timestamp(now_value)
        next_check = timestamp(now_value + timedelta(seconds=retry_after_seconds))
        with self.store.transaction() as connection:
            task = connection.execute("SELECT * FROM tasks WHERE id=?", (task_id,)).fetchone()
            if task is None:
                raise KeyError(task_id)
            existing = connection.execute(
                "SELECT * FROM task_capacity_waits WHERE task_id=?", (task_id,)
            ).fetchone()
            if existing is not None:
                if existing["requirements_sha256"] != requirements_sha256:
                    raise RuntimeError("Task capacity wait no longer matches its requirements")
                return dict(existing)
            connection.execute(
                "INSERT INTO task_capacity_waits(task_id,requirements_sha256,state,reason_code,"
                "first_observed_at,last_checked_at,next_check_at,recovery_attempts,"
                "max_recovery_attempts,recovery_owner_id,recovery_lease_expires_at,resolved_at,"
                "version) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    task_id,
                    requirements_sha256,
                    "waiting",
                    reason_code,
                    now,
                    now,
                    next_check,
                    0,
                    max_recovery_attempts,
                    None,
                    None,
                    None,
                    1,
                ),
            )
            _event(
                self.store,
                connection,
                kind="executionCapacityWaitStarted",
                entity_type="taskCapacityWait",
                entity_id=task_id,
                project_id=task["project_id"],
                task_id=task_id,
                summary="Task is waiting for recoverable execution capacity",
                payload={"taskID": task_id, "reasonCode": reason_code},
            )
            return dict(
                connection.execute(
                    "SELECT * FROM task_capacity_waits WHERE task_id=?", (task_id,)
                ).fetchone()
            )

    def due_capacity_waits(self, *, now: datetime | None = None) -> list[dict[str, Any]]:
        observed = timestamp(now or datetime.now(UTC))
        with self.store.connect() as connection:
            return [
                dict(row)
                for row in connection.execute(
                    "SELECT * FROM task_capacity_waits WHERE state='waiting' "
                    "AND recovery_attempts<max_recovery_attempts AND next_check_at<=? "
                    "AND (recovery_owner_id IS NULL OR recovery_lease_expires_at<=?) "
                    "ORDER BY next_check_at,task_id",
                    (observed, observed),
                ).fetchall()
            ]

    def claim_recovery_attempt(
        self,
        task_id: str,
        *,
        owner_id: str,
        lease_seconds: float = 30.0,
        force: bool = False,
    ) -> bool:
        if not _SEMANTIC_ID.fullmatch(owner_id):
            raise ValueError("recovery owner must be a bounded semantic ID")
        if not 1 <= lease_seconds <= 300:
            raise ValueError("recovery lease must be between one and 300 seconds")
        now_value = datetime.now(UTC)
        now = timestamp(now_value)
        expires = timestamp(now_value + timedelta(seconds=lease_seconds))
        with self.store.transaction() as connection:
            wait = connection.execute(
                "SELECT * FROM task_capacity_waits WHERE task_id=?", (task_id,)
            ).fetchone()
            if (
                wait is None
                or wait["state"] != "waiting"
                or int(wait["recovery_attempts"]) >= int(wait["max_recovery_attempts"])
                or (not force and str(wait["next_check_at"]) > now)
            ):
                return False
            if (
                wait["recovery_owner_id"] is not None
                and str(wait["recovery_lease_expires_at"]) > now
            ):
                return False
            cursor = connection.execute(
                "UPDATE task_capacity_waits SET recovery_owner_id=?,"
                "recovery_lease_expires_at=?,last_checked_at=?,version=version+1 "
                "WHERE task_id=? AND version=? AND state='waiting'",
                (owner_id, expires, now, task_id, int(wait["version"])),
            )
            return cursor.rowcount == 1

    def record_recovery_attempt(
        self, task_id: str, *, succeeded: bool, owner_id: str
    ) -> dict[str, Any]:
        now_value = datetime.now(UTC)
        now = timestamp(now_value)
        with self.store.transaction() as connection:
            wait = connection.execute(
                "SELECT * FROM task_capacity_waits WHERE task_id=?", (task_id,)
            ).fetchone()
            if (
                wait is None
                or wait["state"] != "waiting"
                or wait["recovery_owner_id"] != owner_id
                or wait["recovery_lease_expires_at"] is None
                or _aware_datetime(str(wait["recovery_lease_expires_at"])) <= now_value
            ):
                raise ValueError("Task has no active execution-capacity wait")
            attempts = int(wait["recovery_attempts"]) + 1
            state = (
                "resolved"
                if succeeded
                else ("escalated" if attempts >= int(wait["max_recovery_attempts"]) else "waiting")
            )
            resolved_at = now if state != "waiting" else None
            next_check = timestamp(
                now_value + timedelta(seconds=min(300.0, 5.0 * (2 ** max(0, attempts - 1))))
            )
            cursor = connection.execute(
                "UPDATE task_capacity_waits SET state=?,last_checked_at=?,recovery_attempts=?,"
                "next_check_at=?,recovery_owner_id=NULL,recovery_lease_expires_at=NULL,"
                "resolved_at=?,version=version+1 WHERE task_id=? AND version=?",
                (
                    state,
                    now,
                    attempts,
                    next_check,
                    resolved_at,
                    task_id,
                    int(wait["version"]),
                ),
            )
            if cursor.rowcount != 1:
                raise RuntimeError("execution-capacity recovery ownership changed")
            task = connection.execute("SELECT * FROM tasks WHERE id=?", (task_id,)).fetchone()
            autonomous = connection.execute(
                "SELECT binding.steer_version,goal.state,goal.steer_version AS current_steer "
                "FROM autonomous_task_bindings binding JOIN autonomous_goals goal "
                "ON goal.id=binding.goal_id WHERE binding.task_id=?",
                (task_id,),
            ).fetchone()
            control_allows_admission = autonomous is None or (
                autonomous["state"] == "running"
                and int(autonomous["steer_version"]) == int(autonomous["current_steer"])
            )
            if succeeded and control_allows_admission and task["state"] == TaskState.BLOCKED.value:
                connection.execute(
                    "UPDATE tasks SET state=?,version=version+1,updated_at=?,failure_reason=NULL "
                    "WHERE id=? AND state=?",
                    (TaskState.READY.value, now, task_id, TaskState.BLOCKED.value),
                )
            _event(
                self.store,
                connection,
                kind=(
                    "executionCapacityRecovered"
                    if succeeded
                    else "executionCapacityRecoveryDeferred"
                ),
                entity_type="taskCapacityWait",
                entity_id=task_id,
                project_id=task["project_id"],
                task_id=task_id,
                summary=(
                    "Task execution capacity recovered"
                    if succeeded
                    else "Task execution capacity remains unavailable"
                ),
                payload={"taskID": task_id, "state": state, "attempt": attempts},
                severity=EventSeverity.INFO if succeeded else EventSeverity.WARNING,
            )
            return dict(
                connection.execute(
                    "SELECT * FROM task_capacity_waits WHERE task_id=?", (task_id,)
                ).fetchone()
            )


class ExecutionPlaneRecovery(Protocol):
    async def recover_capacity(
        self,
        *,
        task_id: str,
        requirements: TaskRequirements,
        rejection_codes: tuple[str, ...],
    ) -> bool: ...


@dataclass(frozen=True, slots=True)
class NodeRuntimeProbeResult:
    worker_ids: tuple[str, ...]
    authenticated: EvidenceState
    authorized: EvidenceState
    platform_approval: PlatformApprovalState
    reachable: EvidenceState
    runtime_available: EvidenceState
    healthy: EvidenceState
    capacity_available: EvidenceState
    protocol_version: str | None = None
    runtime_identity: str | None = None
    reason_codes: tuple[str, ...] = ()


class TailscaleCLITransportDiscovery:
    """Read-only fixed-argv Tailscale discovery; it never mutates overlay configuration."""

    def __init__(self, executable: str = "tailscale", *, timeout_seconds: float = 5.0) -> None:
        candidate = shutil.which(executable) if os.sep not in executable else executable
        if candidate is None:
            raise ValueError("Tailscale CLI is unavailable")
        resolved = Path(candidate).expanduser().resolve()
        if not resolved.is_file() or not os.access(resolved, os.X_OK):
            raise ValueError("Tailscale CLI must be an executable regular file")
        if not 1 <= timeout_seconds <= 30:
            raise ValueError("Tailscale discovery timeout must be between one and 30 seconds")
        self.executable = str(resolved)
        self.timeout_seconds = float(timeout_seconds)

    async def inspect(self, binding: Mapping[str, Any]) -> TransportPeerObservation:
        if binding.get("transport_provider") != "tailscale":
            raise ValueError("Tailscale discovery received a different transport binding")
        expected = binding.get("peer_identity_sha256")
        if not isinstance(expected, str) or not _SHA256.fullmatch(expected):
            raise ValueError("Tailscale binding peer fingerprint is invalid")
        environment = {
            key: os.environ[key] for key in ("HOME", "PATH", "TMPDIR") if key in os.environ
        }
        process = await asyncio.create_subprocess_exec(
            self.executable,
            "status",
            "--json",
            stdin=asyncio.subprocess.DEVNULL,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            env=environment,
        )
        try:
            stdout, _stderr = await asyncio.wait_for(
                process.communicate(), timeout=self.timeout_seconds
            )
        except TimeoutError:
            process.kill()
            await process.wait()
            raise RuntimeError("Tailscale discovery exceeded its deadline") from None
        if process.returncode != 0:
            raise RuntimeError("Tailscale discovery failed")
        if len(stdout) > _MAX_TAILSCALE_STATUS_BYTES:
            raise RuntimeError("Tailscale discovery output exceeded its limit")
        try:
            payload = json.loads(stdout)
        except (UnicodeDecodeError, ValueError) as error:
            raise RuntimeError("Tailscale discovery returned malformed JSON") from error
        if not isinstance(payload, Mapping):
            raise RuntimeError("Tailscale discovery response is not an object")
        for observation in parse_tailscale_status(payload):
            if observation.identity_sha256 == expected:
                return observation
        raise LookupError("operator-pinned Tailscale peer was not observed")


@dataclass(frozen=True, slots=True)
class LocalWorkerProbeBinding:
    endpoint_ref: str
    node_id: str
    worker_ids: tuple[str, ...]
    adapter: LocalWorkerAdapter
    authorized: EvidenceState
    platform_approval: PlatformApprovalState
    capacity_observer: Callable[[], Awaitable[EvidenceState]] | None = None

    def __post_init__(self) -> None:
        if not _SEMANTIC_ID.fullmatch(self.endpoint_ref) or not _SEMANTIC_ID.fullmatch(
            self.node_id
        ):
            raise ValueError("Local Worker probe references must be bounded semantic IDs")
        if not self.worker_ids or len(set(self.worker_ids)) != len(self.worker_ids):
            raise ValueError("Local Worker probe must bind unique Worker identities")
        if any(not _SEMANTIC_ID.fullmatch(worker_id) for worker_id in self.worker_ids):
            raise ValueError("Local Worker probe Worker identities must be bounded")


class LocalWorkerFabricRuntimeProbe:
    """Authenticated process-local Local Worker probe with exact durable identity checks."""

    def __init__(self, bindings: Iterable[LocalWorkerProbeBinding]) -> None:
        values = tuple(bindings)
        self._bindings = {binding.endpoint_ref: binding for binding in values}
        if len(self._bindings) != len(values):
            raise ValueError("Local Worker endpoint references must be unique")

    async def observe(self, binding: Mapping[str, Any]) -> NodeRuntimeProbeResult:
        endpoint_ref = binding.get("endpoint_ref")
        node_id = binding.get("node_id")
        configured = self._bindings.get(str(endpoint_ref))
        if configured is None or configured.node_id != node_id:
            return self._unavailable((), "runtimeBindingUnavailable")
        try:
            await configured.adapter.negotiate_job_contract()
        except WorkerJobLaunchRejected as error:
            if error.status_code in {401, 403}:
                return NodeRuntimeProbeResult(
                    worker_ids=configured.worker_ids,
                    authenticated=EvidenceState.NO,
                    authorized=configured.authorized,
                    platform_approval=configured.platform_approval,
                    reachable=EvidenceState.YES,
                    runtime_available=EvidenceState.UNKNOWN,
                    healthy=EvidenceState.UNKNOWN,
                    capacity_available=EvidenceState.UNKNOWN,
                    reason_codes=("runtimeAuthenticationRejected",),
                )
            if error.status_code in {404, 502, 503, 504}:
                return NodeRuntimeProbeResult(
                    worker_ids=configured.worker_ids,
                    authenticated=EvidenceState.UNKNOWN,
                    authorized=configured.authorized,
                    platform_approval=configured.platform_approval,
                    reachable=EvidenceState.YES,
                    runtime_available=EvidenceState.NO,
                    healthy=EvidenceState.NO,
                    capacity_available=EvidenceState.UNKNOWN,
                    reason_codes=("runtimeOriginUnavailable",),
                )
            return self._unavailable(configured.worker_ids, "runtimeHealthRejected")
        except WorkerProtocolError:
            return NodeRuntimeProbeResult(
                worker_ids=configured.worker_ids,
                authenticated=EvidenceState.YES,
                authorized=configured.authorized,
                platform_approval=configured.platform_approval,
                reachable=EvidenceState.YES,
                runtime_available=EvidenceState.YES,
                healthy=EvidenceState.NO,
                capacity_available=EvidenceState.UNKNOWN,
                reason_codes=("runtimeProtocolInvalid",),
            )
        except Exception:  # noqa: BLE001 - a probe converts transport failures into UNKNOWN facts
            return self._unavailable(configured.worker_ids, "runtimeProbeUnavailable")
        if configured.adapter.protocol_version != 2 or configured.adapter.node_id is None:
            return NodeRuntimeProbeResult(
                worker_ids=configured.worker_ids,
                authenticated=EvidenceState.YES,
                authorized=configured.authorized,
                platform_approval=configured.platform_approval,
                reachable=EvidenceState.YES,
                runtime_available=EvidenceState.YES,
                healthy=EvidenceState.UNKNOWN,
                capacity_available=EvidenceState.UNKNOWN,
                protocol_version=str(configured.adapter.protocol_version),
                reason_codes=("runtimeIdentityUnavailable",),
            )
        if configured.adapter.node_id != configured.node_id:
            return NodeRuntimeProbeResult(
                worker_ids=configured.worker_ids,
                authenticated=EvidenceState.YES,
                authorized=configured.authorized,
                platform_approval=configured.platform_approval,
                reachable=EvidenceState.YES,
                runtime_available=EvidenceState.YES,
                healthy=EvidenceState.NO,
                capacity_available=EvidenceState.UNKNOWN,
                protocol_version=str(configured.adapter.protocol_version),
                reason_codes=("runtimeNodeIdentityMismatch",),
            )
        capacity = (
            await configured.capacity_observer()
            if configured.capacity_observer is not None
            else EvidenceState.UNKNOWN
        )
        runtime_identity = ":".join(
            value
            for value in (
                configured.adapter.adapter_instance_id,
                configured.adapter.runtime_instance_id,
            )
            if value is not None
        )
        return NodeRuntimeProbeResult(
            worker_ids=configured.worker_ids,
            authenticated=EvidenceState.YES,
            authorized=configured.authorized,
            platform_approval=configured.platform_approval,
            reachable=EvidenceState.YES,
            runtime_available=EvidenceState.YES,
            healthy=EvidenceState.YES,
            capacity_available=capacity,
            protocol_version=str(configured.adapter.protocol_version),
            runtime_identity=runtime_identity,
            reason_codes=(() if capacity is not EvidenceState.UNKNOWN else ("capacityUnknown",)),
        )

    @staticmethod
    def _unavailable(worker_ids: tuple[str, ...], reason_code: str) -> NodeRuntimeProbeResult:
        return NodeRuntimeProbeResult(
            worker_ids=worker_ids,
            authenticated=EvidenceState.UNKNOWN,
            authorized=EvidenceState.UNKNOWN,
            platform_approval=PlatformApprovalState.UNKNOWN,
            reachable=EvidenceState.NO,
            runtime_available=EvidenceState.UNKNOWN,
            healthy=EvidenceState.UNKNOWN,
            capacity_available=EvidenceState.UNKNOWN,
            reason_codes=(reason_code,),
        )


class PrivateTransportDiscovery(Protocol):
    async def inspect(self, binding: Mapping[str, Any]) -> TransportPeerObservation: ...


class FabricRuntimeProbe(Protocol):
    async def observe(self, binding: Mapping[str, Any]) -> NodeRuntimeProbeResult: ...


class TypedFabricRuntimeBootstrap(Protocol):
    descriptor: FabricRuntimeBootstrapDescriptor

    async def start_runtime(
        self, request: FabricRuntimeStartRequest
    ) -> FabricRuntimeStartResult: ...


class HTTPSFabricRuntimeBootstrap:
    """Fixed-operation authenticated client for an operator-installed Fabric service broker."""

    _PATH = "/v1/fabric/runtime/start"

    def __init__(
        self,
        base_url: str,
        *,
        bearer_token: str,
        broker_authority_id: str,
        broker_registry_id: str,
        service_profile_revision: int,
        service_profile_sha256: str,
        transport: httpx.AsyncBaseTransport | None = None,
        timeout_seconds: float = 15.0,
    ) -> None:
        url = httpx.URL(base_url)
        if (
            url.scheme not in {"http", "https"}
            or not url.host
            or bool(url.username)
            or bool(url.password)
            or url.query
            or url.fragment
            or url.path not in {"", "/"}
        ):
            raise ValueError("Fabric broker endpoint must be an origin without credentials")
        loopback = url.host.lower() in {"127.0.0.1", "::1", "localhost"}
        if url.scheme != "https" and not loopback:
            raise ValueError("non-loopback Fabric broker endpoints require HTTPS")
        if not 32 <= len(bearer_token) <= 512 or any(
            character.isspace() or ord(character) < 0x21 for character in bearer_token
        ):
            raise ValueError("Fabric broker bearer must be a bounded process-local credential")
        if not 1 <= timeout_seconds <= 300:
            raise ValueError("Fabric broker timeout must be between one and 300 seconds")
        normalized = str(url.copy_with(path="/"))
        instance_hash = hashlib.sha256(normalized.encode("utf-8")).hexdigest()[:32]
        self.base_url = normalized
        self.bearer_token = bearer_token
        self.transport = transport
        self.timeout_seconds = float(timeout_seconds)
        self.descriptor = FabricRuntimeBootstrapDescriptor(
            adapter_type="https-fabric-runtime-bootstrap-v1",
            adapter_instance_id=f"fabric-bootstrap-{instance_hash}",
            broker_authority_id=broker_authority_id,
            broker_registry_id=broker_registry_id,
            service_profile_revision=service_profile_revision,
            service_profile_sha256=service_profile_sha256,
            max_operation_seconds=self.timeout_seconds,
        )

    async def start_runtime(self, request: FabricRuntimeStartRequest) -> FabricRuntimeStartResult:
        remaining = (request.deadline_at - datetime.now(UTC)).total_seconds()
        timeout = min(self.timeout_seconds, remaining) if remaining > 0 else self.timeout_seconds
        headers = {
            "accept": "application/json",
            "authorization": f"Bearer {self.bearer_token}",
            "content-type": "application/json",
        }
        async with httpx.AsyncClient(
            base_url=self.base_url,
            headers=headers,
            follow_redirects=False,
            trust_env=False,
            timeout=httpx.Timeout(timeout),
            transport=self.transport,
        ) as client:
            message = client.build_request("POST", self._PATH, json=request.to_protocol())
            response: httpx.Response | None = None
            try:
                async with asyncio.timeout(timeout):
                    response = await client.send(message, stream=True)
                    content = bytearray()
                    async for chunk in response.aiter_bytes(chunk_size=16 * 1024):
                        if len(content) + len(chunk) > _MAX_BOOTSTRAP_RESPONSE_BYTES:
                            raise RuntimeError("Fabric broker response exceeded its limit")
                        content.extend(chunk)
            finally:
                if response is not None:
                    await response.aclose()
        if response is None:
            raise RuntimeError("Fabric broker returned no response")
        try:
            payload = json.loads(content)
        except (UnicodeDecodeError, ValueError) as error:
            raise RuntimeError("Fabric broker returned malformed JSON") from error
        if not isinstance(payload, Mapping):
            raise RuntimeError("Fabric broker response is not an object")
        try:
            result = FabricRuntimeStartResult.from_protocol(payload)
        except ValueError as error:
            raise RuntimeError("Fabric broker returned an invalid typed receipt") from error
        if response.status_code < 200 or response.status_code >= 300:
            if result.accepted:
                raise RuntimeError("Fabric broker accepted receipt used a failure status")
        elif not result.accepted and result.state not in {"startPending", "outcomeUnknown"}:
            raise RuntimeError("Fabric broker rejection receipt used a success status")
        return result


class ExecutionPlaneRecoveryCoordinator:
    """Bounded reconnect-first recovery; bootstrap is a typed, optional last resort."""

    _RECOVERABLE = frozenset(
        {
            "NODE_UNAVAILABLE",
            "RUNTIME_NOT_DISCOVERED",
            "RUNTIME_UNREACHABLE",
            "RUNTIME_NOT_AVAILABLE",
            "RUNTIME_UNHEALTHY",
            "RUNTIME_CAPACITY_UNAVAILABLE",
            "RUNTIME_OBSERVATION_STALE",
            "RUNTIME_EXECUTABILITY_UNKNOWN",
            "WORKER_CONTRACT_UNAVAILABLE",
        }
    )

    def __init__(
        self,
        *,
        repository: ExecutionPlaneRepository,
        discovery: PrivateTransportDiscovery,
        probe: FabricRuntimeProbe,
        recovery_owner_id: str,
        bootstrap: TypedFabricRuntimeBootstrap | None = None,
        on_capacity_verified: Callable[[Mapping[str, Any], NodeRuntimeProbeResult], Awaitable[None]]
        | None = None,
        observation_ttl_seconds: float = 30.0,
    ) -> None:
        if not 1 <= observation_ttl_seconds <= 300:
            raise ValueError("execution observation TTL must be between 1 and 300 seconds")
        if not _SEMANTIC_ID.fullmatch(recovery_owner_id):
            raise ValueError("execution-plane recovery owner must be a bounded semantic ID")
        if bootstrap is not None and not isinstance(
            bootstrap.descriptor, FabricRuntimeBootstrapDescriptor
        ):
            raise ValueError("bootstrap must expose a validated Fabric runtime descriptor")
        self.repository = repository
        self.discovery = discovery
        self.probe = probe
        self.recovery_owner_id = recovery_owner_id
        self.bootstrap = bootstrap
        self.on_capacity_verified = on_capacity_verified
        self.observation_ttl_seconds = observation_ttl_seconds

    async def recover_capacity(
        self,
        *,
        task_id: str,
        requirements: TaskRequirements,
        rejection_codes: tuple[str, ...],
    ) -> bool:
        if not set(rejection_codes).intersection(self._RECOVERABLE):
            return False
        requirements_sha256 = requirements_digest(requirements)
        recovered = False
        for binding in self.repository.bindings(enabled_only=True):
            try:
                peer = await self.discovery.inspect(binding)
            except Exception:
                continue
            if peer.identity_sha256 != binding["peer_identity_sha256"]:
                continue
            probe_result: NodeRuntimeProbeResult | None = None
            if peer.online is EvidenceState.YES:
                try:
                    probe_result = await self.probe.observe(binding)
                except Exception:
                    probe_result = None
            if (
                (
                    probe_result is None
                    or probe_result.reachable is not EvidenceState.YES
                    or probe_result.runtime_available is not EvidenceState.YES
                )
                and self.bootstrap is not None
                and peer.online is EvidenceState.YES
            ):
                descriptor = self.bootstrap.descriptor
                try:
                    unresolved_request = self.repository.unresolved_runtime_start(
                        binding_id=str(binding["id"]),
                        task_id=task_id,
                        requirements_sha256=requirements_sha256,
                    )
                except (RuntimeError, ValueError):
                    continue
                if unresolved_request is not None and (
                    unresolved_request.broker_authority_id != descriptor.broker_authority_id
                    or unresolved_request.broker_registry_id != descriptor.broker_registry_id
                    or unresolved_request.service_profile_revision
                    != descriptor.service_profile_revision
                    or unresolved_request.service_profile_sha256
                    != descriptor.service_profile_sha256
                ):
                    continue
                generation = self.repository.claim_binding_recovery(
                    str(binding["id"]),
                    owner_id=self.recovery_owner_id,
                    lease_seconds=min(300.0, descriptor.max_operation_seconds + 10.0),
                )
                if generation is None:
                    continue
                request: FabricRuntimeStartRequest | None = None
                try:
                    request = unresolved_request or self.repository.begin_runtime_start(
                        binding_id=str(binding["id"]),
                        task_id=task_id,
                        requirements_sha256=requirements_sha256,
                        bootstrap_descriptor=descriptor,
                        owner_id=self.recovery_owner_id,
                        lease_generation=generation,
                        deadline_seconds=descriptor.max_operation_seconds,
                    )
                    try:
                        result = await asyncio.wait_for(
                            self.bootstrap.start_runtime(request),
                            timeout=descriptor.max_operation_seconds,
                        )
                    except Exception:
                        self.repository.record_runtime_start_outcome_unknown(
                            request,
                            reason_code="bootstrapOutcomeUnknown",
                            lease_owner_id=self.recovery_owner_id,
                            lease_generation=generation,
                        )
                        continue
                    self.repository.record_runtime_start_result(
                        request,
                        result,
                        lease_owner_id=self.recovery_owner_id,
                        lease_generation=generation,
                    )
                    if result.state in {"rejectedPreStart", "failed"}:
                        continue
                    try:
                        probe_result = await self.probe.observe(binding)
                    except Exception:
                        probe_result = None
                    if (
                        probe_result is not None
                        and probe_result.reachable is EvidenceState.YES
                        and probe_result.runtime_available is EvidenceState.YES
                        and probe_result.healthy is EvidenceState.YES
                    ):
                        self.repository.record_runtime_start_verified(
                            request,
                            lease_owner_id=self.recovery_owner_id,
                            lease_generation=generation,
                        )
                except (RuntimeError, ValueError):
                    probe_result = None
                finally:
                    self.repository.release_binding_recovery(
                        str(binding["id"]),
                        owner_id=self.recovery_owner_id,
                        generation=generation,
                    )
            if probe_result is None:
                continue
            observed_at = datetime.now(UTC)
            for worker_id in probe_result.worker_ids:
                observation = WorkerExecutionObservation(
                    worker_id=worker_id,
                    node_id=str(binding["node_id"]),
                    binding_id=str(binding["id"]),
                    discovered=EvidenceState.YES,
                    configured=EvidenceState.YES,
                    authenticated=probe_result.authenticated,
                    authorized=probe_result.authorized,
                    platform_approval=probe_result.platform_approval,
                    reachable=probe_result.reachable,
                    runtime_available=probe_result.runtime_available,
                    healthy=probe_result.healthy,
                    capacity_available=probe_result.capacity_available,
                    observed_at=observed_at,
                    valid_until=observed_at + timedelta(seconds=self.observation_ttl_seconds),
                    protocol_version=probe_result.protocol_version,
                    runtime_identity=probe_result.runtime_identity,
                    reason_codes=probe_result.reason_codes,
                )
                self.repository.record_worker_observation(observation)
                recovered = observation.evaluate().executable or recovered
            if recovered and self.on_capacity_verified is not None:
                await self.on_capacity_verified(binding, probe_result)
        return recovered


def _aware_datetime(value: str) -> datetime:
    observed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if observed.tzinfo is None:
        raise ValueError("execution-plane timestamps must be timezone-aware")
    return observed.astimezone(UTC)


def execution_plane_recovery_applicable(rejection_codes: Iterable[str]) -> bool:
    return bool(set(rejection_codes).intersection(ExecutionPlaneRecoveryCoordinator._RECOVERABLE))


def requirements_digest(requirements: TaskRequirements) -> str:
    value = {
        "requiredCapabilities": sorted(requirements.required_capabilities),
        "requiredCapabilityParameters": [
            claim.to_protocol() for claim in requirements.required_capability_parameters
        ],
        "localOnly": requirements.local_only,
        "privacySensitive": requirements.privacy_sensitive,
        "codeWriteRequired": requirements.code_write_required,
        "permissionClass": requirements.permission_class.value,
        "explicitWorkerID": requirements.explicit_worker_override,
    }
    return hashlib.sha256(compact_json(value).encode("utf-8")).hexdigest()
