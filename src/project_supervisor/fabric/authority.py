from __future__ import annotations

import hashlib
import json
import math
import re
import uuid
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

from project_supervisor.domain import EventSeverity, PermissionClass
from project_supervisor.store import StateStore, compact_json, timestamp

from .execution_plane import PlatformApprovalState
from .persistence import _event

AUTHORIZATION_ENVELOPE_VERSION = "authorization-envelope/v1"
_SEMANTIC_ID = re.compile(r"^[A-Za-z][A-Za-z0-9._:-]{0,159}$")
_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_PERMISSION_ORDER = {
    PermissionClass.GREEN: 0,
    PermissionClass.YELLOW: 1,
    PermissionClass.RED: 2,
}


def _identities(values: frozenset[str], label: str) -> tuple[str, ...]:
    if len(values) > 256 or any(not _SEMANTIC_ID.fullmatch(value) for value in values):
        raise ValueError(f"{label} must contain bounded semantic identities")
    return tuple(sorted(values))


def _bounded_budget(value: Mapping[str, int | float]) -> dict[str, int | float]:
    if len(value) > 32:
        raise ValueError("authorization budget has too many dimensions")
    result: dict[str, int | float] = {}
    for key, amount in value.items():
        if not _SEMANTIC_ID.fullmatch(key):
            raise ValueError("authorization budget keys must be semantic IDs")
        if isinstance(amount, bool) or not isinstance(amount, (int, float)):
            raise ValueError("authorization budget values must be numeric")
        if not math.isfinite(amount) or amount < 0:
            raise ValueError("authorization budget values must be finite and non-negative")
        result[key] = amount
    return dict(sorted(result.items()))


@dataclass(frozen=True, slots=True)
class AuthorizationEnvelope:
    authorization_id: str
    project_id: str
    root_task_id: str
    subject: str
    permission_ceiling: PermissionClass
    version: int = 1
    inheritance_policy: str = "narrowOnly"
    capabilities: frozenset[str] = frozenset()
    actions: frozenset[str] = frozenset()
    resources: frozenset[str] = frozenset()
    data_refs: frozenset[str] = frozenset()
    allowed_providers: frozenset[str] = frozenset()
    allowed_worker_classes: frozenset[str] = frozenset()
    allowed_data_classes: frozenset[str] = frozenset()
    denied_data_classes: frozenset[str] = frozenset()
    allowed_action_classes: frozenset[str] = frozenset()
    denied_action_classes: frozenset[str] = frozenset()
    budget: Mapping[str, int | float] = field(default_factory=dict)
    issued_by: str = "supervisor"
    issued_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    expires_at: datetime | None = None
    goal_id: str | None = None
    parent_id: str | None = None
    user_approval_ref: str | None = None
    user_approval_state: str = "notRequired"
    platform_approval_required: bool = False
    platform_approval_state: PlatformApprovalState = PlatformApprovalState.NOT_REQUIRED
    schema_version: str = AUTHORIZATION_ENVELOPE_VERSION

    def __post_init__(self) -> None:
        for value in (
            self.authorization_id,
            self.project_id,
            self.root_task_id,
            self.subject,
            self.issued_by,
        ):
            if not _SEMANTIC_ID.fullmatch(value):
                raise ValueError("authorization identities must be bounded semantic IDs")
        if self.goal_id is not None and not _SEMANTIC_ID.fullmatch(self.goal_id):
            raise ValueError("goal_id must be a bounded semantic ID")
        if self.parent_id is not None and not _SEMANTIC_ID.fullmatch(self.parent_id):
            raise ValueError("parent_id must be a bounded semantic ID")
        if self.schema_version != AUTHORIZATION_ENVELOPE_VERSION:
            raise ValueError("unsupported authorization envelope version")
        if isinstance(self.version, bool) or self.version < 1:
            raise ValueError("authorization version must be positive")
        if self.inheritance_policy not in {"narrowOnly", "noInheritance"}:
            raise ValueError("unsupported authorization inheritance policy")
        if self.issued_at.tzinfo is None or (
            self.expires_at is not None and self.expires_at.tzinfo is None
        ):
            raise ValueError("authorization timestamps must be timezone-aware")
        if self.expires_at is not None and self.expires_at <= self.issued_at:
            raise ValueError("authorization expiry must follow issue time")
        object.__setattr__(self, "issued_at", self.issued_at.astimezone(UTC).replace(microsecond=0))
        if self.expires_at is not None:
            object.__setattr__(
                self,
                "expires_at",
                self.expires_at.astimezone(UTC).replace(microsecond=0),
            )
        if self.user_approval_state not in {
            "notRequired",
            "pending",
            "approved",
            "rejected",
            "expired",
            "unknown",
        }:
            raise ValueError("invalid user approval state")
        if self.platform_approval_required and (
            self.platform_approval_state is PlatformApprovalState.NOT_REQUIRED
        ):
            raise ValueError("required platform approval cannot be marked not-required")
        _identities(self.capabilities, "capabilities")
        _identities(self.actions, "actions")
        _identities(self.resources, "resources")
        _identities(self.data_refs, "data_refs")
        _identities(self.allowed_providers, "allowed_providers")
        _identities(self.allowed_worker_classes, "allowed_worker_classes")
        _identities(self.allowed_data_classes, "allowed_data_classes")
        _identities(self.denied_data_classes, "denied_data_classes")
        _identities(self.allowed_action_classes, "allowed_action_classes")
        _identities(self.denied_action_classes, "denied_action_classes")
        if self.allowed_data_classes.intersection(self.denied_data_classes):
            raise ValueError("authorization data allow and deny sets cannot overlap")
        if self.allowed_action_classes.intersection(self.denied_action_classes):
            raise ValueError("authorization action allow and deny sets cannot overlap")
        _bounded_budget(self.budget)

    @property
    def definition(self) -> dict[str, Any]:
        return {
            "schemaVersion": self.schema_version,
            "authorizationID": self.authorization_id,
            "version": self.version,
            "parentID": self.parent_id,
            "projectID": self.project_id,
            "goalID": self.goal_id,
            "rootTaskID": self.root_task_id,
            "subject": self.subject,
            "permissionCeiling": self.permission_ceiling.value,
            "inheritancePolicy": self.inheritance_policy,
            "capabilities": list(_identities(self.capabilities, "capabilities")),
            "actions": list(_identities(self.actions, "actions")),
            "resources": list(_identities(self.resources, "resources")),
            "dataRefs": list(_identities(self.data_refs, "data_refs")),
            "allowedProviders": list(_identities(self.allowed_providers, "allowed_providers")),
            "allowedWorkerClasses": list(
                _identities(self.allowed_worker_classes, "allowed_worker_classes")
            ),
            "allowedDataClasses": list(
                _identities(self.allowed_data_classes, "allowed_data_classes")
            ),
            "deniedDataClasses": list(_identities(self.denied_data_classes, "denied_data_classes")),
            "allowedActionClasses": list(
                _identities(self.allowed_action_classes, "allowed_action_classes")
            ),
            "deniedActionClasses": list(
                _identities(self.denied_action_classes, "denied_action_classes")
            ),
            "budget": _bounded_budget(self.budget),
            "userApprovalRef": self.user_approval_ref,
            "userApprovalState": self.user_approval_state,
            "platformApprovalRequired": self.platform_approval_required,
            "platformApprovalState": self.platform_approval_state.value,
            "issuedBy": self.issued_by,
            "issuedAt": timestamp(self.issued_at),
            "expiresAt": timestamp(self.expires_at) if self.expires_at is not None else None,
        }

    @property
    def digest(self) -> str:
        return hashlib.sha256(compact_json(self.definition).encode("utf-8")).hexdigest()

    def permits_dispatch(self, *, now: datetime | None = None) -> bool:
        observed = now or datetime.now(UTC)
        if self.expires_at is not None and observed >= self.expires_at:
            return False
        if self.user_approval_state not in {"notRequired", "approved"}:
            return False
        return self.platform_approval_state in {
            PlatformApprovalState.NOT_REQUIRED,
            PlatformApprovalState.APPROVED,
        }

    def derive(
        self,
        *,
        authorization_id: str,
        subject: str,
        permission_ceiling: PermissionClass | None = None,
        capabilities: frozenset[str] | None = None,
        actions: frozenset[str] | None = None,
        resources: frozenset[str] | None = None,
        data_refs: frozenset[str] | None = None,
        allowed_providers: frozenset[str] | None = None,
        allowed_worker_classes: frozenset[str] | None = None,
        allowed_data_classes: frozenset[str] | None = None,
        denied_data_classes: frozenset[str] | None = None,
        allowed_action_classes: frozenset[str] | None = None,
        denied_action_classes: frozenset[str] | None = None,
        budget: Mapping[str, int | float] | None = None,
        expires_at: datetime | None = None,
        issued_by: str = "supervisor",
    ) -> AuthorizationEnvelope:
        if self.inheritance_policy == "noInheritance":
            raise ValueError("authorization does not permit child inheritance")
        child_permission = permission_ceiling or self.permission_ceiling
        child_capabilities = self.capabilities if capabilities is None else capabilities
        child_actions = self.actions if actions is None else actions
        child_resources = self.resources if resources is None else resources
        child_data = self.data_refs if data_refs is None else data_refs
        child_providers = self.allowed_providers if allowed_providers is None else allowed_providers
        child_worker_classes = (
            self.allowed_worker_classes
            if allowed_worker_classes is None
            else allowed_worker_classes
        )
        child_data_classes = (
            self.allowed_data_classes if allowed_data_classes is None else allowed_data_classes
        )
        child_denied_data = (
            self.denied_data_classes if denied_data_classes is None else denied_data_classes
        )
        child_action_classes = (
            self.allowed_action_classes
            if allowed_action_classes is None
            else allowed_action_classes
        )
        child_denied_actions = (
            self.denied_action_classes if denied_action_classes is None else denied_action_classes
        )
        child_budget = dict(self.budget) if budget is None else _bounded_budget(budget)
        if _PERMISSION_ORDER[child_permission] > _PERMISSION_ORDER[self.permission_ceiling]:
            raise ValueError("child authorization cannot widen its permission ceiling")
        for label, child, parent in (
            ("capabilities", child_capabilities, self.capabilities),
            ("actions", child_actions, self.actions),
            ("resources", child_resources, self.resources),
            ("data_refs", child_data, self.data_refs),
            ("allowed_providers", child_providers, self.allowed_providers),
            ("allowed_worker_classes", child_worker_classes, self.allowed_worker_classes),
            ("allowed_data_classes", child_data_classes, self.allowed_data_classes),
            ("allowed_action_classes", child_action_classes, self.allowed_action_classes),
        ):
            if not child.issubset(parent):
                raise ValueError(f"child authorization cannot widen {label}")
        if not self.denied_data_classes.issubset(child_denied_data):
            raise ValueError("child authorization cannot remove denied data classes")
        if not self.denied_action_classes.issubset(child_denied_actions):
            raise ValueError("child authorization cannot remove denied action classes")
        for key, amount in child_budget.items():
            if key not in self.budget or amount > self.budget[key]:
                raise ValueError("child authorization cannot widen its budget")
        child_expiry = expires_at if expires_at is not None else self.expires_at
        if self.expires_at is not None and (child_expiry is None or child_expiry > self.expires_at):
            raise ValueError("child authorization cannot outlive its parent")
        return AuthorizationEnvelope(
            authorization_id=authorization_id,
            parent_id=self.authorization_id,
            project_id=self.project_id,
            goal_id=self.goal_id,
            root_task_id=self.root_task_id,
            subject=subject,
            permission_ceiling=child_permission,
            version=1,
            inheritance_policy="narrowOnly",
            capabilities=child_capabilities,
            actions=child_actions,
            resources=child_resources,
            data_refs=child_data,
            allowed_providers=child_providers,
            allowed_worker_classes=child_worker_classes,
            allowed_data_classes=child_data_classes,
            denied_data_classes=child_denied_data,
            allowed_action_classes=child_action_classes,
            denied_action_classes=child_denied_actions,
            budget=child_budget,
            user_approval_ref=self.user_approval_ref,
            user_approval_state=self.user_approval_state,
            platform_approval_required=self.platform_approval_required,
            platform_approval_state=self.platform_approval_state,
            issued_by=issued_by,
            issued_at=datetime.now(UTC),
            expires_at=child_expiry,
        )


class AuthorizationRepository:
    def __init__(self, store: StateStore) -> None:
        self.store = store

    def issue(self, envelope: AuthorizationEnvelope) -> dict[str, Any]:
        with self.store.transaction() as connection:
            return self.issue_in_transaction(connection, envelope)

    def issue_in_transaction(
        self, connection: Any, envelope: AuthorizationEnvelope
    ) -> dict[str, Any]:
        task = connection.execute(
            "SELECT project_id FROM tasks WHERE id=?", (envelope.root_task_id,)
        ).fetchone()
        if task is None or task["project_id"] != envelope.project_id:
            raise ValueError("authorization root Task does not belong to its project")
        if envelope.goal_id is not None:
            goal = connection.execute(
                "SELECT project_id FROM autonomous_goals WHERE id=?", (envelope.goal_id,)
            ).fetchone()
            if goal is None or goal["project_id"] != envelope.project_id:
                raise ValueError("authorization Goal does not belong to its project")
        if envelope.parent_id is not None:
            parent = self._load(connection, envelope.parent_id)
            self._validate_child(parent, envelope)
        existing = connection.execute(
            "SELECT * FROM authorization_envelopes WHERE id=?", (envelope.authorization_id,)
        ).fetchone()
        if existing is not None:
            if existing["definition_sha256"] != envelope.digest:
                raise RuntimeError("authorization ID replay conflicts with immutable definition")
            return self._project(existing)
        now = timestamp()
        connection.execute(
            "INSERT INTO authorization_envelopes(id,version,parent_id,project_id,goal_id,"
            "root_task_id,subject,schema_version,inheritance_policy,permission_ceiling,"
            "capabilities_json,actions_json,resources_json,data_refs_json,"
            "allowed_providers_json,allowed_worker_classes_json,allowed_data_classes_json,"
            "denied_data_classes_json,allowed_action_classes_json,denied_action_classes_json,"
            "budget_json,user_approval_ref,user_approval_state,platform_approval_required,"
            "platform_approval_state,issued_by,issued_at,expires_at,definition_sha256,created_at) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                envelope.authorization_id,
                envelope.version,
                envelope.parent_id,
                envelope.project_id,
                envelope.goal_id,
                envelope.root_task_id,
                envelope.subject,
                envelope.schema_version,
                envelope.inheritance_policy,
                envelope.permission_ceiling.value,
                compact_json(sorted(envelope.capabilities)),
                compact_json(sorted(envelope.actions)),
                compact_json(sorted(envelope.resources)),
                compact_json(sorted(envelope.data_refs)),
                compact_json(sorted(envelope.allowed_providers)),
                compact_json(sorted(envelope.allowed_worker_classes)),
                compact_json(sorted(envelope.allowed_data_classes)),
                compact_json(sorted(envelope.denied_data_classes)),
                compact_json(sorted(envelope.allowed_action_classes)),
                compact_json(sorted(envelope.denied_action_classes)),
                compact_json(_bounded_budget(envelope.budget)),
                envelope.user_approval_ref,
                envelope.user_approval_state,
                int(envelope.platform_approval_required),
                envelope.platform_approval_state.value,
                envelope.issued_by,
                timestamp(envelope.issued_at),
                timestamp(envelope.expires_at) if envelope.expires_at is not None else None,
                envelope.digest,
                now,
            ),
        )
        _event(
            self.store,
            connection,
            kind=("authorizationInherited" if envelope.parent_id else "authorizationCreated"),
            entity_type="authorizationEnvelope",
            entity_id=envelope.authorization_id,
            project_id=envelope.project_id,
            task_id=envelope.root_task_id,
            summary="Immutable execution authorization created",
            payload={
                "authorizationID": envelope.authorization_id,
                "version": envelope.version,
                "parentID": envelope.parent_id,
                "subject": envelope.subject,
                "permissionCeiling": envelope.permission_ceiling.value,
                "definitionSHA256": envelope.digest,
                "platformApprovalState": envelope.platform_approval_state.value,
            },
            actor=envelope.issued_by,
        )
        return self._project(
            connection.execute(
                "SELECT * FROM authorization_envelopes WHERE id=?",
                (envelope.authorization_id,),
            ).fetchone()
        )

    def get(self, authorization_id: str) -> dict[str, Any]:
        with self.store.connect() as connection:
            row = connection.execute(
                "SELECT * FROM authorization_envelopes WHERE id=?", (authorization_id,)
            ).fetchone()
            if row is None:
                raise KeyError(authorization_id)
            return self._project(row)

    def bind_task(self, authorization_id: str, task_id: str, *, actor: str = "runtime") -> str:
        with self.store.transaction() as connection:
            envelope = self._load(connection, authorization_id)
            task = connection.execute("SELECT * FROM tasks WHERE id=?", (task_id,)).fetchone()
            if task is None:
                raise KeyError(task_id)
            if task["project_id"] != envelope.project_id:
                raise ValueError("authorization and Task projects do not match")
            if task_id != envelope.root_task_id:
                descendant = connection.execute(
                    "WITH RECURSIVE ancestry(task_id,parent_task_id) AS ("
                    "SELECT task_id,parent_task_id FROM autonomous_task_bindings WHERE task_id=? "
                    "UNION ALL SELECT binding.task_id,binding.parent_task_id "
                    "FROM autonomous_task_bindings binding JOIN ancestry "
                    "ON binding.task_id=ancestry.parent_task_id) "
                    "SELECT 1 FROM ancestry WHERE task_id=? LIMIT 1",
                    (task_id, envelope.root_task_id),
                ).fetchone()
                if descendant is None:
                    raise PermissionError(
                        "authorization cannot bind an unrelated Task in the same project"
                    )
            if not envelope.permits_dispatch():
                raise PermissionError("authorization is not executable")
            existing = connection.execute(
                "SELECT id FROM authorization_envelope_bindings "
                "WHERE envelope_id=? AND task_id=? AND run_id IS NULL AND binding_kind='task'",
                (authorization_id, task_id),
            ).fetchone()
            if existing is not None:
                return str(existing["id"])
            identity = f"authorization-binding-{uuid.uuid4()}"
            connection.execute(
                "INSERT INTO authorization_envelope_bindings(id,envelope_id,task_id,run_id,"
                "binding_kind,bound_by,created_at) VALUES (?,?,?,NULL,'task',?,?)",
                (identity, authorization_id, task_id, actor, timestamp()),
            )
            _event(
                self.store,
                connection,
                kind="authorizationBound",
                entity_type="authorizationEnvelopeBinding",
                entity_id=identity,
                project_id=task["project_id"],
                task_id=task_id,
                summary="Authorization bound to canonical Task",
                payload={"authorizationID": authorization_id, "taskID": task_id},
                actor=actor,
            )
            return identity

    @staticmethod
    def _load(connection: Any, authorization_id: str) -> AuthorizationEnvelope:
        row = connection.execute(
            "SELECT * FROM authorization_envelopes WHERE id=?", (authorization_id,)
        ).fetchone()
        if row is None:
            raise KeyError(authorization_id)
        return AuthorizationRepository._from_row(row)

    @staticmethod
    def _validate_child(parent: AuthorizationEnvelope, child: AuthorizationEnvelope) -> None:
        expected = parent.derive(
            authorization_id=child.authorization_id,
            subject=child.subject,
            permission_ceiling=child.permission_ceiling,
            capabilities=child.capabilities,
            actions=child.actions,
            resources=child.resources,
            data_refs=child.data_refs,
            allowed_providers=child.allowed_providers,
            allowed_worker_classes=child.allowed_worker_classes,
            allowed_data_classes=child.allowed_data_classes,
            denied_data_classes=child.denied_data_classes,
            allowed_action_classes=child.allowed_action_classes,
            denied_action_classes=child.denied_action_classes,
            budget=child.budget,
            expires_at=child.expires_at,
            issued_by=child.issued_by,
        )
        if (
            child.project_id != expected.project_id
            or child.goal_id != expected.goal_id
            or child.root_task_id != expected.root_task_id
            or child.user_approval_state != expected.user_approval_state
            or child.platform_approval_state != expected.platform_approval_state
            or child.platform_approval_required != expected.platform_approval_required
        ):
            raise ValueError("child authorization cannot change inherited authority facts")

    @staticmethod
    def _from_row(row: Any) -> AuthorizationEnvelope:
        return AuthorizationEnvelope(
            authorization_id=str(row["id"]),
            parent_id=row["parent_id"],
            project_id=str(row["project_id"]),
            goal_id=row["goal_id"],
            root_task_id=str(row["root_task_id"]),
            subject=str(row["subject"]),
            permission_ceiling=PermissionClass(row["permission_ceiling"]),
            version=int(row["version"]),
            inheritance_policy=str(row["inheritance_policy"]),
            capabilities=frozenset(json.loads(row["capabilities_json"])),
            actions=frozenset(json.loads(row["actions_json"])),
            resources=frozenset(json.loads(row["resources_json"])),
            data_refs=frozenset(json.loads(row["data_refs_json"])),
            allowed_providers=frozenset(json.loads(row["allowed_providers_json"])),
            allowed_worker_classes=frozenset(json.loads(row["allowed_worker_classes_json"])),
            allowed_data_classes=frozenset(json.loads(row["allowed_data_classes_json"])),
            denied_data_classes=frozenset(json.loads(row["denied_data_classes_json"])),
            allowed_action_classes=frozenset(json.loads(row["allowed_action_classes_json"])),
            denied_action_classes=frozenset(json.loads(row["denied_action_classes_json"])),
            budget=json.loads(row["budget_json"]),
            user_approval_ref=row["user_approval_ref"],
            user_approval_state=str(row["user_approval_state"]),
            platform_approval_required=bool(row["platform_approval_required"]),
            platform_approval_state=PlatformApprovalState(row["platform_approval_state"]),
            issued_by=str(row["issued_by"]),
            issued_at=datetime.fromisoformat(str(row["issued_at"]).replace("Z", "+00:00")),
            expires_at=(
                datetime.fromisoformat(str(row["expires_at"]).replace("Z", "+00:00"))
                if row["expires_at"] is not None
                else None
            ),
        )

    @staticmethod
    def _project(row: Any) -> dict[str, Any]:
        return {
            "authorizationID": row["id"],
            "version": int(row["version"]),
            "parentID": row["parent_id"],
            "projectID": row["project_id"],
            "goalID": row["goal_id"],
            "rootTaskID": row["root_task_id"],
            "subject": row["subject"],
            "schemaVersion": row["schema_version"],
            "permissionCeiling": row["permission_ceiling"],
            "inheritancePolicy": row["inheritance_policy"],
            "capabilities": json.loads(row["capabilities_json"]),
            "actions": json.loads(row["actions_json"]),
            "resources": json.loads(row["resources_json"]),
            "dataRefs": json.loads(row["data_refs_json"]),
            "allowedProviders": json.loads(row["allowed_providers_json"]),
            "allowedWorkerClasses": json.loads(row["allowed_worker_classes_json"]),
            "allowedDataClasses": json.loads(row["allowed_data_classes_json"]),
            "deniedDataClasses": json.loads(row["denied_data_classes_json"]),
            "allowedActionClasses": json.loads(row["allowed_action_classes_json"]),
            "deniedActionClasses": json.loads(row["denied_action_classes_json"]),
            "budget": json.loads(row["budget_json"]),
            "userApprovalState": row["user_approval_state"],
            "platformApprovalRequired": bool(row["platform_approval_required"]),
            "platformApprovalState": row["platform_approval_state"],
            "issuedAt": row["issued_at"],
            "expiresAt": row["expires_at"],
            "definitionSHA256": row["definition_sha256"],
        }


class DataProvenanceRepository:
    def __init__(self, store: StateStore) -> None:
        self.store = store

    def register_packet(
        self,
        *,
        project_id: str,
        source_kind: str,
        source_ref: str,
        content_sha256: str,
        classification: str,
        contains_credentials: str,
        created_by: str,
        task_id: str | None = None,
        byte_count: int | None = None,
        packet_id: str | None = None,
    ) -> dict[str, Any]:
        if not _SHA256.fullmatch(content_sha256):
            raise ValueError("packet content identity must be a lowercase SHA-256 digest")
        if classification not in {"public", "internal", "confidential", "restricted", "unknown"}:
            raise ValueError("unsupported evidence classification")
        if contains_credentials not in {"yes", "no", "unknown"}:
            raise ValueError("credential evidence must be tri-state")
        if byte_count is not None and byte_count < 0:
            raise ValueError("packet byte_count cannot be negative")
        for value in (source_kind, source_ref, created_by):
            if not _SEMANTIC_ID.fullmatch(value):
                raise ValueError("packet provenance values must be semantic IDs")
        identity = packet_id or f"evidence-packet-{uuid.uuid4()}"
        with self.store.transaction() as connection:
            if (
                connection.execute("SELECT 1 FROM projects WHERE id=?", (project_id,)).fetchone()
                is None
            ):
                raise KeyError(project_id)
            connection.execute(
                "INSERT INTO data_evidence_packets(id,project_id,task_id,source_kind,source_ref,"
                "content_sha256,classification,byte_count,contains_credentials,created_by,"
                "created_at) VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                (
                    identity,
                    project_id,
                    task_id,
                    source_kind,
                    source_ref,
                    content_sha256,
                    classification,
                    byte_count,
                    contains_credentials,
                    created_by,
                    timestamp(),
                ),
            )
            _event(
                self.store,
                connection,
                kind="dataPacketRegistered",
                entity_type="dataEvidencePacket",
                entity_id=identity,
                project_id=project_id,
                task_id=task_id,
                summary="Bounded evidence identity registered",
                payload={
                    "packetID": identity,
                    "classification": classification,
                    "containsCredentials": contains_credentials,
                    "byteCount": byte_count,
                },
                actor=created_by,
            )
            return dict(
                connection.execute(
                    "SELECT * FROM data_evidence_packets WHERE id=?", (identity,)
                ).fetchone()
            )

    def record_movement(
        self,
        *,
        packet_id: str,
        envelope_id: str,
        destination_kind: str,
        destination_ref: str,
        purpose: str,
        disclosure_state: str,
        recorded_by: str,
        run_id: str | None = None,
        transport_ref: str | None = None,
        movement_id: str | None = None,
    ) -> dict[str, Any]:
        for value in (destination_kind, destination_ref, purpose, recorded_by):
            if not _SEMANTIC_ID.fullmatch(value):
                raise ValueError("movement provenance values must be semantic IDs")
        if disclosure_state not in {"notDisclosed", "disclosed", "unknown"}:
            raise ValueError("disclosure_state must be tri-state")
        identity = movement_id or f"data-movement-{uuid.uuid4()}"
        with self.store.transaction() as connection:
            packet = connection.execute(
                "SELECT * FROM data_evidence_packets WHERE id=?", (packet_id,)
            ).fetchone()
            envelope = connection.execute(
                "SELECT * FROM authorization_envelopes WHERE id=?", (envelope_id,)
            ).fetchone()
            if packet is None or envelope is None:
                raise KeyError(packet_id if packet is None else envelope_id)
            if packet["project_id"] != envelope["project_id"]:
                raise ValueError("data movement cannot cross authorization project scope")
            if packet_id not in set(json.loads(envelope["data_refs_json"])):
                raise PermissionError("authorization does not permit this evidence packet")
            allowed_data = set(json.loads(envelope["allowed_data_classes_json"]))
            denied_data = set(json.loads(envelope["denied_data_classes_json"]))
            if (
                packet["classification"] not in allowed_data
                or packet["classification"] in denied_data
            ):
                raise PermissionError("authorization does not permit this evidence data class")
            if packet["contains_credentials"] != "no" and disclosure_state == "disclosed":
                raise PermissionError("credential-bearing or unknown evidence cannot be disclosed")
            now = timestamp()
            connection.execute(
                "INSERT INTO data_movement_events(id,packet_id,envelope_id,run_id,"
                "destination_kind,destination_ref,purpose,disclosure_state,transport_ref,"
                "occurred_at,recorded_by,created_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    identity,
                    packet_id,
                    envelope_id,
                    run_id,
                    destination_kind,
                    destination_ref,
                    purpose,
                    disclosure_state,
                    transport_ref,
                    now,
                    recorded_by,
                    now,
                ),
            )
            _event(
                self.store,
                connection,
                kind="dataMovementObserved",
                entity_type="dataMovement",
                entity_id=identity,
                project_id=packet["project_id"],
                task_id=packet["task_id"],
                run_id=run_id,
                summary="Evidence movement state observed",
                payload={
                    "movementID": identity,
                    "packetID": packet_id,
                    "destinationKind": destination_kind,
                    "purpose": purpose,
                    "disclosureState": disclosure_state,
                },
                actor=recorded_by,
                severity=(
                    EventSeverity.NOTICE if disclosure_state == "disclosed" else EventSeverity.INFO
                ),
            )
            return dict(
                connection.execute(
                    "SELECT * FROM data_movement_events WHERE id=?", (identity,)
                ).fetchone()
            )
