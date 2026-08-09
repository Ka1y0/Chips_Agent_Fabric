from __future__ import annotations

import hashlib
import hmac
import json
import re
import secrets
import sqlite3
import threading
import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from .domain import (
    EventSeverity,
    FailureClass,
    Harness,
    ModelDescriptor,
    NodeState,
    ProjectPhase,
    Provider,
    ResourceState,
    RoutingDecision,
    RunState,
    TaskRecord,
    TaskState,
    TelemetryValue,
    WorkerSnapshot,
    WorkerState,
)
from .hybrid import ExecutionHistoryRecord
from .protocols.capabilities import CapabilityGrant, GrantState
from .protocols.identity import NodePublicIdentity
from .state_machine import RUN_TRANSITIONS, TASK_TRANSITIONS


def timestamp(value: datetime | None = None) -> str:
    return (value or datetime.now(UTC)).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def compact_json(value: Any) -> str:
    return json.dumps(value, separators=(",", ":"), sort_keys=True)


def redact_sensitive(value: Any) -> Any:
    sensitive = {
        "authorization",
        "cookie",
        "api_key",
        "apikey",
        "access_token",
        "refresh_token",
        "token",
        "signature",
    }
    if isinstance(value, dict):
        return {
            key: "[REDACTED]" if key.lower() in sensitive else redact_sensitive(item)
            for key, item in value.items()
        }
    if isinstance(value, list):
        return [redact_sensitive(item) for item in value]
    if isinstance(value, str):
        patterns = (
            re.compile(r"(?i)(authorization\s*[:=]\s*bearer\s+)[^\s,;]+"),
            re.compile(
                r"(?i)((?:token|api[_-]?key|access[_-]?token|refresh[_-]?token|password|secret)"
                r"\s*[:=]\s*)[^\s,;]+"
            ),
            re.compile(r"\bsk-[A-Za-z0-9_-]{12,}\b"),
        )
        result = value
        for pattern in patterns:
            result = pattern.sub(
                lambda match: f"{match.group(1)}[REDACTED]" if match.groups() else "[REDACTED]",
                result,
            )
        return result
    return value


class StateStore:
    """Synchronous SQLite boundary with serialized writes and transactional event emission."""

    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._write_lock = threading.RLock()
        self._initialize()

    def connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.path, timeout=10, isolation_level=None)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys = ON")
        connection.execute("PRAGMA busy_timeout = 10000")
        return connection

    def _initialize(self) -> None:
        migrations = Path(__file__).with_name("migrations")
        with self._write_lock, self.connect() as connection:
            connection.execute("PRAGMA journal_mode = WAL")
            connection.execute("PRAGMA synchronous = FULL")
            connection.execute(
                "CREATE TABLE IF NOT EXISTS schema_migrations "
                "(version TEXT PRIMARY KEY, applied_at TEXT NOT NULL)"
            )
            applied = {
                row["version"]
                for row in connection.execute("SELECT version FROM schema_migrations").fetchall()
            }
            for migration in sorted(migrations.glob("*.sql")):
                if migration.stem in applied:
                    continue
                connection.executescript(migration.read_text(encoding="utf-8"))
                connection.execute(
                    "INSERT INTO schema_migrations(version, applied_at) VALUES (?, ?)",
                    (migration.stem, timestamp()),
                )

    @contextmanager
    def transaction(self) -> Iterator[sqlite3.Connection]:
        with self._write_lock, self.connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                yield connection
            except Exception:
                connection.rollback()
                raise
            else:
                connection.commit()

    def create_project(
        self, *, project_id: str, name: str, root_path: str, goal: str
    ) -> dict[str, Any]:
        now = timestamp()
        with self.transaction() as connection:
            connection.execute(
                "INSERT INTO projects(id,name,root_path,goal,phase,created_at,updated_at) "
                "VALUES (?,?,?,?,?,?,?)",
                (
                    project_id,
                    name,
                    root_path,
                    goal,
                    ProjectPhase.INITIALIZING.value,
                    now,
                    now,
                ),
            )
            self._append_event(
                connection,
                kind="projectCreated",
                severity=EventSeverity.NOTICE,
                entity_type="project",
                entity_id=project_id,
                project_id=project_id,
                summary=f"Project {name} created",
                payload={"phase": ProjectPhase.INITIALIZING.value},
            )
        return self.get_project(project_id)

    def get_project(self, project_id: str) -> dict[str, Any]:
        with self.connect() as connection:
            row = connection.execute(
                "SELECT * FROM projects WHERE id = ?", (project_id,)
            ).fetchone()
        if row is None:
            raise KeyError(project_id)
        return dict(row)

    def create_task(self, task: TaskRecord, reference: str) -> dict[str, Any]:
        now = timestamp(task.created_at)
        with self.transaction() as connection:
            connection.execute(
                """
                INSERT INTO tasks(
                    id,project_id,reference,title,description,state,topology,priority,labels_json,
                    required_capabilities_json,permission_class,approval_state,minimum_context_tokens,
                    privacy_sensitive,code_write_required,panel_size,preferred_workers_json,
                    attempt_count,version,created_at,updated_at
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    task.id,
                    task.project_id,
                    reference,
                    task.title,
                    task.description,
                    task.state.value,
                    task.topology.value,
                    task.priority,
                    compact_json(sorted(label.value for label in task.requirements.labels)),
                    compact_json(sorted(task.requirements.required_capabilities)),
                    task.requirements.permission_class.value,
                    task.requirements.approval_state.value,
                    task.requirements.minimum_context_tokens,
                    int(task.requirements.privacy_sensitive),
                    int(task.requirements.code_write_required),
                    task.requirements.panel_size,
                    compact_json(list(task.requirements.preferred_workers)),
                    task.attempt_count,
                    1,
                    now,
                    timestamp(task.updated_at),
                ),
            )
            self._append_event(
                connection,
                kind="taskCreated",
                severity=EventSeverity.INFO,
                entity_type="task",
                entity_id=task.id,
                project_id=task.project_id,
                task_id=task.id,
                summary=f"Task {reference} created",
                payload={"state": task.state.value, "topology": task.topology.value},
            )
        return self.get_task(task.id)

    def get_task(self, task_id: str) -> dict[str, Any]:
        with self.connect() as connection:
            row = connection.execute("SELECT * FROM tasks WHERE id = ?", (task_id,)).fetchone()
        if row is None:
            raise KeyError(task_id)
        return dict(row)

    def list_tasks(self, project_id: str | None = None) -> list[dict[str, Any]]:
        query = "SELECT * FROM tasks"
        parameters: tuple[Any, ...] = ()
        if project_id is not None:
            query += " WHERE project_id = ?"
            parameters = (project_id,)
        query += " ORDER BY updated_at DESC, id ASC"
        with self.connect() as connection:
            return [dict(row) for row in connection.execute(query, parameters).fetchall()]

    def add_task_dependency(self, task_id: str, depends_on_task_id: str) -> None:
        """Persist a dependency edge after proving both tasks belong to one project."""

        with self.transaction() as connection:
            rows = connection.execute(
                "SELECT id,project_id FROM tasks WHERE id IN (?,?)",
                (task_id, depends_on_task_id),
            ).fetchall()
            by_id = {row["id"]: row for row in rows}
            if task_id not in by_id:
                raise KeyError(task_id)
            if depends_on_task_id not in by_id:
                raise KeyError(depends_on_task_id)
            if by_id[task_id]["project_id"] != by_id[depends_on_task_id]["project_id"]:
                raise ValueError("task dependencies cannot cross project boundaries")
            connection.execute(
                "INSERT OR IGNORE INTO task_dependencies(task_id,depends_on_task_id) VALUES (?,?)",
                (task_id, depends_on_task_id),
            )

    def unsatisfied_dependencies(self, task_id: str) -> list[str]:
        with self.connect() as connection:
            return [
                row["depends_on_task_id"]
                for row in connection.execute(
                    "SELECT d.depends_on_task_id FROM task_dependencies d "
                    "JOIN tasks prerequisite ON prerequisite.id=d.depends_on_task_id "
                    "WHERE d.task_id=? AND prerequisite.state<>? "
                    "ORDER BY d.depends_on_task_id",
                    (task_id, TaskState.SUCCEEDED.value),
                ).fetchall()
            ]

    def add_acceptance_criterion(
        self,
        *,
        project_id: str,
        kind: str,
        description: str,
        command: list[str] | None = None,
        expected: Any = None,
        criterion_id: str | None = None,
    ) -> str:
        criterion_id = criterion_id or f"criterion-{uuid.uuid4()}"
        now = timestamp()
        with self.transaction() as connection:
            connection.execute(
                "INSERT INTO acceptance_criteria(id,project_id,kind,description,command_json,"
                "expected_json,state,created_at,updated_at) VALUES (?,?,?,?,?,?,?,?,?)",
                (
                    criterion_id,
                    project_id,
                    kind,
                    description,
                    compact_json(command) if command is not None else None,
                    compact_json(expected) if expected is not None else None,
                    "pending",
                    now,
                    now,
                ),
            )
        return criterion_id

    def list_acceptance_criteria(self, project_id: str) -> list[dict[str, Any]]:
        with self.connect() as connection:
            return [
                dict(row)
                for row in connection.execute(
                    "SELECT * FROM acceptance_criteria WHERE project_id=? ORDER BY created_at,id",
                    (project_id,),
                ).fetchall()
            ]

    def record_verification(
        self,
        *,
        task_id: str,
        kind: str,
        passed: bool,
        evidence: dict[str, Any],
        verifier: str,
        criterion_id: str | None = None,
        command: list[str] | None = None,
        exit_code: int | None = None,
    ) -> str:
        verification_id = f"verify-{uuid.uuid4()}"
        safe_evidence = redact_sensitive(evidence)
        with self.transaction() as connection:
            task = connection.execute(
                "SELECT project_id FROM tasks WHERE id=?", (task_id,)
            ).fetchone()
            if task is None:
                raise KeyError(task_id)
            connection.execute(
                "INSERT INTO verifications(id,task_id,criterion_id,kind,command_json,exit_code,"
                "passed,evidence_json,verifier,created_at) VALUES (?,?,?,?,?,?,?,?,?,?)",
                (
                    verification_id,
                    task_id,
                    criterion_id,
                    kind,
                    compact_json(command) if command is not None else None,
                    exit_code,
                    int(passed),
                    compact_json(safe_evidence),
                    verifier,
                    timestamp(),
                ),
            )
            if criterion_id is not None:
                connection.execute(
                    "UPDATE acceptance_criteria SET state=?,evidence_json=?,updated_at=? "
                    "WHERE id=?",
                    (
                        "passed" if passed else "failed",
                        compact_json(safe_evidence),
                        timestamp(),
                        criterion_id,
                    ),
                )
            self._append_event(
                connection,
                kind="verificationCompleted",
                severity=EventSeverity.NOTICE if passed else EventSeverity.WARNING,
                entity_type="task",
                entity_id=task_id,
                project_id=task["project_id"],
                task_id=task_id,
                summary=f"Deterministic verification {'passed' if passed else 'failed'}",
                payload={"verificationID": verification_id, "criterionID": criterion_id},
                actor=verifier,
            )
        return verification_id

    def list_verifications(self, task_id: str) -> list[dict[str, Any]]:
        with self.connect() as connection:
            return [
                dict(row)
                for row in connection.execute(
                    "SELECT * FROM verifications WHERE task_id=? ORDER BY created_at,id",
                    (task_id,),
                ).fetchall()
            ]

    def upsert_node(
        self,
        *,
        node_id: str,
        hostname: str,
        display_name: str,
        role: str,
        state: NodeState,
        operating_system: str | None = None,
        hardware_summary: str | None = None,
        private_endpoint: str | None = None,
        capabilities: set[str] | None = None,
    ) -> None:
        now = timestamp()
        with self.transaction() as connection:
            connection.execute(
                """
                INSERT INTO nodes(
                    id,hostname,display_name,role,state,operating_system,hardware_summary,
                    private_endpoint,capabilities_json,last_heartbeat_at,created_at,updated_at
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
                ON CONFLICT(id) DO UPDATE SET
                    hostname=excluded.hostname,display_name=excluded.display_name,role=excluded.role,
                    state=excluded.state,operating_system=excluded.operating_system,
                    hardware_summary=excluded.hardware_summary,
                    private_endpoint=excluded.private_endpoint,
                    capabilities_json=excluded.capabilities_json,
                    last_heartbeat_at=excluded.last_heartbeat_at,updated_at=excluded.updated_at
                """,
                (
                    node_id,
                    hostname,
                    display_name,
                    role,
                    state.value,
                    operating_system,
                    hardware_summary,
                    private_endpoint,
                    compact_json(sorted(capabilities or set())),
                    now if state is NodeState.ONLINE else None,
                    now,
                    now,
                ),
            )
            self._append_event(
                connection,
                kind="nodeRegistered",
                severity=EventSeverity.INFO,
                entity_type="node",
                entity_id=node_id,
                summary=f"Node {node_id} is {state.value}",
                payload={"state": state.value, "role": role},
                actor="registry",
            )

    def upsert_model(self, model: ModelDescriptor) -> str:
        identity = f"{model.provider.value}:{model.identifier}:{model.context_variant or ''}"
        model_id = f"mdl-{hashlib.sha256(identity.encode()).hexdigest()[:16]}"
        with self.transaction() as connection:
            connection.execute(
                """
                INSERT INTO models(
                    id,provider,identifier,display_name,context_variant,context_window_tokens
                ) VALUES (?,?,?,?,?,?)
                ON CONFLICT(id) DO UPDATE SET
                    display_name=excluded.display_name,
                    context_window_tokens=excluded.context_window_tokens
                """,
                (
                    model_id,
                    model.provider.value,
                    model.identifier,
                    model.display_name,
                    model.context_variant,
                    model.context_window_tokens,
                ),
            )
        return model_id

    def upsert_worker(self, worker: WorkerSnapshot, *, harness_version: str | None = None) -> None:
        model_id = self.upsert_model(worker.model)
        now = timestamp()
        with self.transaction() as connection:
            connection.execute(
                """
                INSERT INTO workers(
                    id,node_id,harness,provider,model_id,state,resource_state,capabilities_json,
                    code_write_allowed,privacy_allowed,quality_score,reliability_score,
                    expected_latency_seconds,monetary_cost_score,harness_version,last_heartbeat_at,
                    created_at,updated_at
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                ON CONFLICT(id) DO UPDATE SET
                    node_id=excluded.node_id,harness=excluded.harness,provider=excluded.provider,
                    model_id=excluded.model_id,state=excluded.state,
                    resource_state=excluded.resource_state,
                    capabilities_json=excluded.capabilities_json,
                    code_write_allowed=excluded.code_write_allowed,
                    privacy_allowed=excluded.privacy_allowed,quality_score=excluded.quality_score,
                    reliability_score=excluded.reliability_score,
                    expected_latency_seconds=excluded.expected_latency_seconds,
                    monetary_cost_score=excluded.monetary_cost_score,
                    harness_version=excluded.harness_version,
                    last_heartbeat_at=excluded.last_heartbeat_at,updated_at=excluded.updated_at
                """,
                (
                    worker.id,
                    worker.node_id,
                    worker.harness.value,
                    worker.provider.value,
                    model_id,
                    worker.state.value,
                    worker.resource_state.value,
                    compact_json(sorted(worker.capabilities)),
                    int(worker.code_write_allowed),
                    int(worker.privacy_allowed),
                    worker.quality_score,
                    worker.reliability_score,
                    worker.expected_latency_seconds,
                    worker.monetary_cost_score,
                    harness_version,
                    now,
                    now,
                    now,
                ),
            )
            self._append_event(
                connection,
                kind="workerRegistered",
                severity=EventSeverity.INFO,
                entity_type="worker",
                entity_id=worker.id,
                worker_id=worker.id,
                summary=f"Worker {worker.id} registered",
                payload={
                    "harness": worker.harness.value,
                    "provider": worker.provider.value,
                    "modelID": model_id,
                    "state": worker.state.value,
                },
                actor="registry",
            )

    def list_nodes(self) -> list[dict[str, Any]]:
        with self.connect() as connection:
            return [
                dict(row)
                for row in connection.execute("SELECT * FROM nodes ORDER BY id ASC").fetchall()
            ]

    def list_workers(self) -> list[dict[str, Any]]:
        with self.connect() as connection:
            rows = connection.execute(
                """
                SELECT w.*,m.identifier AS model_identifier,m.display_name AS model_display_name,
                       m.context_variant,m.context_window_tokens,n.hostname,n.state AS node_state
                FROM workers w
                JOIN nodes n ON n.id=w.node_id
                LEFT JOIN models m ON m.id=w.model_id
                ORDER BY w.id ASC
                """
            ).fetchall()
        return [dict(row) for row in rows]

    def set_worker_state(
        self,
        worker_id: str,
        state: WorkerState,
        *,
        resource_state: ResourceState | None = None,
        model_id: str | None = None,
        actor: str = "runtime",
    ) -> None:
        with self.transaction() as connection:
            worker = connection.execute("SELECT * FROM workers WHERE id=?", (worker_id,)).fetchone()
            if worker is None:
                raise KeyError(worker_id)
            now = timestamp()
            connection.execute(
                "UPDATE workers SET state=?,resource_state=COALESCE(?,resource_state),"
                "model_id=COALESCE(?,model_id),last_heartbeat_at=?,updated_at=? WHERE id=?",
                (
                    state.value,
                    resource_state.value if resource_state else None,
                    model_id,
                    now,
                    now,
                    worker_id,
                ),
            )
            self._append_event(
                connection,
                kind="workerStateChanged",
                severity=EventSeverity.NOTICE,
                entity_type="worker",
                entity_id=worker_id,
                worker_id=worker_id,
                summary=f"Worker state {worker['state']} -> {state.value}",
                payload={"from": worker["state"], "to": state.value},
                actor=actor,
            )

    def worker_snapshots(self) -> list[WorkerSnapshot]:
        snapshots: list[WorkerSnapshot] = []
        for row in self.list_workers():
            provider = Provider(row["provider"])
            snapshots.append(
                WorkerSnapshot(
                    id=row["id"],
                    node_id=row["node_id"],
                    harness=Harness(row["harness"]),
                    provider=provider,
                    model=ModelDescriptor(
                        identifier=row["model_identifier"] or "unknown",
                        display_name=row["model_display_name"] or "Unknown",
                        provider=provider,
                        context_variant=row["context_variant"],
                        context_window_tokens=row["context_window_tokens"],
                    ),
                    state=WorkerState(row["state"]),
                    node_state=NodeState(row["node_state"]),
                    resource_state=ResourceState(row["resource_state"]),
                    capabilities=frozenset(json.loads(row["capabilities_json"])),
                    code_write_allowed=bool(row["code_write_allowed"]),
                    privacy_allowed=bool(row["privacy_allowed"]),
                    quality_score=row["quality_score"],
                    reliability_score=row["reliability_score"],
                    expected_latency_seconds=row["expected_latency_seconds"],
                    monetary_cost_score=row["monetary_cost_score"],
                )
            )
        return snapshots

    def record_execution_history(self, record: ExecutionHistoryRecord) -> None:
        """Persist normalized routing telemetry without changing scheduler policy."""

        with self.transaction() as connection:
            connection.execute(
                """
                INSERT INTO execution_history(
                    id,task_id,task_type,worker_id,provider,model,node_id,topology,
                    latency_seconds,succeeded,failure_class,retry_count,input_tokens,
                    output_tokens,cost_usd,review_outcome,human_accepted,recorded_at
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    record.id,
                    record.task_id,
                    record.task_type,
                    record.worker_id,
                    record.provider,
                    record.model,
                    record.node_id,
                    record.topology.value,
                    record.latency_seconds,
                    int(record.succeeded),
                    record.failure_class,
                    record.retry_count,
                    record.input_tokens,
                    record.output_tokens,
                    record.cost_usd,
                    redact_sensitive(record.review_outcome),
                    int(record.human_accepted) if record.human_accepted is not None else None,
                    timestamp(record.recorded_at),
                ),
            )

    def list_execution_history(
        self, *, task_id: str | None = None, worker_id: str | None = None, limit: int = 200
    ) -> list[dict[str, Any]]:
        if limit < 1 or limit > 1000:
            raise ValueError("limit must be between 1 and 1000")
        clauses: list[str] = []
        parameters: list[Any] = []
        if task_id is not None:
            clauses.append("task_id=?")
            parameters.append(task_id)
        if worker_id is not None:
            clauses.append("worker_id=?")
            parameters.append(worker_id)
        query = "SELECT * FROM execution_history"
        if clauses:
            query += " WHERE " + " AND ".join(clauses)
        query += " ORDER BY recorded_at DESC,id ASC LIMIT ?"
        parameters.append(limit)
        with self.connect() as connection:
            return [dict(row) for row in connection.execute(query, parameters).fetchall()]

    def record_capability_grant(self, grant: CapabilityGrant) -> None:
        """Durably record an attributed grant; this method executes no capability."""

        with self.transaction() as connection:
            task = connection.execute(
                "SELECT project_id FROM tasks WHERE id=?", (grant.task_id,)
            ).fetchone()
            if task is None:
                raise KeyError(grant.task_id)
            connection.execute(
                """
                INSERT INTO capability_grants(
                    id,capability,subject,requested_by,task_id,issued_by,issued_at,
                    expires_at,state,constraints_json,revoked_at,revoked_by
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    grant.grant_id,
                    grant.capability,
                    grant.subject,
                    grant.requested_by,
                    grant.task_id,
                    grant.issued_by,
                    timestamp(grant.issued_at),
                    timestamp(grant.expires_at) if grant.expires_at else None,
                    grant.state.value,
                    compact_json(redact_sensitive(grant.constraints)),
                    timestamp(grant.revoked_at) if grant.revoked_at else None,
                    grant.revoked_by,
                ),
            )
            self._append_event(
                connection,
                kind="capabilityGrantRecorded",
                severity=EventSeverity.NOTICE,
                entity_type="capabilityGrant",
                entity_id=grant.grant_id,
                project_id=task["project_id"],
                task_id=grant.task_id,
                summary=f"Capability grant {grant.grant_id} recorded",
                payload={
                    "capability": grant.capability,
                    "subject": grant.subject,
                    "state": grant.state.value,
                    "expiresAt": timestamp(grant.expires_at) if grant.expires_at else None,
                },
                actor=grant.issued_by,
            )

    def revoke_capability_grant(self, grant_id: str, *, revoked_by: str) -> None:
        if not revoked_by.strip():
            raise ValueError("revoked_by must not be empty")
        now = timestamp()
        with self.transaction() as connection:
            grant = connection.execute(
                "SELECT * FROM capability_grants WHERE id=?", (grant_id,)
            ).fetchone()
            if grant is None:
                raise KeyError(grant_id)
            if GrantState(grant["state"]) is not GrantState.ACTIVE:
                raise ValueError("only an active capability grant can be revoked")
            connection.execute(
                "UPDATE capability_grants SET state='revoked',revoked_at=?,revoked_by=? WHERE id=?",
                (now, revoked_by, grant_id),
            )
            task = connection.execute(
                "SELECT project_id FROM tasks WHERE id=?", (grant["task_id"],)
            ).fetchone()
            self._append_event(
                connection,
                kind="capabilityGrantRevoked",
                severity=EventSeverity.WARNING,
                entity_type="capabilityGrant",
                entity_id=grant_id,
                project_id=task["project_id"],
                task_id=grant["task_id"],
                summary=f"Capability grant {grant_id} revoked",
                payload={"capability": grant["capability"], "subject": grant["subject"]},
                actor=revoked_by,
            )

    def record_node_public_identity(self, identity: NodePublicIdentity) -> None:
        """Persist public identity metadata only; no private key is accepted."""

        with self.transaction() as connection:
            if (
                connection.execute("SELECT 1 FROM nodes WHERE id=?", (identity.node_id,)).fetchone()
                is None
            ):
                raise KeyError(identity.node_id)
            connection.execute(
                """
                INSERT INTO node_public_identities(
                    node_id,key_id,algorithm,public_key_fingerprint,created_at,rotated_from_key_id
                ) VALUES (?,?,?,?,?,?)
                """,
                (
                    identity.node_id,
                    identity.key_id,
                    identity.algorithm,
                    identity.public_key_fingerprint,
                    timestamp(identity.created_at),
                    identity.rotated_from_key_id,
                ),
            )

    def transition_task(
        self,
        task_id: str,
        target: TaskState,
        *,
        actor: str = "supervisor",
        summary: str | None = None,
        payload: dict[str, Any] | None = None,
        expected_version: int | None = None,
    ) -> dict[str, Any]:
        with self.transaction() as connection:
            row = connection.execute("SELECT * FROM tasks WHERE id = ?", (task_id,)).fetchone()
            if row is None:
                raise KeyError(task_id)
            current = TaskState(row["state"])
            TASK_TRANSITIONS.require(current, target)
            if expected_version is not None and row["version"] != expected_version:
                raise RuntimeError(
                    f"task {task_id} version conflict: expected {expected_version}, "
                    f"got {row['version']}"
                )
            now = timestamp()
            started_at = row["started_at"]
            if target is TaskState.RUNNING and started_at is None:
                started_at = now
            finished_at = row["finished_at"]
            if target in {TaskState.SUCCEEDED, TaskState.FAILED, TaskState.CANCELLED}:
                finished_at = now
            cursor = connection.execute(
                "UPDATE tasks SET state=?,started_at=?,finished_at=?,updated_at=?,"
                "version=version+1 "
                "WHERE id=? AND version=?",
                (target.value, started_at, finished_at, now, task_id, row["version"]),
            )
            if cursor.rowcount != 1:
                raise RuntimeError(f"task {task_id} concurrent update")
            self._append_event(
                connection,
                kind="taskStateChanged",
                severity=EventSeverity.NOTICE,
                entity_type="task",
                entity_id=task_id,
                project_id=row["project_id"],
                task_id=task_id,
                summary=summary or f"Task state {current.value} -> {target.value}",
                payload={"from": current.value, "to": target.value, **(payload or {})},
                actor=actor,
            )
        return self.get_task(task_id)

    def persist_routing_decision(
        self, *, task_id: str, decision: RoutingDecision, actor: str = "scheduler"
    ) -> str:
        decision_id = f"rte-{uuid.uuid4()}"
        now = timestamp()
        with self.transaction() as connection:
            task = connection.execute(
                "SELECT project_id FROM tasks WHERE id = ?", (task_id,)
            ).fetchone()
            if task is None:
                raise KeyError(task_id)
            connection.execute(
                "INSERT INTO routing_decisions(id,task_id,topology,policy_version,"
                "selected_workers_json,explanation_json,created_at) VALUES (?,?,?,?,?,?,?)",
                (
                    decision_id,
                    task_id,
                    decision.topology.value,
                    decision.policy_version,
                    compact_json(list(decision.selected_worker_ids)),
                    compact_json(decision.explanation),
                    now,
                ),
            )
            selected = set(decision.selected_worker_ids)
            rejection_by_worker: dict[str, list[Any]] = {}
            for item in decision.rejected:
                rejection_by_worker.setdefault(item.worker_id, []).append(item)
            for candidate in decision.candidates:
                rejections = rejection_by_worker.pop(candidate.worker_id, [None])
                for rejection in rejections:
                    connection.execute(
                        "INSERT INTO routing_candidates(decision_id,worker_id,selected,score,"
                        "components_json,rejection_code,rejection_detail) VALUES (?,?,?,?,?,?,?)",
                        (
                            decision_id,
                            candidate.worker_id,
                            int(candidate.worker_id in selected),
                            candidate.score,
                            compact_json(candidate.components),
                            rejection.reason_code if rejection else "",
                            rejection.detail if rejection else None,
                        ),
                    )
            for worker_id, rejections in rejection_by_worker.items():
                for rejection in rejections:
                    connection.execute(
                        "INSERT INTO routing_candidates(decision_id,worker_id,selected,score,"
                        "components_json,rejection_code,rejection_detail) VALUES (?,?,?,?,?,?,?)",
                        (
                            decision_id,
                            worker_id,
                            0,
                            None,
                            None,
                            rejection.reason_code,
                            rejection.detail,
                        ),
                    )
            self._append_event(
                connection,
                kind="routingDecisionRecorded",
                severity=EventSeverity.INFO,
                entity_type="task",
                entity_id=task_id,
                project_id=task["project_id"],
                task_id=task_id,
                summary=(
                    "Selected " + ", ".join(decision.selected_worker_ids)
                    if decision.selected_worker_ids
                    else "No eligible worker selected"
                ),
                payload={"decisionID": decision_id, **decision.explanation},
                actor=actor,
            )
        return decision_id

    def create_worker_run(
        self,
        *,
        task_id: str,
        worker_id: str,
        attempt: int,
        timeout_at: datetime | None = None,
    ) -> str:
        run_id = f"run-{uuid.uuid4()}"
        now = timestamp()
        with self.transaction() as connection:
            task = connection.execute("SELECT * FROM tasks WHERE id=?", (task_id,)).fetchone()
            if task is None:
                raise KeyError(task_id)
            worker = connection.execute("SELECT * FROM workers WHERE id=?", (worker_id,)).fetchone()
            if worker is None:
                raise KeyError(worker_id)
            connection.execute(
                """
                INSERT INTO worker_runs(
                    id,task_id,worker_id,state,attempt,timeout_at,created_at,updated_at
                ) VALUES (?,?,?,?,?,?,?,?)
                """,
                (
                    run_id,
                    task_id,
                    worker_id,
                    RunState.STARTING.value,
                    attempt,
                    timestamp(timeout_at) if timeout_at else None,
                    now,
                    now,
                ),
            )
            connection.execute(
                "UPDATE tasks SET attempt_count=MAX(attempt_count,?),updated_at=?,"
                "version=version+1 "
                "WHERE id=?",
                (attempt, now, task_id),
            )
            self._append_event(
                connection,
                kind="workerStarted",
                severity=EventSeverity.NOTICE,
                entity_type="workerRun",
                entity_id=run_id,
                project_id=task["project_id"],
                task_id=task_id,
                worker_id=worker_id,
                run_id=run_id,
                summary=f"Worker {worker_id} starting attempt {attempt}",
                payload={"state": RunState.STARTING.value, "attempt": attempt},
                actor="runtime",
            )
        return run_id

    def transition_worker_run(
        self,
        run_id: str,
        target: RunState,
        *,
        process_id: int | None = None,
        exit_code: int | None = None,
        session_id: str | None = None,
        raw_output_reference: str | None = None,
        failure_class: FailureClass | None = None,
        failure_detail: str | None = None,
        payload: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        with self.transaction() as connection:
            row = connection.execute("SELECT * FROM worker_runs WHERE id=?", (run_id,)).fetchone()
            if row is None:
                raise KeyError(run_id)
            current = RunState(row["state"])
            RUN_TRANSITIONS.require(current, target)
            now = timestamp()
            started_at = row["started_at"]
            if target is RunState.RUNNING and started_at is None:
                started_at = now
            terminal = {
                RunState.COMPLETED,
                RunState.FAILED,
                RunState.CANCELLED,
                RunState.TIMED_OUT,
                RunState.INTERRUPTED,
                RunState.AUTH_REQUIRED,
                RunState.RATE_LIMITED,
            }
            ended_at = now if target in terminal else row["ended_at"]
            connection.execute(
                """
                UPDATE worker_runs SET
                    state=?,process_id=COALESCE(?,process_id),session_id=COALESCE(?,session_id),
                    started_at=?,last_event_at=?,ended_at=?,exit_code=COALESCE(?,exit_code),
                    failure_class=COALESCE(?,failure_class),
                    failure_detail=COALESCE(?,failure_detail),
                    raw_output_reference=COALESCE(?,raw_output_reference),updated_at=?
                WHERE id=?
                """,
                (
                    target.value,
                    process_id,
                    session_id,
                    started_at,
                    now,
                    ended_at,
                    exit_code,
                    failure_class.value if failure_class else None,
                    failure_detail,
                    raw_output_reference,
                    now,
                    run_id,
                ),
            )
            task = connection.execute(
                "SELECT project_id FROM tasks WHERE id=?", (row["task_id"],)
            ).fetchone()
            severity = (
                EventSeverity.NOTICE
                if target in {RunState.RUNNING, RunState.COMPLETED}
                else EventSeverity.WARNING
            )
            kind = {
                RunState.RUNNING: "workerRunning",
                RunState.COMPLETED: "workerCompleted",
                RunState.CANCELLED: "workerCancelled",
                RunState.TIMED_OUT: "workerTimedOut",
                RunState.INTERRUPTED: "workerRunInterrupted",
                RunState.AUTH_REQUIRED: "workerAuthRequired",
                RunState.RATE_LIMITED: "workerRateLimited",
            }.get(target, "workerFailed" if target is RunState.FAILED else "workerStateChanged")
            self._append_event(
                connection,
                kind=kind,
                severity=severity,
                entity_type="workerRun",
                entity_id=run_id,
                project_id=task["project_id"],
                task_id=row["task_id"],
                worker_id=row["worker_id"],
                run_id=run_id,
                summary=f"Worker run {current.value} -> {target.value}",
                payload={"from": current.value, "to": target.value, **(payload or {})},
                actor="runtime",
            )
        return self.get_worker_run(run_id)

    def get_worker_run(self, run_id: str) -> dict[str, Any]:
        with self.connect() as connection:
            row = connection.execute("SELECT * FROM worker_runs WHERE id=?", (run_id,)).fetchone()
        if row is None:
            raise KeyError(run_id)
        return dict(row)

    def list_worker_runs(self, task_id: str | None = None) -> list[dict[str, Any]]:
        query = "SELECT * FROM worker_runs"
        parameters: tuple[Any, ...] = ()
        if task_id is not None:
            query += " WHERE task_id=?"
            parameters = (task_id,)
        query += " ORDER BY attempt ASC,created_at ASC,id ASC"
        with self.connect() as connection:
            return [dict(row) for row in connection.execute(query, parameters).fetchall()]

    def save_worker_result(
        self,
        *,
        run_id: str,
        summary: str,
        changed_files: list[str] | None = None,
        commands_run: list[str] | None = None,
        tests: list[dict[str, Any]] | None = None,
        artifacts: list[str] | None = None,
        commit_hash: str | None = None,
        blockers: list[str] | None = None,
        confidence: float | None = None,
        recommended_next_actions: list[str] | None = None,
    ) -> None:
        with self.transaction() as connection:
            run = connection.execute("SELECT * FROM worker_runs WHERE id=?", (run_id,)).fetchone()
            if run is None:
                raise KeyError(run_id)
            connection.execute(
                """
                INSERT INTO worker_results(
                    run_id,summary,changed_files_json,commands_run_json,tests_json,artifacts_json,
                    commit_hash,blockers_json,confidence,recommended_next_actions_json,created_at
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?)
                ON CONFLICT(run_id) DO UPDATE SET
                    summary=excluded.summary,changed_files_json=excluded.changed_files_json,
                    commands_run_json=excluded.commands_run_json,tests_json=excluded.tests_json,
                    artifacts_json=excluded.artifacts_json,commit_hash=excluded.commit_hash,
                    blockers_json=excluded.blockers_json,confidence=excluded.confidence,
                    recommended_next_actions_json=excluded.recommended_next_actions_json
                """,
                (
                    run_id,
                    summary,
                    compact_json(changed_files or []),
                    compact_json(commands_run or []),
                    compact_json(tests or []),
                    compact_json(artifacts or []),
                    commit_hash,
                    compact_json(blockers or []),
                    confidence,
                    compact_json(recommended_next_actions or []),
                    timestamp(),
                ),
            )

    def record_failure(
        self,
        *,
        classification: FailureClass,
        summary: str,
        retryable: bool,
        project_id: str | None = None,
        task_id: str | None = None,
        run_id: str | None = None,
        detail: str | None = None,
    ) -> str:
        failure_id = f"failure-{uuid.uuid4()}"
        safe_summary = str(redact_sensitive(summary))
        safe_detail = str(redact_sensitive(detail)) if detail else None
        with self.transaction() as connection:
            if project_id is None and task_id is not None:
                task = connection.execute(
                    "SELECT project_id FROM tasks WHERE id=?", (task_id,)
                ).fetchone()
                if task is None:
                    raise KeyError(task_id)
                project_id = task["project_id"]
            connection.execute(
                "INSERT INTO failures(id,project_id,task_id,run_id,classification,summary,detail,"
                "retryable,created_at) VALUES (?,?,?,?,?,?,?,?,?)",
                (
                    failure_id,
                    project_id,
                    task_id,
                    run_id,
                    classification.value,
                    safe_summary,
                    safe_detail,
                    int(retryable),
                    timestamp(),
                ),
            )
            self._append_event(
                connection,
                kind="failureRecorded",
                severity=EventSeverity.WARNING,
                entity_type="workerRun" if run_id else "task",
                entity_id=run_id or task_id or failure_id,
                project_id=project_id,
                task_id=task_id,
                run_id=run_id,
                summary=safe_summary,
                payload={
                    "failureID": failure_id,
                    "classification": classification.value,
                    "retryable": retryable,
                },
                actor="runtime",
            )
        return failure_id

    def request_approval(
        self,
        *,
        project_id: str,
        permission_class: str,
        action_type: str,
        action_payload: dict[str, Any],
        requested_by: str,
        task_id: str | None = None,
        reason: str | None = None,
    ) -> str:
        approval_id = f"approval-{uuid.uuid4()}"
        with self.transaction() as connection:
            connection.execute(
                "INSERT INTO approvals(id,project_id,task_id,permission_class,action_type,"
                "action_payload_json,state,requested_at,requested_by,reason) "
                "VALUES (?,?,?,?,?,?,?,?,?,?)",
                (
                    approval_id,
                    project_id,
                    task_id,
                    permission_class,
                    action_type,
                    compact_json(redact_sensitive(action_payload)),
                    "pending",
                    timestamp(),
                    requested_by,
                    reason,
                ),
            )
            self._append_event(
                connection,
                kind="approvalRequested",
                severity=EventSeverity.WARNING,
                entity_type="approval",
                entity_id=approval_id,
                project_id=project_id,
                task_id=task_id,
                summary=f"Human approval required for {action_type}",
                payload={"approvalID": approval_id, "permissionClass": permission_class},
                actor=requested_by,
            )
        return approval_id

    def resolve_approval(
        self,
        approval_id: str,
        *,
        approved: bool,
        resolved_by: str,
        reason: str | None = None,
    ) -> None:
        with self.transaction() as connection:
            row = connection.execute(
                "SELECT * FROM approvals WHERE id=?", (approval_id,)
            ).fetchone()
            if row is None:
                raise KeyError(approval_id)
            if row["state"] != "pending":
                raise ValueError("approval has already been resolved")
            state = "approved" if approved else "rejected"
            connection.execute(
                "UPDATE approvals SET state=?,resolved_at=?,resolved_by=?,"
                "reason=COALESCE(?,reason) "
                "WHERE id=?",
                (state, timestamp(), resolved_by, reason, approval_id),
            )
            self._append_event(
                connection,
                kind="approvalResolved",
                severity=EventSeverity.NOTICE,
                entity_type="approval",
                entity_id=approval_id,
                project_id=row["project_id"],
                task_id=row["task_id"],
                summary=f"Approval {state}",
                payload={"approvalID": approval_id, "state": state},
                actor=resolved_by,
            )

    def list_routing_decisions(self, task_id: str) -> list[dict[str, Any]]:
        with self.connect() as connection:
            return [
                dict(row)
                for row in connection.execute(
                    "SELECT * FROM routing_decisions WHERE task_id=? ORDER BY created_at,id",
                    (task_id,),
                ).fetchall()
            ]

    def record_usage(
        self,
        *,
        metric: str,
        telemetry: TelemetryValue,
        unit: str,
        task_id: str | None = None,
        run_id: str | None = None,
        worker_id: str | None = None,
        model_id: str | None = None,
    ) -> str:
        usage_id = f"usg-{uuid.uuid4()}"
        with self.transaction() as connection:
            connection.execute(
                """
                INSERT INTO usage_records(
                    id,task_id,run_id,worker_id,model_id,metric,value,unit,confidence,
                    unavailable_reason,recorded_at
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    usage_id,
                    task_id,
                    run_id,
                    worker_id,
                    model_id,
                    metric,
                    telemetry.value,
                    unit,
                    telemetry.confidence.value,
                    telemetry.reason.value if telemetry.reason else None,
                    timestamp(),
                ),
            )
        return usage_id

    def upsert_session(
        self,
        *,
        worker_id: str,
        provider_session_id: str,
        state: str = "active",
        resume_cursor: str | None = None,
        model_id: str | None = None,
    ) -> str:
        identity = f"{worker_id}:{provider_session_id}"
        session_id = f"ses-{hashlib.sha256(identity.encode()).hexdigest()[:20]}"
        now = timestamp()
        with self.transaction() as connection:
            connection.execute(
                """
                INSERT INTO sessions(
                    id,worker_id,provider_session_id,model_id,resume_cursor,state,created_at,updated_at
                ) VALUES (?,?,?,?,?,?,?,?)
                ON CONFLICT(id) DO UPDATE SET
                    model_id=COALESCE(excluded.model_id,sessions.model_id),
                    resume_cursor=COALESCE(excluded.resume_cursor,sessions.resume_cursor),
                    state=excluded.state,updated_at=excluded.updated_at
                """,
                (
                    session_id,
                    worker_id,
                    provider_session_id,
                    model_id,
                    resume_cursor,
                    state,
                    now,
                    now,
                ),
            )
        return session_id

    def record_adapter_event(
        self,
        *,
        run_id: str,
        kind: str,
        payload: dict[str, Any],
        summary: str | None = None,
    ) -> int:
        safe_payload = redact_sensitive(payload)
        with self.transaction() as connection:
            run = connection.execute("SELECT * FROM worker_runs WHERE id=?", (run_id,)).fetchone()
            if run is None:
                raise KeyError(run_id)
            task = connection.execute(
                "SELECT project_id FROM tasks WHERE id=?", (run["task_id"],)
            ).fetchone()
            process_id = safe_payload.get("pid")
            connection.execute(
                "UPDATE worker_runs SET process_id=COALESCE(?,process_id),"
                "last_event_at=?,updated_at=? "
                "WHERE id=?",
                (
                    process_id if isinstance(process_id, int) else None,
                    timestamp(),
                    timestamp(),
                    run_id,
                ),
            )
            return self._append_event(
                connection,
                kind="workerAdapterEvent",
                severity=EventSeverity.DEBUG,
                entity_type="workerRun",
                entity_id=run_id,
                project_id=task["project_id"],
                task_id=run["task_id"],
                worker_id=run["worker_id"],
                run_id=run_id,
                summary=summary or f"Adapter event: {kind}",
                payload={"adapterEventKind": kind, **safe_payload},
                actor="adapter",
            )

    def list_events(
        self,
        *,
        after_sequence: int = 0,
        limit: int = 200,
        task_id: str | None = None,
    ) -> list[dict[str, Any]]:
        if limit < 1 or limit > 1000:
            raise ValueError("limit must be between 1 and 1000")
        query = "SELECT * FROM events WHERE sequence > ?"
        parameters: list[Any] = [after_sequence]
        if task_id is not None:
            query += " AND task_id = ?"
            parameters.append(task_id)
        query += " ORDER BY sequence ASC LIMIT ?"
        parameters.append(limit)
        with self.connect() as connection:
            rows = connection.execute(query, parameters).fetchall()
        events: list[dict[str, Any]] = []
        for row in rows:
            item = dict(row)
            item["payload"] = json.loads(item.pop("payload_json"))
            item["event_id"] = item["event_id"] or f"evt-{item['sequence']}"
            events.append(item)
        return events

    def highest_event_sequence(self) -> int:
        with self.connect() as connection:
            row = connection.execute(
                "SELECT COALESCE(MAX(sequence), 0) AS value FROM events"
            ).fetchone()
        return int(row["value"])

    def recover_interrupted(self) -> dict[str, int]:
        active_runs = ("starting", "running", "waiting")
        with self.transaction() as connection:
            runs = connection.execute(
                f"SELECT * FROM worker_runs WHERE state IN ({','.join('?' for _ in active_runs)})",
                active_runs,
            ).fetchall()
            tasks = connection.execute(
                "SELECT * FROM tasks WHERE state = ?", (TaskState.RUNNING.value,)
            ).fetchall()
            now = timestamp()
            for run in runs:
                connection.execute(
                    "UPDATE worker_runs SET state='interrupted',ended_at=?,updated_at=?,"
                    "failure_class='infrastructure',failure_detail='supervisor restart' WHERE id=?",
                    (now, now, run["id"]),
                )
                self._append_event(
                    connection,
                    kind="workerRunInterrupted",
                    severity=EventSeverity.WARNING,
                    entity_type="workerRun",
                    entity_id=run["id"],
                    task_id=run["task_id"],
                    worker_id=run["worker_id"],
                    run_id=run["id"],
                    summary="Worker run interrupted by supervisor restart",
                    payload={"previousState": run["state"]},
                    actor="recovery",
                )
            for task in tasks:
                connection.execute(
                    "UPDATE tasks SET state=?,updated_at=?,version=version+1 WHERE id=?",
                    (TaskState.INTERRUPTED.value, now, task["id"]),
                )
                self._append_event(
                    connection,
                    kind="taskRecovered",
                    severity=EventSeverity.WARNING,
                    entity_type="task",
                    entity_id=task["id"],
                    project_id=task["project_id"],
                    task_id=task["id"],
                    summary="Task marked interrupted during boot recovery",
                    payload={"from": TaskState.RUNNING.value, "to": TaskState.INTERRUPTED.value},
                    actor="recovery",
                )
        return {"runsInterrupted": len(runs), "tasksInterrupted": len(tasks)}

    def issue_api_token(
        self,
        *,
        label: str,
        scopes: set[str],
        expires_at: datetime | None = None,
    ) -> tuple[str, str]:
        token_id = f"tok-{uuid.uuid4()}"
        secret = secrets.token_urlsafe(32)
        salt = secrets.token_bytes(16)
        digest = hashlib.scrypt(secret.encode(), salt=salt, n=2**14, r=8, p=1)
        with self.transaction() as connection:
            connection.execute(
                "INSERT INTO api_tokens(id,label,token_salt,token_hash,scopes_json,"
                "created_at,expires_at) "
                "VALUES (?,?,?,?,?,?,?)",
                (
                    token_id,
                    label,
                    salt,
                    digest,
                    compact_json(sorted(scopes)),
                    timestamp(),
                    timestamp(expires_at) if expires_at else None,
                ),
            )
        return token_id, f"{token_id}.{secret}"

    def verify_api_token(self, presented: str, required_scope: str) -> bool:
        try:
            token_id, secret = presented.split(".", 1)
        except ValueError:
            return False
        with self.connect() as connection:
            row = connection.execute(
                "SELECT * FROM api_tokens WHERE id = ?", (token_id,)
            ).fetchone()
        if row is None or row["revoked_at"] is not None:
            return False
        if row["expires_at"] is not None and row["expires_at"] <= timestamp():
            return False
        scopes = set(json.loads(row["scopes_json"]))
        if required_scope not in scopes and "*" not in scopes:
            return False
        digest = hashlib.scrypt(secret.encode(), salt=row["token_salt"], n=2**14, r=8, p=1)
        return hmac.compare_digest(digest, row["token_hash"])

    def _append_event(
        self,
        connection: sqlite3.Connection,
        *,
        kind: str,
        severity: EventSeverity,
        entity_type: str,
        entity_id: str,
        summary: str,
        payload: dict[str, Any],
        project_id: str | None = None,
        task_id: str | None = None,
        worker_id: str | None = None,
        run_id: str | None = None,
        actor: str = "supervisor",
    ) -> int:
        event_id = f"evt-{uuid.uuid4()}"
        cursor = connection.execute(
            """
            INSERT INTO events(
                event_id,kind,severity,entity_type,entity_id,project_id,task_id,worker_id,run_id,
                summary,payload_json,actor,created_at
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                event_id,
                kind,
                severity.value,
                entity_type,
                entity_id,
                project_id,
                task_id,
                worker_id,
                run_id,
                summary,
                compact_json(payload),
                actor,
                timestamp(),
            ),
        )
        sequence = int(cursor.lastrowid)
        return sequence
