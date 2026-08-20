from __future__ import annotations

import hashlib
import hmac
import json
import os
import re
import secrets
import sqlite3
import threading
import time
import uuid
from collections.abc import Iterable, Iterator, Mapping
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from .domain import (
    EventSeverity,
    FailureClass,
    Harness,
    ModelDescriptor,
    NodeState,
    PermissionClass,
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
from .fabric.capabilities import (
    CapabilityClaim,
    CostMode,
    ObservationFreshness,
    QuotaAvailability,
    SubscriptionState,
    WorkerHealth,
    WorkerLocality,
    WorkerPrivacy,
)
from .hybrid import ExecutionHistoryRecord
from .protocols.capabilities import CapabilityGrant, GrantState
from .protocols.identity import NodePublicIdentity
from .state_machine import RUN_TRANSITIONS, TASK_TRANSITIONS
from .verification import DefinitionOfDoneResult, VerificationPolicyError, VerificationResult

if os.name == "nt":
    import msvcrt
else:
    import fcntl


def timestamp(value: datetime | None = None) -> str:
    return (value or datetime.now(UTC)).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def lease_expiry_timestamp(value: datetime, ttl_seconds: float) -> str:
    """Round lease expiry outward so second precision never shortens the requested TTL."""

    expires_at = value.astimezone(UTC) + timedelta(seconds=float(ttl_seconds))
    if expires_at.microsecond:
        expires_at = expires_at.replace(microsecond=0) + timedelta(seconds=1)
    return timestamp(expires_at)


def _aware_timestamp(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError("timestamp must be timezone-aware")
    return parsed.astimezone(UTC)


def compact_json(value: Any) -> str:
    return json.dumps(value, separators=(",", ":"), sort_keys=True)


def _acquire_file_lock(handle: Any) -> None:
    if os.name != "nt":
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
        return
    handle.seek(0, os.SEEK_END)
    if handle.tell() == 0:
        handle.write(b"\0")
        handle.flush()
    while True:
        handle.seek(0)
        try:
            msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
            return
        except OSError:
            time.sleep(0.05)


def _release_file_lock(handle: Any) -> None:
    if os.name != "nt":
        fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
        return
    handle.seek(0)
    msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)


_BENIGN_ACCOUNTING_KEY_IDENTITIES = frozenset(
    {
        "cachedtokens",
        "cachecreationtokens",
        "cachereadtokens",
        "cachetokens",
        "cachewritetokens",
        "inputtokens",
        "outputtokens",
        "reasoningtokens",
        "remainingtokens",
        "tokencount",
        "tokens",
        "totaltokens",
        "usedtokens",
    }
)
_CREDENTIAL_KEY_IDENTITIES = frozenset(
    {
        "accesskey",
        "accesskeyid",
        "accesstoken",
        "apikey",
        "apisecret",
        "auth",
        "authheader",
        "authheaders",
        "authorization",
        "authorizationheader",
        "authtoken",
        "bearer",
        "bearertoken",
        "clientcredential",
        "clientcredentials",
        "clientkey",
        "clientsecret",
        "clienttoken",
        "cookie",
        "credential",
        "credentials",
        "oauth",
        "oauthcredential",
        "oauthcredentials",
        "oauthsecret",
        "oauthtoken",
        "password",
        "passwd",
        "privatekey",
        "privatetoken",
        "refreshtoken",
        "secret",
        "sessioncookie",
        "sessiontoken",
        "signature",
        "token",
    }
)
_CREDENTIAL_KEY_SUFFIXES = (
    "accesskey",
    "accesskeyid",
    "accesstoken",
    "apikey",
    "apisecret",
    "authheader",
    "authheaders",
    "authorization",
    "authorizationheader",
    "authtoken",
    "bearertoken",
    "clientcredential",
    "clientcredentials",
    "clientkey",
    "clientsecret",
    "clienttoken",
    "cookie",
    "credential",
    "credentials",
    "oauthcredential",
    "oauthcredentials",
    "oauthsecret",
    "oauthtoken",
    "password",
    "passwd",
    "privatekey",
    "privatetoken",
    "refreshtoken",
    "secret",
    "sessiontoken",
    "signature",
    "token",
)


def _compact_key_identity(value: Any) -> str:
    return re.sub(r"[^a-z0-9]", "", str(value).casefold())


def _is_sensitive_key(value: Any) -> bool:
    identity = _compact_key_identity(value)
    if identity in _BENIGN_ACCOUNTING_KEY_IDENTITIES:
        return False
    return identity in _CREDENTIAL_KEY_IDENTITIES or any(
        identity.endswith(suffix) for suffix in _CREDENTIAL_KEY_SUFFIXES
    )


def redact_sensitive(value: Any) -> Any:
    if isinstance(value, dict):
        return {
            key: "[REDACTED]" if _is_sensitive_key(key) else redact_sensitive(item)
            for key, item in value.items()
        }
    if isinstance(value, list):
        return [redact_sensitive(item) for item in value]
    if isinstance(value, str):
        patterns = (
            re.compile(r"(?i)(authorization\s*[:=]\s*bearer\s+)[^\s,;]+"),
            re.compile(
                r"(?i)((?:token|api[ _-]?key|access[ _-]?token|refresh[ _-]?token|"
                r"oauth[ _-]?token|bearer[ _-]?token|auth[ _-]?(?:header|token)|"
                r"authorization(?:[ _-]?header)?|session[ _-]?cookie|client[ _-]?secret|"
                r"private[ _-]?key|password|secret|signature|cookie)"
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


class ExecutionLeaseLostError(RuntimeError):
    """A stale Runtime attempted to mutate work owned by another lease generation."""


class StateStore:
    """Synchronous SQLite boundary with serialized writes and transactional event emission."""

    MIGRATIONS_PATH = Path(__file__).with_name("migrations")

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

    def _require_task_execution_lease(
        self,
        connection: sqlite3.Connection,
        task_id: str,
        owner_id: str,
        generation: int,
    ) -> None:
        lease = connection.execute(
            "SELECT owner_id,generation,state,expires_at FROM task_execution_leases "
            "WHERE task_id=?",
            (task_id,),
        ).fetchone()
        if (
            lease is None
            or lease["owner_id"] != owner_id
            or int(lease["generation"]) != generation
            or lease["state"] != "active"
            or lease["expires_at"] <= timestamp()
        ):
            raise ExecutionLeaseLostError(
                f"task {task_id} execution lease {owner_id}/{generation} is no longer current"
            )

    def task_execution_lease_is_current(
        self,
        task_id: str,
        *,
        owner_id: str,
        generation: int,
    ) -> bool:
        with self.connect() as connection:
            try:
                self._require_task_execution_lease(
                    connection,
                    task_id,
                    owner_id,
                    generation,
                )
            except ExecutionLeaseLostError:
                return False
        return True

    def _initialize(self) -> None:
        migrations = self.MIGRATIONS_PATH
        lock_path = self.path.with_name(f".{self.path.name}.migrations.lock")
        with self._write_lock, lock_path.open("a+b") as migration_lock:
            # SQLite serializes ordinary writes, but journal-mode setup plus a stale migration
            # snapshot can race before that lock exists.  The sidecar lock covers the entire
            # initialization protocol across Supervisor processes on supported Unix hosts.
            _acquire_file_lock(migration_lock)
            try:
                with self.connect() as connection:
                    connection.execute("PRAGMA journal_mode = WAL")
                    connection.execute("PRAGMA synchronous = FULL")
                    connection.execute(
                        "CREATE TABLE IF NOT EXISTS schema_migrations "
                        "(version TEXT PRIMARY KEY, applied_at TEXT NOT NULL)"
                    )
                    applied = {
                        row["version"]
                        for row in connection.execute(
                            "SELECT version FROM schema_migrations"
                        ).fetchall()
                    }
                    for migration in sorted(migrations.glob("*.sql")):
                        if migration.stem in applied:
                            continue
                        version = migration.stem.replace("'", "''")
                        applied_at = timestamp().replace("'", "''")
                        script = (
                            "BEGIN IMMEDIATE;\n"
                            f"{migration.read_text(encoding='utf-8')}\n"
                            "INSERT INTO schema_migrations(version, applied_at) "
                            f"VALUES ('{version}', '{applied_at}');\n"
                            "COMMIT;"
                        )
                        try:
                            connection.executescript(script)
                        except Exception:
                            if connection.in_transaction:
                                connection.rollback()
                            raise
            finally:
                _release_file_lock(migration_lock)

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
        capability_contract = {
            "schemaVersion": "capability-request/v1",
            "required": [
                claim.to_protocol() for claim in task.requirements.required_capability_parameters
            ],
            "requiredManifestSchemaVersion": (task.requirements.required_manifest_schema_version),
            "requiredCatalogVersion": (task.requirements.required_capability_catalog_version),
        }
        try:
            execution_spec = json.loads(
                json.dumps(
                    task.execution_spec,
                    allow_nan=False,
                    ensure_ascii=False,
                    separators=(",", ":"),
                    sort_keys=True,
                )
            )
        except (TypeError, ValueError) as error:
            raise ValueError("task execution spec must be finite JSON") from error
        if not isinstance(execution_spec, dict):
            raise ValueError("task execution spec must be a JSON object")
        if len(compact_json(execution_spec).encode("utf-8")) > 131_072:
            raise ValueError("task execution spec exceeds its byte limit")
        if redact_sensitive(execution_spec) != execution_spec:
            raise ValueError("task execution spec must not contain credential-shaped fields")
        with self.transaction() as connection:
            connection.execute(
                """
                INSERT INTO tasks(
                    id,project_id,reference,title,description,state,topology,priority,labels_json,
                    required_capabilities_json,permission_class,approval_state,minimum_context_tokens,
                    privacy_sensitive,code_write_required,panel_size,preferred_workers_json,
                    preferred_capabilities_json,capability_constraints_json,local_only,
                    minimum_quality,max_incremental_cost_usd,explicit_worker_id,execution_spec_json,
                    attempt_count,version,created_at,updated_at
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
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
                    compact_json(sorted(task.requirements.preferred_capabilities)),
                    compact_json(capability_contract),
                    int(task.requirements.local_only),
                    task.requirements.minimum_quality_score,
                    task.requirements.max_incremental_cost_usd,
                    task.requirements.explicit_worker_override,
                    compact_json(execution_spec),
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
        """Persist one acyclic same-project dependency edge and journal it atomically."""

        if task_id == depends_on_task_id:
            raise ValueError("a task cannot depend on itself")

        with self.transaction() as connection:
            rows = connection.execute(
                "SELECT id,project_id,state FROM tasks WHERE id IN (?,?)",
                (task_id, depends_on_task_id),
            ).fetchall()
            by_id = {row["id"]: row for row in rows}
            if task_id not in by_id:
                raise KeyError(task_id)
            if depends_on_task_id not in by_id:
                raise KeyError(depends_on_task_id)
            if by_id[task_id]["project_id"] != by_id[depends_on_task_id]["project_id"]:
                raise ValueError("task dependencies cannot cross project boundaries")
            existing = connection.execute(
                "SELECT 1 FROM task_dependencies WHERE task_id=? AND depends_on_task_id=?",
                (task_id, depends_on_task_id),
            ).fetchone()
            if existing is not None:
                return
            if by_id[task_id]["state"] not in {
                TaskState.DRAFT.value,
                TaskState.QUEUED.value,
                TaskState.READY.value,
            }:
                raise ValueError("dependencies cannot be added after task execution begins")
            would_cycle = connection.execute(
                "WITH RECURSIVE ancestors(id) AS ("
                "SELECT ? UNION "
                "SELECT dependency.depends_on_task_id FROM task_dependencies dependency "
                "JOIN ancestors ON dependency.task_id=ancestors.id"
                ") SELECT 1 FROM ancestors WHERE id=? LIMIT 1",
                (depends_on_task_id, task_id),
            ).fetchone()
            if would_cycle is not None:
                raise ValueError("task dependency would create a cycle")
            cursor = connection.execute(
                "INSERT OR IGNORE INTO task_dependencies(task_id,depends_on_task_id) VALUES (?,?)",
                (task_id, depends_on_task_id),
            )

            if cursor.rowcount:
                connection.execute(
                    "UPDATE tasks SET definition_revision=definition_revision+1,"
                    "version=version+1,updated_at=? WHERE id=?",
                    (timestamp(), task_id),
                )
                self._append_event(
                    connection,
                    kind="taskDependencyAdded",
                    severity=EventSeverity.INFO,
                    entity_type="task",
                    entity_id=task_id,
                    project_id=by_id[task_id]["project_id"],
                    task_id=task_id,
                    summary=f"Task now depends on {depends_on_task_id}",
                    payload={"dependsOnTaskID": depends_on_task_id},
                    actor="scheduler",
                )

    def task_dependencies(self, task_id: str) -> list[dict[str, Any]]:
        """Return canonical prerequisite identities and states in stable order."""

        with self.connect() as connection:
            task = connection.execute("SELECT 1 FROM tasks WHERE id=?", (task_id,)).fetchone()
            if task is None:
                raise KeyError(task_id)
            return [
                dict(row)
                for row in connection.execute(
                    "SELECT prerequisite.id AS task_id,prerequisite.state "
                    "FROM task_dependencies dependency "
                    "JOIN tasks prerequisite ON prerequisite.id=dependency.depends_on_task_id "
                    "WHERE dependency.task_id=? ORDER BY prerequisite.id",
                    (task_id,),
                ).fetchall()
            ]

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

    @staticmethod
    def _verification_scope_item_snapshot(item: sqlite3.Row) -> dict[str, Any]:
        return {
            "criterionID": item["criterion_id"],
            "kind": item["kind"],
            "description": item["description"],
            "commandJSON": item["command_json"],
            "expectedJSON": item["expected_json"],
            "required": bool(item["required"]),
        }

    @classmethod
    def _assert_task_verification_scope_integrity(
        cls,
        connection: sqlite3.Connection,
        scope: sqlite3.Row,
    ) -> list[sqlite3.Row]:
        """Fail closed unless a sealed scope still matches its canonical definition hash."""

        items = connection.execute(
            "SELECT * FROM task_verification_scope_items WHERE scope_id=? ORDER BY ordinal,id",
            (scope["id"],),
        ).fetchall()
        if scope["sealed_at"] is None or not items:
            raise VerificationPolicyError("task verification scope is not durably sealed")
        if [int(item["ordinal"]) for item in items] != list(range(len(items))):
            raise VerificationPolicyError(
                "task verification scope criteria ordinals are not canonical"
            )

        item_snapshots: list[dict[str, Any]] = []
        for item in items:
            snapshot = cls._verification_scope_item_snapshot(item)
            expected_item_hash = hashlib.sha256(compact_json(snapshot).encode("utf-8")).hexdigest()
            if not hmac.compare_digest(str(item["definition_sha256"]), expected_item_hash):
                raise VerificationPolicyError(
                    "task verification scope criterion definition hash mismatch"
                )
            item_snapshots.append({**snapshot, "definitionSHA256": expected_item_hash})

        scope_snapshot = {
            "schemaVersion": scope["schema_version"],
            "taskID": scope["task_id"],
            "criteriaVersion": int(scope["criteria_version"]),
            "taskDefinitionRevision": int(scope["task_definition_revision"]),
            "goalID": scope["goal_id"],
            "iterationID": scope["iteration_id"],
            "planVersion": scope["plan_version"],
            "steerVersion": scope["steer_version"],
            "items": item_snapshots,
        }
        expected_scope_hash = hashlib.sha256(
            compact_json(scope_snapshot).encode("utf-8")
        ).hexdigest()
        if not hmac.compare_digest(str(scope["definition_sha256"]), expected_scope_hash):
            raise VerificationPolicyError("task verification scope definition hash mismatch")
        return items

    def bind_task_verification_scope(
        self,
        task_id: str,
        *,
        criterion_ids: Iterable[str] | None = None,
        goal_id: str | None = None,
        iteration_id: str | None = None,
        plan_version: int | None = None,
        steer_version: int | None = None,
        actor: str = "verification-policy",
    ) -> dict[str, Any]:
        """Append and activate an immutable Task-scoped criterion snapshot.

        Existing project criteria remain templates and retain their legacy behavior for Tasks
        without an active scope. Rebinding appends N+1 and advances the semantic Task definition
        revision; prior scopes and their item definitions are never rewritten.
        """

        if isinstance(criterion_ids, str):
            raise TypeError("criterion_ids must be an iterable of criterion identifiers")
        identities = tuple(criterion_ids) if criterion_ids is not None else None
        if identities is not None and len(identities) != len(set(identities)):
            raise ValueError("criterion_ids must be unique")
        if plan_version is not None and plan_version < 1:
            raise ValueError("plan_version must be positive")
        if steer_version is not None and steer_version < 0:
            raise ValueError("steer_version must not be negative")

        with self.transaction() as connection:
            task = connection.execute("SELECT * FROM tasks WHERE id=?", (task_id,)).fetchone()
            if task is None:
                raise KeyError(task_id)
            if task["state"] in {
                TaskState.SUCCEEDED.value,
                TaskState.FAILED.value,
                TaskState.CANCELLED.value,
            }:
                raise ValueError("terminal task verification criteria cannot be rebound")

            resolved_goal_id = goal_id
            resolved_plan_version = plan_version
            iteration_steer_version: int | None = None
            if iteration_id is not None:
                iteration = connection.execute(
                    "SELECT iteration.goal_id,iteration.sequence,iteration.state,"
                    "iteration.evaluation_json,goal.project_id "
                    "FROM autonomous_iterations iteration JOIN autonomous_goals goal "
                    "ON goal.id=iteration.goal_id WHERE iteration.id=?",
                    (iteration_id,),
                ).fetchone()
                if iteration is None:
                    raise KeyError(iteration_id)
                if iteration["project_id"] != task["project_id"]:
                    raise ValueError("verification scope iteration belongs to another project")
                if resolved_goal_id is not None and resolved_goal_id != iteration["goal_id"]:
                    raise ValueError("verification scope goal and iteration do not match")
                resolved_goal_id = str(iteration["goal_id"])
                if resolved_plan_version is None:
                    resolved_plan_version = int(iteration["sequence"])
                elif resolved_plan_version != int(iteration["sequence"]):
                    raise ValueError(
                        "verification scope plan_version does not match its iteration sequence"
                    )
                if iteration["state"] in {"completed", "interrupted"}:
                    raise ValueError("historical autonomous iterations cannot receive new criteria")
                started = connection.execute(
                    "SELECT sequence FROM events WHERE kind='goalIterationStarted' "
                    "AND entity_id=? ORDER BY sequence ASC LIMIT 1",
                    (iteration_id,),
                ).fetchone()
                if started is not None:
                    prior_steer = connection.execute(
                        "SELECT payload_json FROM events WHERE kind='goalSteered' "
                        "AND entity_id=? AND sequence<? ORDER BY sequence DESC LIMIT 1",
                        (resolved_goal_id, int(started["sequence"])),
                    ).fetchone()
                    if prior_steer is None:
                        iteration_steer_version = 0
                    else:
                        try:
                            iteration_steer_version = int(
                                json.loads(prior_steer["payload_json"])["steerVersion"]
                            )
                        except (KeyError, TypeError, ValueError, json.JSONDecodeError) as error:
                            raise VerificationPolicyError(
                                "autonomous iteration has invalid steering provenance"
                            ) from error
                elif iteration["evaluation_json"]:
                    try:
                        iteration_steer_version = int(
                            json.loads(iteration["evaluation_json"])["steerVersion"]
                        )
                    except (KeyError, TypeError, ValueError, json.JSONDecodeError) as error:
                        raise VerificationPolicyError(
                            "autonomous iteration has invalid steering provenance"
                        ) from error

            resolved_steer_version = steer_version
            if resolved_goal_id is not None:
                goal = connection.execute(
                    "SELECT project_id,steer_version FROM autonomous_goals WHERE id=?",
                    (resolved_goal_id,),
                ).fetchone()
                if goal is None:
                    raise KeyError(resolved_goal_id)
                if goal["project_id"] != task["project_id"]:
                    raise ValueError("verification scope goal belongs to another project")
                if resolved_steer_version is None:
                    resolved_steer_version = int(goal["steer_version"])
                elif resolved_steer_version != int(goal["steer_version"]):
                    raise ValueError(
                        "verification scope steer_version does not match the current Goal"
                    )
                if iteration_id is not None:
                    if iteration_steer_version is None:
                        if int(goal["steer_version"]) != 0:
                            raise VerificationPolicyError(
                                "autonomous iteration cannot prove its steering provenance"
                            )
                        iteration_steer_version = 0
                    if iteration_steer_version != int(goal["steer_version"]):
                        raise ValueError(
                            "verification scope iteration predates the current Goal steering"
                        )
                    if resolved_steer_version != iteration_steer_version:
                        raise ValueError(
                            "verification scope steer_version does not match its iteration"
                        )
            elif resolved_plan_version is not None or resolved_steer_version is not None:
                raise ValueError("plan/steer versions require a Goal-bound verification scope")

            if identities is None:
                criteria = connection.execute(
                    "SELECT * FROM acceptance_criteria WHERE project_id=? ORDER BY created_at,id",
                    (task["project_id"],),
                ).fetchall()
            elif identities:
                placeholders = ",".join("?" for _ in identities)
                rows = connection.execute(
                    f"SELECT * FROM acceptance_criteria WHERE project_id=? "
                    f"AND id IN ({placeholders})",
                    (task["project_id"], *identities),
                ).fetchall()
                by_id = {row["id"]: row for row in rows}
                missing = [criterion_id for criterion_id in identities if criterion_id not in by_id]
                if missing:
                    raise ValueError(
                        "unknown project acceptance criteria: " + ", ".join(sorted(missing))
                    )
                criteria = [by_id[criterion_id] for criterion_id in identities]
            else:
                criteria = []
            if not criteria:
                raise VerificationPolicyError(
                    "task verification scope requires at least one acceptance criterion"
                )

            previous_version = connection.execute(
                "SELECT COALESCE(MAX(criteria_version),0) AS value "
                "FROM task_verification_scopes WHERE task_id=?",
                (task_id,),
            ).fetchone()
            criteria_version = int(previous_version["value"]) + 1
            definition_revision = int(task["definition_revision"])
            if task["current_verification_scope_id"] is not None:
                definition_revision += 1
            scope_id = f"verification-scope-{uuid.uuid4()}"
            now = timestamp()

            item_snapshots: list[dict[str, Any]] = []
            item_rows: list[tuple[Any, ...]] = []
            for ordinal, criterion in enumerate(criteria):
                snapshot = {
                    "criterionID": criterion["id"],
                    "kind": criterion["kind"],
                    "description": criterion["description"],
                    "commandJSON": criterion["command_json"],
                    "expectedJSON": criterion["expected_json"],
                    "required": True,
                }
                definition_sha256 = hashlib.sha256(
                    compact_json(snapshot).encode("utf-8")
                ).hexdigest()
                item_snapshots.append({**snapshot, "definitionSHA256": definition_sha256})
                item_rows.append(
                    (
                        f"verification-scope-item-{uuid.uuid4()}",
                        scope_id,
                        criterion["id"],
                        criterion["id"],
                        ordinal,
                        1,
                        criterion["kind"],
                        criterion["description"],
                        criterion["command_json"],
                        criterion["expected_json"],
                        definition_sha256,
                        now,
                    )
                )
            scope_snapshot = {
                "schemaVersion": "task-verification-scope/v1",
                "taskID": task_id,
                "criteriaVersion": criteria_version,
                "taskDefinitionRevision": definition_revision,
                "goalID": resolved_goal_id,
                "iterationID": iteration_id,
                "planVersion": resolved_plan_version,
                "steerVersion": resolved_steer_version,
                "items": item_snapshots,
            }
            scope_sha256 = hashlib.sha256(compact_json(scope_snapshot).encode("utf-8")).hexdigest()
            connection.execute(
                "INSERT INTO task_verification_scopes("
                "id,project_id,task_id,criteria_version,task_definition_revision,goal_id,"
                "iteration_id,plan_version,steer_version,schema_version,definition_sha256,"
                "created_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    scope_id,
                    task["project_id"],
                    task_id,
                    criteria_version,
                    definition_revision,
                    resolved_goal_id,
                    iteration_id,
                    resolved_plan_version,
                    resolved_steer_version,
                    "task-verification-scope/v1",
                    scope_sha256,
                    now,
                ),
            )
            connection.executemany(
                "INSERT INTO task_verification_scope_items("
                "id,scope_id,criterion_id,source_criterion_id,ordinal,required,kind,description,"
                "command_json,expected_json,definition_sha256,created_at) "
                "VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
                item_rows,
            )
            sealed = connection.execute(
                "UPDATE task_verification_scopes SET sealed_at=? WHERE id=? AND sealed_at IS NULL",
                (now, scope_id),
            )
            if sealed.rowcount != 1:
                raise RuntimeError("task verification scope could not be sealed")
            sealed_scope = connection.execute(
                "SELECT * FROM task_verification_scopes WHERE id=?", (scope_id,)
            ).fetchone()
            if sealed_scope is None:
                raise RuntimeError("sealed task verification scope disappeared")
            self._assert_task_verification_scope_integrity(connection, sealed_scope)
            connection.execute(
                "UPDATE tasks SET current_verification_scope_id=?,definition_revision=?,"
                "version=version+1,updated_at=? WHERE id=?",
                (scope_id, definition_revision, now, task_id),
            )
            self._append_event(
                connection,
                kind="taskVerificationScopeBound",
                severity=EventSeverity.NOTICE,
                entity_type="taskVerificationScope",
                entity_id=scope_id,
                project_id=task["project_id"],
                task_id=task_id,
                summary=f"Task verification criteria version {criteria_version} bound",
                payload={
                    "verificationScopeID": scope_id,
                    "criteriaVersion": criteria_version,
                    "taskDefinitionRevision": definition_revision,
                    "criterionCount": len(item_rows),
                    "goalID": resolved_goal_id,
                    "iterationID": iteration_id,
                    "planVersion": resolved_plan_version,
                    "steerVersion": resolved_steer_version,
                    "definitionSHA256": scope_sha256,
                },
                actor=actor,
            )
        return self.get_task_verification_scope(scope_id)

    def get_task_verification_scope(self, scope_id: str) -> dict[str, Any]:
        with self.connect() as connection:
            row = connection.execute(
                "SELECT * FROM task_verification_scopes WHERE id=?", (scope_id,)
            ).fetchone()
            if row is None:
                raise KeyError(scope_id)
            self._assert_task_verification_scope_integrity(connection, row)
            return dict(row)

    def list_task_verification_scopes(self, task_id: str) -> list[dict[str, Any]]:
        with self.connect() as connection:
            rows = connection.execute(
                "SELECT * FROM task_verification_scopes WHERE task_id=? "
                "ORDER BY criteria_version,id",
                (task_id,),
            ).fetchall()
            for row in rows:
                self._assert_task_verification_scope_integrity(connection, row)
            return [dict(row) for row in rows]

    def list_task_verification_scope_items(self, scope_id: str) -> list[dict[str, Any]]:
        with self.connect() as connection:
            scope = connection.execute(
                "SELECT * FROM task_verification_scopes WHERE id=?", (scope_id,)
            ).fetchone()
            if scope is None:
                raise KeyError(scope_id)
            rows = self._assert_task_verification_scope_integrity(connection, scope)
            return [dict(row) for row in rows]

    def get_task_verification_context(self, task_id: str) -> dict[str, Any]:
        """Return the immutable provenance token a scoped verifier must echo on apply.

        The token is intentionally compared again inside ``apply_task_verification``'s write
        transaction.  Reading it does not reserve a Task; it lets a delayed verifier prove which
        criteria snapshot, semantic Task revision, and execution attempt it actually evaluated.
        """

        with self.connect() as connection:
            task = connection.execute(
                "SELECT id,current_verification_scope_id,definition_revision FROM tasks WHERE id=?",
                (task_id,),
            ).fetchone()
            if task is None:
                raise KeyError(task_id)
            scope_id = task["current_verification_scope_id"]
            if scope_id is None:
                return {
                    "task_id": task_id,
                    "verification_scope_id": None,
                    "criteria_version": None,
                    "task_definition_revision": int(task["definition_revision"]),
                    "source_attempt": None,
                    "definition_sha256": None,
                }
            scope = connection.execute(
                "SELECT * FROM task_verification_scopes WHERE id=? AND task_id=?",
                (scope_id, task_id),
            ).fetchone()
            if scope is None:
                raise RuntimeError("task references an unknown verification scope")
            self._assert_task_verification_scope_integrity(connection, scope)
            if int(scope["task_definition_revision"]) != int(task["definition_revision"]):
                raise RuntimeError("task verification scope does not match its current definition")
            latest_attempt = connection.execute(
                "SELECT MAX(attempt) AS value FROM worker_runs WHERE task_id=?",
                (task_id,),
            ).fetchone()
            return {
                "task_id": task_id,
                "verification_scope_id": str(scope_id),
                "criteria_version": int(scope["criteria_version"]),
                "task_definition_revision": int(scope["task_definition_revision"]),
                "source_attempt": (
                    int(latest_attempt["value"])
                    if latest_attempt is not None and latest_attempt["value"] is not None
                    else None
                ),
                "definition_sha256": str(scope["definition_sha256"]),
            }

    @staticmethod
    def _task_verification_dispatch_snapshot(
        connection: sqlite3.Connection, task: sqlite3.Row
    ) -> tuple[str | None, int]:
        scope_id = task["current_verification_scope_id"]
        definition_revision = int(task["definition_revision"])
        if scope_id is None:
            return None, definition_revision
        scope = connection.execute(
            "SELECT * FROM task_verification_scopes WHERE id=?",
            (scope_id,),
        ).fetchone()
        if (
            scope is None
            or scope["task_id"] != task["id"]
            or int(scope["task_definition_revision"]) != definition_revision
        ):
            raise RuntimeError("task verification scope does not match its current definition")
        StateStore._assert_task_verification_scope_integrity(connection, scope)
        return str(scope_id), definition_revision

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
                "SELECT project_id,current_verification_scope_id FROM tasks WHERE id=?",
                (task_id,),
            ).fetchone()
            if task is None:
                raise KeyError(task_id)
            if criterion_id is not None:
                if task["current_verification_scope_id"] is not None:
                    raise VerificationPolicyError(
                        "criterion-bearing verification for a scoped Task requires provenance"
                    )
                criterion = connection.execute(
                    "SELECT 1 FROM acceptance_criteria WHERE id=? AND project_id=?",
                    (criterion_id, task["project_id"]),
                ).fetchone()
                if criterion is None:
                    raise ValueError(
                        f"acceptance criterion {criterion_id} does not belong to Task project"
                    )
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
                    "WHERE id=? AND project_id=?",
                    (
                        "passed" if passed else "failed",
                        compact_json(safe_evidence),
                        timestamp(),
                        criterion_id,
                        task["project_id"],
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

    def apply_task_verification(
        self,
        task_id: str,
        result: DefinitionOfDoneResult,
        *,
        verifier: str = "deterministic-verifier",
        max_attempts: int | None = None,
        expected_verification_scope_id: str | None = None,
        expected_task_definition_revision: int | None = None,
        expected_source_attempt: int | None = None,
        expected_steer_version: int | None = None,
    ) -> TaskState:
        """Persist a complete verification decision and Task transition atomically.

        The transaction acquires SQLite's write lock before reading the Task.  Competing verifier
        processes therefore cannot both append contradictory criterion results from the same
        REVIEWING snapshot or leave a succeeded Task paired with a later failed criterion row.
        """

        with self.transaction() as connection:
            task = connection.execute("SELECT * FROM tasks WHERE id=?", (task_id,)).fetchone()
            if task is None:
                raise KeyError(task_id)
            if task["state"] != TaskState.REVIEWING.value:
                raise RuntimeError(f"task {task_id} is not awaiting verification")
            if max_attempts is not None and max_attempts < 1:
                raise ValueError("max_attempts must be positive")
            if expected_steer_version is not None:
                binding = connection.execute(
                    "SELECT binding.steer_version,goal.steer_version AS current_steer_version "
                    "FROM autonomous_task_bindings binding JOIN autonomous_goals goal "
                    "ON goal.id=binding.goal_id WHERE binding.task_id=?",
                    (task_id,),
                ).fetchone()
                if binding is None:
                    if expected_steer_version != 0:
                        raise VerificationPolicyError(
                            "stale verification provenance: Task has no matching steer authority"
                        )
                elif (
                    int(binding["steer_version"]) != expected_steer_version
                    or int(binding["current_steer_version"]) != expected_steer_version
                ):
                    raise VerificationPolicyError(
                        "stale verification provenance: Goal steering version changed"
                    )
            verification_scope_id = task["current_verification_scope_id"]
            scoped = verification_scope_id is not None
            source_attempt: int | None = None
            criteria_version: int | None = None
            if scoped:
                if (
                    expected_verification_scope_id is None
                    or expected_task_definition_revision is None
                    or expected_source_attempt is None
                ):
                    raise VerificationPolicyError(
                        "scoped verification requires its scope, Task revision, and source attempt"
                    )
                if (
                    expected_verification_scope_id != verification_scope_id
                    or expected_task_definition_revision != int(task["definition_revision"])
                ):
                    raise VerificationPolicyError(
                        "stale verification provenance: Task criteria or definition changed"
                    )
                scope = connection.execute(
                    "SELECT * FROM task_verification_scopes WHERE id=? AND task_id=?",
                    (verification_scope_id, task_id),
                ).fetchone()
                if scope is None or int(scope["task_definition_revision"]) != int(
                    task["definition_revision"]
                ):
                    raise VerificationPolicyError(
                        "current task verification scope does not match its definition revision"
                    )
                criteria = self._assert_task_verification_scope_integrity(connection, scope)
                latest_attempt = connection.execute(
                    "SELECT MAX(attempt) AS value FROM worker_runs WHERE task_id=?",
                    (task_id,),
                ).fetchone()
                if latest_attempt is None or latest_attempt["value"] is None:
                    raise VerificationPolicyError(
                        "scoped verification requires a scope-bound worker execution"
                    )
                source_attempt = int(latest_attempt["value"])
                if expected_source_attempt != source_attempt:
                    raise VerificationPolicyError(
                        "stale verification provenance: a newer Task execution attempt exists"
                    )
                latest_runs = connection.execute(
                    "SELECT verification_scope_id,task_definition_revision "
                    "FROM worker_runs WHERE task_id=? AND attempt=? ORDER BY id",
                    (task_id, source_attempt),
                ).fetchall()
                if not latest_runs or any(
                    run["verification_scope_id"] != verification_scope_id
                    or run["task_definition_revision"] is None
                    or int(run["task_definition_revision"]) != int(task["definition_revision"])
                    for run in latest_runs
                ):
                    raise VerificationPolicyError(
                        "stale verification scope: latest execution does not match current task "
                        "criteria/revision"
                    )
                criteria_version = int(scope["criteria_version"])
                criteria_by_id = {criterion["criterion_id"]: criterion for criterion in criteria}
            else:
                criteria = connection.execute(
                    "SELECT * FROM acceptance_criteria WHERE project_id=? ORDER BY created_at,id",
                    (task["project_id"],),
                ).fetchall()
                criteria_by_id = {criterion["id"]: criterion for criterion in criteria}
            result_ids = [item.criterion_id for item in result.results]
            if len(result_ids) != len(set(result_ids)):
                raise VerificationPolicyError(
                    "verification results contain duplicate criterion IDs"
                )
            unknown = (set(result_ids) | set(result.required_failures)) - set(criteria_by_id)
            if unknown:
                raise VerificationPolicyError(
                    "verification referenced unknown criteria: " + ", ".join(sorted(unknown))
                )

            reported_by_id = {item.criterion_id: item for item in result.results}
            canonical_results: list[VerificationResult] = []
            canonical_failures: list[str] = []
            for criterion in criteria:
                criterion_id = criterion["criterion_id"] if scoped else criterion["id"]
                item = reported_by_id.get(criterion_id)
                if item is None:
                    item = VerificationResult(
                        criterion_id=criterion_id,
                        passed=False,
                        summary="required criterion was not evaluated",
                        evidence={"reasonCode": "VERIFICATION_RESULT_MISSING"},
                    )
                canonical_results.append(item)
                if not item.passed or item.criterion_id in result.required_failures:
                    canonical_failures.append(item.criterion_id)

            now = timestamp()
            for item in canonical_results:
                criterion = criteria_by_id[item.criterion_id]
                safe_evidence = redact_sensitive({"summary": item.summary, **item.evidence})
                verification_id = f"verify-{uuid.uuid4()}"
                connection.execute(
                    "INSERT INTO verifications(id,task_id,criterion_id,kind,command_json,"
                    "exit_code,passed,evidence_json,verifier,created_at,verification_scope_id,"
                    "scope_item_id,source_attempt) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    (
                        verification_id,
                        task_id,
                        criterion["source_criterion_id"] if scoped else item.criterion_id,
                        criterion["kind"],
                        criterion["command_json"],
                        item.exit_code,
                        int(item.passed),
                        compact_json(safe_evidence),
                        verifier,
                        now,
                        verification_scope_id,
                        criterion["id"] if scoped else None,
                        source_attempt,
                    ),
                )
                if not scoped:
                    connection.execute(
                        "UPDATE acceptance_criteria SET state=?,evidence_json=?,updated_at=? "
                        "WHERE id=?",
                        (
                            "passed" if item.passed else "failed",
                            compact_json(safe_evidence),
                            now,
                            item.criterion_id,
                        ),
                    )
                self._append_event(
                    connection,
                    kind="verificationCompleted",
                    severity=EventSeverity.NOTICE if item.passed else EventSeverity.WARNING,
                    entity_type="task",
                    entity_id=task_id,
                    project_id=task["project_id"],
                    task_id=task_id,
                    summary=f"Deterministic verification {'passed' if item.passed else 'failed'}",
                    payload={
                        "verificationID": verification_id,
                        "criterionID": item.criterion_id,
                        "verificationScopeID": verification_scope_id,
                        "criteriaVersion": criteria_version,
                        "taskDefinitionRevision": (
                            int(task["definition_revision"]) if scoped else None
                        ),
                        "sourceAttempt": source_attempt,
                    },
                    actor=verifier,
                )

            complete = (
                bool(criteria)
                and result.complete
                and not canonical_failures
                and len(canonical_results) == len(criteria)
            )
            retry_exhausted = (
                not complete
                and max_attempts is not None
                and int(task["attempt_count"]) >= max_attempts
            )
            target = (
                TaskState.SUCCEEDED
                if complete
                else TaskState.FAILED
                if retry_exhausted
                else TaskState.READY
            )
            payload = (
                {"criteria": [item.criterion_id for item in canonical_results]}
                if complete
                else {
                    "requiredFailures": canonical_failures,
                    "reportedComplete": result.complete,
                    "expectedCriteria": [
                        criterion["criterion_id"] if scoped else criterion["id"]
                        for criterion in criteria
                    ],
                    "retryLimitExhausted": retry_exhausted,
                    "maxAttempts": max_attempts,
                }
            )
            summary = (
                "Definition of Done satisfied"
                if complete
                else "Verification failed and execution retry limit is exhausted"
                if retry_exhausted
                else "Verification failed; task returned to ready queue"
            )
            connection.execute(
                "UPDATE tasks SET state=?,updated_at=?,finished_at=?,version=version+1 WHERE id=?",
                (
                    target.value,
                    now,
                    now if target in {TaskState.SUCCEEDED, TaskState.FAILED} else None,
                    task_id,
                ),
            )
            self._append_event(
                connection,
                kind="taskStateChanged",
                severity=EventSeverity.NOTICE,
                entity_type="task",
                entity_id=task_id,
                project_id=task["project_id"],
                task_id=task_id,
                summary=summary,
                payload={
                    "from": TaskState.REVIEWING.value,
                    "to": target.value,
                    **payload,
                },
                actor="runtime",
            )
        return target

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
                    worker_classes_json,
                    code_write_allowed,privacy_allowed,quality_score,reliability_score,
                    expected_latency_seconds,monetary_cost_score,harness_version,last_heartbeat_at,
                    created_at,updated_at
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                ON CONFLICT(id) DO UPDATE SET
                    node_id=excluded.node_id,harness=excluded.harness,provider=excluded.provider,
                    model_id=excluded.model_id,state=excluded.state,
                    resource_state=excluded.resource_state,
                    capabilities_json=excluded.capabilities_json,
                    worker_classes_json=excluded.worker_classes_json,
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
                    compact_json(sorted(worker.worker_classes)),
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
        task_id: str | None = None,
        lease_owner_id: str | None = None,
        lease_generation: int | None = None,
        actor: str = "runtime",
    ) -> None:
        with self.transaction() as connection:
            worker = connection.execute("SELECT * FROM workers WHERE id=?", (worker_id,)).fetchone()
            if worker is None:
                raise KeyError(worker_id)
            lease_values = (lease_owner_id, lease_generation, task_id)
            if any(value is not None for value in lease_values) and any(
                value is None for value in lease_values
            ):
                raise ValueError("task, lease owner, and generation must be supplied together")
            if task_id is not None and lease_owner_id is not None and lease_generation is not None:
                self._require_task_execution_lease(
                    connection,
                    task_id,
                    lease_owner_id,
                    lease_generation,
                )
            effective_state = state
            if state is WorkerState.IDLE:
                active_states = {
                    str(row["state"])
                    for row in connection.execute(
                        "SELECT state FROM worker_runs WHERE worker_id=? "
                        "AND state IN ('starting','running','waiting')",
                        (worker_id,),
                    ).fetchall()
                }
                if RunState.RUNNING.value in active_states:
                    effective_state = WorkerState.RUNNING
                elif RunState.STARTING.value in active_states:
                    effective_state = WorkerState.STARTING
                elif RunState.WAITING.value in active_states:
                    effective_state = WorkerState.WAITING
            now = timestamp()
            connection.execute(
                "UPDATE workers SET state=?,resource_state=COALESCE(?,resource_state),"
                "model_id=COALESCE(?,model_id),last_heartbeat_at=?,updated_at=? WHERE id=?",
                (
                    effective_state.value,
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
                summary=f"Worker state {worker['state']} -> {effective_state.value}",
                payload={"from": worker["state"], "to": effective_state.value},
                actor=actor,
            )

    def claim_workers(self, worker_ids: Iterable[str], *, actor: str = "runtime") -> bool:
        """Atomically reserve idle Workers before launching a Task.

        The in-memory runtime lock only coordinates one process.  This compare-and-set keeps two
        production hosts from selecting the same persisted Worker concurrently.
        """

        identities = tuple(dict.fromkeys(worker_ids))
        if not identities:
            return False
        with self.transaction() as connection:
            placeholders = ",".join("?" for _ in identities)
            rows = connection.execute(
                f"SELECT * FROM workers WHERE id IN ({placeholders}) ORDER BY id", identities
            ).fetchall()
            if len(rows) != len(identities) or any(
                row["state"] != WorkerState.IDLE.value for row in rows
            ):
                return False
            now = timestamp()
            cursor = connection.execute(
                f"UPDATE workers SET state=?,last_heartbeat_at=?,updated_at=? "
                f"WHERE id IN ({placeholders}) AND state=?",
                (WorkerState.STARTING.value, now, now, *identities, WorkerState.IDLE.value),
            )
            if cursor.rowcount != len(identities):
                raise RuntimeError("worker reservation concurrent update")
            by_id = {row["id"]: row for row in rows}
            for worker_id in identities:
                self._append_event(
                    connection,
                    kind="workerStateChanged",
                    severity=EventSeverity.NOTICE,
                    entity_type="worker",
                    entity_id=worker_id,
                    worker_id=worker_id,
                    summary=f"Worker state {by_id[worker_id]['state']} -> starting",
                    payload={"from": by_id[worker_id]["state"], "to": "starting"},
                    actor=actor,
                )
        return True

    def claim_task_dispatch(
        self,
        task_id: str,
        worker_ids: Iterable[str],
        *,
        expected_version: int,
        timeout_at: datetime | None = None,
        lease_owner_id: str | None = None,
        lease_ttl_seconds: float = 30.0,
        max_attempts: int | None = None,
        expected_manifest_digests: Mapping[str, str | None] | None = None,
        expected_execution_observation_ids: Mapping[str, str | None] | None = None,
        expected_authorization_envelope_id: str | None = None,
        actor: str = "runtime",
    ) -> dict[str, Any] | None:
        """Atomically claim a READY Task, its Workers, and durable STARTING executions.

        Returning ``None`` means another process changed the Task or Worker snapshot first.  The
        transaction deliberately creates Worker runs before any adapter can be invoked, closing the
        restart window where a Worker reservation previously had no durable Task/run owner.
        """

        identities = tuple(dict.fromkeys(worker_ids))
        if not identities:
            raise ValueError("a dispatch claim requires at least one worker")
        if lease_owner_id is not None and not lease_owner_id.strip():
            raise ValueError("lease_owner_id cannot be empty")
        if lease_owner_id is not None and lease_ttl_seconds < 1:
            raise ValueError("lease_ttl_seconds must be at least one second")
        if max_attempts is not None and max_attempts < 1:
            raise ValueError("max_attempts must be positive")
        expected_manifests = dict(expected_manifest_digests or {})
        if expected_manifests and set(expected_manifests) != set(identities):
            raise ValueError("manifest dispatch fence must cover every selected Worker")
        expected_execution = dict(expected_execution_observation_ids or {})
        if expected_execution and set(expected_execution) != set(identities):
            raise ValueError("execution observation fence must cover every selected Worker")
        with self.transaction() as connection:
            task = connection.execute("SELECT * FROM tasks WHERE id=?", (task_id,)).fetchone()
            if task is None:
                raise KeyError(task_id)
            if task["state"] != TaskState.READY.value or task["version"] != expected_version:
                return None
            if max_attempts is not None and int(task["attempt_count"]) >= max_attempts:
                return None
            autonomous_binding = connection.execute(
                "SELECT binding.steer_version,goal.state AS goal_state,"
                "goal.steer_version AS current_steer_version "
                "FROM autonomous_task_bindings binding "
                "JOIN autonomous_goals goal ON goal.id=binding.goal_id "
                "WHERE binding.task_id=?",
                (task_id,),
            ).fetchone()
            if autonomous_binding is not None and (
                autonomous_binding["goal_state"] != "running"
                or int(autonomous_binding["steer_version"])
                != int(autonomous_binding["current_steer_version"])
            ):
                return None
            verification_scope_id, task_definition_revision = (
                self._task_verification_dispatch_snapshot(connection, task)
            )
            unsatisfied = connection.execute(
                "SELECT dependency.depends_on_task_id,prerequisite.state "
                "FROM task_dependencies dependency "
                "JOIN tasks prerequisite ON prerequisite.id=dependency.depends_on_task_id "
                "WHERE dependency.task_id=? AND prerequisite.state<>? "
                "ORDER BY dependency.depends_on_task_id LIMIT 1",
                (task_id, TaskState.SUCCEEDED.value),
            ).fetchone()
            if unsatisfied is not None:
                return None
            authorization = None
            authorization_allowed_providers: set[str] = set()
            authorization_allowed_worker_classes: set[str] = set()
            if expected_authorization_envelope_id is not None:
                authorization = connection.execute(
                    "SELECT envelope.* FROM authorization_envelopes envelope "
                    "JOIN authorization_envelope_bindings binding "
                    "ON binding.envelope_id=envelope.id "
                    "WHERE envelope.id=? AND binding.task_id=? AND binding.run_id IS NULL "
                    "AND binding.binding_kind='task'",
                    (expected_authorization_envelope_id, task_id),
                ).fetchone()
                if authorization is None or authorization["project_id"] != task["project_id"]:
                    return None
                if authorization["expires_at"] is not None and _aware_timestamp(
                    str(authorization["expires_at"])
                ) <= datetime.now(UTC):
                    return None
                if authorization["user_approval_state"] not in {"notRequired", "approved"}:
                    return None
                if authorization["platform_approval_state"] not in {
                    "notRequired",
                    "approved",
                }:
                    return None
                if (
                    task["permission_class"] == PermissionClass.RED.value
                    and authorization["permission_ceiling"] != PermissionClass.RED.value
                ):
                    return None
                required_capabilities = set(json.loads(task["required_capabilities_json"]))
                if not required_capabilities.issubset(
                    set(json.loads(authorization["capabilities_json"]))
                ):
                    return None
                authorization_allowed_providers = set(
                    json.loads(authorization["allowed_providers_json"])
                )
                authorization_allowed_worker_classes = set(
                    json.loads(authorization["allowed_worker_classes_json"])
                )
                try:
                    execution_spec = json.loads(task["execution_spec_json"] or "{}")
                except json.JSONDecodeError:
                    return None
                if not isinstance(execution_spec, dict):
                    return None
                required_actions = execution_spec.get("authorizationActionClasses", [])
                required_data_classes = execution_spec.get("authorizationDataClasses", [])
                if not isinstance(required_actions, list) or not isinstance(
                    required_data_classes, list
                ):
                    return None
                if any(
                    not isinstance(value, str)
                    for value in (*required_actions, *required_data_classes)
                ):
                    return None
                if bool(task["code_write_required"]):
                    required_actions = [*required_actions, "code.write"]
                allowed_actions = set(json.loads(authorization["allowed_action_classes_json"]))
                denied_actions = set(json.loads(authorization["denied_action_classes_json"]))
                allowed_data = set(json.loads(authorization["allowed_data_classes_json"]))
                denied_data = set(json.loads(authorization["denied_data_classes_json"]))
                if not set(required_actions).issubset(allowed_actions) or set(
                    required_actions
                ).intersection(denied_actions):
                    return None
                if not set(required_data_classes).issubset(allowed_data) or set(
                    required_data_classes
                ).intersection(denied_data):
                    return None
            placeholders = ",".join("?" for _ in identities)
            workers = connection.execute(
                f"SELECT * FROM workers WHERE id IN ({placeholders}) ORDER BY id", identities
            ).fetchall()
            if len(workers) != len(identities):
                return None
            if authorization is not None:
                for worker in workers:
                    if worker["provider"] not in authorization_allowed_providers:
                        return None
                    worker_classes = set(json.loads(worker["worker_classes_json"] or "[]"))
                    if not worker_classes.intersection(authorization_allowed_worker_classes):
                        return None
            manifest_rows = connection.execute(
                f"SELECT h.worker_id,m.definition_sha256,m.valid_until,m.manifest_json "
                f"FROM worker_capability_manifest_heads h "
                f"JOIN worker_capability_manifests m ON m.id=h.manifest_id "
                f"WHERE h.worker_id IN ({placeholders})",
                identities,
            ).fetchall()
            manifests_by_worker = {str(row["worker_id"]): row for row in manifest_rows}
            if expected_manifests and any(
                (
                    str(manifests_by_worker[worker_id]["definition_sha256"])
                    if worker_id in manifests_by_worker
                    else None
                )
                != expected_manifests[worker_id]
                for worker_id in identities
            ):
                return None
            if expected_execution:
                execution_rows = connection.execute(
                    f"SELECT observation.* FROM worker_execution_observations observation "
                    f"WHERE observation.worker_id IN ({placeholders}) AND observation.version=("
                    "SELECT MAX(latest.version) FROM worker_execution_observations latest "
                    "WHERE latest.worker_id=observation.worker_id)",
                    identities,
                ).fetchall()
                execution_by_worker = {str(row["worker_id"]): row for row in execution_rows}
                for worker_id, expected_observation in expected_execution.items():
                    if expected_observation is None:
                        continue
                    observation = execution_by_worker.get(worker_id)
                    if observation is None or observation["id"] != expected_observation:
                        return None
                    if _aware_timestamp(str(observation["valid_until"])) <= datetime.now(UTC):
                        return None
                    if any(
                        observation[field] != "yes"
                        for field in (
                            "discovered",
                            "configured",
                            "authenticated",
                            "authorized",
                            "reachable",
                            "runtime_available",
                            "healthy",
                            "capacity_available",
                        )
                    ):
                        return None
                    if observation["platform_approval"] not in {"notRequired", "approved"}:
                        return None
            active_counts = {
                str(row["worker_id"]): int(row["active_count"])
                for row in connection.execute(
                    f"SELECT worker_id,COUNT(*) AS active_count FROM worker_runs "
                    f"WHERE worker_id IN ({placeholders}) "
                    "AND state IN ('starting','running','waiting') GROUP BY worker_id",
                    identities,
                ).fetchall()
            }
            now_value = datetime.now(UTC)
            now = timestamp(now_value)
            busy_worker_states = {
                WorkerState.STARTING.value,
                WorkerState.RUNNING.value,
                WorkerState.WAITING.value,
            }
            for worker in workers:
                worker_id = str(worker["id"])
                active_count = active_counts.get(worker_id, 0)
                manifest = manifests_by_worker.get(worker_id)
                if manifest is None:
                    if active_count != 0 or worker["state"] != WorkerState.IDLE.value:
                        return None
                    continue
                valid_until = manifest["valid_until"]
                if valid_until is not None:
                    try:
                        expiry = _aware_timestamp(str(valid_until))
                    except ValueError:
                        return None
                    if expiry <= now_value:
                        return None
                try:
                    max_concurrency = json.loads(manifest["manifest_json"])["maxConcurrency"]
                except (KeyError, TypeError, ValueError):
                    return None
                if (
                    isinstance(max_concurrency, bool)
                    or not isinstance(max_concurrency, int)
                    or max_concurrency < 1
                    or active_count >= max_concurrency
                ):
                    return None
                if active_count == 0:
                    if worker["state"] != WorkerState.IDLE.value:
                        return None
                elif worker["state"] not in busy_worker_states:
                    return None
            pool_rows = connection.execute(
                f"SELECT binding.worker_id,pool.id,pool.max_concurrency "
                f"FROM provider_capacity_pool_workers binding "
                f"JOIN provider_capacity_pools pool ON pool.id=binding.pool_id "
                f"WHERE binding.worker_id IN ({placeholders}) AND pool.enabled=1",
                identities,
            ).fetchall()
            selected_by_pool: dict[str, int] = {}
            pool_limits: dict[str, int] = {}
            pool_by_worker: dict[str, str] = {}
            for pool in pool_rows:
                pool_id = str(pool["id"])
                pool_by_worker[str(pool["worker_id"])] = pool_id
                pool_limits[pool_id] = int(pool["max_concurrency"])
                selected_by_pool[pool_id] = selected_by_pool.get(pool_id, 0) + 1
            for pool_id, selected_count in selected_by_pool.items():
                reserved = int(
                    connection.execute(
                        "SELECT COUNT(*) AS count FROM provider_capacity_reservations "
                        "WHERE pool_id=? AND state='reserved'",
                        (pool_id,),
                    ).fetchone()["count"]
                )
                if reserved + selected_count > pool_limits[pool_id]:
                    return None
            attempt = int(task["attempt_count"]) + 1
            lease_generation: int | None = None
            if lease_owner_id is not None:
                previous_lease = connection.execute(
                    "SELECT generation FROM task_execution_leases WHERE task_id=?",
                    (task_id,),
                ).fetchone()
                lease_generation = (
                    int(previous_lease["generation"]) + 1 if previous_lease is not None else 1
                )
                expires_at = lease_expiry_timestamp(now_value, lease_ttl_seconds)
                connection.execute(
                    "INSERT INTO task_execution_leases(task_id,owner_id,generation,state,"
                    "acquired_at,heartbeat_at,expires_at,released_at) "
                    "VALUES (?,?,?,'active',?,?,?,NULL) "
                    "ON CONFLICT(task_id) DO UPDATE SET owner_id=excluded.owner_id,"
                    "generation=excluded.generation,state='active',"
                    "acquired_at=excluded.acquired_at,heartbeat_at=excluded.heartbeat_at,"
                    "expires_at=excluded.expires_at,released_at=NULL",
                    (
                        task_id,
                        lease_owner_id,
                        lease_generation,
                        now,
                        now,
                        expires_at,
                    ),
                )
            cursor = connection.execute(
                "UPDATE tasks SET state=?,attempt_count=?,started_at=COALESCE(started_at,?),"
                "updated_at=?,version=version+1 WHERE id=? AND state=? AND version=?",
                (
                    TaskState.RUNNING.value,
                    attempt,
                    now,
                    now,
                    task_id,
                    TaskState.READY.value,
                    expected_version,
                ),
            )
            if cursor.rowcount != 1:
                return None
            idle_identities = tuple(
                str(worker["id"]) for worker in workers if worker["state"] == WorkerState.IDLE.value
            )
            if idle_identities:
                idle_placeholders = ",".join("?" for _ in idle_identities)
                cursor = connection.execute(
                    f"UPDATE workers SET state=?,last_heartbeat_at=?,updated_at=? "
                    f"WHERE id IN ({idle_placeholders}) AND state=?",
                    (
                        WorkerState.STARTING.value,
                        now,
                        now,
                        *idle_identities,
                        WorkerState.IDLE.value,
                    ),
                )
                if cursor.rowcount != len(idle_identities):
                    raise RuntimeError("worker reservation concurrent update")

            self._append_event(
                connection,
                kind="taskStateChanged",
                severity=EventSeverity.NOTICE,
                entity_type="task",
                entity_id=task_id,
                project_id=task["project_id"],
                task_id=task_id,
                summary="Task atomically claimed for dispatch",
                payload={
                    "from": TaskState.READY.value,
                    "to": TaskState.RUNNING.value,
                    "attempt": attempt,
                    "workerIDs": list(identities),
                },
                actor=actor,
            )
            if lease_generation is not None:
                self._append_event(
                    connection,
                    kind="taskExecutionLeaseAcquired",
                    severity=EventSeverity.INFO,
                    entity_type="task",
                    entity_id=task_id,
                    project_id=task["project_id"],
                    task_id=task_id,
                    summary="Task execution ownership lease acquired",
                    payload={
                        "ownerID": lease_owner_id,
                        "generation": lease_generation,
                        "expiresAt": expires_at,
                    },
                    actor=actor,
                )

            workers_by_id = {worker["id"]: worker for worker in workers}
            run_ids: dict[str, str] = {}
            for worker_id in identities:
                run_id = f"run-{uuid.uuid4()}"
                run_ids[worker_id] = run_id
                connection.execute(
                    "INSERT INTO worker_runs("
                    "id,task_id,worker_id,state,attempt,timeout_at,verification_scope_id,"
                    "task_definition_revision,created_at,updated_at"
                    ") VALUES (?,?,?,?,?,?,?,?,?,?)",
                    (
                        run_id,
                        task_id,
                        worker_id,
                        RunState.STARTING.value,
                        attempt,
                        timestamp(timeout_at) if timeout_at else None,
                        verification_scope_id,
                        task_definition_revision,
                        now,
                        now,
                    ),
                )
                if authorization is not None:
                    connection.execute(
                        "INSERT INTO authorization_envelope_bindings(id,envelope_id,task_id,run_id,"
                        "binding_kind,bound_by,created_at) VALUES (?,?,?,?,?,?,?)",
                        (
                            f"authorization-binding-{uuid.uuid4()}",
                            authorization["id"],
                            task_id,
                            run_id,
                            "run",
                            actor,
                            now,
                        ),
                    )
                pool_id = pool_by_worker.get(worker_id)
                if pool_id is not None:
                    connection.execute(
                        "INSERT INTO provider_capacity_reservations(run_id,pool_id,task_id,state,"
                        "reserved_at,released_at,release_reason) VALUES "
                        "(?,?,?,'reserved',?,NULL,NULL)",
                        (run_id, pool_id, task_id, now),
                    )
                if workers_by_id[worker_id]["state"] == WorkerState.IDLE.value:
                    self._append_event(
                        connection,
                        kind="workerStateChanged",
                        severity=EventSeverity.NOTICE,
                        entity_type="worker",
                        entity_id=worker_id,
                        project_id=task["project_id"],
                        task_id=task_id,
                        worker_id=worker_id,
                        run_id=run_id,
                        summary="Worker state idle -> starting",
                        payload={"from": WorkerState.IDLE.value, "to": "starting"},
                        actor=actor,
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
                    actor=actor,
                )
        return {
            "attempt": attempt,
            "runIDs": run_ids,
            "leaseGeneration": lease_generation,
            "verificationScopeID": verification_scope_id,
            "taskDefinitionRevision": task_definition_revision,
        }

    def heartbeat_task_execution_lease(
        self,
        task_id: str,
        *,
        owner_id: str,
        generation: int,
        ttl_seconds: float,
    ) -> bool:
        """Renew a live execution claim without allowing an expired owner to resurrect it."""

        if ttl_seconds < 1:
            raise ValueError("ttl_seconds must be at least one second")
        now_value = datetime.now(UTC)
        now = timestamp(now_value)
        expires_at = lease_expiry_timestamp(now_value, ttl_seconds)
        with self.transaction() as connection:
            cursor = connection.execute(
                "UPDATE task_execution_leases SET heartbeat_at=?,expires_at=? "
                "WHERE task_id=? AND owner_id=? AND generation=? AND state='active' "
                "AND expires_at>?",
                (now, expires_at, task_id, owner_id, generation, now),
            )
        return cursor.rowcount == 1

    def release_task_execution_lease(
        self,
        task_id: str,
        *,
        owner_id: str,
        generation: int,
        actor: str = "runtime",
    ) -> bool:
        """Release only the caller's current lease generation."""

        with self.transaction() as connection:
            lease = connection.execute(
                "SELECT * FROM task_execution_leases WHERE task_id=?",
                (task_id,),
            ).fetchone()
            if (
                lease is None
                or lease["owner_id"] != owner_id
                or int(lease["generation"]) != generation
                or lease["state"] != "active"
            ):
                return False
            now = timestamp()
            connection.execute(
                "UPDATE task_execution_leases SET state='released',released_at=?,"
                "heartbeat_at=? WHERE task_id=?",
                (now, now, task_id),
            )
            task = connection.execute(
                "SELECT project_id FROM tasks WHERE id=?", (task_id,)
            ).fetchone()
            if task is not None:
                self._append_event(
                    connection,
                    kind="taskExecutionLeaseReleased",
                    severity=EventSeverity.INFO,
                    entity_type="task",
                    entity_id=task_id,
                    project_id=task["project_id"],
                    task_id=task_id,
                    summary="Task execution ownership lease released",
                    payload={"ownerID": owner_id, "generation": generation},
                    actor=actor,
                )
        return True

    def activate_worker_run(
        self,
        run_id: str,
        *,
        lease_owner_id: str | None = None,
        lease_generation: int | None = None,
        actor: str = "runtime",
    ) -> bool:
        """Move STARTING execution and Worker to RUNNING only while its Task is active.

        This is the durable cancellation fence immediately before an adapter is invoked.
        """

        with self.transaction() as connection:
            run = connection.execute(
                "SELECT run.*,task.project_id,task.state AS task_state "
                "FROM worker_runs run JOIN tasks task ON task.id=run.task_id WHERE run.id=?",
                (run_id,),
            ).fetchone()
            if run is None:
                raise KeyError(run_id)
            if (lease_owner_id is None) != (lease_generation is None):
                raise ValueError("lease owner and generation must be supplied together")
            if lease_owner_id is not None and lease_generation is not None:
                self._require_task_execution_lease(
                    connection,
                    run["task_id"],
                    lease_owner_id,
                    lease_generation,
                )
            if (
                run["state"] != RunState.STARTING.value
                or run["task_state"] != TaskState.RUNNING.value
            ):
                return False
            worker = connection.execute(
                "SELECT state FROM workers WHERE id=?", (run["worker_id"],)
            ).fetchone()
            if worker is None:
                return False
            manifest = connection.execute(
                "SELECT manifest.valid_until,manifest.manifest_json "
                "FROM worker_capability_manifest_heads head "
                "JOIN worker_capability_manifests manifest ON manifest.id=head.manifest_id "
                "WHERE head.worker_id=?",
                (run["worker_id"],),
            ).fetchone()
            allowed_worker_states = {WorkerState.STARTING.value}
            if manifest is not None:
                allowed_worker_states.update(
                    {
                        WorkerState.RUNNING.value,
                        WorkerState.WAITING.value,
                    }
                )
                if manifest["valid_until"] is not None:
                    try:
                        expiry = _aware_timestamp(str(manifest["valid_until"]))
                    except ValueError:
                        return False
                    if expiry <= datetime.now(UTC):
                        return False
                try:
                    max_concurrency = json.loads(manifest["manifest_json"])["maxConcurrency"]
                except (KeyError, TypeError, ValueError):
                    return False
                active_count = int(
                    connection.execute(
                        "SELECT COUNT(*) FROM worker_runs WHERE worker_id=? "
                        "AND state IN ('starting','running','waiting')",
                        (run["worker_id"],),
                    ).fetchone()[0]
                )
                if (
                    isinstance(max_concurrency, bool)
                    or not isinstance(max_concurrency, int)
                    or max_concurrency < 1
                    or active_count > max_concurrency
                ):
                    return False
            if worker["state"] not in allowed_worker_states:
                return False
            now = timestamp()
            run_update = connection.execute(
                "UPDATE worker_runs SET state=?,started_at=COALESCE(started_at,?),"
                "last_event_at=?,updated_at=? WHERE id=? AND state=?",
                (
                    RunState.RUNNING.value,
                    now,
                    now,
                    now,
                    run_id,
                    RunState.STARTING.value,
                ),
            )
            if run_update.rowcount != 1:
                return False
            worker_update = connection.execute(
                "UPDATE workers SET state=?,last_heartbeat_at=?,updated_at=? "
                "WHERE id=? AND state=?",
                (
                    WorkerState.RUNNING.value,
                    now,
                    now,
                    run["worker_id"],
                    worker["state"],
                ),
            )
            if worker_update.rowcount != 1:
                raise RuntimeError("worker activation concurrent update")
            self._append_event(
                connection,
                kind="workerRunning",
                severity=EventSeverity.NOTICE,
                entity_type="workerRun",
                entity_id=run_id,
                project_id=run["project_id"],
                task_id=run["task_id"],
                worker_id=run["worker_id"],
                run_id=run_id,
                summary="Worker run starting -> running",
                payload={"from": RunState.STARTING.value, "to": RunState.RUNNING.value},
                actor=actor,
            )
            if worker["state"] != WorkerState.RUNNING.value:
                self._append_event(
                    connection,
                    kind="workerStateChanged",
                    severity=EventSeverity.NOTICE,
                    entity_type="worker",
                    entity_id=run["worker_id"],
                    project_id=run["project_id"],
                    task_id=run["task_id"],
                    worker_id=run["worker_id"],
                    run_id=run_id,
                    summary=f"Worker state {worker['state']} -> running",
                    payload={"from": worker["state"], "to": WorkerState.RUNNING.value},
                    actor=actor,
                )
        return True

    def cancel_task_execution(self, task_id: str, *, actor: str = "runtime") -> bool:
        """Fence a Task cancellation and every active canonical run in one transaction.

        Provider cancellation is requested separately by the Runtime.  Persisting canonical
        cancellation first prevents either a late local coroutine or a stale lease generation
        from turning an explicit user cancellation into success.
        """

        with self.transaction() as connection:
            task = connection.execute("SELECT * FROM tasks WHERE id=?", (task_id,)).fetchone()
            if task is None:
                raise KeyError(task_id)
            current = TaskState(task["state"])
            if current in {TaskState.SUCCEEDED, TaskState.FAILED, TaskState.CANCELLED}:
                return False
            TASK_TRANSITIONS.require(current, TaskState.CANCELLED)
            now = timestamp()
            active_runs = connection.execute(
                "SELECT * FROM worker_runs WHERE task_id=? "
                "AND state IN ('starting','running','waiting')",
                (task_id,),
            ).fetchall()
            connection.execute(
                "UPDATE tasks SET state=?,finished_at=?,updated_at=?,version=version+1 WHERE id=?",
                (TaskState.CANCELLED.value, now, now, task_id),
            )
            self._append_event(
                connection,
                kind="taskStateChanged",
                severity=EventSeverity.NOTICE,
                entity_type="task",
                entity_id=task_id,
                project_id=task["project_id"],
                task_id=task_id,
                summary="Task cancellation requested",
                payload={"from": current.value, "to": TaskState.CANCELLED.value},
                actor=actor,
            )
            for run in active_runs:
                connection.execute(
                    "UPDATE worker_runs SET state=?,ended_at=?,updated_at=? WHERE id=?",
                    (RunState.CANCELLED.value, now, now, run["id"]),
                )
                connection.execute(
                    "UPDATE provider_capacity_reservations SET state='released',released_at=?,"
                    "release_reason='taskCancelled' WHERE run_id=? AND state='reserved' AND ("
                    "NOT EXISTS (SELECT 1 FROM provider_jobs job WHERE job.run_id=?) OR "
                    "EXISTS (SELECT 1 FROM provider_jobs job WHERE job.run_id=? AND "
                    "(job.launch_state='prepared' OR job.result_collection_state='collected'))) ",
                    (now, run["id"], run["id"], run["id"]),
                )
                connection.execute(
                    "UPDATE provider_jobs SET launch_state="
                    "CASE WHEN launch_state='prepared' THEN 'terminal' ELSE 'uncertain' END,"
                    "result_collection_state="
                    "CASE WHEN launch_state='prepared' THEN 'notAvailable' ELSE 'uncertain' END,"
                    "updated_at=? WHERE run_id=? AND result_collection_state<>'collected'",
                    (now, run["id"]),
                )
                self._append_event(
                    connection,
                    kind="workerCancelled",
                    severity=EventSeverity.WARNING,
                    entity_type="workerRun",
                    entity_id=run["id"],
                    project_id=task["project_id"],
                    task_id=task_id,
                    worker_id=run["worker_id"],
                    run_id=run["id"],
                    summary="Worker run fenced by Task cancellation",
                    payload={
                        "from": run["state"],
                        "to": RunState.CANCELLED.value,
                        "providerCancellationPending": run["state"] != RunState.STARTING.value,
                    },
                    actor=actor,
                )
            for worker_id in sorted({str(run["worker_id"]) for run in active_runs}):
                connection.execute(
                    "UPDATE workers SET state=?,updated_at=? WHERE id=? AND NOT EXISTS ("
                    "SELECT 1 FROM worker_runs active WHERE active.worker_id=? "
                    "AND active.state IN ('starting','running','waiting')) "
                    "AND state IN ('starting','running','waiting','stopping')",
                    (
                        WorkerState.IDLE.value,
                        now,
                        worker_id,
                        worker_id,
                    ),
                )
            lease = connection.execute(
                "SELECT * FROM task_execution_leases WHERE task_id=? AND state='active'",
                (task_id,),
            ).fetchone()
            if lease is not None:
                connection.execute(
                    "UPDATE task_execution_leases SET state='released',released_at=?,"
                    "heartbeat_at=? WHERE task_id=?",
                    (now, now, task_id),
                )
                self._append_event(
                    connection,
                    kind="taskExecutionLeaseReleased",
                    severity=EventSeverity.INFO,
                    entity_type="task",
                    entity_id=task_id,
                    project_id=task["project_id"],
                    task_id=task_id,
                    summary="Task execution lease released by cancellation fence",
                    payload={
                        "ownerID": lease["owner_id"],
                        "generation": int(lease["generation"]),
                        "reasonCode": "TASK_CANCELLED",
                    },
                    actor=actor,
                )
        return True

    def worker_snapshots(self) -> list[WorkerSnapshot]:
        with self.connect() as connection:
            manifest_rows = connection.execute(
                "SELECT m.*,h.generation AS head_generation "
                "FROM worker_capability_manifest_heads h "
                "JOIN worker_capability_manifests m ON m.id=h.manifest_id"
            ).fetchall()
            manifests = {str(row["worker_id"]): row for row in manifest_rows}
            observations = {
                str(row["worker_id"]): row
                for row in connection.execute(
                    "SELECT o.* FROM worker_capability_observations o "
                    "JOIN worker_capability_manifest_heads h "
                    "ON h.worker_id=o.worker_id AND h.manifest_id=o.manifest_id "
                    "WHERE o.version=(SELECT MAX(latest.version) "
                    "FROM worker_capability_observations latest "
                    "WHERE latest.worker_id=o.worker_id AND latest.manifest_id=o.manifest_id)"
                ).fetchall()
            }
            execution_observations = {
                str(row["worker_id"]): row
                for row in connection.execute(
                    "SELECT observation.* FROM worker_execution_observations observation "
                    "WHERE observation.version=(SELECT MAX(latest.version) "
                    "FROM worker_execution_observations latest "
                    "WHERE latest.worker_id=observation.worker_id)"
                ).fetchall()
            }
            active_counts = {
                str(row["worker_id"]): int(row["active_count"])
                for row in connection.execute(
                    "SELECT worker_id,COUNT(*) AS active_count FROM worker_runs "
                    "WHERE state IN ('starting','running','waiting') GROUP BY worker_id"
                ).fetchall()
            }
        snapshots: list[WorkerSnapshot] = []
        for row in self.list_workers():
            provider = Provider(row["provider"])
            manifest = manifests.get(str(row["id"]))
            observation = observations.get(str(row["id"]))
            execution_observation = execution_observations.get(str(row["id"]))
            manifest_value = json.loads(manifest["manifest_json"]) if manifest is not None else {}
            dynamic_value = json.loads(observation["state_json"]) if observation is not None else {}
            claims = tuple(
                CapabilityClaim(
                    str(item["name"]),
                    item.get("parameters", {}),
                )
                for item in manifest_value.get("capabilities", [])
            )
            capabilities = (
                frozenset(claim.name for claim in claims)
                if manifest is not None
                else frozenset(json.loads(row["capabilities_json"]))
            )
            execution_disposition = None
            execution_rejection_code = None
            execution_reason_codes: tuple[str, ...] = ()
            if execution_observation is not None:
                from .fabric.execution_plane import (
                    EvidenceState,
                    PlatformApprovalState,
                    WorkerExecutionObservation,
                )

                execution_reason_codes = tuple(
                    str(value) for value in json.loads(execution_observation["reason_codes_json"])
                )
                evaluated = WorkerExecutionObservation(
                    worker_id=str(execution_observation["worker_id"]),
                    node_id=str(execution_observation["node_id"]),
                    binding_id=execution_observation["binding_id"],
                    discovered=EvidenceState(execution_observation["discovered"]),
                    configured=EvidenceState(execution_observation["configured"]),
                    authenticated=EvidenceState(execution_observation["authenticated"]),
                    authorized=EvidenceState(execution_observation["authorized"]),
                    platform_approval=PlatformApprovalState(
                        execution_observation["platform_approval"]
                    ),
                    reachable=EvidenceState(execution_observation["reachable"]),
                    runtime_available=EvidenceState(execution_observation["runtime_available"]),
                    healthy=EvidenceState(execution_observation["healthy"]),
                    capacity_available=EvidenceState(execution_observation["capacity_available"]),
                    observed_at=_aware_timestamp(execution_observation["observed_at"]),
                    valid_until=_aware_timestamp(execution_observation["valid_until"]),
                    protocol_version=execution_observation["protocol_version"],
                    reason_codes=execution_reason_codes,
                ).evaluate()
                execution_disposition = evaluated.disposition.value
                execution_rejection_code = evaluated.rejection_code
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
                    capabilities=capabilities,
                    code_write_allowed=bool(row["code_write_allowed"]),
                    privacy_allowed=bool(row["privacy_allowed"]),
                    quality_score=row["quality_score"],
                    reliability_score=row["reliability_score"],
                    expected_latency_seconds=row["expected_latency_seconds"],
                    monetary_cost_score=row["monetary_cost_score"],
                    running_tasks=active_counts.get(str(row["id"]), 0),
                    manifest_schema_version=(
                        str(manifest["schema_version"]) if manifest is not None else None
                    ),
                    capability_catalog_version=(
                        str(manifest["catalog_version"]) if manifest is not None else None
                    ),
                    manifest_revision=(int(manifest["revision"]) if manifest is not None else None),
                    manifest_digest=(
                        str(manifest["definition_sha256"]) if manifest is not None else None
                    ),
                    manifest_valid_until=(
                        _aware_timestamp(str(manifest["valid_until"]))
                        if manifest is not None and manifest["valid_until"] is not None
                        else None
                    ),
                    cost_mode=CostMode(manifest_value.get("costMode", "unknown")),
                    subscription_state=SubscriptionState(
                        dynamic_value.get("subscriptionState", "unknown")
                    ),
                    incremental_cost_usd=manifest_value.get("incrementalCostUSD"),
                    quota_state=QuotaAvailability(dynamic_value.get("quota", "unknown")),
                    quota_freshness=ObservationFreshness(
                        dynamic_value.get("quotaFreshness", "unknown")
                    ),
                    locality=WorkerLocality(manifest_value.get("locality", "unknown")),
                    privacy=WorkerPrivacy(manifest_value.get("privacy", "unknown")),
                    health=WorkerHealth(dynamic_value.get("health", "unknown")),
                    health_freshness=ObservationFreshness(
                        dynamic_value.get("healthFreshness", "unknown")
                    ),
                    worker_load=dynamic_value.get("load"),
                    max_concurrency=int(manifest_value.get("maxConcurrency", 1)),
                    capability_claims=claims,
                    execution_observation_id=(
                        str(execution_observation["id"])
                        if execution_observation is not None
                        else None
                    ),
                    execution_schema_version=(
                        str(execution_observation["schema_version"])
                        if execution_observation is not None
                        else None
                    ),
                    execution_disposition=execution_disposition,
                    execution_rejection_code=execution_rejection_code,
                    execution_reason_codes=execution_reason_codes,
                    worker_classes=frozenset(
                        manifest_value.get(
                            "workerClasses",
                            json.loads(row.get("worker_classes_json") or "[]"),
                        )
                    ),
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
        lease_owner_id: str | None = None,
        lease_generation: int | None = None,
    ) -> dict[str, Any]:
        with self.transaction() as connection:
            row = connection.execute("SELECT * FROM tasks WHERE id = ?", (task_id,)).fetchone()
            if row is None:
                raise KeyError(task_id)
            if (lease_owner_id is None) != (lease_generation is None):
                raise ValueError("lease owner and generation must be supplied together")
            if lease_owner_id is not None and lease_generation is not None:
                self._require_task_execution_lease(
                    connection,
                    task_id,
                    lease_owner_id,
                    lease_generation,
                )
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
        selected_json = compact_json(list(decision.selected_worker_ids))
        explanation_json = compact_json(decision.explanation)
        with self.transaction() as connection:
            task = connection.execute(
                "SELECT project_id FROM tasks WHERE id = ?", (task_id,)
            ).fetchone()
            if task is None:
                raise KeyError(task_id)
            previous = connection.execute(
                "SELECT id,topology,policy_version,selected_workers_json,explanation_json "
                "FROM routing_decisions WHERE task_id=? ORDER BY created_at DESC,id DESC LIMIT 1",
                (task_id,),
            ).fetchone()
            if previous is not None and (
                previous["topology"] == decision.topology.value
                and previous["policy_version"] == decision.policy_version
                and previous["selected_workers_json"] == selected_json
                and previous["explanation_json"] == explanation_json
            ):
                return str(previous["id"])
            connection.execute(
                "INSERT INTO routing_decisions(id,task_id,topology,policy_version,"
                "selected_workers_json,explanation_json,created_at) VALUES (?,?,?,?,?,?,?)",
                (
                    decision_id,
                    task_id,
                    decision.topology.value,
                    decision.policy_version,
                    selected_json,
                    explanation_json,
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
            verification_scope_id, task_definition_revision = (
                self._task_verification_dispatch_snapshot(connection, task)
            )
            connection.execute(
                """
                INSERT INTO worker_runs(
                    id,task_id,worker_id,state,attempt,timeout_at,verification_scope_id,
                    task_definition_revision,created_at,updated_at
                ) VALUES (?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    run_id,
                    task_id,
                    worker_id,
                    RunState.STARTING.value,
                    attempt,
                    timestamp(timeout_at) if timeout_at else None,
                    verification_scope_id,
                    task_definition_revision,
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
        lease_owner_id: str | None = None,
        lease_generation: int | None = None,
    ) -> dict[str, Any]:
        with self.transaction() as connection:
            row = connection.execute("SELECT * FROM worker_runs WHERE id=?", (run_id,)).fetchone()
            if row is None:
                raise KeyError(run_id)
            if (lease_owner_id is None) != (lease_generation is None):
                raise ValueError("lease owner and generation must be supplied together")
            if lease_owner_id is not None and lease_generation is not None:
                self._require_task_execution_lease(
                    connection,
                    row["task_id"],
                    lease_owner_id,
                    lease_generation,
                )
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
                    redact_sensitive(failure_detail),
                    raw_output_reference,
                    now,
                    run_id,
                ),
            )
            if target in terminal:
                connection.execute(
                    "UPDATE provider_capacity_reservations SET state='released',released_at=?,"
                    "release_reason=? WHERE run_id=? AND state='reserved' AND ("
                    "NOT EXISTS (SELECT 1 FROM provider_jobs job WHERE job.run_id=?) OR "
                    "EXISTS (SELECT 1 FROM provider_jobs job WHERE job.run_id=? AND "
                    "(job.launch_state='prepared' OR job.result_collection_state='collected'))) ",
                    (now, f"runTerminal:{target.value}", run_id, run_id, run_id),
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

    def prepare_provider_job(
        self,
        *,
        run_id: str,
        adapter_type: str,
        adapter_instance_id: str,
        capabilities: Mapping[str, bool | int],
        lease_owner_id: str,
        lease_generation: int,
        actor: str = "runtime",
    ) -> dict[str, Any]:
        """Persist a fenced execution identity before invoking an external adapter.

        The durable idempotency key belongs to the canonical Worker run, not to a Runtime
        process.  Replaying this method for the same run is therefore harmless; changing the
        adapter contract after preparation is rejected.
        """

        if not adapter_type.strip():
            raise ValueError("adapter_type must not be empty")
        if not adapter_instance_id.strip():
            raise ValueError("adapter_instance_id must not be empty")
        protocol_version = int(capabilities.get("protocol_version", 1))
        if protocol_version < 1:
            raise ValueError("provider job protocol_version must be positive")
        supported = {
            "supports_reconcile": bool(capabilities.get("supports_reconcile", False)),
            "supports_resume": bool(capabilities.get("supports_resume", False)),
            "supports_cancel": bool(capabilities.get("supports_cancel", False)),
            "supports_durable_cancel": bool(capabilities.get("supports_durable_cancel", False)),
            "supports_provider_idempotency": bool(
                capabilities.get("supports_provider_idempotency", False)
            ),
            "supports_stream_reconnect": bool(capabilities.get("supports_stream_reconnect", False)),
            "supports_repeatable_collect": bool(
                capabilities.get("supports_repeatable_collect", False)
            ),
            "supports_idempotent_launch_lookup": bool(
                capabilities.get("supports_idempotent_launch_lookup", False)
            ),
            "supports_durable_launch_registry": bool(
                capabilities.get("supports_durable_launch_registry", False)
            ),
        }
        with self.transaction() as connection:
            run = connection.execute(
                "SELECT run.*,task.project_id,worker.provider "
                "FROM worker_runs run JOIN tasks task ON task.id=run.task_id "
                "JOIN workers worker ON worker.id=run.worker_id WHERE run.id=?",
                (run_id,),
            ).fetchone()
            if run is None:
                raise KeyError(run_id)
            self._require_task_execution_lease(
                connection,
                run["task_id"],
                lease_owner_id,
                lease_generation,
            )
            existing = connection.execute(
                "SELECT * FROM provider_jobs WHERE run_id=?", (run_id,)
            ).fetchone()
            if existing is not None:
                expected = {
                    "adapter_type": adapter_type,
                    "adapter_instance_id": adapter_instance_id,
                    "launch_generation": lease_generation,
                    "protocol_version": protocol_version,
                    **{key: int(value) for key, value in supported.items()},
                }
                if all(existing[key] == value for key, value in expected.items()):
                    return dict(existing)
                raise RuntimeError(f"provider job intent for {run_id} is immutable")

            job_id = f"provider-job-{uuid.uuid4()}"
            idempotency_key = f"supervisor-execution:{run_id}"
            now = timestamp()
            connection.execute(
                "INSERT INTO provider_jobs("
                "id,run_id,task_id,worker_id,adapter_type,adapter_instance_id,provider,"
                "launch_generation,"
                "idempotency_key,launch_state,reconciliation_state,result_collection_state,"
                "supports_reconcile,supports_resume,supports_cancel,supports_durable_cancel,"
                "supports_provider_idempotency,supports_stream_reconnect,"
                "supports_repeatable_collect,supports_idempotent_launch_lookup,"
                "supports_durable_launch_registry,protocol_version,created_at,updated_at) "
                "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    job_id,
                    run_id,
                    run["task_id"],
                    run["worker_id"],
                    adapter_type,
                    adapter_instance_id,
                    run["provider"],
                    lease_generation,
                    idempotency_key,
                    "prepared",
                    "unknown",
                    "pending",
                    *(int(value) for value in supported.values()),
                    protocol_version,
                    now,
                    now,
                ),
            )
            self._append_event(
                connection,
                kind="providerJobPrepared",
                severity=EventSeverity.INFO,
                entity_type="providerJob",
                entity_id=job_id,
                project_id=run["project_id"],
                task_id=run["task_id"],
                worker_id=run["worker_id"],
                run_id=run_id,
                summary="Durable provider execution identity prepared",
                payload={
                    "providerJobID": job_id,
                    "adapterType": adapter_type,
                    "protocolVersion": protocol_version,
                    "launchGeneration": lease_generation,
                    "idempotencyFingerprint": hashlib.sha256(
                        idempotency_key.encode("utf-8")
                    ).hexdigest()[:16],
                    "capabilities": supported,
                },
                actor=actor,
            )
        return self.get_provider_job(run_id)

    def mark_provider_job_launching(
        self,
        run_id: str,
        *,
        lease_owner_id: str,
        lease_generation: int,
        actor: str = "runtime",
    ) -> dict[str, Any]:
        """Record the last safe checkpoint before an adapter launch side effect."""

        with self.transaction() as connection:
            job = self._provider_job_for_update(connection, run_id)
            self._require_task_execution_lease(
                connection, job["task_id"], lease_owner_id, lease_generation
            )
            if job["launch_state"] in {"launching", "bound", "terminal", "uncertain"}:
                return dict(job)
            if job["launch_state"] != "prepared":
                raise RuntimeError(f"provider job {job['id']} cannot be launched")
            now = timestamp()
            connection.execute(
                "UPDATE provider_jobs SET launch_state='launching',launched_at=?,updated_at=? "
                "WHERE id=?",
                (now, now, job["id"]),
            )
            task = connection.execute(
                "SELECT project_id FROM tasks WHERE id=?", (job["task_id"],)
            ).fetchone()
            self._append_event(
                connection,
                kind="providerJobLaunchStarted",
                severity=EventSeverity.INFO,
                entity_type="providerJob",
                entity_id=job["id"],
                project_id=task["project_id"],
                task_id=job["task_id"],
                worker_id=job["worker_id"],
                run_id=run_id,
                summary="Provider job launch boundary entered",
                payload={"providerJobID": job["id"], "launchGeneration": job["launch_generation"]},
                actor=actor,
            )
        return self.get_provider_job(run_id)

    def bind_provider_job_handle(
        self,
        *,
        run_id: str,
        adapter_type: str,
        adapter_instance_id: str,
        handle_version: int,
        provider_job_id: str | None,
        provider_session_id: str | None,
        runtime_pid: int | None,
        runtime_host: str | None,
        runtime_identity: str | None,
        adapter_metadata: Mapping[str, Any],
        lease_owner_id: str,
        lease_generation: int,
        actor: str = "runtime",
    ) -> dict[str, Any]:
        """Bind a provider/runtime handle while rejecting PID-only process identity."""

        if handle_version < 1:
            raise ValueError("provider job handle_version must be positive")
        if not provider_job_id and runtime_pid is None:
            raise ValueError("provider job handle requires provider or runtime identity")
        if runtime_pid is not None and (not runtime_host or not runtime_identity):
            raise ValueError("runtime PID requires host and non-PID process identity")
        safe_metadata = compact_json(redact_sensitive(dict(adapter_metadata)))
        with self.transaction() as connection:
            job = self._provider_job_for_update(connection, run_id)
            self._require_task_execution_lease(
                connection, job["task_id"], lease_owner_id, lease_generation
            )
            if (
                job["adapter_type"] != adapter_type
                or job["adapter_instance_id"] != adapter_instance_id
            ):
                raise RuntimeError("provider job handle came from a different adapter instance")
            values = {
                "handle_version": handle_version,
                # These are canonical lookup identities, not display strings. Mutating an opaque
                # identifier during redaction can make reconciliation query a different job.
                # Public projections omit or redact them independently.
                "provider_job_id": provider_job_id,
                "provider_session_id": provider_session_id,
                "runtime_pid": runtime_pid,
                "runtime_host": runtime_host,
                "runtime_identity": runtime_identity,
                "adapter_metadata_json": safe_metadata,
            }
            if job["launch_state"] == "bound":
                if all(job[key] == value for key, value in values.items()):
                    return dict(job)
                raise RuntimeError(f"provider job handle for {run_id} is immutable")
            bindable_states = {"prepared", "launching"}
            if (
                job["launch_state"] == "uncertain"
                and not job["provider_job_id"]
                and bool(job["supports_provider_idempotency"])
            ):
                bindable_states.add("uncertain")
            if job["launch_state"] not in bindable_states:
                raise RuntimeError(f"provider job {job['id']} cannot bind a handle")
            now = timestamp()
            connection.execute(
                "UPDATE provider_jobs SET handle_version=?,provider_job_id=?,"
                "provider_session_id=?,runtime_pid=?,runtime_host=?,runtime_identity=?,"
                "adapter_metadata_json=?,launch_state='bound',launched_at=COALESCE(launched_at,?),"
                "updated_at=? WHERE id=?",
                (
                    *values.values(),
                    now,
                    now,
                    job["id"],
                ),
            )
            task = connection.execute(
                "SELECT project_id FROM tasks WHERE id=?", (job["task_id"],)
            ).fetchone()
            self._append_event(
                connection,
                kind="providerJobHandleBound",
                severity=EventSeverity.NOTICE,
                entity_type="providerJob",
                entity_id=job["id"],
                project_id=task["project_id"],
                task_id=job["task_id"],
                worker_id=job["worker_id"],
                run_id=run_id,
                summary="Durable provider job handle bound",
                payload={
                    "providerJobID": job["id"],
                    "hasProviderIdentity": bool(provider_job_id),
                    "hasRuntimeIdentity": runtime_pid is not None,
                    "handleVersion": handle_version,
                },
                actor=actor,
            )
        return self.get_provider_job(run_id)

    def record_provider_job_observation(
        self,
        run_id: str,
        *,
        state: str,
        provider_status: str | None = None,
        detail: str | None = None,
        lease_owner_id: str,
        lease_generation: int,
        actor: str = "recovery",
    ) -> dict[str, Any]:
        allowed = {
            "unknown",
            "knownRunning",
            "knownCompleted",
            "knownFailed",
            "knownCancelled",
            "providerNotFound",
            "providerUnreachable",
        }
        if state not in allowed:
            raise ValueError(f"unsupported provider reconciliation state: {state}")
        with self.transaction() as connection:
            job = self._provider_job_for_update(connection, run_id)
            self._require_task_execution_lease(
                connection, job["task_id"], lease_owner_id, lease_generation
            )
            if job["result_collection_state"] == "collected":
                # Canonical collection is a monotonic terminal boundary. Late provider polls may
                # be stale or temporarily unreachable and cannot reopen the execution.
                return dict(job)
            now = timestamp()
            launch_state = (
                "terminal"
                if state in {"knownCompleted", "knownFailed", "knownCancelled"}
                else "uncertain"
                if state in {"unknown", "providerUnreachable"}
                else "bound"
                if state == "knownRunning" and job["provider_job_id"]
                else job["launch_state"]
            )
            connection.execute(
                "UPDATE provider_jobs SET reconciliation_state=?,launch_state=?,"
                "last_reconciled_at=?,updated_at=? WHERE id=?",
                (state, launch_state, now, now, job["id"]),
            )
            if state in {
                "knownCompleted",
                "knownFailed",
                "knownCancelled",
                "providerNotFound",
            }:
                connection.execute(
                    "UPDATE provider_capacity_reservations SET state='released',released_at=?,"
                    "release_reason=? WHERE run_id=? AND state='reserved'",
                    (now, f"providerObserved:{state}", run_id),
                )
            if job["reconciliation_state"] != state:
                task = connection.execute(
                    "SELECT project_id FROM tasks WHERE id=?", (job["task_id"],)
                ).fetchone()
                self._append_event(
                    connection,
                    kind="providerJobReconciled",
                    severity=(
                        EventSeverity.WARNING
                        if state in {"unknown", "providerUnreachable", "providerNotFound"}
                        else EventSeverity.NOTICE
                    ),
                    entity_type="providerJob",
                    entity_id=job["id"],
                    project_id=task["project_id"],
                    task_id=job["task_id"],
                    worker_id=job["worker_id"],
                    run_id=run_id,
                    summary=f"Provider job reconciled as {state}",
                    payload={
                        "providerJobID": job["id"],
                        "state": state,
                        "providerStatus": redact_sensitive(provider_status),
                        "detail": redact_sensitive(detail),
                    },
                    actor=actor,
                )
        return self.get_provider_job(run_id)

    def mark_provider_job_result_collected(
        self,
        run_id: str,
        *,
        terminal_state: str,
        lease_owner_id: str,
        lease_generation: int,
        actor: str = "runtime",
    ) -> dict[str, Any]:
        mapping = {
            RunState.COMPLETED.value: "knownCompleted",
            RunState.FAILED.value: "knownFailed",
            RunState.CANCELLED.value: "knownCancelled",
            RunState.TIMED_OUT.value: "knownFailed",
            RunState.AUTH_REQUIRED.value: "knownFailed",
            RunState.RATE_LIMITED.value: "knownFailed",
        }
        reconciliation_state = mapping.get(terminal_state)
        if reconciliation_state is None:
            raise ValueError("provider result collection requires a terminal Worker run state")
        with self.transaction() as connection:
            job = self._provider_job_for_update(connection, run_id)
            self._require_task_execution_lease(
                connection, job["task_id"], lease_owner_id, lease_generation
            )
            if job["result_collection_state"] == "collected":
                return dict(job)
            now = timestamp()
            connection.execute(
                "UPDATE provider_jobs SET launch_state='terminal',reconciliation_state=?,"
                "result_collection_state='collected',result_collected_at=?,"
                "last_reconciled_at=COALESCE(last_reconciled_at,?),updated_at=? WHERE id=?",
                (reconciliation_state, now, now, now, job["id"]),
            )
            task = connection.execute(
                "SELECT project_id FROM tasks WHERE id=?", (job["task_id"],)
            ).fetchone()
            self._append_event(
                connection,
                kind="providerJobResultCollected",
                severity=EventSeverity.NOTICE,
                entity_type="providerJob",
                entity_id=job["id"],
                project_id=task["project_id"],
                task_id=job["task_id"],
                worker_id=job["worker_id"],
                run_id=run_id,
                summary="Provider job result collected into canonical state",
                payload={"providerJobID": job["id"], "state": reconciliation_state},
                actor=actor,
            )
        return self.get_provider_job(run_id)

    def finalize_provider_job_result(
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
        worker_state: WorkerState = WorkerState.IDLE,
        resource_state: ResourceState | None = None,
        model_id: str | None = None,
        lease_owner_id: str,
        lease_generation: int,
        actor: str = "runtime",
    ) -> dict[str, Any]:
        """Atomically terminalize a run and mark its durable result collected.

        ``save_worker_result`` remains the immutable payload insert.  If a crash happens after
        that insert, replaying collection reaches this transaction without emitting a duplicate
        result event.  A persisted Task cancellation wins: this method records the provider's
        terminal observation but never changes the Task state.
        """

        terminal = {
            RunState.COMPLETED,
            RunState.FAILED,
            RunState.CANCELLED,
            RunState.TIMED_OUT,
            RunState.AUTH_REQUIRED,
            RunState.RATE_LIMITED,
        }
        if target not in terminal:
            raise ValueError("provider result must map to a terminal Worker run state")
        observation = {
            RunState.COMPLETED: "knownCompleted",
            RunState.CANCELLED: "knownCancelled",
        }.get(target, "knownFailed")
        with self.transaction() as connection:
            job = self._provider_job_for_update(connection, run_id)
            self._require_task_execution_lease(
                connection, job["task_id"], lease_owner_id, lease_generation
            )
            run = connection.execute("SELECT * FROM worker_runs WHERE id=?", (run_id,)).fetchone()
            if run is None:
                raise KeyError(run_id)
            if (
                connection.execute(
                    "SELECT 1 FROM worker_results WHERE run_id=?", (run_id,)
                ).fetchone()
                is None
            ):
                raise RuntimeError("canonical Worker result must be saved before finalization")
            current = RunState(run["state"])
            task = connection.execute(
                "SELECT project_id,state FROM tasks WHERE id=?", (run["task_id"],)
            ).fetchone()
            cancellation_wins = (
                current is RunState.CANCELLED and task["state"] == TaskState.CANCELLED.value
            )
            if current in terminal and current is not target and not cancellation_wins:
                raise RuntimeError(
                    f"provider result conflicts with terminal run {run_id}: "
                    f"{current.value} != {target.value}"
                )
            if current not in terminal:
                RUN_TRANSITIONS.require(current, target)
            now = timestamp()
            if current is not target and not cancellation_wins:
                connection.execute(
                    "UPDATE worker_runs SET state=?,process_id=COALESCE(?,process_id),"
                    "session_id=COALESCE(?,session_id),last_event_at=?,ended_at=?,"
                    "exit_code=COALESCE(?,exit_code),failure_class=COALESCE(?,failure_class),"
                    "failure_detail=COALESCE(?,failure_detail),"
                    "raw_output_reference=COALESCE(?,raw_output_reference),updated_at=? WHERE id=?",
                    (
                        target.value,
                        process_id,
                        session_id,
                        now,
                        now,
                        exit_code,
                        failure_class.value if failure_class else None,
                        redact_sensitive(failure_detail),
                        raw_output_reference,
                        now,
                        run_id,
                    ),
                )
                severity = (
                    EventSeverity.NOTICE if target is RunState.COMPLETED else EventSeverity.WARNING
                )
                kind = {
                    RunState.COMPLETED: "workerCompleted",
                    RunState.CANCELLED: "workerCancelled",
                    RunState.TIMED_OUT: "workerTimedOut",
                    RunState.AUTH_REQUIRED: "workerAuthRequired",
                    RunState.RATE_LIMITED: "workerRateLimited",
                }.get(target, "workerFailed")
                self._append_event(
                    connection,
                    kind=kind,
                    severity=severity,
                    entity_type="workerRun",
                    entity_id=run_id,
                    project_id=task["project_id"],
                    task_id=run["task_id"],
                    worker_id=run["worker_id"],
                    run_id=run_id,
                    summary=f"Worker run {current.value} -> {target.value}",
                    payload={"from": current.value, "to": target.value},
                    actor=actor,
                )
            connection.execute(
                "UPDATE provider_capacity_reservations SET state='released',released_at=?,"
                "release_reason=? WHERE run_id=? AND state='reserved'",
                (now, f"providerFinalized:{target.value}", run_id),
            )
            # A cancelled run relinquished Worker ownership when cancellation became canonical.
            # Its later provider observation must not overwrite a health/admin state or a new
            # assignment. Non-cancelled finalization may release the Worker only when no peer run
            # currently owns it.
            if not cancellation_wins and job["result_collection_state"] != "collected":
                worker = connection.execute(
                    "SELECT state FROM workers WHERE id=?", (run["worker_id"],)
                ).fetchone()
                worker_update = connection.execute(
                    "UPDATE workers SET state=?,resource_state=COALESCE(?,resource_state),"
                    "model_id=COALESCE(?,model_id),last_heartbeat_at=?,updated_at=? WHERE id=? "
                    "AND NOT EXISTS (SELECT 1 FROM worker_runs active "
                    "WHERE active.worker_id=? AND active.id<>? "
                    "AND active.state IN ('starting','running','waiting'))",
                    (
                        worker_state.value,
                        resource_state.value if resource_state else None,
                        model_id,
                        now,
                        now,
                        run["worker_id"],
                        run["worker_id"],
                        run_id,
                    ),
                )
                if (
                    worker_update.rowcount == 1
                    and worker is not None
                    and worker["state"] != worker_state.value
                ):
                    self._append_event(
                        connection,
                        kind="workerStateChanged",
                        severity=EventSeverity.NOTICE,
                        entity_type="worker",
                        entity_id=run["worker_id"],
                        worker_id=run["worker_id"],
                        task_id=run["task_id"],
                        run_id=run_id,
                        summary=f"Worker state {worker['state']} -> {worker_state.value}",
                        payload={"from": worker["state"], "to": worker_state.value},
                        actor=actor,
                    )
            if job["result_collection_state"] != "collected":
                connection.execute(
                    "UPDATE provider_jobs SET launch_state='terminal',reconciliation_state=?,"
                    "result_collection_state='collected',result_collected_at=?,"
                    "last_reconciled_at=COALESCE(last_reconciled_at,?),updated_at=? WHERE id=?",
                    (observation, now, now, now, job["id"]),
                )
                self._append_event(
                    connection,
                    kind="providerJobResultCollected",
                    severity=EventSeverity.NOTICE,
                    entity_type="providerJob",
                    entity_id=job["id"],
                    project_id=task["project_id"],
                    task_id=run["task_id"],
                    worker_id=run["worker_id"],
                    run_id=run_id,
                    summary="Provider job result collected into canonical state",
                    payload={"providerJobID": job["id"], "state": observation},
                    actor=actor,
                )
        return self.get_worker_run(run_id)

    @staticmethod
    def _provider_job_for_update(connection: sqlite3.Connection, run_id: str) -> sqlite3.Row:
        job = connection.execute("SELECT * FROM provider_jobs WHERE run_id=?", (run_id,)).fetchone()
        if job is None:
            raise KeyError(run_id)
        return job

    def get_provider_job(self, run_id: str) -> dict[str, Any]:
        with self.connect() as connection:
            row = connection.execute(
                "SELECT * FROM provider_jobs WHERE run_id=?", (run_id,)
            ).fetchone()
        if row is None:
            raise KeyError(run_id)
        return dict(row)

    def list_provider_jobs(
        self, *, task_id: str | None = None, reconcilable_only: bool = False
    ) -> list[dict[str, Any]]:
        clauses: list[str] = []
        parameters: list[Any] = []
        if task_id is not None:
            clauses.append("job.task_id=?")
            parameters.append(task_id)
        if reconcilable_only:
            clauses.extend(
                [
                    "((run.state IN ('starting','running','waiting') "
                    "AND task.state IN ('running','waiting')) OR "
                    "(run.state='cancelled' AND task.state='cancelled' "
                    "AND job.result_collection_state IN ('pending','uncertain')))",
                    "(lease.task_id IS NULL OR lease.state<>'active' OR lease.expires_at<=?)",
                    "job.launch_state<>'prepared'",
                ]
            )
            parameters.append(timestamp())
        query = (
            "SELECT job.*,run.state AS run_state,run.attempt,task.state AS task_state,"
            "task.project_id,task.topology,worker.harness "
            "FROM provider_jobs job JOIN worker_runs run ON run.id=job.run_id "
            "JOIN tasks task ON task.id=job.task_id JOIN workers worker ON worker.id=job.worker_id "
            "LEFT JOIN task_execution_leases lease ON lease.task_id=job.task_id"
        )
        if clauses:
            query += " WHERE " + " AND ".join(clauses)
        query += " ORDER BY job.created_at,job.id"
        with self.connect() as connection:
            return [dict(row) for row in connection.execute(query, parameters).fetchall()]

    def claim_task_reconciliation(
        self,
        task_id: str,
        *,
        owner_id: str,
        lease_ttl_seconds: float,
        allow_cancelled: bool = False,
        actor: str = "recovery",
    ) -> int | None:
        """Take a new lease generation for an orphaned durable provider job."""

        if not owner_id.strip():
            raise ValueError("reconciliation owner_id must not be empty")
        if lease_ttl_seconds < 1:
            raise ValueError("lease_ttl_seconds must be at least one second")
        now_value = datetime.now(UTC)
        now = timestamp(now_value)
        expires_at = lease_expiry_timestamp(now_value, lease_ttl_seconds)
        with self.transaction() as connection:
            task = connection.execute("SELECT * FROM tasks WHERE id=?", (task_id,)).fetchone()
            if task is None:
                raise KeyError(task_id)
            allowed_task_states = {TaskState.RUNNING.value, TaskState.WAITING.value}
            if allow_cancelled:
                allowed_task_states.add(TaskState.CANCELLED.value)
            if task["state"] not in allowed_task_states:
                return None
            lease = connection.execute(
                "SELECT * FROM task_execution_leases WHERE task_id=?", (task_id,)
            ).fetchone()
            if lease is not None and lease["state"] == "active" and lease["expires_at"] > now:
                return None
            generation = int(lease["generation"]) + 1 if lease is not None else 1
            connection.execute(
                "INSERT INTO task_execution_leases(task_id,owner_id,generation,state,acquired_at,"
                "heartbeat_at,expires_at,released_at) VALUES (?,?,?,'active',?,?,?,NULL) "
                "ON CONFLICT(task_id) DO UPDATE SET owner_id=excluded.owner_id,"
                "generation=excluded.generation,state='active',acquired_at=excluded.acquired_at,"
                "heartbeat_at=excluded.heartbeat_at,expires_at=excluded.expires_at,released_at=NULL",
                (task_id, owner_id, generation, now, now, expires_at),
            )
            self._append_event(
                connection,
                kind="taskExecutionLeaseAcquired",
                severity=EventSeverity.INFO,
                entity_type="task",
                entity_id=task_id,
                project_id=task["project_id"],
                task_id=task_id,
                summary="Task execution lease acquired for provider reconciliation",
                payload={
                    "ownerID": owner_id,
                    "generation": generation,
                    "expiresAt": expires_at,
                    "purpose": (
                        "providerCancellationReconciliation"
                        if task["state"] == TaskState.CANCELLED.value
                        else "providerReconciliation"
                    ),
                },
                actor=actor,
            )
        return generation

    def interrupt_missing_provider_job(
        self,
        run_id: str,
        *,
        lease_owner_id: str,
        lease_generation: int,
        actor: str = "recovery",
    ) -> None:
        """Requeue only after a provider definitively reports that the bound job is absent."""

        with self.transaction() as connection:
            job = self._provider_job_for_update(connection, run_id)
            self._require_task_execution_lease(
                connection, job["task_id"], lease_owner_id, lease_generation
            )
            run = connection.execute("SELECT * FROM worker_runs WHERE id=?", (run_id,)).fetchone()
            task = connection.execute(
                "SELECT * FROM tasks WHERE id=?", (job["task_id"],)
            ).fetchone()
            if run["state"] not in {
                RunState.STARTING.value,
                RunState.RUNNING.value,
                RunState.WAITING.value,
            }:
                return
            now = timestamp()
            connection.execute(
                "UPDATE worker_runs SET state=?,ended_at=?,failure_class=?,failure_detail=?,"
                "updated_at=? WHERE id=?",
                (
                    RunState.INTERRUPTED.value,
                    now,
                    FailureClass.INFRASTRUCTURE.value,
                    "provider definitively reported job not found",
                    now,
                    run_id,
                ),
            )
            connection.execute(
                "UPDATE workers SET state=?,updated_at=? WHERE id=? AND NOT EXISTS ("
                "SELECT 1 FROM worker_runs active WHERE active.worker_id=? AND active.id<>? "
                "AND active.state IN ('starting','running','waiting'))",
                (WorkerState.IDLE.value, now, run["worker_id"], run["worker_id"], run_id),
            )
            target = (
                TaskState.INTERRUPTED
                if task["state"] == TaskState.RUNNING.value
                else TaskState.READY
            )
            connection.execute(
                "UPDATE tasks SET state=?,updated_at=?,version=version+1 WHERE id=?",
                (target.value, now, task["id"]),
            )
            connection.execute(
                "UPDATE provider_jobs SET launch_state='terminal',"
                "reconciliation_state='providerNotFound',result_collection_state='notAvailable',"
                "last_reconciled_at=?,updated_at=? WHERE id=?",
                (now, now, job["id"]),
            )
            self._append_event(
                connection,
                kind="workerRunInterrupted",
                severity=EventSeverity.WARNING,
                entity_type="workerRun",
                entity_id=run_id,
                project_id=task["project_id"],
                task_id=task["id"],
                worker_id=run["worker_id"],
                run_id=run_id,
                summary="Worker run interrupted after provider reported job missing",
                payload={"previousState": run["state"], "reasonCode": "PROVIDER_NOT_FOUND"},
                actor=actor,
            )
            self._append_event(
                connection,
                kind="taskRecovered",
                severity=EventSeverity.WARNING,
                entity_type="task",
                entity_id=task["id"],
                project_id=task["project_id"],
                task_id=task["id"],
                summary="Task made retryable after provider confirmed job absence",
                payload={"from": task["state"], "to": target.value},
                actor=actor,
            )

    def request_execution_escalation(
        self,
        *,
        run_id: str,
        code: str,
        summary: str,
        detail: str | None,
        lease_owner_id: str,
        lease_generation: int,
        actor: str = "recovery",
    ) -> str:
        allowed = {
            "PROVIDER_STATE_AMBIGUOUS",
            "EXTERNAL_JOB_UNREACHABLE",
            "IDEMPOTENCY_UNCERTAIN",
            "RESUME_UNSUPPORTED",
            "RESULT_COLLECTION_UNCERTAIN",
        }
        if code not in allowed:
            raise ValueError(f"unsupported execution escalation code: {code}")
        safe_summary = str(redact_sensitive(summary))
        safe_detail = str(redact_sensitive(detail)) if detail else None
        with self.transaction() as connection:
            job = self._provider_job_for_update(connection, run_id)
            self._require_task_execution_lease(
                connection, job["task_id"], lease_owner_id, lease_generation
            )
            existing = connection.execute(
                "SELECT id FROM execution_escalations WHERE provider_job_id=? AND code=? "
                "AND state='open'",
                (job["id"], code),
            ).fetchone()
            if existing is not None:
                return str(existing["id"])
            task = connection.execute(
                "SELECT project_id FROM tasks WHERE id=?", (job["task_id"],)
            ).fetchone()
            goal = connection.execute(
                "SELECT goal_id FROM autonomous_actions WHERE task_id=? "
                "ORDER BY created_at DESC,id DESC LIMIT 1",
                (job["task_id"],),
            ).fetchone()
            escalation_id = f"escalation-{uuid.uuid4()}"
            now = timestamp()
            connection.execute(
                "INSERT INTO execution_escalations(id,project_id,goal_id,task_id,run_id,"
                "provider_job_id,code,state,summary,detail,created_by,created_at,updated_at) "
                "VALUES (?,?,?,?,?,?,?,'open',?,?,?,?,?)",
                (
                    escalation_id,
                    task["project_id"],
                    goal["goal_id"] if goal is not None else None,
                    job["task_id"],
                    run_id,
                    job["id"],
                    code,
                    safe_summary,
                    safe_detail,
                    actor,
                    now,
                    now,
                ),
            )
            self._append_event(
                connection,
                kind="humanEscalationRequested",
                severity=EventSeverity.ERROR,
                entity_type="executionEscalation",
                entity_id=escalation_id,
                project_id=task["project_id"],
                task_id=job["task_id"],
                worker_id=job["worker_id"],
                run_id=run_id,
                summary=safe_summary,
                payload={
                    "escalationID": escalation_id,
                    "providerJobID": job["id"],
                    "code": code,
                },
                actor=actor,
            )
        return escalation_id

    def resolve_execution_escalations(
        self,
        *,
        run_id: str,
        lease_owner_id: str,
        lease_generation: int,
        codes: Iterable[str] | None = None,
        resolution: str = "provider execution state reconciled safely",
        actor: str = "recovery",
    ) -> int:
        """Resolve open reconciliation escalations after a fenced definitive observation."""

        selected = tuple(codes) if codes is not None else None
        with self.transaction() as connection:
            job = self._provider_job_for_update(connection, run_id)
            self._require_task_execution_lease(
                connection, job["task_id"], lease_owner_id, lease_generation
            )
            parameters: list[Any] = [job["id"]]
            query = "SELECT * FROM execution_escalations WHERE provider_job_id=? AND state='open'"
            if selected is not None:
                if not selected:
                    return 0
                placeholders = ",".join("?" for _ in selected)
                query += f" AND code IN ({placeholders})"
                parameters.extend(selected)
            query += " ORDER BY created_at,id"
            rows = connection.execute(query, parameters).fetchall()
            if not rows:
                return 0
            task = connection.execute(
                "SELECT project_id FROM tasks WHERE id=?", (job["task_id"],)
            ).fetchone()
            now = timestamp()
            safe_resolution = str(redact_sensitive(resolution))
            for escalation in rows:
                connection.execute(
                    "UPDATE execution_escalations SET state='resolved',resolved_by=?,"
                    "resolved_at=?,updated_at=? WHERE id=? AND state='open'",
                    (actor, now, now, escalation["id"]),
                )
                self._append_event(
                    connection,
                    kind="humanEscalationResolved",
                    severity=EventSeverity.NOTICE,
                    entity_type="executionEscalation",
                    entity_id=escalation["id"],
                    project_id=task["project_id"],
                    task_id=job["task_id"],
                    worker_id=job["worker_id"],
                    run_id=run_id,
                    summary=f"Execution escalation {escalation['code']} resolved",
                    payload={
                        "escalationID": escalation["id"],
                        "providerJobID": job["id"],
                        "code": escalation["code"],
                        "resolution": safe_resolution,
                    },
                    actor=actor,
                )
        return len(rows)

    def list_execution_escalations(
        self, *, run_id: str | None = None, state: str | None = None
    ) -> list[dict[str, Any]]:
        clauses: list[str] = []
        parameters: list[Any] = []
        if run_id is not None:
            clauses.append("run_id=?")
            parameters.append(run_id)
        if state is not None:
            clauses.append("state=?")
            parameters.append(state)
        query = "SELECT * FROM execution_escalations"
        if clauses:
            query += " WHERE " + " AND ".join(clauses)
        query += " ORDER BY created_at,id"
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
        lease_owner_id: str | None = None,
        lease_generation: int | None = None,
    ) -> None:
        values = {
            "summary": redact_sensitive(summary),
            "changed_files_json": compact_json(redact_sensitive(changed_files or [])),
            "commands_run_json": compact_json(redact_sensitive(commands_run or [])),
            "tests_json": compact_json(redact_sensitive(tests or [])),
            "artifacts_json": compact_json(redact_sensitive(artifacts or [])),
            "commit_hash": redact_sensitive(commit_hash),
            "blockers_json": compact_json(redact_sensitive(blockers or [])),
            "confidence": confidence,
            "recommended_next_actions_json": compact_json(
                redact_sensitive(recommended_next_actions or [])
            ),
        }
        with self.transaction() as connection:
            run = connection.execute("SELECT * FROM worker_runs WHERE id=?", (run_id,)).fetchone()
            if run is None:
                raise KeyError(run_id)
            if (lease_owner_id is None) != (lease_generation is None):
                raise ValueError("lease owner and generation must be supplied together")
            if lease_owner_id is not None and lease_generation is not None:
                self._require_task_execution_lease(
                    connection,
                    run["task_id"],
                    lease_owner_id,
                    lease_generation,
                )
            existing = connection.execute(
                "SELECT * FROM worker_results WHERE run_id=?", (run_id,)
            ).fetchone()
            if existing is not None:
                if all(existing[column] == value for column, value in values.items()):
                    return
                raise RuntimeError(f"worker result for {run_id} is immutable")
            connection.execute(
                """
                INSERT INTO worker_results(
                    run_id,summary,changed_files_json,commands_run_json,tests_json,artifacts_json,
                    commit_hash,blockers_json,confidence,recommended_next_actions_json,created_at
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    run_id,
                    values["summary"],
                    values["changed_files_json"],
                    values["commands_run_json"],
                    values["tests_json"],
                    values["artifacts_json"],
                    values["commit_hash"],
                    values["blockers_json"],
                    values["confidence"],
                    values["recommended_next_actions_json"],
                    timestamp(),
                ),
            )
            task = connection.execute(
                "SELECT project_id FROM tasks WHERE id=?", (run["task_id"],)
            ).fetchone()
            self._append_event(
                connection,
                kind="workerResultRecorded",
                severity=EventSeverity.INFO,
                entity_type="workerRun",
                entity_id=run_id,
                project_id=task["project_id"],
                task_id=run["task_id"],
                worker_id=run["worker_id"],
                run_id=run_id,
                summary="Immutable Worker result recorded",
                payload={
                    "changedFileCount": len(changed_files or []),
                    "testCount": len(tests or []),
                    "artifactCount": len(artifacts or []),
                    "blockerCount": len(blockers or []),
                },
                actor="runtime",
            )

    def record_failure(
        self,
        *,
        classification: FailureClass,
        summary: str,
        retryable: bool,
        record_id: str | None = None,
        project_id: str | None = None,
        task_id: str | None = None,
        run_id: str | None = None,
        detail: str | None = None,
    ) -> str:
        failure_id = record_id or f"failure-{uuid.uuid4()}"
        if not failure_id.strip():
            raise ValueError("failure record_id must not be empty")
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
            expected = {
                "project_id": project_id,
                "task_id": task_id,
                "run_id": run_id,
                "classification": classification.value,
                "summary": safe_summary,
                "detail": safe_detail,
                "retryable": int(retryable),
            }
            existing = connection.execute(
                "SELECT * FROM failures WHERE id=?", (failure_id,)
            ).fetchone()
            if existing is not None:
                if all(existing[key] == value for key, value in expected.items()):
                    return failure_id
                raise RuntimeError(f"failure record {failure_id} conflicts with immutable replay")
            connection.execute(
                "INSERT INTO failures(id,project_id,task_id,run_id,classification,summary,detail,"
                "retryable,created_at) VALUES (?,?,?,?,?,?,?,?,?)",
                (
                    failure_id,
                    *expected.values(),
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
        record_id: str | None = None,
        task_id: str | None = None,
        run_id: str | None = None,
        worker_id: str | None = None,
        model_id: str | None = None,
    ) -> str:
        usage_id = record_id or f"usg-{uuid.uuid4()}"
        if not usage_id.strip():
            raise ValueError("usage record_id must not be empty")
        with self.transaction() as connection:
            existing = connection.execute(
                "SELECT * FROM usage_records WHERE id=?", (usage_id,)
            ).fetchone()
            expected = {
                "task_id": task_id,
                "run_id": run_id,
                "worker_id": worker_id,
                "model_id": model_id,
                "metric": metric,
                "value": telemetry.value,
                "unit": unit,
                "confidence": telemetry.confidence.value,
                "unavailable_reason": (telemetry.reason.value if telemetry.reason else None),
            }
            if existing is not None:
                if all(existing[key] == value for key, value in expected.items()):
                    return usage_id
                raise RuntimeError(f"usage record {usage_id} conflicts with immutable replay")
            connection.execute(
                """
                INSERT INTO usage_records(
                    id,task_id,run_id,worker_id,model_id,metric,value,unit,confidence,
                    unavailable_reason,recorded_at
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    usage_id,
                    *expected.values(),
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
        lease_owner_id: str | None = None,
        lease_generation: int | None = None,
    ) -> int:
        safe_payload = redact_sensitive(payload)
        with self.transaction() as connection:
            run = connection.execute("SELECT * FROM worker_runs WHERE id=?", (run_id,)).fetchone()
            if run is None:
                raise KeyError(run_id)
            if (lease_owner_id is None) != (lease_generation is None):
                raise ValueError("lease owner and generation must be supplied together")
            if lease_owner_id is not None and lease_generation is not None:
                self._require_task_execution_lease(
                    connection,
                    run["task_id"],
                    lease_owner_id,
                    lease_generation,
                )
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

    def recover_interrupted(self, task_ids: Iterable[str] | None = None) -> dict[str, int]:
        active_runs = ("starting", "running", "waiting")
        identities = tuple(dict.fromkeys(task_ids or ()))
        with self.transaction() as connection:
            now = timestamp()
            run_query = (
                f"SELECT * FROM worker_runs run WHERE state IN "
                f"({','.join('?' for _ in active_runs)}) "
                "AND NOT EXISTS (SELECT 1 FROM task_execution_leases lease "
                "WHERE lease.task_id=run.task_id AND lease.state='active' "
                "AND lease.expires_at>?) "
                "AND NOT EXISTS (SELECT 1 FROM provider_jobs job WHERE job.run_id=run.id "
                "AND job.launch_state<>'prepared')"
            )
            run_parameters: tuple[Any, ...] = (*active_runs, now)
            task_query = (
                "SELECT * FROM tasks task WHERE state = ? "
                "AND NOT EXISTS (SELECT 1 FROM task_execution_leases lease "
                "WHERE lease.task_id=task.id AND lease.state='active' "
                "AND lease.expires_at>?) "
                "AND NOT EXISTS (SELECT 1 FROM worker_runs protected_run "
                "JOIN provider_jobs job ON job.run_id=protected_run.id "
                "WHERE protected_run.task_id=task.id "
                "AND protected_run.state IN ('starting','running','waiting') "
                "AND job.launch_state<>'prepared')"
            )
            task_parameters: tuple[Any, ...] = (TaskState.RUNNING.value, now)
            if task_ids is not None:
                if not identities:
                    return {"runsInterrupted": 0, "tasksInterrupted": 0}
                placeholders = ",".join("?" for _ in identities)
                run_query += f" AND task_id IN ({placeholders})"
                run_parameters = (*active_runs, now, *identities)
                task_query += f" AND id IN ({placeholders})"
                task_parameters = (TaskState.RUNNING.value, now, *identities)
            runs = connection.execute(run_query, run_parameters).fetchall()
            tasks = connection.execute(task_query, task_parameters).fetchall()
            for run in runs:
                connection.execute(
                    "UPDATE worker_runs SET state='interrupted',ended_at=?,updated_at=?,"
                    "failure_class='infrastructure',failure_detail='supervisor restart' WHERE id=?",
                    (now, now, run["id"]),
                )
                connection.execute(
                    "UPDATE provider_capacity_reservations SET state='released',released_at=?,"
                    "release_reason='recoveryPreLaunchInterrupted' "
                    "WHERE run_id=? AND state='reserved' AND ("
                    "NOT EXISTS (SELECT 1 FROM provider_jobs job WHERE job.run_id=?) OR "
                    "EXISTS (SELECT 1 FROM provider_jobs job WHERE job.run_id=? "
                    "AND job.launch_state='prepared'))",
                    (now, run["id"], run["id"], run["id"]),
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
            for worker_id in sorted({str(run["worker_id"]) for run in runs}):
                connection.execute(
                    "UPDATE workers SET state='idle',updated_at=? WHERE id=? AND NOT EXISTS ("
                    "SELECT 1 FROM worker_runs active WHERE active.worker_id=? "
                    "AND active.state IN ('starting','running','waiting'))",
                    (now, worker_id, worker_id),
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
            recovered_task_ids = {
                *(str(run["task_id"]) for run in runs),
                *(str(task["id"]) for task in tasks),
            }
            if recovered_task_ids:
                placeholders = ",".join("?" for _ in recovered_task_ids)
                connection.execute(
                    f"UPDATE task_execution_leases SET state='released',released_at=?,"
                    f"heartbeat_at=? WHERE task_id IN ({placeholders}) AND state='active'",
                    (now, now, *sorted(recovered_task_ids)),
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
