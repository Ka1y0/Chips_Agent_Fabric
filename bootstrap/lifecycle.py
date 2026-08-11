"""Durable, approval-scoped bootstrap orchestration FOUNDATION.

This module records a bootstrap plan and externally produced, structured step
results.  It deliberately does not install packages, start services, configure
networks, create keys, read credentials, or execute privileged operations.
SQLite is the durable source of truth so an unfamiliar agent can safely resume
after a process restart without relying on conversation history.
"""

from __future__ import annotations

import hashlib
import json
import re
import sqlite3
from collections.abc import Mapping, Sequence
from contextlib import suppress
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum
from pathlib import Path
from typing import Any

_IDENTIFIER = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$")
_CAPABILITY = re.compile(r"^[a-z][a-z0-9]*(?:[._-][a-z0-9]+)+$")
_MAX_RESULT_BYTES = 32 * 1024
_FORBIDDEN_KEY_IDENTITIES = frozenset(
    {
        "accesstoken",
        "apikey",
        "argv",
        "authorization",
        "authorizationheader",
        "bearer",
        "bearertoken",
        "clientsecret",
        "command",
        "cookie",
        "credential",
        "credentials",
        "oauth",
        "oauthsecret",
        "oauthtoken",
        "password",
        "passwordhash",
        "privatekey",
        "refreshtoken",
        "secret",
        "secretkey",
        "sessioncookie",
        "sessiontoken",
        "shell",
        "signature",
        "token",
    }
)
_CREDENTIAL_TEXT = (
    re.compile(r"(?i)(?:token|api[_-]?key|password|secret|authorization)\s*[:=]"),
    re.compile(r"\bsk-[A-Za-z0-9_-]{12,}\b"),
    re.compile(r"(?i)bearer\s+[A-Za-z0-9._~-]{8,}"),
)


class BootstrapRunState(StrEnum):
    ACTIVE = "active"
    WAITING = "waiting"
    FAILED = "failed"
    COMPLETE = "complete"


class BootstrapStepState(StrEnum):
    PENDING = "pending"
    READY = "ready"
    AWAITING_REVIEW = "awaitingReview"
    AWAITING_APPROVAL = "awaitingApproval"
    SATISFIED = "satisfied"
    FAILED = "failed"
    BLOCKED = "blocked"


class BootstrapOutcome(StrEnum):
    SUCCEEDED = "succeeded"
    FAILED = "failed"


class BootstrapExecutionMode(StrEnum):
    OBSERVED_ONLY = "observedOnly"
    EXTERNALLY_EXECUTED = "externallyExecuted"


_TRUSTED_AUTHORITY_TYPES = frozenset({"human", "enterprise", "privilegeBroker", "trustedNode"})


def _stamp(value: datetime | None = None) -> str:
    observed = value or datetime.now(UTC)
    if observed.tzinfo is None:
        raise ValueError("bootstrap timestamps must be timezone-aware")
    return observed.astimezone(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _canonical(value: object) -> bytes:
    return (json.dumps(value, separators=(",", ":"), sort_keys=True) + "\n").encode()


def _digest(value: object) -> str:
    return hashlib.sha256(_canonical(value)).hexdigest()


def _key_identity(value: object) -> str:
    """Fold common key spellings without treating every token metric as a credential."""

    return "".join(re.findall(r"[a-z0-9]+", str(value).casefold()))


def _safe_json(value: object, *, path: str = "result") -> None:
    """Reject command/credential-shaped material instead of trying to sanitize authority."""

    if isinstance(value, Mapping):
        for key, child in value.items():
            if _key_identity(key) in _FORBIDDEN_KEY_IDENTITIES:
                raise ValueError(f"{path} contains prohibited field {key!r}")
            _safe_json(child, path=f"{path}.{key}")
    elif isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        for index, child in enumerate(value):
            _safe_json(child, path=f"{path}[{index}]")
    elif isinstance(value, str) and any(pattern.search(value) for pattern in _CREDENTIAL_TEXT):
        raise ValueError(f"{path} contains credential-shaped text")
    elif value is not None and not isinstance(value, (str, int, float, bool)):
        raise ValueError(f"{path} contains a non-JSON value")


@dataclass(frozen=True, slots=True)
class BootstrapAuthorizationReference:
    """Non-secret reference to authority verified outside this recorder.

    This is intentionally not a capability grant implementation.  The future
    Privilege Broker must verify the real grant, issuer, signature, expiry,
    replay protection, target constraints, and rollback before doing work.
    """

    grant_reference: str
    capability: str
    subject: str
    verified_by: str
    authority_type: str

    def __post_init__(self) -> None:
        for name, value in {
            "grant_reference": self.grant_reference,
            "subject": self.subject,
            "verified_by": self.verified_by,
        }.items():
            if not value.strip() or not _IDENTIFIER.fullmatch(value):
                raise ValueError(f"{name} must be a non-secret machine identifier")
        if not _CAPABILITY.fullmatch(self.capability):
            raise ValueError("authorization capability must be scoped")
        if self.authority_type not in _TRUSTED_AUTHORITY_TYPES:
            raise ValueError("authorityType must name a legitimate external trust root")

    @classmethod
    def from_protocol(cls, payload: Mapping[str, Any]) -> BootstrapAuthorizationReference:
        if set(payload) != {
            "grantReference",
            "capability",
            "subject",
            "verifiedBy",
            "authorityType",
        }:
            raise ValueError("authorization reference has unknown or missing fields")
        return cls(
            grant_reference=str(payload["grantReference"]),
            capability=str(payload["capability"]),
            subject=str(payload["subject"]),
            verified_by=str(payload["verifiedBy"]),
            authority_type=str(payload["authorityType"]),
        )

    def to_protocol(self) -> dict[str, str]:
        return {
            "grantReference": self.grant_reference,
            "capability": self.capability,
            "subject": self.subject,
            "verifiedBy": self.verified_by,
            "authorityType": self.authority_type,
        }


@dataclass(frozen=True, slots=True)
class BootstrapStepResult:
    schema_version: int
    run_id: str
    step_id: str
    idempotency_key: str
    outcome: BootstrapOutcome
    execution_mode: BootstrapExecutionMode
    actor: str
    observed_at: datetime
    evidence: dict[str, Any]
    authorization: BootstrapAuthorizationReference | None = None

    def __post_init__(self) -> None:
        if self.schema_version != 1:
            raise ValueError("unsupported bootstrap step-result schema version")
        for name, value in {
            "run_id": self.run_id,
            "step_id": self.step_id,
            "idempotency_key": self.idempotency_key,
            "actor": self.actor,
        }.items():
            if not value.strip() or not _IDENTIFIER.fullmatch(value):
                raise ValueError(f"{name} must be a machine identifier")
        if self.observed_at.tzinfo is None:
            raise ValueError("observedAt must be timezone-aware")
        _safe_json(self.evidence)
        if len(_canonical(self.to_protocol())) > _MAX_RESULT_BYTES:
            raise ValueError("bootstrap result exceeds the bounded evidence size")

    @classmethod
    def from_protocol(cls, payload: Mapping[str, Any]) -> BootstrapStepResult:
        required = {
            "schemaVersion",
            "runID",
            "stepID",
            "idempotencyKey",
            "outcome",
            "executionMode",
            "actor",
            "observedAt",
            "evidence",
            "authorization",
        }
        if set(payload) != required:
            raise ValueError("bootstrap result has unknown or missing fields")
        authorization_payload = payload["authorization"]
        authorization = (
            BootstrapAuthorizationReference.from_protocol(authorization_payload)
            if isinstance(authorization_payload, Mapping)
            else None
        )
        if authorization_payload is not None and authorization is None:
            raise ValueError("authorization must be an object or null")
        observed = datetime.fromisoformat(str(payload["observedAt"]).replace("Z", "+00:00"))
        evidence = payload["evidence"]
        if not isinstance(evidence, dict):
            raise ValueError("evidence must be an object")
        return cls(
            schema_version=int(payload["schemaVersion"]),
            run_id=str(payload["runID"]),
            step_id=str(payload["stepID"]),
            idempotency_key=str(payload["idempotencyKey"]),
            outcome=BootstrapOutcome(str(payload["outcome"])),
            execution_mode=BootstrapExecutionMode(str(payload["executionMode"])),
            actor=str(payload["actor"]),
            observed_at=observed,
            evidence=evidence,
            authorization=authorization,
        )

    def to_protocol(self) -> dict[str, Any]:
        return {
            "schemaVersion": self.schema_version,
            "runID": self.run_id,
            "stepID": self.step_id,
            "idempotencyKey": self.idempotency_key,
            "outcome": self.outcome.value,
            "executionMode": self.execution_mode.value,
            "actor": self.actor,
            "observedAt": _stamp(self.observed_at),
            "evidence": self.evidence,
            "authorization": self.authorization.to_protocol() if self.authorization else None,
        }


class BootstrapLifecycleStore:
    """SQLite state machine for one or more resumable bootstrap runs."""

    def __init__(self, path: str | Path) -> None:
        self.path = Path(path).expanduser()
        if self.path.exists() and self.path.is_symlink():
            raise ValueError("bootstrap state database must not be a symbolic link")
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._initialize()
        with suppress(OSError):
            self.path.chmod(0o600)

    def connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.path, timeout=10, isolation_level=None)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys=ON")
        connection.execute("PRAGMA busy_timeout=10000")
        return connection

    def _initialize(self) -> None:
        with self.connect() as connection:
            connection.execute("PRAGMA journal_mode=WAL")
            connection.execute("PRAGMA synchronous=FULL")
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS bootstrap_runs (
                    id TEXT PRIMARY KEY,
                    schema_version INTEGER NOT NULL,
                    plan_digest TEXT NOT NULL,
                    profile_digest TEXT NOT NULL,
                    plan_json TEXT NOT NULL,
                    state TEXT NOT NULL,
                    current_phase TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    completed_at TEXT
                );
                CREATE TABLE IF NOT EXISTS bootstrap_steps (
                    run_id TEXT NOT NULL REFERENCES bootstrap_runs(id) ON DELETE CASCADE,
                    step_id TEXT NOT NULL,
                    ordinal INTEGER NOT NULL,
                    phase TEXT NOT NULL,
                    action TEXT NOT NULL,
                    plan_status TEXT NOT NULL,
                    state TEXT NOT NULL,
                    mutating INTEGER NOT NULL CHECK(mutating IN (0,1)),
                    required_capability TEXT,
                    dependencies_json TEXT NOT NULL,
                    reason TEXT NOT NULL,
                    result_digest TEXT,
                    authorization_reference TEXT,
                    result_json TEXT,
                    updated_at TEXT NOT NULL,
                    PRIMARY KEY(run_id,step_id),
                    UNIQUE(run_id,ordinal)
                );
                CREATE TABLE IF NOT EXISTS bootstrap_result_receipts (
                    idempotency_key TEXT PRIMARY KEY,
                    run_id TEXT NOT NULL,
                    step_id TEXT NOT NULL,
                    result_digest TEXT NOT NULL,
                    recorded_at TEXT NOT NULL,
                    FOREIGN KEY(run_id,step_id) REFERENCES bootstrap_steps(run_id,step_id)
                );
                CREATE TABLE IF NOT EXISTS bootstrap_audit_events (
                    sequence INTEGER PRIMARY KEY AUTOINCREMENT,
                    event_id TEXT NOT NULL UNIQUE,
                    run_id TEXT NOT NULL REFERENCES bootstrap_runs(id) ON DELETE CASCADE,
                    step_id TEXT,
                    kind TEXT NOT NULL,
                    actor TEXT NOT NULL,
                    summary TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    previous_hash TEXT NOT NULL,
                    event_hash TEXT NOT NULL UNIQUE,
                    created_at TEXT NOT NULL
                );
                CREATE TRIGGER IF NOT EXISTS bootstrap_audit_no_update
                BEFORE UPDATE ON bootstrap_audit_events
                BEGIN SELECT RAISE(ABORT,'bootstrap audit is append-only'); END;
                CREATE TRIGGER IF NOT EXISTS bootstrap_audit_no_delete
                BEFORE DELETE ON bootstrap_audit_events
                BEGIN SELECT RAISE(ABORT,'bootstrap audit is append-only'); END;
                """
            )

    def initialize_run(
        self,
        *,
        run_id: str,
        profile: Mapping[str, Any],
        plan: Mapping[str, Any],
        actor: str = "bootstrap",
    ) -> dict[str, Any]:
        if not _IDENTIFIER.fullmatch(run_id) or not _IDENTIFIER.fullmatch(actor):
            raise ValueError("run_id and actor must be machine identifiers")
        self._validate_plan(plan)
        plan_digest = _digest(plan)
        profile_digest = _digest(profile)
        now = _stamp()
        with self.connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                existing = connection.execute(
                    "SELECT plan_digest,profile_digest FROM bootstrap_runs WHERE id=?", (run_id,)
                ).fetchone()
                if existing is not None:
                    if (
                        existing["plan_digest"] != plan_digest
                        or existing["profile_digest"] != profile_digest
                    ):
                        raise ValueError(
                            "bootstrap run already exists with different discovery/plan evidence"
                        )
                    connection.commit()
                    return self.get_run(run_id)
                connection.execute(
                    "INSERT INTO bootstrap_runs(id,schema_version,plan_digest,profile_digest,"
                    "plan_json,state,created_at,updated_at) VALUES (?,?,?,?,?,?,?,?)",
                    (
                        run_id,
                        1,
                        plan_digest,
                        profile_digest,
                        _canonical(plan).decode().strip(),
                        BootstrapRunState.ACTIVE.value,
                        now,
                        now,
                    ),
                )
                for ordinal, step in enumerate(plan["steps"]):
                    state = self._initial_step_state(step, ordinal)
                    connection.execute(
                        "INSERT INTO bootstrap_steps(run_id,step_id,ordinal,phase,action,"
                        "plan_status,state,mutating,required_capability,dependencies_json,reason,"
                        "updated_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
                        (
                            run_id,
                            step["id"],
                            ordinal,
                            step["phase"],
                            step["action"],
                            step["status"],
                            state.value,
                            int(step["mutating"]),
                            step.get("requiredCapability"),
                            _canonical(step["dependsOn"]).decode().strip(),
                            step["reason"],
                            now,
                        ),
                    )
                self._append_audit(
                    connection,
                    run_id=run_id,
                    kind="bootstrapRunInitialized",
                    actor=actor,
                    summary="Durable bootstrap run initialized without host mutation",
                    payload={"planDigest": plan_digest, "profileDigest": profile_digest},
                    created_at=now,
                )
                self._reconcile(connection, run_id, actor=actor, created_at=now)
                connection.commit()
            except Exception:
                connection.rollback()
                raise
        return self.get_run(run_id)

    def record_result(self, result: BootstrapStepResult) -> dict[str, Any]:
        payload = result.to_protocol()
        result_digest = _digest(payload)
        now = _stamp(result.observed_at)
        with self.connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                receipt = connection.execute(
                    "SELECT result_digest,run_id FROM bootstrap_result_receipts "
                    "WHERE idempotency_key=?",
                    (result.idempotency_key,),
                ).fetchone()
                if receipt is not None:
                    if receipt["result_digest"] != result_digest:
                        raise ValueError("idempotency key was already used for a different result")
                    if receipt["run_id"] != result.run_id:
                        raise ValueError("idempotency key belongs to a different bootstrap run")
                    connection.commit()
                    return self.get_run(result.run_id)
                step = connection.execute(
                    "SELECT * FROM bootstrap_steps WHERE run_id=? AND step_id=?",
                    (result.run_id, result.step_id),
                ).fetchone()
                if step is None:
                    raise KeyError(f"unknown bootstrap step {result.step_id}")
                if step["state"] == BootstrapStepState.SATISFIED.value:
                    raise ValueError(
                        "satisfied bootstrap step requires the original idempotency key"
                    )
                if step["state"] not in {
                    BootstrapStepState.READY.value,
                    BootstrapStepState.AWAITING_REVIEW.value,
                    BootstrapStepState.AWAITING_APPROVAL.value,
                }:
                    raise ValueError(f"bootstrap step is not actionable: {step['state']}")
                dependencies = json.loads(step["dependencies_json"])
                if not self._dependencies_satisfied(connection, result.run_id, dependencies):
                    raise ValueError("bootstrap step dependencies are not satisfied")
                mutating = bool(step["mutating"])
                if mutating:
                    self._validate_mutating_result(step, result)
                elif result.execution_mode is not BootstrapExecutionMode.OBSERVED_ONLY:
                    raise ValueError("non-mutating step must use observedOnly execution mode")
                next_state = (
                    BootstrapStepState.SATISFIED
                    if result.outcome is BootstrapOutcome.SUCCEEDED
                    else BootstrapStepState.FAILED
                )
                connection.execute(
                    "UPDATE bootstrap_steps SET state=?,result_digest=?,authorization_reference=?,"
                    "result_json=?,updated_at=? WHERE run_id=? AND step_id=?",
                    (
                        next_state.value,
                        result_digest,
                        result.authorization.grant_reference if result.authorization else None,
                        _canonical(payload).decode().strip(),
                        now,
                        result.run_id,
                        result.step_id,
                    ),
                )
                connection.execute(
                    "INSERT INTO bootstrap_result_receipts(idempotency_key,run_id,step_id,"
                    "result_digest,recorded_at) VALUES (?,?,?,?,?)",
                    (
                        result.idempotency_key,
                        result.run_id,
                        result.step_id,
                        result_digest,
                        now,
                    ),
                )
                self._append_audit(
                    connection,
                    run_id=result.run_id,
                    step_id=result.step_id,
                    kind="bootstrapStepResultRecorded",
                    actor=result.actor,
                    summary=f"Bootstrap step recorded as {next_state.value}",
                    payload={
                        "resultDigest": result_digest,
                        "outcome": result.outcome.value,
                        "executionMode": result.execution_mode.value,
                        "authorizationReference": (
                            result.authorization.grant_reference if result.authorization else None
                        ),
                    },
                    created_at=now,
                )
                self._reconcile(connection, result.run_id, actor="bootstrap", created_at=now)
                connection.commit()
            except Exception:
                connection.rollback()
                raise
        return self.get_run(result.run_id)

    def get_run(self, run_id: str) -> dict[str, Any]:
        with self.connect() as connection:
            run = connection.execute(
                "SELECT * FROM bootstrap_runs WHERE id=?", (run_id,)
            ).fetchone()
            if run is None:
                raise KeyError(run_id)
            steps = connection.execute(
                "SELECT * FROM bootstrap_steps WHERE run_id=? ORDER BY ordinal", (run_id,)
            ).fetchall()
            audit = connection.execute(
                "SELECT sequence,event_id,step_id,kind,actor,summary,payload_json,previous_hash,"
                "event_hash,created_at FROM bootstrap_audit_events WHERE run_id=? "
                "ORDER BY sequence",
                (run_id,),
            ).fetchall()
        projection = {
            "schemaVersion": 1,
            "runID": run_id,
            "state": run["state"],
            "currentPhase": run["current_phase"],
            "planDigest": run["plan_digest"],
            "profileDigest": run["profile_digest"],
            "createdAt": run["created_at"],
            "updatedAt": run["updated_at"],
            "completedAt": run["completed_at"],
            "hostMutationsPerformed": False,
            "privilegedExecutionImplemented": False,
            "steps": [
                {
                    "stepID": step["step_id"],
                    "ordinal": step["ordinal"],
                    "phase": step["phase"],
                    "action": step["action"],
                    "state": step["state"],
                    "mutating": bool(step["mutating"]),
                    "requiredCapability": step["required_capability"],
                    "dependsOn": json.loads(step["dependencies_json"]),
                    "reason": step["reason"],
                    "resultDigest": step["result_digest"],
                    "authorizationReference": step["authorization_reference"],
                    "updatedAt": step["updated_at"],
                }
                for step in steps
            ],
            "audit": [
                {
                    "sequence": event["sequence"],
                    "eventID": event["event_id"],
                    "stepID": event["step_id"],
                    "kind": event["kind"],
                    "actor": event["actor"],
                    "summary": event["summary"],
                    "payload": json.loads(event["payload_json"]),
                    "previousHash": event["previous_hash"],
                    "eventHash": event["event_hash"],
                    "createdAt": event["created_at"],
                }
                for event in audit
            ],
        }
        projection["auditChainValid"] = self._verify_audit_projection(projection)
        return projection

    def verify_audit_chain(self, run_id: str) -> bool:
        return bool(self.get_run(run_id)["auditChainValid"])

    @staticmethod
    def _verify_audit_projection(projection: Mapping[str, Any]) -> bool:
        previous = "0" * 64
        for event in projection["audit"]:
            if event["previousHash"] != previous:
                return False
            body = {
                "eventID": event["eventID"],
                "runID": projection["runID"],
                "stepID": event["stepID"],
                "kind": event["kind"],
                "actor": event["actor"],
                "summary": event["summary"],
                "payload": event["payload"],
                "createdAt": event["createdAt"],
            }
            expected = hashlib.sha256((previous + _canonical(body).decode()).encode()).hexdigest()
            if event["eventHash"] != expected:
                return False
            previous = expected
        return True

    @staticmethod
    def _validate_plan(plan: Mapping[str, Any]) -> None:
        if plan.get("schemaVersion") != 2 or plan.get("mode") != "dryRun":
            raise ValueError("durable bootstrap requires dry-run plan schema v2")
        if plan.get("failClosed") is not True or plan.get("automaticActions") != []:
            raise ValueError("bootstrap plan must remain fail-closed with no automatic actions")
        steps = plan.get("steps")
        if not isinstance(steps, list) or not steps:
            raise ValueError("bootstrap plan requires ordered steps")
        identifiers: set[str] = set()
        for step in steps:
            if not isinstance(step, Mapping):
                raise ValueError("bootstrap step must be an object")
            required = {
                "id",
                "phase",
                "action",
                "status",
                "reason",
                "mutating",
                "dependsOn",
                "requiredCapability",
            }
            if not required <= set(step):
                raise ValueError("bootstrap step is missing lifecycle fields")
            step_id = str(step["id"])
            if not _IDENTIFIER.fullmatch(step_id) or step_id in identifiers:
                raise ValueError("bootstrap step IDs must be unique machine identifiers")
            identifiers.add(step_id)
            dependencies = step["dependsOn"]
            if not isinstance(dependencies, list) or any(
                not isinstance(item, str) or item not in identifiers for item in dependencies
            ):
                raise ValueError("bootstrap dependencies must refer to earlier steps")
            capability = step["requiredCapability"]
            if bool(step["mutating"]):
                if not isinstance(capability, str) or not _CAPABILITY.fullmatch(capability):
                    raise ValueError("mutating bootstrap step requires a scoped capability")
            elif capability is not None:
                raise ValueError("non-mutating bootstrap step cannot claim a capability")

    @staticmethod
    def _initial_step_state(step: Mapping[str, Any], ordinal: int) -> BootstrapStepState:
        if ordinal == 0 and step["status"] == "blocked":
            return BootstrapStepState.BLOCKED
        return BootstrapStepState.PENDING

    @staticmethod
    def _dependencies_satisfied(
        connection: sqlite3.Connection, run_id: str, dependencies: Sequence[str]
    ) -> bool:
        for dependency in dependencies:
            row = connection.execute(
                "SELECT state FROM bootstrap_steps WHERE run_id=? AND step_id=?",
                (run_id, dependency),
            ).fetchone()
            if row is None or row["state"] != BootstrapStepState.SATISFIED.value:
                return False
        return True

    def _reconcile(
        self, connection: sqlite3.Connection, run_id: str, *, actor: str, created_at: str
    ) -> None:
        changed = True
        while changed:
            changed = False
            rows = connection.execute(
                "SELECT * FROM bootstrap_steps WHERE run_id=? ORDER BY ordinal", (run_id,)
            ).fetchall()
            for step in rows:
                if step["state"] != BootstrapStepState.PENDING.value:
                    continue
                dependencies = json.loads(step["dependencies_json"])
                if not self._dependencies_satisfied(connection, run_id, dependencies):
                    continue
                if step["plan_status"] == "notNeeded":
                    next_state = BootstrapStepState.SATISFIED
                    event_kind = "bootstrapStepNotNeeded"
                elif step["plan_status"] == "blocked":
                    next_state = BootstrapStepState.READY
                    event_kind = "bootstrapStepActionable"
                elif step["plan_status"] == "required":
                    next_state = BootstrapStepState.AWAITING_REVIEW
                    event_kind = "bootstrapStepActionable"
                elif step["mutating"]:
                    next_state = BootstrapStepState.AWAITING_APPROVAL
                    event_kind = "bootstrapStepActionable"
                else:
                    next_state = BootstrapStepState.READY
                    event_kind = "bootstrapStepActionable"
                connection.execute(
                    "UPDATE bootstrap_steps SET state=?,updated_at=? WHERE run_id=? AND step_id=?",
                    (next_state.value, created_at, run_id, step["step_id"]),
                )
                self._append_audit(
                    connection,
                    run_id=run_id,
                    step_id=str(step["step_id"]),
                    kind=event_kind,
                    actor=actor,
                    summary=f"Bootstrap step is {next_state.value}",
                    payload={
                        "phase": step["phase"],
                        "action": step["action"],
                        "requiredCapability": step["required_capability"],
                    },
                    created_at=created_at,
                )
                changed = True
        states = connection.execute(
            "SELECT state,phase,ordinal FROM bootstrap_steps WHERE run_id=? ORDER BY ordinal",
            (run_id,),
        ).fetchall()
        current = next(
            (row for row in states if row["state"] != BootstrapStepState.SATISFIED.value), None
        )
        if any(row["state"] == BootstrapStepState.FAILED.value for row in states):
            run_state = BootstrapRunState.FAILED
        elif states and all(row["state"] == BootstrapStepState.SATISFIED.value for row in states):
            run_state = BootstrapRunState.COMPLETE
        elif current is not None and current["state"] in {
            BootstrapStepState.AWAITING_APPROVAL.value,
            BootstrapStepState.AWAITING_REVIEW.value,
            BootstrapStepState.BLOCKED.value,
        }:
            run_state = BootstrapRunState.WAITING
        else:
            run_state = BootstrapRunState.ACTIVE
        completed_at = created_at if run_state is BootstrapRunState.COMPLETE else None
        connection.execute(
            "UPDATE bootstrap_runs SET state=?,current_phase=?,updated_at=?,completed_at=? "
            "WHERE id=?",
            (
                run_state.value,
                current["phase"] if current is not None else "COMMIT",
                created_at,
                completed_at,
                run_id,
            ),
        )

    @staticmethod
    def _validate_mutating_result(step: sqlite3.Row, result: BootstrapStepResult) -> None:
        if result.execution_mode is not BootstrapExecutionMode.EXTERNALLY_EXECUTED:
            raise ValueError("mutating step must be executed by an external authorized broker")
        if result.authorization is None:
            raise ValueError("mutating step requires an external authorization reference")
        if result.authorization.capability != step["required_capability"]:
            raise ValueError("authorization capability does not match the planned operation")
        if (
            result.authorization.authority_type == "privilegeBroker"
            and not result.authorization.verified_by
        ):
            raise ValueError("Privilege Broker result must identify its verifier")

    def _append_audit(
        self,
        connection: sqlite3.Connection,
        *,
        run_id: str,
        kind: str,
        actor: str,
        summary: str,
        payload: Mapping[str, Any],
        created_at: str,
        step_id: str | None = None,
    ) -> None:
        previous_row = connection.execute(
            "SELECT event_hash FROM bootstrap_audit_events WHERE run_id=? "
            "ORDER BY sequence DESC LIMIT 1",
            (run_id,),
        ).fetchone()
        previous_hash = str(previous_row["event_hash"]) if previous_row else "0" * 64
        event_seed = f"{run_id}:{kind}:{created_at}:{step_id or ''}:{previous_hash}"
        event_id = f"bootstrap-event-{hashlib.sha256(event_seed.encode()).hexdigest()[:24]}"
        body = {
            "eventID": event_id,
            "runID": run_id,
            "stepID": step_id,
            "kind": kind,
            "actor": actor,
            "summary": summary,
            "payload": dict(payload),
            "createdAt": created_at,
        }
        event_hash = hashlib.sha256(
            (previous_hash + _canonical(body).decode()).encode()
        ).hexdigest()
        connection.execute(
            "INSERT INTO bootstrap_audit_events(event_id,run_id,step_id,kind,actor,summary,"
            "payload_json,previous_hash,event_hash,created_at) VALUES (?,?,?,?,?,?,?,?,?,?)",
            (
                event_id,
                run_id,
                step_id,
                kind,
                actor,
                summary,
                _canonical(payload).decode().strip(),
                previous_hash,
                event_hash,
                created_at,
            ),
        )
