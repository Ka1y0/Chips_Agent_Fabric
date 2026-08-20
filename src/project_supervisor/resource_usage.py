from __future__ import annotations

import json
import math
import uuid
from collections import Counter
from collections.abc import Iterable, Sequence
from contextlib import AbstractContextManager
from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import StrEnum
from sqlite3 import Connection, Row
from typing import Any, Protocol

from .domain import EvidenceConfidence, UnavailableReason, utc_now
from .hybrid import ResourceRoutingEvidence
from .store import compact_json, redact_sensitive, timestamp


class UsageProvenance(StrEnum):
    """Origin of a resource value; UNKNOWN is never interpreted as zero."""

    PROVIDER_REPORTED = "PROVIDER_REPORTED"
    LOCALLY_MEASURED = "LOCALLY_MEASURED"
    INFERRED = "INFERRED"
    UNKNOWN = "UNKNOWN"


class QuotaState(StrEnum):
    AVAILABLE = "available"
    WARNING = "warning"
    CRITICAL = "critical"
    EXHAUSTED = "exhausted"
    UNKNOWN = "unknown"


class Freshness(StrEnum):
    FRESH = "fresh"
    STALE = "stale"
    UNKNOWN = "unknown"


@dataclass(frozen=True, slots=True)
class UsageDimension:
    value: str | None
    provenance: UsageProvenance
    reason: UnavailableReason | None = None

    def __post_init__(self) -> None:
        if self.value is None:
            if self.provenance is not UsageProvenance.UNKNOWN or self.reason is None:
                raise ValueError("unavailable dimension requires UNKNOWN provenance and a reason")
        elif not self.value.strip():
            raise ValueError("known dimension must not be empty")
        elif self.provenance is UsageProvenance.UNKNOWN or self.reason is not None:
            raise ValueError("known dimension requires non-UNKNOWN provenance and no reason")

    @classmethod
    def known(cls, value: str, provenance: UsageProvenance) -> UsageDimension:
        return cls(value=value, provenance=provenance)

    @classmethod
    def unknown(cls, reason: UnavailableReason = UnavailableReason.NOT_REPORTED) -> UsageDimension:
        return cls(value=None, provenance=UsageProvenance.UNKNOWN, reason=reason)

    def to_protocol(self) -> dict[str, Any]:
        if self.value is None:
            return {
                "state": "unavailable",
                "reason": self.reason.value,
                "provenance": self.provenance.value,
            }
        return {
            "state": "known",
            "value": self.value,
            "provenance": self.provenance.value,
        }


@dataclass(frozen=True, slots=True)
class UsageMetric:
    value: int | float | None
    unit: str | None
    provenance: UsageProvenance
    reason: UnavailableReason | None = None

    def __post_init__(self) -> None:
        if self.value is None:
            if self.unit is not None:
                raise ValueError("unavailable metric cannot claim a unit")
            if self.provenance is not UsageProvenance.UNKNOWN or self.reason is None:
                raise ValueError("unavailable metric requires UNKNOWN provenance and a reason")
            return
        if isinstance(self.value, bool) or not isinstance(self.value, (int, float)):
            raise ValueError("known metric value must be numeric")
        if not math.isfinite(float(self.value)) or self.value < 0:
            raise ValueError("known metric value must be finite and non-negative")
        if self.unit is None or not self.unit.strip():
            raise ValueError("known metric requires a unit")
        if self.provenance is UsageProvenance.UNKNOWN or self.reason is not None:
            raise ValueError("known metric requires non-UNKNOWN provenance and no reason")

    @classmethod
    def known(cls, value: int | float, unit: str, provenance: UsageProvenance) -> UsageMetric:
        return cls(value=value, unit=unit, provenance=provenance)

    @classmethod
    def unknown(cls, reason: UnavailableReason = UnavailableReason.NOT_REPORTED) -> UsageMetric:
        return cls(value=None, unit=None, provenance=UsageProvenance.UNKNOWN, reason=reason)

    def to_protocol(self) -> dict[str, Any]:
        if self.value is None:
            return {
                "state": "unavailable",
                "reason": self.reason.value,
                "provenance": self.provenance.value,
                "unit": None,
            }
        return {
            "state": "known",
            "value": self.value,
            "unit": self.unit,
            "provenance": self.provenance.value,
        }


@dataclass(frozen=True, slots=True)
class UsageInstant:
    value: datetime | None
    provenance: UsageProvenance
    reason: UnavailableReason | None = None

    def __post_init__(self) -> None:
        if self.value is None:
            if self.provenance is not UsageProvenance.UNKNOWN or self.reason is None:
                raise ValueError("unavailable instant requires UNKNOWN provenance and a reason")
        else:
            if self.value.tzinfo is None:
                raise ValueError("known instant must be timezone-aware")
            if self.provenance is UsageProvenance.UNKNOWN or self.reason is not None:
                raise ValueError("known instant requires non-UNKNOWN provenance and no reason")

    @classmethod
    def known(cls, value: datetime, provenance: UsageProvenance) -> UsageInstant:
        return cls(value=value, provenance=provenance)

    @classmethod
    def unknown(cls, reason: UnavailableReason = UnavailableReason.NOT_REPORTED) -> UsageInstant:
        return cls(value=None, provenance=UsageProvenance.UNKNOWN, reason=reason)

    def to_protocol(self) -> dict[str, Any]:
        if self.value is None:
            return {
                "state": "unavailable",
                "reason": self.reason.value,
                "provenance": self.provenance.value,
            }
        return {
            "state": "known",
            "value": _format_time(self.value),
            "provenance": self.provenance.value,
        }


@dataclass(frozen=True, slots=True)
class ResourceObservation:
    """One safe status/local-measurement observation supplied to a post-Task audit."""

    provider: str
    quota_pool_id: str
    account_scope: UsageDimension = field(default_factory=UsageDimension.unknown)
    plan_tier: UsageDimension = field(default_factory=UsageDimension.unknown)
    worker_id: str | None = None
    model: UsageDimension = field(default_factory=UsageDimension.unknown)
    quota_window: UsageDimension = field(default_factory=UsageDimension.unknown)
    used: UsageMetric = field(default_factory=UsageMetric.unknown)
    remaining: UsageMetric = field(default_factory=UsageMetric.unknown)
    reset_at: UsageInstant = field(default_factory=UsageInstant.unknown)
    task_calls: UsageMetric = field(default_factory=UsageMetric.unknown)
    input_tokens: UsageMetric = field(default_factory=UsageMetric.unknown)
    output_tokens: UsageMetric = field(default_factory=UsageMetric.unknown)
    cached_tokens: UsageMetric = field(default_factory=UsageMetric.unknown)
    cost: UsageMetric = field(default_factory=UsageMetric.unknown)
    quota_state: QuotaState = QuotaState.UNKNOWN
    quota_state_provenance: UsageProvenance = UsageProvenance.UNKNOWN
    source: str = "unknown"
    confidence: EvidenceConfidence = EvidenceConfidence.UNKNOWN
    observed_at: datetime = field(default_factory=utc_now)
    fresh_until: datetime | None = None
    reset_observed: bool = False
    metadata: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        for name, value in {
            "provider": self.provider,
            "quota_pool_id": self.quota_pool_id,
            "source": self.source,
        }.items():
            if not value.strip():
                raise ValueError(f"{name} must not be empty")
        if self.worker_id is not None and not self.worker_id.strip():
            raise ValueError("worker_id must be non-empty when supplied")
        if self.observed_at.tzinfo is None:
            raise ValueError("observed_at must be timezone-aware")
        if self.fresh_until is not None:
            if self.fresh_until.tzinfo is None:
                raise ValueError("fresh_until must be timezone-aware")
            if self.fresh_until < self.observed_at:
                raise ValueError("fresh_until cannot precede observed_at")
        if not isinstance(self.metadata, dict):
            raise ValueError("metadata must be an object")
        _validate_count_metric(self.task_calls, "calls")
        for metric in (self.input_tokens, self.output_tokens, self.cached_tokens):
            _validate_count_metric(metric, "tokens")
        if self.quota_state is QuotaState.EXHAUSTED and (
            self.remaining.value is None or self.remaining.value != 0
        ):
            raise ValueError("exhausted quota requires observed zero remaining")
        if (
            self.quota_state is QuotaState.UNKNOWN
            and self.quota_state_provenance is not UsageProvenance.UNKNOWN
        ) or (
            self.quota_state is not QuotaState.UNKNOWN
            and self.quota_state_provenance is UsageProvenance.UNKNOWN
        ):
            raise ValueError("quota state requires explicit matching provenance")


@dataclass(frozen=True, slots=True)
class ResourceSnapshot:
    id: str
    audit_id: str
    project_id: str
    goal_id: str | None
    task_id: str
    run_id: str | None
    observation: ResourceObservation

    def freshness(self, as_of: datetime | None = None) -> Freshness:
        if self.observation.fresh_until is None:
            return Freshness.UNKNOWN
        now = as_of or utc_now()
        if now.tzinfo is None:
            raise ValueError("as_of must be timezone-aware")
        return Freshness.FRESH if now <= self.observation.fresh_until else Freshness.STALE

    def to_protocol(self, *, as_of: datetime | None = None) -> dict[str, Any]:
        item = self.observation
        return {
            "id": self.id,
            "auditID": self.audit_id,
            "projectID": self.project_id,
            "goalID": self.goal_id,
            "taskID": self.task_id,
            "runID": self.run_id,
            "provider": item.provider,
            "accountScope": item.account_scope.to_protocol(),
            "planTier": item.plan_tier.to_protocol(),
            "workerID": item.worker_id,
            "model": item.model.to_protocol(),
            "quotaPoolID": item.quota_pool_id,
            "quotaWindow": item.quota_window.to_protocol(),
            "used": item.used.to_protocol(),
            "remaining": item.remaining.to_protocol(),
            "resetAt": item.reset_at.to_protocol(),
            "taskLocal": {
                "calls": item.task_calls.to_protocol(),
                "inputTokens": item.input_tokens.to_protocol(),
                "outputTokens": item.output_tokens.to_protocol(),
                "cachedTokens": item.cached_tokens.to_protocol(),
                "cost": item.cost.to_protocol(),
            },
            "quotaState": item.quota_state.value,
            "quotaStateProvenance": item.quota_state_provenance.value,
            "observedAt": _format_time(item.observed_at),
            "freshUntil": _format_time(item.fresh_until) if item.fresh_until else None,
            "freshness": self.freshness(as_of).value,
            "source": item.source,
            "confidence": item.confidence.value,
            "metadata": redact_sensitive(item.metadata),
        }


@dataclass(frozen=True, slots=True)
class PostTaskAuditContext:
    audit_id: str
    task_id: str
    run_id: str | None
    project_id: str
    goal_id: str | None
    terminal_state: str
    observed_at: datetime
    cached_snapshots: tuple[ResourceSnapshot, ...]


class ResourceUsageObserver(Protocol):
    """A collector must declare that it performs no inference before it can run here."""

    name: str
    requires_inference: bool

    def observe(self, context: PostTaskAuditContext) -> Sequence[ResourceObservation]: ...


class ResourceUsageStore(Protocol):
    def connect(self) -> Connection: ...

    def transaction(self) -> AbstractContextManager[Connection]: ...


@dataclass(frozen=True, slots=True)
class ResourceAuditResult:
    id: str
    audit_key: str
    project_id: str
    goal_id: str | None
    task_id: str
    run_id: str | None
    terminal_state: str
    status: str
    observer_count: int
    snapshots: tuple[ResourceSnapshot, ...]
    errors: tuple[str, ...]
    observed_at: datetime

    def to_protocol(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "auditKey": self.audit_key,
            "projectID": self.project_id,
            "goalID": self.goal_id,
            "taskID": self.task_id,
            "runID": self.run_id,
            "terminalState": self.terminal_state,
            "status": self.status,
            "observerCount": self.observer_count,
            "snapshotCount": len(self.snapshots),
            "errors": list(self.errors),
            "observedAt": _format_time(self.observed_at),
            "snapshots": [snapshot.to_protocol() for snapshot in self.snapshots],
        }


@dataclass(frozen=True, slots=True)
class MetricAggregate:
    values_by_unit: dict[str, int | float]
    known_count: int
    unknown_count: int

    def to_protocol(self) -> dict[str, Any]:
        return {
            "valuesByUnit": self.values_by_unit,
            "knownCount": self.known_count,
            "unknownCount": self.unknown_count,
        }


@dataclass(frozen=True, slots=True)
class ResourceUsageAggregate:
    scope: dict[str, str | None]
    audit_count: int
    snapshot_count: int
    snapshots_by_provider: dict[str, int]
    snapshots_by_model: dict[str, int]
    snapshots_by_worker: dict[str, int]
    snapshots_by_account_scope: dict[str, int]
    snapshots_by_quota_pool: dict[str, int]
    task_calls: MetricAggregate
    input_tokens: MetricAggregate
    output_tokens: MetricAggregate
    cached_tokens: MetricAggregate
    cost: MetricAggregate
    latest_by_quota_pool: dict[str, ResourceSnapshot]

    def to_protocol(self, *, as_of: datetime | None = None) -> dict[str, Any]:
        return {
            "scope": self.scope,
            "auditCount": self.audit_count,
            "snapshotCount": self.snapshot_count,
            "snapshotsByProvider": self.snapshots_by_provider,
            "snapshotsByModel": self.snapshots_by_model,
            "snapshotsByWorker": self.snapshots_by_worker,
            "snapshotsByAccountScope": self.snapshots_by_account_scope,
            "snapshotsByQuotaPool": self.snapshots_by_quota_pool,
            "taskCalls": self.task_calls.to_protocol(),
            "inputTokens": self.input_tokens.to_protocol(),
            "outputTokens": self.output_tokens.to_protocol(),
            "cachedTokens": self.cached_tokens.to_protocol(),
            "cost": self.cost.to_protocol(),
            "latestByQuotaPool": {
                pool: snapshot.to_protocol(as_of=as_of)
                for pool, snapshot in self.latest_by_quota_pool.items()
            },
        }


class ResourceUsageRepository:
    """SQLite-backed normalized resource ledger, independent of provider implementations."""

    def __init__(self, store: ResourceUsageStore) -> None:
        self.store = store

    def audit_by_key(self, audit_key: str) -> ResourceAuditResult | None:
        with self.store.connect() as connection:
            row = connection.execute(
                "SELECT * FROM post_task_usage_audits WHERE audit_key=?", (audit_key,)
            ).fetchone()
        return self._audit_from_row(row) if row is not None else None

    def record_audit(
        self,
        *,
        audit_id: str,
        audit_key: str,
        task_id: str,
        run_id: str | None,
        terminal_state: str,
        observations: Sequence[ResourceObservation],
        observer_count: int,
        errors: Sequence[str],
        observed_at: datetime,
    ) -> ResourceAuditResult:
        if not audit_key.strip() or not terminal_state.strip():
            raise ValueError("audit_key and terminal_state must not be empty")
        if observed_at.tzinfo is None:
            raise ValueError("observed_at must be timezone-aware")
        safe_errors = tuple(str(item)[:160] for item in redact_sensitive(list(errors)))
        status = "completedWithErrors" if safe_errors else "completed"
        with self.store.transaction() as connection:
            existing = connection.execute(
                "SELECT id FROM post_task_usage_audits WHERE audit_key=?", (audit_key,)
            ).fetchone()
            if existing is not None:
                existing_id = str(existing["id"])
            else:
                task = connection.execute(
                    "SELECT project_id FROM tasks WHERE id=?", (task_id,)
                ).fetchone()
                if task is None:
                    raise KeyError(task_id)
                if run_id is not None:
                    run = connection.execute(
                        "SELECT task_id FROM worker_runs WHERE id=?", (run_id,)
                    ).fetchone()
                    if run is None:
                        raise KeyError(run_id)
                    if run["task_id"] != task_id:
                        raise ValueError("run does not belong to audited task")
                goal = connection.execute(
                    "SELECT goal_id FROM autonomous_actions WHERE task_id=? "
                    "UNION SELECT goal_id FROM autonomy_decision_tasks WHERE task_id=? LIMIT 1",
                    (task_id, task_id),
                ).fetchone()
                project_id = str(task["project_id"])
                goal_id = str(goal["goal_id"]) if goal is not None else None
                now = _format_time(observed_at)
                connection.execute(
                    "INSERT INTO post_task_usage_audits("
                    "id,audit_key,project_id,goal_id,task_id,run_id,terminal_state,status,"
                    "observer_count,snapshot_count,errors_json,observed_at,created_at"
                    ") VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    (
                        audit_id,
                        audit_key,
                        project_id,
                        goal_id,
                        task_id,
                        run_id,
                        terminal_state,
                        status,
                        observer_count,
                        len(observations),
                        compact_json(safe_errors),
                        now,
                        timestamp(),
                    ),
                )
                for observation in observations:
                    snapshot_id = f"resource-snapshot-{uuid.uuid4()}"
                    previous = self._latest_pool_row(connection, observation.quota_pool_id)
                    self._insert_snapshot(
                        connection,
                        snapshot_id=snapshot_id,
                        audit_id=audit_id,
                        project_id=project_id,
                        goal_id=goal_id,
                        task_id=task_id,
                        run_id=run_id,
                        observation=observation,
                    )
                    self._emit_snapshot_events(
                        connection,
                        snapshot_id=snapshot_id,
                        project_id=project_id,
                        goal_id=goal_id,
                        task_id=task_id,
                        run_id=run_id,
                        observation=observation,
                        previous=previous,
                    )
                existing_id = audit_id
        result = self.get_audit(existing_id)
        if result is None:  # defensive: committed row must be readable
            raise RuntimeError("resource audit disappeared after commit")
        return result

    def get_audit(self, audit_id: str) -> ResourceAuditResult | None:
        with self.store.connect() as connection:
            row = connection.execute(
                "SELECT * FROM post_task_usage_audits WHERE id=?", (audit_id,)
            ).fetchone()
        return self._audit_from_row(row) if row is not None else None

    def list_snapshots(
        self,
        *,
        goal_id: str | None = None,
        task_id: str | None = None,
        provider: str | None = None,
        model: str | None = None,
        worker_id: str | None = None,
        account_scope: str | None = None,
        quota_pool_id: str | None = None,
        observed_after: datetime | None = None,
        observed_before: datetime | None = None,
        limit: int = 10_000,
    ) -> list[ResourceSnapshot]:
        if limit < 1 or limit > 10_000:
            raise ValueError("limit must be between 1 and 10000")
        clauses: list[str] = []
        values: list[Any] = []
        for column, value in (
            ("goal_id", goal_id),
            ("task_id", task_id),
            ("provider", provider),
            ("model_value", model),
            ("worker_id", worker_id),
            ("account_scope_value", account_scope),
            ("quota_pool_id", quota_pool_id),
        ):
            if value is not None:
                clauses.append(f"{column}=?")
                values.append(value)
        if observed_after is not None:
            clauses.append("observed_at>=?")
            values.append(_format_time(observed_after))
        if observed_before is not None:
            clauses.append("observed_at<=?")
            values.append(_format_time(observed_before))
        query = "SELECT * FROM resource_usage_snapshots"
        if clauses:
            query += " WHERE " + " AND ".join(clauses)
        query += " ORDER BY observed_at,id LIMIT ?"
        values.append(limit)
        with self.store.connect() as connection:
            rows = connection.execute(query, values).fetchall()
        return [self._snapshot_from_row(row) for row in rows]

    def latest_by_pool(self) -> dict[str, ResourceSnapshot]:
        with self.store.connect() as connection:
            rows = connection.execute(
                "SELECT s.* FROM resource_usage_snapshots s WHERE s.id=("
                "SELECT s2.id FROM resource_usage_snapshots s2 "
                "WHERE s2.quota_pool_id=s.quota_pool_id "
                "ORDER BY s2.observed_at DESC,s2.id DESC LIMIT 1) "
                "ORDER BY s.quota_pool_id"
            ).fetchall()
        return {str(row["quota_pool_id"]): self._snapshot_from_row(row) for row in rows}

    def latest_by_worker(self, worker_ids: Sequence[str]) -> dict[str, ResourceSnapshot]:
        """Return one deterministic latest ledger observation for each requested Worker."""

        requested = tuple(sorted(set(worker_ids)))
        if not requested:
            return {}
        placeholders = ",".join("?" for _ in requested)
        with self.store.connect() as connection:
            rows = connection.execute(
                "SELECT s.* FROM resource_usage_snapshots s WHERE s.worker_id IN ("
                f"{placeholders}) AND s.id=(SELECT s2.id FROM resource_usage_snapshots s2 "
                "WHERE s2.worker_id=s.worker_id ORDER BY s2.observed_at DESC,s2.id DESC LIMIT 1) "
                "ORDER BY s.worker_id",
                requested,
            ).fetchall()
        return {str(row["worker_id"]): self._snapshot_from_row(row) for row in rows}

    def aggregate(
        self,
        *,
        goal_id: str | None = None,
        task_id: str | None = None,
        provider: str | None = None,
        model: str | None = None,
        worker_id: str | None = None,
        account_scope: str | None = None,
        quota_pool_id: str | None = None,
        observed_after: datetime | None = None,
        observed_before: datetime | None = None,
    ) -> ResourceUsageAggregate:
        snapshots = self.list_snapshots(
            goal_id=goal_id,
            task_id=task_id,
            provider=provider,
            model=model,
            worker_id=worker_id,
            account_scope=account_scope,
            quota_pool_id=quota_pool_id,
            observed_after=observed_after,
            observed_before=observed_before,
        )
        audits = {snapshot.audit_id for snapshot in snapshots}
        latest: dict[str, ResourceSnapshot] = {}
        for snapshot in snapshots:
            current = latest.get(snapshot.observation.quota_pool_id)
            if current is None or (
                snapshot.observation.observed_at,
                snapshot.id,
            ) > (current.observation.observed_at, current.id):
                latest[snapshot.observation.quota_pool_id] = snapshot
        return ResourceUsageAggregate(
            scope={
                "goalID": goal_id,
                "taskID": task_id,
                "provider": provider,
                "model": model,
                "workerID": worker_id,
                "accountScope": account_scope,
                "quotaPoolID": quota_pool_id,
                "observedAfter": _format_time(observed_after) if observed_after else None,
                "observedBefore": _format_time(observed_before) if observed_before else None,
            },
            audit_count=len(audits),
            snapshot_count=len(snapshots),
            snapshots_by_provider=dict(Counter(s.observation.provider for s in snapshots)),
            snapshots_by_model=dict(
                Counter(s.observation.model.value or "unavailable" for s in snapshots)
            ),
            snapshots_by_worker=dict(
                Counter(s.observation.worker_id or "unavailable" for s in snapshots)
            ),
            snapshots_by_account_scope=dict(
                Counter(s.observation.account_scope.value or "unavailable" for s in snapshots)
            ),
            snapshots_by_quota_pool=dict(Counter(s.observation.quota_pool_id for s in snapshots)),
            task_calls=_aggregate_metric(snapshots, "task_calls"),
            input_tokens=_aggregate_metric(snapshots, "input_tokens"),
            output_tokens=_aggregate_metric(snapshots, "output_tokens"),
            cached_tokens=_aggregate_metric(snapshots, "cached_tokens"),
            cost=_aggregate_metric(snapshots, "cost"),
            latest_by_quota_pool=latest,
        )

    def _audit_from_row(self, row: Row) -> ResourceAuditResult:
        snapshots = tuple(self.list_snapshots_for_audit(str(row["id"])))
        return ResourceAuditResult(
            id=str(row["id"]),
            audit_key=str(row["audit_key"]),
            project_id=str(row["project_id"]),
            goal_id=str(row["goal_id"]) if row["goal_id"] is not None else None,
            task_id=str(row["task_id"]),
            run_id=str(row["run_id"]) if row["run_id"] is not None else None,
            terminal_state=str(row["terminal_state"]),
            status=str(row["status"]),
            observer_count=int(row["observer_count"]),
            snapshots=snapshots,
            errors=tuple(json.loads(row["errors_json"])),
            observed_at=_parse_time(str(row["observed_at"])),
        )

    def list_snapshots_for_audit(self, audit_id: str) -> list[ResourceSnapshot]:
        with self.store.connect() as connection:
            rows = connection.execute(
                "SELECT * FROM resource_usage_snapshots WHERE audit_id=? ORDER BY observed_at,id",
                (audit_id,),
            ).fetchall()
        return [self._snapshot_from_row(row) for row in rows]

    def _snapshot_from_row(self, row: Row) -> ResourceSnapshot:
        observation = ResourceObservation(
            provider=str(row["provider"]),
            quota_pool_id=str(row["quota_pool_id"]),
            account_scope=_dimension_from_row(row, "account_scope"),
            plan_tier=_dimension_from_row(row, "plan_tier"),
            worker_id=str(row["worker_id"]) if row["worker_id"] is not None else None,
            model=_dimension_from_row(row, "model"),
            quota_window=_dimension_from_row(row, "quota_window"),
            used=_metric_from_row(row, "used"),
            remaining=_metric_from_row(row, "remaining"),
            reset_at=_instant_from_row(row, "reset_at"),
            task_calls=_metric_from_row(row, "task_calls"),
            input_tokens=_metric_from_row(row, "input_tokens"),
            output_tokens=_metric_from_row(row, "output_tokens"),
            cached_tokens=_metric_from_row(row, "cached_tokens"),
            cost=_metric_from_row(row, "cost"),
            quota_state=QuotaState(str(row["quota_state"])),
            quota_state_provenance=UsageProvenance(str(row["quota_state_provenance"])),
            source=str(row["source"]),
            confidence=EvidenceConfidence(str(row["confidence"])),
            observed_at=_parse_time(str(row["observed_at"])),
            fresh_until=(
                _parse_time(str(row["fresh_until"])) if row["fresh_until"] is not None else None
            ),
            metadata=json.loads(row["metadata_json"]),
        )
        return ResourceSnapshot(
            id=str(row["id"]),
            audit_id=str(row["audit_id"]),
            project_id=str(row["project_id"]),
            goal_id=str(row["goal_id"]) if row["goal_id"] is not None else None,
            task_id=str(row["task_id"]),
            run_id=str(row["run_id"]) if row["run_id"] is not None else None,
            observation=observation,
        )

    def _insert_snapshot(
        self,
        connection: Connection,
        *,
        snapshot_id: str,
        audit_id: str,
        project_id: str,
        goal_id: str | None,
        task_id: str,
        run_id: str | None,
        observation: ResourceObservation,
    ) -> None:
        columns = [
            "id",
            "audit_id",
            "project_id",
            "goal_id",
            "task_id",
            "run_id",
            "provider",
            "quota_pool_id",
        ]
        values: list[Any] = [
            snapshot_id,
            audit_id,
            project_id,
            goal_id,
            task_id,
            run_id,
            observation.provider,
            observation.quota_pool_id,
        ]
        for name in ("account_scope", "plan_tier", "model", "quota_window"):
            dimension: UsageDimension = getattr(observation, name)
            columns.extend((f"{name}_value", f"{name}_provenance", f"{name}_reason"))
            values.extend(
                (
                    dimension.value,
                    dimension.provenance.value,
                    dimension.reason.value if dimension.reason else None,
                )
            )
        columns.append("worker_id")
        values.append(observation.worker_id)
        # worker_id belongs before model in the physical table but explicit column lists are stable.
        for name in ("used", "remaining"):
            _append_metric(columns, values, name, getattr(observation, name))
        columns.extend(("reset_at_value", "reset_at_provenance", "reset_at_reason"))
        values.extend(
            (
                _format_time(observation.reset_at.value) if observation.reset_at.value else None,
                observation.reset_at.provenance.value,
                observation.reset_at.reason.value if observation.reset_at.reason else None,
            )
        )
        for name in ("task_calls", "input_tokens", "output_tokens", "cached_tokens", "cost"):
            _append_metric(columns, values, name, getattr(observation, name))
        columns.extend(
            (
                "quota_state",
                "quota_state_provenance",
                "source",
                "confidence",
                "observed_at",
                "fresh_until",
                "metadata_json",
            )
        )
        values.extend(
            (
                observation.quota_state.value,
                observation.quota_state_provenance.value,
                observation.source,
                observation.confidence.value,
                _format_time(observation.observed_at),
                _format_time(observation.fresh_until) if observation.fresh_until else None,
                compact_json(redact_sensitive(observation.metadata)),
            )
        )
        connection.execute(
            f"INSERT INTO resource_usage_snapshots({','.join(columns)}) "
            f"VALUES ({','.join('?' for _ in values)})",
            values,
        )

    def _latest_pool_row(self, connection: Connection, quota_pool_id: str) -> Row | None:
        return connection.execute(
            "SELECT * FROM resource_usage_snapshots WHERE quota_pool_id=? "
            "ORDER BY observed_at DESC,id DESC LIMIT 1",
            (quota_pool_id,),
        ).fetchone()

    def _emit_snapshot_events(
        self,
        connection: Connection,
        *,
        snapshot_id: str,
        project_id: str,
        goal_id: str | None,
        task_id: str,
        run_id: str | None,
        observation: ResourceObservation,
        previous: Row | None,
    ) -> None:
        base = {
            "snapshotID": snapshot_id,
            "goalID": goal_id,
            "provider": observation.provider,
            "quotaPoolID": observation.quota_pool_id,
            "quotaState": observation.quota_state.value,
            "quotaStateProvenance": observation.quota_state_provenance.value,
            "source": observation.source,
            "confidence": observation.confidence.value,
            "observedAt": _format_time(observation.observed_at),
        }
        _append_resource_event(
            connection,
            kind="quota_snapshot",
            severity="info",
            entity_id=observation.quota_pool_id,
            project_id=project_id,
            task_id=task_id,
            worker_id=observation.worker_id,
            run_id=run_id,
            summary=f"Resource snapshot recorded for {observation.quota_pool_id}",
            payload=base,
        )
        deltas = {
            name: metric.to_protocol()
            for name, metric in (
                ("calls", observation.task_calls),
                ("inputTokens", observation.input_tokens),
                ("outputTokens", observation.output_tokens),
                ("cachedTokens", observation.cached_tokens),
                ("cost", observation.cost),
            )
            if metric.value is not None
        }
        if deltas:
            _append_resource_event(
                connection,
                kind="usage_delta",
                severity="info",
                entity_id=observation.quota_pool_id,
                project_id=project_id,
                task_id=task_id,
                worker_id=observation.worker_id,
                run_id=run_id,
                summary="Observed task-local resource usage",
                payload={**base, "taskLocal": deltas},
            )
        event_for_state = {
            QuotaState.WARNING: ("quota_warning", "warning"),
            QuotaState.CRITICAL: ("quota_critical", "warning"),
            QuotaState.EXHAUSTED: ("quota_exhausted", "error"),
        }.get(observation.quota_state)
        if event_for_state is not None:
            _append_resource_event(
                connection,
                kind=event_for_state[0],
                severity=event_for_state[1],
                entity_id=observation.quota_pool_id,
                project_id=project_id,
                task_id=task_id,
                worker_id=observation.worker_id,
                run_id=run_id,
                summary=f"Quota pool is {observation.quota_state.value}",
                payload=base,
            )
        unknown_fields = [
            name
            for name, value in (
                ("accountScope", observation.account_scope.value),
                ("planTier", observation.plan_tier.value),
                ("quotaWindow", observation.quota_window.value),
                ("used", observation.used.value),
                ("remaining", observation.remaining.value),
                ("resetAt", observation.reset_at.value),
                ("cost", observation.cost.value),
            )
            if value is None
        ]
        if unknown_fields:
            _append_resource_event(
                connection,
                kind="usage_unknown",
                severity="info",
                entity_id=observation.quota_pool_id,
                project_id=project_id,
                task_id=task_id,
                worker_id=observation.worker_id,
                run_id=run_id,
                summary="Some resource usage fields are unavailable",
                payload={**base, "unknownFields": unknown_fields},
            )
        if observation.fresh_until is not None and observation.fresh_until < utc_now():
            _append_resource_event(
                connection,
                kind="usage_stale",
                severity="warning",
                entity_id=observation.quota_pool_id,
                project_id=project_id,
                task_id=task_id,
                worker_id=observation.worker_id,
                run_id=run_id,
                summary="Resource usage snapshot is stale",
                payload=base,
            )
        if observation.reset_observed or _reset_detected(previous, observation):
            _append_resource_event(
                connection,
                kind="quota_reset_observed",
                severity="notice",
                entity_id=observation.quota_pool_id,
                project_id=project_id,
                task_id=task_id,
                worker_id=observation.worker_id,
                run_id=run_id,
                summary="Quota reset observed",
                payload={**base, "resetAt": observation.reset_at.to_protocol()},
            )


class ResourceUsageService:
    """Cheap post-Task audit hook; observers requiring model inference are never called."""

    def __init__(
        self,
        repository: ResourceUsageRepository,
        observers: Iterable[ResourceUsageObserver] = (),
    ) -> None:
        self.repository = repository
        self.observers = tuple(observers)

    def audit_task_terminal(
        self,
        *,
        task_id: str,
        run_id: str | None,
        terminal_state: str,
        audit_key: str | None = None,
        observations: Sequence[ResourceObservation] = (),
        observed_at: datetime | None = None,
    ) -> ResourceAuditResult:
        key = audit_key or f"task:{task_id}:run:{run_id or 'none'}:state:{terminal_state}"
        existing = self.repository.audit_by_key(key)
        if existing is not None:
            return existing
        now = observed_at or utc_now()
        context = self._context(
            audit_id=f"resource-audit-{uuid.uuid4()}",
            task_id=task_id,
            run_id=run_id,
            terminal_state=terminal_state,
            observed_at=now,
        )
        collected = list(observations)
        for local in self._invocation_observations(context):
            if not any(
                item.source == local.source
                and item.quota_pool_id == local.quota_pool_id
                and item.metadata == local.metadata
                for item in collected
            ):
                collected.append(local)
        errors: list[str] = []
        called = 0
        for observer in self.observers:
            if observer.requires_inference:
                errors.append(f"{observer.name}:skippedInferenceRequired")
                continue
            called += 1
            try:
                collected.extend(observer.observe(context))
            except Exception as error:  # a failed observer must not erase the terminal Task event
                errors.append(f"{observer.name}:{type(error).__name__}")
        if not collected:
            collected.append(self._unknown_observation(context))
        return self.repository.record_audit(
            audit_id=context.audit_id,
            audit_key=key,
            task_id=task_id,
            run_id=run_id,
            terminal_state=terminal_state,
            observations=tuple(collected),
            observer_count=called,
            errors=errors,
            observed_at=now,
        )

    def _context(
        self,
        *,
        audit_id: str,
        task_id: str,
        run_id: str | None,
        terminal_state: str,
        observed_at: datetime,
    ) -> PostTaskAuditContext:
        with self.repository.store.connect() as connection:
            task = connection.execute(
                "SELECT project_id FROM tasks WHERE id=?", (task_id,)
            ).fetchone()
            if task is None:
                raise KeyError(task_id)
            goal = connection.execute(
                "SELECT goal_id FROM autonomous_actions WHERE task_id=? "
                "UNION SELECT goal_id FROM autonomy_decision_tasks WHERE task_id=? LIMIT 1",
                (task_id, task_id),
            ).fetchone()
        return PostTaskAuditContext(
            audit_id=audit_id,
            task_id=task_id,
            run_id=run_id,
            project_id=str(task["project_id"]),
            goal_id=str(goal["goal_id"]) if goal is not None else None,
            terminal_state=terminal_state,
            observed_at=observed_at,
            cached_snapshots=tuple(self.repository.latest_by_pool().values()),
        )

    def _invocation_observation(self, context: PostTaskAuditContext) -> ResourceObservation | None:
        observations = self._invocation_observations(context)
        return observations[0] if observations else None

    def _invocation_observations(self, context: PostTaskAuditContext) -> list[ResourceObservation]:
        with self.repository.store.connect() as connection:
            if context.run_id is None:
                rows = connection.execute(
                    "SELECT * FROM invocation_telemetry WHERE task_id=? ORDER BY recorded_at,id",
                    (context.task_id,),
                ).fetchall()
            else:
                rows = connection.execute(
                    "SELECT * FROM invocation_telemetry WHERE run_id=?", (context.run_id,)
                ).fetchall()
        return [self._observation_from_invocation(row, context) for row in rows]

    def _observation_from_invocation(
        self, row: Row, context: PostTaskAuditContext
    ) -> ResourceObservation:
        worker_id = str(row["worker_id"])
        provider = str(row["provider"])
        cache_values = [row["cache_read_tokens_value"], row["cache_write_tokens_value"]]
        if all(value is not None for value in cache_values):
            cached = UsageMetric.known(
                int(cache_values[0]) + int(cache_values[1]),
                "tokens",
                _provenance_from_confidence(str(row["cache_read_tokens_confidence"])),
            )
        else:
            cached = UsageMetric.unknown()
        return ResourceObservation(
            provider=provider,
            quota_pool_id=f"local:{provider}:worker:{worker_id}",
            worker_id=worker_id,
            model=(
                UsageDimension.known(str(row["model_value"]), UsageProvenance.PROVIDER_REPORTED)
                if row["model_value"] is not None
                else UsageDimension.unknown(_unavailable(str(row["model_unavailable_reason"])))
            ),
            task_calls=UsageMetric.known(1, "calls", UsageProvenance.LOCALLY_MEASURED),
            input_tokens=_metric_from_invocation(row, "input_tokens"),
            output_tokens=_metric_from_invocation(row, "output_tokens"),
            cached_tokens=cached,
            cost=_metric_from_invocation(row, "cost"),
            quota_state=QuotaState.UNKNOWN,
            source="invocationTelemetry",
            confidence=EvidenceConfidence.EXACT,
            observed_at=context.observed_at,
            metadata={"runID": str(row["run_id"]), "runOutcome": str(row["outcome"])},
        )

    def _unknown_observation(self, context: PostTaskAuditContext) -> ResourceObservation:
        provider = "unknown"
        worker_id: str | None = None
        if context.run_id is not None:
            with self.repository.store.connect() as connection:
                row = connection.execute(
                    "SELECT r.worker_id,w.provider FROM worker_runs r "
                    "JOIN workers w ON w.id=r.worker_id WHERE r.id=?",
                    (context.run_id,),
                ).fetchone()
            if row is not None:
                provider = str(row["provider"])
                worker_id = str(row["worker_id"])
        return ResourceObservation(
            provider=provider,
            quota_pool_id=f"unknown:{provider}:{worker_id or context.task_id}",
            worker_id=worker_id,
            source="postTaskAudit",
            confidence=EvidenceConfidence.UNKNOWN,
            observed_at=context.observed_at,
        )


@dataclass(frozen=True, slots=True)
class QuotaGuardCandidate:
    worker_id: str
    quota_pool_id: str
    capable: bool = True
    premium: bool = False
    provider: str | None = None

    def __post_init__(self) -> None:
        if not self.worker_id.strip() or not self.quota_pool_id.strip():
            raise ValueError("worker_id and quota_pool_id must not be empty")
        if self.provider is not None and not self.provider.strip():
            raise ValueError("candidate provider must not be empty")


@dataclass(frozen=True, slots=True)
class QuotaGuardEvidence:
    worker_id: str
    quota_pool_id: str
    provider: str
    quota_state: QuotaState
    freshness: Freshness
    provenance: UsageProvenance
    source: str
    confidence: EvidenceConfidence
    observed_at: datetime | None
    reason: str | None = None

    def health_score(self) -> float:
        base = {
            QuotaState.AVAILABLE: 1.0,
            QuotaState.WARNING: 0.55,
            QuotaState.CRITICAL: 0.15,
            QuotaState.EXHAUSTED: 0.0,
            QuotaState.UNKNOWN: 0.25,
        }[self.quota_state]
        if self.quota_state is QuotaState.UNKNOWN:
            return base
        confidence = {
            UsageProvenance.PROVIDER_REPORTED: 1.0,
            UsageProvenance.LOCALLY_MEASURED: 0.95,
            UsageProvenance.INFERRED: 0.8,
            UsageProvenance.UNKNOWN: 0.0,
        }[self.provenance]
        return round(base * confidence, 6)

    def to_protocol(self) -> dict[str, Any]:
        return {
            "workerID": self.worker_id,
            "quotaPoolID": self.quota_pool_id,
            "provider": self.provider,
            "quotaState": self.quota_state.value,
            "freshness": self.freshness.value,
            "provenance": self.provenance.value,
            "source": self.source,
            "confidence": self.confidence.value,
            "observedAt": _format_time(self.observed_at) if self.observed_at else None,
            "healthScore": self.health_score(),
            "reason": self.reason,
        }

    def to_routing_evidence(self) -> ResourceRoutingEvidence:
        return ResourceRoutingEvidence(
            worker_id=self.worker_id,
            quota_pool_id=self.quota_pool_id,
            provider=self.provider,
            quota_state=self.quota_state.value,
            freshness=self.freshness.value,
            provenance=self.provenance.value,
            source=self.source,
            confidence=self.confidence.value,
            health_score=self.health_score(),
            observed_at=self.observed_at,
            reason=self.reason,
        )


@dataclass(frozen=True, slots=True)
class QuotaGuardDecision:
    ordered_worker_ids: tuple[str, ...]
    avoided: dict[str, str]
    pool_states: dict[str, str]
    evidence_by_worker: dict[str, QuotaGuardEvidence]
    explanation: str

    @property
    def dispatch_allowed(self) -> bool:
        return bool(self.ordered_worker_ids)

    def to_protocol(self) -> dict[str, Any]:
        return {
            "dispatchAllowed": self.dispatch_allowed,
            "orderedWorkerIDs": list(self.ordered_worker_ids),
            "avoided": self.avoided,
            "poolStates": self.pool_states,
            "evidenceByWorker": {
                worker_id: evidence.to_protocol()
                for worker_id, evidence in sorted(self.evidence_by_worker.items())
            },
            "explanation": self.explanation,
        }

    def routing_evidence(self) -> tuple[ResourceRoutingEvidence, ...]:
        return tuple(
            self.evidence_by_worker[worker_id].to_routing_evidence()
            for worker_id in sorted(self.evidence_by_worker)
        )


class PreDispatchQuotaGuard:
    """Deterministic evidence-only pre-filter; UNKNOWN never becomes available or zero."""

    _RANK = {
        QuotaState.AVAILABLE: 0,
        QuotaState.WARNING: 1,
        QuotaState.UNKNOWN: 2,
        QuotaState.CRITICAL: 3,
        QuotaState.EXHAUSTED: 4,
    }

    def __init__(self, repository: ResourceUsageRepository) -> None:
        self.repository = repository

    def evaluate(
        self,
        candidates: Sequence[QuotaGuardCandidate],
        *,
        as_of: datetime | None = None,
    ) -> QuotaGuardDecision:
        now = as_of or utc_now()
        latest = self.repository.latest_by_pool()
        capable = [candidate for candidate in candidates if candidate.capable]
        evidence = {
            candidate.worker_id: self._effective_evidence(
                candidate,
                latest.get(candidate.quota_pool_id),
                now,
            )
            for candidate in capable
        }
        states = {worker_id: item.quota_state for worker_id, item in evidence.items()}
        available_exists = any(state is QuotaState.AVAILABLE for state in states.values())
        non_exhausted_exists = any(state is not QuotaState.EXHAUSTED for state in states.values())
        avoided: dict[str, str] = {
            candidate.worker_id: "capabilityRequirementsNotMet"
            for candidate in candidates
            if not candidate.capable
        }
        allowed: list[QuotaGuardCandidate] = []
        for candidate in capable:
            state = states[candidate.worker_id]
            if state is QuotaState.EXHAUSTED and non_exhausted_exists:
                avoided[candidate.worker_id] = "quotaExhaustedAlternativeExists"
                continue
            if (
                state is QuotaState.CRITICAL
                and candidate.premium
                and any(
                    other.worker_id != candidate.worker_id
                    and states[other.worker_id] not in {QuotaState.CRITICAL, QuotaState.EXHAUSTED}
                    for other in capable
                )
            ):
                avoided[candidate.worker_id] = "premiumQuotaCriticalAlternativeExists"
                continue
            if state is QuotaState.WARNING and candidate.premium and available_exists:
                avoided[candidate.worker_id] = "premiumQuotaWarningAlternativeExists"
                continue
            if state is QuotaState.EXHAUSTED and not non_exhausted_exists:
                avoided[candidate.worker_id] = "allCapableQuotaPoolsExhausted"
                continue
            allowed.append(candidate)
        allowed.sort(
            key=lambda candidate: (
                self._RANK[states[candidate.worker_id]],
                -evidence[candidate.worker_id].health_score(),
                candidate.worker_id,
            )
        )
        state_payload = {
            candidate.worker_id: states.get(candidate.worker_id, QuotaState.UNKNOWN).value
            for candidate in candidates
        }
        if not allowed:
            explanation = "No capable Worker has a non-exhausted quota pool"
        elif avoided:
            explanation = "Known scarce/exhausted pools were avoided deterministically"
        elif any(state is QuotaState.UNKNOWN for state in states.values()):
            explanation = "Unknown quota remained explicit and was ranked after known capacity"
        else:
            explanation = "No quota evidence required avoiding a capable Worker"
        return QuotaGuardDecision(
            ordered_worker_ids=tuple(item.worker_id for item in allowed),
            avoided=avoided,
            pool_states=state_payload,
            evidence_by_worker={
                candidate.worker_id: evidence.get(
                    candidate.worker_id,
                    QuotaGuardEvidence(
                        worker_id=candidate.worker_id,
                        quota_pool_id=candidate.quota_pool_id,
                        provider=candidate.provider or "unknown",
                        quota_state=QuotaState.UNKNOWN,
                        freshness=Freshness.UNKNOWN,
                        provenance=UsageProvenance.UNKNOWN,
                        source="hardConstraints",
                        confidence=EvidenceConfidence.UNKNOWN,
                        observed_at=None,
                        reason="capabilityRequirementsNotMet",
                    ),
                )
                for candidate in candidates
            },
            explanation=explanation,
        )

    @staticmethod
    def _effective_evidence(
        candidate: QuotaGuardCandidate,
        snapshot: ResourceSnapshot | None,
        as_of: datetime,
    ) -> QuotaGuardEvidence:
        if snapshot is None:
            return QuotaGuardEvidence(
                worker_id=candidate.worker_id,
                quota_pool_id=candidate.quota_pool_id,
                provider=candidate.provider or "unknown",
                quota_state=QuotaState.UNKNOWN,
                freshness=Freshness.UNKNOWN,
                provenance=UsageProvenance.UNKNOWN,
                source="resourceLedger",
                confidence=EvidenceConfidence.UNKNOWN,
                observed_at=None,
                reason="noSnapshot",
            )
        observation = snapshot.observation
        freshness = snapshot.freshness(as_of)
        reason: str | None = None
        state = observation.quota_state
        if candidate.provider is not None and candidate.provider != observation.provider:
            state = QuotaState.UNKNOWN
            reason = "providerMismatch"
        elif freshness is Freshness.STALE:
            state = QuotaState.UNKNOWN
            reason = "staleSnapshot"
        elif freshness is Freshness.UNKNOWN:
            state = QuotaState.UNKNOWN
            reason = "freshnessUnknown"
        elif observation.quota_state is QuotaState.UNKNOWN:
            reason = "quotaStateUnknown"
        elif observation.quota_state_provenance is UsageProvenance.UNKNOWN:
            state = QuotaState.UNKNOWN
            reason = "provenanceUnknown"
        elif observation.confidence is EvidenceConfidence.UNKNOWN:
            state = QuotaState.UNKNOWN
            reason = "confidenceUnknown"
        return QuotaGuardEvidence(
            worker_id=candidate.worker_id,
            quota_pool_id=candidate.quota_pool_id,
            provider=observation.provider,
            quota_state=state,
            freshness=freshness,
            provenance=observation.quota_state_provenance,
            source=observation.source,
            confidence=observation.confidence,
            observed_at=observation.observed_at,
            reason=reason,
        )


def infer_quota_state(*, used: UsageMetric, remaining: UsageMetric) -> QuotaState:
    """Conservative derivation from comparable known values; it never estimates missing values."""

    if used.value is None or remaining.value is None or used.unit != remaining.unit:
        return QuotaState.UNKNOWN
    total = float(used.value) + float(remaining.value)
    if remaining.value == 0:
        return QuotaState.EXHAUSTED
    if total <= 0:
        return QuotaState.UNKNOWN
    ratio = float(remaining.value) / total
    if ratio <= 0.05:
        return QuotaState.CRITICAL
    if ratio <= 0.20:
        return QuotaState.WARNING
    return QuotaState.AVAILABLE


def _validate_count_metric(metric: UsageMetric, expected_unit: str) -> None:
    if metric.value is None:
        return
    if not isinstance(metric.value, int) or isinstance(metric.value, bool):
        raise ValueError(f"{expected_unit} metric must be an integer")
    if metric.unit != expected_unit:
        raise ValueError(f"count metric unit must be {expected_unit}")


def _format_time(value: datetime) -> str:
    return value.astimezone(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _parse_time(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(UTC)


def _append_metric(columns: list[str], values: list[Any], name: str, metric: UsageMetric) -> None:
    columns.extend((f"{name}_value", f"{name}_unit", f"{name}_provenance", f"{name}_reason"))
    values.extend(
        (
            metric.value,
            metric.unit,
            metric.provenance.value,
            metric.reason.value if metric.reason else None,
        )
    )


def _dimension_from_row(row: Row, name: str) -> UsageDimension:
    return UsageDimension(
        value=str(row[f"{name}_value"]) if row[f"{name}_value"] is not None else None,
        provenance=UsageProvenance(str(row[f"{name}_provenance"])),
        reason=(
            _unavailable(str(row[f"{name}_reason"])) if row[f"{name}_reason"] is not None else None
        ),
    )


def _metric_from_row(row: Row, name: str) -> UsageMetric:
    value = row[f"{name}_value"]
    count_names = {"task_calls", "input_tokens", "output_tokens", "cached_tokens"}
    if name in count_names and value is not None:
        value = int(value)
    return UsageMetric(
        value=value,
        unit=str(row[f"{name}_unit"]) if row[f"{name}_unit"] is not None else None,
        provenance=UsageProvenance(str(row[f"{name}_provenance"])),
        reason=(
            _unavailable(str(row[f"{name}_reason"])) if row[f"{name}_reason"] is not None else None
        ),
    )


def _instant_from_row(row: Row, name: str) -> UsageInstant:
    return UsageInstant(
        value=(
            _parse_time(str(row[f"{name}_value"])) if row[f"{name}_value"] is not None else None
        ),
        provenance=UsageProvenance(str(row[f"{name}_provenance"])),
        reason=(
            _unavailable(str(row[f"{name}_reason"])) if row[f"{name}_reason"] is not None else None
        ),
    )


def _unavailable(value: str) -> UnavailableReason:
    try:
        return UnavailableReason(value)
    except ValueError:
        return UnavailableReason.UNKNOWN


def _provenance_from_confidence(value: str) -> UsageProvenance:
    if value == EvidenceConfidence.PROVIDER_REPORTED.value:
        return UsageProvenance.PROVIDER_REPORTED
    if value == EvidenceConfidence.INFERRED.value:
        return UsageProvenance.INFERRED
    if value in {EvidenceConfidence.EXACT.value, EvidenceConfidence.VERIFIED.value}:
        return UsageProvenance.LOCALLY_MEASURED
    return UsageProvenance.UNKNOWN


def _metric_from_invocation(row: Row, name: str) -> UsageMetric:
    value = row[f"{name}_value"]
    if value is None:
        reason = row[f"{name}_reason"]
        unavailable = _unavailable(str(reason)) if reason else UnavailableReason.UNKNOWN
        return UsageMetric.unknown(unavailable)
    if name in {"input_tokens", "output_tokens"}:
        value = int(value)
    provenance = _provenance_from_confidence(str(row[f"{name}_confidence"]))
    if provenance is UsageProvenance.UNKNOWN:
        return UsageMetric.unknown(UnavailableReason.UNKNOWN)
    return UsageMetric.known(value, str(row[f"{name}_unit"]), provenance)


def _aggregate_metric(snapshots: Sequence[ResourceSnapshot], name: str) -> MetricAggregate:
    totals: dict[str, int | float] = {}
    known = 0
    unknown = 0
    for snapshot in snapshots:
        metric: UsageMetric = getattr(snapshot.observation, name)
        if metric.value is None:
            unknown += 1
            continue
        known += 1
        assert metric.unit is not None
        totals[metric.unit] = totals.get(metric.unit, 0) + metric.value
    return MetricAggregate(values_by_unit=totals, known_count=known, unknown_count=unknown)


def _append_resource_event(
    connection: Connection,
    *,
    kind: str,
    severity: str,
    entity_id: str,
    project_id: str,
    task_id: str,
    worker_id: str | None,
    run_id: str | None,
    summary: str,
    payload: dict[str, Any],
) -> None:
    connection.execute(
        "INSERT INTO events(event_id,kind,severity,entity_type,entity_id,project_id,task_id,"
        "worker_id,run_id,summary,payload_json,actor,created_at) "
        "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
        (
            f"evt-{uuid.uuid4()}",
            kind,
            severity,
            "resourcePool",
            entity_id,
            project_id,
            task_id,
            worker_id,
            run_id,
            summary,
            compact_json(redact_sensitive(payload)),
            "resource-auditor",
            timestamp(),
        ),
    )


def _reset_detected(previous: Row | None, current: ResourceObservation) -> bool:
    if previous is None or current.reset_at.value is None:
        return False
    previous_reset = previous["reset_at_value"]
    if previous_reset is None:
        return False
    old_reset = _parse_time(str(previous_reset))
    return current.observed_at >= old_reset and current.reset_at.value > old_reset
