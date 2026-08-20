from __future__ import annotations

import math
from collections import Counter
from collections.abc import Iterator
from contextlib import AbstractContextManager
from dataclasses import dataclass, field
from datetime import datetime
from enum import StrEnum
from sqlite3 import Connection
from typing import Any, Protocol

from .adapters.base import WorkerResult
from .domain import (
    EvidenceConfidence,
    RunState,
    TelemetryValue,
    UnavailableReason,
    utc_now,
)


class TaskRole(StrEnum):
    PRIMARY = "primary"
    REVIEWER = "reviewer"
    PANELIST = "panelist"
    ROUTER = "router"
    VERIFIER = "verifier"
    PLANNER = "planner"
    EVALUATOR = "evaluator"
    FALLBACK = "fallback"
    OTHER = "other"
    UNKNOWN = "unknown"


class InvocationOutcome(StrEnum):
    SUCCESS = "success"
    FAILURE = "failure"
    CANCELLED = "cancelled"
    TIMED_OUT = "timedOut"
    INTERRUPTED = "interrupted"
    AUTH_REQUIRED = "authRequired"
    RATE_LIMITED = "rateLimited"


class ExecutorKind(StrEnum):
    """Classification used only for explicit Codex-offload accounting."""

    CODEX = "codex"
    OFFLOADED = "offloaded"
    UNKNOWN = "unknown"


@dataclass(frozen=True, slots=True)
class ObservedDimension:
    value: str | None
    reason: UnavailableReason | None = None

    def __post_init__(self) -> None:
        if self.value is None and self.reason is None:
            raise ValueError("unavailable dimension requires a reason")
        if self.value is not None and (not self.value.strip() or self.reason is not None):
            raise ValueError("known dimension requires a non-empty value and no reason")

    @classmethod
    def known(cls, value: str) -> ObservedDimension:
        return cls(value=value)

    @classmethod
    def unavailable(
        cls, reason: UnavailableReason = UnavailableReason.NOT_REPORTED
    ) -> ObservedDimension:
        return cls(value=None, reason=reason)

    def to_protocol(self) -> dict[str, Any]:
        if self.value is not None:
            return {"state": "known", "value": self.value}
        return {"state": "unavailable", "reason": self.reason.value}

    @property
    def aggregation_key(self) -> str:
        if self.value is not None:
            return self.value
        return f"unavailable:{self.reason.value}"


@dataclass(frozen=True, slots=True)
class ObservedMetric:
    telemetry: TelemetryValue
    unit: str | None

    def __post_init__(self) -> None:
        metric_value = self.telemetry.value
        if metric_value is not None and (
            isinstance(metric_value, bool)
            or not isinstance(metric_value, int | float)
            or not math.isfinite(metric_value)
        ):
            raise ValueError("known metric value must be a finite number")
        if self.telemetry.known:
            if self.unit is None or not self.unit.strip():
                raise ValueError("known metric requires a unit")
            if self.telemetry.confidence is EvidenceConfidence.UNKNOWN:
                raise ValueError("known metric cannot use unknown confidence")
        elif self.unit is not None:
            raise ValueError("unavailable metric cannot claim a unit")
        if metric_value is not None and metric_value < 0:
            raise ValueError("metric values must not be negative")

    @classmethod
    def known(
        cls,
        value: int | float,
        unit: str,
        confidence: EvidenceConfidence = EvidenceConfidence.PROVIDER_REPORTED,
    ) -> ObservedMetric:
        return cls(TelemetryValue(value, confidence), unit)

    @classmethod
    def unavailable(
        cls, reason: UnavailableReason = UnavailableReason.NOT_REPORTED
    ) -> ObservedMetric:
        return cls(TelemetryValue(None, EvidenceConfidence.UNKNOWN, reason), None)

    def to_protocol(self) -> dict[str, Any]:
        payload = self.telemetry.to_api()
        payload["unit"] = self.unit
        return payload


def _reported_metric(value: int | float | None, unit: str) -> ObservedMetric:
    if value is None:
        return ObservedMetric.unavailable()
    return ObservedMetric.known(value, unit)


@dataclass(frozen=True, slots=True)
class InvocationTelemetry:
    id: str
    task_id: str
    run_id: str
    worker_id: str
    provider: str
    node_id: str
    task_role: TaskRole
    outcome: InvocationOutcome
    duration: ObservedMetric
    model: ObservedDimension = field(default_factory=ObservedDimension.unavailable)
    goal_id: str | None = None
    input_tokens: ObservedMetric = field(default_factory=ObservedMetric.unavailable)
    output_tokens: ObservedMetric = field(default_factory=ObservedMetric.unavailable)
    cache_read_tokens: ObservedMetric = field(default_factory=ObservedMetric.unavailable)
    cache_write_tokens: ObservedMetric = field(default_factory=ObservedMetric.unavailable)
    cost: ObservedMetric = field(default_factory=ObservedMetric.unavailable)
    remaining_quota: ObservedMetric = field(default_factory=ObservedMetric.unavailable)
    retry_count: int = 0
    fallback: bool = False
    executor_kind: ExecutorKind = ExecutorKind.UNKNOWN
    quality_score: ObservedMetric = field(default_factory=ObservedMetric.unavailable)
    recorded_at: datetime = field(default_factory=utc_now)

    def __post_init__(self) -> None:
        for name, value in {
            "id": self.id,
            "task_id": self.task_id,
            "run_id": self.run_id,
            "worker_id": self.worker_id,
            "provider": self.provider,
            "node_id": self.node_id,
        }.items():
            if not value.strip():
                raise ValueError(f"{name} must not be empty")
        if self.goal_id is not None and not self.goal_id.strip():
            raise ValueError("goal_id must be non-empty when supplied")
        if self.retry_count < 0:
            raise ValueError("retry_count must not be negative")
        if (
            self.duration.telemetry.value is None
            or self.duration.unit != "seconds"
            or self.duration.telemetry.confidence is not EvidenceConfidence.EXACT
        ):
            raise ValueError("Supervisor-observed invocation duration must be known in seconds")
        for metric in (
            self.input_tokens,
            self.output_tokens,
            self.cache_read_tokens,
            self.cache_write_tokens,
        ):
            value = metric.telemetry.value
            if value is not None and (not isinstance(value, int) or isinstance(value, bool)):
                raise ValueError("observed token counts must be integers")
        score = self.quality_score.telemetry.value
        if score is not None and (score > 1 or self.quality_score.unit != "ratio"):
            raise ValueError("quality_score must be between zero and one")

    @classmethod
    def from_worker_result(
        cls,
        *,
        telemetry_id: str,
        task_id: str,
        worker_id: str,
        provider: str,
        node_id: str,
        task_role: TaskRole,
        result: WorkerResult,
        goal_id: str | None = None,
        retry_count: int = 0,
        fallback: bool = False,
        executor_kind: ExecutorKind = ExecutorKind.UNKNOWN,
        remaining_quota: ObservedMetric | None = None,
        quality_score: ObservedMetric | None = None,
    ) -> InvocationTelemetry:
        outcome = (
            InvocationOutcome.SUCCESS
            if result.succeeded
            else {
                RunState.CANCELLED: InvocationOutcome.CANCELLED,
                RunState.TIMED_OUT: InvocationOutcome.TIMED_OUT,
                RunState.INTERRUPTED: InvocationOutcome.INTERRUPTED,
                RunState.AUTH_REQUIRED: InvocationOutcome.AUTH_REQUIRED,
                RunState.RATE_LIMITED: InvocationOutcome.RATE_LIMITED,
            }.get(result.state, InvocationOutcome.FAILURE)
        )
        usage = result.usage
        return cls(
            id=telemetry_id,
            goal_id=goal_id,
            task_id=task_id,
            run_id=result.run_id,
            worker_id=worker_id,
            provider=provider,
            model=(
                ObservedDimension.known(result.model)
                if result.model
                else ObservedDimension.unavailable()
            ),
            node_id=node_id,
            task_role=task_role,
            outcome=outcome,
            duration=ObservedMetric.known(
                result.duration_seconds, "seconds", EvidenceConfidence.EXACT
            ),
            input_tokens=_reported_metric(usage.input_tokens, "tokens"),
            output_tokens=_reported_metric(usage.output_tokens, "tokens"),
            cache_read_tokens=_reported_metric(usage.cache_read_tokens, "tokens"),
            cache_write_tokens=_reported_metric(usage.cache_creation_tokens, "tokens"),
            cost=_reported_metric(usage.cost_usd, "USD"),
            remaining_quota=remaining_quota or ObservedMetric.unavailable(),
            retry_count=retry_count,
            fallback=fallback,
            executor_kind=executor_kind,
            quality_score=quality_score or ObservedMetric.unavailable(),
            recorded_at=result.ended_at,
        )

    def to_protocol(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "goalID": self.goal_id,
            "taskID": self.task_id,
            "runID": self.run_id,
            "workerID": self.worker_id,
            "provider": self.provider,
            "model": self.model.to_protocol(),
            "nodeID": self.node_id,
            "taskRole": self.task_role.value,
            "outcome": self.outcome.value,
            "duration": self.duration.to_protocol(),
            "inputTokens": self.input_tokens.to_protocol(),
            "outputTokens": self.output_tokens.to_protocol(),
            "cacheReadTokens": self.cache_read_tokens.to_protocol(),
            "cacheWriteTokens": self.cache_write_tokens.to_protocol(),
            "cost": self.cost.to_protocol(),
            "remainingQuota": self.remaining_quota.to_protocol(),
            "retryCount": self.retry_count,
            "fallback": self.fallback,
            "executorKind": self.executor_kind.value,
            "qualityScore": self.quality_score.to_protocol(),
            "recordedAt": self.recorded_at.isoformat().replace("+00:00", "Z"),
        }


@dataclass(frozen=True, slots=True)
class MetricAggregate:
    observed_sum: ObservedMetric
    known_count: int
    unavailable_count: int

    def to_protocol(self) -> dict[str, Any]:
        return {
            "observedSum": self.observed_sum.to_protocol(),
            "knownCount": self.known_count,
            "unavailableCount": self.unavailable_count,
        }


@dataclass(frozen=True, slots=True)
class LatestMetricAggregate:
    latest: ObservedMetric
    known_count: int
    unavailable_count: int
    observed_at: datetime | None

    def to_protocol(self) -> dict[str, Any]:
        return {
            "latest": self.latest.to_protocol(),
            "knownCount": self.known_count,
            "unavailableCount": self.unavailable_count,
            "observedAt": (
                self.observed_at.isoformat().replace("+00:00", "Z")
                if self.observed_at is not None
                else None
            ),
        }


@dataclass(frozen=True, slots=True)
class OffloadAggregate:
    ratio: ObservedMetric
    codex_calls: int
    offloaded_calls: int
    unknown_calls: int

    def to_protocol(self) -> dict[str, Any]:
        return {
            "ratio": self.ratio.to_protocol(),
            "codexCalls": self.codex_calls,
            "offloadedCalls": self.offloaded_calls,
            "unknownCalls": self.unknown_calls,
        }


@dataclass(frozen=True, slots=True)
class QualityAdjustedOffloadAggregate:
    ratio: ObservedMetric
    scored_codex_calls: int
    scored_offloaded_calls: int
    unscored_calls: int

    def to_protocol(self) -> dict[str, Any]:
        return {
            "ratio": self.ratio.to_protocol(),
            "scoredCodexCalls": self.scored_codex_calls,
            "scoredOffloadedCalls": self.scored_offloaded_calls,
            "unscoredCalls": self.unscored_calls,
        }


@dataclass(frozen=True, slots=True)
class TelemetryAggregate:
    goal_id: str | None
    task_id: str | None
    call_count: int
    calls_by_worker: dict[str, int]
    calls_by_provider: dict[str, int]
    calls_by_model: dict[str, int]
    calls_by_node: dict[str, int]
    calls_by_outcome: dict[str, int]
    calls_by_task_role: dict[str, int]
    retry_call_count: int
    retry_attempt_count: int
    fallback_count: int
    duration: MetricAggregate
    input_tokens: MetricAggregate
    output_tokens: MetricAggregate
    cache_read_tokens: MetricAggregate
    cache_write_tokens: MetricAggregate
    cost: MetricAggregate
    remaining_quota: LatestMetricAggregate
    codex_offload: OffloadAggregate
    quality_adjusted_offload: QualityAdjustedOffloadAggregate

    def to_protocol(self) -> dict[str, Any]:
        return {
            "scope": {"goalID": self.goal_id, "taskID": self.task_id},
            "callCount": self.call_count,
            "callsByWorker": self.calls_by_worker,
            "callsByProvider": self.calls_by_provider,
            "callsByModel": self.calls_by_model,
            "callsByNode": self.calls_by_node,
            "callsByOutcome": self.calls_by_outcome,
            "callsByTaskRole": self.calls_by_task_role,
            "retryCallCount": self.retry_call_count,
            "retryAttemptCount": self.retry_attempt_count,
            "fallbackCount": self.fallback_count,
            "duration": self.duration.to_protocol(),
            "inputTokens": self.input_tokens.to_protocol(),
            "outputTokens": self.output_tokens.to_protocol(),
            "cacheReadTokens": self.cache_read_tokens.to_protocol(),
            "cacheWriteTokens": self.cache_write_tokens.to_protocol(),
            "cost": self.cost.to_protocol(),
            "remainingQuota": self.remaining_quota.to_protocol(),
            "codexOffload": self.codex_offload.to_protocol(),
            "qualityAdjustedOffload": self.quality_adjusted_offload.to_protocol(),
        }


class TelemetryStore(Protocol):
    def connect(self) -> Connection: ...

    def transaction(self) -> AbstractContextManager[Connection]: ...


class InvocationTelemetryRepository:
    """Canonical invocation telemetry without coupling StateStore to V0.2 types."""

    _METRIC_NAMES = (
        "duration",
        "input_tokens",
        "output_tokens",
        "cache_read_tokens",
        "cache_write_tokens",
        "cost",
        "remaining_quota",
        "quality_score",
    )

    def __init__(self, store: TelemetryStore) -> None:
        self.store = store

    def goal_context_for_task(self, task_id: str) -> tuple[str | None, TaskRole | None]:
        with self.store.connect() as connection:
            table = connection.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name='autonomous_actions'"
            ).fetchone()
            if table is None:
                return None, None
            row = connection.execute(
                "SELECT goal_id,role FROM autonomous_actions WHERE task_id=? "
                "ORDER BY created_at DESC,goal_id LIMIT 1",
                (task_id,),
            ).fetchone()
            if row is None:
                decision_table = connection.execute(
                    "SELECT 1 FROM sqlite_master WHERE type='table' "
                    "AND name='autonomy_decision_tasks'"
                ).fetchone()
                if decision_table is not None:
                    row = connection.execute(
                        "SELECT goal_id,phase AS role FROM autonomy_decision_tasks "
                        "WHERE task_id=? LIMIT 1",
                        (task_id,),
                    ).fetchone()
        if row is None:
            return None, None
        role_value = {
            "evaluation": TaskRole.EVALUATOR.value,
            "planning": TaskRole.PLANNER.value,
            "verification": TaskRole.VERIFIER.value,
        }.get(str(row["role"]), str(row["role"]))
        try:
            task_role = TaskRole(role_value)
        except ValueError:
            task_role = TaskRole.OTHER
        return str(row["goal_id"]), task_role

    def goal_id_for_task(self, task_id: str) -> str | None:
        return self.goal_context_for_task(task_id)[0]

    def contains_run(self, run_id: str) -> bool:
        with self.store.connect() as connection:
            row = connection.execute(
                "SELECT 1 FROM invocation_telemetry WHERE run_id=?", (run_id,)
            ).fetchone()
        return row is not None

    def record(self, record: InvocationTelemetry) -> None:
        columns = [
            "id",
            "goal_id",
            "task_id",
            "run_id",
            "worker_id",
            "provider",
            "model_value",
            "model_unavailable_reason",
            "node_id",
            "task_role",
            "outcome",
            "retry_count",
            "fallback",
            "executor_kind",
            "recorded_at",
        ]
        values: list[Any] = [
            record.id,
            record.goal_id,
            record.task_id,
            record.run_id,
            record.worker_id,
            record.provider,
            record.model.value,
            record.model.reason.value if record.model.reason else None,
            record.node_id,
            record.task_role.value,
            record.outcome.value,
            record.retry_count,
            int(record.fallback),
            record.executor_kind.value,
            record.recorded_at.isoformat().replace("+00:00", "Z"),
        ]
        for name in self._METRIC_NAMES:
            metric = getattr(record, name)
            columns.extend(
                (f"{name}_value", f"{name}_unit", f"{name}_confidence", f"{name}_reason")
            )
            values.extend(
                (
                    metric.telemetry.value,
                    metric.unit,
                    metric.telemetry.confidence.value,
                    metric.telemetry.reason.value if metric.telemetry.reason else None,
                )
            )
        placeholders = ",".join("?" for _ in values)
        with self.store.transaction() as connection:
            connection.execute(
                f"INSERT INTO invocation_telemetry({','.join(columns)}) VALUES ({placeholders})",
                values,
            )

    def list(
        self,
        *,
        goal_id: str | None = None,
        task_id: str | None = None,
        limit: int = 1000,
    ) -> list[InvocationTelemetry]:
        if limit < 1 or limit > 10_000:
            raise ValueError("limit must be between 1 and 10000")
        clauses: list[str] = []
        values: list[Any] = []
        if goal_id is not None:
            clauses.append("goal_id=?")
            values.append(goal_id)
        if task_id is not None:
            clauses.append("task_id=?")
            values.append(task_id)
        query = "SELECT * FROM invocation_telemetry"
        if clauses:
            query += " WHERE " + " AND ".join(clauses)
        query += " ORDER BY recorded_at,id LIMIT ?"
        values.append(limit)
        with self.store.connect() as connection:
            rows = connection.execute(query, values).fetchall()
        return [self._from_row(dict(row)) for row in rows]

    def aggregate(
        self, *, goal_id: str | None = None, task_id: str | None = None
    ) -> TelemetryAggregate:
        clauses: list[str] = []
        values: list[Any] = []
        if goal_id is not None:
            clauses.append("goal_id=?")
            values.append(goal_id)
        if task_id is not None:
            clauses.append("task_id=?")
            values.append(task_id)
        count_query = "SELECT COUNT(*) AS count FROM invocation_telemetry"
        if clauses:
            count_query += " WHERE " + " AND ".join(clauses)
        with self.store.connect() as connection:
            count = int(connection.execute(count_query, values).fetchone()["count"])
        if count > 10_000:
            raise ValueError("telemetry aggregate exceeds the 10000-record in-memory safety limit")
        query = "SELECT * FROM invocation_telemetry"
        if clauses:
            query += " WHERE " + " AND ".join(clauses)
        query += " ORDER BY recorded_at,id"
        with self.store.connect() as connection:
            rows = connection.execute(query, values).fetchall()
        records = [self._from_row(dict(row)) for row in rows]
        return aggregate_invocations(records, goal_id=goal_id, task_id=task_id)

    @classmethod
    def _from_row(cls, row: dict[str, Any]) -> InvocationTelemetry:
        metrics = {name: cls._metric_from_row(row, name) for name in cls._METRIC_NAMES}
        return InvocationTelemetry(
            id=row["id"],
            goal_id=row["goal_id"],
            task_id=row["task_id"],
            run_id=row["run_id"],
            worker_id=row["worker_id"],
            provider=row["provider"],
            model=ObservedDimension(
                row["model_value"],
                UnavailableReason(row["model_unavailable_reason"])
                if row["model_unavailable_reason"]
                else None,
            ),
            node_id=row["node_id"],
            task_role=TaskRole(row["task_role"]),
            outcome=InvocationOutcome(row["outcome"]),
            retry_count=row["retry_count"],
            fallback=bool(row["fallback"]),
            executor_kind=ExecutorKind(row["executor_kind"]),
            recorded_at=datetime.fromisoformat(row["recorded_at"].replace("Z", "+00:00")),
            **metrics,
        )

    @staticmethod
    def _metric_from_row(row: dict[str, Any], name: str) -> ObservedMetric:
        value = row[f"{name}_value"]
        reason = row[f"{name}_reason"]
        return ObservedMetric(
            TelemetryValue(
                value,
                EvidenceConfidence(row[f"{name}_confidence"]),
                UnavailableReason(reason) if reason else None,
            ),
            row[f"{name}_unit"],
        )


def _counter(records: list[InvocationTelemetry], key: Any) -> dict[str, int]:
    return dict(sorted(Counter(key(record) for record in records).items()))


def _weakest_confidence(metrics: Iterator[ObservedMetric]) -> EvidenceConfidence:
    rank = {
        EvidenceConfidence.EXACT: 0,
        EvidenceConfidence.VERIFIED: 1,
        EvidenceConfidence.PROVIDER_REPORTED: 2,
        EvidenceConfidence.INFERRED: 3,
    }
    values = [metric.telemetry.confidence for metric in metrics]
    return max(values, key=rank.__getitem__)


def _sum_metric(records: list[InvocationTelemetry], attribute: str) -> MetricAggregate:
    metrics = [getattr(record, attribute) for record in records]
    known = [metric for metric in metrics if metric.telemetry.known]
    if not known:
        observed_sum = ObservedMetric.unavailable()
    else:
        units = {metric.unit for metric in known}
        if len(units) != 1:
            raise ValueError(f"cannot aggregate mixed units for {attribute}: {sorted(units)}")
        observed_sum = ObservedMetric.known(
            sum(metric.telemetry.value for metric in known),
            known[0].unit or "",
            _weakest_confidence(iter(known)),
        )
    return MetricAggregate(observed_sum, len(known), len(metrics) - len(known))


def _latest_quota(records: list[InvocationTelemetry]) -> LatestMetricAggregate:
    known = [record for record in records if record.remaining_quota.telemetry.known]
    unavailable_count = len(records) - len(known)
    if not known:
        return LatestMetricAggregate(ObservedMetric.unavailable(), 0, unavailable_count, None)
    latest = max(known, key=lambda record: (record.recorded_at, record.id))
    return LatestMetricAggregate(
        latest.remaining_quota, len(known), unavailable_count, latest.recorded_at
    )


def _offload(records: list[InvocationTelemetry]) -> OffloadAggregate:
    codex = sum(record.executor_kind is ExecutorKind.CODEX for record in records)
    offloaded = sum(record.executor_kind is ExecutorKind.OFFLOADED for record in records)
    unknown = len(records) - codex - offloaded
    denominator = codex + offloaded
    ratio = (
        ObservedMetric.known(offloaded / denominator, "ratio", EvidenceConfidence.EXACT)
        if denominator
        else ObservedMetric.unavailable()
    )
    return OffloadAggregate(ratio, codex, offloaded, unknown)


def _quality_adjusted_offload(
    records: list[InvocationTelemetry],
) -> QualityAdjustedOffloadAggregate:
    scored = [
        record
        for record in records
        if record.executor_kind is not ExecutorKind.UNKNOWN and record.quality_score.telemetry.known
    ]
    codex = [record for record in scored if record.executor_kind is ExecutorKind.CODEX]
    offloaded = [record for record in scored if record.executor_kind is ExecutorKind.OFFLOADED]
    denominator = sum(float(record.quality_score.telemetry.value) for record in scored)
    numerator = sum(float(record.quality_score.telemetry.value) for record in offloaded)
    ratio = (
        ObservedMetric.known(numerator / denominator, "ratio", EvidenceConfidence.INFERRED)
        if denominator > 0
        else ObservedMetric.unavailable()
    )
    return QualityAdjustedOffloadAggregate(
        ratio=ratio,
        scored_codex_calls=len(codex),
        scored_offloaded_calls=len(offloaded),
        unscored_calls=len(records) - len(scored),
    )


def aggregate_invocations(
    records: list[InvocationTelemetry],
    *,
    goal_id: str | None = None,
    task_id: str | None = None,
) -> TelemetryAggregate:
    return TelemetryAggregate(
        goal_id=goal_id,
        task_id=task_id,
        call_count=len(records),
        calls_by_worker=_counter(records, lambda record: record.worker_id),
        calls_by_provider=_counter(records, lambda record: record.provider),
        calls_by_model=_counter(records, lambda record: record.model.aggregation_key),
        calls_by_node=_counter(records, lambda record: record.node_id),
        calls_by_outcome=_counter(records, lambda record: record.outcome.value),
        calls_by_task_role=_counter(records, lambda record: record.task_role.value),
        retry_call_count=sum(record.retry_count > 0 for record in records),
        retry_attempt_count=sum(record.retry_count for record in records),
        fallback_count=sum(record.fallback for record in records),
        duration=_sum_metric(records, "duration"),
        input_tokens=_sum_metric(records, "input_tokens"),
        output_tokens=_sum_metric(records, "output_tokens"),
        cache_read_tokens=_sum_metric(records, "cache_read_tokens"),
        cache_write_tokens=_sum_metric(records, "cache_write_tokens"),
        cost=_sum_metric(records, "cost"),
        remaining_quota=_latest_quota(records),
        codex_offload=_offload(records),
        quality_adjusted_offload=_quality_adjusted_offload(records),
    )
