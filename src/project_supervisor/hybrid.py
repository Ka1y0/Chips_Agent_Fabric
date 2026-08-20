from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from .domain import ExecutionTopology, TaskRequirements, WorkerSnapshot, utc_now


@dataclass(frozen=True, slots=True)
class ResourceRoutingEvidence:
    """Frozen provider-neutral quota evidence used by one routing decision."""

    worker_id: str
    quota_pool_id: str
    provider: str
    quota_state: str
    freshness: str
    provenance: str
    source: str
    confidence: str
    health_score: float
    observed_at: datetime | None = None
    reason: str | None = None

    def __post_init__(self) -> None:
        required = (self.worker_id, self.quota_pool_id, self.provider, self.source, self.confidence)
        if not all(value.strip() for value in required):
            raise ValueError("resource routing evidence identifiers must not be empty")
        if self.quota_state not in {"available", "warning", "critical", "exhausted", "unknown"}:
            raise ValueError("unsupported quota state")
        if self.freshness not in {"fresh", "stale", "unknown"}:
            raise ValueError("unsupported resource evidence freshness")
        if self.provenance not in {
            "PROVIDER_REPORTED",
            "LOCALLY_MEASURED",
            "INFERRED",
            "UNKNOWN",
        }:
            raise ValueError("unsupported resource evidence provenance")
        if not 0 <= self.health_score <= 1:
            raise ValueError("resource health_score must be between zero and one")
        if self.observed_at is not None and self.observed_at.tzinfo is None:
            raise ValueError("resource observed_at must be timezone-aware")
        if self.quota_state == "unknown" and not self.reason:
            raise ValueError("unknown resource evidence requires an explicit reason")

    def to_protocol(self) -> dict[str, Any]:
        return {
            "workerID": self.worker_id,
            "quotaPoolID": self.quota_pool_id,
            "provider": self.provider,
            "quotaState": self.quota_state,
            "freshness": self.freshness,
            "provenance": self.provenance,
            "source": self.source,
            "confidence": self.confidence,
            "healthScore": self.health_score,
            "observedAt": (
                self.observed_at.isoformat().replace("+00:00", "Z")
                if self.observed_at is not None
                else None
            ),
            "reason": self.reason,
        }


@dataclass(frozen=True, slots=True)
class RoutingInputSnapshot:
    """Frozen, attributable input to one deterministic routing decision."""

    snapshot_id: str
    task_id: str
    requirements: TaskRequirements
    topology: ExecutionTopology
    workers: tuple[WorkerSnapshot, ...]
    observed_at: datetime = field(default_factory=utc_now)
    facts: dict[str, str | int | float | bool] = field(default_factory=dict)
    resource_evidence: tuple[ResourceRoutingEvidence, ...] = ()

    def __post_init__(self) -> None:
        if not self.snapshot_id.strip() or not self.task_id.strip():
            raise ValueError("routing snapshot_id and task_id must not be empty")
        worker_ids = [worker.id for worker in self.workers]
        if len(worker_ids) != len(set(worker_ids)):
            raise ValueError("routing snapshot workers must be unique")
        evidence_ids = [item.worker_id for item in self.resource_evidence]
        if len(evidence_ids) != len(set(evidence_ids)):
            raise ValueError("routing resource evidence must be unique by Worker")
        if not set(evidence_ids) <= set(worker_ids):
            raise ValueError("routing resource evidence references an unknown Worker")

    def explanation(self) -> dict[str, Any]:
        return {
            "snapshotID": self.snapshot_id,
            "observedAt": self.observed_at.isoformat().replace("+00:00", "Z"),
            "workerIDs": sorted(worker.id for worker in self.workers),
            "facts": dict(sorted(self.facts.items())),
            "resourceEvidence": [
                item.to_protocol()
                for item in sorted(self.resource_evidence, key=lambda value: value.worker_id)
            ],
            "adaptiveLearning": False,
        }


@dataclass(frozen=True, slots=True)
class ExecutionHistoryRecord:
    """Normalized outcome telemetry for future, explicitly non-adaptive policy analysis."""

    id: str
    task_id: str
    task_type: str
    worker_id: str
    provider: str
    model: str
    node_id: str
    topology: ExecutionTopology
    latency_seconds: float
    succeeded: bool
    retry_count: int = 0
    failure_class: str | None = None
    input_tokens: int | None = None
    output_tokens: int | None = None
    cost_usd: float | None = None
    review_outcome: str | None = None
    human_accepted: bool | None = None
    recorded_at: datetime = field(default_factory=utc_now)

    def __post_init__(self) -> None:
        required = (
            self.id,
            self.task_id,
            self.task_type,
            self.worker_id,
            self.provider,
            self.model,
            self.node_id,
        )
        if not all(value.strip() for value in required):
            raise ValueError("execution history identifiers must not be empty")
        if self.latency_seconds < 0 or self.retry_count < 0:
            raise ValueError("latency_seconds and retry_count must not be negative")
        token_values = (self.input_tokens, self.output_tokens)
        if any(value is not None and value < 0 for value in token_values):
            raise ValueError("token counts must not be negative")
        if self.cost_usd is not None and self.cost_usd < 0:
            raise ValueError("cost_usd must not be negative")
        if self.succeeded and self.failure_class is not None:
            raise ValueError("successful history cannot carry a failure_class")

    def to_protocol(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "taskID": self.task_id,
            "taskType": self.task_type,
            "workerID": self.worker_id,
            "provider": self.provider,
            "model": self.model,
            "nodeID": self.node_id,
            "topology": self.topology.value,
            "latencySeconds": self.latency_seconds,
            "succeeded": self.succeeded,
            "failureClass": self.failure_class,
            "retryCount": self.retry_count,
            "inputTokens": self.input_tokens,
            "outputTokens": self.output_tokens,
            "costUSD": self.cost_usd,
            "reviewOutcome": self.review_outcome,
            "humanAccepted": self.human_accepted,
            "recordedAt": self.recorded_at.isoformat().replace("+00:00", "Z"),
        }
