from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from .domain import ExecutionTopology, TaskRequirements, WorkerSnapshot, utc_now


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

    def __post_init__(self) -> None:
        if not self.snapshot_id.strip() or not self.task_id.strip():
            raise ValueError("routing snapshot_id and task_id must not be empty")
        worker_ids = [worker.id for worker in self.workers]
        if len(worker_ids) != len(set(worker_ids)):
            raise ValueError("routing snapshot workers must be unique")

    def explanation(self) -> dict[str, Any]:
        return {
            "snapshotID": self.snapshot_id,
            "observedAt": self.observed_at.isoformat().replace("+00:00", "Z"),
            "workerIDs": sorted(worker.id for worker in self.workers),
            "facts": dict(sorted(self.facts.items())),
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
