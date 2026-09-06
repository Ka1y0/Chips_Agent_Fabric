from __future__ import annotations

import math
from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import StrEnum
from typing import Any

from .fabric.capabilities import (
    CapabilityClaim,
    CostMode,
    ObservationFreshness,
    QuotaAvailability,
    SubscriptionState,
    WorkerHealth,
    WorkerLocality,
    WorkerPrivacy,
    validate_manifest_digest,
)


def utc_now() -> datetime:
    return datetime.now(UTC).replace(microsecond=0)


class ProjectPhase(StrEnum):
    INITIALIZING = "initializing"
    PLANNING = "planning"
    DISPATCHING = "dispatching"
    WORKING = "working"
    REVIEWING = "reviewing"
    REPLANNING = "replanning"
    WAITING_FOR_HUMAN = "waitingForHuman"
    PAUSED = "paused"
    BLOCKED = "blocked"
    DONE = "done"
    FAILED = "failed"


class TaskState(StrEnum):
    DRAFT = "draft"
    QUEUED = "queued"
    READY = "ready"
    RUNNING = "running"
    WAITING = "waiting"
    REVIEWING = "reviewing"
    BLOCKED = "blocked"
    INTERRUPTED = "interrupted"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    CANCELLED = "cancelled"


class RunState(StrEnum):
    STARTING = "starting"
    RUNNING = "running"
    WAITING = "waiting"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"
    TIMED_OUT = "timedOut"
    INTERRUPTED = "interrupted"
    AUTH_REQUIRED = "authRequired"
    RATE_LIMITED = "rateLimited"


class WorkerState(StrEnum):
    IDLE = "idle"
    STARTING = "starting"
    RUNNING = "running"
    WAITING = "waiting"
    RATE_LIMITED = "rateLimited"
    STOPPING = "stopping"
    OFFLINE = "offline"
    FAILED = "failed"


class NodeState(StrEnum):
    ONLINE = "online"
    DEGRADED = "degraded"
    OFFLINE = "offline"
    UNPROVISIONED = "unprovisioned"


class ResourceState(StrEnum):
    AVAILABLE = "available"
    WARNING = "warning"
    RATE_LIMITED = "rateLimited"
    BUDGET_EXHAUSTED = "budgetExhausted"
    COOLDOWN = "cooldown"
    UNKNOWN = "unknown"


class EvidenceConfidence(StrEnum):
    EXACT = "exact"
    VERIFIED = "verified"
    PROVIDER_REPORTED = "providerReported"
    INFERRED = "inferred"
    UNKNOWN = "unknown"


class UnavailableReason(StrEnum):
    NOT_REPORTED = "notReported"
    NOT_SUPPORTED = "notSupported"
    PERMISSION_DENIED = "permissionDenied"
    STALE = "stale"
    OFFLINE = "offline"
    UNKNOWN = "unknown"


class ExecutionTopology(StrEnum):
    SINGLE = "single"
    PRIMARY_REVIEWER = "primaryReviewer"
    PARALLEL_PANEL = "parallelPanel"
    CHEAP_FIRST_ESCALATION = "cheapFirstEscalation"
    FALLBACK = "fallback"


class TaskLabel(StrEnum):
    CODING = "coding"
    ARCHITECTURE = "architecture"
    RESEARCH = "research"
    FAST_ROUTING = "fastRouting"
    RAG = "rag"
    REVIEW = "review"
    CREATIVE = "creative"
    LONG_CONTEXT = "longContext"
    PRIVACY_SENSITIVE = "privacySensitive"
    HIGH_UNCERTAINTY = "highUncertainty"


class PermissionClass(StrEnum):
    GREEN = "green"
    YELLOW = "yellow"
    RED = "red"


class ApprovalState(StrEnum):
    NOT_REQUIRED = "notRequired"
    PENDING = "pending"
    APPROVED = "approved"
    REJECTED = "rejected"
    EXPIRED = "expired"


class FailureClass(StrEnum):
    BLOCKED_BY_HUMAN = "blockedByHuman"
    TRANSIENT = "transient"
    IMPLEMENTATION_BUG = "implementationBug"
    VERSION_DIFFERENCE = "versionDifference"
    INFRASTRUCTURE = "infrastructure"
    DESIGN_CHANGE_REQUIRED = "designChangeRequired"
    UNSAFE_TO_CONTINUE = "unsafeToContinue"
    AUTH = "auth"
    RATE_LIMIT = "rateLimit"
    TIMEOUT = "timeout"
    CANCELLED = "cancelled"


class Harness(StrEnum):
    CODEX = "codex"
    CLAUDE_CODE = "claudeCode"
    GROK_BUILD = "grokBuild"
    GOOGLE_AGY = "googleAGY"
    LOCAL_WORKER = "localWorker"
    MOCK = "mock"


class Provider(StrEnum):
    OPENAI = "openai"
    ANTHROPIC = "anthropic"
    XAI = "xai"
    GOOGLE = "google"
    LOCAL = "local"
    MOCK = "mock"


class EventSeverity(StrEnum):
    DEBUG = "debug"
    INFO = "info"
    NOTICE = "notice"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"


@dataclass(frozen=True, slots=True)
class TelemetryValue:
    value: int | float | None
    confidence: EvidenceConfidence
    reason: UnavailableReason | None = None

    def __post_init__(self) -> None:
        if self.value is None and self.reason is None:
            raise ValueError("unavailable telemetry requires a reason")
        if self.value is not None and self.reason is not None:
            raise ValueError("known telemetry cannot have an unavailable reason")

    @property
    def known(self) -> bool:
        return self.value is not None

    def to_api(self) -> dict[str, Any]:
        if self.known:
            return {
                "state": "known",
                "value": self.value,
                "confidence": self.confidence.value,
            }
        return {
            "state": "unavailable",
            "reason": (self.reason or UnavailableReason.UNKNOWN).value,
            "confidence": self.confidence.value,
        }


@dataclass(frozen=True, slots=True)
class ModelDescriptor:
    identifier: str
    display_name: str
    provider: Provider
    context_variant: str | None = None
    context_window_tokens: int | None = None


@dataclass(frozen=True, slots=True)
class TaskRequirements:
    labels: frozenset[TaskLabel]
    required_capabilities: frozenset[str] = frozenset()
    permission_class: PermissionClass = PermissionClass.GREEN
    approval_state: ApprovalState = ApprovalState.NOT_REQUIRED
    minimum_context_tokens: int | None = None
    privacy_sensitive: bool = False
    code_write_required: bool = False
    panel_size: int = 2
    preferred_workers: tuple[str, ...] = ()
    preferred_capabilities: frozenset[str] = frozenset()
    required_capability_parameters: tuple[CapabilityClaim, ...] = ()
    local_only: bool = False
    minimum_quality_score: float | None = None
    max_incremental_cost_usd: float | None = None
    explicit_worker_override: str | None = None
    required_manifest_schema_version: str | None = None
    required_capability_catalog_version: str | None = None

    def __post_init__(self) -> None:
        if self.panel_size < 1:
            raise ValueError("panel_size must be positive")
        if self.minimum_quality_score is not None and not (
            math.isfinite(self.minimum_quality_score) and 0 <= self.minimum_quality_score <= 1
        ):
            raise ValueError("minimum_quality_score must be between zero and one")
        if self.max_incremental_cost_usd is not None and (
            not math.isfinite(self.max_incremental_cost_usd) or self.max_incremental_cost_usd < 0
        ):
            raise ValueError("max_incremental_cost_usd must be finite and non-negative")
        if self.explicit_worker_override is not None and (
            not self.explicit_worker_override or len(self.explicit_worker_override) > 200
        ):
            raise ValueError("explicit_worker_override must be bounded and non-empty")
        for value in (
            self.required_manifest_schema_version,
            self.required_capability_catalog_version,
        ):
            if value is not None and (not value or len(value) > 128):
                raise ValueError("required manifest versions must be bounded and non-empty")
        if any(
            not isinstance(claim, CapabilityClaim) for claim in self.required_capability_parameters
        ):
            raise ValueError("required_capability_parameters must contain CapabilityClaim values")


@dataclass(frozen=True, slots=True)
class WorkerSnapshot:
    id: str
    node_id: str
    harness: Harness
    provider: Provider
    model: ModelDescriptor
    state: WorkerState
    node_state: NodeState
    resource_state: ResourceState
    capabilities: frozenset[str]
    code_write_allowed: bool
    privacy_allowed: bool
    quality_score: float = 0.5
    reliability_score: float = 0.5
    expected_latency_seconds: float = 30.0
    monetary_cost_score: float = 0.5
    node_load: float = 0.0
    running_tasks: int = 0
    manifest_schema_version: str | None = None
    capability_catalog_version: str | None = None
    manifest_revision: int | None = None
    manifest_digest: str | None = None
    manifest_valid_until: datetime | None = None
    cost_mode: CostMode = CostMode.UNKNOWN
    subscription_state: SubscriptionState = SubscriptionState.UNKNOWN
    incremental_cost_usd: float | None = None
    quota_state: QuotaAvailability = QuotaAvailability.UNKNOWN
    quota_freshness: ObservationFreshness = ObservationFreshness.UNKNOWN
    locality: WorkerLocality = WorkerLocality.UNKNOWN
    privacy: WorkerPrivacy = WorkerPrivacy.UNKNOWN
    health: WorkerHealth = WorkerHealth.UNKNOWN
    health_freshness: ObservationFreshness = ObservationFreshness.UNKNOWN
    worker_load: float | None = None
    max_concurrency: int = 1
    capability_claims: tuple[CapabilityClaim, ...] = ()
    execution_observation_id: str | None = None
    execution_schema_version: str | None = None
    execution_disposition: str | None = None
    execution_rejection_code: str | None = None
    execution_reason_codes: tuple[str, ...] = ()
    worker_classes: frozenset[str] = frozenset()

    def __post_init__(self) -> None:
        for label, value in (
            ("quality_score", self.quality_score),
            ("reliability_score", self.reliability_score),
            ("monetary_cost_score", self.monetary_cost_score),
            ("node_load", self.node_load),
        ):
            if not math.isfinite(value) or not 0 <= value <= 1:
                raise ValueError(f"{label} must be between zero and one")
        if self.expected_latency_seconds < 0 or not math.isfinite(self.expected_latency_seconds):
            raise ValueError("expected_latency_seconds must be finite and non-negative")
        if self.running_tasks < 0:
            raise ValueError("running_tasks cannot be negative")
        if self.max_concurrency < 1:
            raise ValueError("max_concurrency must be positive")
        if self.worker_load is not None and (
            not math.isfinite(self.worker_load) or not 0 <= self.worker_load <= 1
        ):
            raise ValueError("worker_load must be between zero and one when known")
        if self.incremental_cost_usd is not None and (
            not math.isfinite(self.incremental_cost_usd) or self.incremental_cost_usd < 0
        ):
            raise ValueError("incremental_cost_usd must be finite and non-negative")
        manifest_values = (
            self.manifest_schema_version,
            self.capability_catalog_version,
            self.manifest_revision,
            self.manifest_digest,
        )
        if any(value is not None for value in manifest_values):
            if any(value is None for value in manifest_values):
                raise ValueError("versioned Worker snapshots require complete manifest identity")
            if self.manifest_revision is None or self.manifest_revision < 1:
                raise ValueError("manifest_revision must be positive")
            assert self.manifest_digest is not None
            validate_manifest_digest(self.manifest_digest)
        if self.manifest_valid_until is not None:
            if self.manifest_schema_version is None:
                raise ValueError("manifest_valid_until requires versioned manifest identity")
            if (
                self.manifest_valid_until.tzinfo is None
                or self.manifest_valid_until.utcoffset() is None
            ):
                raise ValueError("manifest_valid_until must be timezone-aware")
        if any(not isinstance(claim, CapabilityClaim) for claim in self.capability_claims):
            raise ValueError("capability_claims must contain CapabilityClaim values")
        execution_values = (
            self.execution_observation_id,
            self.execution_schema_version,
            self.execution_disposition,
        )
        if any(value is not None for value in execution_values) and any(
            value is None for value in execution_values
        ):
            raise ValueError("versioned execution facets require complete observation identity")
        if self.execution_disposition not in {
            None,
            "executable",
            "notExecutable",
            "unknown",
            "stale",
        }:
            raise ValueError("execution_disposition is invalid")
        if len(self.execution_reason_codes) > 32:
            raise ValueError("execution reason codes exceed their limit")
        if len(self.worker_classes) > 64 or any(
            not value or len(value) > 160 for value in self.worker_classes
        ):
            raise ValueError("worker_classes must be bounded semantic identities")


@dataclass(frozen=True, slots=True)
class Rejection:
    worker_id: str
    reason_code: str
    detail: str


@dataclass(frozen=True, slots=True)
class CandidateScore:
    worker_id: str
    score: float
    components: dict[str, float]


@dataclass(frozen=True, slots=True)
class RoutingDecision:
    topology: ExecutionTopology
    selected_worker_ids: tuple[str, ...]
    candidates: tuple[CandidateScore, ...]
    rejected: tuple[Rejection, ...]
    policy_version: str
    explanation: dict[str, Any]


@dataclass(slots=True)
class TaskRecord:
    id: str
    project_id: str
    title: str
    description: str
    state: TaskState
    topology: ExecutionTopology
    requirements: TaskRequirements
    execution_spec: dict[str, Any] = field(default_factory=dict)
    priority: int = 50
    attempt_count: int = 0
    created_at: datetime = field(default_factory=utc_now)
    updated_at: datetime = field(default_factory=utc_now)
