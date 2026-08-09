from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import StrEnum
from typing import Any


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
    priority: int = 50
    attempt_count: int = 0
    created_at: datetime = field(default_factory=utc_now)
    updated_at: datetime = field(default_factory=utc_now)
