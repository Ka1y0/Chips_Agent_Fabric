from __future__ import annotations

from datetime import UTC, datetime
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field


def api_timestamp(value: datetime | None = None) -> str:
    """Return the API contract's whole-second UTC timestamp."""

    return (value or datetime.now(UTC)).replace(microsecond=0).isoformat().replace("+00:00", "Z")


class APIModel(BaseModel):
    model_config = ConfigDict(extra="forbid", populate_by_name=True)


class PageMeta(APIModel):
    next_cursor: str | None = Field(default=None, alias="nextCursor")
    has_more: bool = Field(default=False, alias="hasMore")
    highest_sequence: int = Field(alias="highestSequence")
    total_count: int | None = Field(default=None, alias="totalCount")


class APIEnvelope[PayloadT](APIModel):
    api_version: Literal["v1"] = Field(default="v1", alias="apiVersion")
    generated_at: str = Field(default_factory=api_timestamp, alias="generatedAt")
    data: PayloadT
    meta: PageMeta | None = None


class APIError(APIModel):
    code: str
    message: str
    retry_after_seconds: int | None = Field(default=None, alias="retryAfterSeconds")


class Telemetry(APIModel):
    state: Literal["known", "unavailable"]
    value: int | float | None = None
    reason: (
        Literal[
            "notReported",
            "notSupported",
            "permissionDenied",
            "stale",
            "offline",
            "unknown",
        ]
        | None
    ) = None


class ModelDescriptor(APIModel):
    identifier: str
    display_name: str = Field(alias="displayName")
    provider: Literal["anthropic", "xai", "google", "openai", "local"]
    context_variant: str | None = Field(default=None, alias="contextVariant")
    context_window_tokens: int | None = Field(default=None, alias="contextWindowTokens")


class GoalBudgetRequest(APIModel):
    """Provider-neutral, operator-supplied guardrails for an autonomous Goal."""

    max_iterations: int = Field(default=12, alias="maxIterations", gt=0)
    max_tasks: int = Field(default=48, alias="maxTasks", gt=0)
    max_failures: int = Field(default=6, alias="maxFailures", gt=0)
    no_progress_limit: int = Field(default=3, alias="noProgressLimit", gt=0)
    max_elapsed_seconds: float | None = Field(default=None, alias="maxElapsedSeconds", gt=0)
    max_total_tokens: int | None = Field(default=None, alias="maxTotalTokens", gt=0)
    max_cost_usd: float | None = Field(default=None, alias="maxCostUSD", gt=0)


class GoalCreateRequest(APIModel):
    project_id: str = Field(alias="projectID", min_length=1, max_length=200)
    intent: str = Field(min_length=1, max_length=100_000)
    goal_id: str | None = Field(default=None, alias="goalID", min_length=1, max_length=200)
    budgets: GoalBudgetRequest | None = None


class GoalPauseRequest(APIModel):
    mode: Literal["soft", "hard"]
    reason: str | None = Field(default=None, min_length=1, max_length=10_000)


class GoalResumeRequest(APIModel):
    reason: str | None = Field(default=None, min_length=1, max_length=10_000)


class GoalSteerRequest(APIModel):
    instruction: str = Field(min_length=1, max_length=100_000)
    priority: int | None = Field(default=None, ge=0, le=100)
    preserve_valid_work: bool = Field(default=True, alias="preserveValidWork")


class GoalStopRequest(APIModel):
    reason: str = Field(min_length=1, max_length=10_000)


class SnapshotFrame(APIModel):
    type: Literal["snapshot"] = "snapshot"
    snapshot: dict[str, Any]


class EventsFrame(APIModel):
    type: Literal["events"] = "events"
    events: list[dict[str, Any]]


class KeepaliveFrame(APIModel):
    type: Literal["keepalive"] = "keepalive"
    sent_at: str = Field(default_factory=api_timestamp, alias="sentAt")
    highest_sequence: int = Field(alias="highestSequence")


def envelope[PayloadT](data: PayloadT, *, meta: PageMeta | None = None) -> dict[str, Any]:
    return APIEnvelope[PayloadT](data=data, meta=meta).model_dump(by_alias=True, exclude_none=True)
