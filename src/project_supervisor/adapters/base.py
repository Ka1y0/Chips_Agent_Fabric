from __future__ import annotations

import inspect
from abc import ABC, abstractmethod
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from project_supervisor.domain import RunState


def event_time() -> datetime:
    return datetime.now(UTC)


class AdapterError(RuntimeError):
    """Base class for errors raised at a worker adapter boundary."""


class UnsafeWorkerRequest(AdapterError):
    """The requested operation violates a worker's immutable safety policy."""


class WorkerUnavailable(AdapterError):
    """The worker executable or remote endpoint is unavailable."""


class WorkerProtocolError(AdapterError):
    """The worker returned a response that does not satisfy its protocol."""


@dataclass(frozen=True, slots=True)
class WorkerRequest:
    run_id: str
    prompt: str
    task_id: str | None = None
    working_directory: Path | None = None
    timeout_seconds: float = 120.0
    code_write_required: bool = False
    session_id: str | None = None
    model: str | None = None
    metadata: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not self.run_id.strip():
            raise ValueError("run_id must not be empty")
        if not self.prompt:
            raise ValueError("prompt must not be empty")
        if self.timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be positive")


@dataclass(frozen=True, slots=True)
class WorkerEvent:
    run_id: str
    kind: str
    timestamp: datetime
    payload: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class Usage:
    input_tokens: int | None = None
    output_tokens: int | None = None
    cache_creation_tokens: int | None = None
    cache_read_tokens: int | None = None
    reasoning_tokens: int | None = None
    total_tokens: int | None = None
    cost_usd: float | None = None
    raw: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class WorkerResult:
    run_id: str
    state: RunState
    pid: int | None
    exit_code: int | None
    started_at: datetime
    ended_at: datetime
    stdout: str
    stderr: str
    final_text: str
    events: tuple[WorkerEvent, ...]
    session_id: str | None = None
    model: str | None = None
    context_variant: str | None = None
    usage: Usage = field(default_factory=Usage)
    error: str | None = None

    @property
    def duration_seconds(self) -> float:
        return max(0.0, (self.ended_at - self.started_at).total_seconds())

    @property
    def succeeded(self) -> bool:
        return self.state is RunState.COMPLETED and self.exit_code == 0


type EventSink = Callable[[WorkerEvent], None | Awaitable[None]]


async def publish_event(sink: EventSink | None, event: WorkerEvent) -> None:
    if sink is None:
        return
    result = sink(event)
    if inspect.isawaitable(result):
        await result


def request_requires_code_write(request: WorkerRequest) -> bool:
    """Recognize code-write intent even when supplied through protocol metadata."""

    if request.code_write_required:
        return True
    metadata = request.metadata
    if metadata.get("code_write") is True or metadata.get("codeWriteRequired") is True:
        return True
    requested = metadata.get("requested_capabilities", ())
    if isinstance(requested, str):
        requested = (requested,)
    return bool(
        {"code_write", "file_write", "source_edit", "patch"}
        & {str(item).lower() for item in requested}
    )


class WorkerAdapter(ABC):
    """Model-independent asynchronous worker boundary."""

    @abstractmethod
    async def execute(
        self,
        request: WorkerRequest,
        *,
        event_sink: EventSink | None = None,
    ) -> WorkerResult:
        """Run one request and return the terminal normalized result."""

    @abstractmethod
    async def cancel(self, run_id: str) -> bool:
        """Request cancellation. Return false when no active run exists."""
