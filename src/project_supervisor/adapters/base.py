from __future__ import annotations

import inspect
from abc import ABC, abstractmethod
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import StrEnum
from pathlib import Path
from typing import Any, Protocol, runtime_checkable

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


class WorkerJobState(StrEnum):
    """Provider-observed state of a durable external Worker job.

    ``PROVIDER_NOT_FOUND`` is deliberately distinct from an unreachable provider: only an
    authoritative not-found response proves that the queried job is absent.
    """

    KNOWN_RUNNING = "knownRunning"
    KNOWN_COMPLETED = "knownCompleted"
    KNOWN_FAILED = "knownFailed"
    KNOWN_CANCELLED = "knownCancelled"
    PROVIDER_NOT_FOUND = "providerNotFound"
    PROVIDER_UNREACHABLE = "providerUnreachable"
    UNKNOWN = "unknown"


class WorkerJobLaunchState(StrEnum):
    """Authoritative state of a provider-side idempotent launch record.

    This is deliberately separate from :class:`WorkerJobState`.  In particular, ``NOT_SEEN``
    means a healthy durable launch registry has confirmed that it never accepted the key; it is
    not interchangeable with an unreachable registry or a missing already-bound provider job.
    """

    NOT_SEEN = "notSeen"
    REJECTED_PRE_LAUNCH = "rejectedPreLaunch"
    RESERVED = "reserved"
    LAUNCHING = "launching"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"
    UNKNOWN = "unknown"


class WorkerJobLaunchDisposition(StrEnum):
    """Whether a launch lookup makes crossing the external boundary safe."""

    DEFINITELY_NOT_LAUNCHED = "definitelyNotLaunched"
    DEFINITELY_LAUNCHED = "definitelyLaunched"
    LAUNCH_IN_PROGRESS = "launchInProgress"
    LAUNCH_OUTCOME_UNKNOWN = "launchOutcomeUnknown"


class WorkerJobLaunchRejected(AdapterError):
    """The adapter can prove that a launch request created no external job.

    Generic transport and HTTP exceptions are deliberately insufficient evidence: an adapter
    may have crossed its provider's side-effect boundary before receiving either one. Adapters
    should raise this type only when their documented protocol makes rejection authoritative.
    """

    def __init__(
        self,
        status_code: int,
        detail: str | None = None,
        *,
        receipt_id: str | None = None,
        reason_code: str | None = None,
        request_digest: str | None = None,
    ) -> None:
        if status_code < 100 or status_code > 599:
            raise ValueError("launch rejection status_code must be an HTTP status")
        self.status_code = status_code
        self.receipt_id = receipt_id
        self.reason_code = reason_code
        self.request_digest = request_digest
        super().__init__(detail or f"provider rejected launch with HTTP {status_code}")


class WorkerJobIdempotencyConflict(AdapterError):
    """One durable launch key was presented with a different immutable request digest.

    This is not a pre-launch rejection: the original payload may already have a live job.  The
    Supervisor must fail closed and must never turn this exception into permission to retry.
    """


class WorkerJobOutcomeUncertain(AdapterError):
    """A bound external job may still be live and must be reconciled before retry."""

    def __init__(self, state: WorkerJobState, detail: str) -> None:
        if state not in {
            WorkerJobState.KNOWN_RUNNING,
            WorkerJobState.PROVIDER_UNREACHABLE,
            WorkerJobState.UNKNOWN,
        }:
            raise ValueError("uncertain Worker job outcome requires a non-terminal state")
        self.state = state
        super().__init__(detail)


@dataclass(frozen=True, slots=True)
class WorkerJobCapabilities:
    """Truthful restart capabilities advertised by a Worker adapter."""

    supports_reconcile: bool = False
    supports_resume: bool = False
    supports_cancel: bool = False
    supports_provider_idempotency: bool = False
    supports_stream_reconnect: bool = False
    supports_repeatable_collect: bool = False
    supports_idempotent_launch_lookup: bool = False
    supports_durable_launch_registry: bool = False

    def to_mapping(self) -> dict[str, bool]:
        return {
            "supportsReconcile": self.supports_reconcile,
            "supportsResume": self.supports_resume,
            "supportsCancel": self.supports_cancel,
            "supportsProviderIdempotency": self.supports_provider_idempotency,
            "supportsStreamReconnect": self.supports_stream_reconnect,
            "supportsRepeatableCollect": self.supports_repeatable_collect,
            "supportsIdempotentLaunchLookup": self.supports_idempotent_launch_lookup,
            "supportsDurableLaunchRegistry": self.supports_durable_launch_registry,
        }


@dataclass(frozen=True, slots=True)
class WorkerJobHandle:
    """Serializable, non-secret identity for one external Worker job."""

    run_id: str
    adapter_type: str
    adapter_instance_id: str
    provider_job_id: str
    created_at: datetime
    provider_session_id: str | None = None
    runtime_pid: int | None = None
    runtime_host: str | None = None
    process_identity: str | None = None
    schema_version: int = 1
    metadata: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        for name, value in {
            "run_id": self.run_id,
            "adapter_type": self.adapter_type,
            "adapter_instance_id": self.adapter_instance_id,
            "provider_job_id": self.provider_job_id,
        }.items():
            if not value.strip():
                raise ValueError(f"{name} must not be empty")
        if self.created_at.tzinfo is None:
            raise ValueError("Worker job handle created_at must be timezone-aware")
        if self.runtime_pid is not None and self.runtime_pid <= 0:
            raise ValueError("Worker job handle runtime_pid must be positive when known")
        if self.schema_version < 1:
            raise ValueError("Worker job handle schema_version must be positive")


@dataclass(frozen=True, slots=True)
class WorkerJobObservation:
    """One non-mutating reconciliation observation of an external job."""

    state: WorkerJobState
    observed_at: datetime
    detail: str | None = None
    metadata: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if self.observed_at.tzinfo is None:
            raise ValueError("Worker job observation observed_at must be timezone-aware")


@dataclass(frozen=True, slots=True)
class WorkerJobLaunchObservation:
    """One side-effect-free lookup of a durable provider launch identity."""

    state: WorkerJobLaunchState
    disposition: WorkerJobLaunchDisposition
    observed_at: datetime
    handle: WorkerJobHandle | None = None
    receipt_id: str | None = None
    request_digest: str | None = None
    detail: str | None = None
    metadata: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if self.observed_at.tzinfo is None:
            raise ValueError("Worker launch observation observed_at must be timezone-aware")
        if (
            self.state
            in {
                WorkerJobLaunchState.RUNNING,
                WorkerJobLaunchState.COMPLETED,
                WorkerJobLaunchState.FAILED,
                WorkerJobLaunchState.CANCELLED,
            }
            and self.handle is None
        ):
            raise ValueError("definitively launched Worker observation requires a durable handle")
        if (
            self.state is WorkerJobLaunchState.NOT_SEEN
            and self.disposition is not WorkerJobLaunchDisposition.DEFINITELY_NOT_LAUNCHED
        ):
            raise ValueError("notSeen launch observation must be definitelyNotLaunched")
        if (
            self.state is WorkerJobLaunchState.REJECTED_PRE_LAUNCH
            and self.disposition is not WorkerJobLaunchDisposition.DEFINITELY_NOT_LAUNCHED
        ):
            raise ValueError("pre-launch rejection must be definitelyNotLaunched")


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


@runtime_checkable
class DurableWorkerAdapter(Protocol):
    """Optional restart-safe external-job contract.

    Existing adapters remain valid ``WorkerAdapter`` implementations without this protocol.
    Orchestration code must inspect ``job_capabilities`` and must not infer support merely from a
    provider or runtime name.
    """

    @property
    def job_capabilities(self) -> WorkerJobCapabilities: ...

    @property
    def adapter_type(self) -> str: ...

    @property
    def adapter_instance_id(self) -> str: ...

    async def start_job(
        self,
        request: WorkerRequest,
        *,
        idempotency_key: str,
        event_sink: EventSink | None = None,
    ) -> WorkerJobHandle: ...

    async def reconcile_job(self, handle: WorkerJobHandle) -> WorkerJobObservation: ...

    async def resume_job(
        self,
        request: WorkerRequest,
        handle: WorkerJobHandle,
        *,
        event_sink: EventSink | None = None,
    ) -> WorkerResult: ...

    async def collect_job(
        self,
        request: WorkerRequest,
        handle: WorkerJobHandle,
        *,
        event_sink: EventSink | None = None,
    ) -> WorkerResult: ...

    async def cancel_job(self, handle: WorkerJobHandle) -> bool: ...


@runtime_checkable
class NegotiatingWorkerAdapter(Protocol):
    """Adapter whose durable contract must be observed before dispatch is frozen.

    Runtimes should await this method before persisting ``adapter_type``, instance identity, or
    capability flags, and again before comparing a reconstructed adapter with a persisted job.
    """

    async def negotiate_job_contract(self) -> WorkerJobCapabilities: ...


@runtime_checkable
class IdempotentLaunchWorkerAdapter(Protocol):
    """Optional side-effect-free lookup for provider-enforced launch identities."""

    async def lookup_launch(
        self,
        request: WorkerRequest,
        *,
        idempotency_key: str,
    ) -> WorkerJobLaunchObservation: ...
