from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Awaitable, Callable, Mapping
from contextlib import suppress
from dataclasses import dataclass, field
from datetime import datetime
from enum import StrEnum
from typing import Any, Protocol, runtime_checkable

from project_supervisor.adapters.base import (
    EventSink,
    WorkerAdapter,
    WorkerEvent,
    WorkerRequest,
    WorkerResult,
)
from project_supervisor.domain import (
    EvidenceConfidence,
    TelemetryValue,
    UnavailableReason,
    utc_now,
)


def unavailable_telemetry() -> TelemetryValue:
    return TelemetryValue(
        None,
        EvidenceConfidence.UNKNOWN,
        UnavailableReason.NOT_REPORTED,
    )


class WorkerAvailability(StrEnum):
    AVAILABLE = "available"
    BUSY = "busy"
    DEGRADED = "degraded"
    OFFLINE = "offline"
    UNKNOWN = "unknown"


@dataclass(frozen=True, slots=True)
class WorkerModel:
    """A model advertised by a Worker, independent of provider naming conventions."""

    id: str
    display_name: str
    context_limit_tokens: int | None = None
    capabilities: frozenset[str] = frozenset()

    def __post_init__(self) -> None:
        if not self.id.strip() or not self.display_name.strip():
            raise ValueError("worker model id and display_name must not be empty")
        if self.context_limit_tokens is not None and self.context_limit_tokens <= 0:
            raise ValueError("context_limit_tokens must be positive when known")

    def to_protocol(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "displayName": self.display_name,
            "contextLimitTokens": self.context_limit_tokens,
            "capabilities": sorted(self.capabilities),
        }


@dataclass(frozen=True, slots=True)
class WorkerPermissions:
    allowed: frozenset[str]
    denied: frozenset[str] = frozenset()

    def __post_init__(self) -> None:
        overlap = self.allowed & self.denied
        if overlap:
            raise ValueError(f"permissions cannot be both allowed and denied: {sorted(overlap)}")

    def to_protocol(self) -> dict[str, Any]:
        return {"allowed": sorted(self.allowed), "denied": sorted(self.denied)}


@dataclass(frozen=True, slots=True)
class WorkerContract:
    """Portable discovery document for one capability-bearing Worker endpoint."""

    worker_id: str
    node_id: str
    provider: str
    implementation: str
    models: tuple[WorkerModel, ...]
    capabilities: frozenset[str]
    permissions: WorkerPermissions
    default_model_id: str | None = None
    protocol_version: str = "1.0"
    supports_incremental_stream: bool = True
    supports_cancel: bool = True
    supports_sessions: bool = False
    cost_visibility: str = "unavailable"
    quota_visibility: str = "unavailable"

    def __post_init__(self) -> None:
        for name, value in {
            "worker_id": self.worker_id,
            "node_id": self.node_id,
            "provider": self.provider,
            "implementation": self.implementation,
            "protocol_version": self.protocol_version,
        }.items():
            if not value.strip():
                raise ValueError(f"{name} must not be empty")
        model_ids = [model.id for model in self.models]
        if len(model_ids) != len(set(model_ids)):
            raise ValueError("worker model ids must be unique")
        if self.default_model_id is not None and self.default_model_id not in model_ids:
            raise ValueError("default_model_id must identify an advertised model")
        visibility = {"exact", "estimated", "unavailable"}
        if self.cost_visibility not in visibility or self.quota_visibility not in visibility:
            raise ValueError("visibility must be exact, estimated, or unavailable")

    def to_protocol(self) -> dict[str, Any]:
        return {
            "protocolVersion": self.protocol_version,
            "identity": {
                "workerID": self.worker_id,
                "nodeID": self.node_id,
                "provider": self.provider,
                "implementation": self.implementation,
            },
            "models": [model.to_protocol() for model in self.models],
            "defaultModelID": self.default_model_id,
            "capabilities": sorted(self.capabilities),
            "permissions": self.permissions.to_protocol(),
            "operations": {
                "execute": True,
                "stream": self.supports_incremental_stream,
                "cancel": self.supports_cancel,
                "sessions": self.supports_sessions,
            },
            "observability": {
                "cost": self.cost_visibility,
                "quota": self.quota_visibility,
            },
        }


@dataclass(frozen=True, slots=True)
class WorkerHealth:
    availability: WorkerAvailability
    checked_at: datetime = field(default_factory=utc_now)
    latency_ms: TelemetryValue = field(default_factory=unavailable_telemetry)
    detail: str | None = None

    def to_protocol(self) -> dict[str, Any]:
        return {
            "availability": self.availability.value,
            "checkedAt": self.checked_at.isoformat().replace("+00:00", "Z"),
            "latencyMs": self.latency_ms.to_api(),
            "detail": self.detail,
        }


@dataclass(frozen=True, slots=True)
class WorkerTelemetry:
    observed_at: datetime = field(default_factory=utc_now)
    latency_ms: TelemetryValue = field(default_factory=unavailable_telemetry)
    remaining_quota: TelemetryValue = field(default_factory=unavailable_telemetry)
    reset_seconds: TelemetryValue = field(default_factory=unavailable_telemetry)
    cost_usd: TelemetryValue = field(default_factory=unavailable_telemetry)
    attributes: Mapping[str, str | int | float | bool] = field(default_factory=dict)

    def to_protocol(self) -> dict[str, Any]:
        return {
            "observedAt": self.observed_at.isoformat().replace("+00:00", "Z"),
            "latencyMs": self.latency_ms.to_api(),
            "remainingQuota": self.remaining_quota.to_api(),
            "resetSeconds": self.reset_seconds.to_api(),
            "costUSD": self.cost_usd.to_api(),
            "attributes": dict(sorted(self.attributes.items())),
        }


HealthProbe = Callable[[], WorkerHealth | Awaitable[WorkerHealth]]
TelemetryProbe = Callable[[], WorkerTelemetry | Awaitable[WorkerTelemetry]]


@runtime_checkable
class WorkerControl(Protocol):
    """Complete provider-neutral control and observation surface."""

    async def discover(self) -> WorkerContract: ...

    async def health(self) -> WorkerHealth: ...

    async def telemetry(self) -> WorkerTelemetry: ...

    async def execute(
        self, request: WorkerRequest, *, event_sink: EventSink | None = None
    ) -> WorkerResult: ...

    def stream(self, request: WorkerRequest) -> AsyncIterator[WorkerEvent | WorkerResult]: ...

    async def cancel(self, run_id: str) -> bool: ...


class GenericWorkerEndpoint:
    """Expose an existing V0 adapter through the generic V0.1 Worker contract.

    The wrapper deliberately delegates execution and cancellation unchanged, so
    provider-specific syntax remains below the adapter boundary.
    """

    def __init__(
        self,
        adapter: WorkerAdapter,
        contract: WorkerContract,
        *,
        health_probe: HealthProbe | None = None,
        telemetry_probe: TelemetryProbe | None = None,
    ) -> None:
        self._adapter = adapter
        self._contract = contract
        self._health_probe = health_probe
        self._telemetry_probe = telemetry_probe

    async def discover(self) -> WorkerContract:
        return self._contract

    async def health(self) -> WorkerHealth:
        if self._health_probe is None:
            return WorkerHealth(WorkerAvailability.UNKNOWN, detail="health probe not configured")
        result = self._health_probe()
        if hasattr(result, "__await__"):
            return await result  # type: ignore[misc]
        return result

    async def telemetry(self) -> WorkerTelemetry:
        if self._telemetry_probe is None:
            return WorkerTelemetry()
        result = self._telemetry_probe()
        if hasattr(result, "__await__"):
            return await result  # type: ignore[misc]
        return result

    async def execute(
        self, request: WorkerRequest, *, event_sink: EventSink | None = None
    ) -> WorkerResult:
        return await self._adapter.execute(request, event_sink=event_sink)

    async def stream(self, request: WorkerRequest) -> AsyncIterator[WorkerEvent | WorkerResult]:
        """Yield normalized events followed by the terminal result.

        A queue decouples adapter event production from the consumer while the
        underlying adapter retains ownership of provider stream parsing.
        """

        queue: asyncio.Queue[WorkerEvent | WorkerResult] = asyncio.Queue()

        async def sink(event: WorkerEvent) -> None:
            await queue.put(event)

        task = asyncio.create_task(self.execute(request, event_sink=sink))
        try:
            while not task.done() or not queue.empty():
                try:
                    yield await asyncio.wait_for(queue.get(), timeout=0.05)
                except TimeoutError:
                    continue
            yield await task
        finally:
            if not task.done():
                await self.cancel(request.run_id)
                task.cancel()
                with suppress(asyncio.CancelledError):
                    await task

    async def cancel(self, run_id: str) -> bool:
        return await self._adapter.cancel(run_id)
