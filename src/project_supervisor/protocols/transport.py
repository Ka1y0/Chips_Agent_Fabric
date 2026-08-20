from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import StrEnum
from typing import Protocol, runtime_checkable

from project_supervisor.domain import (
    EvidenceConfidence,
    TelemetryValue,
    UnavailableReason,
    utc_now,
)


def unavailable_latency() -> TelemetryValue:
    return TelemetryValue(None, EvidenceConfidence.UNKNOWN, UnavailableReason.NOT_REPORTED)


class TransportHealth(StrEnum):
    HEALTHY = "healthy"
    DEGRADED = "degraded"
    UNREACHABLE = "unreachable"
    UNKNOWN = "unknown"


@dataclass(frozen=True, slots=True)
class TransportStatus:
    transport_id: str
    provider: str
    authenticated: bool
    encrypted: bool
    peer_identity: str | None
    reachable: bool
    health: TransportHealth
    latency_ms: TelemetryValue = field(default_factory=unavailable_latency)
    observed_at: datetime = field(default_factory=utc_now)

    def __post_init__(self) -> None:
        if not self.transport_id.strip() or not self.provider.strip():
            raise ValueError("transport_id and provider must not be empty")
        if self.authenticated and not self.peer_identity:
            raise ValueError("authenticated transport requires a peer identity")

    @property
    def safe_for_fabric(self) -> bool:
        return self.authenticated and self.encrypted and self.reachable and bool(self.peer_identity)

    def to_protocol(self) -> dict[str, object]:
        return {
            "transportID": self.transport_id,
            "provider": self.provider,
            "authenticated": self.authenticated,
            "encrypted": self.encrypted,
            "peerIdentity": self.peer_identity,
            "reachable": self.reachable,
            "latencyMs": self.latency_ms.to_api(),
            "health": self.health.value,
            "observedAt": self.observed_at.isoformat().replace("+00:00", "Z"),
        }


@runtime_checkable
class TransportProvider(Protocol):
    """Vendor-neutral private transport boundary; providers never expose key material."""

    @property
    def provider_id(self) -> str: ...

    async def inspect(self, peer_identity: str) -> TransportStatus: ...

    async def endpoint_for(self, peer_identity: str, service: str) -> str: ...
