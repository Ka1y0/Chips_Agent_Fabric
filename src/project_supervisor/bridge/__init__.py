"""Optional, non-authoritative Project_Bridge integration foundation."""

from .codec import BridgeCodec, IdentityJSONCodec
from .foundation import BridgeConfig, BridgeFoundation
from .model import (
    BRIDGE_VERSION,
    IDENTITY_CODEC,
    AuthorityClass,
    BridgeCapabilities,
    BridgeDelivery,
    BridgeEnvelope,
    BridgeLimits,
    BridgeNegotiation,
    BridgeTelemetry,
    BridgeValidationError,
    DerivedBridgeArtifact,
    FallbackReason,
    StructuredArtifact,
    TransferContext,
    canonical_json_bytes,
    negotiate,
)

__all__ = [
    "BRIDGE_VERSION",
    "IDENTITY_CODEC",
    "AuthorityClass",
    "BridgeCapabilities",
    "BridgeCodec",
    "BridgeConfig",
    "BridgeDelivery",
    "BridgeEnvelope",
    "BridgeFoundation",
    "BridgeLimits",
    "BridgeNegotiation",
    "BridgeTelemetry",
    "BridgeValidationError",
    "DerivedBridgeArtifact",
    "FallbackReason",
    "IdentityJSONCodec",
    "StructuredArtifact",
    "TransferContext",
    "canonical_json_bytes",
    "negotiate",
]
