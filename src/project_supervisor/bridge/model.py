from __future__ import annotations

import hashlib
import json
import math
from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any

type JSONScalar = str | int | float | bool | None
type JSONValue = JSONScalar | list[JSONValue] | dict[str, JSONValue]

BRIDGE_VERSION = "fabric-bridge/1.0"
IDENTITY_CODEC = "identity-json/1.0"


class BridgeValidationError(ValueError):
    """Raised when untrusted Bridge data is not an exact supported shape."""


class FallbackReason(StrEnum):
    DISABLED = "disabled"
    FORBIDDEN_AUTHORITY = "forbiddenAuthority"
    UNSUPPORTED = "unsupported"
    VERSION_MISMATCH = "versionMismatch"
    CODEC_MISMATCH = "codecMismatch"
    VALIDATION_FAILED = "validationFailed"
    PAYLOAD_LIMIT = "payloadLimit"
    HASH_MISMATCH = "hashMismatch"
    DECODE_FAILED = "decodeFailed"
    TIMEOUT = "timeout"


class AuthorityClass(StrEnum):
    RED_ACTION = "redAction"
    CODE_WRITE = "codeWrite"
    CREDENTIAL = "credential"
    CAPABILITY_GRANT = "capabilityGrant"
    TOOL_CALL = "toolCall"
    SHELL = "shell"


FORBIDDEN_AUTHORITIES = frozenset(AuthorityClass)


@dataclass(frozen=True, slots=True)
class BridgeLimits:
    max_payload_bytes: int = 131_072
    max_envelope_bytes: int = 262_144
    max_depth: int = 16
    max_container_items: int = 2_048
    max_string_bytes: int = 65_536

    def __post_init__(self) -> None:
        for name in (
            "max_payload_bytes",
            "max_envelope_bytes",
            "max_depth",
            "max_container_items",
            "max_string_bytes",
        ):
            if getattr(self, name) <= 0:
                raise ValueError(f"{name} must be positive")
        if self.max_payload_bytes > self.max_envelope_bytes:
            raise ValueError("max_payload_bytes cannot exceed max_envelope_bytes")


def canonical_json_bytes(value: JSONValue) -> bytes:
    """Return the single Fabric canonical JSON representation used for hashing."""

    try:
        return json.dumps(
            value,
            allow_nan=False,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise BridgeValidationError("value is not canonical JSON") from exc


def sha256_hex(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def validate_json(value: Any, limits: BridgeLimits, *, payload: bool = True) -> JSONValue:
    """Validate a bounded JSON tree without coercing attacker-controlled values."""

    item_count = 0

    def visit(node: Any, depth: int) -> JSONValue:
        nonlocal item_count
        if depth > limits.max_depth:
            raise BridgeValidationError("JSON depth limit exceeded")
        if node is None or isinstance(node, (str, bool, int, float)):
            if isinstance(node, str) and len(node.encode("utf-8")) > limits.max_string_bytes:
                raise BridgeValidationError("JSON string limit exceeded")
            if isinstance(node, float) and not math.isfinite(node):
                raise BridgeValidationError("non-finite JSON number")
            return node
        if isinstance(node, list):
            item_count += len(node)
            if item_count > limits.max_container_items:
                raise BridgeValidationError("JSON container item limit exceeded")
            return [visit(item, depth + 1) for item in node]
        if isinstance(node, dict):
            if any(not isinstance(key, str) for key in node):
                raise BridgeValidationError("JSON object keys must be strings")
            item_count += len(node)
            if item_count > limits.max_container_items:
                raise BridgeValidationError("JSON container item limit exceeded")
            return {key: visit(item, depth + 1) for key, item in node.items()}
        raise BridgeValidationError(f"unsupported JSON value type: {type(node).__name__}")

    validated = visit(value, 0)
    if payload and len(canonical_json_bytes(validated)) > limits.max_payload_bytes:
        raise BridgeValidationError("payload byte limit exceeded")
    return validated


def _non_empty(value: Any, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise BridgeValidationError(f"{field} must be a non-empty string")
    return value


@dataclass(frozen=True, slots=True)
class StructuredArtifact:
    """Canonical structured input retained unchanged whenever Bridge falls back."""

    artifact_id: str
    artifact_type: str
    content: JSONValue

    def __post_init__(self) -> None:
        _non_empty(self.artifact_id, "artifactID")
        _non_empty(self.artifact_type, "artifactType")

    def to_protocol(self) -> dict[str, JSONValue]:
        return {
            "artifactID": self.artifact_id,
            "artifactType": self.artifact_type,
            "content": self.content,
        }

    @classmethod
    def from_protocol(cls, value: Any, limits: BridgeLimits | None = None) -> StructuredArtifact:
        if not isinstance(value, dict):
            raise BridgeValidationError("artifact must be an object")
        expected = {"artifactID", "artifactType", "content"}
        if set(value) != expected:
            raise BridgeValidationError("artifact fields must match the protocol exactly")
        active_limits = limits or BridgeLimits()
        content = validate_json(value["content"], active_limits)
        return cls(
            artifact_id=_non_empty(value["artifactID"], "artifactID"),
            artifact_type=_non_empty(value["artifactType"], "artifactType"),
            content=content,
        )

    @property
    def sha256(self) -> str:
        return sha256_hex(canonical_json_bytes(self.to_protocol()))


@dataclass(frozen=True, slots=True)
class TransferContext:
    task_id: str
    authorities: frozenset[AuthorityClass] = frozenset()

    def __post_init__(self) -> None:
        _non_empty(self.task_id, "taskID")
        unknown = set(self.authorities) - FORBIDDEN_AUTHORITIES
        if unknown:
            raise ValueError(f"unknown authority classes: {sorted(unknown)}")

    @property
    def bridge_allowed(self) -> bool:
        return not self.authorities


@dataclass(frozen=True, slots=True)
class BridgeCapabilities:
    peer_id: str
    versions: tuple[str, ...]
    codecs: tuple[str, ...]
    max_payload_bytes: int
    available: bool = True

    def __post_init__(self) -> None:
        _non_empty(self.peer_id, "peerID")
        if self.max_payload_bytes <= 0:
            raise ValueError("max_payload_bytes must be positive")
        for field_name, values in {"versions": self.versions, "codecs": self.codecs}.items():
            if len(values) != len(set(values)):
                raise ValueError(f"{field_name} must not contain duplicates")
            if any(not value.strip() for value in values):
                raise ValueError(f"{field_name} values must not be empty")

    def to_protocol(self) -> dict[str, JSONValue]:
        return {
            "peerID": self.peer_id,
            "versions": list(self.versions),
            "codecs": list(self.codecs),
            "maxPayloadBytes": self.max_payload_bytes,
            "available": self.available,
        }


@dataclass(frozen=True, slots=True)
class BridgeNegotiation:
    available: bool
    version: str | None = None
    codec_id: str | None = None
    max_payload_bytes: int | None = None
    fallback_reason: FallbackReason | None = None


def negotiate(
    local: BridgeCapabilities,
    remote: BridgeCapabilities,
) -> BridgeNegotiation:
    """Negotiate exact string intersections using stable local preference order."""

    if not local.available or not remote.available:
        return BridgeNegotiation(False, fallback_reason=FallbackReason.UNSUPPORTED)
    version = next((item for item in local.versions if item in set(remote.versions)), None)
    if version is None:
        return BridgeNegotiation(False, fallback_reason=FallbackReason.VERSION_MISMATCH)
    codec_id = next((item for item in local.codecs if item in set(remote.codecs)), None)
    if codec_id is None:
        return BridgeNegotiation(False, fallback_reason=FallbackReason.CODEC_MISMATCH)
    return BridgeNegotiation(
        True,
        version=version,
        codec_id=codec_id,
        max_payload_bytes=min(local.max_payload_bytes, remote.max_payload_bytes),
    )


@dataclass(frozen=True, slots=True)
class BridgeEnvelope:
    bridge_version: str
    codec_id: str
    message_id: str
    sender_id: str
    receiver_id: str
    task_id: str
    payload: StructuredArtifact

    def body_protocol(self) -> dict[str, JSONValue]:
        return {
            "bridgeVersion": self.bridge_version,
            "codecID": self.codec_id,
            "messageID": self.message_id,
            "senderID": self.sender_id,
            "receiverID": self.receiver_id,
            "taskID": self.task_id,
            "payload": self.payload.to_protocol(),
        }

    @property
    def envelope_sha256(self) -> str:
        # Covers every envelope field, not merely a Project_Bridge claim subset.
        return sha256_hex(canonical_json_bytes(self.body_protocol()))

    def to_protocol(self) -> dict[str, JSONValue]:
        return {**self.body_protocol(), "envelopeSha256": self.envelope_sha256}

    def to_bytes(self, limits: BridgeLimits | None = None) -> bytes:
        encoded = canonical_json_bytes(self.to_protocol())
        if len(encoded) > (limits or BridgeLimits()).max_envelope_bytes:
            raise BridgeValidationError("envelope byte limit exceeded")
        return encoded

    @classmethod
    def from_bytes(cls, encoded: bytes, limits: BridgeLimits | None = None) -> BridgeEnvelope:
        active_limits = limits or BridgeLimits()
        if len(encoded) > active_limits.max_envelope_bytes:
            raise BridgeValidationError("envelope byte limit exceeded")

        def strict_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
            result: dict[str, Any] = {}
            for key, value in pairs:
                if key in result:
                    raise BridgeValidationError("duplicate JSON object field")
                result[key] = value
            return result

        try:
            raw = json.loads(encoded, object_pairs_hook=strict_object)
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise BridgeValidationError("envelope is not valid UTF-8 JSON") from exc
        expected = {
            "bridgeVersion",
            "codecID",
            "messageID",
            "senderID",
            "receiverID",
            "taskID",
            "payload",
            "envelopeSha256",
        }
        if not isinstance(raw, dict) or set(raw) != expected:
            raise BridgeValidationError("envelope fields must match the protocol exactly")
        validate_json(raw, active_limits, payload=False)
        envelope = cls(
            bridge_version=_non_empty(raw["bridgeVersion"], "bridgeVersion"),
            codec_id=_non_empty(raw["codecID"], "codecID"),
            message_id=_non_empty(raw["messageID"], "messageID"),
            sender_id=_non_empty(raw["senderID"], "senderID"),
            receiver_id=_non_empty(raw["receiverID"], "receiverID"),
            task_id=_non_empty(raw["taskID"], "taskID"),
            payload=StructuredArtifact.from_protocol(raw["payload"], active_limits),
        )
        supplied_hash = raw["envelopeSha256"]
        if not isinstance(supplied_hash, str) or supplied_hash != envelope.envelope_sha256:
            raise BridgeValidationError("envelope SHA-256 mismatch")
        return envelope


@dataclass(frozen=True, slots=True)
class DerivedBridgeArtifact:
    artifact_id: str
    source_artifact_id: str
    content: JSONValue
    envelope_sha256: str
    codec_id: str
    authoritative: bool = field(default=False, init=False)
    canonical: bool = field(default=False, init=False)

    def to_protocol(self) -> dict[str, JSONValue]:
        return {
            "artifactID": self.artifact_id,
            "sourceArtifactID": self.source_artifact_id,
            "content": self.content,
            "envelopeSha256": self.envelope_sha256,
            "codecID": self.codec_id,
            "authoritative": False,
            "canonical": False,
            "stateUse": "derivedOnly",
        }


@dataclass(frozen=True, slots=True)
class BridgeTelemetry:
    exchange_id: str
    sender_id: str
    receiver_id: str
    bridge_available: bool
    latency_ms: int
    protocol_version: str | None = None
    codec_id: str | None = None
    source_artifact_sha256: str | None = None
    envelope_sha256: str | None = None
    original_bytes: int = 0
    encoded_bytes: int = 0
    fallback_reason: FallbackReason | None = None

    def to_protocol(self) -> dict[str, JSONValue]:
        return {
            "exchangeID": self.exchange_id,
            "senderID": self.sender_id,
            "receiverID": self.receiver_id,
            "bridgeAvailable": self.bridge_available,
            "latencyMs": self.latency_ms,
            "protocolVersion": self.protocol_version,
            "codecID": self.codec_id,
            "sourceArtifactSha256": self.source_artifact_sha256,
            "envelopeSha256": self.envelope_sha256,
            "originalBytes": self.original_bytes,
            "encodedBytes": self.encoded_bytes,
            "fallbackReason": self.fallback_reason.value if self.fallback_reason else None,
        }


@dataclass(frozen=True, slots=True)
class BridgeDelivery:
    """Outcome containing either a derived Bridge artifact or the unchanged source."""

    source: StructuredArtifact
    used_bridge: bool
    telemetry: BridgeTelemetry
    derived: DerivedBridgeArtifact | None = None

    @property
    def fallback_reason(self) -> FallbackReason | None:
        return self.telemetry.fallback_reason
