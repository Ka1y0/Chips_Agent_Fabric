from __future__ import annotations

import asyncio
import time
from collections.abc import Mapping
from dataclasses import dataclass, field

from .codec import BridgeCodec, IdentityJSONCodec
from .model import (
    BRIDGE_VERSION,
    IDENTITY_CODEC,
    BridgeCapabilities,
    BridgeDelivery,
    BridgeEnvelope,
    BridgeLimits,
    BridgeTelemetry,
    BridgeValidationError,
    DerivedBridgeArtifact,
    FallbackReason,
    StructuredArtifact,
    TransferContext,
    canonical_json_bytes,
    negotiate,
)


@dataclass(frozen=True, slots=True)
class BridgeConfig:
    enabled: bool = False
    operation_timeout_seconds: float = 2.0
    limits: BridgeLimits = field(default_factory=BridgeLimits)

    def __post_init__(self) -> None:
        if self.operation_timeout_seconds <= 0:
            raise ValueError("operation_timeout_seconds must be positive")


class BridgeFoundation:
    """Optional V0.1 Bridge path with fail-closed policy and lossless fallback.

    This class does not dispatch workers, mutate canonical state, or grant authority.
    The caller persists the returned source or derived artifact under normal Supervisor rules.
    """

    def __init__(
        self,
        config: BridgeConfig | None = None,
        *,
        codecs: Mapping[str, BridgeCodec] | None = None,
    ) -> None:
        self.config = config or BridgeConfig()
        available_codecs = codecs or {IDENTITY_CODEC: IdentityJSONCodec()}
        self._codecs = dict(available_codecs)
        if any(key != codec.codec_id for key, codec in self._codecs.items()):
            raise ValueError("codec registry keys must equal codec_id")

    def capabilities(self, peer_id: str) -> BridgeCapabilities:
        return BridgeCapabilities(
            peer_id=peer_id,
            versions=(BRIDGE_VERSION,),
            codecs=tuple(sorted(self._codecs)),
            max_payload_bytes=self.config.limits.max_payload_bytes,
            available=self.config.enabled,
        )

    async def exchange(
        self,
        *,
        exchange_id: str,
        message_id: str,
        sender: BridgeCapabilities,
        receiver: BridgeCapabilities,
        context: TransferContext,
        source: StructuredArtifact,
    ) -> BridgeDelivery:
        """Round-trip one derived artifact or deterministically retain ``source``."""

        started = time.monotonic()
        source_bytes = b""
        source_hash: str | None = None

        def fallback(
            reason: FallbackReason,
            *,
            version: str | None = None,
            codec_id: str | None = None,
            envelope_hash: str | None = None,
            encoded_bytes: int = 0,
        ) -> BridgeDelivery:
            return BridgeDelivery(
                source=source,
                used_bridge=False,
                telemetry=BridgeTelemetry(
                    exchange_id=exchange_id,
                    sender_id=sender.peer_id,
                    receiver_id=receiver.peer_id,
                    bridge_available=False,
                    latency_ms=max(0, round((time.monotonic() - started) * 1000)),
                    protocol_version=version,
                    codec_id=codec_id,
                    source_artifact_sha256=source_hash,
                    envelope_sha256=envelope_hash,
                    original_bytes=len(source_bytes),
                    encoded_bytes=encoded_bytes,
                    fallback_reason=reason,
                ),
            )

        if not self.config.enabled:
            return fallback(FallbackReason.DISABLED)
        if not context.bridge_allowed:
            return fallback(FallbackReason.FORBIDDEN_AUTHORITY)

        selected = negotiate(sender, receiver)
        if not selected.available:
            return fallback(selected.fallback_reason or FallbackReason.UNSUPPORTED)
        assert selected.version is not None
        assert selected.codec_id is not None
        assert selected.max_payload_bytes is not None
        codec = self._codecs.get(selected.codec_id)
        if codec is None:
            return fallback(
                FallbackReason.UNSUPPORTED,
                version=selected.version,
                codec_id=selected.codec_id,
            )
        try:
            # Revalidate caller-created values before canonicalization. Disabled
            # or forbidden Bridge paths never traverse attacker-controlled data.
            source = StructuredArtifact.from_protocol(source.to_protocol(), self.config.limits)
            source_bytes = canonical_json_bytes(source.to_protocol())
            source_hash = source.sha256
        except BridgeValidationError:
            return fallback(
                FallbackReason.VALIDATION_FAILED,
                version=selected.version,
                codec_id=selected.codec_id,
            )
        if len(source_bytes) > min(
            selected.max_payload_bytes, self.config.limits.max_payload_bytes
        ):
            return fallback(
                FallbackReason.PAYLOAD_LIMIT,
                version=selected.version,
                codec_id=selected.codec_id,
            )

        try:
            envelope = BridgeEnvelope(
                bridge_version=selected.version,
                codec_id=selected.codec_id,
                message_id=message_id,
                sender_id=sender.peer_id,
                receiver_id=receiver.peer_id,
                task_id=context.task_id,
                payload=source,
            )
            envelope_hash = envelope.envelope_sha256
            encoded = await asyncio.wait_for(
                codec.encode(envelope, self.config.limits),
                timeout=self.config.operation_timeout_seconds,
            )
            if not isinstance(encoded, bytes):
                raise BridgeValidationError("codec encode result must be bytes")
            decoded = await asyncio.wait_for(
                codec.decode(encoded, self.config.limits),
                timeout=self.config.operation_timeout_seconds,
            )
        except TimeoutError:
            return fallback(
                FallbackReason.TIMEOUT,
                version=selected.version,
                codec_id=selected.codec_id,
                envelope_hash=locals().get("envelope_hash"),
                encoded_bytes=len(locals().get("encoded", b"")),
            )
        except BridgeValidationError as exc:
            reason = (
                FallbackReason.HASH_MISMATCH
                if "SHA-256 mismatch" in str(exc)
                else FallbackReason.VALIDATION_FAILED
            )
            return fallback(
                reason,
                version=selected.version,
                codec_id=selected.codec_id,
                envelope_hash=locals().get("envelope_hash"),
                encoded_bytes=len(locals().get("encoded", b"")),
            )
        except Exception:
            # Provider/codec error details are intentionally absent from shared telemetry.
            return fallback(
                FallbackReason.DECODE_FAILED,
                version=selected.version,
                codec_id=selected.codec_id,
                envelope_hash=locals().get("envelope_hash"),
                encoded_bytes=len(locals().get("encoded", b"")),
            )

        if (
            decoded.bridge_version != selected.version
            or decoded.codec_id != selected.codec_id
            or decoded.message_id != message_id
            or decoded.sender_id != sender.peer_id
            or decoded.receiver_id != receiver.peer_id
            or decoded.task_id != context.task_id
        ):
            return fallback(
                FallbackReason.VALIDATION_FAILED,
                version=selected.version,
                codec_id=selected.codec_id,
                envelope_hash=envelope_hash,
                encoded_bytes=len(encoded),
            )
        if decoded.envelope_sha256 != envelope_hash:
            return fallback(
                FallbackReason.HASH_MISMATCH,
                version=selected.version,
                codec_id=selected.codec_id,
                envelope_hash=envelope_hash,
                encoded_bytes=len(encoded),
            )

        derived = DerivedBridgeArtifact(
            artifact_id=f"bridge-derived:{exchange_id}",
            source_artifact_id=source.artifact_id,
            content=decoded.payload.content,
            envelope_sha256=decoded.envelope_sha256,
            codec_id=decoded.codec_id,
        )
        telemetry = BridgeTelemetry(
            exchange_id=exchange_id,
            sender_id=sender.peer_id,
            receiver_id=receiver.peer_id,
            bridge_available=True,
            latency_ms=max(0, round((time.monotonic() - started) * 1000)),
            protocol_version=selected.version,
            codec_id=selected.codec_id,
            source_artifact_sha256=source_hash,
            envelope_sha256=decoded.envelope_sha256,
            original_bytes=len(source_bytes),
            encoded_bytes=len(encoded),
        )
        return BridgeDelivery(source=source, used_bridge=True, telemetry=telemetry, derived=derived)
