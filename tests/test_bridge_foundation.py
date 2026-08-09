from __future__ import annotations

import asyncio
import json
from copy import deepcopy
from pathlib import Path

import jsonschema
import pytest

from project_supervisor.bridge import (
    BRIDGE_VERSION,
    IDENTITY_CODEC,
    AuthorityClass,
    BridgeCapabilities,
    BridgeConfig,
    BridgeEnvelope,
    BridgeFoundation,
    BridgeLimits,
    BridgeValidationError,
    FallbackReason,
    IdentityJSONCodec,
    StructuredArtifact,
    TransferContext,
    negotiate,
)

ROOT = Path(__file__).resolve().parents[1]


def schema(name: str) -> dict[str, object]:
    return json.loads((ROOT / "schemas" / name).read_text())


def peer(
    peer_id: str,
    *,
    versions: tuple[str, ...] = (BRIDGE_VERSION,),
    codecs: tuple[str, ...] = (IDENTITY_CODEC,),
    available: bool = True,
    max_payload_bytes: int = 131_072,
) -> BridgeCapabilities:
    return BridgeCapabilities(
        peer_id=peer_id,
        versions=versions,
        codecs=codecs,
        available=available,
        max_payload_bytes=max_payload_bytes,
    )


def source(content: object | None = None) -> StructuredArtifact:
    return StructuredArtifact("artifact-1", "review", content or {"finding": "bounded"})


async def run_exchange(
    bridge: BridgeFoundation,
    *,
    sender: BridgeCapabilities | None = None,
    receiver: BridgeCapabilities | None = None,
    context: TransferContext | None = None,
    artifact: StructuredArtifact | None = None,
):
    return await bridge.exchange(
        exchange_id="exchange-1",
        message_id="message-1",
        sender=sender or peer("sender"),
        receiver=receiver or peer("receiver"),
        context=context or TransferContext("task-1"),
        source=artifact or source(),
    )


def test_foundation_is_disabled_by_default_and_retains_source() -> None:
    bridge = BridgeFoundation()

    result = asyncio.run(run_exchange(bridge))

    assert not result.used_bridge
    assert result.derived is None
    assert result.source is not None
    assert result.fallback_reason is FallbackReason.DISABLED
    assert bridge.capabilities("local").available is False


def test_disabled_or_enabled_path_bounds_untrusted_caller_content_before_hashing() -> None:
    nested: object = "leaf"
    for _ in range(2_000):
        nested = {"nested": nested}
    unsafe = StructuredArtifact("artifact-unsafe", "review", nested)  # type: ignore[arg-type]

    disabled = asyncio.run(run_exchange(BridgeFoundation(), artifact=unsafe))
    assert disabled.fallback_reason is FallbackReason.DISABLED
    assert disabled.telemetry.source_artifact_sha256 is None

    enabled = asyncio.run(
        run_exchange(BridgeFoundation(BridgeConfig(enabled=True)), artifact=unsafe)
    )
    assert enabled.fallback_reason is FallbackReason.VALIDATION_FAILED
    assert enabled.telemetry.source_artifact_sha256 is None


def test_negotiation_requires_exact_version_and_codec_intersection() -> None:
    selected = negotiate(
        peer("left", versions=("2", "1"), codecs=("b", "a")),
        peer("right", versions=("1", "2"), codecs=("a", "b")),
    )
    assert (selected.version, selected.codec_id) == ("2", "b")

    version_mismatch = negotiate(peer("left", versions=("1.0",)), peer("right", versions=("1",)))
    assert version_mismatch.fallback_reason is FallbackReason.VERSION_MISMATCH

    codec_mismatch = negotiate(peer("left", codecs=("x/1",)), peer("right", codecs=("x/2",)))
    assert codec_mismatch.fallback_reason is FallbackReason.CODEC_MISMATCH

    unavailable = negotiate(peer("left"), peer("right", available=False))
    assert unavailable.fallback_reason is FallbackReason.UNSUPPORTED


def test_identity_codec_round_trip_is_derived_non_authoritative_and_schema_valid() -> None:
    bridge = BridgeFoundation(BridgeConfig(enabled=True))

    result = asyncio.run(run_exchange(bridge))

    assert result.used_bridge
    assert result.fallback_reason is None
    assert result.derived is not None
    assert result.derived.content == {"finding": "bounded"}
    assert result.derived.authoritative is False
    assert result.derived.canonical is False
    assert result.derived.source_artifact_id == "artifact-1"
    assert result.telemetry.bridge_available is True

    jsonschema.validate(
        bridge.capabilities("local").to_protocol(), schema("bridge-capabilities-v1.schema.json")
    )
    jsonschema.validate(
        result.derived.to_protocol(), schema("bridge-derived-artifact-v1.schema.json")
    )
    jsonschema.validate(result.telemetry.to_protocol(), schema("bridge-telemetry-v1.schema.json"))


def test_envelope_hash_covers_metadata_and_payload_and_parser_is_strict() -> None:
    envelope = BridgeEnvelope(
        BRIDGE_VERSION,
        IDENTITY_CODEC,
        "message-1",
        "sender",
        "receiver",
        "task-1",
        source(),
    )
    wire = envelope.to_protocol()
    jsonschema.validate(wire, schema("bridge-envelope-v1.schema.json"))

    changed_metadata = deepcopy(wire)
    changed_metadata["receiverID"] = "another-receiver"
    with pytest.raises(BridgeValidationError, match="SHA-256 mismatch"):
        BridgeEnvelope.from_bytes(json.dumps(changed_metadata).encode())

    changed_payload = deepcopy(wire)
    changed_payload["payload"]["content"] = {"finding": "tampered"}
    with pytest.raises(BridgeValidationError, match="SHA-256 mismatch"):
        BridgeEnvelope.from_bytes(json.dumps(changed_payload).encode())

    extra_field = deepcopy(wire)
    extra_field["unexpected"] = True
    with pytest.raises(BridgeValidationError, match="fields"):
        BridgeEnvelope.from_bytes(json.dumps(extra_field).encode())

    duplicate_field = envelope.to_bytes().replace(
        b'{"bridgeVersion":', b'{"bridgeVersion":"fabric-bridge/1.0","bridgeVersion":', 1
    )
    with pytest.raises(BridgeValidationError, match="duplicate"):
        BridgeEnvelope.from_bytes(duplicate_field)


def test_payload_depth_size_and_field_bounds_are_enforced() -> None:
    limits = BridgeLimits(max_payload_bytes=100, max_envelope_bytes=1_000, max_depth=2)
    too_deep = {"a": {"b": {"c": "value"}}}
    with pytest.raises(BridgeValidationError, match="depth"):
        StructuredArtifact.from_protocol(
            {"artifactID": "a", "artifactType": "review", "content": too_deep}, limits
        )

    with pytest.raises(BridgeValidationError, match="byte limit"):
        StructuredArtifact.from_protocol(
            {"artifactID": "a", "artifactType": "review", "content": "x" * 200}, limits
        )

    with pytest.raises(BridgeValidationError, match="fields"):
        StructuredArtifact.from_protocol(
            {"artifactID": "a", "artifactType": "review", "content": {}, "extra": True}
        )


@pytest.mark.parametrize("authority", list(AuthorityClass))
def test_authority_bearing_content_never_uses_bridge(authority: AuthorityClass) -> None:
    bridge = BridgeFoundation(BridgeConfig(enabled=True))
    context = TransferContext("task-1", frozenset({authority}))

    result = asyncio.run(run_exchange(bridge, context=context))

    assert not result.used_bridge
    assert result.fallback_reason is FallbackReason.FORBIDDEN_AUTHORITY
    assert result.source.content == {"finding": "bounded"}


@pytest.mark.parametrize(
    ("sender", "receiver", "reason"),
    [
        (peer("sender", available=False), peer("receiver"), FallbackReason.UNSUPPORTED),
        (
            peer("sender", versions=("fabric-bridge/2.0",)),
            peer("receiver"),
            FallbackReason.VERSION_MISMATCH,
        ),
        (
            peer("sender", codecs=("unknown/1",)),
            peer("receiver"),
            FallbackReason.CODEC_MISMATCH,
        ),
    ],
)
def test_negotiation_failures_fall_back_unchanged(sender, receiver, reason) -> None:
    result = asyncio.run(
        run_exchange(BridgeFoundation(BridgeConfig(enabled=True)), sender=sender, receiver=receiver)
    )
    assert not result.used_bridge
    assert result.source.content == {"finding": "bounded"}
    assert result.fallback_reason is reason


def test_negotiated_payload_limit_falls_back() -> None:
    result = asyncio.run(
        run_exchange(
            BridgeFoundation(BridgeConfig(enabled=True)),
            receiver=peer("receiver", max_payload_bytes=8),
        )
    )
    assert result.fallback_reason is FallbackReason.PAYLOAD_LIMIT


class CorruptingCodec(IdentityJSONCodec):
    async def decode(self, encoded: bytes, limits: BridgeLimits) -> BridgeEnvelope:
        envelope = await super().decode(encoded, limits)
        return BridgeEnvelope(
            envelope.bridge_version,
            envelope.codec_id,
            envelope.message_id,
            envelope.sender_id,
            envelope.receiver_id,
            envelope.task_id,
            StructuredArtifact("artifact-1", "review", {"finding": "changed"}),
        )


class InvalidCodec(IdentityJSONCodec):
    async def decode(self, encoded: bytes, limits: BridgeLimits) -> BridgeEnvelope:
        return BridgeEnvelope.from_bytes(b"not-json", limits)


class FailingCodec(IdentityJSONCodec):
    async def decode(self, encoded: bytes, limits: BridgeLimits) -> BridgeEnvelope:
        raise RuntimeError("private codec detail must not escape")


class SlowCodec(IdentityJSONCodec):
    async def encode(self, envelope: BridgeEnvelope, limits: BridgeLimits) -> bytes:
        await asyncio.sleep(0.05)
        return await super().encode(envelope, limits)


@pytest.mark.parametrize(
    ("codec", "reason"),
    [
        (CorruptingCodec(), FallbackReason.HASH_MISMATCH),
        (InvalidCodec(), FallbackReason.VALIDATION_FAILED),
        (FailingCodec(), FallbackReason.DECODE_FAILED),
    ],
)
def test_codec_failures_are_reason_coded_and_do_not_leak_details(codec, reason) -> None:
    bridge = BridgeFoundation(BridgeConfig(enabled=True), codecs={IDENTITY_CODEC: codec})

    result = asyncio.run(run_exchange(bridge))

    assert not result.used_bridge
    assert result.fallback_reason is reason
    telemetry = result.telemetry.to_protocol()
    assert "private codec detail" not in json.dumps(telemetry)
    assert "finding" not in json.dumps(telemetry)


def test_timeout_is_reason_coded_and_source_is_retained() -> None:
    bridge = BridgeFoundation(
        BridgeConfig(enabled=True, operation_timeout_seconds=0.001),
        codecs={IDENTITY_CODEC: SlowCodec()},
    )

    result = asyncio.run(run_exchange(bridge))

    assert result.fallback_reason is FallbackReason.TIMEOUT
    assert result.source.content == {"finding": "bounded"}


def test_telemetry_contains_only_allowlisted_safe_metadata() -> None:
    secret_like_content = "never-copy-raw-payload-to-telemetry"
    result = asyncio.run(
        run_exchange(
            BridgeFoundation(BridgeConfig(enabled=True)), artifact=source(secret_like_content)
        )
    )
    telemetry = result.telemetry.to_protocol()

    assert set(telemetry) == {
        "exchangeID",
        "senderID",
        "receiverID",
        "bridgeAvailable",
        "latencyMs",
        "protocolVersion",
        "codecID",
        "sourceArtifactSha256",
        "envelopeSha256",
        "originalBytes",
        "encodedBytes",
        "fallbackReason",
    }
    assert secret_like_content not in json.dumps(telemetry)
