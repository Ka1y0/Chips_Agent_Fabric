from __future__ import annotations

import asyncio
import json
import random
import sys
import zlib
from dataclasses import replace

import pytest

from project_supervisor.bridge.zlib_codec import ZLIB_JSON_CODEC, ZlibJSONCodec, _pack, _unpack


@pytest.mark.parametrize("raw", [b"", b"a", b"abc", b"a" * 4096, bytes(range(256)) * 10,
                                  "中文 / Unicode / 🙂".encode() * 100])
def test_wire_round_trip(raw: bytes) -> None:
    wire = _pack(raw, 65536)
    assert _unpack(wire, 65536) == raw


def test_incompressible_wire_uses_raw_mode_instead_of_expanding_body() -> None:
    raw = random.Random(42).randbytes(1024)
    wire = _pack(raw, 2048)
    assert wire[4:5] == b"J"
    assert len(wire) == len(raw) + 5
    assert _unpack(wire, 2048) == raw


@pytest.mark.parametrize("wire", [
    b"", b"FZ1", b"OTHER", b"FZ1\x00?data", b"FZ1\x00Znot-zlib",
    b"FZ1\x00Z" + zlib.compress(b"test")[:-1],
    b"FZ1\x00Z" + zlib.compress(b"test") + b"trailing",
    b"FZ1\x00Z" + zlib.compress(b"test") + zlib.compress(b"second stream"),
])
def test_wire_rejects_malformed_truncated_and_concatenated_frames(wire: bytes) -> None:
    with pytest.raises(ValueError):
        _unpack(wire, 4096)


def test_decompression_bomb_is_rejected_before_unbounded_allocation() -> None:
    wire = b"FZ1\x00Z" + zlib.compress(b"a" * 100_000)
    with pytest.raises(ValueError, match="decompressed envelope limit"):
        _unpack(wire, 512)


def test_wire_limits_include_framing_overhead() -> None:
    with pytest.raises(ValueError, match="encoded envelope limit"):
        _pack(b"abc", 3)
    with pytest.raises(ValueError, match="uncompressed envelope limit"):
        _pack(b"a" * 100, 99)
    with pytest.raises(ValueError, match="encoded envelope limit"):
        _unpack(b"FZ1\x00J" + b"a" * 100, 50)


async def test_integration_existing_foundation_and_authority_fallback() -> None:
    from project_supervisor.bridge import (
        AuthorityClass,
        BridgeConfig,
        BridgeFoundation,
        FallbackReason,
        StructuredArtifact,
        TransferContext,
    )

    bridge = BridgeFoundation(BridgeConfig(enabled=True), codecs={ZLIB_JSON_CODEC: ZlibJSONCodec()})
    source = StructuredArtifact("artifact-1", "context", {"facts": "repeatable fact; " * 1000})
    arguments = {
        "exchange_id": "exchange-1", "message_id": "message-1",
        "sender": bridge.capabilities("worker-a"), "receiver": bridge.capabilities("worker-b"),
        "source": source,
    }
    delivery = await bridge.exchange(context=TransferContext("task-1"), **arguments)
    assert delivery.used_bridge
    assert delivery.derived.content == source.content
    assert delivery.derived.authoritative is False
    assert delivery.telemetry.encoded_bytes < delivery.telemetry.original_bytes
    denied = await bridge.exchange(
        context=TransferContext("task-1", frozenset({AuthorityClass.CAPABILITY_GRANT})),
        **arguments,
    )
    assert denied.fallback_reason is FallbackReason.FORBIDDEN_AUTHORITY
    assert denied.source is source
    assert not denied.used_bridge
    assert ZLIB_JSON_CODEC not in BridgeFoundation().capabilities("default").codecs


async def test_integration_round_trip_across_real_worker_process_boundary() -> None:
    from project_supervisor.bridge import (
        BRIDGE_VERSION,
        BridgeEnvelope,
        BridgeLimits,
        StructuredArtifact,
    )

    limits = BridgeLimits()
    envelope = BridgeEnvelope(
        BRIDGE_VERSION, ZLIB_JSON_CODEC, "message-1", "worker-a", "worker-b", "task-1",
        StructuredArtifact("artifact-1", "context", {"fact": "exact; " * 1000}),
    )
    wire = await ZlibJSONCodec().encode(envelope, limits)
    code = '''import asyncio, json, sys
from project_supervisor.bridge import BridgeLimits
from project_supervisor.bridge.zlib_codec import ZlibJSONCodec
async def main():
    data = sys.stdin.buffer.read(262145)
    envelope = await ZlibJSONCodec().decode(data, BridgeLimits())
    print(json.dumps(envelope.to_protocol()))
asyncio.run(main())
'''
    process = await asyncio.create_subprocess_exec(
        sys.executable, "-c", code, stdin=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
    )
    try:
        stdout, stderr = await asyncio.wait_for(process.communicate(wire), 10)
    finally:
        if process.returncode is None:
            process.kill()
            await process.wait()
    assert process.returncode == 0, stderr.decode()
    assert json.loads(stdout) == envelope.to_protocol()


async def test_integration_digest_mismatch_and_wrong_codec_are_rejected() -> None:
    from project_supervisor.bridge import (
        BRIDGE_VERSION,
        BridgeEnvelope,
        BridgeLimits,
        BridgeValidationError,
        StructuredArtifact,
    )

    limits = BridgeLimits()
    envelope = BridgeEnvelope(
        BRIDGE_VERSION, ZLIB_JSON_CODEC, "message-1", "worker-a", "worker-b", "task-1",
        StructuredArtifact("artifact-1", "context", {"fact": "original"}),
    )
    raw = envelope.to_bytes(limits).replace(b"original", b"tampered")
    with pytest.raises(BridgeValidationError, match="SHA-256 mismatch"):
        await ZlibJSONCodec().decode(_pack(raw, limits.max_envelope_bytes), limits)
    with pytest.raises(BridgeValidationError, match="codec"):
        await ZlibJSONCodec().encode(replace(envelope, codec_id="identity-json/1.0"), limits)
