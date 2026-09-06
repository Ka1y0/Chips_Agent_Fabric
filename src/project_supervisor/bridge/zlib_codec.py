"""Opt-in lossless wire compression, not semantic compression or token savings.

Wire format: ``FZ1\\0`` + ``Z`` (zlib) or ``J`` (raw canonical JSON) + body.
Limits apply both before compression and while decompressing. The existing
BridgeEnvelope remains responsible for schema and end-to-end digest validation.
"""

from __future__ import annotations

import asyncio
import zlib
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .model import BridgeEnvelope, BridgeLimits

ZLIB_JSON_CODEC = "zlib-json/1.0"
_MAGIC = b"FZ1\x00"


def _pack(raw: bytes, max_bytes: int) -> bytes:
    if not isinstance(raw, bytes) or len(raw) > max_bytes:
        raise ValueError("uncompressed envelope limit exceeded")
    compressed = zlib.compress(raw, level=3)
    mode, body = (b"Z", compressed) if len(compressed) < len(raw) else (b"J", raw)
    framed = _MAGIC + mode + body
    if len(framed) > max_bytes:
        raise ValueError("encoded envelope limit exceeded")
    return framed


def _unpack(encoded: bytes, max_bytes: int) -> bytes:
    if not isinstance(encoded, bytes) or len(encoded) > max_bytes:
        raise ValueError("encoded envelope limit exceeded")
    if len(encoded) < 5 or encoded[:4] != _MAGIC:
        raise ValueError("unsupported compression frame")
    mode, body = encoded[4:5], encoded[5:]
    if mode == b"J":
        return body
    if mode != b"Z":
        raise ValueError("unsupported compression mode")
    try:
        decoder = zlib.decompressobj()
        raw = decoder.decompress(body, max_bytes + 1)
    except zlib.error:
        raise ValueError("invalid compressed envelope") from None
    if len(raw) > max_bytes or decoder.unconsumed_tail:
        raise ValueError("decompressed envelope limit exceeded")
    if not decoder.eof or decoder.unused_data:
        raise ValueError("truncated or trailing compressed data")
    # Never call flush(): its length is an initial buffer size, not an output cap.
    return raw


class ZlibJSONCodec:
    """Register explicitly through BridgeFoundation(codecs=...). Defaults are unchanged."""

    codec_id = ZLIB_JSON_CODEC

    async def encode(self, envelope: BridgeEnvelope, limits: BridgeLimits) -> bytes:
        from .model import BridgeEnvelope, BridgeValidationError

        await asyncio.sleep(0)
        if envelope.codec_id != self.codec_id:
            raise BridgeValidationError("envelope codec does not match encoder")
        raw = envelope.to_bytes(limits)
        # Direct codec callers receive the same validation as Foundation callers.
        BridgeEnvelope.from_bytes(raw, limits)
        try:
            return _pack(raw, limits.max_envelope_bytes)
        except ValueError as error:
            raise BridgeValidationError(str(error)) from None

    async def decode(self, encoded: bytes, limits: BridgeLimits) -> BridgeEnvelope:
        from .model import BridgeEnvelope, BridgeValidationError

        await asyncio.sleep(0)
        try:
            raw = _unpack(encoded, limits.max_envelope_bytes)
        except ValueError as error:
            raise BridgeValidationError(str(error)) from None
        envelope = BridgeEnvelope.from_bytes(raw, limits)
        if envelope.codec_id != self.codec_id:
            raise BridgeValidationError("envelope codec does not match decoder")
        return envelope
