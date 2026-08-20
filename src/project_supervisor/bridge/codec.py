from __future__ import annotations

import asyncio
from typing import Protocol, runtime_checkable

from .model import (
    IDENTITY_CODEC,
    BridgeEnvelope,
    BridgeLimits,
    BridgeValidationError,
)


@runtime_checkable
class BridgeCodec(Protocol):
    """A bounded, non-authoritative transform below the Fabric Bridge boundary."""

    codec_id: str

    async def encode(self, envelope: BridgeEnvelope, limits: BridgeLimits) -> bytes: ...

    async def decode(self, encoded: bytes, limits: BridgeLimits) -> BridgeEnvelope: ...


class IdentityJSONCodec:
    """Deterministic offline codec used to prove the adapter path without an AI model."""

    codec_id = IDENTITY_CODEC

    async def encode(self, envelope: BridgeEnvelope, limits: BridgeLimits) -> bytes:
        await asyncio.sleep(0)
        return envelope.to_bytes(limits)

    async def decode(self, encoded: bytes, limits: BridgeLimits) -> BridgeEnvelope:
        await asyncio.sleep(0)
        envelope = BridgeEnvelope.from_bytes(encoded, limits)
        if envelope.codec_id != self.codec_id:
            raise BridgeValidationError("envelope codec does not match decoder")
        return envelope
