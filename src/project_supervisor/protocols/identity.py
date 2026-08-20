from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from project_supervisor.domain import utc_now

_FINGERPRINT = re.compile(r"^sha256:[0-9a-f]{64}$")


@dataclass(frozen=True, slots=True)
class NodePublicIdentity:
    """Stable public node identity metadata.

    This model intentionally has no private-key or secret field. Key generation
    and secure storage belong to an enrollment implementation, not this model.
    """

    node_id: str
    key_id: str
    algorithm: str
    public_key_fingerprint: str
    created_at: datetime = field(default_factory=utc_now)
    rotated_from_key_id: str | None = None

    def __post_init__(self) -> None:
        if not self.node_id.strip() or not self.key_id.strip() or not self.algorithm.strip():
            raise ValueError("node_id, key_id, and algorithm must not be empty")
        if not _FINGERPRINT.fullmatch(self.public_key_fingerprint):
            raise ValueError("public_key_fingerprint must be lowercase sha256:<64 hex>")
        if self.rotated_from_key_id == self.key_id:
            raise ValueError("rotated_from_key_id must differ from key_id")

    def to_protocol(self) -> dict[str, Any]:
        return {
            "nodeID": self.node_id,
            "keyID": self.key_id,
            "algorithm": self.algorithm,
            "publicKeyFingerprint": self.public_key_fingerprint,
            "createdAt": self.created_at.isoformat().replace("+00:00", "Z"),
            "rotatedFromKeyID": self.rotated_from_key_id,
        }
