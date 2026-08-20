from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import StrEnum
from typing import Any

_SCOPE = re.compile(r"^[a-z][a-z0-9]*(?:[._-][a-z0-9]+)+$")


class GrantState(StrEnum):
    ACTIVE = "active"
    REVOKED = "revoked"
    EXPIRED = "expired"


@dataclass(frozen=True, slots=True)
class CapabilityGrant:
    """Auditable authorization data only; this object performs no privileged action."""

    grant_id: str
    capability: str
    subject: str
    requested_by: str
    task_id: str
    issued_by: str
    issued_at: datetime
    expires_at: datetime | None
    state: GrantState = GrantState.ACTIVE
    constraints: dict[str, str] = field(default_factory=dict)
    revoked_at: datetime | None = None
    revoked_by: str | None = None

    def __post_init__(self) -> None:
        if not all(
            value.strip()
            for value in (
                self.grant_id,
                self.subject,
                self.requested_by,
                self.task_id,
                self.issued_by,
            )
        ):
            raise ValueError("grant identifiers and attribution must not be empty")
        if not _SCOPE.fullmatch(self.capability):
            raise ValueError("capability must be a scoped machine-readable name")
        if self.issued_at.tzinfo is None or (self.expires_at and self.expires_at.tzinfo is None):
            raise ValueError("grant timestamps must be timezone-aware")
        if self.expires_at is not None and self.expires_at <= self.issued_at:
            raise ValueError("expires_at must be later than issued_at")
        if self.state is GrantState.REVOKED and not (self.revoked_at and self.revoked_by):
            raise ValueError("revoked grants require revoked_at and revoked_by")
        if self.state is not GrantState.REVOKED and (self.revoked_at or self.revoked_by):
            raise ValueError("revocation metadata requires revoked state")

    def authorizes(
        self,
        capability: str,
        *,
        subject: str,
        task_id: str,
        now: datetime | None = None,
    ) -> bool:
        observed = now or datetime.now(UTC)
        return (
            self.state is GrantState.ACTIVE
            and self.capability == capability
            and self.subject == subject
            and self.task_id == task_id
            and (self.expires_at is None or observed < self.expires_at)
        )

    def to_protocol(self) -> dict[str, Any]:
        def stamp(value: datetime | None) -> str | None:
            return value.isoformat().replace("+00:00", "Z") if value else None

        return {
            "grantID": self.grant_id,
            "capability": self.capability,
            "subject": self.subject,
            "requestedBy": self.requested_by,
            "taskID": self.task_id,
            "issuedBy": self.issued_by,
            "issuedAt": stamp(self.issued_at),
            "expiresAt": stamp(self.expires_at),
            "state": self.state.value,
            "constraints": dict(sorted(self.constraints.items())),
            "revokedAt": stamp(self.revoked_at),
            "revokedBy": self.revoked_by,
        }
