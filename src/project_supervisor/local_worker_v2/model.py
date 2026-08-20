from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Mapping
from dataclasses import dataclass
from enum import StrEnum
from typing import Any

PROTOCOL_VERSION = 2
IDEMPOTENCY_KEY_MAX_LENGTH = 256
_IDEMPOTENCY_KEY = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,255}$")


class LaunchState(StrEnum):
    RESERVED = "RESERVED"
    REJECTED_PRE_LAUNCH = "REJECTED_PRE_LAUNCH"
    LAUNCHING = "LAUNCHING"
    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"


class LaunchDisposition(StrEnum):
    DEFINITELY_NOT_LAUNCHED = "DEFINITELY_NOT_LAUNCHED"
    DEFINITELY_LAUNCHED = "DEFINITELY_LAUNCHED"
    LAUNCH_IN_PROGRESS = "LAUNCH_IN_PROGRESS"
    LAUNCH_OUTCOME_UNKNOWN = "LAUNCH_OUTCOME_UNKNOWN"


TERMINAL_STATES = frozenset(
    {
        LaunchState.REJECTED_PRE_LAUNCH,
        LaunchState.COMPLETED,
        LaunchState.FAILED,
        LaunchState.CANCELLED,
    }
)


def validate_idempotency_key(value: object) -> str:
    if not isinstance(value, str) or not _IDEMPOTENCY_KEY.fullmatch(value):
        raise ValueError(
            "idempotency_key must be 1-256 characters using letters, digits, '.', '_', ':', or '-'"
        )
    return value


def canonical_request_document(
    *,
    run_id: str,
    task_id: str | None,
    job: Mapping[str, Any],
    authorization: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    document = {
        "protocol_version": PROTOCOL_VERSION,
        "execution": {"run_id": run_id, "task_id": task_id},
        "job": dict(job),
    }
    if authorization is not None:
        document["authorization"] = dict(authorization)
    return document


def request_digest(
    *,
    run_id: str,
    task_id: str | None,
    job: Mapping[str, Any],
    authorization: Mapping[str, Any] | None = None,
) -> str:
    document = canonical_request_document(
        run_id=run_id,
        task_id=task_id,
        job=job,
        authorization=authorization,
    )
    encoded = json.dumps(
        document,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    return "sha256:" + hashlib.sha256(encoded).hexdigest()


def launch_disposition(state: LaunchState, *, has_started_receipt: bool) -> LaunchDisposition:
    if state in {LaunchState.RESERVED, LaunchState.REJECTED_PRE_LAUNCH}:
        return LaunchDisposition.DEFINITELY_NOT_LAUNCHED
    if state is LaunchState.LAUNCHING and not has_started_receipt:
        return LaunchDisposition.LAUNCH_OUTCOME_UNKNOWN
    if state is LaunchState.LAUNCHING:
        return LaunchDisposition.LAUNCH_IN_PROGRESS
    return LaunchDisposition.DEFINITELY_LAUNCHED


@dataclass(frozen=True, slots=True)
class LaunchRecord:
    launch_record_id: str
    authority_scope: str
    idempotency_key: str
    request_digest: str
    protocol_version: int
    driver_id: str | None
    driver_type: str | None
    driver_profile_revision: int | None
    driver_profile_fingerprint: str | None
    launch_runtime_instance_id: str | None
    launch_state: LaunchState
    job_id: str | None
    receipt_id: str
    launch_nonce: str | None
    process_pid: int | None
    process_birth_identity: str | None
    process_host_identity: str | None
    request_received_at: str
    created_at: str
    updated_at: str
    job_created_at: str | None
    terminal_at: str | None
    launch_error: str | None
    result_json: str | None
    exit_code: int | None

    @property
    def has_started_receipt(self) -> bool:
        return self.process_pid is not None and self.process_birth_identity is not None

    @property
    def disposition(self) -> LaunchDisposition:
        return launch_disposition(
            self.launch_state,
            has_started_receipt=self.has_started_receipt,
        )

    @property
    def terminal(self) -> bool:
        return self.launch_state in TERMINAL_STATES

    def public_data(self, *, replayed: bool = True) -> dict[str, Any]:
        return {
            "accepted": self.launch_state is not LaunchState.REJECTED_PRE_LAUNCH,
            "launch_state": self.launch_state.value,
            "disposition": self.disposition.value,
            "launch_record_id": self.launch_record_id,
            "job_id": self.job_id,
            "receipt_id": self.receipt_id,
            "idempotency_key": self.idempotency_key,
            "request_digest": self.request_digest,
            "protocol_version": self.protocol_version,
            "driver_id": self.driver_id,
            "driver_type": self.driver_type,
            "driver_profile_revision": self.driver_profile_revision,
            "driver_profile_fingerprint": self.driver_profile_fingerprint,
            "launch_runtime_instance_id": self.launch_runtime_instance_id,
            "replayed": replayed,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
            "job_created_at": self.job_created_at,
            "terminal_at": self.terminal_at,
            "launch_error": self.launch_error,
        }
