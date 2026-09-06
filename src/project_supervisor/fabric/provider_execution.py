from __future__ import annotations

import hashlib
import json
import re
import uuid
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from enum import StrEnum
from typing import Any

from project_supervisor.domain import EventSeverity
from project_supervisor.store import StateStore, compact_json, timestamp

from .persistence import FusionRepository, _event

_SEMANTIC_ID = re.compile(r"^[A-Za-z][A-Za-z0-9._:-]{0,159}$")
_TERMINAL_STAGES = frozenset(
    {
        "inferenceCompleted",
        "rejectedBeforeProcess",
        "rejectedBeforeDisclosure",
        "rejectedBeforeInference",
        "failedAfterStart",
        "completed",
        "failed",
        "cancelled",
    }
)


class InvocationStage(StrEnum):
    REQUESTED = "requested"
    DISPATCHED = "dispatched"
    LAUNCHING = "launching"
    ACCEPTED = "accepted"
    PROCESS_STARTED = "processStarted"
    DISCLOSURE_REQUESTED = "disclosureRequested"
    MODEL_USED = "modelUsed"
    DATA_DISCLOSED = "dataDisclosed"
    INFERENCE_STARTED = "inferenceStarted"
    INFERENCE_COMPLETED = "inferenceCompleted"
    REJECTED_BEFORE_PROCESS = "rejectedBeforeProcess"
    REJECTED_BEFORE_DISCLOSURE = "rejectedBeforeDisclosure"
    REJECTED_BEFORE_INFERENCE = "rejectedBeforeInference"
    FAILED_AFTER_START = "failedAfterStart"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"
    OUTCOME_UNKNOWN = "outcomeUnknown"


class TriState(StrEnum):
    YES = "yes"
    NO = "no"
    UNKNOWN = "unknown"


@dataclass(frozen=True, slots=True)
class InvocationProjection:
    invocation_id: str
    stage: InvocationStage
    model_used: TriState
    model: str | None
    data_disclosed: TriState
    terminal: bool
    event_count: int


_ALLOWED_TRANSITIONS: dict[InvocationStage, frozenset[InvocationStage]] = {
    InvocationStage.REQUESTED: frozenset(
        {
            InvocationStage.DISPATCHED,
            InvocationStage.LAUNCHING,
            InvocationStage.ACCEPTED,
            InvocationStage.PROCESS_STARTED,
            InvocationStage.REJECTED_BEFORE_PROCESS,
            InvocationStage.REJECTED_BEFORE_DISCLOSURE,
            InvocationStage.REJECTED_BEFORE_INFERENCE,
            InvocationStage.FAILED,
            InvocationStage.CANCELLED,
            InvocationStage.OUTCOME_UNKNOWN,
        }
    ),
    InvocationStage.DISPATCHED: frozenset(
        {
            InvocationStage.ACCEPTED,
            InvocationStage.PROCESS_STARTED,
            InvocationStage.DISCLOSURE_REQUESTED,
            InvocationStage.DATA_DISCLOSED,
            InvocationStage.INFERENCE_STARTED,
            InvocationStage.INFERENCE_COMPLETED,
            InvocationStage.REJECTED_BEFORE_PROCESS,
            InvocationStage.REJECTED_BEFORE_DISCLOSURE,
            InvocationStage.REJECTED_BEFORE_INFERENCE,
            InvocationStage.FAILED_AFTER_START,
            InvocationStage.RUNNING,
            InvocationStage.COMPLETED,
            InvocationStage.FAILED,
            InvocationStage.CANCELLED,
            InvocationStage.OUTCOME_UNKNOWN,
        }
    ),
    InvocationStage.LAUNCHING: frozenset(
        {
            InvocationStage.ACCEPTED,
            InvocationStage.PROCESS_STARTED,
            InvocationStage.DISCLOSURE_REQUESTED,
            InvocationStage.DATA_DISCLOSED,
            InvocationStage.MODEL_USED,
            InvocationStage.INFERENCE_STARTED,
            InvocationStage.INFERENCE_COMPLETED,
            InvocationStage.REJECTED_BEFORE_PROCESS,
            InvocationStage.REJECTED_BEFORE_DISCLOSURE,
            InvocationStage.REJECTED_BEFORE_INFERENCE,
            InvocationStage.FAILED_AFTER_START,
            InvocationStage.RUNNING,
            InvocationStage.COMPLETED,
            InvocationStage.FAILED,
            InvocationStage.CANCELLED,
            InvocationStage.OUTCOME_UNKNOWN,
        }
    ),
    InvocationStage.ACCEPTED: frozenset(
        {
            InvocationStage.PROCESS_STARTED,
            InvocationStage.DISCLOSURE_REQUESTED,
            InvocationStage.DATA_DISCLOSED,
            InvocationStage.MODEL_USED,
            InvocationStage.INFERENCE_STARTED,
            InvocationStage.INFERENCE_COMPLETED,
            InvocationStage.REJECTED_BEFORE_DISCLOSURE,
            InvocationStage.REJECTED_BEFORE_INFERENCE,
            InvocationStage.FAILED_AFTER_START,
            InvocationStage.RUNNING,
            InvocationStage.COMPLETED,
            InvocationStage.FAILED,
            InvocationStage.CANCELLED,
            InvocationStage.OUTCOME_UNKNOWN,
        }
    ),
    InvocationStage.PROCESS_STARTED: frozenset(
        {
            InvocationStage.DISCLOSURE_REQUESTED,
            InvocationStage.DATA_DISCLOSED,
            InvocationStage.MODEL_USED,
            InvocationStage.INFERENCE_STARTED,
            InvocationStage.INFERENCE_COMPLETED,
            InvocationStage.REJECTED_BEFORE_DISCLOSURE,
            InvocationStage.REJECTED_BEFORE_INFERENCE,
            InvocationStage.FAILED_AFTER_START,
            InvocationStage.RUNNING,
            InvocationStage.COMPLETED,
            InvocationStage.FAILED,
            InvocationStage.CANCELLED,
            InvocationStage.OUTCOME_UNKNOWN,
        }
    ),
    InvocationStage.DISCLOSURE_REQUESTED: frozenset(
        {
            InvocationStage.DATA_DISCLOSED,
            InvocationStage.MODEL_USED,
            InvocationStage.INFERENCE_STARTED,
            InvocationStage.INFERENCE_COMPLETED,
            InvocationStage.REJECTED_BEFORE_DISCLOSURE,
            InvocationStage.REJECTED_BEFORE_INFERENCE,
            InvocationStage.FAILED_AFTER_START,
            InvocationStage.FAILED,
            InvocationStage.CANCELLED,
            InvocationStage.OUTCOME_UNKNOWN,
        }
    ),
    InvocationStage.DATA_DISCLOSED: frozenset(
        {
            InvocationStage.MODEL_USED,
            InvocationStage.INFERENCE_STARTED,
            InvocationStage.INFERENCE_COMPLETED,
            InvocationStage.REJECTED_BEFORE_INFERENCE,
            InvocationStage.FAILED_AFTER_START,
            InvocationStage.RUNNING,
            InvocationStage.COMPLETED,
            InvocationStage.FAILED,
            InvocationStage.CANCELLED,
            InvocationStage.OUTCOME_UNKNOWN,
        }
    ),
    InvocationStage.MODEL_USED: frozenset(
        {
            InvocationStage.MODEL_USED,
            InvocationStage.DATA_DISCLOSED,
            InvocationStage.INFERENCE_STARTED,
            InvocationStage.INFERENCE_COMPLETED,
            InvocationStage.FAILED_AFTER_START,
            InvocationStage.RUNNING,
            InvocationStage.COMPLETED,
            InvocationStage.FAILED,
            InvocationStage.CANCELLED,
            InvocationStage.OUTCOME_UNKNOWN,
        }
    ),
    InvocationStage.INFERENCE_STARTED: frozenset(
        {
            InvocationStage.DATA_DISCLOSED,
            InvocationStage.INFERENCE_COMPLETED,
            InvocationStage.FAILED_AFTER_START,
            InvocationStage.RUNNING,
            InvocationStage.COMPLETED,
            InvocationStage.FAILED,
            InvocationStage.CANCELLED,
            InvocationStage.OUTCOME_UNKNOWN,
        }
    ),
    InvocationStage.RUNNING: frozenset(
        {
            InvocationStage.DATA_DISCLOSED,
            InvocationStage.MODEL_USED,
            InvocationStage.INFERENCE_STARTED,
            InvocationStage.INFERENCE_COMPLETED,
            InvocationStage.FAILED_AFTER_START,
            InvocationStage.COMPLETED,
            InvocationStage.FAILED,
            InvocationStage.CANCELLED,
            InvocationStage.OUTCOME_UNKNOWN,
        }
    ),
    InvocationStage.INFERENCE_COMPLETED: frozenset(),
    InvocationStage.REJECTED_BEFORE_PROCESS: frozenset(),
    InvocationStage.REJECTED_BEFORE_DISCLOSURE: frozenset(),
    InvocationStage.REJECTED_BEFORE_INFERENCE: frozenset(),
    InvocationStage.FAILED_AFTER_START: frozenset(),
    InvocationStage.COMPLETED: frozenset(),
    InvocationStage.FAILED: frozenset(),
    InvocationStage.CANCELLED: frozenset(),
    # Unknown is a durable fail-closed checkpoint, not authority to launch again and not proof
    # that this invocation ended.  Recovery may append later authoritative evidence for the same
    # external invocation identity.
    InvocationStage.OUTCOME_UNKNOWN: frozenset(
        {
            InvocationStage.ACCEPTED,
            InvocationStage.PROCESS_STARTED,
            InvocationStage.DISCLOSURE_REQUESTED,
            InvocationStage.DATA_DISCLOSED,
            InvocationStage.MODEL_USED,
            InvocationStage.INFERENCE_STARTED,
            InvocationStage.INFERENCE_COMPLETED,
            InvocationStage.REJECTED_BEFORE_PROCESS,
            InvocationStage.REJECTED_BEFORE_DISCLOSURE,
            InvocationStage.REJECTED_BEFORE_INFERENCE,
            InvocationStage.FAILED_AFTER_START,
            InvocationStage.RUNNING,
            InvocationStage.COMPLETED,
            InvocationStage.FAILED,
            InvocationStage.CANCELLED,
        }
    ),
}


class ProviderInvocationRepository:
    def __init__(self, store: StateStore) -> None:
        self.store = store

    def create(
        self,
        *,
        run_id: str,
        provider: str,
        requested_model: str | None = None,
        envelope_id: str | None = None,
        idempotency_key: str | None = None,
        invocation_id: str | None = None,
        source: str = "runtime",
    ) -> InvocationProjection:
        for value in (provider, source):
            if not _SEMANTIC_ID.fullmatch(value):
                raise ValueError("provider invocation identities must be semantic IDs")
        if requested_model is not None and (
            not requested_model.strip() or len(requested_model) > 256
        ):
            raise ValueError("requested model must be bounded")
        if idempotency_key is not None and (
            not idempotency_key or len(idempotency_key.encode("utf-8")) > 512
        ):
            raise ValueError("provider invocation idempotency identity must be bounded")
        key_digest = (
            hashlib.sha256(idempotency_key.encode("utf-8")).hexdigest()
            if idempotency_key is not None
            else None
        )
        identity = invocation_id or f"provider-invocation-{uuid.uuid4()}"
        with self.store.transaction() as connection:
            run = connection.execute("SELECT * FROM worker_runs WHERE id=?", (run_id,)).fetchone()
            if run is None:
                raise KeyError(run_id)
            existing = connection.execute(
                "SELECT * FROM provider_invocations_v2 WHERE id=?", (identity,)
            ).fetchone()
            if existing is not None:
                if (
                    existing["run_id"] != run_id
                    or existing["provider"] != provider
                    or existing["requested_model"] != requested_model
                    or existing["envelope_id"] != envelope_id
                    or existing["idempotency_key_sha256"] != key_digest
                ):
                    raise RuntimeError(
                        "provider invocation replay conflicts with immutable identity"
                    )
                return self._projection(connection, identity)
            ordinal = int(
                connection.execute(
                    "SELECT COALESCE(MAX(ordinal),0)+1 AS ordinal "
                    "FROM provider_invocations_v2 WHERE run_id=?",
                    (run_id,),
                ).fetchone()["ordinal"]
            )
            if envelope_id is not None:
                binding = connection.execute(
                    "SELECT envelope.allowed_providers_json FROM authorization_envelope_bindings "
                    "binding JOIN authorization_envelopes envelope "
                    "ON envelope.id=binding.envelope_id "
                    "WHERE binding.envelope_id=? AND binding.task_id=? "
                    "AND (binding.run_id=? OR binding.run_id IS NULL)",
                    (envelope_id, run["task_id"], run_id),
                ).fetchone()
                if binding is None:
                    raise PermissionError("provider invocation requires a bound authorization")
                if provider not in set(json.loads(binding["allowed_providers_json"])):
                    raise PermissionError("provider invocation is outside its authorization")
            now = timestamp()
            connection.execute(
                "INSERT INTO provider_invocations_v2(id,run_id,ordinal,provider,requested_model,"
                "envelope_id,idempotency_key_sha256,created_at) VALUES (?,?,?,?,?,?,?,?)",
                (
                    identity,
                    run_id,
                    ordinal,
                    provider,
                    requested_model,
                    envelope_id,
                    key_digest,
                    now,
                ),
            )
            self._append(
                connection,
                identity,
                stage=InvocationStage.REQUESTED,
                event_key="requested",
                model=None,
                detail_code=None,
                source=source,
                observed_at=now,
            )
            return self._projection(connection, identity)

    def observe(
        self,
        invocation_id: str,
        stage: InvocationStage,
        *,
        event_key: str,
        source: str,
        model: str | None = None,
        detail_code: str | None = None,
        observed_at: datetime | None = None,
    ) -> InvocationProjection:
        if not _SEMANTIC_ID.fullmatch(event_key) or not _SEMANTIC_ID.fullmatch(source):
            raise ValueError("provider event identity and source must be semantic IDs")
        if model is not None and (not model.strip() or len(model) > 256):
            raise ValueError("observed provider model must be bounded")
        if detail_code is not None and not _SEMANTIC_ID.fullmatch(detail_code):
            raise ValueError("provider event detail code must be a semantic ID")
        with self.store.transaction() as connection:
            if (
                connection.execute(
                    "SELECT 1 FROM provider_invocations_v2 WHERE id=?", (invocation_id,)
                ).fetchone()
                is None
            ):
                raise KeyError(invocation_id)
            existing = connection.execute(
                "SELECT * FROM provider_invocation_events_v2 WHERE invocation_id=? AND event_key=?",
                (invocation_id, event_key),
            ).fetchone()
            when = timestamp(observed_at or datetime.now(UTC))
            if existing is not None:
                # WorkerEvent has no provider-stable observation ID. For cumulative boundary
                # facts, event_key + semantic fields are the durable identity; reconnects often
                # re-emit the same fact with a fresh adapter timestamp. Compare using the first
                # canonical observation time so the retry clock cannot manufacture a conflict.
                when = str(existing["observed_at"])
            event_value = {
                "invocationID": invocation_id,
                "eventKey": event_key,
                "stage": stage.value,
                "model": model,
                "detailCode": detail_code,
                "source": source,
                "observedAt": when,
            }
            digest = hashlib.sha256(compact_json(event_value).encode("utf-8")).hexdigest()
            if existing is not None:
                if existing["event_sha256"] != digest:
                    raise RuntimeError("provider event replay conflicts with immutable observation")
                return self._projection(connection, invocation_id)
            current = self._projection(connection, invocation_id)
            if current.terminal:
                raise ValueError("terminal provider invocation cannot accept another observation")
            if stage not in _ALLOWED_TRANSITIONS[current.stage]:
                raise ValueError(
                    f"invalid provider invocation transition {current.stage.value} -> {stage.value}"
                )
            self._append(
                connection,
                invocation_id,
                stage=stage,
                event_key=event_key,
                model=model,
                detail_code=detail_code,
                source=source,
                observed_at=when,
                expected_digest=digest,
            )
            return self._projection(connection, invocation_id)

    def get(self, invocation_id: str) -> InvocationProjection:
        with self.store.connect() as connection:
            if (
                connection.execute(
                    "SELECT 1 FROM provider_invocations_v2 WHERE id=?", (invocation_id,)
                ).fetchone()
                is None
            ):
                raise KeyError(invocation_id)
            return self._projection(connection, invocation_id)

    def list(self, *, run_id: str | None = None) -> list[dict[str, Any]]:
        with self.store.connect() as connection:
            if run_id is None:
                rows = connection.execute(
                    "SELECT * FROM provider_invocations_v2 ORDER BY created_at,id"
                ).fetchall()
            else:
                rows = connection.execute(
                    "SELECT * FROM provider_invocations_v2 WHERE run_id=? ORDER BY ordinal",
                    (run_id,),
                ).fetchall()
            return [
                {
                    "invocationID": row["id"],
                    "runID": row["run_id"],
                    "ordinal": int(row["ordinal"]),
                    "provider": row["provider"],
                    "requestedModel": row["requested_model"],
                    "authorizationID": row["envelope_id"],
                    "projection": self._projection(connection, row["id"]),
                    "createdAt": row["created_at"],
                }
                for row in rows
            ]

    def _append(
        self,
        connection: Any,
        invocation_id: str,
        *,
        stage: InvocationStage,
        event_key: str,
        model: str | None,
        detail_code: str | None,
        source: str,
        observed_at: str,
        expected_digest: str | None = None,
    ) -> None:
        current = self._projection(connection, invocation_id, allow_empty=True)
        model_state = current.model_used
        model_used = current.model
        disclosure_state = current.data_disclosed
        inference_evidence = stage in {
            InvocationStage.MODEL_USED,
            InvocationStage.INFERENCE_STARTED,
            InvocationStage.INFERENCE_COMPLETED,
        }
        disclosure_evidence = stage is InvocationStage.DATA_DISCLOSED
        pre_process_rejection = (
            stage is InvocationStage.REJECTED_BEFORE_PROCESS
            or detail_code == "rejectedBeforeProcess"
        )
        pre_disclosure_rejection = (
            stage is InvocationStage.REJECTED_BEFORE_DISCLOSURE
            or detail_code == "rejectedBeforeDisclosure"
        )
        pre_inference_rejection = (
            stage is InvocationStage.REJECTED_BEFORE_INFERENCE
            or detail_code == "rejectedBeforeInference"
        )
        prior_stages = {
            InvocationStage(row["stage"])
            for row in connection.execute(
                "SELECT stage FROM provider_invocation_events_v2 WHERE invocation_id=?",
                (invocation_id,),
            ).fetchall()
        }
        process_evidence = prior_stages.intersection(
            {
                InvocationStage.PROCESS_STARTED,
                InvocationStage.DISCLOSURE_REQUESTED,
                InvocationStage.DATA_DISCLOSED,
                InvocationStage.MODEL_USED,
                InvocationStage.INFERENCE_STARTED,
                InvocationStage.INFERENCE_COMPLETED,
                InvocationStage.FAILED_AFTER_START,
                InvocationStage.RUNNING,
                InvocationStage.COMPLETED,
            }
        )
        if pre_process_rejection and process_evidence:
            raise ValueError("pre-process rejection contradicts prior process evidence")
        if pre_disclosure_rejection and disclosure_state is TriState.YES:
            raise ValueError("pre-disclosure rejection contradicts proven disclosure")
        if inference_evidence:
            if model_state is TriState.NO:
                raise ValueError("provider inference evidence contradicts a proven non-use")
            model_state = TriState.YES
            model_used = model_used or model
        elif pre_process_rejection or pre_disclosure_rejection or pre_inference_rejection:
            if model_state is TriState.YES:
                raise ValueError("provider pre-inference rejection contradicts proven model use")
            model_state = TriState.NO
        if disclosure_evidence:
            if disclosure_state is TriState.NO:
                raise ValueError("provider disclosure evidence contradicts proven non-disclosure")
            disclosure_state = TriState.YES
        elif pre_process_rejection or pre_disclosure_rejection:
            if disclosure_state is TriState.YES:
                raise ValueError("provider pre-disclosure rejection contradicts proven disclosure")
            disclosure_state = TriState.NO
        event_value = {
            "invocationID": invocation_id,
            "eventKey": event_key,
            "stage": stage.value,
            "model": model,
            "detailCode": detail_code,
            "source": source,
            "observedAt": observed_at,
        }
        digest = hashlib.sha256(compact_json(event_value).encode("utf-8")).hexdigest()
        if expected_digest is not None and digest != expected_digest:
            raise RuntimeError("provider event digest changed before persistence")
        ordinal = current.event_count + 1
        identity = f"provider-invocation-event-{uuid.uuid4()}"
        connection.execute(
            "INSERT INTO provider_invocation_events_v2(id,invocation_id,ordinal,event_key,"
            "event_sha256,stage,model_used_state,model_used,data_disclosed_state,detail_code,"
            "observed_at,source,created_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                identity,
                invocation_id,
                ordinal,
                event_key,
                digest,
                stage.value,
                model_state.value,
                model_used,
                disclosure_state.value,
                detail_code,
                observed_at,
                source,
                timestamp(),
            ),
        )
        root = connection.execute(
            "SELECT invocation.run_id,run.task_id,run.worker_id,task.project_id "
            "FROM provider_invocations_v2 invocation "
            "JOIN worker_runs run ON run.id=invocation.run_id "
            "JOIN tasks task ON task.id=run.task_id WHERE invocation.id=?",
            (invocation_id,),
        ).fetchone()
        kind = {
            InvocationStage.REQUESTED: "providerInvocationRequested",
            InvocationStage.PROCESS_STARTED: "providerProcessStarted",
            InvocationStage.MODEL_USED: "providerModelUsed",
            InvocationStage.INFERENCE_STARTED: "providerInferenceStarted",
            InvocationStage.INFERENCE_COMPLETED: "providerInferenceCompleted",
            InvocationStage.DATA_DISCLOSED: "providerDataDisclosed",
            InvocationStage.REJECTED_BEFORE_PROCESS: "providerInvocationRejected",
            InvocationStage.REJECTED_BEFORE_DISCLOSURE: "providerInvocationRejected",
            InvocationStage.REJECTED_BEFORE_INFERENCE: "providerInvocationRejected",
            InvocationStage.COMPLETED: "providerInvocationCompleted",
            InvocationStage.FAILED: "providerInvocationFailed",
            InvocationStage.CANCELLED: "providerInvocationCancelled",
        }.get(stage, "providerInvocationObserved")
        _event(
            self.store,
            connection,
            kind=kind,
            entity_type="providerInvocation",
            entity_id=invocation_id,
            project_id=root["project_id"],
            task_id=root["task_id"],
            worker_id=root["worker_id"],
            run_id=root["run_id"],
            summary=f"Provider invocation stage {stage.value}",
            payload={
                "invocationID": invocation_id,
                "stage": stage.value,
                "modelUsed": model_state.value,
                "dataDisclosed": disclosure_state.value,
                "detailCode": detail_code,
            },
            actor=source,
            severity=(
                EventSeverity.WARNING
                if stage in {InvocationStage.FAILED, InvocationStage.OUTCOME_UNKNOWN}
                else EventSeverity.INFO
            ),
        )

    @staticmethod
    def _projection(
        connection: Any, invocation_id: str, *, allow_empty: bool = False
    ) -> InvocationProjection:
        row = connection.execute(
            "SELECT * FROM provider_invocation_events_v2 WHERE invocation_id=? "
            "ORDER BY ordinal DESC LIMIT 1",
            (invocation_id,),
        ).fetchone()
        if row is None:
            if not allow_empty:
                raise RuntimeError("provider invocation has no lifecycle observation")
            return InvocationProjection(
                invocation_id,
                InvocationStage.REQUESTED,
                TriState.UNKNOWN,
                None,
                TriState.UNKNOWN,
                False,
                0,
            )
        return InvocationProjection(
            invocation_id=invocation_id,
            stage=InvocationStage(row["stage"]),
            model_used=TriState(row["model_used_state"]),
            model=row["model_used"],
            data_disclosed=TriState(row["data_disclosed_state"]),
            terminal=row["stage"] in _TERMINAL_STAGES,
            event_count=int(row["ordinal"]),
        )


class ProviderCapacityRepository:
    def __init__(self, store: StateStore) -> None:
        self.store = store

    def configure_pool(
        self,
        *,
        pool_id: str,
        display_name: str,
        max_concurrency: int,
        worker_ids: tuple[str, ...],
        policy_version: str = "provider-capacity.v1",
    ) -> dict[str, Any]:
        for value in (pool_id, policy_version):
            if not _SEMANTIC_ID.fullmatch(value):
                raise ValueError("provider pool identities must be semantic IDs")
        if not display_name.strip() or len(display_name) > 160:
            raise ValueError("provider pool display name must be bounded")
        if not 1 <= max_concurrency <= 128:
            raise ValueError("provider pool concurrency must be between one and 128")
        if not worker_ids or len(set(worker_ids)) != len(worker_ids):
            raise ValueError("provider pool Workers must be unique and non-empty")
        now = timestamp()
        with self.store.transaction() as connection:
            existing = connection.execute(
                "SELECT * FROM provider_capacity_pools WHERE id=?", (pool_id,)
            ).fetchone()
            if existing is None:
                connection.execute(
                    "INSERT INTO provider_capacity_pools(id,display_name,max_concurrency,"
                    "policy_version,enabled,created_at,updated_at) VALUES (?,?,?,?,1,?,?)",
                    (pool_id, display_name, max_concurrency, policy_version, now, now),
                )
            elif (
                existing["display_name"] != display_name
                or int(existing["max_concurrency"]) != max_concurrency
                or existing["policy_version"] != policy_version
            ):
                raise RuntimeError("provider pool identity is immutable in this protocol version")
            for worker_id in worker_ids:
                if (
                    connection.execute("SELECT 1 FROM workers WHERE id=?", (worker_id,)).fetchone()
                    is None
                ):
                    raise KeyError(worker_id)
                current = connection.execute(
                    "SELECT pool_id FROM provider_capacity_pool_workers WHERE worker_id=?",
                    (worker_id,),
                ).fetchone()
                if current is not None and current["pool_id"] != pool_id:
                    raise RuntimeError("Worker is already bound to another provider capacity pool")
                connection.execute(
                    "INSERT OR IGNORE INTO provider_capacity_pool_workers(pool_id,worker_id,"
                    "created_at) VALUES (?,?,?)",
                    (pool_id, worker_id, now),
                )
            return {
                "poolID": pool_id,
                "maxConcurrency": max_concurrency,
                "workerIDs": list(worker_ids),
                "policyVersion": policy_version,
            }

    def release_run(self, run_id: str, *, reason: str) -> bool:
        if not _SEMANTIC_ID.fullmatch(reason):
            raise ValueError("provider capacity release reason must be a semantic ID")
        with self.store.transaction() as connection:
            cursor = connection.execute(
                "UPDATE provider_capacity_reservations SET state='released',released_at=?,"
                "release_reason=? WHERE run_id=? AND state='reserved'",
                (timestamp(), reason, run_id),
            )
        return cursor.rowcount == 1


class AnalysisResultRepository:
    """Deterministically normalize a pinned JSON result before it can enter fusion."""

    def __init__(self, store: StateStore) -> None:
        self.store = store

    def normalize(self, run_id: str) -> dict[str, Any]:
        with self.store.transaction() as connection:
            row = FusionRepository._source_result_row(connection, run_id)
            if row is None:
                raise ValueError("analysis normalization requires an immutable Worker result")
            result_sha = FusionRepository._source_result_sha256(row)
            existing = connection.execute(
                "SELECT * FROM analysis_result_contributions WHERE run_id=?", (run_id,)
            ).fetchone()
            try:
                value = json.loads(row["summary"])
            except json.JSONDecodeError as error:
                raise ValueError("analysis result summary must be canonical JSON") from error
            if not isinstance(value, dict) or set(value) != {"schemaVersion", "claims", "evidence"}:
                raise ValueError(
                    "analysis result must contain only schemaVersion, claims, evidence"
                )
            if value["schemaVersion"] != "analysis-contribution/v1":
                raise ValueError("unsupported analysis result schema")
            if not isinstance(value["claims"], dict) or not isinstance(value["evidence"], dict):
                raise ValueError("analysis claims and evidence must be objects")
            normalized = {
                "schemaVersion": value["schemaVersion"],
                "claims": value["claims"],
                "evidence": value["evidence"],
            }
            encoded = compact_json(normalized).encode("utf-8")
            if len(encoded) > 262_144:
                raise ValueError("normalized analysis result exceeds its byte limit")
            normalized_sha = hashlib.sha256(encoded).hexdigest()
            if existing is not None:
                if (
                    existing["result_sha256"] != result_sha
                    or existing["normalized_sha256"] != normalized_sha
                ):
                    raise RuntimeError(
                        "analysis normalization replay conflicts with immutable data"
                    )
                return self._projection(existing)
            identity = f"analysis-contribution-{uuid.uuid4()}"
            connection.execute(
                "INSERT INTO analysis_result_contributions(id,run_id,result_sha256,schema_version,"
                "claims_json,evidence_json,normalized_sha256,created_at) VALUES (?,?,?,?,?,?,?,?)",
                (
                    identity,
                    run_id,
                    result_sha,
                    "analysis-contribution/v1",
                    compact_json(value["claims"]),
                    compact_json(value["evidence"]),
                    normalized_sha,
                    timestamp(),
                ),
            )
            return self._projection(
                connection.execute(
                    "SELECT * FROM analysis_result_contributions WHERE id=?", (identity,)
                ).fetchone()
            )

    def normalized_attempt(self, task_id: str) -> list[dict[str, Any]]:
        with self.store.connect() as connection:
            task = connection.execute(
                "SELECT attempt_count FROM tasks WHERE id=?", (task_id,)
            ).fetchone()
            if task is None:
                raise KeyError(task_id)
            rows = connection.execute(
                "SELECT contribution.* FROM analysis_result_contributions contribution "
                "JOIN worker_runs run ON run.id=contribution.run_id "
                "WHERE run.task_id=? AND run.attempt=? ORDER BY run.worker_id,run.id",
                (task_id, int(task["attempt_count"])),
            ).fetchall()
            run_count = connection.execute(
                "SELECT COUNT(*) AS count FROM worker_runs WHERE task_id=? AND attempt=?",
                (task_id, int(task["attempt_count"])),
            ).fetchone()["count"]
            if len(rows) != int(run_count):
                raise ValueError("every current-attempt Worker result must be normalized")
            return [self._projection(row) for row in rows]

    @staticmethod
    def _projection(row: Any) -> dict[str, Any]:
        return {
            "contributionID": row["id"],
            "runID": row["run_id"],
            "resultSHA256": row["result_sha256"],
            "schemaVersion": row["schema_version"],
            "claims": json.loads(row["claims_json"]),
            "evidence": json.loads(row["evidence_json"]),
            "normalizedSHA256": row["normalized_sha256"],
            "createdAt": row["created_at"],
        }


class ProjectWriteAuthorityRepository:
    """Operator-pinned single-writer authority with explicit, non-stealable leases."""

    def __init__(self, store: StateStore) -> None:
        self.store = store

    def configure(
        self,
        *,
        project_id: str,
        owner_kind: str,
        owner_id: str,
        envelope_id: str,
        configured_by: str,
        node_id: str | None = None,
    ) -> dict[str, Any]:
        if owner_kind not in {"worker", "externalManagedExecutor"}:
            raise ValueError("project write owner kind is invalid")
        for value in (project_id, owner_id, envelope_id, configured_by):
            if not _SEMANTIC_ID.fullmatch(value):
                raise ValueError("project write authority identities must be semantic IDs")
        with self.store.transaction() as connection:
            envelope = connection.execute(
                "SELECT * FROM authorization_envelopes WHERE id=?", (envelope_id,)
            ).fetchone()
            if envelope is None or envelope["project_id"] != project_id:
                raise PermissionError("project write authority requires a project authorization")
            allowed_actions = set(json.loads(envelope["allowed_action_classes_json"]))
            denied_actions = set(json.loads(envelope["denied_action_classes_json"]))
            if "code.write" not in allowed_actions or "code.write" in denied_actions:
                raise PermissionError("project write authority requires explicit code.write")
            if node_id is not None and (
                connection.execute("SELECT 1 FROM nodes WHERE id=?", (node_id,)).fetchone() is None
            ):
                raise KeyError(node_id)
            if owner_kind == "worker":
                worker = connection.execute(
                    "SELECT node_id,code_write_allowed FROM workers WHERE id=?", (owner_id,)
                ).fetchone()
                if worker is None or not bool(worker["code_write_allowed"]):
                    raise PermissionError("project write Worker is not write-capable")
                if node_id is not None and worker["node_id"] != node_id:
                    raise PermissionError("project write Worker does not belong to its Node")
            existing = connection.execute(
                "SELECT * FROM project_write_authorities WHERE project_id=?", (project_id,)
            ).fetchone()
            if existing is not None:
                expected = (owner_kind, owner_id, node_id, envelope_id, "active")
                actual = (
                    existing["owner_kind"],
                    existing["owner_id"],
                    existing["node_id"],
                    existing["envelope_id"],
                    existing["state"],
                )
                if actual != expected:
                    raise RuntimeError("project write authority is immutable")
                return dict(existing)
            now = timestamp()
            connection.execute(
                "INSERT INTO project_write_authorities(project_id,owner_kind,owner_id,node_id,"
                "envelope_id,state,generation,configured_by,created_at) "
                "VALUES (?,?,?,?,?,'active',1,?,?)",
                (project_id, owner_kind, owner_id, node_id, envelope_id, configured_by, now),
            )
            _event(
                self.store,
                connection,
                kind="projectWriteAuthorityBound",
                entity_type="projectWriteAuthority",
                entity_id=project_id,
                project_id=project_id,
                summary="Project single-write owner bound by operator authority",
                payload={
                    "projectID": project_id,
                    "ownerKind": owner_kind,
                    "ownerID": owner_id,
                    "generation": 1,
                },
                actor=configured_by,
            )
            return dict(
                connection.execute(
                    "SELECT * FROM project_write_authorities WHERE project_id=?", (project_id,)
                ).fetchone()
            )

    def acquire(
        self,
        *,
        project_id: str,
        owner_id: str,
        ttl_seconds: float,
        task_id: str | None = None,
        run_id: str | None = None,
    ) -> dict[str, Any] | None:
        if not 1 <= ttl_seconds <= 3600:
            raise ValueError("project write lease TTL must be between one and 3600 seconds")
        now_value = datetime.now(UTC)
        now = timestamp(now_value)
        with self.store.transaction() as connection:
            authority = connection.execute(
                "SELECT * FROM project_write_authorities WHERE project_id=? AND state='active'",
                (project_id,),
            ).fetchone()
            if authority is None or authority["owner_id"] != owner_id:
                raise PermissionError("caller is not the canonical project write owner")
            # Expiry alone never authorizes takeover.  An active row must be explicitly released
            # after the owning executor is proven quiescent.
            active = connection.execute(
                "SELECT 1 FROM project_write_leases WHERE project_id=? AND state='active'",
                (project_id,),
            ).fetchone()
            if active is not None:
                return None
            generation = int(
                connection.execute(
                    "SELECT COALESCE(MAX(generation),0)+1 AS value FROM project_write_leases "
                    "WHERE project_id=?",
                    (project_id,),
                ).fetchone()["value"]
            )
            identity = f"project-write-lease-{uuid.uuid4()}"
            expires = timestamp(now_value + timedelta(seconds=ttl_seconds))
            connection.execute(
                "INSERT INTO project_write_leases(id,project_id,owner_id,task_id,run_id,"
                "generation,state,acquired_at,expires_at,released_at,release_reason) "
                "VALUES (?,?,?,?,?,?,'active',?,?,NULL,NULL)",
                (identity, project_id, owner_id, task_id, run_id, generation, now, expires),
            )
            _event(
                self.store,
                connection,
                kind="projectWriteLeaseAcquired",
                entity_type="projectWriteLease",
                entity_id=identity,
                project_id=project_id,
                task_id=task_id,
                run_id=run_id,
                summary="Canonical project write owner acquired its bounded lease",
                payload={"ownerID": owner_id, "generation": generation, "expiresAt": expires},
            )
            return dict(
                connection.execute(
                    "SELECT * FROM project_write_leases WHERE id=?", (identity,)
                ).fetchone()
            )

    def release(self, lease_id: str, *, owner_id: str, reason: str) -> bool:
        if not _SEMANTIC_ID.fullmatch(reason):
            raise ValueError("project write lease release reason must be semantic")
        with self.store.transaction() as connection:
            cursor = connection.execute(
                "UPDATE project_write_leases SET state='released',released_at=?,release_reason=? "
                "WHERE id=? AND owner_id=? AND state='active'",
                (timestamp(), reason, lease_id, owner_id),
            )
            return cursor.rowcount == 1

    @staticmethod
    def require_current(connection: Any, *, project_id: str, owner_id: str, lease_id: str) -> Any:
        row = connection.execute(
            "SELECT lease.*,authority.owner_kind,authority.envelope_id "
            "FROM project_write_leases lease JOIN project_write_authorities authority "
            "ON authority.project_id=lease.project_id WHERE lease.id=? AND lease.project_id=? "
            "AND lease.owner_id=? AND lease.state='active' AND authority.state='active' "
            "AND authority.owner_id=lease.owner_id",
            (lease_id, project_id, owner_id),
        ).fetchone()
        if row is None:
            raise PermissionError("project write lease is not current for the canonical owner")
        if datetime.fromisoformat(str(row["expires_at"]).replace("Z", "+00:00")) <= datetime.now(
            UTC
        ):
            raise PermissionError("project write lease expired; explicit quiescence is required")
        return row


class HypothesisRepository:
    def __init__(self, store: StateStore) -> None:
        self.store = store

    def from_fusion(self, fusion_id: str) -> dict[str, Any]:
        with self.store.transaction() as connection:
            fusion = connection.execute(
                "SELECT * FROM result_fusion_decisions WHERE id=?", (fusion_id,)
            ).fetchone()
            if fusion is None:
                raise KeyError(fusion_id)
            conflicts = json.loads(fusion["conflicts_json"])
            if not isinstance(conflicts, list) or not conflicts:
                raise ValueError("hypothesis arbitration requires a contradictory fusion")
            input_value = {
                "fusionID": fusion_id,
                "inputSetSHA256": fusion["input_set_sha256"],
                "conflicts": conflicts,
            }
            input_sha = hashlib.sha256(compact_json(input_value).encode("utf-8")).hexdigest()
            existing = connection.execute(
                "SELECT * FROM hypothesis_sets WHERE task_id=? AND source_attempt=? "
                "AND input_sha256=?",
                (fusion["task_id"], int(fusion["source_attempt"]), input_sha),
            ).fetchone()
            if existing is not None:
                return self._set_projection(connection, existing)
            set_id = f"hypothesis-set-{uuid.uuid4()}"
            now = timestamp()
            connection.execute(
                "INSERT INTO hypothesis_sets(id,task_id,source_attempt,fusion_id,state,"
                "input_sha256,created_at) VALUES (?,?,?,?,?,?,?)",
                (
                    set_id,
                    fusion["task_id"],
                    int(fusion["source_attempt"]),
                    fusion_id,
                    "experimentRequired",
                    input_sha,
                    now,
                ),
            )
            for conflict in conflicts:
                claim_key = conflict.get("claimKey")
                variants = conflict.get("variants")
                if not isinstance(claim_key, str) or not isinstance(variants, list):
                    raise ValueError("fusion conflict shape is invalid")
                for variant in variants:
                    value = variant.get("value")
                    value_sha = hashlib.sha256(compact_json(value).encode("utf-8")).hexdigest()
                    connection.execute(
                        "INSERT INTO hypotheses(id,set_id,claim_key,value_json,value_sha256,"
                        "provenance_json,confidence,state,created_at) VALUES (?,?,?,?,?,?,NULL,"
                        "'untested',?)",
                        (
                            f"hypothesis-{uuid.uuid4()}",
                            set_id,
                            claim_key,
                            compact_json(value),
                            value_sha,
                            compact_json(variant.get("provenance", [])),
                            now,
                        ),
                    )
            _event(
                self.store,
                connection,
                kind="hypothesisSetCreated",
                entity_type="hypothesisSet",
                entity_id=set_id,
                task_id=fusion["task_id"],
                summary="Contradictory fusion preserved as explicit hypotheses",
                payload={"hypothesisSetID": set_id, "fusionID": fusion_id},
            )
            return self._set_projection(
                connection,
                connection.execute(
                    "SELECT * FROM hypothesis_sets WHERE id=?", (set_id,)
                ).fetchone(),
            )

    def propose_experiment(
        self,
        *,
        set_id: str,
        proposal_key: str,
        operation: str,
        specification: Mapping[str, Any],
        risk_class: str,
        expected_information_gain: float | None,
        envelope_id: str | None,
    ) -> dict[str, Any]:
        if not _SEMANTIC_ID.fullmatch(proposal_key) or not _SEMANTIC_ID.fullmatch(operation):
            raise ValueError("experiment identities must be semantic IDs")
        forbidden = {"command", "argv", "executable", "env", "cwd", "shell"}
        if forbidden.intersection(specification):
            raise ValueError("experiments cannot carry arbitrary process authority")
        if risk_class not in {"green", "yellow", "red"}:
            raise ValueError("experiment risk class is invalid")
        if expected_information_gain is not None and not 0 <= expected_information_gain <= 1:
            raise ValueError("expected information gain must be between zero and one")
        safe = compact_json(dict(specification))
        if len(safe.encode("utf-8")) > 65_536:
            raise ValueError("experiment specification exceeds its byte limit")
        with self.store.transaction() as connection:
            hypothesis_set = connection.execute(
                "SELECT * FROM hypothesis_sets WHERE id=?", (set_id,)
            ).fetchone()
            if hypothesis_set is None:
                raise KeyError(set_id)
            if envelope_id is None:
                raise PermissionError("experiment proposal requires explicit authorization")
            envelope = connection.execute(
                "SELECT envelope.* FROM authorization_envelopes envelope "
                "JOIN tasks task ON task.project_id=envelope.project_id "
                "WHERE envelope.id=? AND task.id=?",
                (envelope_id, hypothesis_set["task_id"]),
            ).fetchone()
            if envelope is None:
                raise PermissionError("experiment authorization does not cover its Task project")
            allowed_actions = set(json.loads(envelope["allowed_action_classes_json"]))
            denied_actions = set(json.loads(envelope["denied_action_classes_json"]))
            if "experiment.run" not in allowed_actions or "experiment.run" in denied_actions:
                raise PermissionError("authorization does not permit experiment.run")
            if envelope["user_approval_state"] not in {"notRequired", "approved"} or envelope[
                "platform_approval_state"
            ] not in {"notRequired", "approved"}:
                raise PermissionError("experiment authorization is not executable")
            if datetime.fromisoformat(
                str(envelope["expires_at"]).replace("Z", "+00:00")
            ) <= datetime.now(UTC):
                raise PermissionError("experiment authorization expired")
            hypothesis_ids = {
                str(row["id"])
                for row in connection.execute(
                    "SELECT id FROM hypotheses WHERE set_id=?", (set_id,)
                ).fetchall()
            }
            predicates = specification.get("hypothesisPredicates")
            if not isinstance(predicates, list) or len(predicates) < 2:
                raise ValueError("experiment must distinguish at least two hypotheses")
            referenced: set[str] = set()
            for predicate in predicates:
                if not isinstance(predicate, dict) or set(predicate) != {
                    "hypothesisID",
                    "observationKey",
                    "operator",
                    "expected",
                }:
                    raise ValueError("experiment hypothesis predicate shape is invalid")
                hypothesis_id = predicate["hypothesisID"]
                observation_key = predicate["observationKey"]
                if hypothesis_id not in hypothesis_ids:
                    raise ValueError("experiment predicate references another hypothesis set")
                if not isinstance(observation_key, str) or not _SEMANTIC_ID.fullmatch(
                    observation_key
                ):
                    raise ValueError("experiment observation key must be semantic")
                if predicate["operator"] != "equals":
                    raise ValueError("experiment predicate operator is unsupported")
                referenced.add(str(hypothesis_id))
            if len(referenced) < 2:
                raise ValueError("experiment must distinguish at least two hypotheses")
            existing = connection.execute(
                "SELECT * FROM experiment_proposals WHERE set_id=? AND proposal_key=?",
                (set_id, proposal_key),
            ).fetchone()
            if existing is not None:
                if (
                    existing["operation"] != operation
                    or existing["specification_json"] != safe
                    or existing["envelope_id"] != envelope_id
                ):
                    raise RuntimeError("experiment proposal key conflicts with prior definition")
                return dict(existing)
            identity = f"experiment-proposal-{uuid.uuid4()}"
            connection.execute(
                "INSERT INTO experiment_proposals(id,set_id,proposal_key,operation,"
                "specification_json,risk_class,expected_information_gain,envelope_id,state,"
                "created_at) VALUES (?,?,?,?,?,?,?,?,?,?)",
                (
                    identity,
                    set_id,
                    proposal_key,
                    operation,
                    safe,
                    risk_class,
                    expected_information_gain,
                    envelope_id,
                    "proposed",
                    timestamp(),
                ),
            )
            return dict(
                connection.execute(
                    "SELECT * FROM experiment_proposals WHERE id=?", (identity,)
                ).fetchone()
            )

    def handoff_to_single_writer(
        self,
        *,
        proposal_id: str,
        target_worker_id: str,
        instruction: Mapping[str, Any],
        single_write_owner: bool,
    ) -> dict[str, Any]:
        del proposal_id, target_worker_id, instruction, single_write_owner
        raise PermissionError(
            "legacy boolean single-writer claims are not authority; use a typed project lease"
        )

    def handoff_to_primary_executor(
        self,
        *,
        proposal_id: str,
        project_id: str,
        owner_id: str,
        write_lease_id: str,
        content_kind: str,
        instruction: Mapping[str, Any],
        expires_at: datetime,
    ) -> dict[str, Any]:
        if content_kind not in {
            "analysisPacket",
            "reviewFinding",
            "hypothesisSet",
            "experimentProposal",
        }:
            raise ValueError("structured executor handoff content kind is invalid")
        if expires_at.tzinfo is None or expires_at <= datetime.now(UTC):
            raise ValueError("structured executor handoff requires a future expiry")
        encoded = compact_json(dict(instruction)).encode("utf-8")
        if len(encoded) > 65_536:
            raise ValueError("experiment handoff exceeds its byte limit")
        digest = hashlib.sha256(encoded).hexdigest()
        with self.store.transaction() as connection:
            proposal = connection.execute(
                "SELECT proposal.*,task.project_id FROM experiment_proposals proposal "
                "JOIN hypothesis_sets set_row ON set_row.id=proposal.set_id "
                "JOIN tasks task ON task.id=set_row.task_id WHERE proposal.id=?",
                (proposal_id,),
            ).fetchone()
            if proposal is None:
                raise KeyError(proposal_id)
            if proposal["project_id"] != project_id:
                raise PermissionError("experiment handoff cannot cross project authority")
            lease = ProjectWriteAuthorityRepository.require_current(
                connection,
                project_id=project_id,
                owner_id=owner_id,
                lease_id=write_lease_id,
            )
            if proposal["envelope_id"] != lease["envelope_id"]:
                raise PermissionError("experiment and single-writer authorization do not match")
            if expires_at > datetime.fromisoformat(str(lease["expires_at"]).replace("Z", "+00:00")):
                raise PermissionError("experiment handoff cannot outlive its write lease")
            existing = connection.execute(
                "SELECT * FROM structured_executor_handoffs WHERE proposal_id=? AND owner_id=?",
                (proposal_id, owner_id),
            ).fetchone()
            if existing is not None:
                if (
                    existing["content_sha256"] != digest
                    or existing["write_lease_id"] != write_lease_id
                    or existing["content_kind"] != content_kind
                ):
                    raise RuntimeError("structured handoff conflicts with immutable prior content")
                return dict(existing)
            identity = f"pc-codex-handoff-{uuid.uuid4()}"
            state = (
                "awaitingExternalExecutor"
                if lease["owner_kind"] == "externalManagedExecutor"
                else "ready"
            )
            connection.execute(
                "INSERT INTO structured_executor_handoffs(id,schema_version,proposal_id,project_id,"
                "owner_kind,owner_id,write_lease_id,envelope_id,content_kind,content_sha256,"
                "expected_result_schema,state,expires_at,created_at) "
                "VALUES (?,'pc-codex-handoff/v1',?,?,?,?,?,?,?,?,'experiment-result/v1',?,?,?)",
                (
                    identity,
                    proposal_id,
                    project_id,
                    lease["owner_kind"],
                    owner_id,
                    write_lease_id,
                    lease["envelope_id"],
                    content_kind,
                    digest,
                    state,
                    timestamp(expires_at),
                    timestamp(),
                ),
            )
            _event(
                self.store,
                connection,
                kind="experimentHandoffCreated",
                entity_type="structuredExecutorHandoff",
                entity_id=identity,
                project_id=project_id,
                task_id=lease["task_id"],
                summary="Typed experiment handed to the canonical project write owner",
                payload={
                    "handoffID": identity,
                    "proposalID": proposal_id,
                    "ownerKind": lease["owner_kind"],
                    "ownerID": owner_id,
                    "contentKind": content_kind,
                    "state": state,
                },
            )
            return dict(
                connection.execute(
                    "SELECT * FROM structured_executor_handoffs WHERE id=?", (identity,)
                ).fetchone()
            )

    def record_experiment_result(
        self,
        *,
        handoff_id: str,
        executor_id: str,
        result: Mapping[str, Any],
        source_run_id: str | None = None,
    ) -> dict[str, Any]:
        value = dict(result)
        if set(value) != {"schemaVersion", "proposalID", "status", "observations"}:
            raise ValueError("experiment result shape is invalid")
        if value["schemaVersion"] != "experiment-result/v1":
            raise ValueError("experiment result schema is unsupported")
        if value["status"] not in {"completed", "failed", "inconclusive"}:
            raise ValueError("experiment result status is invalid")
        if not isinstance(value["observations"], dict):
            raise ValueError("experiment observations must be an object")
        encoded = compact_json(value).encode("utf-8")
        if len(encoded) > 65_536:
            raise ValueError("experiment result exceeds its byte limit")
        digest = hashlib.sha256(encoded).hexdigest()
        with self.store.transaction() as connection:
            handoff = connection.execute(
                "SELECT * FROM structured_executor_handoffs WHERE id=?", (handoff_id,)
            ).fetchone()
            if handoff is None:
                raise KeyError(handoff_id)
            if handoff["owner_id"] != executor_id or handoff["proposal_id"] != value["proposalID"]:
                raise PermissionError("experiment result does not match its typed handoff")
            if datetime.fromisoformat(
                str(handoff["expires_at"]).replace("Z", "+00:00")
            ) <= datetime.now(UTC):
                raise PermissionError("experiment handoff expired")
            ProjectWriteAuthorityRepository.require_current(
                connection,
                project_id=handoff["project_id"],
                owner_id=executor_id,
                lease_id=handoff["write_lease_id"],
            )
            if handoff["owner_kind"] == "worker":
                run = connection.execute(
                    "SELECT run.worker_id,result.run_id AS result_id FROM worker_runs run "
                    "LEFT JOIN worker_results result ON result.run_id=run.id WHERE run.id=?",
                    (source_run_id,),
                ).fetchone()
                if run is None or run["worker_id"] != executor_id or run["result_id"] is None:
                    raise PermissionError("Worker experiment result is not canonical")
            prior = connection.execute(
                "SELECT * FROM experiment_results WHERE handoff_id=? ORDER BY created_at LIMIT 1",
                (handoff_id,),
            ).fetchone()
            if prior is not None:
                if prior["result_sha256"] != digest:
                    raise RuntimeError("experiment handoff already has a different result")
                return self._result_projection(connection, prior)
            result_id = f"experiment-result-{uuid.uuid4()}"
            now = timestamp()
            connection.execute(
                "INSERT INTO experiment_results(id,handoff_id,proposal_id,executor_id,"
                "source_run_id,result_sha256,schema_version,status,observations_json,created_at) "
                "VALUES (?,?,?,?,?,?,'experiment-result/v1',?,?,?)",
                (
                    result_id,
                    handoff_id,
                    handoff["proposal_id"],
                    executor_id,
                    source_run_id,
                    digest,
                    value["status"],
                    compact_json(value["observations"]),
                    now,
                ),
            )
            proposal = connection.execute(
                "SELECT specification_json FROM experiment_proposals WHERE id=?",
                (handoff["proposal_id"],),
            ).fetchone()
            predicates = json.loads(proposal["specification_json"])["hypothesisPredicates"]
            observations = value["observations"]
            for predicate in predicates:
                observed = observations.get(predicate["observationKey"])
                if (
                    value["status"] != "completed"
                    or predicate["observationKey"] not in observations
                ):
                    relation = "inconclusive"
                elif observed == predicate["expected"]:
                    relation = "supports"
                else:
                    relation = "contradicts"
                predicate_sha = hashlib.sha256(compact_json(predicate).encode("utf-8")).hexdigest()
                connection.execute(
                    "INSERT INTO hypothesis_assessments(id,hypothesis_id,experiment_result_id,"
                    "relation,predicate_sha256,created_at) VALUES (?,?,?,?,?,?)",
                    (
                        f"hypothesis-assessment-{uuid.uuid4()}",
                        predicate["hypothesisID"],
                        result_id,
                        relation,
                        predicate_sha,
                        now,
                    ),
                )
            projection = self._result_projection(
                connection,
                connection.execute(
                    "SELECT * FROM experiment_results WHERE id=?", (result_id,)
                ).fetchone(),
            )
            _event(
                self.store,
                connection,
                kind="experimentResolved",
                entity_type="experimentResult",
                entity_id=result_id,
                project_id=handoff["project_id"],
                summary="Canonical experiment evidence assessed competing hypotheses",
                payload={
                    "resultID": result_id,
                    "proposalID": handoff["proposal_id"],
                    "hypothesisState": projection["hypothesisState"],
                },
            )
            return projection

    @staticmethod
    def _result_projection(connection: Any, row: Any) -> dict[str, Any]:
        assessments = connection.execute(
            "SELECT hypothesis_id,relation,predicate_sha256 FROM hypothesis_assessments "
            "WHERE experiment_result_id=? ORDER BY hypothesis_id",
            (row["id"],),
        ).fetchall()
        relations = [str(item["relation"]) for item in assessments]
        state = (
            "resolved"
            if relations.count("supports") == 1
            and relations.count("contradicts") == len(relations) - 1
            else "inconclusive"
        )
        return {
            "resultID": row["id"],
            "handoffID": row["handoff_id"],
            "proposalID": row["proposal_id"],
            "executorID": row["executor_id"],
            "resultSHA256": row["result_sha256"],
            "status": row["status"],
            "observations": json.loads(row["observations_json"]),
            "assessments": [
                {
                    "hypothesisID": item["hypothesis_id"],
                    "relation": item["relation"],
                    "predicateSHA256": item["predicate_sha256"],
                }
                for item in assessments
            ],
            "hypothesisState": state,
        }

    @staticmethod
    def _set_projection(connection: Any, row: Any) -> dict[str, Any]:
        hypotheses = connection.execute(
            "SELECT * FROM hypotheses WHERE set_id=? ORDER BY claim_key,value_sha256,id",
            (row["id"],),
        ).fetchall()
        return {
            "hypothesisSetID": row["id"],
            "taskID": row["task_id"],
            "sourceAttempt": int(row["source_attempt"]),
            "fusionID": row["fusion_id"],
            "state": row["state"],
            "hypotheses": [
                {
                    "hypothesisID": hypothesis["id"],
                    "claimKey": hypothesis["claim_key"],
                    "value": json.loads(hypothesis["value_json"]),
                    "valueSHA256": hypothesis["value_sha256"],
                    "provenance": json.loads(hypothesis["provenance_json"]),
                    "state": hypothesis["state"],
                }
                for hypothesis in hypotheses
            ],
            "createdAt": row["created_at"],
        }
