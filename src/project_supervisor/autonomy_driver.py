from __future__ import annotations

import asyncio
import hashlib
import json
import os
import shutil
import uuid
from collections.abc import Mapping
from pathlib import Path
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, ValidationError, model_validator

from .adapters import (
    AgyAdapter,
    ClaudeAdapter,
    CodexAdapter,
    GrokAdapter,
    LocalWorkerAdapter,
    MockAdapter,
)
from .adapters.base import WorkerAdapter
from .adapters.mock import MockBehavior
from .autonomy import (
    ActionResult,
    EvaluationDisposition,
    GoalContext,
    GoalEvaluation,
    GoalPlan,
    GoalVerification,
    PlannedAction,
    TerminationReason,
)
from .domain import (
    EventSeverity,
    ExecutionTopology,
    Harness,
    PermissionClass,
    Provider,
    TaskLabel,
    TaskRequirements,
    TaskState,
    utc_now,
)
from .runtime import AdapterRegistry, SupervisorRuntime
from .store import StateStore, compact_json, redact_sensitive, timestamp

DECISION_SCHEMA_VERSION = "autonomy-decision/v1"
_DECISION_NAMESPACE = uuid.UUID("dd0addcb-8f42-5e85-a62e-040c54b3c8ce")
_PHASES = frozenset({"evaluation", "planning", "verification"})
_ACTION_ROLES = frozenset(
    {"primary", "reviewer", "panelist", "router", "verifier", "planner", "fallback", "other"}
)
_PAYLOAD_KEYS = frozenset(
    {
        "topology",
        "priority",
        "permissionClass",
        "labels",
        "requiredCapabilities",
        "minimumContextTokens",
        "privacySensitive",
        "codeWriteRequired",
        "panelSize",
        "preferredWorkers",
    }
)
_PROVIDER_FOR_HARNESS = {
    Harness.CODEX: Provider.OPENAI,
    Harness.CLAUDE_CODE: Provider.ANTHROPIC,
    Harness.GROK_BUILD: Provider.XAI,
    Harness.GOOGLE_AGY: Provider.GOOGLE,
    Harness.LOCAL_WORKER: Provider.LOCAL,
    Harness.MOCK: Provider.MOCK,
}


class DecisionDriverError(RuntimeError):
    """A canonical decision task could not be executed or decoded safely."""


class AdapterReconstructionError(RuntimeError):
    """Persisted Worker metadata cannot be reconstructed into a safe adapter."""


class _StrictDecision(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True, str_strip_whitespace=True)

    schema_version: Literal[DECISION_SCHEMA_VERSION] = Field(alias="schemaVersion")
    goal_id: str = Field(alias="goalID", min_length=1, max_length=200)
    iteration_sequence: int = Field(alias="iterationSequence", ge=1)
    steer_version: int = Field(alias="steerVersion", ge=0)


class EvaluationDecision(_StrictDecision):
    decision_type: Literal["evaluation"] = Field(alias="decisionType")
    disposition: Literal["incomplete", "satisfied", "terminate"]
    summary: str = Field(min_length=1, max_length=4000)
    progress_fingerprint: str = Field(alias="progressFingerprint", min_length=1, max_length=256)
    termination_reason: str | None = Field(default=None, alias="terminationReason")
    facts: dict[str, Any] = Field(default_factory=dict)

    @model_validator(mode="after")
    def validate_termination(self) -> EvaluationDecision:
        allowed = {item.value for item in TerminationReason} - {TerminationReason.SUCCESS.value}
        if self.disposition == "terminate" and self.termination_reason not in allowed:
            raise ValueError("terminate evaluation requires an allowed non-success reason")
        if self.disposition != "terminate" and self.termination_reason is not None:
            raise ValueError("terminationReason is only allowed for terminate evaluation")
        return self


class ActionPayload(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)

    topology: Literal[
        "single", "primaryReviewer", "parallelPanel", "cheapFirstEscalation", "fallback"
    ] = "single"
    priority: int = Field(default=50, ge=0, le=100)
    permission_class: Literal["green", "yellow", "red"] = Field(
        default="green", alias="permissionClass"
    )
    labels: list[
        Literal[
            "coding",
            "architecture",
            "research",
            "fastRouting",
            "rag",
            "review",
            "creative",
            "longContext",
            "privacySensitive",
            "highUncertainty",
        ]
    ] = Field(default_factory=lambda: ["research"], min_length=1, max_length=8)
    required_capabilities: list[str] = Field(
        default_factory=list, alias="requiredCapabilities", max_length=16
    )
    minimum_context_tokens: int | None = Field(default=None, alias="minimumContextTokens", gt=0)
    privacy_sensitive: bool = Field(default=False, alias="privacySensitive")
    code_write_required: bool = Field(default=False, alias="codeWriteRequired")
    panel_size: int = Field(default=2, alias="panelSize", ge=1, le=8)
    preferred_workers: list[str] = Field(
        default_factory=list, alias="preferredWorkers", max_length=16
    )

    @model_validator(mode="after")
    def validate_strings(self) -> ActionPayload:
        for group in (self.required_capabilities, self.preferred_workers):
            if any(not item.strip() or len(item) > 200 for item in group):
                raise ValueError(
                    "capability and Worker identifiers must be bounded non-empty strings"
                )
            if len(group) != len(set(group)):
                raise ValueError("capability and Worker identifiers must be unique")
        if len(self.labels) != len(set(self.labels)):
            raise ValueError("labels must be unique")
        return self


class PlannedActionDecision(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True, str_strip_whitespace=True)

    key: str = Field(min_length=1, max_length=120, pattern=r"^[A-Za-z0-9][A-Za-z0-9._-]*$")
    title: str = Field(min_length=1, max_length=300)
    description: str = Field(min_length=1, max_length=12_000)
    role: str = Field(default="primary", min_length=1, max_length=40)
    payload: ActionPayload = Field(default_factory=ActionPayload)

    @model_validator(mode="after")
    def validate_role(self) -> PlannedActionDecision:
        if self.role not in _ACTION_ROLES:
            raise ValueError("action role is not in the provider-neutral allowlist")
        return self


class PlanDecision(_StrictDecision):
    decision_type: Literal["plan"] = Field(alias="decisionType")
    summary: str = Field(min_length=1, max_length=4000)
    rationale: str = Field(default="", max_length=8000)
    actions: list[PlannedActionDecision] = Field(min_length=1, max_length=8)

    @model_validator(mode="after")
    def validate_action_keys(self) -> PlanDecision:
        keys = [action.key for action in self.actions]
        if len(keys) != len(set(keys)):
            raise ValueError("planned action keys must be unique")
        return self


class VerificationDecision(_StrictDecision):
    decision_type: Literal["verification"] = Field(alias="decisionType")
    satisfied: bool
    summary: str = Field(min_length=1, max_length=4000)
    progress_fingerprint: str = Field(alias="progressFingerprint", min_length=1, max_length=256)
    termination_reason: str | None = Field(default=None, alias="terminationReason")
    evidence: dict[str, Any] = Field(default_factory=dict)

    @model_validator(mode="after")
    def validate_termination(self) -> VerificationDecision:
        allowed = {item.value for item in TerminationReason}
        if self.termination_reason is not None and self.termination_reason not in allowed:
            raise ValueError("terminationReason is not allowed")
        if self.satisfied and self.termination_reason not in {
            None,
            TerminationReason.SUCCESS.value,
        }:
            raise ValueError("satisfied verification cannot carry a failure reason")
        if not self.satisfied and self.termination_reason == TerminationReason.SUCCESS.value:
            raise ValueError("incomplete verification cannot carry SUCCESS")
        return self


class ProductionAutonomyDriver:
    """Make autonomy decisions through canonical Hybrid Engine Tasks.

    This class implements ``GoalEvaluator``, ``GoalPlanner`` and ``GoalVerifier``. It never calls a
    provider directly. Each decision is an idempotently named Task submitted to the supplied
    ``SupervisorRuntime`` and permanently bound to its Goal checkpoint by migration 0007.
    """

    def __init__(
        self,
        runtime: SupervisorRuntime,
        *,
        max_context_bytes: int = 64 * 1024,
        max_output_bytes: int = 32 * 1024,
        max_actions: int = 8,
    ) -> None:
        if max_context_bytes < 4096:
            raise ValueError("max_context_bytes must be at least 4096")
        if max_output_bytes < 1024:
            raise ValueError("max_output_bytes must be at least 1024")
        if not 1 <= max_actions <= 8:
            raise ValueError("max_actions must be between 1 and 8")
        self.runtime = runtime
        self.store = runtime.store
        self.max_context_bytes = max_context_bytes
        self.max_output_bytes = max_output_bytes
        self.max_actions = max_actions

    async def evaluate(self, context: GoalContext) -> GoalEvaluation:
        checkpoint = self._checkpoint(context, "evaluation")
        try:
            value, output_hash = await self._decide(context, checkpoint, extra={})
            decision = EvaluationDecision.model_validate(value)
            self._require_binding(decision, checkpoint)
            self._accept_decision(checkpoint, output_hash)
            current_steer = self._current_steer(checkpoint.goal_id)
            if decision.disposition == "satisfied" and current_steer != checkpoint.steer_version:
                return GoalEvaluation(
                    EvaluationDisposition.INCOMPLETE,
                    "A newer human steer arrived; the Goal must be evaluated again",
                    f"steer-pending-{current_steer}",
                    facts={
                        "observedSteerVersion": checkpoint.steer_version,
                        "currentSteerVersion": current_steer,
                    },
                )
            return GoalEvaluation(
                EvaluationDisposition(decision.disposition),
                decision.summary,
                decision.progress_fingerprint,
                termination_reason=(
                    TerminationReason(decision.termination_reason)
                    if decision.termination_reason
                    else None
                ),
                facts=_safe_object(decision.facts),
            )
        except (DecisionDriverError, ValidationError, ValueError, TypeError, KeyError) as error:
            self._reject_decision(checkpoint, error)
            return GoalEvaluation(
                EvaluationDisposition.TERMINATE,
                _safe_failure("Evaluator decision failed closed", error),
                self._failure_fingerprint(checkpoint, error),
                termination_reason=TerminationReason.HUMAN_ESCALATION,
            )

    async def plan(self, context: GoalContext, evaluation: GoalEvaluation) -> GoalPlan:
        checkpoint = self._checkpoint(context, "planning")
        try:
            value, output_hash = await self._decide(
                context,
                checkpoint,
                extra={
                    "evaluation": {
                        "disposition": evaluation.disposition.value,
                        "summary": evaluation.summary,
                        "progressFingerprint": evaluation.progress_fingerprint,
                        "terminationReason": (
                            evaluation.termination_reason.value
                            if evaluation.termination_reason
                            else None
                        ),
                        "facts": _safe_object(evaluation.facts),
                    },
                    "workerInventory": self._planning_worker_inventory(),
                },
            )
            decision = PlanDecision.model_validate(value)
            self._require_binding(decision, checkpoint)
            if len(decision.actions) > self.max_actions:
                raise DecisionDriverError("plan exceeds the configured action limit")
            self._accept_decision(checkpoint, output_hash)
            if self._current_steer(checkpoint.goal_id) != checkpoint.steer_version:
                return GoalPlan(
                    "Plan checkpoint became stale after a human steer",
                    (),
                    "The engine will enter its safe NO_PROGRESS path instead of "
                    "dispatching stale work.",
                )
            actions = tuple(
                PlannedAction(
                    key=item.key,
                    title=item.title,
                    description=item.description,
                    role=item.role,
                    payload=item.payload.model_dump(by_alias=True),
                )
                for item in decision.actions
            )
            return GoalPlan(decision.summary, actions, decision.rationale)
        except (DecisionDriverError, ValidationError, ValueError, TypeError, KeyError) as error:
            self._reject_decision(checkpoint, error)
            # The core already maps an empty plan for an incomplete Goal to durable NO_PROGRESS.
            return GoalPlan(_safe_failure("Planner decision unavailable", error), ())

    async def verify(
        self, context: GoalContext, results: tuple[ActionResult, ...]
    ) -> GoalVerification:
        checkpoint = self._checkpoint(context, "verification")
        try:
            value, output_hash = await self._decide(
                context,
                checkpoint,
                extra={
                    "actionResults": [
                        {
                            "succeeded": result.succeeded,
                            "cancelled": result.cancelled,
                            "summary": _bounded_string(result.summary, 4000),
                            "payload": _safe_object(result.payload),
                        }
                        for result in results[: self.max_actions]
                    ]
                },
            )
            decision = VerificationDecision.model_validate(value)
            self._require_binding(decision, checkpoint)
            self._accept_decision(checkpoint, output_hash)
            current_steer = self._current_steer(checkpoint.goal_id)
            if decision.satisfied and current_steer != checkpoint.steer_version:
                return GoalVerification(
                    False,
                    "A newer human steer arrived before success; re-evaluation is required",
                    f"steer-pending-{current_steer}",
                    evidence={
                        "observedSteerVersion": checkpoint.steer_version,
                        "currentSteerVersion": current_steer,
                    },
                )
            return GoalVerification(
                decision.satisfied,
                decision.summary,
                decision.progress_fingerprint,
                termination_reason=(
                    TerminationReason(decision.termination_reason)
                    if decision.termination_reason
                    else None
                ),
                evidence=_safe_object(decision.evidence),
            )
        except (DecisionDriverError, ValidationError, ValueError, TypeError, KeyError) as error:
            self._reject_decision(checkpoint, error)
            return GoalVerification(
                False,
                _safe_failure("Verifier decision failed closed", error),
                self._failure_fingerprint(checkpoint, error),
                termination_reason=TerminationReason.HUMAN_ESCALATION,
            )

    async def _decide(
        self,
        context: GoalContext,
        checkpoint: _DecisionCheckpoint,
        *,
        extra: dict[str, Any],
    ) -> tuple[dict[str, Any], str]:
        context_value = self._context_value(context)
        request = {
            "schemaVersion": DECISION_SCHEMA_VERSION,
            "decisionType": checkpoint.output_type,
            "goalID": checkpoint.goal_id,
            "iterationSequence": checkpoint.iteration_sequence,
            "steerVersion": checkpoint.steer_version,
            "context": context_value,
            **extra,
        }
        request_json = _bounded_json(request, self.max_context_bytes)
        fingerprint = hashlib.sha256(request_json.encode()).hexdigest()
        task_id = self._task_id(checkpoint)
        prompt = self._prompt(checkpoint, request_json)
        await self._ensure_task(checkpoint, task_id, prompt, fingerprint)
        output = await self._execute_task(task_id)
        if len(output.encode()) > self.max_output_bytes:
            self._finish_binding(task_id, "rejected", None, "output exceeds byte limit")
            self._finish_task(task_id, accepted=False)
            raise DecisionDriverError("decision output exceeds the configured byte limit")
        try:
            value = json.loads(output)
        except json.JSONDecodeError as error:
            self._finish_binding(task_id, "rejected", None, "output is not strict JSON")
            self._finish_task(task_id, accepted=False)
            raise DecisionDriverError("decision output is not one strict JSON object") from error
        if not isinstance(value, dict):
            self._finish_binding(task_id, "rejected", None, "output is not an object")
            self._finish_task(task_id, accepted=False)
            raise DecisionDriverError("decision output must be a JSON object")
        output_hash = hashlib.sha256(output.encode()).hexdigest()
        value = self._normalize_progress_fingerprint(value, checkpoint, output_hash)
        value = self._normalize_satisfied_evaluation_success_reason(value, checkpoint, output_hash)
        return value, output_hash

    def _normalize_satisfied_evaluation_success_reason(
        self,
        value: dict[str, Any],
        checkpoint: _DecisionCheckpoint,
        raw_output_hash: str,
    ) -> dict[str, Any]:
        """Remove only the exact redundant success reason from a satisfied evaluation."""

        if (
            checkpoint.output_type != "evaluation"
            or value.get("disposition") != "satisfied"
            or value.get("terminationReason") != TerminationReason.SUCCESS.value
        ):
            return value
        normalized_value = dict(value)
        normalized_value["terminationReason"] = None
        self._record_decision_normalization(
            checkpoint,
            raw_output_hash=raw_output_hash,
            payload={
                "field": "terminationReason",
                "method": "satisfied-success-implies-null",
            },
        )
        return normalized_value

    def _normalize_progress_fingerprint(
        self,
        value: dict[str, Any],
        checkpoint: _DecisionCheckpoint,
        raw_output_hash: str,
    ) -> dict[str, Any]:
        """Bound only an overlong evaluator/verifier progress identity.

        Provider responses remain strict: absent, empty, whitespace-only, or non-string values are
        left untouched so Pydantic rejects them. The accepted decision binding continues to retain
        the SHA-256 of the exact raw Worker output; the derived fingerprint hashes the exact UTF-8
        decoded field value and never journals that value itself.
        """

        if checkpoint.output_type not in {"evaluation", "verification"}:
            return value
        original = value.get("progressFingerprint")
        if not isinstance(original, str) or not original.strip() or len(original) <= 256:
            return value

        original_hash = hashlib.sha256(original.encode("utf-8")).hexdigest()
        normalized = f"sha256:{original_hash}"
        normalized_value = dict(value)
        normalized_value["progressFingerprint"] = normalized
        self._record_decision_normalization(
            checkpoint,
            raw_output_hash=raw_output_hash,
            payload={
                "field": "progressFingerprint",
                "method": "sha256-exact-utf8",
                "originalCharacterCount": len(original),
                "originalUTF8ByteCount": len(original.encode("utf-8")),
                "originalSha256": original_hash,
                "normalizedFingerprint": normalized,
            },
        )
        return normalized_value

    def _record_decision_normalization(
        self,
        checkpoint: _DecisionCheckpoint,
        *,
        raw_output_hash: str,
        payload: dict[str, Any],
    ) -> None:
        task_id = self._task_id(checkpoint)
        event_payload = {
            "schemaVersion": DECISION_SCHEMA_VERSION,
            "phase": checkpoint.phase,
            "rawOutputSha256": raw_output_hash,
            **payload,
        }
        with self.store.transaction() as connection:
            previous = connection.execute(
                "SELECT payload_json FROM events WHERE task_id=? AND kind=? ORDER BY sequence DESC",
                (task_id, "autonomyDecisionNormalized"),
            ).fetchall()
            for row in previous:
                try:
                    prior = json.loads(row["payload_json"])
                    if (
                        prior.get("rawOutputSha256") == raw_output_hash
                        and prior.get("field") == event_payload["field"]
                        and prior.get("method") == event_payload["method"]
                    ):
                        return
                except (json.JSONDecodeError, AttributeError):
                    continue
            self.store._append_event(
                connection,
                kind="autonomyDecisionNormalized",
                severity=EventSeverity.NOTICE,
                entity_type="autonomyDecision",
                entity_id=task_id,
                project_id=checkpoint.project_id,
                task_id=task_id,
                summary="Autonomy decision field normalized deterministically",
                payload=event_payload,
                actor="autonomy-decision-driver",
            )

    async def _ensure_task(
        self,
        checkpoint: _DecisionCheckpoint,
        task_id: str,
        prompt: str,
        input_hash: str,
    ) -> None:
        try:
            task = await asyncio.to_thread(self.store.get_task, task_id)
            if task["project_id"] != checkpoint.project_id:
                raise DecisionDriverError("deterministic decision Task identity collision")
        except KeyError:
            await self.runtime.submit_task(
                project_id=checkpoint.project_id,
                title=f"Autonomy {checkpoint.phase} decision",
                description=prompt,
                requirements=TaskRequirements(
                    labels=frozenset({TaskLabel.RESEARCH, TaskLabel.REVIEW}),
                    required_capabilities=frozenset(),
                    permission_class=PermissionClass.GREEN,
                    privacy_sensitive=True,
                    code_write_required=False,
                ),
                topology=ExecutionTopology.SINGLE,
                priority=90,
                task_id=task_id,
                reference=(
                    f"DECISION-{task_id[-12:]}-{checkpoint.iteration_sequence}-"
                    f"{checkpoint.phase.upper()}-{checkpoint.steer_version}"
                ),
            )
        with self.store.transaction() as connection:
            existing = connection.execute(
                "SELECT * FROM autonomy_decision_tasks WHERE task_id=?", (task_id,)
            ).fetchone()
            if existing is None:
                connection.execute(
                    "INSERT INTO autonomy_decision_tasks("
                    "task_id,goal_id,iteration_id,iteration_sequence,phase,steer_version,"
                    "schema_version,input_sha256,status,created_at,updated_at"
                    ") VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                    (
                        task_id,
                        checkpoint.goal_id,
                        checkpoint.iteration_id,
                        checkpoint.iteration_sequence,
                        checkpoint.phase,
                        checkpoint.steer_version,
                        DECISION_SCHEMA_VERSION,
                        input_hash,
                        "pending",
                        timestamp(),
                        timestamp(),
                    ),
                )
            else:
                expected = (
                    checkpoint.goal_id,
                    checkpoint.iteration_id,
                    checkpoint.iteration_sequence,
                    checkpoint.phase,
                    checkpoint.steer_version,
                    DECISION_SCHEMA_VERSION,
                )
                actual = tuple(
                    existing[key]
                    for key in (
                        "goal_id",
                        "iteration_id",
                        "iteration_sequence",
                        "phase",
                        "steer_version",
                        "schema_version",
                    )
                )
                if actual != expected:
                    raise DecisionDriverError("decision Task binding does not match its checkpoint")

    async def _execute_task(self, task_id: str) -> str:
        task = await asyncio.to_thread(self.store.get_task, task_id)
        if task["state"] in {
            TaskState.DRAFT.value,
            TaskState.QUEUED.value,
            TaskState.READY.value,
            TaskState.RUNNING.value,
            TaskState.WAITING.value,
            TaskState.INTERRUPTED.value,
        }:
            scope = {task_id}
            await self.runtime.recover(task_ids=scope)
            while True:
                await self.runtime.recover(task_ids=scope)
                await self.runtime.dispatch_ready(task_ids=scope)
                await self.runtime.wait_for_active(task_ids=scope)
                task = await asyncio.to_thread(self.store.get_task, task_id)
                if task["state"] not in {
                    TaskState.READY.value,
                    TaskState.RUNNING.value,
                    TaskState.WAITING.value,
                    TaskState.INTERRUPTED.value,
                }:
                    break
                # A foreign Runtime may own the Worker/Task lease, or a crashed owner's lease may
                # not have expired yet. Poll recovery and capacity at a bounded rate.
                await self.runtime.wait_for_dispatch_capacity(timeout_seconds=1.0)
            task = await asyncio.to_thread(self.store.get_task, task_id)
        if task["state"] not in {TaskState.REVIEWING.value, TaskState.SUCCEEDED.value}:
            raise DecisionDriverError(f"decision Task ended in {task['state']}")
        with self.store.connect() as connection:
            rows = connection.execute(
                "SELECT result.summary FROM worker_results result "
                "JOIN worker_runs run ON run.id=result.run_id "
                "WHERE run.task_id=? ORDER BY run.attempt DESC,result.created_at DESC",
                (task_id,),
            ).fetchall()
        if not rows:
            raise DecisionDriverError("decision Task has no Worker result")
        return str(rows[0]["summary"]).strip()

    def _accept_decision(self, checkpoint: _DecisionCheckpoint, output_hash: str) -> None:
        task_id = self._task_id(checkpoint)
        self._finish_binding(task_id, "accepted", output_hash, None)
        self._finish_task(task_id, accepted=True)

    def _reject_decision(self, checkpoint: _DecisionCheckpoint, error: Exception) -> None:
        task_id = self._task_id(checkpoint)
        with self.store.connect() as connection:
            exists = connection.execute(
                "SELECT 1 FROM autonomy_decision_tasks WHERE task_id=?", (task_id,)
            ).fetchone()
        if exists is None:
            return
        self._finish_binding(
            task_id,
            "rejected",
            None,
            _safe_failure("decision validation rejected", error),
        )
        self._finish_task(task_id, accepted=False)

    def _finish_task(self, task_id: str, *, accepted: bool) -> None:
        task = self.store.get_task(task_id)
        if task["state"] != TaskState.REVIEWING.value:
            return
        self.store.transition_task(
            task_id,
            TaskState.SUCCEEDED if accepted else TaskState.FAILED,
            actor="autonomy-decision-driver",
            summary=(
                "Strict autonomy decision captured"
                if accepted
                else "Autonomy decision rejected by strict validation"
            ),
        )
        task = self.store.get_task(task_id)
        self.runtime.resource_usage.audit_task_terminal(
            task_id=task_id,
            run_id=None,
            terminal_state=task["state"],
            audit_key=f"task:{task_id}:version:{task['version']}:state:{task['state']}",
        )

    def _finish_binding(
        self, task_id: str, status: str, output_hash: str | None, error: str | None
    ) -> None:
        with self.store.transaction() as connection:
            connection.execute(
                "UPDATE autonomy_decision_tasks SET status=?,"
                "output_sha256=COALESCE(?,output_sha256),"
                "error=COALESCE(error,?),completed_at=COALESCE(completed_at,?),updated_at=? "
                "WHERE task_id=?",
                (
                    status,
                    output_hash,
                    _bounded_string(str(redact_sensitive(error)), 1000) if error else None,
                    timestamp(),
                    timestamp(),
                    task_id,
                ),
            )

    def _checkpoint(self, context: GoalContext, phase: str) -> _DecisionCheckpoint:
        if phase not in _PHASES:
            raise ValueError(f"unsupported decision phase: {phase}")
        if not context.iterations:
            raise DecisionDriverError("decision requires an active persisted Goal iteration")
        iteration = context.iterations[-1]
        return _DecisionCheckpoint(
            goal_id=str(context.goal["id"]),
            project_id=str(context.goal["project_id"]),
            iteration_id=str(iteration["id"]),
            iteration_sequence=int(iteration["sequence"]),
            steer_version=int(context.goal["steer_version"]),
            phase=phase,
        )

    def _context_value(self, context: GoalContext) -> dict[str, Any]:
        decision_task_ids = self._decision_task_ids(context.goal["id"])
        return _safe_object(
            {
                "goal": {
                    key: context.goal.get(key)
                    for key in (
                        "id",
                        "project_id",
                        "intent",
                        "effective_intent",
                        "state",
                        "termination_reason",
                        "iteration_count",
                        "no_progress_count",
                        "task_count",
                        "failure_count",
                        "budgets",
                        "steer_version",
                    )
                },
                "project": {
                    key: context.project.get(key) for key in ("id", "name", "goal", "phase")
                },
                "tasks": [
                    _select(
                        item, ("id", "title", "description", "state", "topology", "failure_reason")
                    )
                    for item in context.tasks
                    if item.get("id") not in decision_task_ids
                ][-40:],
                "events": [
                    _select(
                        item,
                        (
                            "sequence",
                            "kind",
                            "entity_type",
                            "entity_id",
                            "task_id",
                            "summary",
                            "payload",
                            "created_at",
                        ),
                    )
                    for item in context.events
                    if item.get("task_id") not in decision_task_ids
                ][-80:],
                "iterations": [
                    _select(
                        item,
                        (
                            "id",
                            "sequence",
                            "state",
                            "progress_fingerprint",
                            "evaluation_json",
                            "plan_json",
                            "verification_json",
                        ),
                    )
                    for item in context.iterations
                ][-12:],
                "actions": [
                    _select(
                        item,
                        (
                            "id",
                            "iteration_id",
                            "action_key",
                            "title",
                            "role",
                            "state",
                            "task_id",
                            "result_json",
                            "error",
                        ),
                    )
                    for item in context.actions
                ][-48:],
                "steers": [
                    _select(
                        item,
                        (
                            "sequence",
                            "instruction",
                            "priority",
                            "preserve_valid_work",
                            "actor",
                            "created_at",
                        ),
                    )
                    for item in context.steers
                ][-20:],
                "workerResults": [
                    _select(
                        item,
                        (
                            "run_id",
                            "summary",
                            "blockers_json",
                            "recommended_next_actions_json",
                            "created_at",
                        ),
                    )
                    for item in context.worker_results
                    if self._run_task_id(item.get("run_id")) not in decision_task_ids
                ][-40:],
                "verifications": [
                    _select(
                        item,
                        ("task_id", "kind", "passed", "evidence_json", "verifier", "created_at"),
                    )
                    for item in context.verifications
                    if item.get("task_id") not in decision_task_ids
                ][-40:],
                "failures": [
                    _select(
                        item, ("task_id", "classification", "summary", "retryable", "created_at")
                    )
                    for item in context.failures
                    if item.get("task_id") not in decision_task_ids
                ][-40:],
            }
        )

    def _decision_task_ids(self, goal_id: str) -> set[str]:
        with self.store.connect() as connection:
            return {
                str(row["task_id"])
                for row in connection.execute(
                    "SELECT task_id FROM autonomy_decision_tasks WHERE goal_id=?", (goal_id,)
                ).fetchall()
            }

    def _run_task_id(self, run_id: Any) -> str | None:
        if not isinstance(run_id, str):
            return None
        with self.store.connect() as connection:
            row = connection.execute(
                "SELECT task_id FROM worker_runs WHERE id=?", (run_id,)
            ).fetchone()
        return str(row["task_id"]) if row else None

    def _planning_worker_inventory(self) -> list[dict[str, Any]]:
        """Return a secret-free canonical capability snapshot for planning.

        A provider-neutral planner must not invent capabilities that no registered Worker can
        satisfy.  This snapshot is advisory input; the deterministic scheduler remains the final
        authority and re-evaluates health, permissions, privacy, and availability at dispatch.
        """

        inventory: list[dict[str, Any]] = []
        observed_at = utc_now()
        for worker in self.store.worker_snapshots():
            if worker.manifest_schema_version is None:
                manifest_status = "legacy"
            elif (
                worker.manifest_valid_until is not None
                and worker.manifest_valid_until <= observed_at
            ):
                manifest_status = "expired"
            else:
                manifest_status = "current"
            capabilities = sorted(worker.capabilities) if manifest_status != "expired" else []
            inventory.append(
                {
                    "workerID": worker.id,
                    "provider": worker.provider.value,
                    "model": _bounded_string(str(redact_sensitive(worker.model.identifier)), 200),
                    "state": worker.state.value,
                    "nodeState": worker.node_state.value,
                    "resourceState": worker.resource_state.value,
                    "capabilities": capabilities,
                    "codeWriteAllowed": worker.code_write_allowed,
                    "privacyAllowed": worker.privacy_allowed,
                    "manifestStatus": manifest_status,
                    "manifestSchemaVersion": worker.manifest_schema_version,
                    "capabilityCatalogVersion": worker.capability_catalog_version,
                    "manifestValidUntil": (
                        worker.manifest_valid_until.isoformat().replace("+00:00", "Z")
                        if worker.manifest_valid_until is not None
                        else None
                    ),
                    "health": worker.health.value,
                    "healthFreshness": worker.health_freshness.value,
                    "quotaState": worker.quota_state.value,
                    "quotaFreshness": worker.quota_freshness.value,
                    "subscriptionState": worker.subscription_state.value,
                    "workerLoad": worker.worker_load,
                    "runningTasks": worker.running_tasks,
                    "maxConcurrency": worker.max_concurrency,
                    "locality": worker.locality.value,
                    "privacy": worker.privacy.value,
                    "costMode": worker.cost_mode.value,
                }
            )
        return inventory

    def _prompt(self, checkpoint: _DecisionCheckpoint, request_json: str) -> str:
        terminal_values = ", ".join(reason.value for reason in TerminationReason)
        label_values = ", ".join(label.value for label in TaskLabel)
        shape = {
            "evaluation": (
                "Return disposition (exactly one of: incomplete, satisfied, terminate), summary, "
                "progressFingerprint, terminationReason, and facts. progressFingerprint MUST be "
                "a stable non-empty string of at most 256 characters. facts MUST be a JSON object "
                "(use {} when empty), never an array. terminationReason MUST be null or exactly "
                f"one of: {terminal_values}. Use incomplete when more work or a later human steer "
                "is required; do not use synonyms such as continue."
            ),
            "planning": (
                f"Return summary, rationale, and 1-{self.max_actions} actions. Every action MUST "
                "be an object with exactly key, title, description, role, and payload. key is a "
                "stable identifier; title and description are non-empty strings. role MUST be "
                f"exactly one of: {', '.join(sorted(_ACTION_ROLES))}. payload MUST be an object "
                f"and permits only these keys: {', '.join(sorted(_PAYLOAD_KEYS))}. topology MUST "
                "be exactly one of: single, primaryReviewer, parallelPanel, "
                "cheapFirstEscalation, fallback. permissionClass MUST be green, yellow, or red. "
                f"labels MUST use only: {label_values}. requiredCapabilities and "
                "preferredWorkers MUST be JSON arrays of strings. Use only capabilities and "
                "Worker IDs present in workerInventory. Every action must be executable by at "
                "least one listed Worker; when no special capability is required, use an empty "
                "requiredCapabilities array instead of inventing a capability. Do not target the "
                "same Worker with multiple actions in one plan, and do not return more actions "
                "than there are distinct eligible Workers with remaining capacity. Treat an "
                "expired manifest as unavailable and preserve unknown health or quota as UNKNOWN, "
                "never as healthy or unlimited; when only one Worker is eligible, return exactly "
                "one action and defer follow-ups to later iterations. Each action "
                "description must request a concise result of at most 800 words. priority MUST be "
                "a JSON integer from 0 through 100; panelSize MUST be a JSON integer from 1 "
                "through 8; privacySensitive and codeWriteRequired MUST be JSON booleans; "
                "minimumContextTokens MUST be a positive JSON integer or omitted. RED may be "
                "requested but never approved."
            ),
            "verification": (
                "Return satisfied as a JSON boolean, summary, progressFingerprint, "
                "terminationReason, and evidence. progressFingerprint MUST be a stable non-empty "
                "string of at most 256 characters. evidence MUST be a JSON object (use {} when "
                "empty), never an array. terminationReason MUST be null or exactly one of: "
                f"{terminal_values}."
            ),
        }[checkpoint.phase]
        return (
            "You are a provider-neutral CHIPS Agent Fabric autonomy decision Worker. "
            "Do not modify files, run tools, grant permissions, or infer missing evidence. "
            "Return exactly one JSON object, no Markdown or prose. "
            f"The object MUST use schemaVersion {DECISION_SCHEMA_VERSION!r}, decisionType "
            f"{checkpoint.output_type!r}, goalID {checkpoint.goal_id!r}, iterationSequence "
            f"{checkpoint.iteration_sequence}, and steerVersion {checkpoint.steer_version}. "
            f"{shape}\nCanonical bounded input:\n{request_json}"
        )

    @staticmethod
    def _task_id(checkpoint: _DecisionCheckpoint) -> str:
        identity = (
            f"{DECISION_SCHEMA_VERSION}:{checkpoint.goal_id}:{checkpoint.phase}:"
            f"{checkpoint.iteration_sequence}:{checkpoint.steer_version}"
        )
        return f"tsk-{uuid.uuid5(_DECISION_NAMESPACE, identity)}"

    def _require_binding(self, decision: _StrictDecision, checkpoint: _DecisionCheckpoint) -> None:
        actual = (decision.goal_id, decision.iteration_sequence, decision.steer_version)
        expected = (
            checkpoint.goal_id,
            checkpoint.iteration_sequence,
            checkpoint.steer_version,
        )
        if actual != expected:
            raise DecisionDriverError("decision output checkpoint does not match its Task binding")

    def _current_steer(self, goal_id: str) -> int:
        with self.store.connect() as connection:
            row = connection.execute(
                "SELECT steer_version FROM autonomous_goals WHERE id=?", (goal_id,)
            ).fetchone()
        if row is None:
            raise KeyError(goal_id)
        return int(row["steer_version"])

    @staticmethod
    def _failure_fingerprint(checkpoint: _DecisionCheckpoint, error: Exception) -> str:
        digest = hashlib.sha256(type(error).__name__.encode()).hexdigest()[:16]
        return f"decision-failure-{checkpoint.phase}-{checkpoint.iteration_sequence}-{digest}"


class _DecisionCheckpoint(BaseModel):
    model_config = ConfigDict(frozen=True)

    goal_id: str
    project_id: str
    iteration_id: str
    iteration_sequence: int
    steer_version: int
    phase: str

    @property
    def output_type(self) -> str:
        return "plan" if self.phase == "planning" else self.phase


def reconstruct_adapter_registry(
    store: StateStore,
    *,
    executable_overrides: Mapping[str, str] | None = None,
    local_worker_endpoints: Mapping[str, str] | None = None,
    local_worker_tokens: Mapping[str, str] | None = None,
    local_worker_drivers: Mapping[str, str] | None = None,
    allow_mock: bool = False,
    mock_behaviors: Mapping[str, MockBehavior] | None = None,
) -> AdapterRegistry:
    """Rebuild adapters from persisted harness identity without persisting secrets.

    Native harnesses use portable PATH discovery unless an explicit Worker-id or harness-value
    override is supplied. Local endpoints, bearer tokens, and registered driver selections must be
    injected by the process owner; they are never reconstructed from SQLite. Mock is refused unless
    explicitly enabled for tests.
    """

    overrides = dict(executable_overrides or {})
    endpoints = dict(local_worker_endpoints or {})
    tokens = dict(local_worker_tokens or {})
    drivers = dict(local_worker_drivers or {})
    behaviors = dict(mock_behaviors or {})
    registry = AdapterRegistry()
    for row in store.list_workers():
        worker_id = str(row["id"])
        try:
            harness = Harness(row["harness"])
            provider = Provider(row["provider"])
        except ValueError as error:
            raise AdapterReconstructionError(
                f"Worker {worker_id} has an unsupported persisted harness/provider"
            ) from error
        expected = _PROVIDER_FOR_HARNESS.get(harness)
        if expected is None or provider is not expected:
            raise AdapterReconstructionError(
                f"Worker {worker_id} harness/provider mismatch: {harness.value}/{provider.value}"
            )
        executable = overrides.get(worker_id, overrides.get(harness.value))
        adapter: WorkerAdapter
        if harness is Harness.CODEX:
            command = executable or "codex"
            _require_executable(command, worker_id)
            adapter = CodexAdapter(command)
        elif harness is Harness.CLAUDE_CODE:
            command = executable or "claude"
            _require_executable(command, worker_id)
            adapter = ClaudeAdapter(command)
        elif harness is Harness.GROK_BUILD:
            command = executable or "grok"
            _require_executable(command, worker_id)
            adapter = GrokAdapter(command)
        elif harness is Harness.GOOGLE_AGY:
            command = executable or "agy"
            _require_executable(command, worker_id)
            adapter = AgyAdapter(command)
        elif harness is Harness.LOCAL_WORKER:
            endpoint = endpoints.get(worker_id)
            if endpoint is None:
                raise AdapterReconstructionError(
                    f"Worker {worker_id} requires an explicitly injected Local Worker endpoint"
                )
            adapter = LocalWorkerAdapter(
                endpoint,
                token=tokens.get(worker_id),
                driver_id=drivers.get(worker_id),
            )
        elif harness is Harness.MOCK:
            if not allow_mock:
                raise AdapterReconstructionError(
                    f"Worker {worker_id} uses Mock; pass allow_mock=True only for tests"
                )
            adapter = MockAdapter(behaviors.get(worker_id))
        else:  # pragma: no cover - exhaustive guard for future enum additions.
            raise AdapterReconstructionError(f"Worker {worker_id} harness is unsupported")
        registry.register(worker_id, adapter)
    return registry


def _require_executable(command: str, worker_id: str) -> None:
    path = Path(command).expanduser()
    if os.path.sep in command:
        available = path.is_file() and os.access(path, os.X_OK)
    else:
        available = shutil.which(command) is not None
    if not available:
        raise AdapterReconstructionError(f"Worker {worker_id} executable unavailable: {command}")


def _select(value: Mapping[str, Any], keys: tuple[str, ...]) -> dict[str, Any]:
    return {key: value.get(key) for key in keys}


def _safe_object(value: Any, *, depth: int = 0) -> Any:
    value = redact_sensitive(value)
    if depth >= 8:
        return "[DEPTH_LIMIT]"
    if isinstance(value, Mapping):
        return {
            _bounded_string(str(key), 120): _safe_object(item, depth=depth + 1)
            for key, item in list(value.items())[:80]
        }
    if isinstance(value, (list, tuple)):
        return [_safe_object(item, depth=depth + 1) for item in value[:80]]
    if isinstance(value, str):
        return _bounded_string(value, 4000)
    if value is None or isinstance(value, (bool, int, float)):
        return value
    return _bounded_string(str(value), 1000)


def _bounded_json(value: dict[str, Any], maximum_bytes: int) -> str:
    safe = _safe_object(value)
    encoded = compact_json(safe)
    if len(encoded.encode()) <= maximum_bytes:
        return encoded
    reduced = {
        "schemaVersion": value.get("schemaVersion"),
        "decisionType": value.get("decisionType"),
        "goalID": value.get("goalID"),
        "iterationSequence": value.get("iterationSequence"),
        "steerVersion": value.get("steerVersion"),
        "context": {
            "goal": _safe_object(value.get("context", {}).get("goal", {})),
            "project": _safe_object(value.get("context", {}).get("project", {})),
            "tasks": _safe_object(value.get("context", {}).get("tasks", [])[-8:]),
            "events": _safe_object(value.get("context", {}).get("events", [])[-12:]),
            "actions": _safe_object(value.get("context", {}).get("actions", [])[-8:]),
            "steers": _safe_object(value.get("context", {}).get("steers", [])[-8:]),
        },
    }
    for key in ("evaluation", "actionResults", "workerInventory"):
        if key in value:
            reduced[key] = _safe_object(value[key])
    encoded = compact_json(reduced)
    if len(encoded.encode()) > maximum_bytes:
        digest = hashlib.sha256(encoded.encode()).hexdigest()
        reduced["context"] = {
            "goal": reduced["context"]["goal"],
            "truncated": True,
            "omittedContextSHA256": digest,
        }
        encoded = compact_json(reduced)
    if len(encoded.encode()) > maximum_bytes:
        raise DecisionDriverError("minimum safe decision context exceeds byte limit")
    return encoded


def _bounded_string(value: str, maximum: int) -> str:
    if len(value) <= maximum:
        return value
    return f"{value[: maximum - 20]}...[TRUNCATED]"


def _safe_failure(prefix: str, error: Exception) -> str:
    detail = _bounded_string(str(redact_sensitive(str(error))), 1000)
    return f"{prefix}: {type(error).__name__}: {detail}"
