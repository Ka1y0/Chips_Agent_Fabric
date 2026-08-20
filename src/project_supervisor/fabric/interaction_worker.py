from __future__ import annotations

import asyncio
import json
import re
from collections.abc import Callable, Mapping
from contextlib import suppress
from dataclasses import replace
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, ValidationError, model_validator

from project_supervisor.adapters.base import (
    EventSink,
    UnsafeWorkerRequest,
    Usage,
    WorkerAdapter,
    WorkerEvent,
    WorkerRequest,
    WorkerResult,
    WorkerUnavailable,
    event_time,
    publish_event,
    request_requires_code_write,
)
from project_supervisor.domain import RunState

from .interaction import (
    ClosedLoopUIExecutor,
    ConfidencePolicy,
    GroundingAmbiguous,
    GroundingNotFound,
    InteractionError,
    InteractionEscalationRequired,
    InteractionRisk,
    PostconditionFailed,
    PreconditionFailed,
    SemanticLocator,
    UIAction,
    UIActionKind,
    UICondition,
    UIConditionKind,
    UIElement,
    UIMilestone,
    UIPlan,
    UIPlanResult,
    UISnapshot,
    UISource,
    UniversalUIAdapter,
)
from .persistence import (
    InteractionRepository,
    InteractionResourceRepository,
    ResourceLeaseBundle,
    SkillRepository,
    UIGraphRepository,
)

_SEMANTIC_ID = re.compile(r"^[a-z][a-z0-9]*(?:[._:-][a-z0-9]+)+$")
_RESOURCE_ID = re.compile(r"^[A-Za-z][A-Za-z0-9._:-]{0,159}$")
_MAX_SPEC_BYTES = 65_536
_MAX_MILESTONES = 16
_MAX_ACTIONS = 64
_SUPPORTED_CHANNELS = frozenset(
    {UISource.API, UISource.DOM, UISource.ACCESSIBILITY, UISource.LOCAL_PARSER}
)


class InteractionSpecError(UnsafeWorkerRequest):
    """The canonical structured interaction specification is missing or invalid."""


class _StrictSpec(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class _LocatorSpec(_StrictSpec):
    semantic_action: str | None = Field(default=None, alias="semanticAction", max_length=160)
    stable_id: str | None = Field(default=None, alias="stableID", max_length=200)
    role: str | None = Field(default=None, max_length=100)
    name: str | None = Field(default=None, max_length=200)
    accessibility_id: str | None = Field(
        default=None,
        alias="accessibilityID",
        max_length=200,
    )
    dom_locator: str | None = Field(default=None, alias="domLocator", max_length=500)
    visual_target: str | None = Field(default=None, alias="visualTarget", max_length=200)

    def to_domain(self) -> SemanticLocator:
        return SemanticLocator(
            semantic_action=self.semantic_action,
            stable_id=self.stable_id,
            role=self.role,
            name=self.name,
            accessibility_id=self.accessibility_id,
            dom_locator=self.dom_locator,
            visual_target=self.visual_target,
        )


class _ConditionSpec(_StrictSpec):
    kind: UIConditionKind
    locator: _LocatorSpec | None = None
    expected: str | None = Field(default=None, max_length=500)

    def to_domain(self) -> UICondition:
        return UICondition(
            kind=self.kind,
            locator=self.locator.to_domain() if self.locator is not None else None,
            expected=self.expected,
        )


class _ActionSpec(_StrictSpec):
    action_id: str = Field(alias="actionID", min_length=1, max_length=160)
    kind: UIActionKind
    locator: _LocatorSpec
    preconditions: tuple[_ConditionSpec, ...]
    postconditions: tuple[_ConditionSpec, ...]
    risk: InteractionRisk = InteractionRisk.LOW
    semantic_action: str | None = Field(
        default=None,
        alias="semanticAction",
        max_length=160,
    )
    keys: tuple[str, ...] = ()
    text: str | None = Field(default=None, max_length=4096)
    timeout_seconds: float = Field(default=5.0, alias="timeoutSeconds", gt=0, le=60)

    def to_domain(self) -> UIAction:
        return UIAction(
            action_id=self.action_id,
            kind=self.kind,
            locator=self.locator.to_domain(),
            preconditions=tuple(item.to_domain() for item in self.preconditions),
            postconditions=tuple(item.to_domain() for item in self.postconditions),
            risk=self.risk,
            semantic_action=self.semantic_action,
            keys=self.keys,
            text=self.text,
            timeout_seconds=self.timeout_seconds,
        )


class _MilestoneSpec(_StrictSpec):
    milestone_id: str = Field(alias="milestoneID", min_length=1, max_length=160)
    actions: tuple[_ActionSpec, ...]

    def to_domain(self) -> UIMilestone:
        return UIMilestone(
            milestone_id=self.milestone_id,
            actions=tuple(item.to_domain() for item in self.actions),
        )


class InteractionExecutionSpec(_StrictSpec):
    """Bounded canonical UI plan carried by ``WorkerRequest.metadata.executionSpec``."""

    schema_version: Literal["ui-plan/v1"] = Field(alias="schemaVersion")
    plan_id: str = Field(alias="planID", min_length=1, max_length=160)
    semantic_goal: str = Field(alias="semanticGoal", min_length=3, max_length=160)
    app_id: str = Field(alias="appID", min_length=3, max_length=160)
    app_version: str = Field(alias="appVersion", min_length=1, max_length=100)
    channel: UISource
    resource_keys: tuple[str, ...] = Field(alias="resourceKeys")
    milestones: tuple[_MilestoneSpec, ...]

    @model_validator(mode="after")
    def validate_contract(self) -> InteractionExecutionSpec:
        if not _SEMANTIC_ID.fullmatch(self.semantic_goal):
            raise ValueError("semanticGoal must be a scoped machine identity")
        if not _SEMANTIC_ID.fullmatch(self.app_id):
            raise ValueError("appID must be a scoped machine identity")
        if self.channel not in _SUPPORTED_CHANNELS:
            raise ValueError("executionSpec requires a deterministic local interaction channel")
        if not 1 <= len(self.resource_keys) <= 16:
            raise ValueError("executionSpec requires between one and sixteen resources")
        if len(self.resource_keys) != len(set(self.resource_keys)):
            raise ValueError("executionSpec resource keys must be unique")
        if any(not _RESOURCE_ID.fullmatch(value) for value in self.resource_keys):
            raise ValueError("executionSpec resource keys must be semantic identities")
        if not 1 <= len(self.milestones) <= _MAX_MILESTONES:
            raise ValueError("executionSpec milestone count is outside policy")
        action_count = sum(len(milestone.actions) for milestone in self.milestones)
        if not 1 <= action_count <= _MAX_ACTIONS:
            raise ValueError("executionSpec action count is outside policy")
        # Domain construction validates locator presence, condition semantics, action-specific
        # fields, globally unique IDs, and the absence of natural-language execution authority.
        self.to_plan()
        return self

    def to_plan(self) -> UIPlan:
        return UIPlan(
            plan_id=self.plan_id,
            semantic_goal=self.semantic_goal,
            milestones=tuple(item.to_domain() for item in self.milestones),
            schema_version=self.schema_version,
        )


def parse_interaction_spec(metadata: Mapping[str, Any]) -> InteractionExecutionSpec:
    raw = metadata.get("executionSpec")
    if not isinstance(raw, Mapping):
        raise InteractionSpecError("metadata.executionSpec is required and must be an object")
    try:
        encoded = json.dumps(raw, separators=(",", ":"), sort_keys=True).encode("utf-8")
    except (TypeError, ValueError) as error:
        raise InteractionSpecError("executionSpec must contain bounded JSON values") from error
    if len(encoded) > _MAX_SPEC_BYTES:
        raise InteractionSpecError("executionSpec exceeds the 65536-byte policy limit")
    try:
        return InteractionExecutionSpec.model_validate(raw)
    except (ValidationError, ValueError) as error:
        raise InteractionSpecError("executionSpec failed strict ui-plan/v1 validation") from error


type UIAdapterFactory = Callable[[InteractionExecutionSpec], UniversalUIAdapter]


class _LeaseGuardedUIAdapter:
    """Fence every observation and side effect with the current resource generation."""

    def __init__(
        self,
        delegate: UniversalUIAdapter,
        resources: InteractionResourceRepository,
        bundle: ResourceLeaseBundle,
    ) -> None:
        self.delegate = delegate
        self.resources = resources
        self.bundle = bundle
        self.adapter_id = f"lease-guarded:{delegate.adapter_id}"
        self._lost = False

    def mark_lost(self) -> None:
        self._lost = True

    def _assert_current(self) -> None:
        try:
            current = not self._lost and self.resources.is_current(self.bundle)
        except Exception as error:
            raise InteractionError("interaction resource lease could not be verified") from error
        if not current:
            raise InteractionError("interaction resource lease generation is no longer current")

    async def observe(self) -> UISnapshot:
        self._assert_current()
        return await self.delegate.observe()

    async def invoke(self, element: UIElement, semantic_action: str) -> None:
        self._assert_current()
        await self.delegate.invoke(element, semantic_action)

    async def press(self, element: UIElement, keys: tuple[str, ...]) -> None:
        self._assert_current()
        await self.delegate.press(element, keys)

    async def type_text(self, element: UIElement, text: str) -> None:
        self._assert_current()
        await self.delegate.type_text(element, text)

    async def wait(self, condition: UICondition, timeout_seconds: float) -> None:
        self._assert_current()
        await self.delegate.wait(condition, timeout_seconds)


class InteractionWorkerAdapter(WorkerAdapter):
    """Supervisor-local semantic UI Worker backed by deterministic structured authority.

    The adapter never inspects ``WorkerRequest.prompt`` and never reconstructs authority from a
    Task row. The Runtime must explicitly propagate the canonical ``executionSpec`` metadata.
    """

    def __init__(
        self,
        *,
        worker_id: str,
        resources: InteractionResourceRepository,
        interactions: InteractionRepository,
        skills: SkillRepository,
        graph: UIGraphRepository,
        ui_adapter_factory: UIAdapterFactory,
        confidence_policy: ConfidencePolicy | None = None,
        resource_ttl_seconds: float = 30.0,
    ) -> None:
        if not _RESOURCE_ID.fullmatch(worker_id):
            raise ValueError("interaction Worker identity must be semantic")
        if not 1 <= resource_ttl_seconds <= 300:
            raise ValueError("interaction resource TTL must be between one and 300 seconds")
        self.worker_id = worker_id
        self.resources = resources
        self.interactions = interactions
        self.skills = skills
        self.graph = graph
        self.ui_adapter_factory = ui_adapter_factory
        self.confidence_policy = confidence_policy or ConfidencePolicy()
        self.resource_ttl_seconds = resource_ttl_seconds
        self._active: dict[str, asyncio.Task[Any]] = {}

    async def execute(
        self,
        request: WorkerRequest,
        *,
        event_sink: EventSink | None = None,
    ) -> WorkerResult:
        if request_requires_code_write(request):
            raise InteractionSpecError("semantic UI Worker does not grant production code writes")
        spec = parse_interaction_spec(request.metadata)
        plan = spec.to_plan()
        plan, skill_hint_ids = self._apply_skill_hints(spec, plan)
        bundle = self.resources.acquire(
            spec.resource_keys,
            owner_id=request.run_id,
            task_id=request.task_id,
            run_id=request.run_id,
            ttl_seconds=self.resource_ttl_seconds,
        )
        if bundle is None:
            raise WorkerUnavailable("interaction resource bundle is owned by another execution")

        execution_id = f"interaction:{request.run_id}"
        started_at = event_time()
        current = asyncio.current_task()
        if current is not None:
            self._active[request.run_id] = current
        started = False
        renewal_task: asyncio.Task[None] | None = None
        try:
            self.interactions.start_execution(
                execution_id=execution_id,
                plan=plan,
                bundle=bundle,
                adapter_kind="semantic-ui-worker/v1",
                channel=spec.channel.value,
                app_id=spec.app_id,
                app_version=spec.app_version,
                task_id=request.task_id,
                run_id=request.run_id,
                worker_id=self.worker_id,
            )
            started = True
            await publish_event(
                event_sink,
                WorkerEvent(
                    request.run_id,
                    "interactionStarted",
                    event_time(),
                    {
                        "executionID": execution_id,
                        "planID": plan.plan_id,
                        "channel": spec.channel.value,
                        "skillHintCount": len(skill_hint_ids),
                    },
                ),
            )
            ui_adapter = _LeaseGuardedUIAdapter(
                self.ui_adapter_factory(spec),
                self.resources,
                bundle,
            )
            renewal_task = asyncio.create_task(
                self._renew_resource_bundle(bundle, ui_adapter),
                name=f"interaction-resource-renewal:{request.run_id}",
            )
            result = await ClosedLoopUIExecutor(
                ui_adapter,
                confidence_policy=self.confidence_policy,
                checkpoint=lambda ordinal, phase, action, snapshot, detail: (
                    self.interactions.record_action_checkpoint(
                        execution_id,
                        action_ordinal=ordinal,
                        phase=phase,
                        action=action,
                        bundle=bundle,
                        snapshot=snapshot,
                        detail_code=detail,
                    )
                ),
            ).execute_plan(plan)
            trajectory_id = self.interactions.record_plan_result(
                execution_id,
                result,
                bundle=bundle,
            )
            summary = self._success_summary(
                execution_id=execution_id,
                trajectory_id=trajectory_id,
                result=result,
                skill_hint_ids=skill_hint_ids,
            )
            await publish_event(
                event_sink,
                WorkerEvent(
                    request.run_id,
                    "interactionCompleted",
                    event_time(),
                    summary,
                ),
            )
            text = json.dumps(summary, separators=(",", ":"), sort_keys=True)
            return WorkerResult(
                run_id=request.run_id,
                state=RunState.COMPLETED,
                pid=None,
                exit_code=0,
                started_at=started_at,
                ended_at=event_time(),
                stdout=text,
                stderr="",
                final_text=text,
                events=(),
                session_id=f"interaction:{request.run_id}",
                usage=Usage(cost_usd=0.0),
            )
        except asyncio.CancelledError:
            if started:
                self._fail_execution(execution_id, bundle=bundle, code="interaction.cancelled")
            return self._failure_result(
                request.run_id,
                started_at,
                RunState.CANCELLED,
                "interaction.cancelled",
            )
        except InteractionError as error:
            code, escalated = self._failure_code(error)
            if started:
                self._fail_execution(
                    execution_id,
                    bundle=bundle,
                    code=code,
                    escalated=escalated,
                )
            await publish_event(
                event_sink,
                WorkerEvent(
                    request.run_id,
                    "interactionFailed",
                    event_time(),
                    {"executionID": execution_id, "errorCode": code, "escalated": escalated},
                ),
            )
            return self._failure_result(request.run_id, started_at, RunState.FAILED, code)
        finally:
            if renewal_task is not None:
                renewal_task.cancel()
                with suppress(asyncio.CancelledError):
                    await renewal_task
            self.resources.release(bundle)
            self._active.pop(request.run_id, None)

    async def cancel(self, run_id: str) -> bool:
        task = self._active.get(run_id)
        if task is None or task.done():
            return False
        task.cancel()
        return True

    async def _renew_resource_bundle(
        self,
        bundle: ResourceLeaseBundle,
        guarded_adapter: _LeaseGuardedUIAdapter,
    ) -> None:
        interval = max(0.25, self.resource_ttl_seconds / 3)
        while True:
            await asyncio.sleep(interval)
            try:
                renewed = self.resources.renew(
                    bundle,
                    ttl_seconds=self.resource_ttl_seconds,
                )
            except Exception:
                guarded_adapter.mark_lost()
                return
            if not renewed:
                guarded_adapter.mark_lost()
                return

    def _apply_skill_hints(
        self,
        spec: InteractionExecutionSpec,
        plan: UIPlan,
    ) -> tuple[UIPlan, tuple[str, ...]]:
        by_action: dict[str, dict[str, Any]] = {}
        lifecycle_rank = {"active": 3, "validated": 2, "candidate": 1}
        for row in self.skills.list(limit=500):
            semantic_action = str(row["semantic_action"])
            if (
                row["app_id"] != spec.app_id
                or row["app_version_constraint"] != spec.app_version
                or row["lifecycle"] not in lifecycle_rank
            ):
                continue
            current = by_action.get(semantic_action)
            rank = (lifecycle_rank[str(row["lifecycle"])], int(row["revision"]))
            if current is None or rank > current["rank"]:
                try:
                    template = json.loads(str(row["template_json"]))
                except (TypeError, json.JSONDecodeError):
                    continue
                if not isinstance(template, dict):
                    continue
                by_action[semantic_action] = {"row": row, "template": template, "rank": rank}

        hint_ids: list[str] = []
        milestones: list[UIMilestone] = []
        for milestone in plan.milestones:
            actions: list[UIAction] = []
            for action in milestone.actions:
                semantic_action = action.semantic_action or action.locator.semantic_action
                hint = by_action.get(semantic_action or "")
                if hint is None:
                    actions.append(action)
                    continue
                template = hint["template"]
                stable_id = template.get("stableID")
                template_action = template.get("semanticAction")
                if (
                    not isinstance(stable_id, str)
                    or not _RESOURCE_ID.fullmatch(stable_id)
                    or template_action != semantic_action
                ):
                    actions.append(action)
                    continue
                hinted_locator = SemanticLocator(stable_id=stable_id)
                actions.append(
                    replace(
                        action,
                        locator=hinted_locator,
                        preconditions=tuple(
                            replace(item, locator=hinted_locator)
                            if item.locator == action.locator
                            else item
                            for item in action.preconditions
                        ),
                        postconditions=tuple(
                            replace(item, locator=hinted_locator)
                            if item.locator == action.locator
                            else item
                            for item in action.postconditions
                        ),
                    )
                )
                hint_ids.append(str(hint["row"]["id"]))
            milestones.append(replace(milestone, actions=tuple(actions)))
        return replace(plan, milestones=tuple(milestones)), tuple(sorted(set(hint_ids)))

    @staticmethod
    def _success_summary(
        *,
        execution_id: str,
        trajectory_id: str,
        result: UIPlanResult,
        skill_hint_ids: tuple[str, ...],
    ) -> dict[str, Any]:
        actions = [action for item in result.milestones for action in item.actions]
        return {
            "schemaVersion": "interaction-worker-result/v1",
            "executionID": execution_id,
            "trajectoryID": trajectory_id,
            "verified": True,
            "observationCount": len(actions) * 2,
            "groundingCount": len(actions),
            "discoveryScans": sum(action.grounding.discovery_scans for action in actions),
            "skillHintApplied": bool(skill_hint_ids),
            "skillHintIDs": list(skill_hint_ids),
            "learningPendingIndependentVerification": True,
            "skillIDs": [],
        }

    def _fail_execution(
        self,
        execution_id: str,
        *,
        bundle: Any,
        code: str,
        escalated: bool = False,
    ) -> None:
        try:
            self.interactions.fail_execution(
                execution_id,
                bundle=bundle,
                error_code=code,
                escalated=escalated,
            )
        except RuntimeError:
            # A lost resource generation must never be used to write a replacement execution.
            return

    @staticmethod
    def _failure_code(error: InteractionError) -> tuple[str, bool]:
        if isinstance(error, InteractionEscalationRequired):
            return "grounding.escalation", True
        if isinstance(error, GroundingAmbiguous):
            return "grounding.ambiguous", True
        if isinstance(error, GroundingNotFound):
            return "grounding.notFound", False
        if isinstance(error, PreconditionFailed):
            return "precondition.failed", False
        if isinstance(error, PostconditionFailed):
            return "postcondition.failed", False
        return "interaction.failed", False

    @staticmethod
    def _failure_result(
        run_id: str,
        started_at: Any,
        state: RunState,
        code: str,
    ) -> WorkerResult:
        return WorkerResult(
            run_id=run_id,
            state=state,
            pid=None,
            exit_code=None,
            started_at=started_at,
            ended_at=event_time(),
            stdout="",
            stderr="",
            final_text="",
            events=(),
            session_id=f"interaction:{run_id}",
            usage=Usage(cost_usd=0.0),
            error=code,
        )
