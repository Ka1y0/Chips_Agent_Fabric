from __future__ import annotations

import math
import re
from collections.abc import Callable, Iterator
from dataclasses import dataclass, field
from datetime import datetime
from enum import StrEnum
from typing import Protocol

_SEMANTIC_ID = re.compile(r"^[a-z][a-z0-9]*(?:[._-][a-z0-9]+)+$")
_SAFE_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,199}$")


class UISource(StrEnum):
    API = "api"
    DOM = "dom"
    ACCESSIBILITY = "accessibility"
    LOCAL_PARSER = "localParser"
    VLM = "vlm"
    MANUAL = "manual"


class LocatorStrategy(StrEnum):
    SEMANTIC_ACTION = "semanticAction"
    STABLE_ID = "stableID"
    ROLE_AND_NAME = "roleAndName"
    ACCESSIBILITY_ID = "accessibilityID"
    DOM_LOCATOR = "domLocator"
    VISUAL_TARGET = "visualTarget"
    GEOMETRY = "geometry"


class InteractionRisk(StrEnum):
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"


class ConfidenceRoute(StrEnum):
    DETERMINISTIC = "deterministic"
    LOCAL_VALIDATION = "localValidation"
    FAST_VLM = "fastVLM"
    STRONG_VLM_OR_ESCALATION = "strongVLMOrEscalation"
    HUMAN_APPROVAL = "humanApproval"


class UIActionKind(StrEnum):
    INVOKE = "invoke"
    PRESS = "press"
    TYPE = "type"
    WAIT = "wait"


class UIConditionKind(StrEnum):
    ELEMENT_PRESENT = "elementPresent"
    ELEMENT_ABSENT = "elementAbsent"
    ELEMENT_ENABLED = "elementEnabled"
    VALUE_EQUALS = "valueEquals"
    WINDOW_EQUALS = "windowEquals"
    FOCUS_EQUALS = "focusEquals"


class InteractionError(RuntimeError):
    """Base failure for deterministic interaction execution."""


class GroundingNotFound(InteractionError):
    pass


class GroundingAmbiguous(InteractionError):
    pass


class PreconditionFailed(InteractionError):
    pass


class PostconditionFailed(InteractionError):
    pass


class InteractionEscalationRequired(InteractionError):
    def __init__(
        self,
        *,
        route: ConfidenceRoute,
        confidence: float,
        risk: InteractionRisk,
    ) -> None:
        self.route = route
        self.confidence = confidence
        self.risk = risk
        super().__init__(
            f"grounding requires {route.value}: confidence={confidence:.3f}, risk={risk.value}"
        )


@dataclass(frozen=True, slots=True)
class UIBounds:
    x: float
    y: float
    width: float
    height: float

    def __post_init__(self) -> None:
        if not all(math.isfinite(value) for value in (self.x, self.y, self.width, self.height)):
            raise ValueError("UI bounds must be finite")
        if self.width < 0 or self.height < 0:
            raise ValueError("UI bounds width and height must not be negative")


@dataclass(frozen=True, slots=True)
class UIElement:
    element_id: str
    role: str
    name: str | None = None
    value: str | None = None
    enabled: bool = True
    visible: bool = True
    bounds: UIBounds | None = None
    actions: frozenset[str] = frozenset()
    semantic_actions: frozenset[str] = frozenset()
    accessibility_id: str | None = None
    dom_locator: str | None = None
    visual_target: str | None = None
    confidence: float = 1.0
    source: UISource = UISource.DOM

    def __post_init__(self) -> None:
        if not _SAFE_ID.fullmatch(self.element_id) or not _SAFE_ID.fullmatch(self.role):
            raise ValueError("UI element identity and role must be bounded semantic identifiers")
        for label, value, maximum in (
            ("name", self.name, 1000),
            ("value", self.value, 4096),
            ("accessibility_id", self.accessibility_id, 200),
            ("dom_locator", self.dom_locator, 1000),
            ("visual_target", self.visual_target, 500),
        ):
            if value is not None and len(value) > maximum:
                raise ValueError(f"UI element {label} exceeds its length limit")
        if not 0 <= self.confidence <= 1:
            raise ValueError("UI element confidence must be between zero and one")
        if any(not _SAFE_ID.fullmatch(value) for value in self.actions):
            raise ValueError("UI element actions must be bounded semantic identifiers")
        if any(not _SEMANTIC_ID.fullmatch(value) for value in self.semantic_actions):
            raise ValueError("UI element semantic actions must be scoped machine identities")


@dataclass(frozen=True, slots=True)
class UIElementChange:
    element_id: str
    changed_fields: tuple[str, ...]
    before: UIElement
    after: UIElement


@dataclass(frozen=True, slots=True)
class UIDiff:
    previous_snapshot_id: str
    current_snapshot_id: str
    added: tuple[str, ...]
    removed: tuple[str, ...]
    changed: tuple[UIElementChange, ...]
    focus_changed: bool
    window_changed: bool

    @property
    def empty(self) -> bool:
        return not (
            self.added or self.removed or self.changed or self.focus_changed or self.window_changed
        )


@dataclass(frozen=True, slots=True)
class UISnapshot:
    snapshot_id: str
    app_id: str
    window_id: str
    timestamp: datetime
    elements: tuple[UIElement, ...]
    focus_element_id: str | None = None
    app_version: str | None = None

    def __post_init__(self) -> None:
        if not all(
            _SAFE_ID.fullmatch(value) for value in (self.snapshot_id, self.app_id, self.window_id)
        ):
            raise ValueError("UI snapshot identities must be bounded semantic identifiers")
        if self.app_version is not None and (not self.app_version or len(self.app_version) > 128):
            raise ValueError("UI snapshot app version must be bounded")
        if self.timestamp.tzinfo is None:
            raise ValueError("UI snapshot timestamp must be timezone-aware")
        if len(self.elements) > 20_000:
            raise ValueError("UI snapshot element count exceeds policy")
        element_ids = tuple(element.element_id for element in self.elements)
        if len(element_ids) != len(set(element_ids)):
            raise ValueError("UI snapshot element identities must be unique")
        if self.focus_element_id is not None and self.focus_element_id not in element_ids:
            raise ValueError("focused element must belong to the UI snapshot")

    def diff(self, previous: UISnapshot) -> UIDiff:
        before = {element.element_id: element for element in previous.elements}
        after = {element.element_id: element for element in self.elements}
        common = sorted(before.keys() & after.keys())
        changed: list[UIElementChange] = []
        comparable_fields = (
            "role",
            "name",
            "value",
            "enabled",
            "visible",
            "bounds",
            "actions",
            "semantic_actions",
            "accessibility_id",
            "dom_locator",
            "visual_target",
            "confidence",
            "source",
        )
        for element_id in common:
            fields = tuple(
                name
                for name in comparable_fields
                if getattr(before[element_id], name) != getattr(after[element_id], name)
            )
            if fields:
                changed.append(
                    UIElementChange(element_id, fields, before[element_id], after[element_id])
                )
        return UIDiff(
            previous_snapshot_id=previous.snapshot_id,
            current_snapshot_id=self.snapshot_id,
            added=tuple(sorted(after.keys() - before.keys())),
            removed=tuple(sorted(before.keys() - after.keys())),
            changed=tuple(changed),
            focus_changed=self.focus_element_id != previous.focus_element_id,
            window_changed=self.window_id != previous.window_id,
        )


@dataclass(frozen=True, slots=True)
class SemanticLocator:
    semantic_action: str | None = None
    stable_id: str | None = None
    role: str | None = None
    name: str | None = None
    accessibility_id: str | None = None
    dom_locator: str | None = None
    visual_target: str | None = None
    geometry: UIBounds | None = None
    allow_geometry: bool = False

    def __post_init__(self) -> None:
        values = (
            self.semantic_action,
            self.stable_id,
            self.role,
            self.name,
            self.accessibility_id,
            self.dom_locator,
            self.visual_target,
        )
        has_semantic_value = any(value is not None and value.strip() for value in values)
        if not has_semantic_value and self.geometry is None:
            raise ValueError("semantic locator requires at least one locator value")
        if (self.role is None) != (self.name is None):
            raise ValueError("role and name must be supplied together")
        if self.semantic_action is not None and not _SEMANTIC_ID.fullmatch(self.semantic_action):
            raise ValueError("semantic action must be a scoped machine identity")
        for label, value in (
            ("stable_id", self.stable_id),
            ("role", self.role),
            ("accessibility_id", self.accessibility_id),
        ):
            if value is not None and not _SAFE_ID.fullmatch(value):
                raise ValueError(f"locator {label} must be a bounded semantic identifier")
        for label, value, maximum in (
            ("name", self.name, 1000),
            ("dom_locator", self.dom_locator, 1000),
            ("visual_target", self.visual_target, 500),
        ):
            if value is not None and len(value) > maximum:
                raise ValueError(f"locator {label} exceeds its length limit")
        if self.geometry is not None and not self.allow_geometry:
            raise ValueError("geometry fallback requires explicit allow_geometry")

    def ground(self, snapshot: UISnapshot) -> Grounding:
        discovery_scans = 0
        for strategy, matches, scans in _locator_strategies(self, snapshot):
            discovery_scans += scans
            if not matches:
                continue
            if len(matches) > 1:
                raise GroundingAmbiguous(
                    f"{strategy.value} matched multiple UI elements: "
                    + ",".join(sorted(item.element_id for item in matches))
                )
            element = matches[0]
            if not element.visible:
                raise GroundingNotFound(
                    f"{strategy.value} matched hidden element {element.element_id}"
                )
            return Grounding(
                element=element,
                strategy=strategy,
                confidence=element.confidence,
                discovery_scans=discovery_scans,
            )
        raise GroundingNotFound(f"no UI element matched locator in snapshot {snapshot.snapshot_id}")


@dataclass(frozen=True, slots=True)
class Grounding:
    element: UIElement
    strategy: LocatorStrategy
    confidence: float
    discovery_scans: int

    def __post_init__(self) -> None:
        if self.discovery_scans < 1:
            raise ValueError("grounding discovery scans must be positive")


def _locator_strategies(
    locator: SemanticLocator, snapshot: UISnapshot
) -> Iterator[tuple[LocatorStrategy, tuple[UIElement, ...], int]]:
    elements = snapshot.elements

    if locator.semantic_action is not None:
        yield (
            LocatorStrategy.SEMANTIC_ACTION,
            tuple(item for item in elements if locator.semantic_action in item.semantic_actions),
            len(elements),
        )
    if locator.stable_id is not None:
        # Stable element identity is an indexed semantic lookup, not a discovery scan over
        # visual candidates. The in-memory tuple is a representation detail of this V0 slice.
        yield (
            LocatorStrategy.STABLE_ID,
            tuple(item for item in elements if item.element_id == locator.stable_id),
            1,
        )
    if locator.role is not None and locator.name is not None:
        yield (
            LocatorStrategy.ROLE_AND_NAME,
            tuple(
                item for item in elements if item.role == locator.role and item.name == locator.name
            ),
            len(elements),
        )
    if locator.accessibility_id is not None:
        yield (
            LocatorStrategy.ACCESSIBILITY_ID,
            tuple(item for item in elements if item.accessibility_id == locator.accessibility_id),
            len(elements),
        )
    if locator.dom_locator is not None:
        yield (
            LocatorStrategy.DOM_LOCATOR,
            tuple(item for item in elements if item.dom_locator == locator.dom_locator),
            len(elements),
        )
    if locator.visual_target is not None:
        yield (
            LocatorStrategy.VISUAL_TARGET,
            tuple(item for item in elements if item.visual_target == locator.visual_target),
            len(elements),
        )
    if locator.geometry is not None:
        yield (
            LocatorStrategy.GEOMETRY,
            tuple(item for item in elements if item.bounds == locator.geometry),
            len(elements),
        )


@dataclass(frozen=True, slots=True)
class ConfidencePolicy:
    deterministic_threshold: float = 0.98
    local_validation_threshold: float = 0.85
    fast_vlm_threshold: float = 0.60
    high_risk_requires_deterministic: bool = True

    def __post_init__(self) -> None:
        if not (
            0
            <= self.fast_vlm_threshold
            < self.local_validation_threshold
            < self.deterministic_threshold
            <= 1
        ):
            raise ValueError("confidence thresholds must be strictly ordered between zero and one")

    def route(self, confidence: float, risk: InteractionRisk) -> ConfidenceRoute:
        if not 0 <= confidence <= 1:
            raise ValueError("grounding confidence must be between zero and one")
        if (
            risk is InteractionRisk.HIGH
            and self.high_risk_requires_deterministic
            and confidence < self.deterministic_threshold
        ):
            return ConfidenceRoute.HUMAN_APPROVAL
        if confidence >= self.deterministic_threshold:
            return ConfidenceRoute.DETERMINISTIC
        if confidence >= self.local_validation_threshold:
            return ConfidenceRoute.LOCAL_VALIDATION
        if confidence >= self.fast_vlm_threshold:
            return ConfidenceRoute.FAST_VLM
        return ConfidenceRoute.STRONG_VLM_OR_ESCALATION


class UniversalUIAdapter(Protocol):
    adapter_id: str

    async def observe(self) -> UISnapshot: ...

    async def invoke(self, element: UIElement, semantic_action: str) -> None: ...

    async def press(self, element: UIElement, keys: tuple[str, ...]) -> None: ...

    async def type_text(self, element: UIElement, text: str) -> None: ...

    async def wait(self, condition: UICondition, timeout_seconds: float) -> None: ...


class LocalScreenParser(Protocol):
    parser_id: str

    async def parse(
        self,
        screenshot: bytes,
        *,
        app_id: str,
        window_id: str,
    ) -> tuple[UIElement, ...]: ...


@dataclass(frozen=True, slots=True)
class UICondition:
    kind: UIConditionKind
    locator: SemanticLocator | None = None
    expected: str | None = None

    def __post_init__(self) -> None:
        locator_required = self.kind in {
            UIConditionKind.ELEMENT_PRESENT,
            UIConditionKind.ELEMENT_ABSENT,
            UIConditionKind.ELEMENT_ENABLED,
            UIConditionKind.VALUE_EQUALS,
        }
        if locator_required and self.locator is None:
            raise ValueError(f"{self.kind.value} condition requires a semantic locator")
        if self.kind in {
            UIConditionKind.WINDOW_EQUALS,
            UIConditionKind.FOCUS_EQUALS,
        } and (self.expected is None or not self.expected.strip()):
            raise ValueError(f"{self.kind.value} condition requires an expected identity")
        if self.kind is UIConditionKind.VALUE_EQUALS and self.expected is None:
            raise ValueError("valueEquals condition requires an expected value")

    def evaluate(self, snapshot: UISnapshot) -> bool:
        if self.kind is UIConditionKind.WINDOW_EQUALS:
            return snapshot.window_id == self.expected
        if self.kind is UIConditionKind.FOCUS_EQUALS:
            return snapshot.focus_element_id == self.expected
        assert self.locator is not None
        try:
            grounding = self.locator.ground(snapshot)
        except GroundingNotFound:
            return self.kind is UIConditionKind.ELEMENT_ABSENT
        if self.kind is UIConditionKind.ELEMENT_ABSENT:
            return False
        if self.kind is UIConditionKind.ELEMENT_PRESENT:
            return True
        if self.kind is UIConditionKind.ELEMENT_ENABLED:
            return grounding.element.enabled
        if self.kind is UIConditionKind.VALUE_EQUALS:
            return grounding.element.value == self.expected
        raise ValueError(f"unsupported UI condition kind: {self.kind}")


@dataclass(frozen=True, slots=True)
class UIAction:
    action_id: str
    kind: UIActionKind
    locator: SemanticLocator
    preconditions: tuple[UICondition, ...]
    postconditions: tuple[UICondition, ...]
    risk: InteractionRisk = InteractionRisk.LOW
    semantic_action: str | None = None
    keys: tuple[str, ...] = ()
    text: str | None = None
    timeout_seconds: float = 5.0

    def __post_init__(self) -> None:
        if not self.action_id.strip():
            raise ValueError("UI action identity must not be empty")
        if not self.preconditions or not self.postconditions:
            raise ValueError("UI actions require explicit preconditions and postconditions")
        if self.timeout_seconds <= 0:
            raise ValueError("UI action timeout must be positive")
        if self.kind is UIActionKind.INVOKE:
            action = self.semantic_action or self.locator.semantic_action
            if action is None or not _SEMANTIC_ID.fullmatch(action):
                raise ValueError("invoke action requires a semantic action identity")
        if self.kind is UIActionKind.PRESS and not self.keys:
            raise ValueError("press action requires at least one key")
        if self.kind is UIActionKind.TYPE and self.text is None:
            raise ValueError("type action requires explicit structured text")


@dataclass(frozen=True, slots=True)
class UIMilestone:
    milestone_id: str
    actions: tuple[UIAction, ...]

    def __post_init__(self) -> None:
        if not self.milestone_id.strip() or not self.actions:
            raise ValueError("UI milestone identity and actions are required")
        action_ids = tuple(action.action_id for action in self.actions)
        if len(action_ids) != len(set(action_ids)):
            raise ValueError("UI milestone action identities must be unique")


@dataclass(frozen=True, slots=True)
class UIPlan:
    plan_id: str
    semantic_goal: str
    milestones: tuple[UIMilestone, ...]
    schema_version: str = "ui-plan/v1"

    def __post_init__(self) -> None:
        if not self.plan_id.strip() or not _SEMANTIC_ID.fullmatch(self.semantic_goal):
            raise ValueError("UI plan requires an identity and a scoped semantic goal")
        if not self.milestones:
            raise ValueError("UI plan requires at least one milestone")
        milestone_ids = tuple(milestone.milestone_id for milestone in self.milestones)
        if len(milestone_ids) != len(set(milestone_ids)):
            raise ValueError("UI plan milestone identities must be unique")
        action_ids = [action.action_id for item in self.milestones for action in item.actions]
        if len(action_ids) != len(set(action_ids)):
            raise ValueError("UI plan action identities must be globally unique")
        if self.schema_version != "ui-plan/v1":
            raise ValueError("unsupported UI plan schema version")


@dataclass(frozen=True, slots=True)
class UIActionResult:
    action_id: str
    semantic_action: str
    risk: InteractionRisk
    precondition_kinds: tuple[str, ...]
    postcondition_kinds: tuple[str, ...]
    before_snapshot: UISnapshot
    after_snapshot: UISnapshot
    grounding: Grounding
    route: ConfidenceRoute
    diff: UIDiff


@dataclass(frozen=True, slots=True)
class UIMilestoneResult:
    milestone_id: str
    actions: tuple[UIActionResult, ...]


@dataclass(frozen=True, slots=True)
class UIPlanResult:
    plan_id: str
    milestones: tuple[UIMilestoneResult, ...]


@dataclass(slots=True)
class ClosedLoopUIExecutor:
    adapter: UniversalUIAdapter
    confidence_policy: ConfidencePolicy = field(default_factory=ConfidencePolicy)
    checkpoint: Callable[[int, str, UIAction, UISnapshot | None, str | None], None] | None = None

    async def execute_plan(self, plan: UIPlan) -> UIPlanResult:
        """Execute structured milestones locally until completion or the first exception."""

        milestones: list[UIMilestoneResult] = []
        action_ordinal = 0
        for milestone in plan.milestones:
            actions: list[UIActionResult] = []
            for action in milestone.actions:
                actions.append(await self.execute_action(action, action_ordinal=action_ordinal))
                action_ordinal += 1
            milestones.append(UIMilestoneResult(milestone.milestone_id, tuple(actions)))
        return UIPlanResult(plan.plan_id, tuple(milestones))

    async def execute_action(self, action: UIAction, *, action_ordinal: int = 0) -> UIActionResult:
        try:
            before = await self.adapter.observe()
            self._checkpoint(action_ordinal, "observed", action, before)
            grounding = action.locator.ground(before)
            self._checkpoint(action_ordinal, "grounded", action, before)
            route = self.confidence_policy.route(grounding.confidence, action.risk)
            if route is not ConfidenceRoute.DETERMINISTIC:
                raise InteractionEscalationRequired(
                    route=route,
                    confidence=grounding.confidence,
                    risk=action.risk,
                )
            failed_preconditions = [
                condition.kind.value
                for condition in action.preconditions
                if not condition.evaluate(before)
            ]
            if failed_preconditions:
                raise PreconditionFailed(
                    f"action {action.action_id} preconditions failed: "
                    + ",".join(failed_preconditions)
                )
            self._checkpoint(action_ordinal, "preconditionChecked", action, before)
            self._checkpoint(action_ordinal, "actionStarted", action, before)
            await self._act(action, grounding.element)
            self._checkpoint(action_ordinal, "actionReturned", action)

            after = await self.adapter.observe()
            failed_postconditions = [
                condition.kind.value
                for condition in action.postconditions
                if not condition.evaluate(after)
            ]
            if failed_postconditions:
                raise PostconditionFailed(
                    f"action {action.action_id} postconditions failed: "
                    + ",".join(failed_postconditions)
                )
            self._checkpoint(action_ordinal, "postconditionVerified", action, after)
            return UIActionResult(
                action_id=action.action_id,
                semantic_action=(
                    action.semantic_action
                    or action.locator.semantic_action
                    or f"ui.{action.kind.value}"
                ),
                risk=action.risk,
                precondition_kinds=tuple(item.kind.value for item in action.preconditions),
                postcondition_kinds=tuple(item.kind.value for item in action.postconditions),
                before_snapshot=before,
                after_snapshot=after,
                grounding=grounding,
                route=route,
                diff=after.diff(before),
            )
        except BaseException as error:
            self._checkpoint(
                action_ordinal,
                "failed",
                action,
                detail_code=f"interaction.{type(error).__name__}",
            )
            raise

    def _checkpoint(
        self,
        action_ordinal: int,
        phase: str,
        action: UIAction,
        snapshot: UISnapshot | None = None,
        detail_code: str | None = None,
    ) -> None:
        if self.checkpoint is not None:
            self.checkpoint(action_ordinal, phase, action, snapshot, detail_code)

    async def _act(self, action: UIAction, element: UIElement) -> None:
        if not element.enabled:
            raise PreconditionFailed(f"action target {element.element_id} is disabled")
        if action.kind is UIActionKind.INVOKE:
            semantic_action = action.semantic_action or action.locator.semantic_action
            assert semantic_action is not None
            if semantic_action not in element.semantic_actions:
                raise PreconditionFailed(
                    f"element {element.element_id} does not support {semantic_action}"
                )
            await self.adapter.invoke(element, semantic_action)
            return
        if action.kind is UIActionKind.PRESS:
            await self.adapter.press(element, action.keys)
            return
        if action.kind is UIActionKind.TYPE:
            assert action.text is not None
            await self.adapter.type_text(element, action.text)
            return
        if action.kind is UIActionKind.WAIT:
            await self.adapter.wait(action.postconditions[0], action.timeout_seconds)
            return
        raise ValueError(f"unsupported UI action kind: {action.kind}")
