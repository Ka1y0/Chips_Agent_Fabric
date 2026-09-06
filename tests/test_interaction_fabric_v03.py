from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime, timedelta

import pytest

from project_supervisor.fabric.interaction import (
    ClosedLoopUIExecutor,
    ConfidencePolicy,
    ConfidenceRoute,
    GroundingAmbiguous,
    GroundingNotFound,
    InteractionEscalationRequired,
    InteractionRisk,
    LocatorStrategy,
    PostconditionFailed,
    SemanticLocator,
    UIAction,
    UIActionKind,
    UIBounds,
    UICondition,
    UIConditionKind,
    UIElement,
    UIMilestone,
    UIPlan,
    UISnapshot,
    UISource,
)

NOW = datetime(2026, 8, 11, tzinfo=UTC)


class DeterministicUIFixture:
    """Stateful local semantic UI with no provider, browser, or model dependency."""

    adapter_id = "fixture-semantic-ui/v1"

    def __init__(self, *, confidence: float = 1.0, transition: bool = True) -> None:
        self.confidence = confidence
        self.transition = transition
        self.state = "home"
        self.observation_count = 0
        self.invocations: list[tuple[str, str]] = []
        self.presses: list[tuple[str, tuple[str, ...]]] = []
        self.typed: list[tuple[str, str]] = []
        self.wait_count = 0

    async def observe(self) -> UISnapshot:
        self.observation_count += 1
        if self.state == "home":
            elements = (
                UIElement(
                    "main-window",
                    "window",
                    name="Fixture",
                    semantic_actions=frozenset({"fixture.window"}),
                ),
                UIElement(
                    "settings",
                    "button",
                    name="Preferences",
                    actions=frozenset({"invoke"}),
                    semantic_actions=frozenset({"fixture.open_preferences"}),
                    accessibility_id="fixture.settings",
                    dom_locator='[data-semantic-action="fixture.open_preferences"]',
                    confidence=self.confidence,
                    source=UISource.DOM,
                ),
            )
            window_id = "main-window"
            focus = "settings"
        else:
            elements = (
                UIElement(
                    "preferences-window",
                    "dialog",
                    name="Preferences",
                    semantic_actions=frozenset({"fixture.preferences"}),
                    source=UISource.DOM,
                ),
            )
            window_id = "preferences-window"
            focus = "preferences-window"
        return UISnapshot(
            snapshot_id=f"snapshot-{self.observation_count}",
            app_id="fixture.app",
            app_version="1.0",
            window_id=window_id,
            timestamp=NOW + timedelta(seconds=self.observation_count),
            elements=elements,
            focus_element_id=focus,
        )

    async def invoke(self, element: UIElement, semantic_action: str) -> None:
        self.invocations.append((element.element_id, semantic_action))
        if semantic_action == "fixture.open_preferences" and self.transition:
            self.state = "preferences"

    async def press(self, element: UIElement, keys: tuple[str, ...]) -> None:
        self.presses.append((element.element_id, keys))

    async def type_text(self, element: UIElement, text: str) -> None:
        self.typed.append((element.element_id, text))

    async def wait(self, condition: UICondition, timeout_seconds: float) -> None:
        assert timeout_seconds > 0
        self.wait_count += 1


def snapshot(
    identity: str,
    *,
    window: str,
    elements: tuple[UIElement, ...],
    focus: str | None = None,
) -> UISnapshot:
    return UISnapshot(
        identity,
        "fixture.app",
        window,
        NOW,
        elements,
        focus_element_id=focus,
        app_version="1.0",
    )


def open_preferences_action(
    *,
    risk: InteractionRisk = InteractionRisk.LOW,
) -> UIAction:
    target = SemanticLocator(
        semantic_action="fixture.open_preferences",
        stable_id="settings",
        role="button",
        name="Preferences",
    )
    return UIAction(
        action_id="open-preferences",
        kind=UIActionKind.INVOKE,
        locator=target,
        semantic_action="fixture.open_preferences",
        risk=risk,
        preconditions=(
            UICondition(UIConditionKind.ELEMENT_ENABLED, locator=target),
            UICondition(UIConditionKind.WINDOW_EQUALS, expected="main-window"),
        ),
        postconditions=(
            UICondition(UIConditionKind.WINDOW_EQUALS, expected="preferences-window"),
            UICondition(
                UIConditionKind.ELEMENT_PRESENT,
                locator=SemanticLocator(stable_id="preferences-window"),
            ),
        ),
    )


def test_ui_snapshot_diff_tracks_semantic_change_focus_and_window() -> None:
    before = snapshot(
        "before",
        window="main",
        elements=(
            UIElement("field", "textbox", value="old"),
            UIElement("removed", "button", name="Old"),
        ),
        focus="field",
    )
    after = snapshot(
        "after",
        window="preferences",
        elements=(
            UIElement(
                "field",
                "textbox",
                value="new",
                bounds=UIBounds(1, 2, 30, 10),
            ),
            UIElement("added", "button", name="New"),
        ),
        focus="added",
    )

    difference = after.diff(before)

    assert difference.added == ("added",)
    assert difference.removed == ("removed",)
    assert difference.changed[0].element_id == "field"
    assert difference.changed[0].changed_fields == ("value", "bounds")
    assert difference.focus_changed
    assert difference.window_changed
    assert not difference.empty


def test_ui_ir_rejects_secret_shaped_or_nonfinite_identity_data() -> None:
    with pytest.raises(ValueError, match="bounded semantic identifiers"):
        UIElement("token=private", "button")
    with pytest.raises(ValueError, match="finite"):
        UIBounds(0, 0, float("nan"), 10)
    with pytest.raises(ValueError, match="bounded semantic identifier"):
        SemanticLocator(stable_id="/private/unsafe/path")


def test_semantic_locator_uses_precedence_and_fails_closed_on_ambiguity() -> None:
    current = snapshot(
        "locator",
        window="main",
        elements=(
            UIElement(
                "semantic-winner",
                "button",
                name="Open",
                semantic_actions=frozenset({"fixture.open"}),
            ),
            UIElement("stable-fallback", "button", name="Other"),
        ),
    )
    grounding = SemanticLocator(
        semantic_action="fixture.open",
        stable_id="stable-fallback",
    ).ground(current)
    assert grounding.element.element_id == "semantic-winner"
    assert grounding.strategy is LocatorStrategy.SEMANTIC_ACTION

    ambiguous = snapshot(
        "ambiguous",
        window="main",
        elements=(
            UIElement("save-a", "button", name="Save"),
            UIElement("save-b", "button", name="Save"),
        ),
    )
    with pytest.raises(GroundingAmbiguous, match="save-a,save-b"):
        SemanticLocator(role="button", name="Save").ground(ambiguous)

    with pytest.raises(GroundingNotFound):
        SemanticLocator(stable_id="missing").ground(current)
    with pytest.raises(ValueError, match="explicit allow_geometry"):
        SemanticLocator(geometry=UIBounds(0, 0, 10, 10))


def test_confidence_policy_is_configurable_and_high_risk_fails_to_approval() -> None:
    policy = ConfidencePolicy(
        deterministic_threshold=0.90,
        local_validation_threshold=0.70,
        fast_vlm_threshold=0.40,
    )
    assert policy.route(0.90, InteractionRisk.LOW) is ConfidenceRoute.DETERMINISTIC
    assert policy.route(0.75, InteractionRisk.LOW) is ConfidenceRoute.LOCAL_VALIDATION
    assert policy.route(0.50, InteractionRisk.LOW) is ConfidenceRoute.FAST_VLM
    assert policy.route(0.20, InteractionRisk.LOW) is ConfidenceRoute.STRONG_VLM_OR_ESCALATION
    assert policy.route(0.89, InteractionRisk.HIGH) is ConfidenceRoute.HUMAN_APPROVAL

    with pytest.raises(ValueError, match="strictly ordered"):
        ConfidencePolicy(
            deterministic_threshold=0.8,
            local_validation_threshold=0.8,
            fast_vlm_threshold=0.4,
        )


async def test_closed_loop_executor_verifies_postcondition_and_returns_diff() -> None:
    adapter = DeterministicUIFixture()
    executor = ClosedLoopUIExecutor(adapter)
    plan = UIPlan(
        "plan-open-preferences",
        "fixture.open_preferences",
        (UIMilestone("open", (open_preferences_action(),)),),
    )

    result = await executor.execute_plan(plan)

    action = result.milestones[0].actions[0]
    assert action.route is ConfidenceRoute.DETERMINISTIC
    assert action.before_snapshot.window_id == "main-window"
    assert action.after_snapshot.window_id == "preferences-window"
    assert action.diff.window_changed
    assert adapter.invocations == [("settings", "fixture.open_preferences")]
    assert adapter.observation_count == 2


async def test_action_issued_is_not_success_without_verified_postcondition() -> None:
    adapter = DeterministicUIFixture(transition=False)
    executor = ClosedLoopUIExecutor(adapter)

    with pytest.raises(PostconditionFailed, match="windowEquals"):
        await executor.execute_action(open_preferences_action())

    assert adapter.invocations == [("settings", "fixture.open_preferences")]
    assert adapter.observation_count == 2


async def test_uncertain_high_risk_grounding_escalates_before_action() -> None:
    adapter = DeterministicUIFixture(confidence=0.90)
    executor = ClosedLoopUIExecutor(adapter)

    with pytest.raises(InteractionEscalationRequired) as caught:
        await executor.execute_action(open_preferences_action(risk=InteractionRisk.HIGH))

    assert caught.value.route is ConfidenceRoute.HUMAN_APPROVAL
    assert adapter.invocations == []
    assert adapter.observation_count == 1


async def test_milestone_execution_stops_on_first_unexpected_state() -> None:
    adapter = DeterministicUIFixture()
    missing = SemanticLocator(semantic_action="fixture.missing")
    unreachable = UIAction(
        action_id="unreachable-action",
        kind=UIActionKind.INVOKE,
        locator=missing,
        preconditions=(UICondition(UIConditionKind.ELEMENT_PRESENT, locator=missing),),
        postconditions=(UICondition(UIConditionKind.WINDOW_EQUALS, expected="never-reached"),),
    )
    plan = UIPlan(
        "plan-stop-on-error",
        "fixture.stop_on_error",
        (
            UIMilestone("first", (open_preferences_action(),)),
            UIMilestone("second", (unreachable,)),
            UIMilestone("third", (replace(unreachable, action_id="never-reached"),)),
        ),
    )

    with pytest.raises(GroundingNotFound):
        await ClosedLoopUIExecutor(adapter).execute_plan(plan)

    assert adapter.invocations == [("settings", "fixture.open_preferences")]
    assert adapter.observation_count == 3


def test_ui_plan_rejects_natural_language_as_execution_authority() -> None:
    with pytest.raises(ValueError, match="scoped semantic goal"):
        UIPlan(
            "natural-language-plan",
            "please click whatever looks right",
            (UIMilestone("open", (open_preferences_action(),)),),
        )

    with pytest.raises(ValueError, match="scoped machine identity"):
        SemanticLocator(semantic_action="click whatever looks right")
