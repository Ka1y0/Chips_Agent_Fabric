from __future__ import annotations

from datetime import UTC, datetime, timedelta

from .interaction import (
    InteractionError,
    UICondition,
    UIElement,
    UISnapshot,
    UISource,
)


class DeterministicSemanticUIFixture:
    """A stateful local semantic UI used for offline Fabric acceptance.

    It models a DOM-like application contract without pretending to control a real browser or OS.
    Every transition is explicit, deterministic, and inspectable.
    """

    adapter_id = "fixture-semantic-ui/v1"

    def __init__(
        self,
        *,
        app_id: str = "fixture.app",
        app_version: str = "1.0",
        grounding_confidence: float = 1.0,
    ) -> None:
        if not app_id.strip() or not app_version.strip():
            raise ValueError("fixture app identity and version must not be empty")
        if not 0 <= grounding_confidence <= 1:
            raise ValueError("fixture grounding confidence must be between zero and one")
        self.app_id = app_id
        self.app_version = app_version
        self.grounding_confidence = grounding_confidence
        self.state = "home"
        self.observation_count = 0
        self.invocations: list[tuple[str, str]] = []
        self.presses: list[tuple[str, tuple[str, ...]]] = []
        self.typed: list[tuple[str, str]] = []
        self.wait_count = 0
        self._clock = datetime(2026, 8, 11, tzinfo=UTC)

    async def observe(self) -> UISnapshot:
        self.observation_count += 1
        if self.state == "home":
            window_id = "fixture.main"
            focus_id = "fixture.settings"
            elements = (
                UIElement(
                    "fixture.main",
                    "window",
                    name="Fixture",
                    semantic_actions=frozenset({"fixture.window"}),
                    source=UISource.DOM,
                ),
                UIElement(
                    "fixture.settings",
                    "button",
                    name="Preferences",
                    actions=frozenset({"invoke"}),
                    semantic_actions=frozenset({"fixture.open_preferences"}),
                    accessibility_id="fixture.settings",
                    dom_locator='[data-semantic-action="fixture.open_preferences"]',
                    confidence=self.grounding_confidence,
                    source=UISource.DOM,
                ),
            )
        else:
            window_id = "fixture.preferences"
            focus_id = "fixture.preferences"
            elements = (
                UIElement(
                    "fixture.preferences",
                    "dialog",
                    name="Preferences",
                    semantic_actions=frozenset({"fixture.preferences"}),
                    source=UISource.DOM,
                ),
            )
        return UISnapshot(
            snapshot_id=f"fixture.snapshot.{self.observation_count}",
            app_id=self.app_id,
            app_version=self.app_version,
            window_id=window_id,
            timestamp=self._clock + timedelta(seconds=self.observation_count),
            elements=elements,
            focus_element_id=focus_id,
        )

    async def invoke(self, element: UIElement, semantic_action: str) -> None:
        if (
            self.state != "home"
            or element.element_id != "fixture.settings"
            or semantic_action != "fixture.open_preferences"
        ):
            raise InteractionError("fixture rejected an unsupported semantic invocation")
        self.invocations.append((element.element_id, semantic_action))
        self.state = "preferences"

    async def press(self, element: UIElement, keys: tuple[str, ...]) -> None:
        if not keys:
            raise InteractionError("fixture key invocation cannot be empty")
        self.presses.append((element.element_id, keys))

    async def type_text(self, element: UIElement, text: str) -> None:
        self.typed.append((element.element_id, text))

    async def wait(self, condition: UICondition, timeout_seconds: float) -> None:
        if timeout_seconds <= 0:
            raise InteractionError("fixture wait must be bounded")
        self.wait_count += 1
        if not condition.evaluate(await self.observe()):
            raise InteractionError("fixture wait condition was not satisfied")
