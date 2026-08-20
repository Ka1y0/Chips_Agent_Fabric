from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from project_supervisor.fabric.interaction import (
    ClosedLoopUIExecutor,
    GroundingNotFound,
    PreconditionFailed,
    SemanticLocator,
    UIAction,
    UIActionKind,
    UICondition,
    UIConditionKind,
    UIElement,
    UIMilestone,
    UIPlan,
    UISnapshot,
)
from project_supervisor.fabric.persistence import (
    InteractionRepository,
    InteractionResourceRepository,
    semantic_snapshot_sha256,
)
from project_supervisor.store import StateStore


class FixtureUI:
    def __init__(self, *, secret: str = "PRIVATE_UI_VALUE") -> None:
        self.state = "home"
        self.observations = 0
        self.secret = secret

    async def observe(self) -> UISnapshot:
        self.observations += 1
        if self.state == "home":
            elements = (
                UIElement(
                    "settings",
                    "button",
                    name="Preferences",
                    value=self.secret,
                    semantic_actions=frozenset({"fixture.open_preferences"}),
                ),
            )
            window = "main"
        else:
            elements = (UIElement("preferences", "dialog", name=self.secret),)
            window = "preferences"
        return UISnapshot(
            f"snapshot-{self.observations}",
            "fixture.app",
            window,
            datetime(2026, 8, 11, 12, 0, self.observations, tzinfo=UTC),
            elements,
            app_version="1.0",
        )

    async def invoke(self, _element: UIElement, semantic_action: str) -> None:
        assert semantic_action == "fixture.open_preferences"
        self.state = "preferences"

    async def press(self, _element: UIElement, _keys: tuple[str, ...]) -> None:
        raise AssertionError("not used")

    async def type_text(self, _element: UIElement, _text: str) -> None:
        raise AssertionError("not used")

    async def wait(self, _condition: UICondition, _timeout_seconds: float) -> None:
        raise AssertionError("not used")


def plan() -> UIPlan:
    target = SemanticLocator(
        semantic_action="fixture.open_preferences",
        stable_id="settings",
        role="button",
        name="Preferences",
    )
    return UIPlan(
        "plan-open-preferences",
        "fixture.open_preferences",
        (
            UIMilestone(
                "open",
                (
                    UIAction(
                        "invoke-settings",
                        UIActionKind.INVOKE,
                        target,
                        semantic_action="fixture.open_preferences",
                        preconditions=(
                            UICondition(UIConditionKind.WINDOW_EQUALS, expected="main"),
                        ),
                        postconditions=(
                            UICondition(UIConditionKind.WINDOW_EQUALS, expected="preferences"),
                        ),
                    ),
                ),
            ),
        ),
    )


def test_ui_graph_semantic_state_ignores_ephemeral_observation_identity() -> None:
    element = UIElement(
        "settings",
        "button",
        name="Preferences",
        semantic_actions=frozenset({"fixture.open_preferences"}),
    )
    first = UISnapshot(
        "observation-a",
        "fixture.app",
        "main",
        datetime(2026, 8, 11, 12, 0, 1, tzinfo=UTC),
        (element,),
        app_version="1.0",
    )
    second = UISnapshot(
        "observation-b",
        "fixture.app",
        "main",
        datetime(2026, 8, 11, 12, 5, 1, tzinfo=UTC),
        (element,),
        app_version="1.0",
    )

    assert semantic_snapshot_sha256(first) == semantic_snapshot_sha256(second)


def test_interaction_resource_bundle_is_atomic_and_generation_fenced(tmp_path) -> None:
    store = StateStore(tmp_path / "state.db")
    resources = InteractionResourceRepository(store)
    resources.register(
        resource_key="desktop:fixture", resource_type="desktopSession", scope_id="fixture"
    )
    resources.register(resource_key="mouse:fixture", resource_type="mouse", scope_id="fixture")
    resources.register(
        resource_key="browser:one", resource_type="browserContext", scope_id="browser-one"
    )
    resources.register(
        resource_key="browser:two", resource_type="browserContext", scope_id="browser-two"
    )

    browser_one = resources.acquire(("browser:one",), owner_id="browser-run-one")
    browser_two = resources.acquire(("browser:two",), owner_id="browser-run-two")
    assert browser_one is not None and browser_two is not None
    assert resources.release(browser_one)
    assert resources.release(browser_two)

    first = resources.acquire(
        ("mouse:fixture", "desktop:fixture"), owner_id="run-one", ttl_seconds=30
    )
    assert first is not None
    assert resources.acquire(("mouse:fixture",), owner_id="run-two") is None

    with store.transaction() as connection:
        connection.execute(
            "UPDATE interaction_resource_leases SET expires_at=? WHERE lease_group_id=?",
            ((datetime.now(UTC) - timedelta(seconds=2)).isoformat(), first.lease_group_id),
        )
    assert (
        resources.acquire(("mouse:fixture", "desktop:fixture"), owner_id="run-two", ttl_seconds=30)
        is None
    )
    assert not resources.is_current(first)
    assert not resources.renew(first)
    assert not resources.release(first)
    with store.connect() as connection:
        assert (
            connection.execute(
                "SELECT COUNT(*) FROM interaction_resource_leases "
                "WHERE lease_group_id=? AND state='active'",
                (first.lease_group_id,),
            ).fetchone()[0]
            == 2
        )

    with pytest.raises(ValueError, match="quiescence"):
        resources.recover_expired(
            first.lease_group_id,
            quiescence_confirmed=False,
            confirmation_actor="recovery-controller",
        )
    assert resources.recover_expired(
        first.lease_group_id,
        quiescence_confirmed=True,
        confirmation_actor="recovery-controller",
    )
    second = resources.acquire(
        ("mouse:fixture", "desktop:fixture"), owner_id="run-two", ttl_seconds=30
    )
    assert second is not None
    assert not resources.is_current(first)
    assert resources.is_current(second)
    assert not resources.release(first)
    assert resources.release(second)


async def test_verified_trajectory_is_durable_without_raw_ui_values(tmp_path) -> None:
    store = StateStore(tmp_path / "state.db")
    resources = InteractionResourceRepository(store)
    resources.register(
        resource_key="desktop:fixture", resource_type="desktopSession", scope_id="fixture"
    )
    repository = InteractionRepository(store, resources)
    adapter = FixtureUI()
    lease = resources.acquire(("desktop:fixture",), owner_id="run-trajectory")
    assert lease is not None
    interaction_plan = plan()
    repository.start_execution(
        execution_id="interaction-trajectory",
        plan=interaction_plan,
        bundle=lease,
        adapter_kind="fixture-semantic-ui/v1",
        channel="dom",
        app_id="fixture.app",
        app_version="1.0",
    )
    result = await ClosedLoopUIExecutor(
        adapter,
        checkpoint=lambda ordinal, phase, action, snapshot, detail: (
            repository.record_action_checkpoint(
                "interaction-trajectory",
                action_ordinal=ordinal,
                phase=phase,
                action=action,
                bundle=lease,
                snapshot=snapshot,
                detail_code=detail,
            )
        ),
    ).execute_plan(interaction_plan)
    trajectory_id = repository.record_plan_result("interaction-trajectory", result, bundle=lease)
    assert trajectory_id.startswith("trajectory-")
    assert resources.release(lease)

    raw_database = (tmp_path / "state.db").read_bytes()
    assert b"PRIVATE_UI_VALUE" not in raw_database
    events = store.list_events(limit=1000)
    assert {event["kind"] for event in events} >= {
        "uiResourceAcquired",
        "uiObserved",
        "uiActionVerified",
        "trajectoryCompleted",
    }


async def test_failed_postcondition_never_creates_trajectory_or_skill(tmp_path) -> None:
    store = StateStore(tmp_path / "state.db")
    resources = InteractionResourceRepository(store)
    resources.register(
        resource_key="desktop:fixture", resource_type="desktopSession", scope_id="fixture"
    )
    lease = resources.acquire(("desktop:fixture",), owner_id="run-failed")
    assert lease is not None
    repository = InteractionRepository(store, resources)
    repository.start_execution(
        execution_id="interaction-failed",
        plan=plan(),
        bundle=lease,
        adapter_kind="fixture-semantic-ui/v1",
        channel="dom",
        app_id="fixture.app",
        app_version="1.0",
    )
    broken = FixtureUI()
    broken.state = "preferences"
    with pytest.raises((GroundingNotFound, PreconditionFailed)):
        await ClosedLoopUIExecutor(broken).execute_plan(plan())
    repository.fail_execution("interaction-failed", bundle=lease, error_code="POSTCONDITION_FAILED")
    with store.connect() as connection:
        trajectory_count = connection.execute(
            "SELECT COUNT(*) FROM interaction_trajectories"
        ).fetchone()[0]
        assert trajectory_count == 0
        assert connection.execute("SELECT COUNT(*) FROM semantic_skills").fetchone()[0] == 0
