from __future__ import annotations

import json
from copy import deepcopy
from dataclasses import asdict, is_dataclass
from pathlib import Path
from typing import Any

from fastapi.testclient import TestClient
from jsonschema import Draft202012Validator

from project_supervisor.api import APISettings, create_app
from project_supervisor.store import StateStore, timestamp


class FakeGoalService:
    """Small provider-neutral contract fixture; core semantics are tested separately."""

    def __init__(self, store: StateStore) -> None:
        self.store = store
        self.rows: dict[str, dict[str, Any]] = {}

    def _return(self, row: dict[str, Any]) -> dict[str, Any]:
        value = deepcopy(row)
        value["event_cursor"] = self.store.highest_event_sequence()
        return value

    def create_goal(
        self,
        *,
        project_id: str,
        intent: str,
        budgets: Any = None,
        goal_id: str | None = None,
        actor: str,
    ) -> dict[str, Any]:
        now = timestamp()
        identifier = goal_id or "goal-generated"
        budget_values = (
            asdict(budgets)
            if budgets is not None and is_dataclass(budgets)
            else {
                "max_iterations": 12,
                "max_tasks": 48,
                "max_failures": 6,
                "no_progress_limit": 3,
                "max_elapsed_seconds": None,
                "max_total_tokens": None,
                "max_cost_usd": None,
            }
        )
        row = {
            "id": identifier,
            "project_id": project_id,
            "intent": intent,
            "effective_intent": intent,
            "state": "created",
            "pause_mode": None,
            "termination_reason": None,
            "termination_detail": None,
            "iteration_count": 0,
            "no_progress_count": 0,
            "task_count": 0,
            "failure_count": 0,
            "budgets": budget_values,
            "steer_version": 0,
            "version": 1,
            "created_at": now,
            "updated_at": now,
            "started_at": None,
            "last_evaluated_at": None,
            "finished_at": None,
            "actor": actor,
        }
        self.rows[identifier] = row
        return self._return(row)

    def list_goals(self, *, project_id: str | None = None) -> list[dict[str, Any]]:
        return [
            self._return(row)
            for row in self.rows.values()
            if project_id is None or row["project_id"] == project_id
        ]

    def get_goal(self, goal_id: str) -> dict[str, Any]:
        if goal_id not in self.rows:
            raise KeyError(goal_id)
        return self._return(self.rows[goal_id])

    def pause(
        self,
        goal_id: str,
        mode: str,
        *,
        reason: str | None,
        actor: str,
    ) -> dict[str, Any]:
        row = self.get_goal(goal_id)
        row.update(
            state=f"{mode}Paused",
            pause_mode=mode,
            termination_detail=reason,
            actor=actor,
        )
        self.rows[goal_id] = row
        return self._return(row)

    def resume(
        self,
        goal_id: str,
        *,
        reason: str | None,
        actor: str,
    ) -> dict[str, Any]:
        row = self.get_goal(goal_id)
        row.update(state="running", pause_mode=None, termination_detail=reason, actor=actor)
        self.rows[goal_id] = row
        return self._return(row)

    def steer(
        self,
        goal_id: str,
        instruction: str,
        *,
        priority: int | None,
        preserve_valid_work: bool,
        actor: str,
    ) -> dict[str, Any]:
        row = self.get_goal(goal_id)
        row.update(
            effective_intent=instruction,
            steer_version=row["steer_version"] + 1,
            actor=actor,
            priority=priority,
            preserve_valid_work=preserve_valid_work,
        )
        self.rows[goal_id] = row
        return self._return(row)

    def stop(self, goal_id: str, *, reason: str, actor: str) -> dict[str, Any]:
        row = self.get_goal(goal_id)
        row.update(
            state="stopped",
            pause_mode=None,
            termination_reason="USER_STOPPED",
            termination_detail=reason,
            actor=actor,
        )
        self.rows[goal_id] = row
        return self._return(row)


def _fixture(tmp_path):  # type: ignore[no-untyped-def]
    store = StateStore(tmp_path / "goals-api.db")
    store.create_project(
        project_id="project-1",
        name="Goal fixture",
        root_path=str(tmp_path / "project"),
        goal="Exercise authenticated Goal controls",
    )
    service = FakeGoalService(store)
    app = create_app(
        store,
        APISettings(
            bind_host="127.0.0.1",
            allow_unauthenticated_loopback=True,
            allow_goal_mutations=True,
        ),
        goal_service=service,
    )
    client = TestClient(app, client=("127.0.0.1", 50000))
    return store, service, client


def test_goal_mutations_require_control_bearer_even_in_loopback_dev(tmp_path) -> None:  # type: ignore[no-untyped-def]
    store, _service, client = _fixture(tmp_path)
    payload = {"projectID": "project-1", "goalID": "goal-1", "intent": "Reach proof"}

    assert client.get("/v1/goals").status_code == 200
    assert client.post("/v1/goals", json=payload).status_code == 401

    _, observe = store.issue_api_token(label="observer", scopes={"observe:read"})
    response = client.post(
        "/v1/goals",
        json=payload,
        headers={"Authorization": f"Bearer {observe}"},
    )
    assert response.status_code == 403

    _, control = store.issue_api_token(label="operator", scopes={"goal:control"})
    response = client.post(
        "/v1/goals",
        json=payload,
        headers={"Authorization": f"Bearer {control}"},
    )
    assert response.status_code == 201
    assert response.json()["data"]["id"] == "goal-1"


def test_goal_controls_round_trip_durable_projection(tmp_path) -> None:  # type: ignore[no-untyped-def]
    store, _service, client = _fixture(tmp_path)
    _, token = store.issue_api_token(label="operator", scopes={"goal:control"})
    headers = {"Authorization": f"Bearer {token}"}
    created = client.post(
        "/v1/goals",
        json={
            "projectID": "project-1",
            "goalID": "goal-1",
            "intent": "Produce verified output",
            "budgets": {"maxIterations": 4, "noProgressLimit": 2},
        },
        headers=headers,
    ).json()["data"]
    assert created["state"] == "created"
    assert created["budgets"]["maxIterations"] == 4
    assert created["eventCursor"] >= 0

    paused = client.post(
        "/v1/goals/goal-1/pause",
        json={"mode": "hard", "reason": "operator checkpoint"},
        headers=headers,
    ).json()["data"]
    assert (paused["state"], paused["pauseMode"]) == ("hardPaused", "hard")

    resumed = client.post(
        "/v1/goals/goal-1/resume",
        json={"reason": "review complete"},
        headers=headers,
    ).json()["data"]
    assert resumed["state"] == "running"

    steered = client.post(
        "/v1/goals/goal-1/steer",
        json={"instruction": "Preserve evidence and add a verifier", "priority": 90},
        headers=headers,
    ).json()["data"]
    assert steered["steerVersion"] == 1
    assert steered["effectiveIntent"] == "Preserve evidence and add a verifier"

    stopped = client.post(
        "/v1/goals/goal-1/stop",
        json={"reason": "operator accepted current state"},
        headers=headers,
    ).json()["data"]
    assert stopped["state"] == "stopped"
    assert stopped["terminationReason"] == "USER_STOPPED"

    observed = client.get("/v1/goals/goal-1").json()["data"]
    assert observed == stopped


def test_goal_mutations_are_disabled_by_default_even_with_control_scope(tmp_path) -> None:  # type: ignore[no-untyped-def]
    store = StateStore(tmp_path / "disabled.db")
    service = FakeGoalService(store)
    _, token = store.issue_api_token(label="operator", scopes={"goal:control"})
    client = TestClient(
        create_app(store, goal_service=service, allow_unauthenticated_loopback=True),
        client=("127.0.0.1", 50000),
    )
    response = client.post(
        "/v1/goals",
        json={"projectID": "project-1", "intent": "Must not start"},
        headers={"Authorization": f"Bearer {token}"},
    )
    assert response.status_code == 403
    assert response.json()["code"] == "goal_mutations_disabled"


def test_goal_and_control_json_schemas_validate_wire_examples(tmp_path) -> None:  # type: ignore[no-untyped-def]
    store, _service, client = _fixture(tmp_path)
    _, token = store.issue_api_token(label="operator", scopes={"goal:control"})
    response = client.post(
        "/v1/goals",
        json={"projectID": "project-1", "goalID": "goal-1", "intent": "Verify schemas"},
        headers={"Authorization": f"Bearer {token}"},
    )
    root = Path(__file__).parents[1]
    goal_schema = json.loads((root / "schemas/goal-v1.schema.json").read_text(encoding="utf-8"))
    control_schema = json.loads(
        (root / "schemas/goal-control-v1.schema.json").read_text(encoding="utf-8")
    )
    Draft202012Validator.check_schema(goal_schema)
    Draft202012Validator.check_schema(control_schema)
    Draft202012Validator(goal_schema).validate(response.json()["data"])
    Draft202012Validator(control_schema).validate(
        {
            "action": "steer",
            "instruction": "Keep existing evidence",
            "priority": 75,
            "preserveValidWork": True,
        }
    )


def test_real_goal_service_api_journals_controls_and_reports_conflicts(tmp_path) -> None:  # type: ignore[no-untyped-def]
    store = StateStore(tmp_path / "real-goals-api.db")
    store.create_project(
        project_id="project-1",
        name="Real API fixture",
        root_path=str(tmp_path / "project"),
        goal="Verify real GoalService integration",
    )
    _, token = store.issue_api_token(label="operator", scopes={"goal:control"})
    headers = {"Authorization": f"Bearer {token}"}
    client = TestClient(
        create_app(
            store,
            APISettings(
                bind_host="127.0.0.1",
                allow_unauthenticated_loopback=True,
                allow_goal_mutations=True,
            ),
        ),
        client=("127.0.0.1", 50000),
    )
    created = client.post(
        "/v1/goals",
        json={"projectID": "project-1", "goalID": "goal-real", "intent": "Finish safely"},
        headers=headers,
    )
    assert created.status_code == 201
    created_cursor = created.json()["data"]["eventCursor"]

    conflict = client.post(
        "/v1/goals/goal-real/resume",
        json={},
        headers=headers,
    )
    assert conflict.status_code == 409
    assert conflict.json()["code"] == "goal_control_conflict"

    paused = client.post(
        "/v1/goals/goal-real/pause",
        json={"mode": "soft", "reason": "inspect state"},
        headers=headers,
    ).json()["data"]
    assert paused["eventCursor"] > created_cursor
    assert paused["state"] == "softPaused"
    assert client.get("/v1/goals/missing").status_code == 404

    events = store.list_events(after_sequence=created_cursor, limit=20)
    pause_event = next(event for event in events if event["kind"] == "goalPaused")
    assert pause_event["actor"] == "human:api"
    replay = client.get("/v1/events", params={"goal": "goal-real"}).json()["data"]
    assert {event["kind"] for event in replay} >= {"goalCreated", "goalPaused"}
    assert all(event["goalID"] == "goal-real" for event in replay)


def test_goal_projection_exposes_current_plan_generated_tasks_and_verifier(tmp_path) -> None:  # type: ignore[no-untyped-def]
    store = StateStore(tmp_path / "goal-execution.db")
    store.create_project(
        project_id="project-1",
        name="Goal execution projection",
        root_path=str(tmp_path / "project"),
        goal="Expose canonical autonomy progress",
    )
    client = TestClient(
        create_app(store, allow_unauthenticated_loopback=True),
        client=("127.0.0.1", 50000),
    )
    from project_supervisor.autonomy import GoalService

    GoalService(store).create_goal(
        project_id="project-1", goal_id="goal-plan", intent="Show the current plan"
    )
    now = timestamp()
    plan = {
        "summary": "Review the evidence",
        "rationale": "One bounded verifier is required",
        "actions": [],
    }
    verification = {
        "satisfied": False,
        "summary": "One follow-up remains",
        "progressFingerprint": "checkpoint-2",
        "terminationReason": None,
        "evidence": {"passed": 3},
    }
    with store.transaction() as connection:
        connection.execute(
            "INSERT INTO autonomous_iterations("
            "id,goal_id,sequence,state,plan_json,verification_json,started_at,updated_at) "
            "VALUES (?,?,?,?,?,?,?,?)",
            (
                "iter-plan",
                "goal-plan",
                1,
                "verifying",
                json.dumps(plan),
                json.dumps(verification),
                now,
                now,
            ),
        )
        connection.execute(
            "INSERT INTO autonomous_actions("
            "id,goal_id,iteration_id,ordinal,action_key,title,description,role,payload_json,"
            "state,task_id,created_at,updated_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                "act-review",
                "goal-plan",
                "iter-plan",
                0,
                "review",
                "Review evidence",
                "Check the retained acceptance evidence",
                "verifier",
                "{}",
                "completed",
                None,
                now,
                now,
            ),
        )

    value = client.get("/v1/goals/goal-plan").json()["data"]

    assert value["currentPlan"] == {
        "iterationID": "iter-plan",
        "sequence": 1,
        "state": "verifying",
        "summary": "Review the evidence",
        "rationale": "One bounded verifier is required",
    }
    assert value["generatedTasks"][0]["taskID"] is None
    assert value["generatedTasks"][0]["role"] == "verifier"
    assert value["latestVerifier"]["summary"] == "One follow-up remains"
    assert value["latestVerifier"]["progressFingerprint"] == "checkpoint-2"
    root = Path(__file__).parents[1]
    schema = json.loads((root / "schemas/goal-v1.schema.json").read_text(encoding="utf-8"))
    Draft202012Validator(schema).validate(value)
