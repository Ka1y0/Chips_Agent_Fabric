from __future__ import annotations

import json

from project_supervisor.cli import main
from project_supervisor.store import StateStore


def _read_json(capsys):  # type: ignore[no-untyped-def]
    return json.loads(capsys.readouterr().out)


def test_cli_goal_lifecycle_is_durable_and_audited(tmp_path, capsys) -> None:  # type: ignore[no-untyped-def]
    root = str(tmp_path)
    assert main(["--data-dir", root, "init"]) == 0
    capsys.readouterr()
    assert (
        main(
            [
                "--data-dir",
                root,
                "project",
                "create",
                "--id",
                "project-1",
                "--name",
                "Goal fixture",
                "--root",
                str(tmp_path / "workspace"),
                "--goal",
                "Exercise human controls",
            ]
        )
        == 0
    )
    capsys.readouterr()

    assert (
        main(
            [
                "--data-dir",
                root,
                "--json",
                "goal",
                "create",
                "--project",
                "project-1",
                "--id",
                "goal-1",
                "--intent",
                "Reach verified success",
                "--max-iterations",
                "4",
                "--no-progress-limit",
                "2",
            ]
        )
        == 0
    )
    created = _read_json(capsys)
    assert created["state"] == "created"
    assert created["budgets"]["max_iterations"] == 4
    assert created["event_cursor"] > 0

    assert (
        main(
            [
                "--data-dir",
                root,
                "--json",
                "goal",
                "pause",
                "goal-1",
                "--mode",
                "hard",
                "--reason",
                "checkpoint",
            ]
        )
        == 0
    )
    assert _read_json(capsys)["state"] == "hardPaused"

    assert (
        main(
            [
                "--data-dir",
                root,
                "--json",
                "goal",
                "resume",
                "goal-1",
                "--reason",
                "checkpoint accepted",
            ]
        )
        == 0
    )
    assert _read_json(capsys)["state"] == "running"

    assert (
        main(
            [
                "--data-dir",
                root,
                "--json",
                "goal",
                "steer",
                "goal-1",
                "--instruction",
                "Add independent verification",
                "--priority",
                "90",
            ]
        )
        == 0
    )
    steered = _read_json(capsys)
    assert steered["steer_version"] == 1
    assert "Add independent verification" in steered["effective_intent"]

    assert main(["--data-dir", root, "--json", "goal", "get", "goal-1"]) == 0
    assert _read_json(capsys)["event_cursor"] >= steered["event_cursor"]
    assert main(["--data-dir", root, "--json", "goal", "list"]) == 0
    assert _read_json(capsys)[0]["id"] == "goal-1"
    assert main(["--data-dir", root, "--json", "telemetry", "--goal", "goal-1"]) == 0
    assert _read_json(capsys)["aggregate"]["callCount"] == 0

    assert (
        main(
            [
                "--data-dir",
                root,
                "--json",
                "goal",
                "stop",
                "goal-1",
                "--reason",
                "operator stop",
            ]
        )
        == 0
    )
    stopped = _read_json(capsys)
    assert stopped["state"] == "stopped"
    assert stopped["termination_reason"] == "USER_STOPPED"

    events = StateStore(tmp_path / "supervisor.db").list_events(limit=100)
    controls = {event["kind"]: event for event in events if event["kind"].startswith("goal")}
    assert {"goalCreated", "goalPaused", "goalResumed", "goalSteered", "goalStopped"} <= set(
        controls
    )
    assert all(controls[kind]["actor"] == "human:cli" for kind in controls)
