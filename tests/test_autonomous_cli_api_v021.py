from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta

from fastapi.testclient import TestClient

from project_supervisor.api import create_app
from project_supervisor.autonomous_host import AutonomousHostRepository
from project_supervisor.autonomy import GoalService
from project_supervisor.cli import build_parser, main
from project_supervisor.domain import (
    EvidenceConfidence,
    ExecutionTopology,
    TaskLabel,
    TaskRecord,
    TaskRequirements,
    TaskState,
)
from project_supervisor.resource_usage import (
    QuotaState,
    ResourceObservation,
    ResourceUsageRepository,
    ResourceUsageService,
    UsageMetric,
    UsageProvenance,
)
from project_supervisor.store import StateStore


def _fixture(tmp_path):  # type: ignore[no-untyped-def]
    store = StateStore(tmp_path / "supervisor.db")
    store.create_project(
        project_id="project-1",
        name="V0.2.1 observability",
        root_path=str(tmp_path),
        goal="Observe hosts and resources",
    )
    goal = GoalService(store).create_goal(
        project_id="project-1", intent="Reach verified success", goal_id="goal-1"
    )
    repository = AutonomousHostRepository(store)
    repository.register_host("host-1", process_id=4321, metadata={"maxConcurrentGoals": 2})
    claim = repository.try_acquire_goal("goal-1", "host-1", lease_ttl_seconds=30)
    assert claim is not None

    task = TaskRecord(
        id="task-1",
        project_id="project-1",
        title="Usage fixture",
        description="Record exact locally measured calls",
        state=TaskState.DRAFT,
        topology=ExecutionTopology.SINGLE,
        requirements=TaskRequirements(labels=frozenset({TaskLabel.RESEARCH})),
    )
    store.create_task(task, "#0001")
    for state in (TaskState.QUEUED, TaskState.READY, TaskState.RUNNING, TaskState.FAILED):
        store.transition_task(task.id, state)
    observed = datetime.now(UTC)
    ResourceUsageService(ResourceUsageRepository(store)).audit_task_terminal(
        task_id="task-1",
        run_id=None,
        terminal_state="failed",
        observations=(
            ResourceObservation(
                provider="local",
                quota_pool_id="pool-local",
                task_calls=UsageMetric.known(1, "calls", UsageProvenance.LOCALLY_MEASURED),
                quota_state=QuotaState.AVAILABLE,
                quota_state_provenance=UsageProvenance.PROVIDER_REPORTED,
                source="fixtureStatus",
                confidence=EvidenceConfidence.EXACT,
                observed_at=observed,
                fresh_until=observed + timedelta(minutes=5),
            ),
        ),
    )
    return store, goal


def test_cli_exposes_autonomous_host_and_resource_observability(tmp_path, capsys) -> None:  # type: ignore[no-untyped-def]
    assert main(["--data-dir", str(tmp_path), "init"]) == 0
    capsys.readouterr()
    _fixture(tmp_path)

    assert (
        main(
            [
                "--data-dir",
                str(tmp_path),
                "--json",
                "autonomous",
                "status",
                "--host",
                "host-1",
                "--goal",
                "goal-1",
            ]
        )
        == 0
    )
    status = json.loads(capsys.readouterr().out)
    assert status["hosts"][0]["host_id"] == "host-1"
    assert status["goalLeases"][0]["generation"] == 1

    assert (
        main(
            [
                "--data-dir",
                str(tmp_path),
                "--json",
                "resources",
                "--provider",
                "local",
            ]
        )
        == 0
    )
    resources = json.loads(capsys.readouterr().out)
    assert resources["aggregate"]["taskCalls"]["valuesByUnit"] == {"calls": 1}
    assert resources["snapshots"][0]["taskLocal"]["calls"]["provenance"] == "LOCALLY_MEASURED"


def test_production_host_cli_requires_explicit_mock_opt_in_and_has_no_token_flag() -> None:
    parser = build_parser()
    args = parser.parse_args(
        [
            "autonomous",
            "run",
            "goal-1",
            "--allow-mock-worker",
            "--executable-override",
            "worker-1=/opt/reviewed/claude",
        ]
    )
    assert args.autonomous_command == "run"
    assert args.allow_mock_worker is True
    assert args.executable_override == ["worker-1=/opt/reviewed/claude"]
    assert not hasattr(args, "local_worker_token")
    assert not hasattr(args, "bearer")


def test_api_projects_hosts_leases_and_normalized_resources(tmp_path) -> None:  # type: ignore[no-untyped-def]
    store, _goal = _fixture(tmp_path)
    client = TestClient(
        create_app(store, allow_unauthenticated_loopback=True),
        client=("127.0.0.1", 50000),
    )

    host = client.get("/v1/autonomy/hosts/host-1").json()["data"]
    assert host["hostID"] == "host-1"
    assert host["activeGoalCount"] == 0
    lease = client.get(
        "/v1/autonomy/leases", params={"goalID": "goal-1", "ownedOnly": True}
    ).json()["data"][0]
    assert lease["hostID"] == "host-1"
    assert lease["recoveryState"] == "fresh"

    snapshots = client.get("/v1/resources/snapshots", params={"provider": "local"}).json()["data"]
    assert snapshots[0]["quotaState"] == "available"
    assert snapshots[0]["remaining"]["state"] == "unavailable"
    aggregate = client.get("/v1/resources/aggregate", params={"quotaPoolID": "pool-local"}).json()[
        "data"
    ]
    assert aggregate["snapshotCount"] == 1
    assert aggregate["taskCalls"]["knownCount"] == 1

    capabilities = client.get("/v1/capabilities").json()["data"]
    assert "autonomousHosts" in capabilities["resources"]
    assert "resourceUsageSnapshots" in capabilities["resources"]
    quota_events = client.get("/v1/events", params={"kind": "quotaChanged"}).json()["data"]
    assert any(event["kind"] == "quotaChanged" for event in quota_events)

    invalid_range = client.get(
        "/v1/resources/aggregate",
        params={
            "observedAfter": "2026-08-10T00:00:00Z",
            "observedBefore": "2026-08-09T00:00:00Z",
        },
    )
    assert invalid_range.status_code == 400
