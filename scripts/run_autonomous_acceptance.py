#!/usr/bin/env python3
"""Deterministic V0.2 autonomy acceptance, including a real process restart.

The script uses only the provider-neutral Mock Worker. It exercises the actual SQLite store,
Hybrid Engine scheduler, SupervisorRuntime dispatcher, telemetry recorder, and restart recovery.
No provider credentials, network services, or production workspaces are accessed.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import subprocess
import sys
import tempfile
import time
from collections import deque
from pathlib import Path
from typing import Any, Never

from project_supervisor.adapters import MockAdapter, MockBehavior
from project_supervisor.autonomy import (
    ActionResult,
    AutonomousIterationEngine,
    EvaluationDisposition,
    GoalEvaluation,
    GoalPlan,
    GoalService,
    GoalVerification,
    PlannedAction,
    SupervisorRuntimeDispatcher,
)
from project_supervisor.domain import (
    Harness,
    ModelDescriptor,
    NodeState,
    Provider,
    ResourceState,
    WorkerSnapshot,
    WorkerState,
)
from project_supervisor.runtime import AdapterRegistry, SupervisorRuntime
from project_supervisor.scheduler import DeterministicScheduler
from project_supervisor.store import StateStore

PROJECT_ID = "project-autonomy-acceptance"
GOAL_ID = "goal-autonomy-acceptance"
WORKER_ID = "worker-deterministic-acceptance"


class SequenceEvaluator:
    def __init__(self, values: list[GoalEvaluation]) -> None:
        self.values = deque(values)

    async def evaluate(self, context: Any) -> GoalEvaluation:
        del context
        if not self.values:
            raise AssertionError("unexpected evaluator call after persisted checkpoint")
        return self.values.popleft()


class SequenceVerifier:
    def __init__(self, values: list[GoalVerification]) -> None:
        self.values = deque(values)

    async def verify(self, context: Any, results: tuple[ActionResult, ...]) -> GoalVerification:
        del context
        if not results or not self.values:
            raise AssertionError("unexpected verifier state")
        return self.values.popleft()


class IterationPlanner:
    async def plan(self, context: Any, evaluation: GoalEvaluation) -> GoalPlan:
        del evaluation
        sequence = int(context.goal["iteration_count"])
        return GoalPlan(
            summary=f"deterministic plan {sequence}",
            actions=(
                PlannedAction(
                    key=f"work-{sequence}",
                    title=f"Acceptance work {sequence}",
                    description=f"Produce deterministic acceptance result {sequence}",
                    role="primary" if sequence == 1 else "verifier",
                    payload={
                        "labels": ["research"],
                        "requiredCapabilities": ["analysis"],
                    },
                ),
            ),
        )


def _incomplete(fingerprint: str) -> GoalEvaluation:
    return GoalEvaluation(
        EvaluationDisposition.INCOMPLETE,
        "Goal requires another verified action",
        fingerprint,
    )


def _verified(satisfied: bool, fingerprint: str) -> GoalVerification:
    return GoalVerification(
        satisfied,
        "Goal verification passed" if satisfied else "Goal remains incomplete",
        fingerprint,
        evidence={"deterministic": True},
    )


def _store(state_dir: Path, *, create: bool) -> StateStore:
    state_dir.mkdir(parents=True, exist_ok=True)
    workspace = state_dir / "workspace"
    workspace.mkdir(exist_ok=True)
    store = StateStore(state_dir / "supervisor.db")
    if create:
        store.create_project(
            project_id=PROJECT_ID,
            name="Autonomous acceptance",
            root_path=str(workspace),
            goal="Prove persisted autonomous iteration and recovery",
        )
        store.upsert_node(
            node_id="node-acceptance",
            hostname="acceptance.invalid",
            display_name="Acceptance Node",
            role="control",
            state=NodeState.ONLINE,
        )
    return store


def _runtime(store: StateStore, state_dir: Path, *, delay: float = 0.0) -> SupervisorRuntime:
    store.upsert_worker(
        WorkerSnapshot(
            id=WORKER_ID,
            node_id="node-acceptance",
            harness=Harness.MOCK,
            provider=Provider.MOCK,
            model=ModelDescriptor("mock-autonomy-v1", "Mock Autonomy V1", Provider.MOCK),
            state=WorkerState.IDLE,
            node_state=NodeState.ONLINE,
            resource_state=ResourceState.AVAILABLE,
            capabilities=frozenset({"analysis"}),
            code_write_allowed=False,
            privacy_allowed=True,
            reliability_score=1.0,
            expected_latency_seconds=delay,
        )
    )
    registry = AdapterRegistry()
    registry.register(
        WORKER_ID,
        MockAdapter(
            MockBehavior(
                text="DETERMINISTIC_WORKER_RESULT",
                delay_seconds=delay,
                model="mock-autonomy-observed-v1",
            )
        ),
    )
    return SupervisorRuntime(
        store=store,
        scheduler=DeterministicScheduler(),
        adapters=registry,
        evidence_root=state_dir / "run-evidence",
        dispatch_lease_ttl_seconds=1.0,
    )


def _engine(
    store: StateStore,
    runtime: SupervisorRuntime,
    *,
    evaluations: list[GoalEvaluation],
    verifications: list[GoalVerification],
) -> AutonomousIterationEngine:
    return AutonomousIterationEngine(
        store=store,
        evaluator=SequenceEvaluator(evaluations),
        planner=IterationPlanner(),
        dispatcher=SupervisorRuntimeDispatcher(runtime),
        verifier=SequenceVerifier(verifications),
        control_poll_seconds=0.005,
    )


async def _multi_iteration(state_dir: Path) -> dict[str, Any]:
    store = _store(state_dir, create=True)
    runtime = _runtime(store, state_dir)
    goal = GoalService(store).create_goal(
        project_id=PROJECT_ID,
        goal_id=GOAL_ID,
        intent="Complete two independently verified deterministic steps",
    )
    result = await _engine(
        store,
        runtime,
        evaluations=[_incomplete("initial"), _incomplete("after-first")],
        verifications=[_verified(False, "after-first"), _verified(True, "complete")],
    ).run(goal["id"])
    tasks = store.list_tasks(PROJECT_ID)
    telemetry = runtime.telemetry.aggregate(goal_id=GOAL_ID).to_protocol()
    event_kinds = [event["kind"] for event in store.list_events(limit=1000)]
    return {
        "goalState": result["state"],
        "terminationReason": result["termination_reason"],
        "iterations": result["iteration_count"],
        "generatedTasks": result["task_count"],
        "taskStates": sorted(task["state"] for task in tasks),
        "telemetry": telemetry,
        "automaticReplanEvents": event_kinds.count("goalReplanRequired"),
        "pass": (
            result["termination_reason"] == "SUCCESS"
            and result["iteration_count"] == 2
            and result["task_count"] == 2
            and all(task["state"] == "succeeded" for task in tasks)
            and telemetry["callCount"] == 2
            and telemetry["callsByTaskRole"] == {"primary": 1, "verifier": 1}
            and event_kinds.count("goalReplanRequired") == 1
        ),
    }


async def _crash_phase(state_dir: Path) -> Never:
    store = _store(state_dir, create=True)
    runtime = _runtime(store, state_dir, delay=30.0)
    goal = GoalService(store).create_goal(
        project_id=PROJECT_ID,
        goal_id=GOAL_ID,
        intent="Recover one in-flight autonomous action after Supervisor restart",
    )
    running = asyncio.create_task(
        _engine(
            store,
            runtime,
            evaluations=[_incomplete("before-crash")],
            verifications=[_verified(True, "complete")],
        ).run(goal["id"])
    )
    # Process startup can contend with the full-suite subprocess tests. The
    # enclosing child timeout remains authoritative; this poll only establishes
    # that launch crossed the durable ambiguity boundary before the hard crash.
    deadline = time.monotonic() + 12
    while time.monotonic() < deadline:
        runs = store.list_worker_runs()
        if runs and runs[0]["state"] == "running":
            try:
                provider_job = store.get_provider_job(runs[0]["id"])
            except KeyError:
                provider_job = None
            if provider_job is not None and provider_job["launch_state"] != "prepared":
                os._exit(23)
        await asyncio.sleep(0.01)
    running.cancel()
    raise RuntimeError("worker did not reach running state before crash deadline")


async def _resume_phase(state_dir: Path) -> dict[str, Any]:
    store = _store(state_dir, create=False)
    runtime = _runtime(store, state_dir)
    deadline = time.monotonic() + 5
    escalations: list[dict[str, Any]] = []
    while time.monotonic() < deadline:
        await runtime.recover()
        escalations = store.list_execution_escalations(state="open")
        if escalations:
            break
        await asyncio.sleep(0.05)
    result = GoalService(store).get_goal(GOAL_ID)
    runs = store.list_worker_runs()
    tasks = store.list_tasks(PROJECT_ID)
    telemetry = runtime.telemetry.aggregate(goal_id=GOAL_ID).to_protocol()
    outcomes = [row["state"] for row in runs]
    return {
        "goalState": result["state"],
        "terminationReason": result["termination_reason"],
        "iterations": result["iteration_count"],
        "generatedTasks": result["task_count"],
        "taskStates": [row["state"] for row in tasks],
        "workerRunStates": outcomes,
        "escalationCodes": [row["code"] for row in escalations],
        "telemetryOutcomes": telemetry["callsByOutcome"],
        "pass": (
            result["state"] == "running"
            and result["termination_reason"] is None
            and result["iteration_count"] == 1
            and result["task_count"] == 1
            and [row["state"] for row in tasks] == ["waiting"]
            and outcomes == ["running"]
            and [row["code"] for row in escalations] == ["PROVIDER_STATE_AMBIGUOUS"]
            and telemetry["callsByOutcome"] == {}
        ),
    }


def _child_environment() -> dict[str, str]:
    return {
        "PATH": os.environ.get("PATH", ""),
        "PYTHONDONTWRITEBYTECODE": "1",
    }


def _run_all(output: Path) -> int:
    with tempfile.TemporaryDirectory(prefix="chips-autonomy-acceptance-") as temporary:
        root = Path(temporary)
        multi = asyncio.run(_multi_iteration(root / "multi"))
        restart_dir = root / "restart"
        script = Path(__file__).resolve()
        crash = subprocess.run(
            [sys.executable, str(script), "_crash", "--state-dir", str(restart_dir)],
            cwd=script.parents[1],
            env=_child_environment(),
            check=False,
            capture_output=True,
            text=True,
            timeout=15,
        )
        if crash.returncode != 23:
            restart: dict[str, Any] = {
                "pass": False,
                "crashExitCode": crash.returncode,
                "safeError": (crash.stderr or crash.stdout)[-1000:],
            }
        else:
            resumed = subprocess.run(
                [sys.executable, str(script), "_resume", "--state-dir", str(restart_dir)],
                cwd=script.parents[1],
                env=_child_environment(),
                check=False,
                capture_output=True,
                text=True,
                timeout=20,
            )
            if resumed.returncode == 0:
                restart = json.loads(resumed.stdout)
            else:
                restart = {
                    "pass": False,
                    "resumeExitCode": resumed.returncode,
                    "safeError": (resumed.stderr or resumed.stdout)[-1000:],
                }
            restart["crashExitCode"] = crash.returncode
        report = {
            "schemaVersion": "autonomous-acceptance-v1",
            "workerKind": "deterministicMock",
            "networkAccess": False,
            "credentialsAccessed": False,
            "productionWorkspaceModified": False,
            "multiIteration": multi,
            "freshProcessRestart": restart,
            "pass": bool(multi.get("pass") and restart.get("pass")),
        }
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        print(json.dumps(report, sort_keys=True))
        return 0 if report["pass"] else 1


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("mode", choices=("all", "_crash", "_resume"), nargs="?", default="all")
    parser.add_argument("--state-dir", type=Path)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    if args.mode == "_crash":
        if args.state_dir is None:
            parser.error("_crash requires --state-dir")
        asyncio.run(_crash_phase(args.state_dir.resolve()))
        return 2
    if args.mode == "_resume":
        if args.state_dir is None:
            parser.error("_resume requires --state-dir")
        print(json.dumps(asyncio.run(_resume_phase(args.state_dir.resolve())), sort_keys=True))
        return 0
    if args.output is None:
        parser.error("all requires --output")
    return _run_all(args.output.resolve())


if __name__ == "__main__":
    raise SystemExit(main())
