#!/usr/bin/env python3
"""Deterministic V0.2.1 production-host acceptance using real subprocesses.

The acceptance starts the installed CLI in separate OS processes and supplies a tiny, temporary
Claude-compatible executable through the production ``--executable-override`` boundary.  The fake
provider never accesses credentials or a network and exists only inside the temporary acceptance
workspace.  No in-process autonomy engine or production provider quota is used.
"""

from __future__ import annotations

import argparse
import json
import os
import signal
import subprocess
import sys
import tempfile
import time
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from project_supervisor.domain import (
    EvidenceConfidence,
    ExecutionTopology,
    Harness,
    ModelDescriptor,
    NodeState,
    Provider,
    ResourceState,
    TaskLabel,
    TaskRecord,
    TaskRequirements,
    TaskState,
    WorkerSnapshot,
    WorkerState,
)
from project_supervisor.resource_usage import (
    QuotaState,
    ResourceObservation,
    ResourceUsageRepository,
    UsageMetric,
    UsageProvenance,
)
from project_supervisor.store import StateStore

PROJECT_ROOT = Path(__file__).resolve().parents[1]
PROJECT_ID = "project-v021-subprocess-acceptance"
WORKER_EXHAUSTED = "worker-claude-exhausted"
WORKER_AVAILABLE = "worker-claude-fixture"


@dataclass(slots=True)
class Fixture:
    root: Path
    data_dir: Path
    workspace: Path
    executable: Path
    environment: dict[str, str]

    @property
    def store(self) -> StateStore:
        return StateStore(self.data_dir / "supervisor.db")


@dataclass(slots=True)
class HostProcess:
    process: subprocess.Popen[str]
    stdout_handle: Any
    stderr_handle: Any
    stdout_path: Path
    stderr_path: Path

    def stop(self, *, force: bool = False) -> int:
        if self.process.poll() is None:
            if force:
                self.process.kill()
            else:
                self.process.send_signal(signal.SIGTERM)
            try:
                self.process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait(timeout=5)
        self.stdout_handle.close()
        self.stderr_handle.close()
        return int(self.process.returncode or 0)


def _child_environment(root: Path) -> dict[str, str]:
    python_path = os.pathsep.join((str(PROJECT_ROOT / "src"), str(PROJECT_ROOT)))
    return {
        "HOME": str(root / "home"),
        "PATH": os.environ.get("PATH", "/usr/bin:/bin"),
        "PYTHONPATH": python_path,
        "PYTHONDONTWRITEBYTECODE": "1",
        "TMPDIR": str(root / "tmp"),
        "NO_COLOR": "1",
    }


def _cli(fixture: Fixture, *arguments: str, timeout: float = 15) -> Any:
    command = [
        sys.executable,
        "-m",
        "project_supervisor.cli",
        "--data-dir",
        str(fixture.data_dir),
        "--json",
        *arguments,
    ]
    completed = subprocess.run(
        command,
        cwd=PROJECT_ROOT,
        env=fixture.environment,
        check=False,
        capture_output=True,
        text=True,
        timeout=timeout,
    )
    if completed.returncode != 0:
        safe = (completed.stderr or completed.stdout)[-2000:]
        raise AssertionError(f"CLI failed ({completed.returncode}): {' '.join(arguments)}\n{safe}")
    return json.loads(completed.stdout) if completed.stdout.strip() else None


def _write_fake_claude(path: Path, *, delay_seconds: float) -> None:
    source = f"""#!/usr/bin/env python3
import json
import os
import sys
import time

args = sys.argv[1:]
try:
    prompt = args[args.index("-p") + 1]
except (ValueError, IndexError):
    print("missing -p", file=sys.stderr)
    raise SystemExit(2)

time.sleep({delay_seconds!r})
marker = "Canonical bounded input:\\n"
if marker not in prompt:
    output = "FAKE_CLAUDE_ACTION_OK"
else:
    request = json.loads(prompt.split(marker, 1)[1])
    kind = request["decisionType"]
    sequence = request["iterationSequence"]
    base = {{
        "schemaVersion": "autonomy-decision/v1",
        "decisionType": kind,
        "goalID": request["goalID"],
        "iterationSequence": sequence,
        "steerVersion": request["steerVersion"],
    }}
    if kind == "evaluation":
        value = {{**base, "disposition": "incomplete", "summary": "work remains",
            "progressFingerprint": f"evaluation-{{sequence}}", "terminationReason": None,
            "facts": {{"source": "deterministic-subprocess"}}}}
    elif kind == "plan":
        effective = request["context"]["goal"].get("effective_intent") or ""
        description = (
            "STEER_APPLIED: complete revised deterministic work"
            if "acceptance-steer" in effective
            else f"Complete deterministic action {{sequence}}"
        )
        action = {{"key": f"work-{{sequence}}", "title": f"Work {{sequence}}",
            "description": description, "role": "primary", "payload": {{
                "topology": "single", "priority": 50, "permissionClass": "green",
                "labels": ["research"], "requiredCapabilities": [],
                "minimumContextTokens": None, "privacySensitive": False,
                "codeWriteRequired": False, "panelSize": 2, "preferredWorkers": []}}}}
        value = {{**base, "summary": "bounded next action", "rationale": "goal incomplete",
            "actions": [action]}}
    else:
        satisfied = sequence >= 2
        value = {{**base, "satisfied": satisfied,
            "summary": "verified" if satisfied else "follow-up required",
            "progressFingerprint": f"verification-{{sequence}}", "terminationReason": None,
            "evidence": {{"resultCount": len(request.get("actionResults", []))}}}}
    output = json.dumps(value, separators=(",", ":"), sort_keys=True)

session = f"fake-session-{{os.getpid()}}"
usage = {{"input_tokens": 7, "output_tokens": max(1, len(output) // 16),
    "cache_read_input_tokens": 0}}
print(json.dumps({{"type": "system", "subtype": "init", "session_id": session}}), flush=True)
print(json.dumps({{"type": "assistant", "session_id": session, "message": {{
    "model": "claude-deterministic-fixture", "content": [{{"type": "text", "text": output}}],
    "usage": usage}}}}), flush=True)
print(json.dumps({{"type": "result", "subtype": "success", "session_id": session,
    "result": output, "total_cost_usd": 0.0, "usage": usage}}), flush=True)
"""
    path.write_text(source, encoding="utf-8")
    path.chmod(0o700)


def _fixture(root: Path, *, delay_seconds: float = 0.20) -> Fixture:
    root.mkdir(parents=True)
    for directory in (root / "home", root / "tmp", root / "workspace"):
        directory.mkdir()
    executable = root / "fake-claude"
    _write_fake_claude(executable, delay_seconds=delay_seconds)
    fixture = Fixture(
        root=root,
        data_dir=root / "state",
        workspace=root / "workspace",
        executable=executable,
        environment=_child_environment(root),
    )
    _cli(fixture, "init")
    _cli(
        fixture,
        "project",
        "create",
        "--id",
        PROJECT_ID,
        "--name",
        "V0.2.1 subprocess acceptance",
        "--root",
        str(fixture.workspace),
        "--goal",
        "Exercise the production autonomous host",
    )
    _seed_workers_and_quota(fixture.store)
    return fixture


def _seed_workers_and_quota(store: StateStore) -> None:
    store.upsert_node(
        node_id="node-v021-acceptance",
        hostname="fixture.invalid",
        display_name="V0.2.1 fixture",
        role="worker",
        state=NodeState.ONLINE,
    )
    for worker_id, quality in ((WORKER_EXHAUSTED, 1.0), (WORKER_AVAILABLE, 0.2)):
        store.upsert_worker(
            WorkerSnapshot(
                id=worker_id,
                node_id="node-v021-acceptance",
                harness=Harness.CLAUDE_CODE,
                provider=Provider.ANTHROPIC,
                model=ModelDescriptor(
                    "claude-deterministic-fixture",
                    "Deterministic fixture",
                    Provider.ANTHROPIC,
                    context_window_tokens=32_000,
                ),
                state=WorkerState.IDLE,
                node_state=NodeState.ONLINE,
                resource_state=ResourceState.AVAILABLE,
                capabilities=frozenset({"reasoning"}),
                code_write_allowed=False,
                privacy_allowed=True,
                quality_score=quality,
                reliability_score=1.0,
                expected_latency_seconds=0.1,
            )
        )

    task_id = "task-quota-fixture"
    store.create_task(
        TaskRecord(
            id=task_id,
            project_id=PROJECT_ID,
            title="Quota fixture",
            description="Persist deterministic exhausted-pool evidence",
            state=TaskState.DRAFT,
            topology=ExecutionTopology.SINGLE,
            requirements=TaskRequirements(labels=frozenset({TaskLabel.RESEARCH})),
        ),
        reference="QUOTA-FIXTURE",
    )
    store.transition_task(task_id, TaskState.QUEUED)
    terminal = store.transition_task(task_id, TaskState.FAILED)
    observed_at = datetime.now(UTC)
    ResourceUsageRepository(store).record_audit(
        audit_id="resource-audit-quota-fixture",
        audit_key=f"task:{task_id}:version:{terminal['version']}:state:failed",
        task_id=task_id,
        run_id=None,
        terminal_state="failed",
        observations=(
            ResourceObservation(
                provider="anthropic",
                quota_pool_id=f"local:anthropic:worker:{WORKER_EXHAUSTED}",
                worker_id=WORKER_EXHAUSTED,
                used=UsageMetric.known(100, "requests", UsageProvenance.PROVIDER_REPORTED),
                remaining=UsageMetric.known(0, "requests", UsageProvenance.PROVIDER_REPORTED),
                quota_state=QuotaState.EXHAUSTED,
                quota_state_provenance=UsageProvenance.PROVIDER_REPORTED,
                source="deterministicAcceptanceFixture",
                confidence=EvidenceConfidence.PROVIDER_REPORTED,
                observed_at=observed_at,
                fresh_until=observed_at + timedelta(hours=1),
            ),
        ),
        observer_count=0,
        errors=(),
        observed_at=observed_at,
    )


def _create_goal(fixture: Fixture, goal_id: str, intent: str) -> dict[str, Any]:
    return _cli(
        fixture,
        "goal",
        "create",
        "--project",
        PROJECT_ID,
        "--id",
        goal_id,
        "--intent",
        intent,
        "--max-iterations",
        "5",
        "--max-tasks",
        "12",
        "--no-progress-limit",
        "3",
    )


def _start_host(fixture: Fixture, host_id: str) -> HostProcess:
    stdout_path = fixture.root / f"{host_id}-stdout.log"
    stderr_path = fixture.root / f"{host_id}-stderr.log"
    stdout_handle = stdout_path.open("w", encoding="utf-8")
    stderr_handle = stderr_path.open("w", encoding="utf-8")
    command = [
        sys.executable,
        "-m",
        "project_supervisor.cli",
        "--data-dir",
        str(fixture.data_dir),
        "--json",
        "autonomous",
        "serve",
        "--host-id",
        host_id,
        "--max-concurrent-goals",
        "1",
        "--poll-interval-seconds",
        "0.02",
        "--heartbeat-interval-seconds",
        "0.05",
        "--lease-ttl-seconds",
        "0.30",
        "--shutdown-grace-seconds",
        "1.0",
        "--executable-override",
        f"claudeCode={fixture.executable}",
    ]
    process = subprocess.Popen(
        command,
        cwd=PROJECT_ROOT,
        env=fixture.environment,
        stdin=subprocess.DEVNULL,
        stdout=stdout_handle,
        stderr=stderr_handle,
        text=True,
    )
    return HostProcess(process, stdout_handle, stderr_handle, stdout_path, stderr_path)


def _wait(description: str, condition: Callable[[], Any], *, timeout: float = 15) -> Any:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        value = condition()
        if value:
            return value
        time.sleep(0.02)
    raise AssertionError(f"timed out waiting for {description}")


def _goal(fixture: Fixture, goal_id: str) -> dict[str, Any]:
    try:
        return fixture.store.get_goal(goal_id)
    except AttributeError:
        from project_supervisor.autonomy import GoalService

        return GoalService(fixture.store).get_goal(goal_id)


def _goal_run(
    fixture: Fixture, goal_id: str, *, action_only: bool = False
) -> dict[str, Any] | None:
    query = (
        "SELECT run.* FROM worker_runs run JOIN autonomous_actions action "
        "ON action.task_id=run.task_id WHERE action.goal_id=? AND run.state='running' "
        "ORDER BY run.created_at DESC LIMIT 1"
        if action_only
        else "SELECT run.* FROM worker_runs run JOIN ("
        "SELECT task_id FROM autonomous_actions WHERE goal_id=? "
        "UNION SELECT task_id FROM autonomy_decision_tasks WHERE goal_id=?"
        ") owned ON owned.task_id=run.task_id WHERE run.state='running' "
        "ORDER BY run.created_at DESC LIMIT 1"
    )
    values = (goal_id,) if action_only else (goal_id, goal_id)
    with fixture.store.connect() as connection:
        row = connection.execute(query, values).fetchone()
    return dict(row) if row is not None else None


def _action_task_states(fixture: Fixture, goal_id: str) -> list[str]:
    with fixture.store.connect() as connection:
        rows = connection.execute(
            "SELECT task.state FROM tasks task JOIN autonomous_actions action "
            "ON action.task_id=task.id WHERE action.goal_id=? ORDER BY task.created_at",
            (goal_id,),
        ).fetchall()
    return [str(row["state"]) for row in rows]


def _launched_action_run(fixture: Fixture, goal_id: str) -> dict[str, Any] | None:
    run = _goal_run(fixture, goal_id, action_only=True)
    if run is None:
        return None
    try:
        job = fixture.store.get_provider_job(run["id"])
    except KeyError:
        return None
    return run if job["launch_state"] != "prepared" else None


def _event_count(fixture: Fixture, *, kind: str, goal_id: str | None = None) -> int:
    with fixture.store.connect() as connection:
        if goal_id is None:
            row = connection.execute(
                "SELECT COUNT(*) AS n FROM events WHERE kind=?", (kind,)
            ).fetchone()
        else:
            row = connection.execute(
                "SELECT COUNT(*) AS n FROM events WHERE kind=? AND (entity_id=? OR "
                "json_extract(payload_json,'$.goalID')=?)",
                (kind, goal_id, goal_id),
            ).fetchone()
    return int(row["n"])


def _host_exists(fixture: Fixture, host_id: str) -> bool:
    with fixture.store.connect() as connection:
        row = connection.execute(
            "SELECT 1 FROM autonomous_hosts WHERE host_id=?", (host_id,)
        ).fetchone()
    return row is not None


def _terminal_success(fixture: Fixture, goal_id: str) -> dict[str, Any] | None:
    goal = _goal(fixture, goal_id)
    return goal if goal["termination_reason"] == "SUCCESS" else None


def _concurrent_and_resources(root: Path) -> dict[str, Any]:
    fixture = _fixture(root)
    goal_id = "goal-concurrent-success"
    _create_goal(fixture, goal_id, "Complete two verified deterministic iterations")
    first = _start_host(fixture, "host-concurrent-a")
    second = _start_host(fixture, "host-concurrent-b")
    duplicate: HostProcess | None = None
    try:
        _wait(
            "both production hosts",
            lambda: len(_cli(fixture, "autonomous", "status")["hosts"]) >= 2,
        )
        duplicate = _start_host(fixture, "host-concurrent-a")
        _wait(
            "duplicate host identity rejection",
            lambda: duplicate.process.poll() is not None,
            timeout=5,
        )
        duplicate_exit = duplicate.stop()
        goal = _wait("multi-iteration success", lambda: _terminal_success(fixture, goal_id))
        status = _cli(fixture, "autonomous", "status", "--goal", goal_id)
        resources = _cli(fixture, "resources", "--goal", goal_id)
        telemetry = _cli(fixture, "telemetry", "--goal", goal_id)
        with fixture.store.connect() as connection:
            tasks = connection.execute(
                "SELECT task.id,task.state FROM tasks task WHERE task.id IN ("
                "SELECT task_id FROM autonomous_actions WHERE goal_id=? UNION "
                "SELECT task_id FROM autonomy_decision_tasks WHERE goal_id=?)",
                (goal_id, goal_id),
            ).fetchall()
            audit_count = int(
                connection.execute(
                    "SELECT COUNT(*) AS n FROM post_task_usage_audits WHERE goal_id=?", (goal_id,)
                ).fetchone()["n"]
            )
            runs = connection.execute(
                "SELECT DISTINCT worker_id FROM worker_runs run WHERE run.task_id IN ("
                "SELECT task_id FROM autonomous_actions WHERE goal_id=? UNION "
                "SELECT task_id FROM autonomy_decision_tasks WHERE goal_id=?)",
                (goal_id, goal_id),
            ).fetchall()
            exhausted_rejections = int(
                connection.execute(
                    "SELECT COUNT(*) AS n FROM routing_candidates WHERE worker_id=? "
                    "AND rejection_code='QUOTA_GUARD'",
                    (WORKER_EXHAUSTED,),
                ).fetchone()["n"]
            )
        snapshots = resources["snapshots"]
        unknown_preserved = any(
            item["remaining"]["provenance"] == "UNKNOWN"
            and item["quotaStateProvenance"] == "UNKNOWN"
            for item in snapshots
        )
        return {
            "goalState": goal["state"],
            "terminationReason": goal["termination_reason"],
            "iterations": goal["iteration_count"],
            "generatedActionTasks": goal["task_count"],
            "hostCount": len(status["hosts"]),
            "duplicateHostIdentityExitCode": duplicate_exit,
            "leaseCount": len(status["goalLeases"]),
            "leaseAcquisitions": _event_count(
                fixture, kind="autonomousGoalLeaseAcquired", goal_id=goal_id
            ),
            "terminalTaskCount": sum(
                row["state"] in {"succeeded", "failed", "cancelled"} for row in tasks
            ),
            "postTaskAuditCount": audit_count,
            "resourceSnapshotCount": len(snapshots),
            "unknownProvenancePreserved": unknown_preserved,
            "quotaGuardExhaustedRejections": exhausted_rejections,
            "workerIDsUsed": sorted(row["worker_id"] for row in runs),
            "telemetryCalls": telemetry["aggregate"]["callCount"],
            "pass": (
                goal["termination_reason"] == "SUCCESS"
                and goal["iteration_count"] == 2
                and goal["task_count"] == 2
                and len(tasks) >= 8
                and audit_count == len(tasks)
                and unknown_preserved
                and exhausted_rejections > 0
                and {row["worker_id"] for row in runs} == {WORKER_AVAILABLE}
                and duplicate_exit != 0
                and _event_count(fixture, kind="autonomousGoalLeaseAcquired", goal_id=goal_id) == 1
            ),
        }
    finally:
        if duplicate is not None and duplicate.process.poll() is None:
            duplicate.stop(force=True)
        first.stop()
        second.stop()


def _controls(root: Path) -> dict[str, Any]:
    fixture = _fixture(root)
    host = _start_host(fixture, "host-controls")
    try:
        _wait(
            "control host startup",
            lambda: _host_exists(fixture, "host-controls"),
        )

        soft_id = "goal-soft-pause"
        _create_goal(fixture, soft_id, "Exercise soft pause and resume")
        _wait("soft-pause action", lambda: _goal_run(fixture, soft_id, action_only=True))
        paused = _cli(fixture, "goal", "pause", soft_id, "--mode", "soft", "--reason", "acceptance")
        _wait(
            "soft action completion",
            lambda: _goal_run(fixture, soft_id, action_only=True) is None,
        )
        before = fixture.store.list_worker_runs()
        time.sleep(0.35)
        after = fixture.store.list_worker_runs()
        soft_no_new_work = (
            len(before) == len(after) and _goal(fixture, soft_id)["state"] == "softPaused"
        )
        _cli(fixture, "goal", "resume", soft_id, "--reason", "acceptance")
        soft_done = _wait("soft resumed success", lambda: _terminal_success(fixture, soft_id))

        hard_id = "goal-hard-pause"
        _create_goal(fixture, hard_id, "Exercise hard pause cancellation and resume")
        active_hard = _wait(
            "hard-pause action", lambda: _goal_run(fixture, hard_id, action_only=True)
        )
        hard_paused = _cli(
            fixture, "goal", "pause", hard_id, "--mode", "hard", "--reason", "acceptance"
        )
        _wait(
            "hard cancellation",
            lambda: any(
                row["state"] == "cancelled" and row["id"] == active_hard["id"]
                for row in fixture.store.list_worker_runs()
            ),
        )
        _cli(fixture, "goal", "resume", hard_id, "--reason", "acceptance")
        hard_done = _wait("hard resumed success", lambda: _terminal_success(fixture, hard_id))

        steer_id = "goal-live-steer"
        _create_goal(fixture, steer_id, "Exercise live steer with valid work preservation")
        _wait("steer first action", lambda: _goal_run(fixture, steer_id, action_only=True))
        steered = _cli(
            fixture,
            "goal",
            "steer",
            steer_id,
            "--instruction",
            "acceptance-steer: apply the revised priority",
            "--priority",
            "90",
        )
        steer_done = _wait("steered goal success", lambda: _terminal_success(fixture, steer_id))
        with fixture.store.connect() as connection:
            steered_actions = connection.execute(
                "SELECT description FROM autonomous_actions WHERE goal_id=? ORDER BY created_at",
                (steer_id,),
            ).fetchall()

        stop_id = "goal-user-stop"
        _create_goal(fixture, stop_id, "Exercise durable user stop")
        _wait("stop action", lambda: _goal_run(fixture, stop_id, action_only=True))
        stopped = _cli(fixture, "goal", "stop", stop_id, "--reason", "acceptance stop")
        _wait("stopped Goal settled", lambda: _goal_run(fixture, stop_id, action_only=True) is None)
        stopped_durable = _goal(fixture, stop_id)

        return {
            "softPauseState": paused["state"],
            "softPauseNoNewDispatch": soft_no_new_work,
            "softResumeTermination": soft_done["termination_reason"],
            "hardPauseState": hard_paused["state"],
            "hardCancellationRunID": active_hard["id"],
            "hardResumeTermination": hard_done["termination_reason"],
            "steerVersion": steered["steer_version"],
            "steerTermination": steer_done["termination_reason"],
            "steeredPlanObserved": any(
                "STEER_APPLIED" in row["description"] for row in steered_actions
            ),
            "stopState": stopped["state"],
            "stopTermination": stopped_durable["termination_reason"],
            "pass": (
                paused["state"] == "softPaused"
                and soft_no_new_work
                and soft_done["termination_reason"] == "SUCCESS"
                and hard_paused["state"] == "hardPaused"
                and hard_done["termination_reason"] == "SUCCESS"
                and steered["steer_version"] == 1
                and steer_done["termination_reason"] == "SUCCESS"
                and any("STEER_APPLIED" in row["description"] for row in steered_actions)
                and stopped["state"] == "stopped"
                and stopped_durable["termination_reason"] == "USER_STOPPED"
            ),
        }
    finally:
        host.stop()


def _restart(root: Path) -> dict[str, Any]:
    fixture = _fixture(root, delay_seconds=0.35)
    goal_id = "goal-crash-recovery"
    _create_goal(fixture, goal_id, "Recover production autonomy after a host crash")
    crashed = _start_host(fixture, "host-before-crash")
    recovered: HostProcess | None = None
    try:
        active = _wait(
            "pre-crash autonomous action",
            lambda: _launched_action_run(fixture, goal_id),
        )
        crash_code = crashed.stop(force=True)
        time.sleep(0.45)
        recovered = _start_host(fixture, "host-after-crash")
        escalation = _wait(
            "post-crash unsupported-resume escalation",
            lambda: next(
                (
                    item
                    for item in fixture.store.list_execution_escalations(state="open")
                    if item["goal_id"] == goal_id
                ),
                None,
            ),
            timeout=20,
        )
        _wait(
            "post-crash action parked for reconciliation",
            lambda: (
                states
                if (states := _action_task_states(fixture, goal_id))
                and all(state == TaskState.WAITING.value for state in states)
                else None
            ),
            timeout=20,
        )
        goal = _goal(fixture, goal_id)
        with fixture.store.connect() as connection:
            run_states = [
                row["state"]
                for row in connection.execute(
                    "SELECT run.state FROM worker_runs run JOIN autonomous_actions action "
                    "ON action.task_id=run.task_id WHERE action.goal_id=? ORDER BY run.created_at",
                    (goal_id,),
                ).fetchall()
            ]
            action_task_states = _action_task_states(fixture, goal_id)
            recovered_events = int(
                connection.execute(
                    "SELECT COUNT(*) AS n FROM events WHERE kind='autonomousGoalLeaseRecovered' "
                    "AND json_extract(payload_json,'$.previousHostID')='host-before-crash'"
                ).fetchone()["n"]
            )
        return {
            "interruptedWorkerPID": active["process_id"],
            "crashedHostExitCode": crash_code,
            "goalState": goal["state"],
            "goalTermination": goal["termination_reason"],
            "actionRunStates": run_states,
            "actionTaskStates": action_task_states,
            "escalationCode": escalation["code"],
            "staleLeaseRecoveryEvents": recovered_events,
            "pass": (
                crash_code != 0
                and goal["state"] == "running"
                and goal["termination_reason"] is None
                and run_states == ["running"]
                and action_task_states == ["waiting"]
                and escalation["code"] == "PROVIDER_STATE_AMBIGUOUS"
                and recovered_events >= 1
            ),
        }
    finally:
        if crashed.process.poll() is None:
            crashed.stop(force=True)
        if recovered is not None:
            recovered.stop()


def _run_all(output: Path) -> int:
    with tempfile.TemporaryDirectory(prefix="chips-autonomous-host-v021-") as temporary:
        root = Path(temporary)
        concurrent = _concurrent_and_resources(root / "concurrent")
        controls = _controls(root / "controls")
        restart = _restart(root / "restart")
        report = {
            "schemaVersion": "autonomous-host-acceptance-v0.2.1",
            "productionCLI": True,
            "realHostSubprocesses": True,
            "realWorkerSubprocesses": True,
            "workerFixture": "temporary-claude-compatible-no-network",
            "credentialsAccessed": False,
            "networkAccess": False,
            "productionWorkspaceModified": False,
            "concurrentMultiIteration": concurrent,
            "humanControls": controls,
            "freshProcessRecovery": restart,
            "pass": bool(concurrent["pass"] and controls["pass"] and restart["pass"]),
        }
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        print(json.dumps(report, sort_keys=True))
        return 0 if report["pass"] else 1


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    return _run_all(args.output.resolve())


if __name__ == "__main__":
    raise SystemExit(main())
