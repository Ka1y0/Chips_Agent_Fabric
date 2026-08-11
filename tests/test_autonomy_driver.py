from __future__ import annotations

import asyncio
import hashlib
import json
import sys
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

import pytest
from jsonschema import Draft202012Validator

from project_supervisor.adapters import ClaudeAdapter, CodexAdapter, LocalWorkerAdapter, MockAdapter
from project_supervisor.adapters.base import WorkerRequest
from project_supervisor.adapters.native import NativeSubprocessAdapter, ParsedOutput
from project_supervisor.autonomy import (
    ActionResult,
    AutonomousIterationEngine,
    EvaluationDisposition,
    GoalService,
    SupervisorRuntimeDispatcher,
    TerminationReason,
)
from project_supervisor.autonomy_driver import (
    AdapterReconstructionError,
    ProductionAutonomyDriver,
    reconstruct_adapter_registry,
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
from project_supervisor.store import StateStore, timestamp


class ScriptDecisionAdapter(NativeSubprocessAdapter):
    """Test-only native adapter: real subprocess, zero provider/network quota."""

    def __init__(self, script: Path, mode: str = "normal") -> None:
        super().__init__(sys.executable, heartbeat_seconds=0.01)
        self.script = script
        self.mode = mode

    def command_arguments(self, request: WorkerRequest) -> Sequence[str]:
        return (str(self.script), self.mode, request.prompt)

    def consume_stdout_line(self, line: str, parsed: ParsedOutput) -> Mapping[str, Any]:
        parsed.final_text = line.strip()
        parsed.provider_event_types.add("json")
        return {"providerEventType": "json"}


@pytest.fixture
def decision_script(tmp_path: Path) -> Path:
    script = tmp_path / "decision_worker.py"
    script.write_text(
        """
import json
import sys
import time

mode, prompt = sys.argv[1], sys.argv[2]
marker = "Canonical bounded input:\\n"
if marker not in prompt:
    print("WORK_ACTION_OK")
    raise SystemExit(0)
request = json.loads(prompt.split(marker, 1)[1])
kind = request["decisionType"]
base = {
    "schemaVersion": "autonomy-decision/v1",
    "decisionType": kind,
    "goalID": request["goalID"],
    "iterationSequence": request["iterationSequence"],
    "steerVersion": request["steerVersion"],
}
if mode == "malformed":
    print("not-json")
elif mode == "slow-success":
    time.sleep(0.15)
    print(json.dumps({**base, "disposition": "satisfied", "summary": "done",
        "progressFingerprint": "done", "terminationReason": None, "facts": {}}))
elif kind == "evaluation":
    disposition = "satisfied" if request["iterationSequence"] > 1 else "incomplete"
    fingerprint = f"evaluation-{request['iterationSequence']}"
    termination_reason = None
    if mode == "satisfied-success-long-fingerprint":
        disposition = "satisfied"
        fingerprint = "σ" + ("x" * 300)
        termination_reason = "SUCCESS"
    elif mode == "long-fingerprint":
        fingerprint = "σ" + ("x" * 300)
    elif mode == "empty-fingerprint":
        fingerprint = ""
    elif mode == "wrong-type-fingerprint":
        fingerprint = {"not": "a string"}
    elif mode == "incomplete-success":
        disposition = "incomplete"
        termination_reason = "SUCCESS"
    elif mode == "terminate-success":
        disposition = "terminate"
        termination_reason = "SUCCESS"
    elif mode == "satisfied-lowercase-success":
        disposition = "satisfied"
        termination_reason = "success"
    elif mode == "satisfied-wrong-type-reason":
        disposition = "satisfied"
        termination_reason = {"not": "a string"}
    elif mode == "satisfied-blocked":
        disposition = "satisfied"
        termination_reason = "BLOCKED"
    print(json.dumps({**base, "disposition": disposition, "summary": disposition,
        "progressFingerprint": fingerprint,
        "terminationReason": termination_reason, "facts": {"source": "test-subprocess"}}))
elif kind == "plan":
    action = {"key": "work-1", "title": "Do bounded work",
        "description": "Return a deterministic work marker", "role": "primary",
        "payload": {"topology": "single", "priority": 50,
            "permissionClass": "green", "labels": ["research"],
            "requiredCapabilities": [], "minimumContextTokens": None,
            "privacySensitive": False, "codeWriteRequired": False,
            "panelSize": 2, "preferredWorkers": []}}
    if mode == "empty-plan":
        print(json.dumps({**base, "summary": "none", "rationale": "none", "actions": []}))
    elif mode == "self-approved-red":
        action["payload"]["permissionClass"] = "red"
        action["payload"]["approvalState"] = "approved"
        print(json.dumps({**base, "summary": "unsafe", "rationale": "unsafe",
            "actions": [action]}))
    else:
        print(json.dumps({**base, "summary": "one action", "rationale": "incomplete",
            "actions": [action]}))
else:
    fingerprint = "work-observed"
    if mode == "long-fingerprint":
        fingerprint = "σ" + ("x" * 300)
    satisfied = request["iterationSequence"] > 1 and not request.get("actionResults")
    print(json.dumps({**base, "satisfied": satisfied,
        "summary": "verified" if satisfied else "follow up",
        "progressFingerprint": fingerprint, "terminationReason": None,
        "evidence": {"resultCount": len(request.get("actionResults", []))}}))
""".strip()
        + "\n",
        encoding="utf-8",
    )
    return script


def _store(tmp_path: Path, *, harness: Harness = Harness.CLAUDE_CODE) -> StateStore:
    tmp_path.mkdir(parents=True, exist_ok=True)
    store = StateStore(tmp_path / "state.db")
    store.create_project(
        project_id="project-1",
        name="Decision driver fixture",
        root_path=str(tmp_path),
        goal="exercise production decisions",
    )
    store.upsert_node(
        node_id="node-1",
        hostname="fixture",
        display_name="Fixture",
        role="worker",
        state=NodeState.ONLINE,
    )
    providers = {
        Harness.CODEX: Provider.OPENAI,
        Harness.CLAUDE_CODE: Provider.ANTHROPIC,
        Harness.GROK_BUILD: Provider.XAI,
        Harness.GOOGLE_AGY: Provider.GOOGLE,
        Harness.LOCAL_WORKER: Provider.LOCAL,
        Harness.MOCK: Provider.MOCK,
    }
    provider = providers[harness]
    store.upsert_worker(
        WorkerSnapshot(
            id="worker-1",
            node_id="node-1",
            harness=harness,
            provider=provider,
            model=ModelDescriptor("fixture", "Fixture", provider, context_window_tokens=32_000),
            state=WorkerState.IDLE,
            node_state=NodeState.ONLINE,
            resource_state=ResourceState.AVAILABLE,
            capabilities=frozenset({"reasoning"}),
            code_write_allowed=True,
            privacy_allowed=True,
        )
    )
    return store


def _runtime(
    store: StateStore, adapter: ScriptDecisionAdapter, tmp_path: Path
) -> SupervisorRuntime:
    registry = AdapterRegistry()
    registry.register("worker-1", adapter)
    return SupervisorRuntime(
        store=store,
        scheduler=DeterministicScheduler(),
        adapters=registry,
        evidence_root=tmp_path / "evidence",
        max_attempts=1,
    )


def _insert_iteration(store: StateStore, goal_id: str, sequence: int = 1) -> str:
    iteration_id = f"iter-{goal_id}-{sequence}"
    now = timestamp()
    with store.transaction() as connection:
        connection.execute(
            "UPDATE autonomous_goals SET state='running',iteration_count=?,started_at=?,"
            "updated_at=? WHERE id=?",
            (sequence, now, now, goal_id),
        )
        connection.execute(
            "INSERT INTO autonomous_iterations(id,goal_id,sequence,state,started_at,updated_at) "
            "VALUES (?,?,?,?,?,?)",
            (iteration_id, goal_id, sequence, "evaluating", now, now),
        )
    return iteration_id


@pytest.mark.asyncio
async def test_real_subprocess_decisions_drive_multi_iteration_goal_to_success(
    tmp_path: Path, decision_script: Path
) -> None:
    store = _store(tmp_path)
    runtime = _runtime(store, ScriptDecisionAdapter(decision_script), tmp_path)
    driver = ProductionAutonomyDriver(runtime)
    engine = AutonomousIterationEngine(
        store=store,
        evaluator=driver,
        planner=driver,
        dispatcher=SupervisorRuntimeDispatcher(runtime),
        verifier=driver,
        control_poll_seconds=0.005,
    )
    goal = GoalService(store).create_goal(project_id="project-1", intent="Complete two passes")

    result = await engine.run(goal["id"])

    assert result["termination_reason"] == TerminationReason.SUCCESS.value
    assert result["iteration_count"] == 2
    with store.connect() as connection:
        bindings = connection.execute(
            "SELECT phase,iteration_sequence,steer_version,status FROM autonomy_decision_tasks "
            "WHERE goal_id=? ORDER BY iteration_sequence,created_at",
            (goal["id"],),
        ).fetchall()
    assert [(row["phase"], row["iteration_sequence"]) for row in bindings] == [
        ("evaluation", 1),
        ("planning", 1),
        ("verification", 1),
        ("evaluation", 2),
        ("verification", 2),
    ]
    assert all(row["status"] == "accepted" for row in bindings)
    with store.connect() as connection:
        decision_task_ids = {
            row["task_id"]
            for row in connection.execute("SELECT task_id FROM autonomy_decision_tasks").fetchall()
        }
    assert decision_task_ids
    assert all(store.get_task(task_id)["state"] == "succeeded" for task_id in decision_task_ids)
    with store.connect() as connection:
        audited_task_ids = {
            row["task_id"]
            for row in connection.execute("SELECT task_id FROM post_task_usage_audits").fetchall()
        }
        planning_prompt = connection.execute(
            "SELECT task.description FROM tasks task JOIN autonomy_decision_tasks decision "
            "ON decision.task_id=task.id WHERE decision.phase='planning' LIMIT 1"
        ).fetchone()["description"]
        verification_prompt = connection.execute(
            "SELECT task.description FROM tasks task JOIN autonomy_decision_tasks decision "
            "ON decision.task_id=task.id WHERE decision.phase='verification' LIMIT 1"
        ).fetchone()["description"]
    assert decision_task_ids <= audited_task_ids
    assert "exactly key, title, description, role, and payload" in planning_prompt
    assert "single, primaryReviewer, parallelPanel" in planning_prompt
    assert "workerInventory" in planning_prompt
    assert "instead of inventing a capability" in planning_prompt
    assert "do not return more actions" in planning_prompt
    assert "when only one Worker is eligible" in planning_prompt
    assert "at most 800 words" in planning_prompt
    assert "priority MUST be a JSON integer from 0 through 100" in planning_prompt
    assert "panelSize MUST be a JSON integer from 1 through 8" in planning_prompt
    assert "progressFingerprint MUST be" in verification_prompt
    assert "at most 256 characters" in verification_prompt
    assert any(
        run["process_id"] is not None
        for run in store.list_worker_runs()
        if run["task_id"] in decision_task_ids
    )


@pytest.mark.asyncio
async def test_malformed_evaluator_fails_closed_to_human_escalation(
    tmp_path: Path, decision_script: Path
) -> None:
    store = _store(tmp_path)
    runtime = _runtime(store, ScriptDecisionAdapter(decision_script, "malformed"), tmp_path)
    driver = ProductionAutonomyDriver(runtime)
    engine = AutonomousIterationEngine(
        store=store,
        evaluator=driver,
        planner=driver,
        dispatcher=SupervisorRuntimeDispatcher(runtime),
        verifier=driver,
    )
    goal = GoalService(store).create_goal(project_id="project-1", intent="Fail closed")

    result = await engine.run(goal["id"])

    assert result["termination_reason"] == TerminationReason.HUMAN_ESCALATION.value
    with store.connect() as connection:
        binding = connection.execute("SELECT * FROM autonomy_decision_tasks").fetchone()
    assert binding["status"] == "rejected"
    assert "not strict JSON" in binding["error"]


@pytest.mark.asyncio
async def test_overlong_progress_fingerprint_is_deterministically_normalized_and_audited(
    tmp_path: Path, decision_script: Path
) -> None:
    store = _store(tmp_path)
    runtime = _runtime(store, ScriptDecisionAdapter(decision_script, "long-fingerprint"), tmp_path)
    driver = ProductionAutonomyDriver(runtime)
    goal = GoalService(store).create_goal(project_id="project-1", intent="Normalize identity")
    _insert_iteration(store, goal["id"])
    context = AutonomousIterationEngine(
        store=store,
        evaluator=driver,
        planner=driver,
        dispatcher=SupervisorRuntimeDispatcher(runtime),
        verifier=driver,
    )._context(goal["id"])

    original = "σ" + ("x" * 300)
    expected = f"sha256:{hashlib.sha256(original.encode('utf-8')).hexdigest()}"
    evaluation = await driver.evaluate(context)
    verification = await driver.verify(context, (ActionResult(True, "work complete"),))

    assert evaluation.progress_fingerprint == expected
    assert verification.progress_fingerprint == expected
    events = [
        event
        for event in store.list_events(limit=500)
        if event["kind"] == "autonomyDecisionNormalized"
    ]
    assert len(events) == 2
    assert {event["payload"]["phase"] for event in events} == {"evaluation", "verification"}
    for event in events:
        payload = event["payload"]
        assert payload["method"] == "sha256-exact-utf8"
        assert payload["originalCharacterCount"] == len(original)
        assert payload["originalUTF8ByteCount"] == len(original.encode("utf-8"))
        assert payload["originalSha256"] == expected.removeprefix("sha256:")
        assert payload["normalizedFingerprint"] == expected
        assert original not in json.dumps(payload)
        with store.connect() as connection:
            raw_output = connection.execute(
                "SELECT result.summary FROM worker_results result "
                "JOIN worker_runs run ON run.id=result.run_id WHERE run.task_id=?",
                (event["task_id"],),
            ).fetchone()["summary"]
            binding = connection.execute(
                "SELECT output_sha256,status FROM autonomy_decision_tasks WHERE task_id=?",
                (event["task_id"],),
            ).fetchone()
        assert payload["rawOutputSha256"] == hashlib.sha256(raw_output.encode()).hexdigest()
        assert binding["output_sha256"] == payload["rawOutputSha256"]
        assert binding["status"] == "accepted"


@pytest.mark.asyncio
async def test_exact_satisfied_success_reason_is_normalized_alongside_overlong_fingerprint(
    tmp_path: Path, decision_script: Path
) -> None:
    store = _store(tmp_path)
    runtime = _runtime(
        store,
        ScriptDecisionAdapter(decision_script, "satisfied-success-long-fingerprint"),
        tmp_path,
    )
    driver = ProductionAutonomyDriver(runtime)
    goal = GoalService(store).create_goal(project_id="project-1", intent="Accept exact success")
    _insert_iteration(store, goal["id"])
    context = AutonomousIterationEngine(
        store=store,
        evaluator=driver,
        planner=driver,
        dispatcher=SupervisorRuntimeDispatcher(runtime),
        verifier=driver,
    )._context(goal["id"])

    evaluation = await driver.evaluate(context)

    original = "σ" + ("x" * 300)
    assert evaluation.disposition is EvaluationDisposition.SATISFIED
    assert evaluation.termination_reason is None
    assert evaluation.progress_fingerprint == (
        f"sha256:{hashlib.sha256(original.encode('utf-8')).hexdigest()}"
    )
    events = [
        event
        for event in store.list_events(limit=500)
        if event["kind"] == "autonomyDecisionNormalized"
    ]
    assert {(event["payload"]["field"], event["payload"]["method"]) for event in events} == {
        ("progressFingerprint", "sha256-exact-utf8"),
        ("terminationReason", "satisfied-success-implies-null"),
    }
    termination_event = next(
        event for event in events if event["payload"]["field"] == "terminationReason"
    )
    assert set(termination_event["payload"]) == {
        "schemaVersion",
        "phase",
        "rawOutputSha256",
        "field",
        "method",
    }
    with store.connect() as connection:
        binding = connection.execute(
            "SELECT decision.output_sha256,decision.status,result.summary "
            "FROM autonomy_decision_tasks decision "
            "JOIN worker_runs run ON run.task_id=decision.task_id "
            "JOIN worker_results result ON result.run_id=run.id"
        ).fetchone()
    assert binding["status"] == "accepted"
    assert binding["output_sha256"] == hashlib.sha256(binding["summary"].encode()).hexdigest()


@pytest.mark.parametrize(
    "mode",
    [
        "incomplete-success",
        "terminate-success",
        "satisfied-lowercase-success",
        "satisfied-wrong-type-reason",
        "satisfied-blocked",
    ],
)
@pytest.mark.asyncio
async def test_contradictory_evaluation_termination_variants_remain_fail_closed(
    tmp_path: Path, decision_script: Path, mode: str
) -> None:
    store = _store(tmp_path)
    runtime = _runtime(store, ScriptDecisionAdapter(decision_script, mode), tmp_path)
    driver = ProductionAutonomyDriver(runtime)
    goal = GoalService(store).create_goal(project_id="project-1", intent="Reject contradiction")
    _insert_iteration(store, goal["id"])
    context = AutonomousIterationEngine(
        store=store,
        evaluator=driver,
        planner=driver,
        dispatcher=SupervisorRuntimeDispatcher(runtime),
        verifier=driver,
    )._context(goal["id"])

    evaluation = await driver.evaluate(context)

    assert evaluation.disposition is EvaluationDisposition.TERMINATE
    assert evaluation.termination_reason is TerminationReason.HUMAN_ESCALATION
    with store.connect() as connection:
        binding = connection.execute("SELECT status,error FROM autonomy_decision_tasks").fetchone()
    assert binding["status"] == "rejected"
    assert binding["error"]
    assert not any(
        event["kind"] == "autonomyDecisionNormalized" for event in store.list_events(limit=500)
    )


@pytest.mark.parametrize("mode", ["empty-fingerprint", "wrong-type-fingerprint"])
@pytest.mark.asyncio
async def test_invalid_progress_fingerprint_still_fails_closed_without_normalization(
    tmp_path: Path, decision_script: Path, mode: str
) -> None:
    store = _store(tmp_path)
    runtime = _runtime(store, ScriptDecisionAdapter(decision_script, mode), tmp_path)
    driver = ProductionAutonomyDriver(runtime)
    goal = GoalService(store).create_goal(project_id="project-1", intent="Reject invalid identity")
    _insert_iteration(store, goal["id"])
    context = AutonomousIterationEngine(
        store=store,
        evaluator=driver,
        planner=driver,
        dispatcher=SupervisorRuntimeDispatcher(runtime),
        verifier=driver,
    )._context(goal["id"])

    evaluation = await driver.evaluate(context)

    assert evaluation.disposition is EvaluationDisposition.TERMINATE
    assert evaluation.termination_reason is TerminationReason.HUMAN_ESCALATION
    with store.connect() as connection:
        binding = connection.execute("SELECT status,error FROM autonomy_decision_tasks").fetchone()
    assert binding["status"] == "rejected"
    assert binding["error"]
    assert not any(
        event["kind"] == "autonomyDecisionNormalized" for event in store.list_events(limit=500)
    )


@pytest.mark.parametrize("mode", ["empty-plan", "self-approved-red"])
@pytest.mark.asyncio
async def test_malformed_or_self_approved_plan_enters_safe_no_progress(
    tmp_path: Path, decision_script: Path, mode: str
) -> None:
    store = _store(tmp_path)
    runtime = _runtime(store, ScriptDecisionAdapter(decision_script, mode), tmp_path)
    driver = ProductionAutonomyDriver(runtime)
    engine = AutonomousIterationEngine(
        store=store,
        evaluator=driver,
        planner=driver,
        dispatcher=SupervisorRuntimeDispatcher(runtime),
        verifier=driver,
    )
    goal = GoalService(store).create_goal(project_id="project-1", intent="No unsafe plans")

    result = await engine.run(goal["id"])

    assert result["termination_reason"] == TerminationReason.NO_PROGRESS.value
    assert result["task_count"] == 0


@pytest.mark.asyncio
async def test_newer_steer_prevents_stale_evaluation_success(
    tmp_path: Path, decision_script: Path
) -> None:
    store = _store(tmp_path)
    runtime = _runtime(store, ScriptDecisionAdapter(decision_script, "slow-success"), tmp_path)
    driver = ProductionAutonomyDriver(runtime)
    service = GoalService(store)
    goal = service.create_goal(project_id="project-1", intent="Honor live steer")
    _insert_iteration(store, goal["id"])
    context = AutonomousIterationEngine(
        store=store,
        evaluator=driver,
        planner=driver,
        dispatcher=SupervisorRuntimeDispatcher(runtime),
        verifier=driver,
    )._context(goal["id"])

    pending = asyncio.create_task(driver.evaluate(context))
    await asyncio.sleep(0.04)
    service.steer(goal["id"], "New acceptance requirement")
    evaluation = await pending

    assert evaluation.disposition is EvaluationDisposition.INCOMPLETE
    assert evaluation.progress_fingerprint == "steer-pending-1"


@pytest.mark.asyncio
async def test_decision_task_identity_is_idempotent_for_same_checkpoint(
    tmp_path: Path, decision_script: Path
) -> None:
    store = _store(tmp_path)
    runtime = _runtime(store, ScriptDecisionAdapter(decision_script), tmp_path)
    driver = ProductionAutonomyDriver(runtime)
    goal = GoalService(store).create_goal(project_id="project-1", intent="Reuse checkpoint")
    _insert_iteration(store, goal["id"])
    context = AutonomousIterationEngine(
        store=store,
        evaluator=driver,
        planner=driver,
        dispatcher=SupervisorRuntimeDispatcher(runtime),
        verifier=driver,
    )._context(goal["id"])

    first = await driver.evaluate(context)
    second = await driver.evaluate(context)

    assert first == second
    with store.connect() as connection:
        assert connection.execute("SELECT count(*) FROM autonomy_decision_tasks").fetchone()[0] == 1
        prompt = connection.execute(
            "SELECT description FROM tasks WHERE reference LIKE 'DECISION-%'"
        ).fetchone()["description"]
        assert "exactly one of: incomplete, satisfied, terminate" in prompt
        assert "do not use synonyms such as continue" in prompt
        assert "facts MUST be a JSON object" in prompt
        assert "at most 256 characters" in prompt
        assert (
            connection.execute(
                "SELECT count(*) FROM tasks WHERE reference LIKE 'DECISION-%'"
            ).fetchone()[0]
            == 1
        )

    other = GoalService(store).create_goal(project_id="project-1", intent="Other Goal")
    _insert_iteration(store, other["id"])
    other_context = AutonomousIterationEngine(
        store=store,
        evaluator=driver,
        planner=driver,
        dispatcher=SupervisorRuntimeDispatcher(runtime),
        verifier=driver,
    )._context(other["id"])
    await driver.evaluate(other_context)
    with store.connect() as connection:
        references = [
            row["reference"]
            for row in connection.execute(
                "SELECT reference FROM tasks WHERE reference LIKE 'DECISION-%'"
            ).fetchall()
        ]
    assert len(references) == len(set(references)) == 2


@pytest.mark.asyncio
async def test_concurrent_decision_tasks_wait_for_shared_worker_capacity(
    tmp_path: Path, decision_script: Path
) -> None:
    store = _store(tmp_path)
    runtime = _runtime(store, ScriptDecisionAdapter(decision_script, "slow-success"), tmp_path)
    driver = ProductionAutonomyDriver(runtime)
    engine = AutonomousIterationEngine(
        store=store,
        evaluator=driver,
        planner=driver,
        dispatcher=SupervisorRuntimeDispatcher(runtime),
        verifier=driver,
    )
    service = GoalService(store)
    goals = [
        service.create_goal(project_id="project-1", intent=f"Shared capacity Goal {index}")
        for index in range(2)
    ]
    contexts = []
    for goal in goals:
        _insert_iteration(store, goal["id"])
        contexts.append(engine._context(goal["id"]))

    capacity_waits = 0
    original_wait = runtime.wait_for_dispatch_capacity

    async def counted_wait(*, timeout_seconds: float = 1.0) -> bool:
        nonlocal capacity_waits
        capacity_waits += 1
        return await original_wait(timeout_seconds=timeout_seconds)

    runtime.wait_for_dispatch_capacity = counted_wait  # type: ignore[method-assign]

    first, second = await asyncio.gather(*(driver.evaluate(context) for context in contexts))

    assert first.disposition is EvaluationDisposition.SATISFIED
    assert second.disposition is EvaluationDisposition.SATISFIED
    assert capacity_waits >= 1
    assert all(run["state"] == "completed" for run in store.list_worker_runs())
    decision_task_ids = {
        row["task_id"] for row in store.list_events(limit=500) if row["kind"] == "dispatchDeferred"
    }
    assert decision_task_ids
    assert all(store.get_task(task_id)["state"] == "succeeded" for task_id in decision_task_ids)


@pytest.mark.asyncio
async def test_malformed_verifier_fails_closed_to_human_escalation(
    tmp_path: Path, decision_script: Path
) -> None:
    store = _store(tmp_path)
    runtime = _runtime(store, ScriptDecisionAdapter(decision_script, "malformed"), tmp_path)
    driver = ProductionAutonomyDriver(runtime)
    goal = GoalService(store).create_goal(project_id="project-1", intent="Verify fail closed")
    _insert_iteration(store, goal["id"])
    context = AutonomousIterationEngine(
        store=store,
        evaluator=driver,
        planner=driver,
        dispatcher=SupervisorRuntimeDispatcher(runtime),
        verifier=driver,
    )._context(goal["id"])

    verification = await driver.verify(context, (ActionResult(True, "work complete"),))

    assert not verification.satisfied
    assert verification.termination_reason is TerminationReason.HUMAN_ESCALATION


def test_decision_schema_accepts_documented_envelope() -> None:
    schema_path = Path(__file__).parents[1] / "schemas/autonomy-decision-v1.schema.json"
    schema = json.loads(schema_path.read_text(encoding="utf-8"))
    Draft202012Validator.check_schema(schema)
    Draft202012Validator(schema).validate(
        {
            "schemaVersion": "autonomy-decision/v1",
            "decisionType": "evaluation",
            "goalID": "goal-1",
            "iterationSequence": 1,
            "steerVersion": 0,
            "disposition": "incomplete",
            "summary": "work remains",
            "progressFingerprint": "one",
            "terminationReason": None,
            "facts": {},
        }
    )


def test_adapter_reconstruction_is_fail_closed_and_explicit(tmp_path: Path) -> None:
    mock_store = _store(tmp_path / "mock", harness=Harness.MOCK)
    with pytest.raises(AdapterReconstructionError, match="allow_mock"):
        reconstruct_adapter_registry(mock_store)
    registry = reconstruct_adapter_registry(mock_store, allow_mock=True)
    assert isinstance(registry.get("worker-1"), MockAdapter)

    native_store = _store(tmp_path / "native", harness=Harness.CLAUDE_CODE)
    native = reconstruct_adapter_registry(
        native_store, executable_overrides={"worker-1": sys.executable}
    )
    assert isinstance(native.get("worker-1"), ClaudeAdapter)
    with pytest.raises(AdapterReconstructionError, match="executable unavailable"):
        reconstruct_adapter_registry(
            native_store,
            executable_overrides={"worker-1": str(tmp_path / "does-not-exist")},
        )

    with native_store.transaction() as connection:
        connection.execute("UPDATE workers SET provider='xai' WHERE id='worker-1'")
    with pytest.raises(AdapterReconstructionError, match="mismatch"):
        reconstruct_adapter_registry(
            native_store, executable_overrides={"worker-1": sys.executable}
        )

    codex_store = _store(tmp_path / "codex", harness=Harness.CODEX)
    codex_registry = reconstruct_adapter_registry(
        codex_store, executable_overrides={"worker-1": sys.executable}
    )
    assert isinstance(codex_registry.get("worker-1"), CodexAdapter)


def test_local_worker_requires_explicit_endpoint_injection(tmp_path: Path) -> None:
    store = _store(tmp_path, harness=Harness.LOCAL_WORKER)
    with pytest.raises(AdapterReconstructionError, match="explicitly injected"):
        reconstruct_adapter_registry(store)
    registry = reconstruct_adapter_registry(
        store,
        local_worker_endpoints={"worker-1": "http://127.0.0.1:7331"},
        local_worker_drivers={"worker-1": "claude-reviewed"},
    )
    assert registry.contains("worker-1")
    adapter = registry.get("worker-1")
    assert isinstance(adapter, LocalWorkerAdapter)
    assert adapter.driver_id == "claude-reviewed"
