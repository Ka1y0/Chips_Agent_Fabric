from __future__ import annotations

import asyncio
import json
import sqlite3
from dataclasses import replace
from datetime import UTC, datetime, timedelta

import pytest
from fastapi.testclient import TestClient

from project_supervisor.adapters import (
    AuthorizationReference,
    EventSink,
    MockAdapter,
    MockBehavior,
    WorkerEvent,
    WorkerRequest,
    WorkerResult,
)
from project_supervisor.adapters.base import event_time, publish_event
from project_supervisor.api import create_app
from project_supervisor.autonomy import GoalBudget, GoalService
from project_supervisor.domain import (
    ExecutionTopology,
    Harness,
    ModelDescriptor,
    NodeState,
    PermissionClass,
    Provider,
    ResourceState,
    RunState,
    TaskLabel,
    TaskRequirements,
    WorkerSnapshot,
    WorkerState,
)
from project_supervisor.fabric.authority import (
    AuthorizationEnvelope,
    AuthorizationRepository,
    DataProvenanceRepository,
)
from project_supervisor.fabric.execution import ChildWorkProposal, SpawnPolicy
from project_supervisor.fabric.execution_plane import PlatformApprovalState
from project_supervisor.fabric.persistence import SpawnRepository
from project_supervisor.fabric.provider_execution import (
    AnalysisResultRepository,
    HypothesisRepository,
    InvocationStage,
    ProjectWriteAuthorityRepository,
    ProviderCapacityRepository,
    ProviderInvocationRepository,
    TriState,
)
from project_supervisor.runtime import AdapterRegistry, SupervisorRuntime
from project_supervisor.scheduler import DeterministicScheduler
from project_supervisor.store import StateStore, timestamp


def _seed(tmp_path, *, worker_count: int = 3):
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    store = StateStore(tmp_path / "state.db")
    store.create_project(
        project_id="project-authority",
        name="Authority fixture",
        root_path=str(workspace),
        goal="Prove narrowed authority and invocation provenance",
    )
    store.upsert_node(
        node_id="node-pc",
        hostname="private-pc",
        display_name="PC Node",
        role="worker",
        state=NodeState.ONLINE,
    )
    registry = AdapterRegistry()
    workers: list[str] = []
    for index in range(worker_count):
        worker_id = f"worker-analysis-{index + 1}"
        workers.append(worker_id)
        store.upsert_worker(
            WorkerSnapshot(
                id=worker_id,
                node_id="node-pc",
                harness=Harness.MOCK,
                provider=Provider.MOCK,
                model=ModelDescriptor("fixture", "Fixture", Provider.MOCK),
                state=WorkerState.IDLE,
                node_state=NodeState.ONLINE,
                resource_state=ResourceState.AVAILABLE,
                capabilities=frozenset({"analysis"}),
                worker_classes=frozenset({"analysisReasoner" if index else "primaryExecutor"}),
                code_write_allowed=index == 0,
                privacy_allowed=True,
            )
        )
    return store, registry, tuple(workers)


async def _submit(runtime: SupervisorRuntime, *, task_id: str, topology=ExecutionTopology.SINGLE):
    return await runtime.submit_task(
        project_id="project-authority",
        task_id=task_id,
        title=task_id,
        description="Bounded deterministic analysis",
        topology=topology,
        requirements=TaskRequirements(
            labels=frozenset({TaskLabel.RESEARCH}),
            required_capabilities=frozenset({"analysis"}),
            panel_size=2,
        ),
    )


def _root_envelope(
    task_id: str,
    *,
    authorization_id: str = "authorization-root",
    platform: PlatformApprovalState = PlatformApprovalState.NOT_REQUIRED,
    data_refs: frozenset[str] = frozenset(),
) -> AuthorizationEnvelope:
    return AuthorizationEnvelope(
        authorization_id=authorization_id,
        project_id="project-authority",
        root_task_id=task_id,
        subject="fabric.task",
        permission_ceiling=PermissionClass.YELLOW,
        capabilities=frozenset({"analysis"}),
        actions=frozenset({"evidence.read", "analysis.propose"}),
        resources=frozenset({"project.read"}),
        data_refs=data_refs,
        allowed_providers=frozenset({"mock"}),
        allowed_worker_classes=frozenset({"primaryExecutor", "analysisReasoner"}),
        allowed_data_classes=frozenset({"confidential", "restricted"}),
        denied_data_classes=frozenset({"credentials", "unrelatedPrivateData"}),
        allowed_action_classes=frozenset({"evidence.read", "analysis.propose"}),
        denied_action_classes=frozenset({"code.write"}),
        budget={"invocations": 3, "seconds": 120},
        issued_by="operator.test",
        expires_at=datetime.now(UTC) + timedelta(minutes=10),
        platform_approval_required=platform is not PlatformApprovalState.NOT_REQUIRED,
        platform_approval_state=platform,
    )


async def test_authorization_narrowing_and_atomic_run_binding(tmp_path) -> None:
    store, registry, (worker_id, *_) = _seed(tmp_path)
    registry.register(worker_id, MockAdapter())
    runtime = SupervisorRuntime(
        store=store,
        scheduler=DeterministicScheduler(),
        adapters=registry,
        evidence_root=tmp_path / "evidence",
    )
    task_id = await runtime.submit_task(
        project_id="project-authority",
        task_id="task-authorized",
        title="Authorized analysis",
        description="Analyze bounded evidence",
        requirements=TaskRequirements(
            labels=frozenset({TaskLabel.RESEARCH}),
            required_capabilities=frozenset({"analysis"}),
        ),
        execution_spec={"authorizationEnvelopeID": "authorization-root"},
    )
    authorizations = AuthorizationRepository(store)
    root = _root_envelope(task_id)
    authorizations.issue(root)
    authorizations.bind_task(root.authorization_id, task_id)

    with pytest.raises(ValueError, match="cannot widen"):
        root.derive(
            authorization_id="authorization-child-wide",
            subject="analysis.worker",
            actions=frozenset({"evidence.read", "analysis.propose", "code.write"}),
            allowed_action_classes=frozenset({"evidence.read", "analysis.propose", "code.write"}),
        )
    child = root.derive(
        authorization_id="authorization-child",
        subject="analysis.worker",
        actions=frozenset({"evidence.read"}),
        budget={"invocations": 1},
    )
    authorizations.issue(child)

    await runtime.run_until_idle()

    run = store.list_worker_runs(task_id)[0]
    with store.connect() as connection:
        binding = connection.execute(
            "SELECT envelope_id FROM authorization_envelope_bindings "
            "WHERE run_id=? AND binding_kind='run'",
            (run["id"],),
        ).fetchone()
    assert binding["envelope_id"] == root.authorization_id


async def test_child_work_gets_a_transactionally_narrowed_authorization(tmp_path) -> None:
    store, _, _ = _seed(tmp_path)
    runtime = SupervisorRuntime(
        store=store,
        scheduler=DeterministicScheduler(),
        adapters=AdapterRegistry(),
        evidence_root=tmp_path / "evidence",
    )
    task_id = await runtime.submit_task(
        project_id="project-authority",
        task_id="task-authority-parent",
        title="Parent authority",
        description="Parent of a bounded child proposal",
        requirements=TaskRequirements(
            labels=frozenset({TaskLabel.RESEARCH}),
            required_capabilities=frozenset({"analysis"}),
        ),
        execution_spec={"authorizationEnvelopeID": "authorization-spawn-root"},
    )
    goals = GoalService(store)
    goals.create_goal(
        project_id="project-authority",
        intent="Govern an authorized child",
        budgets=GoalBudget(max_tasks=3),
        goal_id="goal-authority-spawn",
    )
    with store.transaction() as connection:
        connection.execute(
            "UPDATE autonomous_goals SET state='running',started_at=?,updated_at=? WHERE id=?",
            (timestamp(), timestamp(), "goal-authority-spawn"),
        )
    spawn = SpawnRepository(store, SpawnPolicy(max_depth=2, max_total_children=3))
    spawn.bind_root_task(
        task_id=task_id,
        goal_id="goal-authority-spawn",
        expected_steer_version=0,
    )
    root = replace(
        _root_envelope(task_id, authorization_id="authorization-spawn-root"),
        goal_id="goal-authority-spawn",
    )
    authorizations = AuthorizationRepository(store)
    authorizations.issue(root)
    authorizations.bind_task(root.authorization_id, task_id)

    accepted = spawn.admit(
        ChildWorkProposal(
            proposal_id="proposal-authorized-child",
            goal_id="goal-authority-spawn",
            parent_task_id=task_id,
            proposal_key="authorized-child",
            title="Authorized child",
            description="Read one bounded evidence packet",
            steer_version=0,
            depth=1,
            task_definition_revision=int(store.get_task(task_id)["definition_revision"]),
            payload={
                "requirements": {"requiredCapabilities": ["analysis"]},
                "authorizationNarrowing": {
                    "allowedProviders": ["mock"],
                    "allowedWorkerClasses": ["analysisReasoner"],
                    "actions": ["evidence.read"],
                    "allowedActionClasses": ["evidence.read"],
                    "deniedActionClasses": ["code.write"],
                    "budget": {"invocations": 1, "seconds": 30},
                },
            },
        )
    )

    child = store.get_task(str(accepted["childTaskID"]))
    child_spec = json.loads(child["execution_spec_json"])
    child_authority = authorizations.get(child_spec["authorizationEnvelopeID"])
    assert child_authority["parentID"] == root.authorization_id
    assert child_authority["allowedProviders"] == ["mock"]
    assert child_authority["allowedWorkerClasses"] == ["analysisReasoner"]
    assert child_authority["budget"] == {"invocations": 1, "seconds": 30}

    with pytest.raises(ValueError, match="cannot widen allowed_providers"):
        spawn.admit(
            ChildWorkProposal(
                proposal_id="proposal-authority-widen",
                goal_id="goal-authority-spawn",
                parent_task_id=task_id,
                proposal_key="authority-widen",
                title="Forbidden widening",
                description="Worker attempts to add a provider",
                steer_version=0,
                depth=1,
                task_definition_revision=int(store.get_task(task_id)["definition_revision"]),
                payload={
                    "requirements": {"requiredCapabilities": ["analysis"]},
                    "authorizationNarrowing": {"allowedProviders": ["mock", "grok"]},
                },
            )
        )


async def test_platform_rejection_is_separate_and_never_dispatches(tmp_path) -> None:
    store, registry, (worker_id, *_) = _seed(tmp_path)
    registry.register(worker_id, MockAdapter())
    runtime = SupervisorRuntime(
        store=store,
        scheduler=DeterministicScheduler(),
        adapters=registry,
        evidence_root=tmp_path / "evidence",
    )
    task_id = await _submit(runtime, task_id="task-platform-rejected")
    rejected = _root_envelope(
        task_id,
        authorization_id="authorization-platform-rejected",
        platform=PlatformApprovalState.REJECTED,
    )
    repository = AuthorizationRepository(store)
    repository.issue(rejected)
    with pytest.raises(PermissionError, match="not executable"):
        repository.bind_task(rejected.authorization_id, task_id)
    assert store.list_worker_runs(task_id) == []


def test_data_packet_disclosure_requires_exact_authority_and_no_credentials(tmp_path) -> None:
    store, _, _ = _seed(tmp_path)
    runtime = SupervisorRuntime(
        store=store,
        scheduler=DeterministicScheduler(),
        adapters=AdapterRegistry(),
        evidence_root=tmp_path / "evidence",
    )
    task_id = asyncio.run(_submit(runtime, task_id="task-data"))
    provenance = DataProvenanceRepository(store)
    safe = provenance.register_packet(
        packet_id="packet-safe",
        project_id="project-authority",
        task_id=task_id,
        source_kind="crossfire.report",
        source_ref="report.phase2",
        content_sha256="a" * 64,
        classification="confidential",
        contains_credentials="no",
        created_by="pc.codex",
        byte_count=128,
    )
    unsafe = provenance.register_packet(
        packet_id="packet-unknown-secret",
        project_id="project-authority",
        task_id=task_id,
        source_kind="crossfire.report",
        source_ref="report.unknown",
        content_sha256="b" * 64,
        classification="restricted",
        contains_credentials="unknown",
        created_by="pc.codex",
    )
    envelope = _root_envelope(
        task_id,
        authorization_id="authorization-data",
        data_refs=frozenset({safe["id"], unsafe["id"]}),
    )
    AuthorizationRepository(store).issue(envelope)
    movement = provenance.record_movement(
        packet_id=safe["id"],
        envelope_id=envelope.authorization_id,
        destination_kind="provider.analysis",
        destination_ref="provider.mock",
        purpose="crossfire.review",
        disclosure_state="disclosed",
        recorded_by="runtime",
    )
    assert movement["disclosure_state"] == "disclosed"
    with pytest.raises(PermissionError, match="credential-bearing"):
        provenance.record_movement(
            packet_id=unsafe["id"],
            envelope_id=envelope.authorization_id,
            destination_kind="provider.analysis",
            destination_ref="provider.mock",
            purpose="crossfire.review",
            disclosure_state="disclosed",
            recorded_by="runtime",
        )


def _manual_run(store: StateStore, worker_id: str, task_id: str) -> str:
    task = store.get_task(task_id)
    claim = store.claim_task_dispatch(
        task_id,
        (worker_id,),
        expected_version=int(task["version"]),
    )
    assert claim is not None
    return claim["runIDs"][worker_id]


def test_provider_invocations_are_independent_monotonic_and_tri_state(tmp_path) -> None:
    store, _, workers = _seed(tmp_path)
    runtime = SupervisorRuntime(
        store=store,
        scheduler=DeterministicScheduler(),
        adapters=AdapterRegistry(),
        evidence_root=tmp_path / "evidence",
    )
    task_ids = [
        asyncio.run(_submit(runtime, task_id=f"task-invocation-{index}")) for index in range(3)
    ]
    runs = [
        _manual_run(store, worker_id, task_id)
        for worker_id, task_id in zip(workers, task_ids, strict=True)
    ]
    repository = ProviderInvocationRepository(store)
    rejected = repository.create(
        run_id=runs[0], provider="mock", invocation_id="invocation-rejected"
    )
    assert rejected.model_used is TriState.UNKNOWN
    rejected = repository.observe(
        "invocation-rejected",
        InvocationStage.FAILED,
        event_key="preProcessReject",
        source="fixture",
        detail_code="rejectedBeforeProcess",
    )
    assert rejected.model_used is TriState.NO
    assert rejected.data_disclosed is TriState.NO

    completed = repository.create(
        run_id=runs[1], provider="mock", invocation_id="invocation-completed"
    )
    completed = repository.observe(
        completed.invocation_id,
        InvocationStage.ACCEPTED,
        event_key="accepted",
        source="fixture",
    )
    completed = repository.observe(
        completed.invocation_id,
        InvocationStage.MODEL_USED,
        event_key="modelUsed",
        source="fixture",
        model="fixture-model",
    )
    completed = repository.observe(
        completed.invocation_id,
        InvocationStage.COMPLETED,
        event_key="completed",
        source="fixture",
    )
    assert completed.model_used is TriState.YES
    assert completed.data_disclosed is TriState.UNKNOWN

    disclosed = repository.create(
        run_id=runs[2], provider="mock", invocation_id="invocation-disclosed"
    )
    disclosed = repository.observe(
        disclosed.invocation_id,
        InvocationStage.ACCEPTED,
        event_key="accepted",
        source="fixture",
    )
    disclosed = repository.observe(
        disclosed.invocation_id,
        InvocationStage.DATA_DISCLOSED,
        event_key="dataDisclosed",
        source="fixture",
    )
    disclosed = repository.observe(
        disclosed.invocation_id,
        InvocationStage.MODEL_USED,
        event_key="modelUsed",
        source="fixture",
        model="fixture-model",
    )
    disclosed = repository.observe(
        disclosed.invocation_id,
        InvocationStage.COMPLETED,
        event_key="completed",
        source="fixture",
    )
    assert disclosed.data_disclosed is TriState.YES
    assert len(repository.list()) == 3
    with pytest.raises(ValueError, match="terminal"):
        repository.observe(
            disclosed.invocation_id,
            InvocationStage.RUNNING,
            event_key="late",
            source="fixture",
        )


def test_provider_invocation_exact_boundaries_do_not_infer_hidden_provider_work(
    tmp_path,
) -> None:
    store, _, workers = _seed(tmp_path)
    runtime = SupervisorRuntime(
        store=store,
        scheduler=DeterministicScheduler(),
        adapters=AdapterRegistry(),
        evidence_root=tmp_path / "evidence",
    )
    task_ids = [
        asyncio.run(_submit(runtime, task_id=f"task-boundary-{index}")) for index in range(3)
    ]
    runs = [
        _manual_run(store, worker_id, task_id)
        for worker_id, task_id in zip(workers, task_ids, strict=True)
    ]
    repository = ProviderInvocationRepository(store)

    pre_process = repository.create(
        run_id=runs[0], provider="mock", invocation_id="invocation-pre-process"
    )
    pre_process = repository.observe(
        pre_process.invocation_id,
        InvocationStage.REJECTED_BEFORE_PROCESS,
        event_key="receipt.preProcess",
        source="fixture",
    )
    assert pre_process.model_used is TriState.NO
    assert pre_process.data_disclosed is TriState.NO

    after_process = repository.create(
        run_id=runs[1], provider="mock", invocation_id="invocation-after-process"
    )
    after_process = repository.observe(
        after_process.invocation_id,
        InvocationStage.PROCESS_STARTED,
        event_key="process.started",
        source="fixture",
    )
    after_process = repository.observe(
        after_process.invocation_id,
        InvocationStage.FAILED_AFTER_START,
        event_key="process.failed",
        source="fixture",
    )
    assert after_process.model_used is TriState.UNKNOWN
    assert after_process.data_disclosed is TriState.UNKNOWN

    after_disclosure = repository.create(
        run_id=runs[2], provider="mock", invocation_id="invocation-after-disclosure"
    )
    after_disclosure = repository.observe(
        after_disclosure.invocation_id,
        InvocationStage.DISPATCHED,
        event_key="dispatch.sent",
        source="fixture",
    )
    after_disclosure = repository.observe(
        after_disclosure.invocation_id,
        InvocationStage.DATA_DISCLOSED,
        event_key="provider.disclosed",
        source="fixture",
    )
    after_disclosure = repository.observe(
        after_disclosure.invocation_id,
        InvocationStage.REJECTED_BEFORE_INFERENCE,
        event_key="provider.preInferenceReject",
        source="fixture",
    )
    assert after_disclosure.model_used is TriState.NO
    assert after_disclosure.data_disclosed is TriState.YES
    api = TestClient(
        create_app(store, allow_unauthenticated_loopback=True),
        client=("127.0.0.1", 50000),
    )
    response = api.get("/v1/fabric/provider-invocations")
    assert response.status_code == 200
    projected = {item["invocationID"]: item for item in response.json()["data"]["invocations"]}
    assert projected["invocation-pre-process"]["stage"] == "rejectedBeforeProcess"
    assert projected["invocation-after-process"]["modelUsed"]["state"] == "unknown"
    assert projected["invocation-after-process"]["dataDisclosed"] == "unknown"
    assert projected["invocation-after-disclosure"]["modelUsed"]["state"] == "no"
    assert projected["invocation-after-disclosure"]["dataDisclosed"] == "yes"


def test_provider_invocation_v2_replay_is_immutable(tmp_path) -> None:
    store, _, workers = _seed(tmp_path)
    runtime = SupervisorRuntime(
        store=store,
        scheduler=DeterministicScheduler(),
        adapters=AdapterRegistry(),
        evidence_root=tmp_path / "evidence",
    )
    task_id = asyncio.run(_submit(runtime, task_id="task-invocation-replay"))
    run_id = _manual_run(store, workers[0], task_id)
    repository = ProviderInvocationRepository(store)
    invocation = repository.create(
        run_id=run_id,
        provider="mock",
        invocation_id="invocation-replay",
        idempotency_key="provider-key-stable",
    )
    assert (
        repository.create(
            run_id=run_id,
            provider="mock",
            invocation_id="invocation-replay",
            idempotency_key="provider-key-stable",
        )
        == invocation
    )
    with pytest.raises(RuntimeError, match="immutable identity"):
        repository.create(
            run_id=run_id,
            provider="mock",
            invocation_id="invocation-replay",
            idempotency_key="provider-key-conflict",
        )
    first = repository.observe(
        invocation.invocation_id,
        InvocationStage.PROCESS_STARTED,
        event_key="process.started",
        source="fixture",
        observed_at=datetime(2026, 1, 1, tzinfo=UTC),
    )
    replay = repository.observe(
        invocation.invocation_id,
        InvocationStage.PROCESS_STARTED,
        event_key="process.started",
        source="fixture",
        observed_at=datetime(2026, 1, 2, tzinfo=UTC),
    )
    assert replay == first
    with pytest.raises(RuntimeError, match="conflicts"):
        repository.observe(
            invocation.invocation_id,
            InvocationStage.INFERENCE_STARTED,
            event_key="process.started",
            source="fixture",
            model="different",
        )
    with store.connect() as connection:
        history = connection.execute(
            "SELECT ordinal,stage FROM provider_invocation_events_v2 "
            "WHERE invocation_id=? ORDER BY ordinal",
            (invocation.invocation_id,),
        ).fetchall()
    assert [(row["ordinal"], row["stage"]) for row in history] == [
        (1, "requested"),
        (2, "processStarted"),
    ]
    with (
        pytest.raises(sqlite3.IntegrityError, match="append-only"),
        store.transaction() as connection,
    ):
        connection.execute(
            "UPDATE provider_invocation_events_v2 SET source='forged' "
            "WHERE invocation_id=? AND ordinal=2",
            (invocation.invocation_id,),
        )


def test_provider_invocation_rejects_pre_process_regression_after_unknown(tmp_path) -> None:
    store, _, workers = _seed(tmp_path)
    runtime = SupervisorRuntime(
        store=store,
        scheduler=DeterministicScheduler(),
        adapters=AdapterRegistry(),
        evidence_root=tmp_path / "evidence",
    )
    task_id = asyncio.run(_submit(runtime, task_id="task-invocation-regression"))
    run_id = _manual_run(store, workers[0], task_id)
    repository = ProviderInvocationRepository(store)
    invocation = repository.create(
        run_id=run_id,
        provider="mock",
        invocation_id="invocation-regression",
    )
    repository.observe(
        invocation.invocation_id,
        InvocationStage.PROCESS_STARTED,
        event_key="process.started",
        source="fixture",
    )
    repository.observe(
        invocation.invocation_id,
        InvocationStage.OUTCOME_UNKNOWN,
        event_key="transport.unknown",
        source="fixture",
    )

    with pytest.raises(ValueError, match="prior process evidence"):
        repository.observe(
            invocation.invocation_id,
            InvocationStage.REJECTED_BEFORE_PROCESS,
            event_key="late.preProcess",
            source="fixture",
        )
    projection = repository.get(invocation.invocation_id)
    assert projection.model_used is TriState.UNKNOWN
    assert projection.data_disclosed is TriState.UNKNOWN


def test_failed_result_preserves_actual_model_and_after_start_boundary(tmp_path) -> None:
    store, _, workers = _seed(tmp_path)
    runtime = SupervisorRuntime(
        store=store,
        scheduler=DeterministicScheduler(),
        adapters=AdapterRegistry(),
        evidence_root=tmp_path / "evidence",
    )
    task_id = asyncio.run(_submit(runtime, task_id="task-failed-model"))
    run_id = _manual_run(store, workers[0], task_id)
    repository = ProviderInvocationRepository(store)
    invocation = repository.create(
        run_id=run_id,
        provider="mock",
        invocation_id=f"provider-invocation:{run_id}:1",
    )
    repository.observe(
        invocation.invocation_id,
        InvocationStage.ACCEPTED,
        event_key="provider.accepted",
        source="adapter",
    )
    repository.observe(
        invocation.invocation_id,
        InvocationStage.PROCESS_STARTED,
        event_key="process.started",
        source="fixture",
    )
    now = datetime.now(UTC)
    result = WorkerResult(
        run_id=run_id,
        state=RunState.FAILED,
        pid=None,
        exit_code=17,
        started_at=now,
        ended_at=now,
        stdout="",
        stderr="fixture failure",
        final_text="",
        events=(),
        model="actual-failed-model",
        error="fixture failure",
    )

    asyncio.run(runtime._record_terminal_provider_invocation(run_id, result))

    projection = repository.get(invocation.invocation_id)
    assert projection.stage is InvocationStage.FAILED_AFTER_START
    assert projection.model_used is TriState.YES
    assert projection.model == "actual-failed-model"


def test_terminal_model_mismatch_preserves_first_evidence_and_still_terminalizes(tmp_path) -> None:
    store, _, workers = _seed(tmp_path)
    runtime = SupervisorRuntime(
        store=store,
        scheduler=DeterministicScheduler(),
        adapters=AdapterRegistry(),
        evidence_root=tmp_path / "evidence",
    )
    task_id = asyncio.run(_submit(runtime, task_id="task-model-conflict"))
    run_id = _manual_run(store, workers[0], task_id)
    repository = ProviderInvocationRepository(store)
    invocation = repository.create(
        run_id=run_id,
        provider="mock",
        invocation_id=f"provider-invocation:{run_id}:1",
    )
    repository.observe(
        invocation.invocation_id,
        InvocationStage.ACCEPTED,
        event_key="provider.accepted",
        source="adapter",
    )
    repository.observe(
        invocation.invocation_id,
        InvocationStage.INFERENCE_STARTED,
        event_key="provider.inference",
        source="adapter",
        model="model-first-evidence",
    )
    now = datetime.now(UTC)
    result = WorkerResult(
        run_id=run_id,
        state=RunState.COMPLETED,
        pid=None,
        exit_code=0,
        started_at=now,
        ended_at=now,
        stdout="ok",
        stderr="",
        final_text="ok",
        events=(),
        model="model-terminal-alias",
    )

    asyncio.run(runtime._record_terminal_provider_invocation(run_id, result))

    projection = repository.get(invocation.invocation_id)
    assert projection.stage is InvocationStage.INFERENCE_COMPLETED
    assert projection.terminal is True
    assert projection.model == "model-first-evidence"
    with store.connect() as connection:
        terminal = connection.execute(
            "SELECT detail_code FROM provider_invocation_events_v2 "
            "WHERE invocation_id=? ORDER BY ordinal DESC LIMIT 1",
            (invocation.invocation_id,),
        ).fetchone()
    assert terminal["detail_code"] == "modelIdentityConflict"


def test_terminal_result_enriches_missing_model_identity(tmp_path) -> None:
    store, _, workers = _seed(tmp_path)
    runtime = SupervisorRuntime(
        store=store,
        scheduler=DeterministicScheduler(),
        adapters=AdapterRegistry(),
        evidence_root=tmp_path / "evidence",
    )
    task_id = asyncio.run(_submit(runtime, task_id="task-model-enrichment"))
    run_id = _manual_run(store, workers[0], task_id)
    repository = ProviderInvocationRepository(store)
    invocation = repository.create(
        run_id=run_id,
        provider="mock",
        invocation_id=f"provider-invocation:{run_id}:1",
    )
    repository.observe(
        invocation.invocation_id,
        InvocationStage.ACCEPTED,
        event_key="provider.accepted",
        source="adapter",
    )
    repository.observe(
        invocation.invocation_id,
        InvocationStage.INFERENCE_STARTED,
        event_key="provider.inference",
        source="adapter",
    )
    now = datetime.now(UTC)
    result = WorkerResult(
        run_id=run_id,
        state=RunState.COMPLETED,
        pid=None,
        exit_code=0,
        started_at=now,
        ended_at=now,
        stdout="ok",
        stderr="",
        final_text="ok",
        events=(),
        model="terminal-actual-model",
    )

    asyncio.run(runtime._record_terminal_provider_invocation(run_id, result))

    projection = repository.get(invocation.invocation_id)
    assert projection.terminal is True
    assert projection.model_used is TriState.YES
    assert projection.model == "terminal-actual-model"


def test_provider_invocation_api_omits_secret_shaped_model_metadata(tmp_path) -> None:
    store, _, workers = _seed(tmp_path)
    runtime = SupervisorRuntime(
        store=store,
        scheduler=DeterministicScheduler(),
        adapters=AdapterRegistry(),
        evidence_root=tmp_path / "evidence",
    )
    task_id = asyncio.run(_submit(runtime, task_id="task-secret-model"))
    run_id = _manual_run(store, workers[0], task_id)
    repository = ProviderInvocationRepository(store)
    invocation = repository.create(
        run_id=run_id,
        provider="mock",
        invocation_id="invocation-secret-model",
        requested_model="model-Bearer-secret-fixture",
    )
    repository.observe(
        invocation.invocation_id,
        InvocationStage.ACCEPTED,
        event_key="launch.accepted",
        source="fixture",
    )
    secret_model_fixture = "sk-" + "secret-model-fixture"
    repository.observe(
        invocation.invocation_id,
        InvocationStage.MODEL_USED,
        event_key="model.used",
        source="fixture",
        model=secret_model_fixture,
    )
    api = TestClient(
        create_app(store, allow_unauthenticated_loopback=True),
        client=("127.0.0.1", 50000),
    )

    response = api.get("/v1/fabric/provider-invocations")

    assert response.status_code == 200
    text = response.text
    assert "model-Bearer-secret-fixture" not in text
    assert secret_model_fixture not in text
    projected = response.json()["data"]["invocations"][0]
    assert projected["requestedModel"] is None
    assert projected["modelUsed"]["model"] is None


def test_provider_capacity_pool_enforces_shared_parallel_limit_atomically(tmp_path) -> None:
    store, _, workers = _seed(tmp_path)
    ProviderCapacityRepository(store).configure_pool(
        pool_id="pool.subscription.mock",
        display_name="Existing subscription",
        max_concurrency=2,
        worker_ids=workers,
    )
    runtime = SupervisorRuntime(
        store=store,
        scheduler=DeterministicScheduler(),
        adapters=AdapterRegistry(),
        evidence_root=tmp_path / "evidence",
    )
    tasks = [asyncio.run(_submit(runtime, task_id=f"task-pool-{index}")) for index in range(3)]
    first = _manual_run(store, workers[0], tasks[0])
    second = _manual_run(store, workers[1], tasks[1])
    third_task = store.get_task(tasks[2])
    assert (
        store.claim_task_dispatch(
            tasks[2],
            (workers[2],),
            expected_version=int(third_task["version"]),
        )
        is None
    )
    store.transition_worker_run(first, RunState.FAILED)
    third_task = store.get_task(tasks[2])
    assert (
        store.claim_task_dispatch(
            tasks[2],
            (workers[2],),
            expected_version=int(third_task["version"]),
        )
        is not None
    )
    with store.connect() as connection:
        active = connection.execute(
            "SELECT COUNT(*) AS count FROM provider_capacity_reservations "
            "WHERE pool_id=? AND state='reserved'",
            ("pool.subscription.mock",),
        ).fetchone()["count"]
    assert active == 2
    assert second


async def test_provider_capacity_pool_serializes_concurrent_store_claims(tmp_path) -> None:
    store, _, workers = _seed(tmp_path)
    ProviderCapacityRepository(store).configure_pool(
        pool_id="pool.subscription.concurrent",
        display_name="Concurrent subscription capacity",
        max_concurrency=2,
        worker_ids=workers,
    )
    runtime = SupervisorRuntime(
        store=store,
        scheduler=DeterministicScheduler(),
        adapters=AdapterRegistry(),
        evidence_root=tmp_path / "evidence",
    )
    task_ids = [
        await _submit(runtime, task_id=f"task-pool-concurrent-{index}") for index in range(3)
    ]
    versions = [int(store.get_task(task_id)["version"]) for task_id in task_ids]

    claims = await asyncio.gather(
        *(
            asyncio.to_thread(
                store.claim_task_dispatch,
                task_id,
                (worker_id,),
                expected_version=version,
            )
            for task_id, worker_id, version in zip(task_ids, workers, versions, strict=True)
        )
    )

    assert sum(claim is not None for claim in claims) == 2
    with store.connect() as connection:
        active = connection.execute(
            "SELECT COUNT(*) AS count FROM provider_capacity_reservations "
            "WHERE pool_id=? AND state='reserved'",
            ("pool.subscription.concurrent",),
        ).fetchone()["count"]
    assert active == 2


def test_task_cancellation_releases_provider_capacity_reservation(tmp_path) -> None:
    store, _, (worker_id, *_) = _seed(tmp_path)
    ProviderCapacityRepository(store).configure_pool(
        pool_id="pool.subscription.cancel",
        display_name="Cancellation capacity",
        max_concurrency=1,
        worker_ids=(worker_id,),
    )
    runtime = SupervisorRuntime(
        store=store,
        scheduler=DeterministicScheduler(),
        adapters=AdapterRegistry(),
        evidence_root=tmp_path / "evidence",
    )
    task_id = asyncio.run(_submit(runtime, task_id="task-pool-cancel"))
    run_id = _manual_run(store, worker_id, task_id)

    assert store.cancel_task_execution(task_id)

    with store.connect() as connection:
        reservation = connection.execute(
            "SELECT state,release_reason FROM provider_capacity_reservations WHERE run_id=?",
            (run_id,),
        ).fetchone()
    assert dict(reservation) == {"state": "released", "release_reason": "taskCancelled"}


def test_task_cancellation_keeps_provider_capacity_until_quiescence_is_observed(tmp_path) -> None:
    store, _, (worker_id, *_) = _seed(tmp_path)
    ProviderCapacityRepository(store).configure_pool(
        pool_id="pool.subscription.cancel-uncertain",
        display_name="Cancellation uncertainty capacity",
        max_concurrency=1,
        worker_ids=(worker_id,),
    )
    runtime = SupervisorRuntime(
        store=store,
        scheduler=DeterministicScheduler(),
        adapters=AdapterRegistry(),
        evidence_root=tmp_path / "evidence",
    )
    task_id = asyncio.run(_submit(runtime, task_id="task-pool-cancel-uncertain"))
    task = store.get_task(task_id)
    claim = store.claim_task_dispatch(
        task_id,
        (worker_id,),
        expected_version=int(task["version"]),
        lease_owner_id="runtime-capacity-fixture",
    )
    assert claim is not None
    run_id = claim["runIDs"][worker_id]
    lease_generation = int(claim["leaseGeneration"])
    store.prepare_provider_job(
        run_id=run_id,
        adapter_type="durable-fixture",
        adapter_instance_id="durable-fixture-instance",
        capabilities={
            "supports_reconcile": True,
            "supports_cancel": True,
            "supports_repeatable_collect": True,
        },
        lease_owner_id="runtime-capacity-fixture",
        lease_generation=lease_generation,
    )
    store.mark_provider_job_launching(
        run_id,
        lease_owner_id="runtime-capacity-fixture",
        lease_generation=lease_generation,
    )

    assert store.cancel_task_execution(task_id)
    with store.connect() as connection:
        state = connection.execute(
            "SELECT state FROM provider_capacity_reservations WHERE run_id=?", (run_id,)
        ).fetchone()["state"]
    assert state == "reserved"

    recovery_generation = store.claim_task_reconciliation(
        task_id,
        owner_id="runtime-capacity-recovery",
        lease_ttl_seconds=30,
        allow_cancelled=True,
    )
    assert recovery_generation is not None
    store.record_provider_job_observation(
        run_id,
        state="providerNotFound",
        lease_owner_id="runtime-capacity-recovery",
        lease_generation=recovery_generation,
    )
    with store.connect() as connection:
        reservation = connection.execute(
            "SELECT state,release_reason FROM provider_capacity_reservations WHERE run_id=?",
            (run_id,),
        ).fetchone()
    assert dict(reservation) == {
        "state": "released",
        "release_reason": "providerObserved:providerNotFound",
    }


async def test_prelaunch_crash_releases_capacity_and_closes_invocation(tmp_path) -> None:
    store, _, (worker_id, *_) = _seed(tmp_path)
    ProviderCapacityRepository(store).configure_pool(
        pool_id="pool.subscription.prelaunch-crash",
        display_name="Prelaunch crash capacity",
        max_concurrency=1,
        worker_ids=(worker_id,),
    )
    runtime = SupervisorRuntime(
        store=store,
        scheduler=DeterministicScheduler(),
        adapters=AdapterRegistry(),
        evidence_root=tmp_path / "evidence",
        runtime_id="runtime-prelaunch-recovery",
    )
    task_id = await _submit(runtime, task_id="task-prelaunch-crash")
    task = store.get_task(task_id)
    claim = store.claim_task_dispatch(
        task_id,
        (worker_id,),
        expected_version=int(task["version"]),
        lease_owner_id="runtime-crashed-before-launch",
    )
    assert claim is not None
    run_id = claim["runIDs"][worker_id]
    generation = int(claim["leaseGeneration"])
    store.prepare_provider_job(
        run_id=run_id,
        adapter_type="durable-fixture",
        adapter_instance_id="durable-fixture-instance",
        capabilities={"supports_reconcile": True},
        lease_owner_id="runtime-crashed-before-launch",
        lease_generation=generation,
    )
    ProviderInvocationRepository(store).create(
        run_id=run_id,
        provider="mock",
        invocation_id=f"provider-invocation:{run_id}:1",
    )
    with store.transaction() as connection:
        connection.execute(
            "UPDATE task_execution_leases SET expires_at='1970-01-01T00:00:00Z' WHERE task_id=?",
            (task_id,),
        )

    recovered = await runtime.recover(task_ids={task_id})

    assert recovered == {"runsInterrupted": 1, "tasksInterrupted": 1}
    with store.connect() as connection:
        reservation = connection.execute(
            "SELECT state,release_reason FROM provider_capacity_reservations WHERE run_id=?",
            (run_id,),
        ).fetchone()
    assert dict(reservation) == {
        "state": "released",
        "release_reason": "recoveryPreLaunchInterrupted",
    }
    invocation = ProviderInvocationRepository(store).get(f"provider-invocation:{run_id}:1")
    assert invocation.stage is InvocationStage.REJECTED_BEFORE_PROCESS
    assert invocation.terminal is True
    assert invocation.model_used is TriState.NO
    assert invocation.data_disclosed is TriState.NO


class _PanelGate:
    def __init__(self) -> None:
        self.arrived = 0
        self.both = asyncio.Event()
        self.release = asyncio.Event()


class _GatedAnalysisAdapter(MockAdapter):
    def __init__(self, gate: _PanelGate, text: str) -> None:
        super().__init__(MockBehavior(text=text, model="fixture-model"))
        self.gate = gate

    async def execute(
        self,
        request: WorkerRequest,
        *,
        event_sink: EventSink | None = None,
    ) -> WorkerResult:
        self.gate.arrived += 1
        if self.gate.arrived == 2:
            self.gate.both.set()
        await self.gate.release.wait()
        result = await super().execute(request, event_sink=event_sink)
        await publish_event(
            event_sink,
            WorkerEvent(
                request.run_id,
                "providerDataDisclosed",
                event_time(),
                {"receipt": "typed-adapter-evidence"},
            ),
        )
        await publish_event(
            event_sink,
            WorkerEvent(
                request.run_id,
                "providerInferenceStarted",
                event_time(),
                {"model": "fixture-model"},
            ),
        )
        return result


async def test_same_provider_parallel_results_normalize_fuse_and_preserve_hypotheses(
    tmp_path,
) -> None:
    store, registry, workers = _seed(tmp_path, worker_count=2)
    gate = _PanelGate()
    texts = (
        json.dumps(
            {
                "schemaVersion": "analysis-contribution/v1",
                "claims": {"crossfire.fix": "option-a"},
                "evidence": {"packet": "evidence-a"},
            }
        ),
        json.dumps(
            {
                "schemaVersion": "analysis-contribution/v1",
                "claims": {"crossfire.fix": "option-b"},
                "evidence": {"packet": "evidence-b"},
            }
        ),
    )
    for worker_id, text in zip(workers, texts, strict=True):
        registry.register(worker_id, _GatedAnalysisAdapter(gate, text))
    runtime = SupervisorRuntime(
        store=store,
        scheduler=DeterministicScheduler(),
        adapters=registry,
        evidence_root=tmp_path / "evidence",
    )
    task_id = await _submit(
        runtime,
        task_id="task-panel",
        topology=ExecutionTopology.PARALLEL_PANEL,
    )
    criterion = store.add_acceptance_criterion(
        project_id="project-authority",
        kind="assertion",
        description="Fused hypothesis is independently verified",
    )
    store.bind_task_verification_scope(task_id, criterion_ids=(criterion,))

    summary = await runtime.dispatch_ready()
    assert summary.launched_task_ids == (task_id,)
    await asyncio.wait_for(gate.both.wait(), timeout=1)
    gate.release.set()
    await runtime.wait_for_active()

    runs = store.list_worker_runs(task_id)
    assert len(runs) == 2
    assert {run["worker_id"] for run in runs} == set(workers)
    invocations = ProviderInvocationRepository(store).list()
    assert len(invocations) == 2
    assert {
        (
            item["projection"].model_used,
            item["projection"].data_disclosed,
            item["projection"].model,
        )
        for item in invocations
    } == {(TriState.YES, TriState.YES, "fixture-model")}
    for run in runs:
        await runtime.normalize_analysis_result(run["id"])
    normalized = AnalysisResultRepository(store).normalized_attempt(task_id)
    assert len(normalized) == 2
    fusion = await runtime.fuse_normalized_task_results(task_id)
    assert fusion["classification"] == "contradictory"
    hypotheses = await runtime.create_hypotheses_from_fusion(fusion["fusionID"])
    assert hypotheses["state"] == "experimentRequired"
    assert len(hypotheses["hypotheses"]) == 2

    write_authority = AuthorizationEnvelope(
        authorization_id="authorization-crossfire-writer",
        project_id="project-authority",
        root_task_id=task_id,
        subject="external:pc.codex.primary",
        permission_ceiling=PermissionClass.YELLOW,
        capabilities=frozenset({"analysis"}),
        actions=frozenset({"experiment.run", "code.write"}),
        resources=frozenset({"project.write"}),
        allowed_providers=frozenset({"mock"}),
        allowed_worker_classes=frozenset({"primaryExecutor"}),
        allowed_data_classes=frozenset({"confidential"}),
        denied_data_classes=frozenset({"credentials"}),
        allowed_action_classes=frozenset({"experiment.run", "code.write"}),
        denied_action_classes=frozenset(),
        budget={"experiments": 1, "seconds": 120},
        issued_by="operator.test",
        expires_at=datetime.now(UTC) + timedelta(minutes=5),
        platform_approval_state=PlatformApprovalState.NOT_REQUIRED,
    )
    authorizations = AuthorizationRepository(store)
    authorizations.issue(write_authority)
    authorizations.bind_task(write_authority.authorization_id, task_id)
    arbitration = HypothesisRepository(store)
    predicates = [
        {
            "hypothesisID": hypothesis["hypothesisID"],
            "observationKey": "crossfire.outcome",
            "operator": "equals",
            "expected": hypothesis["value"],
        }
        for hypothesis in hypotheses["hypotheses"]
    ]
    proposal = arbitration.propose_experiment(
        set_id=hypotheses["hypothesisSetID"],
        proposal_key="observe-crossfire-outcome",
        operation="crossfire.observe.room-state",
        specification={"hypothesisPredicates": predicates, "sideEffects": "boundedLocal"},
        risk_class="yellow",
        expected_information_gain=1.0,
        envelope_id=write_authority.authorization_id,
    )
    writer = ProjectWriteAuthorityRepository(store)
    writer.configure(
        project_id="project-authority",
        owner_kind="externalManagedExecutor",
        owner_id="pc.codex.primary",
        node_id="node-pc",
        envelope_id=write_authority.authorization_id,
        configured_by="operator.test",
    )
    lease = writer.acquire(
        project_id="project-authority",
        owner_id="pc.codex.primary",
        task_id=task_id,
        ttl_seconds=120,
    )
    assert lease is not None
    handoff = arbitration.handoff_to_primary_executor(
        proposal_id=proposal["id"],
        project_id="project-authority",
        owner_id="pc.codex.primary",
        write_lease_id=lease["id"],
        content_kind="experimentProposal",
        instruction={"proposalID": proposal["id"], "observation": "crossfire.outcome"},
        expires_at=datetime.now(UTC) + timedelta(seconds=60),
    )
    assert handoff["state"] == "awaitingExternalExecutor"
    with pytest.raises(PermissionError, match="does not match"):
        arbitration.record_experiment_result(
            handoff_id=handoff["id"],
            executor_id="analysis.reasoner",
            result={
                "schemaVersion": "experiment-result/v1",
                "proposalID": proposal["id"],
                "status": "completed",
                "observations": {"crossfire.outcome": predicates[0]["expected"]},
            },
        )
    private_experiment_marker = "PRIVATE_EXPERIMENT_OBSERVATION_MUST_NOT_PROJECT"
    experiment = arbitration.record_experiment_result(
        handoff_id=handoff["id"],
        executor_id="pc.codex.primary",
        result={
            "schemaVersion": "experiment-result/v1",
            "proposalID": proposal["id"],
            "status": "completed",
            "observations": {
                "crossfire.outcome": predicates[0]["expected"],
                "privateDetail": private_experiment_marker,
            },
        },
    )
    assert experiment["hypothesisState"] == "resolved"
    assert [item["relation"] for item in experiment["assessments"]].count("supports") == 1

    second_proposal = arbitration.propose_experiment(
        set_id=hypotheses["hypothesisSetID"],
        proposal_key="observe-crossfire-outcome-after-lease-release",
        operation="crossfire.observe.room-state",
        specification={"hypothesisPredicates": predicates, "sideEffects": "boundedLocal"},
        risk_class="yellow",
        expected_information_gain=1.0,
        envelope_id=write_authority.authorization_id,
    )
    assert writer.release(lease["id"], owner_id="pc.codex.primary", reason="experimentComplete")
    second_lease = writer.acquire(
        project_id="project-authority",
        owner_id="pc.codex.primary",
        task_id=task_id,
        ttl_seconds=120,
    )
    assert second_lease is not None
    second_handoff = arbitration.handoff_to_primary_executor(
        proposal_id=second_proposal["id"],
        project_id="project-authority",
        owner_id="pc.codex.primary",
        write_lease_id=second_lease["id"],
        content_kind="experimentProposal",
        instruction={"proposalID": second_proposal["id"], "observation": "crossfire.outcome"},
        expires_at=datetime.now(UTC) + timedelta(seconds=60),
    )
    assert writer.release(
        second_lease["id"], owner_id="pc.codex.primary", reason="executorQuiescent"
    )
    with pytest.raises(PermissionError, match="lease is not current"):
        arbitration.record_experiment_result(
            handoff_id=second_handoff["id"],
            executor_id="pc.codex.primary",
            result={
                "schemaVersion": "experiment-result/v1",
                "proposalID": second_proposal["id"],
                "status": "completed",
                "observations": {"crossfire.outcome": predicates[0]["expected"]},
            },
        )

    api = TestClient(
        create_app(store, allow_unauthenticated_loopback=True),
        client=("127.0.0.1", 50000),
    )
    hypotheses_response = api.get("/v1/fabric/hypotheses")
    authorities_response = api.get("/v1/fabric/write-authorities")
    assert hypotheses_response.status_code == authorities_response.status_code == 200
    assert private_experiment_marker not in hypotheses_response.text
    projected_set = hypotheses_response.json()["data"]["hypothesisSets"][0]
    assert projected_set["experimentResults"][0]["resultSHA256"] == experiment["resultSHA256"]
    assert authorities_response.json()["data"]["writeAuthorities"][0]["ownerID"] == (
        "pc.codex.primary"
    )


def test_local_worker_v2_digest_binds_authorization_reference() -> None:
    plain = WorkerRequest(run_id="run-plain", task_id="task", prompt="analyze")
    authorized = WorkerRequest(
        run_id="run-plain",
        task_id="task",
        prompt="analyze",
        authorization=AuthorizationReference(
            authorization_id="authorization-test",
            schema_version="authorization-envelope/v1",
            definition_sha256="c" * 64,
            platform_approval_state="approved",
            data_packet_ids=("packet-safe",),
        ),
    )
    from project_supervisor.adapters.local_worker import LocalWorkerAdapter

    assert LocalWorkerAdapter.request_digest(plain) != LocalWorkerAdapter.request_digest(authorized)
