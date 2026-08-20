from __future__ import annotations

import json

import pytest

from project_supervisor.domain import (
    ExecutionTopology,
    Harness,
    ModelDescriptor,
    NodeState,
    Provider,
    ResourceState,
    RunState,
    TaskLabel,
    TaskRecord,
    TaskRequirements,
    TaskState,
    WorkerSnapshot,
    WorkerState,
)
from project_supervisor.store import ExecutionLeaseLostError, StateStore

CAPABILITIES = {
    "protocol_version": 2,
    "supports_reconcile": True,
    "supports_resume": True,
    "supports_cancel": True,
    "supports_durable_cancel": True,
    "supports_provider_idempotency": True,
    "supports_stream_reconnect": False,
    "supports_repeatable_collect": True,
    "supports_idempotent_launch_lookup": True,
    "supports_durable_launch_registry": True,
}


def claimed_execution(tmp_path) -> tuple[StateStore, str, str, int]:
    store = StateStore(tmp_path / "state.db")
    store.create_project(
        project_id="project-1",
        name="Durable provider fixture",
        root_path=str(tmp_path),
        goal="Survive a Runtime restart",
    )
    store.upsert_node(
        node_id="node-1",
        hostname="fixture",
        display_name="Fixture",
        role="worker",
        state=NodeState.ONLINE,
    )
    store.upsert_worker(
        WorkerSnapshot(
            id="worker-1",
            node_id="node-1",
            harness=Harness.MOCK,
            provider=Provider.MOCK,
            model=ModelDescriptor("mock", "Mock", Provider.MOCK),
            state=WorkerState.IDLE,
            node_state=NodeState.ONLINE,
            resource_state=ResourceState.AVAILABLE,
            capabilities=frozenset({"analysis"}),
            code_write_allowed=False,
            privacy_allowed=True,
        )
    )
    store.create_task(
        TaskRecord(
            id="task-1",
            project_id="project-1",
            title="Reconcile provider job",
            description="Retain one external job across restart",
            state=TaskState.DRAFT,
            topology=ExecutionTopology.SINGLE,
            requirements=TaskRequirements(
                labels=frozenset({TaskLabel.RESEARCH}),
                required_capabilities=frozenset({"analysis"}),
            ),
        ),
        "#001",
    )
    store.transition_task("task-1", TaskState.QUEUED)
    ready = store.transition_task("task-1", TaskState.READY)
    claim = store.claim_task_dispatch(
        "task-1",
        ["worker-1"],
        expected_version=ready["version"],
        lease_owner_id="runtime-a",
        lease_ttl_seconds=30,
    )
    assert claim is not None
    run_id = claim["runIDs"]["worker-1"]
    generation = int(claim["leaseGeneration"])
    store.prepare_provider_job(
        run_id=run_id,
        adapter_type="test-durable-v1",
        adapter_instance_id="test-provider-instance",
        capabilities=CAPABILITIES,
        lease_owner_id="runtime-a",
        lease_generation=generation,
    )
    return store, "task-1", run_id, generation


def bind_handle(store: StateStore, run_id: str, generation: int) -> dict:
    store.mark_provider_job_launching(
        run_id,
        lease_owner_id="runtime-a",
        lease_generation=generation,
    )
    return store.bind_provider_job_handle(
        run_id=run_id,
        adapter_type="test-durable-v1",
        adapter_instance_id="test-provider-instance",
        handle_version=1,
        provider_job_id="external-job-1",
        provider_session_id="provider-session-1",
        runtime_pid=None,
        runtime_host=None,
        runtime_identity=None,
        adapter_metadata={
            "cursor": "safe-cursor",
            "authHeaders": {"Authorization": "Bearer never-store-this"},
            "X-Api-Key": "never-store-this-either",
            "accessToken": "also-never-store",
        },
        lease_owner_id="runtime-a",
        lease_generation=generation,
    )


def expire_lease(store: StateStore, task_id: str) -> None:
    with store.transaction() as connection:
        connection.execute(
            "UPDATE task_execution_leases SET expires_at='1970-01-01T00:00:00Z' WHERE task_id=?",
            (task_id,),
        )


def test_provider_job_intent_and_handle_survive_restart_without_secrets(tmp_path) -> None:
    store, _task_id, run_id, generation = claimed_execution(tmp_path)
    before = store.highest_event_sequence()

    bound = bind_handle(store, run_id, generation)
    replay = store.prepare_provider_job(
        run_id=run_id,
        adapter_type="test-durable-v1",
        adapter_instance_id="test-provider-instance",
        capabilities=CAPABILITIES,
        lease_owner_id="runtime-a",
        lease_generation=generation,
    )
    reopened = StateStore(store.path).get_provider_job(run_id)

    assert bound["id"] == replay["id"] == reopened["id"]
    assert reopened["provider_job_id"] == "external-job-1"
    assert reopened["launch_state"] == "bound"
    assert reopened["idempotency_key"] == f"supervisor-execution:{run_id}"
    assert reopened["protocol_version"] == 2
    assert reopened["supports_idempotent_launch_lookup"] == 1
    assert reopened["supports_durable_launch_registry"] == 1
    metadata = json.loads(reopened["adapter_metadata_json"])
    assert metadata == {
        "X-Api-Key": "[REDACTED]",
        "accessToken": "[REDACTED]",
        "authHeaders": "[REDACTED]",
        "cursor": "safe-cursor",
    }
    serialized = json.dumps(reopened)
    assert "never-store" not in serialized
    assert [event["kind"] for event in store.list_events(after_sequence=before)] == [
        "providerJobLaunchStarted",
        "providerJobHandleBound",
    ]


def test_provider_lookup_identity_round_trips_without_display_redaction(tmp_path) -> None:
    store, _task_id, run_id, generation = claimed_execution(tmp_path)
    store.mark_provider_job_launching(
        run_id,
        lease_owner_id="runtime-a",
        lease_generation=generation,
    )

    bound = store.bind_provider_job_handle(
        run_id=run_id,
        adapter_type="test-durable-v1",
        adapter_instance_id="test-provider-instance",
        handle_version=1,
        provider_job_id="job-token=opaque-provider-identity",
        provider_session_id="session-token=opaque-session-identity",
        runtime_pid=None,
        runtime_host=None,
        runtime_identity=None,
        adapter_metadata={},
        lease_owner_id="runtime-a",
        lease_generation=generation,
    )

    assert bound["provider_job_id"] == "job-token=opaque-provider-identity"
    assert bound["provider_session_id"] == "session-token=opaque-session-identity"


def test_reconciliation_claim_generation_fences_stale_observation(tmp_path) -> None:
    store, task_id, run_id, generation = claimed_execution(tmp_path)
    bind_handle(store, run_id, generation)
    expire_lease(store, task_id)

    takeover = store.claim_task_reconciliation(
        task_id,
        owner_id="runtime-b",
        lease_ttl_seconds=30,
    )

    assert takeover == generation + 1
    with pytest.raises(ExecutionLeaseLostError):
        store.record_provider_job_observation(
            run_id,
            state="knownCompleted",
            lease_owner_id="runtime-a",
            lease_generation=generation,
        )
    observed = store.record_provider_job_observation(
        run_id,
        state="knownRunning",
        lease_owner_id="runtime-b",
        lease_generation=takeover,
    )
    assert observed["reconciliation_state"] == "knownRunning"


def test_unreachable_provider_is_held_and_escalated_without_duplicate_recovery(tmp_path) -> None:
    store, task_id, run_id, generation = claimed_execution(tmp_path)
    bind_handle(store, run_id, generation)
    expire_lease(store, task_id)
    takeover = store.claim_task_reconciliation(
        task_id,
        owner_id="runtime-b",
        lease_ttl_seconds=30,
    )
    assert takeover is not None
    store.record_provider_job_observation(
        run_id,
        state="providerUnreachable",
        detail="transport unavailable",
        lease_owner_id="runtime-b",
        lease_generation=takeover,
    )
    first = store.request_execution_escalation(
        run_id=run_id,
        code="EXTERNAL_JOB_UNREACHABLE",
        summary="External job cannot be queried safely",
        detail="retrying may duplicate provider work",
        lease_owner_id="runtime-b",
        lease_generation=takeover,
    )
    second = store.request_execution_escalation(
        run_id=run_id,
        code="EXTERNAL_JOB_UNREACHABLE",
        summary="External job cannot be queried safely",
        detail="retrying may duplicate provider work",
        lease_owner_id="runtime-b",
        lease_generation=takeover,
    )
    store.release_task_execution_lease(
        task_id,
        owner_id="runtime-b",
        generation=takeover,
    )

    recovered = store.recover_interrupted({task_id})

    assert first == second
    assert recovered == {"runsInterrupted": 0, "tasksInterrupted": 0}
    assert store.get_task(task_id)["state"] == TaskState.RUNNING.value
    assert store.get_worker_run(run_id)["state"] == RunState.STARTING.value
    assert len(store.list_execution_escalations(run_id=run_id, state="open")) == 1
    kinds = [event["kind"] for event in store.list_events(task_id=task_id)]
    assert kinds.count("humanEscalationRequested") == 1


def test_completed_collection_finalizes_once_and_persisted_cancellation_wins(tmp_path) -> None:
    store, task_id, run_id, generation = claimed_execution(tmp_path)
    bind_handle(store, run_id, generation)
    assert store.activate_worker_run(
        run_id,
        lease_owner_id="runtime-a",
        lease_generation=generation,
    )
    store.save_worker_result(
        run_id=run_id,
        summary="provider completed",
        lease_owner_id="runtime-a",
        lease_generation=generation,
    )
    assert store.cancel_task_execution(task_id)
    before = store.highest_event_sequence()

    with pytest.raises(ExecutionLeaseLostError):
        store.finalize_provider_job_result(
            run_id,
            RunState.COMPLETED,
            exit_code=0,
            lease_owner_id="runtime-a",
            lease_generation=generation,
        )
    cancellation_generation = store.claim_task_reconciliation(
        task_id,
        owner_id="runtime-cancellation",
        lease_ttl_seconds=30,
        allow_cancelled=True,
    )
    assert cancellation_generation is not None
    store.set_worker_state("worker-1", WorkerState.OFFLINE, actor="health-monitor")
    first = store.finalize_provider_job_result(
        run_id,
        RunState.COMPLETED,
        exit_code=0,
        lease_owner_id="runtime-cancellation",
        lease_generation=cancellation_generation,
    )
    second = store.finalize_provider_job_result(
        run_id,
        RunState.COMPLETED,
        exit_code=0,
        lease_owner_id="runtime-cancellation",
        lease_generation=cancellation_generation,
    )

    assert first["state"] == second["state"] == RunState.CANCELLED.value
    assert store.get_task(task_id)["state"] == TaskState.CANCELLED.value
    assert store.get_provider_job(run_id)["result_collection_state"] == "collected"
    assert {row["id"]: row for row in store.list_workers()}["worker-1"]["state"] == "offline"
    kinds = [event["kind"] for event in store.list_events(after_sequence=before)]
    assert kinds.count("workerCompleted") == 0
    assert kinds.count("providerJobResultCollected") == 1


def test_cancelled_run_finalization_does_not_idle_a_reassigned_worker(tmp_path) -> None:
    store, task_id, run_id, generation = claimed_execution(tmp_path)
    bind_handle(store, run_id, generation)
    assert store.activate_worker_run(
        run_id,
        lease_owner_id="runtime-a",
        lease_generation=generation,
    )
    store.save_worker_result(
        run_id=run_id,
        summary="provider completed during cancellation",
        lease_owner_id="runtime-a",
        lease_generation=generation,
    )
    assert store.cancel_task_execution(task_id)
    cancellation_generation = store.claim_task_reconciliation(
        task_id,
        owner_id="runtime-cancellation",
        lease_ttl_seconds=30,
        allow_cancelled=True,
    )
    assert cancellation_generation is not None

    store.create_task(
        TaskRecord(
            id="task-2",
            project_id="project-1",
            title="Replacement assignment",
            description="Use the Worker after canonical cancellation released it",
            state=TaskState.DRAFT,
            topology=ExecutionTopology.SINGLE,
            requirements=TaskRequirements(
                labels=frozenset({TaskLabel.RESEARCH}),
                required_capabilities=frozenset({"analysis"}),
            ),
        ),
        "#002",
    )
    store.transition_task("task-2", TaskState.QUEUED)
    ready = store.transition_task("task-2", TaskState.READY)
    replacement = store.claim_task_dispatch(
        "task-2",
        ["worker-1"],
        expected_version=int(ready["version"]),
        lease_owner_id="runtime-b",
        lease_ttl_seconds=30,
    )
    assert replacement is not None

    store.finalize_provider_job_result(
        run_id,
        RunState.COMPLETED,
        exit_code=0,
        lease_owner_id="runtime-cancellation",
        lease_generation=cancellation_generation,
    )

    worker = {row["id"]: row for row in store.list_workers()}["worker-1"]
    replacement_run = store.get_worker_run(replacement["runIDs"]["worker-1"])
    assert worker["state"] == WorkerState.STARTING.value
    assert replacement_run["state"] == RunState.STARTING.value


def test_task_cancellation_preserves_newer_worker_health_state(tmp_path) -> None:
    store, task_id, run_id, generation = claimed_execution(tmp_path)
    bind_handle(store, run_id, generation)
    assert store.activate_worker_run(
        run_id,
        lease_owner_id="runtime-a",
        lease_generation=generation,
    )
    store.set_worker_state("worker-1", WorkerState.OFFLINE, actor="health-monitor")

    assert store.cancel_task_execution(task_id)

    worker = {row["id"]: row for row in store.list_workers()}["worker-1"]
    assert worker["state"] == WorkerState.OFFLINE.value
    assert store.get_worker_run(run_id)["state"] == RunState.CANCELLED.value


def test_collected_provider_job_replay_is_terminal_and_does_not_rewrite_worker(tmp_path) -> None:
    store, _task_id, run_id, generation = claimed_execution(tmp_path)
    bind_handle(store, run_id, generation)
    assert store.activate_worker_run(
        run_id,
        lease_owner_id="runtime-a",
        lease_generation=generation,
    )
    store.save_worker_result(
        run_id=run_id,
        summary="provider completed",
        lease_owner_id="runtime-a",
        lease_generation=generation,
    )
    store.finalize_provider_job_result(
        run_id,
        RunState.COMPLETED,
        exit_code=0,
        lease_owner_id="runtime-a",
        lease_generation=generation,
    )
    store.set_worker_state("worker-1", WorkerState.OFFLINE, actor="health-monitor")
    before = store.highest_event_sequence()

    store.finalize_provider_job_result(
        run_id,
        RunState.COMPLETED,
        exit_code=0,
        lease_owner_id="runtime-a",
        lease_generation=generation,
    )
    observed = store.record_provider_job_observation(
        run_id,
        state="providerUnreachable",
        detail="late poll raced with collected completion",
        lease_owner_id="runtime-a",
        lease_generation=generation,
    )

    worker = {row["id"]: row for row in store.list_workers()}["worker-1"]
    assert worker["state"] == WorkerState.OFFLINE.value
    assert observed["launch_state"] == "terminal"
    assert observed["reconciliation_state"] == "knownCompleted"
    assert observed["result_collection_state"] == "collected"
    assert store.highest_event_sequence() == before


def test_provider_not_found_is_the_only_automatic_retry_signal(tmp_path) -> None:
    store, task_id, run_id, generation = claimed_execution(tmp_path)
    bind_handle(store, run_id, generation)
    assert store.activate_worker_run(
        run_id,
        lease_owner_id="runtime-a",
        lease_generation=generation,
    )
    store.record_provider_job_observation(
        run_id,
        state="providerNotFound",
        lease_owner_id="runtime-a",
        lease_generation=generation,
    )

    store.interrupt_missing_provider_job(
        run_id,
        lease_owner_id="runtime-a",
        lease_generation=generation,
    )

    assert store.get_worker_run(run_id)["state"] == RunState.INTERRUPTED.value
    assert store.get_task(task_id)["state"] == TaskState.INTERRUPTED.value
    assert store.get_provider_job(run_id)["result_collection_state"] == "notAvailable"
