import json
from datetime import UTC, datetime, timedelta

import pytest

from project_supervisor.domain import (
    EvidenceConfidence,
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
    TelemetryValue,
    UnavailableReason,
    WorkerSnapshot,
    WorkerState,
)
from project_supervisor.hybrid import ExecutionHistoryRecord
from project_supervisor.protocols.capabilities import CapabilityGrant
from project_supervisor.protocols.identity import NodePublicIdentity
from project_supervisor.state_machine import InvalidTransition
from project_supervisor.store import StateStore


@pytest.fixture
def populated_store(tmp_path) -> StateStore:
    store = StateStore(tmp_path / "state.db")
    store.create_project(
        project_id="project-1",
        name="Fixture",
        root_path=str(tmp_path / "fixture"),
        goal="Analyze safely",
    )
    store.upsert_node(
        node_id="node-1",
        hostname="node-1",
        display_name="Control Node",
        role="control",
        state=NodeState.ONLINE,
        capabilities={"nativeWorkers"},
    )
    store.upsert_worker(
        WorkerSnapshot(
            id="worker-1",
            node_id="node-1",
            harness=Harness.MOCK,
            provider=Provider.MOCK,
            model=ModelDescriptor(
                "mock-v1", "Mock V1", Provider.MOCK, context_window_tokens=32_000
            ),
            state=WorkerState.IDLE,
            node_state=NodeState.ONLINE,
            resource_state=ResourceState.AVAILABLE,
            capabilities=frozenset({"review"}),
            code_write_allowed=False,
            privacy_allowed=True,
        )
    )
    store.create_task(
        TaskRecord(
            id="task-1",
            project_id="project-1",
            title="Analyze fixture",
            description="Read-only",
            state=TaskState.DRAFT,
            topology=ExecutionTopology.SINGLE,
            requirements=TaskRequirements(
                labels=frozenset({TaskLabel.REVIEW}),
                required_capabilities=frozenset({"review"}),
            ),
        ),
        "#001",
    )
    store.transition_task("task-1", TaskState.QUEUED)
    store.transition_task("task-1", TaskState.READY)
    store.transition_task("task-1", TaskState.RUNNING)
    return store


def test_registry_preserves_node_worker_provider_harness_model_distinctions(
    populated_store: StateStore,
) -> None:
    snapshots = populated_store.worker_snapshots()
    assert len(snapshots) == 1
    worker = snapshots[0]
    assert worker.node_id == "node-1"
    assert worker.harness is Harness.MOCK
    assert worker.provider is Provider.MOCK
    assert worker.model.identifier == "mock-v1"


def test_worker_run_lifecycle_and_result_are_durable(populated_store: StateStore) -> None:
    run_id = populated_store.create_worker_run(task_id="task-1", worker_id="worker-1", attempt=1)
    with pytest.raises(InvalidTransition):
        populated_store.transition_worker_run(run_id, RunState.COMPLETED)

    populated_store.transition_worker_run(run_id, RunState.RUNNING, process_id=1234)
    populated_store.transition_worker_run(run_id, RunState.COMPLETED, exit_code=0)
    populated_store.save_worker_result(
        run_id=run_id,
        summary="Fixture analyzed",
        changed_files=[],
        tests=[{"name": "schema", "passed": True}],
        confidence=0.9,
    )

    run = populated_store.get_worker_run(run_id)
    assert run["state"] == RunState.COMPLETED.value
    assert run["process_id"] == 1234
    assert run["exit_code"] == 0
    with populated_store.connect() as connection:
        result = connection.execute(
            "SELECT * FROM worker_results WHERE run_id=?", (run_id,)
        ).fetchone()
    assert result["summary"] == "Fixture analyzed"
    assert json.loads(result["changed_files_json"]) == []


def test_usage_known_zero_is_distinct_from_unavailable(populated_store: StateStore) -> None:
    known_id = populated_store.record_usage(
        metric="inputTokens",
        telemetry=TelemetryValue(0, EvidenceConfidence.EXACT),
        unit="tokens",
        task_id="task-1",
        worker_id="worker-1",
    )
    unknown_id = populated_store.record_usage(
        metric="remainingQuota",
        telemetry=TelemetryValue(
            None,
            EvidenceConfidence.UNKNOWN,
            UnavailableReason.NOT_REPORTED,
        ),
        unit="requests",
        task_id="task-1",
        worker_id="worker-1",
    )
    with populated_store.connect() as connection:
        known = connection.execute("SELECT * FROM usage_records WHERE id=?", (known_id,)).fetchone()
        unknown = connection.execute(
            "SELECT * FROM usage_records WHERE id=?", (unknown_id,)
        ).fetchone()
    assert known["value"] == 0
    assert known["unavailable_reason"] is None
    assert unknown["value"] is None
    assert unknown["unavailable_reason"] == "notReported"


def test_normalized_execution_history_is_durable_and_filterable(
    populated_store: StateStore,
) -> None:
    record = ExecutionHistoryRecord(
        id="history-1",
        task_id="task-1",
        task_type="review",
        worker_id="worker-1",
        provider="mock-provider",
        model="mock-v1",
        node_id="node-1",
        topology=ExecutionTopology.SINGLE,
        latency_seconds=0.25,
        succeeded=False,
        failure_class="transient",
        retry_count=1,
        input_tokens=3,
        output_tokens=0,
        review_outcome="token=must-not-persist",
    )
    populated_store.record_execution_history(record)

    history = populated_store.list_execution_history(worker_id="worker-1")
    assert len(history) == 1
    assert history[0]["task_type"] == "review"
    assert history[0]["failure_class"] == "transient"
    assert history[0]["review_outcome"] == "token=[REDACTED]"
    assert populated_store.list_execution_history(task_id="missing") == []


def test_capability_grant_lifecycle_is_auditable_and_revocable(
    populated_store: StateStore,
) -> None:
    issued = datetime(2026, 8, 9, tzinfo=UTC)
    populated_store.record_capability_grant(
        CapabilityGrant(
            grant_id="grant-1",
            capability="service.start",
            subject="node:node-1",
            requested_by="agent:bootstrap",
            task_id="task-1",
            issued_by="admin:operator",
            issued_at=issued,
            expires_at=issued + timedelta(minutes=5),
        )
    )
    populated_store.revoke_capability_grant("grant-1", revoked_by="admin:operator")

    with populated_store.connect() as connection:
        grant = connection.execute("SELECT * FROM capability_grants WHERE id='grant-1'").fetchone()
    assert grant["state"] == "revoked"
    assert grant["revoked_by"] == "admin:operator"
    assert [event["kind"] for event in populated_store.list_events(task_id="task-1")][-2:] == [
        "capabilityGrantRecorded",
        "capabilityGrantRevoked",
    ]


def test_node_identity_store_accepts_only_public_metadata(populated_store: StateStore) -> None:
    populated_store.record_node_public_identity(
        NodePublicIdentity(
            node_id="node-1",
            key_id="key-1",
            algorithm="ed25519",
            public_key_fingerprint="sha256:" + "b" * 64,
        )
    )
    with populated_store.connect() as connection:
        row = connection.execute("SELECT * FROM node_public_identities").fetchone()
        columns = {
            item[1] for item in connection.execute("PRAGMA table_info(node_public_identities)")
        }
    assert row["public_key_fingerprint"].startswith("sha256:")
    assert columns.isdisjoint({"private_key", "secret", "seed"})
