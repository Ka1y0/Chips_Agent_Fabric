import sqlite3
from datetime import UTC, datetime, timedelta

import pytest

from project_supervisor.domain import (
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
from project_supervisor.scheduler import DeterministicScheduler
from project_supervisor.state_machine import InvalidTransition
from project_supervisor.store import StateStore, timestamp


def task_record() -> TaskRecord:
    return TaskRecord(
        id="task-1",
        project_id="project-1",
        title="Inspect fixture",
        description="Read-only analysis of an isolated fixture",
        state=TaskState.DRAFT,
        topology=ExecutionTopology.SINGLE,
        requirements=TaskRequirements(
            labels=frozenset({TaskLabel.REVIEW}),
            required_capabilities=frozenset({"review"}),
        ),
    )


@pytest.fixture
def store(tmp_path) -> StateStore:
    value = StateStore(tmp_path / "state.db")
    value.create_project(
        project_id="project-1",
        name="Fixture",
        root_path=str(tmp_path / "fixture"),
        goal="Analyze fixture",
    )
    return value


def test_task_state_and_event_commit_together(store: StateStore) -> None:
    store.create_task(task_record(), "#001")
    before = store.highest_event_sequence()
    updated = store.transition_task("task-1", TaskState.QUEUED)
    events = store.list_events(after_sequence=before)

    assert updated["state"] == TaskState.QUEUED.value
    assert len(events) == 1
    assert events[0]["kind"] == "taskStateChanged"
    assert events[0]["payload"] == {"from": "draft", "to": "queued"}


def test_illegal_transition_rolls_back_without_event(store: StateStore) -> None:
    store.create_task(task_record(), "#001")
    before = store.highest_event_sequence()
    with pytest.raises(InvalidTransition):
        store.transition_task("task-1", TaskState.SUCCEEDED)
    assert store.get_task("task-1")["state"] == TaskState.DRAFT.value
    assert store.highest_event_sequence() == before


def test_event_journal_is_database_enforced_append_only(store: StateStore) -> None:
    event = store.list_events(limit=1)[0]
    with store.connect() as connection, pytest.raises(Exception, match="append-only"):
        connection.execute(
            "UPDATE events SET summary='tampered' WHERE sequence=?", (event["sequence"],)
        )
    with store.connect() as connection, pytest.raises(Exception, match="append-only"):
        connection.execute("DELETE FROM events WHERE sequence=?", (event["sequence"],))


def test_routing_decision_and_reasons_are_durable(store: StateStore) -> None:
    record = task_record()
    store.create_task(record, "#001")
    model = ModelDescriptor("mock-model", "Mock Model", Provider.MOCK, context_window_tokens=32_000)
    worker = WorkerSnapshot(
        id="mock-reviewer",
        node_id="mock-node",
        harness=Harness.MOCK,
        provider=Provider.MOCK,
        model=model,
        state=WorkerState.IDLE,
        node_state=NodeState.ONLINE,
        resource_state=ResourceState.AVAILABLE,
        capabilities=frozenset({"review"}),
        code_write_allowed=False,
        privacy_allowed=True,
    )
    decision = DeterministicScheduler().schedule(
        task_id=record.id,
        requirements=record.requirements,
        topology=record.topology,
        workers=[worker],
    )
    decision_id = store.persist_routing_decision(task_id=record.id, decision=decision)

    with store.connect() as connection:
        persisted = connection.execute(
            "SELECT * FROM routing_decisions WHERE id=?", (decision_id,)
        ).fetchone()
        candidate = connection.execute(
            "SELECT * FROM routing_candidates WHERE decision_id=?", (decision_id,)
        ).fetchone()
    assert persisted is not None
    assert candidate["worker_id"] == "mock-reviewer"
    assert candidate["selected"] == 1
    assert store.list_events(task_id="task-1")[-1]["kind"] == "routingDecisionRecorded"


def test_api_token_is_hashed_scoped_expirable_and_never_recoverable(store: StateStore) -> None:
    token_id, token = store.issue_api_token(label="monitor", scopes={"observe:read"})
    assert store.verify_api_token(token, "observe:read")
    assert not store.verify_api_token(token, "tasks:write")

    with store.connect() as connection:
        row = connection.execute("SELECT * FROM api_tokens WHERE id=?", (token_id,)).fetchone()
    assert token.encode() not in bytes(row["token_hash"])
    assert token.encode() not in bytes(row["token_salt"])

    _, expired = store.issue_api_token(
        label="expired",
        scopes={"observe:read"},
        expires_at=datetime.now(UTC) - timedelta(seconds=1),
    )
    assert not store.verify_api_token(expired, "observe:read")


def test_boot_recovery_marks_runs_and_tasks_interrupted(store: StateStore) -> None:
    store.create_task(task_record(), "#001")
    store.transition_task("task-1", TaskState.QUEUED)
    store.transition_task("task-1", TaskState.READY)
    store.transition_task("task-1", TaskState.RUNNING)
    now = timestamp()
    with store.transaction() as connection:
        connection.execute(
            "INSERT INTO nodes(id,hostname,display_name,role,state,created_at,updated_at) "
            "VALUES ('node-1','node-1','Node 1','control','online',?,?)",
            (now, now),
        )
        connection.execute(
            "INSERT INTO workers(id,node_id,harness,provider,state,resource_state,"
            "capabilities_json,code_write_allowed,privacy_allowed,created_at,updated_at) "
            "VALUES ('worker-1','node-1','mock','mock','running','available','[]',0,1,?,?)",
            (now, now),
        )
        connection.execute(
            "INSERT INTO worker_runs(id,task_id,worker_id,state,attempt,created_at,updated_at) "
            "VALUES ('run-1','task-1','worker-1','running',1,?,?)",
            (now, now),
        )

    result = store.recover_interrupted()
    assert result == {"runsInterrupted": 1, "tasksInterrupted": 1}
    assert store.get_task("task-1")["state"] == TaskState.INTERRUPTED.value
    with store.connect() as connection:
        run = connection.execute("SELECT * FROM worker_runs WHERE id='run-1'").fetchone()
    assert run["state"] == "interrupted"


def test_unknown_usage_cannot_be_silently_stored_as_null(store: StateStore) -> None:
    now = timestamp()
    with store.connect() as connection, pytest.raises(sqlite3.IntegrityError):
        connection.execute(
            "INSERT INTO usage_records(id,metric,value,unit,confidence,unavailable_reason,"
            "recorded_at) "
            "VALUES ('usage-1','inputTokens',NULL,'tokens','unknown',NULL,?)",
            (now,),
        )
