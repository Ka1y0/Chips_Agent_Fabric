import sqlite3
import subprocess
import sys
from datetime import UTC, datetime, timedelta

import pytest

import project_supervisor.store as store_module
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
from project_supervisor.scheduler import DeterministicScheduler
from project_supervisor.state_machine import InvalidTransition
from project_supervisor.store import (
    ExecutionLeaseLostError,
    StateStore,
    redact_sensitive,
    timestamp,
)


def task_record(task_id: str = "task-1") -> TaskRecord:
    return TaskRecord(
        id=task_id,
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


def worker_snapshot(worker_id: str) -> WorkerSnapshot:
    return WorkerSnapshot(
        id=worker_id,
        node_id="node-1",
        harness=Harness.MOCK,
        provider=Provider.MOCK,
        model=ModelDescriptor("mock-model", "Mock Model", Provider.MOCK),
        state=WorkerState.IDLE,
        node_state=NodeState.ONLINE,
        resource_state=ResourceState.AVAILABLE,
        capabilities=frozenset({"review"}),
        code_write_allowed=False,
        privacy_allowed=True,
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


def test_sensitive_key_variants_are_recursively_redacted_without_hiding_accounting() -> None:
    marker = "CREDENTIAL-MARKER-MUST-NOT-PERSIST"
    credential_keys = (
        "accessToken",
        "access_token",
        "access-token",
        "apiKey",
        "api_key",
        "clientSecret",
        "client_secret",
        "privateKey",
        "private_key",
        "refreshToken",
        "refresh_token",
        "oauthToken",
        "oauth_token",
        "authToken",
        "auth_header",
        "authorization",
        "cookie",
        "sessionCookie",
        "session_cookie",
        "bearerToken",
        "bearer_token",
        "githubToken",
        "service-token",
        "jwt_token",
        "csrfToken",
        "password",
        "databasePassword",
        "secret",
        "sharedSecret",
        "signature",
        "requestSignature",
    )
    accounting = {
        "input_tokens": 101,
        "outputTokens": 23,
        "token_count": 124,
        "remaining_tokens": 900,
        "cache_read_tokens": 17,
        "cacheWriteTokens": 5,
    }

    redacted = redact_sensitive(
        {
            "nested": [{key: marker for key in credential_keys}],
            "accounting": accounting,
        }
    )

    assert set(redacted["nested"][0]) == set(credential_keys)
    assert set(redacted["nested"][0].values()) == {"[REDACTED]"}
    assert redacted["accounting"] == accounting

    formatted = redact_sensitive(
        f"sessionCookie={marker}; bearerToken={marker}; oauth_token={marker}"
    )
    assert marker not in formatted
    assert redact_sensitive("input_tokens=101 token_count=124") == (
        "input_tokens=101 token_count=124"
    )


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


def test_failed_migration_rolls_back_schema_and_version(tmp_path, monkeypatch) -> None:
    migrations = tmp_path / "migrations"
    migrations.mkdir()
    (migrations / "9999_broken.sql").write_text(
        "CREATE TABLE should_be_rolled_back(id INTEGER PRIMARY KEY);\n"
        "INSERT INTO missing_table(id) VALUES (1);\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(StateStore, "MIGRATIONS_PATH", migrations)
    database = tmp_path / "broken.db"

    with pytest.raises(sqlite3.OperationalError, match="missing_table"):
        StateStore(database)

    with sqlite3.connect(database) as connection:
        partial_table = connection.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='should_be_rolled_back'"
        ).fetchone()
        applied_versions = connection.execute(
            "SELECT version FROM schema_migrations ORDER BY version"
        ).fetchall()
    assert partial_table is None
    assert applied_versions == []


def test_concurrent_store_initializers_serialize_migrations(tmp_path) -> None:
    database = tmp_path / "concurrent.db"
    program = "import sys; from project_supervisor.store import StateStore; StateStore(sys.argv[1])"
    processes = [
        subprocess.Popen(
            [sys.executable, "-c", program, str(database)],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        for _ in range(4)
    ]
    failures = []
    for process in processes:
        stdout, stderr = process.communicate(timeout=20)
        if process.returncode != 0:
            failures.append(stderr or stdout)

    assert failures == []
    store = StateStore(database)
    expected = len(list(store.MIGRATIONS_PATH.glob("*.sql")))
    with store.connect() as connection:
        applied = connection.execute("SELECT COUNT(*) FROM schema_migrations").fetchone()[0]
    assert applied == expected


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


def test_dependency_edges_reject_self_and_cycles_and_duplicate_is_idempotent(
    store: StateStore,
) -> None:
    store.create_task(task_record("task-a"), "#001")
    store.create_task(task_record("task-b"), "#002")
    store.create_task(task_record("task-c"), "#003")

    with pytest.raises(ValueError, match="itself"):
        store.add_task_dependency("task-a", "task-a")

    store.add_task_dependency("task-b", "task-a")
    original_events = [
        event for event in store.list_events(limit=200) if event["kind"] == "taskDependencyAdded"
    ]
    assert len(original_events) == 1
    assert original_events[0]["payload"] == {"dependsOnTaskID": "task-a"}

    store.add_task_dependency("task-b", "task-a")
    with pytest.raises(ValueError, match="cycle"):
        store.add_task_dependency("task-a", "task-b")
    store.transition_task("task-c", TaskState.QUEUED)
    store.transition_task("task-c", TaskState.READY)
    store.transition_task("task-c", TaskState.RUNNING)
    with pytest.raises(ValueError, match="after task execution"):
        store.add_task_dependency("task-c", "task-a")

    assert store.task_dependencies("task-b") == [
        {"task_id": "task-a", "state": TaskState.DRAFT.value}
    ]
    assert [
        event for event in store.list_events(limit=200) if event["kind"] == "taskDependencyAdded"
    ] == original_events


def test_claim_task_dispatch_atomically_reserves_task_worker_and_run(
    store: StateStore,
) -> None:
    store.upsert_node(
        node_id="node-1",
        hostname="node-1",
        display_name="Node 1",
        role="control",
        state=NodeState.ONLINE,
    )
    store.upsert_worker(worker_snapshot("worker-1"))
    store.upsert_worker(worker_snapshot("worker-2"))
    store.create_task(task_record(), "#001")
    store.transition_task("task-1", TaskState.QUEUED)
    ready = store.transition_task("task-1", TaskState.READY)

    claim = store.claim_task_dispatch("task-1", ["worker-1"], expected_version=ready["version"])

    assert claim is not None
    assert claim["attempt"] == 1
    run_id = claim["runIDs"]["worker-1"]
    assert store.get_task("task-1")["state"] == TaskState.RUNNING.value
    assert store.get_worker_run(run_id)["state"] == "starting"
    workers = {worker["id"]: worker for worker in store.list_workers()}
    assert workers["worker-1"]["state"] == WorkerState.STARTING.value
    assert workers["worker-2"]["state"] == WorkerState.IDLE.value

    current_version = store.get_task("task-1")["version"]
    assert (
        store.claim_task_dispatch("task-1", ["worker-2"], expected_version=current_version) is None
    )
    assert len(store.list_worker_runs("task-1")) == 1
    assert {worker["id"]: worker["state"] for worker in store.list_workers()} == {
        "worker-1": WorkerState.STARTING.value,
        "worker-2": WorkerState.IDLE.value,
    }


def test_dispatch_claim_rechecks_dependency_added_after_ready_snapshot(
    store: StateStore,
) -> None:
    store.upsert_node(
        node_id="node-1",
        hostname="node-1",
        display_name="Node 1",
        role="control",
        state=NodeState.ONLINE,
    )
    store.upsert_worker(worker_snapshot("worker-1"))
    store.create_task(task_record("prerequisite"), "#001")
    store.create_task(task_record("dependent"), "#002")
    store.transition_task("dependent", TaskState.QUEUED)
    ready_snapshot = store.transition_task("dependent", TaskState.READY)

    # The dependency edge deliberately arrives after a scheduler could have read the READY row.
    # The old version must be fenced, and a caller using the new version still cannot bypass the
    # claim transaction's canonical DAG check.
    store.add_task_dependency("dependent", "prerequisite")
    stale_claim = store.claim_task_dispatch(
        "dependent",
        ["worker-1"],
        expected_version=ready_snapshot["version"],
    )
    current = store.get_task("dependent")
    canonical_claim = store.claim_task_dispatch(
        "dependent",
        ["worker-1"],
        expected_version=current["version"],
    )

    assert stale_claim is None
    assert canonical_claim is None
    assert current["state"] == TaskState.READY.value
    assert current["version"] == ready_snapshot["version"] + 1
    assert store.list_worker_runs("dependent") == []
    assert store.list_workers()[0]["state"] == WorkerState.IDLE.value


def test_stale_execution_generation_cannot_mutate_replacement_attempt(
    store: StateStore,
) -> None:
    store.upsert_node(
        node_id="node-1",
        hostname="node-1",
        display_name="Node 1",
        role="control",
        state=NodeState.ONLINE,
    )
    store.upsert_worker(worker_snapshot("worker-1"))
    store.create_task(task_record(), "#001")
    store.transition_task("task-1", TaskState.QUEUED)
    ready = store.transition_task("task-1", TaskState.READY)
    first = store.claim_task_dispatch(
        "task-1",
        ["worker-1"],
        expected_version=ready["version"],
        lease_owner_id="runtime-a",
        lease_ttl_seconds=6,
    )
    assert first is not None
    first_run = first["runIDs"]["worker-1"]
    assert store.activate_worker_run(
        first_run,
        lease_owner_id="runtime-a",
        lease_generation=first["leaseGeneration"],
    )
    with store.transaction() as connection:
        connection.execute(
            "UPDATE task_execution_leases SET expires_at='1970-01-01T00:00:00Z' "
            "WHERE task_id='task-1'"
        )
    assert store.recover_interrupted({"task-1"}) == {
        "runsInterrupted": 1,
        "tasksInterrupted": 1,
    }
    requeued = store.transition_task("task-1", TaskState.READY, actor="recovery")
    second = store.claim_task_dispatch(
        "task-1",
        ["worker-1"],
        expected_version=requeued["version"],
        lease_owner_id="runtime-b",
        lease_ttl_seconds=6,
    )
    assert second is not None

    with pytest.raises(ExecutionLeaseLostError):
        store.transition_task(
            "task-1",
            TaskState.WAITING,
            lease_owner_id="runtime-a",
            lease_generation=first["leaseGeneration"],
        )
    with pytest.raises(ExecutionLeaseLostError):
        store.transition_worker_run(
            first_run,
            RunState.COMPLETED,
            lease_owner_id="runtime-a",
            lease_generation=first["leaseGeneration"],
        )
    with pytest.raises(ExecutionLeaseLostError):
        store.set_worker_state(
            "worker-1",
            WorkerState.IDLE,
            task_id="task-1",
            lease_owner_id="runtime-a",
            lease_generation=first["leaseGeneration"],
        )
    assert store.get_task("task-1")["state"] == TaskState.RUNNING.value
    assert store.get_task("task-1")["attempt_count"] == 2
    second_run = store.get_worker_run(second["runIDs"]["worker-1"])
    assert second_run["state"] == RunState.STARTING.value
    assert store.list_workers()[0]["state"] == WorkerState.STARTING.value


def test_execution_lease_expiry_rounds_outward_at_second_boundary(
    store: StateStore,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store.upsert_node(
        node_id="node-1",
        hostname="node-1",
        display_name="Node 1",
        role="control",
        state=NodeState.ONLINE,
    )
    store.upsert_worker(worker_snapshot("worker-1"))
    store.create_task(task_record(), "#001")
    store.transition_task("task-1", TaskState.QUEUED)
    ready = store.transition_task("task-1", TaskState.READY)

    class BoundaryDatetime(datetime):
        @classmethod
        def now(cls, tz=None):  # noqa: ANN206
            return cls(2026, 8, 14, 12, 0, 0, 999_000, tzinfo=tz or UTC)

    monkeypatch.setattr(store_module, "datetime", BoundaryDatetime)
    claim = store.claim_task_dispatch(
        "task-1",
        ["worker-1"],
        expected_version=ready["version"],
        lease_owner_id="runtime-boundary",
        lease_ttl_seconds=1,
    )

    assert claim is not None
    with store.connect() as connection:
        lease = connection.execute(
            "SELECT acquired_at,expires_at FROM task_execution_leases WHERE task_id=?",
            ("task-1",),
        ).fetchone()
    assert dict(lease) == {
        "acquired_at": "2026-08-14T12:00:00Z",
        "expires_at": "2026-08-14T12:00:02Z",
    }
    assert store.heartbeat_task_execution_lease(
        "task-1",
        owner_id="runtime-boundary",
        generation=claim["leaseGeneration"],
        ttl_seconds=1,
    )
    assert store.task_execution_lease_is_current(
        "task-1",
        owner_id="runtime-boundary",
        generation=claim["leaseGeneration"],
    )


def test_worker_result_is_immutable_and_identical_replay_is_idempotent(
    store: StateStore,
) -> None:
    store.upsert_node(
        node_id="node-1",
        hostname="node-1",
        display_name="Node 1",
        role="control",
        state=NodeState.ONLINE,
    )
    store.upsert_worker(worker_snapshot("worker-1"))
    store.create_task(task_record(), "#001")
    store.transition_task("task-1", TaskState.QUEUED)
    ready = store.transition_task("task-1", TaskState.READY)
    claim = store.claim_task_dispatch("task-1", ["worker-1"], expected_version=ready["version"])
    assert claim is not None
    run_id = claim["runIDs"]["worker-1"]
    result = {
        "run_id": run_id,
        "summary": "Fixture analyzed",
        "changed_files": ["report.json"],
        "commands_run": ["pytest -q"],
        "tests": [{"name": "unit", "passed": True}],
        "artifacts": ["report.json"],
        "commit_hash": "abc123",
        "blockers": [],
        "confidence": 0.9,
        "recommended_next_actions": ["verify report"],
    }

    store.save_worker_result(**result)
    with store.connect() as connection:
        original = dict(
            connection.execute("SELECT * FROM worker_results WHERE run_id=?", (run_id,)).fetchone()
        )
    store.save_worker_result(**result)
    with pytest.raises(RuntimeError, match="immutable"):
        store.save_worker_result(**{**result, "summary": "Changed after persistence"})

    with store.connect() as connection:
        persisted = dict(
            connection.execute("SELECT * FROM worker_results WHERE run_id=?", (run_id,)).fetchone()
        )
    result_events = [
        event
        for event in store.list_events(task_id="task-1")
        if event["kind"] == "workerResultRecorded"
    ]
    assert persisted == original
    assert len(result_events) == 1


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
        worker = connection.execute("SELECT * FROM workers WHERE id='worker-1'").fetchone()
    assert run["state"] == "interrupted"
    assert worker["state"] == "idle"


def test_unknown_usage_cannot_be_silently_stored_as_null(store: StateStore) -> None:
    now = timestamp()
    with store.connect() as connection, pytest.raises(sqlite3.IntegrityError):
        connection.execute(
            "INSERT INTO usage_records(id,metric,value,unit,confidence,unavailable_reason,"
            "recorded_at) "
            "VALUES ('usage-1','inputTokens',NULL,'tokens','unknown',NULL,?)",
            (now,),
        )
