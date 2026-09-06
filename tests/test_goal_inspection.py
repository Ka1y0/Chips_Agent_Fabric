from __future__ import annotations

import json
import sqlite3
from contextlib import closing
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from project_supervisor import goal_inspection as inspection

NOW = datetime(2030, 1, 1, tzinfo=UTC)
TEXT = "2030-01-01T00:00:00.000000Z"
SCHEMA = """
CREATE TABLE autonomous_goals (
    id TEXT PRIMARY KEY, state TEXT, termination_reason TEXT, iteration_count INTEGER,
    task_count INTEGER, steer_version INTEGER, version INTEGER, updated_at TEXT, intent TEXT
);
CREATE TABLE autonomous_goal_leases (
    goal_id TEXT PRIMARY KEY, host_id TEXT, generation INTEGER, state TEXT, acquired_at TEXT,
    heartbeat_at TEXT, expires_at TEXT, released_at TEXT, last_error TEXT
);
CREATE TABLE autonomous_hosts (
    host_id TEXT PRIMARY KEY, state TEXT, heartbeat_at TEXT, active_goal_count INTEGER,
    last_error TEXT, metadata_json TEXT
);
CREATE TABLE autonomous_iterations (id TEXT PRIMARY KEY, goal_id TEXT);
CREATE TABLE autonomous_actions (id TEXT PRIMARY KEY, goal_id TEXT, state TEXT);
CREATE TABLE events (sequence INTEGER PRIMARY KEY, kind TEXT, entity_id TEXT, payload_json TEXT);
"""


@pytest.fixture
def database(tmp_path: Path) -> Path:
    path = tmp_path / "state.db"
    with closing(sqlite3.connect(path)) as connection, connection:
        connection.executescript(SCHEMA)
        connection.execute(
            "INSERT INTO autonomous_goals VALUES (?,?,?,?,?,?,?,?,?)",
            ("goal", "running", None, 1, 1, 0, 2, TEXT, "private goal text"),
        )
        connection.execute(
            "INSERT INTO autonomous_hosts VALUES (?,?,?,?,?,?)",
            ("private-host", "running", TEXT, 1, None, "private host metadata"),
        )
        connection.execute(
            "INSERT INTO autonomous_goal_leases VALUES (?,?,?,?,?,?,?,?,?)",
            ("goal", "private-host", 1, "owned", TEXT, TEXT,
             "2030-01-01T00:00:10.000000Z", None, None),
        )
        connection.execute("INSERT INTO autonomous_iterations VALUES ('iteration','goal')")
        connection.execute("INSERT INTO autonomous_actions VALUES ('action','goal','running')")
        connection.execute(
            "INSERT INTO events VALUES (1,'autonomousGoalLeaseAcquired','goal','private payload')"
        )
    return path


def update(database: Path, sql: str, parameters: tuple = ()) -> None:
    with closing(sqlite3.connect(database)) as connection, connection:
        connection.execute(sql, parameters)


def test_snapshot_is_read_only_and_does_not_disclose_content(database: Path) -> None:
    before = database.read_bytes()
    report = inspection.inspect_goal(database, "goal", now=NOW)
    assert report["status"] == "observed"
    assert report["leaseObservation"] == "ownedLive"
    assert report["authoritative"] is False
    assert report["executionQuiescence"] == "unknown"
    assert report["eventCursor"] == 1
    assert report["actionCounts"] == {"running": 1}
    serialized = json.dumps(report)
    for secret in ("private", str(database)):
        assert secret not in serialized
    assert report["goalRef"] != "goal"
    assert report["lease"]["ownerRef"] != "private-host"
    assert database.read_bytes() == before


@pytest.mark.parametrize("offset,expected", [(9.999999, "ownedLive"), (10, "ownedExpired"),
                                             (10.000001, "ownedExpired")])
def test_expiry_boundary_is_exact(database: Path, offset: float, expected: str) -> None:
    report = inspection.inspect_goal(database, "goal", now=NOW + timedelta(seconds=offset))
    assert report["leaseObservation"] == expected
    assert ("LEASE_EXPIRED" in report["findings"]) is (expected == "ownedExpired")


@pytest.mark.parametrize("state", ["softPaused", "hardPaused", "stopped", "terminated"])
@pytest.mark.parametrize("lease_state", ["owned", "released", "lost"])
def test_control_and_lease_are_separate_observations(
    database: Path, state: str, lease_state: str,
) -> None:
    update(database, "UPDATE autonomous_goals SET state=?", (state,))
    update(database, "UPDATE autonomous_goal_leases SET state=?", (lease_state,))
    report = inspection.inspect_goal(database, "goal", now=NOW)
    assert report["goal"]["state"] == state
    assert report["lease"]["state"] == lease_state
    assert "GOAL_NOT_RUNNABLE" in report["findings"]
    assert ("CONTROL_AWAITING_LEASE_RELEASE" in report["findings"]) is (lease_state == "owned")
    assert report["executionQuiescence"] == "unknown"


@pytest.mark.parametrize("value", ["not a timestamp", TEXT[:-1], "secret" * 100, None])
def test_unknown_deadline_never_means_a_live_lease(database: Path, value: str | None) -> None:
    update(database, "UPDATE autonomous_goal_leases SET expires_at=?", (value,))
    report = inspection.inspect_goal(database, "goal", now=NOW)
    assert report["leaseObservation"] == "unknown"
    assert report["lease"]["expiresAt"] is None


def test_host_errors_and_untrusted_enum_values_are_not_echoed(database: Path) -> None:
    update(database, "UPDATE autonomous_hosts SET last_error='private exception' ")
    update(database, "UPDATE autonomous_goals SET termination_reason='private reason'")
    update(database, "UPDATE autonomous_actions SET state='private action'")
    update(database, "UPDATE events SET kind='private event'")
    report = inspection.inspect_goal(database, "goal", now=NOW)
    assert report["host"]["hasError"] is True
    assert "HOST_HAS_RECORDED_ERROR" in report["findings"]
    assert report["goal"]["terminationReason"] == "unknown"
    assert report["actionCounts"] == {"unknown": 1}
    assert report["events"][0]["kind"] == "unknown"
    assert "private" not in json.dumps(report)


def test_bounded_events_are_chronological_scoped_and_explicitly_truncated(database: Path) -> None:
    for sequence, entity in [(2, "iteration"), (3, "other-goal"), (4, "action")]:
        update(
            database, "INSERT INTO events VALUES (?,'goalStarted',?,'private')", (sequence, entity)
        )
    report = inspection.inspect_goal(database, "goal", now=NOW, event_limit=2)
    assert [event["sequence"] for event in report["events"]] == [2, 4]
    assert report["eventsTruncated"] is True
    assert report["eventCursor"] == 4


def test_missing_goal_lease_and_host_are_not_invented(database: Path) -> None:
    update(database, "DELETE FROM autonomous_goal_leases")
    report = inspection.inspect_goal(database, "goal", now=NOW)
    assert report["leaseObservation"] == "absent"
    assert report["lease"] is None
    assert report["host"] is None
    assert inspection.inspect_goal(database, "missing")["reason"] == "goalNotFound"


@pytest.mark.parametrize("limit", [0, 101, True, 1.5, "2"])
def test_invalid_event_limit_fails_before_open(database: Path, limit: object) -> None:
    assert inspection.inspect_goal(database, "goal", event_limit=limit)["reason"] == "invalidInput"


def test_bad_clock_and_identity_are_rejected(database: Path) -> None:
    invalid = inspection.inspect_goal(database, "goal", now=datetime(2030, 1, 1))
    assert invalid["reason"] == "invalidInput"
    assert inspection.inspect_goal(database, "")["reason"] == "invalidInput"
    assert inspection.inspect_goal(database, "x" * 257)["reason"] == "invalidInput"


def test_missing_database_is_not_created(tmp_path: Path) -> None:
    path = tmp_path / "absent-parent" / "secret.db"
    assert inspection.inspect_goal(path, "goal")["reason"] == "databaseUnavailable"
    assert not path.parent.exists()


def test_paths_are_uri_escaped_and_goal_id_is_a_parameter(database: Path) -> None:
    renamed = database.with_name("state?#.db")
    database.rename(renamed)
    assert inspection.inspect_goal(renamed, "goal", now=NOW)["status"] == "observed"
    assert inspection.inspect_goal(renamed, "' OR 1=1 --")["reason"] == "goalNotFound"


def test_database_symlink_and_views_are_rejected(database: Path) -> None:
    link = database.with_name("link.db")
    link.symlink_to(database)
    assert inspection.inspect_goal(link, "goal")["reason"] == "databaseUnavailable"
    update(database, "DROP TABLE autonomous_hosts")
    update(database, "CREATE VIEW autonomous_hosts AS SELECT 'private' AS state")
    assert inspection.inspect_goal(database, "goal")["reason"] == "schemaUnavailable"


def test_bad_database_does_not_leak_path(tmp_path: Path) -> None:
    path = tmp_path / "private-name.db"
    path.write_bytes(b"not sqlite")
    report = inspection.inspect_goal(path, "goal")
    assert report["reason"] == "databaseUnavailable"
    assert "private" not in json.dumps(report)


def test_live_wal_is_read_and_snapshot_does_not_mix_generations(
    database: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    with closing(sqlite3.connect(database)) as writer:
        writer.execute("PRAGMA journal_mode=WAL")
        writer.execute("UPDATE autonomous_goals SET state='softPaused'")
        writer.commit()
        assert Path(str(database) + "-wal").stat().st_size > 0
        original = inspection._snapshot

        def concurrent_write(connection, goal_id, now, event_limit):
            writer.execute("UPDATE autonomous_goals SET state='running'")
            writer.execute("UPDATE autonomous_goal_leases SET generation=2")
            writer.execute("INSERT INTO events VALUES (2,'goalResumed','goal','private')")
            writer.commit()
            return original(connection, goal_id, now, event_limit)

        monkeypatch.setattr(inspection, "_snapshot", concurrent_write)
        report = inspection.inspect_goal(database, "goal", now=NOW)
        assert report["goal"]["state"] == "softPaused"
        assert report["lease"]["generation"] == 1
        assert report["eventCursor"] == 1
        monkeypatch.setattr(inspection, "_snapshot", original)
        latest = inspection.inspect_goal(database, "goal", now=NOW)
        assert latest["goal"]["state"] == "running"
        assert latest["lease"]["generation"] == 2
        assert latest["eventCursor"] == 2


def test_connection_closes_on_success_and_failure(database: Path, monkeypatch) -> None:
    opened = []
    original = inspection.sqlite3.connect

    def recording_connect(*args, **kwargs):
        connection = original(*args, **kwargs)
        opened.append(connection)
        return connection

    monkeypatch.setattr(inspection.sqlite3, "connect", recording_connect)
    inspection.inspect_goal(database, "goal", now=NOW)
    inspection.inspect_goal(database, "absent", now=NOW)
    assert len(opened) == 2
    for connection in opened:
        with pytest.raises(sqlite3.ProgrammingError, match="closed"):
            connection.execute("SELECT 1")


def test_read_transaction_cannot_write_or_initialize(database: Path) -> None:
    with inspection._open_snapshot(database) as connection:
        with pytest.raises(sqlite3.OperationalError):
            connection.execute("UPDATE autonomous_goals SET state='stopped'")
    assert inspection.inspect_goal(database, "goal", now=NOW)["goal"]["state"] == "running"


def test_recent_goals_and_cli_are_bounded_and_machine_readable(database: Path, capsys) -> None:
    for number in range(5):
        update(database, "INSERT INTO autonomous_goals SELECT ?,state,termination_reason,"
               "iteration_count,task_count,steer_version,version,updated_at,intent "
               "FROM autonomous_goals WHERE id='goal'", (f"goal-{number}",))
    reports = inspection.inspect_recent_goals(database)
    assert len(reports) == 3
    assert all(report["status"] == "observed" for report in reports)
    assert inspection.main(["--database", str(database), "--goal", "goal"]) == 0
    assert json.loads(capsys.readouterr().out)["schemaVersion"] == "goal-inspection/v1"
    assert inspection.main(["--database", str(database), "--goal", "absent"]) == 2
    assert json.loads(capsys.readouterr().out)["reason"] == "goalNotFound"
    with pytest.raises(SystemExit) as error:
        inspection.main(["--secret-private-option"])
    assert error.value.code == 2
    captured = capsys.readouterr()
    assert json.loads(captured.out)["reason"] == "invalidInput"
    assert "secret" not in captured.out + captured.err


def test_all_report_shapes_validate_against_closed_schema(database: Path) -> None:
    from jsonschema import Draft202012Validator

    path = Path(__file__).resolve().parents[1] / "schemas" / "goal-inspection-v1.schema.json"
    schema = json.loads(path.read_text(encoding="utf-8"))
    Draft202012Validator.check_schema(schema)
    validator = Draft202012Validator(schema)
    for goal_state in ("running", "softPaused", "hardPaused", "stopped", "terminated"):
        for lease_state in ("owned", "lost", "released"):
            update(database, "UPDATE autonomous_goals SET state=?", (goal_state,))
            update(database, "UPDATE autonomous_goal_leases SET state=?", (lease_state,))
            validator.validate(inspection.inspect_goal(database, "goal", now=NOW))
            expired = inspection.inspect_goal(database, "goal", now=NOW + timedelta(days=1))
            validator.validate(expired)
    validator.validate(inspection.inspect_goal(database, "absent"))
    invalid = inspection.inspect_goal(database, "goal")
    invalid["unexpected"] = "private"
    assert list(validator.iter_errors(invalid))


def test_excessive_state_cardinality_fails_closed(database: Path) -> None:
    with closing(sqlite3.connect(database)) as connection, connection:
        connection.executemany(
            "INSERT INTO autonomous_actions VALUES (?,'goal',?)",
            [(f"action-{number}", f"untrusted-{number}") for number in range(33)],
        )
    assert inspection.inspect_goal(database, "goal")["reason"] == "stateCardinalityExceeded"


def test_query_budget_aborts_scan_without_echoing_database_contents(database: Path, monkeypatch):
    with closing(sqlite3.connect(database)) as connection, connection:
        connection.executemany(
            "INSERT INTO events VALUES (?,'private-kind','other','private-payload')",
            [(number,) for number in range(2, 2002)],
        )
    readings = iter([0.0])
    monkeypatch.setattr(inspection.time, "monotonic", lambda: next(readings, 1.0))
    report = inspection.inspect_goal(database, "goal", now=NOW)
    assert report["reason"] == "databaseUnavailable"
    assert "private" not in json.dumps(report)
