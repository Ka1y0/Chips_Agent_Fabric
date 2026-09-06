"""Bounded, read-only Goal diagnostics. This module never constructs a StateStore."""

from __future__ import annotations

import argparse
import hashlib
import json
import sqlite3
import time
from collections.abc import Iterator, Sequence
from contextlib import closing, contextmanager
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

SCHEMA_VERSION = "goal-inspection/v1"
_TABLES = frozenset({
    "autonomous_goals", "autonomous_goal_leases", "autonomous_hosts",
    "autonomous_iterations", "autonomous_actions", "events",
})
_GOAL_STATES = frozenset({
    "created", "running", "softPaused", "hardPaused", "stopped", "terminated",
})
_LEASE_STATES = frozenset({"owned", "released", "lost"})
_HOST_STATES = frozenset({"starting", "running", "stopping", "stopped", "failed"})
_REASONS = frozenset({
    "SUCCESS", "BLOCKED", "NO_PROGRESS", "ITERATION_LIMIT", "BUDGET_LIMIT",
    "REPEATED_FAILURE", "SAFETY_BOUNDARY", "PERMISSION_REQUIRED", "HUMAN_ESCALATION",
    "USER_STOPPED",
})
_ACTION_STATES = frozenset({
    "planned", "dispatching", "running", "completed", "failed", "cancelled",
})
_EVENT_KINDS = frozenset({
    "autonomousGoalLeaseAcquired", "autonomousGoalLeaseRecovered", "autonomousGoalLeaseLost",
    "autonomousGoalLeaseReleased", "goalCreated", "goalStarted", "goalPaused", "goalResumed",
    "goalStopped", "goalTerminated", "goalSteered", "goalIterationStarted", "goalEvaluated",
    "goalPlanCreated", "goalActionDispatched", "goalActionCompleted", "goalActionFailed",
    "goalActionCancellationRequested", "goalReplanRequired", "goalVerified",
})


class _Unavailable(Exception):
    def __init__(self, reason: str) -> None:
        self.reason = reason


def _base() -> dict[str, Any]:
    return {"schemaVersion": SCHEMA_VERSION, "readOnly": True, "authoritative": False}


def _unavailable(reason: str) -> dict[str, Any]:
    return {**_base(), "status": "unavailable", "reason": reason}


def _enum(value: Any, allowed: frozenset[str]) -> str:
    return value if isinstance(value, str) and value in allowed else "unknown"


def _number(value: Any) -> int | None:
    return value if type(value) is int and 0 <= value <= (2**63 - 1) else None


def _reference(value: Any) -> str | None:
    if not isinstance(value, str) or not value or len(value) > 4096:
        return None
    return "sha256:" + hashlib.sha256(b"fabric-inspection\0" + value.encode("utf-8")).hexdigest()


def _date(value: Any) -> datetime | None:
    if not isinstance(value, str) or len(value) > 64:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if parsed.tzinfo is None or parsed.utcoffset() is None:
            return None
        return parsed.astimezone(UTC)
    except (ValueError, OverflowError):
        return None


def _date_text(value: Any) -> str | None:
    parsed = _date(value)
    return parsed.isoformat(timespec="microseconds").replace("+00:00", "Z") if parsed else None


@contextmanager
def _open_snapshot(database: str | Path) -> Iterator[sqlite3.Connection]:
    candidate = Path(database)
    if candidate.is_symlink() or not candidate.is_file():
        raise _Unavailable("databaseUnavailable")
    # Do not use immutable=1: a live database may have committed changes in WAL.
    with closing(sqlite3.connect(
        candidate.resolve(strict=True).as_uri() + "?mode=ro", uri=True,
        isolation_level=None, timeout=0.1,
    )) as connection:
        connection.row_factory = sqlite3.Row
        connection.setlimit(sqlite3.SQLITE_LIMIT_LENGTH, 262144)
        connection.execute("PRAGMA query_only=ON")
        connection.execute("PRAGMA trusted_schema=OFF")
        deadline = time.monotonic() + 0.5
        remaining = 1000

        def progress() -> int:
            nonlocal remaining
            remaining -= 1
            return int(remaining <= 0 or time.monotonic() >= deadline)

        connection.set_progress_handler(progress, 1000)
        connection.execute("BEGIN")
        # This first read pins a coherent SQLite snapshot. Refuse views as state tables.
        names = {
            row[0] for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name IN (?,?,?,?,?,?)",
                tuple(sorted(_TABLES)),
            )
        }
        if names != _TABLES:
            raise _Unavailable("schemaUnavailable")
        yield connection


def _snapshot(
    connection: sqlite3.Connection, goal_id: str, now: datetime, event_limit: int,
) -> dict[str, Any]:
    goal = connection.execute(
        "SELECT id,state,termination_reason,iteration_count,task_count,steer_version,version "
        "FROM autonomous_goals WHERE id=?", (goal_id,),
    ).fetchone()
    if goal is None:
        raise _Unavailable("goalNotFound")
    lease = connection.execute(
        "SELECT host_id,generation,state,acquired_at,heartbeat_at,expires_at,released_at,"
        "last_error IS NOT NULL AS has_error FROM autonomous_goal_leases WHERE goal_id=?",
        (goal_id,),
    ).fetchone()
    host = None if lease is None else connection.execute(
        "SELECT state,heartbeat_at,active_goal_count,last_error IS NOT NULL AS has_error "
        "FROM autonomous_hosts WHERE host_id=?", (lease["host_id"],),
    ).fetchone()
    events = connection.execute(
        "SELECT sequence,kind FROM events WHERE entity_id=? "
        "OR entity_id IN (SELECT id FROM autonomous_iterations WHERE goal_id=?) "
        "OR entity_id IN (SELECT id FROM autonomous_actions WHERE goal_id=?) "
        "ORDER BY sequence DESC LIMIT ?", (goal_id, goal_id, goal_id, event_limit + 1),
    ).fetchall()
    counts = connection.execute(
        "SELECT state,COUNT(*) AS count FROM autonomous_actions WHERE goal_id=? "
        "GROUP BY state LIMIT 33", (goal_id,),
    ).fetchall()
    if len(counts) > 32:
        raise _Unavailable("stateCardinalityExceeded")
    action_counts: dict[str, int] = {}
    for row in counts:
        state = _enum(row["state"], _ACTION_STATES)
        action_counts[state] = action_counts.get(state, 0) + row["count"]
    cursor = connection.execute("SELECT MAX(sequence) FROM events").fetchone()[0]
    goal_state = _enum(goal["state"], _GOAL_STATES)
    lease_state = "absent" if lease is None else _enum(lease["state"], _LEASE_STATES)
    expiry = None if lease is None else _date(lease["expires_at"])
    observation = lease_state
    if lease_state == "owned":
        observation = (
            "unknown" if expiry is None else "ownedExpired" if expiry <= now else "ownedLive"
        )
    findings = []
    if observation == "ownedExpired":
        findings.append("LEASE_EXPIRED")
    if observation == "lost":
        findings.append("LEASE_LOST")
    if goal_state in {"softPaused", "hardPaused", "stopped", "terminated"}:
        findings.append("GOAL_NOT_RUNNABLE")
        if lease_state == "owned":
            findings.append("CONTROL_AWAITING_LEASE_RELEASE")
    if host is not None and host["has_error"]:
        findings.append("HOST_HAS_RECORDED_ERROR")
    return {
        **_base(), "status": "observed", "capturedAt": now.isoformat().replace("+00:00", "Z"),
        "eventCursor": _number(cursor), "goalRef": _reference(goal["id"]),
        "goal": {
            "state": goal_state, "terminationReason": (
                None if goal["termination_reason"] is None
                else _enum(goal["termination_reason"], _REASONS)
            ),
            "iterationCount": _number(goal["iteration_count"]),
            "taskCount": _number(goal["task_count"]),
            "steerVersion": _number(goal["steer_version"]), "version": _number(goal["version"]),
        },
        "leaseObservation": observation,
        "lease": None if lease is None else {
            "ownerRef": _reference(lease["host_id"]), "generation": _number(lease["generation"]),
            "state": lease_state, "acquiredAt": _date_text(lease["acquired_at"]),
            "heartbeatAt": _date_text(lease["heartbeat_at"]),
            "expiresAt": _date_text(lease["expires_at"]),
            "releasedAt": _date_text(lease["released_at"]), "hasError": bool(lease["has_error"]),
        },
        "host": None if host is None else {
            "state": _enum(host["state"], _HOST_STATES),
            "heartbeatAt": _date_text(host["heartbeat_at"]),
            "activeGoalCount": _number(host["active_goal_count"]),
            "hasError": bool(host["has_error"]),
        },
        "actionCounts": action_counts, "findings": findings,
        "events": [
            {"sequence": _number(row["sequence"]), "kind": _enum(row["kind"], _EVENT_KINDS)}
            for row in reversed(events[:event_limit])
        ],
        "eventsTruncated": len(events) > event_limit,
        "eventScope": "goal-and-its-iterations-and-actions",
        "providerExecution": "notInspected", "executionQuiescence": "unknown",
    }


def inspect_goal(
    database: str | Path, goal_id: str, *, now: datetime | None = None, event_limit: int = 20,
) -> dict[str, Any]:
    """Inspect one explicit Goal without migrations, recovery, prompts, or authority changes.

    References are pseudonymous, not anonymous. Timing/sequence metadata can be sensitive.
    A stopped Goal or released lease is not proof that external execution has stopped.
    """
    if not isinstance(goal_id, str) or not goal_id or len(goal_id) > 256:
        return _unavailable("invalidInput")
    if type(event_limit) is not int or not 1 <= event_limit <= 100:
        return _unavailable("invalidInput")
    if now is not None and (
        not isinstance(now, datetime) or now.tzinfo is None or now.utcoffset() is None
    ):
        return _unavailable("invalidInput")
    try:
        with _open_snapshot(database) as connection:
            observed_at = (now or datetime.now(UTC)).astimezone(UTC)
            return _snapshot(connection, goal_id, observed_at, event_limit)
    except _Unavailable as error:
        return _unavailable(error.reason)
    except (OSError, sqlite3.Error, ValueError, TypeError, OverflowError):
        # Database exceptions may include SQL, filesystem paths or user-provided values.
        return _unavailable("databaseUnavailable")


def inspect_recent_goals(database: str | Path) -> list[dict[str, Any]]:
    """At most three separately consistent snapshots for an explicitly selected test database."""
    try:
        with _open_snapshot(database) as connection:
            ids = [row[0] for row in connection.execute(
                "SELECT id FROM autonomous_goals ORDER BY updated_at DESC,id LIMIT 3"
            )]
        return [inspect_goal(database, goal_id) for goal_id in ids]
    except _Unavailable as error:
        return [_unavailable(error.reason)]
    except (OSError, sqlite3.Error, ValueError, TypeError, OverflowError):
        return [_unavailable("databaseUnavailable")]


class _Parser(argparse.ArgumentParser):
    def error(self, message: str) -> None:
        del message
        print(json.dumps(_unavailable("invalidInput"), sort_keys=True))
        raise SystemExit(2)


def main(argv: Sequence[str] | None = None) -> int:
    parser = _Parser(description="Read a bounded Goal snapshot; never recover or dispatch work")
    parser.add_argument("--database", required=True)
    parser.add_argument("--goal", required=True)
    parser.add_argument("--event-limit", type=int, default=20)
    args = parser.parse_args(argv)
    report = inspect_goal(args.database, args.goal, event_limit=args.event_limit)
    print(json.dumps(report, sort_keys=True, allow_nan=False))
    return 0 if report["status"] == "observed" else 2


if __name__ == "__main__":
    raise SystemExit(main())
