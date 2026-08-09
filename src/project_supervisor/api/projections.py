from __future__ import annotations

import base64
import json
from collections import defaultdict
from dataclasses import dataclass
from typing import Any

from project_supervisor.store import StateStore

from .schemas import PageMeta, api_timestamp

USAGE_FIELDS = (
    "inputTokens",
    "outputTokens",
    "cacheReadTokens",
    "cacheWriteTokens",
    "reasoningTokens",
    "requestCount",
    "estimatedCostUSD",
)
SEVERITY_RANK = {
    name: index
    for index, name in enumerate(("debug", "info", "notice", "warning", "error", "critical"))
}
EVENT_KINDS = {
    "taskAssigned",
    "workerSpawned",
    "modelStarted",
    "toolInvocation",
    "output",
    "phaseChanged",
    "heartbeat",
    "rateLimitWarning",
    "quotaChanged",
    "taskCompleted",
    "taskFailed",
    "retryScheduled",
    "cancellation",
    "sessionEnded",
    "nodeStateChanged",
    "supervisorNotice",
}


def unavailable(reason: str = "notReported") -> dict[str, Any]:
    return {"state": "unavailable", "reason": reason}


def known(value: int | float) -> dict[str, Any]:
    return {"state": "known", "value": value}


def _json(value: str | None, fallback: Any) -> Any:
    if value is None:
        return fallback
    try:
        return json.loads(value)
    except (TypeError, json.JSONDecodeError):
        return fallback


def _harness(value: str | None) -> str | None:
    """Map internal harness names onto the published Cyber Office vocabulary.

    ``lmStudio`` is a wire-compatibility alias for the Windows Local Worker: the
    monitor's ``WorkerHarness`` enum predates the rename. The supervisor still
    never talks to an LM Studio endpoint; see SECURITY.md.
    """

    return {"localWorker": "lmStudio", "mock": "codex"}.get(value, value)


def _provider(value: str | None) -> str | None:
    return "local" if value == "mock" else value


def _model(row: dict[str, Any] | None, provider: str | None = None) -> dict[str, Any]:
    provider = _provider((row or {}).get("model_provider") or provider) or "local"
    identifier = (row or {}).get("model_identifier") or "unknown"
    value: dict[str, Any] = {
        "identifier": identifier,
        "displayName": (row or {}).get("model_display_name") or "Unknown model",
        "provider": provider,
    }
    if (variant := (row or {}).get("context_variant")) is not None:
        value["contextVariant"] = variant
    if (window := (row or {}).get("context_window_tokens")) is not None:
        value["contextWindowTokens"] = window
    return value


def _usage(rows: list[dict[str, Any]]) -> dict[str, Any]:
    by_metric: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        by_metric[row["metric"]].append(row)
    result: dict[str, Any] = {}
    for metric in USAGE_FIELDS:
        records = by_metric.get(metric, [])
        if not records:
            result[metric] = unavailable()
            continue
        missing = next((item for item in records if item["value"] is None), None)
        if missing is not None:
            result[metric] = unavailable(missing["unavailable_reason"] or "unknown")
            continue
        total = sum(item["value"] for item in records)
        if metric != "estimatedCostUSD":
            total = int(total)
        result[metric] = known(total)
    return result


def _task_state(value: str) -> str:
    return {
        "draft": "queued",
        "ready": "queued",
        "reviewing": "running",
        "blocked": "waiting",
        "interrupted": "waiting",
        "succeeded": "completed",
    }.get(value, value)


def _assignment_state(value: str) -> str:
    return {
        "starting": "pending",
        "completed": "done",
        "timedOut": "failed",
        "interrupted": "failed",
        "authRequired": "failed",
        "rateLimited": "waiting",
    }.get(value, value)


def _priority(value: int) -> str:
    if value >= 90:
        return "critical"
    if value >= 67:
        return "high"
    if value >= 34:
        return "normal"
    return "low"


def encode_cursor(*, before: int, high: int) -> str:
    raw = json.dumps({"before": before, "high": high}, separators=(",", ":"), sort_keys=True)
    return base64.urlsafe_b64encode(raw.encode()).decode().rstrip("=")


def decode_cursor(cursor: str) -> tuple[int, int]:
    try:
        padded = cursor + "=" * (-len(cursor) % 4)
        value = json.loads(base64.urlsafe_b64decode(padded).decode())
        before, high = int(value["before"]), int(value["high"])
        if before < 1 or high < 0 or before > high + 1:
            raise ValueError
        return before, high
    except (ValueError, TypeError, KeyError, json.JSONDecodeError) as error:
        raise ValueError("invalid event cursor") from error


@dataclass(slots=True)
class EventPage:
    events: list[dict[str, Any]]
    meta: PageMeta


class SupervisorProjection:
    """Read-only database projection into the Cyber Office v1 wire contract."""

    def __init__(self, store: StateStore, *, version: str = "0.1.0a1") -> None:
        self.store = store
        self.version = version

    def _rows(self, query: str, parameters: tuple[Any, ...] = ()) -> list[dict[str, Any]]:
        with self.store.connect() as connection:
            return [dict(row) for row in connection.execute(query, parameters).fetchall()]

    def _usage_rows(self, *, column: str, identifier: str) -> list[dict[str, Any]]:
        if column not in {"task_id", "worker_id", "run_id"}:
            raise ValueError("unsupported usage dimension")
        return self._rows(f"SELECT * FROM usage_records WHERE {column} = ?", (identifier,))

    def nodes(self) -> list[dict[str, Any]]:
        nodes = self._rows("SELECT * FROM nodes ORDER BY id")
        worker_rows = self._rows("SELECT id,node_id FROM workers ORDER BY id")
        workers_by_node: dict[str, list[str]] = defaultdict(list)
        for row in worker_rows:
            workers_by_node[row["node_id"]].append(row["id"])
        run_counts = self._rows(
            "SELECT w.node_id,r.state,COUNT(*) AS count FROM worker_runs r "
            "JOIN workers w ON w.id=r.worker_id GROUP BY w.node_id,r.state"
        )
        counts: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))
        for row in run_counts:
            counts[row["node_id"]][row["state"]] += row["count"]

        result = []
        for row in nodes:
            offline = row["state"] in {"offline", "unprovisioned"}
            missing = "offline" if offline else "notReported"
            metrics = {
                name: unavailable(missing)
                for name in (
                    "cpuLoadPercent",
                    "cpuCoreCount",
                    "memoryUsedBytes",
                    "memoryTotalBytes",
                    "gpuUtilizationPercent",
                    "vramUsedBytes",
                    "vramTotalBytes",
                    "gpuTemperatureCelsius",
                    "diskFreeBytes",
                    "uptimeSeconds",
                    "loadAverage1m",
                )
            }
            value: dict[str, Any] = {
                "id": row["id"],
                "hostname": row["hostname"],
                "displayName": row["display_name"],
                # The persisted runtime role uses ``worker`` while the Cyber Office v1
                # contract names a compute node ``gpuCompute``.  Keep the storage model
                # independent and translate at the API boundary so live snapshots remain
                # decodable by the native client.
                "role": "gpuCompute" if row["role"] == "worker" else row["role"],
                "status": row["state"],
                "heartbeat": {
                    "lastSeenAt": row["last_heartbeat_at"],
                    "expectedIntervalSeconds": None,
                },
                "metrics": metrics,
                "workerIDs": workers_by_node[row["id"]],
                "runningJobCount": known(
                    sum(counts[row["id"]][state] for state in ("starting", "running", "waiting"))
                ),
                "queuedJobCount": unavailable("notReported"),
            }
            for source, target in (
                ("operating_system", "operatingSystem"),
                ("hardware_summary", "hardwareSummary"),
                ("private_endpoint", "ipAddress"),
            ):
                if row[source] is not None:
                    value[target] = row[source]
            result.append(value)
        return result

    def workers(self) -> list[dict[str, Any]]:
        rows = self._rows(
            "SELECT w.*,n.hostname,m.provider AS model_provider,m.identifier AS model_identifier,"
            "m.display_name AS model_display_name,m.context_variant,m.context_window_tokens "
            "FROM workers w JOIN nodes n ON n.id=w.node_id "
            "LEFT JOIN models m ON m.id=w.model_id ORDER BY w.id"
        )
        active_runs = self._rows(
            "SELECT r.*,t.title AS task_title FROM worker_runs r JOIN tasks t ON t.id=r.task_id "
            "WHERE r.state IN ('starting','running','waiting','rateLimited') "
            "ORDER BY COALESCE(r.started_at,r.created_at) DESC"
        )
        run_by_worker: dict[str, dict[str, Any]] = {}
        for run in active_runs:
            run_by_worker.setdefault(run["worker_id"], run)
        resource_rows = self._rows(
            "SELECT rs.* FROM resource_states rs JOIN (SELECT worker_id,MAX(observed_at) observed "
            "FROM resource_states WHERE worker_id IS NOT NULL GROUP BY worker_id) latest "
            "ON latest.worker_id=rs.worker_id AND latest.observed=rs.observed_at"
        )
        resource_by_worker = {row["worker_id"]: row for row in resource_rows}
        result = []
        for row in rows:
            run = run_by_worker.get(row["id"])
            resource = resource_by_worker.get(row["id"])
            resource_state = (resource or {}).get("state", row["resource_state"])
            quota_state = {
                "available": "healthy",
                "warning": "warning",
                "rateLimited": "rateLimited",
                "budgetExhausted": "exhausted",
                "cooldown": "warning",
                "unknown": "unknown",
            }.get(resource_state, "unknown")
            quota: dict[str, Any] = {
                "state": quota_state,
                "remainingRequests": unavailable(),
                "remainingTokens": unavailable(),
            }
            if resource and resource["detail"] is not None:
                quota["detail"] = resource["detail"]
            if resource and resource["resets_at"] is not None:
                quota["resetsAt"] = resource["resets_at"]
            value: dict[str, Any] = {
                "id": row["id"],
                "harness": _harness(row["harness"]),
                "provider": _provider(row["provider"]),
                "model": _model(row, row["provider"]),
                "nodeID": row["node_id"],
                "hostname": row["hostname"],
                "state": row["state"],
                "heartbeat": {
                    "lastSeenAt": row["last_heartbeat_at"],
                    "expectedIntervalSeconds": None,
                },
                "quota": quota,
                "usage": _usage(self._usage_rows(column="worker_id", identifier=row["id"])),
            }
            if run:
                value.update(
                    {
                        "currentTaskID": run["task_id"],
                        "currentTaskTitle": run["task_title"],
                        "startedAt": run["started_at"],
                        "sessionID": run["session_id"],
                        "processID": run["process_id"],
                        "lastActivity": run["failure_detail"] or run["state"],
                    }
                )
            if row["harness_version"] is not None:
                value["harnessVersion"] = row["harness_version"]
            result.append(value)
        return result

    def tasks(self, *, state: str | None = None, detail: bool = False) -> list[dict[str, Any]]:
        rows = self.store.list_tasks()
        values = [self._task(row, detail=detail) for row in rows]
        if state is not None:
            values = [item for item in values if item["state"] == state]
        return values

    def task(self, task_id: str) -> dict[str, Any]:
        return self._task(self.store.get_task(task_id), detail=True)

    def _task(self, row: dict[str, Any], *, detail: bool) -> dict[str, Any]:
        run_rows = self._rows(
            "SELECT r.*,w.harness,w.provider,w.model_id,n.hostname,"
            "m.provider AS model_provider,m.identifier AS model_identifier,"
            "m.display_name AS model_display_name,m.context_variant,m.context_window_tokens "
            "FROM worker_runs r JOIN workers w ON w.id=r.worker_id "
            "JOIN nodes n ON n.id=w.node_id "
            "LEFT JOIN models m ON m.id=w.model_id "
            "WHERE r.task_id=? ORDER BY r.attempt DESC,r.created_at DESC",
            (row["id"],),
        )
        # The model join above cannot use session ids; fill missing model details from workers.
        if run_rows:
            model_rows = self._rows(
                "SELECT w.id,m.provider AS model_provider,m.identifier AS model_identifier,"
                "m.display_name AS model_display_name,m.context_variant,m.context_window_tokens "
                "FROM workers w LEFT JOIN models m ON m.id=w.model_id"
            )
            model_by_worker = {item["id"]: item for item in model_rows}
            for run in run_rows:
                if not run.get("model_identifier"):
                    run.update(model_by_worker.get(run["worker_id"], {}))
        latest_by_worker: dict[str, dict[str, Any]] = {}
        for run in run_rows:
            latest_by_worker.setdefault(run["worker_id"], run)
        assignments = []
        for run in latest_by_worker.values():
            assignment: dict[str, Any] = {
                "id": run["worker_id"],
                "harness": _harness(run["harness"]),
                "provider": _provider(run["provider"]),
                "model": _model(run, run["provider"]),
                "hostname": run["hostname"],
                "state": _assignment_state(run["state"]),
                "startedAt": run["started_at"],
                "finishedAt": run["ended_at"],
                "sessionID": run["session_id"],
                "usage": _usage(self._usage_rows(column="run_id", identifier=run["id"])),
                "latestDetail": run["failure_detail"] or run["state"],
            }
            assignments.append(assignment)
        latest = self._rows(
            "SELECT summary,created_at FROM events WHERE task_id=? ORDER BY sequence DESC LIMIT 1",
            (row["id"],),
        )
        node_id = None
        hostname = None
        if latest_by_worker:
            first = next(iter(latest_by_worker.values()))
            node_rows = self._rows("SELECT node_id FROM workers WHERE id=?", (first["worker_id"],))
            if node_rows:
                node_id = node_rows[0]["node_id"]
                hostname = first["hostname"]
        value: dict[str, Any] = {
            "id": row["id"],
            "reference": row["reference"],
            "title": row["title"],
            "summary": row["description"] or None,
            "state": _task_state(row["state"]),
            "priority": _priority(row["priority"]),
            "originatingSupervisor": "Project_Supervisor",
            "nodeID": node_id,
            "hostname": hostname,
            "createdAt": row["created_at"],
            "startedAt": row["started_at"],
            "updatedAt": row["updated_at"],
            "finishedAt": row["finished_at"],
            "assignments": assignments,
            "sessions": self._sessions(row["id"]) if detail else [],
            "phases": [],
            "currentPhase": None,
            "progressFraction": unavailable("notReported"),
            "latestEventSummary": latest[0]["summary"] if latest else None,
            "latestEventAt": latest[0]["created_at"] if latest else None,
            "usage": _usage(self._usage_rows(column="task_id", identifier=row["id"])),
            "failureReason": row["failure_reason"],
            "labels": _json(row["labels_json"], []),
        }
        return value

    def _sessions(self, task_id: str) -> list[dict[str, Any]]:
        rows = self._rows(
            "SELECT s.*,r.id AS run_id,r.task_id,r.started_at AS run_started_at,"
            "r.ended_at,r.failure_detail,"
            "w.harness,w.provider,m.provider AS model_provider,m.identifier AS model_identifier,"
            "m.display_name AS model_display_name,m.context_variant,m.context_window_tokens "
            "FROM worker_runs r JOIN sessions s ON s.id=r.session_id "
            "JOIN workers w ON w.id=s.worker_id "
            "LEFT JOIN models m ON m.id=COALESCE(s.model_id,w.model_id) "
            "WHERE r.task_id=? ORDER BY s.created_at",
            (task_id,),
        )
        values = []
        for row in rows:
            values.append(
                {
                    "id": row["id"],
                    "workerID": row["worker_id"],
                    "harness": _harness(row["harness"]),
                    "provider": _provider(row["provider"]),
                    "model": _model(row, row["provider"]),
                    "taskID": task_id,
                    "conversationID": row["provider_session_id"],
                    "startedAt": row["run_started_at"] or row["created_at"],
                    "endedAt": row["ended_at"],
                    "turnCount": unavailable(),
                    "toolCallCount": unavailable(),
                    "usage": _usage(self._usage_rows(column="run_id", identifier=row["run_id"])),
                    "endReason": row["failure_detail"],
                }
            )
        return values

    def status(self) -> dict[str, Any]:
        nodes = self.nodes()
        workers = self.workers()
        tasks = self.tasks()
        services: list[dict[str, Any]] = [
            {
                "id": "svc-project-supervisor",
                "name": "Project_Supervisor",
                "state": "online",
                "detail": f"api v1 · {len(workers)} workers · {len(nodes)} nodes",
                "heartbeat": {"lastSeenAt": api_timestamp(), "expectedIntervalSeconds": None},
            }
        ]
        for node in nodes:
            services.append(
                {
                    "id": f"svc-{node['id']}",
                    "name": node["displayName"],
                    "state": "unknown" if node["status"] == "unprovisioned" else node["status"],
                    "detail": f"{node['hostname']} · {len(node['workerIDs'])} workers",
                    "nodeID": node["id"],
                    "heartbeat": node["heartbeat"],
                }
            )
        return {
            "version": self.version,
            "apiVersion": "v1",
            "services": services,
            "uptimeSeconds": unavailable("notReported"),
            "queuedTaskCount": known(sum(task["state"] == "queued" for task in tasks)),
            "runningTaskCount": known(
                sum(task["state"] in {"running", "waiting"} for task in tasks)
            ),
        }

    def snapshot(self, *, recent_event_limit: int = 200) -> dict[str, Any]:
        return {
            "generatedAt": api_timestamp(),
            "supervisor": self.status(),
            "nodes": self.nodes(),
            "workers": self.workers(),
            "tasks": self.tasks(),
            "recentEvents": self.events(limit=recent_event_limit).events,
        }

    def events(
        self,
        *,
        limit: int = 100,
        cursor: str | None = None,
        after_sequence: int = 0,
        nodes: tuple[str, ...] = (),
        harnesses: tuple[str, ...] = (),
        tasks: tuple[str, ...] = (),
        kinds: tuple[str, ...] = (),
        minimum_severity: str = "debug",
        search: str | None = None,
        ascending: bool = False,
    ) -> EventPage:
        current_high = self.store.highest_event_sequence()
        if cursor:
            before, high = decode_cursor(cursor)
        else:
            high = current_high
            before = high + 1
        where = ["e.sequence > ?", "e.sequence <= ?", "e.sequence < ?"]
        parameters: list[Any] = [after_sequence, high, before]

        def add_in(expression: str, values: tuple[str, ...]) -> None:
            if values:
                where.append(f"{expression} IN ({','.join('?' for _ in values)})")
                parameters.extend(values)

        add_in("n.id", nodes)
        add_in("w.harness", tuple({"lmStudio": "localWorker"}.get(x, x) for x in harnesses))
        add_in("e.task_id", tasks)
        if kinds:
            # Filtering is defined on the wire kind, so normalize in Python below.
            pass
        if minimum_severity not in SEVERITY_RANK:
            raise ValueError("invalid minSeverity")
        allowed_severity = tuple(
            name for name, rank in SEVERITY_RANK.items() if rank >= SEVERITY_RANK[minimum_severity]
        )
        add_in("e.severity", allowed_severity)
        if search:
            where.append("(LOWER(e.summary) LIKE ? OR LOWER(e.payload_json) LIKE ?)")
            needle = f"%{search.lower()}%"
            parameters.extend((needle, needle))
        order = "ASC" if ascending else "DESC"
        # Kind normalization can discard rows, so read all matching rows before slicing.
        query = (
            "SELECT e.*,t.reference AS task_reference,n.id AS node_id,n.hostname,"
            "w.harness,w.provider,"
            "r.session_id,m.provider AS model_provider,m.identifier AS model_identifier,"
            "m.display_name AS model_display_name,m.context_variant,m.context_window_tokens "
            "FROM events e LEFT JOIN tasks t ON t.id=e.task_id "
            "LEFT JOIN workers w ON w.id=COALESCE(e.worker_id,"
            "CASE WHEN e.entity_type='worker' THEN e.entity_id END) "
            "LEFT JOIN nodes n ON n.id=COALESCE(w.node_id,"
            "CASE WHEN e.entity_type='node' THEN e.entity_id END) "
            "LEFT JOIN worker_runs r ON r.id=e.run_id LEFT JOIN models m ON m.id=w.model_id "
            f"WHERE {' AND '.join(where)} ORDER BY e.sequence {order}"
        )
        rows = self._rows(query, tuple(parameters))
        values = [self._event(row) for row in rows]
        if kinds:
            values = [value for value in values if value["kind"] in kinds]
        has_more = len(values) > limit
        page = values[:limit]
        next_cursor = None
        if has_more and page:
            boundary = page[-1]["sequence"] if not ascending else page[0]["sequence"]
            next_cursor = encode_cursor(before=boundary, high=high)
        meta = PageMeta(
            nextCursor=next_cursor,
            hasMore=has_more,
            highestSequence=current_high,
            totalCount=len(values),
        )
        return EventPage(page, meta)

    def _event(self, row: dict[str, Any]) -> dict[str, Any]:
        raw_kind = row["kind"]
        payload = _json(row["payload_json"], {})
        kind = (
            raw_kind
            if raw_kind in EVENT_KINDS
            else {
                "routingDecisionRecorded": "taskAssigned",
                "workerRunInterrupted": "taskFailed",
                "taskRecovered": "taskFailed",
            }.get(raw_kind, "supervisorNotice")
        )
        if raw_kind == "taskStateChanged":
            target = payload.get("to")
            kind = {
                "succeeded": "taskCompleted",
                "failed": "taskFailed",
                "cancelled": "cancellation",
            }.get(target, "phaseChanged")
        event_payload: dict[str, Any]
        if (
            isinstance(payload, dict)
            and payload.get("harness") in {"claude", "grok", "agy", "generic"}
            and isinstance(payload.get("detail"), dict)
        ):
            event_payload = payload
        else:
            fields = (
                {
                    str(key): str(value)
                    for key, value in payload.items()
                    if isinstance(value, (str, int, float, bool))
                }
                if isinstance(payload, dict)
                else {}
            )
            event_payload = {
                "harness": "generic",
                "detail": {"source": row["actor"], "text": row["summary"], "fields": fields},
            }
        value: dict[str, Any] = {
            "id": row["event_id"] or f"evt-{row['sequence']}",
            "sequence": row["sequence"],
            "timestamp": row["created_at"],
            "kind": kind,
            "severity": row["severity"],
            "message": row["summary"],
            "nodeID": row["node_id"],
            "hostname": row["hostname"],
            "workerID": row["worker_id"],
            "harness": _harness(row["harness"]),
            "provider": _provider(row["provider"]),
            "model": _model(row, row["provider"]) if row["worker_id"] else None,
            "taskID": row["task_id"],
            "taskReference": row["task_reference"],
            "sessionID": row["session_id"],
            "payload": event_payload,
        }
        return value
