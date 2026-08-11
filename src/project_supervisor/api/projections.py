from __future__ import annotations

import base64
import hashlib
import json
from collections import defaultdict
from dataclasses import dataclass
from pathlib import PurePosixPath, PureWindowsPath
from typing import Any, Protocol

from project_supervisor.store import StateStore, redact_sensitive

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
    "goalCreated",
    "goalStarted",
    "goalIterationStarted",
    "goalEvaluated",
    "goalPlanCreated",
    "goalActionStateChanged",
    "goalActionCancellationRequested",
    "goalVerified",
    "goalReplanRequired",
    "goalPaused",
    "goalResumed",
    "goalSteered",
    "goalStopped",
    "goalTerminated",
    "taskDependencyAdded",
    "taskExecutionLeaseAcquired",
    "taskExecutionLeaseReleased",
    "workerResultRecorded",
    "verificationCompleted",
    "dispatchDeferred",
    "providerJobPrepared",
    "providerJobLaunchStarted",
    "providerJobHandleBound",
    "providerJobReconciled",
    "providerJobResultCollected",
    "taskVerificationScopeBound",
    "humanEscalationRequested",
    "humanEscalationResolved",
}


class GoalReader(Protocol):
    def list_goals(self, *, project_id: str | None = None) -> list[dict[str, Any]]: ...

    def get_goal(self, goal_id: str) -> dict[str, Any]: ...


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


def _safe_semantic_id(value: Any, *, maximum: int = 200) -> str | None:
    if not isinstance(value, str) or not 1 <= len(value) <= maximum or not value.isascii():
        return None
    if not all(character.isalnum() or character in "._:-" for character in value):
        return None
    return value


def _provider_runtime_profile(value: Any) -> dict[str, Any] | None:
    """Allowlist non-secret Local Worker execution identities from private handle metadata."""

    if not isinstance(value, dict):
        return None
    driver_id = _safe_semantic_id(value.get("driverID"), maximum=128)
    driver_type = _safe_semantic_id(value.get("driverType"), maximum=64)
    revision = value.get("driverProfileRevision")
    fingerprint = value.get("driverProfileFingerprint")
    if (
        driver_id is None
        or driver_type is None
        or not isinstance(revision, int)
        or isinstance(revision, bool)
        or revision < 1
        or not isinstance(fingerprint, str)
        or len(fingerprint) != 71
        or not fingerprint.startswith("sha256:")
        or any(character not in "0123456789abcdef" for character in fingerprint[7:])
    ):
        return None
    result: dict[str, Any] = {
        "driver": {
            "id": driver_id,
            "type": driver_type,
            "profileRevision": revision,
            "profileFingerprint": fingerprint,
        }
    }
    if node_id := _safe_semantic_id(value.get("nodeID")):
        result["nodeID"] = node_id
    if runtime_id := _safe_semantic_id(value.get("launchRuntimeInstanceID")):
        result["launchRuntimeInstanceID"] = runtime_id
    return result


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
        # Runtime persistence uses ``costUSD`` while the established observation
        # contract names the same metric ``estimatedCostUSD``. Select the preferred
        # alias independently for each run: this preserves legacy costs from one run
        # when another uses the canonical name, without counting both aliases for a
        # single run. Records without a run remain independent because there is no
        # durable key proving that two observations describe the same execution.
        if metric == "estimatedCostUSD":
            records = []
            by_run: dict[str, list[dict[str, Any]]] = defaultdict(list)
            unscoped: list[dict[str, Any]] = []
            for item in (*by_metric["estimatedCostUSD"], *by_metric["costUSD"]):
                if item.get("run_id") is None:
                    unscoped.append(item)
                else:
                    by_run[item["run_id"]].append(item)
            records.extend(unscoped)
            for run_records in by_run.values():
                canonical = [item for item in run_records if item["metric"] == "estimatedCostUSD"]
                records.extend(canonical or run_records)
        else:
            records = by_metric[metric]
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


def _evidence_reference(value: str | None) -> str | None:
    """Return only non-host-local evidence references safe for remote observers."""

    if value is None:
        return None
    reference = value.strip()
    if not reference:
        return None
    if reference.lower().startswith("file:"):
        return None
    if PurePosixPath(reference).is_absolute() or PureWindowsPath(reference).is_absolute():
        return None
    return redact_sensitive(reference)


def _public_external_identifier(value: str | None) -> str | None:
    """Project only a bounded non-endpoint external identifier.

    Provider handles are contractually non-secret, but a defensive projection must still avoid
    turning an accidentally persisted URL, host path, control sequence, or recognizable secret
    into remote-observer data.
    """

    if value is None:
        return None
    candidate = str(redact_sensitive(value)).strip()
    if not candidate or len(candidate) > 512 or any(ord(character) < 32 for character in candidate):
        return None
    lowered = candidate.lower()
    if "://" in candidate or lowered.startswith("file:"):
        return None
    if PurePosixPath(candidate).is_absolute() or PureWindowsPath(candidate).is_absolute():
        return None
    return candidate


def _sha256_prefix(value: str | None) -> str | None:
    if value is None:
        return None
    return hashlib.sha256(value.encode("utf-8")).hexdigest()[:16]


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

    def __init__(
        self,
        store: StateStore,
        *,
        version: str = "0.2.1",
        goal_reader: GoalReader | None = None,
    ) -> None:
        self.store = store
        self.version = version
        self.goal_reader = goal_reader

    def goals(self, *, project_id: str | None = None) -> list[dict[str, Any]]:
        if self.goal_reader is None:
            return []
        return [self._goal(row) for row in self.goal_reader.list_goals(project_id=project_id)]

    def goal(self, goal_id: str) -> dict[str, Any]:
        if self.goal_reader is None:
            raise KeyError(goal_id)
        return self._goal(self.goal_reader.get_goal(goal_id))

    def autonomy_hosts(self, *, host_id: str | None = None) -> list[dict[str, Any]]:
        from project_supervisor.autonomous_host import AutonomousHostRepository

        repository = AutonomousHostRepository(self.store)
        rows = [repository.get_host(host_id)] if host_id else repository.list_hosts()
        return [self._autonomy_host(row) for row in rows]

    def autonomy_goal_leases(
        self,
        *,
        host_id: str | None = None,
        goal_id: str | None = None,
        owned_only: bool = False,
    ) -> list[dict[str, Any]]:
        from project_supervisor.autonomous_host import AutonomousHostRepository

        repository = AutonomousHostRepository(self.store)
        if goal_id is not None:
            row = repository.get_goal_lease(goal_id)
            rows = [row] if row is not None else []
            if host_id is not None:
                rows = [item for item in rows if item["host_id"] == host_id]
            if owned_only:
                rows = [item for item in rows if item["state"] == "owned"]
        else:
            rows = repository.list_goal_leases(host_id=host_id, owned_only=owned_only)
        return [self._autonomy_goal_lease(row) for row in rows]

    @staticmethod
    def _autonomy_host(row: dict[str, Any]) -> dict[str, Any]:
        return {
            "hostID": row["host_id"],
            "processID": int(row["process_id"]),
            "state": row["state"],
            "startedAt": row["started_at"],
            "heartbeatAt": row["heartbeat_at"],
            "stoppedAt": row["stopped_at"],
            "activeGoalCount": int(row["active_goal_count"]),
            "lastError": row["last_error"],
            "metadata": row.get("metadata", _json(row.get("metadata_json"), {})),
        }

    @staticmethod
    def _autonomy_goal_lease(row: dict[str, Any]) -> dict[str, Any]:
        return {
            "goalID": row["goal_id"],
            "hostID": row["host_id"],
            "generation": int(row["generation"]),
            "state": row["state"],
            "acquiredAt": row["acquired_at"],
            "heartbeatAt": row["heartbeat_at"],
            "expiresAt": row["expires_at"],
            "releasedAt": row["released_at"],
            "currentIterationID": row["current_iteration_id"],
            "currentActionID": row["current_action_id"],
            "inFlightState": row["in_flight_state"],
            "recoveryState": row["recovery_state"],
            "previousHostID": row["previous_host_id"],
            "lastError": row["last_error"],
        }

    def resource_snapshots(self, **filters: Any) -> list[dict[str, Any]]:
        from project_supervisor.resource_usage import ResourceUsageRepository

        limit = int(filters.pop("limit", 1000))
        repository = ResourceUsageRepository(self.store)
        return [
            snapshot.to_protocol() for snapshot in repository.list_snapshots(**filters, limit=limit)
        ]

    def resource_aggregate(self, **filters: Any) -> dict[str, Any]:
        from project_supervisor.resource_usage import ResourceUsageRepository

        return ResourceUsageRepository(self.store).aggregate(**filters).to_protocol()

    def resource_economics(
        self, *, project_id: str | None = None, goal_id: str | None = None
    ) -> dict[str, Any]:
        from project_supervisor.resource_economics import ResourceEconomicsRepository

        return (
            ResourceEconomicsRepository(self.store)
            .aggregate(project_id=project_id, goal_id=goal_id)
            .to_protocol()
        )

    def node_recovery(self, *, node_id: str | None = None) -> list[dict[str, Any]]:
        from project_supervisor.node_recovery import NodeRuntimeRecoveryService

        return NodeRuntimeRecoveryService(self.store).list_status(node_id=node_id)

    def _goal(self, row: dict[str, Any]) -> dict[str, Any]:
        budgets = row.get("budgets")
        if budgets is None:
            budgets = _json(row.get("budgets_json"), {})
        budget_fields = {
            "maxIterations": "max_iterations",
            "maxTasks": "max_tasks",
            "maxFailures": "max_failures",
            "noProgressLimit": "no_progress_limit",
            "maxElapsedSeconds": "max_elapsed_seconds",
            "maxTotalTokens": "max_total_tokens",
            "maxCostUSD": "max_cost_usd",
        }
        budgets = {
            wire: budgets.get(wire, budgets.get(storage)) for wire, storage in budget_fields.items()
        }
        current_iteration, generated_tasks, latest_verifier = self._goal_execution(row["id"])
        return {
            "id": row["id"],
            "projectID": row["project_id"],
            "intent": row["intent"],
            "effectiveIntent": row.get("effective_intent", row["intent"]),
            "state": row["state"],
            "pauseMode": row.get("pause_mode"),
            "terminationReason": row.get("termination_reason"),
            "terminationDetail": row.get("termination_detail"),
            "iterationCount": int(row.get("iteration_count", 0)),
            "noProgressCount": int(row.get("no_progress_count", 0)),
            "taskCount": int(row.get("task_count", 0)),
            "failureCount": int(row.get("failure_count", 0)),
            "budgets": budgets,
            "steerVersion": int(row.get("steer_version", 0)),
            "version": int(row.get("version", 0)),
            "createdAt": row["created_at"],
            "updatedAt": row["updated_at"],
            "startedAt": row.get("started_at"),
            "lastEvaluatedAt": row.get("last_evaluated_at"),
            "finishedAt": row.get("finished_at"),
            "eventCursor": int(row.get("event_cursor", self.store.highest_event_sequence())),
            "currentPlan": current_iteration,
            "generatedTasks": generated_tasks,
            "latestVerifier": latest_verifier,
        }

    def _goal_execution(
        self, goal_id: str
    ) -> tuple[dict[str, Any] | None, list[dict[str, Any]], dict[str, Any] | None]:
        iterations = self._rows(
            "SELECT * FROM autonomous_iterations WHERE goal_id=? ORDER BY sequence DESC",
            (goal_id,),
        )
        current: dict[str, Any] | None = None
        if iterations:
            row = iterations[0]
            plan = _json(row.get("plan_json"), {})
            current = {
                "iterationID": row["id"],
                "sequence": int(row["sequence"]),
                "state": row["state"],
                "summary": plan.get("summary") if isinstance(plan, dict) else None,
                "rationale": plan.get("rationale") if isinstance(plan, dict) else None,
            }
        actions = self._rows(
            "SELECT id,iteration_id,ordinal,action_key,title,description,role,state,task_id,error "
            "FROM autonomous_actions WHERE goal_id=? ORDER BY created_at,ordinal",
            (goal_id,),
        )
        generated = [
            {
                "actionID": item["id"],
                "iterationID": item["iteration_id"],
                "ordinal": int(item["ordinal"]),
                "key": item["action_key"],
                "title": item["title"],
                "description": item["description"],
                "role": item["role"],
                "state": item["state"],
                "taskID": item["task_id"],
                "error": item["error"],
            }
            for item in actions
        ]
        latest_verifier: dict[str, Any] | None = None
        for iteration in iterations:
            verification = _json(iteration.get("verification_json"), None)
            if isinstance(verification, dict):
                latest_verifier = {
                    "iterationID": iteration["id"],
                    "sequence": int(iteration["sequence"]),
                    "satisfied": bool(verification.get("satisfied", False)),
                    "summary": verification.get("summary"),
                    "progressFingerprint": verification.get("progressFingerprint"),
                    "terminationReason": verification.get("terminationReason"),
                    "evidence": verification.get("evidence", {}),
                }
                break
        return current, generated, latest_verifier

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

    def runs(
        self,
        *,
        task_id: str | None = None,
        worker_id: str | None = None,
        state: str | None = None,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        where: list[str] = []
        parameters: list[Any] = []
        for column, value in (
            ("r.task_id", task_id),
            ("r.worker_id", worker_id),
            ("r.state", state),
        ):
            if value is not None:
                where.append(f"{column}=?")
                parameters.append(value)
        rows = self._rows(
            self._run_query(where) + " ORDER BY COALESCE(r.started_at,r.created_at) DESC,"
            "r.created_at DESC,r.id ASC LIMIT ?",
            (*parameters, limit),
        )
        return self._runs(rows)

    def run(self, run_id: str) -> dict[str, Any]:
        rows = self._rows(self._run_query(["r.id=?"]), (run_id,))
        if not rows:
            raise KeyError(run_id)
        return self._runs(rows)[0]

    @staticmethod
    def _run_query(where: list[str]) -> str:
        query = (
            "SELECT r.*,t.project_id,t.reference AS task_reference,t.title AS task_title,"
            "w.node_id,w.harness,w.provider,n.hostname,"
            "s.provider_session_id,s.state AS session_state,"
            "m.provider AS model_provider,m.identifier AS model_identifier,"
            "m.display_name AS model_display_name,m.context_variant,m.context_window_tokens,"
            "wr.summary AS result_summary,wr.changed_files_json AS result_changed_files_json,"
            "wr.commands_run_json AS result_commands_run_json,wr.tests_json AS result_tests_json,"
            "wr.artifacts_json AS result_artifacts_json,wr.commit_hash AS result_commit_hash,"
            "wr.blockers_json AS result_blockers_json,wr.confidence AS result_confidence,"
            "wr.recommended_next_actions_json AS result_recommended_next_actions_json,"
            "wr.created_at AS result_created_at "
            "FROM worker_runs r JOIN tasks t ON t.id=r.task_id "
            "JOIN workers w ON w.id=r.worker_id JOIN nodes n ON n.id=w.node_id "
            "LEFT JOIN sessions s ON s.id=r.session_id "
            "LEFT JOIN models m ON m.id=COALESCE(s.model_id,w.model_id) "
            "LEFT JOIN worker_results wr ON wr.run_id=r.id"
        )
        if where:
            query += f" WHERE {' AND '.join(where)}"
        return query

    def _runs(self, rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        if not rows:
            return []
        run_ids = tuple(row["id"] for row in rows)
        placeholders = ",".join("?" for _ in run_ids)
        usage_rows = self._rows(
            f"SELECT * FROM usage_records WHERE run_id IN ({placeholders}) ORDER BY recorded_at,id",
            run_ids,
        )
        usage_by_run: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for usage_row in usage_rows:
            usage_by_run[usage_row["run_id"]].append(usage_row)

        provider_jobs = self._rows(
            f"SELECT * FROM provider_jobs WHERE run_id IN ({placeholders}) ORDER BY created_at,id",
            run_ids,
        )
        job_by_run = {job["run_id"]: job for job in provider_jobs}
        escalation_rows = self._rows(
            f"SELECT * FROM execution_escalations WHERE state='open' "
            f"AND run_id IN ({placeholders}) ORDER BY created_at,id",
            run_ids,
        )
        escalations_by_run: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for escalation in escalation_rows:
            escalations_by_run[escalation["run_id"]].append(escalation)
        return [
            self._run(
                row,
                usage_by_run[row["id"]],
                provider_job=job_by_run.get(row["id"]),
                escalations=escalations_by_run[row["id"]],
            )
            for row in rows
        ]

    @staticmethod
    def _run(
        row: dict[str, Any],
        usage_rows: list[dict[str, Any]],
        *,
        provider_job: dict[str, Any] | None,
        escalations: list[dict[str, Any]],
    ) -> dict[str, Any]:
        result = None
        if row["result_summary"] is not None:
            result = redact_sensitive(
                {
                    "summary": row["result_summary"],
                    "changedFiles": _json(row["result_changed_files_json"], []),
                    "commandsRun": _json(row["result_commands_run_json"], []),
                    "tests": _json(row["result_tests_json"], []),
                    "artifacts": _json(row["result_artifacts_json"], []),
                    "commitHash": row["result_commit_hash"],
                    "blockers": _json(row["result_blockers_json"], []),
                    "confidence": row["result_confidence"],
                    "recommendedNextActions": _json(
                        row["result_recommended_next_actions_json"], []
                    ),
                    "createdAt": row["result_created_at"],
                }
            )
        failure = None
        if row["failure_class"] is not None or row["failure_detail"] is not None:
            failure = redact_sensitive(
                {"class": row["failure_class"], "detail": row["failure_detail"]}
            )
        model = _model(row, row["provider"]) if row["model_identifier"] is not None else None
        return {
            "id": row["id"],
            "projectID": row["project_id"],
            "taskID": row["task_id"],
            "taskReference": row["task_reference"],
            "taskTitle": row["task_title"],
            "workerID": row["worker_id"],
            "nodeID": row["node_id"],
            "hostname": row["hostname"],
            "provider": _provider(row["provider"]),
            "harness": _harness(row["harness"]),
            "model": model,
            "sessionID": row["session_id"],
            "providerSessionID": redact_sensitive(row["provider_session_id"]),
            "sessionState": row["session_state"],
            "state": row["state"],
            "attempt": int(row["attempt"]),
            "processID": row["process_id"],
            "createdAt": row["created_at"],
            "startedAt": row["started_at"],
            "lastEventAt": row["last_event_at"],
            "timeoutAt": row["timeout_at"],
            "endedAt": row["ended_at"],
            "updatedAt": row["updated_at"],
            "exitCode": row["exit_code"],
            "failure": failure,
            "usage": _usage(usage_rows),
            "result": result,
            "evidenceReference": _evidence_reference(row["raw_output_reference"]),
            "providerJob": (
                SupervisorProjection._provider_job(provider_job, escalations)
                if provider_job is not None
                else None
            ),
        }

    @staticmethod
    def _provider_job(row: dict[str, Any], escalations: list[dict[str, Any]]) -> dict[str, Any]:
        """Return the explicit public subset of a private durable job handle."""

        runtime_profile = _provider_runtime_profile(_json(row.get("adapter_metadata_json"), {}))
        result = {
            "id": row["id"],
            "adapterType": row["adapter_type"],
            "protocolVersion": int(row.get("protocol_version") or 1),
            "provider": _provider(row["provider"]),
            "externalID": _public_external_identifier(row.get("provider_job_id")),
            "launchGeneration": int(row["launch_generation"]),
            "state": row["launch_state"],
            "reconciliation": {
                "state": row["reconciliation_state"],
                "lastReconciledAt": row["last_reconciled_at"],
            },
            "capabilities": {
                "supportsReconcile": bool(row["supports_reconcile"]),
                "supportsResume": bool(row["supports_resume"]),
                "supportsCancel": bool(row["supports_cancel"]),
                "supportsDurableCancel": bool(row["supports_durable_cancel"]),
                "supportsProviderIdempotency": bool(row["supports_provider_idempotency"]),
                "supportsStreamReconnect": bool(row["supports_stream_reconnect"]),
                "supportsRepeatableCollect": bool(row["supports_repeatable_collect"]),
                "supportsIdempotentLaunchLookup": bool(
                    row.get("supports_idempotent_launch_lookup", 0)
                ),
                "supportsDurableLaunchRegistry": bool(
                    row.get("supports_durable_launch_registry", 0)
                ),
            },
            "resultCollection": {
                "state": row["result_collection_state"],
                "collectedAt": row["result_collected_at"],
            },
            "idempotencyKeyFingerprint": _sha256_prefix(row.get("idempotency_key")),
            "createdAt": row["created_at"],
            "launchedAt": row["launched_at"],
            "updatedAt": row["updated_at"],
            "openEscalations": [
                SupervisorProjection._execution_escalation(escalation) for escalation in escalations
            ],
        }
        if runtime_profile is not None:
            result["runtime"] = runtime_profile
        return result

    def execution_escalations(
        self,
        *,
        run_id: str | None = None,
        task_id: str | None = None,
        state: str | None = None,
        code: str | None = None,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        clauses: list[str] = []
        parameters: list[Any] = []
        for column, value in (
            ("run_id", run_id),
            ("task_id", task_id),
            ("state", state),
            ("code", code),
        ):
            if value is not None:
                clauses.append(f"{column}=?")
                parameters.append(value)
        query = "SELECT * FROM execution_escalations"
        if clauses:
            query += " WHERE " + " AND ".join(clauses)
        query += " ORDER BY created_at DESC,id ASC LIMIT ?"
        rows = self._rows(query, (*parameters, limit))
        return [self._execution_escalation(row) for row in rows]

    def execution_escalation(self, escalation_id: str) -> dict[str, Any]:
        rows = self._rows("SELECT * FROM execution_escalations WHERE id=?", (escalation_id,))
        if not rows:
            raise KeyError(escalation_id)
        return self._execution_escalation(rows[0])

    @staticmethod
    def _execution_escalation(row: dict[str, Any]) -> dict[str, Any]:
        # Detail and actor fields are intentionally private: provider failures may include raw
        # response bodies, endpoint data, or machine-local context even after best-effort storage
        # redaction. The typed public state is sufficient for observation and routing.
        return {
            "id": row["id"],
            "projectID": row["project_id"],
            "goalID": row["goal_id"],
            "taskID": row["task_id"],
            "runID": row["run_id"],
            "providerJobID": row["provider_job_id"],
            "code": row["code"],
            "state": row["state"],
            "summary": str(redact_sensitive(row["summary"])),
            "createdAt": row["created_at"],
            "updatedAt": row["updated_at"],
            "resolvedAt": row["resolved_at"],
        }

    def tasks(self, *, state: str | None = None, detail: bool = False) -> list[dict[str, Any]]:
        rows = self.store.list_tasks()
        values = [self._task(row, detail=detail) for row in rows]
        if state is not None:
            values = [item for item in values if item["state"] == state]
        return values

    def task(self, task_id: str) -> dict[str, Any]:
        return self._task(self.store.get_task(task_id), detail=True)

    def _task(self, row: dict[str, Any], *, detail: bool) -> dict[str, Any]:
        dependencies = self.store.task_dependencies(row["id"])
        verification_scope = self._task_verification_scope(row)
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
            "definitionRevision": int(row["definition_revision"]),
            "verificationScope": verification_scope,
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
            "dependencies": [
                {"taskID": dependency["task_id"], "state": _task_state(dependency["state"])}
                for dependency in dependencies
            ],
            "blockedByDependencies": [
                {"taskID": dependency["task_id"], "state": _task_state(dependency["state"])}
                for dependency in dependencies
                if dependency["state"] != "succeeded"
            ],
        }
        return value

    def _task_verification_scope(self, task: dict[str, Any]) -> dict[str, Any] | None:
        """Project only non-secret provenance for the Task's current criteria snapshot."""

        scope_id = task.get("current_verification_scope_id")
        if scope_id is None:
            return None
        rows = self._rows(
            "SELECT scope.id,scope.criteria_version,scope.task_definition_revision,"
            "scope.goal_id,scope.iteration_id,scope.plan_version,scope.steer_version,"
            "scope.schema_version,scope.definition_sha256,scope.created_at,"
            "(SELECT COUNT(*) FROM task_verification_scope_items item "
            "WHERE item.scope_id=scope.id) AS criterion_count "
            "FROM task_verification_scopes scope WHERE scope.id=? AND scope.task_id=?",
            (scope_id, task["id"]),
        )
        if not rows:
            return {
                "id": scope_id,
                "state": "unavailable",
                "reason": "scopeNotFound",
            }
        scope = rows[0]
        return {
            "id": scope["id"],
            "criteriaVersion": int(scope["criteria_version"]),
            "taskDefinitionRevision": int(scope["task_definition_revision"]),
            "goalID": scope["goal_id"],
            "iterationID": scope["iteration_id"],
            "planVersion": scope["plan_version"],
            "steerVersion": scope["steer_version"],
            "schemaVersion": scope["schema_version"],
            "definitionSHA256": scope["definition_sha256"],
            "criterionCount": int(scope["criterion_count"]),
            "createdAt": scope["created_at"],
        }

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
                    "conversationID": redact_sensitive(row["provider_session_id"]),
                    "startedAt": row["run_started_at"] or row["created_at"],
                    "endedAt": row["ended_at"],
                    "turnCount": unavailable(),
                    "toolCallCount": unavailable(),
                    "usage": _usage(self._usage_rows(column="run_id", identifier=row["run_id"])),
                    "endReason": redact_sensitive(row["failure_detail"]),
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
            "goals": self.goals(),
            "autonomousHosts": self.autonomy_hosts(),
            "autonomousGoalLeases": self.autonomy_goal_leases(),
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
        goals: tuple[str, ...] = (),
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
        add_in(
            "COALESCE(CASE WHEN e.entity_type='goal' THEN e.entity_id END,gi.goal_id,ga.goal_id)",
            goals,
        )
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
            "COALESCE(CASE WHEN e.entity_type='goal' THEN e.entity_id END,"
            "gi.goal_id,ga.goal_id) AS goal_id,"
            "r.session_id,m.provider AS model_provider,m.identifier AS model_identifier,"
            "m.display_name AS model_display_name,m.context_variant,m.context_window_tokens "
            "FROM events e LEFT JOIN tasks t ON t.id=e.task_id "
            "LEFT JOIN workers w ON w.id=COALESCE(e.worker_id,"
            "CASE WHEN e.entity_type='worker' THEN e.entity_id END) "
            "LEFT JOIN nodes n ON n.id=COALESCE(w.node_id,"
            "CASE WHEN e.entity_type='node' THEN e.entity_id END) "
            "LEFT JOIN autonomous_iterations gi ON e.entity_type='goalIteration' "
            "AND gi.id=e.entity_id "
            "LEFT JOIN autonomous_actions ga ON e.entity_type='goalAction' "
            "AND ga.id=e.entity_id "
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
                "quota_snapshot": "quotaChanged",
                "quota_warning": "quotaChanged",
                "quota_critical": "quotaChanged",
                "quota_exhausted": "quotaChanged",
                "quota_reset_observed": "quotaChanged",
                "usage_delta": "quotaChanged",
                "usage_stale": "quotaChanged",
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
            "goalID": row["goal_id"],
            "taskID": row["task_id"],
            "taskReference": row["task_reference"],
            "sessionID": row["session_id"],
            "payload": event_payload,
        }
        return value
