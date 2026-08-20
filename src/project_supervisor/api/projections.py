from __future__ import annotations

import base64
import hashlib
import json
import math
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
    "workerCapabilityManifestRecorded",
    "workerCapabilityObservationRecorded",
    "capabilityRegistered",
    "capabilityUpdated",
    "capabilityDynamicStateUpdated",
    "childWorkProposed",
    "childWorkProposalDecided",
    "subagentRootBound",
    "subagentProposed",
    "subagentAccepted",
    "subagentRejected",
    "resultFusionRecorded",
    "fusionVerificationHandoffCreated",
    "fusionStarted",
    "fusionConflictDetected",
    "fusionCompleted",
    "uiResourceRegistered",
    "uiResourceAcquired",
    "uiResourceReleased",
    "uiExecutionStarted",
    "uiObserved",
    "uiActionVerified",
    "uiActionFailed",
    "uiActionCheckpointed",
    "uiActionOutcomeUnknown",
    "trajectoryCompleted",
    "skillCandidateCreated",
    "skillValidated",
    "skillActivated",
    "skillInvalidated",
    "uiGraphTransitionRecorded",
    "uiResourceRecoveryConfirmed",
    "nodeTransportBound",
    "workerExecutabilityObserved",
    "executionCapacityWaitStarted",
    "executionCapacityRecovered",
    "executionCapacityRecoveryDeferred",
    "nodeExecutionRecoveryLeaseAcquired",
    "nodeRecoveryStarted",
    "nodeRecoveryAccepted",
    "nodeRecoveryRejected",
    "nodeRecoveryFailed",
    "nodeRecoveryOutcomeUnknown",
    "nodeRecovered",
    "nodeEnrollmentIssued",
    "nodeEnrollmentRevoked",
    "nodeEnrollmentAdmitted",
    "authorizationCreated",
    "authorizationInherited",
    "authorizationBound",
    "dataPacketRegistered",
    "dataMovementObserved",
    "providerInvocationRequested",
    "providerInvocationObserved",
    "providerProcessStarted",
    "providerModelUsed",
    "providerDataDisclosed",
    "providerInferenceStarted",
    "providerInferenceCompleted",
    "providerInvocationRejected",
    "providerInvocationCompleted",
    "providerInvocationFailed",
    "providerInvocationCancelled",
    "hypothesisSetCreated",
    "projectWriteAuthorityBound",
    "projectWriteLeaseAcquired",
    "experimentHandoffCreated",
    "experimentResolved",
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


def _safe_semantic_ids(value: Any, *, maximum_items: int = 256) -> list[str]:
    """Extract only bounded semantic identifiers from an untrusted JSON array."""

    if not isinstance(value, list) or len(value) > maximum_items:
        return []
    result: list[str] = []
    for item in value:
        semantic_id = _safe_semantic_id(item)
        if semantic_id is None:
            return []
        result.append(semantic_id)
    return list(dict.fromkeys(result))


def _safe_semantic_version(value: Any, *, maximum: int = 128) -> str | None:
    if (
        not isinstance(value, str)
        or not 1 <= len(value) <= maximum
        or not value.isascii()
        or not value[0].isalnum()
        or ".." in value
        or "://" in value
        or "\\" in value
        or not all(character.isalnum() or character in "._:/-" for character in value)
    ):
        return None
    return value


def _safe_sha256(value: Any, *, prefixed: bool = False) -> str | None:
    if not isinstance(value, str):
        return None
    if prefixed and not value.startswith("sha256:"):
        return None
    digest = value[7:] if prefixed else value
    if len(digest) != 64 or any(character not in "0123456789abcdef" for character in digest):
        return None
    return f"sha256:{digest}" if prefixed else digest


def _safe_finite_number(value: Any) -> int | float | None:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    return value if math.isfinite(float(value)) else None


def _manifest_semantics(value: Any) -> dict[str, Any]:
    """Project the small public subset of a validated manifest JSON document.

    Parameters, descriptions, source material, and unknown extension fields are deliberately
    omitted. A malformed document produces an empty semantic summary rather than raw fallback.
    """

    if not isinstance(value, dict):
        return {}
    capabilities = value.get("capabilities")
    capability_names: list[str] = []
    if isinstance(capabilities, list) and len(capabilities) <= 256:
        for claim in capabilities:
            if not isinstance(claim, dict):
                capability_names = []
                break
            name = _safe_semantic_id(claim.get("name"))
            if name is None:
                capability_names = []
                break
            capability_names.append(name)
    models = _safe_semantic_ids(value.get("models"), maximum_items=128)
    result: dict[str, Any] = {
        "capabilityNames": sorted(set(capability_names)),
        "models": models,
    }
    for source, target, allowed in (
        ("locality", "locality", {"local", "remote", "unknown"}),
        ("privacy", "privacy", {"public", "internal", "sensitive", "restricted", "unknown"}),
        ("costMode", "costMode", {"subscription", "localFree", "paid", "metered", "unknown"}),
    ):
        candidate = value.get(source)
        result[target] = candidate if candidate in allowed else "unknown"
    max_concurrency = value.get("maxConcurrency")
    result["maxConcurrency"] = (
        max_concurrency
        if isinstance(max_concurrency, int)
        and not isinstance(max_concurrency, bool)
        and max_concurrency > 0
        else None
    )
    incremental_cost = _safe_finite_number(value.get("incrementalCostUSD"))
    result["incrementalCostUSD"] = (
        incremental_cost if incremental_cost is not None and incremental_cost >= 0 else None
    )
    return result


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


def _public_provider_model(value: str | None) -> str | None:
    """Return a model identifier only when it is safe as observer-facing metadata."""

    if value is None:
        return None
    candidate = str(redact_sensitive(value)).strip()
    if candidate != value.strip() or _safe_semantic_id(candidate, maximum=256) is None:
        return None
    lowered = candidate.casefold().replace("_", "-")
    credential_markers = (
        "api-key",
        "bearer",
        "cookie",
        "credential",
        "password",
        "private-key",
        "secret",
        "session-token",
        "token",
    )
    if any(marker in lowered for marker in credential_markers):
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

    def fabric_capabilities(self) -> list[dict[str, Any]]:
        """Return current Worker capability heads without exposing either private JSON column."""

        rows = self._rows(
            "SELECT m.id AS manifest_id,m.worker_id,m.revision,m.schema_version,"
            "m.catalog_version,m.provider_id,m.adapter_kind,m.definition_sha256,"
            "m.observed_at AS manifest_observed_at,m.valid_until,m.manifest_json,"
            "h.generation AS head_generation,h.updated_at AS head_updated_at,w.node_id,"
            "o.id AS observation_id,o.version AS observation_version,o.health,o.active_jobs,"
            "o.subscription_state,o.quota_state,o.quota_freshness,"
            "o.observed_at AS observation_observed_at "
            "FROM worker_capability_manifest_heads h "
            "JOIN worker_capability_manifests m "
            "ON m.worker_id=h.worker_id AND m.id=h.manifest_id "
            "JOIN workers w ON w.id=m.worker_id "
            "LEFT JOIN worker_capability_observations o ON o.worker_id=m.worker_id "
            "AND o.manifest_id=m.id "
            "AND o.version=(SELECT MAX(latest.version) "
            "FROM worker_capability_observations latest WHERE latest.worker_id=m.worker_id "
            "AND latest.manifest_id=m.id) "
            "ORDER BY m.worker_id"
        )
        result: list[dict[str, Any]] = []
        for row in rows:
            manifest_id = _safe_semantic_id(row["manifest_id"])
            worker_id = _safe_semantic_id(row["worker_id"])
            node_id = _safe_semantic_id(row["node_id"])
            provider_id = _safe_semantic_id(row["provider_id"])
            adapter_kind = _safe_semantic_id(row["adapter_kind"])
            if None in {manifest_id, worker_id, node_id, provider_id, adapter_kind}:
                continue
            semantics = _manifest_semantics(_json(row["manifest_json"], {}))
            manifest = {
                "id": manifest_id,
                "revision": int(row["revision"]),
                "schemaVersion": _safe_semantic_version(row["schema_version"]),
                "catalogVersion": _safe_semantic_version(row["catalog_version"]),
                "providerID": provider_id,
                "adapterKind": adapter_kind,
                "definitionSHA256": _safe_sha256(row["definition_sha256"], prefixed=True),
                "capabilityNames": semantics.get("capabilityNames", []),
                "models": semantics.get("models", []),
                "locality": semantics.get("locality", "unknown"),
                "privacy": semantics.get("privacy", "unknown"),
                "costMode": semantics.get("costMode", "unknown"),
                "incrementalCostUSD": semantics.get("incrementalCostUSD"),
                "maxConcurrency": semantics.get("maxConcurrency"),
                "observedAt": row["manifest_observed_at"],
                "validUntil": row["valid_until"],
                "headGeneration": int(row["head_generation"]),
                "headUpdatedAt": row["head_updated_at"],
            }
            observation = None
            observation_id = _safe_semantic_id(row["observation_id"])
            if observation_id is not None:
                active_jobs = row["active_jobs"]
                observation = {
                    "id": observation_id,
                    "version": int(row["observation_version"]),
                    "health": row["health"],
                    "activeJobs": (
                        int(active_jobs)
                        if isinstance(active_jobs, int)
                        and not isinstance(active_jobs, bool)
                        and active_jobs >= 0
                        else None
                    ),
                    "subscriptionState": row["subscription_state"],
                    "quotaState": row["quota_state"],
                    "quotaFreshness": row["quota_freshness"],
                    "observedAt": row["observation_observed_at"],
                }
            result.append(
                {
                    "workerID": worker_id,
                    "nodeID": node_id,
                    "manifest": manifest,
                    "observation": observation,
                }
            )
        return result

    def execution_plane(self, *, limit: int = 200) -> list[dict[str, Any]]:
        rows = self._rows(
            "SELECT observation.id,observation.worker_id,observation.node_id,"
            "observation.version,observation.discovered,observation.configured,"
            "observation.authenticated,observation.authorized,observation.platform_approval,"
            "observation.reachable,observation.runtime_available,observation.healthy,"
            "observation.capacity_available,observation.protocol_version,"
            "observation.reason_codes_json,observation.observed_at,observation.valid_until "
            "FROM worker_execution_observations observation WHERE observation.version=("
            "SELECT MAX(latest.version) FROM worker_execution_observations latest "
            "WHERE latest.worker_id=observation.worker_id) ORDER BY observation.worker_id LIMIT ?",
            (limit,),
        )
        return [
            {
                "observationID": _safe_semantic_id(row["id"]),
                "workerID": _safe_semantic_id(row["worker_id"]),
                "nodeID": _safe_semantic_id(row["node_id"]),
                "version": int(row["version"]),
                "facets": {
                    "discovered": row["discovered"],
                    "configured": row["configured"],
                    "authenticated": row["authenticated"],
                    "authorized": row["authorized"],
                    "platformApproval": row["platform_approval"],
                    "reachable": row["reachable"],
                    "runtimeAvailable": row["runtime_available"],
                    "healthy": row["healthy"],
                    "capacityAvailable": row["capacity_available"],
                },
                "protocolVersion": _safe_semantic_version(row["protocol_version"]),
                "reasonCodes": [
                    value
                    for value in _json(row["reason_codes_json"], [])
                    if _safe_semantic_id(value) is not None
                ],
                "observedAt": row["observed_at"],
                "validUntil": row["valid_until"],
            }
            for row in rows
        ]

    def fabric_authorizations(self, *, limit: int = 200) -> list[dict[str, Any]]:
        rows = self._rows(
            "SELECT id,version,parent_id,project_id,goal_id,root_task_id,subject,schema_version,"
            "inheritance_policy,permission_ceiling,capabilities_json,actions_json,resources_json,"
            "allowed_providers_json,allowed_worker_classes_json,allowed_data_classes_json,"
            "denied_data_classes_json,allowed_action_classes_json,denied_action_classes_json,"
            "user_approval_state,platform_approval_required,platform_approval_state,"
            "issued_at,expires_at,definition_sha256 FROM authorization_envelopes "
            "ORDER BY created_at,id LIMIT ?",
            (limit,),
        )
        return [
            {
                "authorizationID": _safe_semantic_id(row["id"]),
                "version": int(row["version"]),
                "parentID": _safe_semantic_id(row["parent_id"]),
                "projectID": _safe_semantic_id(row["project_id"]),
                "goalID": _safe_semantic_id(row["goal_id"]),
                "rootTaskID": _safe_semantic_id(row["root_task_id"]),
                "subject": _safe_semantic_id(row["subject"]),
                "schemaVersion": _safe_semantic_version(row["schema_version"]),
                "permissionCeiling": row["permission_ceiling"],
                "inheritancePolicy": row["inheritance_policy"],
                "capabilities": _json(row["capabilities_json"], []),
                "actions": _json(row["actions_json"], []),
                "resources": _json(row["resources_json"], []),
                "allowedProviders": _json(row["allowed_providers_json"], []),
                "allowedWorkerClasses": _json(row["allowed_worker_classes_json"], []),
                "allowedDataClasses": _json(row["allowed_data_classes_json"], []),
                "deniedDataClasses": _json(row["denied_data_classes_json"], []),
                "allowedActionClasses": _json(row["allowed_action_classes_json"], []),
                "deniedActionClasses": _json(row["denied_action_classes_json"], []),
                "userApprovalState": row["user_approval_state"],
                "platformApprovalRequired": bool(row["platform_approval_required"]),
                "platformApprovalState": row["platform_approval_state"],
                "issuedAt": row["issued_at"],
                "expiresAt": row["expires_at"],
                "definitionSHA256": _safe_sha256(row["definition_sha256"]),
            }
            for row in rows
        ]

    def data_provenance(self, *, limit: int = 200) -> list[dict[str, Any]]:
        rows = self._rows(
            "SELECT movement.id,movement.packet_id,movement.run_id,movement.destination_kind,"
            "movement.purpose,movement.disclosure_state,movement.occurred_at,"
            "packet.project_id,packet.task_id,packet.classification,packet.byte_count,"
            "packet.contains_credentials FROM data_movement_events movement "
            "JOIN data_evidence_packets packet ON packet.id=movement.packet_id "
            "ORDER BY movement.occurred_at,movement.id LIMIT ?",
            (limit,),
        )
        return [
            {
                "movementID": _safe_semantic_id(row["id"]),
                "packetID": _safe_semantic_id(row["packet_id"]),
                "projectID": _safe_semantic_id(row["project_id"]),
                "taskID": _safe_semantic_id(row["task_id"]),
                "runID": _safe_semantic_id(row["run_id"]),
                "classification": row["classification"],
                "byteCount": row["byte_count"],
                "containsCredentials": row["contains_credentials"],
                "destinationKind": row["destination_kind"],
                "purpose": row["purpose"],
                "disclosureState": row["disclosure_state"],
                "occurredAt": row["occurred_at"],
            }
            for row in rows
        ]

    def provider_invocations_v2(self, *, limit: int = 200) -> list[dict[str, Any]]:
        rows = self._rows(
            "SELECT invocation.id,invocation.run_id,invocation.ordinal,invocation.provider,"
            "invocation.requested_model,invocation.envelope_id,invocation.created_at,"
            "observation.stage,observation.model_used_state,observation.model_used,"
            "observation.data_disclosed_state,observation.detail_code,"
            "observation.observed_at FROM provider_invocations_v2 invocation "
            "LEFT JOIN provider_invocation_events_v2 observation "
            "ON observation.invocation_id=invocation.id AND observation.ordinal=("
            "SELECT MAX(latest.ordinal) FROM provider_invocation_events_v2 latest "
            "WHERE latest.invocation_id=invocation.id) "
            "ORDER BY invocation.created_at,invocation.id LIMIT ?",
            (limit,),
        )
        return [
            {
                "invocationID": _safe_semantic_id(row["id"]),
                "runID": _safe_semantic_id(row["run_id"]),
                "ordinal": int(row["ordinal"]),
                "provider": _safe_semantic_id(row["provider"]),
                "requestedModel": _public_provider_model(row["requested_model"]),
                "authorizationID": _safe_semantic_id(row["envelope_id"]),
                "stage": row["stage"],
                "modelUsed": {
                    "state": row["model_used_state"],
                    "model": _public_provider_model(row["model_used"]),
                },
                "dataDisclosed": row["data_disclosed_state"],
                "detailCode": _safe_semantic_id(row["detail_code"]),
                "observedAt": row["observed_at"],
                "createdAt": row["created_at"],
            }
            for row in rows
        ]

    def hypothesis_sets(self, *, limit: int = 200) -> list[dict[str, Any]]:
        rows = self._rows(
            "SELECT id,task_id,source_attempt,fusion_id,state,input_sha256,created_at "
            "FROM hypothesis_sets ORDER BY created_at,id LIMIT ?",
            (limit,),
        )
        result: list[dict[str, Any]] = []
        for row in rows:
            hypotheses = self._rows(
                "SELECT id,claim_key,value_sha256,state FROM hypotheses WHERE set_id=? "
                "ORDER BY claim_key,value_sha256,id",
                (row["id"],),
            )
            proposals = self._rows(
                "SELECT id,proposal_key,operation,risk_class,expected_information_gain,"
                "envelope_id,state,created_at FROM experiment_proposals WHERE set_id=? "
                "ORDER BY created_at,id",
                (row["id"],),
            )
            proposal_ids = tuple(item["id"] for item in proposals)
            handoffs: list[dict[str, Any]] = []
            experiment_results: list[dict[str, Any]] = []
            if proposal_ids:
                placeholders = ",".join("?" for _ in proposal_ids)
                handoffs = self._rows(
                    "SELECT id,proposal_id,owner_kind,owner_id,content_kind,content_sha256,"
                    "expected_result_schema,state,expires_at,created_at "
                    f"FROM structured_executor_handoffs WHERE proposal_id IN ({placeholders}) "
                    "ORDER BY created_at,id",
                    proposal_ids,
                )
                experiment_results = self._rows(
                    "SELECT result.id,result.handoff_id,result.proposal_id,result.executor_id,"
                    "result.result_sha256,result.status,result.created_at "
                    "FROM experiment_results result "
                    f"WHERE result.proposal_id IN ({placeholders}) ORDER BY result.created_at,id",
                    proposal_ids,
                )
            result.append(
                {
                    "hypothesisSetID": _safe_semantic_id(row["id"]),
                    "taskID": _safe_semantic_id(row["task_id"]),
                    "sourceAttempt": int(row["source_attempt"]),
                    "fusionID": _safe_semantic_id(row["fusion_id"]),
                    "state": row["state"],
                    "inputSHA256": _safe_sha256(row["input_sha256"]),
                    "createdAt": row["created_at"],
                    "hypotheses": [
                        {
                            "hypothesisID": _safe_semantic_id(item["id"]),
                            "claimKey": _safe_semantic_id(item["claim_key"]),
                            "valueSHA256": _safe_sha256(item["value_sha256"]),
                            "state": item["state"],
                        }
                        for item in hypotheses
                    ],
                    "experiments": [
                        {
                            "proposalID": _safe_semantic_id(item["id"]),
                            "proposalKey": _safe_semantic_id(item["proposal_key"]),
                            "operation": _safe_semantic_id(item["operation"]),
                            "riskClass": item["risk_class"],
                            "expectedInformationGain": _safe_finite_number(
                                item["expected_information_gain"]
                            ),
                            "authorizationID": _safe_semantic_id(item["envelope_id"]),
                            "state": item["state"],
                            "createdAt": item["created_at"],
                        }
                        for item in proposals
                    ],
                    "handoffs": [
                        {
                            "handoffID": _safe_semantic_id(item["id"]),
                            "proposalID": _safe_semantic_id(item["proposal_id"]),
                            "ownerKind": item["owner_kind"],
                            "ownerID": _safe_semantic_id(item["owner_id"]),
                            "contentKind": item["content_kind"],
                            "contentSHA256": _safe_sha256(item["content_sha256"]),
                            "expectedResultSchema": _safe_semantic_version(
                                item["expected_result_schema"]
                            ),
                            "state": item["state"],
                            "expiresAt": item["expires_at"],
                            "createdAt": item["created_at"],
                        }
                        for item in handoffs
                    ],
                    "experimentResults": [
                        {
                            "resultID": _safe_semantic_id(item["id"]),
                            "handoffID": _safe_semantic_id(item["handoff_id"]),
                            "proposalID": _safe_semantic_id(item["proposal_id"]),
                            "executorID": _safe_semantic_id(item["executor_id"]),
                            "resultSHA256": _safe_sha256(item["result_sha256"]),
                            "status": item["status"],
                            "createdAt": item["created_at"],
                        }
                        for item in experiment_results
                    ],
                }
            )
        return result

    def project_write_authorities(self, *, limit: int = 200) -> list[dict[str, Any]]:
        rows = self._rows(
            "SELECT authority.project_id,authority.owner_kind,authority.owner_id,authority.node_id,"
            "authority.envelope_id,authority.state,authority.generation,authority.created_at,"
            "lease.id AS lease_id,lease.task_id,lease.run_id,lease.generation AS lease_generation,"
            "lease.state AS lease_state,lease.acquired_at,lease.expires_at "
            "FROM project_write_authorities authority LEFT JOIN project_write_leases lease "
            "ON lease.project_id=authority.project_id AND lease.state='active' "
            "ORDER BY authority.project_id LIMIT ?",
            (limit,),
        )
        return [
            {
                "projectID": _safe_semantic_id(row["project_id"]),
                "ownerKind": row["owner_kind"],
                "ownerID": _safe_semantic_id(row["owner_id"]),
                "nodeID": _safe_semantic_id(row["node_id"]),
                "authorizationID": _safe_semantic_id(row["envelope_id"]),
                "state": row["state"],
                "generation": int(row["generation"]),
                "createdAt": row["created_at"],
                "activeLease": (
                    {
                        "leaseID": _safe_semantic_id(row["lease_id"]),
                        "taskID": _safe_semantic_id(row["task_id"]),
                        "runID": _safe_semantic_id(row["run_id"]),
                        "generation": int(row["lease_generation"]),
                        "state": row["lease_state"],
                        "acquiredAt": row["acquired_at"],
                        "expiresAt": row["expires_at"],
                    }
                    if row["lease_id"] is not None
                    else None
                ),
            }
            for row in rows
        ]

    def fabric_routing(
        self, *, task_id: str | None = None, limit: int = 100
    ) -> list[dict[str, Any]]:
        clauses: list[str] = []
        parameters: list[Any] = []
        if task_id is not None:
            clauses.append("task_id=?")
            parameters.append(task_id)
        query = (
            "SELECT id,task_id,topology,policy_version,selected_workers_json,created_at "
            "FROM routing_decisions"
        )
        if clauses:
            query += " WHERE " + " AND ".join(clauses)
        query += " ORDER BY created_at DESC,id LIMIT ?"
        decisions = self._rows(query, (*parameters, limit))
        if not decisions:
            return []
        decision_ids = tuple(row["id"] for row in decisions)
        placeholders = ",".join("?" for _ in decision_ids)
        candidate_rows = self._rows(
            "SELECT decision_id,worker_id,selected,score,rejection_code "
            f"FROM routing_candidates WHERE decision_id IN ({placeholders}) "
            "ORDER BY decision_id,selected DESC,score DESC,worker_id,rejection_code",
            decision_ids,
        )
        candidates_by_decision: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for row in candidate_rows:
            worker_id = _safe_semantic_id(row["worker_id"])
            if worker_id is None:
                continue
            candidates_by_decision[row["decision_id"]].append(
                {
                    "workerID": worker_id,
                    "selected": bool(row["selected"]),
                    "score": _safe_finite_number(row["score"]),
                    "rejectionCode": _safe_semantic_id(row["rejection_code"]),
                }
            )
        result: list[dict[str, Any]] = []
        for row in decisions:
            selected = _safe_semantic_ids(_json(row["selected_workers_json"], []))
            result.append(
                {
                    "id": row["id"],
                    "taskID": row["task_id"],
                    "topology": _safe_semantic_id(row["topology"]),
                    "policyVersion": _safe_semantic_version(row["policy_version"]),
                    "selectedWorkerIDs": selected,
                    "candidates": candidates_by_decision[row["id"]],
                    "createdAt": row["created_at"],
                }
            )
        return result

    def interactions(
        self,
        *,
        task_id: str | None = None,
        state: str | None = None,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        clauses: list[str] = []
        parameters: list[Any] = []
        for column, value in (("e.task_id", task_id), ("e.state", state)):
            if value is not None:
                clauses.append(f"{column}=?")
                parameters.append(value)
        query = (
            "SELECT e.id,e.project_id,e.task_id,e.run_id,e.worker_id,e.adapter_kind,e.channel,"
            "e.app_id,e.app_version,e.plan_sha256,e.state,e.observation_count,"
            "e.grounding_count,e.started_at,e.updated_at,e.finished_at,"
            "t.id AS trajectory_id,t.verified AS trajectory_verified,"
            "t.created_at AS trajectory_created_at FROM interaction_executions e "
            "LEFT JOIN interaction_trajectories t ON t.execution_id=e.id"
        )
        if clauses:
            query += " WHERE " + " AND ".join(clauses)
        query += " ORDER BY e.started_at DESC,e.id LIMIT ?"
        rows = self._rows(query, (*parameters, limit))
        return [self._interaction(row) for row in rows]

    @staticmethod
    def _interaction(row: dict[str, Any]) -> dict[str, Any]:
        trajectory = None
        if row["trajectory_id"] is not None:
            trajectory = {
                "id": row["trajectory_id"],
                "verified": bool(row["trajectory_verified"]),
                "createdAt": row["trajectory_created_at"],
            }
        return {
            "id": row["id"],
            "projectID": row["project_id"],
            "taskID": row["task_id"],
            "runID": row["run_id"],
            "workerID": row["worker_id"],
            "adapterKind": _safe_semantic_id(row["adapter_kind"]),
            "channel": _safe_semantic_id(row["channel"]),
            "appID": _safe_semantic_id(row["app_id"]),
            "appVersion": _safe_semantic_id(row["app_version"], maximum=128),
            "planSHA256": _safe_sha256(row["plan_sha256"]),
            "state": row["state"],
            "observationCount": int(row["observation_count"]),
            "groundingCount": int(row["grounding_count"]),
            "startedAt": row["started_at"],
            "updatedAt": row["updated_at"],
            "finishedAt": row["finished_at"],
            "trajectory": trajectory,
        }

    def interaction_resources(
        self,
        *,
        resource_type: str | None = None,
        active_only: bool = False,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        clauses: list[str] = []
        parameters: list[Any] = []
        if resource_type is not None:
            clauses.append("r.resource_type=?")
            parameters.append(resource_type)
        if active_only:
            clauses.append("l.lease_id IS NOT NULL")
        query = (
            "SELECT r.resource_key,r.resource_type,r.scope_id,r.created_at,r.updated_at,"
            "l.lease_id,l.lease_group_id,l.task_id,l.run_id,l.generation,l.state AS lease_state,"
            "l.acquired_at,l.heartbeat_at,l.expires_at "
            "FROM interaction_resources r LEFT JOIN interaction_resource_leases l "
            "ON l.resource_key=r.resource_key AND l.state='active'"
        )
        if clauses:
            query += " WHERE " + " AND ".join(clauses)
        query += " ORDER BY r.resource_key LIMIT ?"
        rows = self._rows(query, (*parameters, limit))
        result: list[dict[str, Any]] = []
        for row in rows:
            lease = None
            if row["lease_id"] is not None:
                lease = {
                    "leaseID": row["lease_id"],
                    "leaseGroupID": row["lease_group_id"],
                    "taskID": row["task_id"],
                    "runID": row["run_id"],
                    "generation": int(row["generation"]),
                    "state": row["lease_state"],
                    "acquiredAt": row["acquired_at"],
                    "heartbeatAt": row["heartbeat_at"],
                    "expiresAt": row["expires_at"],
                }
            result.append(
                {
                    "resourceKey": _safe_semantic_id(row["resource_key"]),
                    "resourceType": row["resource_type"],
                    "scopeID": _safe_semantic_id(row["scope_id"]),
                    "createdAt": row["created_at"],
                    "updatedAt": row["updated_at"],
                    "activeLease": lease,
                }
            )
        return result

    def semantic_skills(
        self,
        *,
        app_id: str | None = None,
        lifecycle: str | None = None,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        clauses: list[str] = []
        parameters: list[Any] = []
        for column, value in (("s.app_id", app_id), ("s.lifecycle", lifecycle)):
            if value is not None:
                clauses.append(f"{column}=?")
                parameters.append(value)
        query = (
            "SELECT s.id,s.app_id,s.semantic_action,s.app_version_constraint,s.lifecycle,"
            "s.revision,s.success_count,s.failure_count,s.confidence,s.source_trajectory_id,"
            "s.last_verified_at,s.created_at,s.updated_at,COUNT(e.trajectory_id) AS evidence_count,"
            "COALESCE(SUM(e.verified),0) AS verified_evidence_count "
            "FROM semantic_skills s LEFT JOIN semantic_skill_evidence e ON e.skill_id=s.id"
        )
        if clauses:
            query += " WHERE " + " AND ".join(clauses)
        query += " GROUP BY s.id ORDER BY s.updated_at DESC,s.id LIMIT ?"
        rows = self._rows(query, (*parameters, limit))
        return [
            {
                "id": row["id"],
                "appID": _safe_semantic_id(row["app_id"]),
                "semanticAction": _safe_semantic_id(row["semantic_action"]),
                "appVersionConstraint": _safe_semantic_id(
                    row["app_version_constraint"], maximum=128
                ),
                "lifecycle": row["lifecycle"],
                "revision": int(row["revision"]),
                "successCount": int(row["success_count"]),
                "failureCount": int(row["failure_count"]),
                "confidence": _safe_finite_number(row["confidence"]),
                "sourceTrajectoryID": row["source_trajectory_id"],
                "evidenceCount": int(row["evidence_count"]),
                "verifiedEvidenceCount": int(row["verified_evidence_count"]),
                "lastVerifiedAt": row["last_verified_at"],
                "createdAt": row["created_at"],
                "updatedAt": row["updated_at"],
            }
            for row in rows
        ]

    def ui_graph(self, *, app_id: str | None = None, limit: int = 200) -> list[dict[str, Any]]:
        parameters: tuple[Any, ...]
        query = (
            "SELECT app_id,app_version,from_state_sha256,semantic_action,to_state_sha256,"
            "success_count,failure_count,last_verified_at,confidence,created_at,updated_at "
            "FROM ui_graph_edges"
        )
        if app_id is None:
            parameters = (limit,)
        else:
            query += " WHERE app_id=?"
            parameters = (app_id, limit)
        query += " ORDER BY updated_at DESC,app_id,semantic_action LIMIT ?"
        return [
            {
                "appID": _safe_semantic_id(row["app_id"]),
                "appVersion": _safe_semantic_id(row["app_version"], maximum=128),
                "fromStateSHA256": _safe_sha256(row["from_state_sha256"]),
                "semanticAction": _safe_semantic_id(row["semantic_action"]),
                "toStateSHA256": _safe_sha256(row["to_state_sha256"]),
                "successCount": int(row["success_count"]),
                "failureCount": int(row["failure_count"]),
                "lastVerifiedAt": row["last_verified_at"],
                "confidence": _safe_finite_number(row["confidence"]),
                "createdAt": row["created_at"],
                "updatedAt": row["updated_at"],
            }
            for row in self._rows(query, parameters)
        ]

    def spawn_proposals(
        self,
        *,
        goal_id: str | None = None,
        parent_task_id: str | None = None,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        clauses: list[str] = []
        parameters: list[Any] = []
        for column, value in (("p.goal_id", goal_id), ("p.parent_task_id", parent_task_id)):
            if value is not None:
                clauses.append(f"{column}=?")
                parameters.append(value)
        query = (
            "SELECT p.id,p.goal_id,p.parent_task_id,p.source_run_id,p.proposal_key,"
            "p.semantic_digest,p.source_result_sha256,p.depth,p.plan_version,p.steer_version,"
            "p.task_definition_revision,p.created_at,d.id AS decision_id,d.revision AS "
            "decision_revision,d.outcome,d.policy_version,d.reason_code,d.child_task_id,"
            "d.created_at AS decision_created_at FROM child_work_proposals p "
            "LEFT JOIN child_work_proposal_decisions d ON d.proposal_id=p.id "
            "AND d.revision=(SELECT MAX(latest.revision) FROM child_work_proposal_decisions "
            "latest WHERE latest.proposal_id=p.id)"
        )
        if clauses:
            query += " WHERE " + " AND ".join(clauses)
        query += " ORDER BY p.created_at DESC,p.id LIMIT ?"
        rows = self._rows(query, (*parameters, limit))
        result: list[dict[str, Any]] = []
        for row in rows:
            decision = None
            if row["decision_id"] is not None:
                decision = {
                    "id": row["decision_id"],
                    "revision": int(row["decision_revision"]),
                    "outcome": row["outcome"],
                    "policyVersion": _safe_semantic_version(row["policy_version"]),
                    "reasonCode": _safe_semantic_id(row["reason_code"]),
                    "childTaskID": row["child_task_id"],
                    "createdAt": row["decision_created_at"],
                }
            result.append(
                {
                    "id": row["id"],
                    "goalID": row["goal_id"],
                    "parentTaskID": row["parent_task_id"],
                    "sourceRunID": row["source_run_id"],
                    "proposalKey": _safe_semantic_id(row["proposal_key"]),
                    "semanticDigest": _safe_sha256(row["semantic_digest"]),
                    "sourceResultSHA256": _safe_sha256(row["source_result_sha256"]),
                    "depth": int(row["depth"]),
                    "planVersion": int(row["plan_version"]),
                    "steerVersion": int(row["steer_version"]),
                    "taskDefinitionRevision": int(row["task_definition_revision"]),
                    "createdAt": row["created_at"],
                    "latestDecision": decision,
                }
            )
        return result

    def fusion_decisions(
        self, *, task_id: str | None = None, limit: int = 100
    ) -> list[dict[str, Any]]:
        parameters: tuple[Any, ...]
        query = (
            "SELECT id,task_id,source_attempt,policy_version,input_set_sha256,classification,"
            "confidence,verification_required,created_at FROM result_fusion_decisions"
        )
        if task_id is None:
            parameters = (limit,)
        else:
            query += " WHERE task_id=?"
            parameters = (task_id, limit)
        query += " ORDER BY created_at DESC,id LIMIT ?"
        decisions = self._rows(query, parameters)
        if not decisions:
            return []
        decision_ids = tuple(row["id"] for row in decisions)
        placeholders = ",".join("?" for _ in decision_ids)
        handoff_rows = self._rows(
            "SELECT id,fusion_id,verification_scope_id,task_definition_revision,"
            "source_attempt,steer_version,created_at "
            f"FROM fusion_verification_handoffs WHERE fusion_id IN ({placeholders}) "
            "ORDER BY fusion_id,created_at,id",
            decision_ids,
        )
        handoffs_by_fusion: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for row in handoff_rows:
            handoffs_by_fusion[row["fusion_id"]].append(
                {
                    "id": row["id"],
                    "verificationScopeID": row["verification_scope_id"],
                    "taskDefinitionRevision": int(row["task_definition_revision"]),
                    "sourceAttempt": int(row["source_attempt"]),
                    "steerVersion": int(row["steer_version"]),
                    "createdAt": row["created_at"],
                }
            )
        return [
            {
                "id": row["id"],
                "taskID": row["task_id"],
                "sourceAttempt": int(row["source_attempt"]),
                "policyVersion": _safe_semantic_version(row["policy_version"]),
                "inputSetSHA256": _safe_sha256(row["input_set_sha256"]),
                "classification": row["classification"],
                "confidence": _safe_finite_number(row["confidence"]),
                "verificationRequired": bool(row["verification_required"]),
                "createdAt": row["created_at"],
                "verificationHandoffs": handoffs_by_fusion[row["id"]],
            }
            for row in decisions
        ]

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
