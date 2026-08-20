from __future__ import annotations

import hashlib
import json
import math
import re
import uuid
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, replace
from datetime import UTC, datetime, timedelta
from typing import Any

from project_supervisor.domain import EventSeverity, PermissionClass, TaskState
from project_supervisor.store import StateStore, compact_json, redact_sensitive, timestamp
from project_supervisor.verification import DefinitionOfDoneResult

from .capabilities import (
    INITIAL_CAPABILITY_CATALOG,
    WORKER_MANIFEST_SCHEMA_VERSION,
    CapabilityClaim,
    CapabilityError,
    WorkerDynamicState,
    WorkerManifest,
)
from .execution import (
    ChildWorkProposal,
    SpawnContext,
    SpawnDisposition,
    SpawnGovernor,
    SpawnPolicy,
    SpawnReason,
)
from .fusion import (
    ContributionState,
    FusionEngine,
    FusionPolicy,
    FusionResult,
    ResultContribution,
    VerificationHandoffToken,
)
from .interaction import UIAction, UIActionResult, UIPlan, UIPlanResult, UISnapshot

_SEMANTIC_ID = re.compile(r"^[A-Za-z][A-Za-z0-9._:-]{0,159}$")
_RESOURCE_TYPES = frozenset(
    {
        "desktopSession",
        "browserContext",
        "window",
        "mouse",
        "keyboard",
        "clipboard",
        "display",
    }
)


def _sha256(value: Any) -> str:
    return hashlib.sha256(compact_json(value).encode("utf-8")).hexdigest()


def _bounded_mapping(value: Mapping[str, Any], *, maximum_bytes: int = 131_072) -> dict[str, Any]:
    safe = redact_sensitive(dict(value))
    encoded = compact_json(safe).encode("utf-8")
    if len(encoded) > maximum_bytes:
        raise ValueError("Fabric structured payload exceeds its persistence limit")
    return safe


def _event(
    store: StateStore,
    connection: Any,
    *,
    kind: str,
    entity_type: str,
    entity_id: str,
    summary: str,
    payload: Mapping[str, Any],
    project_id: str | None = None,
    task_id: str | None = None,
    worker_id: str | None = None,
    run_id: str | None = None,
    severity: EventSeverity = EventSeverity.INFO,
    actor: str = "fabric",
) -> int:
    # Event payloads are deliberately semantic allowlists assembled by the caller. The shared
    # redactor remains defense in depth; full UI trees, values, prompts, screenshots, and paths
    # never belong here.
    return store._append_event(
        connection,
        kind=kind,
        severity=severity,
        entity_type=entity_type,
        entity_id=entity_id,
        project_id=project_id,
        task_id=task_id,
        worker_id=worker_id,
        run_id=run_id,
        summary=summary,
        payload=redact_sensitive(dict(payload)),
        actor=actor,
    )


class CapabilityRegistryRepository:
    """Durable static manifests plus append-only dynamic observations.

    A manifest is immutable and its mutable head is a narrow generation-CAS pointer. Dynamic
    observations never mutate static capability truth and UNKNOWN values remain explicit.
    """

    def __init__(self, store: StateStore) -> None:
        self.store = store

    def register_manifest(
        self,
        manifest: WorkerManifest,
        *,
        source: str = "operator",
        observed_at: datetime | None = None,
        valid_until: datetime | None = None,
        expected_head_generation: int | None = None,
    ) -> dict[str, Any]:
        if not _SEMANTIC_ID.fullmatch(source):
            raise ValueError("manifest source must be a semantic identity")
        observed = observed_at or datetime.now(UTC)
        if observed.tzinfo is None or (valid_until is not None and valid_until.tzinfo is None):
            raise ValueError("manifest timestamps must be timezone-aware")
        if valid_until is not None and valid_until <= observed:
            raise ValueError("manifest validity must end after its observation")
        protocol = manifest.to_protocol()
        if len(compact_json(protocol).encode("utf-8")) > 131_072:
            raise ValueError("worker capability manifest exceeds its byte limit")
        manifest_id = f"manifest-{uuid.uuid4()}"
        now = timestamp()
        with self.store.transaction() as connection:
            worker = connection.execute(
                "SELECT id,node_id,provider FROM workers WHERE id=?", (manifest.worker_id,)
            ).fetchone()
            if worker is None:
                raise KeyError(manifest.worker_id)
            if worker["node_id"] != manifest.node_id or worker["provider"] != manifest.provider_id:
                raise ValueError("worker manifest identity does not match the canonical Worker")
            head = connection.execute(
                "SELECT * FROM worker_capability_manifest_heads WHERE worker_id=?",
                (manifest.worker_id,),
            ).fetchone()
            generation = int(head["generation"]) if head is not None else 0
            if expected_head_generation is not None and generation != expected_head_generation:
                raise RuntimeError("worker manifest head generation conflict")
            existing = connection.execute(
                "SELECT * FROM worker_capability_manifests "
                "WHERE worker_id=? AND definition_sha256=?",
                (manifest.worker_id, manifest.digest),
            ).fetchone()
            if existing is not None:
                if int(existing["revision"]) != manifest.manifest_revision:
                    raise RuntimeError("manifest digest replay conflicts with revision")
                if head is not None and head["manifest_id"] == existing["id"]:
                    return self._manifest_projection(connection, existing, generation)
                raise RuntimeError("an older immutable manifest cannot be reactivated")
            latest = connection.execute(
                "SELECT COALESCE(MAX(revision),0) AS revision "
                "FROM worker_capability_manifests WHERE worker_id=?",
                (manifest.worker_id,),
            ).fetchone()
            if manifest.manifest_revision != int(latest["revision"]) + 1:
                raise ValueError("worker manifest revision must be consecutive")
            connection.execute(
                "INSERT INTO worker_capability_manifests(id,worker_id,revision,schema_version,"
                "catalog_version,provider_id,adapter_kind,definition_sha256,source,observed_at,"
                "valid_until,manifest_json,created_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    manifest_id,
                    manifest.worker_id,
                    manifest.manifest_revision,
                    manifest.schema_version,
                    manifest.catalog_version,
                    manifest.provider_id,
                    manifest.adapter_kind,
                    manifest.digest,
                    source,
                    timestamp(observed),
                    timestamp(valid_until) if valid_until is not None else None,
                    compact_json(protocol),
                    now,
                ),
            )
            next_generation = generation + 1
            connection.execute(
                "INSERT INTO worker_capability_manifest_heads(worker_id,manifest_id,generation,"
                "updated_at) VALUES (?,?,?,?) ON CONFLICT(worker_id) DO UPDATE SET "
                "manifest_id=excluded.manifest_id,generation=excluded.generation,"
                "updated_at=excluded.updated_at",
                (manifest.worker_id, manifest_id, next_generation, now),
            )
            _event(
                self.store,
                connection,
                kind="capabilityRegistered" if generation == 0 else "capabilityUpdated",
                entity_type="workerCapabilityManifest",
                entity_id=manifest_id,
                summary="Worker capability manifest activated",
                payload={
                    "manifestID": manifest_id,
                    "workerID": manifest.worker_id,
                    "revision": manifest.manifest_revision,
                    "catalogVersion": manifest.catalog_version,
                    "capabilities": sorted(manifest.capability_names),
                    "headGeneration": next_generation,
                },
                worker_id=manifest.worker_id,
                actor=source,
            )
            row = connection.execute(
                "SELECT * FROM worker_capability_manifests WHERE id=?", (manifest_id,)
            ).fetchone()
            return self._manifest_projection(connection, row, next_generation)

    def record_observation(
        self,
        state: WorkerDynamicState,
        *,
        observation_id: str | None = None,
    ) -> dict[str, Any]:
        identity = observation_id or f"cap-observation-{uuid.uuid4()}"
        protocol = state.to_protocol()
        observed_at = state.observed_at or datetime.now(UTC)
        with self.store.transaction() as connection:
            head = connection.execute(
                "SELECT h.manifest_id FROM worker_capability_manifest_heads h WHERE h.worker_id=?",
                (state.worker_id,),
            ).fetchone()
            if head is None:
                raise ValueError("dynamic capability state requires an active manifest")
            version_row = connection.execute(
                "SELECT COALESCE(MAX(version),0)+1 AS version "
                "FROM worker_capability_observations WHERE worker_id=?",
                (state.worker_id,),
            ).fetchone()
            version = int(version_row["version"])
            connection.execute(
                "INSERT INTO worker_capability_observations(id,worker_id,manifest_id,version,"
                "health,active_jobs,subscription_state,quota_state,quota_freshness,state_json,"
                "observed_at,created_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    identity,
                    state.worker_id,
                    head["manifest_id"],
                    version,
                    state.health.value,
                    state.running_tasks,
                    state.subscription_state.value,
                    state.quota.value,
                    state.quota_freshness.value,
                    compact_json(protocol),
                    timestamp(observed_at),
                    timestamp(),
                ),
            )
            _event(
                self.store,
                connection,
                kind="capabilityDynamicStateUpdated",
                entity_type="workerCapabilityObservation",
                entity_id=identity,
                summary="Worker capability dynamic state observed",
                payload={
                    "workerID": state.worker_id,
                    "version": version,
                    "health": state.health.value,
                    "quota": state.quota.value,
                    "quotaFreshness": state.quota_freshness.value,
                    "runningTasks": state.running_tasks,
                },
                worker_id=state.worker_id,
            )
            row = connection.execute(
                "SELECT * FROM worker_capability_observations WHERE id=?", (identity,)
            ).fetchone()
        return dict(row)

    def get_worker(self, worker_id: str) -> dict[str, Any]:
        with self.store.connect() as connection:
            row = connection.execute(
                "SELECT m.*,h.generation AS head_generation "
                "FROM worker_capability_manifest_heads h "
                "JOIN worker_capability_manifests m ON m.id=h.manifest_id "
                "WHERE h.worker_id=?",
                (worker_id,),
            ).fetchone()
            if row is None:
                raise KeyError(worker_id)
            projection = self._manifest_projection(connection, row, int(row["head_generation"]))
        return projection

    def list_workers(self) -> list[dict[str, Any]]:
        with self.store.connect() as connection:
            rows = connection.execute(
                "SELECT m.*,h.generation AS head_generation "
                "FROM worker_capability_manifest_heads h "
                "JOIN worker_capability_manifests m ON m.id=h.manifest_id "
                "ORDER BY m.worker_id"
            ).fetchall()
            return [
                self._manifest_projection(connection, row, int(row["head_generation"]))
                for row in rows
            ]

    @staticmethod
    def _manifest_projection(connection: Any, row: Any, head_generation: int) -> dict[str, Any]:
        manifest = json.loads(row["manifest_json"])
        observation = connection.execute(
            "SELECT * FROM worker_capability_observations WHERE worker_id=? AND manifest_id=? "
            "ORDER BY version DESC LIMIT 1",
            (row["worker_id"], row["id"]),
        ).fetchone()
        return {
            "manifestID": row["id"],
            "workerID": row["worker_id"],
            "revision": int(row["revision"]),
            "schemaVersion": row["schema_version"],
            "catalogVersion": row["catalog_version"],
            "provider": row["provider_id"],
            "adapter": row["adapter_kind"],
            "digest": row["definition_sha256"],
            "headGeneration": head_generation,
            "capabilities": manifest.get("capabilities", []),
            "models": manifest.get("models", []),
            "locality": manifest.get("locality", "unknown"),
            "privacy": manifest.get("privacy", "unknown"),
            "costMode": manifest.get("costMode", "unknown"),
            "incrementalCostUSD": manifest.get("incrementalCostUSD"),
            "maxConcurrency": manifest.get("maxConcurrency", 1),
            "observedAt": row["observed_at"],
            "validUntil": row["valid_until"],
            "dynamic": json.loads(observation["state_json"]) if observation is not None else None,
        }


class SpawnRepository:
    """Supervisor-owned atomic admission for untrusted child-work proposals."""

    _ROOT_KEYS = frozenset(
        {"topology", "priority", "requirements", "executionSpec", "authorizationNarrowing"}
    )
    _REQUIREMENT_KEYS = frozenset(
        {
            "labels",
            "requiredCapabilities",
            "requiredCapabilityParameters",
            "preferredCapabilities",
            "permissionClass",
            "privacySensitive",
            "codeWriteRequired",
            "panelSize",
            "preferredWorkers",
            "localOnly",
            "minimumQuality",
            "maxIncrementalCostUSD",
            "explicitWorkerID",
            "requiredManifestSchemaVersion",
            "requiredCatalogVersion",
        }
    )

    def __init__(self, store: StateStore, policy: SpawnPolicy | None = None) -> None:
        self.store = store
        self.governor = SpawnGovernor(policy)

    def bind_root_task(
        self,
        *,
        task_id: str,
        goal_id: str,
        plan_version: int = 0,
        expected_steer_version: int,
    ) -> dict[str, Any]:
        if plan_version < 0 or expected_steer_version < 0:
            raise ValueError("root task plan and steer versions must be non-negative")
        with self.store.transaction() as connection:
            task = connection.execute("SELECT * FROM tasks WHERE id=?", (task_id,)).fetchone()
            goal = connection.execute(
                "SELECT * FROM autonomous_goals WHERE id=?", (goal_id,)
            ).fetchone()
            if task is None:
                raise KeyError(task_id)
            if goal is None:
                raise KeyError(goal_id)
            if task["project_id"] != goal["project_id"]:
                raise ValueError("autonomous Task binding cannot cross projects")
            if int(goal["steer_version"]) != expected_steer_version:
                raise RuntimeError("Goal steer version changed before Task binding")
            existing = connection.execute(
                "SELECT * FROM autonomous_task_bindings WHERE task_id=?", (task_id,)
            ).fetchone()
            expected = (goal_id, plan_version, expected_steer_version)
            if existing is not None:
                actual = (
                    existing["goal_id"],
                    int(existing["plan_version"]),
                    int(existing["steer_version"]),
                )
                if actual != expected or existing["parent_task_id"] is not None:
                    raise RuntimeError("root Task binding conflicts with immutable authority")
                return dict(existing)
            connection.execute(
                "INSERT INTO autonomous_task_bindings(task_id,goal_id,parent_task_id,"
                "proposal_id,depth,plan_version,steer_version,created_at) "
                "VALUES (?,?,NULL,NULL,0,?,?,?)",
                (task_id, goal_id, plan_version, expected_steer_version, timestamp()),
            )
            _event(
                self.store,
                connection,
                kind="subagentRootBound",
                entity_type="task",
                entity_id=task_id,
                summary="Root Task bound to autonomous spawn authority",
                payload={
                    "goalID": goal_id,
                    "taskID": task_id,
                    "planVersion": plan_version,
                    "steerVersion": expected_steer_version,
                },
                project_id=task["project_id"],
                task_id=task_id,
            )
            row = connection.execute(
                "SELECT * FROM autonomous_task_bindings WHERE task_id=?", (task_id,)
            ).fetchone()
        return dict(row)

    def admit(self, proposal: ChildWorkProposal) -> dict[str, Any]:
        specification = proposal.to_protocol()
        payload = specification["payload"]
        if not isinstance(payload, dict):
            raise ValueError("child-work payload must be a JSON object")
        task_fields = self._task_fields(payload)
        with self.store.transaction() as connection:
            goal = connection.execute(
                "SELECT * FROM autonomous_goals WHERE id=?", (proposal.goal_id,)
            ).fetchone()
            if goal is None:
                raise KeyError(proposal.goal_id)
            parent = connection.execute(
                "SELECT task.*,binding.depth AS spawn_depth,binding.plan_version AS plan_version,"
                "binding.steer_version AS binding_steer_version "
                "FROM tasks task JOIN autonomous_task_bindings binding ON binding.task_id=task.id "
                "WHERE task.id=? AND binding.goal_id=?",
                (proposal.parent_task_id, proposal.goal_id),
            ).fetchone()
            if parent is None:
                raise ValueError("child work requires a parent bound to the same Goal")
            if parent["project_id"] != goal["project_id"]:
                raise ValueError("child work cannot cross project boundaries")
            if proposal.depth != int(parent["spawn_depth"]) + 1:
                raise ValueError("child-work depth does not follow its canonical parent")
            if (
                proposal.task_definition_revision is not None
                and proposal.task_definition_revision != int(parent["definition_revision"])
            ):
                raise ValueError("child proposal uses a stale parent Task revision")
            source_result_sha256: str | None = None
            if proposal.source_run_id is not None:
                source_run = connection.execute(
                    "SELECT run.*,result.summary,result.created_at AS result_created_at "
                    "FROM worker_runs run LEFT JOIN worker_results result ON result.run_id=run.id "
                    "WHERE run.id=?",
                    (proposal.source_run_id,),
                ).fetchone()
                if source_run is None or source_run["task_id"] != proposal.parent_task_id:
                    raise ValueError("proposal source run does not belong to its parent Task")
                if source_run["summary"] is None:
                    raise ValueError("child work requires an immutable source result")
                if source_run["state"] not in {
                    "completed",
                    "failed",
                    "cancelled",
                    "timedOut",
                    "authRequired",
                    "rateLimited",
                    "interrupted",
                }:
                    raise ValueError("child proposal source run is not terminal")
                if proposal.source_attempt is not None and proposal.source_attempt != int(
                    source_run["attempt"]
                ):
                    raise ValueError("proposal source attempt is stale")
                if int(source_run["attempt"]) != int(parent["attempt_count"]):
                    raise ValueError("child proposal source run is not the current Task attempt")
                if int(source_run["task_definition_revision"]) != int(
                    parent["definition_revision"]
                ):
                    raise ValueError("child proposal source run uses a stale Task revision")
                if source_run["verification_scope_id"] != parent["current_verification_scope_id"]:
                    raise ValueError("child proposal source run uses a stale verification scope")
                source_result_sha256 = _sha256(
                    {
                        "runID": proposal.source_run_id,
                        "summary": source_run["summary"],
                        "createdAt": source_run["result_created_at"],
                    }
                )

            existing = connection.execute(
                "SELECT proposal.*,decision.outcome,decision.child_task_id,decision.reason_code "
                "FROM child_work_proposals proposal LEFT JOIN "
                "child_work_proposal_decisions decision ON decision.proposal_id=proposal.id "
                "WHERE proposal.goal_id=? AND proposal.parent_task_id=? "
                "AND proposal.proposal_key=? "
                "ORDER BY decision.revision DESC LIMIT 1",
                (proposal.goal_id, proposal.parent_task_id, proposal.proposal_key),
            ).fetchone()
            if existing is not None:
                if existing["semantic_digest"] != proposal.semantic_digest:
                    return self._reject_conflict(connection, proposal, goal)
                return {
                    "proposalID": existing["id"],
                    "disposition": "replay",
                    "reasonCode": SpawnReason.DUPLICATE_REPLAY.value,
                    "childTaskID": existing["child_task_id"],
                    "canonicalTaskCreated": False,
                }

            context = self._context(connection, proposal, goal, parent)
            decision = self._goal_bounded_governor(goal).evaluate(proposal, context)
            now = timestamp()
            connection.execute(
                "INSERT INTO child_work_proposals(id,goal_id,parent_task_id,source_run_id,"
                "proposal_key,semantic_digest,source_result_sha256,depth,plan_version,"
                "steer_version,task_definition_revision,specification_json,created_at) "
                "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    proposal.proposal_id,
                    proposal.goal_id,
                    proposal.parent_task_id,
                    proposal.source_run_id,
                    proposal.proposal_key,
                    proposal.semantic_digest,
                    source_result_sha256,
                    proposal.depth,
                    int(parent["plan_version"]),
                    proposal.steer_version,
                    int(parent["definition_revision"]),
                    compact_json(specification),
                    now,
                ),
            )
            _event(
                self.store,
                connection,
                kind="subagentProposed",
                entity_type="childWorkProposal",
                entity_id=proposal.proposal_id,
                summary="Worker proposed bounded child work",
                payload={
                    "proposalID": proposal.proposal_id,
                    "goalID": proposal.goal_id,
                    "parentTaskID": proposal.parent_task_id,
                    "depth": proposal.depth,
                    "steerVersion": proposal.steer_version,
                },
                project_id=goal["project_id"],
                task_id=proposal.parent_task_id,
                run_id=proposal.source_run_id,
            )
            child_task_id: str | None = None
            if decision.disposition is SpawnDisposition.ELIGIBLE:
                child_task_id = self._create_child_task(
                    connection,
                    proposal,
                    goal,
                    parent,
                    task_fields,
                    now,
                )
                outcome = "accepted"
                reason_code = "POLICY_ACCEPTED"
                event_kind = "subagentAccepted"
            else:
                outcome = "rejected"
                reason_code = (
                    decision.primary_reason.value
                    if decision.primary_reason is not None
                    else "POLICY_REJECTED"
                )
                event_kind = "subagentRejected"
            decision_id = f"spawn-decision-{uuid.uuid4()}"
            connection.execute(
                "INSERT INTO child_work_proposal_decisions(id,proposal_id,revision,outcome,"
                "policy_version,reason_code,child_task_id,budget_json,created_at) "
                "VALUES (?,?,1,?,?,?,?,?,?)",
                (
                    decision_id,
                    proposal.proposal_id,
                    outcome,
                    decision.policy_version,
                    reason_code,
                    child_task_id,
                    compact_json(decision.budget_delta.to_protocol()),
                    now,
                ),
            )
            _event(
                self.store,
                connection,
                kind=event_kind,
                entity_type="childWorkProposal",
                entity_id=proposal.proposal_id,
                summary=f"Child-work proposal {outcome}",
                payload={
                    "proposalID": proposal.proposal_id,
                    "childTaskID": child_task_id,
                    "reasonCode": reason_code,
                    "policyVersion": decision.policy_version,
                },
                project_id=goal["project_id"],
                task_id=child_task_id or proposal.parent_task_id,
                run_id=proposal.source_run_id,
                severity=(EventSeverity.INFO if outcome == "accepted" else EventSeverity.WARNING),
            )
        return {
            "proposalID": proposal.proposal_id,
            "decisionID": decision_id,
            "disposition": outcome,
            "reasonCode": reason_code,
            "childTaskID": child_task_id,
            "canonicalTaskCreated": child_task_id is not None,
        }

    def admit_from_worker_result(
        self,
        run_id: str,
        envelope: Mapping[str, Any],
    ) -> dict[str, Any]:
        """Turn a typed Worker proposal envelope into Supervisor-owned canonical admission."""

        allowed = {
            "schemaVersion",
            "proposalID",
            "proposalKey",
            "title",
            "description",
            "provider",
            "dependencies",
            "requestedWorkerSlots",
            "estimatedTokens",
            "estimatedSeconds",
            "estimatedCostUSD",
            "taskLockKey",
            "circuitKey",
            "payload",
        }
        value = dict(envelope)
        if set(value) - allowed or value.get("schemaVersion") != "child-work-proposal/v1":
            raise ValueError("Worker child proposal envelope is not child-work-proposal/v1")
        with self.store.connect() as connection:
            source = connection.execute(
                "SELECT run.task_id,run.attempt,run.state,run.task_definition_revision,"
                "run.verification_scope_id,task.attempt_count,task.definition_revision,"
                "task.current_verification_scope_id,binding.goal_id,binding.depth,"
                "binding.steer_version,goal.steer_version AS current_steer_version,"
                "result.run_id AS result_id "
                "FROM worker_runs run JOIN tasks task ON task.id=run.task_id "
                "JOIN autonomous_task_bindings binding ON binding.task_id=run.task_id "
                "JOIN autonomous_goals goal ON goal.id=binding.goal_id "
                "LEFT JOIN worker_results result ON result.run_id=run.id WHERE run.id=?",
                (run_id,),
            ).fetchone()
        if source is None or source["result_id"] is None:
            raise ValueError("Worker child proposal requires an immutable canonical result")
        if source["state"] not in {
            "completed",
            "failed",
            "cancelled",
            "timedOut",
            "authRequired",
            "rateLimited",
            "interrupted",
        }:
            raise ValueError("Worker child proposal source run is not terminal")
        if (
            int(source["attempt"]) != int(source["attempt_count"])
            or int(source["task_definition_revision"]) != int(source["definition_revision"])
            or source["verification_scope_id"] != source["current_verification_scope_id"]
            or int(source["steer_version"]) != int(source["current_steer_version"])
        ):
            raise ValueError("Worker child proposal source result is no longer current")
        return self.admit(
            ChildWorkProposal(
                proposal_id=str(value.get("proposalID", "")),
                goal_id=str(source["goal_id"]),
                parent_task_id=str(source["task_id"]),
                proposal_key=str(value.get("proposalKey", "")),
                title=str(value.get("title", "")),
                description=str(value.get("description", "")),
                steer_version=int(source["steer_version"]),
                depth=int(source["depth"]) + 1,
                source_run_id=run_id,
                source_attempt=int(source["attempt"]),
                task_definition_revision=int(source["task_definition_revision"]),
                provider=value.get("provider"),
                dependencies=tuple(value.get("dependencies", ())),
                requested_worker_slots=value.get("requestedWorkerSlots", 1),
                estimated_tokens=value.get("estimatedTokens"),
                estimated_seconds=value.get("estimatedSeconds"),
                estimated_cost_usd=value.get("estimatedCostUSD"),
                task_lock_key=value.get("taskLockKey"),
                circuit_key=value.get("circuitKey"),
                payload=value.get("payload", {}),
            )
        )

    def _goal_bounded_governor(self, goal: Any) -> SpawnGovernor:
        """Intersect Worker-spawn policy with the canonical Goal budget.

        The pure governor remains reusable, but repository admission is authoritative and must
        never permit a proposal that the owning Goal could not have planned itself.
        """

        try:
            budget = json.loads(goal["budgets_json"])
        except (KeyError, TypeError, json.JSONDecodeError) as error:
            raise ValueError("Goal budget is unavailable for child-work admission") from error
        if not isinstance(budget, dict):
            raise ValueError("Goal budget must be a JSON object")

        def positive_number(name: str, *, integer: bool = False) -> int | float | None:
            value = budget.get(name)
            if value is None:
                return None
            valid_type = isinstance(value, int) if integer else isinstance(value, (int, float))
            if isinstance(value, bool) or not valid_type or not math.isfinite(float(value)):
                raise ValueError(f"Goal budget {name} must be a finite positive number")
            if value <= 0:
                raise ValueError(f"Goal budget {name} must be a finite positive number")
            return int(value) if integer else float(value)

        def lower_limit(
            configured: int | float | None,
            canonical: int | float | None,
        ) -> int | float | None:
            if configured is None:
                return canonical
            if canonical is None:
                return configured
            return min(configured, canonical)

        policy = self.governor.policy
        max_tasks = positive_number("max_tasks", integer=True)
        assert isinstance(max_tasks, int)
        bounded = replace(
            policy,
            max_total_children=min(policy.max_total_children, max_tasks),
            max_total_tokens=lower_limit(
                policy.max_total_tokens,
                positive_number("max_total_tokens", integer=True),
            ),
            max_elapsed_seconds=lower_limit(
                policy.max_elapsed_seconds,
                positive_number("max_elapsed_seconds"),
            ),
            max_cost_usd=lower_limit(
                policy.max_cost_usd,
                positive_number("max_cost_usd"),
            ),
        )
        return SpawnGovernor(bounded)

    def _context(
        self, connection: Any, proposal: ChildWorkProposal, goal: Any, parent: Any
    ) -> SpawnContext:
        existing_rows = connection.execute(
            "SELECT proposal_key,semantic_digest FROM child_work_proposals "
            "WHERE goal_id=? AND parent_task_id=?",
            (proposal.goal_id, proposal.parent_task_id),
        ).fetchall()
        children_for_parent = connection.execute(
            "SELECT COUNT(*) AS count FROM autonomous_task_bindings WHERE parent_task_id=?",
            (proposal.parent_task_id,),
        ).fetchone()
        total_children = connection.execute(
            "SELECT COUNT(*) AS count FROM autonomous_task_bindings WHERE goal_id=? AND depth>0",
            (proposal.goal_id,),
        ).fetchone()
        active_tasks = connection.execute(
            "SELECT COUNT(*) AS count FROM autonomous_task_bindings binding "
            "JOIN tasks task ON task.id=binding.task_id WHERE binding.goal_id=? "
            "AND task.state IN ('ready','running','waiting','reviewing')",
            (proposal.goal_id,),
        ).fetchone()
        active_runs = connection.execute(
            "SELECT worker.provider,COUNT(*) AS count FROM autonomous_task_bindings binding "
            "JOIN worker_runs run ON run.task_id=binding.task_id "
            "JOIN workers worker ON worker.id=run.worker_id WHERE binding.goal_id=? "
            "AND binding.proposal_id IS NULL "
            "AND run.state IN ('starting','running','waiting') GROUP BY worker.provider",
            (proposal.goal_id,),
        ).fetchall()
        reservation_rows = connection.execute(
            "SELECT proposal.parent_task_id,proposal.specification_json,decision.budget_json "
            "FROM child_work_proposal_decisions decision "
            "JOIN child_work_proposals proposal ON proposal.id=decision.proposal_id "
            "JOIN tasks child ON child.id=decision.child_task_id "
            "WHERE proposal.goal_id=? AND decision.outcome='accepted' "
            "AND child.state IN ('ready','running','waiting','reviewing')",
            (proposal.goal_id,),
        ).fetchall()
        reserved_slots = 0
        reserved_tokens_value = 0
        reserved_cost_value = 0.0
        provider_reservations: dict[str, int] = {}
        for row in reservation_rows:
            budget_value = json.loads(row["budget_json"])
            specification_value = json.loads(row["specification_json"])
            slots = int(budget_value["workerSlots"])
            reserved_slots += slots
            token_value = budget_value.get("estimatedTokens")
            cost_value = budget_value.get("estimatedCostUSD")
            if token_value is None:
                reserved_tokens_value = -1
            elif reserved_tokens_value >= 0:
                reserved_tokens_value += int(token_value)
            if cost_value is None:
                reserved_cost_value = -1
            elif reserved_cost_value >= 0:
                reserved_cost_value += float(cost_value)
            provider_value = specification_value.get("provider")
            if isinstance(provider_value, str) and provider_value:
                provider_key = provider_value.casefold()
                provider_reservations[provider_key] = (
                    provider_reservations.get(provider_key, 0) + slots
                )
        active_slots = sum(int(row["count"]) for row in active_runs) + reserved_slots
        completed_runs = connection.execute(
            "SELECT COUNT(*) AS count FROM autonomous_task_bindings binding "
            "JOIN worker_runs run ON run.task_id=binding.task_id WHERE binding.goal_id=? "
            "AND run.state IN ('completed','failed','cancelled','timedOut','authRequired',"
            "'rateLimited','interrupted')",
            (proposal.goal_id,),
        ).fetchone()
        run_count = int(completed_runs["count"])
        consumed_tokens = self._known_usage(connection, proposal.goal_id, "totalTokens", run_count)
        observed_cost = self._known_usage(connection, proposal.goal_id, "costUSD", run_count)
        started = goal["started_at"]
        elapsed = 0.0
        if started is not None:
            elapsed = max(
                0.0,
                (
                    datetime.now(UTC) - datetime.fromisoformat(started.replace("Z", "+00:00"))
                ).total_seconds(),
            )
        lock_rows = connection.execute(
            "SELECT proposal.specification_json FROM child_work_proposals proposal "
            "JOIN child_work_proposal_decisions decision ON decision.proposal_id=proposal.id "
            "WHERE proposal.goal_id=? AND decision.outcome='accepted'",
            (proposal.goal_id,),
        ).fetchall()
        locks = frozenset(
            str(value)
            for row in lock_rows
            if (value := json.loads(row["specification_json"]).get("taskLockKey")) is not None
        )
        circuit_rows = connection.execute(
            "SELECT proposal.parent_task_id,proposal.specification_json "
            "FROM child_work_proposal_decisions decision "
            "JOIN child_work_proposals proposal ON proposal.id=decision.proposal_id "
            "JOIN tasks child ON child.id=decision.child_task_id "
            "WHERE proposal.goal_id=? AND decision.outcome='accepted' AND child.state='failed'",
            (proposal.goal_id,),
        ).fetchall()
        circuit_failures: dict[str, int] = {}
        for row in circuit_rows:
            specification_value = json.loads(row["specification_json"])
            circuit = specification_value.get("circuitKey")
            if not isinstance(circuit, str) or not circuit:
                provider_value = specification_value.get("provider")
                circuit = (
                    f"provider:{provider_value.casefold()}"
                    if isinstance(provider_value, str) and provider_value
                    else f"task:{row['parent_task_id']}"
                )
            circuit_failures[circuit] = circuit_failures.get(circuit, 0) + 1
        active_by_provider = {str(row["provider"]): int(row["count"]) for row in active_runs}
        for provider_key, count in provider_reservations.items():
            active_by_provider[provider_key] = active_by_provider.get(provider_key, 0) + count
        return SpawnContext(
            goal_id=proposal.goal_id,
            goal_state=str(goal["state"]),
            current_steer_version=int(goal["steer_version"]),
            parent_steer_version=int(parent["binding_steer_version"]),
            children_for_parent=int(children_for_parent["count"]),
            total_children=max(int(total_children["count"]), int(goal["task_count"])),
            active_tasks=int(active_tasks["count"]),
            active_worker_slots=active_slots,
            active_by_provider=active_by_provider,
            consumed_tokens=consumed_tokens,
            reserved_tokens=(None if reserved_tokens_value < 0 else reserved_tokens_value),
            elapsed_seconds=elapsed,
            observed_cost_usd=observed_cost,
            reserved_cost_usd=None if reserved_cost_value < 0 else reserved_cost_value,
            existing_proposal_digests={
                str(row["proposal_key"]): str(row["semantic_digest"]) for row in existing_rows
            },
            locked_task_keys=locks,
            circuit_failures=circuit_failures,
        )

    @staticmethod
    def _known_usage(
        connection: Any, goal_id: str, metric: str, completed_run_count: int
    ) -> float | int | None:
        row = connection.execute(
            "SELECT COUNT(DISTINCT usage.run_id) AS runs,SUM(usage.value) AS total "
            "FROM usage_records usage JOIN autonomous_task_bindings binding "
            "ON binding.task_id=usage.task_id WHERE binding.goal_id=? AND usage.metric=? "
            "AND usage.value IS NOT NULL",
            (goal_id, metric),
        ).fetchone()
        if completed_run_count == 0:
            return 0
        if int(row["runs"] or 0) != completed_run_count:
            return None
        total = float(row["total"] or 0)
        return int(total) if metric == "totalTokens" else total

    def _create_child_task(
        self,
        connection: Any,
        proposal: ChildWorkProposal,
        goal: Any,
        parent: Any,
        fields: dict[str, Any],
        now: str,
    ) -> str:
        child_task_id = f"tsk-child-{uuid.uuid4()}"
        reference = f"child:{proposal.proposal_id}"
        requirements = fields["requirements"]
        execution_spec = dict(fields["executionSpec"])
        parent_specification = json.loads(parent["execution_spec_json"] or "{}")
        if not isinstance(parent_specification, dict):
            raise ValueError("parent Task execution spec is invalid")
        parent_authorization_id = parent_specification.get("authorizationEnvelopeID")
        narrowing = fields["authorizationNarrowing"]
        if parent_authorization_id is None and narrowing:
            raise PermissionError("child work cannot create authority without a parent envelope")
        if parent_authorization_id is not None:
            from .authority import AuthorizationRepository

            authority = AuthorizationRepository(self.store)
            parent_envelope = authority._load(connection, str(parent_authorization_id))
            child_authorization_id = (
                "authorization-child-"
                + hashlib.sha256(
                    f"{parent_authorization_id}:{proposal.proposal_id}".encode()
                ).hexdigest()[:32]
            )
            child_envelope = parent_envelope.derive(
                authorization_id=child_authorization_id,
                subject=f"task:{child_task_id}",
                permission_ceiling=PermissionClass(
                    narrowing.get("permissionCeiling", parent_envelope.permission_ceiling.value)
                ),
                capabilities=self._optional_identity_set(narrowing, "capabilities"),
                actions=self._optional_identity_set(narrowing, "actions"),
                resources=self._optional_identity_set(narrowing, "resources"),
                data_refs=self._optional_identity_set(narrowing, "dataRefs"),
                allowed_providers=self._optional_identity_set(narrowing, "allowedProviders"),
                allowed_worker_classes=self._optional_identity_set(
                    narrowing, "allowedWorkerClasses"
                ),
                allowed_data_classes=self._optional_identity_set(narrowing, "allowedDataClasses"),
                denied_data_classes=self._optional_identity_set(narrowing, "deniedDataClasses"),
                allowed_action_classes=self._optional_identity_set(
                    narrowing, "allowedActionClasses"
                ),
                denied_action_classes=self._optional_identity_set(narrowing, "deniedActionClasses"),
                budget=narrowing.get("budget"),
                issued_by="supervisor.spawn",
            )
            authority.issue_in_transaction(connection, child_envelope)
            execution_spec["authorizationEnvelopeID"] = child_authorization_id
        connection.execute(
            "INSERT INTO tasks(id,project_id,reference,title,description,state,topology,priority,"
            "labels_json,required_capabilities_json,permission_class,approval_state,"
            "minimum_context_tokens,privacy_sensitive,code_write_required,panel_size,"
            "preferred_workers_json,preferred_capabilities_json,capability_constraints_json,"
            "local_only,minimum_quality,max_incremental_cost_usd,explicit_worker_id,"
            "execution_spec_json,attempt_count,version,created_at,updated_at) "
            "VALUES (?,?,?,?,?,'ready',?,?,?,?,?,'notRequired',NULL,?,?,?,?,?,?,?,?,?,?,?,0,1,?,?)",
            (
                child_task_id,
                goal["project_id"],
                reference,
                proposal.title,
                proposal.description,
                fields["topology"],
                fields["priority"],
                compact_json(requirements["labels"]),
                compact_json(requirements["requiredCapabilities"]),
                requirements["permissionClass"],
                int(requirements["privacySensitive"]),
                int(requirements["codeWriteRequired"]),
                requirements["panelSize"],
                compact_json(requirements["preferredWorkers"]),
                compact_json(requirements["preferredCapabilities"]),
                compact_json(
                    {
                        "schemaVersion": "capability-request/v1",
                        "required": requirements["requiredCapabilityParameters"],
                        "requiredManifestSchemaVersion": requirements[
                            "requiredManifestSchemaVersion"
                        ],
                        "requiredCatalogVersion": requirements["requiredCatalogVersion"],
                    }
                ),
                int(requirements["localOnly"]),
                requirements["minimumQuality"],
                requirements["maxIncrementalCostUSD"],
                requirements["explicitWorkerID"],
                compact_json(execution_spec),
                now,
                now,
            ),
        )
        for dependency in proposal.dependencies:
            row = connection.execute(
                "SELECT project_id FROM tasks WHERE id=?", (dependency,)
            ).fetchone()
            if row is None or row["project_id"] != goal["project_id"]:
                raise ValueError("child dependency is missing or belongs to another project")
            connection.execute(
                "INSERT INTO task_dependencies(task_id,depends_on_task_id) VALUES (?,?)",
                (child_task_id, dependency),
            )
        connection.execute(
            "INSERT INTO autonomous_task_bindings(task_id,goal_id,parent_task_id,proposal_id,"
            "depth,plan_version,steer_version,created_at) VALUES (?,?,?,?,?,?,?,?)",
            (
                child_task_id,
                proposal.goal_id,
                proposal.parent_task_id,
                proposal.proposal_id,
                proposal.depth,
                int(parent["plan_version"]),
                proposal.steer_version,
                now,
            ),
        )
        connection.execute(
            "UPDATE autonomous_goals SET task_count=task_count+1,updated_at=?,version=version+1 "
            "WHERE id=? AND state='running' AND steer_version=?",
            (now, proposal.goal_id, proposal.steer_version),
        )
        if connection.execute("SELECT changes()").fetchone()[0] != 1:
            raise RuntimeError("Goal authority changed before child Task creation committed")
        if parent_authorization_id is not None:
            binding_id = f"authorization-binding-{uuid.uuid4()}"
            connection.execute(
                "INSERT INTO authorization_envelope_bindings(id,envelope_id,task_id,run_id,"
                "binding_kind,bound_by,created_at) VALUES (?,?,?,NULL,'task',?,?)",
                (
                    binding_id,
                    execution_spec["authorizationEnvelopeID"],
                    child_task_id,
                    "supervisor.spawn",
                    now,
                ),
            )
            _event(
                self.store,
                connection,
                kind="authorizationBound",
                entity_type="authorizationEnvelopeBinding",
                entity_id=binding_id,
                summary="Narrowed authorization bound to child Task",
                payload={
                    "authorizationID": execution_spec["authorizationEnvelopeID"],
                    "parentAuthorizationID": parent_authorization_id,
                    "taskID": child_task_id,
                },
                project_id=goal["project_id"],
                task_id=child_task_id,
                actor="supervisor.spawn",
            )
        return child_task_id

    @staticmethod
    def _optional_identity_set(narrowing: Mapping[str, Any], key: str) -> frozenset[str] | None:
        if key not in narrowing:
            return None
        value = narrowing[key]
        if not isinstance(value, list):
            raise ValueError(f"authorization narrowing {key} must be an array")
        return frozenset(str(item) for item in value)

    @classmethod
    def _task_fields(cls, payload: dict[str, Any]) -> dict[str, Any]:
        unknown = set(payload) - cls._ROOT_KEYS
        if unknown:
            raise ValueError(f"unknown child-work payload fields: {','.join(sorted(unknown))}")
        requirements = payload.get("requirements", {})
        if not isinstance(requirements, dict):
            raise ValueError("child-work requirements must be an object")
        unknown_requirements = set(requirements) - cls._REQUIREMENT_KEYS
        if unknown_requirements:
            raise ValueError(
                "unknown child-work requirement fields: " + ",".join(sorted(unknown_requirements))
            )
        labels = requirements.get("labels", [])
        required = requirements.get("requiredCapabilities", [])
        parameter_requirements = requirements.get("requiredCapabilityParameters", [])
        preferred = requirements.get("preferredCapabilities", [])
        preferred_workers = requirements.get("preferredWorkers", [])
        for label, values in (
            ("labels", labels),
            ("requiredCapabilities", required),
            ("preferredCapabilities", preferred),
            ("preferredWorkers", preferred_workers),
        ):
            if not isinstance(values, list) or any(
                not isinstance(item, str) or not item or len(item) > 200 for item in values
            ):
                raise ValueError(f"{label} must be bounded string identifiers")
            if len(values) != len(set(values)):
                raise ValueError(f"{label} must not contain duplicates")
        allowed_labels = {
            "coding",
            "architecture",
            "research",
            "fastRouting",
            "rag",
            "review",
            "creative",
            "longContext",
            "privacySensitive",
            "highUncertainty",
        }
        if not set(labels) <= allowed_labels:
            raise ValueError("child-work labels contain an unsupported value")
        if not isinstance(parameter_requirements, list) or len(parameter_requirements) > 64:
            raise ValueError("requiredCapabilityParameters must be a bounded array")
        try:
            canonical_required = INITIAL_CAPABILITY_CATALOG.canonicalize_names(required)
            canonical_preferred = INITIAL_CAPABILITY_CATALOG.canonicalize_names(preferred)
            raw_claims: list[CapabilityClaim] = []
            for item in parameter_requirements:
                if (
                    not isinstance(item, dict)
                    or "name" not in item
                    or set(item) - {"name", "parameters"}
                    or not isinstance(item["name"], str)
                    or not isinstance(item.get("parameters", {}), dict)
                ):
                    raise ValueError("requiredCapabilityParameters entries are invalid")
                raw_claims.append(CapabilityClaim(item["name"], item.get("parameters", {})))
            canonical_claims = INITIAL_CAPABILITY_CATALOG.canonicalize_claims(raw_claims)
        except CapabilityError as error:
            raise ValueError("child-work capability contract is invalid") from error
        if not {claim.name for claim in canonical_claims}.issubset(canonical_required):
            raise ValueError("parameter constraints must name a required capability")
        required_manifest_version = requirements.get("requiredManifestSchemaVersion")
        if required_manifest_version not in {None, WORKER_MANIFEST_SCHEMA_VERSION}:
            raise ValueError("child-work manifest schema version is unsupported")
        required_catalog_version = requirements.get("requiredCatalogVersion")
        if required_catalog_version not in {None, INITIAL_CAPABILITY_CATALOG.version}:
            raise ValueError("child-work capability catalog version is unsupported")
        topology = payload.get("topology", "single")
        if topology not in {
            "single",
            "primaryReviewer",
            "parallelPanel",
            "cheapFirstEscalation",
            "fallback",
        }:
            raise ValueError("child-work topology is unsupported")
        priority = payload.get("priority", 50)
        panel_size = requirements.get("panelSize", 1)
        if isinstance(priority, bool) or not isinstance(priority, int) or not 0 <= priority <= 100:
            raise ValueError("child-work priority must be between zero and 100")
        if isinstance(panel_size, bool) or not isinstance(panel_size, int) or panel_size < 1:
            raise ValueError("child-work panel size must be positive")
        permission = requirements.get("permissionClass", "green")
        if permission not in {"green", "yellow"}:
            raise ValueError("child work cannot self-authorize a RED permission class")
        minimum_quality = requirements.get("minimumQuality")
        max_cost = requirements.get("maxIncrementalCostUSD")
        if minimum_quality is not None and (
            isinstance(minimum_quality, bool)
            or not isinstance(minimum_quality, (int, float))
            or not math.isfinite(float(minimum_quality))
            or not 0 <= float(minimum_quality) <= 1
        ):
            raise ValueError("child-work minimum quality must be between zero and one")
        if max_cost is not None and (
            isinstance(max_cost, bool)
            or not isinstance(max_cost, (int, float))
            or not math.isfinite(float(max_cost))
            or float(max_cost) < 0
        ):
            raise ValueError("child-work cost ceiling must be non-negative")
        for field_name in ("privacySensitive", "codeWriteRequired", "localOnly"):
            value = requirements.get(field_name, False)
            if not isinstance(value, bool):
                raise ValueError(f"child-work {field_name} must be a boolean")
        explicit_worker = requirements.get("explicitWorkerID")
        if explicit_worker is not None and (
            not isinstance(explicit_worker, str)
            or not explicit_worker
            or len(explicit_worker) > 200
        ):
            raise ValueError("child-work explicit Worker identity must be bounded")
        execution_spec = payload.get("executionSpec", {})
        if not isinstance(execution_spec, dict):
            raise ValueError("child-work execution spec must be an object")
        if redact_sensitive(execution_spec) != execution_spec:
            raise ValueError("child-work execution spec contains credential-shaped fields")
        if "authorizationEnvelopeID" in execution_spec:
            raise PermissionError("child work cannot self-assign an authorization envelope")
        narrowing = payload.get("authorizationNarrowing", {})
        if not isinstance(narrowing, dict):
            raise ValueError("authorizationNarrowing must be an object")
        allowed_narrowing = {
            "permissionCeiling",
            "capabilities",
            "actions",
            "resources",
            "dataRefs",
            "allowedProviders",
            "allowedWorkerClasses",
            "allowedDataClasses",
            "deniedDataClasses",
            "allowedActionClasses",
            "deniedActionClasses",
            "budget",
        }
        if set(narrowing) - allowed_narrowing:
            raise ValueError("authorizationNarrowing contains unsupported fields")
        for key, value in narrowing.items():
            if key == "permissionCeiling":
                if value not in {"green", "yellow", "red"}:
                    raise ValueError("authorization narrowing permission is invalid")
            elif key == "budget":
                if not isinstance(value, dict):
                    raise ValueError("authorization narrowing budget must be an object")
            elif not isinstance(value, list) or any(
                not isinstance(item, str) or not item or len(item) > 160 for item in value
            ):
                raise ValueError(f"authorization narrowing {key} is invalid")
        return {
            "topology": topology,
            "priority": priority,
            "requirements": {
                "labels": sorted(labels),
                "requiredCapabilities": sorted(canonical_required),
                "requiredCapabilityParameters": [claim.to_protocol() for claim in canonical_claims],
                "preferredCapabilities": sorted(canonical_preferred),
                "permissionClass": permission,
                "privacySensitive": requirements.get("privacySensitive", False),
                "codeWriteRequired": requirements.get("codeWriteRequired", False),
                "panelSize": panel_size,
                "preferredWorkers": preferred_workers,
                "localOnly": requirements.get("localOnly", False),
                "minimumQuality": minimum_quality,
                "maxIncrementalCostUSD": max_cost,
                "explicitWorkerID": explicit_worker,
                "requiredManifestSchemaVersion": required_manifest_version,
                "requiredCatalogVersion": required_catalog_version,
            },
            "executionSpec": execution_spec,
            "authorizationNarrowing": narrowing,
        }

    def _reject_conflict(
        self, connection: Any, proposal: ChildWorkProposal, goal: Any
    ) -> dict[str, Any]:
        _event(
            self.store,
            connection,
            kind="subagentRejected",
            entity_type="childWorkProposal",
            entity_id=proposal.proposal_id,
            summary="Child-work proposal idempotency conflict rejected",
            payload={
                "proposalID": proposal.proposal_id,
                "reasonCode": SpawnReason.IDEMPOTENCY_CONFLICT.value,
            },
            project_id=goal["project_id"],
            task_id=proposal.parent_task_id,
            run_id=proposal.source_run_id,
            severity=EventSeverity.WARNING,
        )
        return {
            "proposalID": proposal.proposal_id,
            "disposition": "rejected",
            "reasonCode": SpawnReason.IDEMPOTENCY_CONFLICT.value,
            "childTaskID": None,
            "canonicalTaskCreated": False,
        }


class FusionRepository:
    """Persist one immutable fusion decision and its independent-verification handoff."""

    def __init__(self, store: StateStore) -> None:
        self.store = store

    @staticmethod
    def _source_result_sha256(row: Any) -> str:
        return _sha256(
            {
                "runID": row["id"],
                "summary": row["summary"],
                "changedFiles": json.loads(row["changed_files_json"]),
                "commandsRun": json.loads(row["commands_run_json"]),
                "tests": json.loads(row["tests_json"]),
                "artifacts": json.loads(row["artifacts_json"]),
                "commitHash": row["commit_hash"],
                "blockers": json.loads(row["blockers_json"]),
                "confidence": row["confidence"],
                "recommendedNextActions": json.loads(row["recommended_next_actions_json"]),
                "createdAt": row["result_created_at"],
            }
        )

    def source_result_sha256(self, run_id: str) -> str:
        with self.store.connect() as connection:
            row = self._source_result_row(connection, run_id)
        if row is None:
            raise ValueError("fusion contribution requires an immutable Worker result")
        return self._source_result_sha256(row)

    @staticmethod
    def _source_result_row(connection: Any, run_id: str) -> Any:
        return connection.execute(
            "SELECT run.id,run.task_id,run.worker_id,run.attempt,run.state,run.exit_code,"
            "run.verification_scope_id,run.task_definition_revision,result.summary,"
            "result.changed_files_json,result.commands_run_json,result.tests_json,"
            "result.artifacts_json,result.commit_hash,result.blockers_json,result.confidence,"
            "result.recommended_next_actions_json,result.created_at AS result_created_at "
            "FROM worker_runs run JOIN worker_results result ON result.run_id=run.id "
            "WHERE run.id=?",
            (run_id,),
        ).fetchone()

    def contribution_from_worker_result(
        self,
        run_id: str,
        *,
        contribution_id: str,
        claims: Mapping[str, Any],
        evidence: Mapping[str, Any] | None = None,
        role: str = "primary",
        state: ContributionState = ContributionState.ASSERTED,
    ) -> ResultContribution:
        """Normalize one semantic contribution around an immutable canonical WorkerResult."""

        with self.store.connect() as connection:
            row = self._source_result_row(connection, run_id)
            if row is None:
                raise ValueError("fusion contribution requires an immutable Worker result")
            task = connection.execute(
                "SELECT definition_revision,current_verification_scope_id FROM tasks WHERE id=?",
                (row["task_id"],),
            ).fetchone()
            binding = connection.execute(
                "SELECT binding.steer_version,goal.steer_version AS current_steer_version "
                "FROM autonomous_task_bindings binding JOIN autonomous_goals goal "
                "ON goal.id=binding.goal_id WHERE binding.task_id=?",
                (row["task_id"],),
            ).fetchone()
        if task is None:
            raise KeyError(str(row["task_id"]))
        current_steer = int(binding["current_steer_version"]) if binding is not None else 0
        if binding is not None and int(binding["steer_version"]) != current_steer:
            raise RuntimeError("Worker result belongs to a stale autonomous steer version")
        if row["verification_scope_id"] != task["current_verification_scope_id"] or int(
            row["task_definition_revision"]
        ) != int(task["definition_revision"]):
            raise RuntimeError("Worker result no longer matches the current Task definition")
        return ResultContribution(
            contribution_id=contribution_id,
            task_id=str(row["task_id"]),
            run_id=run_id,
            worker_id=str(row["worker_id"]),
            verification_scope_id=str(row["verification_scope_id"]),
            task_definition_revision=int(row["task_definition_revision"]),
            source_attempt=int(row["attempt"]),
            source_result_sha256=self._source_result_sha256(row),
            steer_version=current_steer,
            claims=claims,
            evidence=evidence or {},
            role=role,
            state=state,
        )

    def fuse_attempt(
        self,
        task_id: str,
        claims_by_run: Mapping[str, Mapping[str, Any]],
        *,
        evidence_by_run: Mapping[str, Mapping[str, Any]] | None = None,
        policy: FusionPolicy | None = None,
    ) -> dict[str, Any]:
        """Fuse every canonical run from the current attempt and persist the decision."""

        with self.store.connect() as connection:
            task = connection.execute(
                "SELECT attempt_count FROM tasks WHERE id=?", (task_id,)
            ).fetchone()
            if task is None:
                raise KeyError(task_id)
            run_ids = tuple(
                str(row["id"])
                for row in connection.execute(
                    "SELECT id FROM worker_runs WHERE task_id=? AND attempt=? ORDER BY id",
                    (task_id, int(task["attempt_count"])),
                ).fetchall()
            )
        if set(claims_by_run) != set(run_ids):
            raise ValueError("fusion normalization must cover every run in the current attempt")
        evidence_values = dict(evidence_by_run or {})
        contributions = tuple(
            self.contribution_from_worker_result(
                run_id,
                contribution_id=f"contribution:{run_id}",
                claims=claims_by_run[run_id],
                evidence=evidence_values.get(run_id, {}),
            )
            for run_id in run_ids
        )
        return self.persist(FusionEngine(policy).fuse(contributions))

    def persist(self, result: FusionResult, *, fusion_id: str | None = None) -> dict[str, Any]:
        if result.task_id is None or result.source_attempt is None:
            raise ValueError("canonical fusion requires Task and attempt provenance")
        identity = fusion_id or f"fusion-{uuid.uuid4()}"
        with self.store.transaction() as connection:
            task = connection.execute(
                "SELECT * FROM tasks WHERE id=?", (result.task_id,)
            ).fetchone()
            if task is None:
                raise KeyError(result.task_id)
            if int(task["attempt_count"]) != result.source_attempt:
                raise RuntimeError("fusion source attempt is no longer current")
            if int(task["definition_revision"]) != result.task_definition_revision:
                raise RuntimeError("fusion Task definition revision is no longer current")
            if task["current_verification_scope_id"] != result.verification_scope_id:
                raise RuntimeError("fusion verification scope is no longer current")
            binding = connection.execute(
                "SELECT binding.steer_version,goal.steer_version AS current_steer_version "
                "FROM autonomous_task_bindings binding JOIN autonomous_goals goal "
                "ON goal.id=binding.goal_id WHERE binding.task_id=?",
                (result.task_id,),
            ).fetchone()
            current_steer = int(binding["current_steer_version"]) if binding is not None else 0
            if result.steer_version != current_steer or (
                binding is not None and int(binding["steer_version"]) != current_steer
            ):
                raise RuntimeError("fusion steer version is no longer current")
            run_ids = {item.run_id for item in result.contribution_provenance}
            if not run_ids:
                raise ValueError("fusion requires at least one source run")
            if len(run_ids) != len(result.contribution_provenance):
                raise ValueError("fusion permits exactly one contribution per canonical run")
            placeholders = ",".join("?" for _ in run_ids)
            source_rows = connection.execute(
                "SELECT run.id,run.task_id,run.worker_id,run.attempt,run.state,run.exit_code,"
                "run.verification_scope_id,run.task_definition_revision,result.summary,"
                "result.changed_files_json,result.commands_run_json,result.tests_json,"
                "result.artifacts_json,result.commit_hash,result.blockers_json,result.confidence,"
                "result.recommended_next_actions_json,result.created_at AS result_created_at "
                f"FROM worker_runs run JOIN worker_results result ON result.run_id=run.id "
                f"WHERE run.id IN ({placeholders})",
                tuple(sorted(run_ids)),
            ).fetchall()
            expected_runs = {
                str(row["id"])
                for row in connection.execute(
                    "SELECT id FROM worker_runs WHERE task_id=? AND attempt=?",
                    (result.task_id, result.source_attempt),
                ).fetchall()
            }
            provenance_by_run = {item.run_id: item for item in result.contribution_provenance}
            if len(source_rows) != len(run_ids) or run_ids != expected_runs:
                raise ValueError("fusion must cover every canonical run in its Task attempt")
            for row in source_rows:
                provenance = provenance_by_run[str(row["id"])]
                if (
                    row["task_id"] != result.task_id
                    or int(row["attempt"]) != result.source_attempt
                    or provenance.worker_id != row["worker_id"]
                    or row["verification_scope_id"] != result.verification_scope_id
                    or int(row["task_definition_revision"]) != result.task_definition_revision
                    or provenance.source_result_sha256 != self._source_result_sha256(row)
                ):
                    raise ValueError("fusion contribution provenance is not canonical")
            if result.ready_for_verification and any(
                row["state"] != "completed" or row["exit_code"] != 0 for row in source_rows
            ):
                raise ValueError("only successful canonical runs may reach verification handoff")
            existing = connection.execute(
                "SELECT * FROM result_fusion_decisions WHERE task_id=? AND source_attempt=? "
                "AND policy_version=? AND input_set_sha256=?",
                (
                    result.task_id,
                    result.source_attempt,
                    result.policy_version,
                    result.input_set_sha256,
                ),
            ).fetchone()
            if existing is not None:
                return self._projection(connection, existing)
            protocol = result.to_protocol()
            now = timestamp()
            _event(
                self.store,
                connection,
                kind="fusionStarted",
                entity_type="fusionDecision",
                entity_id=identity,
                summary="Deterministic result fusion started",
                payload={
                    "fusionID": identity,
                    "taskID": result.task_id,
                    "sourceAttempt": result.source_attempt,
                    "inputSetSHA256": result.input_set_sha256,
                    "contributionCount": len(result.contribution_provenance),
                },
                project_id=task["project_id"],
                task_id=result.task_id,
            )
            connection.execute(
                "INSERT INTO result_fusion_decisions(id,task_id,source_attempt,policy_version,"
                "input_set_sha256,classification,fused_json,conflicts_json,provenance_json,"
                "confidence,verification_required,created_at) VALUES (?,?,?,?,?,?,?,?,?,NULL,1,?)",
                (
                    identity,
                    result.task_id,
                    result.source_attempt,
                    result.policy_version,
                    result.input_set_sha256,
                    result.status.value,
                    compact_json(
                        {
                            "fusionHash": result.fusion_hash,
                            "claims": protocol["claims"],
                            "missingRequiredClaims": protocol["missingRequiredClaims"],
                        }
                    ),
                    compact_json(protocol["conflicts"]),
                    compact_json(protocol["contributions"]),
                    now,
                ),
            )
            if result.conflicts:
                _event(
                    self.store,
                    connection,
                    kind="fusionConflictDetected",
                    entity_type="fusionDecision",
                    entity_id=identity,
                    summary="Result fusion preserved contradictory claims",
                    payload={
                        "fusionID": identity,
                        "conflictKeys": [item.claim_key for item in result.conflicts],
                        "verificationRequired": True,
                    },
                    project_id=task["project_id"],
                    task_id=result.task_id,
                    severity=EventSeverity.WARNING,
                )
            if result.ready_for_verification:
                handoff = result.verification_handoff()
                connection.execute(
                    "INSERT INTO fusion_verification_handoffs(id,fusion_id,"
                    "verification_scope_id,task_definition_revision,source_attempt,steer_version,"
                    "token_sha256,created_at) VALUES (?,?,?,?,?,?,?,?)",
                    (
                        f"fusion-handoff-{uuid.uuid4()}",
                        identity,
                        handoff.verification_scope_id,
                        handoff.task_definition_revision,
                        handoff.source_attempt,
                        handoff.steer_version,
                        handoff.token_sha256,
                        now,
                    ),
                )
            _event(
                self.store,
                connection,
                kind="fusionCompleted",
                entity_type="fusionDecision",
                entity_id=identity,
                summary=f"Result fusion classified {result.status.value}",
                payload={
                    "fusionID": identity,
                    "status": result.status.value,
                    "fusionHash": result.fusion_hash,
                    "claimCount": len(result.claims),
                    "conflictCount": len(result.conflicts),
                    "verificationRequired": True,
                    "readyForVerification": result.ready_for_verification,
                },
                project_id=task["project_id"],
                task_id=result.task_id,
            )
            row = connection.execute(
                "SELECT * FROM result_fusion_decisions WHERE id=?", (identity,)
            ).fetchone()
            return self._projection(connection, row)

    def apply_independent_verification(
        self,
        fusion_id: str,
        result: DefinitionOfDoneResult,
        *,
        verifier: str = "deterministic-verifier",
        max_attempts: int | None = None,
    ) -> TaskState:
        """Apply verification only through the immutable scope/revision/attempt handoff."""

        with self.store.connect() as connection:
            row = connection.execute(
                "SELECT decision.task_id,decision.classification,handoff.verification_scope_id,"
                "handoff.task_definition_revision,handoff.source_attempt,handoff.steer_version,"
                "handoff.token_sha256,decision.fused_json "
                "FROM result_fusion_decisions decision "
                "JOIN fusion_verification_handoffs handoff ON handoff.fusion_id=decision.id "
                "WHERE decision.id=? ORDER BY handoff.created_at DESC LIMIT 1",
                (fusion_id,),
            ).fetchone()
        if row is None:
            raise ValueError("fusion has no independent-verification handoff")
        if row["classification"] not in {"compatible", "complementary"}:
            raise ValueError("contradictory or insufficient fusion cannot be verified as complete")
        fused = json.loads(row["fused_json"])
        handoff = VerificationHandoffToken(
            task_id=str(row["task_id"]),
            verification_scope_id=str(row["verification_scope_id"]),
            task_definition_revision=int(row["task_definition_revision"]),
            source_attempt=int(row["source_attempt"]),
            steer_version=int(row["steer_version"]),
            fusion_hash=str(fused["fusionHash"]),
        )
        if handoff.token_sha256 != row["token_sha256"]:
            raise RuntimeError("fusion verification handoff integrity check failed")
        state = self.store.apply_task_verification(
            str(row["task_id"]),
            result,
            verifier=verifier,
            max_attempts=max_attempts,
            expected_verification_scope_id=str(row["verification_scope_id"]),
            expected_task_definition_revision=int(row["task_definition_revision"]),
            expected_source_attempt=int(row["source_attempt"]),
            expected_steer_version=int(row["steer_version"]),
        )
        if state is TaskState.SUCCEEDED:
            finalize_verified_interaction_learning(self.store, str(row["task_id"]))
        return state

    def get(self, fusion_id: str) -> dict[str, Any]:
        with self.store.connect() as connection:
            row = connection.execute(
                "SELECT * FROM result_fusion_decisions WHERE id=?", (fusion_id,)
            ).fetchone()
            if row is None:
                raise KeyError(fusion_id)
            return self._projection(connection, row)

    @staticmethod
    def _projection(connection: Any, row: Any) -> dict[str, Any]:
        handoff = connection.execute(
            "SELECT token_sha256,verification_scope_id,task_definition_revision,"
            "source_attempt,steer_version FROM fusion_verification_handoffs "
            "WHERE fusion_id=? ORDER BY created_at DESC LIMIT 1",
            (row["id"],),
        ).fetchone()
        return {
            "fusionID": row["id"],
            "taskID": row["task_id"],
            "sourceAttempt": int(row["source_attempt"]),
            "policyVersion": row["policy_version"],
            "inputSetSHA256": row["input_set_sha256"],
            "classification": row["classification"],
            "fused": json.loads(row["fused_json"]),
            "conflicts": json.loads(row["conflicts_json"]),
            "provenance": json.loads(row["provenance_json"]),
            "verificationRequired": True,
            "verificationHandoff": dict(handoff) if handoff is not None else None,
            "createdAt": row["created_at"],
        }


@dataclass(frozen=True, slots=True)
class ResourceLeaseBundle:
    lease_group_id: str
    owner_id: str
    resources: tuple[tuple[str, int], ...]
    expires_at: datetime
    task_id: str | None = None
    task_lease_owner_id: str | None = None
    task_lease_generation: int | None = None

    def __post_init__(self) -> None:
        if not _SEMANTIC_ID.fullmatch(self.owner_id):
            raise ValueError("resource lease owner must be a semantic identity")
        if not self.lease_group_id.strip() or not self.resources:
            raise ValueError("resource lease group and resources are required")
        keys = tuple(key for key, generation in self.resources if generation > 0)
        if len(keys) != len(self.resources) or len(keys) != len(set(keys)):
            raise ValueError("resource lease keys and generations must be unique and positive")
        if self.expires_at.tzinfo is None:
            raise ValueError("resource lease expiry must be timezone-aware")
        task_lease_values = (self.task_lease_owner_id, self.task_lease_generation)
        if (task_lease_values[0] is None) != (task_lease_values[1] is None):
            raise ValueError("task lease owner and generation must be provided together")
        if self.task_lease_owner_id is not None:
            if self.task_id is None:
                raise ValueError("a task-bound resource lease requires its task identity")
            if not _SEMANTIC_ID.fullmatch(self.task_lease_owner_id):
                raise ValueError("task lease owner must be a semantic identity")
            if self.task_lease_generation is None or self.task_lease_generation < 1:
                raise ValueError("task lease generation must be positive")

    def generation_for(self, resource_key: str) -> int:
        for key, generation in self.resources:
            if key == resource_key:
                return generation
        raise KeyError(resource_key)


class InteractionResourceRepository:
    """Generation-fenced, all-or-none semantic interaction resource leases.

    A browser context is parallel by using a distinct resource key. Global mouse, keyboard,
    clipboard, display, and desktop-session keys remain exclusive by default.
    """

    def __init__(self, store: StateStore) -> None:
        self.store = store

    def register(self, *, resource_key: str, resource_type: str, scope_id: str) -> dict[str, Any]:
        if not _SEMANTIC_ID.fullmatch(resource_key) or not _SEMANTIC_ID.fullmatch(scope_id):
            raise ValueError("interaction resource key and scope must be semantic identities")
        if resource_type not in _RESOURCE_TYPES:
            raise ValueError("unsupported interaction resource type")
        now = timestamp()
        with self.store.transaction() as connection:
            existing = connection.execute(
                "SELECT * FROM interaction_resources WHERE resource_key=?", (resource_key,)
            ).fetchone()
            if existing is not None:
                if existing["resource_type"] != resource_type or existing["scope_id"] != scope_id:
                    raise ValueError("interaction resource identity is immutable")
                return dict(existing)
            connection.execute(
                "INSERT INTO interaction_resources(resource_key,resource_type,scope_id,"
                "created_at,updated_at) VALUES (?,?,?,?,?)",
                (resource_key, resource_type, scope_id, now, now),
            )
            _event(
                self.store,
                connection,
                kind="uiResourceRegistered",
                entity_type="interactionResource",
                entity_id=resource_key,
                summary="Interaction resource registered",
                payload={"resourceKey": resource_key, "resourceType": resource_type},
            )
        return self.get(resource_key)

    def get(self, resource_key: str) -> dict[str, Any]:
        with self.store.connect() as connection:
            row = connection.execute(
                "SELECT * FROM interaction_resources WHERE resource_key=?", (resource_key,)
            ).fetchone()
        if row is None:
            raise KeyError(resource_key)
        return dict(row)

    def acquire(
        self,
        resource_keys: Iterable[str],
        *,
        owner_id: str,
        task_id: str | None = None,
        run_id: str | None = None,
        ttl_seconds: float = 30.0,
        actor: str = "interaction-executor",
    ) -> ResourceLeaseBundle | None:
        keys = tuple(sorted(dict.fromkeys(resource_keys)))
        if not keys:
            raise ValueError("an interaction execution must acquire at least one resource")
        if any(not _SEMANTIC_ID.fullmatch(key) for key in keys):
            raise ValueError("resource keys must be semantic identities")
        if not _SEMANTIC_ID.fullmatch(owner_id):
            raise ValueError("resource lease owner must be a semantic identity")
        if ttl_seconds < 1:
            raise ValueError("resource lease TTL must be at least one second")
        now_value = datetime.now(UTC).replace(microsecond=0)
        now = timestamp(now_value)
        expires_value = now_value + timedelta(seconds=ttl_seconds)
        expires = timestamp(expires_value)
        group_id = f"ui-lease-{uuid.uuid4()}"
        with self.store.transaction() as connection:
            canonical_task_id = task_id
            run_launch_generation: int | None = None
            if run_id is not None:
                run = connection.execute(
                    "SELECT run.task_id,job.launch_generation FROM worker_runs run "
                    "LEFT JOIN provider_jobs job ON job.run_id=run.id WHERE run.id=?",
                    (run_id,),
                ).fetchone()
                if run is None:
                    raise KeyError(run_id)
                run_task_id = str(run["task_id"])
                if run["launch_generation"] is not None:
                    run_launch_generation = int(run["launch_generation"])
                if canonical_task_id is not None and canonical_task_id != run_task_id:
                    raise ValueError("interaction resource run does not belong to task")
                canonical_task_id = run_task_id
            task_lease_owner_id: str | None = None
            task_lease_generation: int | None = None
            if canonical_task_id is not None:
                task = connection.execute(
                    "SELECT id FROM tasks WHERE id=?", (canonical_task_id,)
                ).fetchone()
                if task is None:
                    raise KeyError(canonical_task_id)
                task_lease = connection.execute(
                    "SELECT owner_id,generation,state,expires_at "
                    "FROM task_execution_leases WHERE task_id=?",
                    (canonical_task_id,),
                ).fetchone()
                if task_lease is not None:
                    if task_lease["state"] != "active" or task_lease["expires_at"] <= now:
                        raise RuntimeError("interaction execution task lease is no longer current")
                    task_lease_owner_id = str(task_lease["owner_id"])
                    task_lease_generation = int(task_lease["generation"])
                    if (
                        run_launch_generation is not None
                        and run_launch_generation != task_lease_generation
                    ):
                        raise RuntimeError(
                            "interaction run belongs to a stale task execution generation"
                        )
            placeholders = ",".join("?" for _ in keys)
            resources = connection.execute(
                f"SELECT resource_key FROM interaction_resources "
                f"WHERE resource_key IN ({placeholders})",
                keys,
            ).fetchall()
            if {str(row["resource_key"]) for row in resources} != set(keys):
                missing = sorted(set(keys) - {str(row["resource_key"]) for row in resources})
                raise KeyError(",".join(missing))
            active = connection.execute(
                f"SELECT resource_key,owner_id FROM interaction_resource_leases "
                f"WHERE resource_key IN ({placeholders}) AND state='active' LIMIT 1",
                keys,
            ).fetchone()
            if active is not None:
                return None
            generations: list[tuple[str, int]] = []
            for key in keys:
                row = connection.execute(
                    "SELECT COALESCE(MAX(generation),0) AS generation "
                    "FROM interaction_resource_leases WHERE resource_key=?",
                    (key,),
                ).fetchone()
                generation = int(row["generation"]) + 1
                generations.append((key, generation))
                connection.execute(
                    "INSERT INTO interaction_resource_leases(lease_id,resource_key,"
                    "lease_group_id,owner_id,task_id,run_id,task_lease_owner_id,"
                    "task_lease_generation,generation,state,acquired_at,heartbeat_at,"
                    "expires_at,released_at) "
                    "VALUES (?,?,?,?,?,?,?,?,?,'active',?,?,?,NULL)",
                    (
                        group_id,
                        key,
                        group_id,
                        owner_id,
                        canonical_task_id,
                        run_id,
                        task_lease_owner_id,
                        task_lease_generation,
                        generation,
                        now,
                        now,
                        expires,
                    ),
                )
            _event(
                self.store,
                connection,
                kind="uiResourceAcquired",
                entity_type="interactionLease",
                entity_id=group_id,
                summary="Interaction resource bundle acquired",
                payload={
                    "leaseGroupID": group_id,
                    "ownerID": owner_id,
                    "resourceKeys": list(keys),
                    "expiresAt": expires,
                },
                task_id=canonical_task_id,
                run_id=run_id,
                actor=actor,
            )
        return ResourceLeaseBundle(
            group_id,
            owner_id,
            tuple(generations),
            expires_value,
            canonical_task_id,
            task_lease_owner_id,
            task_lease_generation,
        )

    def recover_expired(
        self,
        lease_group_id: str,
        *,
        quiescence_confirmed: bool,
        confirmation_actor: str,
    ) -> bool:
        """Release an expired bundle only after its external UI authority is quiescent.

        TTL expiry fences the old bundle but deliberately does not transfer mouse/keyboard/UI
        authority.  A recovery controller must first establish that the former executor can no
        longer perform side effects, then record that confirmation here before a replacement may
        acquire the resources.
        """

        if not _SEMANTIC_ID.fullmatch(lease_group_id):
            raise ValueError("resource lease group must be a semantic identity")
        if not quiescence_confirmed:
            raise ValueError("expired resource recovery requires quiescence confirmation")
        if not _SEMANTIC_ID.fullmatch(confirmation_actor):
            raise ValueError("resource recovery actor must be a semantic identity")
        now = timestamp()
        with self.store.transaction() as connection:
            rows = connection.execute(
                "SELECT resource_key,state,expires_at,task_id,run_id "
                "FROM interaction_resource_leases WHERE lease_group_id=? "
                "ORDER BY resource_key",
                (lease_group_id,),
            ).fetchall()
            if not rows:
                raise KeyError(lease_group_id)
            active = [row for row in rows if row["state"] == "active"]
            if not active:
                return False
            if len(active) != len(rows):
                raise RuntimeError("interaction resource bundle has a mixed terminal state")
            if any(row["expires_at"] > now for row in active):
                raise RuntimeError("interaction resource bundle has not expired")
            cursor = connection.execute(
                "UPDATE interaction_resource_leases SET state='expired',released_at=? "
                "WHERE lease_group_id=? AND state='active'",
                (now, lease_group_id),
            )
            if cursor.rowcount != len(rows):
                raise RuntimeError("expired interaction resource recovery was not atomic")
            first = rows[0]
            _event(
                self.store,
                connection,
                kind="uiResourceRecoveryConfirmed",
                entity_type="interactionLease",
                entity_id=lease_group_id,
                summary="Expired interaction resource bundle confirmed quiescent",
                payload={
                    "leaseGroupID": lease_group_id,
                    "resourceKeys": [str(row["resource_key"]) for row in rows],
                    "confirmationActor": confirmation_actor,
                },
                task_id=first["task_id"],
                run_id=first["run_id"],
                actor=confirmation_actor,
            )
        return True

    def renew(self, bundle: ResourceLeaseBundle, *, ttl_seconds: float = 30.0) -> bool:
        if ttl_seconds < 1:
            raise ValueError("resource lease TTL must be at least one second")
        now_value = datetime.now(UTC).replace(microsecond=0)
        now = timestamp(now_value)
        expires_value = now_value + timedelta(seconds=ttl_seconds)
        expires = timestamp(expires_value)
        with self.store.transaction() as connection:
            if not self._require_bundle(connection, bundle, now=now, raise_error=False):
                return False
            cursor = connection.execute(
                "UPDATE interaction_resource_leases SET heartbeat_at=?,expires_at=? "
                "WHERE lease_group_id=? AND owner_id=? AND state='active'",
                (now, expires, bundle.lease_group_id, bundle.owner_id),
            )
            if cursor.rowcount != len(bundle.resources):
                return False
        return True

    def is_current(self, bundle: ResourceLeaseBundle) -> bool:
        with self.store.connect() as connection:
            return self._require_bundle(connection, bundle, now=timestamp(), raise_error=False)

    def assert_current(self, connection: Any, bundle: ResourceLeaseBundle) -> None:
        self._require_bundle(connection, bundle, now=timestamp(), raise_error=True)

    def release(self, bundle: ResourceLeaseBundle, *, actor: str = "interaction-executor") -> bool:
        now = timestamp()
        with self.store.transaction() as connection:
            if not self._require_bundle(connection, bundle, now=now, raise_error=False):
                return False
            cursor = connection.execute(
                "UPDATE interaction_resource_leases SET state='released',released_at=? "
                "WHERE lease_group_id=? AND owner_id=? AND state='active'",
                (now, bundle.lease_group_id, bundle.owner_id),
            )
            if cursor.rowcount != len(bundle.resources):
                raise RuntimeError("interaction resource bundle release was not atomic")
            first = connection.execute(
                "SELECT task_id,run_id FROM interaction_resource_leases "
                "WHERE lease_group_id=? LIMIT 1",
                (bundle.lease_group_id,),
            ).fetchone()
            _event(
                self.store,
                connection,
                kind="uiResourceReleased",
                entity_type="interactionLease",
                entity_id=bundle.lease_group_id,
                summary="Interaction resource bundle released",
                payload={
                    "leaseGroupID": bundle.lease_group_id,
                    "resourceKeys": [key for key, _ in bundle.resources],
                },
                task_id=first["task_id"] if first is not None else None,
                run_id=first["run_id"] if first is not None else None,
                actor=actor,
            )
        return True

    @staticmethod
    def _require_bundle(
        connection: Any,
        bundle: ResourceLeaseBundle,
        *,
        now: str,
        raise_error: bool,
    ) -> bool:
        rows = connection.execute(
            "SELECT resource_key,generation,state,expires_at,task_id,task_lease_owner_id,"
            "task_lease_generation FROM interaction_resource_leases "
            "WHERE lease_group_id=? AND owner_id=? ORDER BY resource_key",
            (bundle.lease_group_id, bundle.owner_id),
        ).fetchall()
        expected = {key: generation for key, generation in bundle.resources}
        current = {
            str(row["resource_key"]): int(row["generation"])
            for row in rows
            if row["state"] == "active" and row["expires_at"] > now
        }
        lease_bindings = {
            (
                str(row["task_id"]) if row["task_id"] is not None else None,
                str(row["task_lease_owner_id"]) if row["task_lease_owner_id"] is not None else None,
                int(row["task_lease_generation"])
                if row["task_lease_generation"] is not None
                else None,
            )
            for row in rows
        }
        expected_binding = (
            bundle.task_id,
            bundle.task_lease_owner_id,
            bundle.task_lease_generation,
        )
        valid = current == expected and lease_bindings == {expected_binding}
        if valid and bundle.task_lease_owner_id is not None:
            task_lease = connection.execute(
                "SELECT owner_id,generation,state,expires_at FROM task_execution_leases "
                "WHERE task_id=?",
                (bundle.task_id,),
            ).fetchone()
            valid = bool(
                task_lease is not None
                and task_lease["owner_id"] == bundle.task_lease_owner_id
                and int(task_lease["generation"]) == bundle.task_lease_generation
                and task_lease["state"] == "active"
                and task_lease["expires_at"] > now
            )
        if not valid and raise_error:
            raise RuntimeError("interaction resource lease generation is no longer current")
        return valid


def safe_snapshot(snapshot: UISnapshot) -> dict[str, Any]:
    """Persist only semantic UI structure; omit values, text bodies, screenshots, and geometry."""

    sources = sorted({element.source.value for element in snapshot.elements})
    return {
        "snapshotID": snapshot.snapshot_id,
        "appID": snapshot.app_id,
        "windowID": snapshot.window_id,
        "source": sources[0] if len(sources) == 1 else "hybrid",
        "focusElementID": snapshot.focus_element_id,
        "elements": [
            {
                "elementID": element.element_id,
                "stableID": element.element_id,
                "role": element.role,
                "enabled": element.enabled,
                "visible": element.visible,
                "actions": sorted(element.actions),
                "semanticActions": sorted(element.semantic_actions),
                "confidence": element.confidence,
                "source": element.source.value,
            }
            for element in sorted(snapshot.elements, key=lambda item: item.element_id)
        ],
    }


def semantic_snapshot_state(snapshot: UISnapshot) -> dict[str, Any]:
    """Stable graph identity that excludes observation IDs, timestamps and parser confidence."""

    return {
        "appID": snapshot.app_id,
        "windowID": snapshot.window_id,
        "focusElementID": snapshot.focus_element_id,
        "elements": [
            {
                "elementID": element.element_id,
                "role": element.role,
                "enabled": element.enabled,
                "visible": element.visible,
                "actions": sorted(element.actions),
                "semanticActions": sorted(element.semantic_actions),
            }
            for element in sorted(snapshot.elements, key=lambda item: item.element_id)
        ],
    }


def semantic_snapshot_sha256(snapshot: UISnapshot) -> str:
    return _sha256(semantic_snapshot_state(snapshot))


def safe_plan(plan: UIPlan) -> dict[str, Any]:
    return {
        "schemaVersion": plan.schema_version,
        "planID": plan.plan_id,
        "semanticGoal": plan.semantic_goal,
        "milestones": [
            {
                "milestoneID": milestone.milestone_id,
                "actions": [
                    {
                        "actionID": action.action_id,
                        "kind": action.kind.value,
                        "semanticAction": action.semantic_action,
                        "risk": action.risk.value,
                        "locator": {
                            "semanticAction": action.locator.semantic_action,
                            "stableID": action.locator.stable_id,
                            "role": action.locator.role,
                            # Element names and typed text are intentionally not persisted here.
                            "hasRoleName": bool(action.locator.role and action.locator.name),
                        },
                    }
                    for action in milestone.actions
                ],
            }
            for milestone in plan.milestones
        ],
    }


class InteractionRepository:
    def __init__(self, store: StateStore, resources: InteractionResourceRepository) -> None:
        self.store = store
        self.resources = resources

    def start_execution(
        self,
        *,
        execution_id: str,
        plan: UIPlan,
        bundle: ResourceLeaseBundle,
        adapter_kind: str,
        channel: str,
        app_id: str,
        app_version: str | None,
        project_id: str | None = None,
        task_id: str | None = None,
        run_id: str | None = None,
        worker_id: str | None = None,
    ) -> dict[str, Any]:
        if not _SEMANTIC_ID.fullmatch(execution_id) or not _SEMANTIC_ID.fullmatch(app_id):
            raise ValueError("interaction execution and app identities must be semantic")
        plan_value = safe_plan(plan)
        plan_sha256 = _sha256(plan_value)
        now = timestamp()
        with self.store.transaction() as connection:
            self.resources.assert_current(connection, bundle)
            if run_id is not None:
                run = connection.execute(
                    "SELECT run.task_id,run.worker_id,task.project_id "
                    "FROM worker_runs run JOIN tasks task ON task.id=run.task_id "
                    "WHERE run.id=?",
                    (run_id,),
                ).fetchone()
                if run is None:
                    raise KeyError(run_id)
                if task_id != run["task_id"] or worker_id != run["worker_id"]:
                    raise ValueError(
                        "interaction execution provenance does not match its Worker run"
                    )
                if project_id is not None and project_id != run["project_id"]:
                    raise ValueError("interaction execution project does not match its Task")
                project_id = str(run["project_id"])
            if task_id is not None:
                uncertain = connection.execute(
                    "SELECT 1 FROM interaction_executions prior "
                    "JOIN interaction_action_checkpoints started "
                    "ON started.execution_id=prior.id "
                    "WHERE prior.task_id=? AND prior.plan_sha256=? "
                    "AND started.phase IN ('actionStarted','outcomeUnknown') "
                    "AND (started.phase='outcomeUnknown' OR NOT EXISTS ("
                    "SELECT 1 FROM interaction_action_checkpoints terminal "
                    "WHERE terminal.execution_id=started.execution_id "
                    "AND terminal.action_ordinal=started.action_ordinal "
                    "AND terminal.phase IN ('postconditionVerified','failed','outcomeUnknown')"
                    ")) LIMIT 1",
                    (task_id, plan_sha256),
                ).fetchone()
                if uncertain is not None:
                    raise RuntimeError(
                        "prior semantic UI action outcome is unknown; blind replay is forbidden"
                    )
            connection.execute(
                "INSERT INTO interaction_executions(id,project_id,task_id,run_id,worker_id,"
                "adapter_kind,channel,app_id,app_version,plan_schema,plan_sha256,state,"
                "observation_count,grounding_count,started_at,updated_at,finished_at) "
                "VALUES (?,?,?,?,?,?,?,?,?,?,?,'planned',0,0,?,?,NULL)",
                (
                    execution_id,
                    project_id,
                    task_id,
                    run_id,
                    worker_id,
                    adapter_kind,
                    channel,
                    app_id,
                    app_version,
                    plan.schema_version,
                    plan_sha256,
                    now,
                    now,
                ),
            )
            _event(
                self.store,
                connection,
                kind="uiExecutionStarted",
                entity_type="interactionExecution",
                entity_id=execution_id,
                summary="Semantic UI execution started",
                payload={
                    "executionID": execution_id,
                    "planID": plan.plan_id,
                    "channel": channel,
                    "appID": app_id,
                },
                project_id=project_id,
                task_id=task_id,
                worker_id=worker_id,
                run_id=run_id,
            )
        return self.get_execution(execution_id)

    def record_action_checkpoint(
        self,
        execution_id: str,
        *,
        action_ordinal: int,
        phase: str,
        action: UIAction,
        bundle: ResourceLeaseBundle,
        snapshot: UISnapshot | None = None,
        detail_code: str | None = None,
    ) -> dict[str, Any]:
        phases = {
            "observed",
            "grounded",
            "preconditionChecked",
            "actionStarted",
            "actionReturned",
            "postconditionVerified",
            "failed",
            "outcomeUnknown",
        }
        if phase not in phases or action_ordinal < 0:
            raise ValueError("interaction checkpoint phase or ordinal is invalid")
        if detail_code is not None and not _SEMANTIC_ID.fullmatch(detail_code):
            raise ValueError("interaction checkpoint detail must be a semantic code")
        snapshot_sha256 = semantic_snapshot_sha256(snapshot) if snapshot is not None else None
        resource_generation = max(generation for _, generation in bundle.resources)
        with self.store.transaction() as connection:
            self.resources.assert_current(connection, bundle)
            execution = connection.execute(
                "SELECT * FROM interaction_executions WHERE id=?", (execution_id,)
            ).fetchone()
            if execution is None:
                raise KeyError(execution_id)
            existing = connection.execute(
                "SELECT * FROM interaction_action_checkpoints WHERE execution_id=? "
                "AND action_ordinal=? AND phase=?",
                (execution_id, action_ordinal, phase),
            ).fetchone()
            expected = (
                bundle.task_lease_owner_id,
                bundle.task_lease_generation,
                bundle.lease_group_id,
                resource_generation,
                snapshot_sha256,
                detail_code,
            )
            if existing is not None:
                actual = (
                    existing["task_lease_owner_id"],
                    existing["task_lease_generation"],
                    existing["resource_lease_group_id"],
                    existing["resource_lease_generation"],
                    existing["snapshot_sha256"],
                    existing["detail_code"],
                )
                if actual != expected:
                    raise RuntimeError(
                        "interaction checkpoint replay conflicts with prior evidence"
                    )
                return dict(existing)
            recorded_phases = {
                str(row["phase"])
                for row in connection.execute(
                    "SELECT phase FROM interaction_action_checkpoints WHERE execution_id=? "
                    "AND action_ordinal=?",
                    (execution_id, action_ordinal),
                ).fetchall()
            }
            terminal_phases = {"postconditionVerified", "failed", "outcomeUnknown"}
            if recorded_phases.intersection(terminal_phases):
                raise RuntimeError("terminal interaction action cannot accept another checkpoint")
            required_predecessor = {
                "observed": None,
                "grounded": "observed",
                "preconditionChecked": "grounded",
                "actionStarted": "preconditionChecked",
                "actionReturned": "actionStarted",
                "postconditionVerified": "actionReturned",
            }
            if phase == "outcomeUnknown":
                raise PermissionError(
                    "outcomeUnknown requires explicit quiescence recovery authority"
                )
            predecessor = required_predecessor.get(phase)
            if phase != "failed" and (
                (predecessor is None and recorded_phases)
                or (predecessor is not None and predecessor not in recorded_phases)
            ):
                raise RuntimeError("interaction checkpoint phase is out of order")
            identity = f"ui-checkpoint-{uuid.uuid4()}"
            connection.execute(
                "INSERT INTO interaction_action_checkpoints(id,execution_id,action_ordinal,"
                "phase,task_lease_owner_id,task_lease_generation,resource_lease_group_id,"
                "resource_lease_generation,snapshot_sha256,detail_code,created_at) "
                "VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                (
                    identity,
                    execution_id,
                    action_ordinal,
                    phase,
                    bundle.task_lease_owner_id,
                    bundle.task_lease_generation,
                    bundle.lease_group_id,
                    resource_generation,
                    snapshot_sha256,
                    detail_code,
                    timestamp(),
                ),
            )
            _event(
                self.store,
                connection,
                kind="uiActionCheckpointed",
                entity_type="interactionActionCheckpoint",
                entity_id=identity,
                summary="Semantic UI action phase durably checkpointed",
                payload={
                    "executionID": execution_id,
                    "actionID": action.action_id,
                    "actionOrdinal": action_ordinal,
                    "phase": phase,
                    "detailCode": detail_code,
                },
                project_id=execution["project_id"],
                task_id=execution["task_id"],
                worker_id=execution["worker_id"],
                run_id=execution["run_id"],
            )
            return dict(
                connection.execute(
                    "SELECT * FROM interaction_action_checkpoints WHERE id=?", (identity,)
                ).fetchone()
            )

    def recover_incomplete_actions(
        self,
        execution_id: str,
        *,
        quiescence_confirmed: bool,
        confirmation_actor: str,
    ) -> int:
        if not quiescence_confirmed:
            raise ValueError("interaction recovery requires quiescence confirmation")
        if not _SEMANTIC_ID.fullmatch(confirmation_actor):
            raise ValueError("interaction recovery actor must be a semantic identity")
        with self.store.transaction() as connection:
            execution = connection.execute(
                "SELECT * FROM interaction_executions WHERE id=?", (execution_id,)
            ).fetchone()
            if execution is None:
                raise KeyError(execution_id)
            incomplete = connection.execute(
                "SELECT started.* FROM interaction_action_checkpoints started "
                "WHERE started.execution_id=? AND started.phase='actionStarted' "
                "AND NOT EXISTS (SELECT 1 FROM interaction_action_checkpoints terminal "
                "WHERE terminal.execution_id=started.execution_id "
                "AND terminal.action_ordinal=started.action_ordinal "
                "AND terminal.phase IN ('postconditionVerified','failed','outcomeUnknown')) "
                "ORDER BY started.action_ordinal",
                (execution_id,),
            ).fetchall()
            if not incomplete:
                return 0
            now = timestamp()
            for started in incomplete:
                connection.execute(
                    "INSERT INTO interaction_action_checkpoints(id,execution_id,action_ordinal,"
                    "phase,task_lease_owner_id,task_lease_generation,resource_lease_group_id,"
                    "resource_lease_generation,snapshot_sha256,detail_code,created_at) "
                    "VALUES (?,?,?,'outcomeUnknown',?,?,?,?,?,'interaction.processInterrupted',?)",
                    (
                        f"ui-checkpoint-{uuid.uuid4()}",
                        execution_id,
                        int(started["action_ordinal"]),
                        started["task_lease_owner_id"],
                        started["task_lease_generation"],
                        started["resource_lease_group_id"],
                        started["resource_lease_generation"],
                        started["snapshot_sha256"],
                        now,
                    ),
                )
            connection.execute(
                "UPDATE interaction_executions SET state='escalated',updated_at=?,finished_at=? "
                "WHERE id=?",
                (now, now, execution_id),
            )
            _event(
                self.store,
                connection,
                kind="uiActionOutcomeUnknown",
                entity_type="interactionExecution",
                entity_id=execution_id,
                summary="Interrupted semantic UI action requires operator reconciliation",
                payload={
                    "executionID": execution_id,
                    "actionOrdinals": [int(row["action_ordinal"]) for row in incomplete],
                    "confirmationActor": confirmation_actor,
                },
                project_id=execution["project_id"],
                task_id=execution["task_id"],
                worker_id=execution["worker_id"],
                run_id=execution["run_id"],
                severity=EventSeverity.WARNING,
                actor=confirmation_actor,
            )
            return len(incomplete)

    def record_plan_result(
        self,
        execution_id: str,
        result: UIPlanResult,
        *,
        bundle: ResourceLeaseBundle,
    ) -> str:
        actions = tuple(action for milestone in result.milestones for action in milestone.actions)
        if not actions:
            raise ValueError("a successful UI plan result must contain actions")
        with self.store.transaction() as connection:
            self.resources.assert_current(connection, bundle)
            execution = connection.execute(
                "SELECT * FROM interaction_executions WHERE id=?", (execution_id,)
            ).fetchone()
            if execution is None:
                raise KeyError(execution_id)
            required_phases = {
                "observed",
                "grounded",
                "preconditionChecked",
                "actionStarted",
                "actionReturned",
                "postconditionVerified",
            }
            for ordinal in range(len(actions)):
                phases = {
                    str(row["phase"])
                    for row in connection.execute(
                        "SELECT phase FROM interaction_action_checkpoints WHERE execution_id=? "
                        "AND action_ordinal=?",
                        (execution_id, ordinal),
                    ).fetchall()
                }
                if phases != required_phases:
                    raise RuntimeError(
                        "interaction trajectory requires a complete verified checkpoint chain"
                    )
            for ordinal, action_result in enumerate(actions):
                before_id = self._insert_snapshot(
                    connection, execution, action_result.before_snapshot
                )
                after_id = self._insert_snapshot(
                    connection, execution, action_result.after_snapshot
                )
                self._insert_action(
                    connection,
                    execution,
                    ordinal,
                    action_result,
                    before_id=before_id,
                    after_id=after_id,
                )
            now = timestamp()
            connection.execute(
                "UPDATE interaction_executions SET state='succeeded',observation_count=?,"
                "grounding_count=?,updated_at=?,finished_at=? WHERE id=?",
                (len(actions) * 2, len(actions), now, now, execution_id),
            )
            trajectory_id = f"trajectory-{uuid.uuid4()}"
            trajectory = {
                "schemaVersion": "interaction-trajectory/v1",
                "executionID": execution_id,
                "planID": result.plan_id,
                "verified": True,
                "actions": [
                    {
                        "actionID": item.action_id,
                        "elementID": item.grounding.element.element_id,
                        "strategy": item.grounding.strategy.value,
                        "confidence": item.grounding.confidence,
                        "route": item.route.value,
                        "semanticAction": item.semantic_action,
                        "risk": item.risk.value,
                        "beforeState": semantic_snapshot_sha256(item.before_snapshot),
                        "afterState": semantic_snapshot_sha256(item.after_snapshot),
                    }
                    for item in actions
                ],
            }
            connection.execute(
                "INSERT INTO interaction_trajectories(id,execution_id,schema_version,verified,"
                "trajectory_json,created_at) VALUES (?,?, 'interaction-trajectory/v1',1,?,?)",
                (trajectory_id, execution_id, compact_json(trajectory), now),
            )
            _event(
                self.store,
                connection,
                kind="trajectoryCompleted",
                entity_type="interactionTrajectory",
                entity_id=trajectory_id,
                summary="Verified semantic interaction trajectory captured",
                payload={
                    "trajectoryID": trajectory_id,
                    "executionID": execution_id,
                    "actionCount": len(actions),
                    "verified": True,
                },
                project_id=execution["project_id"],
                task_id=execution["task_id"],
                worker_id=execution["worker_id"],
                run_id=execution["run_id"],
            )
        return trajectory_id

    def fail_execution(
        self,
        execution_id: str,
        *,
        bundle: ResourceLeaseBundle,
        error_code: str,
        escalated: bool = False,
    ) -> None:
        if not _SEMANTIC_ID.fullmatch(error_code):
            raise ValueError("interaction failure must use a stable error code")
        state = "escalated" if escalated else "failed"
        with self.store.transaction() as connection:
            self.resources.assert_current(connection, bundle)
            execution = connection.execute(
                "SELECT * FROM interaction_executions WHERE id=?", (execution_id,)
            ).fetchone()
            if execution is None:
                raise KeyError(execution_id)
            now = timestamp()
            connection.execute(
                "UPDATE interaction_executions SET state=?,updated_at=?,finished_at=? WHERE id=?",
                (state, now, now, execution_id),
            )
            _event(
                self.store,
                connection,
                kind="uiActionFailed",
                entity_type="interactionExecution",
                entity_id=execution_id,
                summary="Semantic UI execution did not verify",
                payload={"executionID": execution_id, "errorCode": error_code, "state": state},
                project_id=execution["project_id"],
                task_id=execution["task_id"],
                worker_id=execution["worker_id"],
                run_id=execution["run_id"],
                severity=EventSeverity.WARNING,
            )

    def get_execution(self, execution_id: str) -> dict[str, Any]:
        with self.store.connect() as connection:
            row = connection.execute(
                "SELECT * FROM interaction_executions WHERE id=?", (execution_id,)
            ).fetchone()
        if row is None:
            raise KeyError(execution_id)
        return dict(row)

    def list_executions(self, *, limit: int = 100) -> list[dict[str, Any]]:
        if not 1 <= limit <= 500:
            raise ValueError("limit must be between 1 and 500")
        with self.store.connect() as connection:
            return [
                dict(row)
                for row in connection.execute(
                    "SELECT * FROM interaction_executions ORDER BY started_at DESC,id LIMIT ?",
                    (limit,),
                ).fetchall()
            ]

    def _insert_snapshot(self, connection: Any, execution: Any, snapshot: UISnapshot) -> str:
        safe = safe_snapshot(snapshot)
        state_sha256 = semantic_snapshot_sha256(snapshot)
        existing = connection.execute(
            "SELECT id FROM ui_snapshots WHERE execution_id=? AND state_sha256=? "
            "ORDER BY ordinal LIMIT 1",
            (execution["id"], state_sha256),
        ).fetchone()
        if existing is not None:
            return str(existing["id"])
        ordinal_row = connection.execute(
            "SELECT COALESCE(MAX(ordinal),-1)+1 AS ordinal FROM ui_snapshots WHERE execution_id=?",
            (execution["id"],),
        ).fetchone()
        ordinal = int(ordinal_row["ordinal"])
        previous = connection.execute(
            "SELECT id FROM ui_snapshots WHERE execution_id=? ORDER BY ordinal DESC LIMIT 1",
            (execution["id"],),
        ).fetchone()
        identity = f"ui-snapshot-{uuid.uuid4()}"
        connection.execute(
            "INSERT INTO ui_snapshots(id,execution_id,previous_snapshot_id,ordinal,app_id,"
            "window_id,source,state_sha256,safe_tree_json,focus_element_id,observed_at) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?)",
            (
                identity,
                execution["id"],
                previous["id"] if previous is not None else None,
                ordinal,
                snapshot.app_id,
                snapshot.window_id,
                safe["source"],
                state_sha256,
                compact_json(safe),
                snapshot.focus_element_id,
                timestamp(snapshot.timestamp),
            ),
        )
        _event(
            self.store,
            connection,
            kind="uiObserved",
            entity_type="uiSnapshot",
            entity_id=identity,
            summary="Semantic UI snapshot observed",
            payload={
                "executionID": execution["id"],
                "snapshotID": identity,
                "source": safe["source"],
                "elementCount": len(snapshot.elements),
            },
            project_id=execution["project_id"],
            task_id=execution["task_id"],
            worker_id=execution["worker_id"],
            run_id=execution["run_id"],
        )
        return identity

    def _insert_action(
        self,
        connection: Any,
        execution: Any,
        ordinal: int,
        result: UIActionResult,
        *,
        before_id: str,
        after_id: str,
    ) -> None:
        identity = f"ui-action-{uuid.uuid4()}"
        semantic_action = result.semantic_action
        connection.execute(
            "INSERT INTO interaction_actions(id,execution_id,ordinal,semantic_action,"
            "locator_json,grounding_confidence,channel,risk,precondition_json,"
            "postcondition_json,state,before_snapshot_id,after_snapshot_id,error_code,"
            "started_at,finished_at) VALUES (?,?,?,?,?,?,?,?,?,?,'verified',?,?,NULL,?,?)",
            (
                identity,
                execution["id"],
                ordinal,
                semantic_action,
                compact_json(
                    {
                        "strategy": result.grounding.strategy.value,
                        "elementID": result.grounding.element.element_id,
                    }
                ),
                result.grounding.confidence,
                execution["channel"],
                result.risk.value,
                compact_json({"kinds": list(result.precondition_kinds)}),
                compact_json({"kinds": list(result.postcondition_kinds)}),
                before_id,
                after_id,
                timestamp(),
                timestamp(),
            ),
        )
        _event(
            self.store,
            connection,
            kind="uiActionVerified",
            entity_type="interactionAction",
            entity_id=identity,
            summary="Semantic UI action postcondition verified",
            payload={
                "executionID": execution["id"],
                "semanticAction": semantic_action,
                "confidence": result.grounding.confidence,
                "strategy": result.grounding.strategy.value,
            },
            project_id=execution["project_id"],
            task_id=execution["task_id"],
            worker_id=execution["worker_id"],
            run_id=execution["run_id"],
        )


def _verified_trajectory_authority(connection: Any, trajectory_id: str) -> Any:
    """Return a trajectory only when current scoped independent verification proves it."""

    trajectory = connection.execute(
        "SELECT trajectory.*,execution.app_id,execution.app_version,execution.run_id,"
        "execution.state AS execution_state,run.state AS run_state,run.exit_code,run.attempt,"
        "run.verification_scope_id AS run_scope_id,"
        "run.task_definition_revision AS run_definition_revision,task.attempt_count,"
        "task.definition_revision,task.current_verification_scope_id,"
        "task.state AS task_state,result.run_id AS result_id "
        "FROM interaction_trajectories trajectory "
        "JOIN interaction_executions execution ON execution.id=trajectory.execution_id "
        "JOIN worker_runs run ON run.id=execution.run_id "
        "JOIN tasks task ON task.id=run.task_id "
        "LEFT JOIN worker_results result ON result.run_id=run.id WHERE trajectory.id=?",
        (trajectory_id,),
    ).fetchone()
    if (
        trajectory is None
        or not bool(trajectory["verified"])
        or trajectory["execution_state"] != "succeeded"
        or trajectory["run_state"] != "completed"
        or trajectory["exit_code"] != 0
        or trajectory["result_id"] is None
        or int(trajectory["attempt"]) != int(trajectory["attempt_count"])
        or trajectory["task_state"] != TaskState.SUCCEEDED.value
        or trajectory["current_verification_scope_id"] is None
        or trajectory["run_scope_id"] != trajectory["current_verification_scope_id"]
        or int(trajectory["run_definition_revision"]) != int(trajectory["definition_revision"])
    ):
        raise ValueError(
            "learning requires an independently verified current canonical Worker result"
        )
    scope_id = str(trajectory["current_verification_scope_id"])
    expected = connection.execute(
        "SELECT COUNT(*) AS count FROM task_verification_scope_items WHERE scope_id=?",
        (scope_id,),
    ).fetchone()
    passed = connection.execute(
        "SELECT COUNT(DISTINCT scope_item_id) AS count FROM verifications "
        "WHERE task_id=(SELECT task_id FROM interaction_executions WHERE id=?) "
        "AND verification_scope_id=? AND source_attempt=? AND passed=1",
        (trajectory["execution_id"], scope_id, int(trajectory["attempt"])),
    ).fetchone()
    if int(expected["count"]) < 1 or int(passed["count"]) != int(expected["count"]):
        raise ValueError("learning requires complete current scoped verification evidence")
    return trajectory


class SkillRepository:
    """Conservative lifecycle: first verified trajectory is CANDIDATE, second is VALIDATED."""

    def __init__(self, store: StateStore) -> None:
        self.store = store

    def learn(
        self,
        *,
        trajectory_id: str,
        app_id: str,
        app_version: str,
        semantic_action: str,
        template: Mapping[str, Any],
    ) -> dict[str, Any]:
        safe_template = _bounded_mapping(template, maximum_bytes=32_768)
        now = timestamp()
        with self.store.transaction() as connection:
            trajectory = _verified_trajectory_authority(connection, trajectory_id)
            if trajectory["app_id"] != app_id or trajectory["app_version"] != app_version:
                raise ValueError("skill identity does not match its canonical trajectory app")
            trajectory_value = json.loads(trajectory["trajectory_json"])
            matching_actions = [
                item
                for item in trajectory_value.get("actions", [])
                if isinstance(item, dict) and item.get("semanticAction") == semantic_action
            ]
            if not matching_actions or any(
                item.get("elementID") != safe_template.get("stableID") for item in matching_actions
            ):
                raise ValueError("skill template is not proven by its canonical trajectory")
            existing = connection.execute(
                "SELECT * FROM semantic_skills WHERE app_id=? AND semantic_action=? "
                "ORDER BY revision DESC LIMIT 1",
                (app_id, semantic_action),
            ).fetchone()
            if existing is not None and existing["app_version_constraint"] != app_version:
                connection.execute(
                    "UPDATE semantic_skills SET lifecycle='stale',updated_at=? WHERE id=?",
                    (now, existing["id"]),
                )
                _event(
                    self.store,
                    connection,
                    kind="skillInvalidated",
                    entity_type="semanticSkill",
                    entity_id=existing["id"],
                    summary="Semantic skill invalidated by app version change",
                    payload={"skillID": existing["id"], "reasonCode": "APP_VERSION_CHANGED"},
                )
                existing = None
            if existing is None or existing["lifecycle"] in {"stale", "disabled"}:
                revision_row = connection.execute(
                    "SELECT COALESCE(MAX(revision),0)+1 AS revision FROM semantic_skills "
                    "WHERE app_id=? AND semantic_action=?",
                    (app_id, semantic_action),
                ).fetchone()
                skill_id = f"skill-{uuid.uuid4()}"
                connection.execute(
                    "INSERT INTO semantic_skills(id,app_id,semantic_action,"
                    "app_version_constraint,lifecycle,revision,template_json,success_count,"
                    "failure_count,confidence,source_trajectory_id,last_verified_at,created_at,"
                    "updated_at) VALUES (?,?,?,?,'candidate',?,?,0,0,0.6,?,NULL,?,?)",
                    (
                        skill_id,
                        app_id,
                        semantic_action,
                        app_version,
                        int(revision_row["revision"]),
                        compact_json(safe_template),
                        trajectory_id,
                        now,
                        now,
                    ),
                )
                existing_lifecycle = "candidate"
            else:
                skill_id = str(existing["id"])
                existing_lifecycle = str(existing["lifecycle"])
            connection.execute(
                "INSERT OR IGNORE INTO semantic_skill_evidence(skill_id,trajectory_id,outcome,"
                "verified,app_version,observed_at) VALUES (?,?,'success',1,?,?)",
                (skill_id, trajectory_id, app_version, now),
            )
            if connection.execute("SELECT changes()").fetchone()[0] == 0:
                row = connection.execute(
                    "SELECT * FROM semantic_skills WHERE id=?", (skill_id,)
                ).fetchone()
                return dict(row)
            success_row = connection.execute(
                "SELECT COUNT(*) AS count FROM semantic_skill_evidence "
                "WHERE skill_id=? AND outcome='success' AND verified=1",
                (skill_id,),
            ).fetchone()
            success_count = int(success_row["count"])
            lifecycle = (
                "active"
                if existing_lifecycle == "active"
                else "validated"
                if success_count >= 2
                else "candidate"
            )
            confidence = min(0.95, 0.6 + 0.15 * max(0, success_count - 1))
            connection.execute(
                "UPDATE semantic_skills SET lifecycle=?,success_count=?,confidence=?,"
                "last_verified_at=?,updated_at=? WHERE id=?",
                (lifecycle, success_count, confidence, now, now, skill_id),
            )
            kind = "skillValidated" if lifecycle == "validated" else "skillCandidateCreated"
            _event(
                self.store,
                connection,
                kind=kind,
                entity_type="semanticSkill",
                entity_id=skill_id,
                summary=f"Semantic skill {lifecycle}",
                payload={
                    "skillID": skill_id,
                    "semanticAction": semantic_action,
                    "appID": app_id,
                    "lifecycle": lifecycle,
                },
            )
        return self.get(skill_id)

    def activate(self, skill_id: str) -> dict[str, Any]:
        with self.store.transaction() as connection:
            row = connection.execute(
                "SELECT * FROM semantic_skills WHERE id=?", (skill_id,)
            ).fetchone()
            if row is None:
                raise KeyError(skill_id)
            if row["lifecycle"] != "validated":
                raise ValueError("only a validated skill may become active")
            connection.execute(
                "UPDATE semantic_skills SET lifecycle='active',updated_at=? WHERE id=?",
                (timestamp(), skill_id),
            )
            _event(
                self.store,
                connection,
                kind="skillActivated",
                entity_type="semanticSkill",
                entity_id=skill_id,
                summary="Validated semantic skill activated",
                payload={"skillID": skill_id, "lifecycle": "active"},
            )
        return self.get(skill_id)

    def invalidate(self, skill_id: str, *, disabled: bool = False) -> dict[str, Any]:
        lifecycle = "disabled" if disabled else "stale"
        with self.store.transaction() as connection:
            row = connection.execute(
                "SELECT * FROM semantic_skills WHERE id=?", (skill_id,)
            ).fetchone()
            if row is None:
                raise KeyError(skill_id)
            connection.execute(
                "UPDATE semantic_skills SET lifecycle=?,updated_at=? WHERE id=?",
                (lifecycle, timestamp(), skill_id),
            )
            _event(
                self.store,
                connection,
                kind="skillInvalidated",
                entity_type="semanticSkill",
                entity_id=skill_id,
                summary=f"Semantic skill marked {lifecycle}",
                payload={"skillID": skill_id, "lifecycle": lifecycle},
                severity=EventSeverity.WARNING,
            )
        return self.get(skill_id)

    def get(self, skill_id: str) -> dict[str, Any]:
        with self.store.connect() as connection:
            row = connection.execute(
                "SELECT * FROM semantic_skills WHERE id=?", (skill_id,)
            ).fetchone()
        if row is None:
            raise KeyError(skill_id)
        return dict(row)

    def list(self, *, limit: int = 100) -> list[dict[str, Any]]:
        if not 1 <= limit <= 500:
            raise ValueError("limit must be between 1 and 500")
        with self.store.connect() as connection:
            return [
                dict(row)
                for row in connection.execute(
                    "SELECT * FROM semantic_skills ORDER BY updated_at DESC,id LIMIT ?", (limit,)
                ).fetchall()
            ]


class UIGraphRepository:
    def __init__(self, store: StateStore) -> None:
        self.store = store

    def record_transition(
        self,
        *,
        app_id: str,
        app_version: str,
        before: UISnapshot,
        semantic_action: str,
        after: UISnapshot,
        verified: bool,
        confidence: float,
    ) -> dict[str, Any]:
        if verified:
            raise ValueError(
                "verified UI graph transitions require immutable trajectory/action evidence"
            )
        if not 0 <= confidence <= 1:
            raise ValueError("UI graph confidence must be between zero and one")
        before_hash = semantic_snapshot_sha256(before)
        after_hash = semantic_snapshot_sha256(after)
        now = timestamp()
        with self.store.transaction() as connection:
            connection.execute(
                "INSERT INTO ui_graph_edges(app_id,app_version,from_state_sha256,"
                "semantic_action,to_state_sha256,success_count,failure_count,last_verified_at,"
                "confidence,created_at,updated_at) VALUES (?,?,?,?,?,?,?,?,?,?,?) "
                "ON CONFLICT(app_id,app_version,from_state_sha256,semantic_action,to_state_sha256) "
                "DO UPDATE SET success_count=success_count+excluded.success_count,"
                "failure_count=failure_count+excluded.failure_count,"
                "last_verified_at=COALESCE(excluded.last_verified_at,last_verified_at),"
                "confidence=excluded.confidence,updated_at=excluded.updated_at",
                (
                    app_id,
                    app_version,
                    before_hash,
                    semantic_action,
                    after_hash,
                    int(verified),
                    int(not verified),
                    now if verified else None,
                    confidence,
                    now,
                    now,
                ),
            )
            _event(
                self.store,
                connection,
                kind="uiGraphTransitionRecorded",
                entity_type="uiGraphEdge",
                entity_id=f"{app_id}:{semantic_action}",
                summary="UI state transition recorded",
                payload={
                    "appID": app_id,
                    "semanticAction": semantic_action,
                    "verified": verified,
                    "confidence": confidence,
                },
            )
            row = connection.execute(
                "SELECT * FROM ui_graph_edges WHERE app_id=? AND app_version=? "
                "AND from_state_sha256=? AND semantic_action=? AND to_state_sha256=?",
                (app_id, app_version, before_hash, semantic_action, after_hash),
            ).fetchone()
        return dict(row)

    def record_verified_evidence(
        self,
        *,
        trajectory_id: str,
        action_ordinal: int,
        app_id: str,
        app_version: str,
        before_state_sha256: str,
        semantic_action: str,
        after_state_sha256: str,
        confidence: float,
    ) -> dict[str, Any]:
        """Project one immutable verified trajectory action into the aggregate UI graph once."""

        if action_ordinal < 0:
            raise ValueError("UI graph action ordinal must be non-negative")
        if not 0 <= confidence <= 1:
            raise ValueError("UI graph confidence must be between zero and one")
        if any(
            len(value) != 64 or not all(character in "0123456789abcdef" for character in value)
            for value in (before_state_sha256, after_state_sha256)
        ):
            raise ValueError("UI graph state identity must be a lowercase SHA-256 digest")
        now = timestamp()
        with self.store.transaction() as connection:
            existing = connection.execute(
                "SELECT * FROM ui_graph_transition_evidence "
                "WHERE trajectory_id=? AND action_ordinal=?",
                (trajectory_id, action_ordinal),
            ).fetchone()
            expected = {
                "app_id": app_id,
                "app_version": app_version,
                "from_state_sha256": before_state_sha256,
                "semantic_action": semantic_action,
                "to_state_sha256": after_state_sha256,
                "verified": 1,
            }
            if existing is not None:
                if any(existing[key] != value for key, value in expected.items()):
                    raise RuntimeError("UI graph evidence replay conflicts with its trajectory")
                row = connection.execute(
                    "SELECT * FROM ui_graph_edges WHERE app_id=? AND app_version=? "
                    "AND from_state_sha256=? AND semantic_action=? AND to_state_sha256=?",
                    (
                        app_id,
                        app_version,
                        before_state_sha256,
                        semantic_action,
                        after_state_sha256,
                    ),
                ).fetchone()
                if row is None:
                    raise RuntimeError("UI graph evidence exists without its aggregate edge")
                return dict(row)
            _verified_trajectory_authority(connection, trajectory_id)
            action = connection.execute(
                "SELECT execution.app_id,execution.app_version,execution.task_id,execution.run_id,"
                "action.semantic_action,action.grounding_confidence,"
                "before_snapshot.state_sha256 AS before_state_sha256,"
                "after_snapshot.state_sha256 AS after_state_sha256 "
                "FROM interaction_trajectories trajectory "
                "JOIN interaction_executions execution ON execution.id=trajectory.execution_id "
                "JOIN interaction_actions action ON action.execution_id=execution.id "
                "JOIN ui_snapshots before_snapshot ON before_snapshot.id=action.before_snapshot_id "
                "JOIN ui_snapshots after_snapshot ON after_snapshot.id=action.after_snapshot_id "
                "WHERE trajectory.id=? AND action.ordinal=? AND action.state='verified'",
                (trajectory_id, action_ordinal),
            ).fetchone()
            if action is None or any(
                (
                    action["app_id"] != app_id,
                    action["app_version"] != app_version,
                    action["before_state_sha256"] != before_state_sha256,
                    action["semantic_action"] != semantic_action,
                    action["after_state_sha256"] != after_state_sha256,
                    float(action["grounding_confidence"]) != confidence,
                )
            ):
                raise ValueError("UI graph evidence does not match its canonical trajectory action")
            connection.execute(
                "INSERT INTO ui_graph_transition_evidence(trajectory_id,action_ordinal,app_id,"
                "app_version,from_state_sha256,semantic_action,to_state_sha256,verified,"
                "confidence,created_at) VALUES (?,?,?,?,?,?,?,1,?,?)",
                (
                    trajectory_id,
                    action_ordinal,
                    app_id,
                    app_version,
                    before_state_sha256,
                    semantic_action,
                    after_state_sha256,
                    confidence,
                    now,
                ),
            )
            connection.execute(
                "INSERT INTO ui_graph_edges(app_id,app_version,from_state_sha256,"
                "semantic_action,to_state_sha256,success_count,failure_count,last_verified_at,"
                "confidence,created_at,updated_at) VALUES (?,?,?,?,?,1,0,?,?,?,?) "
                "ON CONFLICT(app_id,app_version,from_state_sha256,semantic_action,to_state_sha256) "
                "DO UPDATE SET success_count=success_count+1,"
                "last_verified_at=excluded.last_verified_at,"
                "confidence=excluded.confidence,updated_at=excluded.updated_at",
                (
                    app_id,
                    app_version,
                    before_state_sha256,
                    semantic_action,
                    after_state_sha256,
                    now,
                    confidence,
                    now,
                    now,
                ),
            )
            _event(
                self.store,
                connection,
                kind="uiGraphTransitionRecorded",
                entity_type="uiGraphEdge",
                entity_id=f"{app_id}:{semantic_action}",
                summary="Verified UI state transition recorded",
                payload={
                    "appID": app_id,
                    "semanticAction": semantic_action,
                    "trajectoryID": trajectory_id,
                    "verified": True,
                    "confidence": confidence,
                },
                task_id=action["task_id"],
                run_id=action["run_id"],
            )
            row = connection.execute(
                "SELECT * FROM ui_graph_edges WHERE app_id=? AND app_version=? "
                "AND from_state_sha256=? AND semantic_action=? AND to_state_sha256=?",
                (
                    app_id,
                    app_version,
                    before_state_sha256,
                    semantic_action,
                    after_state_sha256,
                ),
            ).fetchone()
        return dict(row)

    def list(self, *, app_id: str | None = None, limit: int = 200) -> list[dict[str, Any]]:
        if not 1 <= limit <= 500:
            raise ValueError("limit must be between 1 and 500")
        query = "SELECT * FROM ui_graph_edges"
        parameters: tuple[Any, ...]
        if app_id is None:
            parameters = (limit,)
        else:
            query += " WHERE app_id=?"
            parameters = (app_id, limit)
        query += " ORDER BY updated_at DESC,app_id,semantic_action LIMIT ?"
        with self.store.connect() as connection:
            return [dict(row) for row in connection.execute(query, parameters).fetchall()]


def finalize_verified_interaction_learning(store: StateStore, task_id: str) -> tuple[str, ...]:
    """Compile Task trajectories only after independent canonical verification succeeds."""

    with store.connect() as connection:
        task = connection.execute("SELECT state FROM tasks WHERE id=?", (task_id,)).fetchone()
        if task is None:
            raise KeyError(task_id)
        if task["state"] != TaskState.SUCCEEDED.value:
            raise ValueError("interaction learning requires independently verified Task success")
        rows = connection.execute(
            "SELECT trajectory.id AS trajectory_id,execution.app_id,execution.app_version,"
            "action.ordinal,action.semantic_action,action.locator_json,"
            "action.grounding_confidence,before_snapshot.state_sha256 AS before_state_sha256,"
            "after_snapshot.state_sha256 AS after_state_sha256 "
            "FROM interaction_trajectories trajectory "
            "JOIN interaction_executions execution ON execution.id=trajectory.execution_id "
            "JOIN worker_runs run ON run.id=execution.run_id "
            "JOIN tasks task ON task.id=run.task_id "
            "JOIN interaction_actions action ON action.execution_id=execution.id "
            "JOIN ui_snapshots before_snapshot ON before_snapshot.id=action.before_snapshot_id "
            "JOIN ui_snapshots after_snapshot ON after_snapshot.id=action.after_snapshot_id "
            "WHERE execution.task_id=? AND execution.state='succeeded' "
            "AND trajectory.verified=1 AND action.state='verified' "
            "AND run.attempt=task.attempt_count "
            "AND run.task_definition_revision=task.definition_revision "
            "AND run.verification_scope_id=task.current_verification_scope_id "
            "ORDER BY trajectory.id,action.ordinal",
            (task_id,),
        ).fetchall()
    skills = SkillRepository(store)
    graph = UIGraphRepository(store)
    learned: set[str] = set()
    for row in rows:
        locator = json.loads(row["locator_json"])
        stable_id = locator.get("elementID") if isinstance(locator, dict) else None
        semantic_action = str(row["semantic_action"])
        graph.record_verified_evidence(
            trajectory_id=str(row["trajectory_id"]),
            action_ordinal=int(row["ordinal"]),
            app_id=str(row["app_id"]),
            app_version=str(row["app_version"]),
            before_state_sha256=str(row["before_state_sha256"]),
            semantic_action=semantic_action,
            after_state_sha256=str(row["after_state_sha256"]),
            confidence=float(row["grounding_confidence"]),
        )
        if not isinstance(stable_id, str) or not _SEMANTIC_ID.fullmatch(stable_id):
            continue
        skill = skills.learn(
            trajectory_id=str(row["trajectory_id"]),
            app_id=str(row["app_id"]),
            app_version=str(row["app_version"]),
            semantic_action=semantic_action,
            template={"stableID": stable_id, "semanticAction": semantic_action},
        )
        learned.add(str(skill["id"]))
    return tuple(sorted(learned))
