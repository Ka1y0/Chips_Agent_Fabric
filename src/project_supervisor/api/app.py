from __future__ import annotations

import asyncio
import ipaddress
import sqlite3
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from typing import Annotated, Any

from fastapi import Depends, FastAPI, HTTPException, Query, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import JSONResponse

from project_supervisor.domain import RunState
from project_supervisor.store import StateStore

from .projections import SEVERITY_RANK, SupervisorProjection
from .schemas import (
    APIError,
    EventsFrame,
    GoalCreateRequest,
    GoalPauseRequest,
    GoalResumeRequest,
    GoalSteerRequest,
    GoalStopRequest,
    KeepaliveFrame,
    SnapshotFrame,
    envelope,
)


@dataclass(frozen=True, slots=True)
class APISettings:
    bind_host: str = "127.0.0.1"
    allow_unauthenticated_loopback: bool = False
    required_scope: str = "observe:read"
    goal_control_scope: str = "goal:control"
    allow_goal_mutations: bool = False
    keepalive_seconds: float = 15.0
    poll_interval_seconds: float = 0.25
    stream_batch_size: int = 200
    recent_event_limit: int = 200

    def __post_init__(self) -> None:
        if self.keepalive_seconds <= 0 or self.keepalive_seconds > 30:
            raise ValueError("keepalive_seconds must be > 0 and <= 30")
        if self.poll_interval_seconds <= 0:
            raise ValueError("poll_interval_seconds must be > 0")
        if not 1 <= self.stream_batch_size <= 500:
            raise ValueError("stream_batch_size must be between 1 and 500")


def _is_loopback(host: str | None) -> bool:
    if host in {"localhost", "testclient"}:
        return True
    if not host:
        return False
    try:
        return ipaddress.ip_address(host).is_loopback
    except ValueError:
        return False


def _bearer(headers: Mapping[str, str]) -> str | None:
    authorization = headers.get("authorization")
    if not authorization:
        return None
    scheme, separator, value = authorization.partition(" ")
    if not separator or scheme.lower() != "bearer" or not value:
        return None
    return value


def _csv(value: str | None) -> tuple[str, ...]:
    return tuple(item.strip() for item in (value or "").split(",") if item.strip())


def create_app(
    store: StateStore,
    settings: APISettings | None = None,
    *,
    goal_service: Any | None = None,
    allow_unauthenticated_loopback: bool | None = None,
    bind_host: str | None = None,
) -> FastAPI:
    """Create the Cyber Office observation API and optional authenticated Goal controls.

    The keyword overrides keep local test/dev setup concise while APISettings remains the single
    explicit production configuration surface.
    """

    if settings is None:
        settings = APISettings(
            bind_host=bind_host or "127.0.0.1",
            allow_unauthenticated_loopback=(
                bool(allow_unauthenticated_loopback)
                if allow_unauthenticated_loopback is not None
                else False
            ),
        )
    elif allow_unauthenticated_loopback is not None or bind_host is not None:
        raise ValueError("pass APISettings or keyword overrides, not both")
    if goal_service is None:
        from project_supervisor.autonomy import GoalService

        goal_service = GoalService(store)
    projection = SupervisorProjection(store, goal_reader=goal_service)
    app = FastAPI(title="Project_Supervisor API", version="0.2.1")
    app.state.store = store
    app.state.api_settings = settings
    app.state.projection = projection
    app.state.goal_service = goal_service

    def loopback_dev_allowed(peer_host: str | None) -> bool:
        return (
            settings.allow_unauthenticated_loopback
            and _is_loopback(settings.bind_host)
            and _is_loopback(peer_host)
        )

    async def authorize(request: Request) -> None:
        if loopback_dev_allowed(request.client.host if request.client else None):
            return
        token = _bearer(request.headers)
        if token is None:
            raise HTTPException(status_code=401, detail="bearer token required")
        if not store.verify_api_token(token, settings.required_scope):
            raise HTTPException(status_code=403, detail="token invalid or insufficient scope")

    async def authorize_goal_control(request: Request) -> None:
        # The explicit unauthenticated-loopback development override applies to observation only.
        # Goal mutation always requires a scoped bearer, even from loopback.
        token = _bearer(request.headers)
        if token is None:
            raise HTTPException(status_code=401, detail="bearer token required for Goal control")
        if not store.verify_api_token(token, settings.goal_control_scope):
            raise HTTPException(status_code=403, detail="token lacks Goal control scope")
        if not settings.allow_goal_mutations:
            raise HTTPException(status_code=403, detail="Goal mutations are disabled")

    @app.exception_handler(HTTPException)
    async def http_error(_request: Request, error: HTTPException) -> JSONResponse:
        code = {
            401: "token_required",
            403: "insufficient_scope",
            404: "not_found",
            409: "goal_control_conflict",
        }.get(error.status_code, "invalid_request")
        if error.status_code == 404:
            detail = str(error.detail).lower()
            if detail.startswith("unknown goal:"):
                code = "goal_not_found"
            elif detail.startswith("unknown project:"):
                code = "project_not_found"
            elif detail.startswith("unknown run:"):
                code = "run_not_found"
            elif detail.startswith("unknown escalation:"):
                code = "escalation_not_found"
            elif detail.startswith("unknown task:"):
                code = "task_not_found"
            else:
                code = "not_found"
        elif error.status_code == 403 and "disabled" in str(error.detail).lower():
            code = "goal_mutations_disabled"
        body = APIError(code=code, message=str(error.detail)).model_dump(
            by_alias=True, exclude_none=True
        )
        return JSONResponse(status_code=error.status_code, content=body)

    @app.get("/v1/health")
    async def health() -> dict:
        """Credential-free liveness only; no node, worker, or project details."""

        return envelope(
            {
                "status": "ok",
                "apiVersion": "v1",
                "readOnly": not settings.allow_goal_mutations,
            }
        )

    @app.get("/v1/capabilities", dependencies=[Depends(authorize)])
    async def capabilities() -> dict:
        return envelope(
            {
                "observation": {
                    "rest": True,
                    "webSocket": True,
                    "resumeCursor": "eventSequence",
                },
                "resources": [
                    "status",
                    "nodes",
                    "workers",
                    "runs",
                    "executionEscalations",
                    "tasks",
                    "goals",
                    "telemetry",
                    "autonomousHosts",
                    "autonomousGoalLeases",
                    "resourceUsageSnapshots",
                    "resourceUsageAggregates",
                    "resourceEconomics",
                    "nodeRuntimeRecovery",
                    "events",
                ],
                "mutations": (
                    ["goal.create", "goal.pause", "goal.resume", "goal.steer", "goal.stop"]
                    if settings.allow_goal_mutations
                    else []
                ),
                "requiredScope": settings.required_scope,
                "goalControlScope": settings.goal_control_scope,
                "goalControls": {
                    "softPause": "finish in-flight work; stop new dispatch",
                    "hardPause": "request cancellation where supported; freeze durably",
                    "resume": "continue from canonical persisted state",
                    "steer": "journal guidance and re-evaluate while preserving valid work",
                    "stop": "terminate intentionally with a durable reason",
                },
                "safety": {
                    "readOnly": not settings.allow_goal_mutations,
                    "arbitraryShell": False,
                    "providerCredentialsAccepted": False,
                },
            }
        )

    @app.get("/v1/schemas", dependencies=[Depends(authorize)])
    async def schemas() -> dict:
        return envelope(
            {
                "openAPI": "/openapi.json",
                "protocol": "v1",
                "eventCursor": {"field": "sequence", "semantics": "exclusive"},
                "streamFrames": ["snapshot", "events", "keepalive"],
                "goalArtifact": "schemas/goal-v1.schema.json",
                "goalControlArtifact": "schemas/goal-control-v1.schema.json",
                "resourceSnapshotArtifact": "schemas/resource-usage-snapshot-v1.schema.json",
                "resourceAggregateArtifact": "schemas/resource-usage-aggregate-v1.schema.json",
                "resourceEconomicsArtifact": "schemas/resource-economics-v1.schema.json",
                "nodeRuntimeRecoveryArtifact": "schemas/node-runtime-recovery-v1.schema.json",
            }
        )

    @app.get("/v1/fabric/capabilities", dependencies=[Depends(authorize)])
    async def fabric_capabilities() -> dict:
        return envelope(
            {
                "protocolVersion": "fabric-v0.3-dev",
                "workers": await asyncio.to_thread(projection.fabric_capabilities),
            }
        )

    @app.get("/v1/fabric/routing", dependencies=[Depends(authorize)])
    async def fabric_routing(
        task_id: str | None = Query(default=None, alias="taskID"),
        limit: int = Query(default=100, ge=1, le=500),
    ) -> dict:
        return envelope(
            await asyncio.to_thread(projection.fabric_routing, task_id=task_id, limit=limit)
        )

    @app.get("/v1/fabric/execution-plane", dependencies=[Depends(authorize)])
    async def fabric_execution_plane(
        limit: int = Query(default=100, ge=1, le=500),
    ) -> dict:
        return envelope(
            {
                "protocolVersion": "execution-plane/v1",
                "workers": await asyncio.to_thread(
                    projection.execution_plane,
                    limit=limit,
                ),
            }
        )

    @app.get("/v1/fabric/authorizations", dependencies=[Depends(authorize)])
    async def fabric_authorizations(
        limit: int = Query(default=100, ge=1, le=500),
    ) -> dict:
        return envelope(
            {
                "protocolVersion": "authorization-envelope/v1",
                "authorizations": await asyncio.to_thread(
                    projection.fabric_authorizations,
                    limit=limit,
                ),
            }
        )

    @app.get("/v1/fabric/data-provenance", dependencies=[Depends(authorize)])
    async def fabric_data_provenance(
        limit: int = Query(default=100, ge=1, le=500),
    ) -> dict:
        return envelope(
            {
                "movements": await asyncio.to_thread(
                    projection.data_provenance,
                    limit=limit,
                )
            }
        )

    @app.get("/v1/fabric/provider-invocations", dependencies=[Depends(authorize)])
    async def fabric_provider_invocations(
        limit: int = Query(default=100, ge=1, le=500),
    ) -> dict:
        return envelope(
            {
                "invocations": await asyncio.to_thread(
                    projection.provider_invocations_v2,
                    limit=limit,
                )
            }
        )

    @app.get("/v1/fabric/hypotheses", dependencies=[Depends(authorize)])
    async def fabric_hypotheses(
        limit: int = Query(default=100, ge=1, le=500),
    ) -> dict:
        return envelope(
            {
                "hypothesisSets": await asyncio.to_thread(
                    projection.hypothesis_sets,
                    limit=limit,
                )
            }
        )

    @app.get("/v1/fabric/write-authorities", dependencies=[Depends(authorize)])
    async def fabric_write_authorities(
        limit: int = Query(default=100, ge=1, le=500),
    ) -> dict:
        return envelope(
            {
                "writeAuthorities": await asyncio.to_thread(
                    projection.project_write_authorities,
                    limit=limit,
                )
            }
        )

    @app.get("/v1/fabric/spawn-proposals", dependencies=[Depends(authorize)])
    async def fabric_spawn_proposals(
        goal_id: str | None = Query(default=None, alias="goalID"),
        parent_task_id: str | None = Query(default=None, alias="parentTaskID"),
        limit: int = Query(default=100, ge=1, le=500),
    ) -> dict:
        return envelope(
            await asyncio.to_thread(
                projection.spawn_proposals,
                goal_id=goal_id,
                parent_task_id=parent_task_id,
                limit=limit,
            )
        )

    @app.get("/v1/fabric/fusions", dependencies=[Depends(authorize)])
    async def fabric_fusions(
        task_id: str | None = Query(default=None, alias="taskID"),
        limit: int = Query(default=100, ge=1, le=500),
    ) -> dict:
        return envelope(
            await asyncio.to_thread(projection.fusion_decisions, task_id=task_id, limit=limit)
        )

    @app.get("/v1/interactions", dependencies=[Depends(authorize)])
    async def interactions(
        task_id: str | None = Query(default=None, alias="taskID"),
        state: str | None = None,
        limit: int = Query(default=100, ge=1, le=500),
    ) -> dict:
        allowed_states = {
            "planned",
            "observing",
            "grounding",
            "acting",
            "verifying",
            "succeeded",
            "failed",
            "escalated",
        }
        if state is not None and state not in allowed_states:
            raise HTTPException(status_code=400, detail="invalid interaction state")
        return envelope(
            await asyncio.to_thread(
                projection.interactions,
                task_id=task_id,
                state=state,
                limit=limit,
            )
        )

    @app.get("/v1/interaction-resources", dependencies=[Depends(authorize)])
    async def interaction_resources(
        resource_type: str | None = Query(default=None, alias="resourceType"),
        active_only: bool = Query(default=False, alias="activeOnly"),
        limit: int = Query(default=100, ge=1, le=500),
    ) -> dict:
        allowed_types = {
            "desktopSession",
            "browserContext",
            "window",
            "mouse",
            "keyboard",
            "clipboard",
            "display",
        }
        if resource_type is not None and resource_type not in allowed_types:
            raise HTTPException(status_code=400, detail="invalid interaction resource type")
        return envelope(
            await asyncio.to_thread(
                projection.interaction_resources,
                resource_type=resource_type,
                active_only=active_only,
                limit=limit,
            )
        )

    @app.get("/v1/skills", dependencies=[Depends(authorize)])
    async def semantic_skills(
        app_id: str | None = Query(default=None, alias="appID"),
        lifecycle: str | None = None,
        limit: int = Query(default=100, ge=1, le=500),
    ) -> dict:
        allowed_lifecycles = {
            "observed",
            "candidate",
            "validated",
            "active",
            "stale",
            "disabled",
        }
        if lifecycle is not None and lifecycle not in allowed_lifecycles:
            raise HTTPException(status_code=400, detail="invalid skill lifecycle")
        return envelope(
            await asyncio.to_thread(
                projection.semantic_skills,
                app_id=app_id,
                lifecycle=lifecycle,
                limit=limit,
            )
        )

    @app.get("/v1/ui-graph", dependencies=[Depends(authorize)])
    async def ui_graph(
        app_id: str | None = Query(default=None, alias="appID"),
        limit: int = Query(default=200, ge=1, le=500),
    ) -> dict:
        return envelope(await asyncio.to_thread(projection.ui_graph, app_id=app_id, limit=limit))

    @app.get("/v1/status", dependencies=[Depends(authorize)])
    async def status() -> dict:
        return envelope(await asyncio.to_thread(projection.status))

    @app.get("/v1/nodes", dependencies=[Depends(authorize)])
    async def nodes() -> dict:
        return envelope(await asyncio.to_thread(projection.nodes))

    @app.get("/v1/nodes/recovery", dependencies=[Depends(authorize)])
    async def node_recovery(
        node_id: str | None = Query(default=None, alias="nodeID"),
    ) -> dict:
        return envelope(await asyncio.to_thread(projection.node_recovery, node_id=node_id))

    @app.get("/v1/workers", dependencies=[Depends(authorize)])
    async def workers() -> dict:
        return envelope(await asyncio.to_thread(projection.workers))

    @app.get("/v1/runs", dependencies=[Depends(authorize)])
    async def runs(
        task_id: str | None = Query(default=None, alias="taskID"),
        worker_id: str | None = Query(default=None, alias="workerID"),
        state: str | None = None,
        limit: int = Query(default=100, ge=1, le=500),
    ) -> dict:
        if state is not None and state not in {item.value for item in RunState}:
            raise HTTPException(status_code=400, detail="invalid run state")
        return envelope(
            await asyncio.to_thread(
                projection.runs,
                task_id=task_id,
                worker_id=worker_id,
                state=state,
                limit=limit,
            )
        )

    @app.get("/v1/runs/{run_id}", dependencies=[Depends(authorize)])
    async def run(run_id: str) -> dict:
        try:
            value = await asyncio.to_thread(projection.run, run_id)
        except KeyError as error:
            raise HTTPException(status_code=404, detail=f"unknown run: {run_id}") from error
        return envelope(value)

    @app.get("/v1/escalations", dependencies=[Depends(authorize)])
    async def execution_escalations(
        run_id: str | None = Query(default=None, alias="runID"),
        task_id: str | None = Query(default=None, alias="taskID"),
        state: str | None = None,
        code: str | None = None,
        limit: int = Query(default=100, ge=1, le=500),
    ) -> dict:
        if state is not None and state not in {"open", "resolved", "dismissed"}:
            raise HTTPException(status_code=400, detail="invalid escalation state")
        allowed_codes = {
            "PROVIDER_STATE_AMBIGUOUS",
            "EXTERNAL_JOB_UNREACHABLE",
            "IDEMPOTENCY_UNCERTAIN",
            "RESUME_UNSUPPORTED",
            "RESULT_COLLECTION_UNCERTAIN",
        }
        if code is not None and code not in allowed_codes:
            raise HTTPException(status_code=400, detail="invalid escalation code")
        return envelope(
            await asyncio.to_thread(
                projection.execution_escalations,
                run_id=run_id,
                task_id=task_id,
                state=state,
                code=code,
                limit=limit,
            )
        )

    @app.get("/v1/escalations/{escalation_id}", dependencies=[Depends(authorize)])
    async def execution_escalation(escalation_id: str) -> dict:
        try:
            value = await asyncio.to_thread(projection.execution_escalation, escalation_id)
        except KeyError as error:
            raise HTTPException(
                status_code=404, detail=f"unknown escalation: {escalation_id}"
            ) from error
        return envelope(value)

    @app.get("/v1/tasks", dependencies=[Depends(authorize)])
    async def tasks(state: str | None = None) -> dict:
        allowed = {"queued", "running", "waiting", "completed", "failed", "cancelled"}
        if state is not None and state not in allowed:
            raise HTTPException(status_code=400, detail="invalid task state")
        return envelope(await asyncio.to_thread(projection.tasks, state=state))

    @app.get("/v1/tasks/{task_id}", dependencies=[Depends(authorize)])
    async def task(task_id: str) -> dict:
        try:
            value = await asyncio.to_thread(projection.task, task_id)
        except KeyError as error:
            raise HTTPException(status_code=404, detail=f"unknown task: {task_id}") from error
        return envelope(value)

    @app.get("/v1/goals", dependencies=[Depends(authorize)])
    async def goals(project_id: str | None = Query(default=None, alias="projectID")) -> dict:
        return envelope(await asyncio.to_thread(projection.goals, project_id=project_id))

    @app.get("/v1/goals/{goal_id}", dependencies=[Depends(authorize)])
    async def goal(goal_id: str) -> dict:
        try:
            value = await asyncio.to_thread(projection.goal, goal_id)
        except KeyError as error:
            raise HTTPException(status_code=404, detail=f"unknown goal: {goal_id}") from error
        return envelope(value)

    @app.get("/v1/autonomy/hosts", dependencies=[Depends(authorize)])
    async def autonomy_hosts() -> dict:
        return envelope(await asyncio.to_thread(projection.autonomy_hosts))

    @app.get("/v1/autonomy/hosts/{host_id}", dependencies=[Depends(authorize)])
    async def autonomy_host(host_id: str) -> dict:
        try:
            values = await asyncio.to_thread(projection.autonomy_hosts, host_id=host_id)
        except KeyError as error:
            raise HTTPException(
                status_code=404, detail=f"unknown autonomous host: {host_id}"
            ) from error
        return envelope(values[0])

    @app.get("/v1/autonomy/leases", dependencies=[Depends(authorize)])
    async def autonomy_goal_leases(
        host_id: str | None = Query(default=None, alias="hostID"),
        goal_id: str | None = Query(default=None, alias="goalID"),
        owned_only: bool = Query(default=False, alias="ownedOnly"),
    ) -> dict:
        return envelope(
            await asyncio.to_thread(
                projection.autonomy_goal_leases,
                host_id=host_id,
                goal_id=goal_id,
                owned_only=owned_only,
            )
        )

    def _resource_filters(
        *,
        goal_id: str | None,
        task_id: str | None,
        provider: str | None,
        model: str | None,
        worker_id: str | None,
        account_scope: str | None,
        quota_pool_id: str | None,
        observed_after: datetime | None,
        observed_before: datetime | None,
    ) -> dict[str, Any]:
        for value in (observed_after, observed_before):
            if value is not None and value.tzinfo is None:
                raise HTTPException(status_code=400, detail="resource timestamps require timezone")
        if (
            observed_after is not None
            and observed_before is not None
            and observed_after > observed_before
        ):
            raise HTTPException(
                status_code=400, detail="observedAfter cannot exceed observedBefore"
            )
        return {
            "goal_id": goal_id,
            "task_id": task_id,
            "provider": provider,
            "model": model,
            "worker_id": worker_id,
            "account_scope": account_scope,
            "quota_pool_id": quota_pool_id,
            "observed_after": observed_after,
            "observed_before": observed_before,
        }

    @app.get("/v1/resources/snapshots", dependencies=[Depends(authorize)])
    async def resource_snapshots(
        goal_id: str | None = Query(default=None, alias="goalID"),
        task_id: str | None = Query(default=None, alias="taskID"),
        provider: str | None = None,
        model: str | None = None,
        worker_id: str | None = Query(default=None, alias="workerID"),
        account_scope: str | None = Query(default=None, alias="accountScope"),
        quota_pool_id: str | None = Query(default=None, alias="quotaPoolID"),
        observed_after: Annotated[datetime | None, Query(alias="observedAfter")] = None,
        observed_before: Annotated[datetime | None, Query(alias="observedBefore")] = None,
        limit: int = Query(default=1000, ge=1, le=10_000),
    ) -> dict:
        filters = _resource_filters(
            goal_id=goal_id,
            task_id=task_id,
            provider=provider,
            model=model,
            worker_id=worker_id,
            account_scope=account_scope,
            quota_pool_id=quota_pool_id,
            observed_after=observed_after,
            observed_before=observed_before,
        )
        return envelope(
            await asyncio.to_thread(projection.resource_snapshots, **filters, limit=limit)
        )

    @app.get("/v1/resources/aggregate", dependencies=[Depends(authorize)])
    async def resource_aggregate(
        goal_id: str | None = Query(default=None, alias="goalID"),
        task_id: str | None = Query(default=None, alias="taskID"),
        provider: str | None = None,
        model: str | None = None,
        worker_id: str | None = Query(default=None, alias="workerID"),
        account_scope: str | None = Query(default=None, alias="accountScope"),
        quota_pool_id: str | None = Query(default=None, alias="quotaPoolID"),
        observed_after: Annotated[datetime | None, Query(alias="observedAfter")] = None,
        observed_before: Annotated[datetime | None, Query(alias="observedBefore")] = None,
    ) -> dict:
        filters = _resource_filters(
            goal_id=goal_id,
            task_id=task_id,
            provider=provider,
            model=model,
            worker_id=worker_id,
            account_scope=account_scope,
            quota_pool_id=quota_pool_id,
            observed_after=observed_after,
            observed_before=observed_before,
        )
        return envelope(await asyncio.to_thread(projection.resource_aggregate, **filters))

    @app.get("/v1/resources/economics", dependencies=[Depends(authorize)])
    async def resource_economics(
        project_id: str | None = Query(default=None, alias="projectID"),
        goal_id: str | None = Query(default=None, alias="goalID"),
    ) -> dict:
        try:
            value = await asyncio.to_thread(
                projection.resource_economics, project_id=project_id, goal_id=goal_id
            )
        except KeyError as error:
            raise HTTPException(status_code=404, detail=f"unknown goal: {goal_id}") from error
        except ValueError as error:
            raise HTTPException(status_code=400, detail=str(error)) from error
        return envelope(value)

    @app.post(
        "/v1/goals",
        status_code=201,
        dependencies=[Depends(authorize_goal_control)],
    )
    async def create_goal(body: GoalCreateRequest) -> dict:
        from project_supervisor.autonomy import GoalBudget

        budgets = GoalBudget(**body.budgets.model_dump()) if body.budgets is not None else None
        try:
            row = await asyncio.to_thread(
                goal_service.create_goal,
                project_id=body.project_id,
                intent=body.intent,
                budgets=budgets,
                goal_id=body.goal_id,
                actor="human:api",
            )
        except KeyError as error:
            raise HTTPException(
                status_code=404,
                detail=f"unknown project: {body.project_id}",
            ) from error
        except (sqlite3.IntegrityError, ValueError) as error:
            raise HTTPException(status_code=409, detail=str(error)) from error
        return envelope(projection._goal(row))

    @app.post(
        "/v1/goals/{goal_id}/pause",
        dependencies=[Depends(authorize_goal_control)],
    )
    async def pause_goal(goal_id: str, body: GoalPauseRequest) -> dict:
        value = await _apply_goal_control(
            goal_id,
            goal_service.pause,
            body.mode,
            reason=body.reason,
        )
        return envelope(value)

    @app.post(
        "/v1/goals/{goal_id}/resume",
        dependencies=[Depends(authorize_goal_control)],
    )
    async def resume_goal(goal_id: str, body: GoalResumeRequest) -> dict:
        return envelope(await _apply_goal_control(goal_id, goal_service.resume, reason=body.reason))

    @app.post(
        "/v1/goals/{goal_id}/steer",
        dependencies=[Depends(authorize_goal_control)],
    )
    async def steer_goal(goal_id: str, body: GoalSteerRequest) -> dict:
        return envelope(
            await _apply_goal_control(
                goal_id,
                goal_service.steer,
                body.instruction,
                priority=body.priority,
                preserve_valid_work=body.preserve_valid_work,
            )
        )

    @app.post(
        "/v1/goals/{goal_id}/stop",
        dependencies=[Depends(authorize_goal_control)],
    )
    async def stop_goal(goal_id: str, body: GoalStopRequest) -> dict:
        return envelope(await _apply_goal_control(goal_id, goal_service.stop, reason=body.reason))

    @app.get("/v1/telemetry", dependencies=[Depends(authorize)])
    async def telemetry(
        goal_id: str | None = Query(default=None, alias="goalID"),
        task_id: str | None = Query(default=None, alias="taskID"),
        limit: int = Query(default=1000, ge=1, le=10_000),
    ) -> dict:
        """Return normalized invocation facts and aggregates without provider secrets."""

        from project_supervisor.telemetry import InvocationTelemetryRepository

        repository = InvocationTelemetryRepository(store)
        records = await asyncio.to_thread(
            repository.list,
            goal_id=goal_id,
            task_id=task_id,
            limit=limit,
        )
        try:
            aggregate = await asyncio.to_thread(
                repository.aggregate,
                goal_id=goal_id,
                task_id=task_id,
            )
        except ValueError as error:
            raise HTTPException(status_code=400, detail=str(error)) from error
        return envelope(
            {
                "aggregate": aggregate.to_protocol(),
                "invocations": [record.to_protocol() for record in records],
            }
        )

    async def _apply_goal_control(
        goal_id: str,
        operation: Any,
        *args: Any,
        **kwargs: Any,
    ) -> dict[str, Any]:
        try:
            row = await asyncio.to_thread(
                operation,
                goal_id,
                *args,
                **kwargs,
                actor="human:api",
            )
        except KeyError as error:
            raise HTTPException(status_code=404, detail=f"unknown goal: {goal_id}") from error
        except ValueError as error:
            raise HTTPException(status_code=409, detail=str(error)) from error
        return projection._goal(row)

    @app.get("/v1/events", dependencies=[Depends(authorize)])
    async def events(
        limit: int = Query(default=100, ge=1, le=500),
        cursor: str | None = None,
        after_sequence: int = Query(default=0, alias="afterSequence", ge=0),
        node: str | None = None,
        harness: str | None = None,
        task: str | None = None,
        goal: str | None = None,
        kind: str | None = None,
        min_severity: str = Query(default="debug", alias="minSeverity"),
        q: str | None = None,
    ) -> dict:
        if min_severity not in SEVERITY_RANK:
            raise HTTPException(status_code=400, detail="invalid minSeverity")
        try:
            page = await asyncio.to_thread(
                projection.events,
                limit=limit,
                cursor=cursor,
                after_sequence=after_sequence,
                nodes=_csv(node),
                harnesses=_csv(harness),
                tasks=_csv(task),
                goals=_csv(goal),
                kinds=_csv(kind),
                minimum_severity=min_severity,
                search=q,
            )
        except ValueError as error:
            raise HTTPException(status_code=400, detail=str(error)) from error
        return envelope(page.events, meta=page.meta)

    @app.websocket("/v1/stream")
    async def stream(
        websocket: WebSocket,
        after_sequence: int = Query(default=0, alias="afterSequence", ge=0),
    ) -> None:
        peer = websocket.client.host if websocket.client else None
        if not loopback_dev_allowed(peer):
            token = _bearer(websocket.headers)
            if token is None or not store.verify_api_token(token, settings.required_scope):
                await websocket.close(code=4403)
                return
        await websocket.accept()
        last_sent = after_sequence
        loop = asyncio.get_running_loop()
        next_keepalive = loop.time() + settings.keepalive_seconds
        try:
            snapshot = await asyncio.to_thread(
                projection.snapshot, recent_event_limit=settings.recent_event_limit
            )
            await websocket.send_json(
                SnapshotFrame(snapshot=snapshot).model_dump(by_alias=True, exclude_none=True)
            )
            # Replay is intentionally at-least-once: the snapshot may contain the same recent ids,
            # and the client de-duplicates by event id.
            while True:
                page = await asyncio.to_thread(
                    projection.events,
                    limit=settings.stream_batch_size,
                    after_sequence=last_sent,
                    ascending=True,
                )
                if not page.events:
                    break
                await websocket.send_json(
                    EventsFrame(events=page.events).model_dump(by_alias=True, exclude_none=True)
                )
                last_sent = max(event["sequence"] for event in page.events)
                if len(page.events) < settings.stream_batch_size:
                    break

            while True:
                await asyncio.sleep(settings.poll_interval_seconds)
                page = await asyncio.to_thread(
                    projection.events,
                    limit=settings.stream_batch_size,
                    after_sequence=last_sent,
                    ascending=True,
                )
                if page.events:
                    await websocket.send_json(
                        EventsFrame(events=page.events).model_dump(by_alias=True, exclude_none=True)
                    )
                    last_sent = max(event["sequence"] for event in page.events)
                    refreshed = await asyncio.to_thread(
                        projection.snapshot,
                        recent_event_limit=settings.recent_event_limit,
                    )
                    await websocket.send_json(
                        SnapshotFrame(snapshot=refreshed).model_dump(
                            by_alias=True, exclude_none=True
                        )
                    )
                if loop.time() >= next_keepalive:
                    highest = await asyncio.to_thread(store.highest_event_sequence)
                    await websocket.send_json(
                        KeepaliveFrame(highestSequence=highest).model_dump(
                            by_alias=True, exclude_none=True
                        )
                    )
                    next_keepalive = loop.time() + settings.keepalive_seconds
        except (WebSocketDisconnect, RuntimeError):
            return

    return app
