from __future__ import annotations

import asyncio
import ipaddress
from collections.abc import Mapping
from dataclasses import dataclass

from fastapi import Depends, FastAPI, HTTPException, Query, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import JSONResponse

from project_supervisor.store import StateStore

from .projections import SEVERITY_RANK, SupervisorProjection
from .schemas import APIError, EventsFrame, KeepaliveFrame, SnapshotFrame, envelope


@dataclass(frozen=True, slots=True)
class APISettings:
    bind_host: str = "127.0.0.1"
    allow_unauthenticated_loopback: bool = False
    required_scope: str = "observe:read"
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
    allow_unauthenticated_loopback: bool | None = None,
    bind_host: str | None = None,
) -> FastAPI:
    """Create the read-only Cyber Office API.

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
    projection = SupervisorProjection(store)
    app = FastAPI(title="CHIPS Agent Fabric Supervisor API", version="0.1.0a1")
    app.state.store = store
    app.state.api_settings = settings
    app.state.projection = projection

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

    @app.exception_handler(HTTPException)
    async def http_error(_request: Request, error: HTTPException) -> JSONResponse:
        code = {
            401: "token_required",
            403: "insufficient_scope",
            404: "task_not_found",
        }.get(error.status_code, "invalid_request")
        body = APIError(code=code, message=str(error.detail)).model_dump(
            by_alias=True, exclude_none=True
        )
        return JSONResponse(status_code=error.status_code, content=body)

    @app.get("/v1/health")
    async def health() -> dict:
        """Credential-free liveness only; no node, worker, or project details."""

        return envelope({"status": "ok", "apiVersion": "v1", "readOnly": True})

    @app.get("/v1/capabilities", dependencies=[Depends(authorize)])
    async def capabilities() -> dict:
        return envelope(
            {
                "observation": {
                    "rest": True,
                    "webSocket": True,
                    "resumeCursor": "eventSequence",
                },
                "resources": ["status", "nodes", "workers", "tasks", "events"],
                "mutations": [],
                "requiredScope": settings.required_scope,
                "safety": {
                    "readOnly": True,
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
            }
        )

    @app.get("/v1/status", dependencies=[Depends(authorize)])
    async def status() -> dict:
        return envelope(await asyncio.to_thread(projection.status))

    @app.get("/v1/nodes", dependencies=[Depends(authorize)])
    async def nodes() -> dict:
        return envelope(await asyncio.to_thread(projection.nodes))

    @app.get("/v1/workers", dependencies=[Depends(authorize)])
    async def workers() -> dict:
        return envelope(await asyncio.to_thread(projection.workers))

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

    @app.get("/v1/events", dependencies=[Depends(authorize)])
    async def events(
        limit: int = Query(default=100, ge=1, le=500),
        cursor: str | None = None,
        after_sequence: int = Query(default=0, alias="afterSequence", ge=0),
        node: str | None = None,
        harness: str | None = None,
        task: str | None = None,
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
