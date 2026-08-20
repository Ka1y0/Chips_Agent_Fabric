from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Any

from project_supervisor.domain import RunState

from .base import (
    EventSink,
    Usage,
    WorkerAdapter,
    WorkerEvent,
    WorkerRequest,
    WorkerResult,
    event_time,
    publish_event,
)


@dataclass(frozen=True, slots=True)
class MockBehavior:
    text: str = "MOCK_OK"
    delay_seconds: float = 0.0
    exit_code: int = 0
    model: str = "mock-deterministic-v1"
    session_id: str = "mock-session"
    usage: Usage = Usage(input_tokens=1, output_tokens=1, total_tokens=2, cost_usd=0.0)


class MockAdapter(WorkerAdapter):
    """Deterministic async worker used by state-machine and API tests."""

    def __init__(self, behavior: MockBehavior | None = None) -> None:
        self.behavior = behavior or MockBehavior()
        self._cancelled: set[str] = set()
        self._active: set[str] = set()

    async def execute(
        self,
        request: WorkerRequest,
        *,
        event_sink: EventSink | None = None,
    ) -> WorkerResult:
        started_at = event_time()
        events: list[WorkerEvent] = []
        self._active.add(request.run_id)

        async def emit(kind: str, payload: dict[str, Any]) -> None:
            event = WorkerEvent(request.run_id, kind, event_time(), payload)
            events.append(event)
            await publish_event(event_sink, event)

        await emit("processStarted", {"pid": None, "mock": True})
        try:
            if self.behavior.delay_seconds:
                await asyncio.sleep(self.behavior.delay_seconds)
            cancelled = request.run_id in self._cancelled
            if cancelled:
                state = RunState.CANCELLED
                exit_code = None
                text = ""
                error = "mock worker was cancelled"
            else:
                state = RunState.COMPLETED if self.behavior.exit_code == 0 else RunState.FAILED
                exit_code = self.behavior.exit_code
                text = self.behavior.text
                error = None if state is RunState.COMPLETED else "deterministic mock failure"
            await emit("processExited", {"exitCode": exit_code, "state": state.value})
            return WorkerResult(
                run_id=request.run_id,
                state=state,
                pid=None,
                exit_code=exit_code,
                started_at=started_at,
                ended_at=event_time(),
                stdout=text,
                stderr="",
                final_text=text,
                events=tuple(events),
                session_id=f"{self.behavior.session_id}:{request.run_id}",
                model=self.behavior.model,
                usage=self.behavior.usage,
                error=error,
            )
        finally:
            self._active.discard(request.run_id)
            self._cancelled.discard(request.run_id)

    async def cancel(self, run_id: str) -> bool:
        if run_id not in self._active:
            return False
        self._cancelled.add(run_id)
        return True
