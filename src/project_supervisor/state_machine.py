from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from enum import StrEnum

from .domain import ProjectPhase, RunState, TaskState


class InvalidTransition(ValueError):
    """Raised when a persisted entity attempts an illegal state transition."""


@dataclass(frozen=True, slots=True)
class TransitionTable:
    transitions: Mapping[StrEnum, frozenset[StrEnum]]

    def allows(self, current: StrEnum, target: StrEnum) -> bool:
        return current == target or target in self.transitions.get(current, frozenset())

    def require(self, current: StrEnum, target: StrEnum) -> None:
        if not self.allows(current, target):
            raise InvalidTransition(f"illegal transition: {current.value} -> {target.value}")


TASK_TRANSITIONS = TransitionTable(
    {
        TaskState.DRAFT: frozenset({TaskState.QUEUED, TaskState.CANCELLED}),
        TaskState.QUEUED: frozenset(
            {TaskState.READY, TaskState.BLOCKED, TaskState.CANCELLED, TaskState.FAILED}
        ),
        TaskState.READY: frozenset(
            {TaskState.RUNNING, TaskState.BLOCKED, TaskState.CANCELLED, TaskState.FAILED}
        ),
        TaskState.RUNNING: frozenset(
            {
                TaskState.WAITING,
                TaskState.REVIEWING,
                TaskState.SUCCEEDED,
                TaskState.FAILED,
                TaskState.CANCELLED,
                TaskState.INTERRUPTED,
            }
        ),
        TaskState.WAITING: frozenset(
            {
                TaskState.READY,
                TaskState.RUNNING,
                TaskState.BLOCKED,
                TaskState.FAILED,
                TaskState.CANCELLED,
            }
        ),
        TaskState.REVIEWING: frozenset(
            {
                TaskState.READY,
                TaskState.SUCCEEDED,
                TaskState.FAILED,
                TaskState.BLOCKED,
                TaskState.CANCELLED,
            }
        ),
        TaskState.BLOCKED: frozenset({TaskState.READY, TaskState.CANCELLED, TaskState.FAILED}),
        TaskState.INTERRUPTED: frozenset({TaskState.READY, TaskState.FAILED, TaskState.CANCELLED}),
        TaskState.SUCCEEDED: frozenset(),
        TaskState.FAILED: frozenset(),
        TaskState.CANCELLED: frozenset(),
    }
)


RUN_TRANSITIONS = TransitionTable(
    {
        RunState.STARTING: frozenset(
            {
                RunState.RUNNING,
                RunState.FAILED,
                RunState.CANCELLED,
                RunState.AUTH_REQUIRED,
                RunState.RATE_LIMITED,
            }
        ),
        RunState.RUNNING: frozenset(
            {
                RunState.WAITING,
                RunState.COMPLETED,
                RunState.FAILED,
                RunState.CANCELLED,
                RunState.TIMED_OUT,
                RunState.INTERRUPTED,
                RunState.AUTH_REQUIRED,
                RunState.RATE_LIMITED,
            }
        ),
        RunState.WAITING: frozenset(
            {RunState.RUNNING, RunState.COMPLETED, RunState.FAILED, RunState.CANCELLED}
        ),
        RunState.COMPLETED: frozenset(),
        RunState.FAILED: frozenset(),
        RunState.CANCELLED: frozenset(),
        RunState.TIMED_OUT: frozenset(),
        RunState.INTERRUPTED: frozenset(),
        RunState.AUTH_REQUIRED: frozenset(),
        RunState.RATE_LIMITED: frozenset(),
    }
)


PROJECT_TRANSITIONS = TransitionTable(
    {
        ProjectPhase.INITIALIZING: frozenset({ProjectPhase.PLANNING, ProjectPhase.FAILED}),
        ProjectPhase.PLANNING: frozenset(
            {ProjectPhase.DISPATCHING, ProjectPhase.WAITING_FOR_HUMAN, ProjectPhase.FAILED}
        ),
        ProjectPhase.DISPATCHING: frozenset(
            {ProjectPhase.WORKING, ProjectPhase.BLOCKED, ProjectPhase.FAILED}
        ),
        ProjectPhase.WORKING: frozenset(
            {
                ProjectPhase.REVIEWING,
                ProjectPhase.REPLANNING,
                ProjectPhase.WAITING_FOR_HUMAN,
                ProjectPhase.PAUSED,
                ProjectPhase.BLOCKED,
                ProjectPhase.FAILED,
            }
        ),
        ProjectPhase.REVIEWING: frozenset(
            {
                ProjectPhase.REPLANNING,
                ProjectPhase.DISPATCHING,
                ProjectPhase.DONE,
                ProjectPhase.FAILED,
            }
        ),
        ProjectPhase.REPLANNING: frozenset(
            {ProjectPhase.DISPATCHING, ProjectPhase.WAITING_FOR_HUMAN, ProjectPhase.BLOCKED}
        ),
        ProjectPhase.WAITING_FOR_HUMAN: frozenset(
            {
                ProjectPhase.PLANNING,
                ProjectPhase.DISPATCHING,
                ProjectPhase.PAUSED,
                ProjectPhase.FAILED,
            }
        ),
        ProjectPhase.PAUSED: frozenset(
            {
                ProjectPhase.PLANNING,
                ProjectPhase.DISPATCHING,
                ProjectPhase.WORKING,
                ProjectPhase.FAILED,
            }
        ),
        ProjectPhase.BLOCKED: frozenset(
            {ProjectPhase.PLANNING, ProjectPhase.REPLANNING, ProjectPhase.FAILED}
        ),
        ProjectPhase.DONE: frozenset(),
        ProjectPhase.FAILED: frozenset(),
    }
)
