import pytest

from project_supervisor.domain import ProjectPhase, RunState, TaskState
from project_supervisor.state_machine import (
    PROJECT_TRANSITIONS,
    RUN_TRANSITIONS,
    TASK_TRANSITIONS,
    InvalidTransition,
)


def test_task_happy_path_and_terminal_sink() -> None:
    TASK_TRANSITIONS.require(TaskState.DRAFT, TaskState.QUEUED)
    TASK_TRANSITIONS.require(TaskState.QUEUED, TaskState.READY)
    TASK_TRANSITIONS.require(TaskState.READY, TaskState.RUNNING)
    TASK_TRANSITIONS.require(TaskState.RUNNING, TaskState.REVIEWING)
    TASK_TRANSITIONS.require(TaskState.REVIEWING, TaskState.SUCCEEDED)
    with pytest.raises(InvalidTransition):
        TASK_TRANSITIONS.require(TaskState.SUCCEEDED, TaskState.RUNNING)


def test_interrupted_task_can_be_recovered_but_not_claim_success() -> None:
    TASK_TRANSITIONS.require(TaskState.RUNNING, TaskState.INTERRUPTED)
    TASK_TRANSITIONS.require(TaskState.INTERRUPTED, TaskState.READY)
    with pytest.raises(InvalidTransition):
        TASK_TRANSITIONS.require(TaskState.INTERRUPTED, TaskState.SUCCEEDED)


def test_run_and_project_tables_reject_skips() -> None:
    with pytest.raises(InvalidTransition):
        RUN_TRANSITIONS.require(RunState.STARTING, RunState.COMPLETED)
    with pytest.raises(InvalidTransition):
        PROJECT_TRANSITIONS.require(ProjectPhase.INITIALIZING, ProjectPhase.DONE)
