from __future__ import annotations

import asyncio
import json
import uuid
from collections.abc import Callable
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from enum import StrEnum
from typing import Any, Protocol, runtime_checkable

from .domain import (
    ApprovalState,
    EventSeverity,
    ExecutionTopology,
    PermissionClass,
    RunState,
    TaskLabel,
    TaskRequirements,
    TaskState,
)
from .store import StateStore, compact_json, redact_sensitive, timestamp


class GoalState(StrEnum):
    CREATED = "created"
    RUNNING = "running"
    SOFT_PAUSED = "softPaused"
    HARD_PAUSED = "hardPaused"
    STOPPED = "stopped"
    TERMINATED = "terminated"


class PauseMode(StrEnum):
    SOFT = "soft"
    HARD = "hard"


class TerminationReason(StrEnum):
    SUCCESS = "SUCCESS"
    BLOCKED = "BLOCKED"
    NO_PROGRESS = "NO_PROGRESS"
    ITERATION_LIMIT = "ITERATION_LIMIT"
    BUDGET_LIMIT = "BUDGET_LIMIT"
    REPEATED_FAILURE = "REPEATED_FAILURE"
    SAFETY_BOUNDARY = "SAFETY_BOUNDARY"
    PERMISSION_REQUIRED = "PERMISSION_REQUIRED"
    HUMAN_ESCALATION = "HUMAN_ESCALATION"
    USER_STOPPED = "USER_STOPPED"


class EvaluationDisposition(StrEnum):
    INCOMPLETE = "incomplete"
    SATISFIED = "satisfied"
    TERMINATE = "terminate"


class ActionState(StrEnum):
    PLANNED = "planned"
    DISPATCHING = "dispatching"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


@dataclass(frozen=True, slots=True)
class GoalBudget:
    max_iterations: int = 12
    max_tasks: int = 48
    max_failures: int = 6
    no_progress_limit: int = 3
    max_elapsed_seconds: float | None = None
    max_total_tokens: int | None = None
    max_cost_usd: float | None = None

    def __post_init__(self) -> None:
        required = {
            "max_iterations": self.max_iterations,
            "max_tasks": self.max_tasks,
            "max_failures": self.max_failures,
            "no_progress_limit": self.no_progress_limit,
        }
        for name, value in required.items():
            if value < 1:
                raise ValueError(f"{name} must be positive")
        optional = {
            "max_elapsed_seconds": self.max_elapsed_seconds,
            "max_total_tokens": self.max_total_tokens,
            "max_cost_usd": self.max_cost_usd,
        }
        for name, value in optional.items():
            if value is not None and value <= 0:
                raise ValueError(f"{name} must be positive when specified")

    def to_dict(self) -> dict[str, int | float | None]:
        return asdict(self)

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> GoalBudget:
        return cls(**value)


@dataclass(frozen=True, slots=True)
class BudgetObservation:
    total_tokens: int | None = None
    cost_usd: float | None = None


@dataclass(frozen=True, slots=True)
class GoalEvaluation:
    disposition: EvaluationDisposition
    summary: str
    progress_fingerprint: str
    termination_reason: TerminationReason | None = None
    facts: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not self.summary.strip() or not self.progress_fingerprint.strip():
            raise ValueError("evaluation summary and progress fingerprint are required")
        if self.disposition is EvaluationDisposition.TERMINATE:
            if self.termination_reason in {None, TerminationReason.SUCCESS}:
                raise ValueError("terminate evaluations require a non-success reason")
        elif self.termination_reason is not None:
            raise ValueError("termination reason is only valid for terminate evaluations")


@dataclass(frozen=True, slots=True)
class PlannedAction:
    key: str
    title: str
    description: str
    role: str = "primary"
    payload: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not self.key.strip() or not self.title.strip() or not self.description.strip():
            raise ValueError("planned action key, title and description are required")


@dataclass(frozen=True, slots=True)
class GoalPlan:
    summary: str
    actions: tuple[PlannedAction, ...]
    rationale: str = ""


@dataclass(frozen=True, slots=True)
class DispatchHandle:
    reference: str
    task_id: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class ActionResult:
    succeeded: bool
    summary: str
    payload: dict[str, Any] = field(default_factory=dict)
    cancelled: bool = False


@dataclass(frozen=True, slots=True)
class GoalVerification:
    satisfied: bool
    summary: str
    progress_fingerprint: str
    termination_reason: TerminationReason | None = None
    evidence: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not self.summary.strip() or not self.progress_fingerprint.strip():
            raise ValueError("verification summary and progress fingerprint are required")
        if self.satisfied and self.termination_reason not in {None, TerminationReason.SUCCESS}:
            raise ValueError("satisfied verification cannot have a failure reason")
        if not self.satisfied and self.termination_reason is TerminationReason.SUCCESS:
            raise ValueError("incomplete verification cannot terminate as success")


@dataclass(frozen=True, slots=True)
class GoalContext:
    goal: dict[str, Any]
    project: dict[str, Any]
    tasks: tuple[dict[str, Any], ...]
    events: tuple[dict[str, Any], ...]
    iterations: tuple[dict[str, Any], ...]
    actions: tuple[dict[str, Any], ...]
    steers: tuple[dict[str, Any], ...]
    worker_runs: tuple[dict[str, Any], ...]
    worker_results: tuple[dict[str, Any], ...]
    verifications: tuple[dict[str, Any], ...]
    failures: tuple[dict[str, Any], ...]


@runtime_checkable
class GoalEvaluator(Protocol):
    async def evaluate(self, context: GoalContext) -> GoalEvaluation: ...


@runtime_checkable
class GoalPlanner(Protocol):
    async def plan(self, context: GoalContext, evaluation: GoalEvaluation) -> GoalPlan: ...


@runtime_checkable
class GoalVerifier(Protocol):
    async def verify(
        self, context: GoalContext, results: tuple[ActionResult, ...]
    ) -> GoalVerification: ...


@runtime_checkable
class GoalDispatcher(Protocol):
    async def dispatch(
        self, context: GoalContext, action: PlannedAction, action_id: str
    ) -> DispatchHandle: ...

    async def collect(self, handle: DispatchHandle) -> ActionResult: ...

    async def cancel(self, handle: DispatchHandle) -> bool: ...

    async def recover(
        self, context: GoalContext, action: PlannedAction, handle: DispatchHandle
    ) -> DispatchHandle: ...


@runtime_checkable
class BudgetMeter(Protocol):
    async def observe(self, goal_id: str) -> BudgetObservation: ...


class UnknownBudgetMeter:
    async def observe(self, goal_id: str) -> BudgetObservation:
        del goal_id
        return BudgetObservation()


class InvocationTelemetryBudgetMeter:
    """Use canonical invocation telemetry and fail closed when consumption is incomplete."""

    def __init__(self, store: StateStore) -> None:
        self.store = store

    async def observe(self, goal_id: str) -> BudgetObservation:
        return await asyncio.to_thread(self._observe, goal_id)

    def _observe(self, goal_id: str) -> BudgetObservation:
        from .telemetry import InvocationTelemetryRepository

        goal = GoalService(self.store).get_goal(goal_id)
        aggregate = InvocationTelemetryRepository(self.store).aggregate(goal_id=goal_id)
        if aggregate.call_count == 0 and int(goal["task_count"]) == 0:
            return BudgetObservation(total_tokens=0, cost_usd=0.0)
        # A generated action without a corresponding invocation is unmetered. It is never safe to
        # interpret missing telemetry as zero consumption.
        if aggregate.call_count < int(goal["task_count"]):
            return BudgetObservation()
        input_complete = aggregate.input_tokens.known_count == aggregate.call_count
        output_complete = aggregate.output_tokens.known_count == aggregate.call_count
        cost_complete = aggregate.cost.known_count == aggregate.call_count
        total_tokens = None
        if input_complete and output_complete:
            total_tokens = int(
                float(aggregate.input_tokens.observed_sum.telemetry.value or 0)
                + float(aggregate.output_tokens.observed_sum.telemetry.value or 0)
            )
        cost_usd = (
            float(aggregate.cost.observed_sum.telemetry.value or 0) if cost_complete else None
        )
        return BudgetObservation(total_tokens=total_tokens, cost_usd=cost_usd)


class GoalService:
    """Durable, auditable human control plane for autonomous goals."""

    def __init__(self, store: StateStore) -> None:
        self.store = store

    def create_goal(
        self,
        *,
        project_id: str,
        intent: str,
        budgets: GoalBudget | None = None,
        goal_id: str | None = None,
        actor: str = "human",
    ) -> dict[str, Any]:
        if not intent.strip():
            raise ValueError("goal intent is required")
        self.store.get_project(project_id)
        goal_id = goal_id or f"goal-{uuid.uuid4()}"
        budget = budgets or GoalBudget()
        safe_intent = str(redact_sensitive(intent))
        now = timestamp()
        with self.store.transaction() as connection:
            connection.execute(
                "INSERT INTO autonomous_goals("
                "id,project_id,intent,effective_intent,state,budgets_json,created_at,updated_at"
                ") VALUES (?,?,?,?,?,?,?,?)",
                (
                    goal_id,
                    project_id,
                    safe_intent,
                    safe_intent,
                    GoalState.CREATED.value,
                    compact_json(budget.to_dict()),
                    now,
                    now,
                ),
            )
            self.store._append_event(
                connection,
                kind="goalCreated",
                severity=EventSeverity.NOTICE,
                entity_type="goal",
                entity_id=goal_id,
                project_id=project_id,
                summary="Autonomous goal created",
                payload={"state": GoalState.CREATED.value, "budgets": budget.to_dict()},
                actor=actor,
            )
        return self.get_goal(goal_id)

    def get_goal(self, goal_id: str) -> dict[str, Any]:
        with self.store.connect() as connection:
            row = connection.execute(
                "SELECT * FROM autonomous_goals WHERE id=?", (goal_id,)
            ).fetchone()
        if row is None:
            raise KeyError(goal_id)
        return self._project(dict(row))

    def list_goals(self, project_id: str | None = None) -> list[dict[str, Any]]:
        query = "SELECT * FROM autonomous_goals"
        parameters: tuple[Any, ...] = ()
        if project_id is not None:
            query += " WHERE project_id=?"
            parameters = (project_id,)
        query += " ORDER BY created_at,id"
        with self.store.connect() as connection:
            rows = connection.execute(query, parameters).fetchall()
        cursor = self.store.highest_event_sequence()
        return [self._project(dict(row), event_cursor=cursor) for row in rows]

    def pause(
        self,
        goal_id: str,
        mode: PauseMode | str,
        *,
        reason: str | None = None,
        actor: str = "human",
    ) -> dict[str, Any]:
        mode = PauseMode(mode)
        target = GoalState.SOFT_PAUSED if mode is PauseMode.SOFT else GoalState.HARD_PAUSED
        with self.store.transaction() as connection:
            goal = self._require_mutable(connection, goal_id)
            if (
                goal["state"] in {GoalState.SOFT_PAUSED.value, GoalState.HARD_PAUSED.value}
                and goal["state"] == target.value
            ):
                return self.get_goal(goal_id)
            connection.execute(
                "UPDATE autonomous_goals SET state=?,pause_mode=?,updated_at=?,version=version+1 "
                "WHERE id=?",
                (target.value, mode.value, timestamp(), goal_id),
            )
            self.store._append_event(
                connection,
                kind="goalPaused",
                severity=EventSeverity.NOTICE,
                entity_type="goal",
                entity_id=goal_id,
                project_id=goal["project_id"],
                summary=f"Goal {mode.value} pause requested",
                payload={"mode": mode.value, "reason": redact_sensitive(reason)},
                actor=actor,
            )
        return self.get_goal(goal_id)

    def resume(
        self, goal_id: str, *, reason: str | None = None, actor: str = "human"
    ) -> dict[str, Any]:
        with self.store.transaction() as connection:
            goal = self._get_row(connection, goal_id)
            if goal["state"] not in {
                GoalState.SOFT_PAUSED.value,
                GoalState.HARD_PAUSED.value,
            }:
                raise ValueError("only a paused goal can be resumed")
            connection.execute(
                "UPDATE autonomous_goals SET state=?,pause_mode=NULL,updated_at=?,"
                "started_at=COALESCE(started_at,?),version=version+1 WHERE id=?",
                (GoalState.RUNNING.value, timestamp(), timestamp(), goal_id),
            )
            self.store._append_event(
                connection,
                kind="goalResumed",
                severity=EventSeverity.NOTICE,
                entity_type="goal",
                entity_id=goal_id,
                project_id=goal["project_id"],
                summary="Autonomous goal resumed",
                payload={"reason": redact_sensitive(reason)},
                actor=actor,
            )
        return self.get_goal(goal_id)

    def steer(
        self,
        goal_id: str,
        instruction: str,
        *,
        priority: int | None = None,
        preserve_valid_work: bool = True,
        actor: str = "human",
    ) -> dict[str, Any]:
        if not instruction.strip():
            raise ValueError("steer instruction is required")
        if priority is not None and not 0 <= priority <= 100:
            raise ValueError("priority must be between 0 and 100")
        with self.store.transaction() as connection:
            goal = self._require_mutable(connection, goal_id)
            sequence = int(goal["steer_version"]) + 1
            steer_id = f"steer-{uuid.uuid4()}"
            safe_instruction = str(redact_sensitive(instruction))
            effective = f"{goal['effective_intent']}\n\nSteer {sequence}: {safe_instruction}"
            now = timestamp()
            connection.execute(
                "INSERT INTO autonomous_steers("
                "id,goal_id,sequence,instruction,priority,preserve_valid_work,actor,created_at"
                ") VALUES (?,?,?,?,?,?,?,?)",
                (
                    steer_id,
                    goal_id,
                    sequence,
                    safe_instruction,
                    priority,
                    int(preserve_valid_work),
                    actor,
                    now,
                ),
            )
            connection.execute(
                "UPDATE autonomous_goals SET effective_intent=?,steer_version=?,updated_at=?,"
                "version=version+1 WHERE id=?",
                (effective, sequence, now, goal_id),
            )
            self.store._append_event(
                connection,
                kind="goalSteered",
                severity=EventSeverity.NOTICE,
                entity_type="goal",
                entity_id=goal_id,
                project_id=goal["project_id"],
                summary="Human steer persisted; goal requires re-evaluation",
                payload={
                    "steerID": steer_id,
                    "steerVersion": sequence,
                    "instruction": safe_instruction,
                    "priority": priority,
                    "preserveValidWork": preserve_valid_work,
                },
                actor=actor,
            )
        return self.get_goal(goal_id)

    def stop(self, goal_id: str, *, reason: str, actor: str = "human") -> dict[str, Any]:
        if not reason.strip():
            raise ValueError("stop reason is required")
        with self.store.transaction() as connection:
            goal = self._require_mutable(connection, goal_id)
            now = timestamp()
            safe_reason = str(redact_sensitive(reason))
            connection.execute(
                "UPDATE autonomous_goals SET state=?,pause_mode=NULL,termination_reason=?,"
                "termination_detail=?,finished_at=?,updated_at=?,version=version+1 WHERE id=?",
                (
                    GoalState.STOPPED.value,
                    TerminationReason.USER_STOPPED.value,
                    safe_reason,
                    now,
                    now,
                    goal_id,
                ),
            )
            self.store._append_event(
                connection,
                kind="goalStopped",
                severity=EventSeverity.WARNING,
                entity_type="goal",
                entity_id=goal_id,
                project_id=goal["project_id"],
                summary="Goal intentionally stopped by user",
                payload={"reason": safe_reason},
                actor=actor,
            )
        return self.get_goal(goal_id)

    def _project(self, row: dict[str, Any], *, event_cursor: int | None = None) -> dict[str, Any]:
        row["budgets"] = json.loads(row.pop("budgets_json"))
        row["event_cursor"] = (
            self.store.highest_event_sequence() if event_cursor is None else event_cursor
        )
        return row

    @staticmethod
    def _get_row(connection: Any, goal_id: str) -> dict[str, Any]:
        row = connection.execute("SELECT * FROM autonomous_goals WHERE id=?", (goal_id,)).fetchone()
        if row is None:
            raise KeyError(goal_id)
        return dict(row)

    def _require_mutable(self, connection: Any, goal_id: str) -> dict[str, Any]:
        goal = self._get_row(connection, goal_id)
        if goal["state"] in {GoalState.STOPPED.value, GoalState.TERMINATED.value}:
            raise ValueError("terminal goals cannot be controlled")
        return goal


class AutonomousIterationEngine:
    """Provider-neutral persisted evaluate/plan/dispatch/collect/verify loop."""

    def __init__(
        self,
        *,
        store: StateStore,
        evaluator: GoalEvaluator,
        planner: GoalPlanner,
        dispatcher: GoalDispatcher,
        verifier: GoalVerifier,
        budget_meter: BudgetMeter | None = None,
        control_poll_seconds: float = 0.02,
        cancellation_grace_seconds: float = 0.5,
        ownership_guard: Callable[[], None] | None = None,
    ) -> None:
        if control_poll_seconds <= 0:
            raise ValueError("control_poll_seconds must be positive")
        if not 0 < cancellation_grace_seconds <= 30:
            raise ValueError("cancellation_grace_seconds must be between zero and 30")
        self.store = store
        self.goals = GoalService(store)
        self.evaluator = evaluator
        self.planner = planner
        self.dispatcher = dispatcher
        self.verifier = verifier
        self.budget_meter = budget_meter or InvocationTelemetryBudgetMeter(store)
        self.control_poll_seconds = control_poll_seconds
        self.cancellation_grace_seconds = cancellation_grace_seconds
        self.ownership_guard = ownership_guard
        # Keep durable cancellation requests alive after the goal loop's bounded grace period.
        # The grace controls loop latency only; cancelling these Tasks could abandon a provider
        # cancellation after canonical Task state was already fenced to CANCELLED.
        self._background_cancellations: set[asyncio.Task[bool]] = set()

    def set_ownership_guard(self, guard: Callable[[], None]) -> None:
        """Fence every autonomous mutation to the production host's current durable lease."""

        self.ownership_guard = guard

    def _assert_ownership(self) -> None:
        if self.ownership_guard is not None:
            self.ownership_guard()

    def _track_background_cancellation(self, task: asyncio.Task[bool]) -> None:
        self._background_cancellations.add(task)

        def consume_result(done: asyncio.Task[bool]) -> None:
            self._background_cancellations.discard(done)
            if done.cancelled():
                return
            # Retrieving the exception prevents an unobserved-task warning. Durable runtime
            # state and reconciliation escalation remain the authoritative error record.
            done.exception()

        task.add_done_callback(consume_result)

    async def _request_cancellations(
        self,
        requested: list[tuple[dict[str, Any], DispatchHandle]],
    ) -> dict[str, tuple[bool, str]]:
        """Request cancellation without letting the latency bound cancel the request itself."""

        tasks: dict[asyncio.Task[bool], str] = {}
        for row, handle in requested:
            cancellation = asyncio.create_task(
                self.dispatcher.cancel(handle),
                name=f"goal-action-cancel:{row['id']}",
            )
            tasks[cancellation] = str(row["id"])
            self._track_background_cancellation(cancellation)
        if not tasks:
            return {}
        _done, pending = await asyncio.wait(
            tasks,
            timeout=self.cancellation_grace_seconds,
        )
        outcomes: dict[str, tuple[bool, str]] = {}
        for cancellation, action_id in tasks.items():
            if cancellation in pending:
                outcomes[action_id] = (False, "pending")
                continue
            try:
                accepted = cancellation.result()
            except asyncio.CancelledError:
                outcomes[action_id] = (False, "cancelled")
            except Exception:
                outcomes[action_id] = (False, "failed")
            else:
                outcomes[action_id] = (accepted is True, "accepted" if accepted else "unconfirmed")
        return outcomes

    async def run(self, goal_id: str) -> dict[str, Any]:
        while True:
            goal = self.goals.get_goal(goal_id)
            if goal["state"] in {
                GoalState.SOFT_PAUSED.value,
                GoalState.HARD_PAUSED.value,
                GoalState.STOPPED.value,
                GoalState.TERMINATED.value,
            }:
                return goal
            await self.run_once(goal_id)

    async def run_once(self, goal_id: str) -> dict[str, Any]:
        self._assert_ownership()
        goal = self.goals.get_goal(goal_id)
        if goal["state"] == GoalState.CREATED.value:
            self._start(goal_id)
            goal = self.goals.get_goal(goal_id)
        if goal["state"] != GoalState.RUNNING.value:
            return goal
        active = self._active_iteration(goal_id)
        if active is None:
            guard = await self._budget_guard(goal)
            if guard is not None:
                self._terminate(goal_id, guard[0], guard[1])
                return self.goals.get_goal(goal_id)
            active = self._begin_iteration(goal_id)
        await self._advance_iteration(goal_id, active["id"])
        return self.goals.get_goal(goal_id)

    async def _advance_iteration(self, goal_id: str, iteration_id: str) -> None:
        iteration = self._get_iteration(iteration_id)
        context = self._context(goal_id)
        evaluation_data = _loads(iteration["evaluation_json"])
        if evaluation_data is None:
            evaluation = await self.evaluator.evaluate(context)
            self._assert_ownership()
            if not self._record_evaluation(
                iteration_id,
                evaluation,
                expected_steer_version=int(context.goal["steer_version"]),
            ):
                return
            evaluation_steer_version = int(context.goal["steer_version"])
        else:
            evaluation = _evaluation_from_dict(evaluation_data)
            if "steerVersion" not in evaluation_data and int(context.goal["steer_version"]) > 0:
                # Pre-upgrade checkpoints cannot prove whether newer guidance was observed.
                # An impossible version deliberately trips the atomic stale-phase fence.
                evaluation_steer_version = -1
            else:
                evaluation_steer_version = int(evaluation_data.get("steerVersion", 0))

        if not self._phase_is_current(goal_id, iteration_id, evaluation_steer_version):
            return
        if evaluation.disposition is EvaluationDisposition.SATISFIED:
            # Evaluation proposes completion; an independent verification checkpoint remains
            # authoritative. This path also reloads a persisted verification after a crash.
            checkpoint = await self._verification_checkpoint(
                goal_id,
                iteration_id,
                (),
                expected_steer_version=evaluation_steer_version,
            )
            if checkpoint is not None:
                verification, verification_steer_version = checkpoint
                applied = self._apply_verification_outcome(
                    goal_id,
                    iteration_id,
                    verification,
                    failures=0,
                    expected_steer_version=verification_steer_version,
                )
                if not applied:
                    await asyncio.sleep(self.control_poll_seconds)
            return
        if evaluation.disposition is EvaluationDisposition.TERMINATE:
            assert evaluation.termination_reason is not None
            self._terminate_phase_if_current(
                goal_id,
                iteration_id,
                evaluation.termination_reason,
                evaluation.summary,
                expected_steer_version=evaluation_steer_version,
            )
            return
        goal = self.goals.get_goal(goal_id)
        budget = GoalBudget.from_dict(goal["budgets"])
        if goal["no_progress_count"] >= budget.no_progress_limit:
            self._terminate_phase_if_current(
                goal_id,
                iteration_id,
                TerminationReason.NO_PROGRESS,
                "evaluation progress fingerprint repeated beyond configured limit",
                expected_steer_version=evaluation_steer_version,
            )
            return

        iteration = self._get_iteration(iteration_id)
        plan_data = _loads(iteration["plan_json"])
        if plan_data is None:
            if self._control_gate(goal_id):
                return
            planning_context = self._context(goal_id)
            plan = await self.planner.plan(planning_context, evaluation)
            self._assert_ownership()
            if self._steer_changed(planning_context):
                self._interrupt_for_steer(goal_id, iteration_id, planning_context)
                return
            if not plan.actions:
                self._terminate_phase_if_current(
                    goal_id,
                    iteration_id,
                    TerminationReason.NO_PROGRESS,
                    "planner produced no next actions for an incomplete goal",
                    expected_steer_version=int(planning_context.goal["steer_version"]),
                )
                return
            goal = self.goals.get_goal(goal_id)
            budget = GoalBudget.from_dict(goal["budgets"])
            if goal["task_count"] + len(plan.actions) > budget.max_tasks:
                self._terminate_phase_if_current(
                    goal_id,
                    iteration_id,
                    TerminationReason.BUDGET_LIMIT,
                    "planned actions would exceed max_tasks",
                    expected_steer_version=int(planning_context.goal["steer_version"]),
                )
                return
            if not self._record_plan(
                iteration_id,
                goal_id,
                plan,
                expected_steer_version=int(planning_context.goal["steer_version"]),
            ):
                return

        if self._control_gate(goal_id):
            return
        await self._dispatch_and_collect(goal_id, iteration_id)
        self._assert_ownership()
        goal = self.goals.get_goal(goal_id)
        if goal["state"] in {
            GoalState.SOFT_PAUSED.value,
            GoalState.HARD_PAUSED.value,
        }:
            return
        if goal["state"] in {GoalState.STOPPED.value, GoalState.TERMINATED.value}:
            self._finish_iteration(iteration_id, "interrupted")
            return

        actions = self._list_actions(iteration_id=iteration_id)
        if any(item["state"] in {"planned", "dispatching", "running"} for item in actions):
            return
        results = tuple(_result_from_action(item) for item in actions)
        verification_context = self._context(goal_id)
        checkpoint = await self._verification_checkpoint(
            goal_id,
            iteration_id,
            results,
            expected_steer_version=int(verification_context.goal["steer_version"]),
        )
        if checkpoint is None:
            return
        verification, verification_steer_version = checkpoint
        applied = self._apply_verification_outcome(
            goal_id,
            iteration_id,
            verification,
            failures=sum(not result.succeeded for result in results),
            expected_steer_version=verification_steer_version,
        )
        if not applied:
            await asyncio.sleep(self.control_poll_seconds)

    async def _dispatch_and_collect(self, goal_id: str, iteration_id: str) -> None:
        self._set_iteration_state(iteration_id, "dispatching")
        context = self._context(goal_id)
        started_now: set[str] = set()
        for row in self._list_actions(iteration_id=iteration_id):
            if row["state"] not in {ActionState.PLANNED.value, ActionState.DISPATCHING.value}:
                continue
            if self._control_gate(goal_id):
                break
            self._assert_ownership()
            action = _action_from_row(row)
            self._set_action(row["id"], ActionState.DISPATCHING)
            handle = _handle_from_row(row)
            try:
                if handle is None:
                    handle = await self.dispatcher.dispatch(context, action, row["id"])
                else:
                    handle = await self.dispatcher.recover(context, action, handle)
                self._assert_ownership()
                self._set_action(row["id"], ActionState.RUNNING, handle=handle)
                started_now.add(row["id"])
            except Exception as error:
                self._set_action(
                    row["id"], ActionState.FAILED, error=str(redact_sensitive(str(error)))
                )

        running = [
            row
            for row in self._list_actions(iteration_id=iteration_id)
            if row["state"] == "running"
        ]
        if not running:
            return
        self._set_iteration_state(iteration_id, "collecting")
        pending: dict[asyncio.Task[ActionResult], tuple[dict[str, Any], DispatchHandle]] = {}
        for row in running:
            handle = _handle_from_row(row)
            if handle is None:
                self._set_action(row["id"], ActionState.FAILED, error="missing dispatch handle")
                continue
            if row["id"] not in started_now:
                try:
                    handle = await self.dispatcher.recover(context, _action_from_row(row), handle)
                    self._set_action(row["id"], ActionState.RUNNING, handle=handle)
                except Exception as error:
                    self._set_action(
                        row["id"],
                        ActionState.FAILED,
                        error=str(redact_sensitive(str(error))),
                    )
                    continue
            task = asyncio.create_task(self.dispatcher.collect(handle))
            pending[task] = (row, handle)
        cancellation_requested: set[str] = set()
        try:
            while pending:
                done, _ = await asyncio.wait(
                    pending, timeout=self.control_poll_seconds, return_when=asyncio.FIRST_COMPLETED
                )
                current_goal = self.goals.get_goal(goal_id)
                state = current_goal["state"]
                self._assert_ownership()
                if state == GoalState.RUNNING.value:
                    guard = await self._inflight_budget_guard(current_goal)
                    if guard is not None:
                        self._terminate(goal_id, guard[0], guard[1])
                        state = GoalState.TERMINATED.value
                if state in {
                    GoalState.HARD_PAUSED.value,
                    GoalState.STOPPED.value,
                    GoalState.TERMINATED.value,
                }:
                    requested = [
                        (row, handle)
                        for row, handle in pending.values()
                        if row["id"] not in cancellation_requested
                    ]
                    cancel_results = await self._request_cancellations(requested)
                    for row, handle in requested:
                        adapter_accepted, request_state = cancel_results[row["id"]]
                        cancellation_requested.add(row["id"])
                        self._journal(
                            goal_id,
                            "goalActionCancellationRequested",
                            "Cancellation requested for in-flight goal action",
                            {
                                "actionID": row["id"],
                                "dispatchRef": handle.reference,
                                "adapterAccepted": adapter_accepted,
                                "requestState": request_state,
                                "bounded": True,
                            },
                        )
                    cancelled_collectors: list[asyncio.Task[ActionResult]] = []
                    for task, (row, _handle) in list(pending.items()):
                        if row["id"] not in cancellation_requested:
                            continue
                        pending.pop(task)
                        task.cancel()
                        cancelled_collectors.append(task)
                        self._set_action(
                            row["id"],
                            ActionState.CANCELLED,
                            error="collection stopped after terminal control",
                        )
                    if cancelled_collectors:
                        await asyncio.gather(*cancelled_collectors, return_exceptions=True)
                    continue
                for task in done:
                    row, _handle = pending.pop(task)
                    try:
                        result = task.result()
                    except Exception as error:
                        self._set_action(
                            row["id"],
                            ActionState.FAILED,
                            error=str(redact_sensitive(str(error))),
                        )
                        continue
                    target = (
                        ActionState.CANCELLED
                        if result.cancelled
                        else ActionState.COMPLETED
                        if result.succeeded
                        else ActionState.FAILED
                    )
                    self._set_action(row["id"], target, result=result)
        finally:
            if pending:
                await self._request_cancellations(list(pending.values()))
                for task in pending:
                    task.cancel()
                await asyncio.gather(*pending, return_exceptions=True)

    async def _budget_guard(self, goal: dict[str, Any]) -> tuple[TerminationReason, str] | None:
        budget = GoalBudget.from_dict(goal["budgets"])
        if goal["iteration_count"] >= budget.max_iterations:
            return TerminationReason.ITERATION_LIMIT, "maximum autonomous iterations reached"
        if goal["task_count"] >= budget.max_tasks:
            return TerminationReason.BUDGET_LIMIT, "maximum generated tasks reached"
        if goal["failure_count"] >= budget.max_failures:
            return TerminationReason.REPEATED_FAILURE, "maximum failed actions reached"
        if budget.max_elapsed_seconds is not None and goal["started_at"]:
            started = datetime.fromisoformat(goal["started_at"].replace("Z", "+00:00"))
            if (datetime.now(UTC) - started).total_seconds() >= budget.max_elapsed_seconds:
                return TerminationReason.BUDGET_LIMIT, "maximum elapsed time reached"
        if budget.max_total_tokens is not None or budget.max_cost_usd is not None:
            observed = await self.budget_meter.observe(goal["id"])
            if budget.max_total_tokens is not None:
                if observed.total_tokens is None:
                    return TerminationReason.BUDGET_LIMIT, "token budget cannot be safely observed"
                if observed.total_tokens >= budget.max_total_tokens:
                    return TerminationReason.BUDGET_LIMIT, "maximum observed tokens reached"
            if budget.max_cost_usd is not None:
                if observed.cost_usd is None:
                    return TerminationReason.BUDGET_LIMIT, "cost budget cannot be safely observed"
                if observed.cost_usd >= budget.max_cost_usd:
                    return TerminationReason.BUDGET_LIMIT, "maximum observed cost reached"
        return None

    async def _inflight_budget_guard(
        self,
        goal: dict[str, Any],
    ) -> tuple[TerminationReason, str] | None:
        """Enforce wall-clock and observable consumption while collection is blocked."""

        budget = GoalBudget.from_dict(goal["budgets"])
        if budget.max_elapsed_seconds is not None and goal["started_at"]:
            started = datetime.fromisoformat(goal["started_at"].replace("Z", "+00:00"))
            if (datetime.now(UTC) - started).total_seconds() >= budget.max_elapsed_seconds:
                return TerminationReason.BUDGET_LIMIT, "maximum elapsed time reached during work"
        if budget.max_total_tokens is None and budget.max_cost_usd is None:
            return None
        observed = await self.budget_meter.observe(goal["id"])
        # In-flight attempts may not have emitted complete accounting yet. Unknown remains unknown;
        # it is not interpreted as zero or as exhaustion until the normal iteration checkpoint.
        if (
            budget.max_total_tokens is not None
            and observed.total_tokens is not None
            and observed.total_tokens >= budget.max_total_tokens
        ):
            return TerminationReason.BUDGET_LIMIT, "maximum observed tokens reached during work"
        if (
            budget.max_cost_usd is not None
            and observed.cost_usd is not None
            and observed.cost_usd >= budget.max_cost_usd
        ):
            return TerminationReason.BUDGET_LIMIT, "maximum observed cost reached during work"
        return None

    def _phase_is_current(
        self,
        goal_id: str,
        iteration_id: str,
        expected_steer_version: int,
    ) -> bool:
        """Fence a restored phase checkpoint against guidance recorded after it."""

        with self.store.transaction() as connection:
            goal = GoalService._get_row(connection, goal_id)
            return not self._interrupt_stale_phase(
                connection,
                goal,
                iteration_id,
                expected_steer_version,
            )

    async def _verification_checkpoint(
        self,
        goal_id: str,
        iteration_id: str,
        results: tuple[ActionResult, ...],
        *,
        expected_steer_version: int,
    ) -> tuple[GoalVerification, int] | None:
        """Load or durably record the verifier decision for an iteration.

        Persisting the verifier decision before applying its outcome makes both evaluator-
        proposed completion and ordinary action verification restartable.  The steer version
        travels with the decision so a restored checkpoint cannot override newer guidance.
        """

        iteration = self._get_iteration(iteration_id)
        persisted = _loads(iteration["verification_json"])
        if persisted is not None:
            goal = self.goals.get_goal(goal_id)
            if "steerVersion" not in persisted and int(goal["steer_version"]) > 0:
                self._phase_is_current(goal_id, iteration_id, -1)
                return None
            return (
                _verification_from_dict(persisted),
                int(persisted.get("steerVersion", 0)),
            )
        if self._control_gate(goal_id):
            return None
        if not self._phase_is_current(goal_id, iteration_id, expected_steer_version):
            return None
        self._set_iteration_state(iteration_id, "verifying")
        context = self._context(goal_id)
        verification = await self.verifier.verify(context, results)
        self._assert_ownership()
        verification_steer_version = int(context.goal["steer_version"])
        if not self._record_verification(
            iteration_id,
            verification,
            expected_steer_version=verification_steer_version,
        ):
            return None
        return verification, verification_steer_version

    def _apply_verification_outcome(
        self,
        goal_id: str,
        iteration_id: str,
        verification: GoalVerification,
        *,
        failures: int,
        expected_steer_version: int,
    ) -> bool:
        """Apply a persisted verifier checkpoint idempotently after a restart."""

        if verification.satisfied:
            context = self._context(goal_id)
            active_task_states = {
                TaskState.DRAFT.value,
                TaskState.QUEUED.value,
                TaskState.READY.value,
                TaskState.RUNNING.value,
                TaskState.WAITING.value,
                TaskState.REVIEWING.value,
                TaskState.INTERRUPTED.value,
            }
            active_run_states = {
                RunState.STARTING.value,
                RunState.RUNNING.value,
                RunState.WAITING.value,
            }
            if any(task["state"] in active_task_states for task in context.tasks) or any(
                run["state"] in active_run_states for run in context.worker_runs
            ):
                # An LLM verifier cannot turn live canonical side effects into completed history.
                return False
            self._terminate_phase_if_current(
                goal_id,
                iteration_id,
                TerminationReason.SUCCESS,
                verification.summary,
                expected_steer_version=expected_steer_version,
            )
            return True
        if verification.termination_reason is not None:
            self._terminate_phase_if_current(
                goal_id,
                iteration_id,
                verification.termination_reason,
                verification.summary,
                expected_steer_version=expected_steer_version,
            )
            return True
        if not self._phase_is_current(goal_id, iteration_id, expected_steer_version):
            return True
        iteration = self._get_iteration(iteration_id)
        if iteration["state"] != "replanning":
            self._complete_replan(goal_id, iteration_id, verification, failures)
        self._finish_iteration(iteration_id, "completed")
        return True

    def _context(self, goal_id: str) -> GoalContext:
        goal = self.goals.get_goal(goal_id)
        project = self.store.get_project(goal["project_id"])
        with self.store.connect() as connection:
            iterations = tuple(
                dict(row)
                for row in connection.execute(
                    "SELECT * FROM autonomous_iterations WHERE goal_id=? ORDER BY sequence",
                    (goal_id,),
                ).fetchall()
            )
            actions = tuple(
                dict(row)
                for row in connection.execute(
                    "SELECT * FROM autonomous_actions WHERE goal_id=? ORDER BY created_at,id",
                    (goal_id,),
                ).fetchall()
            )
            steers = tuple(
                dict(row)
                for row in connection.execute(
                    "SELECT * FROM autonomous_steers WHERE goal_id=? ORDER BY sequence",
                    (goal_id,),
                ).fetchall()
            )
            task_id_rows = connection.execute(
                "SELECT task_id FROM autonomous_actions WHERE goal_id=? AND task_id IS NOT NULL "
                "UNION SELECT task_id FROM autonomy_decision_tasks WHERE goal_id=?",
                (goal_id, goal_id),
            ).fetchall()
            task_ids = tuple(sorted(str(row["task_id"]) for row in task_id_rows))
            event_entity_ids = (
                goal_id,
                *(str(item["id"]) for item in iterations),
                *(str(item["id"]) for item in actions),
            )
            event_placeholders = ",".join("?" for _ in event_entity_ids)
            if task_ids:
                placeholders = ",".join("?" for _ in task_ids)
                tasks = tuple(
                    dict(row)
                    for row in connection.execute(
                        f"SELECT * FROM tasks WHERE id IN ({placeholders}) "
                        "ORDER BY updated_at DESC,id",
                        task_ids,
                    ).fetchall()
                )
                worker_runs = tuple(
                    dict(row)
                    for row in connection.execute(
                        f"SELECT * FROM worker_runs WHERE task_id IN ({placeholders}) "
                        "ORDER BY created_at,id",
                        task_ids,
                    ).fetchall()
                )
                worker_results = tuple(
                    dict(row)
                    for row in connection.execute(
                        "SELECT result.* FROM worker_results result JOIN worker_runs r "
                        f"ON r.id=result.run_id WHERE r.task_id IN ({placeholders}) "
                        "ORDER BY result.created_at,result.run_id",
                        task_ids,
                    ).fetchall()
                )
                verifications = tuple(
                    dict(row)
                    for row in connection.execute(
                        f"SELECT * FROM verifications WHERE task_id IN ({placeholders}) "
                        "ORDER BY created_at,id",
                        task_ids,
                    ).fetchall()
                )
                failures = tuple(
                    dict(row)
                    for row in connection.execute(
                        f"SELECT * FROM failures WHERE task_id IN ({placeholders}) "
                        "ORDER BY created_at,id",
                        task_ids,
                    ).fetchall()
                )
                event_rows = connection.execute(
                    f"SELECT * FROM events WHERE entity_id IN ({event_placeholders}) "
                    f"OR task_id IN ({placeholders}) ORDER BY sequence DESC LIMIT 1000",
                    (*event_entity_ids, *task_ids),
                ).fetchall()
            else:
                tasks = ()
                worker_runs = ()
                worker_results = ()
                verifications = ()
                failures = ()
                event_rows = connection.execute(
                    f"SELECT * FROM events WHERE entity_id IN ({event_placeholders}) "
                    "ORDER BY sequence DESC LIMIT 1000",
                    event_entity_ids,
                ).fetchall()
            events: list[dict[str, Any]] = []
            for event_row in reversed(event_rows):
                item = dict(event_row)
                item["payload"] = json.loads(item.pop("payload_json"))
                events.append(item)
        return GoalContext(
            goal=goal,
            project=project,
            tasks=tasks,
            events=tuple(events),
            iterations=iterations,
            actions=actions,
            steers=steers,
            worker_runs=worker_runs,
            worker_results=worker_results,
            verifications=verifications,
            failures=failures,
        )

    def _start(self, goal_id: str) -> None:
        with self.store.transaction() as connection:
            goal = GoalService._get_row(connection, goal_id)
            if goal["state"] != GoalState.CREATED.value:
                return
            now = timestamp()
            connection.execute(
                "UPDATE autonomous_goals SET state=?,started_at=?,updated_at=?,version=version+1 "
                "WHERE id=?",
                (GoalState.RUNNING.value, now, now, goal_id),
            )
            self.store._append_event(
                connection,
                kind="goalStarted",
                severity=EventSeverity.NOTICE,
                entity_type="goal",
                entity_id=goal_id,
                project_id=goal["project_id"],
                summary="Autonomous iteration loop started",
                payload={"state": GoalState.RUNNING.value},
                actor="autonomy",
            )

    def _begin_iteration(self, goal_id: str) -> dict[str, Any]:
        with self.store.transaction() as connection:
            goal = GoalService._get_row(connection, goal_id)
            sequence = int(goal["iteration_count"]) + 1
            iteration_id = f"iter-{uuid.uuid4()}"
            now = timestamp()
            connection.execute(
                "INSERT INTO autonomous_iterations("
                "id,goal_id,sequence,state,started_at,updated_at) VALUES (?,?,?,?,?,?)",
                (iteration_id, goal_id, sequence, "evaluating", now, now),
            )
            connection.execute(
                "UPDATE autonomous_goals SET iteration_count=?,updated_at=?,version=version+1 "
                "WHERE id=?",
                (sequence, now, goal_id),
            )
            self.store._append_event(
                connection,
                kind="goalIterationStarted",
                severity=EventSeverity.INFO,
                entity_type="goalIteration",
                entity_id=iteration_id,
                project_id=goal["project_id"],
                summary=f"Autonomous goal iteration {sequence} started",
                payload={"goalID": goal_id, "sequence": sequence},
                actor="autonomy",
            )
        return self._get_iteration(iteration_id)

    def _record_evaluation(
        self,
        iteration_id: str,
        evaluation: GoalEvaluation,
        *,
        expected_steer_version: int,
    ) -> bool:
        iteration = self._get_iteration(iteration_id)
        with self.store.transaction() as connection:
            goal = GoalService._get_row(connection, iteration["goal_id"])
            if self._interrupt_stale_phase(connection, goal, iteration_id, expected_steer_version):
                return False
            repeated = goal["progress_fingerprint"] == evaluation.progress_fingerprint
            no_progress = int(goal["no_progress_count"]) + 1 if repeated else 0
            now = timestamp()
            payload = redact_sensitive(_evaluation_to_dict(evaluation))
            payload["steerVersion"] = expected_steer_version
            connection.execute(
                "UPDATE autonomous_iterations SET evaluation_json=?,progress_fingerprint=?,"
                "updated_at=? WHERE id=?",
                (compact_json(payload), evaluation.progress_fingerprint, now, iteration_id),
            )
            connection.execute(
                "UPDATE autonomous_goals SET progress_fingerprint=?,no_progress_count=?,"
                "last_evaluated_at=?,updated_at=?,version=version+1 WHERE id=?",
                (evaluation.progress_fingerprint, no_progress, now, now, goal["id"]),
            )
            self.store._append_event(
                connection,
                kind="goalEvaluated",
                severity=EventSeverity.INFO,
                entity_type="goalIteration",
                entity_id=iteration_id,
                project_id=goal["project_id"],
                summary=str(redact_sensitive(evaluation.summary)),
                payload=payload,
                actor="evaluator",
            )
        return True

    def _record_plan(
        self,
        iteration_id: str,
        goal_id: str,
        plan: GoalPlan,
        *,
        expected_steer_version: int,
    ) -> bool:
        keys = [action.key for action in plan.actions]
        if len(keys) != len(set(keys)):
            raise ValueError("planned action keys must be unique")
        safe = redact_sensitive(_plan_to_dict(plan))
        with self.store.transaction() as connection:
            goal = GoalService._get_row(connection, goal_id)
            if self._interrupt_stale_phase(connection, goal, iteration_id, expected_steer_version):
                return False
            now = timestamp()
            connection.execute(
                "UPDATE autonomous_iterations SET state='planned',plan_json=?,updated_at=? "
                "WHERE id=?",
                (compact_json(safe), now, iteration_id),
            )
            for ordinal, action in enumerate(plan.actions):
                action_id = f"act-{uuid.uuid5(uuid.NAMESPACE_URL, iteration_id + ':' + action.key)}"
                safe_title = str(redact_sensitive(action.title))
                safe_description = str(redact_sensitive(action.description))
                connection.execute(
                    "INSERT INTO autonomous_actions("
                    "id,goal_id,iteration_id,ordinal,action_key,title,description,role,payload_json,"
                    "state,created_at,updated_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
                    (
                        action_id,
                        goal_id,
                        iteration_id,
                        ordinal,
                        action.key,
                        safe_title,
                        safe_description,
                        action.role,
                        compact_json(redact_sensitive(action.payload)),
                        ActionState.PLANNED.value,
                        now,
                        now,
                    ),
                )
            connection.execute(
                "UPDATE autonomous_goals SET task_count=task_count+?,updated_at=?,"
                "version=version+1 WHERE id=?",
                (len(plan.actions), now, goal_id),
            )
            self.store._append_event(
                connection,
                kind="goalPlanCreated",
                severity=EventSeverity.INFO,
                entity_type="goalIteration",
                entity_id=iteration_id,
                project_id=goal["project_id"],
                summary=str(redact_sensitive(plan.summary)),
                payload=safe,
                actor="planner",
            )
        return True

    def _record_verification(
        self,
        iteration_id: str,
        verification: GoalVerification,
        *,
        expected_steer_version: int,
    ) -> bool:
        iteration = self._get_iteration(iteration_id)
        safe = redact_sensitive(_verification_to_dict(verification))
        safe["steerVersion"] = expected_steer_version
        with self.store.transaction() as connection:
            goal = GoalService._get_row(connection, iteration["goal_id"])
            if self._interrupt_stale_phase(connection, goal, iteration_id, expected_steer_version):
                return False
            connection.execute(
                "UPDATE autonomous_iterations SET verification_json=?,updated_at=? WHERE id=?",
                (compact_json(safe), timestamp(), iteration_id),
            )
            self.store._append_event(
                connection,
                kind="goalVerified",
                severity=(
                    EventSeverity.NOTICE if verification.satisfied else EventSeverity.WARNING
                ),
                entity_type="goalIteration",
                entity_id=iteration_id,
                project_id=goal["project_id"],
                summary=str(redact_sensitive(verification.summary)),
                payload=safe,
                actor="verifier",
            )
        return True

    def _interrupt_stale_phase(
        self,
        connection: Any,
        goal: dict[str, Any],
        iteration_id: str,
        expected_steer_version: int,
    ) -> bool:
        """Atomically fence a phase decision against the human-steer version."""

        current_steer_version = int(goal["steer_version"])
        if current_steer_version == expected_steer_version:
            return False
        now = timestamp()
        connection.execute(
            "UPDATE autonomous_iterations SET state='interrupted',completed_at=?,updated_at=? "
            "WHERE id=?",
            (now, now, iteration_id),
        )
        self.store._append_event(
            connection,
            kind="goalSteerReevaluationScheduled",
            severity=EventSeverity.NOTICE,
            entity_type="goalIteration",
            entity_id=iteration_id,
            project_id=goal["project_id"],
            summary="In-flight autonomy decision became stale after human steer",
            payload={
                "observedSteerVersion": expected_steer_version,
                "currentSteerVersion": current_steer_version,
                "preservedCanonicalWork": True,
            },
            actor="autonomy",
        )
        return True

    def _complete_replan(
        self,
        goal_id: str,
        iteration_id: str,
        verification: GoalVerification,
        failures: int,
    ) -> None:
        with self.store.transaction() as connection:
            goal = GoalService._get_row(connection, goal_id)
            # Verification records a newer checkpoint but does not count a second no-progress
            # observation for this iteration. The next evaluation performs that comparison.
            repeated = goal["progress_fingerprint"] == verification.progress_fingerprint
            no_progress = int(goal["no_progress_count"]) if repeated else 0
            now = timestamp()
            connection.execute(
                "UPDATE autonomous_goals SET progress_fingerprint=?,no_progress_count=?,"
                "failure_count=failure_count+?,updated_at=?,version=version+1 WHERE id=?",
                (verification.progress_fingerprint, no_progress, failures, now, goal_id),
            )
            connection.execute(
                "UPDATE autonomous_iterations SET state='replanning',updated_at=? WHERE id=?",
                (now, iteration_id),
            )
            self.store._append_event(
                connection,
                kind="goalReplanRequired",
                severity=EventSeverity.NOTICE,
                entity_type="goalIteration",
                entity_id=iteration_id,
                project_id=goal["project_id"],
                summary="Goal remains incomplete; next evaluation required",
                payload={
                    "progressFingerprint": verification.progress_fingerprint,
                    "failedActions": failures,
                },
                actor="autonomy",
            )

    def _terminate(self, goal_id: str, reason: TerminationReason, detail: str) -> dict[str, Any]:
        with self.store.transaction() as connection:
            goal = GoalService._get_row(connection, goal_id)
            if goal["state"] in {GoalState.TERMINATED.value, GoalState.STOPPED.value}:
                return goal
            now = timestamp()
            safe_detail = str(redact_sensitive(detail))
            connection.execute(
                "UPDATE autonomous_goals SET state=?,pause_mode=NULL,termination_reason=?,"
                "termination_detail=?,finished_at=?,updated_at=?,version=version+1 WHERE id=?",
                (
                    GoalState.TERMINATED.value,
                    reason.value,
                    safe_detail,
                    now,
                    now,
                    goal_id,
                ),
            )
            self.store._append_event(
                connection,
                kind="goalTerminated",
                severity=(
                    EventSeverity.NOTICE
                    if reason is TerminationReason.SUCCESS
                    else EventSeverity.WARNING
                ),
                entity_type="goal",
                entity_id=goal_id,
                project_id=goal["project_id"],
                summary=f"Autonomous goal terminated: {reason.value}",
                payload={"reason": reason.value, "detail": safe_detail},
                actor="autonomy",
            )
        return self.goals.get_goal(goal_id)

    def _terminate_phase_if_current(
        self,
        goal_id: str,
        iteration_id: str,
        reason: TerminationReason,
        detail: str,
        *,
        expected_steer_version: int,
    ) -> bool:
        """Atomically fence a phase termination against a concurrent human steer."""

        with self.store.transaction() as connection:
            goal = GoalService._get_row(connection, goal_id)
            if self._interrupt_stale_phase(connection, goal, iteration_id, expected_steer_version):
                return False
            if goal["state"] in {GoalState.TERMINATED.value, GoalState.STOPPED.value}:
                return True
            now = timestamp()
            safe_detail = str(redact_sensitive(detail))
            connection.execute(
                "UPDATE autonomous_goals SET state=?,pause_mode=NULL,termination_reason=?,"
                "termination_detail=?,finished_at=?,updated_at=?,version=version+1 WHERE id=?",
                (
                    GoalState.TERMINATED.value,
                    reason.value,
                    safe_detail,
                    now,
                    now,
                    goal_id,
                ),
            )
            connection.execute(
                "UPDATE autonomous_iterations SET state='completed',completed_at=?,updated_at=? "
                "WHERE id=?",
                (now, now, iteration_id),
            )
            self.store._append_event(
                connection,
                kind="goalTerminated",
                severity=(
                    EventSeverity.NOTICE
                    if reason is TerminationReason.SUCCESS
                    else EventSeverity.WARNING
                ),
                entity_type="goal",
                entity_id=goal_id,
                project_id=goal["project_id"],
                summary=f"Autonomous goal terminated: {reason.value}",
                payload={"reason": reason.value, "detail": safe_detail},
                actor="autonomy",
            )
        return True

    def _control_gate(self, goal_id: str) -> bool:
        return self.goals.get_goal(goal_id)["state"] != GoalState.RUNNING.value

    def _steer_changed(self, context: GoalContext) -> bool:
        current = self.goals.get_goal(str(context.goal["id"]))
        return int(current["steer_version"]) != int(context.goal["steer_version"])

    def _interrupt_for_steer(self, goal_id: str, iteration_id: str, context: GoalContext) -> None:
        """Discard only the stale phase decision and start a fresh persisted iteration.

        Work already committed by earlier phases remains canonical and visible to the next
        evaluation.  A human steer that arrives while a model decision is in flight must not be
        converted into NO_PROGRESS merely because that now-stale decision is malformed or empty.
        """

        with self.store.transaction() as connection:
            goal = GoalService._get_row(connection, goal_id)
            now = timestamp()
            connection.execute(
                "UPDATE autonomous_iterations SET state='interrupted',completed_at=?,updated_at=? "
                "WHERE id=?",
                (now, now, iteration_id),
            )
            self.store._append_event(
                connection,
                kind="goalSteerReevaluationScheduled",
                severity=EventSeverity.NOTICE,
                entity_type="goalIteration",
                entity_id=iteration_id,
                project_id=goal["project_id"],
                summary="In-flight autonomy decision became stale after human steer",
                payload={
                    "observedSteerVersion": int(context.goal["steer_version"]),
                    "currentSteerVersion": int(goal["steer_version"]),
                    "preservedCanonicalWork": True,
                },
                actor="autonomy",
            )

    def _active_iteration(self, goal_id: str) -> dict[str, Any] | None:
        with self.store.connect() as connection:
            row = connection.execute(
                "SELECT * FROM autonomous_iterations WHERE goal_id=? "
                "AND state NOT IN ('completed','interrupted') ORDER BY sequence DESC LIMIT 1",
                (goal_id,),
            ).fetchone()
        return dict(row) if row else None

    def _get_iteration(self, iteration_id: str) -> dict[str, Any]:
        with self.store.connect() as connection:
            row = connection.execute(
                "SELECT * FROM autonomous_iterations WHERE id=?", (iteration_id,)
            ).fetchone()
        if row is None:
            raise KeyError(iteration_id)
        return dict(row)

    def _list_actions(self, *, iteration_id: str) -> list[dict[str, Any]]:
        with self.store.connect() as connection:
            return [
                dict(row)
                for row in connection.execute(
                    "SELECT * FROM autonomous_actions WHERE iteration_id=? ORDER BY ordinal",
                    (iteration_id,),
                ).fetchall()
            ]

    def _set_iteration_state(self, iteration_id: str, state: str) -> None:
        with self.store.transaction() as connection:
            connection.execute(
                "UPDATE autonomous_iterations SET state=?,updated_at=? WHERE id=?",
                (state, timestamp(), iteration_id),
            )

    def _finish_iteration(self, iteration_id: str, state: str) -> None:
        with self.store.transaction() as connection:
            now = timestamp()
            connection.execute(
                "UPDATE autonomous_iterations SET state=?,completed_at=?,updated_at=? WHERE id=?",
                (state, now, now, iteration_id),
            )

    def _set_action(
        self,
        action_id: str,
        state: ActionState,
        *,
        handle: DispatchHandle | None = None,
        result: ActionResult | None = None,
        error: str | None = None,
    ) -> None:
        with self.store.transaction() as connection:
            row = connection.execute(
                "SELECT * FROM autonomous_actions WHERE id=?", (action_id,)
            ).fetchone()
            if row is None:
                raise KeyError(action_id)
            handle_json = compact_json(_handle_to_dict(handle)) if handle else None
            result_json = compact_json(redact_sensitive(asdict(result))) if result else None
            connection.execute(
                "UPDATE autonomous_actions SET state=?,task_id=COALESCE(?,task_id),"
                "dispatch_ref=COALESCE(?,dispatch_ref),result_json=COALESCE(?,result_json),"
                "error=COALESCE(?,error),updated_at=? WHERE id=?",
                (
                    state.value,
                    handle.task_id if handle else None,
                    handle_json,
                    result_json,
                    error,
                    timestamp(),
                    action_id,
                ),
            )
            goal = GoalService._get_row(connection, row["goal_id"])
            severity = (
                EventSeverity.INFO if state is not ActionState.FAILED else EventSeverity.WARNING
            )
            self.store._append_event(
                connection,
                kind="goalActionStateChanged",
                severity=severity,
                entity_type="goalAction",
                entity_id=action_id,
                project_id=goal["project_id"],
                task_id=handle.task_id if handle else row["task_id"],
                summary=f"Goal action entered {state.value}",
                payload={"state": state.value, "error": error},
                actor="autonomy",
            )

    def _journal(self, goal_id: str, kind: str, summary: str, payload: dict[str, Any]) -> None:
        with self.store.transaction() as connection:
            goal = GoalService._get_row(connection, goal_id)
            self.store._append_event(
                connection,
                kind=kind,
                severity=EventSeverity.NOTICE,
                entity_type="goal",
                entity_id=goal_id,
                project_id=goal["project_id"],
                summary=summary,
                payload=redact_sensitive(payload),
                actor="autonomy",
            )


class SupervisorRuntimeDispatcher:
    """Hybrid Engine seam backed by SupervisorRuntime and deterministic task identities."""

    def __init__(self, runtime: Any) -> None:
        self.runtime = runtime

    async def dispatch(
        self, context: GoalContext, action: PlannedAction, action_id: str
    ) -> DispatchHandle:
        task_id = f"tsk-{uuid.uuid5(uuid.NAMESPACE_URL, action_id)}"
        try:
            self.runtime.store.get_task(task_id)
        except KeyError:
            await self.runtime.submit_task(
                project_id=context.goal["project_id"],
                title=action.title,
                description=action.description,
                requirements=_requirements_from_payload(action.payload),
                topology=ExecutionTopology(action.payload.get("topology", "single")),
                priority=int(action.payload.get("priority", 50)),
                task_id=task_id,
                reference=f"AUTO-{action_id[-12:]}",
            )
        await self.runtime.dispatch_ready(task_ids={task_id})
        return DispatchHandle(reference=task_id, task_id=task_id)

    async def collect(self, handle: DispatchHandle) -> ActionResult:
        if handle.task_id is None:
            raise ValueError("runtime dispatch handle has no task id")
        scope = {handle.task_id}
        while True:
            # A different Runtime may still own this Task, or its crash lease may be awaiting
            # expiry. Reconcile on every bounded poll; a valid live lease is never interrupted.
            await self.runtime.recover(task_ids=scope)
            await self.runtime.dispatch_ready(task_ids=scope)
            await self.runtime.wait_for_active(task_ids=scope)
            task = self.runtime.store.get_task(handle.task_id)
            if task["state"] not in {
                TaskState.READY.value,
                TaskState.RUNNING.value,
                TaskState.WAITING.value,
                TaskState.INTERRUPTED.value,
            }:
                break
            # This bounded signal wait covers both Worker capacity and foreign execution leases
            # without busy-spinning the event journal or SQLite routing state.
            await self.runtime.wait_for_dispatch_capacity(timeout_seconds=1.0)
        runs = self.runtime.store.list_worker_runs(handle.task_id)
        summaries: list[str] = []
        for run in runs:
            with self.runtime.store.connect() as connection:
                result = connection.execute(
                    "SELECT summary FROM worker_results WHERE run_id=?", (run["id"],)
                ).fetchone()
            if result:
                summaries.append(result["summary"])
        succeeded = task["state"] in {TaskState.REVIEWING.value, TaskState.SUCCEEDED.value}
        if task["state"] == TaskState.REVIEWING.value:
            verified_attempt = max((int(run["attempt"]) for run in runs), default=0)
            attempt_runs = [run for run in runs if int(run["attempt"]) == verified_attempt]
            exit_evidence = [
                {
                    "runID": run["id"],
                    "attempt": int(run["attempt"]),
                    "state": run["state"],
                    "exitCode": run["exit_code"],
                    "considered": int(run["attempt"]) == verified_attempt,
                }
                for run in runs
            ]
            exit_policy_passed = bool(attempt_runs) and all(
                run["state"] == RunState.COMPLETED.value and run["exit_code"] == 0
                for run in attempt_runs
            )
            self.runtime.store.record_verification(
                task_id=handle.task_id,
                kind="workerExitPolicy",
                passed=exit_policy_passed,
                evidence={"verifiedAttempt": verified_attempt, "runs": exit_evidence},
                verifier="runtime-exit-policy",
            )
            if not exit_policy_passed:
                return ActionResult(
                    succeeded=False,
                    summary="Worker exit policy did not verify execution success",
                    payload={"taskID": handle.task_id, "taskState": task["state"]},
                )
            if task.get("current_verification_scope_id") is not None:
                return ActionResult(
                    succeeded=False,
                    summary=(
                        "Worker exit policy passed; Task remains REVIEWING until its current "
                        "version-scoped acceptance criteria are verified"
                    ),
                    payload={
                        "taskID": handle.task_id,
                        "taskState": task["state"],
                        "verificationScopeID": task["current_verification_scope_id"],
                        "taskDefinitionRevision": task.get("definition_revision"),
                    },
                )
            task = self.runtime.store.transition_task(
                handle.task_id,
                TaskState.SUCCEEDED,
                actor="autonomy",
                summary=("Worker exit policy verified; Goal verification remains separate"),
            )
            await self.runtime.audit_task_terminal(handle.task_id, task=task)
        return ActionResult(
            succeeded=succeeded,
            summary="\n".join(summaries) or f"task ended in {task['state']}",
            payload={"taskID": handle.task_id, "taskState": task["state"]},
            cancelled=task["state"] == TaskState.CANCELLED.value,
        )

    async def cancel(self, handle: DispatchHandle) -> bool:
        return bool(handle.task_id and await self.runtime.cancel_task(handle.task_id))

    async def recover(
        self, context: GoalContext, action: PlannedAction, handle: DispatchHandle
    ) -> DispatchHandle:
        del context, action
        await self.runtime.recover(task_ids={handle.task_id} if handle.task_id else set())
        await self.runtime.dispatch_ready(task_ids={handle.task_id} if handle.task_id else set())
        return handle


def _requirements_from_payload(payload: dict[str, Any]) -> TaskRequirements:
    permission_class = PermissionClass(payload.get("permissionClass", "green"))
    return TaskRequirements(
        labels=frozenset(TaskLabel(value) for value in payload.get("labels", ["research"])),
        required_capabilities=frozenset(payload.get("requiredCapabilities", [])),
        permission_class=permission_class,
        # A planner can request RED work, but it can never assert human approval for itself.
        approval_state=(
            ApprovalState.PENDING
            if permission_class is PermissionClass.RED
            else ApprovalState.NOT_REQUIRED
        ),
        minimum_context_tokens=payload.get("minimumContextTokens"),
        privacy_sensitive=bool(payload.get("privacySensitive", False)),
        code_write_required=bool(payload.get("codeWriteRequired", False)),
        panel_size=int(payload.get("panelSize", 2)),
        preferred_workers=tuple(payload.get("preferredWorkers", [])),
    )


def _loads(value: str | None) -> dict[str, Any] | None:
    return json.loads(value) if value else None


def _evaluation_to_dict(value: GoalEvaluation) -> dict[str, Any]:
    return {
        "disposition": value.disposition.value,
        "summary": value.summary,
        "progressFingerprint": value.progress_fingerprint,
        "terminationReason": value.termination_reason.value if value.termination_reason else None,
        "facts": redact_sensitive(value.facts),
    }


def _evaluation_from_dict(value: dict[str, Any]) -> GoalEvaluation:
    reason = value.get("terminationReason")
    return GoalEvaluation(
        disposition=EvaluationDisposition(value["disposition"]),
        summary=value["summary"],
        progress_fingerprint=value["progressFingerprint"],
        termination_reason=TerminationReason(reason) if reason else None,
        facts=value.get("facts", {}),
    )


def _plan_to_dict(value: GoalPlan) -> dict[str, Any]:
    return {
        "summary": value.summary,
        "rationale": value.rationale,
        "actions": [
            {
                "key": action.key,
                "title": action.title,
                "description": action.description,
                "role": action.role,
                "payload": redact_sensitive(action.payload),
            }
            for action in value.actions
        ],
    }


def _verification_to_dict(value: GoalVerification) -> dict[str, Any]:
    return {
        "satisfied": value.satisfied,
        "summary": value.summary,
        "progressFingerprint": value.progress_fingerprint,
        "terminationReason": value.termination_reason.value if value.termination_reason else None,
        "evidence": redact_sensitive(value.evidence),
    }


def _verification_from_dict(value: dict[str, Any]) -> GoalVerification:
    reason = value.get("terminationReason")
    return GoalVerification(
        satisfied=bool(value["satisfied"]),
        summary=value["summary"],
        progress_fingerprint=value["progressFingerprint"],
        termination_reason=TerminationReason(reason) if reason else None,
        evidence=value.get("evidence", {}),
    )


def _action_from_row(row: dict[str, Any]) -> PlannedAction:
    return PlannedAction(
        key=row["action_key"],
        title=row["title"],
        description=row["description"],
        role=row["role"],
        payload=json.loads(row["payload_json"]),
    )


def _handle_to_dict(handle: DispatchHandle) -> dict[str, Any]:
    return {
        "reference": handle.reference,
        "taskID": handle.task_id,
        "metadata": redact_sensitive(handle.metadata),
    }


def _handle_from_row(row: dict[str, Any]) -> DispatchHandle | None:
    if not row["dispatch_ref"]:
        return None
    value = json.loads(row["dispatch_ref"])
    return DispatchHandle(
        reference=value["reference"],
        task_id=value.get("taskID"),
        metadata=value.get("metadata", {}),
    )


def _result_from_action(row: dict[str, Any]) -> ActionResult:
    if row["result_json"]:
        value = json.loads(row["result_json"])
        return ActionResult(
            succeeded=bool(value["succeeded"]),
            summary=value["summary"],
            payload=value.get("payload", {}),
            cancelled=bool(value.get("cancelled", False)),
        )
    return ActionResult(
        succeeded=False,
        summary=row["error"] or f"action ended in {row['state']}",
        cancelled=row["state"] == ActionState.CANCELLED.value,
    )
