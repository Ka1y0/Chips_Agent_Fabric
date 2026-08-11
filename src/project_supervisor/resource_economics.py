from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Protocol

from .domain import UnavailableReason
from .resource_usage import UsageMetric, UsageProvenance


class ResourceEconomicsStore(Protocol):
    def connect(self): ...


@dataclass(frozen=True, slots=True)
class TokenCoverage:
    invocation_count: int
    complete_invocation_count: int
    unknown_invocation_count: int
    successful_goals_with_complete_telemetry: int
    accepted_tasks_with_complete_telemetry: int
    codex_call_count: int
    offloaded_call_count: int
    unknown_executor_call_count: int
    scored_call_count: int
    unscored_call_count: int

    def to_protocol(self) -> dict[str, int]:
        return {
            "invocationCount": self.invocation_count,
            "completeInvocationCount": self.complete_invocation_count,
            "unknownInvocationCount": self.unknown_invocation_count,
            "successfulGoalsWithCompleteTelemetry": self.successful_goals_with_complete_telemetry,
            "acceptedTasksWithCompleteTelemetry": self.accepted_tasks_with_complete_telemetry,
            "codexCallCount": self.codex_call_count,
            "offloadedCallCount": self.offloaded_call_count,
            "unknownExecutorCallCount": self.unknown_executor_call_count,
            "scoredCallCount": self.scored_call_count,
            "unscoredCallCount": self.unscored_call_count,
        }


@dataclass(frozen=True, slots=True)
class ResourceEconomicsAggregate:
    project_id: str | None
    goal_id: str | None
    codex_offload_ratio: UsageMetric
    quality_adjusted_offload: UsageMetric
    tokens_per_successful_goal: UsageMetric
    tokens_per_accepted_task: UsageMetric
    successful_goal_count: int
    accepted_task_count: int
    coverage: TokenCoverage

    def to_protocol(self) -> dict[str, Any]:
        return {
            "scope": {"projectID": self.project_id, "goalID": self.goal_id},
            "tokenDefinition": "inputPlusOutput",
            "codexOffloadRatio": self.codex_offload_ratio.to_protocol(),
            "qualityAdjustedOffload": self.quality_adjusted_offload.to_protocol(),
            "tokensPerSuccessfulGoal": self.tokens_per_successful_goal.to_protocol(),
            "tokensPerAcceptedTask": self.tokens_per_accepted_task.to_protocol(),
            "successfulGoalCount": self.successful_goal_count,
            "acceptedTaskCount": self.accepted_task_count,
            "coverage": self.coverage.to_protocol(),
        }


class ResourceEconomicsRepository:
    """Derived resource economics with strict complete-coverage UNKNOWN semantics.

    Input plus output tokens are used; cache counters are intentionally separate and are not added
    again. A per-outcome rate is known only when every eligible Goal/Task has at least one
    invocation and every included invocation reports both token dimensions.
    """

    def __init__(self, store: ResourceEconomicsStore) -> None:
        self.store = store

    def aggregate(
        self, *, project_id: str | None = None, goal_id: str | None = None
    ) -> ResourceEconomicsAggregate:
        if goal_id is not None:
            with self.store.connect() as connection:
                row = connection.execute(
                    "SELECT project_id FROM autonomous_goals WHERE id=?", (goal_id,)
                ).fetchone()
            if row is None:
                raise KeyError(goal_id)
            actual_project = str(row["project_id"])
            if project_id is not None and project_id != actual_project:
                raise ValueError("goal does not belong to requested project")
            project_id = actual_project

        classification = self._classification(project_id=project_id, goal_id=goal_id)
        classified_count = classification["codex"] + classification["offloaded"]
        offload_value = classification["offloaded"] / classified_count if classified_count else None
        quality_value = (
            classification["offloaded_quality"] / classification["total_quality"]
            if classification["total_quality"] > 0
            else None
        )
        codex_offload = _economics_metric(
            offload_value,
            provenance=UsageProvenance.LOCALLY_MEASURED,
        )
        quality_offload = _economics_metric(
            quality_value,
            provenance=UsageProvenance.INFERRED,
        )

        rows = self._token_rows(project_id=project_id, goal_id=goal_id)
        successful_goal_ids = self._successful_goal_ids(project_id=project_id, goal_id=goal_id)
        accepted_task_ids = self._accepted_task_ids(project_id=project_id, goal_id=goal_id)
        complete_rows = [row for row in rows if _row_tokens(row) is not None]
        complete_goal_ids = {
            identifier
            for identifier in successful_goal_ids
            if _entity_complete(rows, "goal_id", identifier)
        }
        complete_task_ids = {
            identifier
            for identifier in accepted_task_ids
            if _entity_complete(rows, "task_id", identifier)
        }
        tokens_per_goal = _tokens_per_entity(rows, "goal_id", successful_goal_ids)
        tokens_per_task = _tokens_per_entity(rows, "task_id", accepted_task_ids)
        return ResourceEconomicsAggregate(
            project_id=project_id,
            goal_id=goal_id,
            codex_offload_ratio=codex_offload,
            quality_adjusted_offload=quality_offload,
            tokens_per_successful_goal=tokens_per_goal,
            tokens_per_accepted_task=tokens_per_task,
            successful_goal_count=len(successful_goal_ids),
            accepted_task_count=len(accepted_task_ids),
            coverage=TokenCoverage(
                invocation_count=len(rows),
                complete_invocation_count=len(complete_rows),
                unknown_invocation_count=len(rows) - len(complete_rows),
                successful_goals_with_complete_telemetry=len(complete_goal_ids),
                accepted_tasks_with_complete_telemetry=len(complete_task_ids),
                codex_call_count=classification["codex"],
                offloaded_call_count=classification["offloaded"],
                unknown_executor_call_count=classification["unknown"],
                scored_call_count=classification["scored"],
                unscored_call_count=classification["total"] - classification["scored"],
            ),
        )

    def _classification(
        self, *, project_id: str | None, goal_id: str | None
    ) -> dict[str, int | float]:
        clauses: list[str] = []
        parameters: list[str] = []
        if project_id is not None:
            clauses.append("t.project_id=?")
            parameters.append(project_id)
        if goal_id is not None:
            clauses.append("i.goal_id=?")
            parameters.append(goal_id)
        query = (
            "SELECT COUNT(*) AS total,"
            "SUM(CASE WHEN i.executor_kind='codex' THEN 1 ELSE 0 END) AS codex,"
            "SUM(CASE WHEN i.executor_kind='offloaded' THEN 1 ELSE 0 END) AS offloaded,"
            "SUM(CASE WHEN i.executor_kind='unknown' THEN 1 ELSE 0 END) AS unknown,"
            "SUM(CASE WHEN i.executor_kind<>'unknown' AND i.quality_score_value IS NOT NULL "
            "THEN 1 ELSE 0 END) AS scored,"
            "COALESCE(SUM(CASE WHEN i.executor_kind<>'unknown' "
            "THEN i.quality_score_value ELSE 0 END),0) AS total_quality,"
            "COALESCE(SUM(CASE WHEN i.executor_kind='offloaded' "
            "THEN i.quality_score_value ELSE 0 END),0) AS offloaded_quality "
            "FROM invocation_telemetry i JOIN tasks t ON t.id=i.task_id"
        )
        if clauses:
            query += " WHERE " + " AND ".join(clauses)
        with self.store.connect() as connection:
            row = connection.execute(query, parameters).fetchone()
        return {
            "total": int(row["total"]),
            "codex": int(row["codex"] or 0),
            "offloaded": int(row["offloaded"] or 0),
            "unknown": int(row["unknown"] or 0),
            "scored": int(row["scored"] or 0),
            "total_quality": float(row["total_quality"]),
            "offloaded_quality": float(row["offloaded_quality"]),
        }

    def _token_rows(self, *, project_id: str | None, goal_id: str | None) -> list[dict[str, Any]]:
        clauses: list[str] = []
        parameters: list[str] = []
        if project_id is not None:
            clauses.append("t.project_id=?")
            parameters.append(project_id)
        if goal_id is not None:
            clauses.append("i.goal_id=?")
            parameters.append(goal_id)
        query = (
            "SELECT i.goal_id,i.task_id,i.input_tokens_value,i.output_tokens_value "
            "FROM invocation_telemetry i JOIN tasks t ON t.id=i.task_id"
        )
        if clauses:
            query += " WHERE " + " AND ".join(clauses)
        query += " ORDER BY i.recorded_at,i.id"
        with self.store.connect() as connection:
            return [dict(row) for row in connection.execute(query, parameters).fetchall()]

    def _successful_goal_ids(self, *, project_id: str | None, goal_id: str | None) -> set[str]:
        clauses = ["state='terminated'", "termination_reason='SUCCESS'"]
        parameters: list[str] = []
        if project_id is not None:
            clauses.append("project_id=?")
            parameters.append(project_id)
        if goal_id is not None:
            clauses.append("id=?")
            parameters.append(goal_id)
        with self.store.connect() as connection:
            rows = connection.execute(
                "SELECT id FROM autonomous_goals WHERE " + " AND ".join(clauses), parameters
            ).fetchall()
        return {str(row["id"]) for row in rows}

    def _accepted_task_ids(self, *, project_id: str | None, goal_id: str | None) -> set[str]:
        clauses = ["t.state='succeeded'"]
        parameters: list[str] = []
        if project_id is not None:
            clauses.append("t.project_id=?")
            parameters.append(project_id)
        if goal_id is not None:
            clauses.append(
                "(EXISTS(SELECT 1 FROM autonomous_actions a WHERE a.task_id=t.id AND a.goal_id=?) "
                "OR EXISTS(SELECT 1 FROM autonomy_decision_tasks d "
                "WHERE d.task_id=t.id AND d.goal_id=?))"
            )
            parameters.extend((goal_id, goal_id))
        with self.store.connect() as connection:
            rows = connection.execute(
                "SELECT t.id FROM tasks t WHERE " + " AND ".join(clauses), parameters
            ).fetchall()
        return {str(row["id"]) for row in rows}


def _row_tokens(row: dict[str, Any]) -> float | None:
    input_tokens = row["input_tokens_value"]
    output_tokens = row["output_tokens_value"]
    if input_tokens is None or output_tokens is None:
        return None
    return float(input_tokens) + float(output_tokens)


def _entity_complete(rows: list[dict[str, Any]], key: str, identifier: str) -> bool:
    matching = [row for row in rows if row[key] == identifier]
    return bool(matching) and all(_row_tokens(row) is not None for row in matching)


def _tokens_per_entity(rows: list[dict[str, Any]], key: str, identifiers: set[str]) -> UsageMetric:
    if not identifiers or not all(_entity_complete(rows, key, item) for item in identifiers):
        return UsageMetric.unknown(UnavailableReason.NOT_REPORTED)
    relevant = [row for row in rows if row[key] in identifiers]
    total = sum(_row_tokens(row) or 0 for row in relevant)
    return UsageMetric.known(total / len(identifiers), "tokens", UsageProvenance.INFERRED)


def _economics_metric(value: int | float | None, *, provenance: UsageProvenance) -> UsageMetric:
    if value is None:
        return UsageMetric.unknown(UnavailableReason.NOT_REPORTED)
    return UsageMetric.known(value, "ratio", provenance)
