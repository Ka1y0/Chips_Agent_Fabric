from __future__ import annotations

import json
from pathlib import Path

import jsonschema

from project_supervisor.domain import (
    ExecutionTopology,
    Harness,
    ModelDescriptor,
    NodeState,
    Provider,
    ResourceState,
    TaskLabel,
    TaskRequirements,
    WorkerSnapshot,
    WorkerState,
)
from project_supervisor.hybrid import ExecutionHistoryRecord, RoutingInputSnapshot
from project_supervisor.scheduler import DeterministicScheduler


def snapshot_worker(worker_id: str) -> WorkerSnapshot:
    return WorkerSnapshot(
        id=worker_id,
        node_id="node-1",
        harness=Harness.MOCK,
        provider=Provider.MOCK,
        model=ModelDescriptor("mock-v1", "Mock V1", Provider.MOCK, context_window_tokens=4096),
        state=WorkerState.IDLE,
        node_state=NodeState.ONLINE,
        resource_state=ResourceState.UNKNOWN,
        capabilities=frozenset({"review"}),
        code_write_allowed=False,
        privacy_allowed=True,
    )


def test_routing_snapshot_is_deterministic_explainable_and_non_adaptive() -> None:
    snapshot = RoutingInputSnapshot(
        snapshot_id="snapshot-1",
        task_id="task-1",
        requirements=TaskRequirements(
            labels=frozenset({TaskLabel.REVIEW}),
            required_capabilities=frozenset({"review"}),
        ),
        topology=ExecutionTopology.SINGLE,
        workers=(snapshot_worker("worker-b"), snapshot_worker("worker-a")),
        facts={"networkPrivate": True, "queueDepth": 0},
    )
    scheduler = DeterministicScheduler()
    first = scheduler.schedule_snapshot(snapshot)
    second = scheduler.schedule_snapshot(snapshot)
    assert first == second
    assert first.selected_worker_ids == ("worker-a",)
    assert first.explanation["routingInput"]["adaptiveLearning"] is False
    assert first.explanation["routingInput"]["facts"] == {
        "networkPrivate": True,
        "queueDepth": 0,
    }


def test_execution_history_protocol_is_normalized_and_schema_valid() -> None:
    record = ExecutionHistoryRecord(
        id="history-1",
        task_id="task-1",
        task_type="review",
        worker_id="worker-1",
        provider="provider-independent",
        model="model-1",
        node_id="node-1",
        topology=ExecutionTopology.PRIMARY_REVIEWER,
        latency_seconds=1.25,
        succeeded=True,
        retry_count=1,
        input_tokens=10,
        output_tokens=4,
        cost_usd=0.01,
        review_outcome="accepted",
        human_accepted=True,
    )
    schema = json.loads(
        (Path(__file__).parents[1] / "schemas/execution-history-v1.schema.json").read_text()
    )
    jsonschema.Draft202012Validator(schema, format_checker=jsonschema.FormatChecker()).validate(
        record.to_protocol()
    )
