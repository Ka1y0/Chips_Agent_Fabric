from __future__ import annotations

import asyncio
import json
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .adapters.base import (
    UnsafeWorkerRequest,
    WorkerAdapter,
    WorkerEvent,
    WorkerRequest,
    WorkerResult,
    WorkerUnavailable,
)
from .domain import (
    ApprovalState,
    EvidenceConfidence,
    ExecutionTopology,
    FailureClass,
    ModelDescriptor,
    PermissionClass,
    Provider,
    ResourceState,
    RunState,
    TaskLabel,
    TaskRecord,
    TaskRequirements,
    TaskState,
    TelemetryValue,
    UnavailableReason,
    WorkerState,
)
from .scheduler import DeterministicScheduler
from .store import StateStore
from .verification import DefinitionOfDoneResult


@dataclass(frozen=True, slots=True)
class DispatchSummary:
    launched_task_ids: tuple[str, ...]
    blocked_task_ids: tuple[str, ...]


class AdapterRegistry:
    def __init__(self) -> None:
        self._adapters: dict[str, WorkerAdapter] = {}

    def register(self, worker_id: str, adapter: WorkerAdapter) -> None:
        if worker_id in self._adapters:
            raise ValueError(f"adapter already registered for {worker_id}")
        self._adapters[worker_id] = adapter

    def get(self, worker_id: str) -> WorkerAdapter:
        try:
            return self._adapters[worker_id]
        except KeyError as error:
            raise KeyError(f"no adapter registered for worker {worker_id}") from error

    def contains(self, worker_id: str) -> bool:
        return worker_id in self._adapters


class SupervisorRuntime:
    """Dispatch work while SQLite remains the canonical, recoverable source of truth."""

    def __init__(
        self,
        *,
        store: StateStore,
        scheduler: DeterministicScheduler,
        adapters: AdapterRegistry,
        evidence_root: str | Path,
        max_attempts: int = 2,
    ) -> None:
        self.store = store
        self.scheduler = scheduler
        self.adapters = adapters
        self.evidence_root = Path(evidence_root).resolve()
        self.evidence_root.mkdir(parents=True, exist_ok=True)
        if max_attempts < 1:
            raise ValueError("max_attempts must be positive")
        self.max_attempts = max_attempts
        self._active: dict[str, asyncio.Task[None]] = {}
        self._dispatch_lock = asyncio.Lock()

    async def recover(self) -> dict[str, int]:
        result = await asyncio.to_thread(self.store.recover_interrupted)
        for task in await asyncio.to_thread(self.store.list_tasks):
            if task["state"] == TaskState.INTERRUPTED.value:
                await asyncio.to_thread(
                    self.store.transition_task,
                    task["id"],
                    TaskState.READY,
                    actor="recovery",
                    summary="Interrupted task requeued by recovery policy",
                )
        return result

    async def submit_task(
        self,
        *,
        project_id: str,
        title: str,
        description: str,
        requirements: TaskRequirements,
        topology: ExecutionTopology = ExecutionTopology.SINGLE,
        priority: int = 50,
        task_id: str | None = None,
        reference: str | None = None,
    ) -> str:
        task_id = task_id or f"tsk-{uuid.uuid4()}"
        if reference is None:
            existing = await asyncio.to_thread(self.store.list_tasks, project_id)
            reference = f"#{len(existing) + 1:04d}"
        record = TaskRecord(
            id=task_id,
            project_id=project_id,
            title=title,
            description=description,
            state=TaskState.DRAFT,
            topology=topology,
            requirements=requirements,
            priority=priority,
        )
        await asyncio.to_thread(self.store.create_task, record, reference)
        await asyncio.to_thread(self.store.transition_task, task_id, TaskState.QUEUED)
        if requirements.permission_class is PermissionClass.RED and (
            requirements.approval_state is not ApprovalState.APPROVED
        ):
            await asyncio.to_thread(
                self.store.request_approval,
                project_id=project_id,
                task_id=task_id,
                permission_class=requirements.permission_class.value,
                action_type="executeTask",
                action_payload={"taskID": task_id, "title": title},
                requested_by="runtime",
                reason="RED permission class requires explicit human approval",
            )
            await asyncio.to_thread(
                self.store.transition_task,
                task_id,
                TaskState.BLOCKED,
                summary="Task blocked on mandatory human approval",
                payload={"reasonCode": "HUMAN_APPROVAL_REQUIRED"},
            )
        else:
            await asyncio.to_thread(self.store.transition_task, task_id, TaskState.READY)
        return task_id

    async def dispatch_ready(self) -> DispatchSummary:
        launched: list[str] = []
        blocked: list[str] = []
        async with self._dispatch_lock:
            tasks = await asyncio.to_thread(self.store.list_tasks)
            snapshots = await asyncio.to_thread(self.store.worker_snapshots)
            for task in tasks:
                task_id = task["id"]
                if task["state"] != TaskState.READY.value or task_id in self._active:
                    continue
                dependencies = await asyncio.to_thread(self.store.unsatisfied_dependencies, task_id)
                if dependencies:
                    continue
                requirements = self._requirements(task)
                topology = ExecutionTopology(task["topology"])
                candidates = [worker for worker in snapshots if self.adapters.contains(worker.id)]
                if topology in {
                    ExecutionTopology.FALLBACK,
                    ExecutionTopology.CHEAP_FIRST_ESCALATION,
                }:
                    prior_runs = await asyncio.to_thread(self.store.list_worker_runs, task_id)
                    failed_workers = {
                        run["worker_id"]
                        for run in prior_runs
                        if run["state"]
                        in {
                            RunState.FAILED.value,
                            RunState.TIMED_OUT.value,
                            RunState.AUTH_REQUIRED.value,
                            RunState.RATE_LIMITED.value,
                        }
                    }
                    candidates = [
                        worker for worker in candidates if worker.id not in failed_workers
                    ]
                decision = self.scheduler.schedule(
                    task_id=task_id,
                    requirements=requirements,
                    topology=topology,
                    workers=candidates,
                )
                await asyncio.to_thread(
                    self.store.persist_routing_decision,
                    task_id=task_id,
                    decision=decision,
                )
                if not decision.selected_worker_ids:
                    await asyncio.to_thread(
                        self.store.transition_task,
                        task_id,
                        TaskState.BLOCKED,
                        summary="No eligible worker",
                        payload={"rejected": decision.explanation["rejected"]},
                    )
                    blocked.append(task_id)
                    continue
                await asyncio.to_thread(self.store.transition_task, task_id, TaskState.RUNNING)
                active = asyncio.create_task(
                    self._execute_task(task, requirements, decision.selected_worker_ids),
                    name=f"supervisor:{task_id}",
                )
                self._active[task_id] = active
                active.add_done_callback(
                    lambda _done, identity=task_id: self._active.pop(identity, None)
                )
                launched.append(task_id)
        return DispatchSummary(tuple(launched), tuple(blocked))

    async def wait_for_active(self) -> None:
        while self._active:
            await asyncio.gather(*tuple(self._active.values()))

    async def run_until_idle(self) -> None:
        while True:
            summary = await self.dispatch_ready()
            if self._active:
                await self.wait_for_active()
                continue
            if not summary.launched_task_ids:
                return

    async def apply_verification(self, task_id: str, result: DefinitionOfDoneResult) -> TaskState:
        task = await asyncio.to_thread(self.store.get_task, task_id)
        if task["state"] != TaskState.REVIEWING.value:
            raise RuntimeError(f"task {task_id} is not awaiting verification")
        criteria = await asyncio.to_thread(self.store.list_acceptance_criteria, task["project_id"])
        criteria_by_id = {criterion["id"]: criterion for criterion in criteria}
        for item in result.results:
            criterion = criteria_by_id.get(item.criterion_id)
            command = None
            if criterion and criterion["command_json"]:
                command = json.loads(criterion["command_json"])
            await asyncio.to_thread(
                self.store.record_verification,
                task_id=task_id,
                criterion_id=item.criterion_id if criterion else None,
                kind=criterion["kind"] if criterion else "definitionOfDone",
                command=command,
                exit_code=item.exit_code,
                passed=item.passed,
                evidence={"summary": item.summary, **item.evidence},
                verifier="deterministic-verifier",
            )
        if result.complete:
            await asyncio.to_thread(
                self.store.transition_task,
                task_id,
                TaskState.SUCCEEDED,
                summary="Definition of Done satisfied",
                payload={"criteria": [item.criterion_id for item in result.results]},
            )
            return TaskState.SUCCEEDED
        await asyncio.to_thread(
            self.store.transition_task,
            task_id,
            TaskState.READY,
            summary="Verification failed; task returned to ready queue",
            payload={"requiredFailures": list(result.required_failures)},
        )
        return TaskState.READY

    async def cancel_task(self, task_id: str) -> bool:
        runs = await asyncio.to_thread(self.store.list_worker_runs, task_id)
        requested = False
        for run in runs:
            if run["state"] not in {
                RunState.STARTING.value,
                RunState.RUNNING.value,
                RunState.WAITING.value,
            }:
                continue
            adapter = self.adapters.get(run["worker_id"])
            requested = await adapter.cancel(run["id"]) or requested
        task = await asyncio.to_thread(self.store.get_task, task_id)
        if task["state"] not in {
            TaskState.SUCCEEDED.value,
            TaskState.FAILED.value,
            TaskState.CANCELLED.value,
        }:
            await asyncio.to_thread(
                self.store.transition_task,
                task_id,
                TaskState.CANCELLED,
                summary="Task cancellation requested",
            )
            requested = True
        return requested

    async def _execute_task(
        self,
        task: dict[str, Any],
        requirements: TaskRequirements,
        worker_ids: tuple[str, ...],
    ) -> None:
        project = await asyncio.to_thread(self.store.get_project, task["project_id"])
        coroutines = [
            self._execute_worker(
                task=task,
                project=project,
                requirements=requirements,
                worker_id=worker_id,
                attempt=task["attempt_count"] + 1,
            )
            for worker_id in worker_ids
        ]
        results = await asyncio.gather(*coroutines, return_exceptions=True)
        succeeded = all(isinstance(item, WorkerResult) and item.succeeded for item in results)
        if succeeded:
            await asyncio.to_thread(
                self.store.transition_task,
                task["id"],
                TaskState.REVIEWING,
                summary="Worker execution complete; deterministic verification required",
            )
        else:
            failures = [str(item) for item in results if isinstance(item, Exception)]
            failures.extend(
                item.error or item.state.value
                for item in results
                if isinstance(item, WorkerResult) and not item.succeeded
            )
            await asyncio.to_thread(
                self.store.transition_task,
                task["id"],
                TaskState.WAITING,
                summary="One or more worker runs failed; retry or fallback required",
                payload={"failures": failures},
            )
            attempt = task["attempt_count"] + 1
            if attempt < self.max_attempts:
                await asyncio.to_thread(
                    self.store.transition_task,
                    task["id"],
                    TaskState.READY,
                    summary=f"Retry scheduled after attempt {attempt}",
                    payload={"nextAttempt": attempt + 1},
                )

    async def _execute_worker(
        self,
        *,
        task: dict[str, Any],
        project: dict[str, Any],
        requirements: TaskRequirements,
        worker_id: str,
        attempt: int,
    ) -> WorkerResult:
        run_id = await asyncio.to_thread(
            self.store.create_worker_run,
            task_id=task["id"],
            worker_id=worker_id,
            attempt=attempt,
        )
        await asyncio.to_thread(self.store.transition_worker_run, run_id, RunState.RUNNING)
        await asyncio.to_thread(self.store.set_worker_state, worker_id, WorkerState.RUNNING)

        async def event_sink(event: WorkerEvent) -> None:
            await asyncio.to_thread(
                self.store.record_adapter_event,
                run_id=run_id,
                kind=event.kind,
                payload=dict(event.payload),
            )

        request = WorkerRequest(
            run_id=run_id,
            task_id=task["id"],
            prompt=task["description"],
            working_directory=Path(project["root_path"]),
            timeout_seconds=120,
            code_write_required=requirements.code_write_required,
            metadata={
                "labels": [label.value for label in requirements.labels],
                "requested_capabilities": sorted(requirements.required_capabilities),
            },
        )
        adapter = self.adapters.get(worker_id)
        try:
            result = await adapter.execute(request, event_sink=event_sink)
        except Exception as error:
            classification = self._exception_classification(error)
            await asyncio.to_thread(
                self.store.transition_worker_run,
                run_id,
                RunState.FAILED,
                failure_class=classification,
                failure_detail=str(error),
            )
            await asyncio.to_thread(
                self.store.record_failure,
                classification=classification,
                summary="Worker adapter raised an exception",
                detail=str(error),
                retryable=classification in {FailureClass.TRANSIENT, FailureClass.INFRASTRUCTURE},
                task_id=task["id"],
                run_id=run_id,
            )
            await asyncio.to_thread(self.store.set_worker_state, worker_id, WorkerState.IDLE)
            raise

        model_id = await self._record_actual_model(worker_id, result)
        internal_session_id = None
        if result.session_id:
            internal_session_id = await asyncio.to_thread(
                self.store.upsert_session,
                worker_id=worker_id,
                provider_session_id=result.session_id,
                model_id=model_id,
            )
        await asyncio.to_thread(
            self.store.save_worker_result,
            run_id=run_id,
            summary=result.final_text or result.error or result.state.value,
        )
        await self._record_usage(task["id"], run_id, worker_id, result)
        evidence_path = await asyncio.to_thread(self._write_run_evidence, result)
        failure_class = self._result_failure_class(result)
        await asyncio.to_thread(
            self.store.transition_worker_run,
            run_id,
            result.state,
            process_id=result.pid,
            exit_code=result.exit_code,
            session_id=internal_session_id,
            raw_output_reference=str(evidence_path),
            failure_class=failure_class,
            failure_detail=result.error,
        )
        if not result.succeeded:
            await asyncio.to_thread(
                self.store.record_failure,
                classification=failure_class or FailureClass.TRANSIENT,
                summary=f"Worker ended in {result.state.value}",
                detail=result.error,
                retryable=result.state
                in {RunState.FAILED, RunState.TIMED_OUT, RunState.RATE_LIMITED},
                task_id=task["id"],
                run_id=run_id,
            )
        resource_state = {
            RunState.RATE_LIMITED: ResourceState.RATE_LIMITED,
            RunState.AUTH_REQUIRED: ResourceState.UNKNOWN,
        }.get(result.state)
        await asyncio.to_thread(
            self.store.set_worker_state,
            worker_id,
            WorkerState.IDLE if result.state is not RunState.AUTH_REQUIRED else WorkerState.OFFLINE,
            resource_state=resource_state,
            model_id=model_id,
        )
        return result

    async def _record_actual_model(self, worker_id: str, result: WorkerResult) -> str | None:
        if not result.model:
            return None
        worker = next(
            row
            for row in await asyncio.to_thread(self.store.list_workers)
            if row["id"] == worker_id
        )
        return await asyncio.to_thread(
            self.store.upsert_model,
            ModelDescriptor(
                identifier=result.model,
                display_name=result.model,
                provider=Provider(worker["provider"]),
                context_variant=result.context_variant,
            ),
        )

    def _write_run_evidence(self, result: WorkerResult) -> Path:
        path = self.evidence_root / f"{result.run_id}.json"
        payload = {
            "runID": result.run_id,
            "state": result.state.value,
            "pid": result.pid,
            "exitCode": result.exit_code,
            "startedAt": result.started_at.isoformat(),
            "endedAt": result.ended_at.isoformat(),
            "sessionID": result.session_id,
            "model": result.model,
            "contextVariant": result.context_variant,
            "stdout": result.stdout,
            "stderr": result.stderr,
            "finalText": result.final_text,
            "error": result.error,
            "eventTypes": sorted({event.kind for event in result.events}),
        }
        path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
        return path

    @staticmethod
    def _exception_classification(error: Exception) -> FailureClass:
        if isinstance(error, UnsafeWorkerRequest):
            return FailureClass.UNSAFE_TO_CONTINUE
        if isinstance(error, WorkerUnavailable):
            return FailureClass.INFRASTRUCTURE
        return FailureClass.IMPLEMENTATION_BUG

    @staticmethod
    def _result_failure_class(result: WorkerResult) -> FailureClass | None:
        return {
            RunState.AUTH_REQUIRED: FailureClass.AUTH,
            RunState.RATE_LIMITED: FailureClass.RATE_LIMIT,
            RunState.TIMED_OUT: FailureClass.TIMEOUT,
            RunState.CANCELLED: FailureClass.CANCELLED,
            RunState.FAILED: FailureClass.TRANSIENT,
        }.get(result.state)

    async def _record_usage(
        self, task_id: str, run_id: str, worker_id: str, result: WorkerResult
    ) -> None:
        metrics = {
            "inputTokens": (result.usage.input_tokens, "tokens"),
            "outputTokens": (result.usage.output_tokens, "tokens"),
            "cacheWriteTokens": (result.usage.cache_creation_tokens, "tokens"),
            "cacheReadTokens": (result.usage.cache_read_tokens, "tokens"),
            "reasoningTokens": (result.usage.reasoning_tokens, "tokens"),
            "totalTokens": (result.usage.total_tokens, "tokens"),
            "costUSD": (result.usage.cost_usd, "USD"),
        }
        for metric, (value, unit) in metrics.items():
            telemetry = (
                TelemetryValue(value, EvidenceConfidence.PROVIDER_REPORTED)
                if value is not None
                else TelemetryValue(
                    None,
                    EvidenceConfidence.UNKNOWN,
                    UnavailableReason.NOT_REPORTED,
                )
            )
            await asyncio.to_thread(
                self.store.record_usage,
                metric=metric,
                telemetry=telemetry,
                unit=unit,
                task_id=task_id,
                run_id=run_id,
                worker_id=worker_id,
            )

    @staticmethod
    def _requirements(row: dict[str, Any]) -> TaskRequirements:
        return TaskRequirements(
            labels=frozenset(TaskLabel(value) for value in json.loads(row["labels_json"])),
            required_capabilities=frozenset(json.loads(row["required_capabilities_json"])),
            permission_class=PermissionClass(row["permission_class"]),
            approval_state=ApprovalState(row["approval_state"]),
            minimum_context_tokens=row["minimum_context_tokens"],
            privacy_sensitive=bool(row["privacy_sensitive"]),
            code_write_required=bool(row["code_write_required"]),
            panel_size=row["panel_size"],
            preferred_workers=tuple(json.loads(row["preferred_workers_json"])),
        )
