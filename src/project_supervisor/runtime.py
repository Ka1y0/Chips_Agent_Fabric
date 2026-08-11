from __future__ import annotations

import asyncio
import json
import uuid
from contextlib import suppress
from dataclasses import dataclass, replace
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from .adapters.base import (
    DurableWorkerAdapter,
    NegotiatingWorkerAdapter,
    UnsafeWorkerRequest,
    WorkerAdapter,
    WorkerEvent,
    WorkerJobHandle,
    WorkerJobLaunchRejected,
    WorkerJobOutcomeUncertain,
    WorkerJobState,
    WorkerProtocolError,
    WorkerRequest,
    WorkerResult,
    WorkerUnavailable,
)
from .domain import (
    ApprovalState,
    EventSeverity,
    EvidenceConfidence,
    ExecutionTopology,
    FailureClass,
    Harness,
    ModelDescriptor,
    PermissionClass,
    Provider,
    Rejection,
    ResourceState,
    RunState,
    TaskLabel,
    TaskRecord,
    TaskRequirements,
    TaskState,
    TelemetryValue,
    UnavailableReason,
    WorkerState,
    utc_now,
)
from .resource_usage import (
    PreDispatchQuotaGuard,
    QuotaGuardCandidate,
    ResourceUsageRepository,
    ResourceUsageService,
)
from .scheduler import DeterministicScheduler
from .store import StateStore, timestamp
from .telemetry import (
    ExecutorKind,
    InvocationTelemetry,
    InvocationTelemetryRepository,
    TaskRole,
)
from .verification import (
    DefinitionOfDoneResult,
)


@dataclass(frozen=True, slots=True)
class DispatchSummary:
    launched_task_ids: tuple[str, ...]
    blocked_task_ids: tuple[str, ...]
    deferred_task_ids: tuple[str, ...] = ()


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
        telemetry: InvocationTelemetryRepository | None = None,
        resource_usage: ResourceUsageService | None = None,
        quota_guard: PreDispatchQuotaGuard | None = None,
        worker_timeout_seconds: float = 120.0,
        runtime_id: str | None = None,
        dispatch_lease_ttl_seconds: float = 6.0,
        dispatch_wait_timeout_seconds: float = 60.0,
    ) -> None:
        self.store = store
        self.scheduler = scheduler
        self.adapters = adapters
        self.evidence_root = Path(evidence_root).resolve()
        self.evidence_root.mkdir(parents=True, exist_ok=True)
        if max_attempts < 1:
            raise ValueError("max_attempts must be positive")
        self.max_attempts = max_attempts
        if not 1 <= worker_timeout_seconds <= 3600:
            raise ValueError("worker_timeout_seconds must be between 1 and 3600")
        self.worker_timeout_seconds = float(worker_timeout_seconds)
        if not 1 <= dispatch_lease_ttl_seconds <= 3600:
            raise ValueError("dispatch_lease_ttl_seconds must be between 1 and 3600")
        self.runtime_id = runtime_id or f"runtime-{uuid.uuid4()}"
        self.dispatch_lease_ttl_seconds = float(dispatch_lease_ttl_seconds)
        if not 0 < dispatch_wait_timeout_seconds <= 3600:
            raise ValueError("dispatch_wait_timeout_seconds must be between zero and 3600")
        self.dispatch_wait_timeout_seconds = float(dispatch_wait_timeout_seconds)
        self.telemetry = telemetry or InvocationTelemetryRepository(store)
        resource_repository = ResourceUsageRepository(store)
        self.resource_usage = resource_usage or ResourceUsageService(resource_repository)
        self.quota_guard = quota_guard or PreDispatchQuotaGuard(resource_repository)
        self._active: dict[str, asyncio.Task[None]] = {}
        self._dispatch_lock = asyncio.Lock()
        self._capacity_changed = asyncio.Event()

    async def recover(self, *, task_ids: set[str] | None = None) -> dict[str, int]:
        # A provider result may have been atomically finalized just before the process died but
        # before the parent Task moved to REVIEWING/READY. Repair that boundary first so legacy
        # interruption recovery cannot redispatch an already completed external attempt.
        await self._repair_finalized_provider_attempts(task_ids=task_ids)
        # Durable external jobs are reconciled before legacy interruption policy runs.  A
        # reconciliation claim takes a new lease generation without creating another attempt.
        await self._recover_durable_provider_jobs(task_ids=task_ids)
        result = await asyncio.to_thread(self.store.recover_interrupted, task_ids)
        interrupted_runs = [
            run
            for run in await asyncio.to_thread(self.store.list_worker_runs)
            if run["state"] == RunState.INTERRUPTED.value
            and (task_ids is None or run["task_id"] in task_ids)
            and not await asyncio.to_thread(self.telemetry.contains_run, run["id"])
        ]
        for run in interrupted_runs:
            started_at = self._parse_timestamp(run["started_at"] or run["created_at"])
            ended_at = self._parse_timestamp(run["ended_at"] or run["updated_at"])
            task = await asyncio.to_thread(self.store.get_task, run["task_id"])
            fallback = (
                ExecutionTopology(task["topology"])
                in {ExecutionTopology.FALLBACK, ExecutionTopology.CHEAP_FIRST_ESCALATION}
                and run["attempt"] > 1
            )
            await self._record_invocation_telemetry(
                task_id=run["task_id"],
                worker_id=run["worker_id"],
                attempt=run["attempt"],
                task_role=TaskRole.FALLBACK if fallback else TaskRole.UNKNOWN,
                fallback=fallback,
                result=WorkerResult(
                    run_id=run["id"],
                    state=RunState.INTERRUPTED,
                    pid=run["process_id"],
                    exit_code=run["exit_code"],
                    started_at=started_at,
                    ended_at=ended_at,
                    stdout="",
                    stderr="",
                    final_text="",
                    events=(),
                    error="supervisor restart",
                ),
            )
        for task in await asyncio.to_thread(self.store.list_tasks):
            if task["state"] == TaskState.INTERRUPTED.value:
                if task_ids is not None and task["id"] not in task_ids:
                    continue
                if int(task["attempt_count"]) >= self.max_attempts:
                    terminal = await asyncio.to_thread(
                        self.store.transition_task,
                        task["id"],
                        TaskState.FAILED,
                        actor="recovery",
                        summary="Interrupted task exhausted its execution attempt limit",
                        payload={
                            "attemptCount": int(task["attempt_count"]),
                            "maxAttempts": self.max_attempts,
                        },
                    )
                    await self.audit_task_terminal(task["id"], task=terminal)
                else:
                    await asyncio.to_thread(
                        self.store.transition_task,
                        task["id"],
                        TaskState.READY,
                        actor="recovery",
                        summary="Interrupted task requeued by recovery policy",
                    )
        await self.audit_pending_terminal_tasks(task_ids=task_ids)
        return result

    async def _repair_finalized_provider_attempts(self, *, task_ids: set[str] | None) -> None:
        terminal_run_states = {
            RunState.COMPLETED.value,
            RunState.FAILED.value,
            RunState.CANCELLED.value,
            RunState.TIMED_OUT.value,
            RunState.AUTH_REQUIRED.value,
            RunState.RATE_LIMITED.value,
        }
        for task in await asyncio.to_thread(self.store.list_tasks):
            task_id = str(task["id"])
            if task_ids is not None and task_id not in task_ids:
                continue
            if task_id in self._active or task["state"] not in {
                TaskState.RUNNING.value,
                TaskState.WAITING.value,
            }:
                continue
            runs = await asyncio.to_thread(self.store.list_worker_runs, task_id)
            if not runs:
                continue
            attempt = max(int(run["attempt"]) for run in runs)
            attempt_runs = [run for run in runs if int(run["attempt"]) == attempt]
            if not attempt_runs or any(
                run["state"] not in terminal_run_states for run in attempt_runs
            ):
                continue
            try:
                jobs = [
                    await asyncio.to_thread(self.store.get_provider_job, run["id"])
                    for run in attempt_runs
                ]
            except KeyError:
                continue
            if any(job["result_collection_state"] != "collected" for job in jobs):
                continue
            with self.store.connect() as connection:
                recorded = connection.execute(
                    "SELECT COUNT(*) AS count FROM worker_results WHERE run_id IN ("
                    + ",".join("?" for _ in attempt_runs)
                    + ")",
                    tuple(run["id"] for run in attempt_runs),
                ).fetchone()
            if recorded is None or int(recorded["count"]) != len(attempt_runs):
                continue
            generation = await asyncio.to_thread(
                self.store.claim_task_reconciliation,
                task_id,
                owner_id=self.runtime_id,
                lease_ttl_seconds=self.dispatch_lease_ttl_seconds,
            )
            if generation is None:
                continue
            try:
                current = await asyncio.to_thread(self.store.get_task, task_id)
                if current["state"] == TaskState.WAITING.value:
                    await asyncio.to_thread(
                        self.store.transition_task,
                        task_id,
                        TaskState.RUNNING,
                        summary="Finalized provider attempt recovered after restart",
                        lease_owner_id=self.runtime_id,
                        lease_generation=generation,
                    )
                for job in jobs:
                    await self._resolve_reconciliation_escalations(
                        job,
                        resolution="finalized provider attempt repaired after restart",
                        lease_generation=generation,
                    )
                await self._finish_task_attempt(
                    task=task,
                    attempt=attempt,
                    results=[],
                    lease_generation=generation,
                )
            finally:
                await asyncio.to_thread(
                    self.store.release_task_execution_lease,
                    task_id,
                    owner_id=self.runtime_id,
                    generation=generation,
                    actor="recovery",
                )

    async def _recover_durable_provider_jobs(self, *, task_ids: set[str] | None) -> None:
        jobs = await asyncio.to_thread(self.store.list_provider_jobs, reconcilable_only=True)
        by_task: dict[str, list[dict[str, Any]]] = {}
        for job in jobs:
            if task_ids is not None and job["task_id"] not in task_ids:
                continue
            by_task.setdefault(str(job["task_id"]), []).append(job)
        for task_id, task_jobs in by_task.items():
            if task_id in self._active:
                continue
            cancelled = all(job["task_state"] == TaskState.CANCELLED.value for job in task_jobs)
            generation = await asyncio.to_thread(
                self.store.claim_task_reconciliation,
                task_id,
                owner_id=self.runtime_id,
                lease_ttl_seconds=self.dispatch_lease_ttl_seconds,
                allow_cancelled=cancelled,
            )
            if generation is None:
                continue
            keep_lease = False
            heartbeat = asyncio.create_task(
                self._maintain_task_execution_lease(task_id, generation),
                name=f"supervisor-reconcile-lease:{task_id}",
            )
            try:
                if cancelled:
                    await self._recover_cancelled_provider_jobs(
                        task_id=task_id,
                        jobs=task_jobs,
                        lease_generation=generation,
                    )
                else:
                    keep_lease = await self._reconcile_claimed_provider_jobs(
                        task_id=task_id,
                        jobs=task_jobs,
                        lease_generation=generation,
                    )
            finally:
                heartbeat.cancel()
                await asyncio.gather(heartbeat, return_exceptions=True)
                if not keep_lease:
                    await asyncio.to_thread(
                        self.store.release_task_execution_lease,
                        task_id,
                        owner_id=self.runtime_id,
                        generation=generation,
                        actor="recovery",
                    )

    async def _recover_cancelled_provider_jobs(
        self,
        *,
        task_id: str,
        jobs: list[dict[str, Any]],
        lease_generation: int,
    ) -> None:
        """Continue durable provider cancellation without reopening canonical execution."""

        for job in jobs:
            run = await asyncio.to_thread(self.store.get_worker_run, job["run_id"])
            await self._cancel_worker_run(
                task_id=task_id,
                run=run,
                lease_generation=lease_generation,
            )

    async def _reconcile_claimed_provider_jobs(
        self,
        *,
        task_id: str,
        jobs: list[dict[str, Any]],
        lease_generation: int,
    ) -> bool:
        """Observe orphaned jobs and return true when an attachment now owns the lease."""

        task = await asyncio.to_thread(self.store.get_task, task_id)
        project = await asyncio.to_thread(self.store.get_project, task["project_id"])
        requirements = self._requirements(task)
        running: list[tuple[dict[str, Any], WorkerJobHandle, DurableWorkerAdapter]] = []
        results: list[WorkerResult | BaseException] = []
        held_for_escalation = False
        for job in jobs:
            try:
                adapter = self.adapters.get(job["worker_id"])
            except KeyError:
                await self._escalate_reconciliation(
                    job,
                    code="PROVIDER_STATE_AMBIGUOUS",
                    summary="Durable execution has no registered recovery adapter",
                    detail="Recovery cannot query the external job without its worker adapter",
                    lease_generation=lease_generation,
                )
                held_for_escalation = True
                continue
            try:
                await self._negotiate_adapter_job_contract(adapter)
            except Exception as error:
                await self._escalate_reconciliation(
                    job,
                    code="EXTERNAL_JOB_UNREACHABLE",
                    summary="Durable Worker contract could not be observed during recovery",
                    detail=f"{type(error).__name__}: contract negotiation failed before lookup",
                    lease_generation=lease_generation,
                )
                held_for_escalation = True
                continue
            if (
                not isinstance(adapter, DurableWorkerAdapter)
                or not bool(job["supports_reconcile"])
                or not adapter.job_capabilities.supports_reconcile
            ):
                await self._escalate_reconciliation(
                    job,
                    code="PROVIDER_STATE_AMBIGUOUS",
                    summary="Worker adapter cannot reconcile a durable external execution",
                    detail="The adapter does not advertise restart reconciliation support",
                    lease_generation=lease_generation,
                )
                held_for_escalation = True
                continue
            if (
                adapter.adapter_type != job["adapter_type"]
                or adapter.adapter_instance_id != job["adapter_instance_id"]
            ):
                await self._escalate_reconciliation(
                    job,
                    code="PROVIDER_STATE_AMBIGUOUS",
                    summary="Recovery adapter identity does not match the durable launch",
                    detail="The external job was not queried through a different adapter instance",
                    lease_generation=lease_generation,
                )
                held_for_escalation = True
                continue
            handle: WorkerJobHandle | None = None
            if not job["provider_job_id"]:
                native_idempotency = bool(job["supports_provider_idempotency"]) and bool(
                    adapter.job_capabilities.supports_provider_idempotency
                )
                if not native_idempotency:
                    await self._escalate_reconciliation(
                        job,
                        code="IDEMPOTENCY_UNCERTAIN",
                        summary="Provider launch may have occurred before its handle was persisted",
                        detail="No provider identity is available for a safe lookup",
                        lease_generation=lease_generation,
                    )
                    held_for_escalation = True
                    continue
                request = self._worker_request(task, project, requirements, job["run_id"])
                try:
                    # The adapter contract promises native deduplication for this exact key.  This
                    # is the only circumstance in which recovery may cross the launch boundary a
                    # second time for the same canonical Worker run.
                    handle = await adapter.start_job(
                        request,
                        idempotency_key=str(job["idempotency_key"]),
                    )
                    if handle.run_id != job["run_id"]:
                        raise RuntimeError(
                            "provider idempotency replay returned a handle for another run"
                        )
                    rebound = await asyncio.to_thread(
                        self.store.bind_provider_job_handle,
                        run_id=job["run_id"],
                        adapter_type=handle.adapter_type,
                        adapter_instance_id=handle.adapter_instance_id,
                        handle_version=handle.schema_version,
                        provider_job_id=handle.provider_job_id,
                        provider_session_id=handle.provider_session_id,
                        runtime_pid=handle.runtime_pid,
                        runtime_host=handle.runtime_host,
                        runtime_identity=handle.process_identity,
                        adapter_metadata=handle.metadata,
                        lease_owner_id=self.runtime_id,
                        lease_generation=lease_generation,
                    )
                    job = {**job, **rebound}
                except Exception as error:
                    await self._escalate_reconciliation(
                        job,
                        code="IDEMPOTENCY_UNCERTAIN",
                        summary="Provider idempotency replay did not return a durable handle",
                        detail=f"{type(error).__name__}: {error}",
                        lease_generation=lease_generation,
                    )
                    held_for_escalation = True
                    continue
            if handle is None:
                handle = self._provider_job_handle(job)
            provider_status: str | None = None
            try:
                observation = await adapter.reconcile_job(handle)
            except Exception as error:
                observation_state = WorkerJobState.PROVIDER_UNREACHABLE
                observation_detail = f"{type(error).__name__}: {error}"
            else:
                observation_state = observation.state
                observation_detail = observation.detail
                if observation.metadata.get("providerState") is not None:
                    provider_status = str(observation.metadata["providerState"])
            await asyncio.to_thread(
                self.store.record_provider_job_observation,
                job["run_id"],
                state=observation_state.value,
                provider_status=provider_status,
                detail=observation_detail,
                lease_owner_id=self.runtime_id,
                lease_generation=lease_generation,
            )
            if observation_state is WorkerJobState.KNOWN_RUNNING:
                await self._resolve_reconciliation_escalations(
                    job,
                    resolution="provider job is reachable and running",
                    codes=(
                        "EXTERNAL_JOB_UNREACHABLE",
                        "PROVIDER_STATE_AMBIGUOUS",
                        "IDEMPOTENCY_UNCERTAIN",
                    ),
                    lease_generation=lease_generation,
                )
                if not bool(job["supports_resume"]) or not adapter.job_capabilities.supports_resume:
                    await self._escalate_reconciliation(
                        job,
                        code="RESUME_UNSUPPORTED",
                        summary="External job is running but this adapter cannot reattach",
                        detail="The job remains protected from duplicate launch",
                        lease_generation=lease_generation,
                    )
                    held_for_escalation = True
                    continue
                await self._resolve_reconciliation_escalations(
                    job,
                    resolution="provider job is reachable and reattachable",
                    codes=("RESUME_UNSUPPORTED",),
                    lease_generation=lease_generation,
                )
                running.append((job, handle, adapter))
                continue
            if observation_state in {
                WorkerJobState.KNOWN_COMPLETED,
                WorkerJobState.KNOWN_FAILED,
                WorkerJobState.KNOWN_CANCELLED,
            }:
                if not bool(job["supports_repeatable_collect"]):
                    await self._escalate_reconciliation(
                        job,
                        code="RESULT_COLLECTION_UNCERTAIN",
                        summary="Provider result cannot be collected repeatably after restart",
                        detail=(
                            "Canonical result ingestion was deferred to avoid consuming it twice"
                        ),
                        lease_generation=lease_generation,
                    )
                    held_for_escalation = True
                    continue
                request = self._worker_request(task, project, requirements, job["run_id"])
                try:
                    result = await adapter.collect_job(request, handle)
                    self._validate_collected_result(observation_state, result)
                    role, fallback = self._role_for_recovered_run(task, job)
                    result = await self._persist_worker_result(
                        task=task,
                        worker_id=job["worker_id"],
                        run_id=job["run_id"],
                        attempt=int(job["attempt"]),
                        task_role=role,
                        fallback=fallback,
                        lease_generation=lease_generation,
                        result=result,
                    )
                except Exception as error:
                    await self._escalate_reconciliation(
                        job,
                        code="RESULT_COLLECTION_UNCERTAIN",
                        summary="Provider reported terminal but result collection was inconclusive",
                        detail=f"{type(error).__name__}: {error}",
                        lease_generation=lease_generation,
                    )
                    held_for_escalation = True
                    results.append(error)
                else:
                    await self._resolve_reconciliation_escalations(
                        job,
                        resolution="terminal provider result was collected canonically",
                        lease_generation=lease_generation,
                    )
                    results.append(result)
                continue
            if observation_state is WorkerJobState.PROVIDER_NOT_FOUND:
                if len(jobs) != 1:
                    await self._escalate_reconciliation(
                        job,
                        code="PROVIDER_STATE_AMBIGUOUS",
                        summary="One job in a multi-worker attempt is missing",
                        detail="Automatic retry is withheld while peer jobs may still be active",
                        lease_generation=lease_generation,
                    )
                    held_for_escalation = True
                else:
                    await self._resolve_reconciliation_escalations(
                        job,
                        resolution="provider definitively reported that the job is absent",
                        lease_generation=lease_generation,
                    )
                    await asyncio.to_thread(
                        self.store.interrupt_missing_provider_job,
                        job["run_id"],
                        lease_owner_id=self.runtime_id,
                        lease_generation=lease_generation,
                    )
                continue
            code = (
                "EXTERNAL_JOB_UNREACHABLE"
                if observation_state is WorkerJobState.PROVIDER_UNREACHABLE
                else "PROVIDER_STATE_AMBIGUOUS"
            )
            await self._escalate_reconciliation(
                job,
                code=code,
                summary="Provider job state cannot be resolved safely",
                detail=observation_detail,
                lease_generation=lease_generation,
            )
            held_for_escalation = True

        if running:
            current_task = await asyncio.to_thread(self.store.get_task, task_id)
            if current_task["state"] == TaskState.WAITING.value:
                task = await asyncio.to_thread(
                    self.store.transition_task,
                    task_id,
                    TaskState.RUNNING,
                    summary="External provider job reattached after reconciliation",
                    payload={"reasonCode": "PROVIDER_JOB_REATTACHED"},
                    lease_owner_id=self.runtime_id,
                    lease_generation=lease_generation,
                )
            active = asyncio.create_task(
                self._reattach_provider_task(
                    task=task,
                    project=project,
                    requirements=requirements,
                    jobs=running,
                    lease_generation=lease_generation,
                    prior_results=results,
                    finalize_attempt=not held_for_escalation,
                ),
                name=f"supervisor-reattach:{task_id}",
            )
            self._active[task_id] = active
            active.add_done_callback(
                lambda done, identity=task_id: self._active_completed(identity, done)
            )
            return True
        if results and not held_for_escalation:
            current_task = await asyncio.to_thread(self.store.get_task, task_id)
            if current_task["state"] == TaskState.WAITING.value:
                task = await asyncio.to_thread(
                    self.store.transition_task,
                    task_id,
                    TaskState.RUNNING,
                    summary="Terminal provider result recovered after reconciliation",
                    payload={"reasonCode": "PROVIDER_RESULT_RECOVERED"},
                    lease_owner_id=self.runtime_id,
                    lease_generation=lease_generation,
                )
            await self._finish_task_attempt(
                task=task,
                attempt=max(int(job["attempt"]) for job in jobs),
                results=results,
                lease_generation=lease_generation,
            )
        elif held_for_escalation:
            await self._mark_task_waiting_for_reconciliation(
                task=task,
                jobs=jobs,
                lease_generation=lease_generation,
            )
        return False

    async def _reattach_provider_task(
        self,
        *,
        task: dict[str, Any],
        project: dict[str, Any],
        requirements: TaskRequirements,
        jobs: list[tuple[dict[str, Any], WorkerJobHandle, DurableWorkerAdapter]],
        lease_generation: int,
        prior_results: list[WorkerResult | BaseException],
        finalize_attempt: bool,
    ) -> None:
        execution = asyncio.create_task(
            self._resume_provider_jobs(
                task=task,
                project=project,
                requirements=requirements,
                jobs=jobs,
                lease_generation=lease_generation,
                prior_results=prior_results,
                finalize_attempt=finalize_attempt,
            ),
            name=f"supervisor-provider-resume:{task['id']}",
        )
        heartbeat = asyncio.create_task(
            self._maintain_task_execution_lease(task["id"], lease_generation),
            name=f"supervisor-provider-lease:{task['id']}",
        )
        try:
            done, _pending = await asyncio.wait(
                {execution, heartbeat}, return_when=asyncio.FIRST_COMPLETED
            )
            if execution in done:
                await execution
            else:
                # Lease transfer means detach only.  It is never authority to cancel the external
                # provider job that the replacement generation is about to reconcile.
                execution.cancel()
                await asyncio.gather(execution, return_exceptions=True)
        finally:
            heartbeat.cancel()
            await asyncio.gather(heartbeat, return_exceptions=True)
            await asyncio.to_thread(
                self.store.release_task_execution_lease,
                task["id"],
                owner_id=self.runtime_id,
                generation=lease_generation,
                actor="recovery",
            )

    async def _resume_provider_jobs(
        self,
        *,
        task: dict[str, Any],
        project: dict[str, Any],
        requirements: TaskRequirements,
        jobs: list[tuple[dict[str, Any], WorkerJobHandle, DurableWorkerAdapter]],
        lease_generation: int,
        prior_results: list[WorkerResult | BaseException],
        finalize_attempt: bool,
    ) -> None:
        async def resume_one(
            job: dict[str, Any], handle: WorkerJobHandle, adapter: DurableWorkerAdapter
        ) -> WorkerResult | BaseException:
            async def event_sink(event: WorkerEvent) -> None:
                await asyncio.to_thread(
                    self.store.record_adapter_event,
                    run_id=job["run_id"],
                    kind=event.kind,
                    payload=dict(event.payload),
                    lease_owner_id=self.runtime_id,
                    lease_generation=lease_generation,
                )

            request = self._worker_request(task, project, requirements, job["run_id"])
            try:
                result = await adapter.resume_job(request, handle, event_sink=event_sink)
            except asyncio.CancelledError:
                raise
            except Exception as error:
                # A failed attachment is not evidence that the external job stopped.  Preserve
                # this execution attempt so a transport outage cannot create a duplicate launch.
                await asyncio.to_thread(
                    self.store.record_provider_job_observation,
                    job["run_id"],
                    state=WorkerJobState.PROVIDER_UNREACHABLE.value,
                    detail=f"{type(error).__name__}: {error}",
                    lease_owner_id=self.runtime_id,
                    lease_generation=lease_generation,
                )
                await self._escalate_reconciliation(
                    job,
                    code="EXTERNAL_JOB_UNREACHABLE",
                    summary="Provider job reattachment was interrupted",
                    detail=f"{type(error).__name__}: {error}",
                    lease_generation=lease_generation,
                )
                return error
            role, fallback = self._role_for_recovered_run(task, job)
            return await self._persist_worker_result(
                task=task,
                worker_id=job["worker_id"],
                run_id=job["run_id"],
                attempt=int(job["attempt"]),
                task_role=role,
                fallback=fallback,
                lease_generation=lease_generation,
                result=result,
            )

        resumed = await asyncio.gather(
            *(resume_one(job, handle, adapter) for job, handle, adapter in jobs),
            return_exceptions=True,
        )
        all_results = [*prior_results, *resumed]
        uncertain = any(isinstance(item, BaseException) for item in resumed)
        if finalize_attempt and not uncertain:
            await self._finish_task_attempt(
                task=task,
                attempt=max(int(job["attempt"]) for job, _handle, _adapter in jobs),
                results=all_results,
                lease_generation=lease_generation,
            )
        else:
            await self._mark_task_waiting_for_reconciliation(
                task=task,
                jobs=[job for job, _handle, _adapter in jobs],
                lease_generation=lease_generation,
            )

    async def _mark_task_waiting_for_reconciliation(
        self,
        *,
        task: dict[str, Any],
        jobs: list[dict[str, Any]],
        lease_generation: int,
    ) -> None:
        """Park unresolved provider work without making it eligible for fresh dispatch."""

        active_states = {
            RunState.STARTING.value,
            RunState.RUNNING.value,
            RunState.WAITING.value,
        }
        current_runs = {
            run["id"]: run
            for run in await asyncio.to_thread(self.store.list_worker_runs, task["id"])
        }
        for job in jobs:
            run = current_runs.get(job["run_id"])
            if run is None or run["state"] not in active_states:
                continue
            await asyncio.to_thread(
                self.store.set_worker_state,
                job["worker_id"],
                WorkerState.WAITING,
                task_id=task["id"],
                lease_owner_id=self.runtime_id,
                lease_generation=lease_generation,
            )
        current = await asyncio.to_thread(self.store.get_task, task["id"])
        if current["state"] == TaskState.RUNNING.value:
            await asyncio.to_thread(
                self.store.transition_task,
                task["id"],
                TaskState.WAITING,
                summary="External provider work is awaiting durable reconciliation",
                payload={"reasonCode": "PROVIDER_RECONCILIATION_PENDING"},
                lease_owner_id=self.runtime_id,
                lease_generation=lease_generation,
            )

    async def _escalate_reconciliation(
        self,
        job: dict[str, Any],
        *,
        code: str,
        summary: str,
        detail: str | None,
        lease_generation: int,
    ) -> None:
        await asyncio.to_thread(
            self.store.request_execution_escalation,
            run_id=job["run_id"],
            code=code,
            summary=summary,
            detail=detail,
            lease_owner_id=self.runtime_id,
            lease_generation=lease_generation,
        )

    async def _resolve_reconciliation_escalations(
        self,
        job: dict[str, Any],
        *,
        resolution: str,
        lease_generation: int,
        codes: tuple[str, ...] | None = None,
    ) -> None:
        await asyncio.to_thread(
            self.store.resolve_execution_escalations,
            run_id=job["run_id"],
            resolution=resolution,
            codes=codes,
            lease_owner_id=self.runtime_id,
            lease_generation=lease_generation,
        )

    def _worker_request(
        self,
        task: dict[str, Any],
        project: dict[str, Any],
        requirements: TaskRequirements,
        run_id: str,
    ) -> WorkerRequest:
        return WorkerRequest(
            run_id=run_id,
            task_id=task["id"],
            prompt=task["description"],
            working_directory=Path(project["root_path"]),
            timeout_seconds=self.worker_timeout_seconds,
            code_write_required=requirements.code_write_required,
            metadata={
                "labels": [label.value for label in requirements.labels],
                "requested_capabilities": sorted(requirements.required_capabilities),
                "timeout_seconds": self.worker_timeout_seconds,
            },
        )

    def _provider_job_handle(self, job: dict[str, Any]) -> WorkerJobHandle:
        metadata = json.loads(job["adapter_metadata_json"] or "{}")
        return WorkerJobHandle(
            run_id=job["run_id"],
            adapter_type=job["adapter_type"],
            adapter_instance_id=job["adapter_instance_id"],
            provider_job_id=job["provider_job_id"],
            provider_session_id=job["provider_session_id"],
            runtime_pid=job["runtime_pid"],
            runtime_host=job["runtime_host"],
            process_identity=job["runtime_identity"],
            schema_version=int(job["handle_version"]),
            created_at=self._parse_timestamp(job["launched_at"] or job["created_at"]),
            metadata=metadata,
        )

    def _role_for_recovered_run(
        self, task: dict[str, Any], job: dict[str, Any]
    ) -> tuple[TaskRole, bool]:
        run_rows = [
            row
            for row in self.store.list_worker_runs(task["id"])
            if int(row["attempt"]) == int(job["attempt"])
        ]
        worker_ids = tuple(row["worker_id"] for row in run_rows)
        assignments = self._task_roles(
            ExecutionTopology(task["topology"]), worker_ids, attempt=int(job["attempt"])
        )
        for worker_id, role, fallback in assignments:
            if worker_id == job["worker_id"]:
                return role, fallback
        return TaskRole.UNKNOWN, False

    @staticmethod
    def _parse_timestamp(value: str) -> datetime:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))

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

    async def dispatch_ready(self, *, task_ids: set[str] | None = None) -> DispatchSummary:
        launched: list[str] = []
        blocked: list[str] = []
        deferred: list[str] = []
        async with self._dispatch_lock:
            tasks = sorted(
                await asyncio.to_thread(self.store.list_tasks),
                key=lambda row: (-int(row["priority"]), row["created_at"], row["id"]),
            )
            for task in tasks:
                task_id = task["id"]
                if task_ids is not None and task_id not in task_ids:
                    continue
                if task["state"] != TaskState.READY.value or task_id in self._active:
                    continue
                if int(task["attempt_count"]) >= self.max_attempts:
                    try:
                        terminal = await asyncio.to_thread(
                            self.store.transition_task,
                            task_id,
                            TaskState.FAILED,
                            actor="runtime",
                            summary="Task execution attempt limit exhausted before dispatch",
                            payload={
                                "attemptCount": int(task["attempt_count"]),
                                "maxAttempts": self.max_attempts,
                            },
                            expected_version=int(task["version"]),
                        )
                    except (RuntimeError, ValueError):
                        current = await asyncio.to_thread(self.store.get_task, task_id)
                        if current["state"] != TaskState.READY.value:
                            continue
                        raise
                    await self.audit_task_terminal(task_id, task=terminal)
                    continue
                dependencies = await asyncio.to_thread(self.store.task_dependencies, task_id)
                failed_dependencies = [
                    dependency
                    for dependency in dependencies
                    if dependency["state"] in {TaskState.FAILED.value, TaskState.CANCELLED.value}
                ]
                if failed_dependencies:
                    await asyncio.to_thread(
                        self.store.transition_task,
                        task_id,
                        TaskState.BLOCKED,
                        summary="Task blocked by failed prerequisite",
                        payload={
                            "reasonCode": "DEPENDENCY_TERMINAL_FAILURE",
                            "dependencies": [
                                {
                                    "taskID": dependency["task_id"],
                                    "state": dependency["state"],
                                }
                                for dependency in failed_dependencies
                            ],
                        },
                    )
                    blocked.append(task_id)
                    continue
                if any(
                    dependency["state"] != TaskState.SUCCEEDED.value for dependency in dependencies
                ):
                    continue
                requirements = self._requirements(task)
                topology = ExecutionTopology(task["topology"])
                snapshots = await asyncio.to_thread(self.store.worker_snapshots)
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
                (
                    candidates,
                    quota_rejections,
                    quota_explanation,
                    resource_evidence,
                ) = await asyncio.to_thread(self._apply_quota_guard, requirements, candidates)
                decision = self.scheduler.schedule(
                    task_id=task_id,
                    requirements=requirements,
                    topology=topology,
                    workers=candidates,
                    resource_evidence=resource_evidence,
                )
                combined_rejections = (*decision.rejected, *quota_rejections)
                decision = replace(
                    decision,
                    rejected=combined_rejections,
                    explanation={
                        **decision.explanation,
                        "rejected": [
                            {
                                "workerID": item.worker_id,
                                "reasonCode": item.reason_code,
                                "detail": item.detail,
                            }
                            for item in combined_rejections
                        ],
                        "quotaGuard": quota_explanation,
                    },
                )
                await asyncio.to_thread(
                    self.store.persist_routing_decision,
                    task_id=task_id,
                    decision=decision,
                )
                if not decision.selected_worker_ids:
                    if self._transient_capacity_possible(
                        requirements=requirements,
                        topology=topology,
                        candidates=candidates,
                        resource_evidence=resource_evidence,
                    ):
                        wait_exhausted = await asyncio.to_thread(
                            self._record_dispatch_deferred,
                            task,
                            reason_code="WORKER_CAPACITY_BUSY",
                            detail="Otherwise eligible Worker capacity is temporarily busy",
                        )
                        (blocked if wait_exhausted else deferred).append(task_id)
                        continue
                    await asyncio.to_thread(
                        self.store.transition_task,
                        task_id,
                        TaskState.BLOCKED,
                        summary="No eligible worker",
                        payload={"rejected": decision.explanation["rejected"]},
                    )
                    blocked.append(task_id)
                    continue
                contract_observation_failed = False
                for worker_id in decision.selected_worker_ids:
                    adapter = self.adapters.get(worker_id)
                    try:
                        await self._negotiate_adapter_job_contract(adapter)
                    except WorkerJobLaunchRejected:
                        # The adapter proved this request cannot cross its launch boundary and
                        # froze that failure for the immediately following invocation. Claiming a
                        # canonical run preserves existing AUTH/RATE_LIMITED failure semantics.
                        continue
                    except Exception:
                        wait_exhausted = await asyncio.to_thread(
                            self._record_dispatch_deferred,
                            task,
                            reason_code="WORKER_CONTRACT_UNAVAILABLE",
                            detail=(
                                "Selected Worker contract could not be observed before dispatch"
                            ),
                        )
                        (blocked if wait_exhausted else deferred).append(task_id)
                        contract_observation_failed = True
                        break
                if contract_observation_failed:
                    continue
                claim = await asyncio.to_thread(
                    self.store.claim_task_dispatch,
                    task_id,
                    decision.selected_worker_ids,
                    expected_version=int(task["version"]),
                    timeout_at=utc_now() + timedelta(seconds=self.worker_timeout_seconds),
                    lease_owner_id=self.runtime_id,
                    lease_ttl_seconds=self.dispatch_lease_ttl_seconds,
                    max_attempts=self.max_attempts,
                )
                if claim is None:
                    # A peer may have claimed this Task, or a Worker reservation may have changed.
                    # Only a still-READY Task needs a future capacity retry.
                    current = await asyncio.to_thread(self.store.get_task, task_id)
                    if current["state"] == TaskState.READY.value:
                        wait_exhausted = await asyncio.to_thread(
                            self._record_dispatch_deferred,
                            current,
                            reason_code="DISPATCH_CLAIM_RACE",
                            detail="Task or Worker reservation changed before atomic dispatch",
                        )
                        (blocked if wait_exhausted else deferred).append(task_id)
                    continue
                for worker_id, run_id in claim["runIDs"].items():
                    adapter = self.adapters.get(worker_id)
                    adapter_type, adapter_instance_id, capabilities = self._adapter_job_contract(
                        worker_id, adapter
                    )
                    await asyncio.to_thread(
                        self.store.prepare_provider_job,
                        run_id=run_id,
                        adapter_type=adapter_type,
                        adapter_instance_id=adapter_instance_id,
                        capabilities=capabilities,
                        lease_owner_id=self.runtime_id,
                        lease_generation=int(claim["leaseGeneration"]),
                    )
                active = asyncio.create_task(
                    self._execute_task(
                        task,
                        requirements,
                        decision.selected_worker_ids,
                        attempt=int(claim["attempt"]),
                        run_ids=dict(claim["runIDs"]),
                        lease_generation=int(claim["leaseGeneration"]),
                    ),
                    name=f"supervisor:{task_id}",
                )
                self._active[task_id] = active
                active.add_done_callback(
                    lambda done, identity=task_id: self._active_completed(identity, done)
                )
                launched.append(task_id)
        return DispatchSummary(tuple(launched), tuple(blocked), tuple(deferred))

    async def wait_for_active(self, *, task_ids: set[str] | None = None) -> None:
        while True:
            selected = tuple(
                task
                for task_id, task in self._active.items()
                if task_ids is None or task_id in task_ids
            )
            if not selected:
                return
            await asyncio.gather(*selected)

    async def wait_for_dispatch_capacity(self, *, timeout_seconds: float = 1.0) -> bool:
        """Wait without busy-spinning for local Worker capacity to change.

        The timeout permits recovery when a different process owns the Worker and therefore cannot
        signal this runtime's in-memory event.
        """

        if timeout_seconds <= 0:
            raise ValueError("capacity wait timeout must be positive")
        self._capacity_changed.clear()
        try:
            await asyncio.wait_for(self._capacity_changed.wait(), timeout=timeout_seconds)
        except TimeoutError:
            return False
        return True

    def _active_completed(self, task_id: str, _task: asyncio.Task[None]) -> None:
        self._active.pop(task_id, None)
        self._capacity_changed.set()

    async def run_until_idle(self) -> None:
        while True:
            summary = await self.dispatch_ready()
            if self._active:
                await self.wait_for_active()
                continue
            if not summary.launched_task_ids:
                return

    def _apply_quota_guard(
        self,
        requirements: TaskRequirements,
        candidates: list[Any],
    ) -> tuple[list[Any], tuple[Rejection, ...], dict[str, Any], tuple[Any, ...]]:
        guard_candidates: list[QuotaGuardCandidate] = []
        latest_by_worker = self.quota_guard.repository.latest_by_worker(
            [worker.id for worker in candidates]
        )
        capable_by_worker: dict[str, bool] = {}
        for worker in candidates:
            latest = latest_by_worker.get(worker.id)
            quota_pool_id = (
                latest.observation.quota_pool_id
                if latest is not None
                else f"local:{worker.provider.value}:worker:{worker.id}"
            )
            capable = not self.scheduler.constraint_rejections(requirements, worker)
            capable_by_worker[worker.id] = capable
            guard_candidates.append(
                QuotaGuardCandidate(
                    worker_id=worker.id,
                    quota_pool_id=quota_pool_id,
                    capable=capable,
                    # This normalized operator score is provider-neutral: lower values mean a
                    # more expensive/scarce pool. No provider or plan name is inferred here.
                    premium=worker.monetary_cost_score < 0.5,
                    provider=worker.provider.value,
                )
            )
        decision = self.quota_guard.evaluate(guard_candidates)
        allowed = set(decision.ordered_worker_ids)
        rejections = tuple(
            Rejection(worker_id=worker_id, reason_code="QUOTA_GUARD", detail=reason)
            for worker_id, reason in sorted(decision.avoided.items())
            if worker_id not in allowed and capable_by_worker.get(worker_id, False)
        )
        return (
            [
                worker
                for worker in candidates
                if not capable_by_worker[worker.id] or worker.id in allowed
            ],
            rejections,
            decision.to_protocol(),
            decision.routing_evidence(),
        )

    def _transient_capacity_possible(
        self,
        *,
        requirements: TaskRequirements,
        topology: ExecutionTopology,
        candidates: list[Any],
        resource_evidence: tuple[Any, ...],
    ) -> bool:
        busy_states = {WorkerState.STARTING, WorkerState.RUNNING, WorkerState.WAITING}
        if not any(worker.state in busy_states for worker in candidates):
            return False
        capacity_projection = [
            replace(worker, state=WorkerState.IDLE) if worker.state in busy_states else worker
            for worker in candidates
        ]
        probe = self.scheduler.schedule(
            task_id="__capacity_probe__",
            requirements=requirements,
            topology=topology,
            workers=capacity_projection,
            resource_evidence=resource_evidence,
        )
        return bool(probe.selected_worker_ids)

    def _record_dispatch_deferred(
        self,
        task: dict[str, Any],
        *,
        reason_code: str,
        detail: str,
    ) -> bool:
        payload = {"reasonCode": reason_code, "detail": detail}
        with self.store.transaction() as connection:
            previous = connection.execute(
                "SELECT kind,payload_json,created_at FROM events "
                "WHERE task_id=? AND kind='dispatchDeferred' "
                "ORDER BY sequence DESC LIMIT 1",
                (task["id"],),
            ).fetchone()
            if previous is not None and previous["kind"] == "dispatchDeferred":
                try:
                    if json.loads(previous["payload_json"]) == payload:
                        observed = self._parse_timestamp(previous["created_at"])
                        if (
                            utc_now() - observed
                        ).total_seconds() < self.dispatch_wait_timeout_seconds:
                            return False
                        current = connection.execute(
                            "SELECT * FROM tasks WHERE id=?", (task["id"],)
                        ).fetchone()
                        if current is None or current["state"] != TaskState.READY.value:
                            return False
                        now = timestamp()
                        connection.execute(
                            "UPDATE tasks SET state=?,updated_at=?,version=version+1 WHERE id=?",
                            (TaskState.BLOCKED.value, now, task["id"]),
                        )
                        self.store._append_event(
                            connection,
                            kind="taskStateChanged",
                            severity=EventSeverity.WARNING,
                            entity_type="task",
                            entity_id=task["id"],
                            project_id=current["project_id"],
                            task_id=task["id"],
                            summary="Task dispatch wait limit exhausted",
                            payload={
                                "from": TaskState.READY.value,
                                "to": TaskState.BLOCKED.value,
                                "reasonCode": "DISPATCH_WAIT_TIMEOUT",
                                "lastDeferralReason": reason_code,
                                "limitSeconds": self.dispatch_wait_timeout_seconds,
                            },
                            actor="runtime",
                        )
                        return True
                except json.JSONDecodeError:
                    pass
            self.store._append_event(
                connection,
                kind="dispatchDeferred",
                severity=EventSeverity.INFO,
                entity_type="task",
                entity_id=task["id"],
                project_id=task["project_id"],
                task_id=task["id"],
                summary="Task dispatch deferred for transient Worker capacity",
                payload=payload,
                actor="runtime",
            )
        return False

    async def apply_verification(
        self,
        task_id: str,
        result: DefinitionOfDoneResult,
        *,
        expected_verification_scope_id: str | None = None,
        expected_task_definition_revision: int | None = None,
        expected_source_attempt: int | None = None,
    ) -> TaskState:
        state = await asyncio.to_thread(
            self.store.apply_task_verification,
            task_id,
            result,
            max_attempts=self.max_attempts,
            expected_verification_scope_id=expected_verification_scope_id,
            expected_task_definition_revision=expected_task_definition_revision,
            expected_source_attempt=expected_source_attempt,
        )
        if state in {TaskState.SUCCEEDED, TaskState.FAILED}:
            await self.audit_task_terminal(task_id)
        return state

    async def cancel_task(self, task_id: str) -> bool:
        runs = await asyncio.to_thread(self.store.list_worker_runs, task_id)
        active_runs = [
            run
            for run in runs
            if run["state"]
            in {
                RunState.STARTING.value,
                RunState.RUNNING.value,
                RunState.WAITING.value,
            }
        ]
        # Canonical cancellation is persisted before contacting a provider.  Keep the prior run
        # snapshot so explicit user cancellation can still reach jobs atomically fenced out of
        # state mutation by that canonical transition.
        requested = await asyncio.to_thread(self.store.cancel_task_execution, task_id)
        cancellation_generation = None
        if active_runs:
            cancellation_generation = await asyncio.to_thread(
                self.store.claim_task_reconciliation,
                task_id,
                owner_id=self.runtime_id,
                lease_ttl_seconds=self.dispatch_lease_ttl_seconds,
                allow_cancelled=True,
                actor="cancellation",
            )
        try:
            for run in active_runs:
                requested = (
                    await self._cancel_worker_run(
                        task_id=task_id,
                        run=run,
                        lease_generation=cancellation_generation,
                    )
                    or requested
                )
        finally:
            if cancellation_generation is not None:
                await asyncio.to_thread(
                    self.store.release_task_execution_lease,
                    task_id,
                    owner_id=self.runtime_id,
                    generation=cancellation_generation,
                    actor="cancellation",
                )
        active = self._active.get(task_id)
        if active is not None:
            await asyncio.gather(active, return_exceptions=True)
        task = await asyncio.to_thread(self.store.get_task, task_id)
        for run in active_runs:
            if await asyncio.to_thread(self.telemetry.contains_run, run["id"]):
                continue
            role, fallback = self._role_for_recovered_run(task, run)
            started_at = self._parse_timestamp(run["started_at"] or run["created_at"])
            await self._record_invocation_telemetry(
                task_id=task_id,
                worker_id=run["worker_id"],
                attempt=int(run["attempt"]),
                task_role=role,
                fallback=fallback,
                result=WorkerResult(
                    run_id=run["id"],
                    state=RunState.CANCELLED,
                    pid=run["process_id"],
                    exit_code=None,
                    started_at=started_at,
                    ended_at=utc_now(),
                    stdout="",
                    stderr="",
                    final_text="",
                    events=(),
                    error="Task cancellation fenced the Worker execution",
                ),
            )
        if task["state"] == TaskState.CANCELLED.value:
            await self.audit_task_terminal(task_id, task=task)
        self._capacity_changed.set()
        return requested

    async def _cancel_worker_run(
        self,
        *,
        task_id: str,
        run: dict[str, Any],
        lease_generation: int | None,
    ) -> bool:
        try:
            provider_job = await asyncio.to_thread(self.store.get_provider_job, run["id"])
        except KeyError:
            provider_job = None
        try:
            adapter = self.adapters.get(run["worker_id"])
        except KeyError as error:
            await asyncio.to_thread(
                self.store.record_failure,
                classification=FailureClass.INFRASTRUCTURE,
                summary="Provider cancellation adapter is unavailable",
                detail=str(error),
                retryable=False,
                task_id=task_id,
                run_id=run["id"],
            )
            if provider_job is not None and lease_generation is not None:
                await self._escalate_reconciliation(
                    provider_job,
                    code="PROVIDER_STATE_AMBIGUOUS",
                    summary="Cancelled external job has no registered reconciliation adapter",
                    detail=str(error),
                    lease_generation=lease_generation,
                )
            return False
        try:
            durable = isinstance(adapter, DurableWorkerAdapter)
            if durable:
                await self._negotiate_adapter_job_contract(adapter)
            if durable and provider_job is not None:
                if (
                    adapter.adapter_type != provider_job["adapter_type"]
                    or adapter.adapter_instance_id != provider_job["adapter_instance_id"]
                ):
                    raise RuntimeError(
                        "cancellation adapter identity does not match the durable launch"
                    )
                if not provider_job["provider_job_id"]:
                    if lease_generation is not None:
                        await self._escalate_reconciliation(
                            provider_job,
                            code="IDEMPOTENCY_UNCERTAIN",
                            summary="Cancelled provider launch has no durable job handle",
                            detail=(
                                "Recovery will not cross the launch boundary for a cancelled Task"
                            ),
                            lease_generation=lease_generation,
                        )
                    return False
                accepted = False
                if bool(provider_job["supports_cancel"]) and bool(
                    adapter.job_capabilities.supports_cancel
                ):
                    accepted = await adapter.cancel_job(self._provider_job_handle(provider_job))
                if lease_generation is not None:
                    await self._reconcile_cancelled_provider_run(
                        task_id=task_id,
                        run=run,
                        job=provider_job,
                        adapter=adapter,
                        lease_generation=lease_generation,
                    )
                return accepted
            return await adapter.cancel(run["id"])
        except Exception as error:
            await asyncio.to_thread(
                self.store.record_failure,
                classification=FailureClass.INFRASTRUCTURE,
                summary="Provider cancellation request could not be confirmed",
                detail=f"{type(error).__name__}: {error}",
                retryable=False,
                task_id=task_id,
                run_id=run["id"],
            )
            if provider_job is not None and lease_generation is not None:
                await self._escalate_reconciliation(
                    provider_job,
                    code="PROVIDER_STATE_AMBIGUOUS",
                    summary="External job cancellation could not be confirmed",
                    detail=f"{type(error).__name__}: {error}",
                    lease_generation=lease_generation,
                )
            return False

    async def _reconcile_cancelled_provider_run(
        self,
        *,
        task_id: str,
        run: dict[str, Any],
        job: dict[str, Any],
        adapter: DurableWorkerAdapter,
        lease_generation: int,
    ) -> None:
        if not bool(job["supports_reconcile"]):
            await self._escalate_reconciliation(
                job,
                code="PROVIDER_STATE_AMBIGUOUS",
                summary="Cancelled provider job cannot be reconciled to a terminal state",
                detail="The adapter does not advertise reconciliation support",
                lease_generation=lease_generation,
            )
            return
        handle = self._provider_job_handle(job)
        try:
            observation = await adapter.reconcile_job(handle)
        except Exception as error:
            observation = None
            state = WorkerJobState.PROVIDER_UNREACHABLE
            detail = f"{type(error).__name__}: {error}"
        else:
            state = observation.state
            detail = observation.detail
        await asyncio.to_thread(
            self.store.record_provider_job_observation,
            run["id"],
            state=state.value,
            detail=detail,
            lease_owner_id=self.runtime_id,
            lease_generation=lease_generation,
            actor="cancellation",
        )
        if state in {
            WorkerJobState.KNOWN_CANCELLED,
            WorkerJobState.KNOWN_COMPLETED,
            WorkerJobState.KNOWN_FAILED,
        }:
            if not bool(job["supports_repeatable_collect"]):
                await self._escalate_reconciliation(
                    job,
                    code="RESULT_COLLECTION_UNCERTAIN",
                    summary="Cancelled provider job is terminal but its result is not repeatable",
                    detail="Canonical Task cancellation remains authoritative",
                    lease_generation=lease_generation,
                )
                return
            task = await asyncio.to_thread(self.store.get_task, task_id)
            project = await asyncio.to_thread(self.store.get_project, task["project_id"])
            request = self._worker_request(
                task,
                project,
                self._requirements(task),
                run["id"],
            )
            result = await adapter.collect_job(request, handle)
            self._validate_collected_result(state, result)
            role, fallback = self._role_for_recovered_run(task, {**job, **run})
            await self._persist_worker_result(
                task=task,
                worker_id=run["worker_id"],
                run_id=run["id"],
                attempt=int(run["attempt"]),
                task_role=role,
                fallback=fallback,
                lease_generation=lease_generation,
                result=result,
            )
            return
        if state is WorkerJobState.PROVIDER_NOT_FOUND:
            return
        await self._escalate_reconciliation(
            job,
            code=(
                "EXTERNAL_JOB_UNREACHABLE"
                if state is WorkerJobState.PROVIDER_UNREACHABLE
                else "PROVIDER_STATE_AMBIGUOUS"
            ),
            summary="External job cancellation remains unconfirmed",
            detail=detail,
            lease_generation=lease_generation,
        )

    async def audit_task_terminal(
        self, task_id: str, *, task: dict[str, Any] | None = None
    ) -> None:
        task = task or await asyncio.to_thread(self.store.get_task, task_id)
        if task["state"] not in {
            TaskState.SUCCEEDED.value,
            TaskState.FAILED.value,
            TaskState.CANCELLED.value,
        }:
            return
        await asyncio.to_thread(
            self.resource_usage.audit_task_terminal,
            task_id=task_id,
            run_id=None,
            terminal_state=task["state"],
            audit_key=(f"task:{task_id}:version:{task['version']}:state:{task['state']}"),
        )

    async def audit_pending_terminal_tasks(self, *, task_ids: set[str] | None = None) -> None:
        for task in await asyncio.to_thread(self.store.list_tasks):
            if task_ids is not None and task["id"] not in task_ids:
                continue
            await self.audit_task_terminal(task["id"], task=task)

    async def _execute_task(
        self,
        task: dict[str, Any],
        requirements: TaskRequirements,
        worker_ids: tuple[str, ...],
        *,
        attempt: int,
        run_ids: dict[str, str],
        lease_generation: int,
    ) -> None:
        execution = asyncio.create_task(
            self._execute_task_under_lease(
                task,
                requirements,
                worker_ids,
                attempt=attempt,
                run_ids=run_ids,
                lease_generation=lease_generation,
            ),
            name=f"supervisor-execution:{task['id']}",
        )
        heartbeat = asyncio.create_task(
            self._maintain_task_execution_lease(task["id"], lease_generation),
            name=f"supervisor-lease:{task['id']}",
        )
        try:
            done, _pending = await asyncio.wait(
                {execution, heartbeat}, return_when=asyncio.FIRST_COMPLETED
            )
            if execution in done:
                await execution
            else:
                # Lease loss fences this stale Runtime before a peer reconciles/retries the Task.
                # Adapter cancellation remains best-effort because providers differ, but no more
                # canonical Task/run transitions are attempted by this execution coroutine.
                for worker_id, run_id in run_ids.items():
                    adapter = self.adapters.get(worker_id)
                    # Ownership transfer is not user cancellation.  A durable adapter must
                    # detach so the new lease generation can reattach to the same provider job.
                    if isinstance(adapter, DurableWorkerAdapter):
                        continue
                    with suppress(Exception):
                        await adapter.cancel(run_id)
                execution.cancel()
                await asyncio.gather(execution, return_exceptions=True)
                await asyncio.to_thread(
                    self.store.record_failure,
                    classification=FailureClass.INFRASTRUCTURE,
                    summary="Task execution ownership lease was lost",
                    detail="stale Runtime stopped and requested adapter cancellation",
                    retryable=True,
                    task_id=task["id"],
                )
        finally:
            heartbeat.cancel()
            await asyncio.gather(heartbeat, return_exceptions=True)
            await asyncio.to_thread(
                self.store.release_task_execution_lease,
                task["id"],
                owner_id=self.runtime_id,
                generation=lease_generation,
            )

    async def _maintain_task_execution_lease(
        self,
        task_id: str,
        generation: int,
    ) -> None:
        interval = min(30.0, self.dispatch_lease_ttl_seconds / 3)
        while True:
            await asyncio.sleep(interval)
            renewed = await asyncio.to_thread(
                self.store.heartbeat_task_execution_lease,
                task_id,
                owner_id=self.runtime_id,
                generation=generation,
                ttl_seconds=self.dispatch_lease_ttl_seconds,
            )
            if not renewed:
                return

    async def _execute_task_under_lease(
        self,
        task: dict[str, Any],
        requirements: TaskRequirements,
        worker_ids: tuple[str, ...],
        *,
        attempt: int,
        run_ids: dict[str, str],
        lease_generation: int,
    ) -> None:
        project = await asyncio.to_thread(self.store.get_project, task["project_id"])
        role_assignments = self._task_roles(
            ExecutionTopology(task["topology"]), worker_ids, attempt=attempt
        )
        coroutines = [
            self._execute_worker(
                task=task,
                project=project,
                requirements=requirements,
                worker_id=worker_id,
                run_id=run_ids[worker_id],
                attempt=attempt,
                task_role=task_role,
                fallback=fallback,
                lease_generation=lease_generation,
            )
            for worker_id, task_role, fallback in role_assignments
        ]
        results = await asyncio.gather(*coroutines, return_exceptions=True)
        await self._finish_task_attempt(
            task=task,
            attempt=attempt,
            results=results,
            lease_generation=lease_generation,
        )

    async def _finish_task_attempt(
        self,
        *,
        task: dict[str, Any],
        attempt: int,
        results: list[WorkerResult | BaseException] | tuple[WorkerResult | BaseException, ...],
        lease_generation: int,
    ) -> None:
        lease_is_current = await asyncio.to_thread(
            self.store.task_execution_lease_is_current,
            task["id"],
            owner_id=self.runtime_id,
            generation=lease_generation,
        )
        if not lease_is_current:
            return
        current_task = await asyncio.to_thread(self.store.get_task, task["id"])
        if current_task["state"] != TaskState.RUNNING.value:
            return
        attempt_runs = [
            run
            for run in await asyncio.to_thread(self.store.list_worker_runs, task["id"])
            if int(run["attempt"]) == attempt
        ]
        succeeded = bool(attempt_runs) and all(
            run["state"] == RunState.COMPLETED.value and run["exit_code"] == 0
            for run in attempt_runs
        )
        if succeeded:
            await asyncio.to_thread(
                self.store.transition_task,
                task["id"],
                TaskState.REVIEWING,
                summary="Worker execution complete; deterministic verification required",
                lease_owner_id=self.runtime_id,
                lease_generation=lease_generation,
            )
        else:
            failures = [str(item) for item in results if isinstance(item, Exception)]
            failures.extend(
                str(run["failure_detail"] or run["state"])
                for run in attempt_runs
                if run["state"] != RunState.COMPLETED.value or run["exit_code"] != 0
            )
            waiting = await asyncio.to_thread(
                self.store.transition_task,
                task["id"],
                TaskState.WAITING,
                summary="One or more worker runs failed; retry or fallback required",
                payload={"failures": failures},
                lease_owner_id=self.runtime_id,
                lease_generation=lease_generation,
            )
            if attempt < self.max_attempts:
                await asyncio.to_thread(
                    self.store.transition_task,
                    task["id"],
                    TaskState.READY,
                    summary=f"Retry scheduled after attempt {attempt}",
                    payload={"nextAttempt": attempt + 1},
                    lease_owner_id=self.runtime_id,
                    lease_generation=lease_generation,
                )
            else:
                terminal = await asyncio.to_thread(
                    self.store.transition_task,
                    task["id"],
                    TaskState.FAILED,
                    summary=f"Task exhausted {attempt} execution attempts",
                    payload={"failures": failures},
                    expected_version=waiting["version"],
                    lease_owner_id=self.runtime_id,
                    lease_generation=lease_generation,
                )
                await self.audit_task_terminal(task["id"], task=terminal)

    async def _execute_worker(
        self,
        *,
        task: dict[str, Any],
        project: dict[str, Any],
        requirements: TaskRequirements,
        worker_id: str,
        run_id: str,
        attempt: int,
        task_role: TaskRole,
        fallback: bool,
        lease_generation: int,
    ) -> WorkerResult:
        activated = await asyncio.to_thread(
            self.store.activate_worker_run,
            run_id,
            lease_owner_id=self.runtime_id,
            lease_generation=lease_generation,
        )
        if not activated:
            now = utc_now()
            return WorkerResult(
                run_id=run_id,
                state=RunState.CANCELLED,
                pid=None,
                exit_code=None,
                started_at=now,
                ended_at=now,
                stdout="",
                stderr="",
                final_text="",
                events=(),
                error="cancelled before adapter activation",
            )

        async def event_sink(event: WorkerEvent) -> None:
            await asyncio.to_thread(
                self.store.record_adapter_event,
                run_id=run_id,
                kind=event.kind,
                payload=dict(event.payload),
                lease_owner_id=self.runtime_id,
                lease_generation=lease_generation,
            )

        adapter = self.adapters.get(worker_id)
        provider_job = await asyncio.to_thread(self.store.get_provider_job, run_id)
        request = WorkerRequest(
            run_id=run_id,
            task_id=task["id"],
            prompt=task["description"],
            working_directory=Path(project["root_path"]),
            timeout_seconds=self.worker_timeout_seconds,
            code_write_required=requirements.code_write_required,
            metadata={
                "labels": [label.value for label in requirements.labels],
                "requested_capabilities": sorted(requirements.required_capabilities),
                "timeout_seconds": self.worker_timeout_seconds,
            },
        )
        invocation_started_at = utc_now()
        try:
            await asyncio.to_thread(
                self.store.mark_provider_job_launching,
                run_id,
                lease_owner_id=self.runtime_id,
                lease_generation=lease_generation,
            )
            if isinstance(adapter, DurableWorkerAdapter):
                handle = await adapter.start_job(
                    request,
                    idempotency_key=str(provider_job["idempotency_key"]),
                    event_sink=event_sink,
                )
                if handle.run_id != run_id:
                    raise RuntimeError("provider job handle belongs to a different Worker run")
                await asyncio.to_thread(
                    self.store.bind_provider_job_handle,
                    run_id=run_id,
                    adapter_type=handle.adapter_type,
                    adapter_instance_id=handle.adapter_instance_id,
                    handle_version=handle.schema_version,
                    provider_job_id=handle.provider_job_id,
                    provider_session_id=handle.provider_session_id,
                    runtime_pid=handle.runtime_pid,
                    runtime_host=handle.runtime_host,
                    runtime_identity=handle.process_identity,
                    adapter_metadata=handle.metadata,
                    lease_owner_id=self.runtime_id,
                    lease_generation=lease_generation,
                )
                result = await adapter.resume_job(request, handle, event_sink=event_sink)
            else:
                result = await adapter.execute(request, event_sink=event_sink)
        except Exception as error:
            if isinstance(adapter, DurableWorkerAdapter):
                rejected = self._definite_launch_rejection(
                    run_id=run_id,
                    started_at=invocation_started_at,
                    error=error,
                )
                if rejected is not None:
                    return await self._persist_worker_result(
                        task=task,
                        worker_id=worker_id,
                        run_id=run_id,
                        attempt=attempt,
                        task_role=task_role,
                        fallback=fallback,
                        lease_generation=lease_generation,
                        result=rejected,
                    )
                return await self._hold_ambiguous_provider_job(
                    task=task,
                    worker_id=worker_id,
                    run_id=run_id,
                    attempt=attempt,
                    task_role=task_role,
                    fallback=fallback,
                    lease_generation=lease_generation,
                    error=error,
                    invocation_started_at=invocation_started_at,
                )
            classification = self._exception_classification(error)
            await asyncio.to_thread(
                self.store.transition_worker_run,
                run_id,
                RunState.FAILED,
                failure_class=classification,
                failure_detail=str(error),
                lease_owner_id=self.runtime_id,
                lease_generation=lease_generation,
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
            await asyncio.to_thread(
                self.store.set_worker_state,
                worker_id,
                WorkerState.IDLE,
                task_id=task["id"],
                lease_owner_id=self.runtime_id,
                lease_generation=lease_generation,
            )
            synthetic_result = WorkerResult(
                run_id=run_id,
                state=RunState.FAILED,
                pid=None,
                exit_code=None,
                started_at=invocation_started_at,
                ended_at=utc_now(),
                stdout="",
                stderr="",
                final_text="",
                events=(),
                error=type(error).__name__,
            )
            await self._record_invocation_telemetry(
                task_id=task["id"],
                worker_id=worker_id,
                attempt=attempt,
                task_role=task_role,
                fallback=fallback,
                result=synthetic_result,
            )
            raise
        return await self._persist_worker_result(
            task=task,
            worker_id=worker_id,
            run_id=run_id,
            attempt=attempt,
            task_role=task_role,
            fallback=fallback,
            lease_generation=lease_generation,
            result=result,
        )

    @staticmethod
    def _definite_launch_rejection(
        *, run_id: str, started_at: datetime, error: Exception
    ) -> WorkerResult | None:
        """Translate an adapter-proven pre-side-effect rejection.

        A raw HTTP error cannot prove this boundary: a provider or adapter may return one after
        accepting work. Only the explicit durable adapter contract permits automatic retry.
        """

        if not isinstance(error, WorkerJobLaunchRejected):
            return None
        status = error.status_code
        state = (
            RunState.AUTH_REQUIRED
            if status in {401, 403}
            else RunState.RATE_LIMITED
            if status == 429
            else RunState.FAILED
        )
        return WorkerResult(
            run_id=run_id,
            state=state,
            pid=None,
            exit_code=None,
            started_at=started_at,
            ended_at=utc_now(),
            stdout="",
            stderr="",
            final_text="",
            events=(),
            error=f"provider rejected launch request with HTTP {status}",
        )

    async def _persist_worker_result(
        self,
        *,
        task: dict[str, Any],
        worker_id: str,
        run_id: str,
        attempt: int,
        task_role: TaskRole,
        fallback: bool,
        lease_generation: int,
        result: WorkerResult,
    ) -> WorkerResult:
        """Persist a normalized provider result with exactly-once canonical finalization."""

        result = replace(result, run_id=run_id)
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
            lease_owner_id=self.runtime_id,
            lease_generation=lease_generation,
        )
        await self._record_usage(task["id"], run_id, worker_id, result)
        evidence_path = await asyncio.to_thread(self._write_run_evidence, result)
        failure_class = self._result_failure_class(result)
        resource_state = {
            RunState.RATE_LIMITED: ResourceState.RATE_LIMITED,
            RunState.AUTH_REQUIRED: ResourceState.UNKNOWN,
        }.get(result.state)
        if not result.succeeded:
            await asyncio.to_thread(
                self.store.record_failure,
                classification=failure_class or FailureClass.TRANSIENT,
                summary=f"Worker ended in {result.state.value}",
                detail=result.error,
                retryable=result.state
                in {RunState.FAILED, RunState.TIMED_OUT, RunState.RATE_LIMITED},
                record_id=f"failure:{run_id}:terminal",
                task_id=task["id"],
                run_id=run_id,
            )
        await self._record_invocation_telemetry(
            task_id=task["id"],
            worker_id=worker_id,
            attempt=attempt,
            task_role=task_role,
            fallback=fallback,
            result=result,
        )
        # Result, accounting, evidence and invocation telemetry are replay-idempotent.  Commit
        # them before the atomic provider-job terminal marker so that marker means the complete
        # canonical effect is recoverable without ancillary gaps.
        await asyncio.to_thread(
            self.store.finalize_provider_job_result,
            run_id,
            result.state,
            process_id=result.pid,
            exit_code=result.exit_code,
            session_id=internal_session_id,
            raw_output_reference=str(evidence_path),
            failure_class=failure_class,
            failure_detail=result.error,
            worker_state=(
                WorkerState.IDLE
                if result.state is not RunState.AUTH_REQUIRED
                else WorkerState.OFFLINE
            ),
            resource_state=resource_state,
            model_id=model_id,
            lease_owner_id=self.runtime_id,
            lease_generation=lease_generation,
        )
        return result

    async def _hold_ambiguous_provider_job(
        self,
        *,
        task: dict[str, Any],
        worker_id: str,
        run_id: str,
        attempt: int,
        task_role: TaskRole,
        fallback: bool,
        lease_generation: int,
        error: Exception,
        invocation_started_at: datetime,
    ) -> WorkerResult:
        """Fail closed when a durable launch or result query may have crossed the provider."""

        job = await asyncio.to_thread(self.store.get_provider_job, run_id)
        has_handle = bool(job["provider_job_id"])
        if isinstance(error, WorkerJobOutcomeUncertain):
            state = error.state
            code = (
                "EXTERNAL_JOB_UNREACHABLE"
                if state is WorkerJobState.PROVIDER_UNREACHABLE
                else "PROVIDER_STATE_AMBIGUOUS"
            )
        else:
            state = WorkerJobState.PROVIDER_UNREACHABLE if has_handle else WorkerJobState.UNKNOWN
            code = "EXTERNAL_JOB_UNREACHABLE" if has_handle else "IDEMPOTENCY_UNCERTAIN"
        detail = f"{type(error).__name__}: {error}"
        await asyncio.to_thread(
            self.store.record_provider_job_observation,
            run_id,
            state=state.value,
            detail=detail,
            lease_owner_id=self.runtime_id,
            lease_generation=lease_generation,
        )
        await asyncio.to_thread(
            self.store.request_execution_escalation,
            run_id=run_id,
            code=code,
            summary="Provider execution cannot be retried without duplication risk",
            detail=detail,
            lease_owner_id=self.runtime_id,
            lease_generation=lease_generation,
        )
        current_task = await asyncio.to_thread(self.store.get_task, task["id"])
        cancelled = current_task["state"] == TaskState.CANCELLED.value
        await asyncio.to_thread(
            self.store.transition_worker_run,
            run_id,
            RunState.CANCELLED if cancelled else RunState.WAITING,
            failure_class=(FailureClass.CANCELLED if cancelled else FailureClass.INFRASTRUCTURE),
            failure_detail="provider execution state is ambiguous",
            lease_owner_id=self.runtime_id,
            lease_generation=lease_generation,
        )
        await asyncio.to_thread(
            self.store.set_worker_state,
            worker_id,
            WorkerState.IDLE if cancelled else WorkerState.WAITING,
            task_id=task["id"],
            lease_owner_id=self.runtime_id,
            lease_generation=lease_generation,
        )
        if not cancelled:
            current_task = await asyncio.to_thread(self.store.get_task, task["id"])
            if current_task["state"] == TaskState.RUNNING.value:
                await asyncio.to_thread(
                    self.store.transition_task,
                    task["id"],
                    TaskState.WAITING,
                    summary="Provider state is ambiguous; automatic retry withheld",
                    payload={"reasonCode": code},
                    lease_owner_id=self.runtime_id,
                    lease_generation=lease_generation,
                )
        synthetic = WorkerResult(
            run_id=run_id,
            state=RunState.CANCELLED if cancelled else RunState.WAITING,
            pid=None,
            exit_code=None,
            started_at=invocation_started_at,
            ended_at=utc_now(),
            stdout="",
            stderr="",
            final_text="",
            events=(),
            error=code,
        )
        # WAITING is an observation checkpoint, not a terminal invocation outcome.  Reserving
        # the run's unique telemetry identity here would prevent a later reconciled success from
        # being recorded truthfully.
        if cancelled:
            await self._record_invocation_telemetry(
                task_id=task["id"],
                worker_id=worker_id,
                attempt=attempt,
                task_role=task_role,
                fallback=fallback,
                result=synthetic,
            )
        return synthetic

    @staticmethod
    def _validate_collected_result(
        observed_state: WorkerJobState,
        result: WorkerResult,
    ) -> None:
        expected = {
            WorkerJobState.KNOWN_COMPLETED: {RunState.COMPLETED},
            WorkerJobState.KNOWN_FAILED: {
                RunState.FAILED,
                RunState.TIMED_OUT,
                RunState.AUTH_REQUIRED,
                RunState.RATE_LIMITED,
            },
            WorkerJobState.KNOWN_CANCELLED: {RunState.CANCELLED},
        }.get(observed_state)
        if expected is None or result.state not in expected:
            allowed = ", ".join(sorted(state.value for state in expected or ())) or "none"
            raise WorkerProtocolError(
                f"collected result {result.state.value} conflicts with provider observation "
                f"{observed_state.value}; expected {allowed}"
            )

    async def _record_invocation_telemetry(
        self,
        *,
        task_id: str,
        worker_id: str,
        attempt: int,
        task_role: TaskRole,
        fallback: bool,
        result: WorkerResult,
    ) -> None:
        # The ID and run relationship are durable and unique.  A crash after telemetry insert
        # but before provider-job finalization must be able to replay collection harmlessly.
        if await asyncio.to_thread(self.telemetry.contains_run, result.run_id):
            return
        worker = next(
            row
            for row in await asyncio.to_thread(self.store.list_workers)
            if row["id"] == worker_id
        )
        harness = worker.get("harness")
        executor_kind = (
            ExecutorKind.CODEX
            if harness == Harness.CODEX.value
            else ExecutorKind.OFFLOADED
            if harness
            else ExecutorKind.UNKNOWN
        )
        goal_id, planned_role = await asyncio.to_thread(
            self.telemetry.goal_context_for_task, task_id
        )
        record = InvocationTelemetry.from_worker_result(
            telemetry_id=f"invocation-{result.run_id}",
            goal_id=goal_id,
            task_id=task_id,
            worker_id=worker_id,
            provider=worker["provider"],
            node_id=worker["node_id"],
            task_role=planned_role or task_role,
            result=result,
            retry_count=attempt - 1,
            fallback=fallback,
            executor_kind=executor_kind,
        )
        await asyncio.to_thread(self.telemetry.record, record)

    @staticmethod
    def _adapter_job_contract(
        worker_id: str, adapter: WorkerAdapter
    ) -> tuple[str, str, dict[str, bool | int]]:
        if isinstance(adapter, DurableWorkerAdapter):
            capabilities = adapter.job_capabilities
            return (
                adapter.adapter_type,
                adapter.adapter_instance_id,
                {
                    "protocol_version": int(getattr(adapter, "protocol_version", 1)),
                    "supports_reconcile": capabilities.supports_reconcile,
                    "supports_resume": capabilities.supports_resume,
                    "supports_cancel": capabilities.supports_cancel,
                    "supports_durable_cancel": capabilities.supports_cancel,
                    "supports_provider_idempotency": (capabilities.supports_provider_idempotency),
                    "supports_stream_reconnect": capabilities.supports_stream_reconnect,
                    "supports_repeatable_collect": capabilities.supports_repeatable_collect,
                    "supports_idempotent_launch_lookup": (
                        capabilities.supports_idempotent_launch_lookup
                    ),
                    "supports_durable_launch_registry": (
                        capabilities.supports_durable_launch_registry
                    ),
                },
            )
        adapter_type = f"legacy:{type(adapter).__module__}.{type(adapter).__qualname__}"
        return (
            adapter_type,
            f"worker:{worker_id}:{adapter_type}",
            {
                "protocol_version": 1,
                "supports_reconcile": False,
                "supports_resume": False,
                "supports_cancel": False,
                "supports_durable_cancel": False,
                "supports_provider_idempotency": False,
                "supports_stream_reconnect": False,
                "supports_repeatable_collect": False,
                "supports_idempotent_launch_lookup": False,
                "supports_durable_launch_registry": False,
            },
        )

    @staticmethod
    async def _negotiate_adapter_job_contract(adapter: WorkerAdapter) -> None:
        """Freeze an observed adapter contract before persisting or comparing job identity."""

        if isinstance(adapter, NegotiatingWorkerAdapter):
            await adapter.negotiate_job_contract()

    @staticmethod
    def _task_roles(
        topology: ExecutionTopology,
        worker_ids: tuple[str, ...],
        *,
        attempt: int,
    ) -> tuple[tuple[str, TaskRole, bool], ...]:
        if topology is ExecutionTopology.PRIMARY_REVIEWER:
            return tuple(
                (worker_id, TaskRole.PRIMARY if index == 0 else TaskRole.REVIEWER, False)
                for index, worker_id in enumerate(worker_ids)
            )
        if topology is ExecutionTopology.PARALLEL_PANEL:
            return tuple((worker_id, TaskRole.PANELIST, False) for worker_id in worker_ids)
        is_fallback = (
            topology
            in {
                ExecutionTopology.FALLBACK,
                ExecutionTopology.CHEAP_FIRST_ESCALATION,
            }
            and attempt > 1
        )
        return tuple(
            (
                worker_id,
                TaskRole.FALLBACK if is_fallback else TaskRole.PRIMARY,
                is_fallback,
            )
            for worker_id in worker_ids
        )

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
        try:
            with path.open("x", encoding="utf-8") as handle:
                handle.write(json.dumps(payload, indent=2, sort_keys=True))
        except FileExistsError:
            # Evidence belongs to the immutable Worker run.  A repeatable provider collection
            # after restart must not rewrite the first canonical capture.
            pass
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
                record_id=f"usg:{run_id}:{metric}",
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
