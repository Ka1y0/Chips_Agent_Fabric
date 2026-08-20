from __future__ import annotations

import asyncio
from dataclasses import dataclass, field

import httpx

from project_supervisor.adapters import (
    EventSink,
    Usage,
    WorkerAdapter,
    WorkerEvent,
    WorkerJobCapabilities,
    WorkerJobHandle,
    WorkerJobLaunchRejected,
    WorkerJobObservation,
    WorkerJobOutcomeUncertain,
    WorkerJobState,
    WorkerRequest,
    WorkerResult,
)
from project_supervisor.domain import (
    Harness,
    ModelDescriptor,
    NodeState,
    Provider,
    ResourceState,
    RunState,
    TaskLabel,
    TaskRequirements,
    TaskState,
    WorkerSnapshot,
    WorkerState,
    utc_now,
)
from project_supervisor.fabric.provider_execution import ProviderInvocationRepository, TriState
from project_supervisor.runtime import AdapterRegistry, SupervisorRuntime
from project_supervisor.scheduler import DeterministicScheduler
from project_supervisor.store import StateStore


@dataclass
class ExternalJob:
    id: str
    run_id: str
    state: WorkerJobState = WorkerJobState.KNOWN_RUNNING
    text: str = "durable provider result"
    changed: asyncio.Event = field(default_factory=asyncio.Event)


@dataclass
class FakeProvider:
    jobs: dict[str, ExternalJob] = field(default_factory=dict)
    by_idempotency_key: dict[str, str] = field(default_factory=dict)
    start_calls: int = 0
    reconcile_calls: int = 0
    resume_calls: int = 0
    collect_calls: int = 0
    cancel_calls: int = 0
    unreachable: bool = False
    missing: bool = False
    emit_data_disclosed_on_resume: bool = False

    def complete(self, job_id: str) -> None:
        job = self.jobs[job_id]
        job.state = WorkerJobState.KNOWN_COMPLETED
        job.changed.set()


class DurableFakeAdapter(WorkerAdapter):
    adapter_type = "durable-fake-v1"
    adapter_instance_id = "durable-fake-instance"

    def __init__(
        self,
        provider: FakeProvider,
        *,
        supports_resume: bool = True,
        supports_repeatable_collect: bool = True,
    ) -> None:
        self.provider = provider
        self.job_capabilities = WorkerJobCapabilities(
            supports_reconcile=True,
            supports_resume=supports_resume,
            supports_cancel=True,
            supports_provider_idempotency=True,
            supports_stream_reconnect=False,
            supports_repeatable_collect=supports_repeatable_collect,
        )
        self._handles: dict[str, WorkerJobHandle] = {}

    async def execute(
        self,
        request: WorkerRequest,
        *,
        event_sink: EventSink | None = None,
    ) -> WorkerResult:
        handle = await self.start_job(
            request,
            idempotency_key=request.run_id,
            event_sink=event_sink,
        )
        return await self.resume_job(request, handle, event_sink=event_sink)

    async def cancel(self, run_id: str) -> bool:
        handle = self._handles.get(run_id)
        return await self.cancel_job(handle) if handle is not None else False

    async def start_job(
        self,
        request: WorkerRequest,
        *,
        idempotency_key: str,
        event_sink: EventSink | None = None,
    ) -> WorkerJobHandle:
        self.provider.start_calls += 1
        job_id = self.provider.by_idempotency_key.get(idempotency_key)
        if job_id is None:
            job_id = f"external-{len(self.provider.jobs) + 1}"
            self.provider.by_idempotency_key[idempotency_key] = job_id
            self.provider.jobs[job_id] = ExternalJob(job_id, request.run_id)
        handle = WorkerJobHandle(
            run_id=request.run_id,
            adapter_type=self.adapter_type,
            adapter_instance_id=self.adapter_instance_id,
            provider_job_id=job_id,
            provider_session_id=f"session-{job_id}",
            created_at=utc_now(),
            metadata={"safeCursor": "cursor-1"},
        )
        self._handles[request.run_id] = handle
        if event_sink is not None:
            await event_sink(
                WorkerEvent(request.run_id, "remoteJobStarted", utc_now(), {"jobId": job_id})
            )
        return handle

    async def reconcile_job(self, handle: WorkerJobHandle) -> WorkerJobObservation:
        self.provider.reconcile_calls += 1
        if self.provider.unreachable:
            return WorkerJobObservation(
                WorkerJobState.PROVIDER_UNREACHABLE,
                utc_now(),
                detail="test provider unreachable",
            )
        if self.provider.missing or handle.provider_job_id not in self.provider.jobs:
            return WorkerJobObservation(WorkerJobState.PROVIDER_NOT_FOUND, utc_now())
        job = self.provider.jobs[handle.provider_job_id]
        return WorkerJobObservation(
            job.state,
            utc_now(),
            metadata={"providerState": job.state.value},
        )

    async def resume_job(
        self,
        request: WorkerRequest,
        handle: WorkerJobHandle,
        *,
        event_sink: EventSink | None = None,
    ) -> WorkerResult:
        self.provider.resume_calls += 1
        job = self.provider.jobs[handle.provider_job_id]
        if self.provider.emit_data_disclosed_on_resume and event_sink is not None:
            await event_sink(
                WorkerEvent(
                    request.run_id,
                    "providerDataDisclosed",
                    utc_now(),
                    {"model": "durable-fake-model"},
                )
            )
        while job.state is WorkerJobState.KNOWN_RUNNING:
            await job.changed.wait()
            job.changed.clear()
        return self._result(request, handle, job)

    async def collect_job(
        self,
        request: WorkerRequest,
        handle: WorkerJobHandle,
        *,
        event_sink: EventSink | None = None,
    ) -> WorkerResult:
        self.provider.collect_calls += 1
        job = self.provider.jobs[handle.provider_job_id]
        if job.state is WorkerJobState.KNOWN_RUNNING:
            raise RuntimeError("job is not terminal")
        return self._result(request, handle, job)

    async def cancel_job(self, handle: WorkerJobHandle) -> bool:
        self.provider.cancel_calls += 1
        job = self.provider.jobs.get(handle.provider_job_id)
        if job is None:
            return False
        job.state = WorkerJobState.KNOWN_CANCELLED
        job.changed.set()
        return True

    @staticmethod
    def _result(request: WorkerRequest, handle: WorkerJobHandle, job: ExternalJob) -> WorkerResult:
        state = {
            WorkerJobState.KNOWN_COMPLETED: RunState.COMPLETED,
            WorkerJobState.KNOWN_FAILED: RunState.FAILED,
            WorkerJobState.KNOWN_CANCELLED: RunState.CANCELLED,
        }[job.state]
        return WorkerResult(
            run_id=request.run_id,
            state=state,
            pid=None,
            exit_code=0 if state is RunState.COMPLETED else None,
            started_at=handle.created_at,
            ended_at=utc_now(),
            stdout=job.text,
            stderr="",
            final_text=job.text,
            events=(),
            session_id=handle.provider_session_id,
            model="durable-fake-model",
            usage=Usage(total_tokens=2, cost_usd=0.0),
            error=None if state is RunState.COMPLETED else state.value,
        )


def runtime_fixture(
    tmp_path,
    provider: FakeProvider,
    *,
    runtime_id: str,
    supports_resume: bool = True,
    supports_repeatable_collect: bool = True,
    max_attempts: int = 2,
) -> tuple[StateStore, SupervisorRuntime, DurableFakeAdapter]:
    workspace = tmp_path / "workspace"
    workspace.mkdir(exist_ok=True)
    store = StateStore(tmp_path / "state.db")
    try:
        store.get_project("project-1")
    except KeyError:
        store.create_project(
            project_id="project-1",
            name="Reconciliation fixture",
            root_path=str(workspace),
            goal="Resume one durable external job",
        )
        store.upsert_node(
            node_id="node-1",
            hostname="fixture",
            display_name="Fixture",
            role="worker",
            state=NodeState.ONLINE,
        )
        store.upsert_worker(
            WorkerSnapshot(
                id="worker-1",
                node_id="node-1",
                harness=Harness.MOCK,
                provider=Provider.MOCK,
                model=ModelDescriptor("declared", "Declared", Provider.MOCK),
                state=WorkerState.IDLE,
                node_state=NodeState.ONLINE,
                resource_state=ResourceState.AVAILABLE,
                capabilities=frozenset({"analysis"}),
                code_write_allowed=False,
                privacy_allowed=True,
            )
        )
    adapter = DurableFakeAdapter(
        provider,
        supports_resume=supports_resume,
        supports_repeatable_collect=supports_repeatable_collect,
    )
    registry = AdapterRegistry()
    registry.register("worker-1", adapter)
    runtime = SupervisorRuntime(
        store=store,
        scheduler=DeterministicScheduler(),
        adapters=registry,
        evidence_root=tmp_path / f"evidence-{runtime_id}",
        runtime_id=runtime_id,
        dispatch_lease_ttl_seconds=30,
        max_attempts=max_attempts,
    )
    return store, runtime, adapter


async def submit(runtime: SupervisorRuntime) -> str:
    return await runtime.submit_task(
        project_id="project-1",
        task_id="task-1",
        title="Durable provider task",
        description="Run once across a Supervisor restart",
        requirements=TaskRequirements(
            labels=frozenset({TaskLabel.RESEARCH}),
            required_capabilities=frozenset({"analysis"}),
        ),
    )


async def simulate_runtime_crash(runtime: SupervisorRuntime, task_id: str) -> None:
    active = runtime._active[task_id]
    active.cancel()
    await asyncio.gather(active, return_exceptions=True)
    children = [
        task
        for task in asyncio.all_tasks()
        if task is not asyncio.current_task()
        and task.get_name() == f"supervisor-execution:{task_id}"
    ]
    for child in children:
        child.cancel()
    await asyncio.gather(*children, return_exceptions=True)


async def launch_and_crash(
    tmp_path,
    provider: FakeProvider,
    *,
    supports_resume: bool = True,
    supports_repeatable_collect: bool = True,
) -> tuple[StateStore, str, str]:
    store, runtime, _adapter = runtime_fixture(
        tmp_path,
        provider,
        runtime_id="runtime-a",
        supports_resume=supports_resume,
        supports_repeatable_collect=supports_repeatable_collect,
    )
    task_id = await submit(runtime)
    dispatched = await runtime.dispatch_ready()
    assert dispatched.launched_task_ids == (task_id,)
    for _ in range(100):
        jobs = store.list_provider_jobs(task_id=task_id)
        if jobs and jobs[0]["launch_state"] == "bound":
            break
        await asyncio.sleep(0.001)
    else:
        raise AssertionError("provider handle was not durably bound")
    run_id = jobs[0]["run_id"]
    await simulate_runtime_crash(runtime, task_id)
    return store, task_id, run_id


async def test_restart_reattaches_running_job_without_duplicate_launch(tmp_path) -> None:
    provider = FakeProvider()
    store, task_id, run_id = await launch_and_crash(tmp_path, provider)
    _peer_store, peer, _adapter = runtime_fixture(tmp_path, provider, runtime_id="runtime-b")

    await peer.recover(task_ids={task_id})

    assert provider.start_calls == 1
    assert store.get_worker_run(run_id)["state"] == RunState.RUNNING.value
    assert task_id in peer._active
    provider.complete(store.get_provider_job(run_id)["provider_job_id"])
    await peer.wait_for_active(task_ids={task_id})
    assert store.get_task(task_id)["state"] == TaskState.REVIEWING.value
    assert store.get_worker_run(run_id)["state"] == RunState.COMPLETED.value
    assert provider.start_calls == 1


async def test_restart_folds_authoritative_provider_events_emitted_during_resume(tmp_path) -> None:
    provider = FakeProvider(emit_data_disclosed_on_resume=True)
    store, task_id, run_id = await launch_and_crash(tmp_path, provider)
    _peer_store, peer, _adapter = runtime_fixture(tmp_path, provider, runtime_id="runtime-b")

    await peer.recover(task_ids={task_id})
    provider.complete(store.get_provider_job(run_id)["provider_job_id"])
    await peer.wait_for_active(task_ids={task_id})

    invocation = ProviderInvocationRepository(store).get(f"provider-invocation:{run_id}:1")
    assert invocation.data_disclosed is TriState.YES
    with store.connect() as connection:
        stages = [
            row["stage"]
            for row in connection.execute(
                "SELECT stage FROM provider_invocation_events_v2 WHERE invocation_id=? "
                "ORDER BY ordinal",
                (f"provider-invocation:{run_id}:1",),
            ).fetchall()
        ]
    assert stages == [
        "requested",
        "dispatched",
        "accepted",
        "dataDisclosed",
        "inferenceStarted",
        "inferenceCompleted",
    ]
    assert store.get_task(task_id)["state"] == TaskState.REVIEWING.value
    assert store.get_worker_run(run_id)["state"] == RunState.COMPLETED.value
    assert provider.start_calls == 1


async def test_completed_during_crash_is_collected_exactly_once(tmp_path) -> None:
    provider = FakeProvider()
    store, task_id, run_id = await launch_and_crash(tmp_path, provider)
    provider.complete(store.get_provider_job(run_id)["provider_job_id"])
    _peer_store, peer, _adapter = runtime_fixture(tmp_path, provider, runtime_id="runtime-b")

    await peer.recover(task_ids={task_id})
    await peer.recover(task_ids={task_id})

    assert provider.start_calls == 1
    assert provider.collect_calls == 1
    assert store.get_task(task_id)["state"] == TaskState.REVIEWING.value
    assert store.get_provider_job(run_id)["result_collection_state"] == "collected"
    kinds = [event["kind"] for event in store.list_events(task_id=task_id)]
    assert kinds.count("workerResultRecorded") == 1
    assert kinds.count("providerJobResultCollected") == 1
    assert kinds.count("workerCompleted") == 1


async def test_provider_unreachable_does_not_duplicate_launch_and_escalates_once(tmp_path) -> None:
    provider = FakeProvider()
    store, task_id, run_id = await launch_and_crash(tmp_path, provider)
    provider.unreachable = True
    _peer_store, peer, _adapter = runtime_fixture(tmp_path, provider, runtime_id="runtime-b")

    await peer.recover(task_ids={task_id})
    await peer.recover(task_ids={task_id})

    assert provider.start_calls == 1
    assert store.get_task(task_id)["state"] == TaskState.WAITING.value
    assert store.get_worker_run(run_id)["state"] == RunState.RUNNING.value
    escalations = store.list_execution_escalations(run_id=run_id, state="open")
    assert [item["code"] for item in escalations] == ["EXTERNAL_JOB_UNREACHABLE"]
    assert [event["kind"] for event in store.list_events(task_id=task_id)].count(
        "humanEscalationRequested"
    ) == 1


async def test_restart_refuses_to_query_job_through_a_different_adapter_instance(tmp_path) -> None:
    provider = FakeProvider()
    store, task_id, run_id = await launch_and_crash(tmp_path, provider)
    _peer_store, peer, adapter = runtime_fixture(tmp_path, provider, runtime_id="runtime-b")
    adapter.adapter_instance_id = "different-provider-endpoint"

    await peer.recover(task_ids={task_id})

    assert provider.reconcile_calls == 0
    assert provider.start_calls == 1
    assert store.get_task(task_id)["state"] == TaskState.WAITING.value
    assert store.get_worker_run(run_id)["state"] == RunState.RUNNING.value
    assert [
        item["code"] for item in store.list_execution_escalations(run_id=run_id, state="open")
    ] == ["PROVIDER_STATE_AMBIGUOUS"]


async def test_terminal_reconciliation_from_waiting_finishes_in_same_recovery(tmp_path) -> None:
    provider = FakeProvider()
    store, task_id, run_id = await launch_and_crash(tmp_path, provider)
    provider.unreachable = True
    _peer_store, peer, _adapter = runtime_fixture(tmp_path, provider, runtime_id="runtime-b")

    await peer.recover(task_ids={task_id})
    assert store.get_task(task_id)["state"] == TaskState.WAITING.value

    provider.unreachable = False
    provider.complete(store.get_provider_job(run_id)["provider_job_id"])
    await peer.recover(task_ids={task_id})

    assert store.get_task(task_id)["state"] == TaskState.REVIEWING.value
    assert store.get_worker_run(run_id)["state"] == RunState.COMPLETED.value
    assert store.get_provider_job(run_id)["result_collection_state"] == "collected"
    assert provider.collect_calls == 1
    assert store.list_execution_escalations(run_id=run_id, state="open") == []
    resolved = store.list_execution_escalations(run_id=run_id, state="resolved")
    assert [item["code"] for item in resolved] == ["EXTERNAL_JOB_UNREACHABLE"]
    assert [event["kind"] for event in store.list_events(task_id=task_id)].count(
        "humanEscalationResolved"
    ) == 1


async def test_provider_not_found_requeues_without_launching_until_scheduler_runs(tmp_path) -> None:
    provider = FakeProvider()
    store, task_id, run_id = await launch_and_crash(tmp_path, provider)
    provider.missing = True
    _peer_store, peer, _adapter = runtime_fixture(tmp_path, provider, runtime_id="runtime-b")

    await peer.recover(task_ids={task_id})

    assert provider.start_calls == 1
    assert store.get_worker_run(run_id)["state"] == RunState.INTERRUPTED.value
    assert store.get_task(task_id)["state"] == TaskState.READY.value


async def test_running_job_with_unsupported_resume_is_explicitly_escalated(tmp_path) -> None:
    provider = FakeProvider()
    store, task_id, run_id = await launch_and_crash(
        tmp_path,
        provider,
        supports_resume=False,
    )
    _peer_store, peer, _adapter = runtime_fixture(
        tmp_path,
        provider,
        runtime_id="runtime-b",
        supports_resume=False,
    )

    provider.unreachable = True
    await peer.recover(task_ids={task_id})
    assert [
        item["code"] for item in store.list_execution_escalations(run_id=run_id, state="open")
    ] == ["EXTERNAL_JOB_UNREACHABLE"]
    provider.unreachable = False
    await peer.recover(task_ids={task_id})

    assert provider.start_calls == 1
    assert task_id not in peer._active
    escalations = store.list_execution_escalations(run_id=run_id, state="open")
    assert [item["code"] for item in escalations] == ["RESUME_UNSUPPORTED"]
    assert [
        item["code"] for item in store.list_execution_escalations(run_id=run_id, state="resolved")
    ] == ["EXTERNAL_JOB_UNREACHABLE"]
    assert store.get_task(task_id)["state"] == TaskState.WAITING.value


async def test_restart_repairs_completed_attempt_after_post_finalization_crash(
    tmp_path,
) -> None:
    provider = FakeProvider()
    store, task_id, run_id = await launch_and_crash(tmp_path, provider)
    provider.complete(store.get_provider_job(run_id)["provider_job_id"])
    _peer_store, peer, adapter = runtime_fixture(tmp_path, provider, runtime_id="runtime-b")
    job = store.list_provider_jobs(task_id=task_id)[0]
    handle = peer._provider_job_handle(job)
    result = adapter._result(
        WorkerRequest(run_id=run_id, prompt="Run once across a Supervisor restart"),
        handle,
        provider.jobs[handle.provider_job_id],
    )
    generation = store.claim_task_reconciliation(
        task_id,
        owner_id="crash-owner",
        lease_ttl_seconds=30,
    )
    assert generation is not None
    store.request_execution_escalation(
        run_id=run_id,
        code="EXTERNAL_JOB_UNREACHABLE",
        summary="Provider was unreachable before terminal collection",
        detail="test crash window",
        lease_owner_id="crash-owner",
        lease_generation=generation,
    )
    store.save_worker_result(
        run_id=run_id,
        summary=result.final_text,
        lease_owner_id="crash-owner",
        lease_generation=generation,
    )
    await peer._record_usage(task_id, run_id, "worker-1", result)
    store.finalize_provider_job_result(
        run_id,
        RunState.COMPLETED,
        exit_code=0,
        lease_owner_id="crash-owner",
        lease_generation=generation,
    )
    task = store.get_task(task_id)
    role, fallback = peer._role_for_recovered_run(task, job)
    await peer._record_invocation_telemetry(
        task_id=task_id,
        worker_id="worker-1",
        attempt=1,
        task_role=role,
        fallback=fallback,
        result=result,
    )
    assert store.release_task_execution_lease(
        task_id,
        owner_id="crash-owner",
        generation=generation,
    )
    assert store.get_task(task_id)["state"] == TaskState.RUNNING.value

    _next_store, next_runtime, _next_adapter = runtime_fixture(
        tmp_path, provider, runtime_id="runtime-c"
    )
    await next_runtime.recover(task_ids={task_id})
    await next_runtime.recover(task_ids={task_id})
    dispatch = await next_runtime.dispatch_ready()

    assert store.get_task(task_id)["state"] == TaskState.REVIEWING.value
    assert store.get_worker_run(run_id)["state"] == RunState.COMPLETED.value
    assert store.get_provider_job(run_id)["result_collection_state"] == "collected"
    assert store.list_execution_escalations(run_id=run_id, state="open") == []
    assert [
        item["code"] for item in store.list_execution_escalations(run_id=run_id, state="resolved")
    ] == ["EXTERNAL_JOB_UNREACHABLE"]
    assert task_id not in dispatch.launched_task_ids
    assert provider.start_calls == 1
    reviewing_events = [
        event
        for event in store.list_events(task_id=task_id)
        if event["kind"] == "taskStateChanged"
        and event["payload"].get("to") == TaskState.REVIEWING.value
    ]
    assert len(reviewing_events) == 1


async def test_cancel_orphaned_durable_job_converges_without_later_recovery(
    tmp_path,
) -> None:
    provider = FakeProvider()
    store, task_id, run_id = await launch_and_crash(tmp_path, provider)
    _peer_store, peer, _adapter = runtime_fixture(tmp_path, provider, runtime_id="runtime-b")

    assert await peer.cancel_task(task_id)
    reconciliations_after_cancel = provider.reconcile_calls
    await peer.recover(task_ids={task_id})

    assert store.get_task(task_id)["state"] == TaskState.CANCELLED.value
    assert store.get_worker_run(run_id)["state"] == RunState.CANCELLED.value
    provider_job = store.get_provider_job(run_id)
    assert provider_job["reconciliation_state"] == WorkerJobState.KNOWN_CANCELLED.value
    assert provider_job["result_collection_state"] == "collected"
    assert store.list_workers()[0]["state"] == WorkerState.IDLE.value
    assert provider.start_calls == 1
    assert provider.reconcile_calls == reconciliations_after_cancel
    kinds = [event["kind"] for event in store.list_events(task_id=task_id)]
    assert kinds.count("providerJobResultCollected") == 1
    assert kinds.count("workerCancelled") == 1


async def test_restart_claims_cancelled_unresolved_provider_job_without_relaunch(
    tmp_path,
) -> None:
    provider = FakeProvider()
    store, task_id, run_id = await launch_and_crash(tmp_path, provider)
    assert store.cancel_task_execution(task_id, actor="test-crash-window")
    assert store.get_provider_job(run_id)["result_collection_state"] == "uncertain"
    assert [job["run_id"] for job in store.list_provider_jobs(reconcilable_only=True)] == [run_id]
    _peer_store, peer, _adapter = runtime_fixture(tmp_path, provider, runtime_id="runtime-b")

    await peer.recover(task_ids={task_id})
    await peer.recover(task_ids={task_id})

    assert provider.start_calls == 1
    assert provider.cancel_calls == 1
    assert provider.collect_calls == 1
    assert store.get_task(task_id)["state"] == TaskState.CANCELLED.value
    assert store.get_worker_run(run_id)["state"] == RunState.CANCELLED.value
    provider_job = store.get_provider_job(run_id)
    assert provider_job["reconciliation_state"] == WorkerJobState.KNOWN_CANCELLED.value
    assert provider_job["result_collection_state"] == "collected"
    assert store.list_provider_jobs(reconcilable_only=True) == []
    kinds = [event["kind"] for event in store.list_events(task_id=task_id)]
    assert kinds.count("providerJobResultCollected") == 1


async def test_cancelled_provider_unreachable_remains_reconcilable_without_relaunch(
    tmp_path,
) -> None:
    provider = FakeProvider()
    store, task_id, run_id = await launch_and_crash(tmp_path, provider)
    assert store.cancel_task_execution(task_id, actor="test-crash-window")
    _peer_store, peer, adapter = runtime_fixture(tmp_path, provider, runtime_id="runtime-b")

    async def unreachable_cancel(handle: WorkerJobHandle) -> bool:
        del handle
        provider.cancel_calls += 1
        raise ConnectionError("provider cancellation endpoint unreachable")

    adapter.cancel_job = unreachable_cancel  # type: ignore[method-assign]

    await peer.recover(task_ids={task_id})
    await peer.recover(task_ids={task_id})

    assert provider.start_calls == 1
    assert provider.cancel_calls == 2
    assert provider.collect_calls == 0
    assert store.get_task(task_id)["state"] == TaskState.CANCELLED.value
    assert store.get_worker_run(run_id)["state"] == RunState.CANCELLED.value
    assert store.get_provider_job(run_id)["result_collection_state"] == "uncertain"
    assert [job["run_id"] for job in store.list_provider_jobs(reconcilable_only=True)] == [run_id]
    assert [
        item["code"] for item in store.list_execution_escalations(run_id=run_id, state="open")
    ] == ["PROVIDER_STATE_AMBIGUOUS"]


async def test_native_idempotency_replays_uncertain_launch_without_duplicate_job(
    tmp_path,
) -> None:
    provider = FakeProvider()
    store, runtime, adapter = runtime_fixture(tmp_path, provider, runtime_id="runtime-a")
    original_start = adapter.start_job

    async def disconnect_after_provider_start(*args, **kwargs):
        await original_start(*args, **kwargs)
        raise ConnectionError("connection dropped after provider accepted the launch")

    adapter.start_job = disconnect_after_provider_start  # type: ignore[method-assign]
    task_id = await submit(runtime)
    dispatched = await runtime.dispatch_ready()
    assert dispatched.launched_task_ids == (task_id,)
    await runtime.wait_for_active(task_ids={task_id})
    job = store.list_provider_jobs(task_id=task_id)[0]
    run_id = job["run_id"]
    external_job_id = provider.by_idempotency_key[job["idempotency_key"]]
    assert job["launch_state"] == "uncertain"
    assert job["provider_job_id"] is None
    assert provider.start_calls == 1
    assert len(provider.jobs) == 1

    _peer_store, peer, _peer_adapter = runtime_fixture(tmp_path, provider, runtime_id="runtime-b")
    await peer.recover(task_ids={task_id})

    rebound = store.get_provider_job(run_id)
    assert rebound["launch_state"] == "bound"
    assert rebound["provider_job_id"] == external_job_id
    assert provider.start_calls == 2
    assert len(provider.jobs) == 1
    assert task_id in peer._active
    provider.complete(external_job_id)
    await peer.wait_for_active(task_ids={task_id})
    assert store.get_task(task_id)["state"] == TaskState.REVIEWING.value
    telemetry = peer.telemetry.list(task_id=task_id)
    assert len(telemetry) == 1
    assert telemetry[0].outcome.value == "success"
    invocation = ProviderInvocationRepository(store).get(f"provider-invocation:{run_id}:1")
    assert invocation.terminal
    assert invocation.model_used is TriState.YES
    assert invocation.model == "durable-fake-model"
    with store.connect() as connection:
        stages = [
            row["stage"]
            for row in connection.execute(
                "SELECT stage FROM provider_invocation_events_v2 WHERE invocation_id=? "
                "ORDER BY ordinal",
                (f"provider-invocation:{run_id}:1",),
            ).fetchall()
        ]
    assert stages == [
        "requested",
        "dispatched",
        "outcomeUnknown",
        "inferenceStarted",
        "inferenceCompleted",
    ]


async def test_raw_http_launch_error_after_side_effect_is_not_a_definite_rejection(
    tmp_path,
) -> None:
    provider = FakeProvider()
    store, runtime, adapter = runtime_fixture(tmp_path, provider, runtime_id="runtime-a")
    original_start = adapter.start_job

    async def accepted_then_http_error(*args, **kwargs):
        await original_start(*args, **kwargs)
        request = httpx.Request("POST", "https://provider.example/jobs")
        response = httpx.Response(404, request=request)
        raise httpx.HTTPStatusError("late HTTP error", request=request, response=response)

    adapter.start_job = accepted_then_http_error  # type: ignore[method-assign]
    task_id = await submit(runtime)
    await runtime.dispatch_ready()
    await runtime.wait_for_active(task_ids={task_id})

    job = store.list_provider_jobs(task_id=task_id)[0]
    assert store.get_task(task_id)["state"] == TaskState.WAITING.value
    assert store.get_worker_run(job["run_id"])["state"] == RunState.WAITING.value
    assert job["launch_state"] == "uncertain"
    assert len(provider.jobs) == 1
    assert provider.start_calls == 1
    assert (await runtime.dispatch_ready()).launched_task_ids == ()
    assert [
        item["code"]
        for item in store.list_execution_escalations(run_id=job["run_id"], state="open")
    ] == ["IDEMPOTENCY_UNCERTAIN"]


async def test_typed_prelaunch_rejection_is_safely_retryable(tmp_path) -> None:
    provider = FakeProvider()
    store, runtime, adapter = runtime_fixture(tmp_path, provider, runtime_id="runtime-a")

    async def reject_before_launch(*args, **kwargs):
        raise WorkerJobLaunchRejected(429)

    adapter.start_job = reject_before_launch  # type: ignore[method-assign]
    task_id = await submit(runtime)
    await runtime.dispatch_ready()
    await runtime.wait_for_active(task_ids={task_id})

    job = store.list_provider_jobs(task_id=task_id)[0]
    assert provider.start_calls == 0
    assert provider.jobs == {}
    assert store.get_task(task_id)["state"] == TaskState.READY.value
    assert store.get_worker_run(job["run_id"])["state"] == RunState.RATE_LIMITED.value
    assert job["launch_state"] == "terminal"
    assert job["result_collection_state"] == "collected"


async def test_bound_timeout_uncertainty_is_held_without_retry(tmp_path) -> None:
    provider = FakeProvider()
    store, runtime, adapter = runtime_fixture(tmp_path, provider, runtime_id="runtime-a")

    async def timeout_with_live_job(*args, **kwargs):
        raise WorkerJobOutcomeUncertain(
            WorkerJobState.KNOWN_RUNNING,
            "timeout cancellation is not terminal",
        )

    adapter.resume_job = timeout_with_live_job  # type: ignore[method-assign]
    task_id = await submit(runtime)
    await runtime.dispatch_ready()
    await runtime.wait_for_active(task_ids={task_id})

    job = store.list_provider_jobs(task_id=task_id)[0]
    assert store.get_task(task_id)["state"] == TaskState.WAITING.value
    assert store.get_worker_run(job["run_id"])["state"] == RunState.WAITING.value
    assert job["reconciliation_state"] == WorkerJobState.KNOWN_RUNNING.value
    assert len(provider.jobs) == 1
    assert (await runtime.dispatch_ready()).launched_task_ids == ()


async def test_terminal_observation_rejects_inconsistent_collected_result(tmp_path) -> None:
    provider = FakeProvider()
    store, task_id, run_id = await launch_and_crash(tmp_path, provider)
    job = store.get_provider_job(run_id)
    provider.jobs[job["provider_job_id"]].state = WorkerJobState.KNOWN_FAILED
    _peer_store, peer, adapter = runtime_fixture(tmp_path, provider, runtime_id="runtime-b")

    async def inconsistent_success(request, handle, *, event_sink=None):
        now = utc_now()
        return WorkerResult(
            run_id=request.run_id,
            state=RunState.COMPLETED,
            pid=None,
            exit_code=0,
            started_at=now,
            ended_at=now,
            stdout="",
            stderr="",
            final_text="inconsistent success",
            events=(),
        )

    adapter.collect_job = inconsistent_success  # type: ignore[method-assign]
    await peer.recover(task_ids={task_id})

    assert store.get_task(task_id)["state"] == TaskState.WAITING.value
    assert store.get_worker_run(run_id)["state"] == RunState.RUNNING.value
    assert store.get_provider_job(run_id)["result_collection_state"] == "pending"
    assert [
        item["code"] for item in store.list_execution_escalations(run_id=run_id, state="open")
    ] == ["RESULT_COLLECTION_UNCERTAIN"]
