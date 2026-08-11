#!/usr/bin/env python3
"""Offline real-process acceptance for Local Worker V2 native CLI drivers.

Every external executable is a temporary copy of ``fake_native_worker_cli.py``.  The acceptance
uses the production Claude/Grok/AGY/Codex adapters through the daemon-owned native runner and never
reads provider credentials or opens a provider network connection.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import shutil
import subprocess
import sys
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from run_local_worker_v2_restart_acceptance import (
    ManagedProcess,
    _child_environment,
    _free_loopback_port,
    _wait,
    _wait_async,
)

from project_supervisor.adapters import (
    LocalWorkerAdapter,
    WorkerJobLaunchState,
    WorkerJobOutcomeUncertain,
    WorkerJobState,
    WorkerRequest,
)
from project_supervisor.domain import (
    ExecutionTopology,
    Harness,
    ModelDescriptor,
    NodeState,
    Provider,
    ResourceState,
    TaskLabel,
    TaskRequirements,
    TaskState,
    WorkerSnapshot,
    WorkerState,
)
from project_supervisor.local_worker_v2 import LaunchRegistry
from project_supervisor.local_worker_v2.process import (
    process_birth_identity,
    process_identity_matches,
)
from project_supervisor.runtime import AdapterRegistry, SupervisorRuntime
from project_supervisor.scheduler import DeterministicScheduler
from project_supervisor.store import StateStore, timestamp

PROJECT_ROOT = Path(__file__).resolve().parents[1]
FAKE_CLI_SOURCE = PROJECT_ROOT / "scripts" / "fake_native_worker_cli.py"
TEST_HOST_IDENTITY = "local-worker-v2-native-cli-acceptance"
RESULT_TEXT = "NATIVE_CLI_OFFLINE_OK"
PROJECT_ID = "project-local-worker-v2-native-cli"
NODE_ID = "node-local-worker-v2-native-cli"
WORKER_ID = "worker-local-worker-v2-native-cli"
TASK_ID = "task-local-worker-v2-native-cli"
TERMINAL_JOB_STATES = {
    WorkerJobState.KNOWN_COMPLETED,
    WorkerJobState.KNOWN_FAILED,
    WorkerJobState.KNOWN_CANCELLED,
}


@dataclass(frozen=True, slots=True)
class NativeProfile:
    driver_id: str
    driver_type: str
    scenario: str
    executable: Path
    profile_path: Path
    control_dir: Path


def _write_profile(
    root: Path,
    *,
    driver_type: str,
    scenario: str,
    driver_id: str | None = None,
    profile_revision: int = 1,
) -> NativeProfile:
    identifier = driver_id or f"offline-{driver_type}-{scenario}"
    control = root / "driver" / identifier
    control.mkdir(parents=True)
    executable = control / f"fake-native-worker-cli--{scenario}"
    shutil.copy2(FAKE_CLI_SOURCE, executable)
    executable.chmod(0o700)
    profile_path = root / f"{identifier}.json"
    profile_path.write_text(
        json.dumps(
            {
                "driver_id": identifier,
                "driver_type": driver_type,
                "profile_revision": profile_revision,
                "executable": str(executable),
                "max_execution_seconds": 30,
            },
            separators=(",", ":"),
            sort_keys=True,
        ),
        encoding="utf-8",
    )
    profile_path.chmod(0o600)
    return NativeProfile(
        identifier,
        driver_type,
        scenario,
        executable,
        profile_path,
        control,
    )


def _start_daemon(
    *,
    root: Path,
    state_dir: Path,
    port: int,
    profile: NativeProfile,
    sequence: int,
    response_barrier: Path | None = None,
) -> ManagedProcess:
    ready_file = root / f"daemon-{sequence}.ready"
    stdout_path = root / f"daemon-{sequence}.stdout.log"
    stderr_path = root / f"daemon-{sequence}.stderr.log"
    stdout_handle = stdout_path.open("w", encoding="utf-8")
    stderr_handle = stderr_path.open("w", encoding="utf-8")
    command = [
        sys.executable,
        "-m",
        "project_supervisor.local_worker_v2.server",
        "--data-dir",
        str(state_dir),
        "--host",
        "127.0.0.1",
        "--port",
        str(port),
        "--ready-file",
        str(ready_file),
        "--test-host-identity",
        TEST_HOST_IDENTITY,
        "--driver-profile",
        str(profile.profile_path),
        "--default-driver-id",
        profile.driver_id,
    ]
    if response_barrier is not None:
        command.extend(("--test-response-barrier", str(response_barrier)))
    process = subprocess.Popen(
        command,
        cwd=PROJECT_ROOT,
        env=_child_environment(root),
        stdin=subprocess.DEVNULL,
        stdout=stdout_handle,
        stderr=stderr_handle,
        text=True,
    )
    daemon = ManagedProcess(process, stdout_handle, stderr_handle, stdout_path, stderr_path)

    def ready() -> bool:
        if process.poll() is not None:
            stdout_handle.flush()
            stderr_handle.flush()
            safe = stderr_path.read_text(encoding="utf-8")[-4000:]
            raise AssertionError(f"native Local Worker daemon exited during startup: {safe}")
        return ready_file.is_file()

    _wait("native Local Worker daemon readiness", ready, timeout=15)
    return daemon


def _invocations(profile: NativeProfile) -> list[dict[str, Any]]:
    path = profile.control_dir / "invocations.jsonl"
    if not path.is_file():
        return []
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line]


def _request(case: str) -> WorkerRequest:
    return WorkerRequest(
        run_id=f"native-cli-{case}",
        task_id=f"task-native-cli-{case}",
        prompt=f"Return {RESULT_TEXT} for offline case {case}",
        timeout_seconds=20,
        metadata={"worker_role": "GENERAL_REASONING"},
    )


async def _wait_terminal(adapter: LocalWorkerAdapter, handle: Any) -> Any:
    observation = None
    for _ in range(1000):
        observation = await adapter.reconcile_job(handle)
        if observation.state in TERMINAL_JOB_STATES:
            return observation
        await asyncio.sleep(0.02)
    raise AssertionError(f"native CLI job did not become terminal: {observation}")


async def _direct_case(root: Path, *, driver_type: str, scenario: str) -> dict[str, Any]:
    case = f"{driver_type}-{scenario}"
    case_root = root / case
    case_root.mkdir(parents=True)
    state_dir = case_root / "local-worker"
    state_dir.mkdir()
    profile = _write_profile(case_root, driver_type=driver_type, scenario=scenario)
    port = _free_loopback_port()
    base_url = f"http://127.0.0.1:{port}"
    daemon: ManagedProcess | None = None
    fake_birth: str | None = None
    cancellation_accepted: bool | None = None
    try:
        daemon = _start_daemon(
            root=case_root,
            state_dir=state_dir,
            port=port,
            profile=profile,
            sequence=1,
        )
        adapter = LocalWorkerAdapter(
            base_url,
            driver_id=profile.driver_id,
            poll_interval_seconds=0.02,
        )
        capabilities = await adapter.negotiate_job_contract()
        if not capabilities.supports_provider_idempotency:
            raise AssertionError("native daemon did not advertise durable V2 launch identity")
        request = _request(case)
        handle = await adapter.start_job(
            request,
            idempotency_key=f"supervisor-execution:{request.run_id}",
        )
        if scenario == "cancel":
            await _wait_async(
                "fake native CLI start",
                lambda: (profile.control_dir / "started.json").is_file(),
            )
            started = json.loads((profile.control_dir / "started.json").read_text(encoding="utf-8"))
            fake_birth = process_birth_identity(int(started["pid"]))
            cancellation_accepted = await adapter.cancel_job(handle)
            await _wait_terminal(adapter, handle)
            result = await adapter.collect_job(request, handle)
        else:
            result = await adapter.resume_job(request, handle)

        registry = LaunchRegistry(
            state_dir / "launch-registry.sqlite3",
            test_host_identity=TEST_HOST_IDENTITY,
        )
        record = registry.get_launch(f"supervisor-execution:{request.run_id}")
        if record is None or record.job_id is None:
            raise AssertionError("native driver launch is absent from durable registry")
        terminal_path = state_dir / "jobs" / record.job_id / "terminal.json"
        invocations = _invocations(profile)
        started = invocations[0] if invocations else {}
        fake_gone = bool(
            fake_birth and not process_identity_matches(int(started.get("pid", 0)), fake_birth)
        )
        return {
            "case": case,
            "driverType": driver_type,
            "scenario": scenario,
            "resultState": result.state.value,
            "resultText": result.final_text,
            "resultError": result.error,
            "model": result.model,
            "registryState": record.launch_state.value,
            "registryLaunchCount": registry.count_launches(),
            "fakeInvocationCount": len(invocations),
            "fakeCredentialEnvironmentKeys": started.get("credential_environment_keys", []),
            "fakeCancelledMarker": (profile.control_dir / "cancelled.json").is_file(),
            "fakeProcessGone": fake_gone if scenario == "cancel" else None,
            "cancellationAccepted": cancellation_accepted,
            "terminalReceiptBytes": terminal_path.stat().st_size,
            "providerJobID": record.job_id,
            "driverID": record.driver_id,
            "driverProfileRevision": record.driver_profile_revision,
            "driverProfileFingerprint": record.driver_profile_fingerprint,
        }
    finally:
        if daemon is not None:
            await asyncio.to_thread(daemon.stop, force=True)
        release = profile.control_dir / "release"
        release.touch(exist_ok=True)


def _store(root: Path, *, create: bool) -> StateStore:
    workspace = root / "workspace"
    workspace.mkdir(parents=True, exist_ok=True)
    store = StateStore(root / "supervisor" / "supervisor.db")
    if create:
        store.create_project(
            project_id=PROJECT_ID,
            name="Local Worker V2 native CLI acceptance",
            root_path=str(workspace),
            goal="Recover one offline native CLI job without duplicate launch",
        )
        store.upsert_node(
            node_id=NODE_ID,
            hostname="native-cli-acceptance.invalid",
            display_name="Offline native CLI acceptance",
            role="worker",
            state=NodeState.ONLINE,
            capabilities={"read-only-analysis"},
        )
    return store


def _runtime(
    store: StateStore,
    root: Path,
    base_url: str,
    driver_id: str,
) -> tuple[SupervisorRuntime, LocalWorkerAdapter]:
    store.upsert_worker(
        WorkerSnapshot(
            id=WORKER_ID,
            node_id=NODE_ID,
            harness=Harness.LOCAL_WORKER,
            provider=Provider.LOCAL,
            model=ModelDescriptor("server-selected", "Server selected", Provider.LOCAL),
            state=WorkerState.IDLE,
            node_state=NodeState.ONLINE,
            resource_state=ResourceState.AVAILABLE,
            capabilities=frozenset({"read-only-analysis"}),
            code_write_allowed=False,
            privacy_allowed=True,
            reliability_score=1.0,
            expected_latency_seconds=0.01,
        )
    )
    adapter = LocalWorkerAdapter(
        base_url,
        driver_id=driver_id,
        poll_interval_seconds=0.02,
    )
    adapters = AdapterRegistry()
    adapters.register(WORKER_ID, adapter)
    runtime = SupervisorRuntime(
        store=store,
        scheduler=DeterministicScheduler(),
        adapters=adapters,
        evidence_root=root / "supervisor" / "evidence",
        max_attempts=1,
        worker_timeout_seconds=20,
        dispatch_lease_ttl_seconds=6,
    )
    return runtime, adapter


async def _submit(runtime: SupervisorRuntime, reference: str) -> str:
    return await runtime.submit_task(
        project_id=PROJECT_ID,
        task_id=TASK_ID,
        reference=reference,
        title="Run offline native CLI Worker",
        description=f"Return {RESULT_TEXT} through the configured Local Worker driver.",
        topology=ExecutionTopology.SINGLE,
        priority=90,
        requirements=TaskRequirements(
            labels=frozenset({TaskLabel.REVIEW}),
            required_capabilities=frozenset({"read-only-analysis"}),
            code_write_required=False,
        ),
    )


def _start_supervisor_process(root: Path, base_url: str, driver_id: str) -> ManagedProcess:
    stdout_path = root / "supervisor-before-crash.stdout.log"
    stderr_path = root / "supervisor-before-crash.stderr.log"
    stdout_handle = stdout_path.open("w", encoding="utf-8")
    stderr_handle = stderr_path.open("w", encoding="utf-8")
    process = subprocess.Popen(
        [
            sys.executable,
            str(Path(__file__).resolve()),
            "_dispatch",
            "--state-root",
            str(root),
            "--base-url",
            base_url,
            "--driver-id",
            driver_id,
        ],
        cwd=PROJECT_ROOT,
        env=_child_environment(root),
        stdin=subprocess.DEVNULL,
        stdout=stdout_handle,
        stderr=stderr_handle,
        text=True,
    )
    return ManagedProcess(process, stdout_handle, stderr_handle, stdout_path, stderr_path)


async def _dispatch_phase(root: Path, base_url: str, driver_id: str) -> None:
    store = _store(root, create=True)
    runtime, _adapter = _runtime(store, root, base_url, driver_id)
    await _submit(runtime, "#NATIVE-CLI-CRASH")
    await runtime.run_until_idle()
    raise AssertionError("blocking native CLI dispatch unexpectedly became idle")


def _provider_job(store: StateStore) -> dict[str, Any] | None:
    runs = store.list_worker_runs(TASK_ID)
    if len(runs) != 1:
        return None
    try:
        return store.get_provider_job(str(runs[0]["id"]))
    except KeyError:
        return None


async def _wait_stale_lease(store: StateStore) -> None:
    def expired() -> bool:
        with store.connect() as connection:
            row = connection.execute(
                "SELECT state,expires_at FROM task_execution_leases WHERE task_id=?",
                (TASK_ID,),
            ).fetchone()
        return bool(
            row is not None and row["state"] == "active" and str(row["expires_at"]) <= timestamp()
        )

    await _wait_async("hard-crashed Supervisor lease expiry", expired, timeout=12)


async def _recover_runtime(
    root: Path,
    base_url: str,
    driver_id: str,
) -> tuple[StateStore, SupervisorRuntime, LocalWorkerAdapter]:
    store = _store(root, create=False)
    runtime, adapter = _runtime(store, root, base_url, driver_id)
    await adapter.negotiate_job_contract()
    await runtime.recover(task_ids={TASK_ID})
    await asyncio.wait_for(runtime.wait_for_active(task_ids={TASK_ID}), timeout=25)
    return store, runtime, adapter


def _canonical_counts(store: StateStore) -> dict[str, Any]:
    runs = store.list_worker_runs(TASK_ID)
    if len(runs) != 1:
        raise AssertionError(f"expected one native Worker run, observed {len(runs)}")
    run_id = str(runs[0]["id"])
    with store.connect() as connection:
        result_count = int(
            connection.execute(
                "SELECT COUNT(*) FROM worker_results WHERE run_id=?", (run_id,)
            ).fetchone()[0]
        )
    kinds = [event["kind"] for event in store.list_events(task_id=TASK_ID, limit=1000)]
    return {
        "runID": run_id,
        "taskState": store.get_task(TASK_ID)["state"],
        "canonicalResultCount": result_count,
        "workerResultRecordedEvents": kinds.count("workerResultRecorded"),
        "providerResultCollectedEvents": kinds.count("providerJobResultCollected"),
        "openEscalationCount": len(store.list_execution_escalations(run_id=run_id, state="open")),
    }


async def _supervisor_restart_case(root: Path, *, both_and_lost: bool) -> dict[str, Any]:
    label = "both-lost-response" if both_and_lost else "supervisor-only"
    case_root = root / label
    case_root.mkdir(parents=True)
    state_dir = case_root / "local-worker"
    state_dir.mkdir()
    profile = _write_profile(case_root, driver_type="claude", scenario="block")
    response_barrier = case_root / "hold-launch-response" if both_and_lost else None
    port = _free_loopback_port()
    base_url = f"http://127.0.0.1:{port}"
    daemon: ManagedProcess | None = None
    supervisor: ManagedProcess | None = None
    daemon_restarted = False
    try:
        daemon = _start_daemon(
            root=case_root,
            state_dir=state_dir,
            port=port,
            profile=profile,
            sequence=1,
            response_barrier=response_barrier,
        )
        supervisor = _start_supervisor_process(case_root, base_url, profile.driver_id)
        await _wait_async(
            "native fake CLI invocation",
            lambda: len(_invocations(profile)) == 1,
            timeout=15,
        )
        store = _store(case_root, create=False)

        def expected_binding() -> dict[str, Any] | None:
            if supervisor is not None and supervisor.process.poll() is not None:
                supervisor.stderr_handle.flush()
                safe = supervisor.stderr_path.read_text(encoding="utf-8")[-4000:]
                raise AssertionError(f"pre-crash Supervisor exited unexpectedly: {safe}")
            job = _provider_job(store)
            if job is None:
                return None
            bound = bool(job["provider_job_id"])
            return job if bound is not both_and_lost else None

        before = await _wait_async("expected provider-handle binding", expected_binding, timeout=15)
        run_id = str(before["run_id"])
        provider_job_before = before["provider_job_id"]
        supervisor_exit = await asyncio.to_thread(supervisor.stop, force=True)
        supervisor = None
        if both_and_lost:
            await asyncio.to_thread(daemon.stop, force=True)
            daemon = None
            daemon_restarted = True
        await _wait_stale_lease(store)
        if daemon is None:
            daemon = _start_daemon(
                root=case_root,
                state_dir=state_dir,
                port=port,
                profile=profile,
                sequence=2,
            )

        runtime_store = _store(case_root, create=False)
        replacement_runtime, replacement_adapter = _runtime(
            runtime_store, case_root, base_url, profile.driver_id
        )
        await replacement_adapter.negotiate_job_contract()
        task = runtime_store.get_task(TASK_ID)
        project = runtime_store.get_project(PROJECT_ID)
        request = replacement_runtime._worker_request(
            task,
            project,
            replacement_runtime._requirements(task),
            run_id,
        )
        lookup = await replacement_adapter.lookup_launch(
            request,
            idempotency_key=str(before["idempotency_key"]),
        )
        if lookup.handle is None or lookup.state is not WorkerJobLaunchState.RUNNING:
            raise AssertionError(f"replacement Supervisor did not reattach native job: {lookup}")
        (profile.control_dir / "release").touch()
        await replacement_runtime.recover(task_ids={TASK_ID})
        await asyncio.wait_for(replacement_runtime.wait_for_active(task_ids={TASK_ID}), timeout=25)
        counts = _canonical_counts(runtime_store)
        recovered = runtime_store.get_provider_job(run_id)
        return {
            "scenario": label,
            "supervisorHardExitCode": supervisor_exit,
            "daemonRestarted": daemon_restarted,
            "responseLost": both_and_lost,
            "handleUnboundBeforeRecovery": provider_job_before is None,
            "lookupState": lookup.state.value,
            "providerJobPreserved": (recovered["provider_job_id"] == lookup.handle.provider_job_id),
            "fakeInvocationCount": len(_invocations(profile)),
            **counts,
        }
    finally:
        if supervisor is not None:
            await asyncio.to_thread(supervisor.stop, force=True)
        if daemon is not None:
            await asyncio.to_thread(daemon.stop, force=True)
        (profile.control_dir / "release").touch(exist_ok=True)


async def _daemon_restart_case(root: Path) -> dict[str, Any]:
    case_root = root / "daemon-only"
    case_root.mkdir(parents=True)
    state_dir = case_root / "local-worker"
    state_dir.mkdir()
    # Exercise Codex specifically across the daemon process boundary; the provider process remains
    # owned by the durable Local Worker job while the HTTP authority restarts.
    profile = _write_profile(case_root, driver_type="codex", scenario="block")
    port = _free_loopback_port()
    base_url = f"http://127.0.0.1:{port}"
    daemon: ManagedProcess | None = None
    try:
        daemon = _start_daemon(
            root=case_root,
            state_dir=state_dir,
            port=port,
            profile=profile,
            sequence=1,
        )
        store = _store(case_root, create=True)
        runtime, _adapter = _runtime(store, case_root, base_url, profile.driver_id)
        runtime_id = runtime.runtime_id
        await _submit(runtime, "#NATIVE-CLI-DAEMON-RESTART")
        dispatch = asyncio.create_task(runtime.run_until_idle())
        await _wait_async(
            "bound native provider job",
            lambda: bool((job := _provider_job(store)) and job["provider_job_id"]),
            timeout=15,
        )
        await _wait_async(
            "native fake CLI invocation",
            lambda: len(_invocations(profile)) == 1,
            timeout=15,
        )
        await asyncio.to_thread(daemon.stop, force=True)
        daemon = None
        await asyncio.wait_for(dispatch, timeout=15)
        daemon = _start_daemon(
            root=case_root,
            state_dir=state_dir,
            port=port,
            profile=profile,
            sequence=2,
        )
        (profile.control_dir / "release").touch()
        await runtime.recover(task_ids={TASK_ID})
        await asyncio.wait_for(runtime.wait_for_active(task_ids={TASK_ID}), timeout=25)
        return {
            "scenario": "daemon-only",
            "driverType": profile.driver_type,
            "sameSupervisorRuntime": runtime.runtime_id == runtime_id,
            "fakeInvocationCount": len(_invocations(profile)),
            **_canonical_counts(store),
        }
    finally:
        if daemon is not None:
            await asyncio.to_thread(daemon.stop, force=True)
        (profile.control_dir / "release").touch(exist_ok=True)


async def _capability_mismatch_case(root: Path) -> dict[str, Any]:
    case_root = root / "capability-mismatch"
    case_root.mkdir(parents=True)
    state_dir = case_root / "local-worker"
    state_dir.mkdir()
    driver_id = "offline-claude-pinned"
    original = _write_profile(
        case_root / "original",
        driver_type="claude",
        scenario="block",
        driver_id=driver_id,
        profile_revision=1,
    )
    replacement = _write_profile(
        case_root / "replacement",
        driver_type="claude",
        scenario="normal",
        driver_id=driver_id,
        profile_revision=2,
    )
    port = _free_loopback_port()
    base_url = f"http://127.0.0.1:{port}"
    daemon: ManagedProcess | None = None
    handle: Any = None
    runner_pid: int | None = None
    runner_birth: str | None = None
    try:
        daemon = _start_daemon(
            root=case_root,
            state_dir=state_dir,
            port=port,
            profile=original,
            sequence=1,
        )
        request = _request("capability-mismatch")
        key = f"supervisor-execution:{request.run_id}"
        original_adapter = LocalWorkerAdapter(
            base_url,
            driver_id=driver_id,
            poll_interval_seconds=0.02,
        )
        await original_adapter.negotiate_job_contract()
        original_fingerprint = original_adapter.adapter_instance_id
        handle = await original_adapter.start_job(request, idempotency_key=key)
        await _wait_async(
            "original pinned fake CLI invocation",
            lambda: len(_invocations(original)) == 1,
            timeout=15,
        )
        runner_receipt = json.loads(
            (state_dir / "jobs" / handle.provider_job_id / "started.json").read_text(
                encoding="utf-8"
            )
        )
        runner_pid = int(runner_receipt["pid"])
        runner_birth = str(runner_receipt["process_birth_identity"])
        await asyncio.to_thread(daemon.stop, force=True)
        daemon = None
        daemon = _start_daemon(
            root=case_root,
            state_dir=state_dir,
            port=port,
            profile=replacement,
            sequence=2,
        )
        replacement_adapter = LocalWorkerAdapter(
            base_url,
            driver_id=driver_id,
            poll_interval_seconds=0.02,
        )
        await replacement_adapter.negotiate_job_contract()
        reconciled = await replacement_adapter.reconcile_job(handle)
        lookup = await replacement_adapter.lookup_launch(request, idempotency_key=key)
        replay_rejected = False
        try:
            await replacement_adapter.start_job(request, idempotency_key=key)
        except WorkerJobOutcomeUncertain:
            replay_rejected = True
        registry = LaunchRegistry(
            state_dir / "launch-registry.sqlite3",
            test_host_identity=TEST_HOST_IDENTITY,
        )
        record = registry.get_launch(key)
        if record is None:
            raise AssertionError("pinned native launch disappeared after profile replacement")
        return {
            "scenario": "capability-mismatch",
            "adapterIdentityChanged": (
                replacement_adapter.adapter_instance_id != original_fingerprint
            ),
            "reconcileState": reconciled.state.value,
            "lookupState": lookup.state.value,
            "replayRejectedUncertain": replay_rejected,
            "originalFakeInvocationCount": len(_invocations(original)),
            "replacementFakeInvocationCount": len(_invocations(replacement)),
            "registryLaunchCount": registry.count_launches(),
            "registryDriverProfileRevision": record.driver_profile_revision,
        }
    finally:
        (original.control_dir / "release").touch(exist_ok=True)
        if handle is not None:
            terminal = state_dir / "jobs" / handle.provider_job_id / "terminal.json"
            _wait("pinned native runner terminal receipt", terminal.is_file, timeout=15)
        if runner_pid is not None and runner_birth is not None:
            _wait(
                "pinned native runner exit",
                lambda: not process_identity_matches(runner_pid, runner_birth),
                timeout=15,
            )
        if daemon is not None:
            await asyncio.to_thread(daemon.stop, force=True)


def _capability_mismatch_passed(case: dict[str, Any]) -> bool:
    return bool(
        case["adapterIdentityChanged"] is True
        and case["reconcileState"] == WorkerJobState.UNKNOWN.value
        and case["lookupState"] == WorkerJobLaunchState.UNKNOWN.value
        and case["replayRejectedUncertain"] is True
        and case["originalFakeInvocationCount"] == 1
        and case["replacementFakeInvocationCount"] == 0
        and case["registryLaunchCount"] == 1
        and case["registryDriverProfileRevision"] == 1
    )


def _case_passed(case: dict[str, Any]) -> bool:
    scenario = case["scenario"]
    expected_states = (
        {"completed", "failed"}
        if scenario == "control-flood"
        else {"completed"}
        if scenario == "normal"
        else {"cancelled"}
        if scenario == "cancel"
        else {"failed"}
    )
    return bool(
        case["resultState"] in expected_states
        and case["registryState"].lower() == case["resultState"]
        and case["registryLaunchCount"] == 1
        and case["fakeInvocationCount"] == 1
        and not case["fakeCredentialEnvironmentKeys"]
        and case["terminalReceiptBytes"] <= 65_536
        and (scenario != "normal" or case["resultText"] == RESULT_TEXT)
        and (
            scenario != "cancel"
            or (
                case["cancellationAccepted"] is True
                and case["fakeCancelledMarker"] is True
                and case["fakeProcessGone"] is True
            )
        )
    )


def _restart_passed(case: dict[str, Any]) -> bool:
    common = bool(
        case["taskState"] == TaskState.REVIEWING.value
        and case["canonicalResultCount"] == 1
        and case["workerResultRecordedEvents"] == 1
        and case["providerResultCollectedEvents"] == 1
        and case["fakeInvocationCount"] == 1
        and case["openEscalationCount"] == 0
    )
    if case["scenario"] == "daemon-only":
        return common and case["sameSupervisorRuntime"] is True
    return bool(
        common
        and case["supervisorHardExitCode"] != 0
        and case["lookupState"] == WorkerJobLaunchState.RUNNING.value
        and case["providerJobPreserved"] is True
        and case["handleUnboundBeforeRecovery"] is case["responseLost"]
    )


async def _acceptance(root: Path) -> dict[str, Any]:
    cases = []
    for driver_type in ("claude", "grok", "agy", "codex"):
        cases.append(await _direct_case(root, driver_type=driver_type, scenario="normal"))
    for scenario in (
        "cancel",
        "nonzero",
        "malformed",
        "flood",
        "control-flood",
        "wrong-dialect",
    ):
        cases.append(await _direct_case(root, driver_type="claude", scenario=scenario))
    for scenario in (
        "cancel",
        "nonzero",
        "auth-required",
        "malformed",
        "flood",
        "wrong-dialect",
    ):
        cases.append(await _direct_case(root, driver_type="codex", scenario=scenario))
    restart_cases = [
        await _supervisor_restart_case(root, both_and_lost=False),
        await _daemon_restart_case(root),
        await _supervisor_restart_case(root, both_and_lost=True),
    ]
    capability_mismatch = await _capability_mismatch_case(root)
    report = {
        "realDaemonProcesses": True,
        "realNativeRunnerProcesses": True,
        "realFakeCLIProcesses": True,
        "realSupervisorProcess": True,
        "nativeAdapterDialects": ["claude", "grok", "agy", "codex"],
        "cases": cases,
        "restartCases": restart_cases,
        "capabilityMismatch": capability_mismatch,
        "credentialsAccessed": any(case["fakeCredentialEnvironmentKeys"] for case in cases),
        "networkAccess": False,
        "billableProviderUsed": False,
    }
    report["pass"] = (
        all(_case_passed(case) for case in cases)
        and all(_restart_passed(case) for case in restart_cases)
        and _capability_mismatch_passed(capability_mismatch)
    )
    return report


def _run(output: Path) -> int:
    with tempfile.TemporaryDirectory(prefix="chips-local-worker-v2-native-cli-") as temporary:
        report = asyncio.run(_acceptance(Path(temporary)))
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps(report, sort_keys=True))
    return 0 if report["pass"] else 1


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=("all", "_dispatch"), nargs="?", default="all")
    parser.add_argument("--output", type=Path)
    parser.add_argument("--state-root", type=Path)
    parser.add_argument("--base-url")
    parser.add_argument("--driver-id")
    args = parser.parse_args()
    if args.mode == "_dispatch":
        if args.state_root is None or args.base_url is None or args.driver_id is None:
            parser.error("_dispatch requires --state-root, --base-url, and --driver-id")
        asyncio.run(_dispatch_phase(args.state_root.resolve(), args.base_url, args.driver_id))
        return 2
    if args.output is None:
        parser.error("all requires --output")
    return _run(args.output.resolve())


if __name__ == "__main__":
    raise SystemExit(main())
