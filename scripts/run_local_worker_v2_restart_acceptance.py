#!/usr/bin/env python3
"""Offline Local Worker v2 acceptance across real daemon and child processes.

The daemon's test-only response barrier holds the first launch response after the durable
registry and fixed child process exist.  Killing that daemon therefore reproduces the exact
Supervisor crash window where the external launch is real but its provider handle is not yet
canonical.  A replacement daemon and Runtime must recover by the persisted idempotency key.

No provider credentials, model runtime, arbitrary command, or production workspace is used.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import socket
import subprocess
import sys
import tempfile
import time
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import httpx

from project_supervisor.adapters import (
    LocalWorkerAdapter,
    WorkerJobLaunchState,
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
from project_supervisor.runtime import AdapterRegistry, SupervisorRuntime
from project_supervisor.scheduler import DeterministicScheduler
from project_supervisor.store import StateStore, timestamp

PROJECT_ROOT = Path(__file__).resolve().parents[1]
PROJECT_ID = "project-local-worker-v2-restart"
NODE_ID = "node-local-worker-v2-restart"
WORKER_ID = "worker-local-worker-v2-restart"
TASK_ID = "task-local-worker-v2-restart"
RESULT_TEXT = "LOCAL_WORKER_V2_RESTART_OK"
TEST_HOST_IDENTITY = "local-worker-v2-restart-acceptance"


@dataclass(slots=True)
class ManagedProcess:
    process: subprocess.Popen[str]
    stdout_handle: Any
    stderr_handle: Any
    stdout_path: Path
    stderr_path: Path

    def stop(self, *, force: bool) -> int:
        if self.process.poll() is None:
            if force:
                self.process.kill()
            else:
                self.process.terminate()
            try:
                self.process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait(timeout=5)
        self.stdout_handle.close()
        self.stderr_handle.close()
        return int(self.process.returncode if self.process.returncode is not None else 0)


def _free_loopback_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as listener:
        listener.bind(("127.0.0.1", 0))
        return int(listener.getsockname()[1])


def _child_environment(root: Path) -> dict[str, str]:
    home = root / "home"
    temporary = root / "tmp"
    home.mkdir(parents=True, exist_ok=True)
    temporary.mkdir(parents=True, exist_ok=True)
    return {
        "HOME": str(home),
        "PATH": os.environ.get("PATH", "/usr/bin:/bin"),
        "PYTHONPATH": os.pathsep.join((str(PROJECT_ROOT / "src"), str(PROJECT_ROOT))),
        "PYTHONDONTWRITEBYTECODE": "1",
        "TMPDIR": str(temporary),
        "NO_COLOR": "1",
    }


def _wait(description: str, condition: Callable[[], Any], *, timeout: float = 15) -> Any:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        value = condition()
        if value:
            return value
        time.sleep(0.02)
    raise AssertionError(f"timed out waiting for {description}")


async def _wait_async(
    description: str, condition: Callable[[], Any], *, timeout: float = 15
) -> Any:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        value = condition()
        if value:
            return value
        await asyncio.sleep(0.02)
    raise AssertionError(f"timed out waiting for {description}")


def _start_daemon(
    *,
    root: Path,
    state_dir: Path,
    port: int,
    invocation_log: Path,
    child_release_file: Path,
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
        "--test-child-duration-seconds",
        "8",
        "--test-child-result",
        RESULT_TEXT,
        "--test-child-release-file",
        str(child_release_file),
        "--test-child-release-timeout-seconds",
        "60",
        "--test-invocation-log",
        str(invocation_log),
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
    base_url = f"http://127.0.0.1:{port}"

    def ready() -> bool:
        if process.poll() is not None:
            stdout_handle.flush()
            stderr_handle.flush()
            safe = stderr_path.read_text(encoding="utf-8")[-2000:]
            raise AssertionError(f"Local Worker daemon exited during startup: {safe}")
        if not ready_file.exists():
            return False
        try:
            response = httpx.get(
                f"{base_url}/v2/health",
                timeout=0.5,
                follow_redirects=False,
                trust_env=False,
            )
        except httpx.HTTPError:
            return False
        return response.status_code == 200

    _wait("Local Worker v2 daemon readiness", ready, timeout=15)
    return daemon


def _start_dispatch_process(root: Path, base_url: str) -> ManagedProcess:
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
        ],
        cwd=PROJECT_ROOT,
        env=_child_environment(root),
        stdin=subprocess.DEVNULL,
        stdout=stdout_handle,
        stderr=stderr_handle,
        text=True,
    )
    return ManagedProcess(process, stdout_handle, stderr_handle, stdout_path, stderr_path)


def _invocations(path: Path) -> list[str]:
    if not path.exists():
        return []
    return [line.strip() for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]


def _store(root: Path, *, create: bool) -> StateStore:
    workspace = root / "workspace"
    workspace.mkdir(parents=True, exist_ok=True)
    store = StateStore(root / "supervisor" / "supervisor.db")
    if create:
        store.create_project(
            project_id=PROJECT_ID,
            name="Local Worker v2 restart acceptance",
            root_path=str(workspace),
            goal="Recover one idempotent local launch across daemon restarts",
        )
        store.upsert_node(
            node_id=NODE_ID,
            hostname="local-worker-v2.invalid",
            display_name="Local Worker v2 acceptance node",
            role="worker",
            state=NodeState.ONLINE,
            capabilities={"local-inference", "read-only-analysis"},
        )
    return store


def _runtime(
    store: StateStore, root: Path, base_url: str
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
    adapter = LocalWorkerAdapter(base_url, poll_interval_seconds=0.02)
    adapters = AdapterRegistry()
    adapters.register(WORKER_ID, adapter)
    runtime = SupervisorRuntime(
        store=store,
        scheduler=DeterministicScheduler(),
        adapters=adapters,
        evidence_root=root / "supervisor" / "evidence",
        max_attempts=1,
        worker_timeout_seconds=30,
        # State timestamps are second-granularity; leave enough lease horizon for a loaded CI
        # machine to heartbeat while the replacement Runtime waits on the released child.
        dispatch_lease_ttl_seconds=6,
    )
    return runtime, adapter


async def _submit(runtime: SupervisorRuntime) -> str:
    return await runtime.submit_task(
        project_id=PROJECT_ID,
        task_id=TASK_ID,
        reference="#LW-V2-RESTART",
        title="Recover one Local Worker v2 launch",
        description="Return the deterministic offline restart acceptance result.",
        topology=ExecutionTopology.SINGLE,
        priority=90,
        requirements=TaskRequirements(
            labels=frozenset({TaskLabel.REVIEW}),
            required_capabilities=frozenset({"read-only-analysis"}),
            code_write_required=False,
        ),
    )


async def _dispatch_phase(root: Path, base_url: str) -> None:
    """Run the pre-crash Supervisor in its own process until the parent kills it."""

    store = _store(root, create=True)
    runtime, _adapter = _runtime(store, root, base_url)
    await _submit(runtime)
    await runtime.run_until_idle()
    raise AssertionError("barriered Local Worker dispatch unexpectedly became idle")


def _request(runtime: SupervisorRuntime, store: StateStore, run_id: str):
    task = store.get_task(TASK_ID)
    project = store.get_project(PROJECT_ID)
    return runtime._worker_request(task, project, runtime._requirements(task), run_id)


async def _acceptance(root: Path) -> dict[str, Any]:
    daemon_state = root / "local-worker"
    daemon_state.mkdir(parents=True)
    invocation_log = root / "fixture-invocations.log"
    child_release_file = root / "release-fixture-child"
    response_barrier = root / "release-first-response"
    port = _free_loopback_port()
    base_url = f"http://127.0.0.1:{port}"
    daemon: ManagedProcess | None = None
    supervisor: ManagedProcess | None = None
    daemon_exit_codes: list[int] = []
    supervisor_exit_code: int | None = None
    try:
        daemon = _start_daemon(
            root=root,
            state_dir=daemon_state,
            port=port,
            invocation_log=invocation_log,
            child_release_file=child_release_file,
            sequence=1,
            response_barrier=response_barrier,
        )
        supervisor = _start_dispatch_process(root, base_url)

        def child_launched() -> bool:
            if supervisor is not None and supervisor.process.poll() is not None:
                supervisor.stderr_handle.flush()
                safe = supervisor.stderr_path.read_text(encoding="utf-8")[-2000:]
                raise AssertionError(f"pre-crash Supervisor exited early: {safe}")
            return len(_invocations(invocation_log)) == 1

        await _wait_async("fixed child process launch", child_launched, timeout=15)
        first_store = _store(root, create=False)
        task_id = TASK_ID

        def unbound_job() -> dict[str, Any] | None:
            runs = first_store.list_worker_runs(task_id)
            if len(runs) != 1:
                return None
            try:
                job = first_store.get_provider_job(runs[0]["id"])
            except KeyError:
                return None
            return job if not job["provider_job_id"] else None

        before_crash = await _wait_async(
            "durable Supervisor launch intent without a bound handle",
            unbound_job,
            timeout=10,
        )
        original_run_id = str(before_crash["run_id"])
        idempotency_key = str(before_crash["idempotency_key"])
        supervisor_exit_code = await asyncio.to_thread(supervisor.stop, force=True)
        supervisor = None
        pre_recovery_run = first_store.get_worker_run(original_run_id)
        pre_recovery_task = first_store.get_task(task_id)
        if pre_recovery_run["state"] != "running" or pre_recovery_task["state"] != "running":
            raise AssertionError("hard Supervisor loss unexpectedly settled canonical work")
        daemon_exit_codes.append(await asyncio.to_thread(daemon.stop, force=True))
        daemon = None

        ambiguous = first_store.get_provider_job(original_run_id)
        if ambiguous["provider_job_id"] is not None:
            raise AssertionError("lost launch response unexpectedly bound a provider handle")

        def stale_lease_expired() -> bool:
            with first_store.connect() as connection:
                row = connection.execute(
                    "SELECT state,expires_at FROM task_execution_leases WHERE task_id=?",
                    (task_id,),
                ).fetchone()
            return bool(
                row is not None
                and row["state"] == "active"
                and str(row["expires_at"]) <= timestamp()
            )

        await _wait_async(
            "hard-crashed Supervisor execution lease expiry",
            stale_lease_expired,
            timeout=10,
        )

        daemon = _start_daemon(
            root=root,
            state_dir=daemon_state,
            port=port,
            invocation_log=invocation_log,
            child_release_file=child_release_file,
            sequence=2,
        )
        recovered_store = _store(root, create=False)
        recovered_runtime, recovered_adapter = _runtime(recovered_store, root, base_url)
        capabilities = await recovered_adapter.negotiate_job_contract()
        if not capabilities.supports_provider_idempotency:
            raise AssertionError("v2 daemon did not negotiate server-enforced idempotency")
        request = _request(recovered_runtime, recovered_store, original_run_id)
        running_lookup = await recovered_adapter.lookup_launch(
            request,
            idempotency_key=idempotency_key,
        )
        if running_lookup.state is not WorkerJobLaunchState.RUNNING:
            raise AssertionError(
                f"restarted daemon did not observe running child: {running_lookup}"
            )
        if running_lookup.handle is None:
            raise AssertionError("running launch lookup omitted its durable handle")
        original_provider_job_id = running_lookup.handle.provider_job_id
        child_release_file.touch()

        await recovered_runtime.recover(task_ids={task_id})
        await asyncio.wait_for(
            recovered_runtime.wait_for_active(task_ids={task_id}),
            timeout=20,
        )
        recovered_job = recovered_store.get_provider_job(original_run_id)
        recovered_task = recovered_store.get_task(task_id)
        if recovered_job["provider_job_id"] != original_provider_job_id:
            raise AssertionError("Supervisor recovery did not bind the original Local Worker job")
        if recovered_task["state"] != TaskState.REVIEWING.value:
            raise AssertionError(f"recovered Task did not reach REVIEWING: {recovered_task}")

        daemon_exit_codes.append(await asyncio.to_thread(daemon.stop, force=True))
        daemon = None
        daemon = _start_daemon(
            root=root,
            state_dir=daemon_state,
            port=port,
            invocation_log=invocation_log,
            child_release_file=child_release_file,
            sequence=3,
        )
        final_store = _store(root, create=False)
        final_runtime, final_adapter = _runtime(final_store, root, base_url)
        await final_adapter.negotiate_job_contract()
        final_request = _request(final_runtime, final_store, original_run_id)
        terminal_lookup = await final_adapter.lookup_launch(
            final_request,
            idempotency_key=idempotency_key,
        )
        if terminal_lookup.state is not WorkerJobLaunchState.COMPLETED:
            raise AssertionError(
                f"terminal launch did not survive daemon restart: {terminal_lookup}"
            )
        if terminal_lookup.handle is None:
            raise AssertionError("terminal launch lookup omitted its durable handle")
        repeated = await final_adapter.collect_job(final_request, terminal_lookup.handle)
        if repeated.final_text != RESULT_TEXT:
            raise AssertionError("terminal result was not repeatably collectable")
        await final_runtime.recover(task_ids={task_id})

        with final_store.connect() as connection:
            canonical_result_count = int(
                connection.execute(
                    "SELECT COUNT(*) FROM worker_results WHERE run_id=?",
                    (original_run_id,),
                ).fetchone()[0]
            )
        event_kinds = [
            event["kind"] for event in final_store.list_events(task_id=task_id, limit=1000)
        ]
        open_escalations = final_store.list_execution_escalations(
            run_id=original_run_id,
            state="open",
        )
        registry = LaunchRegistry(
            daemon_state / "launch-registry.sqlite3",
            test_host_identity=TEST_HOST_IDENTITY,
        )
        registry_launch_count = registry.count_launches()
        invocations = _invocations(invocation_log)
        report = {
            "protocolVersion": 2,
            "realDaemonProcesses": True,
            "realSupervisorProcess": True,
            "realWorkerSubprocess": True,
            "responseLostAfterLaunch": True,
            "supervisorRuntimeRecreated": True,
            "supervisorHardExitCode": supervisor_exit_code,
            "leaseExpiredBeforeTakeover": True,
            "handleUnboundBeforeRecovery": ambiguous["provider_job_id"] is None,
            "runningRestartObserved": running_lookup.state is WorkerJobLaunchState.RUNNING,
            "terminalRestartObserved": terminal_lookup.state is WorkerJobLaunchState.COMPLETED,
            "lookupRecoveredOriginalJob": (
                recovered_job["provider_job_id"]
                == terminal_lookup.handle.provider_job_id
                == original_provider_job_id
            ),
            "canonicalResultCount": canonical_result_count,
            "providerResultCollectedEvents": event_kinds.count("providerJobResultCollected"),
            "workerResultRecordedEvents": event_kinds.count("workerResultRecorded"),
            "fixtureInvocationCount": len(invocations),
            "registryLaunchCount": registry_launch_count,
            "openEscalationCount": len(open_escalations),
            "daemonHardExitCodes": daemon_exit_codes,
            "credentialsAccessed": False,
            "billableProviderUsed": False,
        }
        report["pass"] = bool(
            report["handleUnboundBeforeRecovery"]
            and report["runningRestartObserved"]
            and report["terminalRestartObserved"]
            and report["lookupRecoveredOriginalJob"]
            and canonical_result_count == 1
            and report["providerResultCollectedEvents"] == 1
            and report["workerResultRecordedEvents"] == 1
            and len(invocations) == 1
            and invocations == [original_provider_job_id]
            and registry_launch_count == 1
            and not open_escalations
            and supervisor_exit_code is not None
            and supervisor_exit_code != 0
            and all(code != 0 for code in daemon_exit_codes)
        )
        return report
    finally:
        if supervisor is not None:
            await asyncio.to_thread(supervisor.stop, force=True)
        if daemon is not None:
            await asyncio.to_thread(daemon.stop, force=True)


def _run(output: Path) -> int:
    with tempfile.TemporaryDirectory(prefix="chips-local-worker-v2-restart-") as temporary:
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
    args = parser.parse_args()
    if args.mode == "_dispatch":
        if args.state_root is None or args.base_url is None:
            parser.error("_dispatch requires --state-root and --base-url")
        asyncio.run(_dispatch_phase(args.state_root.resolve(), args.base_url))
        return 2
    if args.output is None:
        parser.error("all requires --output")
    return _run(args.output.resolve())


if __name__ == "__main__":
    raise SystemExit(main())
