from __future__ import annotations

import argparse
import asyncio
import hmac
import json
import math
import os
import re
import signal
import stat
import sys
import uuid
from collections.abc import AsyncIterator, Mapping, Sequence
from contextlib import asynccontextmanager, suppress
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from project_supervisor.adapters.native import child_environment

from .drivers import (
    DriverCatalog,
    DriverProfile,
    DriverProfileError,
    DriverType,
    validate_driver_id,
)
from .model import (
    PROTOCOL_VERSION,
    LaunchDisposition,
    LaunchRecord,
    LaunchState,
    canonical_request_document,
    request_digest,
    validate_idempotency_key,
)
from .process import (
    atomic_write_json,
    job_lock_is_held,
    process_identity_matches,
    read_json_receipt,
)
from .registry import IdempotencyConflict, LaunchRegistry, RegistryUnavailable

_ALLOWED_JOB_TYPES = {"inference.chat", "inference.structured"}
_ALLOWED_ROLES = {"FAST_ROUTER", "GENERAL_REASONING", "RAG", "UNCENSORED_REVIEWER"}
_V2_TOP_FIELDS = frozenset(
    {
        "protocol_version",
        "authority_id",
        "registry_id",
        "idempotency_key",
        "request_digest",
        "execution",
        "job",
    }
)
_EXECUTION_FIELDS = frozenset({"run_id", "task_id"})
_JOB_FIELDS = frozenset({"driver_id", "job_type", "role", "prompt"})
_STRUCTURED_JOB_FIELDS = _JOB_FIELDS | {"schema"}
_V1_JOB_FIELDS = frozenset({"job_type", "role", "prompt"})
_V1_STRUCTURED_JOB_FIELDS = _V1_JOB_FIELDS | {"schema"}
_MAX_REQUEST_BYTES = 256 * 1024
_MAX_PROMPT_BYTES = 128 * 1024
_MAX_JSON_DEPTH = 32
_MAX_JSON_NODES = 20_000
_AUTH_TOKEN = re.compile(r"^[A-Za-z0-9._~-]{32,512}$")
_UNKNOWN_LAUNCH_ERRORS = frozenset(
    {
        "PROCESS_IDENTITY_OR_NONCE_LOCK_MISMATCH",
        "DRIVER_PROFILE_IDENTITY_MISSING",
        "DRIVER_PROFILE_MISMATCH",
        "DRIVER_EXECUTABLE_CHANGED",
    }
)


def _error(
    status_code: int,
    code: str,
    message: str,
    *,
    data: Mapping[str, Any] | None = None,
) -> JSONResponse:
    return JSONResponse(
        status_code=status_code,
        content={
            "protocol_version": PROTOCOL_VERSION,
            "error": {"code": code, "message": message},
            **({"data": dict(data)} if data is not None else {}),
        },
    )


def _strict_json_tree(value: object) -> bool:
    remaining = _MAX_JSON_NODES

    def visit(item: object, depth: int) -> bool:
        nonlocal remaining
        remaining -= 1
        if remaining < 0 or depth > _MAX_JSON_DEPTH:
            return False
        if item is None or isinstance(item, (str, bool, int)):
            return True
        if isinstance(item, float):
            return math.isfinite(item)
        if isinstance(item, list):
            return all(visit(child, depth + 1) for child in item)
        if isinstance(item, Mapping):
            return all(
                isinstance(key, str) and visit(child, depth + 1) for key, child in item.items()
            )
        return False

    return visit(value, 0)


async def _json_body(request: Request) -> tuple[Mapping[str, Any] | None, JSONResponse | None]:
    chunks: list[bytes] = []
    total = 0
    async for chunk in request.stream():
        total += len(chunk)
        if total > _MAX_REQUEST_BYTES:
            return None, _error(
                413,
                "LAUNCH_REQUEST_TOO_LARGE",
                "request body must be non-empty and at most 256 KiB",
            )
        chunks.append(chunk)
    raw = b"".join(chunks)
    if not raw:
        return None, _error(
            413,
            "LAUNCH_REQUEST_TOO_LARGE",
            "request body must be non-empty and at most 256 KiB",
        )
    try:
        value = json.loads(
            raw.decode("utf-8"),
            parse_constant=lambda _value: (_ for _ in ()).throw(ValueError("non-finite JSON")),
        )
    except (UnicodeError, ValueError):
        return None, _error(400, "INVALID_JSON", "request body must be strict UTF-8 JSON")
    if not isinstance(value, Mapping) or not _strict_json_tree(value):
        return None, _error(400, "INVALID_LAUNCH_REQUEST", "request body is not a bounded object")
    return value, None


def _load_auth_token(path: Path) -> str:
    """Load one owner-only production bearer without retaining its path or bytes in state."""

    candidate = Path(path)
    try:
        link_metadata = candidate.lstat()
        if stat.S_ISLNK(link_metadata.st_mode):
            raise ValueError("authentication token file must not be a symbolic link")
        metadata = candidate.stat()
        if not stat.S_ISREG(metadata.st_mode) or stat.S_IMODE(metadata.st_mode) != 0o600:
            raise ValueError("authentication token file must be a regular owner-only 0600 file")
        if hasattr(os, "getuid") and metadata.st_uid != os.getuid():
            raise ValueError("authentication token file must be owned by the daemon user")
        if not 1 <= metadata.st_size <= 1024:
            raise ValueError("authentication token file has an invalid size")
        raw = candidate.read_text(encoding="ascii")
    except ValueError:
        raise
    except (OSError, UnicodeError) as error:
        raise ValueError("authentication token file is unavailable") from error
    token = raw.rstrip("\r\n")
    if raw not in {token, token + "\n", token + "\r\n"} or not _AUTH_TOKEN.fullmatch(token):
        raise ValueError("authentication token must be one bounded URL-safe bearer value")
    return token


@dataclass(frozen=True, slots=True)
class DeterministicChildConfig:
    duration_seconds: float = 0.1
    result: str = "LOCAL_WORKER_V2_OK"
    release_file: Path | None = None
    release_timeout_seconds: float = 60.0
    invocation_log: Path | None = None


@dataclass(frozen=True, slots=True)
class LaunchRequestSpec:
    run_id: str
    task_id: str | None
    job: Mapping[str, Any]
    profile: DriverProfile


class LocalWorkerV2Service:
    """Durable launch authority over fixed server-registered driver profiles."""

    def __init__(
        self,
        registry: LaunchRegistry,
        *,
        spool_root: Path,
        catalog: DriverCatalog | None = None,
        child: DeterministicChildConfig | None = None,
        before_spawn_barrier: Path | None = None,
        response_barrier: Path | None = None,
        runtime_instance_id: str | None = None,
    ) -> None:
        self.registry = registry
        self.spool_root = Path(spool_root)
        self.child = child or DeterministicChildConfig()
        self.catalog = catalog or DriverCatalog([DriverProfile.test()], default_driver_id="test")
        self.before_spawn_barrier = before_spawn_barrier
        self.response_barrier = response_barrier
        self.runtime_instance_id = runtime_instance_id or f"local-worker-runtime-{uuid.uuid4()}"
        if not self.runtime_instance_id.strip() or len(self.runtime_instance_id) > 200:
            raise ValueError("runtime_instance_id must be a bounded non-empty identity")
        self._processes: dict[str, asyncio.subprocess.Process] = {}
        self._reapers: set[asyncio.Task[None]] = set()
        self.spool_root.mkdir(parents=True, exist_ok=True, mode=0o700)
        with suppress(OSError):
            os.chmod(self.spool_root, 0o700)

    @staticmethod
    async def _wait_for_file(path: Path) -> None:
        while not path.is_file():
            await asyncio.sleep(0.01)

    def _job_dir(self, job_id: str) -> Path:
        return self.spool_root / job_id

    def _profile_for_record(self, record: LaunchRecord) -> DriverProfile | None:
        if (
            record.driver_id is None
            or record.driver_type is None
            or record.driver_profile_revision is None
            or record.driver_profile_fingerprint is None
        ):
            return None
        profile = self.catalog.find(record.driver_id)
        if profile is None:
            return None
        if (
            profile.driver_type.value != record.driver_type
            or profile.profile_revision != record.driver_profile_revision
            or profile.profile_fingerprint != record.driver_profile_fingerprint
        ):
            return None
        return profile

    async def _validate_record_profile(
        self, record: LaunchRecord
    ) -> tuple[LaunchRecord, DriverProfile | None]:
        profile = self._profile_for_record(record)
        if profile is None:
            reason = (
                "DRIVER_PROFILE_IDENTITY_MISSING"
                if record.driver_id is None
                else "DRIVER_PROFILE_MISMATCH"
            )
            changed = await asyncio.to_thread(
                self.registry.record_launch_error, record.idempotency_key, reason
            )
            return changed, None
        return record, profile

    def _receipt_matches(
        self,
        record: LaunchRecord,
        profile: DriverProfile,
        receipt: Mapping[str, Any],
    ) -> bool:
        matches = (
            receipt.get("protocol_version") == PROTOCOL_VERSION
            and receipt.get("job_id") == record.job_id
            and receipt.get("idempotency_key") == record.idempotency_key
            and receipt.get("launch_nonce") == record.launch_nonce
            and receipt.get("request_digest") == record.request_digest
            and receipt.get("process_host_identity") == self.registry.process_host_identity
            and (
                record.process_host_identity is None
                or record.process_host_identity == self.registry.process_host_identity
            )
        )
        if not matches:
            return False
        if profile.driver_type is DriverType.TEST:
            # The original deterministic acceptance runner predates registered profiles.  Its
            # receipt remains valid because the registry/spool identity is still nonce fenced.
            return True
        return (
            receipt.get("driver_id") == record.driver_id
            and receipt.get("driver_type") == record.driver_type
            and receipt.get("driver_profile_revision") == record.driver_profile_revision
            and receipt.get("driver_profile_fingerprint") == record.driver_profile_fingerprint
            and receipt.get("launch_runtime_instance_id") == record.launch_runtime_instance_id
            and receipt.get("driver_executable_sha256") == profile.executable_sha256
        )

    @staticmethod
    def _safe_child_environment() -> dict[str, str]:
        environment = child_environment(os.environ)
        environment.pop("VIRTUAL_ENV", None)
        environment["PYTHONUNBUFFERED"] = "1"
        return environment

    def _track_process(self, job_id: str, process: asyncio.subprocess.Process) -> None:
        self._processes[job_id] = process

        async def reap() -> None:
            try:
                await process.wait()
            finally:
                self._processes.pop(job_id, None)

        task = asyncio.create_task(reap())
        self._reapers.add(task)
        task.add_done_callback(self._reapers.discard)

    async def drain_terminal_processes(self) -> None:
        """Reap wrappers that already published terminal receipts without waiting on live jobs."""

        waiters = [
            asyncio.create_task(process.wait())
            for job_id, process in tuple(self._processes.items())
            if (self._job_dir(job_id) / "terminal.json").is_file()
        ]
        if not waiters:
            return
        _done, pending = await asyncio.wait(waiters, timeout=1.0)
        for waiter in pending:
            waiter.cancel()
        await asyncio.gather(*pending, return_exceptions=True)

    def _test_command(self, job_dir: Path) -> list[str]:
        command = [
            sys.executable,
            "-m",
            "project_supervisor.local_worker_v2.runner",
            "--job-dir",
            str(job_dir),
            "--duration-seconds",
            str(max(0.0, self.child.duration_seconds)),
            "--result",
            self.child.result,
            "--release-timeout-seconds",
            str(max(0.1, self.child.release_timeout_seconds)),
        ]
        if self.child.release_file is not None:
            command.extend(("--release-file", str(self.child.release_file)))
        if self.child.invocation_log is not None:
            command.extend(("--invocation-log", str(self.child.invocation_log)))
        return command

    @staticmethod
    def _native_spec(
        record: LaunchRecord,
        profile: DriverProfile,
        request: LaunchRequestSpec,
        process_host_identity: str,
    ) -> bytes:
        assert record.job_id is not None and record.launch_nonce is not None
        assert record.launch_runtime_instance_id is not None
        assert profile.executable is not None and profile.executable_sha256 is not None
        job_type = str(request.job["job_type"])
        schema = request.job.get("schema") if job_type == "inference.structured" else None
        value = {
            "schema_version": "local-worker-native-execution/v1",
            "launch": {
                "protocol_version": PROTOCOL_VERSION,
                "job_id": record.job_id,
                "idempotency_key": record.idempotency_key,
                "launch_nonce": record.launch_nonce,
                "request_digest": record.request_digest,
                "process_host_identity": process_host_identity,
                "launch_runtime_instance_id": record.launch_runtime_instance_id,
            },
            "driver": {
                "driver_id": profile.driver_id,
                "driver_type": profile.driver_type.value,
                "profile_revision": profile.profile_revision,
                "profile_fingerprint": profile.profile_fingerprint,
                "executable": str(profile.executable),
                "executable_sha256": profile.executable_sha256,
                "max_execution_seconds": profile.max_execution_seconds,
            },
            "request": {
                "run_id": request.run_id,
                "task_id": request.task_id,
                "job_type": job_type,
                "role": request.job["role"],
                "prompt": request.job["prompt"],
                "schema": schema,
                "timeout_seconds": profile.max_execution_seconds,
            },
        }
        encoded = json.dumps(
            value, ensure_ascii=False, separators=(",", ":"), sort_keys=True
        ).encode("utf-8")
        if len(encoded) > _MAX_REQUEST_BYTES:
            raise ValueError("native execution spec exceeds 256 KiB")
        return encoded

    async def _spawn(
        self,
        record: LaunchRecord,
        profile: DriverProfile,
        request: LaunchRequestSpec,
        job_dir: Path,
    ) -> None:
        if profile.driver_type is DriverType.TEST:
            command = self._test_command(job_dir)
            stdin: int = asyncio.subprocess.DEVNULL
            execution_spec = None
        else:
            command = [
                sys.executable,
                "-m",
                "project_supervisor.local_worker_v2.native_cli_runner",
                "--job-dir",
                str(job_dir),
            ]
            stdin = asyncio.subprocess.PIPE
            execution_spec = self._native_spec(
                record, profile, request, self.registry.process_host_identity
            )
        process_options: dict[str, Any] = {
            "stdin": stdin,
            "stdout": asyncio.subprocess.DEVNULL,
            "stderr": asyncio.subprocess.DEVNULL,
            "env": self._safe_child_environment(),
        }
        if os.name == "nt":
            process_options["creationflags"] = 0x00000200  # CREATE_NEW_PROCESS_GROUP
        else:
            process_options["start_new_session"] = True
        process = await asyncio.create_subprocess_exec(*command, **process_options)
        assert record.job_id is not None
        self._track_process(record.job_id, process)
        if execution_spec is not None:
            if process.stdin is None:
                raise RuntimeError("native runner stdin pipe was not created")
            process.stdin.write(execution_spec)
            await process.stdin.drain()
            process.stdin.close()
            with suppress(BrokenPipeError, ConnectionResetError):
                await process.stdin.wait_closed()

    async def launch(
        self,
        record: LaunchRecord,
        request: LaunchRequestSpec,
    ) -> LaunchRecord:
        """Cross the process boundary once, guarded by the registry's RESERVED CAS."""

        if record.launch_state is not LaunchState.RESERVED:
            return await self.reconcile(record)
        record, profile = await self._validate_record_profile(record)
        if profile is None:
            return record
        if not profile.is_current():
            return await asyncio.to_thread(
                self.registry.record_launch_error,
                record.idempotency_key,
                "DRIVER_EXECUTABLE_CHANGED",
            )
        if request.profile.profile_fingerprint != profile.profile_fingerprint:
            return await asyncio.to_thread(
                self.registry.record_launch_error,
                record.idempotency_key,
                "DRIVER_PROFILE_MISMATCH",
            )
        if self.before_spawn_barrier is not None:
            await self._wait_for_file(self.before_spawn_barrier)
        claimed, owns_spawn = await asyncio.to_thread(
            self.registry.claim_spawn,
            record.idempotency_key,
            runtime_instance_id=self.runtime_instance_id,
        )
        if not owns_spawn:
            return await self.reconcile(claimed)
        assert claimed.job_id is not None and claimed.launch_nonce is not None
        job_dir = self._job_dir(claimed.job_id)
        job_dir.mkdir(parents=True, exist_ok=True, mode=0o700)
        atomic_write_json(
            job_dir / "launch.json",
            {
                "protocol_version": PROTOCOL_VERSION,
                "job_id": claimed.job_id,
                "idempotency_key": claimed.idempotency_key,
                "launch_nonce": claimed.launch_nonce,
                "request_digest": claimed.request_digest,
                "process_host_identity": self.registry.process_host_identity,
                "launch_runtime_instance_id": claimed.launch_runtime_instance_id,
                "driver_id": profile.driver_id,
                "driver_type": profile.driver_type.value,
                "driver_profile_revision": profile.profile_revision,
                "driver_profile_fingerprint": profile.profile_fingerprint,
                "driver_executable_sha256": profile.executable_sha256,
            },
        )
        try:
            await self._spawn(claimed, profile, request, job_dir)
        except (OSError, RuntimeError, ValueError) as error:
            # LAUNCHING precedes OS spawn.  A local error after that checkpoint is still uncertain:
            # the wrapper may exist even if the daemon lost the pipe or response.
            return await asyncio.to_thread(
                self.registry.record_launch_error,
                claimed.idempotency_key,
                f"PROCESS_SPAWN_{type(error).__name__}",
            )

        started_path = job_dir / "started.json"
        deadline = asyncio.get_running_loop().time() + 5.0
        while asyncio.get_running_loop().time() < deadline:
            started = read_json_receipt(started_path)
            if started is not None and self._receipt_matches(claimed, profile, started):
                return await self.reconcile(claimed)
            await asyncio.sleep(0.01)
        return await asyncio.to_thread(
            self.registry.record_launch_error,
            claimed.idempotency_key,
            "PROCESS_START_RECEIPT_UNAVAILABLE",
        )

    async def reconcile(self, record: LaunchRecord) -> LaunchRecord:
        record, profile = await self._validate_record_profile(record)
        if profile is None or record.job_id is None or record.launch_nonce is None:
            return record
        job_dir = self._job_dir(record.job_id)
        terminal = read_json_receipt(job_dir / "terminal.json")
        if terminal is not None and self._receipt_matches(record, profile, terminal):
            raw_state = terminal.get("launch_state")
            try:
                terminal_state = LaunchState(str(raw_state))
            except ValueError:
                return await asyncio.to_thread(
                    self.registry.record_launch_error,
                    record.idempotency_key,
                    "INVALID_TERMINAL_RECEIPT_STATE",
                )
            result = terminal.get("result")
            if not isinstance(result, Mapping):
                return await asyncio.to_thread(
                    self.registry.record_launch_error,
                    record.idempotency_key,
                    "INVALID_TERMINAL_RECEIPT_RESULT",
                )
            raw_exit = terminal.get("exit_code")
            exit_code = (
                raw_exit if isinstance(raw_exit, int) and not isinstance(raw_exit, bool) else None
            )
            return await asyncio.to_thread(
                self.registry.record_terminal,
                idempotency_key=record.idempotency_key,
                job_id=record.job_id,
                launch_nonce=record.launch_nonce,
                request_digest=record.request_digest,
                state=terminal_state,
                result=dict(result),
                exit_code=exit_code,
            )
        started = read_json_receipt(job_dir / "started.json")
        if started is None or not self._receipt_matches(record, profile, started):
            return record
        pid = started.get("pid")
        birth = started.get("process_birth_identity")
        host_identity = started.get("process_host_identity")
        if (
            not isinstance(pid, int)
            or isinstance(pid, bool)
            or pid <= 0
            or not isinstance(birth, str)
            or not birth
            or host_identity != self.registry.process_host_identity
        ):
            return await asyncio.to_thread(
                self.registry.record_launch_error,
                record.idempotency_key,
                "INVALID_PROCESS_IDENTITY_RECEIPT",
            )
        birth_matches, nonce_lock_held = await asyncio.gather(
            asyncio.to_thread(process_identity_matches, pid, birth),
            asyncio.to_thread(job_lock_is_held, job_dir / f"alive-{record.launch_nonce}.lock"),
        )
        attached = await asyncio.to_thread(
            self.registry.record_started,
            idempotency_key=record.idempotency_key,
            job_id=record.job_id,
            launch_nonce=record.launch_nonce,
            request_digest=record.request_digest,
            pid=pid,
            process_birth_identity=birth,
            process_host_identity=host_identity,
        )
        if birth_matches and nonce_lock_held:
            return attached
        return await asyncio.to_thread(
            self.registry.record_launch_error,
            record.idempotency_key,
            "PROCESS_IDENTITY_OR_NONCE_LOCK_MISMATCH",
        )

    async def recover(self) -> None:
        """Observe crash-era jobs; never launch a process merely because the daemon restarted."""

        records = await asyncio.to_thread(self.registry.list_launches)
        for record in records:
            if record.launch_state in {LaunchState.LAUNCHING, LaunchState.RUNNING}:
                await self.reconcile(record)

    async def lookup(self, idempotency_key: str) -> LaunchRecord | None:
        record = await asyncio.to_thread(self.registry.get_launch, idempotency_key)
        return await self.reconcile(record) if record is not None else None

    async def job(self, job_id: str) -> LaunchRecord | None:
        record = await asyncio.to_thread(self.registry.get_job, job_id)
        return await self.reconcile(record) if record is not None else None

    async def cancel(self, record: LaunchRecord) -> tuple[LaunchRecord, bool]:
        record = await self.reconcile(record)
        profile = self._profile_for_record(record)
        if record.terminal or profile is None or not profile.supports_cancel:
            return record, False
        if (
            record.launch_state is not LaunchState.RUNNING
            or record.process_pid is None
            or record.process_birth_identity is None
            or record.launch_nonce is None
            or record.job_id is None
            or record.process_host_identity != self.registry.process_host_identity
        ):
            return record, False
        birth_matches, nonce_lock_held = await asyncio.gather(
            asyncio.to_thread(
                process_identity_matches, record.process_pid, record.process_birth_identity
            ),
            asyncio.to_thread(
                job_lock_is_held,
                self._job_dir(record.job_id) / f"alive-{record.launch_nonce}.lock",
            ),
        )
        if not birth_matches or not nonce_lock_held:
            return await self.reconcile(record), False
        try:
            os.kill(record.process_pid, signal.SIGTERM)
        except OSError:
            return await self.reconcile(record), False
        return record, True


def _capabilities(catalog: DriverCatalog) -> dict[str, bool]:
    return {
        "supports_reconcile": True,
        "supports_resume": True,
        "supports_cancel": catalog.default.supports_cancel,
        "supports_repeatable_collect": True,
        "supports_provider_idempotency": True,
        "supports_idempotent_launch_lookup": True,
        "supports_durable_launch_registry": True,
        "supports_server_driver_profiles": True,
        "supports_stream_reconnect": False,
    }


def _projected_launch_data(record: LaunchRecord, *, replayed: bool) -> dict[str, Any]:
    data = record.public_data(replayed=replayed)
    if record.launch_error in _UNKNOWN_LAUNCH_ERRORS:
        data["launch_state"] = "UNKNOWN"
        data["disposition"] = LaunchDisposition.LAUNCH_OUTCOME_UNKNOWN.value
    return data


def _job_data(record: LaunchRecord) -> dict[str, Any]:
    projected_state = (
        "UNKNOWN" if record.launch_error in _UNKNOWN_LAUNCH_ERRORS else record.launch_state.value
    )
    provider_result: dict[str, Any] = {}
    if record.result_json:
        try:
            value = json.loads(record.result_json)
        except ValueError:
            value = None
        if isinstance(value, dict):
            provider_result = value
    provider_result.pop("state", None)
    return {
        **provider_result,
        "id": record.job_id,
        "job_id": record.job_id,
        "state": projected_state,
        "launch_record_id": record.launch_record_id,
        "receipt_id": record.receipt_id,
        "driver_id": record.driver_id,
        "driver_type": record.driver_type,
        "driver_profile_revision": record.driver_profile_revision,
        "driver_profile_fingerprint": record.driver_profile_fingerprint,
        "launch_runtime_instance_id": record.launch_runtime_instance_id,
        "created_at": record.created_at,
        "updated_at": record.updated_at,
        "terminal_at": record.terminal_at,
        "launch_error": record.launch_error,
    }


def create_app(
    registry: LaunchRegistry,
    *,
    spool_root: Path,
    catalog: DriverCatalog | None = None,
    child: DeterministicChildConfig | None = None,
    ready_file: Path | None = None,
    before_spawn_barrier: Path | None = None,
    response_barrier: Path | None = None,
    auth_token: str | None = None,
) -> FastAPI:
    if auth_token is not None and not _AUTH_TOKEN.fullmatch(auth_token):
        raise ValueError("auth_token must be one bounded URL-safe bearer value")
    service = LocalWorkerV2Service(
        registry,
        spool_root=spool_root,
        catalog=catalog,
        child=child,
        before_spawn_barrier=before_spawn_barrier,
        response_barrier=response_barrier,
    )

    @asynccontextmanager
    async def lifespan(_app: FastAPI) -> AsyncIterator[None]:
        await service.recover()
        if ready_file is not None:
            atomic_write_json(
                ready_file,
                {
                    "ready": True,
                    "authority_id": registry.authority_id,
                    "registry_id": registry.registry_id,
                    "node_id": registry.node_id,
                    "runtime_instance_id": service.runtime_instance_id,
                    "protocol_version": PROTOCOL_VERSION,
                },
            )
        try:
            yield
        finally:
            await service.drain_terminal_processes()
            if ready_file is not None:
                with suppress(FileNotFoundError):
                    ready_file.unlink()

    app = FastAPI(title="CHIPS Local Worker", version="2.0", lifespan=lifespan)
    app.state.local_worker_service = service
    app.state.launch_registry = registry

    if auth_token is not None:

        @app.middleware("http")
        async def require_bearer(request: Request, call_next):  # type: ignore[no-untyped-def]
            authorization = request.headers.get("authorization", "")
            prefix = "Bearer "
            supplied = authorization[len(prefix) :] if authorization.startswith(prefix) else ""
            if not supplied or not hmac.compare_digest(supplied, auth_token):
                response = _error(
                    401,
                    "AUTHENTICATION_REQUIRED",
                    "valid bearer authentication is required",
                )
                response.headers["www-authenticate"] = "Bearer"
                return response
            return await call_next(request)

    def identity_data() -> dict[str, str]:
        return {
            "authority_id": registry.authority_id,
            "registry_id": registry.registry_id,
        }

    def driver_data(profile: DriverProfile, *, launched: bool) -> dict[str, Any]:
        return {
            "driver_id": profile.driver_id,
            "driver_type": profile.driver_type.value,
            "driver_profile_revision": profile.profile_revision,
            "driver_profile_fingerprint": profile.profile_fingerprint,
            "launch_runtime_instance_id": service.runtime_instance_id if launched else None,
        }

    def launch_data(record: LaunchRecord, *, replayed: bool) -> dict[str, Any]:
        return {**_projected_launch_data(record, replayed=replayed), **identity_data()}

    def unavailable_data(profile: DriverProfile | None = None) -> dict[str, Any]:
        return {
            "launch_state": "UNKNOWN",
            "disposition": LaunchDisposition.LAUNCH_OUTCOME_UNKNOWN.value,
            "protocol_version": PROTOCOL_VERSION,
            **(driver_data(profile, launched=False) if profile is not None else {}),
            **identity_data(),
        }

    def health_data(protocol_version: int) -> dict[str, Any]:
        registry.check_available()
        return {
            "status": "ok",
            "protocol_version": protocol_version,
            "protocol_versions": [1, 2],
            "authority_id": registry.authority_id,
            "registry_id": registry.registry_id,
            "node_id": registry.node_id,
            "runtime_instance_id": service.runtime_instance_id,
            "default_driver_id": service.catalog.default_driver_id,
            "drivers": service.catalog.public_data(),
            "capabilities": _capabilities(service.catalog),
        }

    @app.get("/v1/health")
    async def health_v1() -> JSONResponse:
        try:
            data = health_data(1)
        except RegistryUnavailable:
            return _error(503, "REGISTRY_UNAVAILABLE", "durable launch registry is unavailable")
        return JSONResponse({"protocol_version": 1, "data": data})

    @app.get("/v2/health")
    async def health_v2() -> JSONResponse:
        try:
            data = health_data(2)
        except RegistryUnavailable:
            return _error(503, "REGISTRY_UNAVAILABLE", "durable launch registry is unavailable")
        return JSONResponse({"protocol_version": PROTOCOL_VERSION, "data": data})

    async def durable_rejection(
        *,
        key: str,
        digest: str,
        reason: str,
        profile: DriverProfile | None,
    ) -> JSONResponse:
        try:
            kwargs: dict[str, Any] = {}
            if profile is not None:
                kwargs = {
                    "driver_id": profile.driver_id,
                    "driver_type": profile.driver_type.value,
                    "driver_profile_revision": profile.profile_revision,
                    "driver_profile_fingerprint": profile.profile_fingerprint,
                }
            rejected, created = await asyncio.to_thread(
                registry.reject_pre_launch,
                idempotency_key=key,
                request_digest=digest,
                reason=reason,
                **kwargs,
            )
        except IdempotencyConflict:
            return _error(
                409,
                "IDEMPOTENCY_CONFLICT",
                "idempotency key is bound to a different request digest",
            )
        except RegistryUnavailable:
            return _error(
                503,
                "REGISTRY_UNAVAILABLE",
                "durable launch registry is unavailable",
                data=unavailable_data(profile),
            )
        return _error(
            422,
            "PRE_LAUNCH_REJECTED",
            "Local Worker rejected the request before external execution",
            data=launch_data(rejected, replayed=not created),
        )

    async def parse_launch(
        body: Mapping[str, Any],
    ) -> tuple[str, str, LaunchRequestSpec] | JSONResponse:
        try:
            key = validate_idempotency_key(body.get("idempotency_key"))
        except ValueError as error:
            return _error(400, "INVALID_IDEMPOTENCY_KEY", str(error))
        if body.get("protocol_version") != PROTOCOL_VERSION:
            return _error(400, "UNSUPPORTED_PROTOCOL", "protocol_version must be 2")
        try:
            registry.check_available()
        except RegistryUnavailable:
            return _error(
                503,
                "REGISTRY_UNAVAILABLE",
                "durable launch registry is unavailable",
                data=unavailable_data(),
            )
        if (
            body.get("authority_id") != registry.authority_id
            or body.get("registry_id") != registry.registry_id
        ):
            return _error(
                409,
                "LAUNCH_AUTHORITY_MISMATCH",
                "launch request targets a different Local Worker registry incarnation",
                data={**unavailable_data(), "launch_state": "NOT_ACCEPTED"},
            )
        execution = body.get("execution")
        job = body.get("job")
        if not isinstance(execution, Mapping) or not isinstance(job, Mapping):
            return _error(400, "INVALID_LAUNCH_REQUEST", "execution and job must be objects")
        run_id = execution.get("run_id")
        task_id = execution.get("task_id")
        if not isinstance(run_id, str) or not run_id.strip() or len(run_id) > 256:
            return _error(400, "INVALID_LAUNCH_REQUEST", "execution.run_id is required")
        if task_id is not None and (
            not isinstance(task_id, str) or not task_id.strip() or len(task_id) > 256
        ):
            return _error(400, "INVALID_LAUNCH_REQUEST", "execution.task_id must be bounded")
        computed = request_digest(run_id=run_id, task_id=task_id, job=job)
        raw_driver_id = job.get("driver_id")
        profile: DriverProfile | None = None
        try:
            driver_id = validate_driver_id(raw_driver_id)
            profile = service.catalog.find(driver_id)
        except DriverProfileError:
            driver_id = None

        reason: str | None = None
        expected_job_fields = (
            _STRUCTURED_JOB_FIELDS if job.get("job_type") == "inference.structured" else _JOB_FIELDS
        )
        if set(body) != _V2_TOP_FIELDS:
            reason = "UNSUPPORTED_REQUEST_FIELDS"
        elif set(execution) != _EXECUTION_FIELDS:
            reason = "UNSUPPORTED_EXECUTION_FIELDS"
        elif set(job) != expected_job_fields:
            reason = "UNSUPPORTED_JOB_FIELDS"
        elif body.get("request_digest") != computed:
            reason = "REQUEST_DIGEST_MISMATCH"
        elif driver_id is None or profile is None:
            reason = "DRIVER_NOT_REGISTERED"
        elif not profile.supports_execution:
            reason = "DRIVER_UNAVAILABLE"
        elif job.get("job_type") not in _ALLOWED_JOB_TYPES:
            reason = "UNSUPPORTED_JOB_TYPE"
        elif job.get("role") not in _ALLOWED_ROLES:
            reason = "UNSUPPORTED_WORKER_ROLE"
        elif not isinstance(job.get("prompt"), str) or not job.get("prompt"):
            reason = "INVALID_PROMPT"
        elif len(str(job["prompt"]).encode("utf-8")) > _MAX_PROMPT_BYTES:
            reason = "PROMPT_TOO_LARGE"
        elif job.get("job_type") == "inference.structured" and not isinstance(
            job.get("schema"), Mapping
        ):
            reason = "INVALID_RESPONSE_SCHEMA"
        if reason is not None:
            return await durable_rejection(
                key=key,
                digest=computed,
                reason=reason,
                profile=profile,
            )
        assert profile is not None
        document = canonical_request_document(run_id=run_id, task_id=task_id, job=job)
        return key, computed, LaunchRequestSpec(run_id, task_id, document["job"], profile)

    @app.post("/v2/launches")
    async def create_launch(request: Request) -> JSONResponse:
        body, error = await _json_body(request)
        if error is not None:
            return error
        assert body is not None
        parsed = await parse_launch(body)
        if isinstance(parsed, JSONResponse):
            return parsed
        key, digest, request_spec = parsed
        profile = request_spec.profile
        try:
            record, created = await asyncio.to_thread(
                registry.reserve,
                idempotency_key=key,
                request_digest=digest,
                driver_id=profile.driver_id,
                driver_type=profile.driver_type.value,
                driver_profile_revision=profile.profile_revision,
                driver_profile_fingerprint=profile.profile_fingerprint,
            )
            if record.launch_state is LaunchState.REJECTED_PRE_LAUNCH:
                return _error(
                    422,
                    "PRE_LAUNCH_REJECTED",
                    "Local Worker rejected the request before external execution",
                    data=launch_data(record, replayed=True),
                )
            record = await service.launch(record, request_spec)
            if response_barrier is not None:
                await service._wait_for_file(response_barrier)
        except IdempotencyConflict:
            return _error(
                409,
                "IDEMPOTENCY_CONFLICT",
                "idempotency key is bound to a different request digest",
            )
        except RegistryUnavailable:
            return _error(
                503,
                "REGISTRY_UNAVAILABLE",
                "durable launch registry is unavailable",
                data=unavailable_data(profile),
            )
        return JSONResponse(
            status_code=202 if created else 200,
            content={
                "protocol_version": PROTOCOL_VERSION,
                "data": launch_data(record, replayed=not created),
            },
        )

    @app.post("/v1/jobs")
    async def create_job_v1(request: Request) -> JSONResponse:
        """Compatibility launch without pretending that Protocol V1 is idempotent."""

        body, error = await _json_body(request)
        if error is not None:
            return error
        assert body is not None
        expected_fields = (
            _V1_STRUCTURED_JOB_FIELDS
            if body.get("job_type") == "inference.structured"
            else _V1_JOB_FIELDS
        )
        profile = service.catalog.default
        if (
            set(body) != expected_fields
            or body.get("job_type") not in _ALLOWED_JOB_TYPES
            or body.get("role") not in _ALLOWED_ROLES
            or not isinstance(body.get("prompt"), str)
            or not body.get("prompt")
            or len(str(body.get("prompt")).encode("utf-8")) > _MAX_PROMPT_BYTES
            or not profile.supports_execution
        ):
            return _error(422, "PRE_LAUNCH_REJECTED", "invalid Protocol V1 job request")
        identity = str(uuid.uuid4())
        key = f"local-worker-v1:{identity}"
        run_id = f"v1-run-{identity}"
        digest = request_digest(run_id=run_id, task_id=None, job=body)
        request_spec = LaunchRequestSpec(run_id, None, body, profile)
        try:
            record, _created = await asyncio.to_thread(
                registry.reserve,
                idempotency_key=key,
                request_digest=digest,
                driver_id=profile.driver_id,
                driver_type=profile.driver_type.value,
                driver_profile_revision=profile.profile_revision,
                driver_profile_fingerprint=profile.profile_fingerprint,
            )
            record = await service.launch(record, request_spec)
        except RegistryUnavailable:
            return _error(503, "REGISTRY_UNAVAILABLE", "durable launch registry is unavailable")
        return JSONResponse(
            status_code=201,
            content={
                "protocol_version": 1,
                "data": {"id": record.job_id, "state": record.launch_state.value},
            },
        )

    @app.get("/v2/launches/{idempotency_key:path}")
    async def lookup_launch(idempotency_key: str, driver_id: str | None = None) -> JSONResponse:
        try:
            selected_id = validate_driver_id(driver_id or service.catalog.default_driver_id)
            selected_profile = service.catalog.find(selected_id)
            if selected_profile is None:
                return _error(
                    409,
                    "DRIVER_NOT_REGISTERED",
                    "requested Local Worker driver is not registered",
                    data=unavailable_data(),
                )
            raw_record = await asyncio.to_thread(registry.get_launch, idempotency_key)
            if raw_record is not None and raw_record.driver_id != selected_id:
                return _error(
                    409,
                    "DRIVER_ID_MISMATCH",
                    "launch key is bound to a different server-side driver",
                    data={
                        **unavailable_data(selected_profile),
                        "idempotency_key": idempotency_key,
                    },
                )
            record = await service.reconcile(raw_record) if raw_record is not None else None
        except (DriverProfileError, ValueError) as error:
            return _error(400, "INVALID_LAUNCH_LOOKUP", str(error))
        except RegistryUnavailable:
            return _error(
                503,
                "REGISTRY_UNAVAILABLE",
                "durable launch registry is unavailable",
                data=unavailable_data(locals().get("selected_profile")),
            )
        if record is None:
            return _error(
                404,
                "LAUNCH_NOT_SEEN",
                "durable registry has never accepted this idempotency key",
                data={
                    "accepted": False,
                    "idempotency_key": idempotency_key,
                    "launch_state": "NOT_SEEN",
                    "disposition": LaunchDisposition.DEFINITELY_NOT_LAUNCHED.value,
                    "protocol_version": PROTOCOL_VERSION,
                    **driver_data(selected_profile, launched=False),
                    **identity_data(),
                },
            )
        return JSONResponse(
            {"protocol_version": PROTOCOL_VERSION, "data": launch_data(record, replayed=True)}
        )

    async def get_job_record(job_id: str) -> LaunchRecord | JSONResponse:
        try:
            record = await service.job(job_id)
        except (ValueError, RegistryUnavailable) as error:
            if isinstance(error, RegistryUnavailable):
                return _error(503, "REGISTRY_UNAVAILABLE", "durable launch registry is unavailable")
            return _error(400, "INVALID_JOB_ID", str(error))
        if record is None:
            return _error(404, "JOB_NOT_FOUND", "Local Worker job was not found")
        return record

    @app.get("/v2/jobs/{job_id}")
    async def get_job_v2(job_id: str) -> JSONResponse:
        record = await get_job_record(job_id)
        if isinstance(record, JSONResponse):
            return record
        return JSONResponse(
            {
                "protocol_version": PROTOCOL_VERSION,
                "data": {**_job_data(record), **identity_data()},
            }
        )

    @app.post("/v2/jobs/{job_id}/cancel")
    async def cancel_job_v2(job_id: str) -> JSONResponse:
        record = await get_job_record(job_id)
        if isinstance(record, JSONResponse):
            return record
        record, accepted = await service.cancel(record)
        return JSONResponse(
            status_code=202 if accepted else 200,
            content={
                "protocol_version": PROTOCOL_VERSION,
                "data": {
                    **_job_data(record),
                    **identity_data(),
                    "protocol_version": PROTOCOL_VERSION,
                    "cancel_accepted": accepted,
                },
            },
        )

    @app.get("/v1/jobs/{job_id}")
    async def get_job_v1(job_id: str) -> JSONResponse:
        record = await get_job_record(job_id)
        if isinstance(record, JSONResponse):
            return record
        return JSONResponse({"protocol_version": 1, "data": _job_data(record)})

    @app.post("/v1/jobs/{job_id}/cancel")
    async def cancel_job_v1(job_id: str) -> JSONResponse:
        record = await get_job_record(job_id)
        if isinstance(record, JSONResponse):
            return record
        record, accepted = await service.cancel(record)
        return JSONResponse(
            status_code=202 if accepted else 200,
            content={"protocol_version": 1, "data": {**_job_data(record), "accepted": accepted}},
        )

    return app


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="loopback-only CHIPS Local Worker Protocol V2")
    parser.add_argument("--data-dir", required=True)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, required=True)
    parser.add_argument("--ready-file")
    parser.add_argument("--node-id")
    parser.add_argument("--driver-profile", action="append", default=[])
    parser.add_argument("--default-driver-id")
    parser.add_argument("--production", action="store_true")
    parser.add_argument("--auth-token-file")
    parser.add_argument("--test-child-duration-seconds", type=float)
    parser.add_argument("--test-child-result")
    parser.add_argument("--test-child-release-file")
    parser.add_argument("--test-child-release-timeout-seconds", type=float)
    parser.add_argument("--test-invocation-log")
    parser.add_argument("--test-before-spawn-barrier")
    parser.add_argument("--test-response-barrier")
    parser.add_argument("--test-host-identity")
    return parser


def _catalog_from_args(args: argparse.Namespace) -> DriverCatalog:
    profiles = [DriverProfile.from_file(Path(path)) for path in args.driver_profile]
    if args.production:
        test_values = (
            args.test_child_duration_seconds,
            args.test_child_result,
            args.test_child_release_file,
            args.test_child_release_timeout_seconds,
            args.test_invocation_log,
            args.test_before_spawn_barrier,
            args.test_response_barrier,
            args.test_host_identity,
        )
        if any(value is not None for value in test_values):
            raise SystemExit("--production rejects every --test-* option")
        if not profiles or args.default_driver_id is None:
            raise SystemExit("--production requires driver profiles and --default-driver-id")
        if args.auth_token_file is None:
            raise SystemExit("--production requires --auth-token-file")
        if any(profile.driver_type is DriverType.TEST for profile in profiles):
            raise SystemExit("--production rejects the deterministic TestDriver")
    elif not profiles:
        profiles = [DriverProfile.test()]
    if args.default_driver_id is None:
        if len(profiles) != 1:
            raise SystemExit("multiple driver profiles require --default-driver-id")
        default_driver_id = profiles[0].driver_id
    else:
        default_driver_id = args.default_driver_id
    return DriverCatalog(profiles, default_driver_id=default_driver_id)


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.host not in {"127.0.0.1", "::1", "localhost"}:
        raise SystemExit("the bundled Local Worker daemon is loopback-only")
    if not 1 <= args.port <= 65535:
        raise SystemExit("--port must be in 1..65535")
    try:
        catalog = _catalog_from_args(args)
    except DriverProfileError as error:
        raise SystemExit(f"invalid Local Worker driver profile: {error}") from error
    try:
        auth_token = _load_auth_token(Path(args.auth_token_file)) if args.auth_token_file else None
    except ValueError as error:
        raise SystemExit(f"invalid Local Worker authentication: {error}") from error
    data_dir = Path(args.data_dir).resolve()
    registry = LaunchRegistry(
        data_dir / "launch-registry.sqlite3",
        node_id=args.node_id,
        test_host_identity=args.test_host_identity,
    )
    child = DeterministicChildConfig(
        duration_seconds=(
            args.test_child_duration_seconds
            if args.test_child_duration_seconds is not None
            else 0.1
        ),
        result=args.test_child_result or "LOCAL_WORKER_V2_OK",
        release_file=(Path(args.test_child_release_file) if args.test_child_release_file else None),
        release_timeout_seconds=(
            args.test_child_release_timeout_seconds
            if args.test_child_release_timeout_seconds is not None
            else 60.0
        ),
        invocation_log=(Path(args.test_invocation_log) if args.test_invocation_log else None),
    )
    app = create_app(
        registry,
        spool_root=data_dir / "jobs",
        catalog=catalog,
        child=child,
        ready_file=Path(args.ready_file) if args.ready_file else None,
        before_spawn_barrier=(
            Path(args.test_before_spawn_barrier) if args.test_before_spawn_barrier else None
        ),
        response_barrier=Path(args.test_response_barrier) if args.test_response_barrier else None,
        auth_token=auth_token,
    )
    import uvicorn

    uvicorn.run(app, host=args.host, port=args.port, log_level="warning")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
