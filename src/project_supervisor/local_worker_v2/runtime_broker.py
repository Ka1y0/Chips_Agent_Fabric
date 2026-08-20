from __future__ import annotations

import argparse
import asyncio
import hashlib
import hmac
import json
import os
import platform
import re
import sqlite3
import time
import uuid
from collections.abc import AsyncIterator, Mapping, Sequence
from contextlib import asynccontextmanager, suppress
from dataclasses import dataclass, replace
from datetime import UTC, datetime, timedelta
from enum import StrEnum
from pathlib import Path
from typing import Any, Protocol

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from project_supervisor.fabric.execution_plane import (
    FabricRuntimeStartRequest,
    FabricRuntimeStartResult,
)

from .process import local_host_identity
from .server import _load_auth_token

_SEMANTIC_ID = re.compile(r"^[A-Za-z][A-Za-z0-9._:-]{0,159}$")
_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_BEARER = re.compile(r"^[A-Za-z0-9._~-]{32,512}$")
_MAX_REQUEST_BYTES = 16 * 1024
_MAX_GENERATION = 2**31 - 1
_SCHEMA_VERSION = "2"
_PROFILE_SCHEMA = "fabric-runtime-broker-profile/v1"


def _timestamp(value: datetime | None = None) -> str:
    return (value or datetime.now(UTC)).isoformat().replace("+00:00", "Z")


def _canonical(value: Mapping[str, Any]) -> str:
    return json.dumps(value, ensure_ascii=True, separators=(",", ":"), sort_keys=True)


class BrokerConflict(RuntimeError):
    pass


class BrokerProfileMismatch(BrokerConflict):
    pass


class BrokerUnavailable(RuntimeError):
    pass


@dataclass(frozen=True, slots=True)
class RuntimeBrokerProfile:
    broker_authority_id: str
    node_id: str
    binding_id: str
    service_profile_id: str
    service_profile_revision: int
    target_service_name: str
    expected_service_config_sha256: str | None = None
    schema_version: str = _PROFILE_SCHEMA

    def __post_init__(self) -> None:
        identities = (
            self.broker_authority_id,
            self.node_id,
            self.binding_id,
            self.service_profile_id,
            self.target_service_name,
        )
        if any(not _SEMANTIC_ID.fullmatch(value) for value in identities):
            raise ValueError("runtime broker profile identities must be bounded semantic IDs")
        if self.service_profile_revision < 1:
            raise ValueError("runtime broker profile revision must be positive")
        if self.expected_service_config_sha256 is not None and not _SHA256.fullmatch(
            self.expected_service_config_sha256
        ):
            raise ValueError("runtime broker expected service configuration must be SHA-256")
        if self.schema_version != _PROFILE_SCHEMA:
            raise ValueError("unsupported runtime broker profile schema")

    @property
    def definition(self) -> dict[str, Any]:
        return {
            "schemaVersion": self.schema_version,
            "brokerAuthorityID": self.broker_authority_id,
            "nodeID": self.node_id,
            "bindingID": self.binding_id,
            "serviceProfileID": self.service_profile_id,
            "serviceProfileRevision": self.service_profile_revision,
            "targetKind": "windowsService",
            "targetServiceName": self.target_service_name,
            "expectedServiceConfigSHA256": self.expected_service_config_sha256,
        }

    @property
    def digest(self) -> str:
        return hashlib.sha256(_canonical(self.definition).encode("utf-8")).hexdigest()


class ServiceState(StrEnum):
    RUNNING = "running"
    START_PENDING = "startPending"
    STOPPED = "stopped"
    UNKNOWN = "unknown"


class StartDisposition(StrEnum):
    ACCEPTED = "accepted"
    ALREADY_RUNNING = "alreadyRunning"
    REJECTED_PRE_START = "rejectedPreStart"
    UNKNOWN = "unknown"


class FixedServiceController(Protocol):
    async def observe(self) -> ServiceState: ...

    async def start(self, *, deadline_at: datetime) -> StartDisposition: ...


class WindowsSCMServiceController:
    """Query/start one operator-pinned service through the Windows SCM API."""

    _SC_MANAGER_CONNECT = 0x0001
    _SERVICE_QUERY_CONFIG = 0x0001
    _SERVICE_QUERY_STATUS = 0x0004
    _SERVICE_START = 0x0010
    _SC_STATUS_PROCESS_INFO = 0
    _SERVICE_STOPPED = 0x00000001
    _SERVICE_START_PENDING = 0x00000002
    _SERVICE_RUNNING = 0x00000004
    _ERROR_INSUFFICIENT_BUFFER = 122
    _ERROR_SERVICE_ALREADY_RUNNING = 1056
    _DEFINITE_PRE_START_ERRORS = frozenset({5, 1058, 1060, 1072})

    def __init__(
        self,
        service_name: str,
        *,
        expected_config_sha256: str,
        timeout_seconds: float = 15.0,
    ) -> None:
        if platform.system() != "Windows":
            raise ValueError("Windows SCM control is available only on Windows")
        if not _SEMANTIC_ID.fullmatch(service_name):
            raise ValueError("Windows service name must be a bounded semantic ID")
        if not _SHA256.fullmatch(expected_config_sha256):
            raise ValueError("Windows service configuration digest must be pinned")
        if not 1 <= timeout_seconds <= 60:
            raise ValueError("Windows service deadline must be between one and 60 seconds")
        self.service_name = service_name
        self.expected_config_sha256 = expected_config_sha256
        self.timeout_seconds = float(timeout_seconds)

    @staticmethod
    def _api() -> tuple[Any, Any, Any]:
        import ctypes
        from ctypes import wintypes

        class ServiceStatusProcess(ctypes.Structure):
            _fields_ = [
                ("service_type", wintypes.DWORD),
                ("current_state", wintypes.DWORD),
                ("controls_accepted", wintypes.DWORD),
                ("win32_exit_code", wintypes.DWORD),
                ("service_specific_exit_code", wintypes.DWORD),
                ("check_point", wintypes.DWORD),
                ("wait_hint", wintypes.DWORD),
                ("process_id", wintypes.DWORD),
                ("service_flags", wintypes.DWORD),
            ]

        class QueryServiceConfig(ctypes.Structure):
            _fields_ = [
                ("service_type", wintypes.DWORD),
                ("start_type", wintypes.DWORD),
                ("error_control", wintypes.DWORD),
                ("binary_path_name", wintypes.LPWSTR),
                ("load_order_group", wintypes.LPWSTR),
                ("tag_id", wintypes.DWORD),
                ("dependencies", ctypes.c_void_p),
                ("service_start_name", wintypes.LPWSTR),
                ("display_name", wintypes.LPWSTR),
            ]

        api = ctypes.WinDLL("advapi32", use_last_error=True)
        api.OpenSCManagerW.argtypes = [wintypes.LPCWSTR, wintypes.LPCWSTR, wintypes.DWORD]
        api.OpenSCManagerW.restype = wintypes.HANDLE
        api.OpenServiceW.argtypes = [wintypes.HANDLE, wintypes.LPCWSTR, wintypes.DWORD]
        api.OpenServiceW.restype = wintypes.HANDLE
        api.CloseServiceHandle.argtypes = [wintypes.HANDLE]
        api.CloseServiceHandle.restype = wintypes.BOOL
        api.QueryServiceStatusEx.argtypes = [
            wintypes.HANDLE,
            wintypes.INT,
            wintypes.LPBYTE,
            wintypes.DWORD,
            ctypes.POINTER(wintypes.DWORD),
        ]
        api.QueryServiceStatusEx.restype = wintypes.BOOL
        api.QueryServiceConfigW.argtypes = [
            wintypes.HANDLE,
            ctypes.c_void_p,
            wintypes.DWORD,
            ctypes.POINTER(wintypes.DWORD),
        ]
        api.QueryServiceConfigW.restype = wintypes.BOOL
        api.StartServiceW.argtypes = [
            wintypes.HANDLE,
            wintypes.DWORD,
            ctypes.POINTER(wintypes.LPCWSTR),
        ]
        api.StartServiceW.restype = wintypes.BOOL
        return api, ServiceStatusProcess, QueryServiceConfig

    def _with_service(self, access: int, operation: Any) -> Any:
        import ctypes

        api, status_type, config_type = self._api()
        manager = api.OpenSCManagerW(None, None, self._SC_MANAGER_CONNECT)
        if not manager:
            raise BrokerUnavailable("Windows SCM is unavailable")
        service = None
        try:
            service = api.OpenServiceW(manager, self.service_name, access)
            if not service:
                raise BrokerUnavailable("the pinned Windows service is unavailable")
            return operation(api, service, status_type, config_type)
        finally:
            if service:
                api.CloseServiceHandle(service)
            api.CloseServiceHandle(manager)
            ctypes.set_last_error(0)

    @staticmethod
    def _status(api: Any, service: Any, status_type: Any) -> int:
        import ctypes
        from ctypes import wintypes

        status = status_type()
        needed = wintypes.DWORD()
        if not api.QueryServiceStatusEx(
            service,
            WindowsSCMServiceController._SC_STATUS_PROCESS_INFO,
            ctypes.cast(ctypes.byref(status), wintypes.LPBYTE),
            ctypes.sizeof(status),
            ctypes.byref(needed),
        ):
            raise BrokerUnavailable("Windows service status is unavailable")
        return int(status.current_state)

    @staticmethod
    def _read_windows_multisz(
        pointer: int | None,
        *,
        buffer_address: int,
        buffer_size: int,
    ) -> tuple[str, ...]:
        """Decode one SCM UTF-16LE MULTI_SZ without reading outside its returned buffer."""

        import ctypes

        if not pointer:
            return ()
        address = int(pointer)
        end = buffer_address + buffer_size
        if address < buffer_address or address >= end or (end - address) < 4:
            raise BrokerUnavailable("Windows service dependency identity is unavailable")
        raw = ctypes.string_at(address, end - address)
        terminator = next(
            (
                offset
                for offset in range(0, len(raw) - 3, 2)
                if raw[offset : offset + 4] == b"\0\0\0\0"
            ),
            None,
        )
        if terminator is None:
            raise BrokerUnavailable("Windows service dependency identity is unavailable")
        try:
            decoded = raw[:terminator].decode("utf-16-le")
        except UnicodeDecodeError as error:
            raise BrokerUnavailable("Windows service dependency identity is unavailable") from error
        dependencies = tuple(decoded.split("\0")) if decoded else ()
        if any(not dependency for dependency in dependencies):
            raise BrokerUnavailable("Windows service dependency identity is unavailable")
        return dependencies

    @staticmethod
    def _service_config_sha256(definition: Mapping[str, Any]) -> str:
        return hashlib.sha256(_canonical(dict(definition)).encode("utf-8")).hexdigest()

    @staticmethod
    def _config_digest(api: Any, service: Any, config_type: Any) -> str:
        import ctypes
        from ctypes import wintypes

        needed = wintypes.DWORD()
        api.QueryServiceConfigW(service, None, 0, ctypes.byref(needed))
        if ctypes.get_last_error() != WindowsSCMServiceController._ERROR_INSUFFICIENT_BUFFER:
            raise BrokerUnavailable("Windows service configuration is unavailable")
        buffer = ctypes.create_string_buffer(needed.value)
        if not api.QueryServiceConfigW(
            service, ctypes.byref(buffer), needed.value, ctypes.byref(needed)
        ):
            raise BrokerUnavailable("Windows service configuration is unavailable")
        config = ctypes.cast(ctypes.byref(buffer), ctypes.POINTER(config_type)).contents
        dependencies = WindowsSCMServiceController._read_windows_multisz(
            config.dependencies,
            buffer_address=ctypes.addressof(buffer),
            buffer_size=needed.value,
        )
        definition = {
            "serviceType": int(config.service_type),
            "startType": int(config.start_type),
            "errorControl": int(config.error_control),
            "binaryPathName": config.binary_path_name or "",
            "loadOrderGroup": config.load_order_group or "",
            "tagID": int(config.tag_id),
            "dependencies": dependencies,
            "serviceStartName": config.service_start_name or "",
        }
        return WindowsSCMServiceController._service_config_sha256(definition)

    def _observe_sync(self) -> ServiceState:
        def operation(api: Any, service: Any, status_type: Any, config_type: Any) -> ServiceState:
            if self._config_digest(api, service, config_type) != self.expected_config_sha256:
                raise BrokerProfileMismatch("Windows service configuration identity changed")
            state = self._status(api, service, status_type)
            if state == self._SERVICE_RUNNING:
                return ServiceState.RUNNING
            if state == self._SERVICE_START_PENDING:
                return ServiceState.START_PENDING
            if state == self._SERVICE_STOPPED:
                return ServiceState.STOPPED
            return ServiceState.UNKNOWN

        return self._with_service(
            self._SERVICE_QUERY_STATUS | self._SERVICE_QUERY_CONFIG,
            operation,
        )

    async def observe(self) -> ServiceState:
        return await asyncio.to_thread(self._observe_sync)

    def _start_sync(self, deadline_at: datetime) -> StartDisposition:
        import ctypes

        def operation(
            api: Any, service: Any, status_type: Any, config_type: Any
        ) -> StartDisposition:
            if self._config_digest(api, service, config_type) != self.expected_config_sha256:
                raise BrokerProfileMismatch("Windows service configuration identity changed")
            if deadline_at <= datetime.now(UTC):
                return StartDisposition.REJECTED_PRE_START
            state = self._status(api, service, status_type)
            if state == self._SERVICE_RUNNING:
                return StartDisposition.ALREADY_RUNNING
            if state == self._SERVICE_START_PENDING:
                return StartDisposition.ACCEPTED
            if state != self._SERVICE_STOPPED:
                return StartDisposition.REJECTED_PRE_START
            if deadline_at <= datetime.now(UTC):
                return StartDisposition.REJECTED_PRE_START
            if api.StartServiceW(service, 0, None):
                return StartDisposition.ACCEPTED
            error = ctypes.get_last_error()
            if error == self._ERROR_SERVICE_ALREADY_RUNNING:
                return StartDisposition.ALREADY_RUNNING
            if error in self._DEFINITE_PRE_START_ERRORS:
                return StartDisposition.REJECTED_PRE_START
            return StartDisposition.UNKNOWN

        return self._with_service(
            self._SERVICE_QUERY_STATUS | self._SERVICE_QUERY_CONFIG | self._SERVICE_START,
            operation,
        )

    async def start(self, *, deadline_at: datetime) -> StartDisposition:
        return await asyncio.to_thread(self._start_sync, deadline_at)


class TestMarkerServiceController:
    """Test-only fixed start boundary used by real broker restart acceptance."""

    def __init__(
        self,
        marker: Path,
        invocation_log: Path,
        *,
        after_start_barrier: Path | None = None,
    ) -> None:
        self.marker = Path(marker)
        self.invocation_log = Path(invocation_log)
        self.after_start_barrier = (
            Path(after_start_barrier) if after_start_barrier is not None else None
        )

    async def observe(self) -> ServiceState:
        return ServiceState.RUNNING if self.marker.is_file() else ServiceState.STOPPED

    async def start(self, *, deadline_at: datetime) -> StartDisposition:
        if deadline_at <= datetime.now(UTC):
            return StartDisposition.REJECTED_PRE_START
        self.marker.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        try:
            descriptor = os.open(self.marker, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        except FileExistsError:
            return StartDisposition.ALREADY_RUNNING
        try:
            os.write(descriptor, b"running\n")
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
        log_descriptor = os.open(
            self.invocation_log,
            os.O_WRONLY | os.O_CREAT | os.O_APPEND,
            0o600,
        )
        try:
            os.write(log_descriptor, b"start\n")
            os.fsync(log_descriptor)
        finally:
            os.close(log_descriptor)
        while self.after_start_barrier is not None and not self.after_start_barrier.is_file():
            await asyncio.sleep(0.01)
        return StartDisposition.ACCEPTED


class RuntimeBrokerRegistry:
    """Durable node authority, generation fence, and start-operation registry."""

    def __init__(
        self,
        database_path: Path,
        profile: RuntimeBrokerProfile,
        *,
        test_host_identity: str | None = None,
    ) -> None:
        self.database_path = Path(database_path)
        self.profile = profile
        self.host_identity = self._host_identity(test_host_identity)
        self._registry_id = ""
        if not self.database_path.is_file():
            raise BrokerUnavailable("runtime broker registry has not been initialized")
        self._registry_id = self._verify_registry()["broker_registry_id"]

    @staticmethod
    def _host_identity(test_host_identity: str | None) -> str:
        identity = (
            "sha256:"
            + hashlib.sha256(f"runtime-broker-test:{test_host_identity}".encode()).hexdigest()
            if test_host_identity is not None
            else local_host_identity()
        )
        if identity is None:
            raise BrokerUnavailable("stable broker host identity is unavailable")
        return identity

    @classmethod
    def initialize(
        cls,
        database_path: Path,
        profile: RuntimeBrokerProfile,
        *,
        test_host_identity: str | None = None,
        initial_lease_generation: int = 0,
    ) -> str:
        if not 0 <= initial_lease_generation < _MAX_GENERATION:
            raise ValueError("initial broker lease generation is invalid")
        path = Path(database_path)
        path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        with suppress(OSError):
            os.chmod(path.parent, 0o700)
        candidate_registry_id = f"broker-registry-{uuid.uuid4()}"
        host_identity = cls._host_identity(test_host_identity)
        connection = sqlite3.connect(path, isolation_level=None, timeout=10.0)
        try:
            connection.execute("PRAGMA busy_timeout=10000")
            for attempt in range(1000):
                try:
                    connection.execute("PRAGMA journal_mode=WAL")
                    break
                except sqlite3.OperationalError as error:
                    if "locked" not in str(error).lower() or attempt == 999:
                        raise
                    time.sleep(0.01)
            connection.execute("PRAGMA synchronous=FULL")
            connection.execute("BEGIN IMMEDIATE")
            for statement in cls._schema():
                connection.execute(statement)
            observed = dict(connection.execute("SELECT key,value FROM broker_meta").fetchall())
            if not observed:
                metadata = {
                    "schema_version": _SCHEMA_VERSION,
                    "broker_authority_id": profile.broker_authority_id,
                    "broker_registry_id": candidate_registry_id,
                    "profile_sha256": profile.digest,
                    "host_identity": host_identity,
                }
                connection.executemany(
                    "INSERT INTO broker_meta(key,value) VALUES (?,?)", metadata.items()
                )
                connection.execute(
                    "INSERT INTO runtime_start_fence(singleton,highest_binding_generation,"
                    "highest_lease_generation,current_idempotency_key,updated_at) "
                    "VALUES (1,0,?,NULL,?)",
                    (initial_lease_generation, _timestamp()),
                )
                registry_id = candidate_registry_id
            else:
                expected = {
                    "schema_version": _SCHEMA_VERSION,
                    "broker_authority_id": profile.broker_authority_id,
                    "profile_sha256": profile.digest,
                    "host_identity": host_identity,
                }
                if any(observed.get(key) != value for key, value in expected.items()):
                    raise BrokerUnavailable("runtime broker registry identity changed")
                registry_id = str(observed.get("broker_registry_id", ""))
                if (
                    not _SEMANTIC_ID.fullmatch(registry_id)
                    or connection.execute(
                        "SELECT 1 FROM runtime_start_fence WHERE singleton=1"
                    ).fetchone()
                    is None
                ):
                    raise BrokerUnavailable("runtime broker registry is incomplete")
            connection.commit()
        except BaseException:
            connection.rollback()
            raise
        finally:
            connection.close()
        with suppress(OSError):
            os.chmod(path, 0o600)
        return registry_id

    @staticmethod
    def _schema() -> tuple[str, ...]:
        return (
            "CREATE TABLE IF NOT EXISTS broker_meta(key TEXT PRIMARY KEY,value TEXT NOT NULL)",
            "CREATE TABLE IF NOT EXISTS runtime_start_fence("
            "singleton INTEGER PRIMARY KEY CHECK(singleton=1),"
            "highest_binding_generation INTEGER NOT NULL CHECK(highest_binding_generation>=0),"
            "highest_lease_generation INTEGER NOT NULL CHECK(highest_lease_generation>=0),"
            "current_idempotency_key TEXT,updated_at TEXT NOT NULL)",
            "CREATE TABLE IF NOT EXISTS runtime_start_operations("
            "attempt_id TEXT PRIMARY KEY,idempotency_key TEXT NOT NULL UNIQUE,"
            "receipt_id TEXT NOT NULL UNIQUE,request_sha256 TEXT NOT NULL,"
            "binding_generation INTEGER NOT NULL CHECK(binding_generation>0),"
            "lease_generation INTEGER NOT NULL CHECK(lease_generation>0),"
            "state TEXT NOT NULL,disposition TEXT NOT NULL,boundary_crossed TEXT NOT NULL,"
            "service_state TEXT NOT NULL,result_json TEXT,reason_code TEXT NOT NULL,"
            "created_at TEXT NOT NULL,updated_at TEXT NOT NULL)",
            "CREATE TABLE IF NOT EXISTS runtime_start_observations("
            "id TEXT PRIMARY KEY,attempt_id TEXT NOT NULL REFERENCES "
            "runtime_start_operations(attempt_id) ON DELETE RESTRICT,"
            "ordinal INTEGER NOT NULL CHECK(ordinal>0),event_key TEXT NOT NULL,"
            "state TEXT NOT NULL,service_state TEXT NOT NULL,event_sha256 TEXT NOT NULL,"
            "observed_at TEXT NOT NULL,UNIQUE(attempt_id,ordinal),"
            "UNIQUE(attempt_id,event_key))",
            "CREATE TRIGGER IF NOT EXISTS runtime_start_observations_no_update BEFORE UPDATE ON "
            "runtime_start_observations BEGIN SELECT RAISE(ABORT,'Broker observations "
            "are append-only'); END",
            "CREATE TRIGGER IF NOT EXISTS runtime_start_observations_no_delete BEFORE DELETE ON "
            "runtime_start_observations BEGIN SELECT RAISE(ABORT,'Broker observations "
            "are append-only'); END",
        )

    def _connect(self) -> sqlite3.Connection:
        try:
            connection = sqlite3.connect(
                f"file:{self.database_path}?mode=rw",
                uri=True,
                isolation_level=None,
                timeout=10.0,
            )
            connection.row_factory = sqlite3.Row
            connection.execute("PRAGMA busy_timeout=10000")
            connection.execute("PRAGMA foreign_keys=ON")
            return connection
        except sqlite3.Error as error:
            raise BrokerUnavailable("runtime broker registry is unavailable") from error

    @property
    def registry_id(self) -> str:
        return self._registry_id

    def _metadata(self, connection: sqlite3.Connection) -> dict[str, str]:
        try:
            rows = connection.execute("SELECT key,value FROM broker_meta").fetchall()
        except sqlite3.Error as error:
            raise BrokerUnavailable("runtime broker registry metadata is unavailable") from error
        metadata = {str(row["key"]): str(row["value"]) for row in rows}
        expected = {
            "schema_version": _SCHEMA_VERSION,
            "broker_authority_id": self.profile.broker_authority_id,
            "profile_sha256": self.profile.digest,
            "host_identity": self.host_identity,
        }
        if self._registry_id:
            expected["broker_registry_id"] = self._registry_id
        if any(
            metadata.get(key) != value for key, value in expected.items()
        ) or not _SEMANTIC_ID.fullmatch(metadata.get("broker_registry_id", "")):
            raise BrokerUnavailable("runtime broker registry identity changed")
        return metadata

    def _verify_registry(self) -> dict[str, str]:
        try:
            with self._connect() as connection:
                metadata = self._metadata(connection)
                if connection.execute("PRAGMA integrity_check").fetchone()[0] != "ok":
                    raise BrokerUnavailable("runtime broker registry integrity check failed")
                return metadata
        except sqlite3.Error as error:
            raise BrokerUnavailable("runtime broker registry verification failed") from error

    def reserve(self, request: FabricRuntimeStartRequest) -> tuple[dict[str, Any], bool]:
        if (
            request.broker_authority_id != self.profile.broker_authority_id
            or request.broker_registry_id != self.registry_id
            or request.node_id != self.profile.node_id
            or request.binding_id != self.profile.binding_id
            or request.service_name != self.profile.service_profile_id
            or request.service_profile_revision != self.profile.service_profile_revision
            or request.service_profile_sha256 != self.profile.digest
        ):
            raise BrokerProfileMismatch("runtime start request does not match the pinned profile")
        if (
            request.binding_generation > _MAX_GENERATION
            or request.lease_generation > _MAX_GENERATION
        ):
            raise BrokerConflict("runtime start generation exceeds the broker bound")
        now = _timestamp()
        try:
            with self._connect() as connection:
                connection.execute("BEGIN IMMEDIATE")
                try:
                    self._metadata(connection)
                    existing = connection.execute(
                        "SELECT * FROM runtime_start_operations WHERE idempotency_key=?",
                        (request.idempotency_key,),
                    ).fetchone()
                    if existing is not None:
                        if existing["request_sha256"] != request.digest:
                            raise BrokerConflict(
                                "runtime start idempotency key conflicts with another request"
                            )
                        if existing["state"] in {
                            "reserved",
                            "dispatching",
                            "startPending",
                            "outcomeUnknown",
                        }:
                            fence = connection.execute(
                                "SELECT * FROM runtime_start_fence WHERE singleton=1"
                            ).fetchone()
                            if (
                                fence is None
                                or fence["current_idempotency_key"] != request.idempotency_key
                                or int(fence["highest_binding_generation"])
                                != request.binding_generation
                                or int(fence["highest_lease_generation"])
                                != request.lease_generation
                            ):
                                raise BrokerConflict(
                                    "runtime start replay is no longer the current generation"
                                )
                        connection.commit()
                        return dict(existing), True
                    if connection.execute(
                        "SELECT 1 FROM runtime_start_operations WHERE attempt_id=?",
                        (request.attempt_id,),
                    ).fetchone():
                        raise BrokerConflict("runtime start attempt identity conflicts")
                    fence = connection.execute(
                        "SELECT * FROM runtime_start_fence WHERE singleton=1"
                    ).fetchone()
                    if fence is None:
                        raise BrokerUnavailable("runtime start generation fence is missing")
                    highest_binding = int(fence["highest_binding_generation"])
                    highest_lease = int(fence["highest_lease_generation"])
                    if request.binding_generation < highest_binding:
                        raise BrokerConflict("runtime start binding generation is stale")
                    if request.binding_generation > max(1, highest_binding + 1):
                        raise BrokerConflict("runtime start binding generation skipped its fence")
                    if request.lease_generation != highest_lease + 1:
                        raise BrokerConflict("runtime start lease generation is not the next fence")
                    same_generation = connection.execute(
                        "SELECT 1 FROM runtime_start_operations WHERE binding_generation=? "
                        "AND lease_generation=?",
                        (request.binding_generation, request.lease_generation),
                    ).fetchone()
                    if same_generation is not None:
                        raise BrokerConflict("runtime start generation already has another key")
                    unresolved = connection.execute(
                        "SELECT 1 FROM runtime_start_operations WHERE state IN "
                        "('reserved','dispatching','startPending','outcomeUnknown') LIMIT 1"
                    ).fetchone()
                    if unresolved is not None:
                        raise BrokerConflict("a prior runtime start outcome remains unknown")
                    receipt_id = f"runtime-receipt-{uuid.uuid4()}"
                    connection.execute(
                        "INSERT INTO runtime_start_operations("
                        "attempt_id,idempotency_key,receipt_id,request_sha256,"
                        "binding_generation,lease_generation,state,disposition,boundary_crossed,"
                        "service_state,result_json,reason_code,created_at,updated_at) "
                        "VALUES (?,?,?,?,?,?,'reserved','definitelyNotRequested','no','unknown',"
                        "NULL,'runtimeStartReserved',?,?)",
                        (
                            request.attempt_id,
                            request.idempotency_key,
                            receipt_id,
                            request.digest,
                            request.binding_generation,
                            request.lease_generation,
                            now,
                            now,
                        ),
                    )
                    connection.execute(
                        "UPDATE runtime_start_fence SET highest_binding_generation=?,"
                        "highest_lease_generation=?,current_idempotency_key=?,updated_at=? "
                        "WHERE singleton=1",
                        (
                            max(highest_binding, request.binding_generation),
                            request.lease_generation,
                            request.idempotency_key,
                            now,
                        ),
                    )
                    self._append_observation(
                        connection,
                        request.attempt_id,
                        event_key="reserved",
                        state="reserved",
                        service_state="unknown",
                        observed_at=now,
                    )
                    row = connection.execute(
                        "SELECT * FROM runtime_start_operations WHERE attempt_id=?",
                        (request.attempt_id,),
                    ).fetchone()
                    connection.commit()
                    return dict(row), False
                except BaseException:
                    connection.rollback()
                    raise
        except (BrokerConflict, BrokerUnavailable):
            raise
        except sqlite3.Error as error:
            raise BrokerUnavailable("runtime broker reservation failed") from error

    def claim_dispatch(self, request: FabricRuntimeStartRequest) -> bool:
        """CAS the only transition that authorizes an external start boundary."""

        now = _timestamp()
        try:
            with self._connect() as connection:
                connection.execute("BEGIN IMMEDIATE")
                try:
                    self._metadata(connection)
                    fence = connection.execute(
                        "SELECT * FROM runtime_start_fence WHERE singleton=1"
                    ).fetchone()
                    if (
                        fence is None
                        or fence["current_idempotency_key"] != request.idempotency_key
                        or int(fence["highest_binding_generation"]) != request.binding_generation
                        or int(fence["highest_lease_generation"]) != request.lease_generation
                    ):
                        raise BrokerConflict("runtime start dispatch lost its generation fence")
                    if request.deadline_at <= datetime.now(UTC):
                        connection.commit()
                        return False
                    cursor = connection.execute(
                        "UPDATE runtime_start_operations SET state='dispatching',"
                        "disposition='definitelyStartRequested',boundary_crossed='unknown',"
                        "service_state='stopped',reason_code='runtimeStartDispatching',"
                        "updated_at=? WHERE attempt_id=? AND request_sha256=? "
                        "AND state='reserved'",
                        (now, request.attempt_id, request.digest),
                    )
                    if cursor.rowcount == 1:
                        self._append_observation(
                            connection,
                            request.attempt_id,
                            event_key="dispatching",
                            state="dispatching",
                            service_state="stopped",
                            observed_at=now,
                        )
                        connection.commit()
                        return True
                    row = connection.execute(
                        "SELECT state FROM runtime_start_operations WHERE attempt_id=? "
                        "AND request_sha256=?",
                        (request.attempt_id, request.digest),
                    ).fetchone()
                    if row is None:
                        raise BrokerConflict("runtime start dispatch identity changed")
                    connection.commit()
                    return False
                except BaseException:
                    connection.rollback()
                    raise
        except (BrokerConflict, BrokerUnavailable):
            raise
        except sqlite3.Error as error:
            raise BrokerUnavailable("runtime broker dispatch claim failed") from error

    def operation(self, request: FabricRuntimeStartRequest) -> dict[str, Any]:
        try:
            with self._connect() as connection:
                self._metadata(connection)
                row = connection.execute(
                    "SELECT * FROM runtime_start_operations WHERE attempt_id=? "
                    "AND request_sha256=?",
                    (request.attempt_id, request.digest),
                ).fetchone()
                if row is None:
                    raise BrokerConflict("runtime start operation identity changed")
                return dict(row)
        except (BrokerConflict, BrokerUnavailable):
            raise
        except sqlite3.Error as error:
            raise BrokerUnavailable("runtime broker operation is unavailable") from error

    def transition(
        self,
        request: FabricRuntimeStartRequest,
        *,
        expected_states: frozenset[str],
        state: str,
        disposition: str,
        boundary_crossed: str,
        service_state: str,
        reason_code: str,
        result: FabricRuntimeStartResult | None = None,
    ) -> dict[str, Any]:
        now = _timestamp()
        try:
            with self._connect() as connection:
                connection.execute("BEGIN IMMEDIATE")
                try:
                    self._metadata(connection)
                    row = connection.execute(
                        "SELECT * FROM runtime_start_operations WHERE attempt_id=?",
                        (request.attempt_id,),
                    ).fetchone()
                    if row is None or row["request_sha256"] != request.digest:
                        raise BrokerConflict("runtime start request is not canonical")
                    if row["state"] == state:
                        connection.commit()
                        return dict(row)
                    if row["state"] not in expected_states:
                        raise BrokerConflict("runtime start transition is not monotonic")
                    encoded = _canonical(result.to_protocol()) if result is not None else None
                    connection.execute(
                        "UPDATE runtime_start_operations SET state=?,disposition=?,"
                        "boundary_crossed=?,service_state=?,result_json=?,reason_code=?,"
                        "updated_at=? WHERE attempt_id=?",
                        (
                            state,
                            disposition,
                            boundary_crossed,
                            service_state,
                            encoded,
                            reason_code,
                            now,
                            request.attempt_id,
                        ),
                    )
                    self._append_observation(
                        connection,
                        request.attempt_id,
                        event_key=state,
                        state=state,
                        service_state=service_state,
                        observed_at=now,
                    )
                    updated = connection.execute(
                        "SELECT * FROM runtime_start_operations WHERE attempt_id=?",
                        (request.attempt_id,),
                    ).fetchone()
                    connection.commit()
                    return dict(updated)
                except BaseException:
                    connection.rollback()
                    raise
        except (BrokerConflict, BrokerUnavailable):
            raise
        except sqlite3.Error as error:
            raise BrokerUnavailable("runtime broker transition failed") from error

    @staticmethod
    def _append_observation(
        connection: sqlite3.Connection,
        attempt_id: str,
        *,
        event_key: str,
        state: str,
        service_state: str,
        observed_at: str,
    ) -> None:
        ordinal = int(
            connection.execute(
                "SELECT COALESCE(MAX(ordinal),0)+1 AS ordinal "
                "FROM runtime_start_observations WHERE attempt_id=?",
                (attempt_id,),
            ).fetchone()["ordinal"]
        )
        persisted_event_key = event_key
        if connection.execute(
            "SELECT 1 FROM runtime_start_observations WHERE attempt_id=? AND event_key=?",
            (attempt_id, persisted_event_key),
        ).fetchone():
            persisted_event_key = f"{event_key}:{ordinal}"
        value = {
            "attemptID": attempt_id,
            "eventKey": persisted_event_key,
            "state": state,
            "serviceState": service_state,
            "observedAt": observed_at,
        }
        connection.execute(
            "INSERT INTO runtime_start_observations(id,attempt_id,ordinal,event_key,state,"
            "service_state,event_sha256,observed_at) VALUES (?,?,?,?,?,?,?,?)",
            (
                f"broker-observation-{uuid.uuid4()}",
                attempt_id,
                ordinal,
                persisted_event_key,
                state,
                service_state,
                hashlib.sha256(_canonical(value).encode("utf-8")).hexdigest(),
                observed_at,
            ),
        )


class RuntimeBrokerService:
    def __init__(
        self,
        registry: RuntimeBrokerRegistry,
        controller: FixedServiceController,
    ) -> None:
        self.registry = registry
        self.controller = controller
        self._operation_lock = asyncio.Lock()
        self._tasks_lock = asyncio.Lock()
        self._tasks: dict[str, asyncio.Task[FabricRuntimeStartResult]] = {}

    async def start(self, request: FabricRuntimeStartRequest) -> FabricRuntimeStartResult:
        async with self._tasks_lock:
            task = self._tasks.get(request.idempotency_key)
            if task is None or task.done():
                task = asyncio.create_task(self._start_serialized(request))
                self._tasks[request.idempotency_key] = task
        try:
            return await asyncio.shield(task)
        finally:
            if task.done():
                async with self._tasks_lock:
                    if self._tasks.get(request.idempotency_key) is task:
                        self._tasks.pop(request.idempotency_key, None)

    async def _start_serialized(
        self, request: FabricRuntimeStartRequest
    ) -> FabricRuntimeStartResult:
        async with self._operation_lock:
            row, replay = self.registry.reserve(request)
            if row["result_json"] is not None and row["state"] not in {
                "startPending",
                "outcomeUnknown",
            }:
                stored = FabricRuntimeStartResult.from_protocol(json.loads(row["result_json"]))
                return replace(stored, idempotent_replay=True)
            if row["state"] in {"dispatching", "startPending", "outcomeUnknown"}:
                return await self._reconcile_uncertain(request, row, replay=replay)
            if row["state"] != "reserved":
                raise BrokerConflict("runtime start operation is in an unsupported state")
            now = datetime.now(UTC)
            if (
                request.requested_at > now + timedelta(seconds=30)
                or request.deadline_at - request.requested_at > timedelta(seconds=120)
                or request.deadline_at <= now
            ):
                return self._finish(
                    request,
                    row,
                    state="rejectedPreStart",
                    disposition="definitelyNotRequested",
                    boundary_crossed="no",
                    service_state="unknown",
                    reason_code="runtimeStartDeadlineRejected",
                    replay=replay,
                )
            observed = await self.controller.observe()
            current = self.registry.operation(request)
            if current["state"] != "reserved":
                return await self._await_dispatch_owner(request)
            if observed is ServiceState.RUNNING:
                return self._finish(
                    request,
                    row,
                    state="alreadyRunning",
                    disposition="desiredStateSatisfied",
                    boundary_crossed="no",
                    service_state="running",
                    reason_code="runtimeAlreadyRunning",
                    replay=replay,
                )
            if observed is ServiceState.START_PENDING:
                return self._finish(
                    request,
                    row,
                    state="startPending",
                    disposition="startInProgress",
                    boundary_crossed="no",
                    service_state="startPending",
                    reason_code="runtimeStartAlreadyPending",
                    replay=replay,
                )
            if observed is not ServiceState.STOPPED:
                return self._finish(
                    request,
                    row,
                    state="rejectedPreStart",
                    disposition="definitelyNotRequested",
                    boundary_crossed="no",
                    service_state="unknown",
                    reason_code="runtimeServiceStateRejected",
                    replay=replay,
                )
            if request.deadline_at <= datetime.now(UTC):
                return self._finish(
                    request,
                    row,
                    state="rejectedPreStart",
                    disposition="definitelyNotRequested",
                    boundary_crossed="no",
                    service_state="stopped",
                    reason_code="runtimeStartDeadlineRejected",
                    replay=replay,
                )
            if not self.registry.claim_dispatch(request):
                if request.deadline_at <= datetime.now(UTC):
                    return self._finish(
                        request,
                        row,
                        state="rejectedPreStart",
                        disposition="definitelyNotRequested",
                        boundary_crossed="no",
                        service_state="stopped",
                        reason_code="runtimeStartDeadlineRejectedAtClaim",
                        replay=replay,
                    )
                return await self._await_dispatch_owner(request)
            if request.deadline_at <= datetime.now(UTC):
                return self._finish(
                    request,
                    row,
                    state="rejectedPreStart",
                    disposition="definitelyNotRequested",
                    boundary_crossed="no",
                    service_state="stopped",
                    reason_code="runtimeStartDeadlineRejectedAtBoundary",
                    replay=replay,
                    expected_states=frozenset({"dispatching"}),
                )
            disposition = await self.controller.start(deadline_at=request.deadline_at)
            observed = await self.controller.observe()
            if observed is ServiceState.RUNNING:
                if disposition is StartDisposition.ACCEPTED:
                    state = "running"
                    boundary_crossed = "yes"
                    reason_code = "runtimeStartAccepted"
                else:
                    state = "alreadyRunning"
                    boundary_crossed = (
                        "unknown" if disposition is StartDisposition.UNKNOWN else "no"
                    )
                    reason_code = (
                        "runtimeObservedRunningAfterUnknownStart"
                        if disposition is StartDisposition.UNKNOWN
                        else "runtimeAlreadyRunningAfterConcurrentStart"
                    )
                return self._finish(
                    request,
                    row,
                    state=state,
                    disposition="desiredStateSatisfied",
                    boundary_crossed=boundary_crossed,
                    service_state="running",
                    reason_code=reason_code,
                    replay=replay,
                    expected_states=frozenset({"dispatching"}),
                )
            if observed is ServiceState.START_PENDING:
                boundary_crossed = {
                    StartDisposition.ACCEPTED: "yes",
                    StartDisposition.ALREADY_RUNNING: "no",
                    StartDisposition.REJECTED_PRE_START: "no",
                    StartDisposition.UNKNOWN: "unknown",
                }[disposition]
                return self._finish(
                    request,
                    row,
                    state="startPending",
                    disposition="startInProgress",
                    boundary_crossed=boundary_crossed,
                    service_state="startPending",
                    reason_code=(
                        "runtimeStartPending"
                        if disposition is StartDisposition.ACCEPTED
                        else "runtimeObservedConcurrentStartPending"
                    ),
                    replay=replay,
                    expected_states=frozenset({"dispatching"}),
                )
            if disposition is StartDisposition.REJECTED_PRE_START:
                return self._finish(
                    request,
                    row,
                    state="rejectedPreStart",
                    disposition="definitelyNotRequested",
                    boundary_crossed="no",
                    service_state=observed.value,
                    reason_code="runtimeStartRejectedBeforeBoundary",
                    replay=replay,
                    expected_states=frozenset({"dispatching"}),
                )
            if disposition is StartDisposition.ALREADY_RUNNING:
                return self._finish(
                    request,
                    row,
                    state="outcomeUnknown",
                    disposition="startOutcomeUnknown",
                    boundary_crossed="no",
                    service_state=observed.value,
                    reason_code="runtimePreviouslyRunningButCurrentStateUnknown",
                    replay=replay,
                    expected_states=frozenset({"dispatching"}),
                )
            return self._finish(
                request,
                row,
                state="outcomeUnknown",
                disposition="startOutcomeUnknown",
                boundary_crossed=("yes" if disposition is StartDisposition.ACCEPTED else "unknown"),
                service_state=observed.value,
                reason_code="runtimeStartOutcomeUnknown",
                replay=replay,
                expected_states=frozenset({"dispatching"}),
            )

    async def _await_dispatch_owner(
        self, request: FabricRuntimeStartRequest
    ) -> FabricRuntimeStartResult:
        for _ in range(200):
            row = self.registry.operation(request)
            if row["result_json"] is not None:
                stored = FabricRuntimeStartResult.from_protocol(json.loads(row["result_json"]))
                return replace(stored, idempotent_replay=True)
            if row["state"] != "dispatching":
                return await self._reconcile_uncertain(request, row, replay=True)
            await asyncio.sleep(0.01)
        row = self.registry.operation(request)
        return FabricRuntimeStartResult(
            attempt_id=request.attempt_id,
            receipt_id=str(row["receipt_id"]),
            request_sha256=request.digest,
            binding_id=request.binding_id,
            binding_generation=request.binding_generation,
            node_id=request.node_id,
            service_name=request.service_name,
            broker_authority_id=request.broker_authority_id,
            broker_registry_id=request.broker_registry_id,
            service_profile_revision=request.service_profile_revision,
            service_profile_sha256=request.service_profile_sha256,
            idempotency_key=request.idempotency_key,
            lease_generation=request.lease_generation,
            state="startPending",
            disposition="startInProgress",
            external_start_boundary_crossed="unknown",
            service_state="unknown",
            accepted=False,
            reason_code="runtimeStartOwnedByConcurrentBroker",
            observed_at=datetime.now(UTC),
            idempotent_replay=True,
        )

    async def _reconcile_uncertain(
        self,
        request: FabricRuntimeStartRequest,
        row: Mapping[str, Any],
        *,
        replay: bool,
    ) -> FabricRuntimeStartResult:
        observed = await self.controller.observe()
        if observed is ServiceState.RUNNING:
            return self._finish(
                request,
                row,
                state="running",
                disposition="desiredStateSatisfied",
                boundary_crossed=str(row["boundary_crossed"]),
                service_state="running",
                reason_code="runtimeObservedRunningAfterUncertainStart",
                replay=replay,
                expected_states=frozenset({"dispatching", "startPending", "outcomeUnknown"}),
            )
        if observed is ServiceState.START_PENDING:
            return self._finish(
                request,
                row,
                state="startPending",
                disposition="startInProgress",
                boundary_crossed=str(row["boundary_crossed"]),
                service_state="startPending",
                reason_code="runtimeStartStillPending",
                replay=replay,
                expected_states=frozenset({"dispatching", "outcomeUnknown"}),
            )
        return self._finish(
            request,
            row,
            state="outcomeUnknown",
            disposition="startOutcomeUnknown",
            boundary_crossed="unknown",
            service_state=observed.value,
            reason_code="runtimeStartOutcomeRemainsUnknown",
            replay=replay,
            expected_states=frozenset({"dispatching", "startPending", "outcomeUnknown"}),
        )

    def _finish(
        self,
        request: FabricRuntimeStartRequest,
        row: Mapping[str, Any],
        *,
        state: str,
        disposition: str,
        boundary_crossed: str,
        service_state: str,
        reason_code: str,
        replay: bool,
        expected_states: frozenset[str] = frozenset({"reserved"}),
    ) -> FabricRuntimeStartResult:
        result = FabricRuntimeStartResult(
            attempt_id=request.attempt_id,
            receipt_id=str(row["receipt_id"]),
            request_sha256=request.digest,
            binding_id=request.binding_id,
            binding_generation=request.binding_generation,
            node_id=request.node_id,
            service_name=request.service_name,
            broker_authority_id=request.broker_authority_id,
            broker_registry_id=request.broker_registry_id,
            service_profile_revision=request.service_profile_revision,
            service_profile_sha256=request.service_profile_sha256,
            idempotency_key=request.idempotency_key,
            lease_generation=request.lease_generation,
            state=state,
            disposition=disposition,
            external_start_boundary_crossed=boundary_crossed,
            service_state=service_state,
            accepted=state in {"running", "alreadyRunning"},
            reason_code=reason_code,
            observed_at=datetime.now(UTC),
            idempotent_replay=replay,
        )
        updated = self.registry.transition(
            request,
            expected_states=expected_states,
            state=state,
            disposition=disposition,
            boundary_crossed=boundary_crossed,
            service_state=service_state,
            reason_code=reason_code,
            result=replace(result, idempotent_replay=False),
        )
        if updated["result_json"] is not None:
            stored = FabricRuntimeStartResult.from_protocol(json.loads(updated["result_json"]))
            return replace(stored, idempotent_replay=replay)
        return result


def _reject_json_constant(_value: str) -> None:
    raise ValueError("non-finite JSON constants are forbidden")


def _pairs_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    value: dict[str, Any] = {}
    for key, item in pairs:
        if key in value:
            raise ValueError("duplicate JSON keys are forbidden")
        value[key] = item
    return value


async def _json_body(request: Request) -> Mapping[str, Any] | None:
    chunks: list[bytes] = []
    total = 0
    async for chunk in request.stream():
        total += len(chunk)
        if total > _MAX_REQUEST_BYTES:
            return None
        chunks.append(chunk)
    raw = b"".join(chunks)
    if not raw:
        return None
    try:
        value = json.loads(
            raw.decode("utf-8"),
            object_pairs_hook=_pairs_object,
            parse_constant=_reject_json_constant,
        )
    except (UnicodeError, ValueError):
        return None
    return value if isinstance(value, Mapping) else None


def create_runtime_broker_app(
    service: RuntimeBrokerService,
    *,
    auth_token: str,
) -> FastAPI:
    if not _BEARER.fullmatch(auth_token):
        raise ValueError("runtime broker bearer must be a URL-safe bounded token")

    @asynccontextmanager
    async def lifespan(_app: FastAPI) -> AsyncIterator[None]:
        service.registry._verify_registry()
        yield

    app = FastAPI(
        title="CHIPS Fabric Runtime Broker",
        version="1",
        lifespan=lifespan,
        docs_url=None,
        redoc_url=None,
        openapi_url=None,
    )

    @app.middleware("http")
    async def authenticate(request: Request, call_next):
        observed = request.headers.get("authorization", "")
        expected = f"Bearer {auth_token}"
        if not hmac.compare_digest(observed, expected):
            response = JSONResponse(
                status_code=401,
                content={"error": {"code": "AUTH_REQUIRED", "message": "Bearer required"}},
            )
        else:
            response = await call_next(request)
        response.headers["cache-control"] = "no-store"
        return response

    @app.get("/v1/health")
    async def health() -> dict[str, Any]:
        return {
            "schemaVersion": "fabric-runtime-broker-health/v1",
            "brokerAuthorityID": service.registry.profile.broker_authority_id,
            "brokerRegistryID": service.registry.registry_id,
            "nodeID": service.registry.profile.node_id,
            "serviceProfileID": service.registry.profile.service_profile_id,
            "serviceProfileRevision": service.registry.profile.service_profile_revision,
            "serviceProfileSHA256": service.registry.profile.digest,
            "operation": "fabric.runtime.start",
            "durableIdempotency": True,
            "generationFencing": True,
            "deadlineEnforcement": True,
        }

    @app.post("/v1/fabric/runtime/start")
    async def start_runtime(request: Request):
        payload = await _json_body(request)
        if payload is None:
            return JSONResponse(
                status_code=400,
                content={"error": {"code": "INVALID_REQUEST", "message": "invalid body"}},
            )
        try:
            typed = FabricRuntimeStartRequest.from_protocol(payload)
            result = await service.start(typed)
        except BrokerProfileMismatch:
            return JSONResponse(
                status_code=409,
                content={
                    "error": {
                        "code": "BROKER_OR_PROFILE_IDENTITY_MISMATCH",
                        "message": "runtime start authority does not match this broker",
                    }
                },
            )
        except BrokerConflict:
            return JSONResponse(
                status_code=409,
                content={
                    "error": {
                        "code": "IDEMPOTENCY_OR_GENERATION_CONFLICT",
                        "message": "runtime start conflicts with durable broker state",
                    }
                },
            )
        except BrokerUnavailable:
            return JSONResponse(
                status_code=503,
                content={
                    "error": {
                        "code": "BROKER_STATE_UNAVAILABLE",
                        "message": "runtime start state is unavailable",
                    }
                },
            )
        except ValueError:
            return JSONResponse(
                status_code=400,
                content={"error": {"code": "INVALID_REQUEST", "message": "invalid body"}},
            )
        status = 200
        if result.state in {"startPending", "outcomeUnknown"}:
            status = 202
        elif result.state == "rejectedPreStart":
            status = 422
        elif result.state == "failed":
            status = 503
        return JSONResponse(status_code=status, content=result.to_protocol())

    return app


def _load_windows_credential(target: str) -> str:
    if platform.system() != "Windows":
        raise ValueError("Windows Credential Manager is available only on Windows")
    if not _SEMANTIC_ID.fullmatch(target):
        raise ValueError("credential target must be a bounded semantic ID")
    import ctypes
    from ctypes import wintypes

    class Credential(ctypes.Structure):
        _fields_ = [
            ("flags", wintypes.DWORD),
            ("type", wintypes.DWORD),
            ("target_name", wintypes.LPWSTR),
            ("comment", wintypes.LPWSTR),
            ("last_written", wintypes.FILETIME),
            ("blob_size", wintypes.DWORD),
            ("blob", ctypes.POINTER(ctypes.c_ubyte)),
            ("persist", wintypes.DWORD),
            ("attribute_count", wintypes.DWORD),
            ("attributes", ctypes.c_void_p),
            ("target_alias", wintypes.LPWSTR),
            ("user_name", wintypes.LPWSTR),
        ]

    api = ctypes.WinDLL("advapi32", use_last_error=True)
    pointer = ctypes.POINTER(Credential)()
    api.CredReadW.argtypes = [
        wintypes.LPCWSTR,
        wintypes.DWORD,
        wintypes.DWORD,
        ctypes.POINTER(ctypes.POINTER(Credential)),
    ]
    api.CredReadW.restype = wintypes.BOOL
    api.CredFree.argtypes = [ctypes.c_void_p]
    if not api.CredReadW(target, 1, 0, ctypes.byref(pointer)):
        raise ValueError("runtime broker credential is unavailable")
    try:
        raw = ctypes.string_at(pointer.contents.blob, pointer.contents.blob_size)
        token = raw.decode("ascii")
    finally:
        api.CredFree(pointer)
    if not _BEARER.fullmatch(token):
        raise ValueError("runtime broker credential has an invalid shape")
    return token


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="fixed-profile CHIPS Fabric runtime broker")
    parser.add_argument("--data-dir", type=Path, required=True)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, required=True)
    parser.add_argument("--broker-authority-id", required=True)
    parser.add_argument("--node-id", required=True)
    parser.add_argument("--binding-id", required=True)
    parser.add_argument("--service-profile-id", required=True)
    parser.add_argument("--service-profile-revision", type=int, required=True)
    parser.add_argument("--target-service-name", required=True)
    parser.add_argument("--expected-service-config-sha256")
    parser.add_argument("--initialize-registry", action="store_true")
    parser.add_argument("--initial-lease-generation", type=int, default=0)
    parser.add_argument("--production", action="store_true")
    parser.add_argument("--auth-credential-target")
    parser.add_argument("--auth-token-file", type=Path)
    parser.add_argument("--test-service-marker", type=Path)
    parser.add_argument("--test-invocation-log", type=Path)
    parser.add_argument("--test-after-start-barrier", type=Path)
    parser.add_argument("--test-host-identity")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.host not in {"127.0.0.1", "::1", "localhost"}:
        raise SystemExit("runtime broker binds only a loopback interface")
    profile = RuntimeBrokerProfile(
        broker_authority_id=args.broker_authority_id,
        node_id=args.node_id,
        binding_id=args.binding_id,
        service_profile_id=args.service_profile_id,
        service_profile_revision=args.service_profile_revision,
        target_service_name=args.target_service_name,
        expected_service_config_sha256=args.expected_service_config_sha256,
    )
    test_options = (
        args.test_service_marker,
        args.test_invocation_log,
        args.test_after_start_barrier,
        args.test_host_identity,
        args.auth_token_file,
    )
    if args.production:
        if any(value is not None for value in test_options):
            raise SystemExit("production runtime broker rejects every test option")
        if args.auth_credential_target is None or profile.expected_service_config_sha256 is None:
            raise SystemExit(
                "production runtime broker requires Credential Manager auth and a pinned service"
            )
        token = _load_windows_credential(args.auth_credential_target)
        controller: FixedServiceController = WindowsSCMServiceController(
            profile.target_service_name,
            expected_config_sha256=profile.expected_service_config_sha256,
        )
        test_identity = None
    else:
        if (
            args.auth_credential_target is not None
            or args.auth_token_file is None
            or args.test_service_marker is None
            or args.test_invocation_log is None
        ):
            raise SystemExit("test runtime broker requires only fixed test files and token file")
        token = _load_auth_token(args.auth_token_file)
        controller = TestMarkerServiceController(
            args.test_service_marker,
            args.test_invocation_log,
            after_start_barrier=args.test_after_start_barrier,
        )
        test_identity = args.test_host_identity or "runtime-broker-test-host"
    database_path = args.data_dir / "runtime-broker.db"
    if args.initialize_registry:
        registry_id = RuntimeBrokerRegistry.initialize(
            database_path,
            profile,
            test_host_identity=test_identity,
            initial_lease_generation=args.initial_lease_generation,
        )
        print(
            _canonical(
                {
                    "brokerAuthorityID": profile.broker_authority_id,
                    "brokerRegistryID": registry_id,
                    "serviceProfileSHA256": profile.digest,
                }
            )
        )
    registry = RuntimeBrokerRegistry(
        database_path,
        profile,
        test_host_identity=test_identity,
    )
    app = create_runtime_broker_app(RuntimeBrokerService(registry, controller), auth_token=token)
    import uvicorn

    uvicorn.run(app, host=args.host, port=args.port, log_level="warning")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
