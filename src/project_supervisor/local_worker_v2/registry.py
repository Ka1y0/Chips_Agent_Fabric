from __future__ import annotations

import hashlib
import json
import os
import sqlite3
import threading
import uuid
from collections.abc import Iterator, Mapping
from contextlib import contextmanager, suppress
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from .drivers import DriverProfile
from .model import PROTOCOL_VERSION, LaunchRecord, LaunchState, validate_idempotency_key
from .process import local_host_identity

_SCHEMA_VERSION = "2"
_BASE_SCHEMA = """
CREATE TABLE IF NOT EXISTS registry_meta (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS launch_records (
    launch_record_id TEXT PRIMARY KEY,
    authority_scope TEXT NOT NULL,
    idempotency_key TEXT NOT NULL,
    request_digest TEXT NOT NULL,
    protocol_version INTEGER NOT NULL CHECK(protocol_version = 2),
    launch_state TEXT NOT NULL CHECK(launch_state IN (
        'RESERVED','REJECTED_PRE_LAUNCH','LAUNCHING','RUNNING',
        'COMPLETED','FAILED','CANCELLED'
    )),
    job_id TEXT UNIQUE,
    receipt_id TEXT NOT NULL UNIQUE,
    launch_nonce TEXT UNIQUE,
    process_pid INTEGER CHECK(process_pid IS NULL OR process_pid > 0),
    process_birth_identity TEXT,
    process_host_identity TEXT,
    request_received_at TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    job_created_at TEXT,
    terminal_at TEXT,
    launch_error TEXT,
    result_json TEXT,
    exit_code INTEGER,
    UNIQUE(authority_scope, idempotency_key)
);

CREATE INDEX IF NOT EXISTS launch_records_state
ON launch_records(launch_state, updated_at, launch_record_id);
"""
_MIGRATION_1_TO_2 = (
    "ALTER TABLE launch_records ADD COLUMN driver_id TEXT",
    "ALTER TABLE launch_records ADD COLUMN driver_type TEXT",
    "ALTER TABLE launch_records ADD COLUMN driver_profile_revision INTEGER "
    "CHECK(driver_profile_revision IS NULL OR driver_profile_revision > 0)",
    "ALTER TABLE launch_records ADD COLUMN driver_profile_fingerprint TEXT",
    "ALTER TABLE launch_records ADD COLUMN launch_runtime_instance_id TEXT",
)
_V2_COLUMNS = frozenset(
    {
        "driver_id",
        "driver_type",
        "driver_profile_revision",
        "driver_profile_fingerprint",
        "launch_runtime_instance_id",
    }
)
_DEFAULT_TEST_PROFILE = DriverProfile.test()


def timestamp() -> str:
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")


class RegistryUnavailable(RuntimeError):
    """The durable launch registry could not authoritatively answer a query."""


class IdempotencyConflict(RuntimeError):
    def __init__(self, existing: LaunchRecord) -> None:
        self.existing = existing
        super().__init__("idempotency key is already bound to a different request digest")


class LaunchRegistry:
    """Small cross-process-safe SQLite authority for Local Worker launch identities."""

    def __init__(
        self,
        database_path: Path,
        *,
        authority_scope: str = "local-loopback",
        node_id: str | None = None,
        test_host_identity: str | None = None,
    ) -> None:
        if not authority_scope.strip():
            raise ValueError("authority_scope must not be empty")
        self.database_path = Path(database_path)
        self.authority_scope = authority_scope
        if node_id is not None and (not node_id.strip() or len(node_id) > 200):
            raise ValueError("node_id must be a bounded non-empty identity")
        self._configured_node_id = node_id
        if test_host_identity is not None and not test_host_identity.strip():
            raise ValueError("test_host_identity must not be empty")
        self._host_identity = (
            "sha256:"
            + hashlib.sha256(f"local-worker-test-host:{test_host_identity}".encode()).hexdigest()
            if test_host_identity is not None
            else local_host_identity()
        )
        self.database_path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        with suppress(OSError):
            os.chmod(self.database_path.parent, 0o700)
        self._initialize()
        self._identity_snapshot = (
            self._meta("authority_id"),
            self._meta("registry_id"),
            self._meta("node_id"),
            self._meta("process_host_identity"),
        )

    def _connect(self) -> sqlite3.Connection:
        try:
            connection = sqlite3.connect(
                self.database_path,
                timeout=10.0,
                isolation_level=None,
            )
            connection.row_factory = sqlite3.Row
            connection.execute("PRAGMA foreign_keys=ON")
            connection.execute("PRAGMA busy_timeout=10000")
            expected = getattr(self, "_identity_snapshot", None)
            if expected is not None:
                rows = connection.execute(
                    "SELECT key,value FROM registry_meta WHERE key IN "
                    "('authority_id','registry_id','node_id','process_host_identity')"
                ).fetchall()
                observed = {row["key"]: row["value"] for row in rows}
                current = (
                    observed.get("authority_id"),
                    observed.get("registry_id"),
                    observed.get("node_id"),
                    observed.get("process_host_identity"),
                )
                if current != expected:
                    connection.close()
                    raise RegistryUnavailable(
                        "launch registry identity changed after capability negotiation"
                    )
            return connection
        except RegistryUnavailable:
            raise
        except sqlite3.Error as error:
            raise RegistryUnavailable(
                f"launch registry unavailable: {type(error).__name__}"
            ) from error

    def _initialize(self) -> None:
        try:
            with self._connect() as connection:
                connection.execute("PRAGMA journal_mode=WAL")
                connection.execute("PRAGMA synchronous=FULL")
                connection.execute("BEGIN IMMEDIATE")
                try:
                    # sqlite3.executescript() implicitly commits before running a script.  Execute
                    # these simple statements one by one so BEGIN IMMEDIATE remains authoritative.
                    for statement in _BASE_SCHEMA.split(";"):
                        if statement.strip():
                            connection.execute(statement)
                    version = connection.execute(
                        "SELECT value FROM registry_meta WHERE key='schema_version'"
                    ).fetchone()
                    if version is None:
                        connection.execute(
                            "INSERT INTO registry_meta(key,value) VALUES ('schema_version',?)",
                            ("1",),
                        )
                        current_version = "1"
                    else:
                        current_version = str(version["value"])
                    if current_version == "1":
                        for statement in _MIGRATION_1_TO_2:
                            connection.execute(statement)
                        connection.execute(
                            "UPDATE registry_meta SET value=? WHERE key='schema_version'",
                            (_SCHEMA_VERSION,),
                        )
                    elif current_version != _SCHEMA_VERSION:
                        raise RegistryUnavailable(
                            f"unsupported Local Worker registry schema {current_version}"
                        )
                    columns = {
                        str(row["name"])
                        for row in connection.execute(
                            "PRAGMA table_info(launch_records)"
                        ).fetchall()
                    }
                    if not _V2_COLUMNS.issubset(columns):
                        raise RegistryUnavailable("Local Worker registry schema v2 is incomplete")
                    host_identity = self._host_identity
                    if host_identity is None:
                        raise RegistryUnavailable(
                            "stable local host identity is unavailable; "
                            "durable PID recovery disabled"
                        )
                    for key, prefix in (
                        ("authority_id", "local-worker-authority"),
                        ("registry_id", "local-worker-registry"),
                        ("node_id", "local-worker-node"),
                    ):
                        configured = self._configured_node_id if key == "node_id" else None
                        connection.execute(
                            "INSERT OR IGNORE INTO registry_meta(key,value) VALUES (?,?)",
                            (key, configured or f"{prefix}-{uuid.uuid4()}"),
                        )
                    if self._configured_node_id is not None:
                        bound_node = connection.execute(
                            "SELECT value FROM registry_meta WHERE key='node_id'"
                        ).fetchone()
                        if bound_node is None or bound_node["value"] != self._configured_node_id:
                            raise RegistryUnavailable(
                                "launch registry is bound to a different stable node identity"
                            )
                    bound_host = connection.execute(
                        "SELECT value FROM registry_meta WHERE key='process_host_identity'"
                    ).fetchone()
                    if bound_host is None:
                        connection.execute(
                            "INSERT INTO registry_meta(key,value) "
                            "VALUES ('process_host_identity',?)",
                            (host_identity,),
                        )
                    elif bound_host["value"] != host_identity:
                        raise RegistryUnavailable(
                            "launch registry belongs to a different host; process state is unknown"
                        )
                    connection.commit()
                except BaseException:
                    connection.rollback()
                    raise
            with suppress(OSError):
                os.chmod(self.database_path, 0o600)
        except RegistryUnavailable:
            raise
        except (OSError, sqlite3.Error) as error:
            raise RegistryUnavailable(
                f"launch registry initialization failed: {type(error).__name__}"
            ) from error

    @contextmanager
    def _transaction(self) -> Iterator[sqlite3.Connection]:
        try:
            with self._connect() as connection:
                connection.execute("BEGIN IMMEDIATE")
                try:
                    yield connection
                    connection.commit()
                except BaseException:
                    connection.rollback()
                    raise
        except (IdempotencyConflict, KeyError, RuntimeError, ValueError):
            raise
        except sqlite3.Error as error:
            raise RegistryUnavailable(
                f"launch registry unavailable: {type(error).__name__}"
            ) from error

    def _meta(self, key: str) -> str:
        try:
            with self._connect() as connection:
                row = connection.execute(
                    "SELECT value FROM registry_meta WHERE key=?", (key,)
                ).fetchone()
        except sqlite3.Error as error:
            raise RegistryUnavailable(
                f"launch registry unavailable: {type(error).__name__}"
            ) from error
        if row is None:
            raise RegistryUnavailable(f"launch registry identity {key} is unavailable")
        return str(row["value"])

    @property
    def authority_id(self) -> str:
        return self._identity_snapshot[0]

    @property
    def registry_id(self) -> str:
        return self._identity_snapshot[1]

    @property
    def node_id(self) -> str:
        return self._identity_snapshot[2]

    @property
    def process_host_identity(self) -> str:
        return self._identity_snapshot[3]

    def check_available(self) -> None:
        """Verify that the negotiated registry incarnation is still authoritative."""

        with self._connect() as connection:
            connection.execute("SELECT 1").fetchone()

    @staticmethod
    def _record(row: sqlite3.Row) -> LaunchRecord:
        return LaunchRecord(
            launch_record_id=row["launch_record_id"],
            authority_scope=row["authority_scope"],
            idempotency_key=row["idempotency_key"],
            request_digest=row["request_digest"],
            protocol_version=int(row["protocol_version"]),
            driver_id=row["driver_id"],
            driver_type=row["driver_type"],
            driver_profile_revision=row["driver_profile_revision"],
            driver_profile_fingerprint=row["driver_profile_fingerprint"],
            launch_runtime_instance_id=row["launch_runtime_instance_id"],
            launch_state=LaunchState(row["launch_state"]),
            job_id=row["job_id"],
            receipt_id=row["receipt_id"],
            launch_nonce=row["launch_nonce"],
            process_pid=row["process_pid"],
            process_birth_identity=row["process_birth_identity"],
            process_host_identity=row["process_host_identity"],
            request_received_at=row["request_received_at"],
            created_at=row["created_at"],
            updated_at=row["updated_at"],
            job_created_at=row["job_created_at"],
            terminal_at=row["terminal_at"],
            launch_error=row["launch_error"],
            result_json=row["result_json"],
            exit_code=row["exit_code"],
        )

    @staticmethod
    def _by_key(
        connection: sqlite3.Connection,
        authority_scope: str,
        idempotency_key: str,
    ) -> sqlite3.Row | None:
        return connection.execute(
            "SELECT * FROM launch_records WHERE authority_scope=? AND idempotency_key=?",
            (authority_scope, idempotency_key),
        ).fetchone()

    def reserve(
        self,
        *,
        idempotency_key: str,
        request_digest: str,
        driver_id: str = _DEFAULT_TEST_PROFILE.driver_id,
        driver_type: str = _DEFAULT_TEST_PROFILE.driver_type.value,
        driver_profile_revision: int = _DEFAULT_TEST_PROFILE.profile_revision,
        driver_profile_fingerprint: str = _DEFAULT_TEST_PROFILE.profile_fingerprint,
    ) -> tuple[LaunchRecord, bool]:
        key = validate_idempotency_key(idempotency_key)
        if not request_digest.startswith("sha256:") or len(request_digest) != 71:
            raise ValueError("request_digest must be a sha256 digest")
        self._validate_driver_binding(
            driver_id,
            driver_type,
            driver_profile_revision,
            driver_profile_fingerprint,
        )
        now = timestamp()
        with self._transaction() as connection:
            existing_row = self._by_key(connection, self.authority_scope, key)
            if existing_row is not None:
                existing = self._record(existing_row)
                if existing.request_digest != request_digest:
                    raise IdempotencyConflict(existing)
                return existing, False
            record_id = f"launch-{uuid.uuid4()}"
            job_id = f"local-job-{uuid.uuid4()}"
            receipt_id = f"receipt-{uuid.uuid4()}"
            connection.execute(
                "INSERT INTO launch_records("
                "launch_record_id,authority_scope,idempotency_key,request_digest,protocol_version,"
                "driver_id,driver_type,driver_profile_revision,driver_profile_fingerprint,"
                "launch_state,job_id,receipt_id,request_received_at,created_at,updated_at"
                ") VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    record_id,
                    self.authority_scope,
                    key,
                    request_digest,
                    PROTOCOL_VERSION,
                    driver_id,
                    driver_type,
                    driver_profile_revision,
                    driver_profile_fingerprint,
                    LaunchState.RESERVED.value,
                    job_id,
                    receipt_id,
                    now,
                    now,
                    now,
                ),
            )
            row = self._by_key(connection, self.authority_scope, key)
            assert row is not None
            return self._record(row), True

    def reject_pre_launch(
        self,
        *,
        idempotency_key: str,
        request_digest: str,
        reason: str,
        driver_id: str | None = None,
        driver_type: str | None = None,
        driver_profile_revision: int | None = None,
        driver_profile_fingerprint: str | None = None,
    ) -> tuple[LaunchRecord, bool]:
        key = validate_idempotency_key(idempotency_key)
        if not request_digest.startswith("sha256:") or len(request_digest) != 71:
            raise ValueError("request_digest must be a sha256 digest")
        driver_values = (
            driver_id,
            driver_type,
            driver_profile_revision,
            driver_profile_fingerprint,
        )
        if any(value is not None for value in driver_values):
            if not all(value is not None for value in driver_values):
                raise ValueError("pre-launch driver identity must be complete when known")
            self._validate_driver_binding(
                str(driver_id),
                str(driver_type),
                int(driver_profile_revision),
                str(driver_profile_fingerprint),
            )
        now = timestamp()
        with self._transaction() as connection:
            existing_row = self._by_key(connection, self.authority_scope, key)
            if existing_row is not None:
                existing = self._record(existing_row)
                if existing.request_digest != request_digest:
                    raise IdempotencyConflict(existing)
                return existing, False
            connection.execute(
                "INSERT INTO launch_records("
                "launch_record_id,authority_scope,idempotency_key,request_digest,protocol_version,"
                "driver_id,driver_type,driver_profile_revision,driver_profile_fingerprint,"
                "launch_state,receipt_id,request_received_at,created_at,updated_at,"
                "terminal_at,launch_error) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    f"launch-{uuid.uuid4()}",
                    self.authority_scope,
                    key,
                    request_digest,
                    PROTOCOL_VERSION,
                    driver_id,
                    driver_type,
                    driver_profile_revision,
                    driver_profile_fingerprint,
                    LaunchState.REJECTED_PRE_LAUNCH.value,
                    f"receipt-{uuid.uuid4()}",
                    now,
                    now,
                    now,
                    now,
                    reason[:1000],
                ),
            )
            row = self._by_key(connection, self.authority_scope, key)
            assert row is not None
            return self._record(row), True

    def claim_spawn(
        self,
        idempotency_key: str,
        *,
        runtime_instance_id: str = "runtime-unspecified",
    ) -> tuple[LaunchRecord, bool]:
        key = validate_idempotency_key(idempotency_key)
        if not runtime_instance_id.strip() or len(runtime_instance_id) > 200:
            raise ValueError("runtime_instance_id must be a bounded non-empty identity")
        with self._transaction() as connection:
            row = self._by_key(connection, self.authority_scope, key)
            if row is None:
                raise KeyError(key)
            record = self._record(row)
            if record.launch_state is not LaunchState.RESERVED:
                return record, False
            now = timestamp()
            connection.execute(
                "UPDATE launch_records SET launch_state=?,launch_nonce=?,"
                "launch_runtime_instance_id=?,updated_at=? "
                "WHERE launch_record_id=? AND launch_state=?",
                (
                    LaunchState.LAUNCHING.value,
                    f"nonce-{uuid.uuid4()}",
                    runtime_instance_id,
                    now,
                    record.launch_record_id,
                    LaunchState.RESERVED.value,
                ),
            )
            updated = self._by_key(connection, self.authority_scope, key)
            assert updated is not None
            return self._record(updated), True

    @staticmethod
    def _validate_driver_binding(
        driver_id: str,
        driver_type: str,
        profile_revision: int,
        profile_fingerprint: str,
    ) -> None:
        if not driver_id.strip() or len(driver_id) > 128:
            raise ValueError("driver_id must be a bounded non-empty identity")
        if not driver_type.strip() or len(driver_type) > 32:
            raise ValueError("driver_type must be a bounded non-empty identity")
        if profile_revision < 1:
            raise ValueError("driver profile revision must be positive")
        if not profile_fingerprint.startswith("sha256:") or len(profile_fingerprint) != 71:
            raise ValueError("driver profile fingerprint must be a sha256 digest")

    def record_started(
        self,
        *,
        idempotency_key: str,
        job_id: str,
        launch_nonce: str,
        request_digest: str,
        pid: int,
        process_birth_identity: str,
        process_host_identity: str,
    ) -> LaunchRecord:
        key = validate_idempotency_key(idempotency_key)
        if pid <= 0:
            raise ValueError("process pid must be positive")
        with self._transaction() as connection:
            row = self._by_key(connection, self.authority_scope, key)
            if row is None:
                raise KeyError(key)
            record = self._record(row)
            expected = (record.job_id, record.launch_nonce, record.request_digest)
            if expected != (job_id, launch_nonce, request_digest):
                raise RuntimeError("started receipt does not match the durable launch identity")
            if record.launch_state not in {LaunchState.LAUNCHING, LaunchState.RUNNING}:
                raise RuntimeError(f"cannot attach process to {record.launch_state.value}")
            values = (pid, process_birth_identity, process_host_identity)
            if record.launch_state is LaunchState.RUNNING:
                if (
                    record.process_pid,
                    record.process_birth_identity,
                    record.process_host_identity,
                ) != values:
                    raise RuntimeError("running process identity is immutable")
                return record
            now = timestamp()
            connection.execute(
                "UPDATE launch_records SET launch_state=?,process_pid=?,"
                "process_birth_identity=?,process_host_identity=?,job_created_at=?,updated_at=?,"
                "launch_error=NULL "
                "WHERE launch_record_id=?",
                (
                    LaunchState.RUNNING.value,
                    *values,
                    now,
                    now,
                    record.launch_record_id,
                ),
            )
            updated = self._by_key(connection, self.authority_scope, key)
            assert updated is not None
            return self._record(updated)

    def record_launch_error(self, idempotency_key: str, detail: str) -> LaunchRecord:
        key = validate_idempotency_key(idempotency_key)
        with self._transaction() as connection:
            row = self._by_key(connection, self.authority_scope, key)
            if row is None:
                raise KeyError(key)
            connection.execute(
                "UPDATE launch_records SET launch_error=?,updated_at=? WHERE launch_record_id=?",
                (detail[:1000], timestamp(), row["launch_record_id"]),
            )
            updated = self._by_key(connection, self.authority_scope, key)
            assert updated is not None
            return self._record(updated)

    def record_terminal(
        self,
        *,
        idempotency_key: str,
        job_id: str,
        launch_nonce: str,
        request_digest: str,
        state: LaunchState,
        result: Mapping[str, Any],
        exit_code: int | None,
    ) -> LaunchRecord:
        if state not in {LaunchState.COMPLETED, LaunchState.FAILED, LaunchState.CANCELLED}:
            raise ValueError("terminal process receipt has a non-terminal state")
        key = validate_idempotency_key(idempotency_key)
        result_json = json.dumps(
            dict(result), ensure_ascii=False, separators=(",", ":"), sort_keys=True
        )
        with self._transaction() as connection:
            row = self._by_key(connection, self.authority_scope, key)
            if row is None:
                raise KeyError(key)
            record = self._record(row)
            if (record.job_id, record.launch_nonce, record.request_digest) != (
                job_id,
                launch_nonce,
                request_digest,
            ):
                raise RuntimeError("terminal receipt does not match the durable launch identity")
            if record.launch_state in {
                LaunchState.COMPLETED,
                LaunchState.FAILED,
                LaunchState.CANCELLED,
            }:
                if (
                    record.launch_state is state
                    and record.result_json == result_json
                    and record.exit_code == exit_code
                ):
                    return record
                raise RuntimeError("terminal launch result is immutable")
            now = timestamp()
            connection.execute(
                "UPDATE launch_records SET launch_state=?,result_json=?,exit_code=?,"
                "terminal_at=?,updated_at=?,launch_error=NULL WHERE launch_record_id=?",
                (state.value, result_json, exit_code, now, now, record.launch_record_id),
            )
            updated = self._by_key(connection, self.authority_scope, key)
            assert updated is not None
            return self._record(updated)

    def get_launch(self, idempotency_key: str) -> LaunchRecord | None:
        key = validate_idempotency_key(idempotency_key)
        try:
            with self._connect() as connection:
                row = self._by_key(connection, self.authority_scope, key)
        except sqlite3.Error as error:
            raise RegistryUnavailable(
                f"launch registry unavailable: {type(error).__name__}"
            ) from error
        return self._record(row) if row is not None else None

    def get_job(self, job_id: str) -> LaunchRecord | None:
        if not job_id.strip() or len(job_id) > 256:
            raise ValueError("job_id must be a bounded non-empty string")
        try:
            with self._connect() as connection:
                row = connection.execute(
                    "SELECT * FROM launch_records WHERE authority_scope=? AND job_id=?",
                    (self.authority_scope, job_id),
                ).fetchone()
        except sqlite3.Error as error:
            raise RegistryUnavailable(
                f"launch registry unavailable: {type(error).__name__}"
            ) from error
        return self._record(row) if row is not None else None

    def list_launches(self) -> list[LaunchRecord]:
        try:
            with self._connect() as connection:
                rows = connection.execute(
                    "SELECT * FROM launch_records WHERE authority_scope=? "
                    "ORDER BY created_at,launch_record_id",
                    (self.authority_scope,),
                ).fetchall()
        except sqlite3.Error as error:
            raise RegistryUnavailable(
                f"launch registry unavailable: {type(error).__name__}"
            ) from error
        return [self._record(row) for row in rows]

    def count_launches(self) -> int:
        return len(self.list_launches())

    def close_thread_connections(self) -> None:
        """Compatibility hook: this registry opens one bounded connection per operation."""

        # Deliberately no shared connection to close.  Keeping this hook makes process-boundary
        # acceptance explicit without exposing SQLite internals.
        assert threading.current_thread() is not None
