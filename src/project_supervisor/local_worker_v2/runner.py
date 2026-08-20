from __future__ import annotations

import argparse
import os
import signal
import time
from datetime import UTC, datetime
from pathlib import Path

from .model import PROTOCOL_VERSION, LaunchState
from .process import (
    acquire_job_lock,
    atomic_write_json,
    process_birth_identity,
    read_json_receipt,
    release_job_lock,
)


def timestamp() -> str:
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")


def _append_invocation(path: Path | None, job_id: str) -> None:
    if path is None:
        return
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o600)
    try:
        os.write(descriptor, f"{job_id}\n".encode())
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def run(args: argparse.Namespace) -> int:
    job_dir = Path(args.job_dir)
    launch = read_json_receipt(job_dir / "launch.json")
    if launch is None:
        return 70
    required = {
        "job_id",
        "idempotency_key",
        "launch_nonce",
        "request_digest",
        "process_host_identity",
    }
    if not required.issubset(launch):
        return 70
    pid = os.getpid()
    birth = process_birth_identity(pid)
    if birth is None:
        return 70
    lock_path = job_dir / f"alive-{launch['launch_nonce']}.lock"
    try:
        lock_descriptor = acquire_job_lock(lock_path)
    except (OSError, RuntimeError):
        return 70

    base = {
        "protocol_version": PROTOCOL_VERSION,
        "job_id": str(launch["job_id"]),
        "idempotency_key": str(launch["idempotency_key"]),
        "launch_nonce": str(launch["launch_nonce"]),
        "request_digest": str(launch["request_digest"]),
        "process_host_identity": str(launch["process_host_identity"]),
        "pid": pid,
        "process_birth_identity": birth,
    }
    try:
        atomic_write_json(job_dir / "started.json", {**base, "started_at": timestamp()})
        _append_invocation(
            Path(args.invocation_log) if args.invocation_log else None, base["job_id"]
        )

        cancelled = False

        def cancel(_signal: int, _frame: object) -> None:
            nonlocal cancelled
            cancelled = True

        signal.signal(signal.SIGTERM, cancel)
        if hasattr(signal, "SIGINT"):
            signal.signal(signal.SIGINT, cancel)

        deadline = time.monotonic() + max(0.0, args.duration_seconds)
        release_file = Path(args.release_file) if args.release_file else None
        release_deadline = time.monotonic() + max(0.1, args.release_timeout_seconds)
        while not cancelled:
            if release_file is not None:
                if release_file.is_file():
                    break
                if time.monotonic() >= release_deadline:
                    state = LaunchState.FAILED
                    result = {
                        "state": "FAILED",
                        "error": "deterministic child release deadline expired",
                    }
                    exit_code = 1
                    break
            elif time.monotonic() >= deadline:
                break
            time.sleep(0.02)
        else:
            state = LaunchState.CANCELLED
            result = {"state": "CANCELLED", "error": "local worker job was cancelled"}
            exit_code = 143

        if not cancelled and "state" not in locals():
            state = LaunchState.COMPLETED
            result = {
                "state": "SUCCEEDED",
                "selected_model": "deterministic-local-worker",
                "result": {
                    "content": args.result,
                    "model": "deterministic-local-worker",
                },
                "metrics": {"total_tokens": 0},
            }
            exit_code = 0
        atomic_write_json(
            job_dir / "terminal.json",
            {
                **base,
                "launch_state": state.value,
                "exit_code": exit_code,
                "terminal_at": timestamp(),
                "result": result,
            },
        )
        return exit_code
    finally:
        release_job_lock(lock_descriptor)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="fixed deterministic Local Worker test child")
    parser.add_argument("--job-dir", required=True)
    parser.add_argument("--duration-seconds", type=float, default=0.1)
    parser.add_argument("--result", default="LOCAL_WORKER_V2_OK")
    parser.add_argument("--release-file")
    parser.add_argument("--release-timeout-seconds", type=float, default=60.0)
    parser.add_argument("--invocation-log")
    return parser


def main() -> int:
    return run(build_parser().parse_args())


if __name__ == "__main__":
    raise SystemExit(main())
