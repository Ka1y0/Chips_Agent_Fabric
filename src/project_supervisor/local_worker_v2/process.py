from __future__ import annotations

import hashlib
import json
import os
import platform
import subprocess
import uuid
from collections.abc import Mapping
from contextlib import suppress
from pathlib import Path
from typing import Any


def _hash_identity(value: str) -> str:
    return "sha256:" + hashlib.sha256(value.encode("utf-8")).hexdigest()


def local_host_identity() -> str | None:
    """Return a non-reversible stable host fingerprint, or fail closed when unavailable."""

    candidates: list[str] = []
    if platform.system() == "Windows":
        try:
            import winreg

            with winreg.OpenKey(
                winreg.HKEY_LOCAL_MACHINE,
                r"SOFTWARE\Microsoft\Cryptography",
            ) as key:
                value, _kind = winreg.QueryValueEx(key, "MachineGuid")
                if isinstance(value, str) and value.strip():
                    candidates.append(f"windows-machine-guid:{value.strip()}")
        except (ImportError, OSError):
            pass
    else:
        for path in (Path("/etc/machine-id"), Path("/var/lib/dbus/machine-id")):
            try:
                value = path.read_text(encoding="ascii").strip()
            except (OSError, UnicodeError):
                continue
            if value:
                candidates.append(f"machine-id:{value}")
                break
    if not candidates:
        node = platform.node().strip()
        hardware = uuid.getnode()
        # uuid.getnode() sets the multicast bit when it had to invent a random value.  Such a value
        # is not a restart-stable host identity and therefore cannot bind durable PIDs safely.
        invented = bool(hardware & (1 << 40))
        if node and not invented:
            candidates.append(f"node-hardware:{node}:{hardware:012x}")
    if not candidates:
        return None
    return _hash_identity("local-worker-host:" + candidates[0])


def _lock_byte(descriptor: int, *, acquire: bool) -> bool:
    if os.name == "nt":
        import msvcrt

        os.lseek(descriptor, 0, os.SEEK_SET)
        try:
            msvcrt.locking(
                descriptor,
                msvcrt.LK_NBLCK if acquire else msvcrt.LK_UNLCK,
                1,
            )
        except OSError:
            return False
        return True
    import fcntl

    try:
        fcntl.flock(
            descriptor,
            (fcntl.LOCK_EX | fcntl.LOCK_NB) if acquire else fcntl.LOCK_UN,
        )
    except OSError:
        return False
    return True


def acquire_job_lock(path: Path) -> int:
    """Hold a job-owned lock for the child lifetime; the returned fd must remain open."""

    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    descriptor = os.open(path, os.O_RDWR | os.O_CREAT, 0o600)
    try:
        if os.fstat(descriptor).st_size == 0:
            os.write(descriptor, b"0")
            os.fsync(descriptor)
        if not _lock_byte(descriptor, acquire=True):
            raise RuntimeError("job-owned process lock is already held")
        return descriptor
    except BaseException:
        os.close(descriptor)
        raise


def release_job_lock(descriptor: int) -> None:
    try:
        _lock_byte(descriptor, acquire=False)
    finally:
        os.close(descriptor)


def job_lock_is_held(path: Path) -> bool:
    """Verify a live job-owned nonce lock without trusting PID/birth alone."""

    try:
        descriptor = os.open(path, os.O_RDWR)
    except OSError:
        return False
    try:
        if _lock_byte(descriptor, acquire=True):
            _lock_byte(descriptor, acquire=False)
            return False
        return True
    finally:
        os.close(descriptor)


def _linux_birth(pid: int) -> str | None:
    try:
        boot_id = Path("/proc/sys/kernel/random/boot_id").read_text(encoding="ascii").strip()
        stat = Path(f"/proc/{pid}/stat").read_text(encoding="ascii")
    except (OSError, UnicodeError):
        return None
    end = stat.rfind(")")
    if end < 0:
        return None
    fields_after_command = stat[end + 2 :].split()
    # /proc/<pid>/stat field 3 is index 0 here; process start ticks are field 22.
    if len(fields_after_command) <= 19:
        return None
    return _hash_identity(f"linux:{boot_id}:{pid}:{fields_after_command[19]}")


def _windows_birth(pid: int) -> str | None:
    try:
        import ctypes
        from ctypes import wintypes

        process_query_limited_information = 0x1000
        kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
        kernel32.OpenProcess.argtypes = [wintypes.DWORD, wintypes.BOOL, wintypes.DWORD]
        kernel32.OpenProcess.restype = wintypes.HANDLE
        kernel32.GetProcessTimes.argtypes = [
            wintypes.HANDLE,
            ctypes.POINTER(wintypes.FILETIME),
            ctypes.POINTER(wintypes.FILETIME),
            ctypes.POINTER(wintypes.FILETIME),
            ctypes.POINTER(wintypes.FILETIME),
        ]
        kernel32.GetProcessTimes.restype = wintypes.BOOL
        kernel32.CloseHandle.argtypes = [wintypes.HANDLE]
        handle = kernel32.OpenProcess(process_query_limited_information, False, pid)
        if not handle:
            return None
        try:
            created = wintypes.FILETIME()
            exited = wintypes.FILETIME()
            kernel = wintypes.FILETIME()
            user = wintypes.FILETIME()
            if not kernel32.GetProcessTimes(
                handle,
                ctypes.byref(created),
                ctypes.byref(exited),
                ctypes.byref(kernel),
                ctypes.byref(user),
            ):
                return None
            ticks = (created.dwHighDateTime << 32) | created.dwLowDateTime
            return _hash_identity(f"windows:{pid}:{ticks}")
        finally:
            kernel32.CloseHandle(handle)
    except (AttributeError, ImportError, OSError):
        return None


def _darwin_birth(pid: int) -> str | None:
    """Read macOS process birth time through libproc without shelling out to `ps`."""

    try:
        import ctypes

        class ProcBSDInfo(ctypes.Structure):
            _fields_ = [
                ("pbi_flags", ctypes.c_uint32),
                ("pbi_status", ctypes.c_uint32),
                ("pbi_xstatus", ctypes.c_uint32),
                ("pbi_pid", ctypes.c_uint32),
                ("pbi_ppid", ctypes.c_uint32),
                ("pbi_uid", ctypes.c_uint32),
                ("pbi_gid", ctypes.c_uint32),
                ("pbi_ruid", ctypes.c_uint32),
                ("pbi_rgid", ctypes.c_uint32),
                ("pbi_svuid", ctypes.c_uint32),
                ("pbi_svgid", ctypes.c_uint32),
                ("rfu_1", ctypes.c_uint32),
                ("pbi_comm", ctypes.c_char * 16),
                ("pbi_name", ctypes.c_char * 32),
                ("pbi_nfiles", ctypes.c_uint32),
                ("pbi_pgid", ctypes.c_uint32),
                ("pbi_pjobc", ctypes.c_uint32),
                ("e_tdev", ctypes.c_uint32),
                ("e_tpgid", ctypes.c_uint32),
                ("pbi_nice", ctypes.c_int32),
                ("pbi_start_tvsec", ctypes.c_uint64),
                ("pbi_start_tvusec", ctypes.c_uint64),
            ]

        library = ctypes.CDLL("/usr/lib/libproc.dylib", use_errno=True)
        library.proc_pidinfo.argtypes = [
            ctypes.c_int,
            ctypes.c_int,
            ctypes.c_uint64,
            ctypes.c_void_p,
            ctypes.c_int,
        ]
        library.proc_pidinfo.restype = ctypes.c_int
        info = ProcBSDInfo()
        size = ctypes.sizeof(info)
        observed = library.proc_pidinfo(pid, 3, 0, ctypes.byref(info), size)
        if observed != size or info.pbi_pid != pid or info.pbi_start_tvsec == 0:
            return None
        return _hash_identity(f"darwin:{pid}:{info.pbi_start_tvsec}:{info.pbi_start_tvusec}")
    except (AttributeError, OSError):
        return None


def _posix_ps_birth(pid: int) -> str | None:
    try:
        completed = subprocess.run(
            ["ps", "-o", "lstart=", "-p", str(pid)],
            capture_output=True,
            check=False,
            text=True,
            timeout=2,
        )
    except (OSError, subprocess.SubprocessError):
        return None
    started = completed.stdout.strip()
    if completed.returncode != 0 or not started:
        return None
    return _hash_identity(f"posix:{pid}:{started}")


def process_birth_identity(pid: int) -> str | None:
    """Return an opaque PID-birth fingerprint, never a PID-only identity."""

    if pid <= 0:
        return None
    system = platform.system()
    if system == "Linux":
        return _linux_birth(pid)
    if system == "Windows":
        return _windows_birth(pid)
    if system == "Darwin":
        return _darwin_birth(pid)
    if os.name == "posix":
        return _posix_ps_birth(pid)
    return None


def process_identity_matches(pid: int, expected_birth_identity: str) -> bool:
    observed = process_birth_identity(pid)
    return observed is not None and observed == expected_birth_identity


def atomic_write_json(path: Path, value: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    encoded = json.dumps(
        dict(value), ensure_ascii=False, separators=(",", ":"), sort_keys=True
    ).encode("utf-8")
    temporary = path.with_name(f".{path.name}.{uuid.uuid4()}.tmp")
    descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        with os.fdopen(descriptor, "wb", closefd=True) as stream:
            stream.write(encoded)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
        try:
            directory = os.open(path.parent, os.O_RDONLY)
        except OSError:
            return
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    finally:
        with suppress(FileNotFoundError):
            temporary.unlink()


def read_json_receipt(path: Path, *, maximum_bytes: int = 65_536) -> dict[str, Any] | None:
    try:
        stat = path.stat()
        if not path.is_file() or stat.st_size > maximum_bytes:
            return None
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, ValueError):
        return None
    return value if isinstance(value, dict) else None
