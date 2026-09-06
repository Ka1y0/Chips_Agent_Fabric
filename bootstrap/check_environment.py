#!/usr/bin/env python3
"""Portable, read-only CHIPS Agent Fabric machine discovery.

This module deliberately does not authenticate, contact model servers, inspect
credential stores, install software, open sockets, or request privileges.
"""

from __future__ import annotations

import argparse
import json
import os
import platform
import shutil
import socket
import subprocess
from collections.abc import Callable, Mapping, Sequence
from pathlib import Path
from typing import Any

CommandRunner = Callable[[Sequence[str]], subprocess.CompletedProcess[str]]

AI_CLIS = ("claude", "grok", "agy", "codex")
RUNTIMES = ("python3", "node", "docker", "podman", "git")
MODEL_RUNTIMES = ("lms", "lmstudio", "ollama", "llama-server")
PRIVATE_TRANSPORTS = ("tailscale", "wg")
NODE_RUNTIMES = ("chips-node", "project-supervisor-worker")
_TRUSTED_POSIX_COMMAND_DIRS = frozenset({"/bin", "/sbin", "/usr/bin", "/usr/sbin"})


def _run_read_only(command: Sequence[str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(  # noqa: S603 - fixed read-only commands, no shell
        list(command),
        check=False,
        capture_output=True,
        text=True,
        timeout=5,
    )


def _safe_run(runner: CommandRunner, command: Sequence[str]) -> str | None:
    try:
        completed = runner(command)
    except (OSError, subprocess.SubprocessError):
        return None
    if completed.returncode != 0:
        return None
    value = completed.stdout.strip()
    return value or None


def _trusted_system_command(
    system: str,
    name: str,
    which: Callable[[str], str | None],
) -> str | None:
    """Resolve an OS probe only from a trusted system-owned command location."""

    candidate = which(name)
    if not candidate:
        return None
    path = Path(candidate)
    if not path.is_absolute():
        return None
    if system in {"Darwin", "Linux"}:
        return str(path) if str(path.parent) in _TRUSTED_POSIX_COMMAND_DIRS else None
    if system == "Windows":
        normalized = str(path).replace("\\", "/").lower()
        if "/windows/system32/" in normalized:
            return str(path)
    return None


def _memory_bytes(system: str) -> int | None:
    if system == "Windows":
        try:
            import ctypes

            class MemoryStatus(ctypes.Structure):
                _fields_ = [
                    ("length", ctypes.c_ulong),
                    ("memory_load", ctypes.c_ulong),
                    ("total_physical", ctypes.c_ulonglong),
                    ("available_physical", ctypes.c_ulonglong),
                    ("total_page_file", ctypes.c_ulonglong),
                    ("available_page_file", ctypes.c_ulonglong),
                    ("total_virtual", ctypes.c_ulonglong),
                    ("available_virtual", ctypes.c_ulonglong),
                    ("available_extended_virtual", ctypes.c_ulonglong),
                ]

            status = MemoryStatus()
            status.length = ctypes.sizeof(status)
            if ctypes.windll.kernel32.GlobalMemoryStatusEx(ctypes.byref(status)):
                return int(status.total_physical)
        except (AttributeError, OSError, ValueError):
            return None
        return None
    try:
        page_size = os.sysconf("SC_PAGE_SIZE")
        pages = os.sysconf("SC_PHYS_PAGES")
    except (AttributeError, OSError, ValueError):
        return None
    if not isinstance(page_size, int) or not isinstance(pages, int):
        return None
    return page_size * pages


def _gpu_inventory(
    system: str, which: Callable[[str], str | None], runner: CommandRunner
) -> list[dict[str, Any]]:
    system_profiler = _trusted_system_command(system, "system_profiler", which)
    if system == "Darwin" and system_profiler:
        raw = _safe_run(runner, (system_profiler, "SPDisplaysDataType", "-json"))
        if raw:
            try:
                payload = json.loads(raw)
                return [
                    {
                        "name": item.get("sppci_model") or item.get("_name") or "unknown",
                        "vram": item.get("spdisplays_vram")
                        or item.get("spdisplays_vram_shared")
                        or "unavailable",
                        "evidence": "system_profiler",
                    }
                    for item in payload.get("SPDisplaysDataType", [])
                ]
            except (json.JSONDecodeError, AttributeError):
                pass
    nvidia_smi = _trusted_system_command(system, "nvidia-smi", which)
    if nvidia_smi:
        raw = _safe_run(
            runner,
            (
                nvidia_smi,
                "--query-gpu=name,memory.total",
                "--format=csv,noheader,nounits",
            ),
        )
        if raw:
            result = []
            for line in raw.splitlines():
                name, separator, memory = line.rpartition(",")
                result.append(
                    {
                        "name": name.strip() if separator else line.strip(),
                        "vramMiB": int(memory.strip())
                        if separator and memory.strip().isdigit()
                        else None,
                        "evidence": "nvidia-smi",
                    }
                )
            return result
    powershell = _trusted_system_command(system, "powershell", which)
    if system == "Windows" and powershell:
        raw = _safe_run(
            runner,
            (
                powershell,
                "-NoProfile",
                "-NonInteractive",
                "-Command",
                "Get-CimInstance Win32_VideoController | "
                "Select-Object Name,AdapterRAM | ConvertTo-Json -Compress",
            ),
        )
        if raw:
            try:
                items = json.loads(raw)
                if isinstance(items, dict):
                    items = [items]
                return [
                    {
                        "name": item.get("Name", "unknown"),
                        "vramBytes": item.get("AdapterRAM"),
                        "evidence": "Win32_VideoController",
                    }
                    for item in items
                ]
            except (json.JSONDecodeError, TypeError, AttributeError):
                pass
    return []


def _default_state_dir(system: str, environ: Mapping[str, str]) -> Path | None:
    explicit = environ.get("PROJECT_SUPERVISOR_DATA_DIR")
    if explicit:
        return Path(explicit).expanduser()
    home = environ.get("HOME") or environ.get("USERPROFILE")
    if not home:
        return None
    if system == "Darwin":
        return Path(home) / "Library" / "Application Support" / "Project_Supervisor"
    if system == "Windows":
        local = environ.get("LOCALAPPDATA")
        return Path(local) / "Project_Supervisor" if local else None
    return (
        Path(environ.get("XDG_STATE_HOME", Path(home) / ".local" / "state")) / "project-supervisor"
    )


def _interface_names() -> list[str]:
    try:
        return sorted(name for _index, name in socket.if_nameindex())
    except OSError:
        return []


def _lm_studio_application(system: str, environ: Mapping[str, str]) -> str | None:
    home = environ.get("HOME") or environ.get("USERPROFILE")
    candidates: list[Path] = []
    if system == "Darwin":
        candidates.append(Path("/Applications/LM Studio.app"))
        if home:
            candidates.append(Path(home) / "Applications" / "LM Studio.app")
    elif system == "Windows" and environ.get("LOCALAPPDATA"):
        candidates.append(
            Path(environ["LOCALAPPDATA"]) / "Programs" / "LM Studio" / "LM Studio.exe"
        )
    for path in candidates:
        try:
            if path.exists():
                return str(path)
        except OSError:
            # Optional application discovery is deliberately best-effort.  A protected parent
            # directory on Windows must not make the whole credential-free bootstrap fail.
            continue
    return None


def inspect(
    *,
    environ: Mapping[str, str] | None = None,
    which: Callable[[str], str | None] = shutil.which,
    runner: CommandRunner = _run_read_only,
    system: str | None = None,
) -> dict[str, object]:
    """Return a credential-free machine profile without changing host state."""

    env = os.environ if environ is None else environ
    os_name = system or platform.system()
    commands = sorted(
        set(
            AI_CLIS
            + RUNTIMES
            + MODEL_RUNTIMES
            + PRIVATE_TRANSPORTS
            + NODE_RUNTIMES
            + ("project-supervisor",)
        )
    )
    executables = {command: which(command) for command in commands}
    state_dir = _default_state_dir(os_name, env)
    cpu_name = platform.processor().strip() or platform.machine()
    sysctl = _trusted_system_command(os_name, "sysctl", which)
    if os_name == "Darwin" and sysctl:
        cpu_name = _safe_run(runner, (sysctl, "-n", "machdep.cpu.brand_string")) or cpu_name
    return {
        "schemaVersion": 2,
        "discoveryMode": "readOnly",
        "host": {
            "operatingSystem": os_name,
            "release": platform.release(),
            "architecture": platform.machine(),
            "hostname": platform.node(),
            "cpu": {"logicalCount": os.cpu_count(), "name": cpu_name},
            "ramBytes": _memory_bytes(os_name),
            "gpus": _gpu_inventory(os_name, which, runner),
        },
        "resources": {
            "runtimes": {name: executables[name] for name in RUNTIMES},
            "modelServers": {name: executables[name] for name in MODEL_RUNTIMES},
            "localInferenceApplications": {"lmStudio": _lm_studio_application(os_name, env)},
            "aiCLIs": {name: executables[name] for name in AI_CLIS},
            "privateTransports": {name: executables[name] for name in PRIVATE_TRANSPORTS},
        },
        "fabric": {
            "supervisorExecutable": executables["project-supervisor"],
            "nodeRuntimes": {name: executables[name] for name in NODE_RUNTIMES},
            "stateDirectoryCandidate": str(state_dir) if state_dir else None,
            "existingConfig": bool(state_dir and (state_dir / "config.json").is_file()),
            "existingStateDatabase": bool(state_dir and (state_dir / "supervisor.db").is_file()),
            "identityPresent": bool(
                state_dir
                and any(
                    (state_dir / name).is_file()
                    for name in ("node-identity.json", "fabric-identity.json")
                )
            ),
        },
        "networkHints": {
            "loopbackOnlyDefault": True,
            "privateTransportInstalled": any(executables[name] for name in PRIVATE_TRANSPORTS),
            "connectivityProbed": False,
            "listenersInspected": False,
            "interfaceNames": _interface_names(),
        },
        "mutationsPerformed": False,
        "authenticationInspected": False,
        "credentialsInspected": False,
    }


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--json", action="store_true", help="emit JSON")
    args = parser.parse_args(argv)
    result = inspect()
    if args.json:
        print(json.dumps(result, indent=2, sort_keys=True))
    else:
        host = result["host"]
        assert isinstance(host, dict)
        print(f"{host['operatingSystem']} {host['release']} ({host['architecture']})")
        resources = result["resources"]
        assert isinstance(resources, dict)
        for category, entries in resources.items():
            print(f"{category}:")
            assert isinstance(entries, dict)
            for name, path in entries.items():
                print(f"  {name}: {path or 'NOT_INSTALLED'}")
        print(
            "No changes made; connectivity, listeners, authentication, "
            "and credentials were not inspected."
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
