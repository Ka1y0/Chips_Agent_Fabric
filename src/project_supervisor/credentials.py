"""Read-only retrieval of operator-managed worker credentials.

A value returned by this module is a live secret. It may be passed to an adapter
constructor and held in process memory only. Never print it, write it to
configuration, evidence, manifests, logs, or a URL, and never place it in a
subprocess argument vector where another local user could observe it.
"""

from __future__ import annotations

import os
import subprocess
import sys
from collections.abc import Callable, Mapping, Sequence

ENV_LOCAL_WORKER_TOKEN = "PROJECT_SUPERVISOR_LOCAL_WORKER_TOKEN"
ENV_RUNTIME_BROKER_TOKEN = "PROJECT_SUPERVISOR_RUNTIME_BROKER_TOKEN"
KEYCHAIN_SERVICE = "project-supervisor-local-worker"
RUNTIME_BROKER_KEYCHAIN_SERVICE = "project-supervisor-runtime-broker"

type CommandRunner = Callable[[Sequence[str]], subprocess.CompletedProcess[str]]


class CredentialError(RuntimeError):
    """A required credential is not available from an approved secure store."""


def _from_environment(environ: Mapping[str, str]) -> str | None:
    value = environ.get(ENV_LOCAL_WORKER_TOKEN)
    return value if value and value.strip() else None


def _from_keychain(service: str, account: str, runner: CommandRunner) -> str | None:
    """Read one generic password from the macOS login Keychain.

    ``security`` writes the secret to stdout, so the value is captured and kept
    in memory. Neither the value nor the raw stderr is propagated on failure.
    """

    if sys.platform != "darwin":
        return None
    command = ("security", "find-generic-password", "-w", "-s", service, "-a", account)
    try:
        completed = runner(command)
    except (OSError, subprocess.SubprocessError) as error:
        raise CredentialError(f"could not query the Keychain: {type(error).__name__}") from None
    if completed.returncode != 0:
        return None
    value = (completed.stdout or "").strip()
    return value or None


def _default_runner(command: Sequence[str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        list(command),
        capture_output=True,
        text=True,
        check=False,
        timeout=15,
    )


def local_worker_token(
    *,
    account: str,
    service: str = KEYCHAIN_SERVICE,
    environ: Mapping[str, str] | None = None,
    runner: CommandRunner | None = None,
) -> str:
    """Return the Local Worker bearer from a short-lived env var or the Keychain.

    The environment is checked first so an operator can inject a token for one
    process without storing it. Raises :class:`CredentialError` rather than
    returning an empty credential, so callers fail closed.
    """

    environment = environ if environ is not None else os.environ
    value = _from_environment(environment) or _from_keychain(
        service, account, runner or _default_runner
    )
    if not value:
        raise CredentialError(
            f"no Local Worker bearer available: set {ENV_LOCAL_WORKER_TOKEN} for this process "
            f"or add Keychain item service={service!r} account={account!r}"
        )
    return value


def runtime_broker_token(
    *,
    account: str,
    service: str = RUNTIME_BROKER_KEYCHAIN_SERVICE,
    environ: Mapping[str, str] | None = None,
    runner: CommandRunner | None = None,
) -> str:
    """Return one admitted runtime-broker bearer from process memory or macOS Keychain.

    The short-lived environment override and persisted Keychain service are distinct from Local
    Worker credentials.  Neither value enters Supervisor JSON configuration or SQLite.
    """

    environment = environ if environ is not None else os.environ
    injected = environment.get(ENV_RUNTIME_BROKER_TOKEN)
    value = (injected if injected and injected.strip() else None) or _from_keychain(
        service, account, runner or _default_runner
    )
    if not value:
        raise CredentialError(
            "no runtime broker bearer available from the approved process environment "
            f"or Keychain service={service!r} account={account!r}"
        )
    return value
