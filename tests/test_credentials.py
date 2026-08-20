from __future__ import annotations

import subprocess
from collections.abc import Sequence

import pytest

import project_supervisor.credentials as credentials_module
from project_supervisor.credentials import (
    ENV_LOCAL_WORKER_TOKEN,
    KEYCHAIN_SERVICE,
    CredentialError,
    local_worker_token,
)

SECRET = "test-only-placeholder-bearer"


def completed(
    command: Sequence[str], *, returncode: int = 0, stdout: str = ""
) -> subprocess.CompletedProcess[str]:
    return subprocess.CompletedProcess(list(command), returncode, stdout, "")


def test_short_lived_environment_token_is_preferred_and_never_shells_out() -> None:
    def runner(command: Sequence[str]) -> subprocess.CompletedProcess[str]:
        raise AssertionError(f"Keychain must not be queried: {command}")

    token = local_worker_token(
        account="worker-node-01",
        environ={ENV_LOCAL_WORKER_TOKEN: SECRET},
        runner=runner,
    )
    assert token == SECRET


def test_keychain_lookup_uses_a_fixed_argument_vector_without_the_secret(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(credentials_module.sys, "platform", "darwin")
    seen: list[Sequence[str]] = []

    def runner(command: Sequence[str]) -> subprocess.CompletedProcess[str]:
        seen.append(command)
        return completed(command, stdout=f"{SECRET}\n")

    token = local_worker_token(account="worker-node-01", environ={}, runner=runner)

    assert token == SECRET
    assert list(seen[0]) == [
        "security",
        "find-generic-password",
        "-w",
        "-s",
        KEYCHAIN_SERVICE,
        "-a",
        "worker-node-01",
    ]
    assert SECRET not in " ".join(seen[0])


@pytest.mark.parametrize(
    "outcome",
    [
        {"returncode": 44, "stdout": ""},
        {"returncode": 0, "stdout": "   \n"},
    ],
)
def test_missing_or_blank_keychain_credential_fails_closed(
    outcome: dict[str, object],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(credentials_module.sys, "platform", "darwin")

    def runner(command: Sequence[str]) -> subprocess.CompletedProcess[str]:
        return completed(command, **outcome)  # type: ignore[arg-type]

    with pytest.raises(CredentialError, match="no Local Worker bearer available"):
        local_worker_token(account="worker-node-01", environ={}, runner=runner)


def test_blank_environment_token_falls_back_to_the_keychain(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(credentials_module.sys, "platform", "darwin")

    def runner(command: Sequence[str]) -> subprocess.CompletedProcess[str]:
        return completed(command, stdout=SECRET)

    token = local_worker_token(
        account="worker-node-01",
        environ={ENV_LOCAL_WORKER_TOKEN: "   "},
        runner=runner,
    )
    assert token == SECRET


def test_keychain_failure_is_reported_without_leaking_process_output(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(credentials_module.sys, "platform", "darwin")

    def runner(command: Sequence[str]) -> subprocess.CompletedProcess[str]:
        raise subprocess.TimeoutExpired(list(command), 15, output=SECRET)

    with pytest.raises(CredentialError) as error:
        local_worker_token(account="worker-node-01", environ={}, runner=runner)

    assert SECRET not in str(error.value)
    assert error.value.__cause__ is None


def test_non_macos_hosts_fail_closed_without_invoking_keychain(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(credentials_module.sys, "platform", "linux")

    def runner(command: Sequence[str]) -> subprocess.CompletedProcess[str]:
        raise AssertionError(f"non-macOS host must not invoke Keychain: {command}")

    with pytest.raises(CredentialError, match="no Local Worker bearer available"):
        local_worker_token(account="worker-node-01", environ={}, runner=runner)
