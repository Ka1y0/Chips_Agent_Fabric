import json
import subprocess
from pathlib import Path

import pytest

from bootstrap import check_environment as discovery
from bootstrap import chips as bootstrap

ROOT = Path(__file__).parents[1]


def test_discovery_is_read_only_and_does_not_probe_connectivity(tmp_path: Path) -> None:
    commands: list[tuple[str, ...]] = []

    def which(name: str) -> str | None:
        return f"/portable/bin/{name}" if name in {"python3", "claude", "tailscale"} else None

    def runner(command):  # type: ignore[no-untyped-def]
        commands.append(tuple(command))
        return subprocess.CompletedProcess(command, 0, "Test CPU\n", "")

    profile = discovery.inspect(
        environ={"HOME": str(tmp_path)}, which=which, runner=runner, system="Darwin"
    )
    assert profile["discoveryMode"] == "readOnly"
    assert profile["mutationsPerformed"] is False
    assert profile["authenticationInspected"] is False
    assert profile["credentialsInspected"] is False
    assert profile["networkHints"]["connectivityProbed"] is False
    assert profile["resources"]["aiCLIs"]["claude"] == "/portable/bin/claude"
    assert commands == []


def test_plan_is_deterministic_explainable_and_fail_closed() -> None:
    profile = {
        "host": {"operatingSystem": "Linux"},
        "fabric": {
            "existingStateDatabase": False,
            "supervisorExecutable": None,
            "identityPresent": False,
        },
        "resources": {
            "aiCLIs": {"claude": "/bin/claude", "grok": None},
            "modelServers": {"ollama": None},
        },
    }
    first = bootstrap.build_plan(profile)
    second = bootstrap.build_plan(profile)
    assert first == second
    assert first["failClosed"] is True
    assert first["automaticActions"] == []
    assert {step["id"] for step in first["steps"]} >= {
        "establish-node-identity",
        "configure-private-transport",
        "register-workers",
    }
    assert (
        next(step for step in first["steps"] if step["id"] == "install-supervisor")["status"]
        == "approvalRequired"
    )


def test_emit_bundle_requires_empty_explicit_directory_and_has_safe_config(tmp_path: Path) -> None:
    output = tmp_path / "generated"
    profile = {"host": {"operatingSystem": "Linux", "hostname": "fixture"}}
    plan = {"mode": "dryRun"}
    written = bootstrap.emit_bundle(output, profile, plan)
    assert written == [
        "bootstrap-plan.json",
        "checksums.json",
        "machine-profile.json",
        "supervisor-config.example.json",
    ]
    config = json.loads((output / "supervisor-config.example.json").read_text())
    assert config["host"] == "127.0.0.1"
    assert config["read_only_api"] is True
    assert config["private_transport"] is False
    assert "token" not in json.dumps(config).lower()
    with pytest.raises(ValueError, match="refusing to overwrite"):
        bootstrap.emit_bundle(output, profile, plan)


def test_redacted_profile_removes_host_identity_paths_and_interfaces() -> None:
    profile = {
        "host": {"hostname": "private-host", "operatingSystem": "Linux"},
        "resources": {
            "aiCLIs": {"claude": "/private/bin/claude", "grok": None},
            "localInferenceApplications": {"lmStudio": "/private/LM Studio"},
        },
        "fabric": {
            "stateDirectoryCandidate": "/private/state",
            "supervisorExecutable": "/private/bin/project-supervisor",
            "nodeRuntimes": {"chips-node": "/private/bin/chips-node"},
        },
        "networkHints": {"interfaceNames": ["private0", "private1"]},
    }

    redacted = bootstrap.redact_host_details(profile)

    assert redacted["host"]["hostname"] == "[REDACTED_LOCAL_HOST]"
    assert redacted["resources"]["aiCLIs"]["claude"] == "[DETECTED]"
    assert redacted["resources"]["aiCLIs"]["grok"] is None
    assert redacted["fabric"]["stateDirectoryCandidate"] == "[REDACTED_LOCAL_PATH]"
    assert redacted["networkHints"] == {"interfaceNames": [], "interfaceCount": 2}
    assert "/private" not in json.dumps(redacted)
    assert profile["host"]["hostname"] == "private-host"


def test_linux_gpu_and_state_discovery_are_portable(tmp_path: Path) -> None:
    def which(name: str) -> str | None:
        return f"/usr/bin/{name}" if name == "nvidia-smi" else None

    def runner(command):  # type: ignore[no-untyped-def]
        assert command[0] == "/usr/bin/nvidia-smi"
        return subprocess.CompletedProcess(command, 0, "Fixture GPU, 16384\n", "")

    profile = discovery.inspect(
        environ={"HOME": str(tmp_path), "XDG_STATE_HOME": str(tmp_path / "state")},
        which=which,
        runner=runner,
        system="Linux",
    )
    assert profile["host"]["gpus"] == [
        {"name": "Fixture GPU", "vramMiB": 16384, "evidence": "nvidia-smi"}
    ]
    assert profile["fabric"]["stateDirectoryCandidate"] == str(
        tmp_path / "state" / "project-supervisor"
    )


def test_windows_discovery_uses_local_app_data_without_credentials(tmp_path: Path) -> None:
    profile = discovery.inspect(
        environ={
            "USERPROFILE": str(tmp_path),
            "LOCALAPPDATA": str(tmp_path / "Local"),
        },
        which=lambda _name: None,
        system="Windows",
    )
    assert profile["fabric"]["stateDirectoryCandidate"] == str(
        tmp_path / "Local" / "Project_Supervisor"
    )
    assert profile["credentialsInspected"] is False
    assert profile["resources"]["privateTransports"] == {"tailscale": None, "wg": None}


def test_windows_optional_lm_studio_permission_error_is_unavailable(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    original_exists = Path.exists

    def protected_exists(path: Path) -> bool:
        if path.name == "LM Studio.exe":
            raise PermissionError("fixture protected directory")
        return original_exists(path)

    monkeypatch.setattr(Path, "exists", protected_exists)
    profile = discovery.inspect(
        environ={
            "USERPROFILE": str(tmp_path),
            "LOCALAPPDATA": str(tmp_path / "Local"),
        },
        which=lambda _name: None,
        system="Windows",
    )

    assert profile["resources"]["localInferenceApplications"] == {"lmStudio": None}
    assert profile["mutationsPerformed"] is False


def test_discovery_never_executes_path_shadowed_probe(tmp_path: Path) -> None:
    commands: list[tuple[str, ...]] = []

    def runner(command):  # type: ignore[no-untyped-def]
        commands.append(tuple(command))
        raise AssertionError("untrusted PATH command must not execute")

    profile = discovery.inspect(
        environ={"HOME": str(tmp_path)},
        which=lambda name: f"/tmp/untrusted/{name}",
        runner=runner,
        system="Linux",
    )

    assert commands == []
    assert profile["host"]["gpus"] == []


def test_emit_refuses_symbolic_link(tmp_path: Path) -> None:
    target = tmp_path / "target"
    target.mkdir()
    link = tmp_path / "link"
    link.symlink_to(target, target_is_directory=True)
    with pytest.raises(ValueError, match="symbolic link"):
        bootstrap.emit_bundle(link, {"profile": True}, {"plan": True})


def test_cli_defaults_to_dry_run_without_writing(monkeypatch, capsys, tmp_path: Path) -> None:  # type: ignore[no-untyped-def]
    monkeypatch.setattr(
        bootstrap,
        "inspect",
        lambda: {
            "host": {"operatingSystem": "Linux"},
            "fabric": {
                "existingStateDatabase": False,
                "supervisorExecutable": "/bin/project-supervisor",
                "identityPresent": False,
            },
            "resources": {"aiCLIs": {}, "modelServers": {}},
        },
    )
    assert bootstrap.main(["bootstrap", "--json"]) == 0
    output = json.loads(capsys.readouterr().out)
    assert output["dryRun"] is True
    assert output["filesWritten"] == []
    assert list(tmp_path.iterdir()) == []


def test_cli_refuses_partial_emit_arguments(tmp_path: Path) -> None:
    with pytest.raises(SystemExit) as raised:
        bootstrap.main(["bootstrap", "--output-dir", str(tmp_path / "generated")])
    assert raised.value.code == 2


def test_machine_document_hierarchy_is_present_and_labels_foundation() -> None:
    required = [
        "AGENTS.md",
        "FABRIC_INTENT.md",
        "docs/ARCHITECTURE.md",
        "docs/BOOTSTRAP.md",
        "docs/BOOTSTRAP_PROTOCOL.md",
        "docs/WORKER_PROTOCOL.md",
        "docs/NODE_PROTOCOL.md",
        "docs/CAPABILITY_MODEL.md",
        "docs/TRUST_MODEL.md",
        "docs/SCHEDULER.md",
        "SECURITY.md",
        "docs/RECOVERY.md",
        "docs/LLM_OPERATIONS.md",
        "docs/TROUBLESHOOTING.md",
        "docs/UNKNOWN_LLM_ACCEPTANCE.md",
    ]
    for relative in required:
        content = (ROOT / relative).read_text(encoding="utf-8")
        assert content.startswith("# "), relative
        assert content.strip(), relative
    agents = (ROOT / "AGENTS.md").read_text(encoding="utf-8")
    assert "python3 bootstrap/chips.py bootstrap --json" in agents
    assert "FOUNDATION" in agents
    assert "Cyber Office" in agents
    assert "Supervisor UI" in agents
