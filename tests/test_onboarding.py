from __future__ import annotations

import tomllib
from pathlib import Path

from bootstrap.onboarding import OnboardingPreset, OnboardingState, build_onboarding_guide


def profile(
    *,
    system: str = "Darwin",
    supervisor: str | None = "/usr/local/bin/project-supervisor",
    ai_cli: str | None = "/usr/local/bin/codex",
    model_server: str | None = "/usr/local/bin/lms",
    node_runtime: str | None = None,
    private_transport: bool = False,
    identity: bool = True,
) -> dict[str, object]:
    return {
        "host": {"operatingSystem": system},
        "resources": {
            "aiCLIs": {"codex": ai_cli},
            "modelServers": {"lms": model_server, "ollama": None},
            "localInferenceApplications": {"lmStudio": None},
        },
        "fabric": {
            "supervisorExecutable": supervisor,
            "nodeRuntimes": {"chips-node": node_runtime},
            "existingStateDatabase": False,
            "existingConfig": False,
            "identityPresent": identity,
        },
        "networkHints": {"privateTransportInstalled": private_transport},
    }


def test_hybrid_workstation_is_ready_without_performing_mutations() -> None:
    guide = build_onboarding_guide(profile())
    protocol = guide.to_protocol()

    assert guide.preset is OnboardingPreset.HYBRID_WORKSTATION
    assert guide.state is OnboardingState.READY
    assert guide.detected_workers == ("codex", "lms")
    assert protocol["mutationsPerformed"] is False
    assert protocol["credentialsInspected"] is False
    assert all(not step.mutating for step in guide.safe_steps)


def test_cluster_node_preset_requires_private_transport_review() -> None:
    guide = build_onboarding_guide(
        profile(node_runtime="/usr/local/bin/chips-node", private_transport=True)
    )

    assert guide.preset is OnboardingPreset.CLUSTER_NODE
    assert any(step.step_id == "verify-private-transport" for step in guide.approval_steps)


def test_missing_supervisor_and_identity_are_explicit_approval_steps() -> None:
    guide = build_onboarding_guide(profile(supervisor=None, identity=False))

    assert guide.state is OnboardingState.NEEDS_REVIEW
    assert {step.step_id for step in guide.approval_steps} >= {
        "install-supervisor",
        "enroll-node-identity",
    }


def test_unsupported_platform_is_blocked() -> None:
    guide = build_onboarding_guide(profile(system="Plan9"))

    assert guide.state is OnboardingState.BLOCKED
    assert guide.safe_steps[2].status == "blocked"


def test_package_exposes_one_command_onboarding_entrypoint() -> None:
    root = Path(__file__).resolve().parents[1]
    config = tomllib.loads((root / "pyproject.toml").read_text(encoding="utf-8"))

    assert config["project"]["scripts"]["chips-onboard"] == "bootstrap.onboarding:main"
