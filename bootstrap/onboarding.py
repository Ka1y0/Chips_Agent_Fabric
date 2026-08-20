#!/usr/bin/env python3
"""One-command, read-only onboarding guide for CHIPS Agent Fabric.

The guide turns the existing machine discovery snapshot into a concise first-run
path. It never installs packages, reads credentials, opens sockets, or starts
services. Mutating work remains explicit and approval-gated.
"""

from __future__ import annotations

import argparse
import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from enum import StrEnum
from typing import Any


def _inspect_machine() -> Mapping[str, Any]:
    try:
        from .check_environment import inspect
    except ImportError:  # Direct ``python bootstrap/onboarding.py`` execution.
        from check_environment import inspect
    return inspect()


class OnboardingPreset(StrEnum):
    MINIMAL = "minimal"
    LOCAL_ONLY = "localOnly"
    CLOUD_ONLY = "cloudOnly"
    HYBRID_WORKSTATION = "hybridWorkstation"
    CLUSTER_NODE = "clusterNode"


class OnboardingState(StrEnum):
    READY = "ready"
    NEEDS_REVIEW = "needsReview"
    BLOCKED = "blocked"


@dataclass(frozen=True, slots=True)
class OnboardingStep:
    step_id: str
    title: str
    status: str
    reason: str
    mutating: bool = False
    approval_required: bool = False

    def to_protocol(self) -> dict[str, object]:
        return {
            "stepID": self.step_id,
            "title": self.title,
            "status": self.status,
            "reason": self.reason,
            "mutating": self.mutating,
            "approvalRequired": self.approval_required,
        }


@dataclass(frozen=True, slots=True)
class OnboardingGuide:
    preset: OnboardingPreset
    state: OnboardingState
    summary: str
    detected_workers: tuple[str, ...]
    local_model_runtimes: tuple[str, ...]
    safe_steps: tuple[OnboardingStep, ...]
    approval_steps: tuple[OnboardingStep, ...]
    next_commands: tuple[str, ...]

    def to_protocol(self) -> dict[str, object]:
        return {
            "schemaVersion": 1,
            "preset": self.preset.value,
            "state": self.state.value,
            "summary": self.summary,
            "detectedWorkers": list(self.detected_workers),
            "localModelRuntimes": list(self.local_model_runtimes),
            "safeSteps": [step.to_protocol() for step in self.safe_steps],
            "approvalSteps": [step.to_protocol() for step in self.approval_steps],
            "nextCommands": list(self.next_commands),
            "mutationsPerformed": False,
            "credentialsInspected": False,
            "connectivityProbed": False,
        }


def _mapping(value: object) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _detected_names(value: object) -> tuple[str, ...]:
    mapping = _mapping(value)
    return tuple(sorted(str(name) for name, path in mapping.items() if path))


def build_onboarding_guide(profile: Mapping[str, Any]) -> OnboardingGuide:
    """Build a deterministic, non-mutating onboarding path from discovery data."""

    host = _mapping(profile.get("host"))
    resources = _mapping(profile.get("resources"))
    fabric = _mapping(profile.get("fabric"))
    network = _mapping(profile.get("networkHints"))

    supported = host.get("operatingSystem") in {"Darwin", "Windows", "Linux"}
    ai_clis = _detected_names(resources.get("aiCLIs"))
    model_servers = _detected_names(resources.get("modelServers"))
    local_apps = _detected_names(resources.get("localInferenceApplications"))
    node_runtimes = _detected_names(fabric.get("nodeRuntimes"))
    local_models = tuple(sorted(set((*model_servers, *local_apps))))
    worker_candidates = tuple(sorted(set((*ai_clis, *local_models, *node_runtimes))))

    private_transport = bool(network.get("privateTransportInstalled"))
    if private_transport and node_runtimes:
        preset = OnboardingPreset.CLUSTER_NODE
    elif ai_clis and local_models:
        preset = OnboardingPreset.HYBRID_WORKSTATION
    elif local_models:
        preset = OnboardingPreset.LOCAL_ONLY
    elif ai_clis:
        preset = OnboardingPreset.CLOUD_ONLY
    else:
        preset = OnboardingPreset.MINIMAL

    supervisor_present = bool(fabric.get("supervisorExecutable"))
    if not supported:
        state = OnboardingState.BLOCKED
        summary = "This operating system is outside the supported onboarding foundation."
    elif supervisor_present and worker_candidates:
        state = OnboardingState.READY
        summary = "The machine is ready for a reviewed loopback-first Fabric initialization."
    else:
        state = OnboardingState.NEEDS_REVIEW
        summary = "The machine can be prepared, but one or more explicit setup steps remain."

    safe_steps = (
        OnboardingStep(
            "inspect-machine",
            "Inspect machine capabilities",
            "complete",
            "Discovery was read-only and did not inspect authentication or credentials.",
        ),
        OnboardingStep(
            "preserve-existing-state",
            "Preserve existing Fabric state",
            "review"
            if fabric.get("existingStateDatabase") or fabric.get("existingConfig")
            else "notNeeded",
            "Existing state is never overwritten by onboarding."
            if fabric.get("existingStateDatabase") or fabric.get("existingConfig")
            else "No existing state or configuration was discovered.",
        ),
        OnboardingStep(
            "generate-loopback-config",
            "Generate a loopback-only starter configuration",
            "ready" if supported else "blocked",
            "The starter profile keeps administrative and model endpoints off the public network.",
        ),
        OnboardingStep(
            "catalog-worker-candidates",
            "Build a candidate Worker catalog",
            "ready" if worker_candidates else "waiting",
            "Candidates are discovery hints and require separate health and "
            "capability verification."
            if worker_candidates
            else "No AI CLI, local model runtime, or node runtime was discovered.",
        ),
    )

    approval_steps: list[OnboardingStep] = []
    if not supervisor_present:
        approval_steps.append(
            OnboardingStep(
                "install-supervisor",
                "Install Project Supervisor",
                "approvalRequired",
                "Package installation is mutating and remains outside the read-only guide.",
                mutating=True,
                approval_required=True,
            )
        )
    if ai_clis:
        approval_steps.append(
            OnboardingStep(
                "verify-provider-sessions",
                "Verify provider sessions",
                "approvalRequired",
                "Provider login, plan, and quota must be verified without copying credentials.",
                approval_required=True,
            )
        )
    if local_models:
        approval_steps.append(
            OnboardingStep(
                "verify-local-model-endpoints",
                "Verify local model endpoints",
                "approvalRequired",
                "Detected applications are candidates until a loopback health and "
                "model probe passes.",
                approval_required=True,
            )
        )
    if not fabric.get("identityPresent"):
        approval_steps.append(
            OnboardingStep(
                "enroll-node-identity",
                "Enroll a durable node identity",
                "approvalRequired",
                "Identity enrollment changes canonical trust state and must be attributable.",
                mutating=True,
                approval_required=True,
            )
        )
    if preset is OnboardingPreset.CLUSTER_NODE:
        approval_steps.append(
            OnboardingStep(
                "verify-private-transport",
                "Verify private cluster transport",
                "approvalRequired",
                "Installation alone is not proof of authenticated encrypted reachability.",
                approval_required=True,
            )
        )

    return OnboardingGuide(
        preset=preset,
        state=state,
        summary=summary,
        detected_workers=worker_candidates,
        local_model_runtimes=local_models,
        safe_steps=safe_steps,
        approval_steps=tuple(approval_steps),
        next_commands=(
            "chips bootstrap --json",
            "chips bootstrap --emit --output-dir ./fabric-onboarding",
            "project-supervisor init",
        ),
    )


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="chips-onboard")
    parser.add_argument("--json", action="store_true", help="emit machine-readable output")
    args = parser.parse_args(argv)
    guide = build_onboarding_guide(_inspect_machine())
    if args.json:
        print(json.dumps(guide.to_protocol(), indent=2, sort_keys=True))
    else:
        print("CHIPS Agent Fabric onboarding")
        print(f"Preset: {guide.preset.value}")
        print(f"State: {guide.state.value}")
        print(guide.summary)
        if guide.detected_workers:
            print("Detected Worker candidates: " + ", ".join(guide.detected_workers))
        print(
            "No installation, login, network probe, service start, or credential read "
            "was performed."
        )
    return 3 if guide.state is OnboardingState.BLOCKED else 0


if __name__ == "__main__":
    raise SystemExit(main())
