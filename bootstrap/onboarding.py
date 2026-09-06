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


def _bootstrap_plan(profile: Mapping[str, Any]) -> Mapping[str, Any]:
    try:
        from .chips import build_plan
    except ImportError:  # Direct ``python bootstrap/onboarding.py`` execution.
        from chips import build_plan
    return build_plan(dict(profile))


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
    bootstrap_plan_schema_version: int
    bootstrap_plan_milestone: str

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
            "bootstrapPlanSchemaVersion": self.bootstrap_plan_schema_version,
            "bootstrapPlanMilestone": self.bootstrap_plan_milestone,
            "automaticActions": [],
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
    """Project the canonical dry-run bootstrap plan into a concise onboarding guide."""

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

    plan = _bootstrap_plan(profile)
    raw_steps = plan.get("steps")
    if not isinstance(raw_steps, list):
        raise ValueError("canonical bootstrap plan did not provide ordered steps")

    projected: list[OnboardingStep] = []
    for raw in raw_steps:
        if not isinstance(raw, Mapping):
            raise ValueError("canonical bootstrap plan contains an invalid step")
        mutating = raw.get("mutating") is True
        status = str(raw.get("status", "blocked"))
        projected.append(
            OnboardingStep(
                step_id=str(raw.get("id", "")),
                title=str(raw.get("action", "")).replace(".", " ").title(),
                status=status,
                reason=str(raw.get("reason", "")),
                mutating=mutating,
                approval_required=mutating and status != "notNeeded",
            )
        )
    safe_steps = tuple(step for step in projected if not step.mutating)
    approval_steps = tuple(
        step for step in projected if step.mutating and step.status != "notNeeded"
    )

    return OnboardingGuide(
        preset=preset,
        state=state,
        summary=summary,
        detected_workers=worker_candidates,
        local_model_runtimes=local_models,
        safe_steps=safe_steps,
        approval_steps=approval_steps,
        next_commands=("chips bootstrap --redact-host --json",),
        bootstrap_plan_schema_version=int(plan["schemaVersion"]),
        bootstrap_plan_milestone=str(plan["milestone"]),
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
