"""Explainable planning layer above per-stage Worker scheduling."""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum

from .cluster import (
    AgentRole,
    ClusterBlueprint,
    ClusterStage,
    computer_use_blueprint,
    parallel_work_blueprint,
)


class HybridWorkloadKind(StrEnum):
    GENERAL = "general"
    CODING = "coding"
    RESEARCH = "research"
    COMPUTER_USE = "computerUse"
    CREATIVE = "creative"
    LONG_CONTEXT = "longContext"


class HybridExecutionMode(StrEnum):
    SINGLE = "single"
    PRIMARY_REVIEWER = "primaryReviewer"
    PARALLEL_PANEL = "parallelPanel"
    DAG_CLUSTER = "dagCluster"
    CHEAP_FIRST_ESCALATION = "cheapFirstEscalation"


_DEFAULT_CAPABILITIES = {
    HybridWorkloadKind.GENERAL: frozenset({"reasoning"}),
    HybridWorkloadKind.CODING: frozenset({"coding"}),
    HybridWorkloadKind.RESEARCH: frozenset({"research"}),
    HybridWorkloadKind.COMPUTER_USE: frozenset(
        {"vision.grounding", "reasoning", "computer.use", "review"}
    ),
    HybridWorkloadKind.CREATIVE: frozenset({"creative"}),
    HybridWorkloadKind.LONG_CONTEXT: frozenset({"longContext"}),
}


@dataclass(frozen=True, slots=True)
class HybridRequest:
    request_id: str
    kind: HybridWorkloadKind
    required_capabilities: frozenset[str] = frozenset()
    requested_parallelism: int = 1
    require_independent_review: bool = False
    privacy_sensitive: bool = False
    latency_sensitive: bool = False
    high_uncertainty: bool = False
    visual_interaction_steps: int = 0
    bridge_allowed: bool = True
    authority_bearing: bool = False

    def __post_init__(self) -> None:
        if not self.request_id.strip():
            raise ValueError("request_id must not be empty")
        if not 1 <= self.requested_parallelism <= 32:
            raise ValueError("requested_parallelism must be between one and 32")
        if self.visual_interaction_steps < 0:
            raise ValueError("visual_interaction_steps must not be negative")


@dataclass(frozen=True, slots=True)
class HybridExecutionPlan:
    request_id: str
    mode: HybridExecutionMode
    blueprint: ClusterBlueprint
    local_first: bool
    bridge_enabled: bool
    facts: tuple[str, ...]

    def to_protocol(self) -> dict[str, object]:
        return {
            "schemaVersion": 1,
            "requestID": self.request_id,
            "mode": self.mode.value,
            "localFirst": self.local_first,
            "bridgeEnabled": self.bridge_enabled,
            "facts": list(self.facts),
            "blueprint": self.blueprint.to_protocol(),
            "adaptiveLearning": False,
        }


class HybridEngine:
    """Choose a bounded topology; the scheduler still selects each concrete Worker."""

    def plan(
        self,
        request: HybridRequest,
        *,
        local_capabilities: frozenset[str] = frozenset(),
    ) -> HybridExecutionPlan:
        required = request.required_capabilities or _DEFAULT_CAPABILITIES[request.kind]
        facts: list[str] = []

        if request.kind is HybridWorkloadKind.COMPUTER_USE:
            visual_replicas = min(4, max(1, request.requested_parallelism))
            blueprint = computer_use_blueprint(
                request.request_id,
                visual_replicas=visual_replicas,
            )
            mode = HybridExecutionMode.DAG_CLUSTER
            facts.append(
                "computer use is split into fast visual grounding, planning, action, and review"
            )
            if request.visual_interaction_steps > 10:
                facts.append(
                    "long visual interaction favors a persistent low-latency perception lane"
                )
        elif request.requested_parallelism > 1 or request.high_uncertainty:
            capability = required
            fanout = request.requested_parallelism
            if request.high_uncertainty and fanout == 1:
                fanout = 2
            blueprint = parallel_work_blueprint(
                request.request_id,
                capability=capability,
                fanout=fanout,
                independent_review=request.require_independent_review,
            )
            mode = HybridExecutionMode.PARALLEL_PANEL
            facts.append("bounded fan-out is used to collect independent work before synthesis")
        elif request.require_independent_review:
            blueprint = parallel_work_blueprint(
                request.request_id,
                capability=required,
                fanout=1,
                independent_review=True,
            )
            mode = HybridExecutionMode.PRIMARY_REVIEWER
            facts.append("a separate review stage is required before completion")
        else:
            stage = ClusterStage(
                "execute",
                AgentRole.DOMAIN_WORKER,
                required,
                latency_weight=0.7 if request.latency_sensitive else 0.35,
                quality_weight=0.3 if request.latency_sensitive else 0.65,
            )
            blueprint = ClusterBlueprint(
                workload_id=request.request_id,
                stages=(stage,),
                max_parallelism=1,
                bridge_recommended=False,
            )
            mode = (
                HybridExecutionMode.CHEAP_FIRST_ESCALATION
                if request.latency_sensitive
                else HybridExecutionMode.SINGLE
            )
            facts.append("one bounded stage is sufficient for the requested workload")

        local_first = request.privacy_sensitive and required <= local_capabilities
        if request.privacy_sensitive:
            facts.append(
                "privacy-sensitive work is local-first"
                if local_first
                else "local capabilities are insufficient; no silent privacy downgrade is allowed"
            )

        multi_stage = len(blueprint.stages) > 1 or any(
            stage.replicas > 1 for stage in blueprint.stages
        )
        bridge_enabled = request.bridge_allowed and not request.authority_bearing and multi_stage
        if request.authority_bearing:
            facts.append("authority-bearing content is excluded from Bridge transfer")
        elif multi_stage and bridge_enabled:
            facts.append("Bridge is enabled for bounded non-authoritative inter-stage context")

        return HybridExecutionPlan(
            request_id=request.request_id,
            mode=mode,
            blueprint=blueprint,
            local_first=local_first,
            bridge_enabled=bridge_enabled,
            facts=tuple(facts),
        )
