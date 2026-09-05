"""Bounded role decomposition above canonical Worker routing.

This module decides which stages and dependencies exist. It deliberately does
not select Workers; ``DeterministicScheduler`` owns that decision for every
stage instance from one frozen capability/resource snapshot.
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from enum import StrEnum

from .fabric.execution import SpawnPolicy


class AgentRole(StrEnum):
    ORCHESTRATOR = "orchestrator"
    PLANNER = "planner"
    FAST_VISION = "fastVision"
    COMPUTER_ACTOR = "computerActor"
    DOMAIN_WORKER = "domainWorker"
    REVIEWER = "reviewer"
    SYNTHESIZER = "synthesizer"


@dataclass(frozen=True, slots=True)
class ClusterStage:
    stage_id: str
    role: AgentRole
    required_capabilities: frozenset[str]
    preferred_capabilities: frozenset[str] = frozenset()
    depends_on: tuple[str, ...] = ()
    replicas: int = 1
    latency_weight: float = 0.5
    quality_weight: float = 0.5
    cost_weight: float = 0.0
    independent_from_roles: frozenset[AgentRole] = frozenset()
    resource_kinds: frozenset[str] = frozenset()

    def __post_init__(self) -> None:
        if not self.stage_id.strip():
            raise ValueError("stage_id must not be empty")
        if not self.required_capabilities or any(
            not value.strip() for value in self.required_capabilities
        ):
            raise ValueError("required_capabilities must contain canonical non-empty IDs")
        if not 1 <= self.replicas <= 32:
            raise ValueError("replicas must be between one and 32")
        weights = (self.latency_weight, self.quality_weight, self.cost_weight)
        if (
            any(
                not isinstance(value, (int, float))
                or isinstance(value, bool)
                or not math.isfinite(value)
                or value < 0
                for value in weights
            )
            or not math.isfinite(sum(weights))
            or sum(weights) <= 0
        ):
            raise ValueError(
                "cluster stage weights must be finite, non-negative with a positive sum"
            )
        if self.stage_id in self.depends_on:
            raise ValueError("a stage cannot depend on itself")


def _waves(stages: tuple[ClusterStage, ...]) -> dict[str, int]:
    unresolved = {stage.stage_id: set(stage.depends_on) for stage in stages}
    waves: dict[str, int] = {}
    while unresolved:
        ready = sorted(
            stage_id
            for stage_id, dependencies in unresolved.items()
            if dependencies <= waves.keys()
        )
        if not ready:
            raise ValueError("cluster blueprint dependencies must form a DAG")
        for stage_id in ready:
            dependencies = unresolved.pop(stage_id)
            waves[stage_id] = (
                0 if not dependencies else 1 + max(waves[value] for value in dependencies)
            )
    return waves


@dataclass(frozen=True, slots=True)
class ClusterBlueprint:
    workload_id: str
    stages: tuple[ClusterStage, ...]
    max_parallelism: int
    bridge_recommended: bool = True
    spawn_policy: SpawnPolicy = field(default_factory=SpawnPolicy)

    def __post_init__(self) -> None:
        if not self.workload_id.strip():
            raise ValueError("workload_id must not be empty")
        if not self.stages:
            raise ValueError("cluster blueprint must contain at least one stage")
        if self.max_parallelism < 1:
            raise ValueError("max_parallelism must be positive")
        stage_ids = [stage.stage_id for stage in self.stages]
        if len(stage_ids) != len(set(stage_ids)):
            raise ValueError("cluster stage IDs must be unique")
        known = set(stage_ids)
        for stage in self.stages:
            unknown = set(stage.depends_on) - known
            if unknown:
                raise ValueError(
                    f"stage {stage.stage_id} has unknown dependencies: "
                    + ",".join(sorted(unknown))
                )
        waves = _waves(self.stages)
        if max(waves.values()) > self.spawn_policy.max_depth:
            raise ValueError("cluster DAG exceeds the canonical spawn depth limit")
        if sum(stage.replicas for stage in self.stages) > self.spawn_policy.max_total_children:
            raise ValueError("cluster DAG exceeds the canonical total-child limit")
        if self.max_parallelism > self.spawn_policy.max_parallel_tasks:
            raise ValueError("cluster parallelism exceeds the canonical active-task limit")
        if any(stage.replicas > self.spawn_policy.max_children_per_parent for stage in self.stages):
            raise ValueError("cluster fanout exceeds the canonical per-parent limit")

    def to_protocol(self) -> dict[str, object]:
        return {
            "schemaVersion": 1,
            "workloadID": self.workload_id,
            "maxParallelism": self.max_parallelism,
            "bridgeRecommended": self.bridge_recommended,
            "workerSelectionAuthority": "deterministicScheduler",
            "governance": {
                "maxDepth": self.spawn_policy.max_depth,
                "maxChildrenPerParent": self.spawn_policy.max_children_per_parent,
                "maxTotalChildren": self.spawn_policy.max_total_children,
                "maxParallelTasks": self.spawn_policy.max_parallel_tasks,
                "maxParallelWorkerSlots": self.spawn_policy.max_parallel_worker_slots,
            },
            "stages": [
                {
                    "stageID": stage.stage_id,
                    "role": stage.role.value,
                    "requiredCapabilities": sorted(stage.required_capabilities),
                    "preferredCapabilities": sorted(stage.preferred_capabilities),
                    "dependsOn": list(stage.depends_on),
                    "replicas": stage.replicas,
                    "independentFromRoles": sorted(
                        role.value for role in stage.independent_from_roles
                    ),
                    "resourceKinds": sorted(stage.resource_kinds),
                    "routingWeights": {
                        "latency": stage.latency_weight,
                        "quality": stage.quality_weight,
                        "cost": stage.cost_weight,
                    },
                }
                for stage in self.stages
            ],
        }


@dataclass(frozen=True, slots=True)
class ClusterStageInstance:
    instance_id: str
    stage_id: str
    replica: int
    wave: int
    role: AgentRole
    required_capabilities: frozenset[str]
    preferred_capabilities: frozenset[str]
    independent_from_roles: frozenset[AgentRole]
    resource_kinds: frozenset[str]
    latency_weight: float = 0.5
    quality_weight: float = 0.5
    cost_weight: float = 0.0

    def to_protocol(self) -> dict[str, object]:
        return {
            "instanceID": self.instance_id,
            "stageID": self.stage_id,
            "replica": self.replica,
            "wave": self.wave,
            "role": self.role.value,
            "requiredCapabilities": sorted(self.required_capabilities),
            "preferredCapabilities": sorted(self.preferred_capabilities),
            "independentFromRoles": sorted(role.value for role in self.independent_from_roles),
            "resourceKinds": sorted(self.resource_kinds),
            "routingWeights": {
                "latency": self.latency_weight,
                "quality": self.quality_weight,
                "cost": self.cost_weight,
            },
            "workerID": None,
        }


@dataclass(frozen=True, slots=True)
class ClusterPlan:
    blueprint: ClusterBlueprint
    instances: tuple[ClusterStageInstance, ...]

    def to_protocol(self) -> dict[str, object]:
        return {
            "schemaVersion": 1,
            "blueprint": self.blueprint.to_protocol(),
            "instances": [value.to_protocol() for value in self.instances],
            "workerSelectionPending": True,
        }


class ClusterPlanner:
    """Expand a bounded DAG; never rank or assign concrete Workers."""

    def expand(self, blueprint: ClusterBlueprint) -> ClusterPlan:
        waves = _waves(blueprint.stages)
        instances = tuple(
            ClusterStageInstance(
                instance_id=f"{stage.stage_id}:{replica}",
                stage_id=stage.stage_id,
                replica=replica,
                wave=waves[stage.stage_id],
                role=stage.role,
                required_capabilities=stage.required_capabilities,
                preferred_capabilities=stage.preferred_capabilities,
                independent_from_roles=stage.independent_from_roles,
                resource_kinds=stage.resource_kinds,
                latency_weight=stage.latency_weight,
                quality_weight=stage.quality_weight,
                cost_weight=stage.cost_weight,
            )
            for stage in blueprint.stages
            for replica in range(1, stage.replicas + 1)
        )
        return ClusterPlan(blueprint=blueprint, instances=instances)


def computer_use_blueprint(
    workload_id: str,
    *,
    visual_replicas: int = 1,
    spawn_policy: SpawnPolicy | None = None,
) -> ClusterBlueprint:
    """Split computer use into perception, reasoning, action, verification, and synthesis."""

    if not 1 <= visual_replicas <= 4:
        raise ValueError("visual_replicas must be between one and four")
    stages = (
        ClusterStage(
            "observe-ground",
            AgentRole.FAST_VISION,
            frozenset({"read-image"}),
            preferred_capabilities=frozenset({"fast-routing"}),
            replicas=visual_replicas,
            latency_weight=0.8,
            quality_weight=0.2,
            resource_kinds=frozenset({"display"}),
        ),
        ClusterStage(
            "plan",
            AgentRole.PLANNER,
            frozenset({"reasoning"}),
            depends_on=("observe-ground",),
            latency_weight=0.25,
            quality_weight=0.75,
        ),
        ClusterStage(
            "act",
            AgentRole.COMPUTER_ACTOR,
            frozenset({"control-gui"}),
            depends_on=("plan",),
            preferred_capabilities=frozenset({"control-browser"}),
            latency_weight=0.6,
            quality_weight=0.4,
            resource_kinds=frozenset({"display", "keyboard", "mouse"}),
        ),
        ClusterStage(
            "verify",
            AgentRole.REVIEWER,
            frozenset({"read-image", "verify-result"}),
            depends_on=("act",),
            preferred_capabilities=frozenset({"reasoning"}),
            latency_weight=0.35,
            quality_weight=0.65,
            independent_from_roles=frozenset({AgentRole.COMPUTER_ACTOR}),
            resource_kinds=frozenset({"display"}),
        ),
        ClusterStage(
            "synthesize",
            AgentRole.SYNTHESIZER,
            frozenset({"reasoning"}),
            depends_on=("verify",),
            latency_weight=0.3,
            quality_weight=0.7,
        ),
    )
    return ClusterBlueprint(
        workload_id=workload_id,
        stages=stages,
        max_parallelism=max(visual_replicas, 2),
        bridge_recommended=True,
        spawn_policy=spawn_policy or SpawnPolicy(),
    )


def parallel_work_blueprint(
    workload_id: str,
    *,
    capability: str | frozenset[str],
    fanout: int,
    independent_review: bool = True,
    spawn_policy: SpawnPolicy | None = None,
) -> ClusterBlueprint:
    """Create a bounded fan-out/fusion DAG for research, coding, or creative work."""

    capabilities = (
        frozenset({capability}) if isinstance(capability, str) else frozenset(capability)
    )
    if not capabilities or any(not item.strip() for item in capabilities):
        raise ValueError("capability must not be empty")
    if not 1 <= fanout <= 32:
        raise ValueError("fanout must be between one and 32")
    stages: list[ClusterStage] = [
        ClusterStage(
            "decompose",
            AgentRole.PLANNER,
            frozenset({"reasoning"}),
            latency_weight=0.25,
            quality_weight=0.75,
        ),
        ClusterStage(
            "parallel-work",
            AgentRole.DOMAIN_WORKER,
            capabilities,
            depends_on=("decompose",),
            replicas=fanout,
            latency_weight=0.45,
            quality_weight=0.45,
            cost_weight=0.1,
        ),
        ClusterStage(
            "synthesize",
            AgentRole.SYNTHESIZER,
            frozenset({"reasoning"}),
            depends_on=("parallel-work",),
            latency_weight=0.25,
            quality_weight=0.75,
        ),
    ]
    if independent_review:
        stages.append(
            ClusterStage(
                "review",
                AgentRole.REVIEWER,
                frozenset({"verify-result"}),
                depends_on=("synthesize",),
                latency_weight=0.2,
                quality_weight=0.8,
                independent_from_roles=frozenset(
                    {AgentRole.DOMAIN_WORKER, AgentRole.SYNTHESIZER}
                ),
            )
        )
    return ClusterBlueprint(
        workload_id=workload_id,
        stages=tuple(stages),
        max_parallelism=fanout,
        bridge_recommended=fanout > 1,
        spawn_policy=spawn_policy or SpawnPolicy(),
    )
