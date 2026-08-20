"""Deterministic role decomposition for larger heterogeneous Agent clusters."""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from enum import StrEnum


class AgentRole(StrEnum):
    ORCHESTRATOR = "orchestrator"
    PLANNER = "planner"
    FAST_VISION = "fastVision"
    COMPUTER_ACTOR = "computerActor"
    DOMAIN_WORKER = "domainWorker"
    REVIEWER = "reviewer"
    SYNTHESIZER = "synthesizer"


@dataclass(frozen=True, slots=True)
class ClusterWorker:
    worker_id: str
    node_id: str
    capabilities: frozenset[str]
    quality_score: float
    latency_score: float
    cost_score: float = 0.5
    max_concurrency: int = 1

    def __post_init__(self) -> None:
        if not self.worker_id.strip() or not self.node_id.strip():
            raise ValueError("worker_id and node_id must not be empty")
        for name in ("quality_score", "latency_score", "cost_score"):
            value = getattr(self, name)
            if not 0 <= value <= 1:
                raise ValueError(f"{name} must be between zero and one")
        if self.max_concurrency < 1:
            raise ValueError("max_concurrency must be positive")


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

    def __post_init__(self) -> None:
        if not self.stage_id.strip():
            raise ValueError("stage_id must not be empty")
        if self.replicas < 1:
            raise ValueError("replicas must be positive")
        weights = (self.latency_weight, self.quality_weight, self.cost_weight)
        if any(value < 0 for value in weights) or sum(weights) <= 0:
            raise ValueError("cluster stage weights must be non-negative with a positive sum")
        if self.stage_id in self.depends_on:
            raise ValueError("a stage cannot depend on itself")


@dataclass(frozen=True, slots=True)
class ClusterBlueprint:
    workload_id: str
    stages: tuple[ClusterStage, ...]
    max_parallelism: int
    bridge_recommended: bool = True

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
                    f"stage {stage.stage_id} has unknown dependencies: {','.join(sorted(unknown))}"
                )
        self._validate_acyclic()

    def _validate_acyclic(self) -> None:
        dependencies = {stage.stage_id: set(stage.depends_on) for stage in self.stages}
        remaining = set(dependencies)
        while remaining:
            ready = {stage_id for stage_id in remaining if not dependencies[stage_id] & remaining}
            if not ready:
                raise ValueError("cluster blueprint dependencies must form a DAG")
            remaining -= ready

    def to_protocol(self) -> dict[str, object]:
        return {
            "schemaVersion": 1,
            "workloadID": self.workload_id,
            "maxParallelism": self.max_parallelism,
            "bridgeRecommended": self.bridge_recommended,
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
                }
                for stage in self.stages
            ],
        }


@dataclass(frozen=True, slots=True)
class ClusterAssignment:
    stage_id: str
    replica: int
    role: AgentRole
    worker_id: str
    node_id: str
    score: float
    independence_degraded: bool = False

    def to_protocol(self) -> dict[str, object]:
        return {
            "stageID": self.stage_id,
            "replica": self.replica,
            "role": self.role.value,
            "workerID": self.worker_id,
            "nodeID": self.node_id,
            "score": self.score,
            "independenceDegraded": self.independence_degraded,
        }


@dataclass(frozen=True, slots=True)
class ClusterPlan:
    blueprint: ClusterBlueprint
    assignments: tuple[ClusterAssignment, ...]
    unassigned_stages: tuple[str, ...]

    @property
    def complete(self) -> bool:
        return not self.unassigned_stages

    def to_protocol(self) -> dict[str, object]:
        return {
            "schemaVersion": 1,
            "blueprint": self.blueprint.to_protocol(),
            "assignments": [assignment.to_protocol() for assignment in self.assignments],
            "unassignedStages": list(self.unassigned_stages),
            "complete": self.complete,
        }


class ClusterPlanner:
    """Assign typed stages to bounded Worker capacity with stable tie-breaking."""

    def assign(
        self,
        blueprint: ClusterBlueprint,
        workers: tuple[ClusterWorker, ...],
    ) -> ClusterPlan:
        by_id: dict[str, ClusterWorker] = {}
        for worker in workers:
            if worker.worker_id in by_id:
                raise ValueError(f"duplicate cluster worker: {worker.worker_id}")
            by_id[worker.worker_id] = worker

        usage: dict[tuple[str, int], int] = defaultdict(int)
        roles_by_worker: dict[str, set[AgentRole]] = defaultdict(set)
        stage_waves: dict[str, int] = {}
        assignments: list[ClusterAssignment] = []
        unassigned: list[str] = []

        for stage in blueprint.stages:
            wave = (
                0
                if not stage.depends_on
                else 1 + max(stage_waves[dependency] for dependency in stage.depends_on)
            )
            stage_waves[stage.stage_id] = wave
            for replica in range(1, stage.replicas + 1):
                eligible = [
                    worker
                    for worker in by_id.values()
                    if usage[(worker.worker_id, wave)] < worker.max_concurrency
                    and stage.required_capabilities <= worker.capabilities
                ]
                if not eligible:
                    unassigned.append(f"{stage.stage_id}:{replica}")
                    continue

                independent = [
                    worker
                    for worker in eligible
                    if not (roles_by_worker[worker.worker_id] & stage.independent_from_roles)
                ]
                degraded = not independent and bool(stage.independent_from_roles)
                candidates = independent or eligible
                selected = min(
                    candidates,
                    key=lambda worker: (
                        -self._score(stage, worker),
                        usage[(worker.worker_id, wave)],
                        worker.worker_id,
                    ),
                )
                score = self._score(stage, selected)
                assignments.append(
                    ClusterAssignment(
                        stage_id=stage.stage_id,
                        replica=replica,
                        role=stage.role,
                        worker_id=selected.worker_id,
                        node_id=selected.node_id,
                        score=round(score, 9),
                        independence_degraded=degraded,
                    )
                )
                usage[(selected.worker_id, wave)] += 1
                roles_by_worker[selected.worker_id].add(stage.role)

        return ClusterPlan(
            blueprint=blueprint,
            assignments=tuple(assignments),
            unassigned_stages=tuple(unassigned),
        )

    @staticmethod
    def _score(stage: ClusterStage, worker: ClusterWorker) -> float:
        denominator = stage.latency_weight + stage.quality_weight + stage.cost_weight
        base = (
            worker.latency_score * stage.latency_weight
            + worker.quality_score * stage.quality_weight
            + worker.cost_score * stage.cost_weight
        ) / denominator
        if stage.preferred_capabilities:
            preferred_fit = len(stage.preferred_capabilities & worker.capabilities) / len(
                stage.preferred_capabilities
            )
            base += preferred_fit * 0.1
        return min(1.1, base)


def computer_use_blueprint(
    workload_id: str,
    *,
    visual_replicas: int = 1,
) -> ClusterBlueprint:
    """Split complex computer use into perception, reasoning, action, and verification."""

    if visual_replicas < 1:
        raise ValueError("visual_replicas must be positive")
    stages = (
        ClusterStage(
            "observe-ground",
            AgentRole.FAST_VISION,
            frozenset({"vision.grounding"}),
            preferred_capabilities=frozenset({"latency.fast"}),
            replicas=visual_replicas,
            latency_weight=0.8,
            quality_weight=0.2,
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
            frozenset({"computer.use"}),
            depends_on=("plan",),
            preferred_capabilities=frozenset({"ui.atomicActions"}),
            latency_weight=0.6,
            quality_weight=0.4,
        ),
        ClusterStage(
            "verify",
            AgentRole.REVIEWER,
            frozenset({"vision.grounding", "review"}),
            depends_on=("act",),
            preferred_capabilities=frozenset({"reasoning"}),
            latency_weight=0.35,
            quality_weight=0.65,
            independent_from_roles=frozenset({AgentRole.COMPUTER_ACTOR}),
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
    )


def parallel_work_blueprint(
    workload_id: str,
    *,
    capability: str | frozenset[str],
    fanout: int,
    independent_review: bool = True,
) -> ClusterBlueprint:
    """Create a bounded fan-out/fusion plan for research, coding, or creative work."""

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
                frozenset({"review"}),
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
    )
