from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field

from .domain import (
    ApprovalState,
    CandidateScore,
    ExecutionTopology,
    NodeState,
    PermissionClass,
    Rejection,
    ResourceState,
    RoutingDecision,
    TaskRequirements,
    WorkerSnapshot,
    WorkerState,
)
from .hybrid import ResourceRoutingEvidence, RoutingInputSnapshot


@dataclass(frozen=True, slots=True)
class SchedulerWeights:
    capability_fit: float = 3.0
    expected_quality: float = 2.0
    availability: float = 2.0
    quota_health: float = 1.5
    monetary_cost: float = 1.0
    latency: float = 1.0
    historical_reliability: float = 1.5
    node_load: float = 1.0
    privacy: float = 2.0
    context_fit: float = 1.5

    def as_dict(self) -> dict[str, float]:
        return {
            "capabilityFit": self.capability_fit,
            "expectedQuality": self.expected_quality,
            "availability": self.availability,
            "quotaHealth": self.quota_health,
            "monetaryCost": self.monetary_cost,
            "latency": self.latency,
            "historicalReliability": self.historical_reliability,
            "nodeLoad": self.node_load,
            "privacy": self.privacy,
            "contextFit": self.context_fit,
        }


@dataclass(frozen=True, slots=True)
class SchedulerConfig:
    policy_version: str = "v0.4-resource-aware"
    weights: SchedulerWeights = field(default_factory=SchedulerWeights)


class DeterministicScheduler:
    """Pure V0 scheduler: constraints first, configured scoring second, stable ID tie-break."""

    def __init__(self, config: SchedulerConfig | None = None) -> None:
        self.config = config or SchedulerConfig()

    def schedule(
        self,
        *,
        task_id: str,
        requirements: TaskRequirements,
        topology: ExecutionTopology,
        workers: Iterable[WorkerSnapshot],
        resource_evidence: Iterable[ResourceRoutingEvidence] = (),
    ) -> RoutingDecision:
        if topology is ExecutionTopology.PARALLEL_PANEL and requirements.panel_size < 1:
            raise ValueError("parallel panel size must be at least one")

        ordered_workers = self._workers(workers)
        evidence_by_worker = self._resource_evidence(resource_evidence)
        unknown_evidence = sorted(
            set(evidence_by_worker) - {worker.id for worker in ordered_workers}
        )
        if unknown_evidence:
            raise ValueError(
                "resource routing evidence references unknown Workers: "
                + ", ".join(unknown_evidence)
            )

        eligible: list[WorkerSnapshot] = []
        rejected: list[Rejection] = []
        for worker in ordered_workers:
            reasons = self._hard_constraints(requirements, worker)
            if reasons:
                rejected.extend(reasons)
            else:
                eligible.append(worker)

        scores = [
            self._score(requirements, worker, evidence_by_worker.get(worker.id))
            for worker in eligible
        ]
        score_by_id = {score.worker_id: score for score in scores}
        ranked = sorted(eligible, key=lambda worker: (-score_by_id[worker.id].score, worker.id))
        selected = self._select(topology, requirements, ranked, score_by_id)

        if topology is ExecutionTopology.PRIMARY_REVIEWER and len(selected) < 2:
            rejected.append(
                Rejection(
                    worker_id="__topology__",
                    reason_code="REVIEWER_UNAVAILABLE",
                    detail=(
                        "PRIMARY_REVIEWER requires two distinct eligible workers "
                        "and a review capability"
                    ),
                )
            )
            selected = ()
        if topology is ExecutionTopology.PARALLEL_PANEL and len(selected) < requirements.panel_size:
            rejected.append(
                Rejection(
                    worker_id="__topology__",
                    reason_code="PANEL_SIZE_UNAVAILABLE",
                    detail=(
                        f"requested panel size {requirements.panel_size}, "
                        f"eligible workers {len(selected)}"
                    ),
                )
            )
            selected = ()

        explanation = {
            "taskID": task_id,
            "topology": topology.value,
            "policyVersion": self.config.policy_version,
            "weights": self.config.weights.as_dict(),
            "selected": list(selected),
            "rejected": [
                {"workerID": item.worker_id, "reasonCode": item.reason_code, "detail": item.detail}
                for item in rejected
            ],
            "resourceEvidence": [
                evidence.to_protocol()
                for evidence in sorted(evidence_by_worker.values(), key=lambda item: item.worker_id)
            ],
            "tieBreak": "workerID ascending after score",
        }
        return RoutingDecision(
            topology=topology,
            selected_worker_ids=selected,
            candidates=tuple(sorted(scores, key=lambda item: (-item.score, item.worker_id))),
            rejected=tuple(rejected),
            policy_version=self.config.policy_version,
            explanation=explanation,
        )

    def schedule_snapshot(self, snapshot: RoutingInputSnapshot) -> RoutingDecision:
        """Schedule a frozen V0.1 input without introducing adaptive behavior."""

        decision = self.schedule(
            task_id=snapshot.task_id,
            requirements=snapshot.requirements,
            topology=snapshot.topology,
            workers=snapshot.workers,
            resource_evidence=snapshot.resource_evidence,
        )
        return RoutingDecision(
            topology=decision.topology,
            selected_worker_ids=decision.selected_worker_ids,
            candidates=decision.candidates,
            rejected=decision.rejected,
            policy_version=decision.policy_version,
            explanation={**decision.explanation, "routingInput": snapshot.explanation()},
        )

    def constraint_rejections(
        self, requirements: TaskRequirements, worker: WorkerSnapshot
    ) -> tuple[Rejection, ...]:
        """Expose the deterministic hard-constraint phase to pre-routing policy overlays."""

        return self._hard_constraints(requirements, worker)

    def _hard_constraints(
        self, requirements: TaskRequirements, worker: WorkerSnapshot
    ) -> tuple[Rejection, ...]:
        reasons: list[Rejection] = []

        def reject(code: str, detail: str) -> None:
            reasons.append(Rejection(worker_id=worker.id, reason_code=code, detail=detail))

        if worker.node_state is not NodeState.ONLINE:
            reject("NODE_UNAVAILABLE", f"node state is {worker.node_state.value}")
        if worker.state is not WorkerState.IDLE:
            reject("WORKER_UNAVAILABLE", f"worker state is {worker.state.value}")
        if worker.resource_state in {
            ResourceState.RATE_LIMITED,
            ResourceState.BUDGET_EXHAUSTED,
            ResourceState.COOLDOWN,
        }:
            reject("RESOURCE_UNAVAILABLE", f"resource state is {worker.resource_state.value}")
        missing = requirements.required_capabilities - worker.capabilities
        if missing:
            reject("CAPABILITY_MISMATCH", f"missing capabilities: {','.join(sorted(missing))}")
        if requirements.code_write_required and not worker.code_write_allowed:
            reject("CODE_WRITE_FORBIDDEN", "worker policy forbids production code mutation")
        if requirements.privacy_sensitive and not worker.privacy_allowed:
            reject("PRIVACY_MISMATCH", "worker is not authorized for privacy-sensitive tasks")
        if requirements.minimum_context_tokens is not None:
            context = worker.model.context_window_tokens
            if context is None:
                reject("CONTEXT_UNKNOWN", "task requires a verified context window")
            elif context < requirements.minimum_context_tokens:
                reject(
                    "CONTEXT_INSUFFICIENT",
                    f"requires {requirements.minimum_context_tokens}, worker reports {context}",
                )
        if (
            requirements.permission_class is PermissionClass.RED
            and requirements.approval_state is not ApprovalState.APPROVED
        ):
            reject("HUMAN_APPROVAL_REQUIRED", "RED task lacks a durable approved record")
        return tuple(reasons)

    def _score(
        self,
        requirements: TaskRequirements,
        worker: WorkerSnapshot,
        resource_evidence: ResourceRoutingEvidence | None = None,
    ) -> CandidateScore:
        required = requirements.required_capabilities
        capability_fit = (
            1.0 if not required else len(required & worker.capabilities) / len(required)
        )
        context_fit = 1.0
        if requirements.minimum_context_tokens:
            context_fit = min(
                1.0,
                (worker.model.context_window_tokens or 0) / requirements.minimum_context_tokens,
            )
        components = {
            "capabilityFit": capability_fit,
            "expectedQuality": self._clamp(worker.quality_score),
            "availability": 1.0 if worker.state is WorkerState.IDLE else 0.0,
            "quotaHealth": (
                resource_evidence.health_score
                if resource_evidence is not None
                else {
                    ResourceState.AVAILABLE: 1.0,
                    ResourceState.WARNING: 0.5,
                    ResourceState.UNKNOWN: 0.25,
                }.get(worker.resource_state, 0.0)
            ),
            "monetaryCost": self._clamp(worker.monetary_cost_score),
            "latency": 1.0 / (1.0 + max(worker.expected_latency_seconds, 0.0) / 60.0),
            "historicalReliability": self._clamp(worker.reliability_score),
            "nodeLoad": 1.0 - self._clamp(worker.node_load),
            "privacy": 1.0
            if (not requirements.privacy_sensitive or worker.privacy_allowed)
            else 0.0,
            "contextFit": context_fit,
        }
        weights = self.config.weights.as_dict()
        denominator = sum(weights.values())
        score = sum(components[key] * weights[key] for key in components) / denominator
        return CandidateScore(worker_id=worker.id, score=round(score, 9), components=components)

    @staticmethod
    def _workers(values: Iterable[WorkerSnapshot]) -> tuple[WorkerSnapshot, ...]:
        result: list[WorkerSnapshot] = []
        seen: set[str] = set()
        duplicates: set[str] = set()
        for value in values:
            if value.id in seen:
                duplicates.add(value.id)
            else:
                seen.add(value.id)
            result.append(value)
        if duplicates:
            raise ValueError("workers must be unique by ID: " + ", ".join(sorted(duplicates)))
        return tuple(sorted(result, key=lambda item: item.id))

    @staticmethod
    def _resource_evidence(
        values: Iterable[ResourceRoutingEvidence],
    ) -> Mapping[str, ResourceRoutingEvidence]:
        result: dict[str, ResourceRoutingEvidence] = {}
        for value in values:
            if value.worker_id in result:
                raise ValueError("resource routing evidence must be unique by Worker")
            result[value.worker_id] = value
        return result

    @staticmethod
    def _select(
        topology: ExecutionTopology,
        requirements: TaskRequirements,
        ranked: list[WorkerSnapshot],
        score_by_id: dict[str, CandidateScore],
    ) -> tuple[str, ...]:
        if not ranked:
            return ()
        if topology is ExecutionTopology.SINGLE:
            return (ranked[0].id,)
        if topology is ExecutionTopology.FALLBACK:
            preferred_rank = {
                worker_id: index for index, worker_id in enumerate(requirements.preferred_workers)
            }
            ordered = sorted(
                ranked,
                key=lambda worker: (
                    preferred_rank.get(worker.id, len(preferred_rank)),
                    -score_by_id[worker.id].score,
                    worker.id,
                ),
            )
            return (ordered[0].id,)
        if topology is ExecutionTopology.CHEAP_FIRST_ESCALATION:
            ordered = sorted(
                ranked,
                key=lambda worker: (
                    -worker.monetary_cost_score,
                    -score_by_id[worker.id].score,
                    worker.id,
                ),
            )
            return (ordered[0].id,)
        if topology is ExecutionTopology.PRIMARY_REVIEWER:
            primary = ranked[0]
            reviewer = next(
                (
                    worker
                    for worker in ranked
                    if worker.id != primary.id and "review" in worker.capabilities
                ),
                None,
            )
            return (primary.id, reviewer.id) if reviewer else (primary.id,)
        if topology is ExecutionTopology.PARALLEL_PANEL:
            return tuple(worker.id for worker in ranked[: requirements.panel_size])
        raise ValueError(f"unsupported topology: {topology}")

    @staticmethod
    def _clamp(value: float) -> float:
        return min(1.0, max(0.0, value))
