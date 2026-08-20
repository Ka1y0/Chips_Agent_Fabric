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
    utc_now,
)
from .fabric.capabilities import (
    INITIAL_CAPABILITY_CATALOG,
    WORKER_MANIFEST_SCHEMA_VERSION,
    CapabilityCatalog,
    CapabilityClaim,
    CapabilityError,
    CostMode,
    ObservationFreshness,
    QuotaAvailability,
    SubscriptionState,
    WorkerHealth,
    WorkerLocality,
    WorkerPrivacy,
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
    preferred_capability_fit: float = 0.75
    cost_mode_preference: float = 1.5
    worker_health: float = 1.5
    worker_load: float = 1.25
    quota_freshness: float = 0.75

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
            "preferredCapabilityFit": self.preferred_capability_fit,
            "costModePreference": self.cost_mode_preference,
            "workerHealth": self.worker_health,
            "workerLoad": self.worker_load,
            "quotaFreshness": self.quota_freshness,
        }


@dataclass(frozen=True, slots=True)
class SchedulerConfig:
    policy_version: str = "v0.5-capability-aware"
    weights: SchedulerWeights = field(default_factory=SchedulerWeights)


class DeterministicScheduler:
    """Pure scheduler: safety constraints, transparent scoring, stable ID tie-break."""

    def __init__(
        self,
        config: SchedulerConfig | None = None,
        capability_catalog: CapabilityCatalog | None = None,
    ) -> None:
        self.config = config or SchedulerConfig()
        self.capability_catalog = capability_catalog or INITIAL_CAPABILITY_CATALOG

    def schedule(
        self,
        *,
        task_id: str,
        requirements: TaskRequirements,
        topology: ExecutionTopology,
        workers: Iterable[WorkerSnapshot],
        resource_evidence: Iterable[ResourceRoutingEvidence] = (),
    ) -> RoutingDecision:
        evidence_by_worker = self._resource_evidence(resource_evidence)
        ordered_workers = self._workers(workers)
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
            "candidateScores": [
                {
                    "workerID": candidate.worker_id,
                    "score": candidate.score,
                    "components": candidate.components,
                }
                for candidate in sorted(scores, key=lambda item: (-item.score, item.worker_id))
            ],
            "capabilityCatalogVersion": self.capability_catalog.version,
            "costPreferenceOrder": [
                "activeSubscription",
                "localFree",
                "paid",
                "metered",
                "unknown",
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

        if (
            requirements.explicit_worker_override is not None
            and worker.id != requirements.explicit_worker_override
        ):
            reject(
                "EXPLICIT_OVERRIDE_MISMATCH",
                f"task explicitly targets worker {requirements.explicit_worker_override}",
            )
        if worker.node_state is not NodeState.ONLINE:
            reject("NODE_UNAVAILABLE", f"node state is {worker.node_state.value}")
        if worker.execution_schema_version is not None and (
            worker.execution_disposition != "executable"
        ):
            reject(
                worker.execution_rejection_code or "RUNTIME_EXECUTABILITY_UNKNOWN",
                "Worker runtime executability is "
                f"{worker.execution_disposition}; reasons="
                + ",".join(worker.execution_reason_codes),
            )
        versioned_worker = worker.manifest_schema_version is not None
        if (
            versioned_worker
            and worker.manifest_valid_until is not None
            and worker.manifest_valid_until <= utc_now()
        ):
            reject(
                "MANIFEST_EXPIRED",
                f"Worker capability manifest expired at {worker.manifest_valid_until.isoformat()}",
            )
        if not versioned_worker and worker.state is not WorkerState.IDLE:
            reject("WORKER_UNAVAILABLE", f"worker state is {worker.state.value}")
        elif versioned_worker and worker.running_tasks < worker.max_concurrency:
            allowed_states = (
                {WorkerState.IDLE}
                if worker.running_tasks == 0
                else {WorkerState.STARTING, WorkerState.RUNNING, WorkerState.WAITING}
            )
            if worker.state not in allowed_states:
                reject(
                    "WORKER_UNAVAILABLE",
                    "Worker aggregate state is inconsistent with its active-run capacity",
                )
        if worker.health is WorkerHealth.UNHEALTHY:
            reject("WORKER_UNHEALTHY", "latest Worker health observation is unhealthy")
        if worker.running_tasks >= worker.max_concurrency:
            reject(
                "WORKER_CAPACITY_EXHAUSTED",
                f"running tasks {worker.running_tasks} reached maximum {worker.max_concurrency}",
            )
        if worker.resource_state in {
            ResourceState.RATE_LIMITED,
            ResourceState.BUDGET_EXHAUSTED,
            ResourceState.COOLDOWN,
        }:
            reject("RESOURCE_UNAVAILABLE", f"resource state is {worker.resource_state.value}")
        if (
            worker.manifest_schema_version is not None
            and worker.quota_state is QuotaAvailability.EXHAUSTED
        ):
            reject(
                "CAPABILITY_QUOTA_EXHAUSTED",
                "latest capability-registry quota observation is exhausted",
            )
        worker_capabilities: frozenset[str] = worker.capabilities
        required_capabilities = requirements.required_capabilities
        worker_claims: dict[str, CapabilityClaim] = {}
        required_claims: tuple[CapabilityClaim, ...] = ()
        if worker.manifest_schema_version is not None:
            if worker.manifest_schema_version != WORKER_MANIFEST_SCHEMA_VERSION:
                reject(
                    "MANIFEST_SCHEMA_UNSUPPORTED",
                    f"unsupported manifest schema {worker.manifest_schema_version}",
                )
            if worker.capability_catalog_version != self.capability_catalog.version:
                reject(
                    "CAPABILITY_CATALOG_UNSUPPORTED",
                    "worker capability catalog does not match the active scheduler catalog",
                )
            else:
                try:
                    canonical_worker_claims = self.capability_catalog.canonicalize_claims(
                        worker.capability_claims or tuple(worker.capabilities)
                    )
                    worker_claims = {claim.name: claim for claim in canonical_worker_claims}
                    worker_capabilities = frozenset(worker_claims)
                    declared_names = self.capability_catalog.canonicalize_names(worker.capabilities)
                    if worker.capability_claims and declared_names != worker_capabilities:
                        raise CapabilityError(
                            "Worker capability names and parameter claims disagree"
                        )
                    required_capabilities = self.capability_catalog.canonicalize_names(
                        requirements.required_capabilities
                    )
                    self.capability_catalog.canonicalize_names(requirements.preferred_capabilities)
                    required_claims = self.capability_catalog.canonicalize_claims(
                        requirements.required_capability_parameters
                    )
                    if not {claim.name for claim in required_claims}.issubset(
                        required_capabilities
                    ):
                        raise CapabilityError(
                            "parameter constraints must name a required capability"
                        )
                except CapabilityError as error:
                    reject("CAPABILITY_CONTRACT_INVALID", str(error))
        elif requirements.required_capability_parameters:
            reject(
                "CAPABILITY_PARAMETERS_UNVERIFIED",
                "legacy Worker snapshot cannot prove parameterized capability limits",
            )
        if (
            requirements.required_manifest_schema_version is not None
            and worker.manifest_schema_version != requirements.required_manifest_schema_version
        ):
            reject(
                "MANIFEST_VERSION_MISMATCH",
                "worker manifest schema does not satisfy the task requirement",
            )
        if (
            requirements.required_capability_catalog_version is not None
            and worker.capability_catalog_version
            != requirements.required_capability_catalog_version
        ):
            reject(
                "CAPABILITY_CATALOG_VERSION_MISMATCH",
                "worker capability catalog does not satisfy the task requirement",
            )
        missing = required_capabilities - worker_capabilities
        if missing:
            reject("CAPABILITY_MISMATCH", f"missing capabilities: {','.join(sorted(missing))}")
        for required_claim in required_claims:
            worker_claim = worker_claims.get(required_claim.name)
            definition = self.capability_catalog.resolve(required_claim.name)
            if worker_claim is None or not definition.satisfies(worker_claim, required_claim):
                reject(
                    "CAPABILITY_PARAMETER_MISMATCH",
                    f"worker limits do not satisfy parameters for {required_claim.name}",
                )
        if requirements.local_only and worker.locality is not WorkerLocality.LOCAL:
            reject("LOCALITY_MISMATCH", "task requires a manifest-declared local Worker")
        if (
            requirements.minimum_quality_score is not None
            and worker.quality_score < requirements.minimum_quality_score
        ):
            reject(
                "QUALITY_BELOW_MINIMUM",
                f"requires {requirements.minimum_quality_score}, "
                f"worker reports {worker.quality_score}",
            )
        if requirements.max_incremental_cost_usd is not None:
            incremental_cost = self._effective_incremental_cost(worker)
            if incremental_cost is None:
                reject(
                    "INCREMENTAL_COST_UNKNOWN",
                    "task has a cost ceiling but Worker incremental cost is unknown",
                )
            elif incremental_cost > requirements.max_incremental_cost_usd:
                reject(
                    "INCREMENTAL_COST_LIMIT",
                    f"task ceiling {requirements.max_incremental_cost_usd}, "
                    f"worker incremental cost {incremental_cost}",
                )
        if requirements.code_write_required and not worker.code_write_allowed:
            reject("CODE_WRITE_FORBIDDEN", "worker policy forbids production code mutation")
        if requirements.privacy_sensitive and not worker.privacy_allowed:
            reject("PRIVACY_MISMATCH", "worker is not authorized for privacy-sensitive tasks")
        if (
            requirements.privacy_sensitive
            and worker.manifest_schema_version is not None
            and worker.privacy not in {WorkerPrivacy.SENSITIVE, WorkerPrivacy.RESTRICTED}
        ):
            reject(
                "PRIVACY_CLASS_MISMATCH",
                "versioned manifest lacks a privacy class suitable for sensitive work",
            )
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
        preferred = requirements.preferred_capabilities
        worker_capabilities = worker.capabilities
        if worker.manifest_schema_version is not None:
            worker_capabilities = self.capability_catalog.canonicalize_names(worker.capabilities)
            required = self.capability_catalog.canonicalize_names(required)
            preferred = self.capability_catalog.canonicalize_names(preferred)
        capability_fit = (
            1.0 if not required else len(required & worker_capabilities) / len(required)
        )
        preferred_capability_fit = (
            1.0 if not preferred else len(preferred & worker_capabilities) / len(preferred)
        )
        context_fit = 1.0
        if requirements.minimum_context_tokens:
            context_fit = min(
                1.0,
                (worker.model.context_window_tokens or 0) / requirements.minimum_context_tokens,
            )
        quota_health = (
            resource_evidence.health_score
            if resource_evidence is not None
            else {
                ResourceState.AVAILABLE: 1.0,
                ResourceState.WARNING: 0.5,
                ResourceState.UNKNOWN: 0.25,
            }.get(worker.resource_state, 0.0)
        )
        if worker.manifest_schema_version is not None:
            quota_health = min(
                quota_health,
                {
                    QuotaAvailability.AVAILABLE: 1.0,
                    QuotaAvailability.SCARCE: 0.4,
                    QuotaAvailability.EXHAUSTED: 0.0,
                    QuotaAvailability.UNKNOWN: 0.25,
                }[worker.quota_state],
            )
        components = {
            "capabilityFit": capability_fit,
            "expectedQuality": self._clamp(worker.quality_score),
            "availability": self._capacity_score(worker),
            "quotaHealth": quota_health,
            "monetaryCost": self._clamp(worker.monetary_cost_score),
            "latency": 1.0 / (1.0 + max(worker.expected_latency_seconds, 0.0) / 60.0),
            "historicalReliability": self._clamp(worker.reliability_score),
            "nodeLoad": 1.0 - self._clamp(worker.node_load),
            "privacy": 1.0
            if (not requirements.privacy_sensitive or worker.privacy_allowed)
            else 0.0,
            "contextFit": context_fit,
            "preferredCapabilityFit": preferred_capability_fit,
            "costModePreference": self._cost_mode_score(worker),
            "workerHealth": self._health_score(worker),
            "workerLoad": self._worker_load_score(worker),
            "quotaFreshness": self._freshness_score(worker.quota_freshness),
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

    def _select(
        self,
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
                    -self._cost_mode_score(worker),
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
                    if worker.id != primary.id
                    and {"review", "review-code"} & self._worker_capability_names(worker)
                ),
                None,
            )
            return (primary.id, reviewer.id) if reviewer else (primary.id,)
        if topology is ExecutionTopology.PARALLEL_PANEL:
            return tuple(worker.id for worker in ranked[: requirements.panel_size])
        raise ValueError(f"unsupported topology: {topology}")

    def _worker_capability_names(self, worker: WorkerSnapshot) -> frozenset[str]:
        if worker.manifest_schema_version is None:
            return worker.capabilities
        return self.capability_catalog.canonicalize_names(worker.capabilities)

    @staticmethod
    def _effective_incremental_cost(worker: WorkerSnapshot) -> float | None:
        if worker.cost_mode is CostMode.LOCAL_FREE:
            return 0.0
        if (
            worker.cost_mode is CostMode.SUBSCRIPTION
            and worker.subscription_state is SubscriptionState.AVAILABLE
        ):
            return 0.0
        return worker.incremental_cost_usd

    @staticmethod
    def _cost_mode_score(worker: WorkerSnapshot) -> float:
        if (
            worker.cost_mode is CostMode.SUBSCRIPTION
            and worker.subscription_state is SubscriptionState.AVAILABLE
        ):
            return 1.0
        return {
            CostMode.LOCAL_FREE: 0.8,
            CostMode.PAID: 0.6,
            CostMode.METERED: 0.35,
            CostMode.SUBSCRIPTION: 0.2,
            CostMode.UNKNOWN: 0.1,
        }[worker.cost_mode]

    @staticmethod
    def _health_score(worker: WorkerSnapshot) -> float:
        base = {
            WorkerHealth.HEALTHY: 1.0,
            WorkerHealth.DEGRADED: 0.4,
            WorkerHealth.UNHEALTHY: 0.0,
            WorkerHealth.UNKNOWN: 0.25,
        }[worker.health]
        if worker.health_freshness is ObservationFreshness.STALE:
            return min(base, 0.2)
        if worker.health_freshness is ObservationFreshness.UNKNOWN:
            return min(base, 0.25)
        return base

    @staticmethod
    def _worker_load_score(worker: WorkerSnapshot) -> float:
        if worker.worker_load is not None:
            return 1.0 - worker.worker_load
        if worker.manifest_schema_version is not None:
            return 0.25
        return 1.0 - worker.node_load

    @staticmethod
    def _capacity_score(worker: WorkerSnapshot) -> float:
        return max(0.0, 1.0 - (worker.running_tasks / worker.max_concurrency))

    @staticmethod
    def _freshness_score(value: ObservationFreshness) -> float:
        return {
            ObservationFreshness.FRESH: 1.0,
            ObservationFreshness.STALE: 0.1,
            ObservationFreshness.UNKNOWN: 0.25,
        }[value]

    @staticmethod
    def _clamp(value: float) -> float:
        return min(1.0, max(0.0, value))
