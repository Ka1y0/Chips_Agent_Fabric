from __future__ import annotations

import hashlib
import json
import math
from collections.abc import Mapping
from dataclasses import dataclass, field
from enum import StrEnum
from types import MappingProxyType
from typing import Any

type JSONScalar = str | int | float | bool | None
type JSONValue = JSONScalar | list["JSONValue"] | dict[str, "JSONValue"]

CHILD_WORK_PROPOSAL_VERSION = "child-work-proposal/v1"
SPAWN_POLICY_VERSION = "spawn-policy/v1"


class SpawnValidationError(ValueError):
    """Raised when an untrusted child-work proposal cannot be normalized safely."""


class SpawnDisposition(StrEnum):
    """A pure policy outcome; none of these values creates a canonical Task."""

    ELIGIBLE = "eligible"
    REPLAY = "replay"
    REJECTED = "rejected"


class SpawnReason(StrEnum):
    GOAL_ID_MISMATCH = "GOAL_ID_MISMATCH"
    GOAL_NOT_RUNNING = "GOAL_NOT_RUNNING"
    STALE_STEER_VERSION = "STALE_STEER_VERSION"
    STALE_PARENT_STEER_VERSION = "STALE_PARENT_STEER_VERSION"
    DUPLICATE_REPLAY = "DUPLICATE_REPLAY"
    IDEMPOTENCY_CONFLICT = "IDEMPOTENCY_CONFLICT"
    PAYLOAD_LIMIT = "PAYLOAD_LIMIT"
    DEPTH_LIMIT = "DEPTH_LIMIT"
    CHILDREN_LIMIT = "CHILDREN_LIMIT"
    TOTAL_CHILDREN_LIMIT = "TOTAL_CHILDREN_LIMIT"
    TASK_CONCURRENCY_LIMIT = "TASK_CONCURRENCY_LIMIT"
    WORKER_CONCURRENCY_LIMIT = "WORKER_CONCURRENCY_LIMIT"
    PROVIDER_CONCURRENCY_LIMIT = "PROVIDER_CONCURRENCY_LIMIT"
    TOKEN_BUDGET_UNOBSERVABLE = "TOKEN_BUDGET_UNOBSERVABLE"
    TOKEN_BUDGET_EXCEEDED = "TOKEN_BUDGET_EXCEEDED"
    TIME_BUDGET_UNOBSERVABLE = "TIME_BUDGET_UNOBSERVABLE"
    TIME_BUDGET_EXCEEDED = "TIME_BUDGET_EXCEEDED"
    COST_BUDGET_UNOBSERVABLE = "COST_BUDGET_UNOBSERVABLE"
    COST_BUDGET_EXCEEDED = "COST_BUDGET_EXCEEDED"
    TASK_LOCKED = "TASK_LOCKED"
    CIRCUIT_OPEN = "CIRCUIT_OPEN"


def _require_identifier(value: str, field_name: str, *, maximum: int = 256) -> str:
    if not isinstance(value, str) or not value.strip():
        raise SpawnValidationError(f"{field_name} must be a non-empty string")
    if len(value) > maximum:
        raise SpawnValidationError(f"{field_name} exceeds its length limit")
    return value


def _validate_non_negative(value: int | float | None, field_name: str) -> None:
    if value is None:
        return
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise SpawnValidationError(f"{field_name} must be numeric")
    if not math.isfinite(float(value)) or value < 0:
        raise SpawnValidationError(f"{field_name} must be finite and non-negative")


def _normalize_json(value: Any) -> JSONValue:
    """Return an isolated JSON value whose object keys have canonical ordering."""

    try:
        encoded = json.dumps(
            value,
            allow_nan=False,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
        normalized = json.loads(encoded)
    except (TypeError, ValueError) as error:
        raise SpawnValidationError("proposal payload must be finite canonical JSON") from error
    return normalized


def _freeze_json(value: JSONValue) -> Any:
    if isinstance(value, dict):
        return MappingProxyType({key: _freeze_json(item) for key, item in value.items()})
    if isinstance(value, list):
        return tuple(_freeze_json(item) for item in value)
    return value


def _thaw_json(value: Any) -> JSONValue:
    if isinstance(value, Mapping):
        return {str(key): _thaw_json(item) for key, item in sorted(value.items())}
    if isinstance(value, tuple):
        return [_thaw_json(item) for item in value]
    return value


def _canonical_json_bytes(value: Any) -> bytes:
    return json.dumps(
        value,
        allow_nan=False,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")


def _sha256(value: Any) -> str:
    return hashlib.sha256(_canonical_json_bytes(value)).hexdigest()


def _freeze_int_mapping(value: Mapping[str, int], field_name: str) -> Mapping[str, int]:
    normalized: dict[str, int] = {}
    for raw_key, raw_value in value.items():
        key = _require_identifier(raw_key, field_name)
        if isinstance(raw_value, bool) or not isinstance(raw_value, int) or raw_value < 0:
            raise SpawnValidationError(f"{field_name} values must be non-negative integers")
        normalized[key] = raw_value
    return MappingProxyType(dict(sorted(normalized.items())))


def _freeze_string_mapping(value: Mapping[str, str], field_name: str) -> Mapping[str, str]:
    normalized = {
        _require_identifier(key, field_name): _require_identifier(digest, f"{field_name} digest")
        for key, digest in value.items()
    }
    return MappingProxyType(dict(sorted(normalized.items())))


@dataclass(frozen=True, slots=True)
class ChildWorkProposal:
    """Non-authoritative proposal for one child Task.

    A valid instance and an ``ELIGIBLE`` decision are inputs to an atomic repository transaction;
    neither one is evidence that a canonical Task exists or that authority was granted.
    """

    proposal_id: str
    goal_id: str
    parent_task_id: str
    proposal_key: str
    title: str
    description: str
    steer_version: int
    depth: int = 1
    source_run_id: str | None = None
    source_attempt: int | None = None
    task_definition_revision: int | None = None
    provider: str | None = None
    dependencies: tuple[str, ...] = ()
    requested_worker_slots: int = 1
    estimated_tokens: int | None = None
    estimated_seconds: float | None = None
    estimated_cost_usd: float | None = None
    task_lock_key: str | None = None
    circuit_key: str | None = None
    payload: Mapping[str, Any] = field(default_factory=dict, repr=False, compare=False)
    schema_version: str = CHILD_WORK_PROPOSAL_VERSION
    request_digest: str = field(init=False)
    payload_size_bytes: int = field(init=False)

    def __post_init__(self) -> None:
        for name in ("proposal_id", "goal_id", "parent_task_id", "proposal_key"):
            _require_identifier(getattr(self, name), name)
        _require_identifier(self.title, "title", maximum=500)
        _require_identifier(self.description, "description", maximum=20_000)
        if self.schema_version != CHILD_WORK_PROPOSAL_VERSION:
            raise SpawnValidationError("unsupported child-work proposal schema version")
        if isinstance(self.steer_version, bool) or self.steer_version < 0:
            raise SpawnValidationError("steer_version must be a non-negative integer")
        if isinstance(self.depth, bool) or self.depth < 1:
            raise SpawnValidationError("depth must be a positive integer")
        if isinstance(self.requested_worker_slots, bool) or self.requested_worker_slots < 1:
            raise SpawnValidationError("requested_worker_slots must be positive")
        if self.source_run_id is not None:
            _require_identifier(self.source_run_id, "source_run_id")
        for name in ("source_attempt", "task_definition_revision"):
            value = getattr(self, name)
            if value is not None and (isinstance(value, bool) or value < 1):
                raise SpawnValidationError(f"{name} must be positive when supplied")
        if self.provider is not None:
            _require_identifier(self.provider, "provider", maximum=128)
        if self.task_lock_key is not None:
            _require_identifier(self.task_lock_key, "task_lock_key")
        if self.circuit_key is not None:
            _require_identifier(self.circuit_key, "circuit_key")
        for name in ("estimated_tokens", "estimated_seconds", "estimated_cost_usd"):
            _validate_non_negative(getattr(self, name), name)

        dependencies = tuple(sorted(set(self.dependencies)))
        if any(not isinstance(item, str) or not item.strip() for item in dependencies):
            raise SpawnValidationError("dependencies must be non-empty string identifiers")
        if len(dependencies) != len(self.dependencies):
            raise SpawnValidationError("dependencies must not contain duplicates")
        object.__setattr__(self, "dependencies", dependencies)

        normalized_payload = _normalize_json(dict(self.payload))
        if not isinstance(normalized_payload, dict):
            raise SpawnValidationError("proposal payload must be a JSON object")
        payload_bytes = _canonical_json_bytes(normalized_payload)
        object.__setattr__(self, "payload", _freeze_json(normalized_payload))
        object.__setattr__(self, "payload_size_bytes", len(payload_bytes))
        object.__setattr__(self, "request_digest", _sha256(self._digest_protocol()))

    @property
    def canonical_creation_authority(self) -> bool:
        return False

    @property
    def semantic_digest(self) -> str:
        """Stable proposal meaning, suitable for atomic repository replay checks."""

        return self.request_digest

    @property
    def provider_key(self) -> str | None:
        return self.provider.casefold() if self.provider is not None else None

    @property
    def effective_circuit_key(self) -> str:
        if self.circuit_key is not None:
            return self.circuit_key
        if self.provider_key is not None:
            return f"provider:{self.provider_key}"
        return f"task:{self.parent_task_id}"

    def _digest_protocol(self) -> dict[str, Any]:
        return {
            "schemaVersion": self.schema_version,
            "goalID": self.goal_id,
            "parentTaskID": self.parent_task_id,
            "proposalKey": self.proposal_key,
            "title": self.title,
            "description": self.description,
            "steerVersion": self.steer_version,
            "depth": self.depth,
            "provider": self.provider_key,
            "dependencies": list(self.dependencies),
            "requestedWorkerSlots": self.requested_worker_slots,
            "estimatedTokens": self.estimated_tokens,
            "estimatedSeconds": self.estimated_seconds,
            "estimatedCostUSD": self.estimated_cost_usd,
            "taskLockKey": self.task_lock_key,
            "circuitKey": self.circuit_key,
            "payload": _thaw_json(self.payload),
        }

    def to_protocol(self) -> dict[str, Any]:
        return {
            **self._digest_protocol(),
            "proposalID": self.proposal_id,
            "sourceRunID": self.source_run_id,
            "sourceAttempt": self.source_attempt,
            "taskDefinitionRevision": self.task_definition_revision,
            "requestDigest": self.request_digest,
            "semanticDigest": self.semantic_digest,
            "canonicalCreationAuthority": False,
        }


@dataclass(frozen=True, slots=True)
class SpawnPolicy:
    policy_version: str = SPAWN_POLICY_VERSION
    max_depth: int = 4
    max_children_per_parent: int = 8
    max_total_children: int = 48
    max_parallel_tasks: int = 8
    max_parallel_worker_slots: int = 16
    provider_parallel_limits: Mapping[str, int] = field(default_factory=dict)
    max_total_tokens: int | None = None
    max_elapsed_seconds: float | None = None
    max_cost_usd: float | None = None
    max_payload_bytes: int = 64 * 1024
    circuit_failure_limit: int = 3

    def __post_init__(self) -> None:
        _require_identifier(self.policy_version, "policy_version")
        for name in (
            "max_depth",
            "max_children_per_parent",
            "max_total_children",
            "max_parallel_tasks",
            "max_parallel_worker_slots",
            "max_payload_bytes",
            "circuit_failure_limit",
        ):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, int) or value < 1:
                raise SpawnValidationError(f"{name} must be a positive integer")
        for name in ("max_total_tokens", "max_elapsed_seconds", "max_cost_usd"):
            value = getattr(self, name)
            _validate_non_negative(value, name)
            if value == 0:
                raise SpawnValidationError(f"{name} must be positive when supplied")
        normalized_limits = {
            key.casefold(): value for key, value in self.provider_parallel_limits.items()
        }
        frozen = _freeze_int_mapping(normalized_limits, "provider_parallel_limits")
        if any(value < 1 for value in frozen.values()):
            raise SpawnValidationError("provider concurrency limits must be positive")
        object.__setattr__(self, "provider_parallel_limits", frozen)

    def evaluate(self, proposal: ChildWorkProposal, context: SpawnContext) -> SpawnDecision:
        return SpawnGovernor(self).evaluate(proposal, context)


@dataclass(frozen=True, slots=True)
class SpawnContext:
    """Canonical counters and fences observed by the repository before admission."""

    goal_id: str
    goal_state: str
    current_steer_version: int
    parent_steer_version: int | None = None
    children_for_parent: int = 0
    total_children: int = 0
    active_tasks: int = 0
    active_worker_slots: int = 0
    active_by_provider: Mapping[str, int] = field(default_factory=dict)
    consumed_tokens: int | None = None
    reserved_tokens: int | None = None
    elapsed_seconds: float | None = None
    observed_cost_usd: float | None = None
    reserved_cost_usd: float | None = None
    existing_proposal_digests: Mapping[str, str] = field(default_factory=dict)
    locked_task_keys: frozenset[str] = frozenset()
    open_circuits: frozenset[str] = frozenset()
    circuit_failures: Mapping[str, int] = field(default_factory=dict)

    def __post_init__(self) -> None:
        _require_identifier(self.goal_id, "goal_id")
        _require_identifier(self.goal_state, "goal_state", maximum=64)
        if isinstance(self.current_steer_version, bool) or self.current_steer_version < 0:
            raise SpawnValidationError("current_steer_version must be non-negative")
        if self.parent_steer_version is not None and (
            isinstance(self.parent_steer_version, bool) or self.parent_steer_version < 0
        ):
            raise SpawnValidationError("parent_steer_version must be non-negative when known")
        for name in (
            "children_for_parent",
            "total_children",
            "active_tasks",
            "active_worker_slots",
        ):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                raise SpawnValidationError(f"{name} must be a non-negative integer")
        for name in (
            "consumed_tokens",
            "reserved_tokens",
            "elapsed_seconds",
            "observed_cost_usd",
            "reserved_cost_usd",
        ):
            _validate_non_negative(getattr(self, name), name)
        active = {key.casefold(): value for key, value in self.active_by_provider.items()}
        object.__setattr__(
            self, "active_by_provider", _freeze_int_mapping(active, "active_by_provider")
        )
        object.__setattr__(
            self,
            "existing_proposal_digests",
            _freeze_string_mapping(self.existing_proposal_digests, "existing_proposal_digests"),
        )
        object.__setattr__(
            self,
            "circuit_failures",
            _freeze_int_mapping(self.circuit_failures, "circuit_failures"),
        )
        object.__setattr__(
            self,
            "locked_task_keys",
            frozenset(
                _require_identifier(item, "locked_task_keys") for item in self.locked_task_keys
            ),
        )
        object.__setattr__(
            self,
            "open_circuits",
            frozenset(_require_identifier(item, "open_circuits") for item in self.open_circuits),
        )


@dataclass(frozen=True, slots=True)
class SpawnBudgetDelta:
    child_tasks: int
    worker_slots: int
    estimated_tokens: int | None
    estimated_seconds: float | None
    estimated_cost_usd: float | None

    def to_protocol(self) -> dict[str, int | float | None]:
        return {
            "childTasks": self.child_tasks,
            "workerSlots": self.worker_slots,
            "estimatedTokens": self.estimated_tokens,
            "estimatedSeconds": self.estimated_seconds,
            "estimatedCostUSD": self.estimated_cost_usd,
        }


@dataclass(frozen=True, slots=True)
class SpawnDecision:
    disposition: SpawnDisposition
    proposal_id: str
    proposal_key: str
    request_digest: str
    policy_version: str
    observed_steer_version: int
    reasons: tuple[SpawnReason, ...]
    budget_delta: SpawnBudgetDelta

    @property
    def eligible(self) -> bool:
        return self.disposition is SpawnDisposition.ELIGIBLE

    @property
    def accepted(self) -> bool:
        """Compatibility spelling for policy acceptance, not Task creation."""

        return self.eligible

    @property
    def idempotent_replay(self) -> bool:
        return self.disposition is SpawnDisposition.REPLAY

    @property
    def may_attempt_atomic_commit(self) -> bool:
        return self.eligible

    @property
    def canonical_task_created(self) -> bool:
        return False

    @property
    def primary_reason(self) -> SpawnReason | None:
        return self.reasons[0] if self.reasons else None

    def to_protocol(self) -> dict[str, Any]:
        return {
            "disposition": self.disposition.value,
            "proposalID": self.proposal_id,
            "proposalKey": self.proposal_key,
            "requestDigest": self.request_digest,
            "policyVersion": self.policy_version,
            "observedSteerVersion": self.observed_steer_version,
            "reasons": [reason.value for reason in self.reasons],
            "budgetDelta": self.budget_delta.to_protocol(),
            "mayAttemptAtomicCommit": self.may_attempt_atomic_commit,
            "canonicalTaskCreated": False,
        }


class SpawnGovernor:
    """Pure, deterministic child-work admission policy.

    The governor performs no writes and grants no authority. A caller must repeat every mutable
    fence and reserve all counters inside the repository transaction that creates canonical Tasks.
    """

    def __init__(self, policy: SpawnPolicy | None = None) -> None:
        self.policy = policy or SpawnPolicy()

    def decide(self, proposal: ChildWorkProposal, context: SpawnContext) -> SpawnDecision:
        return self.evaluate(proposal, context)

    def evaluate(self, proposal: ChildWorkProposal, context: SpawnContext) -> SpawnDecision:
        reasons: list[SpawnReason] = []
        policy = self.policy

        if proposal.goal_id != context.goal_id:
            reasons.append(SpawnReason.GOAL_ID_MISMATCH)
        if context.goal_state != "running":
            reasons.append(SpawnReason.GOAL_NOT_RUNNING)
        if proposal.steer_version != context.current_steer_version:
            reasons.append(SpawnReason.STALE_STEER_VERSION)
        if (
            context.parent_steer_version is not None
            and context.parent_steer_version != context.current_steer_version
        ):
            reasons.append(SpawnReason.STALE_PARENT_STEER_VERSION)

        existing_digest = context.existing_proposal_digests.get(proposal.proposal_key)
        if existing_digest is not None and existing_digest != proposal.request_digest:
            reasons.append(SpawnReason.IDEMPOTENCY_CONFLICT)

        # Returning an existing identity is harmless, but only while the Goal and steer fence are
        # current. A paused/stopped/stale controller still receives a fail-closed result.
        if (
            not reasons
            and existing_digest is not None
            and existing_digest == proposal.request_digest
        ):
            return self._decision(
                proposal,
                context,
                SpawnDisposition.REPLAY,
                (SpawnReason.DUPLICATE_REPLAY,),
            )

        if proposal.payload_size_bytes > policy.max_payload_bytes:
            reasons.append(SpawnReason.PAYLOAD_LIMIT)
        if proposal.depth > policy.max_depth:
            reasons.append(SpawnReason.DEPTH_LIMIT)
        if context.children_for_parent + 1 > policy.max_children_per_parent:
            reasons.append(SpawnReason.CHILDREN_LIMIT)
        if context.total_children + 1 > policy.max_total_children:
            reasons.append(SpawnReason.TOTAL_CHILDREN_LIMIT)
        if context.active_tasks + 1 > policy.max_parallel_tasks:
            reasons.append(SpawnReason.TASK_CONCURRENCY_LIMIT)
        if (
            context.active_worker_slots + proposal.requested_worker_slots
            > policy.max_parallel_worker_slots
        ):
            reasons.append(SpawnReason.WORKER_CONCURRENCY_LIMIT)

        provider = proposal.provider_key
        if (
            provider is not None
            and provider in policy.provider_parallel_limits
            and context.active_by_provider.get(provider, 0) + proposal.requested_worker_slots
            > policy.provider_parallel_limits[provider]
        ):
            reasons.append(SpawnReason.PROVIDER_CONCURRENCY_LIMIT)

        if (
            proposal.task_lock_key is not None
            and proposal.task_lock_key in context.locked_task_keys
        ):
            reasons.append(SpawnReason.TASK_LOCKED)
        circuit = proposal.effective_circuit_key
        if (
            circuit in context.open_circuits
            or context.circuit_failures.get(circuit, 0) >= policy.circuit_failure_limit
        ):
            reasons.append(SpawnReason.CIRCUIT_OPEN)

        if policy.max_total_tokens is not None:
            if (
                proposal.estimated_tokens is None
                or context.consumed_tokens is None
                or context.reserved_tokens is None
            ):
                reasons.append(SpawnReason.TOKEN_BUDGET_UNOBSERVABLE)
            elif (
                context.consumed_tokens + context.reserved_tokens + proposal.estimated_tokens
                > policy.max_total_tokens
            ):
                reasons.append(SpawnReason.TOKEN_BUDGET_EXCEEDED)

        if policy.max_elapsed_seconds is not None:
            if context.elapsed_seconds is None or proposal.estimated_seconds is None:
                reasons.append(SpawnReason.TIME_BUDGET_UNOBSERVABLE)
            elif context.elapsed_seconds + proposal.estimated_seconds > policy.max_elapsed_seconds:
                reasons.append(SpawnReason.TIME_BUDGET_EXCEEDED)

        if policy.max_cost_usd is not None:
            if (
                proposal.estimated_cost_usd is None
                or context.observed_cost_usd is None
                or context.reserved_cost_usd is None
            ):
                reasons.append(SpawnReason.COST_BUDGET_UNOBSERVABLE)
            elif (
                context.observed_cost_usd + context.reserved_cost_usd + proposal.estimated_cost_usd
                > policy.max_cost_usd
            ):
                reasons.append(SpawnReason.COST_BUDGET_EXCEEDED)

        return self._decision(
            proposal,
            context,
            SpawnDisposition.REJECTED if reasons else SpawnDisposition.ELIGIBLE,
            tuple(reasons),
        )

    def _decision(
        self,
        proposal: ChildWorkProposal,
        context: SpawnContext,
        disposition: SpawnDisposition,
        reasons: tuple[SpawnReason, ...],
    ) -> SpawnDecision:
        return SpawnDecision(
            disposition=disposition,
            proposal_id=proposal.proposal_id,
            proposal_key=proposal.proposal_key,
            request_digest=proposal.request_digest,
            policy_version=self.policy.policy_version,
            observed_steer_version=context.current_steer_version,
            reasons=reasons,
            budget_delta=SpawnBudgetDelta(
                child_tasks=1,
                worker_slots=proposal.requested_worker_slots,
                estimated_tokens=proposal.estimated_tokens,
                estimated_seconds=proposal.estimated_seconds,
                estimated_cost_usd=proposal.estimated_cost_usd,
            ),
        )


def evaluate_spawn(
    proposal: ChildWorkProposal,
    context: SpawnContext,
    policy: SpawnPolicy | None = None,
) -> SpawnDecision:
    return SpawnGovernor(policy).evaluate(proposal, context)


# Explicit compatibility name for callers that prefer the domain phrase over the actor noun.
SpawnGovernance = SpawnGovernor
