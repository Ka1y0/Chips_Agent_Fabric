from __future__ import annotations

import hashlib
import json
from collections import defaultdict
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from enum import StrEnum
from types import MappingProxyType
from typing import Any

type JSONScalar = str | int | float | bool | None
type JSONValue = JSONScalar | list["JSONValue"] | dict[str, "JSONValue"]

RESULT_CONTRIBUTION_VERSION = "result-contribution/v1"
FUSION_RESULT_VERSION = "result-fusion/v1"
FUSION_POLICY_VERSION = "deterministic-fusion/v1"
VERIFICATION_HANDOFF_VERSION = "verification-handoff/v1"


class FusionValidationError(ValueError):
    """Raised when contribution identity or canonical JSON invariants are violated."""


class StaleVerificationHandoff(FusionValidationError):
    """A verifier attempted to apply a token to a different canonical execution snapshot."""


class ContributionState(StrEnum):
    ASSERTED = "asserted"
    ABSTAINED = "abstained"
    FAILED = "failed"


class FusionStatus(StrEnum):
    COMPATIBLE = "compatible"
    COMPLEMENTARY = "complementary"
    CONTRADICTORY = "contradictory"
    INSUFFICIENT = "insufficientEvidence"


def _require_text(value: str, field_name: str, *, maximum: int = 256) -> str:
    if not isinstance(value, str) or not value.strip():
        raise FusionValidationError(f"{field_name} must be a non-empty string")
    if len(value) > maximum:
        raise FusionValidationError(f"{field_name} exceeds its length limit")
    return value


def _normalize_json(value: Any) -> JSONValue:
    try:
        encoded = json.dumps(
            value,
            allow_nan=False,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
        return json.loads(encoded)
    except (TypeError, ValueError) as error:
        raise FusionValidationError("fusion values must be finite canonical JSON") from error


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


def canonical_json_bytes(value: Any) -> bytes:
    return json.dumps(
        value,
        allow_nan=False,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")


def _sha256(value: Any) -> str:
    return hashlib.sha256(canonical_json_bytes(value)).hexdigest()


def _normalize_object(value: Mapping[str, Any], field_name: str) -> Mapping[str, Any]:
    normalized = _normalize_json(dict(value))
    if not isinstance(normalized, dict):
        raise FusionValidationError(f"{field_name} must be a JSON object")
    if any(not key.strip() or len(key) > 256 for key in normalized):
        raise FusionValidationError(f"{field_name} keys must be bounded non-empty strings")
    return _freeze_json(normalized)


@dataclass(frozen=True, slots=True)
class ResultContribution:
    """One versioned, immutable semantic contribution from a canonical Worker run."""

    contribution_id: str
    task_id: str
    run_id: str
    worker_id: str
    verification_scope_id: str
    task_definition_revision: int
    source_attempt: int
    source_result_sha256: str
    steer_version: int = 0
    claims: Mapping[str, Any] = field(default_factory=dict, repr=False, compare=False)
    role: str = "primary"
    state: ContributionState = ContributionState.ASSERTED
    evidence: Mapping[str, Any] = field(default_factory=dict, repr=False, compare=False)
    schema_version: str = RESULT_CONTRIBUTION_VERSION
    digest: str = field(init=False)

    def __post_init__(self) -> None:
        for name in (
            "contribution_id",
            "task_id",
            "run_id",
            "worker_id",
            "verification_scope_id",
            "role",
        ):
            _require_text(getattr(self, name), name)
        if self.schema_version != RESULT_CONTRIBUTION_VERSION:
            raise FusionValidationError("unsupported result contribution schema version")
        if isinstance(self.task_definition_revision, bool) or self.task_definition_revision < 1:
            raise FusionValidationError("task_definition_revision must be positive")
        if isinstance(self.source_attempt, bool) or self.source_attempt < 1:
            raise FusionValidationError("source_attempt must be positive")
        if len(self.source_result_sha256) != 64 or any(
            character not in "0123456789abcdef" for character in self.source_result_sha256
        ):
            raise FusionValidationError("source_result_sha256 must be a lowercase SHA-256 digest")
        if isinstance(self.steer_version, bool) or self.steer_version < 0:
            raise FusionValidationError("steer_version must be a non-negative integer")
        try:
            state = ContributionState(self.state)
        except ValueError as error:
            raise FusionValidationError("unsupported contribution state") from error
        object.__setattr__(self, "state", state)
        normalized_claims = _normalize_object(self.claims, "claims")
        normalized_evidence = _normalize_object(self.evidence, "evidence")
        if state is not ContributionState.ASSERTED and normalized_claims:
            raise FusionValidationError("non-asserted contributions cannot carry claims")
        object.__setattr__(self, "claims", normalized_claims)
        object.__setattr__(self, "evidence", normalized_evidence)
        object.__setattr__(self, "digest", _sha256(self.to_protocol(include_digest=False)))

    @property
    def attempt(self) -> int:
        return self.source_attempt

    def claims_protocol(self) -> dict[str, JSONValue]:
        return dict(_thaw_json(self.claims))

    def to_protocol(self, *, include_digest: bool = True) -> dict[str, Any]:
        value = {
            "schemaVersion": self.schema_version,
            "contributionID": self.contribution_id,
            "taskID": self.task_id,
            "runID": self.run_id,
            "workerID": self.worker_id,
            "role": self.role,
            "verificationScopeID": self.verification_scope_id,
            "taskDefinitionRevision": self.task_definition_revision,
            "sourceAttempt": self.source_attempt,
            "sourceResultSHA256": self.source_result_sha256,
            "steerVersion": self.steer_version,
            "state": self.state.value,
            "claims": self.claims_protocol(),
            "evidence": _thaw_json(self.evidence),
        }
        if include_digest:
            value["digest"] = self.digest
        return value


@dataclass(frozen=True, slots=True)
class ContributionProvenance:
    contribution_id: str
    contribution_digest: str
    worker_id: str
    run_id: str
    source_result_sha256: str
    role: str
    state: ContributionState

    @classmethod
    def from_contribution(cls, contribution: ResultContribution) -> ContributionProvenance:
        return cls(
            contribution_id=contribution.contribution_id,
            contribution_digest=contribution.digest,
            worker_id=contribution.worker_id,
            run_id=contribution.run_id,
            source_result_sha256=contribution.source_result_sha256,
            role=contribution.role,
            state=contribution.state,
        )

    def to_protocol(self) -> dict[str, str]:
        return {
            "contributionID": self.contribution_id,
            "contributionDigest": self.contribution_digest,
            "workerID": self.worker_id,
            "runID": self.run_id,
            "sourceResultSHA256": self.source_result_sha256,
            "role": self.role,
            "state": self.state.value,
        }


def _provenance_sort_key(value: ContributionProvenance) -> tuple[str, str, str, str]:
    return (value.worker_id, value.run_id, value.contribution_id, value.contribution_digest)


@dataclass(frozen=True, slots=True)
class FusedClaim:
    claim_key: str
    value: Any = field(repr=False, compare=False)
    value_sha256: str = ""
    provenance: tuple[ContributionProvenance, ...] = ()

    def __post_init__(self) -> None:
        _require_text(self.claim_key, "claim_key")
        normalized = _normalize_json(self.value)
        object.__setattr__(self, "value", _freeze_json(normalized))
        expected_hash = _sha256(normalized)
        if self.value_sha256 and self.value_sha256 != expected_hash:
            raise FusionValidationError("fused claim value hash mismatch")
        object.__setattr__(self, "value_sha256", expected_hash)
        object.__setattr__(
            self, "provenance", tuple(sorted(self.provenance, key=_provenance_sort_key))
        )

    def to_protocol(self) -> dict[str, Any]:
        return {
            "claimKey": self.claim_key,
            "value": _thaw_json(self.value),
            "valueSHA256": self.value_sha256,
            "provenance": [item.to_protocol() for item in self.provenance],
        }


@dataclass(frozen=True, slots=True)
class ConflictVariant:
    value: Any = field(repr=False, compare=False)
    value_sha256: str = ""
    provenance: tuple[ContributionProvenance, ...] = ()

    def __post_init__(self) -> None:
        normalized = _normalize_json(self.value)
        object.__setattr__(self, "value", _freeze_json(normalized))
        expected_hash = _sha256(normalized)
        if self.value_sha256 and self.value_sha256 != expected_hash:
            raise FusionValidationError("conflict variant value hash mismatch")
        object.__setattr__(self, "value_sha256", expected_hash)
        object.__setattr__(
            self, "provenance", tuple(sorted(self.provenance, key=_provenance_sort_key))
        )

    def to_protocol(self) -> dict[str, Any]:
        return {
            "value": _thaw_json(self.value),
            "valueSHA256": self.value_sha256,
            "provenance": [item.to_protocol() for item in self.provenance],
        }


@dataclass(frozen=True, slots=True)
class FusionConflict:
    claim_key: str
    variants: tuple[ConflictVariant, ...]
    reason_code: str = "CLAIM_VALUE_CONFLICT"

    def __post_init__(self) -> None:
        _require_text(self.claim_key, "claim_key")
        if len(self.variants) < 2:
            raise FusionValidationError("a fusion conflict requires at least two variants")
        ordered = tuple(sorted(self.variants, key=lambda item: item.value_sha256))
        if len({item.value_sha256 for item in ordered}) != len(ordered):
            raise FusionValidationError("fusion conflict variants must be distinct")
        object.__setattr__(self, "variants", ordered)

    def to_protocol(self) -> dict[str, Any]:
        return {
            "claimKey": self.claim_key,
            "reasonCode": self.reason_code,
            "variants": [item.to_protocol() for item in self.variants],
        }


@dataclass(frozen=True, slots=True)
class FusionPolicy:
    policy_version: str = FUSION_POLICY_VERSION
    minimum_contributions: int = 1
    required_claim_keys: frozenset[str] = frozenset()
    require_all_contributions_asserted: bool = True
    max_contributions: int = 64
    max_claims: int = 256
    max_input_bytes: int = 1024 * 1024
    verification_required: bool = True

    def __post_init__(self) -> None:
        _require_text(self.policy_version, "policy_version")
        for name in ("minimum_contributions", "max_contributions", "max_claims", "max_input_bytes"):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, int) or value < 1:
                raise FusionValidationError(f"{name} must be a positive integer")
        if self.minimum_contributions > self.max_contributions:
            raise FusionValidationError("minimum_contributions exceeds max_contributions")
        if not self.verification_required:
            raise FusionValidationError("fusion policy cannot waive independent verification")
        required = frozenset(
            _require_text(item, "required_claim_keys") for item in self.required_claim_keys
        )
        object.__setattr__(self, "required_claim_keys", required)


@dataclass(frozen=True, slots=True)
class VerificationHandoffToken:
    task_id: str
    verification_scope_id: str
    task_definition_revision: int
    source_attempt: int
    steer_version: int
    fusion_hash: str
    verification_required: bool = True
    schema_version: str = VERIFICATION_HANDOFF_VERSION
    token_sha256: str = field(init=False)

    def __post_init__(self) -> None:
        for name in ("task_id", "verification_scope_id", "fusion_hash"):
            _require_text(getattr(self, name), name)
        if len(self.fusion_hash) != 64 or any(
            character not in "0123456789abcdef" for character in self.fusion_hash
        ):
            raise FusionValidationError("fusion_hash must be a lowercase SHA-256 digest")
        if isinstance(self.task_definition_revision, bool) or self.task_definition_revision < 1:
            raise FusionValidationError("task_definition_revision must be positive")
        if isinstance(self.source_attempt, bool) or self.source_attempt < 1:
            raise FusionValidationError("source_attempt must be positive")
        if isinstance(self.steer_version, bool) or self.steer_version < 0:
            raise FusionValidationError("steer_version must be a non-negative integer")
        if self.schema_version != VERIFICATION_HANDOFF_VERSION:
            raise FusionValidationError("unsupported verification handoff schema version")
        if not self.verification_required:
            raise FusionValidationError("fusion handoff cannot waive independent verification")
        object.__setattr__(self, "token_sha256", _sha256(self.to_protocol(include_hash=False)))

    @property
    def attempt(self) -> int:
        return self.source_attempt

    def is_current(
        self,
        *,
        task_id: str,
        verification_scope_id: str,
        task_definition_revision: int,
        source_attempt: int,
        steer_version: int,
        fusion_hash: str,
    ) -> bool:
        return (
            self.task_id == task_id
            and self.verification_scope_id == verification_scope_id
            and self.task_definition_revision == task_definition_revision
            and self.source_attempt == source_attempt
            and self.steer_version == steer_version
            and self.fusion_hash == fusion_hash
            and self.verification_required
        )

    def assert_current(
        self,
        *,
        task_id: str,
        verification_scope_id: str,
        task_definition_revision: int,
        source_attempt: int,
        steer_version: int,
        fusion_hash: str,
    ) -> None:
        if not self.is_current(
            task_id=task_id,
            verification_scope_id=verification_scope_id,
            task_definition_revision=task_definition_revision,
            source_attempt=source_attempt,
            steer_version=steer_version,
            fusion_hash=fusion_hash,
        ):
            raise StaleVerificationHandoff(
                "verification handoff does not match the current "
                "scope/revision/attempt/steer/fusion"
            )

    require_current = assert_current

    def to_protocol(self, *, include_hash: bool = True) -> dict[str, Any]:
        value = {
            "schemaVersion": self.schema_version,
            "taskID": self.task_id,
            "verificationScopeID": self.verification_scope_id,
            "taskDefinitionRevision": self.task_definition_revision,
            "sourceAttempt": self.source_attempt,
            "steerVersion": self.steer_version,
            "fusionHash": self.fusion_hash,
            "verificationRequired": self.verification_required,
        }
        if include_hash:
            value["tokenSHA256"] = self.token_sha256
        return value


@dataclass(frozen=True, slots=True)
class FusionResult:
    status: FusionStatus
    policy_version: str
    input_set_sha256: str
    fusion_hash: str
    task_id: str | None
    verification_scope_id: str | None
    task_definition_revision: int | None
    source_attempt: int | None
    steer_version: int | None
    claims: tuple[FusedClaim, ...]
    conflicts: tuple[FusionConflict, ...]
    contribution_provenance: tuple[ContributionProvenance, ...]
    missing_required_claims: tuple[str, ...] = ()
    verification_required: bool = True
    schema_version: str = FUSION_RESULT_VERSION

    @property
    def ready_for_verification(self) -> bool:
        return self.status in {FusionStatus.COMPATIBLE, FusionStatus.COMPLEMENTARY}

    @property
    def normalized_claims(self) -> dict[str, JSONValue]:
        return {item.claim_key: _thaw_json(item.value) for item in self.claims}

    def verification_handoff(self) -> VerificationHandoffToken:
        if not self.ready_for_verification:
            raise FusionValidationError("contradictory or insufficient fusion cannot be handed off")
        if (
            self.task_id is None
            or self.verification_scope_id is None
            or self.task_definition_revision is None
            or self.source_attempt is None
            or self.steer_version is None
        ):
            raise FusionValidationError("fusion has no complete verification provenance")
        return VerificationHandoffToken(
            task_id=self.task_id,
            verification_scope_id=self.verification_scope_id,
            task_definition_revision=self.task_definition_revision,
            source_attempt=self.source_attempt,
            steer_version=self.steer_version,
            fusion_hash=self.fusion_hash,
            verification_required=self.verification_required,
        )

    to_verification_handoff = verification_handoff

    def to_protocol(self) -> dict[str, Any]:
        return {
            "schemaVersion": self.schema_version,
            "status": self.status.value,
            "policyVersion": self.policy_version,
            "inputSetSHA256": self.input_set_sha256,
            "fusionHash": self.fusion_hash,
            "taskID": self.task_id,
            "verificationScopeID": self.verification_scope_id,
            "taskDefinitionRevision": self.task_definition_revision,
            "sourceAttempt": self.source_attempt,
            "steerVersion": self.steer_version,
            "claims": [item.to_protocol() for item in self.claims],
            "conflicts": [item.to_protocol() for item in self.conflicts],
            "contributions": [item.to_protocol() for item in self.contribution_provenance],
            "missingRequiredClaims": list(self.missing_required_claims),
            "verificationRequired": self.verification_required,
            "readyForVerification": self.ready_for_verification,
        }


class FusionEngine:
    """Arrival-order-invariant semantic fusion with no last-write-wins path."""

    def __init__(self, policy: FusionPolicy | None = None) -> None:
        self.policy = policy or FusionPolicy()

    def fuse(self, contributions: Iterable[ResultContribution]) -> FusionResult:
        supplied = tuple(contributions)
        if len(supplied) > self.policy.max_contributions:
            raise FusionValidationError("contribution count exceeds the configured limit")

        by_id: dict[str, ResultContribution] = {}
        for contribution in supplied:
            existing = by_id.get(contribution.contribution_id)
            if existing is not None and existing.digest != contribution.digest:
                raise FusionValidationError(
                    "contribution identifier was replayed with different canonical content"
                )
            by_id[contribution.contribution_id] = contribution
        ordered = tuple(
            sorted(
                by_id.values(),
                key=lambda item: (item.worker_id, item.run_id, item.contribution_id, item.digest),
            )
        )
        encoded_inputs = canonical_json_bytes([item.to_protocol() for item in ordered])
        if len(encoded_inputs) > self.policy.max_input_bytes:
            raise FusionValidationError("fusion input exceeds the configured byte limit")

        task_id, scope_id, revision, attempt, steer_version = self._common_provenance(ordered)
        all_provenance = tuple(
            sorted(
                (ContributionProvenance.from_contribution(item) for item in ordered),
                key=_provenance_sort_key,
            )
        )
        asserted = tuple(item for item in ordered if item.state is ContributionState.ASSERTED)
        claim_count = sum(len(item.claims) for item in asserted)
        if claim_count > self.policy.max_claims:
            raise FusionValidationError("claim count exceeds the configured limit")

        variants_by_claim: dict[str, dict[str, tuple[Any, list[ContributionProvenance]]]] = (
            defaultdict(dict)
        )
        for contribution in asserted:
            provenance = ContributionProvenance.from_contribution(contribution)
            for claim_key, frozen_value in contribution.claims.items():
                value = _thaw_json(frozen_value)
                value_hash = _sha256(value)
                existing = variants_by_claim[claim_key].get(value_hash)
                if existing is None:
                    variants_by_claim[claim_key][value_hash] = (value, [provenance])
                else:
                    existing[1].append(provenance)

        claims: list[FusedClaim] = []
        conflicts: list[FusionConflict] = []
        for claim_key in sorted(variants_by_claim):
            variants = variants_by_claim[claim_key]
            if len(variants) == 1:
                value_hash, (value, sources) = next(iter(variants.items()))
                claims.append(
                    FusedClaim(
                        claim_key=claim_key,
                        value=value,
                        value_sha256=value_hash,
                        provenance=tuple(sources),
                    )
                )
                continue
            conflicts.append(
                FusionConflict(
                    claim_key=claim_key,
                    variants=tuple(
                        ConflictVariant(
                            value=value,
                            value_sha256=value_hash,
                            provenance=tuple(sources),
                        )
                        for value_hash, (value, sources) in variants.items()
                    ),
                )
            )

        present_keys = set(variants_by_claim)
        missing = tuple(sorted(self.policy.required_claim_keys - present_keys))
        insufficient = (
            len(asserted) < self.policy.minimum_contributions
            or not present_keys
            or bool(missing)
            or (self.policy.require_all_contributions_asserted and len(asserted) != len(ordered))
        )
        if conflicts:
            status = FusionStatus.CONTRADICTORY
        elif insufficient:
            status = FusionStatus.INSUFFICIENT
        else:
            claim_set_hashes = {_sha256(item.claims_protocol()) for item in asserted}
            status = (
                FusionStatus.COMPATIBLE
                if len(claim_set_hashes) == 1
                else FusionStatus.COMPLEMENTARY
            )

        input_set_sha256 = _sha256(sorted(item.digest for item in ordered))
        fusion_body = {
            "schemaVersion": FUSION_RESULT_VERSION,
            "policyVersion": self.policy.policy_version,
            "inputSetSHA256": input_set_sha256,
            "taskID": task_id,
            "verificationScopeID": scope_id,
            "taskDefinitionRevision": revision,
            "sourceAttempt": attempt,
            "steerVersion": steer_version,
            "status": status.value,
            "claims": [item.to_protocol() for item in claims],
            "conflicts": [item.to_protocol() for item in conflicts],
            "contributions": [item.to_protocol() for item in all_provenance],
            "missingRequiredClaims": list(missing),
            "verificationRequired": True,
        }
        return FusionResult(
            status=status,
            policy_version=self.policy.policy_version,
            input_set_sha256=input_set_sha256,
            fusion_hash=_sha256(fusion_body),
            task_id=task_id,
            verification_scope_id=scope_id,
            task_definition_revision=revision,
            source_attempt=attempt,
            steer_version=steer_version,
            claims=tuple(claims),
            conflicts=tuple(conflicts),
            contribution_provenance=all_provenance,
            missing_required_claims=missing,
            verification_required=True,
        )

    @staticmethod
    def _common_provenance(
        contributions: tuple[ResultContribution, ...],
    ) -> tuple[str | None, str | None, int | None, int | None, int | None]:
        if not contributions:
            return None, None, None, None, None
        identities = {
            (
                item.task_id,
                item.verification_scope_id,
                item.task_definition_revision,
                item.source_attempt,
                item.steer_version,
            )
            for item in contributions
        }
        if len(identities) != 1:
            raise FusionValidationError(
                "contributions do not share task/scope/revision/attempt/steer provenance"
            )
        task_id, scope_id, revision, attempt, steer_version = next(iter(identities))
        return task_id, scope_id, revision, attempt, steer_version


def fuse_contributions(
    contributions: Iterable[ResultContribution],
    policy: FusionPolicy | None = None,
) -> FusionResult:
    return FusionEngine(policy).fuse(contributions)


# Short aliases retain readable call sites without weakening the versioned contract names.
Contribution = ResultContribution
FusionContribution = ResultContribution
VerificationHandoff = VerificationHandoffToken
