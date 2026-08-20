"""Semantic communication policy and auditable support accounting for Bridge."""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
from enum import StrEnum


class SupportClass(StrEnum):
    TRANSMITTED = "transmitted"
    PRIOR = "prior"
    SYNERGISTIC = "synergistic"
    INVENTED = "invented"
    CONTRADICTED = "contradicted"


class TransferMode(StrEnum):
    NATURAL_LANGUAGE = "naturalLanguage"
    STRUCTURED_STATE = "structuredState"
    SYMBOLIC = "symbolic"
    LOSSLESS = "lossless"


@dataclass(frozen=True, slots=True)
class SupportContribution:
    unit_id: str
    category: SupportClass
    source_ids: tuple[str, ...] = ()
    confidence: float = 1.0

    def __post_init__(self) -> None:
        if not self.unit_id.strip():
            raise ValueError("unit_id must not be empty")
        if len(self.source_ids) != len(set(self.source_ids)):
            raise ValueError("source_ids must not contain duplicates")
        if any(not source_id.strip() for source_id in self.source_ids):
            raise ValueError("source_ids must not contain empty values")
        if not 0 <= self.confidence <= 1:
            raise ValueError("confidence must be between zero and one")
        if self.category in {
            SupportClass.TRANSMITTED,
            SupportClass.SYNERGISTIC,
            SupportClass.CONTRADICTED,
        } and not self.source_ids:
            raise ValueError(f"{self.category.value} support requires at least one source")

    def to_protocol(self) -> dict[str, object]:
        return {
            "unitID": self.unit_id,
            "category": self.category.value,
            "sourceIDs": list(self.source_ids),
            "confidence": self.confidence,
        }


@dataclass(frozen=True, slots=True)
class SupportAccounting:
    exchange_id: str
    contributions: tuple[SupportContribution, ...]

    def __post_init__(self) -> None:
        if not self.exchange_id.strip():
            raise ValueError("exchange_id must not be empty")
        unit_ids = [item.unit_id for item in self.contributions]
        if len(unit_ids) != len(set(unit_ids)):
            raise ValueError("support accounting unit IDs must be unique")

    @property
    def counts(self) -> dict[str, int]:
        observed = Counter(item.category.value for item in self.contributions)
        return {category.value: observed[category.value] for category in SupportClass}

    def to_protocol(self) -> dict[str, object]:
        return {
            "schemaVersion": 1,
            "exchangeID": self.exchange_id,
            "counts": self.counts,
            "contributions": [item.to_protocol() for item in self.contributions],
        }


@dataclass(frozen=True, slots=True)
class SemanticTransferRequest:
    message_id: str
    payload_bytes: int
    budget_bytes: int
    receiver_prior_overlap: float
    novelty: float
    exactness: float
    integrity_required: bool = False
    symbolic_available: bool = False
    lossless_available: bool = True

    def __post_init__(self) -> None:
        if not self.message_id.strip():
            raise ValueError("message_id must not be empty")
        if self.payload_bytes < 0:
            raise ValueError("payload_bytes must not be negative")
        if self.budget_bytes < 1:
            raise ValueError("budget_bytes must be positive")
        for name in ("receiver_prior_overlap", "novelty", "exactness"):
            value = getattr(self, name)
            if not 0 <= value <= 1:
                raise ValueError(f"{name} must be between zero and one")


@dataclass(frozen=True, slots=True)
class SemanticTransferDecision:
    message_id: str
    mode: TransferMode
    include_source_digest: bool
    budget_satisfied: bool
    fallback_required: bool
    rationale: tuple[str, ...]

    def to_protocol(self) -> dict[str, object]:
        return {
            "schemaVersion": 1,
            "messageID": self.message_id,
            "mode": self.mode.value,
            "includeSourceDigest": self.include_source_digest,
            "budgetSatisfied": self.budget_satisfied,
            "fallbackRequired": self.fallback_required,
            "rationale": list(self.rationale),
        }


class SemanticBridgePolicy:
    """Choose communication form from exactness, prior overlap, novelty, and budget."""

    def decide(self, request: SemanticTransferRequest) -> SemanticTransferDecision:
        fits_budget = request.payload_bytes <= request.budget_bytes
        rationale: list[str] = []

        if request.integrity_required or request.exactness >= 0.85:
            if request.lossless_available:
                mode = TransferMode.LOSSLESS
                fallback_required = not fits_budget
                rationale.append("exact or integrity-sensitive data requires lossless transfer")
                if fallback_required:
                    rationale.append("lossless payload exceeds budget and must use source fallback")
            else:
                mode = TransferMode.STRUCTURED_STATE
                fallback_required = True
                rationale.append(
                    "lossless transfer is unavailable; canonical source fallback is required"
                )
            return SemanticTransferDecision(
                request.message_id,
                mode,
                include_source_digest=True,
                budget_satisfied=fits_budget,
                fallback_required=fallback_required,
                rationale=tuple(rationale),
            )

        budget_ratio = request.budget_bytes / max(request.payload_bytes, 1)
        if request.symbolic_available and request.exactness >= 0.6 and budget_ratio < 0.6:
            mode = TransferMode.SYMBOLIC
            rationale.append("symbolic transfer preserves structure under a tight byte budget")
        elif request.receiver_prior_overlap >= 0.7 and request.novelty <= 0.5:
            mode = TransferMode.STRUCTURED_STATE
            rationale.append("high receiver prior allows a compact structured delta")
        elif request.novelty >= 0.65:
            mode = TransferMode.NATURAL_LANGUAGE
            rationale.append("high novelty benefits from explanatory natural-language context")
        else:
            mode = TransferMode.STRUCTURED_STATE
            rationale.append("structured state is the stable default for mixed semantic content")

        return SemanticTransferDecision(
            request.message_id,
            mode,
            include_source_digest=True,
            budget_satisfied=fits_budget,
            fallback_required=False,
            rationale=tuple(rationale),
        )
