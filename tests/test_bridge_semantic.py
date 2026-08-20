from __future__ import annotations

import json
from pathlib import Path

import jsonschema
import pytest

from project_supervisor.bridge.semantic import (
    SemanticBridgePolicy,
    SemanticTransferRequest,
    SupportAccounting,
    SupportClass,
    SupportContribution,
    TransferMode,
)

ROOT = Path(__file__).resolve().parents[1]


def request(**changes: object) -> SemanticTransferRequest:
    values = {
        "message_id": "message-1",
        "payload_bytes": 1_000,
        "budget_bytes": 2_000,
        "receiver_prior_overlap": 0.2,
        "novelty": 0.5,
        "exactness": 0.5,
    }
    values.update(changes)
    return SemanticTransferRequest(**values)


def test_support_accounting_uses_typed_categories_and_validates_schema() -> None:
    accounting = SupportAccounting(
        "exchange-1",
        (
            SupportContribution("claim-1", SupportClass.TRANSMITTED, ("source-1",)),
            SupportContribution("claim-2", SupportClass.PRIOR),
            SupportContribution("claim-3", SupportClass.SYNERGISTIC, ("source-1", "prior-1")),
            SupportContribution("claim-4", SupportClass.INVENTED, confidence=0.2),
            SupportContribution("claim-5", SupportClass.CONTRADICTED, ("source-2",)),
        ),
    )
    protocol = accounting.to_protocol()
    schema = json.loads((ROOT / "schemas/bridge-support-accounting-v1.schema.json").read_text())

    jsonschema.Draft202012Validator(schema).validate(protocol)
    assert protocol["counts"] == {
        "transmitted": 1,
        "prior": 1,
        "synergistic": 1,
        "invented": 1,
        "contradicted": 1,
    }


def test_integrity_sensitive_transfer_requires_lossless_and_source_fallback_when_over_budget(
) -> None:
    decision = SemanticBridgePolicy().decide(
        request(payload_bytes=8_000, budget_bytes=1_000, integrity_required=True)
    )

    assert decision.mode is TransferMode.LOSSLESS
    assert not decision.budget_satisfied
    assert decision.fallback_required
    assert decision.include_source_digest


def test_high_prior_overlap_uses_compact_structured_state() -> None:
    decision = SemanticBridgePolicy().decide(
        request(receiver_prior_overlap=0.9, novelty=0.2)
    )

    assert decision.mode is TransferMode.STRUCTURED_STATE


def test_high_novelty_uses_explanatory_natural_language() -> None:
    decision = SemanticBridgePolicy().decide(request(novelty=0.9))

    assert decision.mode is TransferMode.NATURAL_LANGUAGE


def test_tight_budget_and_exact_structure_prefer_symbolic_mode() -> None:
    decision = SemanticBridgePolicy().decide(
        request(
            payload_bytes=10_000,
            budget_bytes=2_000,
            exactness=0.7,
            symbolic_available=True,
        )
    )

    assert decision.mode is TransferMode.SYMBOLIC
    assert not decision.budget_satisfied
    assert decision.fallback_required


def test_authority_bearing_content_is_rejected_to_canonical_fallback() -> None:
    decision = SemanticBridgePolicy().decide(request(authority_bearing=True))
    protocol = decision.to_protocol()

    assert not decision.bridge_allowed
    assert decision.fallback_required
    assert protocol["authoritative"] is False
    assert protocol["canonical"] is False
    assert "canonical structured fallback" in decision.rationale[0]


def test_source_backed_support_categories_require_sources() -> None:
    with pytest.raises(ValueError, match="requires at least one source"):
        SupportContribution("claim", SupportClass.TRANSMITTED)
