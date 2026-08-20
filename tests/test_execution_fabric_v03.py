from __future__ import annotations

from dataclasses import replace
from itertools import permutations

import pytest

from project_supervisor.fabric.execution import (
    ChildWorkProposal,
    SpawnContext,
    SpawnDisposition,
    SpawnGovernor,
    SpawnPolicy,
    SpawnReason,
)
from project_supervisor.fabric.fusion import (
    ContributionState,
    FusionEngine,
    FusionPolicy,
    FusionStatus,
    FusionValidationError,
    ResultContribution,
    StaleVerificationHandoff,
)


def proposal(**overrides: object) -> ChildWorkProposal:
    values: dict[str, object] = {
        "proposal_id": "proposal-1",
        "goal_id": "goal-1",
        "parent_task_id": "task-parent",
        "proposal_key": "inspect-follow-up",
        "title": "Inspect the deterministic follow-up",
        "description": "Collect bounded evidence without granting new authority.",
        "steer_version": 3,
        "depth": 2,
        "source_run_id": "run-parent",
        "source_attempt": 1,
        "task_definition_revision": 4,
        "provider": "local",
        "requested_worker_slots": 1,
        "estimated_tokens": 100,
        "estimated_seconds": 5.0,
        "estimated_cost_usd": 0.25,
        "payload": {"mode": "readOnly", "targets": ["a", "b"]},
    }
    values.update(overrides)
    return ChildWorkProposal(**values)  # type: ignore[arg-type]


def spawn_context(**overrides: object) -> SpawnContext:
    values: dict[str, object] = {
        "goal_id": "goal-1",
        "goal_state": "running",
        "current_steer_version": 3,
        "consumed_tokens": 1_000,
        "reserved_tokens": 200,
        "elapsed_seconds": 10.0,
        "observed_cost_usd": 1.0,
        "reserved_cost_usd": 0.5,
    }
    values.update(overrides)
    return SpawnContext(**values)  # type: ignore[arg-type]


def spawn_policy(**overrides: object) -> SpawnPolicy:
    values: dict[str, object] = {
        "max_depth": 3,
        "max_children_per_parent": 3,
        "max_total_children": 12,
        "max_parallel_tasks": 4,
        "max_parallel_worker_slots": 6,
        "provider_parallel_limits": {"local": 2},
        "max_total_tokens": 2_000,
        "max_elapsed_seconds": 60.0,
        "max_cost_usd": 3.0,
    }
    values.update(overrides)
    return SpawnPolicy(**values)  # type: ignore[arg-type]


def contribution(
    identity: str,
    claims: dict[str, object],
    *,
    worker_id: str | None = None,
    state: ContributionState = ContributionState.ASSERTED,
    attempt: int = 2,
    revision: int = 7,
    scope_id: str = "verification-scope-2",
    steer_version: int = 3,
) -> ResultContribution:
    return ResultContribution(
        contribution_id=f"contribution-{identity}",
        task_id="task-fusion",
        run_id=f"run-{identity}",
        worker_id=worker_id or f"worker-{identity}",
        verification_scope_id=scope_id,
        task_definition_revision=revision,
        source_attempt=attempt,
        source_result_sha256="0" * 64,
        steer_version=steer_version,
        role="reviewer" if identity == "b" else "panelist",
        state=state,
        claims=claims,
        evidence={"source": identity},
    )


def test_eligible_proposal_is_only_input_to_an_atomic_repository_commit() -> None:
    candidate = proposal()

    decision = SpawnGovernor(spawn_policy()).evaluate(candidate, spawn_context())

    assert decision.disposition is SpawnDisposition.ELIGIBLE
    assert decision.accepted
    assert decision.may_attempt_atomic_commit
    assert not decision.canonical_task_created
    assert not candidate.canonical_creation_authority
    assert decision.budget_delta.child_tasks == 1
    assert decision.budget_delta.worker_slots == 1


def test_fanout_is_bounded_per_parent_without_mutating_context() -> None:
    policy = spawn_policy(max_children_per_parent=2)
    first = proposal(proposal_id="proposal-a", proposal_key="a")
    second = proposal(proposal_id="proposal-b", proposal_key="b")
    third = proposal(proposal_id="proposal-c", proposal_key="c")

    assert policy.evaluate(first, spawn_context(children_for_parent=0)).eligible
    assert policy.evaluate(second, spawn_context(children_for_parent=1)).eligible
    rejected = policy.evaluate(third, spawn_context(children_for_parent=2))

    assert rejected.disposition is SpawnDisposition.REJECTED
    assert SpawnReason.CHILDREN_LIMIT in rejected.reasons
    assert spawn_context(children_for_parent=2).children_for_parent == 2


@pytest.mark.parametrize(
    ("candidate", "context", "policy", "reason"),
    [
        (
            proposal(depth=4),
            spawn_context(),
            spawn_policy(max_depth=3),
            SpawnReason.DEPTH_LIMIT,
        ),
        (
            proposal(),
            spawn_context(total_children=12),
            spawn_policy(max_total_children=12),
            SpawnReason.TOTAL_CHILDREN_LIMIT,
        ),
        (
            proposal(),
            spawn_context(active_tasks=4),
            spawn_policy(max_parallel_tasks=4),
            SpawnReason.TASK_CONCURRENCY_LIMIT,
        ),
        (
            proposal(requested_worker_slots=2),
            spawn_context(active_worker_slots=5),
            spawn_policy(max_parallel_worker_slots=6),
            SpawnReason.WORKER_CONCURRENCY_LIMIT,
        ),
        (
            proposal(requested_worker_slots=2),
            spawn_context(active_by_provider={"local": 1}),
            spawn_policy(provider_parallel_limits={"local": 2}),
            SpawnReason.PROVIDER_CONCURRENCY_LIMIT,
        ),
        (
            proposal(estimated_tokens=801),
            spawn_context(consumed_tokens=1_000, reserved_tokens=200),
            spawn_policy(max_total_tokens=2_000),
            SpawnReason.TOKEN_BUDGET_EXCEEDED,
        ),
        (
            proposal(estimated_seconds=51),
            spawn_context(elapsed_seconds=10),
            spawn_policy(max_elapsed_seconds=60),
            SpawnReason.TIME_BUDGET_EXCEEDED,
        ),
        (
            proposal(estimated_cost_usd=1.51),
            spawn_context(observed_cost_usd=1, reserved_cost_usd=0.5),
            spawn_policy(max_cost_usd=3),
            SpawnReason.COST_BUDGET_EXCEEDED,
        ),
    ],
)
def test_structural_budget_and_concurrency_gates_fail_closed(
    candidate: ChildWorkProposal,
    context: SpawnContext,
    policy: SpawnPolicy,
    reason: SpawnReason,
) -> None:
    decision = policy.evaluate(candidate, context)

    assert decision.disposition is SpawnDisposition.REJECTED
    assert reason in decision.reasons
    assert not decision.may_attempt_atomic_commit


@pytest.mark.parametrize(
    ("proposal_changes", "context_changes", "policy_changes", "reason"),
    [
        ({"estimated_tokens": None}, {}, {}, SpawnReason.TOKEN_BUDGET_UNOBSERVABLE),
        ({"estimated_seconds": None}, {}, {}, SpawnReason.TIME_BUDGET_UNOBSERVABLE),
        ({"estimated_cost_usd": None}, {}, {}, SpawnReason.COST_BUDGET_UNOBSERVABLE),
        ({}, {"consumed_tokens": None}, {}, SpawnReason.TOKEN_BUDGET_UNOBSERVABLE),
        ({}, {"elapsed_seconds": None}, {}, SpawnReason.TIME_BUDGET_UNOBSERVABLE),
        ({}, {"observed_cost_usd": None}, {}, SpawnReason.COST_BUDGET_UNOBSERVABLE),
    ],
)
def test_configured_budgets_never_interpret_unknown_as_zero(
    proposal_changes: dict[str, object],
    context_changes: dict[str, object],
    policy_changes: dict[str, object],
    reason: SpawnReason,
) -> None:
    decision = spawn_policy(**policy_changes).evaluate(
        proposal(**proposal_changes), spawn_context(**context_changes)
    )

    assert reason in decision.reasons
    assert not decision.eligible


def test_duplicate_replay_and_conflicting_key_have_distinct_outcomes() -> None:
    original = proposal()
    replay = proposal(proposal_id="proposal-retry")
    replay_context = spawn_context(
        existing_proposal_digests={original.proposal_key: original.request_digest}
    )

    repeated = spawn_policy().evaluate(replay, replay_context)
    conflict = spawn_policy().evaluate(
        proposal(proposal_id="proposal-conflict", description="Different immutable request"),
        replay_context,
    )

    assert repeated.disposition is SpawnDisposition.REPLAY
    assert repeated.reasons == (SpawnReason.DUPLICATE_REPLAY,)
    assert repeated.idempotent_replay
    assert not repeated.may_attempt_atomic_commit
    assert conflict.disposition is SpawnDisposition.REJECTED
    assert SpawnReason.IDEMPOTENCY_CONFLICT in conflict.reasons


@pytest.mark.parametrize("goal_state", ["softPaused", "hardPaused", "paused", "halted", "stopped"])
def test_pause_halt_and_stop_never_admit_child_work(goal_state: str) -> None:
    decision = spawn_policy().evaluate(proposal(), spawn_context(goal_state=goal_state))

    assert decision.disposition is SpawnDisposition.REJECTED
    assert decision.primary_reason is SpawnReason.GOAL_NOT_RUNNING


def test_goal_context_cannot_be_reused_for_another_goal() -> None:
    decision = spawn_policy().evaluate(proposal(), spawn_context(goal_id="goal-other"))

    assert decision.disposition is SpawnDisposition.REJECTED
    assert SpawnReason.GOAL_ID_MISMATCH in decision.reasons


def test_stale_steer_task_lock_and_open_circuit_are_independent_fail_closed_gates() -> None:
    stale = spawn_policy().evaluate(proposal(steer_version=2), spawn_context())
    locked = spawn_policy().evaluate(
        proposal(task_lock_key="workspace:one"),
        spawn_context(locked_task_keys=frozenset({"workspace:one"})),
    )
    circuit = spawn_policy().evaluate(
        proposal(circuit_key="provider:local"),
        spawn_context(circuit_failures={"provider:local": 3}),
    )

    assert SpawnReason.STALE_STEER_VERSION in stale.reasons
    assert SpawnReason.TASK_LOCKED in locked.reasons
    assert SpawnReason.CIRCUIT_OPEN in circuit.reasons
    assert not stale.canonical_task_created
    assert not locked.canonical_task_created
    assert not circuit.canonical_task_created


def test_fusion_is_arrival_order_invariant_and_normalizes_nested_json() -> None:
    contributions = (
        contribution("a", {"ready": True, "metadata": {"b": 2, "a": 1}}),
        contribution("b", {"metadata": {"a": 1, "b": 2}, "ready": True}),
        contribution("c", {"artifact": {"sha256": "abc"}}),
    )

    results = [FusionEngine().fuse(order) for order in permutations(contributions)]

    assert {item.status for item in results} == {FusionStatus.COMPLEMENTARY}
    assert len({item.input_set_sha256 for item in results}) == 1
    assert len({item.fusion_hash for item in results}) == 1
    assert len({str(item.to_protocol()) for item in results}) == 1
    assert results[0].normalized_claims == {
        "artifact": {"sha256": "abc"},
        "metadata": {"a": 1, "b": 2},
        "ready": True,
    }
    assert results[0].verification_required


def test_two_true_and_one_false_remains_an_explicit_conflict() -> None:
    result = FusionEngine(FusionPolicy(minimum_contributions=3)).fuse(
        (
            contribution("a", {"complete": True}),
            contribution("b", {"complete": True}),
            contribution("c", {"complete": False}),
        )
    )

    assert result.status is FusionStatus.CONTRADICTORY
    assert "complete" not in result.normalized_claims
    assert len(result.conflicts) == 1
    conflict = result.conflicts[0]
    assert conflict.claim_key == "complete"
    by_value = {variant.value: variant for variant in conflict.variants}
    assert {item.worker_id for item in by_value[True].provenance} == {"worker-a", "worker-b"}
    assert {item.worker_id for item in by_value[False].provenance} == {"worker-c"}
    assert not result.ready_for_verification
    with pytest.raises(FusionValidationError, match="cannot be handed off"):
        result.verification_handoff()


def test_complementary_claims_are_unioned_without_losing_provenance() -> None:
    result = FusionEngine().fuse(
        (
            contribution("a", {"testsPassed": 12}),
            contribution("b", {"artifactExists": True}),
        )
    )

    assert result.status is FusionStatus.COMPLEMENTARY
    assert result.normalized_claims == {"artifactExists": True, "testsPassed": 12}
    assert {claim.claim_key for claim in result.claims} == {"artifactExists", "testsPassed"}
    assert {source.run_id for source in result.contribution_provenance} == {"run-a", "run-b"}


def test_insufficient_contributions_missing_claims_and_failed_runs_fail_closed() -> None:
    too_few = FusionEngine(FusionPolicy(minimum_contributions=2)).fuse(
        (contribution("a", {"complete": True}),)
    )
    missing = FusionEngine(FusionPolicy(required_claim_keys=frozenset({"verified"}))).fuse(
        (contribution("a", {"complete": True}),)
    )
    failed = FusionEngine().fuse(
        (
            contribution("a", {"complete": True}),
            contribution("b", {}, state=ContributionState.FAILED),
        )
    )

    assert too_few.status is FusionStatus.INSUFFICIENT
    assert missing.status is FusionStatus.INSUFFICIENT
    assert missing.missing_required_claims == ("verified",)
    assert failed.status is FusionStatus.INSUFFICIENT
    assert failed.verification_required


def test_duplicate_contribution_replay_is_idempotent_but_conflicting_identity_is_rejected() -> None:
    original = contribution("a", {"complete": True})
    single = FusionEngine().fuse((original,))
    replayed = FusionEngine().fuse((original, original))

    assert replayed.fusion_hash == single.fusion_hash
    assert replayed.input_set_sha256 == single.input_set_sha256
    conflicting = replace(original, claims={"complete": False})
    with pytest.raises(FusionValidationError, match="replayed with different"):
        FusionEngine().fuse((original, conflicting))


def test_verification_handoff_binds_scope_revision_attempt_steer_and_fusion_hash() -> None:
    result = FusionEngine().fuse((contribution("a", {"complete": True}),))
    token = result.verification_handoff()

    assert token.verification_required
    assert token.is_current(
        task_id="task-fusion",
        verification_scope_id="verification-scope-2",
        task_definition_revision=7,
        source_attempt=2,
        steer_version=3,
        fusion_hash=result.fusion_hash,
    )
    with pytest.raises(StaleVerificationHandoff, match="scope/revision/attempt/steer/fusion"):
        token.assert_current(
            task_id="task-fusion",
            verification_scope_id="verification-scope-2",
            task_definition_revision=7,
            source_attempt=3,
            steer_version=3,
            fusion_hash=result.fusion_hash,
        )
    with pytest.raises(StaleVerificationHandoff):
        token.assert_current(
            task_id="task-fusion",
            verification_scope_id="verification-scope-new",
            task_definition_revision=8,
            source_attempt=2,
            steer_version=3,
            fusion_hash="0" * 64,
        )

    with pytest.raises(StaleVerificationHandoff, match="steer"):
        token.assert_current(
            task_id="task-fusion",
            verification_scope_id="verification-scope-2",
            task_definition_revision=7,
            source_attempt=2,
            steer_version=4,
            fusion_hash=result.fusion_hash,
        )


@pytest.mark.parametrize("field", ["revision", "attempt", "scope_id", "steer_version"])
def test_contributions_from_different_execution_versions_never_fuse(field: str) -> None:
    changed = {
        "revision": 8,
        "attempt": 3,
        "scope_id": "verification-scope-new",
        "steer_version": 4,
    }
    with pytest.raises(FusionValidationError, match="scope/revision/attempt/steer"):
        FusionEngine().fuse(
            (
                contribution("a", {"complete": True}),
                contribution("b", {"complete": True}, **{field: changed[field]}),
            )
        )
