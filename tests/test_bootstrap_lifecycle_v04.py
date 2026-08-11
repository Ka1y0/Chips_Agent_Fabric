from __future__ import annotations

import json
import sqlite3
from datetime import UTC, datetime
from pathlib import Path

import jsonschema
import pytest

from bootstrap.chips import build_plan
from bootstrap.lifecycle import (
    BootstrapAuthorizationReference,
    BootstrapExecutionMode,
    BootstrapLifecycleStore,
    BootstrapOutcome,
    BootstrapStepResult,
)

ROOT = Path(__file__).parents[1]


def profile(*, installed: bool = False) -> dict[str, object]:
    return {
        "schemaVersion": 2,
        "host": {"operatingSystem": "Linux"},
        "fabric": {
            "existingStateDatabase": False,
            "supervisorExecutable": "/usr/bin/project-supervisor" if installed else None,
            "identityPresent": False,
        },
        "resources": {
            "aiCLIs": {"claude": "/usr/bin/claude", "grok": None},
            "modelServers": {"ollama": None},
        },
    }


def result(
    step_id: str,
    *,
    key: str,
    mutating: bool = False,
    evidence: dict[str, object] | None = None,
) -> BootstrapStepResult:
    authorization = (
        BootstrapAuthorizationReference(
            grant_reference=f"grant-{step_id}",
            capability="package.install",
            subject="node-fixture",
            verified_by="operator-fixture",
            authority_type="human",
        )
        if mutating
        else None
    )
    return BootstrapStepResult(
        schema_version=1,
        run_id="bootstrap-fixture",
        step_id=step_id,
        idempotency_key=key,
        outcome=BootstrapOutcome.SUCCEEDED,
        execution_mode=(
            BootstrapExecutionMode.EXTERNALLY_EXECUTED
            if mutating
            else BootstrapExecutionMode.OBSERVED_ONLY
        ),
        actor="operator-fixture",
        observed_at=datetime(2026, 8, 10, 1, 2, 3, tzinfo=UTC),
        evidence=evidence or {"check": "passed"},
        authorization=authorization,
    )


def validate_schema(name: str, payload: dict[str, object]) -> None:
    schema = json.loads((ROOT / "schemas" / name).read_text(encoding="utf-8"))
    jsonschema.Draft202012Validator(schema, format_checker=jsonschema.FormatChecker()).validate(
        payload
    )


def test_plan_v2_is_machine_valid_and_keeps_every_mutation_capability_scoped() -> None:
    plan = build_plan(profile())
    validate_schema("bootstrap-plan-v2.schema.json", plan)
    assert plan["mode"] == "dryRun"
    assert plan["automaticActions"] == []
    assert plan["failClosed"] is True
    mutating = [step for step in plan["steps"] if step["mutating"]]
    assert mutating
    assert all(step["requiredCapability"] for step in mutating)
    assert all(step["status"] in {"approvalRequired", "notNeeded"} for step in mutating)
    assert next(step for step in plan["steps"] if step["id"] == "run-acceptance")["dependsOn"] == [
        "verify-worker-contracts"
    ]


def test_durable_run_survives_restart_is_idempotent_and_has_valid_audit_chain(
    tmp_path: Path,
) -> None:
    state_path = tmp_path / "private" / "bootstrap.sqlite3"
    machine = profile()
    plan = build_plan(machine)
    repository = BootstrapLifecycleStore(state_path)
    first = repository.initialize_run(
        run_id="bootstrap-fixture", profile=machine, plan=plan, actor="bootstrap-agent"
    )
    validate_schema("bootstrap-run-v1.schema.json", first)
    assert first["hostMutationsPerformed"] is False
    assert first["privilegedExecutionImplemented"] is False
    assert first["auditChainValid"] is True
    assert first["steps"][0]["state"] == "ready"
    assert repository.verify_audit_chain("bootstrap-fixture")
    original_audit_count = len(first["audit"])

    reopened = BootstrapLifecycleStore(state_path)
    repeated = reopened.initialize_run(
        run_id="bootstrap-fixture", profile=machine, plan=plan, actor="bootstrap-agent"
    )
    assert len(repeated["audit"]) == original_audit_count
    assert repeated["planDigest"] == first["planDigest"]
    assert reopened.verify_audit_chain("bootstrap-fixture")

    advanced = reopened.record_result(result("validate-platform", key="result-validate"))
    install = next(step for step in advanced["steps"] if step["stepID"] == "install-supervisor")
    assert install["state"] == "awaitingApproval"
    assert advanced["state"] == "waiting"
    assert reopened.verify_audit_chain("bootstrap-fixture")

    idempotent = reopened.record_result(result("validate-platform", key="result-validate"))
    assert idempotent == advanced
    with pytest.raises(ValueError, match="different result"):
        reopened.record_result(
            result(
                "validate-platform",
                key="result-validate",
                evidence={"check": "different"},
            )
        )


def test_mutating_result_requires_matching_external_authority_and_executes_nothing(
    tmp_path: Path,
) -> None:
    repository = BootstrapLifecycleStore(tmp_path / "bootstrap.sqlite3")
    machine = profile()
    repository.initialize_run(run_id="bootstrap-fixture", profile=machine, plan=build_plan(machine))
    repository.record_result(result("validate-platform", key="validate"))

    without_authority = BootstrapStepResult(
        schema_version=1,
        run_id="bootstrap-fixture",
        step_id="install-supervisor",
        idempotency_key="install-without-authority",
        outcome=BootstrapOutcome.SUCCEEDED,
        execution_mode=BootstrapExecutionMode.OBSERVED_ONLY,
        actor="operator-fixture",
        observed_at=datetime(2026, 8, 10, tzinfo=UTC),
        evidence={"package": "reviewed"},
    )
    validate_schema("bootstrap-step-result-v1.schema.json", without_authority.to_protocol())
    with pytest.raises(ValueError, match="external authorized broker"):
        repository.record_result(without_authority)

    accepted = repository.record_result(
        result("install-supervisor", key="install-approved", mutating=True)
    )
    install = next(step for step in accepted["steps"] if step["stepID"] == "install-supervisor")
    assert install["state"] == "satisfied"
    assert install["authorizationReference"] == "grant-install-supervisor"
    assert accepted["hostMutationsPerformed"] is False
    assert repository.verify_audit_chain("bootstrap-fixture")


def test_results_reject_shell_or_credential_material_and_model_self_approval() -> None:
    with pytest.raises(ValueError, match="prohibited field"):
        result("validate-platform", key="unsafe-command", evidence={"command": "install"})
    with pytest.raises(ValueError, match="credential-shaped"):
        result("validate-platform", key="unsafe-token", evidence={"note": "token=not-safe"})
    with pytest.raises(ValueError, match="legitimate external trust root"):
        BootstrapAuthorizationReference(
            grant_reference="grant-1",
            capability="package.install",
            subject="node-1",
            verified_by="planner-1",
            authority_type="model",
        )


@pytest.mark.parametrize(
    "credential_key",
    [
        "accessToken",
        "access_token",
        "access-token",
        "access token",
        "apiKey",
        "api_key",
        "api-key",
        "api key",
        "clientSecret",
        "client_secret",
        "client-secret",
        "client secret",
        "privateKey",
        "private_key",
        "private-key",
        "private key",
        "refreshToken",
        "refresh_token",
        "refresh-token",
        "refresh token",
        "bearerToken",
        "bearer_token",
        "bearer-token",
        "bearer token",
        "password",
        "cookie",
        "sessionCookie",
        "session_cookie",
        "session-cookie",
        "session cookie",
        "authorization",
    ],
)
def test_common_credential_key_variants_are_rejected_before_persistence(
    tmp_path: Path,
    credential_key: str,
) -> None:
    state_path = tmp_path / "bootstrap.sqlite3"
    repository = BootstrapLifecycleStore(state_path)
    machine = profile()
    repository.initialize_run(
        run_id="bootstrap-fixture",
        profile=machine,
        plan=build_plan(machine),
    )
    marker = "SENSITIVE_FIXTURE_MARKER_7f91"

    with pytest.raises(ValueError, match="prohibited field"):
        result(
            "validate-platform",
            key="credential-variant",
            evidence={"nested": [{"provider": {credential_key: marker}}]},
        )

    with repository.connect() as connection:
        persisted_results = connection.execute(
            "SELECT COUNT(*) FROM bootstrap_steps WHERE result_json IS NOT NULL"
        ).fetchone()[0]
    database_bytes = b"".join(
        path.read_bytes()
        for path in state_path.parent.glob(f"{state_path.name}*")
        if path.is_file()
    )
    assert persisted_results == 0
    assert marker.encode() not in database_bytes


def test_benign_token_accounting_evidence_remains_allowed(tmp_path: Path) -> None:
    state_path = tmp_path / "bootstrap.sqlite3"
    repository = BootstrapLifecycleStore(state_path)
    machine = profile()
    repository.initialize_run(
        run_id="bootstrap-fixture",
        profile=machine,
        plan=build_plan(machine),
    )
    accounting = {
        "input_tokens": 120,
        "token_count": 150,
        "remaining_tokens": 880,
    }

    recorded = repository.record_result(
        result(
            "validate-platform",
            key="benign-token-accounting",
            evidence={"usage": accounting},
        )
    )

    assert recorded["steps"][0]["state"] == "satisfied"
    with repository.connect() as connection:
        persisted = json.loads(
            connection.execute(
                "SELECT result_json FROM bootstrap_steps WHERE step_id='validate-platform'"
            ).fetchone()[0]
        )
    assert persisted["evidence"]["usage"] == accounting


def test_audit_table_is_append_only_and_plan_drift_fails_closed(tmp_path: Path) -> None:
    state_path = tmp_path / "bootstrap.sqlite3"
    machine = profile()
    repository = BootstrapLifecycleStore(state_path)
    repository.initialize_run(run_id="bootstrap-fixture", profile=machine, plan=build_plan(machine))
    with repository.connect() as connection, pytest.raises(sqlite3.DatabaseError):
        connection.execute("DELETE FROM bootstrap_audit_events")

    changed = profile(installed=True)
    with pytest.raises(ValueError, match="different discovery/plan"):
        repository.initialize_run(
            run_id="bootstrap-fixture", profile=changed, plan=build_plan(changed)
        )
