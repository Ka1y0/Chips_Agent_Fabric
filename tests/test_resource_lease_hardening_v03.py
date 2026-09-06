from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from project_supervisor.domain import (
    ExecutionTopology,
    Harness,
    ModelDescriptor,
    NodeState,
    Provider,
    ResourceState,
    TaskLabel,
    TaskRecord,
    TaskRequirements,
    TaskState,
    WorkerSnapshot,
    WorkerState,
)
from project_supervisor.fabric.persistence import InteractionResourceRepository
from project_supervisor.store import StateStore


def _canonical_store(tmp_path) -> StateStore:
    store = StateStore(tmp_path / "state.db")
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    store.create_project(
        project_id="project-resource-fence",
        name="Resource fence",
        root_path=str(workspace),
        goal="Fence semantic UI resources",
    )
    store.upsert_node(
        node_id="node-resource-fence",
        hostname="fixture",
        display_name="Fixture",
        role="worker",
        state=NodeState.ONLINE,
    )
    store.upsert_worker(
        WorkerSnapshot(
            id="worker-resource-fence",
            node_id="node-resource-fence",
            harness=Harness.MOCK,
            provider=Provider.LOCAL,
            model=ModelDescriptor("fixture", "Fixture", Provider.LOCAL),
            state=WorkerState.IDLE,
            node_state=NodeState.ONLINE,
            resource_state=ResourceState.AVAILABLE,
            capabilities=frozenset({"CONTROL_GUI"}),
            code_write_allowed=False,
            privacy_allowed=True,
        )
    )
    store.create_task(
        TaskRecord(
            id="task-resource-fence",
            project_id="project-resource-fence",
            title="Control fixture UI",
            description="Exercise generation fencing",
            state=TaskState.DRAFT,
            topology=ExecutionTopology.SINGLE,
            requirements=TaskRequirements(
                labels=frozenset({TaskLabel.FAST_ROUTING}),
                required_capabilities=frozenset({"CONTROL_GUI"}),
            ),
        ),
        "#resource-fence",
    )
    store.transition_task("task-resource-fence", TaskState.QUEUED)
    ready = store.transition_task("task-resource-fence", TaskState.READY)
    claim = store.claim_task_dispatch(
        "task-resource-fence",
        ["worker-resource-fence"],
        expected_version=int(ready["version"]),
        lease_owner_id="runtime-a",
        lease_ttl_seconds=30,
    )
    assert claim is not None
    run_id = str(claim["runIDs"]["worker-resource-fence"])
    generation = int(claim["leaseGeneration"])
    store.prepare_provider_job(
        run_id=run_id,
        adapter_type="semantic-ui-worker/v1",
        adapter_instance_id="worker-resource-fence",
        capabilities={},
        lease_owner_id="runtime-a",
        lease_generation=generation,
    )
    return store


def test_task_execution_takeover_fences_bound_gui_bundle(tmp_path) -> None:
    store = _canonical_store(tmp_path)
    run = store.list_worker_runs("task-resource-fence")[0]
    job = store.get_provider_job(str(run["id"]))
    resources = InteractionResourceRepository(store)
    resources.register(
        resource_key="browser:resource-fence",
        resource_type="browserContext",
        scope_id="resource-fence",
    )
    bundle = resources.acquire(
        ("browser:resource-fence",),
        owner_id=str(run["id"]),
        task_id="task-resource-fence",
        run_id=str(run["id"]),
    )
    assert bundle is not None
    assert bundle.task_id == "task-resource-fence"
    assert bundle.task_lease_owner_id == "runtime-a"
    assert bundle.task_lease_generation == int(job["launch_generation"])
    with store.connect() as connection:
        persisted = connection.execute(
            "SELECT task_lease_owner_id,task_lease_generation "
            "FROM interaction_resource_leases WHERE lease_group_id=?",
            (bundle.lease_group_id,),
        ).fetchone()
    assert dict(persisted) == {
        "task_lease_owner_id": "runtime-a",
        "task_lease_generation": int(job["launch_generation"]),
    }

    with store.transaction() as connection:
        connection.execute(
            "UPDATE task_execution_leases SET owner_id='runtime-b',generation=generation+1,"
            "heartbeat_at=?,expires_at=? WHERE task_id='task-resource-fence'",
            (
                datetime.now(UTC).isoformat(),
                (datetime.now(UTC) + timedelta(seconds=30)).isoformat(),
            ),
        )

    assert not resources.is_current(bundle)
    assert not resources.renew(bundle)
    assert not resources.release(bundle)
    with store.connect() as connection, pytest.raises(RuntimeError, match="generation"):
        resources.assert_current(connection, bundle)
    with pytest.raises(RuntimeError, match="stale task execution generation"):
        resources.acquire(
            ("browser:resource-fence",),
            owner_id=str(run["id"]),
            task_id="task-resource-fence",
            run_id=str(run["id"]),
        )
