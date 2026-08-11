from __future__ import annotations

import argparse
import asyncio
import json
import signal
import sqlite3
import sys
from collections.abc import Sequence
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from .config import ConfigurationError, default_data_dir, initialize_config, load_config
from .domain import (
    ApprovalState,
    ExecutionTopology,
    PermissionClass,
    TaskLabel,
    TaskRequirements,
)
from .runtime import AdapterRegistry, SupervisorRuntime
from .scheduler import DeterministicScheduler
from .store import StateStore


def _json_default(value: object) -> str:
    if isinstance(value, Path):
        return str(value)
    raise TypeError(f"cannot serialize {type(value).__name__}")


def _emit(value: Any, *, as_json: bool) -> None:
    if as_json:
        print(json.dumps(value, indent=2, sort_keys=True, default=_json_default))
        return
    if isinstance(value, str):
        print(value)
        return
    print(json.dumps(value, indent=2, sort_keys=True, default=_json_default))


def _load(args: argparse.Namespace, *, require_initialized: bool = True):
    config = load_config(data_dir=args.data_dir, config_path=args.config)
    if require_initialized and not config.database_path.is_file():
        raise FileNotFoundError(
            f"state database not initialized: {config.database_path}; run `project-supervisor init`"
        )
    return config


def _counts(store: StateStore) -> dict[str, int]:
    tables = ("projects", "nodes", "workers", "tasks", "events")
    with store.connect() as connection:
        return {
            table: int(connection.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0])
            for table in tables
        }


def _init(args: argparse.Namespace) -> int:
    data_dir = Path(args.data_dir or default_data_dir()).expanduser()
    if args.config is not None and Path(args.config).expanduser() != data_dir / "config.json":
        raise ConfigurationError("init writes configuration only to DATA_DIR/config.json")
    config = initialize_config(data_dir, force=args.force)
    StateStore(config.database_path)
    _emit(
        {
            "initialized": True,
            "dataDir": str(config.data_dir),
            "database": str(config.database_path),
            "config": str(config.config_path),
            "listen": f"{config.host}:{config.port}",
            "loopbackOnly": config.loopback_only,
        },
        as_json=args.json,
    )
    return 0


def _status(args: argparse.Namespace) -> int:
    config = _load(args)
    store = StateStore(config.database_path)
    result = {
        "status": "ready",
        "database": str(config.database_path),
        "listen": f"{config.host}:{config.port}",
        "loopbackOnly": config.loopback_only,
        "privateTransport": config.private_transport,
        "tlsEnabled": config.tls_certificate is not None,
        "readOnlyAPI": config.read_only_api,
        "highestEventSequence": store.highest_event_sequence(),
        "counts": _counts(store),
    }
    _emit(result, as_json=args.json)
    return 0


def _tasks(args: argparse.Namespace) -> int:
    config = _load(args)
    rows = StateStore(config.database_path).list_tasks(project_id=args.project)
    if args.state:
        rows = [row for row in rows if row["state"] == args.state]
    if args.limit is not None:
        rows = rows[: args.limit]
    if args.json:
        _emit(rows, as_json=True)
    elif not rows:
        print("No tasks.")
    else:
        print("REFERENCE\tSTATE\tTITLE\tUPDATED")
        for row in rows:
            print(f"{row['reference']}\t{row['state']}\t{row['title']}\t{row['updated_at']}")
    return 0


def _logs(args: argparse.Namespace) -> int:
    config = _load(args)
    events = StateStore(config.database_path).list_events(
        after_sequence=args.after,
        limit=args.limit,
        task_id=args.task,
    )
    if args.json:
        _emit(events, as_json=True)
    elif not events:
        print("No events.")
    else:
        for event in events:
            print(
                f"{event['sequence']}\t{event['created_at']}\t{event['severity']}\t"
                f"{event['kind']}\t{event['summary']}"
            )
    return 0


def _token_create(args: argparse.Namespace) -> int:
    config = _load(args)
    expires_at = None
    if args.expires_in_hours is not None:
        expires_at = datetime.now(UTC) + timedelta(hours=args.expires_in_hours)
    token_id, token = StateStore(config.database_path).issue_api_token(
        label=args.label,
        scopes=set(args.scope),
        expires_at=expires_at,
    )
    result = {
        "id": token_id,
        "token": token,
        "scopes": sorted(set(args.scope)),
        "expiresAt": expires_at.replace(microsecond=0).isoformat().replace("+00:00", "Z")
        if expires_at
        else None,
        "warning": "This token is shown once. Store it in an OS credential manager.",
    }
    _emit(result, as_json=args.json)
    return 0


def _project_create(args: argparse.Namespace) -> int:
    config = _load(args)
    project = StateStore(config.database_path).create_project(
        project_id=args.id,
        name=args.name,
        root_path=str(Path(args.root).expanduser().resolve()),
        goal=args.goal,
    )
    _emit(project, as_json=args.json)
    return 0


def _task_create(args: argparse.Namespace) -> int:
    config = _load(args)
    store = StateStore(config.database_path)
    runtime = SupervisorRuntime(
        store=store,
        scheduler=DeterministicScheduler(),
        adapters=AdapterRegistry(),
        evidence_root=config.data_dir / "evidence",
        worker_timeout_seconds=config.worker_timeout_seconds,
    )
    requirements = TaskRequirements(
        labels=frozenset(TaskLabel(value) for value in args.label),
        required_capabilities=frozenset(args.capability),
        permission_class=PermissionClass(args.permission),
        approval_state=(
            ApprovalState.PENDING
            if args.permission == PermissionClass.RED.value
            else ApprovalState.NOT_REQUIRED
        ),
        minimum_context_tokens=args.minimum_context_tokens,
        privacy_sensitive=args.privacy_sensitive,
        code_write_required=args.code_write_required,
        panel_size=args.panel_size,
        preferred_workers=tuple(args.preferred_worker),
    )
    task_id = asyncio.run(
        runtime.submit_task(
            project_id=args.project,
            title=args.title,
            description=args.description,
            requirements=requirements,
            topology=ExecutionTopology(args.topology),
            priority=args.priority,
            task_id=args.id,
            reference=args.reference,
        )
    )
    _emit(store.get_task(task_id), as_json=args.json)
    return 0


def _goal_service(args: argparse.Namespace):
    from .autonomy import GoalService

    config = _load(args)
    return GoalService(StateStore(config.database_path))


def _goal_create(args: argparse.Namespace) -> int:
    from .autonomy import GoalBudget

    budgets = GoalBudget(
        max_iterations=args.max_iterations,
        max_tasks=args.max_tasks,
        max_failures=args.max_failures,
        no_progress_limit=args.no_progress_limit,
        max_elapsed_seconds=args.max_elapsed_seconds,
        max_total_tokens=args.max_total_tokens,
        max_cost_usd=args.max_cost_usd,
    )
    row = _goal_service(args).create_goal(
        project_id=args.project,
        intent=args.intent,
        budgets=budgets,
        goal_id=args.id,
        actor="human:cli",
    )
    _emit(row, as_json=args.json)
    return 0


def _goal_list(args: argparse.Namespace) -> int:
    rows = _goal_service(args).list_goals(project_id=args.project)
    if args.state:
        rows = [row for row in rows if row["state"] == args.state]
    if args.limit is not None:
        rows = rows[: args.limit]
    if args.json:
        _emit(rows, as_json=True)
    elif not rows:
        print("No goals.")
    else:
        print("ID\tSTATE\tITERATION\tTASKS\tUPDATED\tINTENT")
        for row in rows:
            print(
                f"{row['id']}\t{row['state']}\t{row['iteration_count']}\t"
                f"{row['task_count']}\t{row['updated_at']}\t{row['intent']}"
            )
    return 0


def _goal_get(args: argparse.Namespace) -> int:
    _emit(_goal_service(args).get_goal(args.goal_id), as_json=args.json)
    return 0


def _goal_pause(args: argparse.Namespace) -> int:
    row = _goal_service(args).pause(
        args.goal_id,
        args.mode,
        reason=args.reason,
        actor="human:cli",
    )
    _emit(row, as_json=args.json)
    return 0


def _goal_resume(args: argparse.Namespace) -> int:
    row = _goal_service(args).resume(
        args.goal_id,
        reason=args.reason,
        actor="human:cli",
    )
    _emit(row, as_json=args.json)
    return 0


def _goal_steer(args: argparse.Namespace) -> int:
    row = _goal_service(args).steer(
        args.goal_id,
        args.instruction,
        priority=args.priority,
        preserve_valid_work=not args.do_not_preserve_valid_work,
        actor="human:cli",
    )
    _emit(row, as_json=args.json)
    return 0


def _goal_stop(args: argparse.Namespace) -> int:
    row = _goal_service(args).stop(
        args.goal_id,
        reason=args.reason,
        actor="human:cli",
    )
    _emit(row, as_json=args.json)
    return 0


def _telemetry(args: argparse.Namespace) -> int:
    from .telemetry import InvocationTelemetryRepository

    config = _load(args)
    repository = InvocationTelemetryRepository(StateStore(config.database_path))
    records = repository.list(goal_id=args.goal, task_id=args.task, limit=args.limit)
    result = {
        "aggregate": repository.aggregate(goal_id=args.goal, task_id=args.task).to_protocol(),
        "invocations": [record.to_protocol() for record in records],
    }
    _emit(result, as_json=args.json)
    return 0


def _key_value_pairs(values: Sequence[str], *, option: str) -> dict[str, str]:
    result: dict[str, str] = {}
    for item in values:
        key, separator, value = item.partition("=")
        if not separator or not key.strip() or not value.strip():
            raise ValueError(f"{option} requires KEY=VALUE")
        key = key.strip()
        if key in result:
            raise ValueError(f"duplicate {option} key: {key}")
        result[key] = value.strip()
    return result


def _parse_datetime(value: str | None, *, option: str) -> datetime | None:
    if value is None:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as error:
        raise ValueError(f"{option} must be an RFC 3339 timestamp") from error
    if parsed.tzinfo is None:
        raise ValueError(f"{option} must include a timezone")
    return parsed.astimezone(UTC)


def _resources(args: argparse.Namespace) -> int:
    from .resource_usage import ResourceUsageRepository

    config = _load(args)
    repository = ResourceUsageRepository(StateStore(config.database_path))
    filters = {
        "goal_id": args.goal,
        "task_id": args.task,
        "provider": args.provider,
        "model": args.model,
        "worker_id": args.worker,
        "account_scope": args.account_scope,
        "quota_pool_id": args.quota_pool,
        "observed_after": _parse_datetime(args.observed_after, option="--observed-after"),
        "observed_before": _parse_datetime(args.observed_before, option="--observed-before"),
    }
    snapshots = repository.list_snapshots(**filters, limit=args.limit)
    result = {
        "aggregate": repository.aggregate(**filters).to_protocol(),
        "snapshots": [snapshot.to_protocol() for snapshot in snapshots],
    }
    _emit(result, as_json=args.json)
    return 0


def _autonomous_status(args: argparse.Namespace) -> int:
    from .autonomous_host import AutonomousHostRepository

    config = _load(args)
    repository = AutonomousHostRepository(StateStore(config.database_path))
    hosts = [repository.get_host(args.host)] if args.host else repository.list_hosts()
    if args.goal:
        lease = repository.get_goal_lease(args.goal)
        leases = [lease] if lease is not None else []
    else:
        leases = repository.list_goal_leases(host_id=args.host)
    _emit({"hosts": hosts, "goalLeases": leases}, as_json=args.json)
    return 0


def _local_worker_tokens(endpoints: dict[str, str]) -> dict[str, str]:
    """Resolve Worker bearer credentials in memory without CLI token arguments."""

    from .credentials import local_worker_token

    return {worker_id: local_worker_token(account=worker_id) for worker_id in endpoints}


def _build_autonomous_host(args: argparse.Namespace):
    from .autonomous_host import AutonomousHost, AutonomousHostConfig
    from .autonomy import AutonomousIterationEngine, SupervisorRuntimeDispatcher
    from .autonomy_driver import ProductionAutonomyDriver, reconstruct_adapter_registry

    config = _load(args)
    store = StateStore(config.database_path)
    overrides = _key_value_pairs(args.executable_override, option="--executable-override")
    endpoints = _key_value_pairs(args.local_worker_endpoint, option="--local-worker-endpoint")
    drivers = _key_value_pairs(args.local_worker_driver, option="--local-worker-driver")
    adapters = reconstruct_adapter_registry(
        store,
        executable_overrides=overrides,
        local_worker_endpoints=endpoints,
        local_worker_tokens=_local_worker_tokens(endpoints),
        local_worker_drivers=drivers,
        allow_mock=args.allow_mock_worker,
    )
    runtime = SupervisorRuntime(
        store=store,
        scheduler=DeterministicScheduler(),
        adapters=adapters,
        evidence_root=config.data_dir / "evidence",
        worker_timeout_seconds=config.worker_timeout_seconds,
    )
    driver = ProductionAutonomyDriver(runtime)
    dispatcher = SupervisorRuntimeDispatcher(runtime)

    def engine_factory(_goal_id: str) -> AutonomousIterationEngine:
        return AutonomousIterationEngine(
            store=store,
            evaluator=driver,
            planner=driver,
            dispatcher=dispatcher,
            verifier=driver,
        )

    return (
        AutonomousHost(
            store=store,
            engine_factory=engine_factory,
            config=AutonomousHostConfig(
                max_concurrent_goals=args.max_concurrent_goals,
                poll_interval_seconds=args.poll_interval_seconds,
                heartbeat_interval_seconds=args.heartbeat_interval_seconds,
                lease_ttl_seconds=args.lease_ttl_seconds,
                shutdown_grace_seconds=args.shutdown_grace_seconds,
            ),
            host_id=args.host_id,
        ),
        runtime,
    )


async def _operate_autonomous_host(args: argparse.Namespace) -> dict[str, Any] | None:
    host, runtime = _build_autonomous_host(args)
    # Compensating recovery closes a crash window between a terminal Task transition and its
    # logical POST_TASK_USAGE_AUDIT. Audit keys are versioned and idempotent.
    await runtime.audit_pending_terminal_tasks()
    loop = asyncio.get_running_loop()
    installed: list[signal.Signals] = []
    for value in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(value, host.request_shutdown)
            installed.append(value)
        except (NotImplementedError, RuntimeError):
            # Windows event loops may not support signal handlers. KeyboardInterrupt remains safe.
            pass
    try:
        if args.autonomous_command == "run":
            return await host.run_goal(args.goal_id)
        await host.serve()
        return None
    finally:
        for value in installed:
            loop.remove_signal_handler(value)


def _autonomous_operate(args: argparse.Namespace) -> int:
    result = asyncio.run(_operate_autonomous_host(args))
    if result is not None:
        _emit(result, as_json=args.json)
    return 0


def _serve(args: argparse.Namespace) -> int:
    config = _load(args)
    from uvicorn import run

    from .api import APISettings, create_app

    allow_loopback = bool(args.allow_unauthenticated_loopback and config.loopback_only)
    app = create_app(
        StateStore(config.database_path),
        APISettings(
            bind_host=config.host,
            allow_unauthenticated_loopback=allow_loopback,
            allow_goal_mutations=not config.read_only_api,
        ),
    )
    run(
        app,
        host=config.host,
        port=config.port,
        log_level=config.log_level.lower(),
        ssl_certfile=str(config.tls_certificate) if config.tls_certificate else None,
        ssl_keyfile=str(config.tls_private_key) if config.tls_private_key else None,
    )
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="project-supervisor",
        description="Local-first CHIPS Agent Fabric control plane",
    )
    parser.add_argument("--data-dir", help="project-owned local state directory")
    parser.add_argument("--config", help="explicit JSON configuration file")
    parser.add_argument("--json", action="store_true", help="emit machine-readable JSON")
    subcommands = parser.add_subparsers(dest="command", required=True)

    init = subcommands.add_parser("init", help="initialize local configuration and state")
    init.add_argument("--force", action="store_true", help="replace config; never replaces state")
    init.set_defaults(handler=_init)

    status = subcommands.add_parser("status", help="show safe local supervisor status")
    status.set_defaults(handler=_status)

    tasks = subcommands.add_parser("tasks", help="list persisted tasks")
    tasks.add_argument("--project", help="filter by project ID")
    tasks.add_argument("--state", help="filter by exact task state")
    tasks.add_argument("--limit", type=int, help="maximum rows")
    tasks.set_defaults(handler=_tasks)

    logs = subcommands.add_parser("logs", help="read normalized event journal")
    logs.add_argument("--after", type=int, default=0, help="exclusive event sequence cursor")
    logs.add_argument("--limit", type=int, default=200)
    logs.add_argument("--task", help="filter by task ID")
    logs.set_defaults(handler=_logs)

    token = subcommands.add_parser("token", help="manage scoped API tokens")
    token_commands = token.add_subparsers(dest="token_command", required=True)
    create = token_commands.add_parser("create", help="create a hashed-at-rest token")
    create.add_argument("--label", required=True)
    create.add_argument(
        "--scope",
        action="append",
        default=None,
        help="scope to grant; repeatable (default: observe:read)",
    )
    create.add_argument("--expires-in-hours", type=float)
    create.set_defaults(handler=_token_create)

    project = subcommands.add_parser("project", help="manage durable projects")
    project_commands = project.add_subparsers(dest="project_command", required=True)
    project_create = project_commands.add_parser("create", help="create a project record")
    project_create.add_argument("--id", required=True)
    project_create.add_argument("--name", required=True)
    project_create.add_argument("--root", required=True)
    project_create.add_argument("--goal", required=True)
    project_create.set_defaults(handler=_project_create)

    task = subcommands.add_parser("task", help="manage durable tasks")
    task_commands = task.add_subparsers(dest="task_command", required=True)
    task_create = task_commands.add_parser("create", help="create and queue a task")
    task_create.add_argument("--project", required=True)
    task_create.add_argument("--id")
    task_create.add_argument("--reference")
    task_create.add_argument("--title", required=True)
    task_create.add_argument("--description", required=True)
    task_create.add_argument(
        "--label",
        action="append",
        choices=[value.value for value in TaskLabel],
        required=True,
    )
    task_create.add_argument("--capability", action="append", default=[])
    task_create.add_argument(
        "--topology",
        choices=[value.value for value in ExecutionTopology],
        default=ExecutionTopology.SINGLE.value,
    )
    task_create.add_argument(
        "--permission",
        choices=[value.value for value in PermissionClass],
        default=PermissionClass.GREEN.value,
    )
    task_create.add_argument("--minimum-context-tokens", type=int)
    task_create.add_argument("--privacy-sensitive", action="store_true")
    task_create.add_argument("--code-write-required", action="store_true")
    task_create.add_argument("--panel-size", type=int, default=2)
    task_create.add_argument("--preferred-worker", action="append", default=[])
    task_create.add_argument("--priority", type=int, default=50)
    task_create.set_defaults(handler=_task_create)

    goal = subcommands.add_parser("goal", help="manage durable autonomous Goals")
    goal_commands = goal.add_subparsers(dest="goal_command", required=True)

    goal_create = goal_commands.add_parser("create", help="create a guarded autonomous Goal")
    goal_create.add_argument("--project", required=True)
    goal_create.add_argument("--intent", required=True)
    goal_create.add_argument("--id")
    goal_create.add_argument("--max-iterations", type=int, default=12)
    goal_create.add_argument("--max-tasks", type=int, default=48)
    goal_create.add_argument("--max-failures", type=int, default=6)
    goal_create.add_argument("--no-progress-limit", type=int, default=3)
    goal_create.add_argument("--max-elapsed-seconds", type=float)
    goal_create.add_argument("--max-total-tokens", type=int)
    goal_create.add_argument("--max-cost-usd", type=float)
    goal_create.set_defaults(handler=_goal_create)

    goal_list = goal_commands.add_parser("list", help="list durable autonomous Goals")
    goal_list.add_argument("--project")
    goal_list.add_argument("--state")
    goal_list.add_argument("--limit", type=int)
    goal_list.set_defaults(handler=_goal_list)

    goal_get = goal_commands.add_parser("get", help="show one durable Goal")
    goal_get.add_argument("goal_id")
    goal_get.set_defaults(handler=_goal_get)

    goal_pause = goal_commands.add_parser("pause", help="soft- or hard-pause a Goal")
    goal_pause.add_argument("goal_id")
    goal_pause.add_argument("--mode", choices=("soft", "hard"), required=True)
    goal_pause.add_argument("--reason")
    goal_pause.set_defaults(handler=_goal_pause)

    goal_resume = goal_commands.add_parser("resume", help="resume a paused Goal")
    goal_resume.add_argument("goal_id")
    goal_resume.add_argument("--reason")
    goal_resume.set_defaults(handler=_goal_resume)

    goal_steer = goal_commands.add_parser("steer", help="inject durable Goal guidance")
    goal_steer.add_argument("goal_id")
    goal_steer.add_argument("--instruction", required=True)
    goal_steer.add_argument("--priority", type=int)
    goal_steer.add_argument(
        "--do-not-preserve-valid-work",
        action="store_true",
        help="allow the engine to invalidate otherwise reusable work when justified",
    )
    goal_steer.set_defaults(handler=_goal_steer)

    goal_stop = goal_commands.add_parser("stop", help="durably stop a Goal")
    goal_stop.add_argument("goal_id")
    goal_stop.add_argument("--reason", required=True)
    goal_stop.set_defaults(handler=_goal_stop)

    telemetry = subcommands.add_parser(
        "telemetry", help="show normalized Worker invocation telemetry"
    )
    telemetry.add_argument("--goal", help="filter by autonomous Goal ID")
    telemetry.add_argument("--task", help="filter by task ID")
    telemetry.add_argument("--limit", type=int, default=1000)
    telemetry.set_defaults(handler=_telemetry)

    resources = subcommands.add_parser(
        "resources", help="show normalized subscription/quota resource snapshots"
    )
    resources.add_argument("--goal", help="filter by autonomous Goal ID")
    resources.add_argument("--task", help="filter by Task ID")
    resources.add_argument("--provider")
    resources.add_argument("--model")
    resources.add_argument("--worker")
    resources.add_argument("--account-scope")
    resources.add_argument("--quota-pool")
    resources.add_argument("--observed-after", help="inclusive RFC 3339 lower bound")
    resources.add_argument("--observed-before", help="inclusive RFC 3339 upper bound")
    resources.add_argument("--limit", type=int, default=1000)
    resources.set_defaults(handler=_resources)

    autonomous = subcommands.add_parser(
        "autonomous", help="run and observe the production Autonomous Iteration host"
    )
    autonomous_commands = autonomous.add_subparsers(dest="autonomous_command", required=True)

    def add_host_options(command: argparse.ArgumentParser) -> None:
        command.add_argument("--host-id", help="stable operator-selected host identity")
        command.add_argument("--max-concurrent-goals", type=int, default=2)
        command.add_argument("--poll-interval-seconds", type=float, default=1.0)
        command.add_argument("--heartbeat-interval-seconds", type=float, default=2.0)
        command.add_argument("--lease-ttl-seconds", type=float, default=10.0)
        command.add_argument("--shutdown-grace-seconds", type=float, default=10.0)
        command.add_argument(
            "--executable-override",
            action="append",
            default=[],
            metavar="WORKER_OR_HARNESS=PATH",
            help="explicit native Worker executable; repeatable",
        )
        command.add_argument(
            "--local-worker-endpoint",
            action="append",
            default=[],
            metavar="WORKER_ID=URL",
            help="explicit Local Worker endpoint; bearer stays in approved secret storage",
        )
        command.add_argument(
            "--local-worker-driver",
            action="append",
            default=[],
            metavar="WORKER_ID=DRIVER_ID",
            help="operator-selected server-side Local Worker driver profile; repeatable",
        )
        command.add_argument(
            "--allow-mock-worker",
            action="store_true",
            help="explicit test-only opt-in for persisted Mock Workers",
        )

    autonomous_run = autonomous_commands.add_parser(
        "run", help="advance one Goal until it suspends or terminates"
    )
    autonomous_run.add_argument("goal_id")
    add_host_options(autonomous_run)
    autonomous_run.set_defaults(handler=_autonomous_operate)

    autonomous_serve = autonomous_commands.add_parser(
        "serve", help="continuously advance eligible Goals with bounded concurrency"
    )
    add_host_options(autonomous_serve)
    autonomous_serve.set_defaults(handler=_autonomous_operate)

    autonomous_status = autonomous_commands.add_parser(
        "status", help="show persisted production host and Goal lease state"
    )
    autonomous_status.add_argument("--host", help="filter by host ID")
    autonomous_status.add_argument("--goal", help="filter by Goal ID")
    autonomous_status.set_defaults(handler=_autonomous_status)

    serve = subcommands.add_parser(
        "serve",
        help="serve REST/WebSocket; Goal controls require explicit config and scoped auth",
    )
    serve.add_argument(
        "--allow-unauthenticated-loopback",
        action="store_true",
        help="explicit development-only loopback access without a bearer token",
    )
    serve.set_defaults(handler=_serve)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if getattr(args, "scope", None) is None:
        args.scope = ["observe:read"]
    if getattr(args, "limit", None) is not None and args.limit < 1:
        parser.error("--limit must be positive")
    if args.command == "logs" and args.limit > 1000:
        parser.error("logs --limit cannot exceed 1000")
    if getattr(args, "after", 0) < 0:
        parser.error("--after cannot be negative")
    if getattr(args, "expires_in_hours", None) is not None and args.expires_in_hours <= 0:
        parser.error("--expires-in-hours must be positive")
    if (
        getattr(args, "minimum_context_tokens", None) is not None
        and args.minimum_context_tokens <= 0
    ):
        parser.error("--minimum-context-tokens must be positive")
    if getattr(args, "panel_size", 1) <= 0:
        parser.error("--panel-size must be positive")
    if getattr(args, "max_concurrent_goals", 1) <= 0:
        parser.error("--max-concurrent-goals must be positive")
    for name in (
        "poll_interval_seconds",
        "heartbeat_interval_seconds",
        "lease_ttl_seconds",
        "shutdown_grace_seconds",
    ):
        value = getattr(args, name, None)
        if value is not None and value <= 0:
            parser.error(f"--{name.replace('_', '-')} must be positive")
    priority = getattr(args, "priority", None)
    if priority is not None and not 0 <= priority <= 100:
        parser.error("--priority must be between 0 and 100")
    for name in (
        "max_iterations",
        "max_tasks",
        "max_failures",
        "no_progress_limit",
        "max_elapsed_seconds",
        "max_total_tokens",
        "max_cost_usd",
    ):
        value = getattr(args, name, None)
        if value is not None and value <= 0:
            parser.error(f"--{name.replace('_', '-')} must be positive")
    try:
        return int(args.handler(args))
    except (
        ConfigurationError,
        FileExistsError,
        FileNotFoundError,
        KeyError,
        RuntimeError,
        ValueError,
        sqlite3.Error,
    ) as error:
        print(f"error: {error}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
