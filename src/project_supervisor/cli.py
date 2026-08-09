from __future__ import annotations

import argparse
import json
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
    import asyncio

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

    serve = subcommands.add_parser("serve", help="serve the read-only REST/WebSocket API")
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
    if not 0 <= getattr(args, "priority", 50) <= 100:
        parser.error("--priority must be between 0 and 100")
    try:
        return int(args.handler(args))
    except (
        ConfigurationError,
        FileExistsError,
        FileNotFoundError,
        KeyError,
        sqlite3.Error,
    ) as error:
        print(f"error: {error}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
