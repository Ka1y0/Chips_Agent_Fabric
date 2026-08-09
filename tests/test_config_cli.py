import json
from pathlib import Path

import pytest

from project_supervisor.cli import main
from project_supervisor.config import ConfigurationError, SupervisorConfig, load_config
from project_supervisor.store import StateStore


def test_config_defaults_to_loopback_and_read_only(tmp_path: Path) -> None:
    config = load_config(data_dir=tmp_path, environ={})
    assert config.host == "127.0.0.1"
    assert config.loopback_only
    assert config.read_only_api
    assert not config.private_transport


def test_non_loopback_requires_private_transport_and_tls(tmp_path: Path) -> None:
    with pytest.raises(ConfigurationError, match="private transport"):
        SupervisorConfig(data_dir=tmp_path, host="10.0.0.2").validate()
    with pytest.raises(ConfigurationError, match="requires TLS"):
        SupervisorConfig(
            data_dir=tmp_path,
            host="10.0.0.2",
            private_transport=True,
        ).validate()


def test_remote_config_accepts_existing_tls_files(tmp_path: Path) -> None:
    certificate = tmp_path / "server.crt"
    private_key = tmp_path / "server.key"
    certificate.write_text("test certificate", encoding="utf-8")
    private_key.write_text("test private key", encoding="utf-8")
    config = SupervisorConfig(
        data_dir=tmp_path,
        host="100.64.0.10",
        private_transport=True,
        tls_certificate=certificate,
        tls_private_key=private_key,
    ).validate()
    assert not config.loopback_only


def test_init_status_tasks_and_logs_are_local_and_machine_readable(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    assert main(["--data-dir", str(tmp_path), "--json", "init"]) == 0
    initialized = json.loads(capsys.readouterr().out)
    assert initialized["loopbackOnly"] is True
    assert (tmp_path / "config.json").is_file()
    assert (tmp_path / "supervisor.db").is_file()

    store = StateStore(tmp_path / "supervisor.db")
    store.create_project(
        project_id="project-1",
        name="Fixture",
        root_path=str(tmp_path / "fixture"),
        goal="Test safe CLI reads",
    )

    assert main(["--data-dir", str(tmp_path), "--json", "status"]) == 0
    status = json.loads(capsys.readouterr().out)
    assert status["status"] == "ready"
    assert status["counts"]["projects"] == 1
    assert status["loopbackOnly"] is True

    assert main(["--data-dir", str(tmp_path), "tasks"]) == 0
    assert capsys.readouterr().out.strip() == "No tasks."

    assert main(["--data-dir", str(tmp_path), "--json", "logs"]) == 0
    logs = json.loads(capsys.readouterr().out)
    assert logs[0]["kind"] == "projectCreated"


def test_token_create_is_scoped_hashed_and_shown_once(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    assert main(["--data-dir", str(tmp_path), "init"]) == 0
    capsys.readouterr()
    assert (
        main(
            [
                "--data-dir",
                str(tmp_path),
                "--json",
                "token",
                "create",
                "--label",
                "monitor",
            ]
        )
        == 0
    )
    output = json.loads(capsys.readouterr().out)
    assert output["scopes"] == ["observe:read"]
    store = StateStore(tmp_path / "supervisor.db")
    assert store.verify_api_token(output["token"], "observe:read")
    with store.connect() as connection:
        row = connection.execute(
            "SELECT token_hash FROM api_tokens WHERE id=?", (output["id"],)
        ).fetchone()
    assert output["token"].encode() not in bytes(row["token_hash"])


def test_cli_refuses_reads_before_init(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    assert main(["--data-dir", str(tmp_path), "status"]) == 2
    assert "run `project-supervisor init`" in capsys.readouterr().err


def test_cli_creates_project_and_typed_task(tmp_path: Path, capsys) -> None:  # type: ignore[no-untyped-def]
    assert main(["--data-dir", str(tmp_path), "init"]) == 0
    capsys.readouterr()
    assert (
        main(
            [
                "--data-dir",
                str(tmp_path),
                "project",
                "create",
                "--id",
                "project-1",
                "--name",
                "Fixture",
                "--root",
                str(tmp_path / "workspace"),
                "--goal",
                "Exercise the CLI",
            ]
        )
        == 0
    )
    capsys.readouterr()
    assert (
        main(
            [
                "--data-dir",
                str(tmp_path),
                "--json",
                "task",
                "create",
                "--project",
                "project-1",
                "--title",
                "Analyze",
                "--description",
                "Read-only fixture analysis",
                "--label",
                "research",
                "--capability",
                "analysis",
                "--topology",
                "fallback",
                "--preferred-worker",
                "worker-1",
            ]
        )
        == 0
    )
    task = json.loads(capsys.readouterr().out)
    assert task["state"] == "ready"
    assert task["topology"] == "fallback"


def test_serve_is_explicit_loopback_and_does_not_start_workers(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys
) -> None:  # type: ignore[no-untyped-def]
    assert main(["--data-dir", str(tmp_path), "init"]) == 0
    capsys.readouterr()
    observed = {}

    def fake_run(app, **kwargs):  # type: ignore[no-untyped-def]
        observed["app"] = app
        observed.update(kwargs)

    monkeypatch.setattr("uvicorn.run", fake_run)
    assert (
        main(
            [
                "--data-dir",
                str(tmp_path),
                "serve",
                "--allow-unauthenticated-loopback",
            ]
        )
        == 0
    )
    assert observed["host"] == "127.0.0.1"
    assert observed["port"] == 7330
    assert observed["app"].state.api_settings.allow_unauthenticated_loopback is True
