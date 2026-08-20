from __future__ import annotations

import ipaddress
import json
import os
import stat
import sys
from collections.abc import Mapping
from contextlib import suppress
from dataclasses import asdict, dataclass, replace
from pathlib import Path
from typing import Any

ENV_PREFIX = "PROJECT_SUPERVISOR_"
CONFIG_FILENAME = "config.json"
DATABASE_FILENAME = "supervisor.db"


class ConfigurationError(ValueError):
    """Raised when a configuration would be ambiguous or unsafe."""


def default_data_dir() -> Path:
    override = os.environ.get(f"{ENV_PREFIX}DATA_DIR")
    if override:
        return Path(override).expanduser()
    if sys.platform == "darwin":
        return Path.home() / "Library" / "Application Support" / "Project_Supervisor"
    if os.name == "nt":
        return Path(os.environ.get("LOCALAPPDATA", Path.home())) / "Project_Supervisor"
    return Path(os.environ.get("XDG_STATE_HOME", Path.home() / ".local" / "state")) / (
        "project-supervisor"
    )


def _boolean(value: object, *, name: str) -> bool:
    if isinstance(value, bool):
        return value
    normalized = str(value).strip().lower()
    if normalized in {"1", "true", "yes", "on"}:
        return True
    if normalized in {"0", "false", "no", "off"}:
        return False
    raise ConfigurationError(f"{name} must be true or false")


def _is_loopback(host: str) -> bool:
    if host.strip().lower() == "localhost":
        return True
    try:
        return ipaddress.ip_address(host).is_loopback
    except ValueError:
        return False


@dataclass(frozen=True, slots=True)
class SupervisorConfig:
    """Operator configuration. Provider credentials never belong in this object."""

    data_dir: Path
    host: str = "127.0.0.1"
    port: int = 7330
    private_transport: bool = False
    tls_certificate: Path | None = None
    tls_private_key: Path | None = None
    read_only_api: bool = True
    log_level: str = "INFO"
    worker_timeout_seconds: float = 120.0

    @property
    def database_path(self) -> Path:
        return self.data_dir / DATABASE_FILENAME

    @property
    def config_path(self) -> Path:
        return self.data_dir / CONFIG_FILENAME

    @property
    def loopback_only(self) -> bool:
        return _is_loopback(self.host)

    def validate(self, *, require_tls_files: bool = True) -> SupervisorConfig:
        if not 1 <= self.port <= 65535:
            raise ConfigurationError("port must be between 1 and 65535")
        if self.log_level.upper() not in {"CRITICAL", "ERROR", "WARNING", "INFO", "DEBUG"}:
            raise ConfigurationError("log_level must be CRITICAL, ERROR, WARNING, INFO, or DEBUG")
        if not 1 <= self.worker_timeout_seconds <= 3600:
            raise ConfigurationError("worker_timeout_seconds must be between 1 and 3600")
        if (self.tls_certificate is None) != (self.tls_private_key is None):
            raise ConfigurationError("TLS certificate and private key must be configured together")
        if not self.loopback_only:
            if not self.private_transport:
                raise ConfigurationError(
                    "non-loopback listening requires an authenticated private transport"
                )
            if self.tls_certificate is None or self.tls_private_key is None:
                raise ConfigurationError("non-loopback listening requires TLS")
        if require_tls_files and self.tls_certificate is not None:
            for name, path in (
                ("TLS certificate", self.tls_certificate),
                ("TLS private key", self.tls_private_key),
            ):
                if not path.is_file():
                    raise ConfigurationError(f"{name} does not exist: {path}")
        return self

    def to_file_dict(self) -> dict[str, Any]:
        values = asdict(self)
        values.pop("data_dir")
        for key in ("tls_certificate", "tls_private_key"):
            value = values[key]
            values[key] = str(value) if value is not None else None
        return values


def load_config(
    *,
    data_dir: str | Path | None = None,
    config_path: str | Path | None = None,
    environ: Mapping[str, str] | None = None,
    require_tls_files: bool = True,
) -> SupervisorConfig:
    environment = environ if environ is not None else os.environ
    resolved_data_dir = Path(
        data_dir or environment.get(f"{ENV_PREFIX}DATA_DIR") or default_data_dir()
    ).expanduser()
    path = Path(config_path).expanduser() if config_path else resolved_data_dir / CONFIG_FILENAME
    file_values: dict[str, Any] = {}
    if path.exists():
        try:
            loaded = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as error:
            raise ConfigurationError(f"cannot read configuration {path}: {error}") from error
        if not isinstance(loaded, dict):
            raise ConfigurationError("configuration root must be a JSON object")
        file_values = loaded

    allowed = {
        "host",
        "port",
        "private_transport",
        "tls_certificate",
        "tls_private_key",
        "read_only_api",
        "log_level",
        "worker_timeout_seconds",
    }
    unknown = set(file_values) - allowed
    if unknown:
        raise ConfigurationError(f"unknown configuration keys: {', '.join(sorted(unknown))}")

    values: dict[str, Any] = {
        "data_dir": resolved_data_dir,
        "host": file_values.get("host", "127.0.0.1"),
        "port": file_values.get("port", 7330),
        "private_transport": file_values.get("private_transport", False),
        "tls_certificate": file_values.get("tls_certificate"),
        "tls_private_key": file_values.get("tls_private_key"),
        "read_only_api": file_values.get("read_only_api", True),
        "log_level": file_values.get("log_level", "INFO"),
        "worker_timeout_seconds": file_values.get("worker_timeout_seconds", 120.0),
    }
    environment_keys = {
        "host": "HOST",
        "port": "PORT",
        "private_transport": "PRIVATE_TRANSPORT",
        "tls_certificate": "TLS_CERTIFICATE",
        "tls_private_key": "TLS_PRIVATE_KEY",
        "read_only_api": "READ_ONLY_API",
        "log_level": "LOG_LEVEL",
        "worker_timeout_seconds": "WORKER_TIMEOUT_SECONDS",
    }
    for field, suffix in environment_keys.items():
        environment_value = environment.get(f"{ENV_PREFIX}{suffix}")
        if environment_value is not None:
            values[field] = environment_value

    try:
        values["port"] = int(values["port"])
    except (TypeError, ValueError) as error:
        raise ConfigurationError("port must be an integer") from error
    try:
        values["worker_timeout_seconds"] = float(values["worker_timeout_seconds"])
    except (TypeError, ValueError) as error:
        raise ConfigurationError("worker_timeout_seconds must be numeric") from error
    for field in ("private_transport", "read_only_api"):
        values[field] = _boolean(values[field], name=field)
    for field in ("tls_certificate", "tls_private_key"):
        if values[field] in {None, ""}:
            values[field] = None
        else:
            values[field] = Path(values[field]).expanduser()
    values["host"] = str(values["host"]).strip()
    values["log_level"] = str(values["log_level"]).upper()
    return SupervisorConfig(**values).validate(require_tls_files=require_tls_files)


def initialize_config(data_dir: str | Path, *, force: bool = False) -> SupervisorConfig:
    target = Path(data_dir).expanduser()
    config = SupervisorConfig(data_dir=target).validate()
    target.mkdir(parents=True, exist_ok=True, mode=0o700)
    with suppress(OSError):
        target.chmod(stat.S_IRWXU)
    if config.config_path.exists() and not force:
        raise FileExistsError(f"configuration already exists: {config.config_path}")
    config.config_path.write_text(
        json.dumps(config.to_file_dict(), indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    with suppress(OSError):
        config.config_path.chmod(stat.S_IRUSR | stat.S_IWUSR)
    return config


def with_data_dir(config: SupervisorConfig, data_dir: str | Path) -> SupervisorConfig:
    """Return a copy rooted at an explicit local-state directory."""

    return replace(config, data_dir=Path(data_dir).expanduser())
