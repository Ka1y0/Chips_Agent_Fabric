from __future__ import annotations

import hashlib
import json
import os
import re
import stat
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path
from typing import Any

_DRIVER_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$")
_FINGERPRINT = re.compile(r"^sha256:[0-9a-f]{64}$")
_PROFILE_FIELDS = frozenset(
    {
        "driver_id",
        "driver_type",
        "profile_revision",
        "executable",
        "max_execution_seconds",
    }
)


class DriverType(StrEnum):
    TEST = "test"
    CLAUDE = "claude"
    GROK = "grok"
    AGY = "agy"
    CODEX = "codex"


class DriverProfileError(ValueError):
    """An operator-supplied driver profile is unsafe or malformed."""


class DriverUnavailable(RuntimeError):
    """A requested driver is unknown or deliberately unsupported."""


def validate_driver_id(value: object) -> str:
    if not isinstance(value, str) or not _DRIVER_ID.fullmatch(value):
        raise DriverProfileError(
            "driver_id must be 1-128 safe ASCII letters, digits, '.', '_', ':', or '-'"
        )
    return value


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    try:
        with path.open("rb") as stream:
            while chunk := stream.read(1024 * 1024):
                digest.update(chunk)
    except OSError as error:
        raise DriverProfileError(
            f"driver executable cannot be hashed: {type(error).__name__}"
        ) from error
    return "sha256:" + digest.hexdigest()


def _fingerprint(value: Mapping[str, Any]) -> str:
    encoded = json.dumps(
        dict(value),
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    return "sha256:" + hashlib.sha256(encoded).hexdigest()


@dataclass(frozen=True, slots=True)
class DriverProfile:
    """Immutable operator-owned execution profile.

    Only a safe semantic projection is exposed over HTTP. The resolved executable path and its
    content digest remain process-local, but both contribute to the durable profile fingerprint.
    """

    driver_id: str
    driver_type: DriverType
    profile_revision: int
    profile_fingerprint: str
    executable: Path | None
    executable_sha256: str | None
    executable_device: int | None
    executable_inode: int | None
    executable_uid: int | None
    executable_mode: int | None
    max_execution_seconds: float
    available: bool

    def __post_init__(self) -> None:
        validate_driver_id(self.driver_id)
        if self.profile_revision < 1:
            raise DriverProfileError("profile_revision must be positive")
        if not _FINGERPRINT.fullmatch(self.profile_fingerprint):
            raise DriverProfileError("profile_fingerprint must be a sha256 digest")
        if not 1 <= self.max_execution_seconds <= 3600:
            raise DriverProfileError("max_execution_seconds must be between 1 and 3600")
        native = self.driver_type in {
            DriverType.CLAUDE,
            DriverType.GROK,
            DriverType.AGY,
            DriverType.CODEX,
        }
        executable_identity = (
            self.executable,
            self.executable_sha256,
            self.executable_device,
            self.executable_inode,
            self.executable_uid,
            self.executable_mode,
        )
        if native != all(value is not None for value in executable_identity):
            raise DriverProfileError(
                "native drivers require one resolved executable and complete file identity"
            )
        if self.executable is not None and not self.executable.is_absolute():
            raise DriverProfileError("driver executable must be absolute after resolution")

    @property
    def supports_execution(self) -> bool:
        return self.available and self.is_current()

    @property
    def supports_cancel(self) -> bool:
        if self.driver_type is DriverType.TEST:
            return self.supports_execution
        # NativeSubprocessAdapter has verified process-group teardown on POSIX.  Windows durable
        # cancellation needs Job Object ownership before the daemon may advertise it.
        return self.supports_execution and os.name != "nt"

    def is_current(self) -> bool:
        """Return whether the reviewed executable identity still matches disk.

        This closes the common path-replacement gap between daemon startup and launch.  A second
        check in the native runner narrows (but cannot universally eliminate) the final
        hash-to-exec race on platforms without descriptor-based execution.
        """

        if self.driver_type is DriverType.TEST:
            return self.available
        assert self.executable is not None and self.executable_sha256 is not None
        try:
            metadata = self.executable.stat()
            if not stat.S_ISREG(metadata.st_mode) or not os.access(self.executable, os.X_OK):
                return False
            observed_identity = (
                metadata.st_dev,
                metadata.st_ino,
                metadata.st_uid,
                stat.S_IMODE(metadata.st_mode),
            )
            expected_identity = (
                self.executable_device,
                self.executable_inode,
                self.executable_uid,
                self.executable_mode,
            )
            if observed_identity != expected_identity:
                return False
            return _sha256_file(self.executable) == self.executable_sha256
        except (DriverProfileError, OSError):
            return False

    def public_data(self) -> dict[str, Any]:
        runtime_available = self.is_current()
        return {
            "driver_id": self.driver_id,
            "driver_type": self.driver_type.value,
            "profile_revision": self.profile_revision,
            "profile_fingerprint": self.profile_fingerprint,
            "available": runtime_available,
            "supports_execution": self.supports_execution,
            "supports_cancel": self.supports_cancel,
        }

    @classmethod
    def test(
        cls,
        *,
        driver_id: str = "test",
        profile_revision: int = 1,
        max_execution_seconds: float = 3600.0,
        profile_material: Mapping[str, Any] | None = None,
    ) -> DriverProfile:
        identifier = validate_driver_id(driver_id)
        material = {
            "driver_id": identifier,
            "driver_type": DriverType.TEST.value,
            "profile_revision": profile_revision,
            "max_execution_seconds": float(max_execution_seconds),
            "test_configuration": dict(profile_material or {}),
        }
        return cls(
            driver_id=identifier,
            driver_type=DriverType.TEST,
            profile_revision=profile_revision,
            profile_fingerprint=_fingerprint(material),
            executable=None,
            executable_sha256=None,
            executable_device=None,
            executable_inode=None,
            executable_uid=None,
            executable_mode=None,
            max_execution_seconds=float(max_execution_seconds),
            available=True,
        )

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> DriverProfile:
        unknown = set(value) - _PROFILE_FIELDS
        if unknown:
            raise DriverProfileError(
                f"driver profile contains unsupported fields: {','.join(sorted(unknown))}"
            )
        try:
            identifier = validate_driver_id(value.get("driver_id"))
            driver_type = DriverType(value.get("driver_type"))
        except (TypeError, ValueError) as error:
            raise DriverProfileError("driver profile type is unsupported") from error
        revision = value.get("profile_revision", 1)
        if not isinstance(revision, int) or isinstance(revision, bool) or revision < 1:
            raise DriverProfileError("profile_revision must be a positive integer")
        raw_timeout = value.get("max_execution_seconds", 120.0)
        if not isinstance(raw_timeout, (int, float)) or isinstance(raw_timeout, bool):
            raise DriverProfileError("max_execution_seconds must be numeric")
        timeout = float(raw_timeout)
        if not 1 <= timeout <= 3600:
            raise DriverProfileError("max_execution_seconds must be between 1 and 3600")

        raw_executable = value.get("executable")
        executable: Path | None = None
        executable_digest: str | None = None
        executable_device: int | None = None
        executable_inode: int | None = None
        executable_uid: int | None = None
        executable_mode: int | None = None
        native = driver_type in {
            DriverType.CLAUDE,
            DriverType.GROK,
            DriverType.AGY,
            DriverType.CODEX,
        }
        if native:
            if not isinstance(raw_executable, str) or not raw_executable.strip():
                raise DriverProfileError("native driver profile requires executable")
            try:
                executable = Path(raw_executable).expanduser().resolve(strict=True)
                metadata = executable.stat()
            except OSError as error:
                raise DriverProfileError(
                    f"driver executable is unavailable: {type(error).__name__}"
                ) from error
            if not stat.S_ISREG(metadata.st_mode) or not os.access(executable, os.X_OK):
                raise DriverProfileError("driver executable must be a regular executable file")
            mode = stat.S_IMODE(metadata.st_mode)
            if os.name != "nt":
                if mode & 0o022:
                    raise DriverProfileError("driver executable must not be group/world writable")
                allowed_owners = {os.geteuid(), 0}
                if metadata.st_uid not in allowed_owners:
                    raise DriverProfileError(
                        "driver executable must be owned by the daemon user or root"
                    )
            executable_digest = _sha256_file(executable)
            executable_device = metadata.st_dev
            executable_inode = metadata.st_ino
            executable_uid = metadata.st_uid
            executable_mode = mode
        elif raw_executable is not None:
            raise DriverProfileError("test drivers must not configure an executable")

        available = True
        material = {
            "driver_id": identifier,
            "driver_type": driver_type.value,
            "profile_revision": revision,
            "executable": str(executable) if executable else None,
            "executable_sha256": executable_digest,
            "executable_device": executable_device,
            "executable_inode": executable_inode,
            "executable_uid": executable_uid,
            "executable_mode": executable_mode,
            "max_execution_seconds": timeout,
            "available": available,
        }
        return cls(
            driver_id=identifier,
            driver_type=driver_type,
            profile_revision=revision,
            profile_fingerprint=_fingerprint(material),
            executable=executable,
            executable_sha256=executable_digest,
            executable_device=executable_device,
            executable_inode=executable_inode,
            executable_uid=executable_uid,
            executable_mode=executable_mode,
            max_execution_seconds=timeout,
            available=available,
        )

    @classmethod
    def from_file(cls, path: Path) -> DriverProfile:
        try:
            resolved = Path(path).expanduser().resolve(strict=True)
            metadata = resolved.stat()
            if not stat.S_ISREG(metadata.st_mode) or metadata.st_size > 64 * 1024:
                raise DriverProfileError("driver profile must be a regular file of at most 64 KiB")
            value = json.loads(resolved.read_text(encoding="utf-8"))
        except DriverProfileError:
            raise
        except (OSError, UnicodeError, ValueError) as error:
            raise DriverProfileError(
                f"driver profile cannot be read: {type(error).__name__}"
            ) from error
        if not isinstance(value, Mapping):
            raise DriverProfileError("driver profile must contain one JSON object")
        return cls.from_mapping(value)


class DriverCatalog:
    """Immutable registry of reviewed server-side driver profiles."""

    def __init__(self, profiles: Iterable[DriverProfile], *, default_driver_id: str) -> None:
        by_id: dict[str, DriverProfile] = {}
        for profile in profiles:
            if profile.driver_id in by_id:
                raise DriverProfileError(f"duplicate driver profile: {profile.driver_id}")
            by_id[profile.driver_id] = profile
        if not by_id:
            raise DriverProfileError("driver catalog must not be empty")
        default = validate_driver_id(default_driver_id)
        if default not in by_id:
            raise DriverProfileError("default_driver_id is not registered")
        if not by_id[default].supports_execution:
            raise DriverProfileError("default driver must support execution")
        self._profiles = by_id
        self.default_driver_id = default

    @property
    def default(self) -> DriverProfile:
        return self._profiles[self.default_driver_id]

    @property
    def profiles(self) -> tuple[DriverProfile, ...]:
        return tuple(self._profiles[key] for key in sorted(self._profiles))

    def get(self, driver_id: str) -> DriverProfile:
        identifier = validate_driver_id(driver_id)
        profile = self._profiles.get(identifier)
        if profile is None:
            raise DriverUnavailable("requested driver is not registered")
        if not profile.supports_execution:
            raise DriverUnavailable("requested driver is registered but unsupported")
        return profile

    def find(self, driver_id: str | None) -> DriverProfile | None:
        return self._profiles.get(driver_id or "")

    def public_data(self) -> list[dict[str, Any]]:
        return [self._profiles[key].public_data() for key in sorted(self._profiles)]
