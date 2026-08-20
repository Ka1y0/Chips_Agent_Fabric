"""Durable Local Worker Protocol V2 authority.

This package is intentionally separate from Supervisor canonical state: a Local Worker may run on
another host and owns its own launch registry. Production execution is restricted to immutable,
operator-registered driver profiles; clients cannot choose executables, argv, environment, or cwd.
"""

from .drivers import (
    DriverCatalog,
    DriverProfile,
    DriverProfileError,
    DriverType,
    DriverUnavailable,
)
from .model import (
    LaunchDisposition,
    LaunchRecord,
    LaunchState,
    canonical_request_document,
    request_digest,
)
from .registry import LaunchRegistry, RegistryUnavailable

__all__ = [
    "DriverCatalog",
    "DriverProfile",
    "DriverProfileError",
    "DriverType",
    "DriverUnavailable",
    "LaunchDisposition",
    "LaunchRecord",
    "LaunchRegistry",
    "LaunchState",
    "RegistryUnavailable",
    "canonical_request_document",
    "request_digest",
]
