"""Readiness checks for explicitly launched, unauthenticated loopback test daemons.

A lifespan marker proves application initialization, not that the listening socket
exists. Acceptance callers must observe both the marker and a matching HTTP identity.
This module is a test harness helper, not a production discovery or enrollment API.
"""

from __future__ import annotations

import http.client
import json
from contextlib import closing
from pathlib import Path
from typing import Any

_MARKER_LIMIT = 8_192
_RESPONSE_LIMIT = 32_768
_IDENTITY_FIELDS = ("authority_id", "registry_id", "node_id", "runtime_instance_id")


def _object(raw: bytes) -> dict[str, Any]:
    def unique_pairs(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in pairs:
            if key in result:
                raise ValueError("duplicate JSON key")
            result[key] = value
        return result

    try:
        value = json.loads(raw.decode("utf-8"), object_pairs_hook=unique_pairs)
    except (UnicodeError, ValueError, RecursionError):
        raise AssertionError("daemon readiness document is not a JSON object") from None
    if not isinstance(value, dict):
        raise AssertionError("daemon readiness document is not a JSON object")
    return value


def loopback_health_ready(ready_file: Path, port: int, driver_id: str) -> bool:
    """Require a matching HTTP V2 identity before starting acceptance traffic.

    Only connection/startup timing failures are retryable by the caller's existing
    bounded polling loop. Invalid, redirected, oversized, or wrong-identity replies
    fail immediately. The peer is an explicitly spawned test daemon, not an arbitrary
    service. No proxy, authentication, redirect, enrollment, or job launch is used.
    """

    if type(port) is not int or not 1 <= port <= 65_535:
        raise ValueError("test daemon port must be an integer in 1..65535")
    if not isinstance(driver_id, str) or not driver_id.strip():
        raise ValueError("test daemon driver identity must not be empty")
    try:
        with ready_file.open("rb") as handle:
            raw = handle.read(_MARKER_LIMIT + 1)
    except FileNotFoundError:
        return False
    if len(raw) > _MARKER_LIMIT:
        raise AssertionError("daemon readiness marker exceeds the size limit")
    marker = _object(raw)
    if (
        marker.get("ready") is not True
        or type(marker.get("protocol_version")) is not int
        or marker["protocol_version"] != 2
        or any(
            not isinstance(marker.get(field), str) or not 1 <= len(marker[field]) <= 200
            for field in _IDENTITY_FIELDS
        )
    ):
        raise AssertionError("daemon readiness marker has an invalid identity")

    try:
        # HTTPConnection does not inherit proxy configuration or follow redirects.
        with closing(http.client.HTTPConnection("127.0.0.1", port, timeout=0.25)) as client:
            client.request(
                "GET", "/v2/health", headers={"Accept": "application/json", "Connection": "close"}
            )
            response = client.getresponse()
            if response.status != 200:
                raise AssertionError("daemon readiness health request was rejected")
            if response.getheader("Content-Encoding", "identity").lower() != "identity":
                raise AssertionError("daemon readiness response must be uncompressed")
            if response.getheader("Content-Type", "").split(";", 1)[0] != "application/json":
                raise AssertionError("daemon readiness response must be JSON")
            raw = response.read(_RESPONSE_LIMIT + 1)
    except (OSError, http.client.HTTPException):
        # Startup can publish the marker before the listening socket is active.
        # Returning False does not change the caller's original startup deadline.
        return False
    if len(raw) > _RESPONSE_LIMIT:
        raise AssertionError("daemon readiness response exceeds the size limit")
    document = _object(raw)
    data = document.get("data")
    if (
        type(document.get("protocol_version")) is not int
        or document["protocol_version"] != 2
        or not isinstance(data, dict)
        or any(data.get(field) != marker[field] for field in _IDENTITY_FIELDS)
        or data.get("default_driver_id") != driver_id
    ):
        raise AssertionError("daemon readiness health identity does not match the startup marker")
    return True
