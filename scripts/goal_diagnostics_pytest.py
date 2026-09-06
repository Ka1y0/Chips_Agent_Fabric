"""Opt-in pytest failure diagnostics for the isolated autonomous-host unit fixtures."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from project_supervisor.goal_inspection import inspect_recent_goals

_FILES = frozenset({"test_autonomous_host.py", "test_goal_inspection_integration.py"})


def attach_failure_snapshot(item: Any, report: Any) -> None:
    if report.when != "call" or not report.failed or Path(str(item.path)).name not in _FILES:
        return
    store = item.funcargs.get("store")
    if store is None:
        return
    try:
        reports = inspect_recent_goals(store.path)
        encoded = json.dumps(reports, sort_keys=True, allow_nan=False)
        if len(encoded.encode("utf-8")) > 65536:
            encoded = '{"status":"unavailable","reason":"diagnosticTooLarge"}'
    except Exception:
        # The diagnostic must neither replace the failure nor echo exception/fixture contents.
        encoded = '{"status":"unavailable","reason":"diagnosticUnavailable"}'
    report.sections.append(("Fabric Goal snapshots (not execution authority)", encoded))


@pytest.hookimpl(hookwrapper=True)
def pytest_runtest_makereport(item, call):
    del call
    outcome = yield
    attach_failure_snapshot(item, outcome.get_result())
