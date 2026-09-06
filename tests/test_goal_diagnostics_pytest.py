from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from scripts import goal_diagnostics_pytest as plugin


def item(name="test_autonomous_host.py"):
    return SimpleNamespace(path=Path(name), funcargs={"store": SimpleNamespace(path="fixture.db")})


def report(when="call", failed=True):
    return SimpleNamespace(when=when, failed=failed, sections=[])


@pytest.mark.parametrize("when,failed,name", [
    ("setup", True, "test_autonomous_host.py"),
    ("teardown", True, "test_autonomous_host.py"),
    ("call", False, "test_autonomous_host.py"),
    ("call", True, "unrelated.py"),
])
def test_diagnostics_do_not_run_outside_targeted_failures(monkeypatch, when, failed, name):
    def forbidden(_path):
        pytest.fail("unexpected inspection")
    monkeypatch.setattr(plugin, "inspect_recent_goals", forbidden)
    result = report(when, failed)
    plugin.attach_failure_snapshot(item(name), result)
    assert result.sections == []


def test_targeted_failure_retains_original_and_adds_structured_snapshot(monkeypatch):
    monkeypatch.setattr(
        plugin, "inspect_recent_goals", lambda _path: [{"leaseObservation": "lost"}]
    )
    result = report()
    plugin.attach_failure_snapshot(item(), result)
    assert result.failed is True
    assert json.loads(result.sections[0][1]) == [{"leaseObservation": "lost"}]


def test_diagnostic_failure_is_static_and_does_not_mask_test(monkeypatch):
    def fail(_path):
        raise RuntimeError("private path and provider message")
    monkeypatch.setattr(plugin, "inspect_recent_goals", fail)
    result = report()
    plugin.attach_failure_snapshot(item(), result)
    assert result.failed is True
    assert json.loads(result.sections[0][1])["reason"] == "diagnosticUnavailable"
    assert "private" not in result.sections[0][1]


def test_snapshot_size_is_bounded(monkeypatch):
    monkeypatch.setattr(plugin, "inspect_recent_goals", lambda _path: [{"oversized": "x" * 65536}])
    result = report()
    plugin.attach_failure_snapshot(item(), result)
    assert json.loads(result.sections[0][1])["reason"] == "diagnosticTooLarge"
