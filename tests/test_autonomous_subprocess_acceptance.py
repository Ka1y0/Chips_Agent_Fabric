from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path


def test_fresh_process_restart_and_multi_iteration_acceptance(tmp_path: Path) -> None:
    root = Path(__file__).parents[1]
    output = tmp_path / "acceptance.json"
    result = subprocess.run(
        [
            sys.executable,
            str(root / "scripts/run_autonomous_acceptance.py"),
            "all",
            "--output",
            str(output),
        ],
        cwd=root,
        env={
            "PATH": os.environ.get("PATH", ""),
            "PYTHONDONTWRITEBYTECODE": "1",
        },
        check=False,
        capture_output=True,
        text=True,
        # The acceptance script has independent 10s crash and 20s resume
        # subprocess guards, plus the multi-iteration phase. Keep the outer
        # guard above their combined ceiling so scheduler load cannot mask the
        # script's own deterministic failure reports.
        timeout=45,
    )

    assert result.returncode == 0, result.stderr or result.stdout
    report = json.loads(output.read_text(encoding="utf-8"))
    assert report["pass"] is True
    assert report["multiIteration"]["generatedTasks"] == 2
    assert report["multiIteration"]["automaticReplanEvents"] == 1
    assert report["multiIteration"]["taskStates"] == ["succeeded", "succeeded"]
    assert report["freshProcessRestart"]["crashExitCode"] == 23
    assert report["freshProcessRestart"]["goalState"] == "running"
    assert report["freshProcessRestart"]["taskStates"] == ["waiting"]
    assert report["freshProcessRestart"]["workerRunStates"] == ["running"]
    assert report["freshProcessRestart"]["escalationCodes"] == ["PROVIDER_STATE_AMBIGUOUS"]
    assert report["freshProcessRestart"]["telemetryOutcomes"] == {}
    assert report["credentialsAccessed"] is False
    assert report["networkAccess"] is False
