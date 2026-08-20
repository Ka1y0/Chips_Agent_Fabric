from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path


def test_production_autonomous_host_real_subprocess_acceptance(tmp_path: Path) -> None:
    root = Path(__file__).parents[1]
    output = tmp_path / "autonomous-host-acceptance.json"
    runner = tmp_path / "run-autonomous-host-acceptance.py"
    runner.write_text(
        """from pathlib import Path
import sys

from scripts import run_autonomous_host_acceptance as acceptance

_original_fixture = acceptance._fixture


def _fixture(root: Path, *, delay_seconds: float = 0.20):
    # Ensure the hard-pause scenario observes live work instead of a completed fixture.
    if root.name == "controls":
        delay_seconds = max(delay_seconds, 1.0)
    return _original_fixture(root, delay_seconds=delay_seconds)


acceptance._fixture = _fixture
raise SystemExit(acceptance._run_all(Path(sys.argv[1]).resolve()))
""",
        encoding="utf-8",
    )
    result = subprocess.run(
        [sys.executable, str(runner), str(output)],
        cwd=root,
        env={
            "HOME": str(tmp_path / "home"),
            "PATH": os.environ.get("PATH", "/usr/bin:/bin"),
            "PYTHONPATH": os.pathsep.join((str(root / "src"), str(root))),
            "PYTHONDONTWRITEBYTECODE": "1",
            "TMPDIR": str(tmp_path),
            "NO_COLOR": "1",
        },
        check=False,
        capture_output=True,
        text=True,
        timeout=120,
    )

    assert result.returncode == 0, result.stderr or result.stdout
    report = json.loads(output.read_text(encoding="utf-8"))
    assert report["pass"] is True
    assert report["productionCLI"] is True
    assert report["realHostSubprocesses"] is True
    assert report["realWorkerSubprocesses"] is True

    concurrent = report["concurrentMultiIteration"]
    assert concurrent["terminationReason"] == "SUCCESS"
    assert concurrent["iterations"] == 2
    assert concurrent["generatedActionTasks"] == 2
    assert concurrent["leaseAcquisitions"] == 1
    assert concurrent["duplicateHostIdentityExitCode"] != 0
    assert concurrent["postTaskAuditCount"] == concurrent["terminalTaskCount"]
    assert concurrent["unknownProvenancePreserved"] is True
    assert concurrent["quotaGuardExhaustedRejections"] > 0
    assert concurrent["workerIDsUsed"] == ["worker-claude-fixture"]

    controls = report["humanControls"]
    assert controls["softPauseNoNewDispatch"] is True
    assert controls["softResumeTermination"] == "SUCCESS"
    assert controls["hardResumeTermination"] == "SUCCESS"
    assert controls["steeredPlanObserved"] is True
    assert controls["stopTermination"] == "USER_STOPPED"

    recovery = report["freshProcessRecovery"]
    assert recovery["goalState"] == "running"
    assert recovery["goalTermination"] is None
    assert recovery["actionRunStates"] == ["running"]
    assert recovery["actionTaskStates"] == ["waiting"]
    assert recovery["escalationCode"] == "PROVIDER_STATE_AMBIGUOUS"
    assert recovery["staleLeaseRecoveryEvents"] >= 1

    assert report["credentialsAccessed"] is False
    assert report["networkAccess"] is False
