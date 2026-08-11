from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path


def test_local_worker_v2_survives_real_daemon_restarts(tmp_path: Path) -> None:
    root = Path(__file__).parents[1]
    output = tmp_path / "local-worker-v2-restart.json"
    result = subprocess.run(
        [
            sys.executable,
            str(root / "scripts/run_local_worker_v2_restart_acceptance.py"),
            "--output",
            str(output),
        ],
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
        timeout=90,
    )

    assert result.returncode == 0, result.stderr or result.stdout
    report = json.loads(output.read_text(encoding="utf-8"))
    assert report["pass"] is True
    assert report["protocolVersion"] == 2
    assert report["realDaemonProcesses"] is True
    assert report["realSupervisorProcess"] is True
    assert report["realWorkerSubprocess"] is True
    assert report["supervisorRuntimeRecreated"] is True
    assert report["supervisorHardExitCode"] != 0
    assert report["leaseExpiredBeforeTakeover"] is True
    assert report["responseLostAfterLaunch"] is True
    assert report["handleUnboundBeforeRecovery"] is True
    assert report["runningRestartObserved"] is True
    assert report["terminalRestartObserved"] is True
    assert report["lookupRecoveredOriginalJob"] is True
    assert report["canonicalResultCount"] == 1
    assert report["providerResultCollectedEvents"] == 1
    assert report["workerResultRecordedEvents"] == 1
    assert report["fixtureInvocationCount"] == 1
    assert report["registryLaunchCount"] == 1
    assert report["openEscalationCount"] == 0
    assert report["credentialsAccessed"] is False
    assert report["billableProviderUsed"] is False
