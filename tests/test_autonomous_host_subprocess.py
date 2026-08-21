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
_original_launched_action_run = acceptance._launched_action_run
_restart_gates: dict[str, tuple[Path, Path]] = {}


def _gate_action_worker(fixture):
    started = fixture.root / "gated-action-worker-started"
    release = fixture.root / "gated-action-worker-release"
    fixture.environment["FABRIC_ACCEPTANCE_WORKER_STARTED"] = str(started)
    fixture.environment["FABRIC_ACCEPTANCE_WORKER_RELEASE"] = str(release)

    source = fixture.executable.read_text(encoding="utf-8")
    source = source.replace(
        "import time\\n",
        "import time\\nfrom pathlib import Path\\n",
        1,
    )
    delay_line = "time.sleep(0.05)\\n"
    gate = r'''gate_started = os.environ.get("FABRIC_ACCEPTANCE_WORKER_STARTED")
gate_release = os.environ.get("FABRIC_ACCEPTANCE_WORKER_RELEASE")
if gate_started and gate_release and "Canonical bounded input:\\n" not in prompt:
    Path(gate_started).write_text(str(os.getpid()), encoding="utf-8")
    gate_deadline = time.monotonic() + 60
    while not Path(gate_release).exists():
        if time.monotonic() >= gate_deadline:
            raise SystemExit(124)
        time.sleep(0.01)
else:
    time.sleep(0.05)
'''
    if delay_line not in source:
        raise RuntimeError("fake Worker fixture does not contain the expected delay boundary")
    fixture.executable.write_text(source.replace(delay_line, gate, 1), encoding="utf-8")
    _restart_gates[str(fixture.root)] = (started, release)
    return fixture


def _fixture(
    root: Path,
    *,
    delay_seconds: float = 0.20,
):
    # Ensure the hard-pause scenario observes live work instead of a completed fixture.
    if root.name == "controls":
        delay_seconds = max(delay_seconds, 1.0)
    if root.name == "restart":
        return _gate_action_worker(_original_fixture(root, delay_seconds=0.05))
    return _original_fixture(root, delay_seconds=delay_seconds)


def _launched_action_run(fixture, goal_id):
    run = _original_launched_action_run(fixture, goal_id)
    if run is None:
        return None
    gate = _restart_gates.get(str(fixture.root))
    if gate is None:
        return run
    started, _release = gate
    if not started.is_file():
        return None
    try:
        started_pid = int(started.read_text(encoding="utf-8").strip())
        run_pid = int(run["process_id"])
    except (OSError, TypeError, ValueError):
        return None
    return run if run_pid == started_pid else None


acceptance._fixture = _fixture
acceptance._launched_action_run = _launched_action_run
try:
    exit_code = acceptance._run_all(Path(sys.argv[1]).resolve())
finally:
    for _started, release in _restart_gates.values():
        release.touch(exist_ok=True)
raise SystemExit(exit_code)
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
