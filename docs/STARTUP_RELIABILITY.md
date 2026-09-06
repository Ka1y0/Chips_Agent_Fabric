# Startup is part of execution

Status: implemented changes with focused regression gates. This is not a claim that every
intermittent host failure in issue #2 has been resolved.

## Leased engine initialization

The previous Host invoked and awaited `engine_factory` before starting its heartbeat monitor.
An asynchronous factory could therefore outlive the acquired Goal lease before `engine.run`
started. Making the timeout larger only hides this uncovered lifecycle interval.

The Host now monitors one task spanning factory initialization and engine execution. It checks
ownership before invoking the factory and after it returns, preserves strict unexpired-generation
renewal, and drains the initialization task on cancellation or lease loss. A factory returning
late, or swallowing cancellation without clearing the task cancellation request, cannot start a
new engine run. A failed factory still releases its own claim for the existing recovery policy.
No new permission, persistent service, automatic enrollment or resource acquisition is introduced.

Factories assemble engines; they must not dispatch Goal work themselves. Synchronous factories
must be quick and non-blocking. A factory performing blocking I/O on the event loop can still
starve the heartbeat. Uncooperative cancellation, host-incarnation fencing, clock jumps and
post-sample database latency are not solved by moving the initialization boundary.

Host configuration now rejects non-finite or non-positive timings and non-integer concurrency.
Default values and the requirement that lease TTL exceed the heartbeat interval are unchanged.

## A marker is not a listening socket

The Local Worker V2 application writes its ready marker during lifespan startup. That indicates
application recovery has finished, but the server may not yet have begun listening. The native
CLI acceptance previously started its first HTTP request as soon as this file existed.

The acceptance now waits for a successful loopback `/v2/health` response with the same authority,
registry, node and runtime incarnation as the marker, plus the requested default driver. The
existing 15-second startup polling window is unchanged. Startup connection failures are retried
only by this readiness gate; launch, cancellation and replay assertions are not relaxed. A failed
startup also reaps the test daemon and closes its logs.

The helper is for explicitly spawned, unauthenticated test daemons only. It uses bounded JSON
reads and a short socket timeout without proxies or redirects. It is not a production enrollment
API or protection against a malicious slow peer. An endpoint that was ready can still fail later;
normal adapter failure and recovery semantics remain necessary.

## Reproduce the focused checks

From an installed development checkout:

```sh
python -m pytest -v --tb=short \
  tests/test_autonomous_host_startup.py tests/test_acceptance_readiness.py
python -m pytest -v --tb=short tests/test_autonomous_lease_boundaries.py
python -m pytest -v --tb=short tests/test_local_worker_v2_native_cli_subprocess.py
```

The startup tests use real SQLite repositories with a controlled lease clock and an engine seam,
not live provider inference. The readiness tests include a real bound-but-not-listening socket,
then activate the listener explicitly without racing an arbitrary sleep. The existing native CLI
acceptance continues to exercise real processes with deterministic fake provider CLIs.

These focused tests also run inside the full unit selection and must not be double-counted.
No credentials, user workspace, model weights or billable provider account are required. Keep
issue #2 open until the remaining live-STOP and other historical failures are separately diagnosed.
