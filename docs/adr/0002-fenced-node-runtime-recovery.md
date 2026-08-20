# ADR 0002: Fenced node-runtime recovery host

Status: accepted for V0.4

## Context

V0.3 could probe a loopback model runtime and issue a typed `runtime.start` after external
authorization, but it had no automatic monitor and no cross-process single-flight owner. A process
could crash after requesting start but before verification, and two Supervisors could otherwise
attempt the same recovery. A remote privileged side effect cannot be made safe by an in-process
lock or by SQLite state alone.

## Decision

1. One SQLite lease row exists per recovery policy. Ownership uses a monotonically increasing
   generation, expiry, heartbeat, compare-and-set release, and transactional assertions before
   every canonical recovery mutation.
2. Stale acquisition increments the generation and atomically terminates unfinished prior attempts.
   The successor probes before issuing another start, so an already recovered runtime becomes an
   observation rather than another side effect.
3. A node adapter must durably enforce the Supervisor generation as a fencing token. It must also
   enforce a request deadline and recovery-ID idempotency. An adapter that only carries these fields
   but does not enforce them is rejected by the descriptor contract.
4. Adapter requests expose one operation, `runtime.start`. There is no command, argv, environment,
   credential, package, firewall, listener, or arbitrary administrative field.
5. A monitor host uses persistent per-policy next-observation checkpoints, bounded concurrency,
   heartbeat, normal interval, and bounded exponential failure backoff. Restart resumes from SQLite.
6. Deployment bindings are process-local and injected. Missing probe/adapter/authorizer bindings
   fail closed and are never reconstructed from model output or chat history.
7. The observation API remains read-only. Policy mutation, capability grant, and recovery execution
   are not added to the REST surface by this ADR.

## Consequences

- SQLite prevents duplicate live Supervisor owners and fences stale state writers.
- Node-side enforcement prevents a delayed old owner from producing an accepted privileged action.
- Recovery host liveness and polling schedule are observable and replayable without storing secrets.
- Real Windows self-healing still depends on an operator-installed, allowlisted Privilege Broker or
  Node Runtime adapter. The generic seam is implemented; a real pc-gpu-01 recovery gate remains an
  environment-specific acceptance step.
- LM Studio, Worker, raw MCP, and administrative ports remain loopback/private and are never opened
  as part of recovery.
