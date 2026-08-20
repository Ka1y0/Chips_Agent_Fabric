# Node runtime recovery

Status: **V0.4 IMPLEMENTED CONTROL PLANE / DEPLOYMENT FOUNDATION**. Durable monitoring, fencing,
restart reconciliation, and the typed adapter boundary are implemented and deterministic-test
verified. A real Windows Privilege Broker/LM Studio service adapter has not been installed or
host-verified, so this is not yet a claim of complete pc-gpu-01 self-healing.

## Implemented flow

```text
persistent next-observation checkpoint
  → side-effect-free loopback health/model probe
  → healthy: observe, checkpoint, schedule next bounded poll
  → unavailable: Node DEGRADED; eligible Workers offline
  → acquire durable per-policy lease generation
  → external capability authorizer approves service.start
  → typed node adapter receives runtime.start + deadline + idempotency key + fencing generation
  → side-effect-free health/model re-verification
  → READY, or fail closed with bounded exponential backoff
```

SQLite migrations `0008_node_runtime_recovery.sql` and `0009_node_recovery_host.sql` persist
policies, attempts, monitor hosts, per-policy polling checkpoints, and one fenced lease per policy.
`RuntimeRecoveryMonitor` discovers only enabled/due policies, caps concurrent policies, heartbeats
while work is active, and waits on an event/timeout rather than busy-spinning. Healthy and failed
polls receive distinct intervals; failure backoff is bounded to one day. A restart reads
`next_observation_at` from SQLite instead of forgetting or immediately hammering a runtime.

## Probe boundary

`RuntimeRecoveryPolicy.backend_endpoint` must be an explicit-port HTTP(S) loopback URL. Credentials,
application paths, query strings, fragments, LAN addresses, tailnet addresses, and public addresses
are rejected.
Encrypted Fabric transport terminates at the Node Runtime; LM Studio or another model runtime stays
node-local.

`LoopbackModelsHTTPProbe` performs only a direct `GET` to `/v1/models`. It does not use environment
proxy settings, follow redirects, send authorization, execute `lms`, launch a GUI, start a daemon,
or fall back to another transport. Timeout is capped at five seconds and the body at one MiB.

This probe pattern was independently compared with project-owned
`Project_LMStudio_MCP/src/lmstudio/api.ts` at private commit
`322e286a8a2254a2b3d95752eb3ba2d004b765a1`. That MIT-licensed project also uses a short bounded
`GET /v1/models` probe and a hard `NO_GUI_AUTOSTART` invariant. Fabric reuses the safety principle,
not its MCP server, credential handling, or mutating model-control code.

## Fenced single-flight ownership

Every observation/recovery call acquires `node_runtime_recovery_leases.policy_id`, which is the
SQLite primary key. A live, unexpired owner excludes all contenders. A stale takeover atomically:

1. increments `generation`;
2. records the previous owner;
3. marks unfinished prior attempts `failed/staleLeaseRecovered`;
4. emits `runtimeRecoveryLeaseRecovered`; and
5. fences every later state/event mutation from the old generation.

Lease assertions occur inside the same `BEGIN IMMEDIATE` transaction as canonical mutations. An
old caller cannot mark a Node READY, change a Worker, or append a recovery outcome after losing its
generation. Releases are compare-and-set by policy, owner, and generation.

Because a remote side effect can outlive a Supervisor process, SQLite fencing alone is insufficient.
The node adapter request therefore carries:

- `leaseGeneration` — monotonically increasing fencing token;
- `idempotencyKey` — exactly the durable recovery ID, reused across bounded retries;
- `requestedAt` and `deadlineAt`; and
- an opaque capability authorization reference.

The adapter descriptor must affirm node-side enforcement of fencing, deadline, and idempotency,
must expose only `runtime.start`, and must declare a maximum operation duration that leaves ten
seconds of lease margin. Accepted results must echo the observed fencing generation. The machine
contract is [`../schemas/node-runtime-adapter-v1.schema.json`](../schemas/node-runtime-adapter-v1.schema.json).

## Capability and adapter boundary

A configured policy is not authority. `NodeRuntimeRecoveryService` defaults to a deny-all
authorizer. Each attempt carries node, runtime, capability, operation, requester, and attempt
attribution. Authorization returns only an opaque `grant-*`, `approval-*`, or `capability-*`
reference; bearer credentials are rejected by the type contract.

The typed request has no executable, shell, argv, environment, credential, firewall, listener,
UAC, Defender, CORS, Funnel, MCP, or administrator field. A production Windows adapter must be an
operator-installed Node Runtime/Privilege Broker implementation that maps the one typed operation to
an allowlisted local service action and durably rejects stale fencing generations. It must not be a
generic command runner.

`RuntimeRecoveryBinding` is process-local. The probe, adapter, and authorizer—including any approved
secure-store handles—are injected by deployment code and never serialized into policy, checkpoint,
lease, event, or schema data. Missing bindings fail closed as `bindingUnavailable` with backoff and
do not mark the Node degraded because no health observation was made.

## Restart recovery and observability

Monitor rows expose process/state/heartbeat, active policy count, observation count, verified
recovery count, stale-takeover count, and a redacted last error. Per-policy read projections expose:

- latest attempt and terminal/failure state;
- lease owner, generation, heartbeat, expiry, and recovery state;
- last monitor state/observation/error;
- consecutive failure count; and
- next scheduled observation.

`GET /v1/nodes/recovery` remains observation-only. It cannot configure a policy, provide a binding,
grant authority, or trigger recovery. Canonical state and normalized events remain in SQLite.

## Current external blocker

The generic production-safe seam is implemented, but this repository deliberately contains no
machine-specific Windows service command and has no deployed signed Privilege Broker. Completing
real pc-gpu-01 recovery requires an operator-reviewed Node Runtime adapter that can prove:

1. authenticated private node identity;
2. an allowlisted LM Studio service installation/start mechanism;
3. node-side durable generation fencing, deadlines, and idempotency;
4. no LAN/public/raw-MCP exposure and loopback `127.0.0.1:1234` verification; and
5. real expected-model readiness after a controlled stop/start acceptance test.

Until that external adapter and trust root exist, report `NODE_SELF_HEALING =
MATERIAL_PROGRESS_WITH_BLOCKER`, not complete production self-healing.
