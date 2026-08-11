# Universal Worker protocol

## Boundary

A Worker is a capability-bearing execution peer, not a model name, CLI, node, or provider. The
Supervisor scheduler consumes normalized Worker semantics; adapters contain provider-specific CLI
flags, event shapes, sessions, authentication behavior, and error mappings.

## Generic concepts

Every adapter should expose, where observable: stable Worker identity, node/provider/harness,
models, capabilities, context limits, availability, latency, permissions, health, execute, stream,
cancel, session/resume, telemetry, usage, cost, and quota. Unknown observations carry an explicit
unavailable reason and confidence; they are never synthesized.

## Execute lifecycle

1. Supervisor validates task requirements, privacy, permission class, and policy.
2. Scheduler selects an eligible Worker from one frozen snapshot and persists reasons.
3. Adapter accepts a bounded normalized request with opaque task/run/session identifiers.
4. Adapter emits normalized start/progress/heartbeat/tool/usage/result/error events.
5. Supervisor owns cancellation, timeout, verification, state transitions, and persistence.

At-least-once events require deduplication. Provider prose cannot grant authority or report a
terminal Supervisor state. Raw output may be retained only as sanitized evidence by reference.

## Existing implementations

Codex, Claude, Grok, and AGY are native subprocess adapters. The Local Worker is a versioned HTTP(S)
adapter to a loopback-only model runtime behind authenticated private transport. Their exact support
is in `CAPABILITIES.md` and tests. New providers must not require scheduler-core semantic changes.

Local inference remains read-only/non-code by policy and must be rejected for production code,
patch, or repository-edit tasks at both scheduling and adapter boundaries.

`WorkerContract.permissions` is an advertisement for discovery and routing, not a capability grant
or a generic enforcement engine. Existing scheduler and adapter checks remain authoritative until a
future policy executor binds a verified contract to a task-scoped grant. Native adapters inherit an
allowlist of non-secret OS context rather than the entire parent environment; provider-specific
variables must be supplied explicitly by trusted configuration and are never emitted.

## Local Worker protocol generations

Local Worker v1 remains compatible: health is observed at `GET /v1/health`, work is created with
`POST /v1/jobs`, and a known job can be queried or cancelled by ID. V1 has no server-side launch
key. Once its POST crosses the transport boundary, response loss is ambiguous and Supervisor must
not infer that the job was rejected or launch it again.

Local Worker v2 adds a durable launch registry owned by the daemon. Before dispatch, Supervisor
observes the daemon's protocol versions, durable authority/registry identities, and capability
flags. It advertises provider idempotency only when all of these are present and operational:

- a restart-safe registry;
- lookup by idempotency key;
- same-key/same-digest replay returning the existing canonical launch; and
- same-key/different-digest rejection without creating another job.

Supervisor sends its already-persisted `supervisor-execution:<run-id>` key to
`POST /v2/launches`. The daemon independently canonicalizes the immutable execution and job input,
computes its SHA-256 request digest, and rejects a client digest that does not match. The registry
stores the digest, not the prompt or credential-bearing request payload.

An accepted response includes protocol version, idempotency key, request digest, launch-record ID,
job ID, receipt ID, launch state, and failure disposition. A pre-launch rejection receipt is valid
only when the daemon can prove no child crossed the spawn boundary. `IDEMPOTENCY_CONFLICT` is not a
pre-launch rejection because the launch originally bound to that key may already be running.

`GET /v2/launches/{idempotency_key}` is side-effect free and distinguishes `NOT_SEEN`,
`REJECTED_PRE_LAUNCH`, `RESERVED`, `LAUNCHING`, `RUNNING`, `COMPLETED`, `FAILED`, `CANCELLED`, and
`UNKNOWN`. `NOT_SEEN` is authoritative only when the durable registry itself is healthy. Database,
transport, authentication, or schema failures produce unavailable/unknown behavior and never
permission to relaunch.

The v2 daemon commits `RESERVED` before entering the OS spawn boundary. A same-digest replay may
continue a reserved record. It commits `LAUNCHING` and a launch nonce before spawning, then records
a child receipt containing job ID, nonce, host identity, PID, and process-birth identity. On daemon
restart it reconciles receipts and process identity; it never automatically spawns a `LAUNCHING`
record. PID alone is never sufficient, and a mismatch is ambiguous rather than absent.

This supports exactly one logical Local Worker launch per idempotency key when requests reach the
same healthy durable Local Worker authority. It does not establish universal exactly-once side
effects across other providers, arbitrary downstream tools, storage loss, or authority changes.

## Implemented execution topology

The runtime has two real execution paths. A persisted Worker using `codex`, `claudeCode`,
`grokBuild`, or `googleAgy` is launched directly by Supervisor through its native subprocess
adapter. A persisted
`localWorker` is launched through `LocalWorkerAdapter`, which negotiates HTTP v1/v2 with a separate
Local Worker authority. Autonomous evaluator/planner/action Tasks use the same scheduler/runtime
and do not bypass either boundary.

Codex uses the installed CLI's provider-owned login session; Supervisor neither reads its
credential store nor introduces an API key, and account/subscription source remains unknown unless
the CLI reports it. Google execution is the AGY adapter, not a separately
implemented Gemini adapter. A local model remains behind an external Local Worker; Supervisor never
talks directly to LM Studio. The bundled v2 daemon is the implemented local runtime; historical
Windows Worker v1 deployments remain external and compatible.

## Registered Local Worker drivers

The production v2 daemon resolves a semantic `driver_id` against an immutable, operator-loaded
catalog. A profile contains a driver type, positive revision, executable content hash (for native
drivers), trusted owner/mode/device/inode identity, bounded execution deadline, and a derived profile
fingerprint. Health advertises only a safe profile projection. The executable path, argv template,
environment, working directory, and credentials are never accepted from HTTP.

Supervisor freezes the selected node, authority, registry, and driver-profile fingerprint in its
adapter instance identity before persisting provider-job intent. The request digest includes the
selected `driver_id`; launch/lookup/status/cancel receipts echo the persisted driver identity. A
profile change after restart is therefore an identity mismatch and cannot silently reattach or
launch another CLI.

The catalog supports registered Codex, Claude, Grok, and AGY profiles through native adapters. The
deterministic profile is a test driver and production mode rejects it. A Codex profile is available
only while its operator-pinned executable identity remains current; no wrapper invents Codex
provider resume, provider idempotency, or stream reconnect. Future drivers can join this catalog
without changing the v2 launch protocol.

Local Worker Claude/Grok invocations are narrower than the direct native-adapter default: their
server-owned argv forces provider plan mode and an empty tool set, with customizations/web/subagents/
memory disabled where the pinned CLI contract exposes those controls. AGY retains its sandboxed plan
contract. A changed or unsupported CLI fails closed; none of these provider controls is represented
as an OS sandbox or as authority beyond the selected read-only Worker capability.

Codex uses fixed `codex exec --json` arguments, `read-only` sandbox mode, approval `never`, ephemeral
sessions, ignored user configuration, strict configuration parsing, and an isolated working
directory. The prompt is written to stdin and never placed in argv. A private bounded
`--output-last-message` file is the authoritative terminal result; every JSONL line is structured,
bounded, and must contain a typed event, and the final agent message must agree with the terminal
file. Structured requests also pass a private schema file and are independently validated locally.
Codex does not expose a verified no-tool switch, so production profiles require the authenticated
loopback daemon and a least-privilege service account; read-only must not be interpreted as secrecy
from every host-readable file.

Driver execution preserves the Supervisor-assigned provider/adapter identity and reports model,
session, and usage only when the CLI actually supplies them. Subscription/API/local source,
account, plan, quota, and reset metadata remain `UNKNOWN` when the provider does not expose them;
the Local Worker never guesses or independently switches providers. Subscription-first routing is
still a Supervisor policy decision, while the daemon executes only the already-authorized driver
assignment.

## Bounded native process runtime

Native adapters construct argument vectors without a shell, inherit only an allowlisted environment,
use an isolated working directory, redact captured output, and enforce independent stdout, stderr,
line, event, diagnostic, and wall-clock limits. Malformed structured output, a missing/duplicate
terminal event, an error terminal, overflow, or a pipe/framing failure is canonical `FAILED` even if
the CLI exits zero. Cancellation targets the owned POSIX process group with graceful termination and
bounded force-kill escalation; Windows does not inherit that claim without a verified Job Object or
equivalent process-tree implementation.

The daemon launches a fixed logical runner and passes its bounded execution document through
anonymous stdin. The registry stores identities and the request digest, never the prompt. The
runner writes nonce-, host-, PID-birth-, driver-, and executable-hash-bound started/terminal
receipts. Daemon restart reconciles those receipts and never respawns a `LAUNCHING` record.

Failure semantics remain fail-closed: admission/pre-launch rejection proves no process started;
running and completed receipts prove a launch; process/profile/receipt mismatch is unknown; and
`UNKNOWN` is never permission to retry. Exactly-once refers only to one logical Local Worker launch
inside one intact authority/registry, not to arbitrary tools invoked by a provider CLI.

The bundled TCP daemon remains loopback-only. Its `--production` mode also requires an owner-only
0600 bearer file and authenticates every health/control route; unauthenticated local-trust mode is
limited to tests and deliberate single-user development. Non-loopback and multi-client deployment
remain disabled until a scoped remote identity/transport design exists.
