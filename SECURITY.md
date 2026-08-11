# Security

## Trust model

Supervisor is a control plane, not a general remote shell. SQLite is canonical state;
provider credentials remain in provider-owned authentication stores and below adapter boundaries.
Normalized events must never contain tokens, cookies, private keys, or raw credential files.
Identity, encrypted transport, and capability authorization are separate requirements; none implies
the others. See `docs/TRUST_MODEL.md` and `docs/CAPABILITY_MODEL.md`.

## Bootstrap and privileged operations

The universal bootstrap FOUNDATION is read-only by default. `--emit` writes only non-secret review
files to a new explicit directory. An explicit `--state-db` may write only a durable SQLite
plan/audit recorder. Both remain host-operation dry-runs: neither installs, logs in, connects to a
Fabric, starts services, requests elevation, opens listeners, configures networking, creates identity
keys, or registers Workers. Structured results record an external operation after the fact; they do
not execute it, verify a real grant, or grant authority. Command/credential-shaped evidence and model
self-approval are rejected.

Future routine privilege must pass an authenticated Node Runtime and allowlisted Privilege Broker.
Grants are explicit, least-privilege, task/target scoped, attributable, auditable, revocable, and
time-bounded where practical. The Broker must not accept arbitrary shell commands. An initial trust
root may come from legitimate administrator installation, enterprise provisioning, or trusted
enrollment; “zero-touch” does not mean bypassing OS consent or security controls.

## Safe defaults

- Listen on loopback only (`127.0.0.1`).
- Expose observation APIs read-only by default.
- Keep Goal mutations disabled by default and require a separate `goal:control` bearer when enabled.
- Require durable approval for RED actions.
- Keep API tokens capability-scoped and salted-hash-only at rest.
- Treat missing quota, usage, and cost as unavailable, never zero.
- Forbid local-model workers from writing production code.
- Use isolated working directories for worker processes.
- Preserve append-only event history and explicit cancellation/timeout evidence.

Autonomous planning does not broaden authority. Planned actions still pass the existing deterministic
Hybrid Engine constraints, Worker capability/permission checks, RED approval rules, adapter safety
policy, and local-model code-write prohibition. Evaluator, planner, or Worker prose cannot grant a
capability or override a safety boundary. Token/cost limits with unobservable consumption fail closed.

## Remote access

Non-loopback configuration is rejected unless both `private_transport` and TLS are configured. This
validation is a guardrail, not proof that a network is private. Operators must separately verify the
overlay identity, routing, certificate lifecycle, firewall rules, and absence of public exposure.

Do not expose raw MCP, a model server, arbitrary filesystem access, arbitrary URL proxying, or shell
execution through the monitor API. Mobile clients receive only their declared capability tier.

Tailscale is the host-verified V0 private transport, not a permanent architectural dependency.
Transport adapters must expose authenticated/encrypted peer identity, reachability, latency, and
health semantics. WireGuard, Headscale, or native mTLS support requires separate implementation and
verification; no fallback may expose an ordinary-LAN or public listener.

### Bundled Local Worker v2 runtime

The bundled daemon is deliberately loopback-only; its CLI rejects `0.0.0.0`, ordinary-LAN, and
public binds. Test/development mode may use explicit single-user local trust. `--production` instead
requires `--auth-token-file`: one daemon-owned, non-symlink, exact-0600 file containing a bounded
URL-safe bearer. Constant-time middleware protects every v1/v2 health, launch, lookup, job, and
cancel route, and the token never enters registry state or responses. This is still a single local
authority, not an authenticated remote/multi-tenant design. A future remote deployment requires a
separately verified client identity, rotation/revocation, scoped namespaces, and mTLS or approved
overlay transport. Until then, remote mode is disabled rather than trusting a network location.

Production execution is an allowlisted driver service, not a generic RCE endpoint. The wire schema
rejects unknown fields and never accepts executable, shell, argv, environment, working directory,
plugin/tool, credential, or arbitrary URL fields. Operator-owned profiles resolve an absolute
regular executable, hash it, bind a revision/fingerprint into the launch identity, and are rechecked
before spawn. Prompt input is carried only inside a bounded anonymous execution document; the
durable registry stores its digest rather than its contents.

The native process runtime uses no shell interpolation, strips ambient credential/proxy/module
variables, fixes an isolated per-job working directory, bounds stdout/stderr/lines/events/diagnostic
data, and treats malformed or missing terminal output as failure. POSIX cancellation terminates the
owned process group with bounded force escalation. Windows Job Object containment and native driver
cancellation are not verified and therefore must not be advertised as supported. Forced loss of the
logical runner while a separately-sessioned provider CLI is alive remains an ambiguity that cannot
authorize relaunch; OS-level containment is the next hardening step.

Registered Local Worker Claude and Grok profiles use pinned provider contracts that force plan mode,
an empty built-in-tool set, and provider-specific web/subagent/memory/customization denial flags.
Unsupported flags fail before model execution. AGY uses its provider `sandbox`/`plan` mode. These
controls do not replace OS containment, so profiles remain an explicit operator grant to run the
installed CLI as a least-privilege daemon service account. The current Claude/Grok/AGY dialects
place prompt text in process arguments, so secrets must not be placed in those prompts until a
verified stdin or protected request-file dialect exists. Provider-created files, including AGY's
diagnostic file, have bounded ingestion but no cross-platform filesystem quota yet.

The Codex profile uses fixed `codex exec` arguments, never a shell; prompt input is anonymous stdin
rather than argv. It forces read-only sandboxing, approval `never`, ephemeral execution, ignored user
configuration, strict config parsing, no search/additional directories, a private bounded terminal
file, and independent validation of requested JSON Schema. Codex exposes no reviewed equivalent of
Claude/Grok's empty-tool mode, however. Therefore the authenticated daemon and a least-privilege
service account remain part of the production authority boundary, and tasks must not assume that
Codex read-only mode hides every file readable by that account. Provider credentials remain in the
CLI-owned login store and are never read, copied, persisted, or projected by Supervisor.

### Windows Local Worker V0

The approved V0 path is defense in depth:

1. Tailscale device identity and tailnet policy;
2. Tailscale HTTPS Serve terminating only to `http://127.0.0.1:7331`; and
3. the Worker's independent bearer authentication.

The Worker remains loopback-only. Tailscale Funnel, subnet routing, exit-node advertisement, public
DNS exposure, ordinary-LAN binding, LM Studio `:1234`, and raw MCP exposure are out of scope and
prohibited. The Local Worker adapter disables redirects and rejects embedded URL credentials. It
also rejects every non-loopback HTTP URL and every non-loopback request without a bearer token.
Its dedicated tailnet HTTP client ignores inherited proxy environment variables so private Worker
traffic cannot be diverted through a general outbound proxy; normal TLS verification remains on.

A rejected credential is not treated as a transient fault. The adapter maps `401`/`403` to the
`authRequired` run state and `429` to `rateLimited`; the runtime then records `FailureClass.AUTH` and
marks the worker offline instead of retrying against an endpoint that will keep refusing. Timeouts
cancel the remote job on a best-effort basis, and a failed cancel never downgrades an observed
timeout into a generic transport failure.

The monitor API publishes the Local Worker harness as `lmStudio`. That value is a wire-compatibility
alias for the Cyber Office `WorkerHarness` enum only; the supervisor never contacts an LM Studio
endpoint.

### Node runtime recovery V0.4

The recovery monitor may probe only an explicit loopback HTTP(S) model endpoint and exposes no REST
mutation route. A configured recovery policy is not a capability grant. A deployment must inject an
external authorizer and a typed Node Runtime adapter that exposes only `runtime.start` and enforces
generation fencing, deadline, and recovery-ID idempotency at the node-side trust boundary.

Lease state prevents duplicate live Supervisor owners but does not authorize the side effect. The
adapter request deliberately has no shell, executable, argv, environment, credential, firewall,
listener, UAC, Defender, CORS, Funnel, MCP, or administrator field. A missing binding or authorization
fails closed. Runtime recovery must never change the loopback-only LM Studio boundary or the private
Worker transport merely to make a health gate pass.

## Secrets

`.env.example`, manifests, event payloads, test evidence, and documentation contain no real secret.
Store client bearer tokens in an OS credential manager. Provider login remains human-controlled.
Token creation prints a secret once; redirect it only into a secure credential-ingestion workflow,
not a plaintext file.

Worker bearers follow the same rule. The private endpoint hostname is configuration; the bearer is
not. Acceptance scripts may retrieve a bearer from the OS credential manager into process memory,
but must never print it, persist it in evidence, pass it in a URL, or enable HTTP client tracing.

`project_supervisor.credentials` is the only supported retrieval path. It reads
`PROJECT_SUPERVISOR_LOCAL_WORKER_TOKEN` from a short-lived process environment first, then falls back
to the macOS login Keychain via a fixed `security find-generic-password` argument vector that never
contains the secret. It fails closed rather than returning an empty credential, and never propagates
process output on error. Add the item with `-w` last so the value is prompted for:

```sh
security add-generic-password -U -s project-supervisor-local-worker -a worker-node-01 -w
```

If sensitive output is detected, stop displaying it, redact copies, rotate the affected credential
through its official provider flow, and record only `SENSITIVE_OUTPUT_DETECTED` in shared evidence.

## Reporting

Use the canonical repository host's private security-advisory channel when it is available. If no
private advisory channel is configured, do not publish exploit details or captured runtime evidence;
contact a maintainer through an already trusted private channel and share only the minimum redacted
reproduction. Maintainers should acknowledge, triage, coordinate a fix and disclosure window, and
credit reporters who want attribution. Never attach credentials, private topology, or unrelated user
data to a report.
