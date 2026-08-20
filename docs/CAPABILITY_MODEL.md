# Capability and permission model FOUNDATION

## Principle

Authority is an explicit data object, not ambient administrator privilege or a sentence in a model
prompt. A grant binds requester, task, node, operation, resource constraints, issuer, issue/expiry
time, nonce, policy version, and audit correlation. It is authenticated, least-privilege, revocable,
and attributable.

Initial scopes include `package.install`, `service.install`, `service.start`, `service.stop`,
`network.private.configure`, `credential.store`, `worker.register`, `worker.upgrade`,
`firewall.fabric-rule.configure`, and `model.runtime.configure`. Scope names do not themselves prove
implementation or grant access.

## Privilege Broker

The proposed flow is Supervisor request → signed/authenticated capability grant → Node Runtime →
Privilege Broker → one allowlisted operation. The Broker validates signature, scope, target, task,
expiry, replay nonce, local policy, executable/arguments, and rollback plan; records intent/result;
then returns a structured outcome. It must not expose an arbitrary command or unrestricted shell.

RED/high-risk operations require a durable human approval rooted in legitimate administrator action.
Routine operations may become zero-touch only after policy pre-authorization and trust establishment.
Revocation and expiry override queued work. A model cannot approve its own grant.

## Current state

V0 contains permission classes, deterministic policy checks, scoped observer tokens, and durable RED
approval concepts. V0.4 bootstrap persists proposed capability actions as `awaitingApproval` and can
record a bounded non-secret external result with an authorization reference. It does not validate a
real grant, implement the Privilege Broker, or execute privileged actions. This is a **FOUNDATION**,
not an automated privilege system.

The implemented `CapabilityGrant` record checks scoped naming, subject/task matching, expiry and
revocation state, attribution, and timezone-aware timestamps. It is not signed, does not verify an
issuer trust chain or replay nonce, and does not interpret constraint values. Persistence is an
audit foundation only; no privileged executor may treat the current record alone as sufficient
authority.

## Worker restart capabilities

Worker restart behavior is also explicit data, not something inferred from a provider or harness
name. An adapter may advertise `supportsReconcile`, `supportsResume`, `supportsCancel`,
`supportsProviderIdempotency`, `supportsStreamReconnect`, `supportsRepeatableCollect`,
`supportsIdempotentLaunchLookup`, and `supportsDurableLaunchRegistry` through the optional
durable-job contract. Supervisor persists the advertised values on each provider-job
intent; the public `supportsDurableCancel` value is derived from support for handle-based cancellation
on that durable adapter. Recovery requires both the persisted claim and support from the currently
registered adapter, plus an exact adapter type/instance match. Capability flags do not authorize an
operation or expand the Task's grant. Only an adapter's explicit pre-launch rejection type proves
that retry is safe; generic transport or HTTP failures remain ambiguous after the launch boundary.

The Local Worker HTTP v1 adapter advertises reconciliation, resume, cancellation, and repeatable
terminal collection, but not provider-side idempotency. A negotiated Local Worker v2 authority also
advertises durable launch lookup/registry and provider idempotency after those guarantees have been
observed from the server. Neither generation advertises stream reconnection, and neither weakens the
immutable read-only/no-code-write policy.

A production v2 authority may additionally advertise `supportsServerDriverProfiles` plus an
allowlisted driver catalog. This is execution capability discovery, not authorization. Supervisor's
process owner selects a driver ID; Task prose and Worker metadata cannot select an executable or
expand the Task grant. The persisted adapter identity incorporates the stable node/authority/
registry and driver profile revision/fingerprint, while the daemon's runtime instance remains an
ephemeral observation. Per-driver cancellation is truthful: an unverified Windows native process
tree must advertise false even when POSIX process-group cancellation is implemented.

Native Codex, Claude, Grok, and AGY CLI adapters implement the legacy execution interface. Supervisor
records their durable restart capabilities as false. They may cancel a child process while the
owning Supervisor is alive, but they do not support restart-safe attachment, result collection, or
durable cancellation. Unknown external state therefore requires reconciliation or human escalation,
not an inferred permission to launch again.

When one of those CLIs runs behind Local Worker v2, restart-safe lookup/reconcile/resume/collection
belongs to the Local Worker logical job and registry. It does not promote the underlying provider
CLI to provider-native resume or idempotency. Codex account type, quota, reset time, model, and usage
remain `UNKNOWN` unless a validated machine event supplies the value.
