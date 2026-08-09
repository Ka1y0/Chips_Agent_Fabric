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
approval concepts. V0.1 bootstrap emits proposed capability actions as `approvalRequired`; it does
not implement the Privilege Broker or execute privileged actions. This is a **FOUNDATION**, not an
automated privilege system.

The implemented `CapabilityGrant` record checks scoped naming, subject/task matching, expiry and
revocation state, attribution, and timezone-aware timestamps. It is not signed, does not verify an
issuer trust chain or replay nonce, and does not interpret constraint values. Persistence is an
audit foundation only; no privileged executor may treat the current record alone as sufficient
authority.
