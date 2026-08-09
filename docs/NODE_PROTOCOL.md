# Node protocol FOUNDATION

A Node is a machine identity and resource boundary. It is distinct from a Worker, provider, model,
session, and transport address.

## Enrollment semantics

A future enrollment exchange must bind a stable public-key identity to an operator-approved Fabric,
prove possession, authenticate both peers, negotiate protocol versions, and return only scoped node
authority. Private keys remain in OS secure storage and never enter git, logs, manifests, events, or
Bridge payloads. Enrollment material must expire or be single-use.

## Node advertisement

A Node advertises signed/current facts: node ID, supported protocol versions, OS/architecture,
resources, Worker references, capabilities, policy restrictions, availability, load, transport
health, and observation timestamps. Advertised capability is not authorization; Supervisor applies
task grants and policy independently.

## Transport-neutral health

Supervisor consumes `authenticated`, `encrypted`, `peerIdentity`, `reachable`, `latency`, and
`transportHealth`, not vendor commands. Tailscale, WireGuard, Headscale, or native mTLS adapters may
produce these semantics. No one transport is architectural authority.

`TransportStatus.safeForFabric` represents the base conjunction of authenticated, encrypted,
identified, and reachable observations. It does not establish freshness, certificate validity,
firewall policy, or health consistency by itself. Dispatch policy must apply an observation-age
limit and provider-specific verification before using a route.

## Recovery

On restart, a Node reuses its stable identity, validates canonical state and grants, reconnects over
an authenticated private transport, reconciles active work by opaque run/session IDs, and emits an
auditable recovery event. Identity mismatch, rollback, stale grant, or ambiguous ownership fails
closed and requires review.

This document is a **FOUNDATION**. Cryptographic enrollment and portable node-runtime services are
not claimed complete until implementations, schemas, threat review, and multi-platform tests exist.
