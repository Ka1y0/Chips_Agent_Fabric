# Blockers and external gates

## V0 baseline

No unresolved technical blocker remains for the approved private V0 scope. Authenticated private
transport, loopback-only Worker/model runtime, real inference, structured output, polling,
cancellation, timeout, Supervisor persistence, and Cyber Office REST/WebSocket observation passed.
Private deployment details remain in excluded local evidence.

## V0.2 beta release gate

The target is package `0.2.0b1` and prerelease tag `v0.2.0-beta.1`. No known design blocker prevents
the candidate, but release remains fail-closed on the current full test, lint, package, migration,
recovery, scope, secret, and remote-verification gates. The active release authorization permits a
normal fast-forward update, a new annotated prerelease tag, and a GitHub prerelease only after those
gates pass. It does not permit a force push, history rewrite, remote visibility change, or mutation
of historical V0.1.

Private topology/evidence under `artifacts/` is never a source-release input. The deterministic,
privacy-audited archive is the only candidate suitable for broader redistribution unless the Git
history receives a separate provenance/privacy approval; this release does not rewrite that history.

## Foundation limits, not release blockers

- Zero-touch install/enrollment/recovery and Privilege Broker execution are not complete.
- Windows/Linux bootstrap discovery is contract-tested but not fully real-host validated.
- Generic transport interfaces exist; only the sanitized V0 Tailscale topology has a real-host gate.
- Generic Worker permission fields and capability-grant records do not yet form a privileged policy
  executor.
- Project_Bridge is an optional disabled-by-default foundation; no licensed production codec exists.
- The source archive is byte-reproducible, while a fully locked multi-platform dependency
  environment remains future work.
- Windows Job Object process-tree containment is pending.
- Full canonical reconstruction solely from the append-only event journal is pending.
- Provider-native resume, reconnect, and idempotency remain unavailable where the underlying CLI
  does not implement them; Local Worker V2 does not manufacture those provider capabilities.
- Advanced cross-machine recovery still requires operator policy and authority reconciliation.

These limits are acceptable only because documentation, schemas, and capability status label them
FOUNDATION/PLANNED rather than COMPLETE.
