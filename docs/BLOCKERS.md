# Blockers and external gates

## V0 baseline

No unresolved technical blocker remains for the approved private V0 scope. Authenticated private
transport, loopback-only Worker/model runtime, real inference, structured output, polling,
cancellation, timeout, Supervisor persistence, and Cyber Office REST/WebSocket observation passed.
Private deployment details remain in excluded local evidence.

## V0.1 publication boundary

The `v0.1.0-alpha.1` public seed is authorized only from the audited clean archive and a new Git root.
The private development history remains non-publishable because it retains private V0 evidence.
Mirroring, signing infrastructure, runtime exposure, and later feature development are separate
decisions and are not implied by publication of this source release.

## Foundation limits, not release blockers

- Zero-touch install/enrollment/recovery and Privilege Broker execution are not complete.
- Windows/Linux bootstrap discovery is contract-tested but not fully real-host validated.
- Generic transport interfaces exist; only the sanitized V0 Tailscale topology has a real-host gate.
- Generic Worker permission fields and capability-grant records do not yet form a privileged policy
  executor.
- Project_Bridge is an optional disabled-by-default foundation; no licensed production codec exists.
- The source archive is byte-reproducible, while a fully locked multi-platform dependency
  environment remains future work.

These limits are acceptable only because documentation, schemas, and capability status label them
FOUNDATION/PLANNED rather than COMPLETE.
