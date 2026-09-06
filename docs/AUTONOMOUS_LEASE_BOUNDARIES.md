# Autonomous Goal lease boundary hardening

Date: 2026-09-06. Scope: the existing autonomous host and Goal lease repository.
This is not a declaration that every intermittent failure in issue #2 is resolved.

## Findings and changes

A Goal heartbeat previously matched the owner, generation and `owned` state but did not require an
unexpired lease. With no contender yet present, an expired owner could renew its old generation and
become valid again. Renewal now requires `expires_at > now` in the same conditional SQL update.
Equality is expired. A rejected renewal leaves the lease row unchanged; continued work requires a
fresh acquisition and a higher generation, even when the requesting host is the same.

Acquisition previously sampled time before entering the SQLite write transaction. Waiting for the
write lock could consume the new lease's lifetime or cause a contender to evaluate expiry using a
stale time. Acquisition now samples time after transaction entry. Heartbeat expiry is similarly
checked after both position observation and write-transaction entry, not before those waits.

An engine ownership guard can raise `GoalLeaseLost` before the host heartbeat detects it. The host
now carries that loss into `release_goal(lost=True)`, instead of journaling a normal release with
`recovery_state=complete`. Existing generation predicates still prevent a stale owner from releasing
or overwriting its replacement's lease.

## Regression coverage

`tests/test_autonomous_lease_boundaries.py` exercises the production repository against `StateStore`
and its SQLite schema. A controlled clock models exact expiry and transaction-entry delays without
sleep-based timing races. Cases cover renewal at/after expiry, live renewal, same/other-host takeover,
old-generation heartbeat/release rejection, delayed acquisition, delayed observation/renewal, and
loss detected by either the engine guard or completion heartbeat.

The host/engine loss test uses a minimal typed engine fixture, not a live provider. The other
fixtures use real canonical Goal/host/lease storage. Existing host, worker, recovery and subprocess
acceptance assertions remain unchanged. CI runs this focused file before the full unit selection;
it is also included in that selection and must not be counted twice.

## Compatibility and limits

No schema, migration, package version, default timeout, provider adapter, external side effect or
public API route changes. Valid, unexpired leases retain their existing renewal behavior. Expired
renewal is deliberately stricter and fails closed.

The timestamp fix excludes time spent waiting to enter the write transaction, not time spent doing
work or committing after the sample. It does not eliminate synchronous event-loop blocking, clock
jumps, slow engine initialization, or every host/daemon lifecycle race. The separate engine guard
is not a transaction-wide proof for arbitrary user code or external side effects.

Issue #2 remains open until the previously reported CI stalls and turnover have their own causal
reproductions and repeated validation. A successful run is evidence for that run, not a guarantee of
production cluster reliability.
