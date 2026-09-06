# Goal inspection: evidence before recovery

Status: IMPLEMENTED read-only diagnostic command and targeted pytest failure capture.
This is not automatic repair, new execution authority, or proof that issue #2 is resolved.

An AI operator needs to distinguish a persisted control request from lease release and from
external process quiescence. These are three different observations, not synonyms for success.

## Installed command

```sh
chips-goal-inspect --database /explicit/private/state.db --goal EXACT_GOAL_ID
# Equivalent from an installed Python environment:
python -m project_supervisor.goal_inspection --database /explicit/private/state.db --goal EXACT_GOAL_ID
```

The database and Goal must be explicitly selected. The command does not find private directories,
construct StateStore, initialize/migrate a database, start a service, open a network connection,
read provider credentials, send messages, renew a lease, or resume/dispatch work. JSON is always
printed; exit 0 means the snapshot was readable, not that the Goal succeeded. Exit 2 returns a
static unavailable reason without echoing SQL, filesystem paths, or private exception text.

## Contract

Output uses `goal-inspection/v1` and `schemas/goal-inspection-v1.schema.json`.
`authoritative` is always false. It includes:

- current Goal state, termination category, counters, and steer/version metadata;
- lease state/generation and validated timestamps, with a pseudonymous owner reference;
- host state/heartbeat/count and the presence, never the text, of a recorded error;
- action-state counts and a bounded chronological tail of relevant event kinds/sequences;
- the global event cursor for the same SQLite snapshot and explicit truncation;
- independent `leaseObservation` and `executionQuiescence` fields.

`leaseObservation` distinguishes `absent`, `ownedLive`, `ownedExpired`, `released`, `lost`, and
`unknown`. Exact expiry is expired, not live. Unknown or invalid deadlines never imply ownership.
`executionQuiescence` remains `unknown` because this command does not reconcile external jobs.
A paused/stopped Goal with an owned lease reports `CONTROL_AWAITING_LEASE_RELEASE`; this is an
observation to investigate, not permission to replace the owner or retry the work.

One read transaction pins a coherent database snapshot, including committed WAL records. A
concurrent writer cannot cause old Goal state to be combined with a new lease generation. The
command deliberately does not use SQLite's `immutable` URI option on potentially live state.
It uses `mode=ro`, query-only mode, explicit connection closure, a short busy timeout, a bounded
SQLite VM work/time budget, fixed parameterized queries and bounded rows/values. Views cannot
stand in for canonical tables. Unsupported/missing schema is unavailable, never auto-upgraded.
These limits are not a hard real-time deadline against a hung filesystem.

Read-only means no canonical/application state mutation. SQLite may still need or create its
normal WAL/shared-memory sidecars where the filesystem permits them. Do not delete sidecars, copy
only the main file of a live database, or mark live data immutable merely to make inspection work.
See the [SQLite WAL documentation](https://www.sqlite.org/wal.html#read_only_databases).

## Disclosure boundary

No intents, prompts, plan payloads, result bodies, event payloads, exception text, hostnames,
process IDs, credentials or original identity strings are returned. Only allowlisted categorical
values, normalized numbers/timestamps and SHA-256 identity references are projected. Unknown event
kinds remain `unknown`, not arbitrary text. Hashed references are pseudonyms, not anonymization:
small identity spaces can be guessed, and times/counters/event sequences can still be sensitive.
Review any report before sharing it. Nothing is automatically uploaded by the command.

## CI failure capture and reproduction

CI explicitly loads `scripts.goal_diagnostics_pytest`. Only failed call phases in the two named
host/control test files trigger capture, while fixture state still exists. At most three Goals
are inspected. Bounded JSON is attached to the original failure; a diagnostic error cannot replace
it or turn it into a pass. The existing host tests, assertions and timeouts remain unchanged.
This plugin targets synthetic test databases; do not enable it for live private deployments.

```sh
python -m pytest -q tests/test_goal_inspection.py tests/test_goal_diagnostics_pytest.py
python -m pytest -v -p scripts.goal_diagnostics_pytest \
  tests/test_autonomous_host.py tests/test_goal_inspection_integration.py
```

The integration tests use the actual StateStore, GoalService, Host and iteration engine with a
controlled lease clock and deterministic blocking dispatcher. They exercise soft pause, hard pause
and STOP, each with a live lease and at exact expiry. Live control releases its claim; expiry must
surface GoalLeaseLost and must not report ordinary release. Neither case permits automatic new
work on the paused/stopped Goal. Existing real-clock tests remain in CI rather than being replaced
by these controlled-clock cases. No live models, provider accounts or real multi-host deployment
are covered by this acceptance.
