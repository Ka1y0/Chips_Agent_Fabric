# Recovery

SQLite and its append-only event journal are canonical. Provider chat, process stdout, Cyber Office,
Bridge traffic, and in-memory state are not recovery authorities.

## Safe procedure

1. Stop dispatch without deleting state; preserve database/WAL and sanitized logs.
2. Identify the data directory and instance lease. Do not run two writers against one database.
3. Verify filesystem ownership, package version, migrations, configuration, and integrity backup.
4. Reconcile each persisted non-terminal task/run with its opaque process/job/session handle.
5. Resume only when identity, adapter support, grant validity, and remote ownership are unambiguous.
6. Otherwise record interruption/blockage and require deterministic retry or review.
7. Start loopback/private services, replay events after the last committed sequence, and verify
   idempotent observer behavior.

Never delete or rewrite events to make state appear consistent. Never regenerate a node identity
over an existing unknown identity. A restore must preserve Fabric/node IDs and document restored
sequence/time. Secrets are restored through their OS/provider process, not database or source archive.

Portable automatic restart/service recovery remains a **FOUNDATION** until exercised on clean macOS,
Windows, and Linux hosts.
