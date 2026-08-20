# Contributing

Thank you for helping make CHIPS Agent Fabric portable, auditable, and safe.

Start with `AGENTS.md`, then read `FABRIC_INTENT.md`, `SECURITY.md`, and the
protocol document relevant to your change. The naming boundary is strict:
Supervisor is the headless orchestration kernel; Cyber Office is every
human-facing surface; Project_Bridge is an optional communication plane.

## Development flow

1. Create a focused branch and keep unrelated local state out of the change.
2. Preserve deterministic task state, event journaling, and explicit unknown
   telemetry values.
3. Add or update tests for behavior and failure paths.
4. Run `python -m pytest -q` and `ruff check .` in the project environment.
5. Run the clean-machine and public-archive checks described in
   `docs/BOOTSTRAP.md` and `RELEASE_CHECKLIST.md`.
6. Explain security, compatibility, migration, and recovery impact in review.

Never commit credentials, private keys, bearer values, certificates, personal
logs, machine-specific private topology, generated state databases, or private
acceptance evidence. Test fixtures must use unmistakable placeholders.

## Local-model boundary

Under the current policy, local inference may classify, reason, and review. It
must not author production code, create patches, execute shell mutation, or edit
repository files. Changes to this policy require an explicit reviewed decision.

## Compatibility

Protocol changes must be versioned, reject ambiguous input, retain deterministic
fallback behavior, and update schemas plus machine-facing documentation. Avoid
provider, model, operating-system, hostname, or home-directory assumptions.

Contributions are accepted under the Apache License 2.0 unless explicitly
marked otherwise in a separate agreement.
