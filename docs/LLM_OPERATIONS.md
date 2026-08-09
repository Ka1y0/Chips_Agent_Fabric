# LLM operations runbook

An unfamiliar capable coding agent operates Fabric through machine contracts, not Cyber Office UI.

1. Read `AGENTS.md` and `FABRIC_INTENT.md`; inspect `CAPABILITIES.md` before trusting roadmap text.
2. Run the bootstrap dry-run. Do not infer auth, reachability, compatibility, or authority from PATH.
3. If initialized, use `project-supervisor --json status`, `tasks`, and `logs --after SEQUENCE` with
   an operator-specified data directory. Do not guess private paths.
4. Propose the smallest capability and explain scope, target, trust root, risk, rollback, and evidence.
5. Dispatch only after scheduler/policy acceptance. Monitor normalized events and terminal state.
6. Classify evidence as exact, observed/provider-reported, inferred, or unavailable.
7. On failure, preserve sanitized facts, recover deterministically, and ask only for an external
   trust/account decision that cannot be resolved locally.

Never expose secrets, auto-login, install silently, weaken OS/network controls, publish without the
release gate, or allow local AI to modify production code. Model output cannot grant capability,
approve RED work, mutate canonical state, or certify its own acceptance.

Before claiming completion run focused and full tests, lint, fresh-install/state initialization,
schema validation, secret/privacy scans, and the relevant real integration gate. FOUNDATION is not
COMPLETE.
