# Troubleshooting

| Symptom | Safe diagnosis | Next action |
|---|---|---|
| Bootstrap reports unsupported OS | Inspect `host.operatingSystem`; preserve profile | Add/test a platform adapter; do not force a different OS value |
| Utility is `null` | PATH discovery found nothing | Treat capability unavailable; do not auto-install |
| Existing database/identity found | Review candidate data directory and ownership | Preserve it; require explicit migration/enrollment decision |
| Non-empty emit directory rejected | Existing artifacts would be overwritten | Choose a new directory or archive it deliberately |
| Worker installed but unavailable | PATH is not authentication or protocol proof | Run the provider-specific safe status/gate in the correct user context |
| Private Worker unreachable | Inspect normalized transport health, TLS, identity, and local service | Do not fall back to public/LAN/model-port exposure |
| 401/403 | Credential rejected or wrong trust context | Stop retries; repair through official secure login/storage flow |
| Stream stalls | Compare heartbeat/deadline/process or remote job state | Supervisor cancels on deadline and records timeout evidence |
| Unknown usage/quota/model | Provider did not report trustworthy metadata | Record unavailable; never estimate unless explicitly labelled |
| Recovery is ambiguous | Multiple writers, missing session, identity mismatch, or stale grant | Fail closed and follow `RECOVERY.md` |

Redact before sharing diagnostics. Never enable HTTP tracing with bearer auth, print environment
secrets, dump credential stores, or preserve raw sensitive stdout. Use isolated fixtures for tests.
