# Universal bootstrap FOUNDATION

`chips.py` is a portable, standard-library bootstrap entrypoint:

```sh
python3 bootstrap/chips.py bootstrap --json
python3 bootstrap/chips.py bootstrap --emit --output-dir PATH --redact-host --json
```

After installing the package, use `chips bootstrap`. Raw discovery contains local host metadata and
must stay machine-local; `--redact-host` replaces host identity, local paths, and interface names for
review artifacts. Windows uses `.\.venv\Scripts\chips.exe`; see `docs/BOOTSTRAP.md`.

The first command is strictly read-only. The second writes a review bundle only into a new or empty
explicit directory. Both remain dry-runs: they never install packages, authenticate, contact remote
services, inspect listeners/credentials, start services, elevate privileges, create identity keys,
configure firewalls/transports, or register Workers.

Discovery covers macOS, Windows, and a Linux foundation: OS, architecture, hostname, CPU, RAM,
GPU/VRAM where an installed read-only system utility reports it, common runtimes, model server
executables, AI CLIs, private-transport executables, Supervisor/node runtime presence, and existing
state/identity file presence. An executable is only a candidate; it is not proof of authentication,
model availability, reachability, authority, or compatibility.

Generated files:

- `machine-profile.json` — machine-specific discovery; do not commit it.
- `bootstrap-plan.json` — deterministic steps and explicit approval boundaries.
- `supervisor-config.example.json` — restrictive loopback/read-only candidate configuration.
- `checksums.json` — SHA-256 integrity for the other generated files.

This is a **FOUNDATION**, not a completed installer or enrollment service. The planned privileged
execution boundary is described in `docs/CAPABILITY_MODEL.md` and `docs/TRUST_MODEL.md`.
