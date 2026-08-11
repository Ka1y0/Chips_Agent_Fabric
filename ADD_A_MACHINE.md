# Add a machine

V0 enrollment is intentionally operator-controlled. The bootstrap skeleton performs discovery only;
it does not install tools, authenticate accounts, modify firewalls, or expose services.

## 1. Inspect the candidate

Copy or clone this repository on the candidate and run:

```sh
python3 bootstrap/check_environment.py --json
```

Review the OS, architecture, Python availability, and discovered worker executables. Discovery does
not prove authentication or authorization.

## 2. Establish private transport

V0 uses the official Tailscale client as the preferred authenticated private overlay. Keep worker
daemons and local model servers bound to loopback. Do not expose raw shell, raw MCP, LM Studio, or a
worker port to the public Internet or ordinary LAN.

For a Windows Local Worker, use this topology:

```text
Project_Supervisor -> Tailscale HTTPS Serve -> 127.0.0.1:7331 Worker
```

Install the official Tailscale clients, authenticate both machines into the same tailnet, and leave
the Worker bound to `127.0.0.1:7331`.

Serve's HTTPS listener needs a `*.ts.net` certificate, so the tailnet must have **HTTPS Certificates**
enabled first (admin console → DNS). Confirm with `tailscale status --json`: an empty `CertDomains`
means the feature is off and `tailscale serve` cannot terminate HTTPS.

`tailscale serve` configures only the node it runs on — there is no remote-node flag — so run this on
the Windows node itself, after inspecting the installed CLI help:

```powershell
tailscale serve --bg http://127.0.0.1:7331
tailscale serve status
```

The resulting `https://<node>.<tailnet>.ts.net` URL is private to the tailnet. Never run
`tailscale funnel`, advertise an exit node or subnet route, or change the Worker/LM Studio listener
to `0.0.0.0`. Keep Tailscale identity and the Worker's own bearer authentication enabled together.

Any non-loopback Supervisor listener requires:

- `private_transport: true`;
- a TLS certificate and matching private key;
- an operator-reviewed firewall/overlay route; and
- a scoped client token stored in an OS credential manager.

The Supervisor never stores the Worker bearer in runtime JSON, `.env`, manifests, logs, or evidence.
Place it in the Mac login Keychain (or another OS credential manager) and inject it only into the
adapter process. A non-loopback `LocalWorkerAdapter` fails closed unless its URL is HTTPS and a
non-empty bearer is supplied.

## 3. Authenticate workers manually

Use each provider's official interactive login in the normal user session. Do not copy OAuth files,
cookies, API keys, or Keychain entries between machines. Project_Supervisor may later validate status
without reading secret values.

## 4. Register capability metadata

Start from `manifests/node.example.json` and `manifests/worker.example.json`. These are reviewable
templates, not auto-applied enrollment requests. Report only capabilities actually verified on the
machine. Local-model workers must retain `codeWriteAllowed: false`.

## 5. Test and enroll

Run isolated, minimal worker probes before enabling dispatch. Enrollment should record the node,
workers, harness versions, model observations, and sanitized evidence. Never infer readiness from an
installed executable alone.

Before enrollment, verify from the control node: tailnet reachability, authenticated and
unauthenticated health behavior, FAST and GENERAL jobs, structured output, polling, cancellation,
and timeout. Also verify `tailscale serve status` shows Serve only (never Funnel), the Worker still
listens only on loopback, and LM Studio/raw MCP have no remote listener.
