# Trust model

## Assets

Canonical state, node private keys, provider/Worker credentials, capability grants, task artifacts,
source repositories, model privacy, audit events, and release identity require protection.

## Boundaries

- Operator/OS trust root ↔ installer or enterprise provisioning.
- Supervisor ↔ Node Runtime over authenticated encrypted transport.
- Node Runtime ↔ narrowly scoped Privilege Broker.
- Supervisor ↔ Worker adapter ↔ provider/model runtime.
- Supervisor read projections ↔ Cyber Office/authorized agents.
- Optional Project_Bridge payloads ↔ canonical structured state.

Identity proves who a peer is; transport proves protected delivery; a capability grant proves what a
peer may request. None substitutes for the others. Use defense in depth and OS secure storage.

## Initial trust

Legitimate roots include an administrator-started installer, OS service installation, enterprise
provisioning, an existing trusted node enrollment, or securely supplied short-lived enrollment
material. Bootstrap must explain the root used and persist only public identity/reference metadata.

## Threat posture

Fail closed for identity mismatch, expired/replayed grants, unknown protocol versions, public or
ordinary-LAN exposure, missing TLS/private transport, credential-like output, rollback ambiguity,
and unsupported privileged operations. Never bypass firewall, Defender, Gatekeeper, Keychain, or OS
consent. Never expose raw MCP/model/Worker/admin ports or arbitrary remote shell.

Tailscale is the proven V0 transport, not a permanent dependency. Transport adapters must normalize
authentication, encryption, peer identity, reachability, latency, and health.
