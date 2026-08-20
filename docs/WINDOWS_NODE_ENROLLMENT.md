# Windows Fabric Node artifact and enrollment

## Maturity

The Mac-side Phase 2B artifact builder, typed request/bundle/receipt contracts, pending enrollment
state, HMAC receipt admission, transport pinning, and Windows Credential Manager primitive are
**IMPLEMENTED** and deterministic-test covered. Real Windows installation, SCM configuration,
Tailscale health, restart acceptance, and receipt production remain environment-specific gates.
No Node is admitted merely because an artifact was copied or a peer was reachable.

The deployment artifact is a private dogfood application payload, not a GitHub release. It contains
one deterministic pure-Python wheel, fixed service profile, verification tool, enrollment schemas,
acceptance contract, and rollback contract. It does not contain a Python interpreter or Windows
dependency wheels; PC preflight must prove a dedicated Python 3.12 runtime and the manifest-listed
dependencies before service installation. Editable installs, a Git checkout, and random
`PYTHONPATH` are prohibited for the long-lived runtime.

This artifact is not yet a production-ready Windows service installer. The broker entrypoint is a
console/ASGI process and no bundled `ServiceMain` host or equivalent reviewed Windows service
wrapper currently proves that SCM can keep it alive under the fixed virtual service account. The
SCM profile and acceptance contract are therefore pinned design inputs, not evidence that the
service was installed. Until a real Windows acceptance proves the service host, the PC helper may
create the machine-bound request and verify the artifact, but it must not claim installation or
emit an enrollment receipt.

## ISSUE → ENROLL → ADMIT

`ISSUE` starts with `mac-phase2-artifact-request.json`. The request is bounded to Windows x64 and
contains the exact hostname, Windows build, Tailscale peer identity, HTTPS Serve origin, and a
SHA-256 machine binding. It contains neither raw MachineGuid nor a credential. The Mac verifies the
request and artifact, binds its canonical Node public identity, creates one short-lived pending
enrollment, and emits a non-secret bundle plus a separate mode-0600 secret sidecar. Reissuing the
same live request with different credential material is rejected.

`ENROLL` occurs on Windows. The standalone verifier checks the archive digest, exact manifest file
set, member hashes, safe paths, target, expiry, host request, and service profile before any
mutation. The bootstrap credential is written to Windows Credential Manager without appearing in
argv, a service definition, a log, the bundle, or the receipt. Tailscale provides private transport
reachability; the Fabric bearer remains a separate application-authentication layer. The broker
binds only `127.0.0.1:7331`; Funnel and a public listener are prohibited.

`ADMIT` requires a typed HMAC receipt and the still-current pending enrollment. The Mac verifies the
enrollment ID, expiry, hostname, peer fingerprint, machine binding, target, artifact and manifest,
Mac-issued service profile, broker runtime profile, authenticated local/tailnet health, SCM
acceptance, restart acceptance, and replay identity. Only then are the Node and Tailscale binding
created atomically. A changed replay is rejected.

The current bootstrap auth model uses one strong bearer both for broker application authentication
and HMAC receipt possession proof. It does not pretend to implement a separate one-time
cryptographic enrollment credential. The sidecar is single-purpose and should be deleted after
Windows Credential Manager provisioning; the Mac retains only a salted verifier in SQLite and the
operator-controlled secure-store copy needed for later broker calls. A future protocol revision
should separate enrollment proof from long-lived broker authentication and add mutual public-key
proof of possession.

Windows Credential Manager is scoped to the identity that writes the credential. A credential
written by interactive PC Codex is not proof that `NT SERVICE\ChipsFabricNodeBroker` can read it.
The current `provision-credential` primitive is safe with respect to argv/files/log projection, but
its service-account visibility is not yet proven or automatically provisioned. Enrollment must
remain pending until either the credential is provisioned while executing as the service identity,
or a reviewed machine-DPAPI secret plus service-SID ACL implementation replaces this contract.

## Fixed Windows service profile

`windows-fabric-node-service-profile/v1` fixes the installation/state/log roots, broker entrypoint,
service name, virtual service account, ordered `Tcpip` dependency, delayed-auto start and recovery
policy, Credential Manager target template, loopback endpoint, and exact Tailscale no-op policy.
Only the listed identity bindings may be substituted from an admitted bundle. A request cannot
supply executable, argv, cwd, environment, service name, account, listener, firewall, PowerShell,
or arbitrary command material.

The Mac-issued Windows service profile digest and the host-bound broker
`RuntimeBrokerProfile.digest` are distinct identities. Both are carried and pinned by admission;
neither may stand in for the other. The Windows SCM configuration digest must include the complete
ordered `MULTI_SZ` dependency list.

The old `D:\AI\Projects\Project_Supervisor_Worker` is detect-only legacy state. The profile forbids
starting, deleting, overwriting, or migrating it. CrossFire and other project data are outside the
artifact and enrollment authority.

## Operator commands

Build from the current exact dirty Mac tree into a new output root:

```text
PYTHONDONTWRITEBYTECODE=1 .venv/bin/python scripts/build_windows_node_artifact.py \
  --output-dir artifacts/windows-node-enrollment
```

The output `PC_ENROLLMENT_HANDOFF.md` names every file and digest. If the exact PC request is not
present, stop at `WAITING_FOR_PC_ENROLLMENT_REQUEST`; do not guess the machine binding. After the
request arrives, `scripts/manage_windows_node_enrollment.py issue` creates the pending state,
bundle, and separate secret sidecar. Its `admit` command consumes the real PC receipt. None of these
commands installs software remotely or changes Tailscale.

An admitted autonomous host may opt into the pinned broker with
`--execution-plane-enrollment`, one `--execution-plane-worker`, and an explicit platform-approval
flag. Pending enrollment cannot be wired. The broker bearer is read from the distinct
`project-supervisor-runtime-broker` Keychain service or a short-lived process environment value and
is never stored in Supervisor JSON/SQLite.
