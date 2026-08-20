# License decision record

## V0.1 decision

CHIPS Agent Fabric V0.1 uses the Apache License 2.0. The standard license text is in `LICENSE`, the
package metadata uses SPDX identifier `Apache-2.0`, and project plus dependency attribution is in
`NOTICE` and `THIRD_PARTY_NOTICES.md`.

The choice supports a portable, interoperable agent-fabric project and provides explicit copyright
and patent terms. This is an engineering release record, not legal advice. The person or entity that
publishes the repository remains responsible for confirming ownership of every contribution,
trademarks, notices, and authority to license the work.

## Provenance boundary

No Project_Bridge source is included because that neighboring research prototype has no license.
Other cross-project patterns are referenced or independently adapted; substantial third-party code
must retain its original notice and exact source revision if it is introduced later.

## Publication boundary

The license gate can pass before publication, but publication is a separate human action. The
current run may build and audit a release candidate; it must not push, mirror, tag a public remote,
or create hosting accounts. Private V0 evidence and Git history containing local topology are not
part of the public source payload. A future publisher must start the public history from the audited
clean archive or perform an equivalent history scrub and verification.
