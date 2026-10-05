# Plugin trust and publisher model

Integrated Recorder records two independent facts about each plugin:

1. **Admission provenance** says how the Runtime Host admitted the exact executable bytes.
2. **Publisher affiliation** says who maintains the plugin according to the admitting source.

Publisher affiliation never determines admission provenance. In particular, first-party software can be Registry-distributed, and a custom Registry does not receive official Registry authority.

## Admission provenance

The Runtime Host uses exactly three selectable provenance classes:

| Provenance | Admission evidence | Public authority | Default publisher | Public label |
| --- | --- | --- | --- | --- |
| `bundled` | Host-owned inventory names the executable shipped with the Core release; the Host probes and imports it. | `core_release` | `first_party` | Bundled with the Core release |
| `registry` | A Registry release selects the executable and the Host verifies its HTTPS origin policy, exact size, SHA-256, plugin identity, version, and protocol before import. | `official` or `custom` | Registry metadata for the official Registry; `unknown` for a custom Registry | Official Registry / Registry approved only for the canonical official Registry |
| `operator` | The administrator supplies a local executable; the Host still probes, hashes, snapshots, and pins it. | `local` | `unknown` | Local / operator trusted; not reviewed by Integrated Recorder |

The official Registry authority is recognized only for the canonical HTTPS catalog endpoint under `https://integrated-recorder.github.io/plugin-registry/`. A custom Registry remains `custom` even if its document claims `first_party`; the Host reports its publisher as `unknown`. Redirects to an unrelated origin cannot establish official authority. Registry v1/v2 remain readable and do not gain a publisher claim that their wire format never carried.

The API's legacy `reviewed` boolean is retained for wire compatibility. `true` means that the Host admitted the executable through the authoritative distribution path (a Core release for bundled plugins or the canonical official Registry for Registry plugins); it does not indicate an independent human review. Solo-maintainer mode does not require a separate approving review. When a second active trusted maintainer joins, the Registry intends to restore at least one required approval. This value does not claim that executable code is safe, malware-free, or sandboxed. Plugins are native executables and run with the Core service's operating-system privileges.

## Publisher affiliation

The publisher values are `first_party`, `third_party`, and `unknown`. The official Registry controls publisher metadata as catalog data subject to the Registry's required CI and artifact verification. Registry policy requires a `first_party` repository to belong to the `integrated-recorder` organization. Operator-supplied binaries always have publisher `unknown`; plugin descriptors cannot set publisher affiliation.

Examples:

```text
source.hls       bundled / first_party
storage.local    bundled / first_party
source.owncast   registry / official / first_party
source.soop      registry / official / first_party
community.foo    registry / official / third_party
custom.bar       operator / local / unknown
```

These rows describe the trust classification when those plugin identities are admitted through the named path. `source.owncast` is first-party software, but its distribution trust is Registry-based rather than bundled.

## Immutable identity and migration

Executable bytes and their trust evidence are separate. The artifact identity is the SHA-256 digest of the executable. An immutable adapter or storage set records both the artifact reference and the Host-assigned attestation. The attestation is included in set identity, so identical bytes admitted through operator and Registry paths produce distinct sets and application generations. A matching digest never silently promotes existing operator state to Registry provenance.

Generation and recording references pin the selected set and its provenance. A recording is not moved to another set merely because the same bytes later receive stronger distribution evidence. Existing immutable manifests without provenance remain readable as `legacy_unclassified`; this is an internal migration state, not a user-selectable provenance. It is shown conservatively as unavailable provenance and is never rewritten in place to claim Registry or bundled admission.

Bundled identities `source.hls` and `storage.local` are reserved. Registry policy rejects both IDs, and Host classification prevents a local executable from replacing the Host-declared bundled implementation. Directory ordering does not choose trust or identity precedence.

## Registry publisher schema

Registry v3 adds publisher metadata while preserving v1/v2 compatibility. The publisher declaration is intentionally small:

```json
"publisher": { "kind": "first_party" }
```

or:

```json
"publisher": { "kind": "third_party" }
```

The registry document remains the approval authority for the exact artifact URL, platform, byte size, SHA-256, descriptor identity, and protocol. Artifact hosting transports bytes; it does not approve them. Registry approval means admission through the official catalog's PR and required CI/artifact-validation process; it does not imply an independent human approval. It also does not provide publisher PKI, reproducible builds, a sandbox, rollback protection, or protection against compromise of the Registry repository or its GitHub organization. Signed catalog snapshots or a TUF-style trust system require a future protocol and are not implied by v3.

## Native code and secrets

All plugin classes are trusted local executable code for the purpose of process launch. None is sandboxed. A plugin may receive only the configuration and secrets required for its declared capability, but can otherwise act with the service user's operating-system privileges. Do not put secrets in descriptor fields, logs, structured errors, or stdout protocol diagnostics. Local plugins are protocol-validated but have not been approved through the official Registry process.
