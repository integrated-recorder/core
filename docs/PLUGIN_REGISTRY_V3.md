# Plugin Registry v3 and trust provenance

Registry v3 extends the typed Plugin Registry v2 document with a small publisher-affiliation field. It does not replace the artifact approval or executable verification model, and it does not change Adapter Protocol v1 or Storage Provider Protocol v1.

The canonical wire schema is [`schemas/plugin-registry-v3.schema.json`](schemas/plugin-registry-v3.schema.json). The official Registry source repository vendors this schema from the Core contract and records the Core source commit and schema digest in its schema provenance file. v1 and v2 documents remain readable; their wire formats do not make publisher claims, so Core does not infer one for them.

## Publisher field

Each v3 plugin has exactly one publisher kind:

```json
"publisher": { "kind": "first_party" }
```

or:

```json
"publisher": { "kind": "third_party" }
```

The official Registry policy requires `first_party` repositories to belong to the `integrated-recorder` GitHub organization. That is a curated Registry policy, distinct from the Core wire schema. Third-party plugins may use other repositories.

Publisher affiliation is not trust provenance. The Host records how an exact binary was admitted (`bundled`, `registry`, or `operator`) separately from who the Registry says publishes it (`first_party`, `third_party`, or `unknown`). A descriptor cannot set either value.

## Registry authority

Core treats only this exact configured HTTPS endpoint and unchanged final response URL as the official v3 authority:

```text
https://integrated-recorder.github.io/plugin-registry/catalog-v3.json
```

Redirects or custom endpoints do not inherit official authority. A custom Registry's `publisher.kind: first_party` claim is not treated as an official first-party assertion; its effective publisher is `unknown`, with custom/operator-controlled authority.

Official Registry approval means that a pull request passed required CI and exact-artifact validation, and its catalog entry was merged. In current solo-maintainer mode, an independent approving review is not required. This approval identifies distribution metadata; it is not a safety, malware-free, publisher-PKI, or sandbox claim. Plugins are native executables and are not sandboxed.

## Artifact identity and compatibility

A Registry release pins plugin ID/type, version, source commit, protocol name/version, target OS/architecture, executable filename, HTTPS URL, exact byte size, and lowercase SHA-256. The Runtime Host downloads and verifies the exact bytes, then probes the executable and checks its descriptor identity before importing it into the existing immutable adapter or storage-provider lifecycle.

v3 does not permit remote `source.hls` or `storage.local` entries. Those IDs are reserved for Core-bundled reference plugins. A publisher change or changed executable bytes require a Registry PR that passes required CI and immutable artifact verification, plus a new plugin version under the append-only Registry policy; the same version must not silently point to different bytes.

Trust evidence is separate from artifact bytes. An operator-imported binary and a Registry-approved copy with the same digest remain distinct immutable set identities. Existing recordings remain pinned to their original application generation and plugin sets.

## Owncast release status

The standalone Owncast source plugin is first-party software, but it is not bundled with Core. Version `0.2.0` is published by the public `source.owncast` repository and approved in the official Registry v3 catalog. Its Linux amd64 and arm64 artifact URLs, byte sizes, SHA-256 digests, descriptor identity, and source commit are pinned in the catalog and verified by Registry CI. Solo-maintainer mode does not require an independent approving review. Placeholder URLs, sizes, hashes, or release identities are not valid catalog entries.

## Limits

Registry v3 does not add publisher PKI, signed catalog snapshots, rollback/freeze protection, reproducible-build proof, malware scanning, or a TUF-style trust chain. The operational trust root remains access control and Registry-approved changes to the official Registry repository and its publication workflow. Registry compromise or an approved malicious plugin remains outside the guarantees of v3.
