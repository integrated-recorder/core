# Building signed application releases

Official application releases are built for `linux/amd64` and `linux/arm64` from
`v*` tags by `.github/workflows/release.yml`. The workflow runs the web checks
and Go test suite before building the Runtime Host, Control Plane, Recorder
Engine, and bundled generic HLS source adapter. The Docker image separately
bundles the `storage.local` Storage Provider Protocol v1 executable. Owncast
is first-party software but is not bundled. Its Registry distribution is
pending public release artifacts and independent Registry approval.

Each target has its own signed manifest and artifact set:

```text
release-linux-amd64.json
release-linux-amd64.json.sig
runtime-host-linux-amd64
control-plane-linux-amd64
recorder-engine-linux-amd64
adapter-runtime-linux-amd64
```

The same names use `arm64` for the other target. The signed release protocol
retains the `adapter-runtime` role for compatibility; that artifact now
contains the `integrated-recorder-adapter-hls` generic HLS source executable.
It is not a platform-specific Owncast adapter. `storage.local` is built into
the production Docker image as a separate executable; it is not currently part
of the signed application update bundle. The Host imports bundled plugins
through their respective protocol-specific immutable catalog paths. Generic
adapter host code is part of the Control Plane and Recorder Engine binaries.
`cmd/release-pack` uses fixed protocol/schema compatibility declarations and
hashes the exact
packaged regular files. Compatibility is not inferred from semantic version
ordering. The published release also includes `SHA256SUMS` for all platform
manifests, signatures, and executable artifacts.

The release workflow requires the repository secret
`IR_RELEASE_SIGNING_PRIVATE_KEY_BASE64`, containing a base64-encoded Ed25519
private key, and the non-secret repository variable
`IR_RELEASE_SIGNING_KEY_ID`. It fails closed if either value is missing or the
private key is malformed. Production private signing material must remain in
the secret store; no development or test key is used to publish releases.

The Runtime Host must separately be configured with the matching trusted
Ed25519 public key under the same key ID before it can install a remote
release. The release manifest and signature establish authenticity only when
verified against that externally provisioned trust root. In a deployed Host,
`GET /api/runtime/update` reports status and authenticated, CSRF-protected
`POST` requests to `/check`, `/stage`, `/activate`, and `/rollback` operate the
application release lifecycle. Development builds and deployments without a
trust root fail closed. This updates the application Control/Engine generation;
it does not replace the Runtime Host or container image.

Build identities are injected into Runtime Host, Control Plane, and Recorder
Engine through `internal/buildinfo` linker variables. Local development builds
retain their explicit `dev` identity and cannot install remote updates. The
release's `adapter-runtime` compatibility artifact is the bundled HLS
executable, and `storage.local` is the bundled local storage executable in the
Runtime Host image. Owncast is first-party software distributed through the
Registry, not a bundled plugin. Generic adapter discovery is Host-owned:
the exact Host-declared HLS executable is imported from `/adapters`;
`/external-adapters` is considered only when operator plugins are explicitly
enabled. The Host validates and snapshots each admitted executable into
immutable content-addressed artifacts and adapter sets. It does not scan
`/adapters` for arbitrary files. Bundled `storage.local` is imported into the same immutable
storage-provider artifact/set lifecycle as Registry providers, but it does not
require the Registry and cannot be uninstalled. The application generation is
the tuple `(application release, adapter set, storage provider set)`, so
changing either plugin set activates a new generation without restarting the
Host or container. Application release updates carry the active storage set;
the bundled provider executable changes only with the Runtime Host/container
image maintenance class. On Host startup, a changed bundled provider artifact
is imported and selected through the same immutable lifecycle, while existing
Recording generations retain their pinned set. Existing Recordings remain
pinned to their Engine, adapter artifacts, and storage-provider artifact until
their leases drain. Existing
`/data/recordings` archives are adopted in place as the local provider root;
the Host-owned `/data/runtime` tree is not exposed to the provider. Application
release activation carries or deliberately updates the provider-set identity;
rollback must retain the exact older set while it remains a target.
This local restartless lifecycle is implemented. Plugin Registry v1/v2/v3
clients support operator-configured curated HTTPS catalogs and manual artifact
install/update through the same immutable import path; v3 adds publisher
affiliation while preserving v1/v2 parsing. The registry pins
artifact size and SHA-256; publisher PKI/signatures, a community registry
service, and automatic remote adapter updates remain follow-on work. See
[`PLUGIN_REGISTRY_V1.md`](PLUGIN_REGISTRY_V1.md) and
[`PLUGIN_TRUST_MODEL.md`](PLUGIN_TRUST_MODEL.md) for the Registry schemas,
trust provenance, and runtime contract.
