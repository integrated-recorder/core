# Storage Provider lifecycle and Plugin Registry v2

This guide describes the bundled `storage.local` provider and how Runtime Host
installs and activates external Storage Provider Protocol v1 executables. The
local provider is the first production plugin and reference implementation; it
uses the same process, protocol, immutable catalog, and generation path as an
external provider. For wire details and provider-side requirements, see
[Storage Provider Protocol v1](STORAGE_PROVIDER_PROTOCOL_V1.md).

## Responsibility boundary

Integrated Recorder Core owns the canonical archive: Recording identity and
documents, segment and track ordinals, metadata and gap semantics, integrity,
publication order, owner fencing, ingest admission, retries, and logical
object keys. A storage provider only maps a validated logical key to physical
bytes. It does not receive a Core Recording model, contact source adapters,
allocate ordinals, or authorize a canonical write.

There is no special Core local-storage backend in the production archive path.
Every production generation pins an immutable storage-provider set, including
the default local primary. Core and `PhysicalObjectStore` own archive
semantics; `storage.local` and other providers only map logical keys to
physical objects and stream their bytes. The local executable is bundled with
the product and available without Registry configuration. A provider process is
trusted executable code running under the same OS account; this protocol does
not sandbox it.

```text
                    ┌── source adapter executable
Plugin Registry ────┤     Adapter Protocol v1
                    ├── bundled `storage.local`
                    └── Registry storage provider executable
                          Storage Provider Protocol v1
                                   │
                                   ▼
                         PhysicalObjectStore
                                   │
                                   ▼
Core archive model ── logical keys, canonical commits, ownership fencing
```

The Runtime Host imports the bundled `storage.local` executable through the
normal storage catalog: descriptor probe, content-addressed artifact, immutable
provider set, and generation pin. It is represented as provider ID `local`,
distribution `bundled`, and cannot be uninstalled. Registry connectivity is
not needed to install, select, or run it. A provider failure is explicit and
fails the selected generation closed; Core never bypasses Protocol v1 to open
local files directly.

The Host supplies a fixed private archive root at the existing
`<DATA_DIR>/recordings`. The API cannot set an arbitrary filesystem path for
`storage.local`, and the provider is not granted the `/data` root. Existing
recordings stay in place: logical keys in the `recordings/<id>/...` namespace
retain their legacy physical locations beneath that root. Non-recording object
namespaces are stored beneath a provider-private namespace under the same root
so they cannot collide with legacy recording files. This is an adoption of the
old layout, not a byte-copy migration. Core runtime state, including generations,
owners, catalogs, desired state, secrets, IPC files, and recovery coordination,
remains under `/data/runtime` and is never part of the provider archive root.

On Host startup the bundled executable is imported and probed before the
archive is loaded. Legacy generation records without a provider-set identity
are adopted into the Host-selected local provider set without rewriting the
archive. The exact provider process and configuration for the selected set
must be ready before LoadAll/recovery or application readiness proceeds. A
missing or unavailable provider does not trigger direct-local fallback.

## Plugin Registry compatibility and v2

`IR_PLUGIN_REGISTRY_URL` optionally configures the static HTTPS approval
catalog. There is no built-in production registry URL. The registry approves
an exact executable; it does not build or clone source. A GitHub Release or CDN
is transport only. Publisher PKI/signatures are not part of the current trust
model: the configured registry pins the artifact filename, platform, exact
size, and lowercase SHA-256, and Runtime Host accepts only matching bytes.

Registry schema v1 remains source-adapter-only and keeps its existing wire
format. Schema v2 requires each plugin to declare `type` as `source` or
`storage`; every release must name the matching protocol and version. A v2
source release uses `{"name":"source","version":1}` and a v2 storage
release uses `{"name":"storage","version":1}`. Unknown fields, duplicate
plugin IDs/release versions/platforms, unsupported protocols, unsafe URLs,
wrong filenames, and invalid sizes or digests are rejected. Plugin IDs are
unique across both types. Current artifact targets are `linux/amd64`,
`linux/arm64`, `darwin/amd64`, and `darwin/arm64`; no nearest-platform fallback
is attempted.

Example schema-v2 storage entry (the digest, size, repository, and URL below
are illustrative and do not identify a published provider):

```json
{
  "schema_version": 2,
  "plugins": [
    {
      "id": "example-storage",
      "type": "storage",
      "name": "Example Storage",
      "repository": "https://github.com/example/integrated-recorder-storage-example",
      "channels": {"stable": "1.0.0"},
      "releases": [
        {
          "version": "1.0.0",
          "protocol": {"name": "storage", "version": 1},
          "source_commit": "0123456789abcdef0123456789abcdef01234567",
          "artifacts": [
            {
              "os": "linux",
              "arch": "amd64",
              "url": "https://github.com/example/integrated-recorder-storage-example/releases/download/v1.0.0/integrated-recorder-storage-example-storage",
              "filename": "integrated-recorder-storage-example-storage",
              "size": 123456,
              "sha256": "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
            }
          ]
        }
      ]
    }
  ]
}
```

The storage filename is exactly `integrated-recorder-storage-<plugin-id>`.
The ID `local` is reserved for the bundled provider and must not be published
as a Registry plugin.
Runtime Host downloads only catalog-selected HTTPS URLs using bounded,
cancelable requests and restricted redirects, verifies exact size and SHA-256,
then probes the executable. The probe checks descriptor ID, version, Protocol
version, schema, and required capabilities before the bytes are published to
the Host-owned immutable storage catalog. An adapter executable registered as
storage (or the reverse) fails the type-specific descriptor check.

## Install, configure, probe, activate

### Bundled local provider

Registry providers follow separate install, configure, probe, and activate
operations. The bundled local provider is already installed by the Host and has
a Host-managed root rather than user-editable filesystem configuration:

1. **Bundled import** — Runtime Host locates the packaged `storage.local`
   executable, probes its descriptor, imports its exact bytes into the immutable
   catalog, and creates/selects its configured set. This does not require a
   Registry and does not move existing archive files.
2. **Default selection** — a fresh installation selects the imported local set
   as its primary. Existing legacy generations with an empty storage-set field
   are adopted to the same selected set as a one-time runtime-state migration.
   The local provider is mandatory and cannot be uninstalled or assigned an
   arbitrary root through a public API.

### Registry providers

Registry providers then use these separate operations:

1. **Install/update** (`POST /api/runtime/plugins/{id}/install` or
   `/update`) verifies and imports the executable. Storage artifacts enter
   `/data/runtime/storage-providers/artifacts/<sha256>`; installation alone
   does not select a backend or write recordings there.
2. **Configure** (`PUT /api/runtime/storage/providers/{id}/config`) validates
   values against the provider schema and stores a private immutable
   configuration set. Secret values are write-only in the API and are not
   returned by configuration reads. The set files have private permissions,
   but secrets are not encrypted at rest.
3. **Probe** (`POST /api/runtime/storage/providers/{id}/probe`) starts that
   exact executable and configuration. The provider validates its own
   configuration, then Core exercises a random object in the reserved
   `_integrated-recorder/system-probes/v1/` namespace: put, stat, list, read
   and hash comparison, range read, delete, and missing-object confirmation.
   Probe cleanup is bounded and does not use the Recording namespace.
4. **Activate** (`POST /api/runtime/storage/providers/{id}/activate`) stages
   and readies a new application generation with the selected provider set.
   The authenticated `/storage` page exposes the same configure, probe, and
   primary-selection steps. `/adapters` is where registry executables are
   installed and updated.

Runtime Host serializes application updates, source-plugin reconciliation,
storage-plugin installation/update, and backend activation through its common
operation gate. The local bundled executable is reported as a plugin with
`distribution: bundled`; remote plugins use `distribution: registry`. Public
status contains bounded IDs, versions, and health
states; it does not expose artifact paths/digests, registry URLs, provider
endpoints, IPC credentials, or secret values. Mutations use the normal Runtime
Host authentication and CSRF checks.

## Object I/O and failure behavior

The Core-to-provider data plane streams raw object bytes over the private
authenticated Unix socket. It does not base64-encode payloads or forward
filesystem paths. Requests have exact content lengths and bounded object
sizes; cancellation and backpressure propagate across the stream. Core stages
and hashes an incoming object on private local disk before streaming it to the
provider, avoiding an additional whole-object RAM copy. Providers may retry
only bounded transport failures; Core retains authority over archive retry
and backoff.

A successful `Put` publishes one complete object. A failed or interrupted
publication may leave the previous complete object or the new complete
object, but never a partial object at a canonical key. A lost response is
ambiguous: Core verifies the stored object before deciding whether to accept
the commit or retry. The provider does not create alternate keys or decide
canonical commit ordering. The Core-owned owner fence is checked before
canonical publication, so an Engine that has lost its Recording epoch cannot
write via the provider.

If a provider child exits, its owning generation may restart only the exact
generation-pinned immutable executable with the same private configuration
and a fresh private IPC identity. Runtime does not replay the failed object
operation. Core owns retries and resolves an ambiguous `Put` only after
independent size and SHA-256 verification of the complete object. A provider
failure never falls back to direct local filesystem access. The local provider
itself is a Protocol v1 child process, not a Core fast path.

Protocol v1 supports bounded list pagination, stat, full and byte-range reads,
idempotent delete, and complete-object atomic replacement. The Control
Plane's VOD segment endpoint supports one HTTP byte range and streams only
that range through the backend-neutral range reader. Integrity checks read
provider objects through Core and compare stored size and SHA-256 with
canonical metadata. If a configured provider is unavailable, the selected
generation fails closed; Core never silently switches to an empty local
archive.

## Primary backend and generations

An application generation is identified by
`(application release, adapter set, storage provider set)`. A production
generation always has a non-empty provider-set identity; the local default is
an ordinary immutable set that references the imported `local` artifact. An
empty set in an older generation record is a legacy marker adopted by Runtime
Host before that generation is used. Provider artifacts and their
configuration snapshots are immutable and generation-pinned. A storage
provider update can therefore activate a new generation while existing
Recordings remain on their original Engine and provider executable. Old sets
remain protected while active/draining generations, rollback state, or
Recording references need them.

V1 permits one canonical primary backend per installation. Switching to a
different physical backend is rejected while Recording leases exist and
unless both the current archive and target backend are proven empty. There is
no implicit migration between physical backend kinds. The one-time legacy
local adoption preserves the existing `/data/recordings` root and does not move
objects. A new provider set using the same provider ID and configuration can
represent a provider executable update. After configuring
and probing that set, an administrator explicitly activates it as a new
generation; existing generations remain pinned. Active Recording handover is
eligible only when both the adapter-set identity and storage-provider-set
identity match exactly.

Uninstall is rejected while a provider is the active primary. After it has
been deactivated, uninstall removes the provider from the installed/desired
inventory; it does not rewrite or immediately delete immutable objects held
by a generation. Provider artifacts and sets are eligible for collection only
after their generation and Recording references are gone. A registry outage
does not disable an already installed provider or make registry access a
runtime dependency.

## Current scope

This release provides the Storage Provider Protocol, the bundled `storage.local`
reference/production plugin, executable lifecycle, Registry v2 type support,
and a conformance runner. It does not include a production S3, Backblaze B2,
WebDAV, or SFTP provider. It also does not provide
publisher PKI or plugin signatures, sandboxing, automatic plugin updates,
archive migration, multi-pool placement, tiering, replication/mirroring,
provider dependency resolution, or cross-storage-set Recording handover.
