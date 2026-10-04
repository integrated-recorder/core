# Plugin Registry

Integrated Recorder's Plugin Registry is an approval and distribution index. It is **not a build service**: publishers build adapter executables in their own CI and host the resulting artifacts on GitHub Releases, a CDN, or another HTTPS host. The registry document approves an exact release artifact by recording its platform, filename, byte size, and lowercase SHA-256 digest.

For v1 and v2, the configured registry repository is the approval authority. Artifact hosting is transport only. Publisher PKI and detached plugin signatures are not part of these versions. The Runtime Host obtains the catalog from its operator-configured fixed HTTPS URL, then accepts an artifact only when its downloaded bytes match both the registry size and SHA-256. A change to hosted bytes therefore fails verification. A successful hash check is followed by the type-specific Protocol v1 executable probe and exact descriptor identity check; only then can a source executable enter the immutable adapter catalog or a storage executable enter the immutable storage-provider catalog.

## Configuration and platform selection

Set `IR_PLUGIN_REGISTRY_URL` on the Runtime Host to the HTTPS URL of a static Registry document. There is no built-in production URL. If the setting is empty, the registry is reported as unavailable/not configured; local adapters and local-primary storage continue to work. Installation selects only the exact `stable` channel entry and the artifact matching the Runtime Host's `GOOS` and `GOARCH`; supported targets are `linux/amd64`, `linux/arm64`, `darwin/amd64`, and `darwin/arm64`. It does not infer compatibility from version ordering or search for a close platform match.

Catalog requests and artifact downloads are bounded, context-cancelable HTTPS requests. URLs with credentials or unsupported schemes are rejected, redirects are bounded and HTTPS-only, catalog and artifact sizes are limited, and the production HTTP transport refuses loopback, private, link-local, and other non-public destinations. Artifact URLs are taken only from the validated registry document; callers cannot submit an arbitrary download URL.

## Registry document

Schema version 1 is source-only and remains supported unchanged for existing catalogs; its schema is [`schemas/plugin-registry-v1.schema.json`](schemas/plugin-registry-v1.schema.json). Schema version 2 adds an explicit plugin `type` (`source` or `storage`) and typed release `protocol` (`{"name":"source","version":1}` or `{"name":"storage","version":1}`); its schema is [`schemas/plugin-registry-v2.schema.json`](schemas/plugin-registry-v2.schema.json). A v1 plugin is interpreted as `source`. Runtime validation is stricter than generic JSON decoding: unknown properties, invalid UTF-8, trailing JSON, duplicate plugin IDs, release versions, or platform artifacts, mismatched plugin/protocol types, unsupported protocol versions, unsafe file names, invalid HTTPS URLs, malformed digests, and out-of-range sizes are rejected. The catalog is limited to 2 MiB, 256 plugins, 128 releases per plugin, and 16 artifacts per release. Channel names are reserved for `stable`, `beta`, and `development`; installation currently honors `stable` only. The storage plugin ID `local` is reserved for the bundled `storage.local` provider and must not be published as a Registry plugin.

An empty, valid catalog is included at [`../protocol/plugin-registry-v1/empty-catalog.json`](../protocol/plugin-registry-v1/empty-catalog.json). A populated registry entry has this shape:

```json
{
  "schema_version": 1,
  "plugins": [
    {
      "id": "adapter-id",
      "name": "Adapter display name",
      "repository": "https://github.com/publisher/adapter-repository",
      "channels": {"stable": "1.2.3"},
      "releases": [
        {
          "version": "1.2.3",
          "protocol_version": 1,
          "source_commit": "0123456789abcdef0123456789abcdef01234567",
          "artifacts": [
            {
              "os": "linux",
              "arch": "amd64",
              "url": "https://github.com/publisher/adapter-repository/releases/download/v1.2.3/integrated-recorder-adapter-adapter-id",
              "filename": "integrated-recorder-adapter-adapter-id",
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

The values above illustrate field shape only and do not identify a published plugin. A production registry must use the exact artifact bytes and digest produced by the publisher's release workflow. Registry maintainers should publish a new version for changed bytes and must not silently repoint the same version to another executable.

Schema version 2 can list both source and storage releases. For example, the following storage entry approves an externally built Storage Provider Protocol v1 executable; it does not ask the Registry or Core to build it:

```json
{
  "schema_version": 2,
  "plugins": [
    {
      "id": "example-storage",
      "type": "storage",
      "name": "Example Storage",
      "repository": "https://github.com/publisher/example-storage",
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
              "url": "https://github.com/publisher/example-storage/releases/download/v1.0.0/integrated-recorder-storage-example-storage",
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

This example is illustrative only. Registry entries must contain the actual release size and digest. v2 requires storage filenames to be `integrated-recorder-storage-<id>` and source filenames to be `integrated-recorder-adapter-<id>`; a storage entry cannot install an Adapter Protocol executable or vice versa.

## Install lifecycle

`GET /api/runtime/plugins` returns a bounded, typed registry/install projection. `POST /api/runtime/plugins/refresh` fetches and validates the configured catalog. `POST /api/runtime/plugins/{id}/install` and `/update` use the selected stable release, verify it, and probe its type-specific descriptor. A source plugin commits a durable desired source selection and invokes the normal adapter-set reconciliation and application-generation readiness/activation path. A storage plugin install only stores a verified immutable executable; configuration, object probe, and primary-backend activation are separate operations on `/api/runtime/storage`. `DELETE /api/runtime/plugins/{id}` removes the desired source selection or uninstalls an inactive storage provider; active storage providers must first be deactivated through the normal safe backend-selection flow. Mutations use the Runtime Host's normal authentication and CSRF rules.

The remote registry, installed desired artifacts, configured storage-provider sets, immutable artifact catalogs, and active generation are separate state. Source snapshots are stored below `/data/runtime/plugin-registry`; storage provider artifacts and configuration sets are stored below `/data/runtime/storage-providers`. Partially downloaded files are never candidates. Registry outage does not disable already-installed source or storage plugins. Removing a plugin does not delete immutable artifacts held by active, draining, rollback, or recording-pinned generations. Storage-provider updates use new immutable sets and application generations; existing recordings retain their original provider set. Changing the physical backend kind is allowed only when both archive namespaces are empty and no recording leases are active. Core never migrates existing archives automatically.

An adapter install or update changes the immutable adapter set and therefore activates a new application generation. New recordings use that generation. Existing recordings stay pinned to their original Engine and immutable adapter artifact; registry updates never authorize cross-adapter-set live handover. Application update and plugin mutations share the Host operation gate, so an application release and adapter set cannot be mixed across a generation.

## Out of scope

Registry v1/v2 does not clone source, build binaries, accept community uploads, resolve plugin dependencies, rank plugins, implement publisher signatures/PKI, schedule automatic updates, sandbox plugins, or perform cross-adapter-set/storage-set handover. The configured curated registry repository is the approval authority. Storage Provider Protocol v1 is physical object I/O only; Core continues to own the canonical archive format and write authority. There is one canonical primary backend per installation; placement, migration, mirroring, replication, tiering, and restore remain out of scope.
