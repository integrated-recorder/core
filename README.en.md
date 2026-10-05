# Integrated Recorder

[한국어](README.md) | **English**

A headless, segment-native live-stream archival server written in Go.

Integrated Recorder follows the source manifest, stores original media segments without transcoding or remuxing, and reconstructs them as browser-playable VOD. The recording path does **not** depend on FFmpeg or Streamlink.

> [!NOTE]
> Integrated Recorder is the **spiritual successor** to [Twitch Auto Recorder](https://github.com/dltkddnr04/Twitch-Auto-Recorder) and [AfreecaTV Auto Recorder](https://github.com/dltkddnr04/AfreecaTV-Auto-Recorder).
>
> Those projects focused on automatically detecting live broadcasts and saving them locally, using platform-specific discovery with Streamlink/FFmpeg-based recording. Integrated Recorder keeps the same goal of unattended stream archiving, but is a ground-up redesign around a headless Go service, direct segment preservation, browser control, and long-term archival.

## Principles

- **Preserve the source:** store original media payloads unchanged.
- **Segment-native:** do not turn every recording into one monolithic file during acquisition.
- **Headless first:** designed to run continuously in Docker.
- **Browser controlled:** the API and web interface are the intended control surface.
- **Rebuildable projections:** VOD playlists, preview indexes, exports, and future UI data should be reproducible from the archive.
- **FFmpeg is for derivatives only:** it is not required for acquisition or canonical archival. It is used only for optional projections such as preview frames and remux exports.
- **Register once for automatic recording:** when an adapter declares the `watch` capability, a durable Watch monitors it and creates a separate Recording for each detected broadcast session.

See [Architecture](docs/ARCHITECTURE.md) for the detailed design and storage direction.

Plugin admission provenance and publisher affiliation are independent metadata. Runtime Host assigns `bundled`, `registry`, or `operator` provenance; publisher affiliation is `first_party`, `third_party`, or `unknown`. The bundled plugins are `source.hls` and `storage.local`. Owncast is first-party software, but it is not included in the Core image and will be installable from the official Registry only after its public release and Registry approval. Registry v3 records publisher affiliation and pins the exact artifact URL, size, SHA-256, descriptor identity, and protocol. Custom Registries and locally supplied `/external-adapters` executables are not presented as project-reviewed. Plugins are native executables and are not sandboxed. See the [Plugin Trust Model](docs/PLUGIN_TRUST_MODEL.md).

The Runtime Host uses the Registry configured in `IR_PLUGIN_REGISTRY_URL` for manual refresh/install/update. The official catalog is `https://integrated-recorder.github.io/plugin-registry/catalog-v3.json`; Registry v1/v2 custom catalogs remain compatible. The Registry is not a build service or publisher-signing system. Existing recordings continue to use the Engine, adapter artifact/set, and storage-provider artifact/set pinned when they started. The default file secret store and storage-provider configuration use restricted permissions but do not encrypt values at rest.

Core continues to own archive format and write authority. `storage.local` is the first bundled reference Storage Provider Protocol v1 plugin and the default primary provider, available without a Plugin Registry. Recording writes, playback, integrity checks, deletion, and recovery use the same provider process path for local and remote storage. The provider only maps logical object keys to physical bytes; it does not decide Recording, segment ordinals, metadata, gaps, or ownership semantics. Runtime Host imports the bundled executable into its ordinary immutable artifact/set catalog and pins the set in each application generation. The local provider receives only the Host-controlled existing `/data/recordings` archive root; public APIs cannot grant it an arbitrary filesystem path. Existing archive files are adopted in place, without moving their bytes. Core runtime state remains separate under `/data/runtime`. V1 has one primary backend at a time; changing physical backend kind when an archive exists requires a future explicit migration and is not performed automatically. No production S3/B2/WebDAV provider is included. See [Storage Provider Protocol v1](docs/STORAGE_PROVIDER_PROTOCOL_V1.md), the [storage-provider lifecycle guide](docs/STORAGE_PROVIDER_V1.md), and the [architecture](docs/ARCHITECTURE.md).

## Current status

**Milestone 1, the external adapter protocol, and the restartless immutable adapter lifecycle are complete.**

Management UI v2 connects recording search/pagination, tags/deletion, integrity checks and cancellation, adapter controls, capability-driven resource browsing, workflows, notifications, supported settings and optional recording retention, global search, request-log viewing, the Preview Frame Index, and automatic recording Watches to backend APIs. A Watch is a durable recording intent; each detected broadcast becomes a separate Recording. Automatic recording is shown only for adapters that declare the `watch` capability. Fresh installs use the `/setup` wizard to claim the administrator and complete installation diagnostics before normal operation starts. If FFmpeg is available, segment preview generation and separate remux exports are offered without changing the canonical recording.

The production container separates a stable-listener Runtime Host, replaceable Control Plane, and Recorder Engine that owns segment acquisition and canonical archive writes. Activating an application, adapter, or storage-provider generation leaves existing Engine generations using their pinned artifacts until their recordings finish; new recordings use the new default generation. Replacing the Host/container itself is a separate maintenance operation. See the [architecture](docs/ARCHITECTURE.md) and [release process](docs/RELEASING.md) for generation and signed-release behavior.

Scene previews are an opt-in derivative per recording and default to disabled. A background service produces at most one reusable frame for each committed primary-track segment. It first tries the target segment alone (including the required fMP4 init object); only after decode failure does it stage bounded prior-segment context. Posters, storyboards, and future navigation views reuse the stored frames, and slow or failed FFmpeg work never blocks acquisition.

Currently supported:

- external executable adapters, with generic direct HLS in bundled `source.hls` and platform integrations distributed by Registry;
- schema-rendered input and settings forms with generic resource discovery and configuration challenge/resume;
- hierarchical settings with separate stored and effective projections;
- lazy adapter-process restart with bounded backoff and a fresh describe handshake;
- adapter-declared refresh for expiring media sources;
- a bounded single-rendition HLS subset, including complete segments in LL-HLS playlists;
- direct acquisition of MPEG-TS/fMP4-style source objects;
- SHA-256 and size metadata for captured payloads;
- manifest snapshots;
- duplicate suppression, bounded retries, and gap detection;
- restart-safe recording metadata;
- generated finite HLS VOD playback;
- browser playback and seek;
- Storage Provider Protocol v1 over Core-owned archive semantics, with `storage.local` as the bundled production/reference plugin. No production cloud provider is included.

The earlier Owncast TV example acceptance used the implementation bundled at that time: 90 media segments, about 270 seconds of VOD, zero detected gaps, successful restart/reload, and seeks at 0:00, 2:15, and 4:27. This work separates Owncast from Core and prepares Registry distribution. It is not installable from the official catalog until the public release and Registry approval are complete.

Not supported yet:

- chat timeline;
- finalized TAR/index archive format;
- HDD/NAS/LTO tiering, multi-pool placement, archive migration, and replication/mirroring;
- additional platform-specific adapter submissions;
- external audio rendition synchronization;
- encrypted HLS and partial-only/delta LL-HLS;
- DRM workflows;
- multi-user and role-based authorization;
- transcoding or additional export formats.

## Run

Requires Go 1.23+.

```sh
mkdir -p adapters
go build -o adapters/integrated-recorder-adapter-hls ./cmd/adapters/hls
DATA_DIR=./data ADAPTER_DIR=./adapters ADDR=127.0.0.1:8080 go run ./cmd/archiver
```

`cmd/archiver` is the legacy monolithic local development path and does not provide production Runtime Host generation updates, cross-process global resource coordination, or the bundled storage-provider process lifecycle. Production deployment and application updates use the Docker image's `runtime-host` entrypoint.

Then open `http://localhost:8080/`.

Docker configuration is included. The standard runtime image includes Alpine Linux's `ffmpeg` package for scene previews and MKV remux derivatives. Host installations do not require FFmpeg; canonical recording and VOD playback work without it. Alpine v3.21 package metadata identifies the `ffmpeg` license expression as `GPL-2.0-or-later AND LGPL-2.1-or-later`. FFmpeg upstream notes that optional GPL-covered components can affect distribution licensing. Before redistribution, check the exact image package metadata and the [Alpine package record](https://pkgs.alpinelinux.org/package/v3.21/community/x86/ffmpeg) and [FFmpeg legal considerations](https://ffmpeg.org/legal.html).

```sh
docker compose up -d
```

Open `http://localhost:8080/` in a browser to complete setup. Obtain the one-time setup code with:

```sh
docker compose exec archiver runtime-host setup-code
```

The container uses a named `/data` volume and publishes the Runtime Host listener on host loopback. The image includes the Runtime Host, initial Control/Engine release, the Host-declared bundled adapter `/adapters/integrated-recorder-adapter-hls`, and the standalone `storage.local` provider executable. Owncast is not bundled in the Core image. At startup the Host probes and imports `storage.local` through its normal immutable storage artifact/set lifecycle and uses the existing `/data/recordings` root, so legacy archive bytes need no move. `/data/runtime` remains reserved for Host-owned generations, catalogs, owners, secrets, IPC files, and recovery state.

Local development/custom executables can be installed under `./adapter-binaries` with the `integrated-recorder-adapter-*` filename convention. To allow them in a production Runtime Host, set `IR_ALLOW_OPERATOR_PLUGINS=1`; the Host periodically probes, hashes, and imports them from the read-only `/external-adapters` mount into an immutable adapter set without restarting the Host or container. They are labeled `Local / Operator trusted` and `Not reviewed by Integrated Recorder`. Existing recordings stay on their original Engine and adapter/storage provider sets. To use a Registry, set an HTTPS `IR_PLUGIN_REGISTRY_URL`; the official catalog is `https://integrated-recorder.github.io/plugin-registry/catalog-v3.json`. The `/adapters` page provides manual refresh, install, update, and uninstall for Registry plugins; `storage.local` is bundled, mandatory, and not uninstallable or dependent on Registry availability. Approved artifact size/SHA-256 and its type-specific Protocol v1 descriptor are verified before the existing immutable import lifecycle. A custom catalog is not an official review. Publisher PKI, automatic plugin updates, and sandboxing are not provided. If the selected provider is unavailable, the generation fails closed; Core does not fall back to direct filesystem access. Remote application updates require a separately provisioned Ed25519 public trust key; deployments without one fail closed. The unauthenticated control API is intended for a trusted host/private network or an authenticated reverse proxy; do not expose it directly to untrusted networks.

## API

| Method | Endpoint | Purpose |
| --- | --- | --- |
| `GET` | `/healthz` | Health check |
| `GET` | `/api/adapters` | Available adapters and status |
| `GET` | `/api/adapters/{id}/schema` | Adapter input and settings schema |
| `GET` / `PUT` | `/api/adapters/{id}/config` | Plugin-defined settings; secret values are never returned |
| `POST` | `/api/recordings` | Start recording |
| `GET` | `/api/resolve-workflows/{id}` | Inspect a suspended resource/configuration workflow |
| `POST` | `/api/resolve-workflows/{id}/continue` | Submit answers and resume a workflow |
| `DELETE` | `/api/resolve-workflows/{id}` | Cancel a suspended workflow |
| `GET` | `/api/recordings` | List recordings |
| `GET` | `/api/v2/recordings` | Search, filter, sort, and cursor-page recordings |
| `GET` | `/api/dashboard` | Real recording, storage, integrity, and adapter status |
| `GET` / `PUT` | `/api/recordings/{id}/tags` | Manage tags |
| `DELETE` | `/api/recordings/{id}` | Delete an inactive recording |
| `POST` | `/api/recordings/{id}/integrity/verify` | Start asynchronous archive verification |
| `POST` | `/api/integrity/jobs/{job_id}/cancel` | Cancel an active integrity verification |
| `GET` | `/api/logs` | Query the bounded application request log |
| `GET` | `/api/recordings/{id}/archive/index` | List canonical archive objects |
| `GET` | `/api/recordings/{id}/previews` | Get a bounded sample from the Preview Frame Index |
| `GET` | `/api/recordings/{id}/previews/{archive_ordinal}` | Get an individual scene preview frame |
| `POST` | `/api/recordings/{id}/previews` | Enable or reconcile per-segment preview generation |
| `GET` | `/api/adapters/{id}/resources` | List resources when the adapter advertises browse capability |
| `POST` | `/api/adapters/{id}/restart`, `/enable`, `/disable` | Manage discovered adapter processes |
| `POST` | `/api/recordings/{id}/exports` | Request MKV remux when FFmpeg is available |
| `GET` | `/api/recordings/{id}/thumbnail` | Read a compatibility poster projection from the Preview Frame Index |
| `POST` | `/api/recordings/{id}/thumbnail/regenerate` | Compatibility endpoint to request preview generation/reconciliation |
| `GET` / `PUT` | `/api/settings` | Supported UI theme and integrity concurrency settings |
| `POST` | `/api/auth/login`, `/logout`, `/bootstrap` | Single-administrator session authentication |
| `GET` | `/api/recordings/{id}` | Recording details |
| `POST` | `/api/recordings/{id}/stop` | Stop recording |
| `GET` | `/api/recordings/{id}/play/master.m3u8` | Generated VOD master playlist |
| `GET` | `/api/recordings/{id}/play/tracks/{track}/playlist.m3u8` | Generated VOD media playlist |
| `GET` | `/api/recordings/{id}/play/segments/{segmentID}` | Original stored payload |

Start a recording with the bundled direct-HLS adapter:

```sh
curl -X POST http://localhost:8080/api/recordings \
  -H 'Content-Type: application/json' \
  -d '{"adapter_id":"hls","input":{"manifest_url":"https://media.example/live/index.m3u8"},"title":"optional title","preview_mode":"segment"}'
```

`preview_mode` is optional and defaults to `disabled`. Setting it to `segment` schedules scene previews asynchronously after canonical segments are committed.

## Development

Web UI development requires Node.js 20 or newer. Run the Go API and Vite development server in separate terminals.

```sh
go run ./cmd/archiver
npm --prefix web ci
npm --prefix web run dev
```

Build the React assets into the Go embed directory before building a production binary. `make build` runs the UI build followed by the Go build.

```sh
npm --prefix web run build
go build ./...
# or
make build
```

For container first-run setup, open `/setup` in the browser and run `docker compose exec archiver runtime-host setup-code` to obtain the one-time code. Setup completion and its readiness gate are owned by the Runtime Host and persisted under `<DATA_DIR>/runtime`.

Validation commands:

```sh
go test -race -count=1 ./...
go vet ./...
```

## Roadmap

- [x] Original segment acquisition + restart-safe VOD playback
- [x] Platform-agnostic Core + Adapter Protocol v1 + bundled HLS bridge
- [ ] Owncast source plugin release and official Registry approval
- [x] Three-tier plugin admission provenance, separate publisher affiliation, and trust-aware runtime projection
- [x] Restartless immutable adapter lifecycle (Host import, adapter-set generations, and recording pinning)
- [x] Resource discovery, configuration inheritance, and challenge/resume foundation
- [ ] Chat timeline
- [ ] Finalized archive packaging + random-access index
- [x] React management SPA and connected product API foundation
- [ ] Additional platform adapters such as CHZZK, SOOP, and Twitch
- [ ] Hot/cold storage lifecycle, including HDD/NAS/LTO
- [x] Optional segment-based Preview Frame Index and remux-only export (when FFmpeg is available)

## Documentation

- [Architecture](docs/ARCHITECTURE.md)

## License

GNU Affero General Public License v3.0 only (**AGPL-3.0-only**). See [LICENSE](LICENSE).
