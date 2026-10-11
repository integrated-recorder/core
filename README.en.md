# Integrated Recorder

[한국어](README.md) | **English**

Integrated Recorder Core is the runtime that preserves original live-stream media segments and broadcast metadata, then serves browser playback.

> [!NOTE]
> The project is under active development. Core does not yet have a stable public release.

## Role and boundaries

Core owns the Runtime Host, Recorder Engine, Control Plane, Web UI/API, and canonical archive. It is responsible for Recording identity and lifecycle, media-acquisition orchestration, segment ordinals, gaps and integrity, the metadata timeline, VOD reconstruction, plugin and application-generation lifecycle, and writer fencing.

Source Plugins provide platform-specific behavior; Storage Providers place physical objects. Core does not host or build third-party plugin source. Bundled reference implementations such as `source.hls` and `storage.local` are maintained in the Core repository, which also remains the authority for canonical archive meaning and write access.

## What it does

- Preserves original media bytes in a segment-native archive. FFmpeg and Streamlink are not required on the canonical recording path.
- Records manifests and metadata, then builds browser-playable VOD playlists from the archive.
- Supports long-running headless operation centered on Docker and a browser UI.
- FFmpeg may be used for optional derivative features such as previews and remux exports. The Core image includes FFmpeg for these optional features.

## Bundled components

- **source.hls** — a generic direct-HLS Source Plugin with no platform-specific behavior.
- **storage.local** — the reference Storage Plugin using Storage Provider Protocol v1.

Owncast is a first-party Source Plugin, but it is not bundled in Core or the Docker image. It is distributed as v0.2.0 through the official Plugin Registry. Plugin executables are native code and are not sandboxed. See the [Plugin Trust Model](docs/PLUGIN_TRUST_MODEL.md) for admission provenance and its limits.

Operator-supplied plugins are disabled by default; set `IR_ALLOW_OPERATOR_PLUGINS=1` to use them in a production Runtime Host. They are native executables that have not been reviewed by the project. The default file secret store restricts permissions but does not encrypt stored values.

## Authentication and network exposure

The default Runtime Host uses built-in user authentication. First-run setup requires the one-time claim code printed to the local console or container logs. After setup, the browser authenticates with a session cookie. Mutation requests require a CSRF token. `AUTH_DISABLED=1` is a development escape hatch and is accepted only when the listener binds to loopback. The Runtime Host fails closed on public binds.

For production network access, configure HTTPS, a trusted reverse proxy, and an appropriate network boundary. Set `COOKIE_SECURE=1` behind a TLS reverse proxy to require Secure cookies.

## Quick start

Docker is the primary way to run Core.

```sh
docker compose up -d
docker compose logs archiver
```

Use the one-time setup code printed to the local Runtime Host console or container logs, then open [http://localhost:8080/](http://localhost:8080/) and complete `/setup`. If logs are unavailable, run `docker compose exec archiver runtime-host setup-code`. The container uses a named `/data` volume.

The official Plugin Registry catalog is `https://integrated-recorder.github.io/plugin-registry/catalog-v3.json`. The Registry distributes plugin metadata; it does not build plugins. Already installed plugins and archives remain usable when the Registry is unavailable.

## Archive and storage

Core defines Recording identity, metadata, ordinals, gaps, integrity, and canonical commit ordering. A Storage Plugin maps logical object keys to physical bytes. Local storage also uses Storage Provider Protocol. See [Architecture](docs/ARCHITECTURE.md), [Storage Provider Protocol v1](docs/STORAGE_PROVIDER_PROTOCOL_V1.md), and the [provider lifecycle guide](docs/STORAGE_PROVIDER_V1.md).

## Development

The legacy development entrypoint does not provide the production Runtime Host generation lifecycle.

```sh
mkdir -p adapters
go build -o adapters/integrated-recorder-adapter-hls ./cmd/adapters/hls
DATA_DIR=./data ADAPTER_DIR=./adapters ADDR=127.0.0.1:8080 go run ./cmd/archiver
```

See the Makefile and CI workflows for Web UI development and validation commands. Release and signed-update procedures are documented in [RELEASING](docs/RELEASING.md).

## Documentation

- [Architecture](docs/ARCHITECTURE.md)
- [Plugin Trust Model](docs/PLUGIN_TRUST_MODEL.md)
- [Storage Provider Protocol v1](docs/STORAGE_PROVIDER_PROTOCOL_V1.md)
- [Storage Provider lifecycle](docs/STORAGE_PROVIDER_V1.md)
- [Release process](docs/RELEASING.md)
- [Plugin Registry](https://github.com/integrated-recorder/plugin-registry)

## Current limitations

The project is under active development. Chat timelines, multi-pool storage placement and replication, DRM, and multi-user role management are not available yet. See the linked guides and release notes for current capabilities and limitations.

## License

GNU Affero General Public License v3.0 only (**AGPL-3.0-only**). See [LICENSE](LICENSE).
