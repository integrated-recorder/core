# Integrated Recorder

[한국어](README.md) | **English**

Integrated Recorder Core is the runtime that preserves original live-stream media segments and broadcast metadata, then serves browser playback.

> [!NOTE]
> The current public version is the `v0.1.0-rc.8` prerelease. The installer selects a published stable release when one exists, and otherwise selects the newest published prerelease.

## Quick install

Supported targets are Linux `amd64`/`arm64`, Docker Desktop's local Linux engine on macOS, and WSL2 with Docker integration enabled. Install Docker Engine with Compose v2.20 or newer, Git, and Python 3.9 or newer. The Docker engine must be local to the host and able to access the recording directory.

```sh
git clone https://github.com/integrated-recorder/core.git
cd core
./scripts/install.sh --recordings-dir /absolute/path/to/recordings
```

The installer selects an official GitHub release, resolves its published tag to an exact commit SHA, and builds the image from that tag's source. It prints a notice when it selects a prerelease because no stable release is available. Pin a version with `--version v0.1.0-rc.8`. An existing install keeps its recorded version unless you explicitly pass `--upgrade`.

Application state, credentials, Runtime Host state, and plugin/generation metadata live in a dedicated Docker volume mounted at `/data`. Only the recording archive is bind-mounted from the requested host directory to `/data/recordings`. The application runs as container UID:GID `10001:10001`; the installer checks real file creation, read, rename, and deletion as that identity. It uses host port `8080` when free, otherwise selects and reports another loopback port. The default bind is `127.0.0.1`; LAN access requires an explicit `--bind 0.0.0.0` or a specific host interface address.

Open the browser URL printed by the installer and enter the first Owner login and password at `/setup`. There is no setup code or CLI claim step. Atomic durable account creation is allowed once, and success signs the Owner in immediately. If you expose the service on a LAN before setup, the first visitor can claim Owner; restrict network access and complete setup yourself first.

The default install is loopback HTTP. LAN access is an explicit choice with `--bind 0.0.0.0`; configure a firewall, and create the first Owner before allowing other LAN users to connect. After Owner setup, enable HTTPS in **Settings → Runtime environment → HTTPS**. Runtime Host directly serves Go TLS; no proxy container is added. A self-signed certificate is generated with the supplied real hostname/IP and detected interface IPs in its SAN. Browsers will warn until that certificate is trusted. For a public-domain certificate, enter one domain and select automatic ACME issuance; DNS must point here, and external TCP 443 must reach the host. Rerun the installer with `--https-port 443` to publish the TLS-ALPN validation port. Runtime Host obtains it on the first HTTPS connection and renews it directly. HTTP-to-HTTPS redirection waits for the certificate to be ready; keep external access restricted until issuance succeeds. To use a private CA, first enable self-signed HTTPS, reconnect over HTTPS, then upload the matching PEM certificate chain and private key; clients must trust that CA. Private-key uploads are never sent over cleartext HTTP. Self-signed certificates renew automatically at startup within 30 days of expiry and can be rotated from the UI; uploaded certificates can be replaced by uploading a renewed pair. Set the HTTP-to-HTTPS redirect in the UI. Host ports are selectable during install with `--port` and `--https-port`; rerunning the installer changes port mappings without selecting a different application release.

The installer prints the exact Compose project/file prefix and selected port. Add `logs -f archiver`, `stop`, `start`, or `restart` to that prefix. Check the running version/channel at `/api/setup/status` on the printed loopback URL. Back up both the `/data` named volume and the selected recordings directory. A normal rerun keeps the pinned release; use `--upgrade` to select an upgrade. Correct permission failures only on the dedicated archive directory, and do not delete a detected existing state volume to bypass an installation check.

See the [deployment guide](docs/DEPLOYMENT.md) for operations, backups/upgrades, NFS permissions, and recovery. The Korean guide is [here](docs/DEPLOYMENT.ko.md).

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

The default Runtime Host uses built-in user authentication. The browser creates the first Owner with a user-chosen login and password; the durable atomic user store permanently closes bootstrap after the first account. After setup, the browser authenticates with an HttpOnly session cookie. Mutation requests require a CSRF token. The installer never disables authentication. Before LAN or domain exposure, configure the firewall and HTTPS; credentials over plain HTTP on a LAN are not encrypted.

HTTPS runs inside the Runtime Host process with Go's TLS server. TLS requests receive Secure, HttpOnly session cookies while existing SameSite and CSRF protections remain in force. The installer binds both application ports only to loopback by default and does not publish them to the public internet. Installing the bundled release and trusting future signed application updates are separate: remote updates remain disabled until an authentic official Ed25519 public trust root is configured.

Change passwords and manage users or sessions under Settings → Account & security. There is no remote password-reset endpoint. An operator with direct server access can use the Compose project/file prefix printed by the installer with `exec -it archiver runtime-host reset-password <login>`, then enter the new password at the terminal prompt. The password is not passed as a command-line argument.

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
- [Deployment and operations](docs/DEPLOYMENT.md)
- [Plugin Trust Model](docs/PLUGIN_TRUST_MODEL.md)
- [Storage Provider Protocol v1](docs/STORAGE_PROVIDER_PROTOCOL_V1.md)
- [Storage Provider lifecycle](docs/STORAGE_PROVIDER_V1.md)
- [Release process](docs/RELEASING.md)
- [Plugin Registry](https://github.com/integrated-recorder/plugin-registry)

## Current limitations

Core provides a platform-independent timed-event archive and replay foundation. Streamer integrations are provided through Source Plugins. Multi-pool storage placement and replication, and DRM, are not yet available. See the linked guides for current capabilities and limitations.

## License

GNU Affero General Public License v3.0 only (**AGPL-3.0-only**). See [LICENSE](LICENSE).
