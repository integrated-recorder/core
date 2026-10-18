# Deployment and operations

This guide documents the supported Docker installation. For a fresh install, the only application-specific input is the absolute host directory that will hold recording archives.

## Supported hosts and prerequisites

The image runs a Linux application build for `amd64` or `arm64`. Supported local Docker targets include Linux, Docker Desktop's local Linux engine on macOS, and Docker Engine integrated with WSL2. The installer requires Docker Engine, Docker Compose v2.20 or newer, Git, and Python 3.9 or newer. It rejects remote Docker contexts because a bind mount must refer to the same host directory supplied by the operator.

The Docker daemon must be able to read the checked-out source and access the recording path. On Docker Desktop, share the source and recording directories with Docker Desktop. The installer builds the official source locally; Integrated Recorder does not currently publish a Docker image.

## Fresh installation

From the official repository checkout:

```sh
git clone https://github.com/integrated-recorder/core.git
cd core
./scripts/install.sh --recordings-dir /absolute/path/to/recordings
```

The installer reads published release metadata from `integrated-recorder/core`. It prefers the latest published stable release, and selects the newest published prerelease only when no stable release exists. Drafts are excluded. It resolves the selected tag to a commit SHA, checks the release manifest identity and required role artifacts, fetches that exact tag, checks that Git resolves it to the same SHA, and builds only that detached source tree. Use `--version v0.1.0-rc.8` to pin a release. When a prerelease is selected, the installer says so before building.

The Docker build receives `VERSION`, `COMMIT`, `BUILD_TIME`, and `RELEASE_CHANNEL`. `BUILD_TIME` is the selected commit's Git committer timestamp in the same `%cI` representation used by the official release workflow. The installer checks the image labels and the actual Runtime Host and Control Plane build-info strings; the Recorder Engine, HLS adapter, and storage provider are also checked for presence in the image. After startup it checks the Host's reported version/channel, authentication state, health endpoint, and setup page. It does not claim the locally built executables are byte-for-byte identical to the signed Linux release binaries. The official source release has signed executable manifests, but this Docker source build does not independently verify those signatures.

## Deployment layout and persistent data

The installer writes private deployment metadata and a generated `compose.yaml` in the platform's per-user application-data directory:

| Host | Default install directory |
| --- | --- |
| Linux / WSL2 | `${XDG_DATA_HOME:-$HOME/.local/share}/integrated-recorder` |
| macOS | `$HOME/Library/Application Support/Integrated Recorder` |

It records the selected tag, commit, build time, channel, architecture, host port, source checkout, and canonical archive path in `deployment.json`. A pending deployment record allows an interrupted installation to resume safely. The exact release source is cached under `source/<commit>/` and is checked for modifications before a build.

Compose uses the isolated project `integrated-recorder` and a dedicated named volume `integrated-recorder_application-state` mounted at `/data`. `/data` holds durable application configuration, authentication and sessions, Runtime Host owner/generation state, plugin catalogs and artifacts, storage-provider metadata, recovery coordination, and other management state. It is not a disposable cache.

Only the archive root is a bind mount: the chosen host directory is mounted at `/data/recordings`. The nested bind mount overlays that directory inside the parent `/data` named volume, so recording media is not silently stored in Docker's application-state volume. Compose uses a long bind-mount declaration with host-path auto-creation disabled; the installer also probes the exact resolved path before starting the service. It never moves or copies existing archive contents.

## Directory ownership and NFS/POSIX storage

The production container runs as UID:GID `10001:10001`. The application sets the dedicated archive root to mode `0700`, then creates private archive objects below it. The installer creates the requested directory if needed and runs a disposable test as UID:GID 10001 that checks `chmod`, write, read, same-directory rename, and delete. The probe file is removed before installation continues.

If a newly created or empty dedicated archive directory is not writable, the installer may change ownership of that exact directory to `10001:10001` using a short-lived container. It never recursively changes ownership and never changes the parent media directory or neighboring archives. It does not attempt that ownership adjustment on a nonempty directory. If validation fails, it stops before reporting success and gives a narrowly scoped correction. For NFS with root-squash, assign the numeric UID/GID or a suitable ACL on the NFS server; changing ownership from a client container may be prohibited. Do not weaken permissions on a shared parent directory.

The installer validates the directory at install time and assumes an existing NFS mount remains available. It does not add an NFS watcher or silently fall back to Docker-managed storage. If the Docker daemon cannot access the path, the permission probe fails and the app is not started.

## Network and browser setup

The Runtime Host listens inside the container on port `8080`. The installer uses host port `8080` when available; otherwise it binds an available port on `127.0.0.1` and prints the actual URL. `--port N` requests a specific loopback port and fails if it is already occupied. The installer does not expose the login/setup UI to the LAN or public internet.

Open the printed `/setup` URL and choose the first Owner login and password. There is no setup code or CLI claim step. The first account is created once through the browser and is signed in immediately. If LAN access is enabled before setup, the first person to reach the page can become Owner; restrict network access and create the account yourself first.

### Network binding and HTTPS

Default installation binds HTTP to `127.0.0.1`. To explicitly allow LAN access, add `--bind 0.0.0.0`, or provide a specific host interface IP. HTTP on a LAN is unencrypted; use a firewall and prefer HTTPS for credentials. A first-run LAN setup can be claimed by the first visitor.

The Runtime Host directly serves HTTPS with Go's TLS stack; there is no proxy/TLS container. Compose publishes the HTTP port (default `8080`) and TLS port (default `8443`) on the selected host address. Both bind to loopback by default. Choose ports with `--port` and `--https-port`, and choose LAN exposure explicitly with `--bind 0.0.0.0` or a specific interface IP. These mappings can be changed by rerunning the installer without changing the pinned app release. Confirm firewall policy before using LAN binding; the first browser to complete setup becomes Owner.

After first Owner setup, use **Settings → Runtime environment → HTTPS**. Enter the DNS name or IP clients actually use and first enable the generated self-signed listener. Reopen the UI at its HTTPS URL, accept/trust that certificate for the configured name, then upload a replacement PEM certificate chain and matching private key. The server validates key match, validity, and all configured names. Private-key uploads are accepted only over HTTPS, never over cleartext HTTP. Key material is saved in `/data/security/https.json` inside the private application data volume with 0600 file permissions and is never returned by the API. Session cookies set over HTTPS are Secure, HttpOnly, and retain the existing SameSite/CSRF rules.

The HTTPS screen can request a public ACME certificate for one DNS name. DNS must resolve to this host's public address, inbound TCP 443 must reach the configured HTTPS mapping, and the installer must publish host port 443 with `--https-port 443`. Runtime Host uses ACME TLS-ALPN directly and stores the resulting private key in its protected ACME cache. Issuance starts on the first HTTPS connection; status remains pending until the certificate is actually obtained, and HTTP redirect stays off until the certificate is ready. Keep public access restricted until the first HTTPS request succeeds; while issuance is pending, the application remains available over its explicitly configured HTTP binding for recovery. An issuance failure is reported without exposing provider errors; verify DNS and port forwarding, then retry by visiting the domain. Certificates renew automatically. For private CA, upload a matching PEM pair over HTTPS; clients must trust that CA. Self-signed certificates have a two-year lifetime and are regenerated automatically at Runtime Host startup within 30 days of expiration; operators can rotate them sooner from the UI. Replacing a user-provided certificate uploads a new validated pair. Port conflicts prevent TLS listener activation and are shown as a safe status/error; no unrelated service is stopped.

Existing installs retain their release pin, data volume, archive path and bind settings on an ordinary rerun. `--bind`, `--port`, and `--https-port` update network publication only. HTTPS certificate configuration remains in persistent `/data` and is enabled or disabled through the authenticated UI. Disabling HTTPS removes only its listener and does not remove certificate/application/archive data. HTTP/HTTPS changes do not imply an application release upgrade.

The HTTPS status separates the saved HTTP redirect preference from whether redirects are currently active. Disabling HTTPS stops the TLS listener and HTTP redirects while preserving the certificate mode, hosts, certificate/private key, and redirect preference. The settings page moves the current browser from the HTTPS URL to the published HTTP port and preserves the authenticated session and CSRF token. **Re-enable saved HTTPS settings** starts the existing configuration again; it reuses a valid certificate, renews a near-expiry self-signed certificate under the existing policy, and refuses an expired custom certificate. A disabled configuration remains disabled after Runtime Host restart.

## Normal operation

The installer prints the exact project/file command prefix for the generated Compose file. Use that prefix for each operation:

```sh
docker compose --project-name integrated-recorder --file /path/to/install/compose.yaml logs -f archiver
docker compose --project-name integrated-recorder --file /path/to/install/compose.yaml stop
docker compose --project-name integrated-recorder --file /path/to/install/compose.yaml start
docker compose --project-name integrated-recorder --file /path/to/install/compose.yaml restart
```

Use `docker compose ... ps` to inspect service state. The container health check polls `/healthz`; the installer also checks `/api/setup/status` and the browser setup route before it marks installation successful. `deployment.json` and Docker image labels contain the selected release identity. The Runtime Host setup status/API reports the active Host build identity. The installer never stores Owner credentials.

To verify the running version, use the host port printed by the installer:

```sh
curl -fsS http://127.0.0.1:<selected-port>/api/setup/status
```

The response includes the Runtime Host version and release channel. The selected source commit and build timestamp are also recorded in the private `deployment.json` file in the install directory.

Do not use `docker compose down -v`, `docker volume prune`, or global Docker cleanup as routine operations. Those actions can delete durable application state or unrelated services.

## Release pinning and upgrades

Re-running the installer without `--upgrade` keeps the version in `deployment.json`; it does not ask GitHub for a newer application version or replace the pinned release. An interrupted fresh installation resumes its pending release. If an explicitly requested upgrade was interrupted, repeat it with `--upgrade` (and the same `--version` if one was specified) to resume; a routine rerun refuses to finish a different pending version. The configured recording directory is also pinned and cannot be changed by a routine rerun.

To explicitly select a newer official release, run the maintained installer from an up-to-date official repository checkout:

```sh
./scripts/install.sh --upgrade --recordings-dir /same/absolute/recordings/path
```

Add `--version vX.Y.Z` to pin the upgrade target. The installer builds the exact selected tag, records the new identity only after the service passes health/setup checks, and reuses the existing `/data` volume and archive bind. It attempts to restart the prior Compose definition if the new container fails readiness. Host/container replacement is a maintenance event and is not the Runtime Host's restartless application-generation handover; plan upgrades when no live recording is in progress. Application data schema rollback is not guaranteed. Keep a verified backup before upgrades and follow release notes.

An older manually created Compose deployment or detected `recording-data`/application-state volume is not adopted automatically. The installer refuses to guess its layout, mount point, or ownership. Preserve it and plan a separately verified migration; do not point a fresh installation at an existing archive tree without confirming the storage layout.

## Backups and recovery

Back up both the named `/data` volume and the host recording directory. They represent different parts of one deployment: the archive alone is not sufficient to preserve authentication, generation/provider metadata, and plugin configuration, while `/data` alone does not contain the media archive when the bind mount is active. Stop the service for a consistent file-level backup, or use storage/filesystem snapshots with equivalent consistency guarantees. Protect backup copies because `/data` contains credentials and secret configuration; file permissions restrict access but values are not encrypted at rest by default.

Do not routinely delete or recreate the application-state volume to troubleshoot. For startup problems, inspect the deployment's logs and `docker compose ... ps`; for archive permission errors, rerun the installer after correcting ownership on only the dedicated archive directory. For missing/corrupt installation metadata, stop and inspect the existing volumes before recovery rather than starting a fresh project over them. Password recovery is local-only and is described by the Runtime Host's `reset-password` command.

## Release trust and Plugin Registry

The installer builds the Docker image from the selected official Git tag; it does not pull an application image and does not copy private signing material. `IR_RELEASE_TRUSTED_KEYS_JSON` is empty in the generated Compose file, so the bundled application is usable but future signed remote application-generation updates remain disabled until an authentic Ed25519 public trust root is configured. Obtain that public key through the project's trusted release/deployment process. Never copy a GitHub Actions private signing key to the host or invent a trust root.

The official Plugin Registry catalog is configured separately and remains enabled for normal plugin discovery/update behavior. Plugin artifacts and desired state are stored under `/data`; plugin lifecycle updates are distinct from rebuilding or replacing the Docker image.
