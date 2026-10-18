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

Open the printed `/setup` URL. Retrieve the one-time claim code on the host with the exact Compose command printed by the installer, ending in:

```sh
exec -T archiver runtime-host setup-code
```

Enter that code in the browser and choose the first Owner login and password. The login is user-selected; `owner` is a role name, not a required login. The installer does not generate, persist, or report the setup code, login, or password. Authentication remains enabled.

To reach a loopback-only deployment from another computer, use an SSH tunnel or configure a trusted HTTPS reverse proxy on the host. For an HTTPS reverse proxy, expose only the proxy to the intended private network, keep Runtime Host reachable only on loopback, change `COOKIE_SECURE` from `"0"` to `"1"` in the generated Compose file, and recreate the service. Do not publish port 8080 directly to an untrusted network.

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
