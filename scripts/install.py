#!/usr/bin/env python3
"""Install the official Integrated Recorder release as an isolated Compose project."""

from __future__ import annotations

import argparse
import datetime as dt
import json
import ipaddress
import mmap
import os
import re
import shutil
import socket
import stat
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any, Callable, Iterable


REPOSITORY = "integrated-recorder/core"
REPOSITORY_URL = "https://github.com/integrated-recorder/core.git"
REPOSITORY_SOURCE = "https://github.com/integrated-recorder/core"
API_ROOT = "https://api.github.com/repos/integrated-recorder/core"
COMPOSE_PROJECT = "integrated-recorder"
STATE_VOLUME = "integrated-recorder_application-state"
CONTAINER_UID = 10001
CONTAINER_GID = 10001
CONTAINER_HTTP_PORT = 8080
CONTAINER_HTTPS_PORT = 8443
MANIFEST_ROLES = ("runtime-host", "control-plane", "recorder-engine", "adapter-runtime")
SEMVER = re.compile(
    r"^v(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)"
    r"(?:-(?:0|[1-9][0-9]*|[0-9]*[A-Za-z-][0-9A-Za-z-]*)"
    r"(?:\.(?:0|[1-9][0-9]*|[0-9]*[A-Za-z-][0-9A-Za-z-]*))*)?"
    r"(?:\+[0-9A-Za-z-]+(?:\.[0-9A-Za-z-]+)*)?$"
)
SHA256 = re.compile(r"^[0-9a-f]{64}$")
COMMIT_SHA = re.compile(r"^[0-9a-f]{40}$")
KEY_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$")


class InstallError(RuntimeError):
    pass


def default_install_dir() -> Path:
    if sys.platform == "darwin":
        return Path.home() / "Library" / "Application Support" / "Integrated Recorder"
    xdg = os.environ.get("XDG_DATA_HOME")
    return (Path(xdg) if xdg else Path.home() / ".local" / "share") / "integrated-recorder"


def absolute_path(value: str | Path, label: str) -> Path:
    path = Path(value).expanduser()
    if not path.is_absolute():
        raise InstallError(f"{label} must be an absolute path: {value}")
    try:
        return path.resolve(strict=False)
    except OSError as exc:
        raise InstallError(f"Cannot resolve {label}: {exc.strerror or 'path error'}") from None


def validate_release_tag(tag: str) -> None:
    if not SEMVER.fullmatch(tag):
        raise InstallError("Release tag must be a version tag such as v0.1.0-rc.8.")


def channel_for_tag(tag: str, prerelease: bool) -> str:
    validate_release_tag(tag)
    channel = "prerelease" if "-" in tag.split("+", 1)[0] else "stable"
    if prerelease != (channel == "prerelease"):
        raise InstallError("GitHub release prerelease flag does not match its version tag.")
    return channel


def parse_timestamp(value: str) -> dt.datetime:
    try:
        parsed = dt.datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        raise InstallError("The selected release commit has an invalid committer timestamp.") from None
    if parsed.tzinfo is None:
        raise InstallError("The selected release commit timestamp has no timezone.")
    return parsed


def select_release(releases: Iterable[dict[str, Any]], version: str | None = None) -> dict[str, Any]:
    published = [
        release for release in releases
        if not release.get("draft") and release.get("published_at") and release.get("tag_name")
    ]
    if version:
        validate_release_tag(version)
        matches = [release for release in published if release.get("tag_name") == version]
        if not matches:
            raise InstallError(f"Official published release {version} was not found (or is a draft).")
        selected = matches[0]
        channel_for_tag(version, bool(selected.get("prerelease")))
        return selected

    stable = [release for release in published if not release.get("prerelease")]
    candidates = stable or [release for release in published if release.get("prerelease")]
    if not candidates:
        raise InstallError("No published official stable or prerelease release is available.")
    candidates.sort(key=lambda release: release.get("published_at") or "", reverse=True)
    selected = candidates[0]
    channel_for_tag(selected["tag_name"], bool(selected.get("prerelease")))
    return selected


def pinned_release_decision(
    current: dict[str, Any] | None,
    pending: dict[str, Any] | None,
    requested_version: str | None,
    upgrade: bool,
) -> tuple[str, dict[str, Any] | None]:
    """Return (policy, stored release); network selection is performed by the caller."""
    if pending is not None:
        pending_tag = pending.get("tag")
        if current is not None and pending_tag != current.get("tag") and not upgrade:
            raise InstallError("A previous application upgrade is pending; pass --upgrade to resume it.")
        if requested_version and requested_version != pending.get("tag") and not upgrade:
            raise InstallError("A different version is pending; pass --upgrade to select another release.")
        if not upgrade or requested_version == pending.get("tag"):
            return "resume-pending", pending
    if current is not None and not upgrade:
        if requested_version and requested_version != current.get("tag"):
            raise InstallError("This installation is pinned. Pass --upgrade to change its application release.")
        return "preserve-current", current
    if upgrade:
        return "select-upgrade", None
    return "select-fresh", None


def choose_port(requested: int | None, existing: int | None = None, available: Callable[[int], bool] | None = None, default_port: int = 8080, option: str = "--port") -> int:
    available = available or local_port_available
    if requested is not None:
        if not 1 <= requested <= 65535:
            raise InstallError("Host port must be between 1 and 65535.")
        if requested != existing and not available(requested):
            raise InstallError(f"Host port {requested} is unavailable. Choose another port with {option}.")
        return requested
    if existing is not None:
        return existing
    if available(default_port):
        return default_port
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
            probe.bind(("127.0.0.1", 0))
            return int(probe.getsockname()[1])
    except OSError:
        raise InstallError(f"No available host port could be selected; specify {option} explicitly.") from None


def local_port_available(port: int) -> bool:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
        try:
            probe.bind(("127.0.0.1", port))
            return True
        except OSError as exc:
            if exc.errno in (1, 13):
                raise InstallError(f"Cannot verify host port {port}; check it from an environment with local socket access.") from None
            return False


def bind_port_available(bind: str, port: int) -> bool:
    family = socket.AF_INET6 if ":" in bind else socket.AF_INET
    with socket.socket(family, socket.SOCK_STREAM) as probe:
        try:
            probe.bind((bind, port, 0, 0) if family == socket.AF_INET6 else (bind, port))
            return True
        except OSError as exc:
            if exc.errno in (1, 13):
                raise InstallError(f"Cannot verify port {port} on bind address {bind}; check it from an environment with local socket access.") from None
            return False


def normalize_bind(value: str) -> str:
    try:
        return ipaddress.ip_address(value).compressed
    except ValueError:
        raise InstallError("--bind must be an IPv4 or IPv6 interface address such as 127.0.0.1 or 0.0.0.0.") from None


def docker_architecture(value: str) -> str:
    os_name, sep, arch = value.strip().lower().partition("/")
    if not sep or os_name != "linux":
        raise InstallError("Docker must target a Linux engine.")
    normalized = {"x86_64": "amd64", "x64": "amd64", "aarch64": "arm64"}.get(arch, arch)
    if normalized not in ("amd64", "arm64"):
        raise InstallError(f"Docker engine architecture {arch or 'unknown'} is unsupported; use Linux amd64 or arm64.")
    return normalized


def validate_manifest(
    data: bytes,
    release: dict[str, Any],
    commit: str,
    architecture: str,
    commit_timestamp: str | None = None,
) -> dict[str, Any]:
    if not data or len(data) > 256 * 1024:
        raise InstallError("The official release manifest is empty or exceeds the supported size.")
    try:
        manifest = json.loads(data.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        raise InstallError("The official release manifest is not valid JSON.") from None
    tag = release["tag_name"]
    channel = channel_for_tag(tag, bool(release.get("prerelease")))
    if (
        manifest.get("manifest_schema_version") != 1
        or not isinstance(manifest.get("key_id"), str)
        or not KEY_ID.fullmatch(manifest["key_id"])
        or manifest.get("release_version") != tag[1:]
        or manifest.get("commit") != commit
        or manifest.get("channel") != channel
        or manifest.get("platform") != "linux"
        or manifest.get("architecture") != architecture
    ):
        raise InstallError("The signed-release manifest identity does not match the selected GitHub release and tag.")
    manifest_time = manifest.get("build_time")
    if not isinstance(manifest_time, str):
        raise InstallError("The official release manifest has no build timestamp.")
    if commit_timestamp is not None and parse_timestamp(manifest_time) != parse_timestamp(commit_timestamp):
        raise InstallError("The signed-release manifest timestamp does not match the selected commit.")
    artifacts = manifest.get("artifacts")
    if not isinstance(artifacts, list) or len(artifacts) != len(MANIFEST_ROLES):
        raise InstallError("The official release manifest does not declare the expected application roles.")
    by_role: dict[str, Any] = {}
    for artifact in artifacts:
        if not isinstance(artifact, dict):
            raise InstallError("The official release manifest contains an invalid artifact entry.")
        role = artifact.get("role")
        filename = artifact.get("filename")
        if role not in MANIFEST_ROLES or role in by_role:
            raise InstallError("The official release manifest has missing or duplicate application roles.")
        if filename != f"{role}-linux-{architecture}":
            raise InstallError("The official release manifest artifact filename does not match its declared role.")
        if not isinstance(artifact.get("size"), int) or artifact["size"] <= 0 or not SHA256.fullmatch(str(artifact.get("sha256", ""))):
            raise InstallError("The official release manifest contains an invalid artifact size or digest.")
        by_role[role] = artifact
    if set(by_role) != set(MANIFEST_ROLES):
        raise InstallError("The official release manifest does not include every application role.")
    return manifest


def quote_yaml(value: str) -> str:
    # Compose interpolates dollar signs even inside quoted YAML scalars.
    return json.dumps(value.replace("$", "$$"), ensure_ascii=False)


def render_compose(config: dict[str, Any]) -> str:
    release = config["release"]
    source_dir = quote_yaml(str(config["source_dir"]))
    recordings_dir = quote_yaml(str(config["recordings_dir"]))
    version = quote_yaml(release["version"])
    commit = quote_yaml(release["commit"])
    build_time = quote_yaml(release["build_time"])
    channel = quote_yaml(release["channel"])
    source = quote_yaml(release.get("source", REPOSITORY_SOURCE))
    tag = quote_yaml(release["tag"])
    image = quote_yaml(config["image"])
    architecture = config["architecture"]
    port = str(config["port"])
    https_port = str(config.get("https_port", CONTAINER_HTTPS_PORT))
    bind = config.get("bind", "127.0.0.1")
    bind_yaml = quote_yaml(bind)
    return f'''name: {COMPOSE_PROJECT}
services:
  archiver:
    image: {image}
    platform: linux/{architecture}
    build:
      context: {source_dir}
      dockerfile: Dockerfile
      args:
        VERSION: {version}
        COMMIT: {commit}
        BUILD_TIME: {build_time}
        RELEASE_CHANNEL: {channel}
      labels:
        org.opencontainers.image.source: {source}
        org.opencontainers.image.version: {version}
        org.opencontainers.image.revision: {commit}
        org.opencontainers.image.created: {build_time}
        com.integrated-recorder.release-channel: {channel}
        com.integrated-recorder.release-tag: {tag}
    user: "{CONTAINER_UID}:{CONTAINER_GID}"
    init: true
    restart: unless-stopped
    stop_grace_period: 60s
    ports:
      - target: {CONTAINER_HTTP_PORT}
        published: "{port}"
        host_ip: {bind_yaml}
        protocol: tcp
      - target: {CONTAINER_HTTPS_PORT}
        published: "{https_port}"
        host_ip: {bind_yaml}
        protocol: tcp
    environment:
      ADDR: ":{CONTAINER_HTTP_PORT}"
      IR_HTTPS_LISTEN_ADDR: ":{CONTAINER_HTTPS_PORT}"
      IR_HTTPS_PUBLIC_PORT: "{https_port}"
      COOKIE_SECURE: "0"
      DATA_DIR: /data
      ADAPTER_DIR: /external-adapters
      IR_ALLOW_OPERATOR_PLUGINS: "0"
      IR_PLUGIN_REGISTRY_URL: https://integrated-recorder.github.io/plugin-registry/catalog-v3.json
      IR_RELEASE_TRUSTED_KEYS_JSON: ""
    volumes:
      - type: volume
        source: application-state
        target: /data
      - type: bind
        source: {recordings_dir}
        target: /data/recordings
        bind:
          create_host_path: false
    labels:
      org.opencontainers.image.source: https://github.com/{REPOSITORY}
      org.opencontainers.image.version: {version}
      org.opencontainers.image.revision: {commit}
      org.opencontainers.image.created: {build_time}
      com.integrated-recorder.release-channel: {channel}
      com.integrated-recorder.release-tag: {quote_yaml(release["tag"])}
    healthcheck:
      test: ["CMD-SHELL", "wget -q -O - http://127.0.0.1:{CONTAINER_HTTP_PORT}/healthz >/dev/null"]
      interval: 10s
      timeout: 5s
      retries: 12
      start_period: 20s
volumes:
  application-state:
    name: {STATE_VOLUME}
'''

def validate_setup_status(status: dict[str, Any], release: dict[str, Any]) -> None:
    if status.get("auth_disabled") is not False:
        raise InstallError("Runtime Host did not confirm that production authentication is enabled.")
    if status.get("version") != release["version"] or status.get("release_channel") != release["channel"]:
        raise InstallError("Runtime Host setup status does not match the selected image build identity.")
    if status.get("state") not in ("uninitialized", "setup_in_progress", "ready"):
        raise InstallError("Runtime Host is not ready for setup or normal use; inspect its local container logs.")
    if status.get("state") == "uninitialized" and status.get("administrator_configured"):
        raise InstallError("Runtime Host setup state is inconsistent; the installation was not accepted.")


def permission_failure_message(path: Path) -> str:
    quoted = shlex_quote(str(path))
    return (
        f"The recording directory is not writable by container UID:GID {CONTAINER_UID}:{CONTAINER_GID}. "
        f"The installer did not change its contents. For a dedicated empty directory, set only its owner with:\n"
        f"  sudo chown {CONTAINER_UID}:{CONTAINER_GID} -- {quoted}\n"
        "For NFS with root-squash, set numeric ownership/ACLs on the NFS server instead. "
        "Then rerun the installer; it validates create, read, rename, and delete as the application user."
    )


def shlex_quote(value: str) -> str:
    return "'" + value.replace("'", "'\\''") + "'"


def binary_embeds_identity(binary_path: Path, release: dict[str, Any]) -> list[str]:
    expected = (release["version"], release["commit"], release["build_time"], release["channel"])
    if not binary_path.is_file() or binary_path.stat().st_size == 0:
        return list(expected)
    with binary_path.open("rb") as stream:
        with mmap.mmap(stream.fileno(), 0, access=mmap.ACCESS_READ) as content:
            return [value for value in expected if content.find(value.encode("utf-8")) < 0]


def atomic_write(path: Path, content: bytes, mode: int = 0o600) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temp_name = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    try:
        os.fchmod(fd, mode)
        with os.fdopen(fd, "wb") as output:
            output.write(content)
            output.flush()
            os.fsync(output.fileno())
        os.replace(temp_name, path)
    except BaseException:
        try:
            os.unlink(temp_name)
        except FileNotFoundError:
            pass
        raise


class GitHub:
    def __init__(self) -> None:
        # Respect the operator's ordinary HTTPS proxy configuration for GitHub.
        self.opener = urllib.request.build_opener()

    def read(self, url: str, accept: str = "application/vnd.github+json", limit: int = 2 * 1024 * 1024) -> bytes:
        request = urllib.request.Request(url, headers={"Accept": accept, "User-Agent": "integrated-recorder-installer/1"})
        try:
            with self.opener.open(request, timeout=25) as response:
                data = response.read(limit + 1)
        except urllib.error.HTTPError as exc:
            code = exc.code
            raise InstallError(f"Official GitHub request failed with HTTP {code}.") from None
        except (urllib.error.URLError, TimeoutError, OSError):
            raise InstallError("Could not reach the official GitHub repository over HTTPS.") from None
        if len(data) > limit:
            raise InstallError("Official GitHub response exceeded the installer size limit.")
        return data

    def json(self, path: str) -> Any:
        try:
            return json.loads(self.read(API_ROOT + path).decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError):
            raise InstallError("Official GitHub returned invalid release metadata.") from None

    def releases(self) -> list[dict[str, Any]]:
        found: list[dict[str, Any]] = []
        for page in range(1, 21):
            page_items = self.json(f"/releases?per_page=100&page={page}")
            if not isinstance(page_items, list):
                raise InstallError("Official GitHub release listing has an unexpected format.")
            found.extend(item for item in page_items if isinstance(item, dict))
            if len(page_items) < 100:
                return found
        raise InstallError("Official GitHub has too many releases for bounded selection.")

    def resolve_tag(self, tag: str) -> str:
        encoded = urllib.parse.quote(tag, safe="")
        ref = self.json(f"/git/ref/tags/{encoded}")
        obj = ref.get("object", {})
        for _ in range(5):
            kind = obj.get("type")
            sha = obj.get("sha")
            if not isinstance(sha, str) or not re.fullmatch(r"[0-9a-f]{40}", sha):
                break
            if kind == "commit":
                return sha
            if kind != "tag":
                break
            tag_object = self.json(f"/git/tags/{sha}")
            obj = tag_object.get("object", {})
        raise InstallError("The selected GitHub tag does not resolve to a commit.")

    def commit_timestamp(self, sha: str) -> str:
        commit = self.json(f"/commits/{sha}")
        value = commit.get("commit", {}).get("committer", {}).get("date")
        if not isinstance(value, str):
            raise InstallError("The selected release commit has no committer timestamp.")
        parse_timestamp(value)
        return value

    def manifest(self, release: dict[str, Any], architecture: str, commit: str, commit_timestamp: str) -> dict[str, Any]:
        name = f"release-linux-{architecture}.json"
        assets = {asset.get("name"): asset for asset in release.get("assets", []) if isinstance(asset, dict)}
        signature_name = name + ".sig"
        if name not in assets or signature_name not in assets:
            raise InstallError(f"Published release is missing {name} or its signature asset.")
        url = assets[name].get("browser_download_url")
        if not isinstance(url, str) or not url.startswith("https://github.com/integrated-recorder/core/releases/download/"):
            raise InstallError("The official release manifest asset URL is invalid.")
        return validate_manifest(
            self.read(url, accept="application/octet-stream", limit=256 * 1024),
            release,
            commit,
            architecture,
            commit_timestamp,
        )


def run(args: list[str], *, capture: bool = True, check: bool = True, input_text: str | None = None) -> subprocess.CompletedProcess[str]:
    try:
        completed = subprocess.run(
            args,
            check=False,
            text=True,
            stdout=subprocess.PIPE if capture else None,
            stderr=subprocess.PIPE if capture else None,
            input=input_text,
        )
    except FileNotFoundError:
        raise InstallError(f"Required command is unavailable: {args[0]}") from None
    if check and completed.returncode != 0:
        detail = (completed.stderr or completed.stdout or "").strip()
        if len(detail) > 600:
            detail = detail[-600:]
        message = f"Command failed: {args[0]} (exit {completed.returncode})."
        if detail:
            message += f"\n{detail}"
        raise InstallError(message)
    return completed


def git_output(source: Path, *args: str) -> str:
    return run(["git", "-C", str(source), *args]).stdout.strip()


def prepare_source(install_dir: Path, release: dict[str, Any], commit: str, api: GitHub) -> tuple[Path, str]:
    source_dir = install_dir / "source" / commit
    if source_dir.exists():
        if not (source_dir / ".git").exists():
            raise InstallError(f"The pinned source path exists but is not a Git checkout: {source_dir}")
        actual = git_output(source_dir, "rev-parse", "HEAD")
        dirty = git_output(source_dir, "status", "--porcelain", "--untracked-files=all")
        if actual != commit or dirty:
            raise InstallError("The cached release source was modified; it was preserved and will not be built.")
        local_tag = run(["git", "-C", str(source_dir), "rev-parse", f"{release['tag']}^{{commit}}"], check=False).stdout.strip()
        if local_tag and local_tag != commit:
            raise InstallError("The cached release tag resolves to a different commit.")
    else:
        parent = source_dir.parent
        parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        temp = Path(tempfile.mkdtemp(prefix=f".{commit}.", dir=parent))
        shutil.rmtree(temp)
        try:
            run(["git", "init", "--quiet", str(temp)])
            run(["git", "-C", str(temp), "remote", "add", "origin", REPOSITORY_URL])
            refspec = f"refs/tags/{release['tag']}:refs/tags/{release['tag']}"
            run(["git", "-C", str(temp), "fetch", "--depth=1", "origin", refspec], capture=False)
            actual = git_output(temp, "rev-parse", f"{release['tag']}^{{commit}}")
            if actual != commit:
                raise InstallError("The fetched official tag does not resolve to the verified GitHub commit.")
            run(["git", "-C", str(temp), "checkout", "--quiet", "--detach", commit])
            if git_output(temp, "status", "--porcelain", "--untracked-files=all"):
                raise InstallError("The freshly fetched release source is not clean.")
            os.replace(temp, source_dir)
        except BaseException:
            shutil.rmtree(temp, ignore_errors=True)
            raise
    local_commit = git_output(source_dir, "rev-parse", "HEAD")
    if local_commit != commit:
        raise InstallError("The application source checkout does not match the selected release commit.")
    timestamp = git_output(source_dir, "show", "-s", "--format=%cI", commit)
    if not timestamp:
        timestamp = api.commit_timestamp(commit)
    parse_timestamp(timestamp)
    return source_dir, timestamp


def check_compose_version() -> None:
    output = run(["docker", "compose", "version", "--short"]).stdout.strip()
    match = re.search(r"v?(\d+)\.(\d+)(?:\.(\d+))?", output)
    if not match or (int(match.group(1)), int(match.group(2))) < (2, 20):
        raise InstallError("Docker Compose v2.20 or newer is required for safe nested bind-mount validation.")


def docker_preflight() -> str:
    check_compose_version()
    context_data = run(["docker", "context", "inspect"]).stdout
    try:
        contexts = json.loads(context_data)
        host = contexts[0]["Endpoints"]["docker"]["Host"]
    except (json.JSONDecodeError, IndexError, KeyError, TypeError):
        raise InstallError("Could not determine the active Docker context safely.") from None
    if not isinstance(host, str) or host.startswith(("tcp://", "ssh://")):
        raise InstallError("The active Docker context is remote. Use a local Docker Engine so the recording path refers to this host.")
    if not host.startswith(("unix://", "npipe://")):
        raise InstallError("The active Docker context is not a supported local socket context.")
    platform = run(["docker", "info", "--format", "{{.OSType}}/{{.Architecture}}"]).stdout.strip()
    return docker_architecture(platform)


def ensure_install_dir(path: Path, has_marker: bool) -> None:
    try:
        if path.exists() and not path.is_dir():
            raise InstallError("Installation directory exists but is not a directory.")
        if path.exists() and not has_marker and any(path.iterdir()):
            raise InstallError("Installation directory is nonempty but has no Integrated Recorder metadata; it was left untouched.")
        path.mkdir(parents=True, exist_ok=True, mode=0o700)
        info = path.stat()
        if info.st_uid != os.getuid():
            raise InstallError("Installation directory must be owned by the current user; existing data was left untouched.")
        if stat.S_IMODE(info.st_mode) & 0o077:
            os.chmod(path, 0o700)
    except InstallError:
        raise
    except OSError:
        raise InstallError("Installation directory must be private and writable by the current user.") from None


def load_metadata(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        raise InstallError(f"Installation metadata is unreadable or malformed: {path}") from None
    if not isinstance(value, dict) or value.get("schema_version") != 1 or not isinstance(value.get("release"), dict):
        raise InstallError("Installation metadata version is unsupported; existing data was left untouched.")
    release = value["release"]
    try:
        validate_release_tag(release["tag"])
        if release["version"] != release["tag"][1:] or not COMMIT_SHA.fullmatch(release["commit"]):
            raise ValueError
        if release["channel"] not in ("stable", "prerelease"):
            raise ValueError
    except (KeyError, TypeError, ValueError):
        raise InstallError("Installation metadata has an invalid pinned release identity.") from None
    return value


def docker_existing_installation() -> str | None:
    project_ids = run([
        "docker", "ps", "-aq", "--filter", f"label=com.docker.compose.project={COMPOSE_PROJECT}"
    ]).stdout.split()
    if project_ids:
        return f"Compose project {COMPOSE_PROJECT} already contains container {project_ids[0][:12]}"
    ids = run(["docker", "ps", "-aq", "--filter", "label=com.docker.compose.service=archiver"]).stdout.split()
    for container_id in ids:
        inspect = run(["docker", "inspect", container_id]).stdout
        try:
            item = json.loads(inspect)[0]
            config = item.get("Config", {})
            labels = config.get("Labels") or {}
            entrypoint = config.get("Entrypoint") or []
            if labels.get("org.opencontainers.image.source") == f"https://github.com/{REPOSITORY}" or (
                entrypoint and entrypoint[0] == "/usr/local/bin/runtime-host"
            ):
                project = labels.get("com.docker.compose.project", "unknown")
                return f"container {container_id[:12]} (Compose project {project})"
        except (json.JSONDecodeError, IndexError, TypeError):
            continue
    volumes = run(["docker", "volume", "ls", "--format", "{{.Name}}"]).stdout.splitlines()
    protected = [
        name for name in volumes
        if name == STATE_VOLUME or re.fullmatch(r"(?:core|integrated-recorder)_recording-data", name)
    ]
    if protected:
        return f"existing data volume {protected[0]}"
    networks = run(["docker", "network", "ls", "--format", "{{.Name}}"])
    if f"{COMPOSE_PROJECT}_default" in networks.stdout.splitlines():
        return f"existing Compose network {COMPOSE_PROJECT}_default"
    return None


def verify_existing_state_volume(required: bool) -> None:
    if not required:
        return
    result = run(
        ["docker", "volume", "inspect", "--format={{.Name}}", STATE_VOLUME],
        check=False,
    )
    if result.returncode != 0 or result.stdout.strip() != STATE_VOLUME:
        raise InstallError(
            f"The existing installation state volume {STATE_VOLUME} is missing or unreadable; "
            "refusing to create an empty replacement. Restore or inspect the volume before retrying."
        )


def ensure_archive_dir(path: Path) -> bool:
    created = not path.exists()
    try:
        if created:
            path.mkdir(parents=True, mode=0o750)
        if not path.is_dir():
            raise InstallError("Recording storage path is not a directory.")
        return created
    except InstallError:
        raise
    except OSError as exc:
        raise InstallError(f"Cannot create recording directory ({exc.strerror or 'permission denied'}): {path}") from None


def render_permission_probe(image: str, path: Path, uid: int, gid: int) -> str:
    return f'''services:
  probe:
    image: {quote_yaml(image)}
    user: "{uid}:{gid}"
    network_mode: none
    volumes:
      - type: bind
        source: {quote_yaml(str(path))}
        target: /recordings
        bind:
          create_host_path: false
'''


def probe_archive_permissions(image: str, path: Path, created: bool) -> None:
    probe = (
        "set -eu; p=/recordings/.ir-install-probe-" + os.urandom(12).hex() + "; q=$p.renamed; "
        "trap 'rm -f \"$p\" \"$q\"' EXIT HUP INT TERM; "
        "printf probe > \"$p\"; test \"$(cat \"$p\")\" = probe; "
        "mv \"$p\" \"$q\"; test -f \"$q\"; rm \"$q\"; trap - EXIT HUP INT TERM"
    )
    project = f"ir-permission-probe-{os.getpid()}-{os.urandom(4).hex()}"
    with tempfile.TemporaryDirectory(prefix="integrated-recorder-permission-probe-") as temp_dir:
        compose_file = Path(temp_dir) / "compose.yaml"

        def run_probe(uid: int, gid: int, command: str) -> subprocess.CompletedProcess[str]:
            atomic_write(compose_file, render_permission_probe(image, path, uid, gid).encode("utf-8"), 0o600)
            return run(
                ["docker", "compose", "--project-name", project, "--file", str(compose_file),
                 "run", "--rm", "--no-deps", "--entrypoint", "/bin/sh", "probe", "-ec", command],
                check=False,
            )

        checked = run_probe(CONTAINER_UID, CONTAINER_GID, probe)
        if checked.returncode == 0:
            return
        # Ownership changes are limited to a directory this invocation made.
        # Existing directories, including empty ones, retain their metadata.
        if created:
            chown = run_probe(0, 0, f"chown {CONTAINER_UID}:{CONTAINER_GID} /recordings")
            if chown.returncode == 0:
                checked = run_probe(CONTAINER_UID, CONTAINER_GID, probe)
                if checked.returncode == 0:
                    return
    raise InstallError(permission_failure_message(path))


def verify_image_binaries(image: str, release: dict[str, Any]) -> None:
    with tempfile.TemporaryDirectory(prefix="integrated-recorder-image-check-") as output_dir:
        output = Path(output_dir)
        copy_script = (
            "set -eu; mkdir -p /tmp/ir-build-identity; "
            "cp /usr/local/bin/runtime-host /tmp/ir-build-identity/runtime-host; "
            "cp /opt/integrated-recorder/initial/control-plane /tmp/ir-build-identity/control-plane; "
            "cp /opt/integrated-recorder/initial/recorder-engine /tmp/ir-build-identity/recorder-engine; "
            "cp /adapters/integrated-recorder-adapter-hls /tmp/ir-build-identity/adapter-runtime; "
            "cp /usr/local/lib/integrated-recorder/plugins/storage.local /tmp/ir-build-identity/storage.local; "
        )
        container_id = run(
            ["docker", "create", "--network", "none", "--user", "0:0",
             "--entrypoint", "/bin/sh", image, "-ec", copy_script]
        ).stdout.strip()
        if not container_id:
            raise InstallError("Docker could not create the isolated image identity inspection container.")
        try:
            run(["docker", "start", "--attach", container_id])
            for name in ("runtime-host", "control-plane", "recorder-engine", "adapter-runtime", "storage.local"):
                run(["docker", "cp", f"{container_id}:/tmp/ir-build-identity/{name}", str(output / name)])
        finally:
            run(["docker", "rm", "--force", container_id], check=False)
        for name in ("runtime-host", "control-plane", "recorder-engine", "adapter-runtime", "storage.local"):
            if not (output / name).is_file() or (output / name).stat().st_size == 0:
                raise InstallError(f"Built image is missing its required {name} executable.")
        # Runtime Host and Control Plane expose the selected application build
        # identity. The separate Engine executable does not expose buildinfo.
        for name in ("runtime-host", "control-plane"):
            missing = binary_embeds_identity(output / name, release)
            if missing:
                fields = ", ".join(missing)
                raise InstallError(f"Built {name} binary does not contain the selected non-development identity fields: {fields}.")


def verify_image_labels(image: str, release: dict[str, Any]) -> None:
    output = run(["docker", "image", "inspect", "--format={{json .Config.Labels}}", image]).stdout.strip()
    try:
        labels = json.loads(output)
    except json.JSONDecodeError:
        raise InstallError("Docker image does not expose readable build-identity labels.") from None
    expected = {
        "org.opencontainers.image.source": release.get("source", REPOSITORY_SOURCE),
        "org.opencontainers.image.version": release["version"],
        "org.opencontainers.image.revision": release["commit"],
        "org.opencontainers.image.created": release["build_time"],
        "com.integrated-recorder.release-channel": release["channel"],
        "com.integrated-recorder.release-tag": release["tag"],
    }
    if not isinstance(labels, dict) or any(labels.get(key) != value for key, value in expected.items()):
        raise InstallError("Docker image labels do not match the selected release identity.")


def compose_command(compose_file: Path, *args: str) -> list[str]:
    return ["docker", "compose", "--project-name", COMPOSE_PROJECT, "--file", str(compose_file), *args]


def write_compose(path: Path, config: dict[str, Any]) -> None:
    atomic_write(path, render_compose(config).encode("utf-8"), 0o600)


def write_metadata(path: Path, metadata: dict[str, Any]) -> None:
    atomic_write(path, (json.dumps(metadata, indent=2, sort_keys=True) + "\n").encode("utf-8"), 0o600)


def get_setup_status(port: int) -> dict[str, Any]:
    url = f"http://127.0.0.1:{port}/api/setup/status"
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    try:
        with opener.open(url, timeout=5) as response:
            value = json.loads(response.read(64 * 1024).decode("utf-8"))
    except urllib.error.HTTPError as exc:
        raise InstallError(f"Runtime Host setup-status endpoint returned HTTP {exc.code}.") from None
    except (urllib.error.URLError, TimeoutError, OSError, UnicodeDecodeError, json.JSONDecodeError):
        raise InstallError("Runtime Host did not return a valid setup-status response.") from None
    if not isinstance(value, dict):
        raise InstallError("Runtime Host setup-status response has an unexpected format.")
    return value


def verify_browser_ui(port: int) -> None:
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    request = urllib.request.Request(f"http://127.0.0.1:{port}/setup", headers={"Accept": "text/html"})
    try:
        with opener.open(request, timeout=5) as response:
            body = response.read(256 * 1024)
            content_type = response.headers.get("Content-Type", "")
            if response.status != 200 or "text/html" not in content_type or b'id="root"' not in body:
                raise InstallError("Runtime Host is healthy but did not serve the Integrated Recorder setup page.")
    except urllib.error.HTTPError as exc:
        raise InstallError(f"Runtime Host setup page returned HTTP {exc.code}.") from None
    except (urllib.error.URLError, TimeoutError, OSError):
        raise InstallError("Runtime Host setup page is not reachable on its loopback port.") from None


def wait_for_runtime(port: int, release: dict[str, Any], timeout: int = 180) -> dict[str, Any]:
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    deadline = time.monotonic() + timeout
    last_error = "not ready"
    while time.monotonic() < deadline:
        try:
            with opener.open(f"http://127.0.0.1:{port}/healthz", timeout=3) as response:
                health = json.loads(response.read(8192).decode("utf-8"))
            if isinstance(health, dict) and health.get("status") == "ok":
                status = get_setup_status(port)
                validate_setup_status(status, release)
                verify_browser_ui(port)
                return status
        except InstallError as exc:
            last_error = str(exc)
        except (urllib.error.URLError, TimeoutError, OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
            last_error = type(exc).__name__
        time.sleep(2)
    raise InstallError(f"Runtime Host did not become healthy within {timeout} seconds ({last_error}). Check local container logs.")


def network_settings(args: argparse.Namespace, existing: dict[str, Any] | None, install_dir: Path) -> tuple[str, int]:
    if args.bind:
        bind = normalize_bind(args.bind)
    elif existing and existing.get("bind"):
        bind = normalize_bind(existing["bind"])
    else:
        bind = "127.0.0.1"
    previous_port = (existing or {}).get("https_port")
    port = choose_port(args.https_port, int(previous_port) if previous_port else None, lambda value: bind_port_available(bind, value), CONTAINER_HTTPS_PORT, "--https-port")
    return bind, port


def release_record(release: dict[str, Any], commit: str, build_time: str, manifest: dict[str, Any]) -> dict[str, Any]:
    tag = release["tag_name"]
    return {
        "tag": tag,
        "source": REPOSITORY_SOURCE,
        "version": tag[1:],
        "commit": commit,
        "build_time": build_time,
        "channel": channel_for_tag(tag, bool(release.get("prerelease"))),
        "architecture_manifest": manifest.get("architecture"),
        "manifest_identity_checked": True,
        "manifest_signature_independently_verified": False,
    }


def read_pending(path: Path) -> dict[str, Any] | None:
    return load_metadata(path)


def prepare_release(api: GitHub, architecture: str, requested_version: str | None) -> tuple[dict[str, Any], str, str, dict[str, Any]]:
    repo = api.json("")
    if repo.get("full_name") != REPOSITORY:
        raise InstallError("GitHub repository identity is not the official integrated-recorder/core repository.")
    releases = api.releases()
    selected = select_release(releases, requested_version)
    tag = selected["tag_name"]
    channel = channel_for_tag(tag, bool(selected.get("prerelease")))
    commit = api.resolve_tag(tag)
    if not COMMIT_SHA.fullmatch(commit):
        raise InstallError("The selected release tag resolved to an invalid commit identity.")
    build_time = api.commit_timestamp(commit)
    manifest = api.manifest(selected, architecture, commit, build_time)
    identity = release_record(selected, commit, build_time, manifest)
    if identity["channel"] != channel:
        raise InstallError("Release channel resolution was inconsistent.")
    return selected, commit, build_time, identity


def validate_existing_recording_path(metadata: dict[str, Any], requested: Path | None) -> Path:
    try:
        stored = absolute_path(metadata["recordings_dir"], "stored recording directory")
    except (KeyError, TypeError):
        raise InstallError("Existing installation does not record its archive path; refusing to guess or remount it.") from None
    if requested is not None and requested != stored:
        raise InstallError("This installation already uses a different archive path. Data migration is not automatic; the existing path was preserved.")
    return stored


def cli_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Build and run the official Integrated Recorder release as an isolated local Docker Compose project."
    )
    parser.add_argument("--recordings-dir", help="absolute host directory for canonical recording archives")
    parser.add_argument("--version", help="pin a published official release tag, for example v0.1.0-rc.8")
    parser.add_argument("--port", type=int, help="application host port (default: 8080 if free, otherwise an available port)")
    parser.add_argument("--bind", help="explicit host interface IPv4/IPv6 address (default: 127.0.0.1)")
    parser.add_argument("--https-port", type=int, help="host port mapped to Runtime Host HTTPS (default: 8443 if free)")
    parser.add_argument("--install-dir", help="deployment metadata and exact release source (platform-specific user-data default)")
    parser.add_argument("--upgrade", action="store_true", help="explicitly select/build another release for an existing installation")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = cli_parser().parse_args(argv)
    try:
        install(args)
        return 0
    except InstallError as exc:
        print(f"Integrated Recorder installation failed: {exc}", file=sys.stderr)
        return 1
    except KeyboardInterrupt:
        print("Integrated Recorder installation interrupted. Existing Docker data was not removed.", file=sys.stderr)
        return 130


def install(args: argparse.Namespace) -> None:
    install_dir = absolute_path(args.install_dir, "installation directory") if args.install_dir else default_install_dir().resolve(strict=False)
    metadata_path = install_dir / "deployment.json"
    pending_path = install_dir / "deployment.pending.json"
    current = load_metadata(metadata_path)
    pending = read_pending(pending_path)
    ensure_install_dir(install_dir, current is not None or pending is not None)
    requested_recordings = absolute_path(args.recordings_dir, "recording directory") if args.recordings_dir else None
    if current or pending:
        archive = validate_existing_recording_path(pending or current, requested_recordings)
    else:
        if requested_recordings is None:
            raise InstallError("A fresh installation requires --recordings-dir with an absolute host path.")
        archive = requested_recordings

    prior_deployment = pending or current
    selected_bind, selected_https_port = network_settings(args, prior_deployment, install_dir)

    policy, stored_release = pinned_release_decision(
        current.get("release") if current else None,
        pending.get("release") if pending else None,
        args.version,
        bool(args.upgrade),
    )
    architecture = docker_preflight()
    verify_existing_state_volume(
        current is not None or bool(pending and pending.get("compose_started"))
    )
    if not current and not pending:
        existing = docker_existing_installation()
        if existing:
            raise InstallError(f"An existing Runtime Host Docker installation was detected ({existing}); no volume or container was changed.")

    api = GitHub()
    selected_release: dict[str, Any] | None = None
    manifest: dict[str, Any] | None = None
    if policy in ("preserve-current", "resume-pending") and stored_release:
        release_identity = dict(stored_release)
        if release_identity.get("architecture_manifest") != architecture:
            # The image is architecture-specific; a preexisting install is not silently moved across daemon architectures.
            raise InstallError("The active installation was built for a different Docker architecture; use a new install directory for a platform change.")
        if args.version and args.version != release_identity["tag"]:
            raise InstallError("Requested version does not match the pinned release.")
        if "build_time" not in release_identity or "channel" not in release_identity:
            raise InstallError("Pinned release metadata is incomplete; existing installation was left unchanged.")
        tag = release_identity["tag"]
        commit = release_identity["commit"]
        build_time = release_identity["build_time"]
        channel = release_identity["channel"]
        version = release_identity["version"]
        release_identity.setdefault("manifest_identity_checked", True)
        release_identity.setdefault("manifest_signature_independently_verified", False)
    else:
        selected_release, commit, build_time, release_identity = prepare_release(api, architecture, args.version)
        tag = selected_release["tag_name"]
        version = tag[1:]
        channel = release_identity["channel"]
        if channel == "prerelease":
            print(f"Notice: selected official prerelease {tag}.", file=sys.stderr)

    if policy == "preserve-current" and current:
        selected_port = choose_port(args.port, int(current["port"]))
    elif policy == "resume-pending" and pending:
        selected_port = choose_port(args.port, int(pending["port"]))
    else:
        selected_port = choose_port(args.port)

    archive_created = ensure_archive_dir(archive)
    source_dir, local_build_time = prepare_source(install_dir, {"tag": tag}, commit, api)
    if parse_timestamp(local_build_time) != parse_timestamp(build_time):
        raise InstallError("The fetched tag's Git committer time differs from official commit metadata.")
    # Use Git's exact %cI representation, matching the official release job.
    build_time = local_build_time
    release_identity["build_time"] = build_time
    image = f"localhost/integrated-recorder-core:{commit}"
    release_data = {
        "tag": tag,
        "source": REPOSITORY_SOURCE,
        "version": version,
        "commit": commit,
        "build_time": build_time,
        "channel": channel,
        "architecture_manifest": architecture,
        "manifest_identity_checked": bool(release_identity.get("manifest_identity_checked")),
        "manifest_signature_independently_verified": False,
    }
    candidate = {
        "schema_version": 1,
        "repository": REPOSITORY,
        "release": release_data,
        "recordings_dir": str(archive),
        "port": selected_port,
        "architecture": architecture,
        "compose_project": COMPOSE_PROJECT,
        "state_volume": STATE_VOLUME,
        "source_dir": str(source_dir),
        "image": image,
        "network_exposure": selected_bind,
        "bind": selected_bind,
        "https_port": selected_https_port,
        "container_uid": CONTAINER_UID,
        "container_gid": CONTAINER_GID,
    }
    old_compose = install_dir / "compose.yaml"
    candidate_compose = install_dir / "compose.pending.yaml"
    write_compose(candidate_compose, candidate)
    write_metadata(pending_path, candidate)

    run(compose_command(candidate_compose, "build", "archiver"), capture=False)
    verify_image_labels(image, release_data)
    verify_image_binaries(image, release_data)
    probe_archive_permissions(image, archive, archive_created)

    try:
        candidate["compose_started"] = True
        write_metadata(pending_path, candidate)
        run(compose_command(candidate_compose, "up", "-d", "--no-build", "--wait", "--wait-timeout", "180", "--remove-orphans", "archiver"), capture=False)
        setup_status = wait_for_runtime(selected_port, release_data)
    except InstallError:
        if old_compose.is_file() and current is not None:
            try:
                run(compose_command(old_compose, "up", "-d", "--no-build", "--wait", "--wait-timeout", "180", "archiver"), capture=False)
            except InstallError:
                print("The previous Compose definition could not be restored automatically; its data volume was not removed.", file=sys.stderr)
        raise

    os.replace(candidate_compose, old_compose)
    candidate.pop("compose_started", None)
    candidate["installed_at"] = dt.datetime.now(dt.timezone.utc).isoformat()
    candidate["setup_state"] = setup_status["state"]
    candidate["owner_setup_required"] = not bool(setup_status.get("administrator_configured")) and not bool(setup_status.get("auth_disabled"))
    write_metadata(metadata_path, candidate)
    try:
        pending_path.unlink()
    except FileNotFoundError:
        pass

    print_install_result(install_dir, candidate)


def print_install_result(install_dir: Path, metadata: dict[str, Any]) -> None:
    release = metadata["release"]
    bind = metadata.get("bind", "127.0.0.1")
    display_host = discover_host_address() if bind == "0.0.0.0" else bind
    if ":" in display_host and not display_host.startswith("["):
        display_host = f"[{display_host}]"
    url = f"http://{display_host}:{metadata['port']}/"
    https_url = f"https://{display_host}:{metadata.get('https_port', CONTAINER_HTTPS_PORT)}/"
    bind_description = f"{bind}:{metadata['port']} (HTTP), {bind}:{metadata.get('https_port', CONTAINER_HTTPS_PORT)} (HTTPS; enable in Web UI)"
    print("Integrated Recorder is running.")
    print(f"Release: {release['tag']} ({release['channel']})")
    print(f"Source commit: {release['commit']}")
    print(f"Docker image: {metadata['image']}")
    print(f"Application state: Docker volume {metadata['state_volume']} mounted at /data")
    print(f"Recording archives: {metadata['recordings_dir']} mounted at /data/recordings")
    print(f"Container identity: {metadata['container_uid']}:{metadata['container_gid']}")
    print(f"Network exposure: {bind_description}")
    print(f"First-run browser URL: {url.rstrip('/')}/setup")
    print(f"HTTPS URL after enabling it in Web UI: {https_url}")
    if metadata.get("owner_setup_required"):
        print("Create the first Owner account in the browser. Choose its login and password; the installer does not create credentials.")
        if bind not in ("127.0.0.1", "::1"):
            print("Warning: the first browser to create an account becomes Owner. Restrict network access until you create your account.")
            print("Warning: this explicit LAN binding is not encrypted until HTTPS is enabled in the Web UI.")
    else:
        print("Existing account state was preserved; Owner onboarding was not repeated.")
    compose = f"docker compose --project-name {COMPOSE_PROJECT} --file {shlex_quote(str(install_dir / 'compose.yaml'))}"
    print("Operations:")
    print(f"  {compose} logs -f archiver")
    print(f"  {compose} stop")
    print(f"  {compose} start")
    print(f"  {compose} restart")
    print("Future signed application updates are disabled until an authentic public release trust root is configured.")


def discover_host_address() -> str:
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as probe:
            probe.connect(("1.1.1.1", 53))
            return probe.getsockname()[0]
    except OSError:
        return "127.0.0.1"


if __name__ == "__main__":
    raise SystemExit(main())
