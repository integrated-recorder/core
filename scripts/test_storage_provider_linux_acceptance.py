"""Opt-in Linux Docker acceptance for official upgrades and storage diagnostics.

These tests are intended for a fresh ephemeral GitHub Actions runner only.
They use unique Compose projects and named volumes, and cleanup is scoped to
those exact resources. No credentials, external source URLs, or raw provider
stderr are printed.

Run with IR_RUN_STORAGE_UPGRADE_DOCKER_E2E=1 or
IR_RUN_STORAGE_DIAGNOSTIC_DOCKER_E2E=1.
"""

from __future__ import annotations

import datetime as dt
import hashlib
import json
import os
import re
import shutil
import subprocess
import tempfile
import time
import unittest
import uuid
from pathlib import Path
from unittest import mock

import install


RC9_TAG = "v0.1.0-rc.9"
RC9_COMMIT = "5f7761e32cb1156115c24dfd9e62d49661a16a81"
RC10_TAG = "v0.1.0-rc.10"
RC10_COMMIT = "85e1daa7eba52f562b3b1a1c3ce881c39b9afcd3"


def docker(args: list[str], *, check: bool = True) -> subprocess.CompletedProcess[str]:
    result = subprocess.run(
        ["docker", *args], text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=False
    )
    if check and result.returncode:
        detail = (result.stderr or result.stdout).strip()
        if len(detail) > 1200:
            detail = detail[-1200:]
        raise AssertionError(f"docker command failed ({result.returncode}): {args[0]}\n{detail}")
    return result


def compose_file_for(install_dir: Path) -> Path:
    pending = install_dir / "compose.pending.yaml"
    current = install_dir / "compose.yaml"
    return current if current.exists() else pending


def compose_container(install_dir: Path) -> str:
    path = compose_file_for(install_dir)
    result = install.run(install.compose_command(path, "ps", "-q", "archiver"), check=False)
    container_id = result.stdout.strip()
    if result.returncode or not container_id:
        raise AssertionError("isolated Compose project has no Runtime Host container")
    return container_id


def generation_snapshot(container_id: str) -> dict:
    output = docker(["exec", container_id, "cat", "/data/runtime/state/generations.json"]).stdout
    return json.loads(output)


def active_generation(container_id: str) -> tuple[dict, dict]:
    state = generation_snapshot(container_id)
    active_id = state.get("active_generation_id")
    generation = state.get("generations", {}).get(active_id)
    if not active_id or not isinstance(generation, dict) or generation.get("state") != "active":
        raise AssertionError("Runtime Host did not persist one active application generation")
    return state, generation


def assert_storage_pin(test: unittest.TestCase, container_id: str, set_id: str) -> dict:
    catalog_root = "/data/runtime/storage-providers"
    catalog = json.loads(docker(["exec", container_id, "cat", f"{catalog_root}/state.json"]).stdout)
    manifest_path = f"{catalog_root}/sets/{set_id}/manifest.json"
    manifest = json.loads(docker(["exec", container_id, "cat", manifest_path]).stdout)
    artifact = manifest.get("artifact")
    test.assertIsInstance(artifact, dict)
    digest = artifact.get("digest")
    test.assertRegex(digest or "", r"^[0-9a-f]{64}$")
    docker(["exec", container_id, "/bin/sh", "-ec", f"test -x {catalog_root}/artifacts/{digest}/provider"])
    return catalog


def assert_running_identity(test: unittest.TestCase, container_id: str, tag: str, commit: str) -> dict:
    inspect = json.loads(docker(["inspect", container_id]).stdout)[0]
    test.assertEqual(inspect["State"]["Status"], "running")
    test.assertEqual(inspect["State"]["Health"]["Status"], "healthy")
    labels = inspect["Config"].get("Labels") or {}
    test.assertEqual(labels.get("org.opencontainers.image.version"), tag[1:])
    test.assertEqual(labels.get("org.opencontainers.image.revision"), commit)
    test.assertEqual(labels.get("com.integrated-recorder.release-channel"), "prerelease")
    state, generation = active_generation(container_id)
    test.assertEqual(generation["version"], tag[1:])
    test.assertEqual(generation["commit"], commit)
    test.assertTrue(generation.get("storage_provider_set_id"))
    test.assertTrue(generation.get("adapter_set_id"))
    return {"registry": state, "generation": generation}


def state_marker(container_id: str, name: str) -> None:
    docker([
        "exec", "--user", f"{install.CONTAINER_UID}:{install.CONTAINER_GID}", container_id,
        "/bin/sh", "-ec", f"printf preserved > /data/{name} && test \"$(cat /data/{name})\" = preserved",
    ])


def release_directories(container_id: str) -> set[str]:
    output = docker([
        "exec", container_id, "/bin/sh", "-c",
        'for d in /data/runtime/releases/release-*; do if [ -d "$d" ]; then basename "$d"; fi; done',
    ]).stdout
    return set(output.splitlines())


def run_upgrade_archive_fixture(test: unittest.TestCase, container_id: str, action: str, set_id: str) -> None:
    """Exercise a real v2 archive through the container's pinned provider."""
    binary = os.environ.get("IR_DOCKER_ARCHIVE_FIXTURE_BINARY", "")
    if not binary or not Path(binary).is_file():
        test.fail("Linux acceptance did not provide the compiled storageprocess archive fixture test binary")
    container_binary = "/tmp/ir-storage-upgrade-archive-fixture.test"
    docker(["cp", binary, f"{container_id}:{container_binary}"])
    result = docker([
        "exec", "--user", f"{install.CONTAINER_UID}:{install.CONTAINER_GID}",
        "--env", f"IR_DOCKER_ARCHIVE_FIXTURE_ACTION={action}",
        "--env", f"IR_DOCKER_ARCHIVE_FIXTURE_DATA_DIR=/data",
        "--env", f"IR_DOCKER_ARCHIVE_FIXTURE_SET_ID={set_id}",
        container_id, container_binary, "-test.run", "^TestDockerImageUpgradeArchiveFixture$", "-test.v",
    ])
    test.assertIn("PASS", result.stdout)
    test.assertNotIn("FAIL", result.stdout)


def build_diagnostic_image() -> tuple[str, str, str]:
    commit = subprocess.run(["git", "rev-parse", "HEAD"], check=True, text=True, stdout=subprocess.PIPE).stdout.strip()
    created = subprocess.run(["git", "show", "-s", "--format=%cI", commit], check=True, text=True, stdout=subprocess.PIPE).stdout.strip()
    timestamp = dt.datetime.fromisoformat(created.replace("Z", "+00:00"))
    if not commit or len(commit) != 40 or timestamp.tzinfo is None:
        raise AssertionError("diagnostic build identity is incomplete")
    version = "0.1.0-rc.10-diagnostic." + commit[:12]
    image = f"localhost/integrated-recorder-storage-diagnostic:{commit[:12]}"
    release_identity = {
        "tag": "diagnostic-" + commit[:12],
        "version": version,
        "commit": commit,
        "build_time": created,
        "channel": "prerelease",
        "source": install.REPOSITORY_SOURCE,
    }
    docker([
        "build", "--file", "Dockerfile", "--tag", image,
        "--label", f"org.opencontainers.image.source={install.REPOSITORY_SOURCE}",
        "--label", f"org.opencontainers.image.version={version}",
        "--label", f"org.opencontainers.image.revision={commit}",
        "--label", f"org.opencontainers.image.created={created}",
        "--label", "com.integrated-recorder.release-channel=prerelease",
        "--label", f"com.integrated-recorder.release-tag=diagnostic-{commit[:12]}",
        "--build-arg", f"VERSION={version}",
        "--build-arg", f"COMMIT={commit}",
        "--build-arg", f"BUILD_TIME={created}",
        "--build-arg", "RELEASE_CHANNEL=prerelease", ".",
    ])
    install.verify_image_labels(image, release_identity)
    install.verify_image_binaries(image, release_identity)
    return image, version, commit


def safe_startup_failure_class(output: str) -> str:
    """Map known startup messages to fixed codes without returning raw logs."""
    classifications = (
        ("confirm initial Recorder Engine inventory", "initial_engine_inventory_unavailable"),
        ("engine inventory endpoint is unavailable", "initial_engine_inventory_unavailable"),
        ("active runtime generation is inconsistent", "active_generation_inconsistent"),
        ("active installed release failed verification", "active_installed_release_verification"),
        ("active runtime registry does not match its signed release", "active_registry_release_mismatch"),
        ("Runtime Host storage provider catalog is unavailable", "storage_catalog_open_failed"),
        ("initialize bundled storage.local provider", "bundled_storage_initialization_failed"),
        ("active Runtime Host storage provider set is unavailable", "active_storage_provider_set_unavailable"),
        ("active Runtime Host storage provider is unavailable", "active_storage_provider_probe_failed"),
        ("Runtime Host adapter catalog is unavailable", "adapter_catalog_open_failed"),
        ("Runtime Host plugin registry state is unavailable", "plugin_registry_state_unavailable"),
        ("Runtime Host plugin source state is unavailable", "plugin_source_state_unavailable"),
    )
    for marker, classification in classifications:
        if marker in output:
            return classification
    return "not_reported"


class DiagnosticStartupClassificationTests(unittest.TestCase):
    def test_known_bootstrap_failures_map_to_bounded_classes(self):
        cases = {
            "active installed release failed verification: /private/path": "active_installed_release_verification",
            "initialize bundled storage.local provider: storage provider initialization failed": "bundled_storage_initialization_failed",
            "Runtime Host storage provider catalog is unavailable": "storage_catalog_open_failed",
            "unclassified failure /private/path": "not_reported",
        }
        for output, expected in cases.items():
            with self.subTest(expected=expected):
                self.assertEqual(safe_startup_failure_class(output), expected)


def diagnose_rc9_state_with_diagnostic_image(
    root: Path,
    state_volume: str,
    archive: Path,
    candidate_container_id: str,
) -> str:
    """Probe an isolated clone of the restored RC9 state with instrumented code."""
    image, diagnostic_version, diagnostic_commit = build_diagnostic_image()
    clone_volume = f"{install.COMPOSE_PROJECT}-diagnostic-state-{uuid.uuid4().hex[:8]}"
    clone_archive = root / "diagnostic-cloned-recordings"
    clone_archive.mkdir()
    container_name = "ir-storage-rc9-state-diagnostic-" + uuid.uuid4().hex[:8]
    container_id = ""
    volume_created = False
    result = "container=not_started health=not_checked stage=not_reported code=not_reported"
    try:
        # Stop only the failed candidate container so the isolated acceptance
        # volume is quiescent. The enclosing installer then performs its normal
        # RC9 rollback after this diagnostic clone is removed.
        docker(["stop", "--time", "10", candidate_container_id], check=False)
        docker(["volume", "create", clone_volume])
        volume_created = True
        docker([
            "run", "--rm", "--user", "0:0", "--entrypoint", "/bin/sh",
            "--volume", f"{state_volume}:/source:ro", "--volume", f"{clone_volume}:/dest", "alpine:3.21", "-ec",
            "uid=$(stat -c %u /source); gid=$(stat -c %g /source); mode=$(stat -c %a /source); "
            "cp -a /source/. /dest/; chown \"$uid:$gid\" /dest; chmod \"$mode\" /dest",
        ])
        snapshot_output = docker([
            "run", "--rm", "--user", "0:0", "--entrypoint", "/bin/sh",
            "--volume", f"{clone_volume}:/data:ro", "alpine:3.21", "-ec",
            "cat /data/runtime/state/generations.json; printf '\\n__RELEASE_DIRS__\\n'; "
            "for d in /data/runtime/releases/release-*; do if [ -d \"$d\" ]; then basename \"$d\"; fi; done; exit 0",
        ]).stdout
        marker = "\n__RELEASE_DIRS__\n"
        snapshot_text, separator, release_text = snapshot_output.partition(marker)
        generation_state = json.loads(snapshot_text) if separator else {}
        active_id = generation_state.get("active_generation_id", "")
        active = generation_state.get("generations", {}).get(active_id, {})
        active_version = str(active.get("version", "unknown"))
        active_commit = str(active.get("commit", "unknown"))
        expected_release = "release-" + hashlib.sha256(
            (active_version + "\x00" + active_commit.lower()).encode("utf-8")
        ).hexdigest()
        release_dirs = set(release_text.splitlines())
        active_release_dir = "present" if expected_release in release_dirs else "missing"
        docker([
            "run", "--rm", "--user", "0:0", "--entrypoint", "/bin/sh",
            "--volume", f"{archive}:/source:ro", "--volume", f"{clone_archive}:/dest", "alpine:3.21", "-ec",
            "cp -a /source/. /dest/; chown -R 10001:10001 /dest",
        ])
        container_id = docker([
            "run", "-d", "--name", container_name,
            "--volume", f"{clone_volume}:/data", "--volume", f"{clone_archive}:/data/recordings", image,
        ]).stdout.strip()
        deadline = time.monotonic() + 90
        healthy = False
        state = "unknown"
        while time.monotonic() < deadline:
            state = docker(["inspect", "--format={{.State.Status}}", container_id], check=False).stdout.strip() or "unknown"
            if state != "running":
                break
            response = docker([
                "exec", container_id, "/bin/sh", "-ec", "wget -q -O- http://127.0.0.1:8080/healthz",
            ], check=False)
            if response.returncode == 0:
                try:
                    payload = json.loads(response.stdout)
                except json.JSONDecodeError:
                    payload = None
                healthy = isinstance(payload, dict) and payload.get("status") == "ok"
                if healthy:
                    break
            time.sleep(2)

        inspect_result = docker(["inspect", container_id], check=False)
        exit_code = "unknown"
        if inspect_result.returncode == 0:
            try:
                exit_code = str(json.loads(inspect_result.stdout)[0].get("State", {}).get("ExitCode", "unknown"))
            except (json.JSONDecodeError, IndexError, TypeError):
                pass
        app_logs = docker(["logs", "--tail", "100", container_id], check=False)
        combined = (app_logs.stdout or "") + (app_logs.stderr or "")
        matches = re.findall(r"stage=([a-z][a-z0-9_]{0,63}) code=([a-z][a-z0-9_]{0,63})", combined)
        stage, code = matches[-1] if matches else ("not_reported", "not_reported")
        startup_class = safe_startup_failure_class(combined)
        result = (
            f"container={state} health={'ok' if healthy else 'failed'} exit_code={exit_code} "
            f"stage={stage} code={code} startup_class={startup_class} "
            f"active_version={active_version} active_commit={active_commit} active_release_dir={active_release_dir} "
            f"image={diagnostic_version} commit={diagnostic_commit}"
        )
        return result
    finally:
        if container_id:
            docker(["rm", "--force", container_id], check=False)
        if volume_created:
            docker(["volume", "rm", clone_volume], check=False)
        if clone_archive.exists():
            docker([
                "run", "--rm", "--user", "0:0", "--entrypoint", "/bin/sh",
                "--volume", f"{root}:/acceptance", "alpine:3.21", "-ec",
                f"chown -R {os.getuid()}:{os.getgid()} /acceptance/diagnostic-cloned-recordings && chmod -R u+rwX /acceptance/diagnostic-cloned-recordings",
            ], check=False)


class OfficialRC9OfficialReleaseDockerUpgradeTests(unittest.TestCase):
    """Exercise official installer rollback, pending retry and release upgrades."""

    @unittest.skipUnless(
        os.environ.get("IR_RUN_STORAGE_UPGRADE_DOCKER_E2E") == "1",
        "requires a fresh Linux Docker runner; set IR_RUN_STORAGE_UPGRADE_DOCKER_E2E=1",
    )
    def test_official_rc9_install_target_and_pending_retry(self):
        if shutil.which("docker") is None:
            self.fail("Docker acceptance was requested but Docker is unavailable")
        project = f"ir-storage-upgrade-{uuid.uuid4().hex[:12]}"
        volume = f"{project}-state"
        with tempfile.TemporaryDirectory(prefix="ir storage upgrade acceptance ") as temp:
            root = Path(temp)
            install_dir = root / "deployment with spaces"
            archive = root / "recordings with spaces"
            archive_marker = "linux-upgrade-archive-marker"
            data_marker = "linux-upgrade-state-marker"
            target_tag = os.environ.get("IR_OFFICIAL_UPGRADE_TARGET_TAG", RC10_TAG)
            target_commit = os.environ.get("IR_EXPECTED_RELEASE_COMMIT", RC10_COMMIT)
            require_archive_fixture = os.environ.get("IR_REQUIRE_ARCHIVE_FIXTURE") == "1"
            if target_tag not in (RC10_TAG, "v0.1.0-rc.11"):
                self.fail("official upgrade acceptance only supports the RC10 baseline or the published RC11 tag")
            if target_tag == "v0.1.0-rc.11":
                if not re.fullmatch(r"[0-9a-f]{40}", target_commit):
                    self.fail("RC11 acceptance must pin the exact published release commit")
                if not require_archive_fixture:
                    self.fail("RC11 acceptance must verify a real V2 archive fixture")
            args_rc9 = install.cli_parser().parse_args([
                "--install-dir", str(install_dir), "--recordings-dir", str(archive), "--version", RC9_TAG,
            ])
            args_target = install.cli_parser().parse_args([
                "--install-dir", str(install_dir), "--upgrade", "--version", target_tag,
            ])
            with mock.patch.object(install, "COMPOSE_PROJECT", project), mock.patch.object(install, "STATE_VOLUME", volume):
                try:
                    install.install(args_rc9)
                    meta_rc9 = install.load_metadata(install_dir / "deployment.json")
                    self.assertIsNotNone(meta_rc9)
                    self.assertEqual(meta_rc9["release"]["tag"], RC9_TAG)
                    self.assertEqual(meta_rc9["release"]["commit"], RC9_COMMIT)
                    container = compose_container(install_dir)
                    rc9 = assert_running_identity(self, container, RC9_TAG, RC9_COMMIT)
                    setup = install.get_setup_status(meta_rc9["port"], meta_rc9["bind"])
                    self.assertEqual(setup["version"], RC9_TAG[1:])
                    self.assertFalse(setup["auth_disabled"])

                    inspect = json.loads(docker(["inspect", container]).stdout)[0]
                    mounts = {entry["Destination"]: entry for entry in inspect.get("Mounts", [])}
                    self.assertEqual(mounts["/data"]["Type"], "volume")
                    self.assertEqual(mounts["/data"]["Name"], volume)
                    self.assertEqual(mounts["/data/recordings"]["Type"], "bind")
                    self.assertEqual(Path(mounts["/data/recordings"]["Source"]).resolve(), archive.resolve())
                    docker([
                        "exec", "--user", f"{install.CONTAINER_UID}:{install.CONTAINER_GID}", container,
                        "/bin/sh", "-ec", f"printf archive-preserved > /data/recordings/{archive_marker}",
                    ])
                    state_marker(container, data_marker)

                    # The active generation must have a valid provider set and the
                    # immutable RC9 source image has been verified by install.py.
                    storage_set = rc9["generation"]["storage_provider_set_id"]
                    assert_storage_pin(self, container, storage_set)
                    old_tuple = (
                        rc9["generation"].get("adapter_set_id"),
                        rc9["generation"].get("storage_provider_set_id"),
                        rc9["generation"].get("storage_instance_id"),
                    )
                    self.assertTrue(all(old_tuple))
                    if require_archive_fixture:
                        run_upgrade_archive_fixture(self, container, "create", storage_set)
                    release_dirs_before = release_directories(container)
                    print(f"RC9 baseline healthy: generation={rc9['registry']['active_generation_id']} storage_set={storage_set}")

                    # Inject an installer verification failure after the new
                    # image has started. The Runtime Host may already have
                    # durably activated the image-bundled generation by this
                    # point. In that case the installer must keep the new image
                    # and pending metadata for forward recovery; restoring RC9
                    # over the newer registry would be unsafe.
                    original_wait_for_runtime = install.wait_for_runtime
                    rejected_candidate_readiness = False
                    candidate_startup_diagnostic = "stage=not_reported code=not_reported"

                    def reject_candidate_once(*args, **kwargs):
                        nonlocal rejected_candidate_readiness
                        if not rejected_candidate_readiness:
                            rejected_candidate_readiness = True
                            raise install.InstallError("acceptance injected readiness rejection")
                        return original_wait_for_runtime(*args, **kwargs)

                    original_run = install.run

                    def capture_candidate_startup_failure(command, *args, **kwargs):
                        nonlocal candidate_startup_diagnostic
                        argv = [str(item) for item in command]
                        is_candidate_up = str(install_dir / "compose.pending.yaml") in argv and "up" in argv
                        try:
                            return original_run(command, *args, **kwargs)
                        except install.InstallError:
                            if is_candidate_up:
                                candidate_compose = install_dir / "compose.pending.yaml"
                                logs = original_run(install.compose_command(
                                    candidate_compose, "logs", "--no-color", "--tail", "100", "archiver",
                                ), check=False)
                                ps = original_run(install.compose_command(candidate_compose, "ps", "-q", "archiver"), check=False)
                                container_id = (ps.stdout or "").strip()
                                runtime_state = "unknown"
                                health_state = "unknown"
                                health_exit = "unknown"
                                container_logs = ""
                                if container_id:
                                    inspected = original_run(["docker", "inspect", container_id], check=False)
                                    try:
                                        details = json.loads(inspected.stdout)[0]
                                        runtime_state = str(details.get("State", {}).get("Status", "unknown"))
                                        health = details.get("State", {}).get("Health") or {}
                                        health_state = str(health.get("Status", "unknown"))
                                        health_log = health.get("Log") or []
                                        if health_log:
                                            health_exit = str(health_log[-1].get("ExitCode", "unknown"))
                                    except (json.JSONDecodeError, IndexError, TypeError):
                                        pass
                                    app_logs = original_run(["docker", "logs", "--tail", "100", container_id], check=False)
                                    container_logs = (app_logs.stdout or "") + (app_logs.stderr or "")
                                combined = (logs.stdout or "") + (logs.stderr or "") + container_logs
                                matches = re.findall(r"stage=([a-z][a-z0-9_]{0,63}) code=([a-z][a-z0-9_]{0,63})", combined)
                                if matches:
                                    stage, code = matches[-1]
                                    candidate_startup_diagnostic = f"stage={stage} code={code}"
                                else:
                                    candidate_startup_diagnostic = (
                                        "stage=not_reported code=not_reported startup_class="
                                        + safe_startup_failure_class(combined)
                                    )
                                if container_id:
                                    try:
                                        cloned = diagnose_rc9_state_with_diagnostic_image(
                                            root, volume, archive, container_id,
                                        )
                                        candidate_startup_diagnostic += f"; cloned_state_probe={cloned}"
                                    except Exception:
                                        # Keep failure output bounded and do not
                                        # leak paths or raw process diagnostics.
                                        candidate_startup_diagnostic += "; cloned_state_probe=failed"
                                print(
                                    f"{target_tag} candidate Compose startup failed; container={runtime_state} "
                                    f"health={health_state} health_exit={health_exit} {candidate_startup_diagnostic}"
                                )
                            raise

                    candidate_error = None
                    with mock.patch.object(install, "run", side_effect=capture_candidate_startup_failure):
                        with mock.patch.object(install, "wait_for_runtime", side_effect=reject_candidate_once):
                            try:
                                install.install(args_target)
                            except install.InstallError as exc:
                                candidate_error = exc
                    if candidate_error is None:
                        self.fail("acceptance readiness rejection was not reached")
                    candidate_failed_before_readiness = "acceptance injected readiness rejection" not in str(candidate_error)
                    pending_path = install_dir / "deployment.pending.json"
                    pending = install.read_pending(pending_path)
                    self.assertIsNotNone(pending)
                    self.assertEqual(pending["release"]["tag"], target_tag)
                    self.assertTrue(pending.get("compose_started"))
                    self.assertTrue((install_dir / "compose.pending.yaml").is_file())

                    if candidate_failed_before_readiness:
                        rollback_container = compose_container(install_dir)
                        rolled_back = assert_running_identity(self, rollback_container, RC9_TAG, RC9_COMMIT)
                        self.assertEqual(rolled_back["registry"]["active_generation_id"], rc9["registry"]["active_generation_id"])
                        self.assertEqual(install.wait_for_runtime(meta_rc9["port"], meta_rc9["release"], timeout=30, bind=meta_rc9["bind"])["version"], RC9_TAG[1:])
                        self.assertEqual(docker([
                            "exec", "--user", f"{install.CONTAINER_UID}:{install.CONTAINER_GID}", rollback_container,
                            "/bin/sh", "-ec", f"test \"$(cat /data/{data_marker})\" = preserved && test \"$(cat /data/recordings/{archive_marker})\" = archive-preserved",
                        ]).returncode, 0)
                        rollback_release_dirs = release_directories(rollback_container)
                        self.assertTrue(release_dirs_before.issubset(rollback_release_dirs))
                        if require_archive_fixture:
                            run_upgrade_archive_fixture(self, rollback_container, "verify", storage_set)
                        self.fail(
                            f"{target_tag} candidate failed before injected readiness rejection; "
                            "automatic RC9 rollback was healthy and preserved generation, state, "
                            f"archive, and release directories; {candidate_startup_diagnostic}"
                        )

                    pending_container = compose_container(install_dir)
                    pending_active = assert_running_identity(self, pending_container, target_tag, target_commit)
                    self.assertNotEqual(
                        pending_active["registry"]["active_generation_id"],
                        rc9["registry"]["active_generation_id"],
                        "candidate generation was not active when post-start verification failed",
                    )
                    pending_status = install.wait_for_runtime(
                        meta_rc9["port"],
                        {"version": target_tag[1:], "commit": target_commit, "channel": "prerelease"},
                        timeout=30,
                        bind=meta_rc9["bind"],
                    )
                    self.assertEqual(pending_status["version"], target_tag[1:])
                    self.assertEqual(docker([
                        "exec", "--user", f"{install.CONTAINER_UID}:{install.CONTAINER_GID}", pending_container,
                        "/bin/sh", "-ec", f"test \"$(cat /data/{data_marker})\" = preserved && test \"$(cat /data/recordings/{archive_marker})\" = archive-preserved",
                    ]).returncode, 0)
                    self.assertTrue(release_dirs_before.issubset(release_directories(pending_container)))
                    pending_tuple = (
                        pending_active["generation"].get("adapter_set_id"),
                        pending_active["generation"].get("storage_provider_set_id"),
                        pending_active["generation"].get("storage_instance_id"),
                    )
                    self.assertEqual(pending_tuple, old_tuple)
                    if require_archive_fixture:
                        run_upgrade_archive_fixture(self, pending_container, "verify", storage_set)

                    # Official retry resumes the exact pending release identity;
                    # it neither changes application data nor creates a second
                    # generation after the candidate is already active.
                    pending_generation_id = pending_active["registry"]["active_generation_id"]
                    install.install(args_target)
                    final_metadata = install.load_metadata(install_dir / "deployment.json")
                    self.assertIsNone(install.read_pending(pending_path))
                    self.assertEqual(final_metadata["release"]["tag"], target_tag)
                    self.assertEqual(final_metadata["release"]["commit"], target_commit)
                    self.assertEqual(final_metadata["release"]["channel"], "prerelease")
                    self.assertTrue(final_metadata["release"].get("manifest_identity_checked"))
                    final_container = compose_container(install_dir)
                    upgraded = assert_running_identity(self, final_container, target_tag, target_commit)
                    self.assertEqual(
                        upgraded["registry"]["active_generation_id"],
                        pending_generation_id,
                        "forward recovery replaced an already active generation",
                    )
                    target_status = install.wait_for_runtime(
                        final_metadata["port"], final_metadata["release"], timeout=30, bind=final_metadata["bind"],
                    )
                    self.assertEqual(target_status["version"], target_tag[1:])
                    self.assertFalse(target_status["auth_disabled"])
                    self.assertEqual(docker([
                        "exec", "--user", f"{install.CONTAINER_UID}:{install.CONTAINER_GID}", final_container,
                        "/bin/sh", "-ec", f"test \"$(cat /data/{data_marker})\" = preserved && test \"$(cat /data/recordings/{archive_marker})\" = archive-preserved",
                    ]).returncode, 0)
                    old_generation = upgraded["registry"]["generations"].get(rc9["registry"]["active_generation_id"])
                    if old_generation is not None:
                        self.assertEqual(old_generation["commit"], RC9_COMMIT)
                    active_tuple = (
                        upgraded["generation"].get("adapter_set_id"),
                        upgraded["generation"].get("storage_provider_set_id"),
                        upgraded["generation"].get("storage_instance_id"),
                    )
                    self.assertEqual(active_tuple, old_tuple, "application image upgrade changed immutable adapter/storage pins")
                    active_set = upgraded["generation"]["storage_provider_set_id"]
                    assert_storage_pin(self, final_container, active_set)
                    release_dirs_after = release_directories(final_container)
                    self.assertTrue(release_dirs_before.issubset(release_dirs_after))
                    if require_archive_fixture:
                        run_upgrade_archive_fixture(self, final_container, "verify", active_set)

                    # Container replacement must not stage another generation
                    # or alter provider pins; archive objects remain readable.
                    install.run(install.compose_command(
                        install_dir / "compose.yaml", "up", "-d", "--no-build", "--force-recreate",
                        "--wait", "--wait-timeout", "180", "archiver",
                    ))
                    restarted_container = compose_container(install_dir)
                    restarted = assert_running_identity(self, restarted_container, target_tag, target_commit)
                    self.assertEqual(
                        restarted["registry"]["active_generation_id"],
                        upgraded["registry"]["active_generation_id"],
                        "container recreation unexpectedly created another application generation",
                    )
                    self.assertEqual(
                        (
                            restarted["generation"].get("adapter_set_id"),
                            restarted["generation"].get("storage_provider_set_id"),
                            restarted["generation"].get("storage_instance_id"),
                        ),
                        old_tuple,
                        "container recreation changed immutable provider pins",
                    )
                    assert_storage_pin(self, restarted_container, active_set)
                    if require_archive_fixture:
                        run_upgrade_archive_fixture(self, restarted_container, "verify", active_set)
                    print(
                        f"{target_tag} upgrade healthy: runtime-host={target_tag[1:]}/{target_commit}; "
                        f"generation={restarted['registry']['active_generation_id']} storage_set={active_set}; "
                        f"archive_fixture={require_archive_fixture}"
                    )
                finally:
                    for compose_path in (install_dir / "compose.yaml", install_dir / "compose.pending.yaml"):
                        if compose_path.exists():
                            install.run(install.compose_command(compose_path, "down", "--remove-orphans"), check=False)
                    volume_info = docker(["volume", "inspect", "--format={{.Name}}", volume], check=False)
                    if volume_info.returncode == 0 and volume_info.stdout.strip() == volume:
                        docker(["volume", "rm", volume])
                    # The installer correctly gives a newly created archive
                    # directory to UID 10001. Restore only this temporary test
                    # directory so TemporaryDirectory can remove its own files.
                    if archive.exists():
                        cleanup = docker([
                            "run", "--rm", "--user", "0:0", "--entrypoint", "/bin/sh",
                            "--volume", f"{root}:/acceptance", "alpine:3.21", "-ec",
                            f"chown -R {os.getuid()}:{os.getgid()} '/acceptance/{archive.name}' && chmod -R u+rwX '/acceptance/{archive.name}'",
                        ], check=False)
                        if cleanup.returncode != 0:
                            raise AssertionError("could not restore ownership of the isolated archive test directory")


class CandidateImageBundleUpgradeDockerTests(unittest.TestCase):
    """Exercise RC9 -> source-built diagnostic image with the real installer."""

    @unittest.skipUnless(
        os.environ.get("IR_RUN_BUNDLE_UPGRADE_DOCKER_E2E") == "1",
        "requires an isolated Linux Docker runner; set IR_RUN_BUNDLE_UPGRADE_DOCKER_E2E=1",
    )
    def test_rc9_image_bundle_migrates_to_candidate_and_resumes_after_verification_retry(self):
        if shutil.which("docker") is None:
            self.fail("Docker candidate upgrade acceptance was requested but Docker is unavailable")
        candidate_commit = subprocess.run(
            ["git", "rev-parse", "HEAD"], check=True, text=True, stdout=subprocess.PIPE,
        ).stdout.strip()
        candidate_time = subprocess.run(
            ["git", "show", "-s", "--format=%cI", candidate_commit], check=True, text=True, stdout=subprocess.PIPE,
        ).stdout.strip()
        if not re.fullmatch(r"[0-9a-f]{40}", candidate_commit):
            self.fail("candidate image commit identity is invalid")
        version = "0.1.0-rc.10-diagnostic." + candidate_commit[:12]
        tag = "v" + version
        selected = {"tag_name": tag, "prerelease": True, "draft": False}
        identity = {
            "channel": "prerelease",
            "manifest_identity_checked": False,
            "manifest_signature_independently_verified": False,
        }
        release_identity = {
            "tag": tag,
            "source": install.REPOSITORY_SOURCE,
            "version": version,
            "commit": candidate_commit,
            "build_time": candidate_time,
            "channel": "prerelease",
            "architecture_manifest": "amd64",
            "manifest_identity_checked": False,
            "manifest_signature_independently_verified": False,
        }
        project = "ir-bundle-upgrade-" + uuid.uuid4().hex[:12]
        volume = project + "-state"
        with tempfile.TemporaryDirectory(prefix="ir bundle upgrade acceptance ") as temp:
            root = Path(temp)
            install_dir = root / "deployment with spaces"
            archive = root / "recordings with spaces"
            archive_marker = "candidate-archive-marker"
            state_marker = "candidate-state-marker"
            args_rc9 = install.cli_parser().parse_args([
                "--install-dir", str(install_dir), "--recordings-dir", str(archive), "--version", RC9_TAG,
            ])
            args_candidate = install.cli_parser().parse_args([
                "--install-dir", str(install_dir), "--upgrade", "--version", tag,
            ])

            def install_candidate():
                with mock.patch.object(install, "prepare_release", return_value=(selected, candidate_commit, candidate_time, identity)), \
                     mock.patch.object(install, "prepare_source", return_value=(Path.cwd().resolve(), candidate_time)):
                    install.install(args_candidate)

            with mock.patch.object(install, "COMPOSE_PROJECT", project), mock.patch.object(install, "STATE_VOLUME", volume):
                try:
                    # RC9 is installed from the exact published tag via the
                    # unmodified official release selection and source path.
                    install.install(args_rc9)
                    rc9_metadata = install.load_metadata(install_dir / "deployment.json")
                    rc9_container = compose_container(install_dir)
                    rc9 = assert_running_identity(self, rc9_container, RC9_TAG, RC9_COMMIT)
                    old_generation_id = rc9["registry"]["active_generation_id"]
                    old_tuple = (
                        rc9["generation"].get("adapter_set_id"),
                        rc9["generation"].get("storage_provider_set_id"),
                        rc9["generation"].get("storage_instance_id"),
                    )
                    self.assertTrue(all(old_tuple))
                    docker([
                        "exec", "--user", f"{install.CONTAINER_UID}:{install.CONTAINER_GID}", rc9_container,
                        "/bin/sh", "-ec", f"printf state-preserved > /data/{state_marker}; printf archive-preserved > /data/recordings/{archive_marker}",
                    ])
                    old_release_dirs = release_directories(rc9_container)

                    # Seed and verify a format-v2 archive through the real
                    # RC9 storage.local executable. The same immutable set is
                    # then used after the candidate image transition.
                    run_upgrade_archive_fixture(self, rc9_container, "create", old_tuple[1])

                    # Inject a Host startup/provenance failure before candidate
                    # activation. It must restore the known-good RC9 Compose
                    # while retaining the pending candidate for an exact retry.
                    original_run = install.run
                    preactivation_injected = False

                    def reject_before_activation(command, *args, **kwargs):
                        nonlocal preactivation_injected
                        argv = [str(item) for item in command]
                        pending_file = str(install_dir / "compose.pending.yaml")
                        is_candidate_start = pending_file in argv and "up" in argv and "archiver" in argv
                        if is_candidate_start and not preactivation_injected:
                            preactivation_injected = True
                            compose_text = (install_dir / "compose.pending.yaml").read_text(encoding="utf-8")
                            marker = f"IR_IMAGE_BUNDLE_UPGRADE_FROM_COMMIT: {json.dumps(RC9_COMMIT)}"
                            replacement = f"IR_IMAGE_BUNDLE_UPGRADE_FROM_COMMIT: {json.dumps('0' * 40)}"
                            replacements = compose_text.count(marker)
                            if replacements != 1:
                                raise AssertionError("candidate Compose did not contain one exact prior-image commit marker")
                            compose_text = compose_text.replace(marker, replacement, 1)
                            (install_dir / "compose.pending.yaml").write_text(compose_text, encoding="utf-8")
                            for index, value in enumerate(argv[:-1]):
                                if value == "--wait-timeout":
                                    argv[index + 1] = "20"
                                    break
                            return original_run(argv, *args, **kwargs)
                        return original_run(command, *args, **kwargs)

                    preactivation_error = None
                    with mock.patch.object(install, "run", side_effect=reject_before_activation):
                        try:
                            install_candidate()
                        except install.InstallError as exc:
                            preactivation_error = exc
                    self.assertTrue(preactivation_injected, "pre-activation failure injection was not reached")
                    self.assertIsNotNone(preactivation_error, "invalid prior-image provenance unexpectedly started candidate")
                    preactivation_pending = install.read_pending(install_dir / "deployment.pending.json")
                    self.assertIsNotNone(preactivation_pending)
                    self.assertEqual(preactivation_pending["release"]["commit"], candidate_commit)
                    rolled_back_container = compose_container(install_dir)
                    rolled_back = assert_running_identity(self, rolled_back_container, RC9_TAG, RC9_COMMIT)
                    self.assertEqual(rolled_back["registry"]["active_generation_id"], old_generation_id)
                    self.assertEqual(rolled_back["generation"]["storage_provider_set_id"], old_tuple[1])
                    self.assertTrue(old_release_dirs.issubset(release_directories(rolled_back_container)))

                    # The pre-activation rollback preserved canonical archive
                    # content and the pending retry will upgrade the same bytes.
                    run_upgrade_archive_fixture(self, rolled_back_container, "verify", old_tuple[1])

                    # Exercise a later pre-activation boundary as well: let
                    # the candidate Host verify provenance, then make its
                    # bundled Engine exit before readiness. Startup may fail
                    # before a staged generation is durably published. If a
                    # failed candidate is recorded, the restored Host may
                    # retire it when it has no leases; the durable active
                    # pointer and its provider pins must remain unchanged.
                    original_run = install.run
                    staged_failure_injected = False

                    def fail_candidate_engine_before_activation(command, *args, **kwargs):
                        nonlocal staged_failure_injected
                        argv = [str(item) for item in command]
                        pending_file = str(install_dir / "compose.pending.yaml")
                        is_candidate_start = pending_file in argv and "up" in argv and "archiver" in argv
                        if is_candidate_start and not staged_failure_injected:
                            staged_failure_injected = True
                            original_run(install.compose_command(
                                install_dir / "compose.pending.yaml", "create", "--force-recreate", "archiver",
                            ), check=False)
                            container = original_run(install.compose_command(
                                install_dir / "compose.pending.yaml", "ps", "-aq", "archiver",
                            ), check=False).stdout.strip()
                            if not container:
                                raise AssertionError("candidate container was not created for staged failure injection")
                            with tempfile.NamedTemporaryFile("w", encoding="utf-8", delete=False) as engine:
                                engine.write("#!/bin/sh\nexit 42\n")
                                engine_path = Path(engine.name)
                            engine_path.chmod(0o755)
                            try:
                                docker(["cp", str(engine_path), f"{container}:/opt/integrated-recorder/initial/recorder-engine"])
                            finally:
                                engine_path.unlink(missing_ok=True)
                            for index, value in enumerate(argv[:-1]):
                                if value == "--wait-timeout":
                                    argv[index + 1] = "30"
                                    break
                            return original_run(argv, *args, **kwargs)
                        return original_run(command, *args, **kwargs)

                    staged_failure = None
                    with mock.patch.object(install, "run", side_effect=fail_candidate_engine_before_activation):
                        try:
                            install_candidate()
                        except install.InstallError as exc:
                            staged_failure = exc
                    self.assertTrue(staged_failure_injected, "staged Engine failure injection was not reached")
                    self.assertIsNotNone(staged_failure, "candidate Engine failure unexpectedly passed readiness")
                    staged_pending = install.read_pending(install_dir / "deployment.pending.json")
                    self.assertIsNotNone(staged_pending)
                    self.assertEqual(staged_pending["release"]["commit"], candidate_commit)
                    staged_rollback_container = compose_container(install_dir)
                    staged_rollback = assert_running_identity(self, staged_rollback_container, RC9_TAG, RC9_COMMIT)
                    self.assertEqual(staged_rollback["registry"]["active_generation_id"], old_generation_id)
                    self.assertEqual(staged_rollback["registry"].get("staged_generation_id", ""), "")
                    candidate_generations = [
                        generation for generation in staged_rollback["registry"]["generations"].values()
                        if generation.get("version") == version and generation.get("commit", "").lower() == candidate_commit.lower()
                    ]
                    self.assertTrue(
                        all(generation.get("state") in ("failed", "retired") for generation in candidate_generations),
                        "pre-activation candidate unexpectedly became active or remained staged",
                    )
                    candidate_ids = {generation.get("id") for generation in candidate_generations}
                    self.assertFalse(
                        any(
                            lease.get("engine_generation") in candidate_ids
                            for lease in staged_rollback["registry"].get("leases", {}).values()
                        ),
                        "failed pre-activation candidate unexpectedly owns a Recording lease",
                    )
                    self.assertEqual(staged_rollback["generation"]["storage_provider_set_id"], old_tuple[1])
                    run_upgrade_archive_fixture(self, staged_rollback_container, "verify", old_tuple[1])

                    # Fail installer-level acceptance only after the candidate
                    # Host has made its generation activation durable. The
                    # installer must not start RC9 against RC10 registry state.
                    original_wait = install.wait_for_runtime
                    rejected_once = False

                    def reject_once(*args, **kwargs):
                        nonlocal rejected_once
                        if not rejected_once:
                            rejected_once = True
                            raise install.InstallError("acceptance injected post-activation verification rejection")
                        return original_wait(*args, **kwargs)

                    candidate_error = None
                    with mock.patch.object(install, "wait_for_runtime", side_effect=reject_once):
                        try:
                            install_candidate()
                        except install.InstallError as exc:
                            candidate_error = exc
                    self.assertIsNotNone(candidate_error)
                    if "post-activation verification rejection" not in str(candidate_error):
                        # Compose's --wait can fail before the installer's HTTP
                        # readiness probe runs. Preserve sanitized container
                        # evidence before the isolated project is torn down so
                        # a startup/health regression is distinguishable from
                        # the deliberate post-activation retry injection.
                        container = install.run(
                            install.compose_command(install_dir / "compose.pending.yaml", "ps", "-q", "archiver"),
                            check=False,
                        ).stdout.strip()
                        if container:
                            inspect = docker(["inspect", "--format={{json .State}}", container], check=False)
                            logs = docker(["logs", "--tail", "120", container], check=False)
                            print("Candidate container state before retry:", (inspect.stdout or inspect.stderr or "unavailable")[:12000])
                            print("Candidate Runtime Host logs before retry:", ((logs.stdout or "") + (logs.stderr or ""))[-20000:])
                        self.fail(f"candidate did not reach the intended post-activation verification hook: {candidate_error}")
                    pending_path = install_dir / "deployment.pending.json"
                    pending = install.read_pending(pending_path)
                    self.assertIsNotNone(pending)
                    self.assertEqual(pending["release"]["commit"], candidate_commit)
                    self.assertEqual(pending["bundle_upgrade_from"], {"version": RC9_TAG[1:], "commit": RC9_COMMIT})
                    pending_compose = install_dir / "compose.pending.yaml"
                    self.assertIn("IR_IMAGE_BUNDLE_UPGRADE_FROM_VERSION", pending_compose.read_text(encoding="utf-8"))

                    candidate_container = compose_container(install_dir)
                    upgraded = assert_running_identity(self, candidate_container, tag, candidate_commit)
                    run_upgrade_archive_fixture(self, candidate_container, "verify", old_tuple[1])
                    self.assertNotEqual(upgraded["registry"]["active_generation_id"], old_generation_id)
                    old = upgraded["registry"]["generations"].get(old_generation_id)
                    self.assertIsNotNone(old)
                    self.assertEqual(old["version"], RC9_TAG[1:])
                    self.assertEqual(old["commit"], RC9_COMMIT)
                    self.assertEqual(old["state"], "draining")
                    self.assertTrue(old.get("engine_dormant"), "lease-free RC9 source Engine was not marked dormant")
                    self.assertFalse(
                        any(lease.get("engine_generation") == old_generation_id for lease in upgraded["registry"].get("leases", {}).values()),
                        "RC9 source unexpectedly retains a Recording lease",
                    )
                    new_tuple = (
                        upgraded["generation"].get("adapter_set_id"),
                        upgraded["generation"].get("storage_provider_set_id"),
                        upgraded["generation"].get("storage_instance_id"),
                    )
                    self.assertEqual(new_tuple, old_tuple)
                    self.assertTrue(old_release_dirs.issubset(release_directories(candidate_container)))
                    self.assertEqual(docker([
                        "exec", "--user", f"{install.CONTAINER_UID}:{install.CONTAINER_GID}", candidate_container,
                        "/bin/sh", "-ec", f"test \"$(cat /data/{state_marker})\" = state-preserved && test \"$(cat /data/recordings/{archive_marker})\" = archive-preserved",
                    ]).returncode, 0)

                    # Retrying the same pending candidate verifies forward
                    # recovery: it reuses the activated generation, removes the
                    # one-shot legacy hint, and preserves both mounted stores.
                    install_candidate()
                    final_metadata = install.load_metadata(install_dir / "deployment.json")
                    self.assertIsNone(install.read_pending(pending_path))
                    self.assertEqual(final_metadata["release"]["version"], version)
                    final_compose = (install_dir / "compose.yaml").read_text(encoding="utf-8")
                    self.assertNotIn("IR_IMAGE_BUNDLE_UPGRADE_FROM_", final_compose)
                    final_container = compose_container(install_dir)
                    final = assert_running_identity(self, final_container, tag, candidate_commit)
                    self.assertEqual(final["registry"]["active_generation_id"], upgraded["registry"]["active_generation_id"])
                    self.assertEqual(install.wait_for_runtime(final_metadata["port"], final_metadata["release"], timeout=30, bind=final_metadata["bind"])["version"], version)
                    self.assertEqual(docker([
                        "exec", "--user", f"{install.CONTAINER_UID}:{install.CONTAINER_GID}", final_container,
                        "/bin/sh", "-ec", f"printf restart-probe > /data/restart-probe && test \"$(cat /data/{state_marker})\" = state-preserved && test \"$(cat /data/recordings/{archive_marker})\" = archive-preserved",
                    ]).returncode, 0)
                    try:
                        install.run(install.compose_command(install_dir / "compose.yaml", "up", "-d", "--no-build", "--force-recreate", "--wait", "--wait-timeout", "180", "archiver"))
                    except install.InstallError:
                        failed_container = compose_container(install_dir)
                        inspect = docker(["inspect", failed_container]).stdout
                        try:
                            container_state = json.loads(inspect)[0].get("State", {})
                        except (json.JSONDecodeError, IndexError, TypeError):
                            container_state = {}
                        # The Host may be in Docker's restart loop, so do not
                        # depend on docker exec into the failing container.
                        # Read only the isolated named volume from a helper
                        # container and capture Host logs directly.
                        registry_probe = docker([
                            "run", "--rm", "--volume", f"{volume}:/data:ro", "alpine:3.21",
                            "cat", "/data/runtime/state/generations.json",
                        ], check=False)
                        try:
                            failed_registry = json.loads(registry_probe.stdout) if registry_probe.returncode == 0 else {}
                        except json.JSONDecodeError:
                            failed_registry = {}
                        generation_summary = {
                            key: {
                                "version": value.get("version"),
                                "state": value.get("state"),
                                "engine_dormant": value.get("engine_dormant", False),
                            }
                            for key, value in failed_registry.get("generations", {}).items()
                        }
                        failure_logs = docker(["logs", "--tail", "120", failed_container], check=False)
                        combined_logs = (failure_logs.stdout or "") + (failure_logs.stderr or "")
                        matches = re.findall(r"stage=([a-z][a-z0-9_]{0,63}) code=([a-z][a-z0-9_]{0,63})", combined_logs)
                        stage, code = matches[-1] if matches else ("not_reported", "not_reported")
                        print(
                            "Candidate restart failed: "
                            f"container={container_state.get('Status', 'unknown')} "
                            f"health={(container_state.get('Health') or {}).get('Status', 'unknown')} "
                            f"startup_class={safe_startup_failure_class(combined_logs)} "
                            f"stage={stage} code={code} "
                            f"active_generation={failed_registry.get('active_generation_id', 'unknown')} "
                            f"generations={json.dumps(generation_summary, sort_keys=True)} "
                            f"lease_count={len(failed_registry.get('leases', {}))}"
                        )
                        raise
                    restarted = compose_container(install_dir)
                    restarted_status = assert_running_identity(self, restarted, tag, candidate_commit)
                    restarted_registry = restarted_status["registry"]
                    restarted_generation_id = restarted_registry["active_generation_id"]
                    self.assertEqual(restarted_registry.get("staged_generation_id", ""), "")
                    self.assertEqual(restarted_registry.get("activation_previous_generation_id", ""), "")
                    self.assertEqual(restarted_generation_id, final["registry"]["active_generation_id"])
                    self.assertEqual(
                        (
                            restarted_status["generation"].get("adapter_set_id"),
                            restarted_status["generation"].get("storage_provider_set_id"),
                            restarted_status["generation"].get("storage_instance_id"),
                        ),
                        new_tuple,
                        "same-image restart changed the active generation's immutable provider pins",
                    )
                    assert_storage_pin(self, restarted, new_tuple[1])
                    self.assertEqual(install.wait_for_runtime(final_metadata["port"], final_metadata["release"], timeout=30, bind=final_metadata["bind"])["version"], version)

                    # A second recreation must reuse the same active generation
                    # and provider tuple rather than stage repeatedly.
                    install.run(install.compose_command(install_dir / "compose.yaml", "up", "-d", "--no-build", "--force-recreate", "--wait", "--wait-timeout", "180", "archiver"))
                    stable_container = compose_container(install_dir)
                    stable_status = assert_running_identity(self, stable_container, tag, candidate_commit)
                    run_upgrade_archive_fixture(self, stable_container, "verify", old_tuple[1])
                    self.assertEqual(stable_status["registry"]["active_generation_id"], restarted_generation_id)
                    self.assertEqual(stable_status["generation"]["storage_provider_set_id"], restarted_status["generation"]["storage_provider_set_id"])
                    self.assertEqual(stable_status["generation"]["storage_instance_id"], new_tuple[2])
                    assert_storage_pin(self, stable_container, stable_status["generation"]["storage_provider_set_id"])
                    self.assertEqual(docker([
                        "exec", "--user", f"{install.CONTAINER_UID}:{install.CONTAINER_GID}", stable_container,
                        "/bin/sh", "-ec", f"test \"$(cat /data/{state_marker})\" = state-preserved && test \"$(cat /data/recordings/{archive_marker})\" = archive-preserved",
                    ]).returncode, 0)
                    print(
                        "Candidate Docker image upgrade PASS: "
                        f"runtime-host={version}/{candidate_commit}; active-generation={stable_status['registry']['active_generation_id']}; "
                        f"RC9-generation-retained={old_generation_id}; adapter/storage pins preserved across restarts"
                    )
                finally:
                    for compose_path in (install_dir / "compose.yaml", install_dir / "compose.pending.yaml"):
                        if compose_path.exists():
                            install.run(install.compose_command(compose_path, "down", "--remove-orphans"), check=False)
                    volume_info = docker(["volume", "inspect", "--format={{.Name}}", volume], check=False)
                    if volume_info.returncode == 0 and volume_info.stdout.strip() == volume:
                        docker(["volume", "rm", volume])
                    if archive.exists():
                        cleanup = docker([
                            "run", "--rm", "--user", "0:0", "--entrypoint", "/bin/sh",
                            "--volume", f"{root}:/acceptance", "alpine:3.21", "-ec",
                            f"chown -R {os.getuid()}:{os.getgid()} '/acceptance/{archive.name}' && chmod -R u+rwX '/acceptance/{archive.name}'",
                        ], check=False)
                        if cleanup.returncode != 0:
                            raise AssertionError("could not restore ownership of the isolated candidate archive")


class DiagnosticImageLinuxDockerTests(unittest.TestCase):
    """Run normal and deliberately failing provider startup in a Docker image."""

    @unittest.skipUnless(
        os.environ.get("IR_RUN_STORAGE_DIAGNOSTIC_DOCKER_E2E") == "1",
        "requires a fresh Linux Docker runner; set IR_RUN_STORAGE_DIAGNOSTIC_DOCKER_E2E=1",
    )
    def test_diagnostic_image_normal_provider_and_safe_failure_codes(self):
        if shutil.which("docker") is None:
            self.fail("Docker acceptance was requested but Docker is unavailable")
        commit = subprocess.run(["git", "rev-parse", "HEAD"], check=True, text=True, stdout=subprocess.PIPE).stdout.strip()
        created = subprocess.run(["git", "show", "-s", "--format=%cI", commit], check=True, text=True, stdout=subprocess.PIPE).stdout.strip()
        timestamp = dt.datetime.fromisoformat(created.replace("Z", "+00:00"))
        if not commit or len(commit) != 40 or timestamp.tzinfo is None:
            self.fail("diagnostic build identity is incomplete")
        diagnostic_version = "0.1.0-rc.10-diagnostic." + commit[:12]
        image = f"localhost/integrated-recorder-storage-diagnostic:{commit[:12]}"
        release_identity = {
            "tag": "diagnostic-" + commit[:12],
            "version": diagnostic_version,
            "commit": commit,
            "build_time": created,
            "channel": "prerelease",
            "source": install.REPOSITORY_SOURCE,
        }
        docker([
            "build", "--file", "Dockerfile", "--tag", image,
            "--label", f"org.opencontainers.image.source={install.REPOSITORY_SOURCE}",
            "--label", f"org.opencontainers.image.version={diagnostic_version}",
            "--label", f"org.opencontainers.image.revision={commit}",
            "--label", f"org.opencontainers.image.created={created}",
            "--label", "com.integrated-recorder.release-channel=prerelease",
            "--label", f"com.integrated-recorder.release-tag=diagnostic-{commit[:12]}",
            "--build-arg", f"VERSION={diagnostic_version}",
            "--build-arg", f"COMMIT={commit}",
            "--build-arg", f"BUILD_TIME={created}",
            "--build-arg", "RELEASE_CHANNEL=prerelease", ".",
        ])
        install.verify_image_labels(image, release_identity)
        install.verify_image_binaries(image, release_identity)
        image_digest = docker(["image", "inspect", "--format={{index .RepoDigests 0}}", image], check=False).stdout.strip()
        print(f"Diagnostic image identity: version={diagnostic_version} commit={commit} digest={image_digest or 'local image ID only'}")

        with tempfile.TemporaryDirectory(prefix="ir storage diagnostic ", ignore_cleanup_errors=True) as temp:
            root = Path(temp)
            normal_data, normal_archive = root / "normal-data", root / "normal-archive"
            normal_data.mkdir()
            normal_archive.mkdir()
            docker(["run", "--rm", "--user", "0:0", "--entrypoint", "/bin/sh", "--volume", f"{normal_data}:/data", "--volume", f"{normal_archive}:/data/recordings", image, "-ec", "chown -R 10001:10001 /data"])
            normal = docker([
                "run", "-d", "--name", "ir-storage-diagnostic-normal-" + uuid.uuid4().hex[:8],
                "--volume", f"{normal_data}:/data", "--volume", f"{normal_archive}:/data/recordings", image,
            ]).stdout.strip()
            try:
                health_deadline = time.monotonic() + 240
                while time.monotonic() < health_deadline:
                    state = docker(["inspect", "--format={{.State.Status}}", normal], check=False).stdout.strip()
                    if state != "running":
                        self.fail("diagnostic Runtime Host container stopped before readiness")
                    response = docker([
                        "exec", normal, "/bin/sh", "-ec", "wget -q -O- http://127.0.0.1:8080/healthz",
                    ], check=False)
                    if response.returncode == 0:
                        try:
                            health = json.loads(response.stdout)
                        except json.JSONDecodeError:
                            health = None
                        if isinstance(health, dict) and health.get("status") == "ok":
                            break
                    time.sleep(2)
                else:
                    self.fail("diagnostic Runtime Host did not pass /healthz readiness")
                if not isinstance(health, dict) or health.get("status") != "ok":
                    self.fail("diagnostic Runtime Host returned an invalid health response")
                _, generation = active_generation(normal)
                self.assertEqual(generation["version"], diagnostic_version)
                self.assertEqual(generation["commit"], commit)
                self.assertTrue(generation.get("storage_provider_set_id"))
                catalog = json.loads(docker(["exec", normal, "cat", "/data/runtime/storage-providers/state.json"]).stdout)
                self.assertTrue(catalog.get("installed"))
                print("Diagnostic normal startup: /healthz ok; bundled import, staged probe, catalog publication, provider startup and physical probe completed")
            finally:
                docker(["rm", "--force", normal], check=False)

            # Non-executable source: expect a stage/code, not raw path/error.
            nonexec = root / "non-executable-storage.local"
            nonexec.write_bytes(b"not an executable provider")
            os.chmod(nonexec, 0o644)
            nonexec_data, nonexec_archive = root / "nonexec-data", root / "nonexec-archive"
            nonexec_data.mkdir()
            nonexec_archive.mkdir()
            docker(["run", "--rm", "--user", "0:0", "--entrypoint", "/bin/sh", "--volume", f"{nonexec_data}:/data", "--volume", f"{nonexec_archive}:/data/recordings", image,
                    "-ec", "chown -R 10001:10001 /data"])
            result = docker([
                "run", "--rm", "--volume", f"{nonexec_data}:/data", "--volume", f"{nonexec_archive}:/data/recordings",
                "--volume", f"{nonexec}:/diagnostic/storage.local:ro", "--env", "IR_STORAGE_LOCAL_PLUGIN=/diagnostic/storage.local", image,
            ], check=False)
            combined = (result.stdout or "") + (result.stderr or "")
            self.assertNotEqual(result.returncode, 0, "non-executable provider unexpectedly passed validation")
            if "stage=bundled_executable_probe code=executable_invalid" not in combined:
                self.fail("non-executable failure did not report the expected safe stage/code; captured output withheld")
            if str(nonexec) in combined:
                self.fail("non-executable diagnostic exposed its source path")
            print("Diagnostic non-executable failure: stage=bundled_executable_probe code=executable_invalid; source path withheld")

            # Child stderr can contain arbitrary sensitive bytes. Only a fixed
            # category is allowed into Runtime Host startup diagnostics.
            child = root / "failing-storage.local"
            child.write_text("#!/bin/sh\nprintf '%s\\n' 'operation not permitted /private/path PRIVATE_SENTINEL' >&2\nexit 1\n", encoding="utf-8")
            os.chmod(child, 0o755)
            child_data, child_archive = root / "child-data", root / "child-archive"
            child_data.mkdir()
            child_archive.mkdir()
            docker(["run", "--rm", "--user", "0:0", "--entrypoint", "/bin/sh", "--volume", f"{child_data}:/data", "--volume", f"{child_archive}:/data/recordings", image,
                    "-ec", "chown -R 10001:10001 /data"])
            result = docker([
                "run", "--rm", "--volume", f"{child_data}:/data", "--volume", f"{child_archive}:/data/recordings",
                "--volume", f"{child}:/diagnostic/storage.local:ro", "--env", "IR_STORAGE_LOCAL_PLUGIN=/diagnostic/storage.local", image,
            ], check=False)
            combined = (result.stdout or "") + (result.stderr or "")
            self.assertNotEqual(result.returncode, 0, "failing provider unexpectedly passed validation")
            if "PRIVATE_SENTINEL" in combined or "/private/path" in combined:
                self.fail("Runtime Host leaked bounded child stderr or a child-supplied path")
            if "stage=bundled_executable_probe code=child_exit_permission_denied" not in combined:
                self.fail("child exit did not report the expected safe stage/code; captured output withheld")
            print("Diagnostic child failure: stage=bundled_executable_probe code=child_exit_permission_denied; stderr redacted")

            # Physical permission failure is distinct from import: preserve an
            # already valid bundled artifact, then make only its private marker
            # non-writable for UID 10001 inside the isolated archive.
            physical_data, physical_archive = root / "physical-data", root / "physical-archive"
            physical_data.mkdir()
            physical_archive.mkdir()
            docker([
                "run", "--rm", "--user", "0:0", "--entrypoint", "/bin/sh", "--volume", f"{physical_data}:/data", "--volume", f"{physical_archive}:/data/recordings", image,
                "-ec", "marker=$(printf '\\037').ir-storage-local; mkdir -p \"/data/recordings/$marker/other\"; chown -R 10001:10001 /data; chown -R 0:0 \"/data/recordings/$marker\"; chmod 0555 \"/data/recordings/$marker\" \"/data/recordings/$marker/other\"",
            ])
            physical = docker([
                "run", "--rm", "--volume", f"{physical_data}:/data", "--volume", f"{physical_archive}:/data/recordings", image,
            ], check=False)
            combined = (physical.stdout or "") + (physical.stderr or "")
            self.assertNotEqual(physical.returncode, 0, "non-writable physical marker unexpectedly passed the probe")
            if "stage=physical_storage_probe code=write" not in combined:
                self.fail("physical write failure did not report the expected safe stage/code; captured output withheld")
            if str(physical_archive) in combined:
                self.fail("physical probe diagnostic exposed the archive path")
            print("Diagnostic physical probe failure: stage=physical_storage_probe code=write; archive path withheld")

            # Restore the host user's ownership and write permissions only
            # within this one temporary root; the image correctly owns its
            # dedicated bind mounts as UID 10001 while the tests run.
            docker([
                "run", "--rm", "--user", "0:0", "--entrypoint", "/bin/sh",
                "--volume", f"{root}:/acceptance", image, "-ec",
                f"chown -R {os.getuid()}:{os.getgid()} /acceptance && chmod -R u+rwX /acceptance",
            ])


if __name__ == "__main__":
    unittest.main(verbosity=2)
