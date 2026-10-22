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
import json
import os
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


class OfficialRC9RC10DockerUpgradeTests(unittest.TestCase):
    """Exercise installer build, pending retry, rollback, and successful upgrade."""

    @unittest.skipUnless(
        os.environ.get("IR_RUN_STORAGE_UPGRADE_DOCKER_E2E") == "1",
        "requires a fresh Linux Docker runner; set IR_RUN_STORAGE_UPGRADE_DOCKER_E2E=1",
    )
    def test_official_rc9_install_rc10_upgrade_and_pending_rollback_retry(self):
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
            args_rc9 = install.cli_parser().parse_args([
                "--install-dir", str(install_dir), "--recordings-dir", str(archive), "--version", RC9_TAG,
            ])
            args_rc10 = install.cli_parser().parse_args([
                "--install-dir", str(install_dir), "--upgrade", "--version", RC10_TAG,
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
                    release_dirs_before = release_directories(container)
                    print(f"RC9 baseline healthy: generation={rc9['registry']['active_generation_id']} storage_set={storage_set}")

                    # Inject failure after the candidate container has passed
                    # Compose health but before installer readiness accepts it.
                    # This exercises the real installer rollback/pending path.
                    original_wait_for_runtime = install.wait_for_runtime
                    rejected_candidate_readiness = False

                    def reject_candidate_once(*args, **kwargs):
                        nonlocal rejected_candidate_readiness
                        if not rejected_candidate_readiness:
                            rejected_candidate_readiness = True
                            raise install.InstallError("acceptance injected readiness rejection")
                        return original_wait_for_runtime(*args, **kwargs)

                    with mock.patch.object(install, "wait_for_runtime", side_effect=reject_candidate_once):
                        with self.assertRaisesRegex(install.InstallError, "acceptance injected"):
                            install.install(args_rc10)
                    pending_path = install_dir / "deployment.pending.json"
                    pending = install.read_pending(pending_path)
                    self.assertIsNotNone(pending)
                    self.assertEqual(pending["release"]["tag"], RC10_TAG)
                    self.assertTrue(pending.get("compose_started"))
                    self.assertTrue((install_dir / "compose.pending.yaml").is_file())

                    rollback_container = compose_container(install_dir)
                    rolled_back = assert_running_identity(self, rollback_container, RC9_TAG, RC9_COMMIT)
                    self.assertEqual(rolled_back["registry"]["active_generation_id"], rc9["registry"]["active_generation_id"])
                    self.assertEqual(install.wait_for_runtime(meta_rc9["port"], meta_rc9["release"], timeout=30, bind=meta_rc9["bind"])["version"], RC9_TAG[1:])
                    self.assertEqual(docker([
                        "exec", "--user", f"{install.CONTAINER_UID}:{install.CONTAINER_GID}", rollback_container,
                        "/bin/sh", "-ec", f"test \"$(cat /data/{data_marker})\" = preserved && test \"$(cat /data/recordings/{archive_marker})\" = archive-preserved",
                    ]).returncode, 0)

                    # Official retry resumes the exact pending RC10 identity;
                    # it neither changes nor replaces the application data.
                    install.install(args_rc10)
                    final_metadata = install.load_metadata(install_dir / "deployment.json")
                    self.assertIsNone(install.read_pending(pending_path))
                    self.assertEqual(final_metadata["release"]["tag"], RC10_TAG)
                    self.assertEqual(final_metadata["release"]["commit"], RC10_COMMIT)
                    final_container = compose_container(install_dir)
                    rc10 = assert_running_identity(self, final_container, RC10_TAG, RC10_COMMIT)
                    self.assertEqual(install.wait_for_runtime(final_metadata["port"], final_metadata["release"], timeout=30, bind=final_metadata["bind"])["version"], RC10_TAG[1:])
                    self.assertEqual(docker([
                        "exec", "--user", f"{install.CONTAINER_UID}:{install.CONTAINER_GID}", final_container,
                        "/bin/sh", "-ec", f"test \"$(cat /data/{data_marker})\" = preserved && test \"$(cat /data/recordings/{archive_marker})\" = archive-preserved",
                    ]).returncode, 0)
                    old_generation = rc10["registry"]["generations"].get(rc9["registry"]["active_generation_id"])
                    if old_generation is not None:
                        self.assertEqual(old_generation["commit"], RC9_COMMIT)
                    active_set = rc10["generation"]["storage_provider_set_id"]
                    assert_storage_pin(self, final_container, active_set)
                    release_dirs_after = release_directories(final_container)
                    self.assertTrue(release_dirs_before.issubset(release_dirs_after))
                    print(f"RC10 upgrade healthy: generation={rc10['registry']['active_generation_id']} storage_set={active_set}")
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
