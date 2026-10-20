import json
import ipaddress
import os
import shutil
import socket
import subprocess
import tempfile
import unittest
import uuid
from pathlib import Path
from unittest import mock

import install


COMMIT = "0547bd128ec309d281100bc1f3a45f35c233b00b"
BUILD_TIME_API = "2025-04-05T06:07:08Z"
BUILD_TIME_GIT = "2025-04-05T06:07:08+00:00"


def release(tag="v1.2.3", prerelease=False, published="2025-01-01T00:00:00Z", draft=False):
    return {
        "tag_name": tag,
        "prerelease": prerelease,
        "draft": draft,
        "published_at": published,
        "assets": [],
    }


def manifest(tag="v1.2.3", commit=COMMIT, channel="stable", architecture="amd64", build_time=BUILD_TIME_API):
    return {
        "manifest_schema_version": 1,
        "release_version": tag[1:],
        "commit": commit,
        "build_time": build_time,
        "channel": channel,
        "key_id": "official-release-2026",
        "platform": "linux",
        "architecture": architecture,
        "artifacts": [
            {
                "role": role,
                "filename": f"{role}-linux-{architecture}",
                "size": 123,
                "sha256": "a" * 64,
            }
            for role in install.MANIFEST_ROLES
        ],
    }


def deployment_config(recordings_dir="/mnt/archive with spaces/recordings", version="1.2.3", channel="stable"):
    return {
        "release": {
            "tag": "v" + version,
            "source": install.REPOSITORY_SOURCE,
            "version": version,
            "commit": COMMIT,
            "build_time": BUILD_TIME_GIT,
            "channel": channel,
        },
        "source_dir": "/tmp/integrated recorder/source",
        "recordings_dir": recordings_dir,
        "architecture": "amd64",
        "port": 18080,
        "image": "localhost/integrated-recorder-core:" + COMMIT,
    }


class ReleaseSelectionTests(unittest.TestCase):
    def test_latest_stable_wins_over_newer_prerelease_and_draft_is_excluded(self):
        releases = [
            release("v2.0.0-rc.1", True, "2025-03-01T00:00:00Z"),
            release("v1.9.0", False, "2025-02-01T00:00:00Z"),
            release("v3.0.0", False, "2025-04-01T00:00:00Z", draft=True),
            release("v1.8.0", False, "2025-01-01T00:00:00Z"),
        ]
        self.assertEqual(install.select_release(releases)["tag_name"], "v1.9.0")

    def test_latest_published_prerelease_is_fallback_when_no_stable_exists(self):
        releases = [
            release("v0.1.0-rc.7", True, "2025-02-01T00:00:00Z"),
            release("v0.1.0-rc.8", True, "2025-03-01T00:00:00Z"),
            release("v0.1.0-rc.9", True, "2025-04-01T00:00:00Z", draft=True),
        ]
        selected = install.select_release(releases)
        self.assertEqual(selected["tag_name"], "v0.1.0-rc.8")
        self.assertEqual(install.channel_for_tag(selected["tag_name"], selected["prerelease"]), "prerelease")

    def test_explicit_published_tag_is_pinned_and_drafts_are_not_installable(self):
        releases = [release("v0.1.0-rc.8", True)]
        self.assertEqual(install.select_release(releases, "v0.1.0-rc.8"), releases[0])
        with self.assertRaises(install.InstallError):
            install.select_release([release("v0.1.0-rc.8", True, draft=True)], "v0.1.0-rc.8")

    def test_existing_install_is_pinned_unless_upgrade_is_explicit(self):
        pinned = {"tag": "v1.0.0", "commit": COMMIT, "channel": "stable"}
        self.assertEqual(install.pinned_release_decision(pinned, None, None, False), ("preserve-current", pinned))
        with self.assertRaises(install.InstallError):
            install.pinned_release_decision(pinned, None, "v2.0.0", False)
        self.assertEqual(install.pinned_release_decision(pinned, None, None, True), ("select-upgrade", None))

    def test_interrupted_upgrade_is_not_resumed_without_explicit_upgrade(self):
        current = {"tag": "v1.0.0", "commit": COMMIT, "channel": "stable"}
        pending = {"tag": "v2.0.0", "commit": "b" * 40, "channel": "stable"}
        with self.assertRaisesRegex(install.InstallError, "pass --upgrade to resume"):
            install.pinned_release_decision(current, pending, None, False)
        self.assertEqual(
            install.pinned_release_decision(current, pending, "v2.0.0", True),
            ("resume-pending", pending),
        )

    def test_tag_commit_channel_architecture_roles_and_timestamp_are_checked(self):
        selected = release("v0.1.0-rc.8", True)
        candidate = manifest("v0.1.0-rc.8", COMMIT, "prerelease", "arm64")
        data = json.dumps(candidate).encode()
        checked = install.validate_manifest(data, selected, COMMIT, "arm64", BUILD_TIME_GIT)
        self.assertEqual(checked["architecture"], "arm64")
        for bad in (
            manifest("v0.1.0-rc.8", "b" * 40, "prerelease", "arm64"),
            manifest("v0.1.0-rc.8", COMMIT, "stable", "arm64"),
            manifest("v0.1.0-rc.8", COMMIT, "prerelease", "amd64"),
            manifest("v0.1.0-rc.8", COMMIT, "prerelease", "arm64", "2025-04-05T06:07:09Z"),
        ):
            with self.assertRaises(install.InstallError):
                install.validate_manifest(json.dumps(bad).encode(), selected, COMMIT, "arm64", BUILD_TIME_GIT)

    def test_manifest_schema_and_signing_key_id_are_validated(self):
        selected = release("v0.1.0-rc.8", True)
        for bad in (
            dict(manifest("v0.1.0-rc.8", COMMIT, "prerelease"), manifest_schema_version=2),
            dict(manifest("v0.1.0-rc.8", COMMIT, "prerelease"), key_id="../untrusted"),
        ):
            with self.assertRaises(install.InstallError):
                install.validate_manifest(json.dumps(bad).encode(), selected, COMMIT, "amd64", BUILD_TIME_GIT)

    def test_compose_injects_release_labels_for_rc8_dockerfile_without_final_labels(self):
        config = deployment_config(version="0.1.0-rc.8", channel="prerelease")
        config["release"].update(tag="v0.1.0-rc.8", build_time=BUILD_TIME_GIT)
        config.update(bind="127.0.0.1", tls=None)
        # RC8's final stage ended at ENTRYPOINT and had no LABEL declarations.
        rc8_final_stage = "FROM alpine:3.21\nUSER 10001:10001\nENTRYPOINT [\"/usr/local/bin/runtime-host\"]\n"
        self.assertNotIn("LABEL", rc8_final_stage)
        rendered = install.render_compose(config)
        self.assertIn("      labels:\n        org.opencontainers.image.source:", rendered)
        for value in ("0.1.0-rc.8", COMMIT, BUILD_TIME_GIT, "prerelease"):
            self.assertIn(f'"{value}"', rendered)
        for argument in ("VERSION", "COMMIT", "BUILD_TIME", "RELEASE_CHANNEL"):
            self.assertIn(f"        {argument}:", rendered)

    def test_direct_tls_ports_are_mapped_to_runtime_host_without_proxy_container(self):
        config = deployment_config()
        config.update(bind="0.0.0.0", https_port=18443)
        rendered = install.render_compose(config)
        self.assertIn('host_ip: "0.0.0.0"', rendered)
        self.assertIn('target: 8443\n        published: "18443"', rendered)
        self.assertIn('IR_HTTPS_LISTEN_ADDR: ":8443"', rendered)
        self.assertIn('IR_HTTPS_PUBLIC_PORT: "18443"', rendered)
        self.assertIn('IR_HTTP_PUBLIC_PORT: "18080"', rendered)
        self.assertNotIn("caddy:", rendered.lower())
        self.assertNotIn("COOKIE_SECURE: \"1\"", rendered)

    def test_network_options_are_persisted_and_existing_defaults_are_preserved(self):
        parser = install.cli_parser()
        with tempfile.TemporaryDirectory() as root:
            args = parser.parse_args(["--recordings-dir", "/tmp/records", "--bind", "0.0.0.0"])
            with mock.patch.object(install, "bind_port_available", return_value=True):
                bind, tls_port = install.network_settings(args, None, Path(root))
            self.assertEqual(bind, "0.0.0.0")
            self.assertTrue(1 <= tls_port <= 65535)
            args = parser.parse_args(["--recordings-dir", "/tmp/records"])
            bind, tls_port = install.network_settings(args, {"bind": "192.0.2.10", "https_port": 18443}, Path(root))
            self.assertEqual(bind, "192.0.2.10")
            self.assertEqual(tls_port, 18443)

    def test_network_port_is_reselected_when_existing_install_changes_bind_address(self):
        args = install.cli_parser().parse_args(["--recordings-dir", "/tmp/records", "--bind", "100.78.57.60"])
        seen = []

        def available(bind, port):
            seen.append((bind, port))
            return True

        with mock.patch.object(install, "bind_port_available", side_effect=available):
            bind, tls_port = install.network_settings(
                args, {"bind": "127.0.0.1", "https_port": 18443}, Path("/tmp")
            )
        self.assertEqual(bind, "100.78.57.60")
        self.assertEqual(tls_port, install.CONTAINER_HTTPS_PORT)
        self.assertEqual(seen, [("100.78.57.60", install.CONTAINER_HTTPS_PORT)])

    def test_https_port_is_user_configurable(self):
        args = install.cli_parser().parse_args(["--recordings-dir", "/tmp/x", "--https-port", "19443"])
        with mock.patch.object(install, "bind_port_available", return_value=True):
            bind, tls_port = install.network_settings(args, None, Path("/tmp"))
        self.assertEqual(bind, "127.0.0.1")
        self.assertEqual(tls_port, 19443)

    def test_exact_tag_checkout_is_verified_against_the_selected_sha(self):
        with tempfile.TemporaryDirectory() as root:
            root_path = Path(root)
            source = root_path / "origin"
            bare = root_path / "official.git"
            install_dir = root_path / "install"
            source.mkdir()
            subprocess.run(["git", "init", str(source)], check=True, capture_output=True)
            subprocess.run(["git", "-C", str(source), "config", "user.name", "Installer Test"], check=True)
            subprocess.run(["git", "-C", str(source), "config", "user.email", "installer@example.test"], check=True)
            (source / "Dockerfile").write_text("FROM scratch\n", encoding="utf-8")
            subprocess.run(["git", "-C", str(source), "add", "Dockerfile"], check=True)
            subprocess.run(["git", "-C", str(source), "commit", "-m", "fixture"], check=True, capture_output=True)
            subprocess.run(["git", "-C", str(source), "tag", "v1.2.3"], check=True)
            subprocess.run(["git", "clone", "--bare", str(source), str(bare)], check=True, capture_output=True)
            expected = subprocess.run(
                ["git", "-C", str(source), "rev-parse", "HEAD"], check=True, capture_output=True, text=True
            ).stdout.strip()
            with mock.patch.object(install, "REPOSITORY_URL", str(bare)):
                checkout, build_time = install.prepare_source(install_dir, {"tag": "v1.2.3"}, expected, mock.Mock())
                self.assertEqual(subprocess.run(
                    ["git", "-C", str(checkout), "rev-parse", "HEAD"],
                    check=True, capture_output=True, text=True,
                ).stdout.strip(), expected)
                self.assertIsNotNone(install.parse_timestamp(build_time).tzinfo)
                with self.assertRaises(install.InstallError):
                    install.prepare_source(install_dir, {"tag": "v1.2.3"}, "0" * 40, mock.Mock())


class ComposeAndInstallSafetyTests(unittest.TestCase):
    def test_compose_uses_exact_build_identity_and_separated_persistent_mounts(self):
        config = deployment_config()
        compose = install.render_compose(config)
        self.assertIn('VERSION: "1.2.3"', compose)
        self.assertIn(f'COMMIT: "{COMMIT}"', compose)
        self.assertIn(f'BUILD_TIME: "{BUILD_TIME_GIT}"', compose)
        self.assertIn('RELEASE_CHANNEL: "stable"', compose)
        build_labels = compose.split("      labels:\n", 1)[1].split("    user:", 1)[0]
        for label, value in (
            ("org.opencontainers.image.source", install.REPOSITORY_SOURCE),
            ("org.opencontainers.image.version", "1.2.3"),
            ("org.opencontainers.image.revision", COMMIT),
            ("org.opencontainers.image.created", BUILD_TIME_GIT),
            ("com.integrated-recorder.release-channel", "stable"),
            ("com.integrated-recorder.release-tag", "v1.2.3"),
        ):
            self.assertIn(f'{label}: "{value}"', build_labels)
        self.assertIn('com.integrated-recorder.release-channel: "stable"', compose)
        self.assertIn("source: application-state\n        target: /data", compose)
        self.assertIn("target: /data/recordings", compose)
        self.assertIn('source: "/mnt/archive with spaces/recordings"', compose)
        self.assertIn("create_host_path: false", compose)
        self.assertIn('published: "18080"', compose)
        self.assertIn('host_ip: "127.0.0.1"', compose)
        self.assertIn('COOKIE_SECURE: "0"', compose)
        self.assertNotIn("AUTH_DISABLED", compose)
        self.assertNotIn("auth_disabled: \"1\"", compose)

    def test_rc8_dockerfile_without_labels_gets_verified_labels_from_compose(self):
        dockerfile = (Path(install.__file__).resolve().parent / "testdata" / "Dockerfile.v0.1.0-rc.8").read_text(encoding="utf-8")
        self.assertNotIn("LABEL ", dockerfile)
        for name in ("VERSION", "COMMIT", "BUILD_TIME", "RELEASE_CHANNEL"):
            self.assertIn(f"ARG {name}=", dockerfile)
        config = deployment_config("/srv/recordings", version="0.1.0-rc.8", channel="prerelease")
        compose = install.render_compose(config)
        build_section = compose.split("    build:\n", 1)[1].split("    user:", 1)[0]
        self.assertIn('VERSION: "0.1.0-rc.8"', build_section)
        self.assertIn(f'COMMIT: "{COMMIT}"', build_section)
        self.assertIn(f'BUILD_TIME: "{BUILD_TIME_GIT}"', build_section)
        self.assertIn('RELEASE_CHANNEL: "prerelease"', build_section)
        self.assertIn(f'org.opencontainers.image.source: "{install.REPOSITORY_SOURCE}"', build_section)
        self.assertIn('com.integrated-recorder.release-tag: "v0.1.0-rc.8"', build_section)

    def test_compose_build_labels_follow_future_pinned_release_metadata(self):
        config = deployment_config(version="2.4.0-rc.2", channel="prerelease")
        config["release"].update({"tag": "v2.4.0-rc.2", "commit": "c" * 40, "build_time": "2030-01-02T03:04:05+00:00"})
        build_section = install.render_compose(config).split("    build:\n", 1)[1].split("    user:", 1)[0]
        self.assertIn('VERSION: "2.4.0-rc.2"', build_section)
        self.assertIn(f'COMMIT: "{"c" * 40}"', build_section)
        self.assertIn('BUILD_TIME: "2030-01-02T03:04:05+00:00"', build_section)
        self.assertIn('com.integrated-recorder.release-tag: "v2.4.0-rc.2"', build_section)
        self.assertIn('org.opencontainers.image.source: "https://github.com/integrated-recorder/core"', build_section)

    def test_image_label_verification_rejects_wrong_release_metadata(self):
        expected = deployment_config()["release"]
        expected_labels = {
            "org.opencontainers.image.source": install.REPOSITORY_SOURCE,
            "org.opencontainers.image.version": expected["version"],
            "org.opencontainers.image.revision": expected["commit"],
            "org.opencontainers.image.created": expected["build_time"],
            "com.integrated-recorder.release-channel": expected["channel"],
            "com.integrated-recorder.release-tag": expected["tag"],
        }
        with mock.patch.object(install, "run", return_value=subprocess.CompletedProcess([], 0, json.dumps(expected_labels), "")):
            install.verify_image_labels("image:test", expected)
        for key in expected_labels:
            wrong = dict(expected_labels, **{key: "incorrect"})
            with self.subTest(label=key), mock.patch.object(
                install, "run", return_value=subprocess.CompletedProcess([], 0, json.dumps(wrong), "")
            ):
                with self.assertRaisesRegex(install.InstallError, "labels do not match"):
                    install.verify_image_labels("image:test", expected)

    def test_retry_after_rc8_image_label_failure_preserves_pending_source_and_archives(self):
        with tempfile.TemporaryDirectory(prefix="installer rc8 retry ") as root:
            install_dir = Path(root) / "install"
            archive = Path(root) / "recordings"
            source = install_dir / "source" / COMMIT
            install_dir.mkdir()
            archive.mkdir()
            source.mkdir(parents=True)
            (source / ".git").mkdir()
            (source / "Dockerfile").write_text(
                (Path(install.__file__).resolve().parent / "testdata" / "Dockerfile.v0.1.0-rc.8").read_text(encoding="utf-8"),
                encoding="utf-8",
            )
            source_marker = source / "keep-source.txt"
            source_marker.write_text("cached official RC8 source", encoding="utf-8")
            archive_marker = archive / "keep-recording.bin"
            archive_marker.write_bytes(b"existing recording archive")
            release_data = {
                "tag": "v0.1.0-rc.8",
                "source": install.REPOSITORY_SOURCE,
                "version": "0.1.0-rc.8",
                "commit": COMMIT,
                "build_time": BUILD_TIME_GIT,
                "channel": "prerelease",
                "architecture_manifest": "amd64",
                "manifest_identity_checked": True,
                "manifest_signature_independently_verified": False,
            }
            pending = {
                "schema_version": 1,
                "repository": install.REPOSITORY,
                "release": release_data,
                "recordings_dir": str(archive),
                "port": 18080,
                "architecture": "amd64",
                "compose_project": install.COMPOSE_PROJECT,
                "state_volume": install.STATE_VOLUME,
                "source_dir": str(source),
                "image": f"localhost/integrated-recorder-core:{COMMIT}",
                "network_exposure": "127.0.0.1",
                "container_uid": install.CONTAINER_UID,
                "container_gid": install.CONTAINER_GID,
            }
            pending_path = install_dir / "deployment.pending.json"
            install.write_metadata(pending_path, pending)
            args = install.cli_parser().parse_args([
                "--install-dir", str(install_dir), "--recordings-dir", str(archive),
            ])
            image_labels = {
                "org.opencontainers.image.source": install.REPOSITORY_SOURCE,
                "org.opencontainers.image.version": "0.1.0-rc.8",
                "org.opencontainers.image.revision": COMMIT,
                "org.opencontainers.image.created": BUILD_TIME_GIT,
                "com.integrated-recorder.release-channel": "prerelease",
                "com.integrated-recorder.release-tag": "v0.1.0-rc.8",
            }
            failure_labels = dict(image_labels, **{"org.opencontainers.image.revision": "wrong-commit"})
            inspect_attempts = 0

            def fake_run(argv, **kwargs):
                nonlocal inspect_attempts
                if argv[1:3] == ["image", "inspect"]:
                    inspect_attempts += 1
                    labels = failure_labels if inspect_attempts == 1 else image_labels
                    return subprocess.CompletedProcess(argv, 0, json.dumps(labels), "")
                return subprocess.CompletedProcess(argv, 0, "", "")

            common_patches = (
                mock.patch.object(install, "docker_preflight", return_value="amd64"),
                mock.patch.object(install, "verify_existing_state_volume"),
                mock.patch.object(install, "prepare_source", return_value=(source, BUILD_TIME_GIT)),
                mock.patch.object(install, "verify_image_binaries"),
                mock.patch.object(install, "probe_archive_permissions"),
                mock.patch.object(install, "wait_for_runtime", return_value={
                    "state": "uninitialized", "administrator_configured": False, "auth_disabled": False,
                    "version": "0.1.0-rc.8", "release_channel": "prerelease",
                }),
                mock.patch.object(install, "run", side_effect=fake_run),
                mock.patch.object(install, "network_settings", return_value=("127.0.0.1", 8443)),
            )
            with common_patches[0], common_patches[1], common_patches[2], common_patches[3], common_patches[4], common_patches[5], common_patches[6], common_patches[7]:
                with self.assertRaisesRegex(install.InstallError, "image labels do not match"):
                    install.install(args)
                pending_after_failure = install.read_pending(pending_path)
                self.assertEqual(pending_after_failure["release"], release_data)
                self.assertEqual(pending_after_failure["recordings_dir"], str(archive.resolve()))
                self.assertFalse(pending_after_failure.get("compose_started", False))
                self.assertTrue((install_dir / "compose.pending.yaml").is_file())
                self.assertEqual(source_marker.read_text(encoding="utf-8"), "cached official RC8 source")
                self.assertEqual(archive_marker.read_bytes(), b"existing recording archive")

                install.install(args)

            self.assertEqual(inspect_attempts, 2)
            self.assertIsNone(install.read_pending(pending_path))
            current = install.load_metadata(install_dir / "deployment.json")
            self.assertEqual(current["release"]["tag"], "v0.1.0-rc.8")
            self.assertEqual(current["release"]["commit"], COMMIT)
            self.assertEqual(current["recordings_dir"], str(archive.resolve()))
            self.assertEqual(source_marker.read_text(encoding="utf-8"), "cached official RC8 source")
            self.assertEqual(archive_marker.read_bytes(), b"existing recording archive")

    def test_retry_after_non_loopback_readiness_failure_preserves_pending_install_and_data(self):
        with tempfile.TemporaryDirectory(prefix="installer nonloopback retry ") as root:
            install_dir = Path(root) / "install dir"
            archive = Path(root) / "recordings with spaces"
            source = install_dir / "source" / COMMIT
            install_dir.mkdir()
            archive.mkdir()
            source.mkdir(parents=True)
            (source / ".git").mkdir()
            source_marker = source / "cached-source.txt"
            source_marker.write_text("exact pinned release source", encoding="utf-8")
            archive_marker = archive / "keep-recording.bin"
            archive_marker.write_bytes(b"recording bytes survive retry")
            release_data = {
                "tag": "v0.1.0-rc.8",
                "source": install.REPOSITORY_SOURCE,
                "version": "0.1.0-rc.8",
                "commit": COMMIT,
                "build_time": BUILD_TIME_GIT,
                "channel": "prerelease",
                "architecture_manifest": "amd64",
                "manifest_identity_checked": True,
                "manifest_signature_independently_verified": False,
            }
            pending = {
                "schema_version": 1,
                "repository": install.REPOSITORY,
                "release": release_data,
                "recordings_dir": str(archive),
                "port": 18080,
                "bind": "100.78.57.60",
                "network_exposure": "100.78.57.60",
                "https_port": 18443,
                "architecture": "amd64",
                "compose_project": install.COMPOSE_PROJECT,
                "state_volume": install.STATE_VOLUME,
                "source_dir": str(source),
                "image": f"localhost/integrated-recorder-core:{COMMIT}",
                "container_uid": install.CONTAINER_UID,
                "container_gid": install.CONTAINER_GID,
                "compose_started": True,
            }
            pending_path = install_dir / "deployment.pending.json"
            install.write_metadata(pending_path, pending)
            args = install.cli_parser().parse_args([
                "--install-dir", str(install_dir), "--recordings-dir", str(archive),
            ])
            ready_status = {
                "state": "uninitialized",
                "administrator_configured": False,
                "auth_disabled": False,
                "version": "0.1.0-rc.8",
                "release_channel": "prerelease",
            }
            readiness = mock.Mock(side_effect=[
                install.InstallError("Runtime Host did not become healthy on bind address 100.78.57.60."),
                ready_status,
            ])
            state_volume = mock.patch.object(install, "verify_existing_state_volume")
            with state_volume as verify_volume, \
                 mock.patch.object(install, "docker_preflight", return_value="amd64"), \
                 mock.patch.object(install, "prepare_source", return_value=(source, BUILD_TIME_GIT)), \
                 mock.patch.object(install, "verify_image_labels"), \
                 mock.patch.object(install, "verify_image_binaries"), \
                 mock.patch.object(install, "probe_archive_permissions"), \
                 mock.patch.object(install, "wait_for_runtime", readiness) as wait_runtime, \
                 mock.patch.object(install, "bind_port_available", return_value=True), \
                 mock.patch.object(install, "run", return_value=subprocess.CompletedProcess([], 0, "", "")):
                with self.assertRaisesRegex(install.InstallError, "100.78.57.60"):
                    install.install(args)

                pending_after_failure = install.read_pending(pending_path)
                self.assertTrue(pending_after_failure["compose_started"])
                self.assertEqual(pending_after_failure["release"], release_data)
                self.assertEqual(pending_after_failure["recordings_dir"], str(archive.resolve()))
                self.assertEqual(pending_after_failure["bind"], "100.78.57.60")
                self.assertTrue((install_dir / "compose.pending.yaml").is_file())
                self.assertEqual(source_marker.read_text(encoding="utf-8"), "exact pinned release source")
                self.assertEqual(archive_marker.read_bytes(), b"recording bytes survive retry")

                install.install(args)

            verify_volume.assert_called_with(True)
            wait_runtime.assert_has_calls([
                mock.call(18080, mock.ANY, bind="100.78.57.60"),
                mock.call(18080, mock.ANY, bind="100.78.57.60"),
            ])
            self.assertIsNone(install.read_pending(pending_path))
            current = install.load_metadata(install_dir / "deployment.json")
            self.assertEqual(current["release"], release_data)
            self.assertEqual(current["recordings_dir"], str(archive.resolve()))
            self.assertEqual(current["bind"], "100.78.57.60")
            self.assertTrue(current["owner_setup_required"])
            self.assertEqual(source_marker.read_text(encoding="utf-8"), "exact pinned release source")
            self.assertEqual(archive_marker.read_bytes(), b"recording bytes survive retry")

    def test_compose_escapes_compose_interpolation_for_dollar_in_paths(self):
        compose = install.render_compose(deployment_config("/mnt/archive/$recordings"))
        self.assertIn("/mnt/archive/$$recordings", compose)

    def test_existing_data_volume_is_stable_and_no_global_cleanup_is_used(self):
        compose = install.render_compose(deployment_config())
        self.assertIn(f"name: {install.STATE_VOLUME}", compose)
        source = Path(install.__file__).read_text(encoding="utf-8")
        for forbidden in (
            "docker compose down -v", "docker system prune", "docker volume prune",
            "docker network prune", "docker volume rm", "docker rm --force $(docker ps",
        ):
            self.assertNotIn(forbidden, source)

    def test_existing_compose_project_is_detected_before_installation(self):
        with mock.patch.object(install, "run", side_effect=[
            subprocess.CompletedProcess([], 0, "abc123def456\n", ""),
        ]) as command:
            message = install.docker_existing_installation()
        self.assertIn("Compose project integrated-recorder", message)
        self.assertEqual(command.call_count, 1)

        with mock.patch.object(install, "run", side_effect=[
            subprocess.CompletedProcess([], 0, "", ""),
            subprocess.CompletedProcess([], 0, "", ""),
            subprocess.CompletedProcess([], 0, "", ""),
            subprocess.CompletedProcess([], 0, "integrated-recorder_default\n", ""),
        ]):
            message = install.docker_existing_installation()
        self.assertIn("existing Compose network integrated-recorder_default", message)

    def test_existing_install_never_silently_creates_a_replacement_state_volume(self):
        with mock.patch.object(install, "run", return_value=subprocess.CompletedProcess([], 1, "", "missing")) as command:
            with self.assertRaisesRegex(install.InstallError, "refusing to create an empty replacement"):
                install.verify_existing_state_volume(True)
        self.assertEqual(command.call_count, 1)
        with mock.patch.object(install, "run", return_value=subprocess.CompletedProcess([], 1, "", "missing")) as command:
            install.verify_existing_state_volume(False)
        command.assert_not_called()

    def test_nonempty_unmarked_install_directory_is_preserved_and_rejected(self):
        with tempfile.TemporaryDirectory() as root:
            install_dir = Path(root) / "existing deployment"
            install_dir.mkdir()
            sentinel = install_dir / "user-data.txt"
            sentinel.write_text("keep", encoding="utf-8")
            with self.assertRaises(install.InstallError):
                install.ensure_install_dir(install_dir, has_marker=False)
            self.assertEqual(sentinel.read_text(encoding="utf-8"), "keep")

    def test_deployment_metadata_round_trip_is_idempotent(self):
        with tempfile.TemporaryDirectory() as root:
            metadata_path = Path(root) / "deployment.json"
            metadata = {
                "schema_version": 1,
                "release": deployment_config()["release"],
                "recordings_dir": "/mnt/archive with spaces/recordings",
                "port": 18080,
            }
            install.write_metadata(metadata_path, metadata)
            original = metadata_path.read_bytes()
            self.assertEqual(install.load_metadata(metadata_path), metadata)
            install.write_metadata(metadata_path, install.load_metadata(metadata_path))
            self.assertEqual(metadata_path.read_bytes(), original)

    def test_runtime_startup_acceptance_requires_health_setup_and_html(self):
        health = mock.MagicMock()
        health.__enter__.return_value = health
        health.read.return_value = b'{"status":"ok"}'
        with mock.patch.object(install.urllib.request, "build_opener") as opener_factory, \
             mock.patch.object(install, "get_setup_status") as setup_status, \
             mock.patch.object(install, "verify_browser_ui") as verify_ui:
            opener_factory.return_value.open.return_value = health
            status = {
                "state": "uninitialized",
                "administrator_configured": False,
                "auth_disabled": False,
                "version": "1.2.3",
                "release_channel": "stable",
            }
            setup_status.return_value = status
            bind = "100.78.57.60"
            self.assertEqual(install.wait_for_runtime(18080, deployment_config()["release"], timeout=1, bind=bind), status)
            opener_factory.return_value.open.assert_called_once_with(
                "http://100.78.57.60:18080/healthz", timeout=3
            )
            setup_status.assert_called_once_with(18080, bind)
            verify_ui.assert_called_once_with(18080, bind)

    def test_probe_addresses_follow_ipv4_ipv6_and_wildcard_bind_semantics(self):
        for bind, expected in (
            ("127.0.0.1", "127.0.0.1"),
            ("100.78.57.60", "100.78.57.60"),
            ("0.0.0.0", "127.0.0.1"),
            ("::1", "::1"),
            ("2001:db8::20", "2001:db8::20"),
            ("::", "::1"),
        ):
            self.assertEqual(install.runtime_probe_host(bind), expected, bind)
        self.assertEqual(
            install.runtime_probe_url("2001:db8::20", 18080, "/healthz"),
            "http://[2001:db8::20]:18080/healthz",
        )

    def test_setup_status_and_setup_html_probes_use_the_selected_bind(self):
        setup_response = mock.MagicMock()
        setup_response.read.return_value = b'{"state":"uninitialized"}'
        setup_response.__enter__.return_value = setup_response
        with mock.patch.object(install.urllib.request, "build_opener") as opener_factory:
            opener = opener_factory.return_value
            opener.open.return_value = setup_response
            self.assertEqual(install.get_setup_status(18080, "100.78.57.60"), {"state": "uninitialized"})
            opener.open.assert_called_once_with("http://100.78.57.60:18080/api/setup/status", timeout=5)

        page_response = mock.MagicMock()
        page_response.status = 200
        page_response.headers = {"Content-Type": "text/html; charset=utf-8"}
        page_response.read.return_value = b'<html><div id="root"></div></html>'
        page_response.__enter__.return_value = page_response
        with mock.patch.object(install.urllib.request, "build_opener") as opener_factory:
            opener = opener_factory.return_value
            opener.open.return_value = page_response
            install.verify_browser_ui(18080, "100.78.57.60")
            request = opener.open.call_args.args[0]
            self.assertEqual(request.full_url, "http://100.78.57.60:18080/setup")
            self.assertIn("text/html", request.get_header("Accept"))

    def test_ipv6_bind_uses_bracketed_url_and_ipv6_socket_for_port_selection(self):
        fake_socket = mock.MagicMock()
        fake_socket.__enter__.return_value = fake_socket
        fake_socket.getsockname.return_value = ("::1", 49152, 0, 0)
        with mock.patch.object(install, "bind_port_available", side_effect=[False, True]) as available, \
             mock.patch.object(install.socket, "socket", return_value=fake_socket) as socket_factory:
            self.assertEqual(install.choose_port(None, bind="::"), 49152)
        self.assertEqual(available.call_args_list[0].args, ("::", 8080))
        socket_factory.assert_called_once_with(install.socket.AF_INET6, install.socket.SOCK_STREAM)
        fake_socket.bind.assert_called_once_with(("::", 0, 0, 0))

    def test_first_run_status_requires_enabled_auth_and_known_build_identity(self):
        status = {
            "state": "uninitialized",
            "administrator_configured": False,
            "auth_disabled": False,
            "version": "1.2.3",
            "release_channel": "stable",
        }
        install.validate_setup_status(status, deployment_config()["release"])
        invalid = dict(status, auth_disabled=True)
        with self.assertRaises(install.InstallError):
            install.validate_setup_status(invalid, deployment_config()["release"])
        invalid = dict(status, version="dev")
        with self.assertRaises(install.InstallError):
            install.validate_setup_status(invalid, deployment_config()["release"])

    def test_browser_setup_route_is_verified_as_html_not_only_as_health_json(self):
        response = mock.MagicMock()
        response.status = 200
        response.headers = {"Content-Type": "text/html; charset=utf-8"}
        response.read.return_value = b"<html><div id=\"root\"></div></html>"
        response.__enter__.return_value = response
        for bind, url_host in (("127.0.0.1", "127.0.0.1"), ("::1", "[::1]"), ("0.0.0.0", "127.0.0.1"), ("::", "[::1]")):
            with self.subTest(bind=bind), mock.patch.object(
                install.urllib.request, "build_opener", return_value=mock.Mock(open=mock.Mock(return_value=response))
            ) as opener_factory:
                install.verify_browser_ui(8080, bind)
                request = opener_factory.return_value.open.call_args.args[0]
                self.assertEqual(request.full_url, f"http://{url_host}:8080/setup")
                self.assertEqual(request.get_header("Accept"), "text/html")
        response.read.assert_called_with(256 * 1024)

    def test_actual_compiled_identity_check_requires_each_application_identity_field(self):
        with tempfile.TemporaryDirectory() as root:
            binary = Path(root) / "runtime-host"
            release_info = deployment_config()["release"]
            binary.write_bytes("|".join(release_info.values()).encode())
            self.assertEqual(install.binary_embeds_identity(binary, release_info), [])
            binary.write_bytes(b"dev|unknown|development")
            self.assertEqual(
                set(install.binary_embeds_identity(binary, release_info)),
                {release_info["version"], release_info["commit"], release_info["build_time"], release_info["channel"]},
            )

    def test_image_identity_probe_checks_host_and_control_plane_binaries(self):
        release_info = deployment_config()["release"]
        seen = []

        def fake_run(argv, **kwargs):
            seen.append(argv)
            if argv[1:2] == ["create"]:
                return subprocess.CompletedProcess(argv, 0, "container-id\n", "")
            if argv[1:3] == ["cp", "container-id:/tmp/ir-build-identity/runtime-host"]:
                Path(argv[-1]).write_bytes("|".join(release_info.values()).encode())
            elif argv[1:3] == ["cp", "container-id:/tmp/ir-build-identity/control-plane"]:
                Path(argv[-1]).write_bytes("|".join(release_info.values()).encode())
            elif argv[1:2] == ["cp"]:
                Path(argv[-1]).write_bytes(b"present")
            return subprocess.CompletedProcess(argv, 0, "", "")

        with mock.patch.object(install, "run", side_effect=fake_run):
            install.verify_image_binaries("image:test", release_info)
        self.assertIn(["docker", "start", "--attach", "container-id"], seen)
        self.assertIn(["docker", "rm", "--force", "container-id"], seen)

        def invalid_host(argv, **kwargs):
            result = fake_run(argv, **kwargs)
            if argv[1:3] == ["cp", "container-id:/tmp/ir-build-identity/runtime-host"]:
                Path(argv[-1]).write_bytes(b"development")
            return result

        with mock.patch.object(install, "run", side_effect=invalid_host):
            with self.assertRaisesRegex(install.InstallError, "runtime-host binary does not contain"):
                install.verify_image_binaries("image:test", release_info)

    def test_host_port_selection_avoids_conflict_and_preserves_an_existing_port(self):
        self.assertEqual(install.choose_port(None, existing=19080, available=lambda _: False), 19080)
        with self.assertRaises(install.InstallError):
            install.choose_port(8080, available=lambda _: False)
        with mock.patch.object(install, "bind_port_available", side_effect=lambda bind, port: port != 8080) as available:
            fake_socket = mock.MagicMock()
            fake_socket.__enter__.return_value = fake_socket
            fake_socket.getsockname.return_value = ("127.0.0.1", 49152)
            with mock.patch.object(install.socket, "socket", return_value=fake_socket):
                self.assertEqual(install.choose_port(None), 49152)
        available.assert_called_once_with("127.0.0.1", 8080)

    def test_host_port_conflict_probe_uses_selected_interface(self):
        with mock.patch.object(install, "bind_port_available", return_value=False) as available:
            with self.assertRaisesRegex(install.InstallError, "unavailable"):
                install.choose_port(18080, bind="100.78.57.60")
        available.assert_called_once_with("100.78.57.60", 18080)

    def test_default_and_https_port_conflicts_are_checked_on_selected_bind(self):
        with mock.patch.object(install, "bind_port_available", side_effect=[False, True]) as available:
            fake_socket = mock.MagicMock()
            fake_socket.__enter__.return_value = fake_socket
            fake_socket.getsockname.return_value = ("100.78.57.60", 49152)
            with mock.patch.object(install.socket, "socket", return_value=fake_socket):
                self.assertEqual(install.choose_port(None, bind="100.78.57.60"), 49152)
        self.assertEqual(
            available.call_args_list,
            [mock.call("100.78.57.60", 8080)],
        )

        args = install.cli_parser().parse_args([
            "--recordings-dir", "/tmp/records", "--bind", "100.78.57.60", "--https-port", "18443"
        ])
        with mock.patch.object(install, "bind_port_available", return_value=False) as https_available:
            with self.assertRaisesRegex(install.InstallError, "18443 is unavailable"):
                install.network_settings(args, None, Path("/tmp"))
        https_available.assert_called_once_with("100.78.57.60", 18443)

    def test_docker_preflight_requires_local_linux_engine_on_supported_architecture(self):
        install.docker_architecture("linux/aarch64")
        self.assertEqual(install.docker_architecture("linux/x86_64"), "amd64")
        with self.assertRaisesRegex(install.InstallError, "Linux engine"):
            install.docker_architecture("darwin/arm64")
        with self.assertRaisesRegex(install.InstallError, "unsupported"):
            install.docker_architecture("linux/ppc64le")

        with mock.patch.object(install, "check_compose_version"), mock.patch.object(
            install, "run", side_effect=[
                subprocess.CompletedProcess([], 0, '[{"Endpoints":{"docker":{"Host":"ssh://remote"}}}]', ""),
            ]
        ):
            with self.assertRaisesRegex(install.InstallError, "remote"):
                install.docker_preflight()

    def test_permission_failure_is_actionable_and_does_not_chown_nonempty_archive(self):
        with tempfile.TemporaryDirectory(prefix="archive permissions ") as temp_dir:
            directory = Path(temp_dir) / "existing recordings"
            directory.mkdir(mode=0o751)
            original = directory / "keep.bin"
            original.write_bytes(b"existing archive data")
            directory.chmod(0o751)
            before = directory.stat()
            contents_before = original.read_bytes()

            message = install.permission_failure_message(directory)
            self.assertIn("10001:10001", message)
            self.assertIn("NFS with root-squash", message)
            self.assertIn(str(directory), message)
            with mock.patch.object(
                install,
                "run",
                return_value=subprocess.CompletedProcess([], 1, "", "permission denied"),
            ) as command:
                with self.assertRaises(install.InstallError):
                    install.probe_archive_permissions("image:test", directory, created=False)

            self.assertEqual(command.call_count, 1)
            invocation = command.call_args.args[0]
            self.assertIn("docker", invocation[0])
            probe_command = invocation[-1]
            self.assertNotIn("chmod", probe_command)
            self.assertNotIn("chown", probe_command)
            after = directory.stat()
            self.assertEqual((after.st_uid, after.st_gid, after.st_mode), (before.st_uid, before.st_gid, before.st_mode))
            self.assertEqual(original.read_bytes(), contents_before)
            self.assertEqual(sorted(path.name for path in directory.iterdir()), ["keep.bin"])

    def test_existing_empty_directory_is_not_chowned_or_chmodded(self):
        with tempfile.TemporaryDirectory(prefix="empty archive permissions ") as temp_dir:
            directory = Path(temp_dir) / "empty recordings"
            directory.mkdir(mode=0o751)
            directory.chmod(0o751)
            before = directory.stat()

            with mock.patch.object(
                install,
                "run",
                return_value=subprocess.CompletedProcess([], 1, "", "permission denied"),
            ) as command:
                with self.assertRaises(install.InstallError):
                    install.probe_archive_permissions("image:test", directory, created=False)

            self.assertEqual(command.call_count, 1)
            probe_command = command.call_args.args[0][-1]
            self.assertNotIn("chmod", probe_command)
            self.assertNotIn("chown", probe_command)
            after = directory.stat()
            self.assertEqual((after.st_uid, after.st_gid, after.st_mode), (before.st_uid, before.st_gid, before.st_mode))
            self.assertEqual(list(directory.iterdir()), [])

    def test_new_empty_directory_can_only_receive_nonrecursive_directory_chown(self):
        directory = Path("/mnt/media/new folder, comma")
        seen_compose = []

        def fake_run(argv, **kwargs):
            compose_path = Path(argv[argv.index("--file") + 1])
            seen_compose.append(compose_path.read_text(encoding="utf-8"))
            if len(seen_compose) == 1:
                return subprocess.CompletedProcess(argv, 1, "", "permission denied")
            if "chown 10001:10001 /recordings" in argv[-1]:
                return subprocess.CompletedProcess(argv, 0, "", "")
            return subprocess.CompletedProcess(argv, 0, "", "")

        with mock.patch.object(install, "run", side_effect=fake_run) as command:
            install.probe_archive_permissions("image:test", directory, created=True)
        self.assertEqual(command.call_count, 3)
        self.assertIn('source: "/mnt/media/new folder, comma"', seen_compose[0])
        self.assertIn("network_mode: none", seen_compose[0])
        self.assertNotIn("-R", " ".join(command.call_args_list[1].args[0]))

    def test_startup_verifies_tag_sha_outside_branch_and_releases_without_auto_upgrade(self):
        source = Path(install.__file__).read_text(encoding="utf-8")
        self.assertIn('"fetch", "--depth=1", "origin", refspec', source)
        self.assertIn('git_output(temp, "rev-parse", f"{release[\'tag\']}^{{commit}}")', source)
        self.assertIn("if policy in (\"preserve-current\", \"resume-pending\")", source)
        self.assertIn("Pass --upgrade to change its application release", source)


class IsolatedDockerInstallAcceptanceTests(unittest.TestCase):
    """Opt-in real Docker acceptance. Set IR_RUN_INSTALL_DOCKER_E2E=1 to run.

    The test owns a unique Compose project, named state volume, install dir and
    archive directory. Cleanup is explicitly scoped to those resources and
    never uses global Docker cleanup commands.
    """

    @unittest.skipUnless(
        os.environ.get("IR_RUN_INSTALL_DOCKER_E2E") == "1",
        "set IR_RUN_INSTALL_DOCKER_E2E=1 to run the isolated real Docker install acceptance",
    )
    def test_fresh_install_on_selected_host_interface_and_retry_is_idempotent(self):
        if shutil.which("docker") is None:
            self.fail("Docker integration was requested, but the Docker CLI is unavailable.")
        architecture = install.docker_preflight()
        bind = os.environ.get("IR_INSTALL_DOCKER_TEST_BIND") or install.discover_host_address()
        bind = install.normalize_bind(bind)
        address = ipaddress.ip_address(bind)
        if address.is_loopback or address.is_unspecified:
            self.fail("Set IR_INSTALL_DOCKER_TEST_BIND to a non-loopback address assigned to this Docker host.")
        if not install.bind_port_available(bind, 0):
            self.skipTest(f"Docker host address {bind} is not assigned locally.")

        project = f"ir-installer-e2e-{uuid.uuid4().hex[:12]}"
        volume = f"{project}-state"
        version = os.environ.get("IR_INSTALL_DOCKER_TEST_VERSION")
        with tempfile.TemporaryDirectory(prefix="integrated recorder docker install ") as root:
            root_path = Path(root)
            install_dir = root_path / "deployment with spaces"
            archive = root_path / "recordings with spaces"
            archive.mkdir()
            archive_marker = archive / "host-archive-marker.bin"
            archive_marker.write_bytes(b"preserved through container recreation")
            argv = ["--install-dir", str(install_dir), "--recordings-dir", str(archive), "--bind", bind]
            if version:
                argv.extend(["--version", version])
            args = install.cli_parser().parse_args(argv)
            compose_file = install_dir / "compose.yaml"
            pending_compose = install_dir / "compose.pending.yaml"
            state_marker = ".installer-acceptance-state-marker"

            def container_id() -> str:
                result = install.run(install.compose_command(compose_file, "ps", "-q", "archiver"))
                value = result.stdout.strip()
                self.assertTrue(value, "isolated installer Compose project has no archiver container")
                return value

            def assert_data_mounts(container: str, metadata: dict) -> None:
                inspect = json.loads(install.run(["docker", "inspect", container]).stdout)[0]
                mounts = {entry.get("Destination"): entry for entry in inspect.get("Mounts", [])}
                self.assertEqual(mounts["/data"].get("Type"), "volume")
                self.assertEqual(mounts["/data"].get("Name"), volume)
                self.assertEqual(mounts["/data/recordings"].get("Type"), "bind")
                self.assertEqual(Path(mounts["/data/recordings"].get("Source", "")).resolve(), archive.resolve())
                self.assertEqual(metadata["bind"], bind)
                self.assertEqual(metadata["architecture"], architecture)
                published = inspect.get("NetworkSettings", {}).get("Ports", {})
                for target, host_port in (
                    (f"{install.CONTAINER_HTTP_PORT}/tcp", str(metadata["port"])),
                    (f"{install.CONTAINER_HTTPS_PORT}/tcp", str(metadata["https_port"])),
                ):
                    bindings = published.get(target) or []
                    self.assertIn(
                        {"HostIp": bind, "HostPort": host_port},
                        bindings,
                        f"{target} is not published on the selected host address",
                    )

            with mock.patch.object(install, "COMPOSE_PROJECT", project), \
                 mock.patch.object(install, "STATE_VOLUME", volume):
                try:
                    self.assertIsNone(install.docker_existing_installation())
                    install.install(args)
                    metadata = install.load_metadata(install_dir / "deployment.json")
                    self.assertFalse((install_dir / "deployment.pending.json").exists())
                    self.assertTrue(metadata["owner_setup_required"])
                    self.assertEqual(metadata["setup_state"], "uninitialized")
                    current_container = container_id()
                    assert_data_mounts(current_container, metadata)
                    archive_marker.write_bytes(b"still present after the first install")

                    install.run([
                        "docker", "exec", "--user", f"{install.CONTAINER_UID}:{install.CONTAINER_GID}",
                        current_container, "/bin/sh", "-ec", f"printf state-preserved > /data/{state_marker}",
                    ])
                    install.run(install.compose_command(
                        compose_file, "up", "-d", "--no-build", "--force-recreate", "--wait",
                        "--wait-timeout", "180", "archiver",
                    ))
                    current_container = container_id()
                    assert_data_mounts(current_container, metadata)
                    state_read = install.run([
                        "docker", "exec", "--user", f"{install.CONTAINER_UID}:{install.CONTAINER_GID}",
                        current_container, "/bin/sh", "-ec", f"test \"$(cat /data/{state_marker})\" = state-preserved",
                    ])
                    self.assertEqual(state_read.returncode, 0)
                    self.assertEqual(archive_marker.read_bytes(), b"still present after the first install")

                    # A second ordinary invocation is idempotent and exercises
                    # the pinned-existing-release path rather than selecting a
                    # newly published release.
                    install.install(args)
                    metadata_after_retry = install.load_metadata(install_dir / "deployment.json")
                    self.assertEqual(metadata_after_retry["release"], metadata["release"])
                    self.assertEqual(archive_marker.read_bytes(), b"still present after the first install")
                    current_container = container_id()
                    assert_data_mounts(current_container, metadata_after_retry)
                    self.assertEqual(install.run([
                        "docker", "exec", "--user", f"{install.CONTAINER_UID}:{install.CONTAINER_GID}",
                        current_container, "/bin/sh", "-ec", f"test \"$(cat /data/{state_marker})\" = state-preserved",
                    ]).returncode, 0)
                finally:
                    cleanup_compose = compose_file if compose_file.exists() else pending_compose
                    if cleanup_compose.exists():
                        install.run(install.compose_command(cleanup_compose, "down", "--remove-orphans"), check=False)
                    volume_probe = install.run(
                        ["docker", "volume", "inspect", "--format={{.Name}}", volume], check=False
                    )
                    if volume_probe.returncode == 0 and volume_probe.stdout.strip() == volume:
                        install.run(["docker", "volume", "rm", volume])


if __name__ == "__main__":
    unittest.main()
