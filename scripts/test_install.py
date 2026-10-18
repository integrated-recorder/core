import json
import subprocess
import tempfile
import unittest
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
        self.assertIn('com.integrated-recorder.release-channel: "stable"', compose)
        self.assertIn("source: application-state\n        target: /data", compose)
        self.assertIn("target: /data/recordings", compose)
        self.assertIn('source: "/mnt/archive with spaces/recordings"', compose)
        self.assertIn("create_host_path: false", compose)
        self.assertIn('published: "18080"', compose)
        self.assertIn("host_ip: 127.0.0.1", compose)
        self.assertIn('COOKIE_SECURE: "0"', compose)
        self.assertNotIn("AUTH_DISABLED", compose)
        self.assertNotIn("auth_disabled: \"1\"", compose)

    def test_dockerfile_accepts_selected_build_identity_and_labels_final_image(self):
        dockerfile = (Path(install.__file__).resolve().parent.parent / "Dockerfile").read_text(encoding="utf-8")
        for name in ("VERSION", "COMMIT", "BUILD_TIME", "RELEASE_CHANNEL"):
            self.assertIn(f"ARG {name}=", dockerfile)
        for label in (
            "org.opencontainers.image.version=",
            "org.opencontainers.image.revision=",
            "org.opencontainers.image.created=",
            "com.integrated-recorder.release-channel=",
        ):
            self.assertIn(label, dockerfile)

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
                "claim_required": True,
                "auth_disabled": False,
                "version": "1.2.3",
                "release_channel": "stable",
            }
            setup_status.return_value = status
            self.assertEqual(install.wait_for_runtime(18080, deployment_config()["release"], timeout=1), status)
            verify_ui.assert_called_once_with(18080)

    def test_first_run_status_requires_enabled_auth_and_known_build_identity(self):
        status = {
            "state": "uninitialized",
            "administrator_configured": False,
            "claim_required": True,
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
        with mock.patch.object(install.urllib.request, "build_opener", return_value=mock.Mock(open=mock.Mock(return_value=response))):
            install.verify_browser_ui(8080)
        response.read.assert_called_once_with(256 * 1024)

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
        with mock.patch.object(install, "local_port_available", side_effect=lambda port: port != 8080):
            fake_socket = mock.MagicMock()
            fake_socket.__enter__.return_value = fake_socket
            fake_socket.getsockname.return_value = ("127.0.0.1", 49152)
            with mock.patch.object(install.socket, "socket", return_value=fake_socket):
                self.assertEqual(install.choose_port(None), 49152)

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
        directory = Path("/mnt/media/recordings with spaces")
        message = install.permission_failure_message(directory)
        self.assertIn("10001:10001", message)
        self.assertIn("NFS with root-squash", message)
        self.assertIn("'/mnt/media/recordings with spaces'", message)
        with mock.patch.object(install, "run", return_value=subprocess.CompletedProcess([], 1, "", "permission denied")) as command:
            with self.assertRaises(install.InstallError):
                install.probe_archive_permissions("image:test", directory, created_or_empty=False)
        self.assertEqual(command.call_count, 1)
        self.assertIn("docker", command.call_args.args[0][0])
        self.assertNotIn("chown", command.call_args.args[0])

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
            install.probe_archive_permissions("image:test", directory, created_or_empty=True)
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


if __name__ == "__main__":
    unittest.main()
