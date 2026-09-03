from __future__ import annotations

import os
import shutil
import subprocess
from collections.abc import Iterator
from pathlib import Path

import pytest
import yaml

PROJECT_ROOT = Path(__file__).resolve().parents[3]
COMPOSE_FILE = "examples/demo/docker-compose.yml"
BUILD_COMMAND = (
    "-f",
    COMPOSE_FILE,
    "build",
    "demo-generator",
    "chitragupta",
    "chitragupta-ui",
)
GENERATOR_COMMAND = ("-f", COMPOSE_FILE, "run", "--rm", "demo-generator")
FINAL_UP_COMMAND = ("-f", COMPOSE_FILE, "up", "--detach", "--wait", "chitragupta", "chitragupta-ui")
UI_URL = "http://127.0.0.1:8081"
API_URL = "http://127.0.0.1:8080"


def _write_executable(path: Path, contents: str) -> None:
    path.write_text(contents, encoding="utf-8")
    path.chmod(0o755)


def _copy_public_demo(tmp_path: Path) -> Path:
    workspace = tmp_path / "workspace"
    workspace.mkdir()

    launcher = PROJECT_ROOT / "demo"
    assert launcher.is_file(), "The public ./demo launcher is required"
    shutil.copy2(launcher, workspace / "demo")

    demo_tree = PROJECT_ROOT / "examples" / "demo"
    assert demo_tree.is_dir(), "The minimal examples/demo Compose tree is required"
    shutil.copytree(demo_tree, workspace / "examples" / "demo")
    return workspace


def _fake_command_environment(
    tmp_path: Path,
    *,
    anchor: str,
    failure: str | None = None,
    include_docker: bool = True,
    include_docker_compose: bool = True,
    compose_support: str = "plugin",
    uid: str = "4242",
    gid: str = "4343",
    id_failure: str | None = None,
) -> tuple[dict[str, str], Path, Path]:
    fake_bin = tmp_path / "fake-bin"
    fake_bin.mkdir()
    command_log = tmp_path / "docker.log"
    date_log = tmp_path / "date.log"
    id_log = tmp_path / "id.log"

    _write_executable(
        fake_bin / "date",
        """#!/usr/bin/env bash
set -euo pipefail
printf '%s\\n' "$*" >>"$DEMO_DATE_LOG"
if [[ "$*" != "-u +%F" ]]; then
    exit 91
fi
printf '%s\\n' "$DEMO_FAKE_ANCHOR"
""",
    )
    _write_executable(
        fake_bin / "id",
        """#!/usr/bin/env bash
set -euo pipefail
printf '%s\\n' "$*" >>"$DEMO_ID_LOG"
case "$1" in
    -u)
        if [[ "${DEMO_ID_FAILURE:-}" == "uid" ]]; then exit 46; fi
        printf '%s\\n' "$DEMO_FAKE_UID"
        ;;
    -g)
        if [[ "${DEMO_ID_FAILURE:-}" == "gid" ]]; then exit 47; fi
        printf '%s\\n' "$DEMO_FAKE_GID"
        ;;
    *)
        exit 48
        ;;
esac
""",
    )
    if include_docker:
        _write_executable(
            fake_bin / "docker",
            """#!/usr/bin/env bash
set -euo pipefail
printf '%s\\t%s\\t%s\\t%s\\n' "${DEMO_ANCHOR_DATE:-}" "${DEMO_UID:-}" "${DEMO_GID:-}" "$*" >>"$DEMO_DOCKER_LOG"

case "$*" in
    "compose version")
        if [[ "${DEMO_COMPOSE_SUPPORT:-}" != "plugin" ]]; then exit 41; fi
        ;;
    "compose -f examples/demo/docker-compose.yml build demo-generator chitragupta chitragupta-ui")
        if [[ "${DEMO_FAKE_FAILURE:-}" == "build" ]]; then exit 42; fi
        ;;
    "compose -f examples/demo/docker-compose.yml run --rm demo-generator")
        if [[ "${DEMO_FAKE_FAILURE:-}" == "generator" ]]; then exit 43; fi
        printf 'GENERATED\\n'
        ;;
    "compose -f examples/demo/docker-compose.yml up --detach --wait chitragupta chitragupta-ui")
        case "${DEMO_FAKE_FAILURE:-}" in
            api-health)
                printf 'chitragupta failed its health check\\n' >&2
                exit 44
                ;;
            ui-health)
                printf 'chitragupta-ui failed its health check\\n' >&2
                exit 45
                ;;
        esac
        printf 'SERVICES_HEALTHY\\n'
        ;;
    *)
        exit 49
esac
""",
        )
    if include_docker_compose:
        _write_executable(
            fake_bin / "docker-compose",
            """#!/usr/bin/env bash
set -euo pipefail
printf '%s\\t%s\\t%s\\t%s\\n' "${DEMO_ANCHOR_DATE:-}" "${DEMO_UID:-}" "${DEMO_GID:-}" "$*" >>"$DEMO_DOCKER_LOG"

case "$*" in
    "version")
        if [[ "${DEMO_COMPOSE_SUPPORT:-}" != "standalone" ]]; then exit 51; fi
        ;;
    "-f examples/demo/docker-compose.yml build demo-generator chitragupta chitragupta-ui")
        if [[ "${DEMO_FAKE_FAILURE:-}" == "build" ]]; then exit 52; fi
        ;;
    "-f examples/demo/docker-compose.yml run --rm demo-generator")
        if [[ "${DEMO_FAKE_FAILURE:-}" == "generator" ]]; then exit 53; fi
        printf 'GENERATED\\n'
        ;;
    "-f examples/demo/docker-compose.yml up --detach --wait chitragupta chitragupta-ui")
        case "${DEMO_FAKE_FAILURE:-}" in
            api-health)
                printf 'chitragupta failed its health check\\n' >&2
                exit 54
                ;;
            ui-health)
                printf 'chitragupta-ui failed its health check\\n' >&2
                exit 55
                ;;
        esac
        printf 'SERVICES_HEALTHY\\n'
        ;;
    *)
        exit 56
esac
""",
        )

    bash_path = shutil.which("bash")
    assert bash_path is not None, "bash is required to exercise the public shell launcher"
    isolated_bin = tmp_path / "runtime-bin"
    isolated_bin.mkdir()
    shutil.copy2(bash_path, isolated_bin / "bash")
    mkdir_path = shutil.which("mkdir")
    assert mkdir_path is not None, "mkdir is required to exercise the public shell launcher"
    shutil.copy2(mkdir_path, isolated_bin / "mkdir")
    runtime_bin = isolated_bin
    environment = os.environ.copy()
    environment.update(
        {
            "DEMO_DATE_LOG": str(date_log),
            "DEMO_DOCKER_LOG": str(command_log),
            "DEMO_ID_LOG": str(id_log),
            "DEMO_FAKE_ANCHOR": anchor,
            "DEMO_FAKE_UID": uid,
            "DEMO_FAKE_GID": gid,
            "DEMO_COMPOSE_SUPPORT": compose_support,
            "PATH": f"{fake_bin}:{runtime_bin}",
        }
    )
    if failure is not None:
        environment["DEMO_FAKE_FAILURE"] = failure
    if id_failure is not None:
        environment["DEMO_ID_FAILURE"] = id_failure
    return environment, command_log, date_log


def _run_demo(workspace: Path, environment: dict[str, str], *arguments: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [str(workspace / "demo"), *arguments],
        cwd=workspace,
        env=environment,
        capture_output=True,
        text=True,
        check=False,
    )


def _docker_calls(command_log: Path) -> list[tuple[str, str, str, tuple[str, ...]]]:
    if not command_log.exists():
        return []
    return [
        (anchor, uid, gid, tuple(command.split()))
        for line in command_log.read_text(encoding="utf-8").splitlines()
        for anchor, uid, gid, command in [line.split("\t", maxsplit=3)]
    ]


def _date_calls(date_log: Path) -> list[str]:
    if not date_log.exists():
        return []
    return date_log.read_text(encoding="utf-8").splitlines()


def _combined_output(result: subprocess.CompletedProcess[str]) -> str:
    return f"{result.stdout}{result.stderr}"


def _normalized_command(command: tuple[str, ...]) -> tuple[str, ...]:
    return command[1:] if command[:1] == ("compose",) else command


def _generator_calls(
    calls: list[tuple[str, str, str, tuple[str, ...]]],
) -> Iterator[tuple[int, str, str, str, tuple[str, ...]]]:
    for index, (anchor, uid, gid, command) in enumerate(calls):
        if _normalized_command(command) == GENERATOR_COMMAND:
            yield index, anchor, uid, gid, command


def _up_calls(
    calls: list[tuple[str, str, str, tuple[str, ...]]],
) -> Iterator[tuple[int, str, str, str, tuple[str, ...]]]:
    for index, (anchor, uid, gid, command) in enumerate(calls):
        if _normalized_command(command) == FINAL_UP_COMMAND:
            yield index, anchor, uid, gid, command


def _selected_command(form: str, command: tuple[str, ...]) -> tuple[str, ...]:
    return ("compose", *command) if form == "plugin" else command


def test_demo_rejects_arguments_before_invoking_docker(tmp_path: Path) -> None:
    workspace = _copy_public_demo(tmp_path)
    environment, command_log, _date_log = _fake_command_environment(tmp_path, anchor="2026-09-02")

    result = _run_demo(workspace, environment, "--profile", "showcase")

    assert result.returncode != 0
    assert _combined_output(result).strip() == "Usage: ./demo"
    assert _docker_calls(command_log) == []


def test_demo_reports_when_docker_is_missing(tmp_path: Path) -> None:
    workspace = _copy_public_demo(tmp_path)
    environment, command_log, _date_log = _fake_command_environment(
        tmp_path,
        anchor="2026-09-02",
        include_docker=False,
        include_docker_compose=False,
    )

    result = _run_demo(workspace, environment)

    assert result.returncode != 0
    assert "Docker with Compose support is required" in _combined_output(result)
    assert _docker_calls(command_log) == []


def test_demo_reports_when_docker_has_no_compose_support(tmp_path: Path) -> None:
    workspace = _copy_public_demo(tmp_path)
    environment, command_log, _date_log = _fake_command_environment(
        tmp_path,
        anchor="2026-09-02",
        compose_support="none",
    )

    result = _run_demo(workspace, environment)

    assert result.returncode != 0
    assert "Docker with Compose support is required" in _combined_output(result)
    assert _docker_calls(command_log) == [
        ("", "", "", ("compose", "version")),
        ("", "", "", ("version",)),
    ]


@pytest.mark.parametrize(
    ("uid", "gid", "id_failure"),
    [
        ("not-numeric", "4343", None),
        ("4242", "4343", "gid"),
    ],
    ids=["non-numeric-uid", "gid-command-failure"],
)
def test_demo_rejects_an_unusable_numeric_identity_before_state_creation(
    tmp_path: Path,
    uid: str,
    gid: str,
    id_failure: str | None,
) -> None:
    workspace = _copy_public_demo(tmp_path)
    environment, command_log, date_log = _fake_command_environment(
        tmp_path,
        anchor="2026-09-02",
        uid=uid,
        gid=gid,
        id_failure=id_failure,
    )

    result = _run_demo(workspace, environment)

    assert result.returncode != 0
    assert "Numeric UID/GID could not be determined" in _combined_output(result)
    assert [call[3] for call in _docker_calls(command_log)] == [("compose", "version")]
    assert _date_calls(date_log) == []
    assert not (workspace / ".demo").exists()


def test_demo_stops_before_generation_when_image_build_fails(tmp_path: Path) -> None:
    workspace = _copy_public_demo(tmp_path)
    environment, command_log, _date_log = _fake_command_environment(tmp_path, anchor="2026-09-02", failure="build")

    result = _run_demo(workspace, environment)
    calls = _docker_calls(command_log)

    assert result.returncode == 42
    assert [command for _anchor, _uid, _gid, command in calls] == [
        ("compose", "version"),
        _selected_command("plugin", BUILD_COMMAND),
    ]
    assert list(_generator_calls(calls)) == []
    assert list(_up_calls(calls)) == []
    assert UI_URL not in _combined_output(result)
    assert API_URL not in _combined_output(result)


def test_demo_stops_before_api_and_ui_startup_when_generation_fails(tmp_path: Path) -> None:
    workspace = _copy_public_demo(tmp_path)
    environment, command_log, _date_log = _fake_command_environment(
        tmp_path,
        anchor="2026-09-02",
        failure="generator",
    )

    result = _run_demo(workspace, environment)
    calls = _docker_calls(command_log)

    assert result.returncode != 0
    assert [command for _anchor, _uid, _gid, command in calls] == [
        ("compose", "version"),
        _selected_command("plugin", BUILD_COMMAND),
        _selected_command("plugin", GENERATOR_COMMAND),
    ]
    assert list(_generator_calls(calls)) == [
        (2, "2026-09-02", "4242", "4343", _selected_command("plugin", GENERATOR_COMMAND))
    ]
    assert list(_up_calls(calls)) == []
    assert UI_URL not in _combined_output(result)
    assert API_URL not in _combined_output(result)


@pytest.mark.parametrize(
    ("failure", "failure_detail"),
    [
        ("api-health", "chitragupta failed its health check"),
        ("ui-health", "chitragupta-ui failed its health check"),
    ],
)
def test_demo_does_not_report_success_when_a_service_health_check_fails(
    tmp_path: Path,
    failure: str,
    failure_detail: str,
) -> None:
    workspace = _copy_public_demo(tmp_path)
    environment, command_log, _date_log = _fake_command_environment(
        tmp_path,
        anchor="2026-09-02",
        failure=failure,
    )

    result = _run_demo(workspace, environment)
    calls = _docker_calls(command_log)

    assert result.returncode != 0
    assert failure_detail in _combined_output(result)
    assert calls[-1][3] == _selected_command("plugin", FINAL_UP_COMMAND)
    assert UI_URL not in _combined_output(result)
    assert API_URL not in _combined_output(result)


@pytest.mark.parametrize("compose_form", ["plugin", "standalone"])
@pytest.mark.parametrize("anchor", ["2026-09-02", "2026-09-03"])
def test_demo_selects_one_compose_form_and_reuses_it_for_build_generation_and_startup(
    tmp_path: Path,
    compose_form: str,
    anchor: str,
) -> None:
    workspace = _copy_public_demo(tmp_path)
    environment, command_log, date_log = _fake_command_environment(
        tmp_path,
        anchor=anchor,
        compose_support=compose_form,
    )

    result = _run_demo(workspace, environment)
    calls = _docker_calls(command_log)
    output = _combined_output(result)
    generator_calls = list(_generator_calls(calls))
    up_calls = list(_up_calls(calls))

    assert result.returncode == 0
    assert _date_calls(date_log) == ["-u +%F"]
    expected_probe_commands = [("compose", "version")]
    if compose_form == "standalone":
        expected_probe_commands.append(("version",))
    assert [call[3] for call in calls] == [
        *expected_probe_commands,
        _selected_command(compose_form, BUILD_COMMAND),
        _selected_command(compose_form, GENERATOR_COMMAND),
        _selected_command(compose_form, FINAL_UP_COMMAND),
    ]
    assert generator_calls == [
        (len(expected_probe_commands) + 1, anchor, "4242", "4343", _selected_command(compose_form, GENERATOR_COMMAND))
    ]
    assert up_calls == [
        (len(expected_probe_commands) + 2, anchor, "4242", "4343", _selected_command(compose_form, FINAL_UP_COMMAND))
    ]
    assert generator_calls[0][0] < up_calls[0][0]
    assert all("demo-generator" not in command for _index, _anchor, _uid, _gid, command in up_calls)
    operation_calls = calls[len(expected_probe_commands) :]
    assert all(call[1:3] == ("4242", "4343") for call in operation_calls)
    assert output.index("GENERATED") < output.index("SERVICES_HEALTHY") < output.index(UI_URL) < output.index(API_URL)


def _available_compose_command() -> list[str] | None:
    docker = shutil.which("docker")
    if docker is not None:
        plugin = subprocess.run(
            [docker, "compose", "version"],
            capture_output=True,
            text=True,
            check=False,
            timeout=30,
        )
        if plugin.returncode == 0:
            return [docker, "compose"]

    standalone = shutil.which("docker-compose")
    if standalone is not None:
        probe = subprocess.run(
            [standalone, "version"],
            capture_output=True,
            text=True,
            check=False,
            timeout=30,
        )
        if probe.returncode == 0:
            return [standalone]
    return None


def test_demo_compose_renders_non_default_numeric_users_and_internal_network() -> None:
    compose_command = _available_compose_command()
    if compose_command is None:
        pytest.skip("Docker Compose is not available")

    environment = os.environ.copy()
    environment.update({"DEMO_UID": "4242", "DEMO_GID": "4343"})
    rendered = subprocess.run(
        [
            *compose_command,
            "-f",
            str(PROJECT_ROOT / COMPOSE_FILE),
            "config",
        ],
        cwd=PROJECT_ROOT,
        env=environment,
        capture_output=True,
        text=True,
        check=False,
        timeout=60,
    )

    assert rendered.returncode == 0, rendered.stderr
    config = yaml.safe_load(rendered.stdout)
    services = config["services"]
    assert services["demo-generator"]["user"] == "4242:4343"
    assert services["chitragupta"]["user"] == "4242:4343"
    assert config["networks"]["demo"]["internal"] is True


def test_demo_compose_uses_the_application_generator_with_two_distinct_tenant_databases() -> None:
    compose = yaml.safe_load((PROJECT_ROOT / COMPOSE_FILE).read_text(encoding="utf-8"))
    config = yaml.safe_load((PROJECT_ROOT / "examples/demo/config.yaml").read_text(encoding="utf-8"))
    tenants = list(config["tenants"].values())
    connection_strings = {tenant["storage"]["connection_string"] for tenant in tenants}

    assert compose["services"]["demo-generator"]["entrypoint"] == ["python", "-m", "demo.generator"]
    assert [tenant["ecosystem"] for tenant in tenants] == ["confluent_cloud", "self_managed_kafka"]
    assert len(connection_strings) == 2
    assert all(connection_string.startswith("sqlite:////app/data/") for connection_string in connection_strings)


def test_shipped_demo_config_validates_ccloud_topic_attribution_metrics() -> None:
    from plugins.confluent_cloud.config import CCloudPluginConfig

    config = yaml.safe_load((PROJECT_ROOT / "examples/demo/config.yaml").read_text(encoding="utf-8"))
    ccloud = CCloudPluginConfig.from_plugin_settings(config["tenants"]["clean-confluent"]["plugin_settings"])

    assert ccloud.topic_attribution.enabled is True
    assert ccloud.metrics is not None
    assert ccloud.metrics.url == "http://prometheus.invalid:9090"
