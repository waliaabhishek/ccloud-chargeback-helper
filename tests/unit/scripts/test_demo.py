from __future__ import annotations

import hashlib
import json
import os
import shutil
import socket
import struct
import subprocess
import sys
import zlib
from pathlib import Path
from typing import Any

import pytest
import yaml

PROJECT_ROOT = Path(__file__).resolve().parents[3]
BASE = "examples/demo/docker-compose.yml"
GRAFANA = "examples/demo/docker-compose.grafana.yml"
MEDIA = "examples/demo/docker-compose.media.yml"
BASE_FILES = ("-f", BASE)
ALL_FILES = ("-f", BASE, "-f", GRAFANA)
MEDIA_FILES = ("-f", BASE, "-f", MEDIA)
MEDIA_PROJECT = ("-p", "chitragupta-demo-media")
MEDIA_SPEC_PATH = "/opt/chitragupta-demo-media/capture-spec.json"
MEDIA_ROOT_PATH = "/app/media"
GRAFANA_PLUGIN_ENV_PATH = "examples/shared/grafana/plugins.env"
GRAFANA_PLUGIN_ENV_REFERENCE = "../shared/grafana/plugins.env"
GRAFANA_PLUGIN_ENV_KEY = "GF_PLUGINS_PREINSTALL_SYNC"
EXPECTED_GRAFANA_PLUGIN_PINS = "frser-sqlite-datasource@4.0.6,marcusolsson-treemap-panel@2.1.1"
UPDATED_GRAFANA_PLUGIN_PINS = "frser-sqlite-datasource@4.0.7,marcusolsson-treemap-panel@2.1.2"
GRAFANA_PLUGIN_INSTALL_KEYS = frozenset({"GF_INSTALL_PLUGINS", "GF_PLUGINS_PREINSTALL", GRAFANA_PLUGIN_ENV_KEY})
GRAFANA_COMPOSE_STACKS = (
    ("demo", (BASE, GRAFANA)),
    ("dev", ("examples/dev/docker-compose.yml",)),
    ("ccloud-full", ("examples/ccloud-full/docker-compose.yml",)),
    ("ccloud-grafana", ("examples/ccloud-grafana/docker-compose.yml",)),
    ("self-managed-full", ("examples/self-managed-full/docker-compose.yml",)),
)
GRAFANA_EXAMPLE_ENV_FILES = (
    "examples/dev/.env.example",
    "examples/ccloud-full/.env.example",
    "examples/ccloud-grafana/.env.example",
    "examples/self-managed-full/.env.example",
)


def _write_executable(path: Path, contents: str) -> None:
    path.write_text(contents, encoding="utf-8")
    path.chmod(0o755)


def _copy_public_demo(tmp_path: Path) -> Path:
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    shutil.copy2(PROJECT_ROOT / "demo", workspace / "demo")
    shutil.copytree(PROJECT_ROOT / "examples" / "demo", workspace / "examples" / "demo")
    shared_grafana = workspace / "examples" / "shared" / "grafana"
    shared_grafana.mkdir(parents=True)
    shutil.copy2(PROJECT_ROOT / GRAFANA_PLUGIN_ENV_PATH, shared_grafana / "plugins.env")
    return workspace


def _copy_grafana_compose_fixture(tmp_path: Path) -> Path:
    workspace = tmp_path / "grafana-compose-fixture"
    for _stack_name, compose_files in GRAFANA_COMPOSE_STACKS:
        for compose_file in compose_files:
            destination = workspace / compose_file
            destination.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(PROJECT_ROOT / compose_file, destination)
    for environment_example in GRAFANA_EXAMPLE_ENV_FILES:
        destination = workspace / Path(environment_example).with_suffix("")
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(PROJECT_ROOT / environment_example, destination)
    shared_plugins = workspace / GRAFANA_PLUGIN_ENV_PATH
    shared_plugins.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(PROJECT_ROOT / GRAFANA_PLUGIN_ENV_PATH, shared_plugins)
    return workspace


def _write_proc_table(path: Path, rows: tuple[tuple[str, int, int], ...]) -> None:
    lines = ["  sl  local_address rem_address   st tx_queue rx_queue tr tm->when retrnsmt   uid  timeout inode\n"]
    lines.extend(
        f"{index:4d}: {address}:{port:04X} 00000000:0000 0A 00000000:00000000 00:00000000 00000000 0 0 {inode}\n"
        for index, (address, port, inode) in enumerate(rows)
    )
    path.write_text("".join(lines), encoding="utf-8")


def _fake_environment(
    tmp_path: Path,
    *,
    compose_form: str = "plugin",
    failure: str = "",
    git_tag: str = "",
    git_available: bool = True,
    platform: str = "Linux",
    lan_address: str = "192.168.50.20",
    docker_ports: str = "",
    lsof_output: str = "",
    lsof_status: int = 0,
    lsof_stderr: str = "",
    lsof_requires_untruncated_command: bool = False,
    include_lsof: bool = True,
    ss_output: str | None = None,
    fuser_output: str | None = None,
    interactive_services: str = "",
    interactive_service_ids: str = "",
    media_services: str = "",
    media_service_ids: str = "",
    git_head: str = "a" * 40,
    git_status: str = "",
    git_describe_status: int = 128,
    git_describe_output: str | None = None,
    gh_available: bool = True,
    gh_release_exists: bool = False,
    gh_stable_release_exists: bool | None = None,
    gh_failure: str = "",
    media_outputs: bool = False,
    media_fixture: Path | None = None,
    cat_failure: bool = False,
    dispatch_real_media_validator: bool = False,
    require_stop_before_manifest: bool = False,
) -> tuple[dict[str, str], Path]:
    """Copy only host-process boundaries; launcher and Compose files stay real."""
    fake_bin = tmp_path / "fake-bin"
    fake_bin.mkdir()
    runtime_bin = tmp_path / "runtime-bin"
    runtime_bin.mkdir()
    command_log = tmp_path / "commands.log"
    host_log = tmp_path / "host.log"
    proc_root = tmp_path / "proc"
    (proc_root / "net").mkdir(parents=True)
    _write_proc_table(proc_root / "net" / "tcp", ())
    _write_proc_table(proc_root / "net" / "tcp6", ())

    handler = fake_bin / "compose-handler"
    _write_executable(
        handler,
        """#!/usr/bin/env bash
set -euo pipefail
printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
  "$DEMO_FORM" "$DEMO_IMAGE_TAG" "$DEMO_BIND_ADDRESS" \
  "$DEMO_UI_PORT" "$DEMO_API_PORT" "$DEMO_GRAFANA_PORT" \
  "$DEMO_PROFILE" "$DEMO_STATE_DIR" "$*" >>"$DEMO_COMMAND_LOG"
printf '%s\t%s\n' "$DEMO_ANCHOR_DATE" "$DEMO_UID:$DEMO_GID" >>"$DEMO_RUNTIME_LOG"
printf '%s\t%s\n' "${DEMO_MEDIA_DIR:-}" "${COMPOSE_PROJECT_NAME:-}" >>"$DEMO_MEDIA_RUNTIME_LOG"
if [[ "$1" == version ]]; then [[ "$DEMO_FORM" == "$DEMO_SUPPORT" ]]; exit; fi
command=" $* "
is_media_project=0
[[ "$command" == *" chitragupta-demo-media "* ]] && is_media_project=1
if [[ "$command" == *" port "* ]]; then printf '%s' "$DEMO_OWNED_PORT"; exit; fi
if [[ "$command" == *" pull "* ]]; then
  [[ "$DEMO_FAILURE" != pull ]] || { echo "pull denied" >&2; exit 71; }
  echo PULLED; exit
fi
if [[ "$command" == *" build "* ]]; then
  [[ "$DEMO_FAILURE" != build ]] || { echo "build denied" >&2; exit 72; }
  echo BUILT; exit
fi
if [[ "$command" == *" run "* ]]; then
  if [[ "$command" == *" media-tool validate "* && "$DEMO_DISPATCH_REAL_MEDIA_VALIDATOR" == 1 ]]; then
    "$DEMO_TEST_PYTHON" "$DEMO_MEDIA_TOOL_PATH" validate \
      --spec "$DEMO_MEDIA_SPEC" --media-root "$DEMO_MEDIA_DIR"
    exit $?
  fi
  case "$DEMO_FAILURE" in
    media-spec) [[ "$command" != *" media-tool spec "* ]] || { echo "invalid media spec" >&2; exit 80; } ;;
    media-catalog) [[ "$command" != *" media-tool catalog "* ]] || { echo "catalog rejected" >&2; exit 81; } ;;
    media-capture) [[ "$command" != *" media-capture "* ]] || { echo "capture failed" >&2; exit 82; } ;;
    media-encoder) [[ "$command" != *" media-encoder "* ]] || { echo "encoder failed" >&2; exit 83; } ;;
    media-manifest) [[ "$command" != *" media-tool manifest "* ]] || { echo "manifest rejected" >&2; exit 84; } ;;
  esac
  if [[ "$DEMO_FAILURE" == media-final-catalog && "$command" == *" media-tool catalog "* ]]; then
    if [[ -f "$DEMO_MEDIA_CATALOG_MARKER" ]]; then
      echo "final catalog rejected" >&2
      exit 86
    fi
    : >"$DEMO_MEDIA_CATALOG_MARKER"
  fi
  [[ "$DEMO_FAILURE" != generator ]] || { echo "generator failed" >&2; exit 73; }
  if [[ "$command" == *" media-tool spec "* ]]; then
    printf '%s\n' '{"profile":"showcase","anchor_date":"2026-08-31"}'
    exit
  fi
  if [[ "$DEMO_MEDIA_OUTPUTS" == 1 && "$command" == *" media-tool manifest "* ]]; then
    if [[ "$DEMO_REQUIRE_STOP_BEFORE_MANIFEST" == 1 && ! -f "$DEMO_MEDIA_DIR/.api-stopped" ]]; then
      echo "manifest ran before API/UI stop" >&2
      exit 85
    fi
    if [[ -n "$DEMO_MEDIA_FIXTURE" ]]; then
      mkdir -p "$DEMO_MEDIA_DIR"
      cp -R "$DEMO_MEDIA_FIXTURE"/. "$DEMO_MEDIA_DIR/"
      echo GENERATED; exit
    fi
    mkdir -p "$DEMO_MEDIA_DIR/assets"
    for asset in chitragupta-demo-dashboard.png chitragupta-demo-cost-explorer.png \
      chitragupta-demo-topic-attribution.png chitragupta-demo-pipeline-status.png \
      chitragupta-demo-focus-mapping-preview.png chitragupta-demo-dashboard-poster.webp \
      chitragupta-demo-walkthrough.mp4; do
      printf 'generated-%s' "$asset" >"$DEMO_MEDIA_DIR/assets/$asset"
    done
    printf '{"generated": true}\\n' >"$DEMO_MEDIA_DIR/manifest.json"
  fi
  echo GENERATED; exit
fi
if [[ "$command" == *" up "* ]]; then
  case "$DEMO_FAILURE" in
    up) echo "chitragupta-ui failed its health check" >&2; exit 74 ;;
    up-bind) echo "chitragupta-ui: Bind for 0.0.0.0:8081 failed: port is already allocated" >&2; exit 75 ;;
    up-bind-local) echo "chitragupta-ui: Bind for 127.0.0.1:8081 failed: port is already allocated" >&2; exit 75 ;;
    up-bind-prefix) echo "chitragupta-ui: Bind for 0.0.0.0:80810 failed: port is already allocated" >&2; exit 75 ;;
    up-bind-api-hyphenated)
      printf '%s%s\n' \
        "Error response from daemon: driver failed programming external connectivity " \
        "on endpoint chitragupta-chitragupta-1 (abc123): Bind for 127.0.0.1:8080 failed: port is already allocated" >&2
      exit 75
      ;;
    up-bind-ui-hyphenated)
      printf '%s%s%s\n' \
        "Error response from daemon: driver failed programming external connectivity " \
        "on endpoint chitragupta-chitragupta-ui-1 (def456): " \
        "Bind for 127.0.0.1:8081 failed: port is already allocated" >&2
      exit 75
      ;;
    grafana-health) echo "grafana failed its health check" >&2; exit 78 ;;
  esac
  echo SERVICES_HEALTHY; exit
fi
if [[ "$command" == *" ps "* ]]; then
  [[ "$DEMO_FAILURE" != operation ]] || { echo "Compose operation failed" >&2; exit 76; }
  if ((is_media_project)); then
    if [[ "$command" == *" --quiet "* ]]; then
      printf '%s' "$DEMO_MEDIA_SERVICE_IDS"
    else
      printf '%s' "$DEMO_MEDIA_SERVICES"
    fi
  else
    if [[ "$command" == *" --quiet "* ]]; then
      printf '%s' "$DEMO_INTERACTIVE_SERVICE_IDS"
    else
      printf '%s' "$DEMO_INTERACTIVE_SERVICES"
    fi
  fi
  exit
fi
if [[ "$command" == *" logs "* || "$command" == *" down "* || "$command" == *" stop "* ]]; then
  [[ "$DEMO_FAILURE" != operation ]] || { echo "Compose operation failed" >&2; exit 76; }
  [[ "$DEMO_FAILURE" != teardown || "$command" != *" down "* ]] || { echo "media teardown failed" >&2; exit 79; }
  if [[ "$DEMO_REQUIRE_STOP_BEFORE_MANIFEST" == 1 && "$command" == *" stop "* ]]; then
    mkdir -p "$DEMO_MEDIA_DIR"
    : >"$DEMO_MEDIA_DIR/.api-stopped"
  fi
  echo COMPOSE_OPERATION; exit
fi
echo "Unsupported fake Compose command: $*" >&2; exit 77
""",
    )
    _write_executable(
        fake_bin / "docker",
        """#!/usr/bin/env bash
set -euo pipefail
if [[ "$1" == compose ]]; then shift; DEMO_FORM=plugin exec "$DEMO_HANDLER" "$@"; fi
if [[ "$1" == ps ]]; then printf '%s' "$DEMO_DOCKER_PORTS"; fi
""",
    )
    _write_executable(
        fake_bin / "docker-compose",
        """#!/usr/bin/env bash
set -euo pipefail
DEMO_FORM=standalone exec "$DEMO_HANDLER" "$@"
""",
    )
    _write_executable(
        fake_bin / "git",
        """#!/usr/bin/env bash
set -euo pipefail
[[ "$DEMO_GIT_AVAILABLE" == 1 ]] || exit 127
printf '%s\\n' "$*" >>"$DEMO_GIT_LOG"
if [[ "$*" == "rev-parse HEAD" ]]; then echo "$DEMO_GIT_HEAD"; exit; fi
if [[ "$*" == "status --porcelain" ]]; then printf '%s' "$DEMO_GIT_STATUS"; exit; fi
if [[ "$*" == "describe --tags --exact-match --match v*.*.* HEAD" ]]; then
  if [[ -n "$DEMO_GIT_TAG" ]]; then echo "$DEMO_GIT_TAG"; exit; fi
  printf '%s\n' "$DEMO_GIT_DESCRIBE_OUTPUT" >&2
  exit "$DEMO_GIT_DESCRIBE_STATUS"
fi
exit 1
""",
    )
    _write_executable(
        fake_bin / "gh",
        """#!/usr/bin/env bash
set -euo pipefail
[[ "$DEMO_GH_AVAILABLE" == 1 ]] || exit 127
printf '%s\\n' "$*" >>"$DEMO_GH_LOG"
case "$DEMO_GH_FAILURE" in
  stable-view) [[ "$*" != *"release view v"* ]] || exit 5 ;;
  release-view) [[ "$*" != *"release view demo-media"* ]] || exit 6 ;;
  release-create) [[ "$*" != *"release create"* ]] || exit 7 ;;
  release-edit) [[ "$*" != *"release edit"* ]] || exit 8 ;;
  release-upload) [[ "$*" != *"release upload"* ]] || exit 9 ;;
esac
if [[ "$*" == *"release view demo-media"* ]]; then
  [[ "$*" == "release view demo-media --json isDraft,isPrerelease,url" ]] || {
    echo "unsupported demo-media release view fields" >&2
    exit 10
  }
  [[ "$DEMO_GH_RELEASE_EXISTS" == 1 ]] || exit 1
fi
if [[ "$*" == *"release view v"* && "$DEMO_GH_STABLE_RELEASE_EXISTS" != 1 ]]; then exit 1; fi
if [[ "$*" == *"release view"* ]]; then
  printf '%s\\n' '{"isDraft":false,"isPrerelease":false,"url":"https://example.test/demo-media"}'
fi
if [[ "$*" == *"release create"* || "$*" == *"release edit"* || "$*" == *"release upload"* ]]; then
  printf '%s\\n' 'https://example.test/demo-media'
fi
""",
    )
    if not gh_available:
        (fake_bin / "gh").unlink()
    _write_executable(
        fake_bin / "id",
        """#!/usr/bin/env bash
[[ "$DEMO_ID_FAILURE" != "$1" ]] || exit 46
[[ "$1" == -u ]] && echo 4242 || { [[ "$1" == -g ]] && echo 4343; }
""",
    )
    _write_executable(
        fake_bin / "date",
        ('#!/usr/bin/env bash\nprintf \'%s\\n\' "$*" >>"$DEMO_DATE_LOG"\n[[ "$*" == "-u +%F" ]] && echo 2026-09-04\n'),
    )
    _write_executable(fake_bin / "uname", f"#!/usr/bin/env bash\necho {platform}\n")
    _write_executable(
        fake_bin / "ip",
        """#!/usr/bin/env bash
[[ -z "$DEMO_LAN_ADDRESS" ]] || echo "1.1.1.1 via 192.168.50.1 dev eth0 src $DEMO_LAN_ADDRESS"
""",
    )
    _write_executable(
        fake_bin / "route",
        "#!/usr/bin/env bash\n[[ -z \"$DEMO_LAN_ADDRESS\" ]] || echo '   interface: en0'\n",
    )
    _write_executable(
        fake_bin / "ifconfig",
        '#!/usr/bin/env bash\n[[ -z "$DEMO_LAN_ADDRESS" ]] || echo "inet $DEMO_LAN_ADDRESS netmask 0xffffff00"\n',
    )
    if include_lsof:
        _write_executable(
            fake_bin / "lsof",
            """#!/usr/bin/env bash
if [[ "$DEMO_LSOF_REQUIRE_UNTRUNCATED_COMMAND" == 1 && " $* " != *" +c 0 "* ]]; then
  printf '%s\n' "lsof: command names may be truncated without +c 0" >&2
  exit 1
fi
printf 'lsof\t%s\n' "$*" >>"$DEMO_HOST_LOG"
printf '%s' "$DEMO_LSOF_OUTPUT"
printf '%s' "$DEMO_LSOF_STDERR" >&2
exit "$DEMO_LSOF_STATUS"
""",
        )
    if ss_output is not None:
        _write_executable(
            fake_bin / "ss",
            """#!/usr/bin/env bash
printf 'ss\t%s\n' "$*" >>"$DEMO_HOST_LOG"
printf '%s' "$DEMO_SS_OUTPUT"
""",
        )
    if fuser_output is not None:
        _write_executable(
            fake_bin / "fuser",
            """#!/usr/bin/env bash
printf 'fuser\t%s\n' "$*" >>"$DEMO_HOST_LOG"
printf '%s' "$DEMO_FUSER_OUTPUT"
""",
        )
    for command in ("awk", "cat", "cp", "cut", "find", "grep", "mkdir", "readlink", "rm", "rmdir", "sed", "sort", "tr"):
        source = shutil.which(command)
        assert source is not None
        shutil.copy2(source, runtime_bin / command)
    if cat_failure:
        _write_executable(
            runtime_bin / "cat",
            """#!/usr/bin/env bash
set -euo pipefail
if [[ "$*" == *"chitragupta-demo-dashboard-poster.webp"* ]]; then
  echo "poster copy failed" >&2
  exit 91
fi
exec /usr/bin/cat "$@"
""",
        )
    bash = shutil.which("bash")
    assert bash is not None
    shutil.copy2(bash, runtime_bin / "bash")

    environment = os.environ.copy()
    environment.update(
        {
            "DEMO_COMMAND_LOG": str(command_log),
            "DEMO_HOST_LOG": str(host_log),
            "DEMO_DATE_LOG": str(tmp_path / "date.log"),
            "DEMO_HANDLER": str(handler),
            "DEMO_SUPPORT": compose_form,
            "DEMO_FAILURE": failure,
            "DEMO_GIT_TAG": git_tag,
            "DEMO_GIT_AVAILABLE": "1" if git_available else "0",
            "DEMO_GIT_HEAD": git_head,
            "DEMO_GIT_STATUS": git_status,
            "DEMO_GIT_DESCRIBE_STATUS": str(git_describe_status),
            "DEMO_GIT_DESCRIBE_OUTPUT": (
                git_describe_output
                if git_describe_output is not None
                else f"fatal: no tag exactly matches '{git_head}'"
            ),
            "DEMO_GIT_LOG": str(tmp_path / "git.log"),
            "DEMO_GH_AVAILABLE": "1" if gh_available else "0",
            "DEMO_GH_LOG": str(tmp_path / "gh.log"),
            "DEMO_GH_RELEASE_EXISTS": "1" if gh_release_exists else "0",
            "DEMO_GH_STABLE_RELEASE_EXISTS": "1" if gh_stable_release_exists else "0",
            "DEMO_GH_FAILURE": gh_failure,
            "DEMO_INTERACTIVE_SERVICES": interactive_services,
            "DEMO_INTERACTIVE_SERVICE_IDS": interactive_service_ids,
            "DEMO_MEDIA_SERVICES": media_services,
            "DEMO_MEDIA_SERVICE_IDS": media_service_ids,
            "DEMO_MEDIA_OUTPUTS": "1" if media_outputs else "0",
            "DEMO_MEDIA_FIXTURE": str(media_fixture) if media_fixture is not None else "",
            "DEMO_MEDIA_CATALOG_MARKER": str(tmp_path / "media-catalog.marker"),
            "DEMO_CAT_FAILURE": "1" if cat_failure else "0",
            "DEMO_DISPATCH_REAL_MEDIA_VALIDATOR": "1" if dispatch_real_media_validator else "0",
            "DEMO_REQUIRE_STOP_BEFORE_MANIFEST": "1" if require_stop_before_manifest else "0",
            "DEMO_TEST_PYTHON": sys.executable,
            "DEMO_MEDIA_TOOL_PATH": str(tmp_path / "workspace" / "examples" / "demo" / "media" / "tool.py"),
            "DEMO_MEDIA_SPEC": str(tmp_path / "workspace" / "examples" / "demo" / "media" / "capture-spec.json"),
            "DEMO_LAN_ADDRESS": lan_address,
            "DEMO_DOCKER_PORTS": docker_ports,
            "DEMO_LSOF_OUTPUT": lsof_output,
            "DEMO_LSOF_STATUS": str(lsof_status),
            "DEMO_LSOF_STDERR": lsof_stderr,
            "DEMO_LSOF_REQUIRE_UNTRUNCATED_COMMAND": "1" if lsof_requires_untruncated_command else "0",
            "DEMO_SS_OUTPUT": ss_output or "",
            "DEMO_FUSER_OUTPUT": fuser_output or "",
            "DEMO_OWNED_PORT": "",
            "DEMO_PROC_ROOT": str(proc_root),
            "DEMO_RUNTIME_LOG": str(tmp_path / "runtime.log"),
            "DEMO_MEDIA_RUNTIME_LOG": str(tmp_path / "media-runtime.log"),
            "DEMO_IMAGE_TAG": "",
            "DEMO_BIND_ADDRESS": "",
            "DEMO_UI_PORT": "",
            "DEMO_API_PORT": "",
            "DEMO_GRAFANA_PORT": "",
            "DEMO_PROFILE": "",
            "DEMO_STATE_DIR": "",
            "DEMO_ANCHOR_DATE": "",
            "DEMO_UID": "",
            "DEMO_GID": "",
            "DEMO_ID_FAILURE": "",
            "PATH": f"{fake_bin}:{runtime_bin}",
        }
    )
    return environment, command_log


def _set_linux_listener(
    environment: dict[str, str],
    *,
    table: str,
    address: str,
    port: int,
    pid: int | None = 4127,
    command: str = "python3",
) -> None:
    proc_root = Path(environment["DEMO_PROC_ROOT"])
    _write_proc_table(proc_root / "net" / table, ((address, port, 9001),))
    if pid is not None:
        fd = proc_root / str(pid) / "fd"
        fd.mkdir(parents=True)
        (fd / "7").symlink_to("socket:[9001]")
        (proc_root / str(pid) / "comm").write_text(f"{command}\n", encoding="utf-8")


def _set_proc_command(environment: dict[str, str], pid: int, command: str) -> None:
    proc_root = Path(environment["DEMO_PROC_ROOT"])
    process_dir = proc_root / str(pid)
    process_dir.mkdir(parents=True, exist_ok=True)
    (process_dir / "comm").write_text(f"{command}\n", encoding="utf-8")


def _run(workspace: Path, environment: dict[str, str], *arguments: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [str(workspace / "demo"), *arguments],
        cwd=workspace,
        env=environment,
        capture_output=True,
        text=True,
        check=False,
    )


def _calls(command_log: Path) -> list[tuple[str, str, str, str, str, str, str, str, tuple[str, ...]]]:
    if not command_log.exists():
        return []
    calls = []
    for line in command_log.read_text(encoding="utf-8").splitlines():
        fields = line.split("\t", maxsplit=8)
        calls.append((*fields[:8], tuple(fields[8].split())))
    return calls


def _commands(command_log: Path) -> list[tuple[str, ...]]:
    return [call[-1] for call in _calls(command_log)]


def _non_port_commands(command_log: Path) -> list[tuple[str, ...]]:
    return [command for command in _commands(command_log) if "port" not in command]


def _non_port_calls(command_log: Path) -> list[tuple[str, str, str, str, str, str, str, str, tuple[str, ...]]]:
    return [call for call in _calls(command_log) if "port" not in call[-1]]


def _output(result: subprocess.CompletedProcess[str]) -> str:
    return f"{result.stdout}{result.stderr}"


def _snapshot(path: Path) -> dict[Path, tuple[bytes, int]]:
    return {
        item.relative_to(path): (item.read_bytes(), item.stat().st_mtime_ns)
        for item in path.rglob("*")
        if item.is_file()
    }


def _up(*, grafana: bool = False) -> tuple[str, ...]:
    files = ALL_FILES if grafana else BASE_FILES
    services = ("chitragupta", "chitragupta-ui", "grafana") if grafana else ("chitragupta", "chitragupta-ui")
    orphan_flag = () if grafana else ("--remove-orphans",)
    return (*files, "up", "--detach", "--wait", "--force-recreate", *orphan_flag, *services)


def _media_compose(*arguments: str) -> tuple[str, ...]:
    return (*MEDIA_PROJECT, *MEDIA_FILES, *arguments)


def _media_commands(command_log: Path) -> list[tuple[str, ...]]:
    return [command for command in _commands(command_log) if MEDIA in command]


def _gh_commands(environment: dict[str, str]) -> list[str]:
    gh_log = Path(environment["DEMO_GH_LOG"])
    if not gh_log.exists():
        return []
    return gh_log.read_text(encoding="utf-8").splitlines()


def _write_fake_publish_workspace(workspace: Path) -> Path:
    media_root = workspace / ".demo" / "media"
    assets = media_root / "assets"
    assets.mkdir(parents=True)
    for name in (
        "chitragupta-demo-dashboard.png",
        "chitragupta-demo-cost-explorer.png",
        "chitragupta-demo-topic-attribution.png",
        "chitragupta-demo-pipeline-status.png",
        "chitragupta-demo-focus-mapping-preview.png",
        "chitragupta-demo-walkthrough.mp4",
    ):
        (assets / name).write_text(name, encoding="utf-8")
    (media_root / "manifest.json").write_text(
        '{"source_commit":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","source_worktree_clean":true}\n',
        encoding="utf-8",
    )
    return media_root


def _write_json(path: Path, value: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2) + "\n", encoding="utf-8")


def _valid_png() -> bytes:
    width, height = 1600, 900

    def chunk(kind: bytes, contents: bytes) -> bytes:
        checksum = struct.pack(">I", zlib.crc32(kind + contents) & 0xFFFFFFFF)
        return struct.pack(">I", len(contents)) + kind + contents + checksum

    row = b"\x00" + (b"\x1a\x2b\x3c" * width)
    header = struct.pack(">IIBBBBB", width, height, 8, 2, 0, 0, 0)
    return b"\x89PNG\r\n\x1a\n" + b"".join(
        (chunk(b"IHDR", header), chunk(b"IDAT", zlib.compress(row * height)), chunk(b"IEND", b""))
    )


def _valid_webp() -> bytes:
    width, height = 960, 540
    payload = b"\x00\x00\x00\x00" + (width - 1).to_bytes(3, "little") + (height - 1).to_bytes(3, "little")
    chunk = b"VP8X" + struct.pack("<I", len(payload)) + payload
    return b"RIFF" + struct.pack("<I", len(b"WEBP") + len(chunk)) + b"WEBP" + chunk


def _write_real_media_fixture(workspace: Path, fixture_root: Path) -> Path:
    media_root = fixture_root / "media"
    assets = media_root / "assets"
    assets.mkdir(parents=True)
    png_names = (
        "chitragupta-demo-dashboard.png",
        "chitragupta-demo-cost-explorer.png",
        "chitragupta-demo-topic-attribution.png",
        "chitragupta-demo-pipeline-status.png",
        "chitragupta-demo-focus-mapping-preview.png",
    )
    for name in png_names:
        (assets / name).write_bytes(_valid_png())
    (assets / "chitragupta-demo-dashboard-poster.webp").write_bytes(_valid_webp())
    (assets / "chitragupta-demo-walkthrough.mp4").write_bytes(b"production-shaped-mp4")
    state = media_root / "state"
    state.mkdir()
    state_metadata = {
        "schema_version": 1,
        "generator_version": 1,
        "profile": "showcase",
        "anchor_date": "2026-08-31",
    }
    _write_json(state / "demo-state.json", state_metadata)
    for name in ("confluent-cloud.db", "self-managed-kafka.db"):
        (state / name).write_bytes(name.encode())
    work = media_root / "work"
    work.mkdir()
    (work / "captions.srt").write_text(
        "1\n00:00:00,000 --> 00:00:13,000\nSynthetic tenant cost reconciles across usage and shared spend.\n\n"
        "2\n00:00:13,000 --> 00:00:30,000\nTenant cost is the analytical root.\n\n"
        "3\n00:00:30,000 --> 00:00:44,000\n"
        "Topic-level attribution exposes cost concentration and candidates for review.\n\n"
        "4\n00:00:44,000 --> 00:00:56,000\nPersisted processing state remains inspectable in API-only mode.\n\n"
        "5\n00:00:56,000 --> 00:01:15,000\nGenerate a FOCUS 1.4 mapping preview from synthetic persisted evidence.\n",
        encoding="utf-8",
    )
    _write_json(
        work / "browser-observations.json",
        {
            "ui_origin": "http://chitragupta-ui",
            "requests": [{"url": "http://chitragupta-ui/dashboard", "kind": "document"}],
            "api_identifiers_match_catalog": True,
            "dom_identifiers_match_catalog": True,
        },
    )
    _write_json(
        work / "encoder-result.json",
        {
            "duration_seconds": 75,
            "video_codec": "h264",
            "width": 1600,
            "height": 900,
            "frame_rate": 30,
            "audio_stream_count": 0,
            "captions_burned_in": True,
            "caption_filter": "subtitles",
            "caption_source": "captions.srt",
            "webm_removed": True,
        },
    )
    source_identifiers = [
        "11111111-1111-4111-8111-111111111111",
        "lkc-commerce:topic:orders.created.v1",
    ]
    evidence_files = []
    for name in ("demo-state.json", "confluent-cloud.db", "self-managed-kafka.db"):
        path = state / name
        evidence_files.append(
            {"path": name, "sha256": hashlib.sha256(path.read_bytes()).hexdigest(), "bytes": path.stat().st_size}
        )
    _write_json(
        work / "synthetic-catalog.json",
        {
            "schema_version": 1,
            "anchor_date": "2026-08-31",
            "profile": "showcase",
            "primary_tenant": {
                "name": "clean-confluent",
                "id": "northstar-confluent",
                "ecosystem": "confluent_cloud",
            },
            "representative_identifiers": ["cluster-commerce", "topic-orders"],
            "source_identifiers": source_identifiers,
            "tenants": [],
            "scenarios": [
                {"scenario": {"organization_id": source_identifiers[0], "resource_id": source_identifiers[1]}}
            ],
            "database_evidence": {
                "schema_version": 1,
                "validated": True,
                "state_metadata": state_metadata,
                "files": evidence_files,
            },
        },
    )
    result = subprocess.run(
        [
            sys.executable,
            str(workspace / "examples/demo/media/tool.py"),
            "manifest",
            "--spec",
            str(workspace / "examples/demo/media/capture-spec.json"),
            "--media-root",
            str(media_root),
            "--source-commit",
            "a" * 40,
            "--source-worktree-clean",
            "true",
        ],
        cwd=workspace,
        env={**os.environ, "PYTHONPATH": str(PROJECT_ROOT / "src")},
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr
    return media_root


@pytest.mark.parametrize(
    "arguments",
    [
        ("--profile", "showcase"),
        ("reset", "--lan"),
        ("status", "chitragupta"),
        ("--showcase", "reset"),
    ],
)
def test_demo_rejects_unsupported_grammar_before_docker_or_state_mutation(
    tmp_path: Path,
    arguments: tuple[str, ...],
) -> None:
    workspace = _copy_public_demo(tmp_path)
    environment, command_log = _fake_environment(tmp_path)

    result = _run(workspace, environment, *arguments)

    assert result.returncode == 2
    assert "./demo [--showcase] [--grafana] [--lan] [--build]" in _output(result)
    assert _commands(command_log) == []
    assert not (workspace / ".demo").exists()


@pytest.mark.parametrize(
    ("arguments", "switch", "value"),
    [
        (("--ui-port", ""), "--ui-port", ""),
        (("--api-port", "abc"), "--api-port", "abc"),
        (("--ui-port", "0"), "--ui-port", "0"),
        (("--grafana-port", "65536"), "--grafana-port", "65536"),
        (("--ui-port",), "--ui-port", ""),
        (("--api-port", "999999999999999999999999999999"), "--api-port", "999999999999999999999999999999"),
    ],
)
def test_demo_rejects_invalid_port_values_before_docker_or_state_mutation(
    tmp_path: Path,
    arguments: tuple[str, ...],
    switch: str,
    value: str,
) -> None:
    workspace = _copy_public_demo(tmp_path)
    environment, command_log = _fake_environment(tmp_path)

    result = _run(workspace, environment, *arguments)

    assert result.returncode == 2
    assert f"{switch} {value}" in _output(result)
    assert "1..65535" in _output(result)
    assert _commands(command_log) == []
    assert not (workspace / ".demo").exists()


@pytest.mark.parametrize(
    ("arguments", "expected_ui_port", "expected_api_port"),
    [
        (("--ui-port", "1", "--api-port", "65535"), "1", "65535"),
        (("--ui-port", "00001", "--api-port", "065535"), "1", "65535"),
    ],
)
def test_demo_accepts_port_bounds_and_normalizes_leading_zero_decimal_values(
    tmp_path: Path,
    arguments: tuple[str, ...],
    expected_ui_port: str,
    expected_api_port: str,
) -> None:
    workspace = _copy_public_demo(tmp_path)
    environment, command_log = _fake_environment(tmp_path)

    result = _run(workspace, environment, *arguments)

    assert result.returncode == 0
    assert {(call[3], call[4]) for call in _calls(command_log)[1:]} == {(expected_ui_port, expected_api_port)}
    assert f"UI: http://127.0.0.1:{expected_ui_port}" in _output(result)
    assert f"API: http://127.0.0.1:{expected_api_port}" in _output(result)


@pytest.mark.parametrize(
    ("arguments", "detail"),
    [
        (("--ui-port", "8080"), "Port 8080 is requested by both API and UI; choose distinct ports."),
        (
            ("--grafana", "--grafana-port", "8080"),
            "Port 8080 is requested by both API and Grafana; choose distinct ports.",
        ),
        (
            ("--grafana", "--grafana-port", "8081"),
            "Port 8081 is requested by both UI and Grafana; choose distinct ports.",
        ),
    ],
)
def test_demo_rejects_every_duplicate_selected_port_pair_before_host_inspection(
    tmp_path: Path,
    arguments: tuple[str, ...],
    detail: str,
) -> None:
    workspace = _copy_public_demo(tmp_path)
    environment, command_log = _fake_environment(tmp_path)

    result = _run(workspace, environment, *arguments)

    assert result.returncode == 1
    assert detail in _output(result)
    assert _commands(command_log) == [("version",)]


def test_demo_ignores_an_occupied_grafana_port_when_grafana_is_not_selected(tmp_path: Path) -> None:
    workspace = _copy_public_demo(tmp_path)
    environment, command_log = _fake_environment(
        tmp_path,
        docker_ports="other-grafana	abc123	0.0.0.0:3000->3000/tcp\n",
    )

    result = _run(workspace, environment, "--grafana-port", "3000")

    assert result.returncode == 0
    assert _up() in _commands(command_log)
    assert "Grafana:" not in _output(result)


def test_demo_checks_the_current_compose_service_port_before_allowing_recreation(tmp_path: Path) -> None:
    workspace = _copy_public_demo(tmp_path)
    environment, command_log = _fake_environment(tmp_path)
    environment["DEMO_OWNED_PORT"] = "127.0.0.1:8080"

    result = _run(workspace, environment)

    assert result.returncode == 0
    assert (*ALL_FILES, "port", "chitragupta", "8080") in _commands(command_log)
    assert _up() in _commands(command_log)


@pytest.mark.parametrize(
    ("published_binding", "listener_address", "arguments"),
    [
        ("127.0.0.1:8080", "0100007F", ("--lan",)),
        ("0.0.0.0:8080", "00000000", ()),
    ],
)
def test_demo_forced_recreation_allows_its_current_service_docker_proxy_across_bind_transitions(
    tmp_path: Path,
    published_binding: str,
    listener_address: str,
    arguments: tuple[str, ...],
) -> None:
    workspace = _copy_public_demo(tmp_path)
    environment, command_log = _fake_environment(
        tmp_path,
        docker_ports=f"chitragupta-current\tabc123\t{published_binding}->8080/tcp\n",
    )
    environment["DEMO_OWNED_PORT"] = published_binding
    _set_linux_listener(
        environment,
        table="tcp",
        address=listener_address,
        port=8080,
        command="docker-proxy",
    )

    result = _run(workspace, environment, *arguments)

    assert result.returncode == 0
    assert (*ALL_FILES, "port", "chitragupta", "8080") in _commands(command_log)
    assert _up() in _commands(command_log)
    assert "Port 8080 for API is in use" not in _output(result)


def test_demo_allows_current_exact_linux_binding_without_readable_listener_ownership(tmp_path: Path) -> None:
    workspace = _copy_public_demo(tmp_path)
    environment, command_log = _fake_environment(
        tmp_path,
        docker_ports="chitragupta-current\tabc123\t127.0.0.1:18080->8080/tcp\n",
    )
    environment["DEMO_OWNED_PORT"] = "127.0.0.1:18080"
    _set_linux_listener(environment, table="tcp", address="0100007F", port=18080, pid=None)

    result = _run(workspace, environment, "--api-port", "18080", "--grafana")

    assert result.returncode == 0
    assert (*ALL_FILES, "port", "chitragupta", "8080") in _commands(command_log)
    assert _up(grafana=True) in _commands(command_log)
    assert "Port 18080 for API has a non-Docker listener" not in _output(result)


def test_demo_allows_current_localhost_service_but_reports_secondary_external_mapping_for_lan(
    tmp_path: Path,
) -> None:
    mappings = (
        "chitragupta-current\tabc123\t127.0.0.1:8080->8080/tcp\n"
        "secondary-external\tdef456\t10.20.30.40:8080->8080/tcp\n"
    )

    local_root = tmp_path / "local"
    local_root.mkdir()
    local_workspace = _copy_public_demo(local_root)
    local_environment, local_log = _fake_environment(local_root, docker_ports=mappings)
    local_environment["DEMO_OWNED_PORT"] = "127.0.0.1:8080"
    local_result = _run(local_workspace, local_environment)

    lan_root = tmp_path / "lan"
    lan_root.mkdir()
    lan_workspace = _copy_public_demo(lan_root)
    lan_environment, lan_log = _fake_environment(lan_root, docker_ports=mappings)
    lan_environment["DEMO_OWNED_PORT"] = "127.0.0.1:8080"
    lan_result = _run(lan_workspace, lan_environment, "--lan")

    assert local_result.returncode == 0
    assert _up() in _commands(local_log)
    assert lan_result.returncode == 1
    assert "Port 8080 for API is in use by Docker container secondary-external (def456)." in _output(lan_result)
    assert "chitragupta-current" not in _output(lan_result)
    assert not any("pull" in command for command in _commands(lan_log))


@pytest.mark.parametrize(
    ("arguments", "name", "container_id", "mapping", "expected_status"),
    [
        ((), "secondary-api", "a1b2c3", "10.20.30.40:8080->8080/tcp", 0),
        ((), "wildcard-api", "d4e5f6", "0.0.0.0:8080->8080/tcp", 1),
        ((), "loopback-api", "f7e8d9", "127.0.0.1:8080->8080/tcp", 1),
        (("--lan",), "secondary-api", "a1b2c3", "10.20.30.40:8080->8080/tcp", 1),
    ],
)
def test_demo_checks_docker_mapping_address_overlap_before_reporting_external_owners(
    tmp_path: Path,
    arguments: tuple[str, ...],
    name: str,
    container_id: str,
    mapping: str,
    expected_status: int,
) -> None:
    workspace = _copy_public_demo(tmp_path)
    environment, command_log = _fake_environment(
        tmp_path,
        docker_ports=f"{name}\t{container_id}\t{mapping}\n",
    )

    result = _run(workspace, environment, *arguments)

    assert result.returncode == expected_status
    if expected_status:
        assert f"Port 8080 for API is in use by Docker container {name} ({container_id})." in _output(result)
        assert not any("pull" in command for command in _commands(command_log))
    else:
        assert _up() in _commands(command_log)


def test_demo_rejects_a_similarly_named_external_docker_container(tmp_path: Path) -> None:
    workspace = _copy_public_demo(tmp_path)
    environment, command_log = _fake_environment(
        tmp_path,
        docker_ports="chitragupta-shadow	abc123	0.0.0.0:8080->8080/tcp\n",
    )

    result = _run(workspace, environment)

    assert result.returncode == 1
    assert "Port 8080 for API is in use by Docker container chitragupta-shadow (abc123)." in _output(result)
    assert not any("pull" in command for command in _commands(command_log))


def test_demo_composes_startup_switches_and_uses_the_last_port_override(tmp_path: Path) -> None:
    workspace = _copy_public_demo(tmp_path)
    environment, command_log = _fake_environment(tmp_path, git_tag="v2.3.4")

    result = _run(
        workspace,
        environment,
        "--showcase",
        "--grafana",
        "--lan",
        "--build",
        "--ui-port",
        "8011",
        "--api-port",
        "8010",
        "--grafana-port",
        "3010",
        "--ui-port",
        "8111",
    )

    assert result.returncode == 0
    assert _non_port_commands(command_log) == [
        ("version",),
        (*ALL_FILES, "pull", "grafana"),
        (*ALL_FILES, "build", "chitragupta", "chitragupta-ui"),
        (*ALL_FILES, "run", "--rm", "demo-generator"),
        _up(grafana=True),
    ]
    assert {(call[1], call[2], call[3], call[4], call[5], call[6]) for call in _non_port_calls(command_log)[1:]} == {
        ("local", "0.0.0.0", "8111", "8010", "3010", "showcase")
    }
    output = _output(result)
    assert (
        "WARNING: --lan exposes unauthenticated demo services and writable synthetic state to the local network."
        in output
    )
    assert "UI: http://192.168.50.20:8111" in output
    assert "API: http://192.168.50.20:8010" in output
    assert "Grafana: http://192.168.50.20:3010" in output


@pytest.mark.parametrize("compose_form", ["plugin", "standalone"])
@pytest.mark.parametrize("action", ["status", "logs", "down"])
def test_demo_operations_use_the_selected_frontend_and_both_compose_definitions(
    tmp_path: Path,
    compose_form: str,
    action: str,
) -> None:
    workspace = _copy_public_demo(tmp_path)
    environment, command_log = _fake_environment(tmp_path, compose_form=compose_form)

    result = _run(workspace, environment, action)

    probes = [("version",)] if compose_form == "plugin" else [("version",), ("version",)]
    assert result.returncode == 0
    assert _commands(command_log) == [*probes, (*ALL_FILES, "ps" if action == "status" else action)]
    forms = [call[0] for call in _calls(command_log)]
    assert forms == (["plugin", "plugin"] if compose_form == "plugin" else ["plugin", "standalone", "standalone"])
    assert not (workspace / ".demo").exists()


@pytest.mark.parametrize("action", ["status", "logs", "down"])
def test_demo_operations_propagate_compose_failure_without_creating_state(tmp_path: Path, action: str) -> None:
    workspace = _copy_public_demo(tmp_path)
    environment, command_log = _fake_environment(tmp_path, failure="operation")

    result = _run(workspace, environment, action)

    assert result.returncode == 76
    assert "Compose operation failed" in _output(result)
    assert _commands(command_log) == [("version",), (*ALL_FILES, "ps" if action == "status" else action)]
    assert not (workspace / ".demo").exists()


@pytest.mark.parametrize("compose_form", ["plugin", "standalone"])
@pytest.mark.parametrize("tag", ["v2.3.4", "v2.3.4-rc.1", "v12.34.56", ""])
def test_demo_resolves_exact_release_tag_or_latest_into_published_image_environment(
    tmp_path: Path,
    compose_form: str,
    tag: str,
) -> None:
    workspace = _copy_public_demo(tmp_path)
    environment, command_log = _fake_environment(tmp_path, compose_form=compose_form, git_tag=tag)

    result = _run(workspace, environment)

    assert result.returncode == 0
    probes = [("version",)] if compose_form == "plugin" else [("version",), ("version",)]
    assert _non_port_commands(command_log) == [
        *probes,
        (*BASE_FILES, "pull", "chitragupta", "chitragupta-ui"),
        (*BASE_FILES, "run", "--rm", "demo-generator"),
        _up(),
    ]
    forms = [call[0] for call in _non_port_calls(command_log)]
    expected_forms = ["plugin"] * 4
    if compose_form == "standalone":
        expected_forms = ["plugin", "standalone", "standalone", "standalone", "standalone"]
    assert forms == expected_forms
    assert {call[1] for call in _non_port_calls(command_log)[len(probes) :]} == {tag or "latest"}


def test_demo_uses_latest_when_git_is_unavailable(tmp_path: Path) -> None:
    workspace = _copy_public_demo(tmp_path)
    environment, command_log = _fake_environment(tmp_path, git_available=False)

    result = _run(workspace, environment)

    assert result.returncode == 0
    assert {call[1] for call in _non_port_calls(command_log)[1:]} == {"latest"}


def test_demo_builds_current_checkout_without_pulling_released_application_images(tmp_path: Path) -> None:
    workspace = _copy_public_demo(tmp_path)
    environment, command_log = _fake_environment(tmp_path)

    result = _run(workspace, environment, "--build")

    assert result.returncode == 0
    assert _non_port_commands(command_log) == [
        ("version",),
        (*BASE_FILES, "build", "chitragupta", "chitragupta-ui"),
        (*BASE_FILES, "run", "--rm", "demo-generator"),
        _up(),
    ]
    assert {call[1] for call in _non_port_calls(command_log)[1:]} == {"local"}


def test_demo_compose_uses_published_images_checkout_builds_and_parameterized_bindings() -> None:
    compose = yaml.safe_load((PROJECT_ROOT / BASE).read_text(encoding="utf-8"))
    services = compose["services"]

    assert services["demo-generator"]["image"] == "ghcr.io/waliaabhishek/chitragupta:${DEMO_IMAGE_TAG:-latest}"
    assert services["chitragupta"]["image"] == "ghcr.io/waliaabhishek/chitragupta:${DEMO_IMAGE_TAG:-latest}"
    assert services["chitragupta-ui"]["image"] == "ghcr.io/waliaabhishek/chitragupta-ui:${DEMO_IMAGE_TAG:-latest}"
    assert services["chitragupta"]["build"] == {"context": "../..", "dockerfile": "Dockerfile"}
    assert services["chitragupta-ui"]["build"] == {"context": "../../frontend", "dockerfile": "Dockerfile"}
    assert services["chitragupta"]["ports"] == ["${DEMO_BIND_ADDRESS:-127.0.0.1}:${DEMO_API_PORT:-8080}:8080"]
    assert services["chitragupta-ui"]["ports"] == ["${DEMO_BIND_ADDRESS:-127.0.0.1}:${DEMO_UI_PORT:-8081}:80"]


def test_demo_compose_preserves_generator_profile_and_selected_state_ownership_contracts() -> None:
    compose = yaml.safe_load((PROJECT_ROOT / BASE).read_text(encoding="utf-8"))
    services = compose["services"]
    generator = services["demo-generator"]
    state_mount = "$" + "{DEMO_STATE_DIR:-../../.demo/state/clean}:/app/data:rw"

    assert generator["command"] == [
        "--config",
        "/app/config/config.yaml",
        "--anchor",
        "$" + "{DEMO_ANCHOR_DATE:-}",
        "--profile",
        "$" + "{DEMO_PROFILE:-clean}",
        "--state-dir",
        "/app/data",
    ]
    assert generator["user"] == services["chitragupta"]["user"] == "$" + "{DEMO_UID}:$" + "{DEMO_GID}"
    assert state_mount in generator["volumes"]
    assert state_mount in services["chitragupta"]["volumes"]


@pytest.mark.parametrize("platform", ["Linux", "Darwin"])
def test_demo_lan_uses_a_routable_display_address_and_an_all_ipv4_bind(tmp_path: Path, platform: str) -> None:
    workspace = _copy_public_demo(tmp_path)
    environment, command_log = _fake_environment(tmp_path, platform=platform, lan_address="10.20.30.40")

    result = _run(workspace, environment, "--lan", "--ui-port", "9081", "--api-port", "9080")

    assert result.returncode == 0
    assert {(call[2], call[3], call[4]) for call in _calls(command_log)[1:]} == {("0.0.0.0", "9081", "9080")}
    assert "UI: http://10.20.30.40:9081" in _output(result)
    assert "API: http://10.20.30.40:9080" in _output(result)
    assert "0.0.0.0" not in _output(result)


def test_demo_rejects_lan_without_an_ipv4_address_before_image_or_state_work(tmp_path: Path) -> None:
    workspace = _copy_public_demo(tmp_path)
    environment, command_log = _fake_environment(tmp_path, lan_address="")

    result = _run(workspace, environment, "--lan")

    assert result.returncode == 1
    assert "--lan" in _output(result)
    assert "without --lan" in _output(result)
    assert _commands(command_log) == [("version",)]
    assert not (workspace / ".demo").exists()


def test_demo_default_and_override_urls_include_only_selected_services(tmp_path: Path) -> None:
    workspace = _copy_public_demo(tmp_path)
    environment, command_log = _fake_environment(tmp_path)

    result = _run(workspace, environment, "--ui-port", "8181", "--api-port", "8180", "--grafana-port", "3100")

    assert result.returncode == 0
    assert {(call[2], call[3], call[4], call[5]) for call in _calls(command_log)[1:]} == {
        ("127.0.0.1", "8181", "8180", "3100")
    }
    output = _output(result)
    assert "UI: http://127.0.0.1:8181" in output
    assert "API: http://127.0.0.1:8180" in output
    assert "Grafana:" not in output


def test_demo_rejects_duplicate_selected_ports_before_host_inspection_or_image_work(tmp_path: Path) -> None:
    workspace = _copy_public_demo(tmp_path)
    environment, command_log = _fake_environment(tmp_path)

    result = _run(workspace, environment, "--ui-port", "8080")

    assert result.returncode == 1
    assert "Port 8080 is requested by both API and UI; choose distinct ports." in _output(result)
    assert _commands(command_log) == [("version",)]
    assert not (workspace / ".demo").exists()


def test_demo_allows_the_current_demo_service_to_recreate_its_requested_port(tmp_path: Path) -> None:
    workspace = _copy_public_demo(tmp_path)
    environment, command_log = _fake_environment(tmp_path)
    environment["DEMO_OWNED_PORT"] = "127.0.0.1:8080"

    result = _run(workspace, environment)

    assert result.returncode == 0
    assert _up() in _commands(command_log)


def test_demo_reports_external_docker_container_and_linux_process_port_owners(tmp_path: Path) -> None:
    docker_root = tmp_path / "docker"
    docker_root.mkdir()
    docker_workspace = _copy_public_demo(docker_root)
    docker_environment, docker_log = _fake_environment(
        docker_root,
        docker_ports="other-api\tabc123\t0.0.0.0:8080->8080/tcp\n",
    )
    docker_result = _run(docker_workspace, docker_environment)

    process_root = tmp_path / "process"
    process_root.mkdir()
    process_workspace = _copy_public_demo(process_root)
    process_environment, process_log = _fake_environment(process_root)
    _set_linux_listener(process_environment, table="tcp", address="00000000", port=8081)
    process_result = _run(process_workspace, process_environment)

    assert docker_result.returncode == process_result.returncode == 1
    assert "Port 8080 for API is in use by Docker container other-api (abc123)." in _output(docker_result)
    assert "Port 8081 for UI is in use by process python3 (PID 4127)." in _output(process_result)
    assert not any("pull" in command for command in _commands(docker_log))
    assert not any("pull" in command for command in _commands(process_log))


def test_demo_linux_bind_address_rules_cover_secondary_ipv4_mapped_and_native_ipv6(tmp_path: Path) -> None:
    lan_root = tmp_path / "lan"
    lan_root.mkdir()
    lan_workspace = _copy_public_demo(lan_root)
    lan_environment, lan_log = _fake_environment(lan_root)
    _set_linux_listener(lan_environment, table="tcp", address="0A01A8C0", port=8081, command="secondary-app")
    lan_result = _run(lan_workspace, lan_environment, "--lan")

    local_root = tmp_path / "local"
    local_root.mkdir()
    local_workspace = _copy_public_demo(local_root)
    local_environment, local_log = _fake_environment(local_root)
    _set_linux_listener(local_environment, table="tcp", address="0A01A8C0", port=8081, command="secondary-app")
    local_result = _run(local_workspace, local_environment)

    mapped_root = tmp_path / "mapped"
    mapped_root.mkdir()
    mapped_workspace = _copy_public_demo(mapped_root)
    mapped_environment, mapped_log = _fake_environment(mapped_root)
    _set_linux_listener(
        mapped_environment,
        table="tcp6",
        address="0000000000000000FFFF00000100007F",
        port=8080,
        command="mapped-owner",
    )
    mapped_result = _run(mapped_workspace, mapped_environment)

    native_root = tmp_path / "native"
    native_root.mkdir()
    native_workspace = _copy_public_demo(native_root)
    native_environment, native_log = _fake_environment(native_root)
    _set_linux_listener(
        native_environment,
        table="tcp6",
        address="00000000000000000000000000000001",
        port=8081,
        command="ipv6-only",
    )
    native_result = _run(native_workspace, native_environment)

    assert lan_result.returncode == mapped_result.returncode == 1
    assert "Port 8081 for UI is in use by process secondary-app (PID 4127)." in _output(lan_result)
    assert "Port 8080 for API is in use by process mapped-owner (PID 4127)." in _output(mapped_result)
    assert local_result.returncode == native_result.returncode == 0
    assert _up() in _commands(local_log)
    assert _up() in _commands(native_log)
    assert not any("pull" in command for command in _commands(lan_log))
    assert not any("pull" in command for command in _commands(mapped_log))


def test_demo_fails_closed_when_linux_authoritative_listener_ownership_is_unavailable(tmp_path: Path) -> None:
    workspace = _copy_public_demo(tmp_path)
    environment, command_log = _fake_environment(tmp_path)
    _set_linux_listener(environment, table="tcp", address="00000000", port=8081, pid=None)

    result = _run(workspace, environment)

    assert result.returncode == 1
    assert "Port 8081 for UI has a non-Docker listener" in _output(result)
    assert "Linux process ownership could not be read from /proc" in _output(result)
    assert not any("pull" in command for command in _commands(command_log))


@pytest.mark.parametrize(
    ("source", "lsof_output", "ss_output", "fuser_output", "pid", "command", "expected_host_calls"),
    [
        (
            "lsof",
            (
                "COMMAND PID USER FD TYPE DEVICE SIZE/OFF NODE NAME\n"
                "lsof-owner 5121 root 7u IPv4 0 0 TCP *:8081 (LISTEN)\n"
            ),
            None,
            None,
            5121,
            "lsof-owner",
            ["lsof\t-nP -iTCP:8081 -sTCP:LISTEN"],
        ),
        (
            "ss",
            "",
            'LISTEN 0 4096 *:8081 *:* users:(("ss-owner",pid=5122,fd=7))\n',
            None,
            5122,
            "ss-owner",
            ["lsof\t-nP -iTCP:8081 -sTCP:LISTEN", "ss\t-ltnp sport = :8081"],
        ),
        (
            "fuser",
            "",
            "",
            "5123\n",
            5123,
            "fuser-owner",
            [
                "lsof\t-nP -iTCP:8081 -sTCP:LISTEN",
                "ss\t-ltnp sport = :8081",
                "fuser\t-n tcp 8081",
            ],
        ),
    ],
)
def test_demo_uses_linux_ownership_fallbacks_in_lsof_ss_fuser_order(
    tmp_path: Path,
    source: str,
    lsof_output: str,
    ss_output: str | None,
    fuser_output: str | None,
    pid: int,
    command: str,
    expected_host_calls: list[str],
) -> None:
    workspace = _copy_public_demo(tmp_path)
    environment, _command_log = _fake_environment(
        tmp_path,
        lsof_output=lsof_output,
        ss_output=ss_output,
        fuser_output=fuser_output,
    )
    _set_linux_listener(environment, table="tcp", address="00000000", port=8081, pid=None)
    if source == "fuser":
        _set_proc_command(environment, pid, command)

    result = _run(workspace, environment)

    assert result.returncode == 1
    assert f"Port 8081 for UI is in use by process {command} (PID {pid})." in _output(result)
    assert Path(environment["DEMO_HOST_LOG"]).read_text(encoding="utf-8").splitlines() == expected_host_calls


def test_demo_continues_with_listener_free_linux_kernel_tables_without_optional_ownership_tools(tmp_path: Path) -> None:
    workspace = _copy_public_demo(tmp_path)
    environment, command_log = _fake_environment(tmp_path, include_lsof=False)

    result = _run(workspace, environment)

    assert result.returncode == 0
    assert _up() in _commands(command_log)


@pytest.mark.skipif(not sys.platform.startswith("linux"), reason="requires Linux /proc TCP tables")
def test_demo_uses_the_real_linux_proc_listener_and_owner_for_an_ipv4_conflict(tmp_path: Path) -> None:
    try:
        listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        listener.bind(("127.0.0.1", 0))
        listener.listen()
    except OSError as error:
        pytest.skip(f"IPv4 listener binding is unavailable: {error}")
    try:
        port = listener.getsockname()[1]
        workspace = _copy_public_demo(tmp_path)
        environment, command_log = _fake_environment(tmp_path)
        environment.pop("DEMO_PROC_ROOT")

        result = _run(workspace, environment, "--api-port", str(port))

        assert result.returncode == 1
        assert f"Port {port} for API is in use by process " in _output(result)
        assert f"(PID {os.getpid()})." in _output(result)
        assert not any("pull" in command for command in _commands(command_log))
    finally:
        listener.close()


@pytest.mark.skipif(not sys.platform.startswith("linux"), reason="requires Linux /proc TCP tables")
def test_demo_allows_a_real_ipv6_only_wildcard_listener_to_defer_to_compose(tmp_path: Path) -> None:
    try:
        listener = socket.socket(socket.AF_INET6, socket.SOCK_STREAM)
        listener.setsockopt(socket.IPPROTO_IPV6, socket.IPV6_V6ONLY, 1)
        listener.bind(("::", 0))
        listener.listen()
    except OSError as error:
        pytest.skip(f"IPv6-only listener binding is unavailable: {error}")
    try:
        port = listener.getsockname()[1]
        workspace = _copy_public_demo(tmp_path)
        environment, command_log = _fake_environment(tmp_path)
        environment.pop("DEMO_PROC_ROOT")

        result = _run(workspace, environment, "--ui-port", str(port))

        assert result.returncode == 0
        assert _up() in _commands(command_log)
        assert f"Port {port} for UI is in use" not in _output(result)
    finally:
        listener.close()


@pytest.mark.parametrize(
    ("lsof_output", "lsof_status", "arguments", "expected_status", "expected_detail"),
    [
        (
            "COMMAND PID USER FD TYPE DEVICE SIZE/OFF NODE NAME\ngrafana 981 root 7u IPv4 0 0 TCP *:3000 (LISTEN)\n",
            0,
            ("--grafana",),
            1,
            "Port 3000 for Grafana is in use by process grafana (PID 981).",
        ),
        ("", 0, (), 0, ""),
        ("", 1, (), 0, ""),
    ],
)
def test_demo_uses_the_macos_lsof_listener_contract(
    tmp_path: Path,
    lsof_output: str,
    lsof_status: int,
    arguments: tuple[str, ...],
    expected_status: int,
    expected_detail: str,
) -> None:
    workspace = _copy_public_demo(tmp_path)
    environment, command_log = _fake_environment(
        tmp_path,
        platform="Darwin",
        lsof_output=lsof_output,
        lsof_status=lsof_status,
    )

    result = _run(workspace, environment, *arguments)

    assert result.returncode == expected_status
    host_calls = Path(environment["DEMO_HOST_LOG"]).read_text(encoding="utf-8").splitlines()
    assert "lsof\t-nP +c 0 -iTCP:8080 -sTCP:LISTEN" in host_calls
    if expected_detail:
        assert expected_detail in _output(result)
    else:
        assert _up() in _commands(command_log)


@pytest.mark.parametrize(
    ("command", "pid", "expected_status", "expected_detail"),
    [
        ("com.docker.backend", 981, 0, ""),
    ],
)
def test_demo_macos_requests_untruncated_lsof_commands_and_distinguishes_current_docker_proxy(
    tmp_path: Path,
    command: str,
    pid: int,
    expected_status: int,
    expected_detail: str,
) -> None:
    workspace = _copy_public_demo(tmp_path)
    environment, command_log = _fake_environment(
        tmp_path,
        platform="Darwin",
        docker_ports="chitragupta-current\tabc123\t127.0.0.1:8080->8080/tcp\n",
        lsof_output=(
            "COMMAND PID USER FD TYPE DEVICE SIZE/OFF NODE NAME\n"
            f"{command} {pid} root 7u IPv4 0 0 TCP 127.0.0.1:8080 (LISTEN)\n"
        ),
        lsof_requires_untruncated_command=True,
    )
    environment["DEMO_OWNED_PORT"] = "127.0.0.1:8080"

    result = _run(workspace, environment)

    host_log = Path(environment["DEMO_HOST_LOG"])
    host_calls = host_log.read_text(encoding="utf-8").splitlines() if host_log.exists() else []
    assert "lsof\t-nP +c 0 -iTCP:8080 -sTCP:LISTEN" in host_calls
    assert result.returncode == expected_status
    if expected_detail:
        assert expected_detail in _output(result)
        assert not any("pull" in command for command in _commands(command_log))
    else:
        assert _up() in _commands(command_log)
        assert "Port 8080 for API is in use" not in _output(result)


def test_demo_macos_allows_confirmed_current_binding_without_command_identity_but_rejects_other_endpoint(
    tmp_path: Path,
) -> None:
    workspace = _copy_public_demo(tmp_path)
    environment, command_log = _fake_environment(
        tmp_path,
        platform="Darwin",
        docker_ports=(
            "chitragupta-current\tabc123\t127.0.0.1:18080->8080/tcp\nexternal-ui\tdef456\t127.0.0.1:18081->80/tcp\n"
        ),
        lsof_output=(
            "COMMAND PID USER FD TYPE DEVICE SIZE/OFF NODE NAME\n"
            "unidentified-listener 981 root 7u IPv4 0 0 TCP 127.0.0.1:18080 (LISTEN)\n"
        ),
    )
    environment["DEMO_OWNED_PORT"] = "127.0.0.1:18080"

    result = _run(workspace, environment, "--api-port", "18080", "--ui-port", "18081")
    output = _output(result)

    assert result.returncode == 1
    assert (*ALL_FILES, "port", "chitragupta", "8080") in _commands(command_log)
    assert "Port 18080 for API is in use" not in output
    assert (*ALL_FILES, "port", "chitragupta-ui", "80") in _commands(command_log)
    assert "Port 18081 for UI is in use by Docker container external-ui (def456)." in output
    assert not any("pull" in command for command in _commands(command_log))


def test_demo_treats_macos_lsof_no_match_exit_status_as_a_free_port(tmp_path: Path) -> None:
    workspace = _copy_public_demo(tmp_path)
    environment, command_log = _fake_environment(tmp_path, platform="Darwin", lsof_status=1)

    result = _run(workspace, environment)

    assert result.returncode == 0
    assert _up() in _commands(command_log)
    assert (
        "lsof\t-nP +c 0 -iTCP:8080 -sTCP:LISTEN"
        in Path(environment["DEMO_HOST_LOG"]).read_text(encoding="utf-8").splitlines()
    )


def test_demo_treats_macos_lsof_header_only_status_zero_as_a_free_port(tmp_path: Path) -> None:
    workspace = _copy_public_demo(tmp_path)
    environment, command_log = _fake_environment(
        tmp_path,
        platform="Darwin",
        lsof_output="COMMAND PID USER FD TYPE DEVICE SIZE/OFF NODE NAME\n",
    )

    result = _run(workspace, environment)

    assert result.returncode == 0
    assert _up() in _commands(command_log)


def test_demo_rejects_macos_lsof_unparseable_non_header_listener_record(tmp_path: Path) -> None:
    workspace = _copy_public_demo(tmp_path)
    environment, command_log = _fake_environment(
        tmp_path,
        platform="Darwin",
        lsof_output="COMMAND PID USER FD TYPE DEVICE SIZE/OFF NODE NAME\nunparseable listener record\n",
    )

    result = _run(workspace, environment)

    assert result.returncode == 1
    assert "Port 8080 for API has a non-Docker listener, but macOS lsof could not identify its process." in _output(
        result
    )
    assert not any("pull" in command for command in _commands(command_log))


def test_demo_rejects_macos_lsof_status_one_with_inspection_diagnostic_stderr(tmp_path: Path) -> None:
    workspace = _copy_public_demo(tmp_path)
    environment, command_log = _fake_environment(
        tmp_path,
        platform="Darwin",
        lsof_status=1,
        lsof_stderr="lsof: /dev/kmem: Permission denied\n",
    )

    result = _run(workspace, environment)

    assert result.returncode == 1
    assert "Port 8080 for API has a non-Docker listener, but macOS lsof could not identify its process." in _output(
        result
    )
    assert (
        "lsof\t-nP +c 0 -iTCP:8080 -sTCP:LISTEN"
        in Path(environment["DEMO_HOST_LOG"]).read_text(encoding="utf-8").splitlines()
    )
    assert not any("pull" in command for command in _commands(command_log))


def test_demo_reports_when_macos_lsof_cannot_be_resolved(tmp_path: Path) -> None:
    workspace = _copy_public_demo(tmp_path)
    environment, command_log = _fake_environment(tmp_path, platform="Darwin", include_lsof=False)

    result = _run(workspace, environment)

    assert result.returncode == 1
    assert "Port 8080 for API has a non-Docker listener, but macOS lsof could not identify its process." in _output(
        result
    )
    assert not any("pull" in command for command in _commands(command_log))


@pytest.mark.parametrize(
    ("record", "arguments", "expected_status", "expected_detail"),
    [
        ("ipv4-secondary", (), 0, ""),
        ("ipv6-loopback", (), 0, ""),
        ("ipv6-wildcard", (), 0, ""),
        ("ipv4-mapped", (), 1, "Port 8080 for API is in use by process mapped-owner (PID 4127)."),
    ],
)
def test_demo_applies_macos_lsof_bind_specificity_and_defers_native_ipv6_wildcards(
    tmp_path: Path,
    record: str,
    arguments: tuple[str, ...],
    expected_status: int,
    expected_detail: str,
) -> None:
    endpoint_by_record = {
        "ipv4-secondary": ("IPv4", "10.20.30.40:8081", "secondary-owner", 4127),
        "ipv6-loopback": ("IPv6", "[::1]:8081", "ipv6-loopback", 4127),
        "ipv6-wildcard": ("IPv6", "*:8081", "ipv6-wildcard", 4127),
        "ipv4-mapped": ("IPv6", "[::ffff:127.0.0.1]:8080", "mapped-owner", 4127),
    }
    address_type, endpoint, command, pid = endpoint_by_record[record]
    lsof_output = (
        "COMMAND PID USER FD TYPE DEVICE SIZE/OFF NODE NAME\n"
        f"{command} {pid} root 7u {address_type} 0 0 TCP {endpoint} (LISTEN)\n"
    )
    workspace = _copy_public_demo(tmp_path)
    environment, command_log = _fake_environment(tmp_path, platform="Darwin", lsof_output=lsof_output)

    result = _run(workspace, environment, *arguments)

    assert result.returncode == expected_status
    if expected_detail:
        assert expected_detail in _output(result)
    else:
        assert _up() in _commands(command_log)


def test_demo_defers_native_ipv6_wildcard_until_compose_proves_an_ipv4_bind_conflict(tmp_path: Path) -> None:
    success_root = tmp_path / "success"
    success_root.mkdir()
    success_workspace = _copy_public_demo(success_root)
    success_environment, success_log = _fake_environment(success_root)
    _set_linux_listener(
        success_environment,
        table="tcp6",
        address="00000000000000000000000000000000",
        port=8081,
        command="ipv6-only",
    )
    success = _run(success_workspace, success_environment)

    failure_root = tmp_path / "failure"
    failure_root.mkdir()
    failure_workspace = _copy_public_demo(failure_root)
    failure_environment, failure_log = _fake_environment(failure_root, failure="up-bind")
    _set_linux_listener(
        failure_environment,
        table="tcp6",
        address="00000000000000000000000000000000",
        port=8081,
        command="dual-stack-owner",
    )
    bind_failure = _run(failure_workspace, failure_environment, "--lan")
    output = _output(bind_failure)

    assert success.returncode == 0
    assert "Port 8081 for UI is in use" not in _output(success)
    assert _up() in _commands(success_log)
    assert bind_failure.returncode == 75
    assert "Port 8081 for UI is in use by process dual-stack-owner (PID 4127)." in output
    assert output.index("Port 8081 for UI") < output.index(
        "Demo startup failed. Containers were left in place for diagnosis."
    )
    assert not any(command[-1] in {"stop", "down"} for command in _commands(failure_log))


def test_demo_does_not_misdiagnose_native_ipv6_after_a_non_bind_health_failure(tmp_path: Path) -> None:
    workspace = _copy_public_demo(tmp_path)
    environment, command_log = _fake_environment(tmp_path, failure="up")
    _set_linux_listener(
        environment,
        table="tcp6",
        address="00000000000000000000000000000000",
        port=8081,
        command="dual-stack-owner",
    )

    result = _run(workspace, environment)

    assert result.returncode == 74
    assert "chitragupta-ui failed its health check" in _output(result)
    assert "Port 8081 for UI is in use" not in _output(result)
    assert "Demo startup failed. Containers were left in place for diagnosis." in _output(result)
    assert not any(command[-1] in {"stop", "down"} for command in _commands(command_log))


@pytest.mark.parametrize(
    ("failure", "arguments", "expected_bind"),
    [
        ("up-bind-local", (), "127.0.0.1:8081"),
        ("up-bind", ("--lan",), "0.0.0.0:8081"),
    ],
)
def test_demo_reports_only_the_exact_requested_bind_for_a_saved_ambiguous_listener(
    tmp_path: Path,
    failure: str,
    arguments: tuple[str, ...],
    expected_bind: str,
) -> None:
    workspace = _copy_public_demo(tmp_path)
    environment, command_log = _fake_environment(tmp_path, failure=failure)
    _set_linux_listener(
        environment,
        table="tcp6",
        address="00000000000000000000000000000000",
        port=8081,
        command="dual-stack-owner",
    )

    result = _run(workspace, environment, *arguments)

    output = _output(result)
    assert result.returncode == 75
    assert expected_bind in output
    assert "Port 8081 for UI is in use by process dual-stack-owner (PID 4127)." in output
    assert output.index("Port 8081 for UI") < output.index(
        "Demo startup failed. Containers were left in place for diagnosis."
    )
    assert not any(command[-1] in {"stop", "down"} for command in _commands(command_log))


@pytest.mark.parametrize(
    ("failure", "port", "service", "unexpected_service"),
    [
        ("up-bind-api-hyphenated", 8080, "API", "UI"),
        ("up-bind-ui-hyphenated", 8081, "UI", "API"),
    ],
)
def test_demo_correlates_hyphenated_compose_endpoint_bind_errors_to_the_exact_service_and_port(
    tmp_path: Path,
    failure: str,
    port: int,
    service: str,
    unexpected_service: str,
) -> None:
    workspace = _copy_public_demo(tmp_path)
    environment, command_log = _fake_environment(tmp_path, failure=failure)
    _set_linux_listener(
        environment,
        table="tcp6",
        address="00000000000000000000000000000000",
        port=port,
        command="dual-stack-owner",
    )

    result = _run(workspace, environment)

    output = _output(result)
    assert result.returncode == 75
    assert f"Port {port} for {service} is in use by process dual-stack-owner (PID 4127)." in output
    assert f"Port {port} for {unexpected_service} is in use" not in output
    assert output.index(f"Port {port} for {service}") < output.index(
        "Demo startup failed. Containers were left in place for diagnosis."
    )
    assert not any(command[-1] in {"stop", "down"} for command in _commands(command_log))


def test_demo_does_not_promote_a_prefix_matching_port_to_a_bind_conflict(tmp_path: Path) -> None:
    workspace = _copy_public_demo(tmp_path)
    environment, command_log = _fake_environment(tmp_path, failure="up-bind-prefix")
    _set_linux_listener(
        environment,
        table="tcp6",
        address="00000000000000000000000000000000",
        port=8081,
        command="dual-stack-owner",
    )

    result = _run(workspace, environment)

    assert result.returncode == 75
    assert "Port 8081 for UI is in use" not in _output(result)
    assert "Demo startup failed. Containers were left in place for diagnosis." in _output(result)


def test_demo_reports_unmapped_ambiguous_listener_ownership_only_after_a_matching_bind_failure(tmp_path: Path) -> None:
    workspace = _copy_public_demo(tmp_path)
    environment, command_log = _fake_environment(tmp_path, failure="up-bind", include_lsof=False)
    _set_linux_listener(
        environment,
        table="tcp6",
        address="00000000000000000000000000000000",
        port=8081,
        pid=None,
    )

    result = _run(workspace, environment, "--lan")

    assert result.returncode == 75
    assert "Port 8081 for UI has a non-Docker listener" in _output(result)
    assert "Linux process ownership could not be read from /proc" in _output(result)
    assert "Demo startup failed. Containers were left in place for diagnosis." in _output(result)


def test_demo_reset_preflights_conflicts_before_stop_or_selected_state_deletion(tmp_path: Path) -> None:
    workspace = _copy_public_demo(tmp_path)
    selected_state = workspace / ".demo" / "state" / "showcase"
    selected_state.mkdir(parents=True)
    (selected_state / "keep.db").write_bytes(b"unchanged")
    (workspace / ".demo" / "active-profile").write_text("showcase\n", encoding="utf-8")
    before = _snapshot(selected_state)
    environment, command_log = _fake_environment(tmp_path)
    _set_linux_listener(environment, table="tcp", address="00000000", port=8080)

    result = _run(workspace, environment, "reset")

    assert result.returncode == 1
    assert "Port 8080 for API is in use by process python3 (PID 4127)." in _output(result)
    assert not any(command[-1] == "stop" for command in _commands(command_log))
    assert _snapshot(selected_state) == before


def test_demo_grafana_override_mounts_selected_profile_read_only_and_uses_confluent_database() -> None:
    override = yaml.safe_load((PROJECT_ROOT / GRAFANA).read_text(encoding="utf-8"))
    grafana = override["services"]["grafana"]
    datasource_path = PROJECT_ROOT / "examples/demo/grafana/provisioning/datasources/datasource.yml"
    datasource = datasource_path.read_text(encoding="utf-8")
    shared_plugins = PROJECT_ROOT / GRAFANA_PLUGIN_ENV_PATH

    assert shared_plugins.read_text(encoding="utf-8") == f"{GRAFANA_PLUGIN_ENV_KEY}={EXPECTED_GRAFANA_PLUGIN_PINS}\n"
    assert grafana["image"] == "grafana/grafana:12.4.0"
    assert grafana["env_file"] == [GRAFANA_PLUGIN_ENV_REFERENCE]
    assert GRAFANA_PLUGIN_INSTALL_KEYS.isdisjoint(grafana["environment"])
    assert grafana["ports"] == ["${DEMO_BIND_ADDRESS:-127.0.0.1}:${DEMO_GRAFANA_PORT:-3000}:3000"]
    assert "${DEMO_STATE_DIR:-../../.demo/state/clean}:/var/lib/grafana/data/demo:ro" in grafana["volumes"]
    assert "examples/shared/grafana/provisioning/dashboards" in "\n".join(grafana["volumes"])
    assert "/var/lib/grafana/data/demo/confluent-cloud.db" in datasource


@pytest.mark.parametrize(("stack_name", "compose_files"), GRAFANA_COMPOSE_STACKS)
def test_every_grafana_example_consumes_the_shared_plugin_declaration(
    stack_name: str,
    compose_files: tuple[str, ...],
) -> None:
    for compose_file in compose_files:
        compose = yaml.safe_load((PROJECT_ROOT / compose_file).read_text(encoding="utf-8"))
        grafana = compose["services"].get("grafana")
        if grafana is None:
            continue

        assert grafana["image"] == "grafana/grafana:12.4.0", stack_name
        assert grafana["env_file"] == [GRAFANA_PLUGIN_ENV_REFERENCE], stack_name
        assert GRAFANA_PLUGIN_INSTALL_KEYS.isdisjoint(grafana["environment"]), stack_name


def test_demo_public_copy_includes_only_the_shared_grafana_plugin_declaration(tmp_path: Path) -> None:
    workspace = _copy_public_demo(tmp_path)
    shared_grafana = workspace / "examples/shared/grafana"

    assert [path.name for path in shared_grafana.iterdir()] == ["plugins.env"]
    assert (shared_grafana / "plugins.env").read_text(encoding="utf-8") == (
        f"{GRAFANA_PLUGIN_ENV_KEY}={EXPECTED_GRAFANA_PLUGIN_PINS}\n"
    )


@pytest.mark.parametrize("action", ["status", "logs", "down"])
def test_demo_operations_preserve_profile_state_and_active_profile(tmp_path: Path, action: str) -> None:
    workspace = _copy_public_demo(tmp_path)
    state = workspace / ".demo" / "state" / "clean"
    state.mkdir(parents=True)
    (state / "confluent-cloud.db").write_bytes(b"synthetic data")
    active_profile = workspace / ".demo" / "active-profile"
    active_profile.write_text("clean\n", encoding="utf-8")
    before = _snapshot(workspace / ".demo")
    environment, command_log = _fake_environment(tmp_path)

    result = _run(workspace, environment, action)

    assert result.returncode == 0
    assert _snapshot(workspace / ".demo") == before
    assert _commands(command_log)[-1] == (*ALL_FILES, "ps" if action == "status" else action)


def test_demo_reset_stops_optional_grafana_before_deleting_only_the_selected_profile(tmp_path: Path) -> None:
    workspace = _copy_public_demo(tmp_path)
    clean_state = workspace / ".demo" / "state" / "clean"
    showcase_state = workspace / ".demo" / "state" / "showcase"
    clean_state.mkdir(parents=True)
    showcase_state.mkdir(parents=True)
    (clean_state / "obsolete.db").write_bytes(b"reset")
    (showcase_state / "keep.db").write_bytes(b"keep")
    showcase_before = _snapshot(showcase_state)
    environment, command_log = _fake_environment(tmp_path)

    result = _run(workspace, environment, "reset", "--clean")

    assert result.returncode == 0
    assert (*ALL_FILES, "stop", "chitragupta", "chitragupta-ui", "grafana") in _commands(command_log)
    assert not (clean_state / "obsolete.db").exists()
    assert _snapshot(showcase_state) == showcase_before


@pytest.mark.parametrize(
    ("failure", "arguments", "status", "detail", "forbidden"),
    [
        ("pull", (), 71, "released-image acquisition", "run"),
        ("build", ("--build",), 72, "checkout build", "run"),
        ("generator", (), 73, "generation failed", "up"),
    ],
)
def test_demo_acquisition_and_generator_failures_preserve_existing_stack_without_recreation(
    tmp_path: Path,
    failure: str,
    arguments: tuple[str, ...],
    status: int,
    detail: str,
    forbidden: str,
) -> None:
    workspace = _copy_public_demo(tmp_path)
    environment, command_log = _fake_environment(tmp_path, failure=failure)

    result = _run(workspace, environment, *arguments)

    assert result.returncode == status
    assert detail in _output(result)
    assert not any(forbidden in command for command in _commands(command_log))
    assert not any(command[-1] in {"stop", "down"} for command in _commands(command_log))


def test_demo_failed_health_startup_keeps_active_profile_and_prints_retained_container_commands(tmp_path: Path) -> None:
    workspace = _copy_public_demo(tmp_path)
    active_profile = workspace / ".demo" / "active-profile"
    active_profile.parent.mkdir()
    active_profile.write_text("clean\n", encoding="utf-8")
    environment, command_log = _fake_environment(tmp_path, failure="up")

    result = _run(workspace, environment, "--showcase")
    output = _output(result)

    assert result.returncode == 74
    assert "Demo startup failed. Containers were left in place for diagnosis." in output
    assert "Run './demo status' to inspect service state." in output
    assert "Run './demo logs' to inspect service output." in output
    assert "Run './demo down' to stop the stack." in output
    assert "UI: http://" not in output
    assert "API: http://" not in output
    assert active_profile.read_text(encoding="utf-8") == "clean\n"
    assert not any(command[-1] in {"stop", "down"} for command in _commands(command_log))


def test_demo_selected_grafana_health_failure_retains_containers_and_suppresses_urls(tmp_path: Path) -> None:
    workspace = _copy_public_demo(tmp_path)
    environment, command_log = _fake_environment(tmp_path, failure="grafana-health")

    result = _run(workspace, environment, "--grafana")

    output = _output(result)
    assert result.returncode == 78
    assert "grafana failed its health check" in output
    assert "Demo startup failed. Containers were left in place for diagnosis." in output
    assert "Grafana: http://" not in output
    assert "UI: http://" not in output
    assert "API: http://" not in output
    assert not any(command[-1] in {"stop", "down"} for command in _commands(command_log))


def test_demo_preserves_profile_generator_health_uid_date_and_network_baseline_invariants(tmp_path: Path) -> None:
    workspace = _copy_public_demo(tmp_path)
    environment, command_log = _fake_environment(tmp_path)

    clean = _run(workspace, environment)
    clean_state = workspace / ".demo" / "state" / "clean"
    (clean_state / "evaluator-tag.txt").write_text("preserve me", encoding="utf-8")
    clean_before = _snapshot(clean_state)
    showcase = _run(workspace, environment, "--showcase")

    compose = yaml.safe_load((PROJECT_ROOT / BASE).read_text(encoding="utf-8"))
    services = compose["services"]
    assert clean.returncode == showcase.returncode == 0
    assert _snapshot(clean_state) == clean_before
    assert (workspace / ".demo" / "active-profile").read_text(encoding="utf-8") == "showcase\n"
    assert services["demo-generator"]["entrypoint"] == ["python", "-m", "demo.generator"]
    assert services["demo-generator"]["networks"] == ["demo"]
    assert "demo" in services["chitragupta"]["networks"]
    assert compose["networks"]["demo"]["internal"] is True
    assert all(
        "demo-generator" not in command for command in _commands(command_log) if " up " in f" {' '.join(command)} "
    )


def test_demo_rejects_malformed_active_profile_before_stop_or_selected_state_deletion(tmp_path: Path) -> None:
    workspace = _copy_public_demo(tmp_path)
    state = workspace / ".demo" / "state" / "clean"
    state.mkdir(parents=True)
    (state / "keep.db").write_bytes(b"keep")
    (workspace / ".demo" / "active-profile").write_text("malformed\n", encoding="utf-8")
    before = _snapshot(state)
    environment, command_log = _fake_environment(tmp_path)

    result = _run(workspace, environment, "reset")

    assert result.returncode == 1
    assert "Invalid active Demo profile 'malformed'" in _output(result)
    assert _commands(command_log) == []
    assert _snapshot(state) == before


def test_demo_reports_when_both_supported_compose_frontends_are_absent(tmp_path: Path) -> None:
    workspace = _copy_public_demo(tmp_path)
    environment, command_log = _fake_environment(tmp_path)
    fake_bin = Path(environment["DEMO_HANDLER"]).parent
    (fake_bin / "docker").unlink()
    (fake_bin / "docker-compose").unlink()

    result = _run(workspace, environment)

    assert result.returncode == 1
    assert "Docker with Compose support is required" in _output(result)
    assert _commands(command_log) == []


@pytest.mark.parametrize("identity_switch", ["-u", "-g"])
def test_demo_rejects_missing_numeric_uid_or_gid_before_state_creation(
    tmp_path: Path,
    identity_switch: str,
) -> None:
    workspace = _copy_public_demo(tmp_path)
    environment, command_log = _fake_environment(tmp_path)
    environment["DEMO_ID_FAILURE"] = identity_switch

    result = _run(workspace, environment)

    assert result.returncode == 1
    assert "Numeric UID/GID could not be determined" in _output(result)
    assert _commands(command_log) == [("version",)]
    assert not (workspace / ".demo").exists()


def test_demo_invokes_utc_date_once_after_preflight_and_before_generation(tmp_path: Path) -> None:
    workspace = _copy_public_demo(tmp_path)
    environment, command_log = _fake_environment(tmp_path)

    result = _run(workspace, environment)

    assert result.returncode == 0
    assert Path(environment["DEMO_DATE_LOG"]).read_text(encoding="utf-8").splitlines() == ["-u +%F"]
    assert _commands(command_log).index((*BASE_FILES, "run", "--rm", "demo-generator")) < _commands(command_log).index(
        _up()
    )


@pytest.mark.parametrize(
    ("arguments", "active_profile", "selected_profile"),
    [
        (("reset",), None, "clean"),
        (("reset",), "showcase", "showcase"),
        (("reset", "--clean"), "showcase", "clean"),
        (("reset", "--showcase"), "clean", "showcase"),
    ],
)
def test_demo_reset_profile_precedence_preserves_the_inactive_dataset(
    tmp_path: Path,
    arguments: tuple[str, ...],
    active_profile: str | None,
    selected_profile: str,
) -> None:
    workspace = _copy_public_demo(tmp_path)
    selected = workspace / ".demo" / "state" / selected_profile
    inactive_profile = "showcase" if selected_profile == "clean" else "clean"
    inactive = workspace / ".demo" / "state" / inactive_profile
    selected.mkdir(parents=True)
    inactive.mkdir(parents=True)
    (selected / "obsolete.db").write_bytes(b"reset")
    (inactive / "keep.db").write_bytes(b"keep")
    if active_profile is not None:
        (workspace / ".demo" / "active-profile").write_text(f"{active_profile}\n", encoding="utf-8")
    inactive_before = _snapshot(inactive)
    environment, _command_log = _fake_environment(tmp_path)

    result = _run(workspace, environment, *arguments)

    assert result.returncode == 0
    assert f"Resetting {selected_profile} demo state at .demo/state/{selected_profile}" in _output(result)
    assert not (selected / "obsolete.db").exists()
    assert _snapshot(inactive) == inactive_before
    assert (workspace / ".demo" / "active-profile").read_text(encoding="utf-8") == f"{selected_profile}\n"


def test_shipped_demo_config_declares_both_generated_tenant_databases_and_validates_metrics() -> None:
    from plugins.confluent_cloud.config import CCloudPluginConfig

    config = yaml.safe_load((PROJECT_ROOT / "examples/demo/config.yaml").read_text(encoding="utf-8"))
    tenants = list(config["tenants"].values())
    database_paths = {tenant["storage"]["connection_string"] for tenant in tenants}
    ccloud = CCloudPluginConfig.from_plugin_settings(config["tenants"]["clean-confluent"]["plugin_settings"])

    assert {tenant["ecosystem"] for tenant in tenants} == {"confluent_cloud", "self_managed_kafka"}
    assert database_paths == {
        "sqlite:////app/data/confluent-cloud.db",
        "sqlite:////app/data/self-managed-kafka.db",
    }
    assert ccloud.topic_attribution.enabled is True
    assert ccloud.metrics is not None
    assert ccloud.metrics.url == "http://prometheus.invalid:9090"


@pytest.mark.parametrize("compose_form", ["plugin", "standalone"])
def test_demo_preserves_reset_profile_precedence_stop_before_delete_and_inactive_state(
    tmp_path: Path,
    compose_form: str,
) -> None:
    workspace = _copy_public_demo(tmp_path)
    clean_state = workspace / ".demo" / "state" / "clean"
    showcase_state = workspace / ".demo" / "state" / "showcase"
    clean_state.mkdir(parents=True)
    showcase_state.mkdir(parents=True)
    (clean_state / "keep.db").write_bytes(b"clean")
    (showcase_state / "obsolete.db").write_bytes(b"showcase")
    (workspace / ".demo" / "active-profile").write_text("showcase\n", encoding="utf-8")
    clean_before = _snapshot(clean_state)
    environment, command_log = _fake_environment(tmp_path, compose_form=compose_form)

    result = _run(workspace, environment, "reset")

    assert result.returncode == 0
    commands = _commands(command_log)
    stop_index = commands.index((*ALL_FILES, "stop", "chitragupta", "chitragupta-ui", "grafana"))
    generation_index = commands.index((*BASE_FILES, "run", "--rm", "demo-generator"))
    forms = [call[0] for call in _calls(command_log)]
    expected_prefix = ["plugin"] if compose_form == "plugin" else ["plugin", "standalone"]
    assert forms[: len(expected_prefix)] == expected_prefix
    assert all(form == compose_form for form in forms[len(expected_prefix) :])
    assert stop_index < generation_index
    assert _snapshot(clean_state) == clean_before
    assert not (showcase_state / "obsolete.db").exists()
    assert (workspace / ".demo" / "active-profile").read_text(encoding="utf-8") == "showcase\n"


def test_demo_preserves_numeric_identity_anchor_and_generator_before_healthy_service_startup(tmp_path: Path) -> None:
    workspace = _copy_public_demo(tmp_path)
    environment, command_log = _fake_environment(tmp_path)

    result = _run(workspace, environment)

    runtime_log = Path(environment["DEMO_RUNTIME_LOG"])
    output = _output(result)
    assert result.returncode == 0
    assert [
        entry for entry in runtime_log.read_text(encoding="utf-8").splitlines() if entry.startswith("2026-09-04\t")
    ] == [
        "2026-09-04\t4242:4343",
        "2026-09-04\t4242:4343",
        "2026-09-04\t4242:4343",
    ]
    assert output.index("GENERATED") < output.index("SERVICES_HEALTHY") < output.index("UI: http://")
    assert _commands(command_log).index((*BASE_FILES, "run", "--rm", "demo-generator")) < _commands(command_log).index(
        _up()
    )


def test_demo_rejects_unusable_numeric_identity_before_state_creation_or_image_acquisition(tmp_path: Path) -> None:
    workspace = _copy_public_demo(tmp_path)
    environment, command_log = _fake_environment(tmp_path)
    environment["DEMO_ID_FAILURE"] = "-u"

    result = _run(workspace, environment)

    assert result.returncode == 1
    assert "Numeric UID/GID could not be determined" in _output(result)
    assert _commands(command_log) == [("version",)]
    assert not (workspace / ".demo").exists()


def _available_compose_command() -> list[str] | None:
    commands = _available_compose_commands()
    return commands[0] if commands else None


def _available_compose_commands() -> list[list[str]]:
    commands: list[list[str]] = []
    docker = shutil.which("docker")
    if docker is not None:
        probe = subprocess.run(
            [docker, "compose", "version"],
            capture_output=True,
            text=True,
            check=False,
            timeout=30,
        )
        if probe.returncode == 0:
            commands.append([docker, "compose"])
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
            commands.append([standalone])
    return commands


def _render_compose_config(
    compose_command: list[str],
    working_directory: Path,
    environment: dict[str, str],
    compose_files: tuple[str, ...] = (),
) -> dict[str, Any]:
    command = [*compose_command]
    for compose_file in compose_files:
        command.extend(("-f", compose_file))
    rendered = subprocess.run(
        [*command, "config"],
        cwd=working_directory,
        env=environment,
        capture_output=True,
        text=True,
        check=False,
        timeout=60,
    )

    assert rendered.returncode == 0, rendered.stderr
    config = yaml.safe_load(rendered.stdout)
    assert isinstance(config, dict)
    return config


def _assert_rendered_grafana_plugin_environment(config: dict[str, Any], expected_pins: str) -> None:
    grafana_environment = config["services"]["grafana"]["environment"]

    assert grafana_environment[GRAFANA_PLUGIN_ENV_KEY] == expected_pins
    assert "GF_INSTALL_PLUGINS" not in grafana_environment
    assert "GF_PLUGINS_PREINSTALL" not in grafana_environment


def test_demo_production_compose_renders_base_and_grafana_with_nondefault_runtime_values() -> None:
    compose_command = _available_compose_command()
    if compose_command is None:
        pytest.skip("Docker Compose is not available")

    environment = os.environ.copy()
    environment.update(
        {
            "DEMO_UID": "4242",
            "DEMO_GID": "4343",
            "DEMO_IMAGE_TAG": "v2.3.4",
            "DEMO_BIND_ADDRESS": "127.0.0.1",
            "DEMO_UI_PORT": "9081",
            "DEMO_API_PORT": "9080",
            "DEMO_GRAFANA_PORT": "3900",
            "DEMO_PROFILE": "showcase",
            "DEMO_STATE_DIR": "../../.demo/state/showcase",
        }
    )
    rendered = subprocess.run(
        [*compose_command, "-f", BASE, "-f", GRAFANA, "config"],
        cwd=PROJECT_ROOT,
        env=environment,
        capture_output=True,
        text=True,
        check=False,
        timeout=60,
    )

    assert rendered.returncode == 0, rendered.stderr
    config = yaml.safe_load(rendered.stdout)
    assert config["services"]["chitragupta"]["user"] == "4242:4343"
    api_port = config["services"]["chitragupta"]["ports"]
    grafana_port = config["services"]["grafana"]["ports"]
    assert len(api_port) == len(grafana_port) == 1
    assert api_port[0]["host_ip"] == grafana_port[0]["host_ip"] == "127.0.0.1"
    assert api_port[0]["published"] == "9080"
    assert api_port[0]["target"] == 8080
    assert grafana_port[0]["published"] == "3900"
    assert grafana_port[0]["target"] == 3000
    _assert_rendered_grafana_plugin_environment(config, EXPECTED_GRAFANA_PLUGIN_PINS)
    assert any(
        mount["target"] == "/var/lib/grafana/data/demo" and mount["read_only"]
        for mount in config["services"]["grafana"]["volumes"]
    )


def test_demo_renders_base_and_merged_definitions_with_every_available_compose_frontend() -> None:
    compose_commands = _available_compose_commands()
    if not compose_commands:
        pytest.skip("Docker Compose is not available")

    environment = os.environ.copy()
    environment.update(
        {
            "DEMO_UID": "4242",
            "DEMO_GID": "4343",
            "DEMO_IMAGE_TAG": "v12.34.56",
            "DEMO_BIND_ADDRESS": "127.0.0.1",
            "DEMO_UI_PORT": "9081",
            "DEMO_API_PORT": "9080",
            "DEMO_GRAFANA_PORT": "3900",
            "DEMO_PROFILE": "showcase",
            "DEMO_STATE_DIR": "../../.demo/state/showcase",
        }
    )
    showcase_state = (PROJECT_ROOT / ".demo/state/showcase").resolve()
    datasource = (PROJECT_ROOT / "examples/demo/grafana/provisioning/datasources/datasource.yml").resolve()

    for compose_command in compose_commands:
        base = subprocess.run(
            [*compose_command, "-f", BASE, "config"],
            cwd=PROJECT_ROOT,
            env=environment,
            capture_output=True,
            text=True,
            check=False,
            timeout=60,
        )
        merged = subprocess.run(
            [*compose_command, "-f", BASE, "-f", GRAFANA, "config"],
            cwd=PROJECT_ROOT,
            env=environment,
            capture_output=True,
            text=True,
            check=False,
            timeout=60,
        )

        assert base.returncode == 0, base.stderr
        assert merged.returncode == 0, merged.stderr
        base_config = yaml.safe_load(base.stdout)
        merged_config = yaml.safe_load(merged.stdout)
        assert "grafana" not in base_config["services"]
        for config in (base_config, merged_config):
            services = config["services"]
            assert config["networks"]["demo"]["internal"] is True
            assert services["demo-generator"]["networks"] == {"demo": None}
            api_networks = set(services["chitragupta"]["networks"])
            ui_networks = set(services["chitragupta-ui"]["networks"])
            published_networks = api_networks - {"demo"}
            assert ui_networks - {"demo"} == published_networks
            assert len(published_networks) == 1
            published_network = published_networks.pop()
            assert config["networks"][published_network].get("internal", False) is False
            assert published_network not in services["demo-generator"]["networks"]
        assert merged_config["services"]["grafana"]["networks"] == {"default": None}
        _assert_rendered_grafana_plugin_environment(merged_config, EXPECTED_GRAFANA_PLUGIN_PINS)
        grafana_volumes = merged_config["services"]["grafana"]["volumes"]
        state_mount = next(mount for mount in grafana_volumes if mount["target"] == "/var/lib/grafana/data/demo")
        datasource_mount = next(
            mount
            for mount in grafana_volumes
            if mount["target"] == "/etc/grafana/provisioning/datasources/datasource.yml"
        )
        assert Path(state_mount["source"]).resolve() == showcase_state
        assert state_mount["read_only"] is True
        assert Path(datasource_mount["source"]).resolve() == datasource
        assert datasource_mount["read_only"] is True


def test_all_grafana_example_stacks_render_shared_plugin_pins_and_propagate_one_edit(tmp_path: Path) -> None:
    compose_commands = _available_compose_commands()
    if not compose_commands:
        pytest.skip("Docker Compose is not available")

    workspace = _copy_grafana_compose_fixture(tmp_path)
    demo_directory = workspace / "examples/demo"
    shared_plugins = workspace / GRAFANA_PLUGIN_ENV_PATH
    environment = os.environ.copy()
    environment.update(
        {
            "DEMO_UID": "4242",
            "DEMO_GID": "4343",
            "DEMO_IMAGE_TAG": "v12.34.56",
            "DEMO_BIND_ADDRESS": "127.0.0.1",
            "DEMO_UI_PORT": "9081",
            "DEMO_API_PORT": "9080",
            "DEMO_GRAFANA_PORT": "3900",
            "DEMO_PROFILE": "showcase",
            "DEMO_STATE_DIR": "../../.demo/state/showcase",
        }
    )
    showcase_state = (workspace / ".demo/state/showcase").resolve()
    datasource = (demo_directory / "grafana/provisioning/datasources/datasource.yml").resolve()

    for compose_command in compose_commands:
        shared_plugins.write_text(
            f"{GRAFANA_PLUGIN_ENV_KEY}={EXPECTED_GRAFANA_PLUGIN_PINS}\n",
            encoding="utf-8",
        )
        base_config = _render_compose_config(
            compose_command,
            demo_directory,
            environment,
            ("docker-compose.yml",),
        )
        merged_config = _render_compose_config(
            compose_command,
            demo_directory,
            environment,
            ("docker-compose.yml", "docker-compose.grafana.yml"),
        )

        assert "grafana" not in base_config["services"]
        _assert_rendered_grafana_plugin_environment(merged_config, EXPECTED_GRAFANA_PLUGIN_PINS)
        grafana_volumes = merged_config["services"]["grafana"]["volumes"]
        state_mount = next(mount for mount in grafana_volumes if mount["target"] == "/var/lib/grafana/data/demo")
        datasource_mount = next(
            mount
            for mount in grafana_volumes
            if mount["target"] == "/etc/grafana/provisioning/datasources/datasource.yml"
        )
        assert Path(state_mount["source"]).resolve() == showcase_state
        assert state_mount["read_only"] is True
        assert Path(datasource_mount["source"]).resolve() == datasource
        assert datasource_mount["read_only"] is True

        for _stack_name, compose_files in GRAFANA_COMPOSE_STACKS[1:]:
            stack_directory = workspace / Path(compose_files[0]).parent
            config = _render_compose_config(compose_command, stack_directory, environment)
            _assert_rendered_grafana_plugin_environment(config, EXPECTED_GRAFANA_PLUGIN_PINS)

        shared_plugins.write_text(
            f"{GRAFANA_PLUGIN_ENV_KEY}={UPDATED_GRAFANA_PLUGIN_PINS}\n",
            encoding="utf-8",
        )
        merged_config = _render_compose_config(
            compose_command,
            demo_directory,
            environment,
            ("docker-compose.yml", "docker-compose.grafana.yml"),
        )
        _assert_rendered_grafana_plugin_environment(merged_config, UPDATED_GRAFANA_PLUGIN_PINS)
        for _stack_name, compose_files in GRAFANA_COMPOSE_STACKS[1:]:
            stack_directory = workspace / Path(compose_files[0]).parent
            config = _render_compose_config(compose_command, stack_directory, environment)
            _assert_rendered_grafana_plugin_environment(config, UPDATED_GRAFANA_PLUGIN_PINS)


def test_demo_prints_exact_default_ui_api_and_grafana_urls(tmp_path: Path) -> None:
    workspace = _copy_public_demo(tmp_path)
    environment, _command_log = _fake_environment(tmp_path)

    result = _run(workspace, environment, "--grafana")

    assert result.returncode == 0
    assert "UI: http://127.0.0.1:8081" in _output(result)
    assert "API: http://127.0.0.1:8080" in _output(result)
    assert "Grafana: http://127.0.0.1:3000" in _output(result)


@pytest.mark.parametrize(
    "arguments",
    [
        ("media", "--build"),
        ("media", "publish", "again"),
        ("media", "status", "--showcase"),
        ("media", "unknown"),
    ],
)
def test_demo_rejects_invalid_media_grammar_before_docker_git_or_media_mutation(
    tmp_path: Path,
    arguments: tuple[str, ...],
) -> None:
    workspace = _copy_public_demo(tmp_path)
    media_root = workspace / ".demo" / "media"
    media_root.mkdir(parents=True)
    sentinel = media_root / "retain-before-grammar-check"
    sentinel.write_text("retain", encoding="utf-8")
    environment, command_log = _fake_environment(tmp_path)

    result = _run(workspace, environment, *arguments)

    assert result.returncode == 2
    assert "./demo media" in _output(result)
    assert "./demo media publish" in _output(result)
    assert _commands(command_log) == []
    assert not Path(environment["DEMO_GIT_LOG"]).exists()
    assert _gh_commands(environment) == []
    assert sentinel.read_text(encoding="utf-8") == "retain"


def test_demo_media_refuses_active_interactive_stack_before_build_or_media_deletion(tmp_path: Path) -> None:
    workspace = _copy_public_demo(tmp_path)
    media_root = workspace / ".demo" / "media"
    media_root.mkdir(parents=True)
    sentinel = media_root / "previous-capture.txt"
    sentinel.write_text("keep", encoding="utf-8")
    environment, command_log = _fake_environment(
        tmp_path,
        interactive_services="chitragupta running (healthy)\n",
        interactive_service_ids="running-id\n",
    )

    result = _run(workspace, environment, "media")

    assert result.returncode == 1
    assert "Interactive Demo Stack is active. Run './demo down' before './demo media'." in _output(result)
    assert any(command[-2:] == ("ps", "--quiet") and GRAFANA in command for command in _commands(command_log))
    assert not any("build" in command or "pull" in command or "up" in command for command in _commands(command_log))
    assert sentinel.read_text(encoding="utf-8") == "keep"
    assert _gh_commands(environment) == []


def test_demo_media_ignores_stopped_interactive_services_before_build(tmp_path: Path) -> None:
    workspace = _copy_public_demo(tmp_path)
    environment, command_log = _fake_environment(
        tmp_path,
        interactive_services="chitragupta exited (0)\nchitragupta-ui exited (0)\n",
        interactive_service_ids="",
        failure="build",
    )

    result = _run(workspace, environment, "media")

    assert result.returncode == 72
    assert "Interactive Demo Stack is active" not in _output(result)
    assert _media_compose("build", "chitragupta", "chitragupta-ui", "media-capture") in _commands(command_log)
    assert any(command[-2:] == ("ps", "--quiet") and GRAFANA in command for command in _commands(command_log))


def test_demo_media_refuses_retained_media_stack_without_replacing_capture_evidence(tmp_path: Path) -> None:
    workspace = _copy_public_demo(tmp_path)
    media_root = workspace / ".demo" / "media"
    media_root.mkdir(parents=True)
    sentinel = media_root / "failed-encoder.webm"
    sentinel.write_text("inspect", encoding="utf-8")
    environment, command_log = _fake_environment(tmp_path, media_services="NAME\n", media_service_ids="media-capture\n")

    result = _run(workspace, environment, "media")

    assert result.returncode == 1
    expected_detail = "A Demo media stack already exists. Inspect it or run './demo media down' before capturing again."
    assert expected_detail in _output(result)
    commands = _commands(command_log)
    assert any(command[-3:] == ("ps", "--all", "--quiet") and MEDIA in command for command in commands)
    assert not any(
        "build" in command or "pull" in command or "up" in command or "down" in command for command in commands
    )
    assert sentinel.read_text(encoding="utf-8") == "inspect"
    assert _gh_commands(environment) == []


def test_demo_media_all_state_probe_accepts_a_header_only_empty_stack(tmp_path: Path) -> None:
    workspace = _copy_public_demo(tmp_path)
    media_root = workspace / ".demo" / "media"
    media_root.mkdir(parents=True)
    sentinel = media_root / "previous-capture.txt"
    sentinel.write_text("keep", encoding="utf-8")
    environment, command_log = _fake_environment(
        tmp_path,
        media_services="NAME IMAGE COMMAND SERVICE STATUS\n",
        media_service_ids="",
        failure="build",
    )

    result = _run(workspace, environment, "media")

    assert result.returncode != 0
    assert "A Demo media stack already exists" not in _output(result)
    assert any(command[-3:] == ("ps", "--all", "--quiet") for command in _commands(command_log))
    assert sentinel.read_text(encoding="utf-8") == "keep"


def test_demo_media_all_state_probe_blocks_a_stopped_container_id(tmp_path: Path) -> None:
    workspace = _copy_public_demo(tmp_path)
    media_root = workspace / ".demo" / "media"
    media_root.mkdir(parents=True)
    sentinel = media_root / "stopped-capture.webm"
    sentinel.write_text("inspect", encoding="utf-8")
    environment, command_log = _fake_environment(
        tmp_path,
        media_services="media-capture exited (0)\n",
        media_service_ids="deadbeef\n",
    )

    result = _run(workspace, environment, "media")

    assert result.returncode == 1
    assert "A Demo media stack already exists" in _output(result)
    assert any(command[-3:] == ("ps", "--all", "--quiet") for command in _commands(command_log))
    assert sentinel.read_text(encoding="utf-8") == "inspect"


def test_demo_media_prepares_bind_mount_directories_before_spec_preflight(tmp_path: Path) -> None:
    workspace = _copy_public_demo(tmp_path)
    media_root = workspace / ".demo" / "media"
    environment, command_log = _fake_environment(tmp_path, failure="media-spec")

    result = _run(workspace, environment, "media")

    assert result.returncode == 80
    assert media_root.is_dir()
    assert (media_root / "state").is_dir()
    assert _media_compose("run", "--rm", "--no-deps", "media-tool", "spec", "--spec", MEDIA_SPEC_PATH) in _commands(
        command_log
    )


def test_demo_media_builds_current_checkout_generates_isolated_showcase_state_and_cleans_only_media_project(
    tmp_path: Path,
) -> None:
    workspace = _copy_public_demo(tmp_path)
    interactive_state = workspace / ".demo" / "state" / "showcase"
    interactive_state.mkdir(parents=True)
    interactive_sentinel = interactive_state / "interactive.db"
    interactive_sentinel.write_text("do not delete", encoding="utf-8")
    environment, command_log = _fake_environment(tmp_path, media_outputs=True, require_stop_before_manifest=True)

    result = _run(workspace, environment, "media")

    assert result.returncode == 0, _output(result)
    commands = _commands(command_log)
    media_commands = _media_commands(command_log)
    assert all(MEDIA_PROJECT[1] in command and command[2:6] == MEDIA_FILES for command in media_commands)
    assert _media_compose("build", "chitragupta", "chitragupta-ui", "media-capture") in commands
    assert _media_compose("pull", "media-encoder") in commands
    assert _media_compose("run", "--rm", "--no-deps", "media-tool", "spec", "--spec", MEDIA_SPEC_PATH) in commands
    assert _media_compose("run", "--rm", "demo-generator") in commands
    catalog_command = _media_compose(
        "run",
        "--rm",
        "--no-deps",
        "media-tool",
        "catalog",
        "--spec",
        MEDIA_SPEC_PATH,
        "--config",
        "/app/config/config.yaml",
        "--output",
        "/app/media/work/synthetic-catalog.json",
        "--srt-output",
        "/app/media/work/captions.srt",
        "--state-dir",
        "/app/media/state",
    )
    assert catalog_command in commands
    assert _media_compose("up", "--detach", "--wait", "--force-recreate", "chitragupta", "chitragupta-ui") in commands
    assert (
        _media_compose(
            "run",
            "--rm",
            "--no-deps",
            "media-capture",
            "--spec",
            MEDIA_SPEC_PATH,
            "--catalog",
            "/app/media/work/synthetic-catalog.json",
            "--output",
            MEDIA_ROOT_PATH,
        )
        in commands
    )
    assert _media_compose("run", "--rm", "--no-deps", "media-encoder", MEDIA_SPEC_PATH, MEDIA_ROOT_PATH) in commands
    stop_command = _media_compose("stop", "chitragupta", "chitragupta-ui")
    manifest_command = _media_compose(
        "run",
        "--rm",
        "--no-deps",
        "media-tool",
        "manifest",
        "--spec",
        MEDIA_SPEC_PATH,
        "--media-root",
        MEDIA_ROOT_PATH,
        "--source-commit",
        "a" * 40,
        "--source-worktree-clean",
        "true",
    )
    down_command = _media_compose("down")
    catalog_indexes = [index for index, command in enumerate(commands) if command == catalog_command]
    assert len(catalog_indexes) == 2
    assert stop_command in commands
    assert manifest_command in commands
    assert down_command in commands
    stop_index = commands.index(stop_command)
    manifest_index = commands.index(manifest_command)
    assert catalog_indexes[0] < stop_index < catalog_indexes[1] < manifest_index < commands.index(down_command)
    assert (
        commands.index(_media_compose("build", "chitragupta", "chitragupta-ui", "media-capture"))
        < commands.index(_media_compose("run", "--rm", "demo-generator"))
        < commands.index(
            _media_compose("up", "--detach", "--wait", "--force-recreate", "chitragupta", "chitragupta-ui")
        )
    )
    generator_call = next(
        call for call in _calls(command_log) if call[-1] == _media_compose("run", "--rm", "demo-generator")
    )
    assert generator_call[1] == "local"
    assert generator_call[6] == "showcase"
    assert Path(generator_call[7]) == (workspace / ".demo" / "media" / "state").resolve()
    assert interactive_sentinel.read_text(encoding="utf-8") == "do not delete"
    docs_assets = workspace / "docs/assets/demo"
    assert not any(path.suffix in {".png", ".mp4", ".webm"} for path in docs_assets.glob("*"))
    assert (
        "Release poster: .demo/media/assets/chitragupta-demo-dashboard-poster.webp (tracked poster unchanged)"
        in _output(result)
    )
    assert (workspace / ".demo/media/assets/chitragupta-demo-walkthrough.mp4").is_file()
    assert ".demo/media" in _output(result)
    assert _gh_commands(environment) == []


def test_demo_media_dirty_authoring_capture_copies_reviewed_poster_to_docs(tmp_path: Path) -> None:
    workspace = _copy_public_demo(tmp_path)
    environment, _command_log = _fake_environment(tmp_path, media_outputs=True, git_status=" M demo")

    result = _run(workspace, environment, "media")

    assert result.returncode == 0, _output(result)
    poster = workspace / "docs/assets/demo/chitragupta-demo-dashboard-poster.webp"
    assert poster.read_text(encoding="utf-8") == "generated-chitragupta-demo-dashboard-poster.webp"
    assert "Reviewed poster: docs/assets/demo/chitragupta-demo-dashboard-poster.webp" in _output(result)
    assert "copied from .demo/media/assets/chitragupta-demo-dashboard-poster.webp" in _output(result)


def test_demo_media_clean_recapture_keeps_existing_reviewed_poster_unchanged(tmp_path: Path) -> None:
    workspace = _copy_public_demo(tmp_path)
    reviewed_poster = workspace / "docs/assets/demo/chitragupta-demo-dashboard-poster.webp"
    reviewed_poster.parent.mkdir(parents=True)
    reviewed_poster.write_text("reviewed poster", encoding="utf-8")
    environment, _command_log = _fake_environment(tmp_path, media_outputs=True)

    result = _run(workspace, environment, "media")

    assert result.returncode == 0, _output(result)
    assert reviewed_poster.read_text(encoding="utf-8") == "reviewed poster"
    assert (
        "Release poster: .demo/media/assets/chitragupta-demo-dashboard-poster.webp (tracked poster unchanged)"
        in _output(result)
    )
    assert (workspace / ".demo/media/assets/chitragupta-demo-dashboard-poster.webp").read_text(
        encoding="utf-8"
    ) == "generated-chitragupta-demo-dashboard-poster.webp"


@pytest.mark.parametrize("action", ["status", "logs", "down"])
def test_demo_media_diagnostics_target_only_the_retained_media_project(tmp_path: Path, action: str) -> None:
    workspace = _copy_public_demo(tmp_path)
    media_root = workspace / ".demo" / "media"
    media_root.mkdir(parents=True)
    sentinel = media_root / "captured.mp4"
    sentinel.write_text("retain output", encoding="utf-8")
    environment, command_log = _fake_environment(tmp_path, media_services="chitragupta\n")

    result = _run(workspace, environment, "media", action)

    assert result.returncode == 0, _output(result)
    expected_action = ("ps", "--all") if action == "status" else (action,)
    assert _commands(command_log)[-1] == _media_compose(*expected_action)
    assert sentinel.read_text(encoding="utf-8") == "retain output"
    assert _gh_commands(environment) == []


@pytest.mark.parametrize("failure", ["up", "media-capture", "media-encoder", "media-final-catalog", "media-manifest"])
def test_demo_media_retains_post_start_failures_for_diagnosis_without_interactive_teardown(
    tmp_path: Path,
    failure: str,
) -> None:
    workspace = _copy_public_demo(tmp_path)
    environment, command_log = _fake_environment(tmp_path, failure=failure)

    result = _run(workspace, environment, "media")

    assert result.returncode != 0
    assert "Demo media capture failed. Media containers were left in place for diagnosis." in _output(result)
    assert "Run './demo media status' to inspect service state." in _output(result)
    assert "Run './demo media logs' to inspect service output." in _output(result)
    assert "Run './demo media down' to stop the media stack." in _output(result)
    commands = _commands(command_log)
    assert _media_compose("down") not in commands
    assert not any(command == (*ALL_FILES, "down") for command in commands)
    assert _gh_commands(environment) == []


@pytest.mark.parametrize("failure", ["build", "pull", "media-spec", "generator", "media-catalog"])
def test_demo_media_reports_pre_start_failure_without_claiming_retained_containers(
    tmp_path: Path,
    failure: str,
) -> None:
    workspace = _copy_public_demo(tmp_path)
    environment, command_log = _fake_environment(tmp_path, failure=failure)

    result = _run(workspace, environment, "media")

    assert result.returncode != 0
    assert "Media containers were left in place" not in _output(result)
    assert _media_compose("down") not in _commands(command_log)
    assert _gh_commands(environment) == []


@pytest.mark.parametrize("failure", ["build", "pull", "media-spec"])
def test_demo_media_does_not_delete_previous_outputs_until_preflight_build_and_specification_succeed(
    tmp_path: Path,
    failure: str,
) -> None:
    workspace = _copy_public_demo(tmp_path)
    media_root = workspace / ".demo" / "media"
    media_root.mkdir(parents=True)
    sentinel = media_root / "previous-manifest.json"
    sentinel.write_text("retain until preflight completes", encoding="utf-8")
    environment, _command_log = _fake_environment(tmp_path, failure=failure)

    result = _run(workspace, environment, "media")

    assert result.returncode != 0
    assert sentinel.read_text(encoding="utf-8") == "retain until preflight completes"


def test_demo_media_publish_rejects_unvalidated_local_state_without_github_or_capture_activity(tmp_path: Path) -> None:
    workspace = _copy_public_demo(tmp_path)
    environment, command_log = _fake_environment(tmp_path)

    result = _run(workspace, environment, "media", "publish")

    assert result.returncode == 1
    assert "Validated Demo media is unavailable. Run './demo media' first." in _output(result)
    assert _gh_commands(environment) == []
    generation_tokens = ("build", "demo-generator", "media-capture", "media-encoder")
    assert not any(token in command for command in _commands(command_log) for token in generation_tokens)


def test_demo_media_publish_dispatches_the_real_validator_through_the_compose_boundary(tmp_path: Path) -> None:
    workspace = _copy_public_demo(tmp_path)
    media_root = workspace / ".demo" / "media"
    media_root.mkdir(parents=True)
    (media_root / "manifest.json").write_text("{}", encoding="utf-8")
    environment, command_log = _fake_environment(tmp_path, dispatch_real_media_validator=True)

    result = _run(workspace, environment, "media", "publish")

    assert result.returncode == 1
    assert "Demo media validation failed; publication was not attempted." in _output(result)
    assert _media_compose(
        "run",
        "--pull",
        "never",
        "--rm",
        "--no-deps",
        "media-tool",
        "validate",
        "--spec",
        MEDIA_SPEC_PATH,
        "--media-root",
        MEDIA_ROOT_PATH,
    ) in _commands(command_log)
    assert _gh_commands(environment) == []


def test_demo_media_publish_warns_for_an_unreleased_clean_commit_and_uploads_only_stable_assets(tmp_path: Path) -> None:
    workspace = _copy_public_demo(tmp_path)
    media_root = workspace / ".demo" / "media"
    assets = media_root / "assets"
    assets.mkdir(parents=True)
    stable_assets = (
        "chitragupta-demo-dashboard.png",
        "chitragupta-demo-cost-explorer.png",
        "chitragupta-demo-topic-attribution.png",
        "chitragupta-demo-pipeline-status.png",
        "chitragupta-demo-focus-mapping-preview.png",
        "chitragupta-demo-walkthrough.mp4",
        "manifest.json",
    )
    for name in stable_assets:
        destination = media_root / name if name == "manifest.json" else assets / name
        destination.write_text(name, encoding="utf-8")
    environment, command_log = _fake_environment(tmp_path)

    result = _run(workspace, environment, "media", "publish")

    assert result.returncode == 0, _output(result)
    warning = "WARNING: source commit aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa is not a published stable release"
    assert warning in _output(result)
    generation_tokens = ("build", "demo-generator", "media-capture", "media-encoder")
    assert not any(token in command for command in _commands(command_log) for token in generation_tokens)
    gh_commands = _gh_commands(environment)
    assert any(
        command.startswith("release create demo-media") and "--latest=false" in command for command in gh_commands
    )
    upload = next(command for command in gh_commands if command.startswith("release upload demo-media"))
    assert "--clobber" in upload
    assert {Path(path).name for path in upload.split() if path.startswith(str(media_root))} == set(stable_assets)
    assert "chitragupta-demo-dashboard-poster.webp" not in upload


def test_demo_media_publish_rejects_a_real_git_describe_failure(tmp_path: Path) -> None:
    workspace = _copy_public_demo(tmp_path)
    _write_fake_publish_workspace(workspace)
    environment, _command_log = _fake_environment(
        tmp_path,
        git_describe_status=128,
        git_describe_output="fatal: not a git repository (or any of the parent directories): .git",
    )

    result = _run(workspace, environment, "media", "publish")

    assert result.returncode == 128
    assert "Unable to inspect the source stable tag (git describe exited 128)." in _output(result)
    assert "not a git repository" in _output(result)
    assert _gh_commands(environment) == []


def test_demo_media_publish_keeps_a_published_exact_stable_tag_silent(tmp_path: Path) -> None:
    workspace = _copy_public_demo(tmp_path)
    _write_fake_publish_workspace(workspace)
    environment, _command_log = _fake_environment(
        tmp_path,
        git_tag="v1.2.3",
        gh_stable_release_exists=True,
    )

    result = _run(workspace, environment, "media", "publish")

    assert result.returncode == 0, _output(result)
    assert "not a published stable release" not in _output(result)
    assert "has no published GitHub Release" not in _output(result)


def test_demo_media_publish_keeps_an_existing_release_non_latest(tmp_path: Path) -> None:
    workspace = _copy_public_demo(tmp_path)
    media_root = workspace / ".demo" / "media"
    media_root.mkdir(parents=True)
    (media_root / "manifest.json").write_text("{}", encoding="utf-8")
    environment, _command_log = _fake_environment(tmp_path, gh_release_exists=True)

    result = _run(workspace, environment, "media", "publish")

    assert result.returncode == 0, _output(result)
    gh_commands = _gh_commands(environment)
    assert "release view demo-media --json isDraft,isPrerelease,url" in gh_commands
    assert not any(command.startswith("release create demo-media") for command in gh_commands)
    assert any(command.startswith("release edit demo-media") and "--latest=false" in command for command in gh_commands)


@pytest.mark.parametrize(
    ("failure", "expected_status", "diagnostic"),
    [
        ("release-create", 7, "Demo media GitHub Release creation failed"),
        ("release-edit", 8, "Demo media GitHub Release edit failed"),
        ("release-upload", 9, "Demo media GitHub Release upload failed"),
    ],
)
def test_demo_media_publish_preserves_github_failure_status_and_branch(
    tmp_path: Path,
    failure: str,
    expected_status: int,
    diagnostic: str,
) -> None:
    workspace = _copy_public_demo(tmp_path)
    _write_fake_publish_workspace(workspace)
    environment, _command_log = _fake_environment(
        tmp_path,
        gh_release_exists=failure == "release-edit",
        gh_failure=failure,
    )

    result = _run(workspace, environment, "media", "publish")

    assert result.returncode == expected_status
    assert diagnostic in _output(result)


def test_demo_media_publish_rejects_dirty_worktree_before_github_release_actions(tmp_path: Path) -> None:
    workspace = _copy_public_demo(tmp_path)
    _write_fake_publish_workspace(workspace)
    environment, _command_log = _fake_environment(tmp_path, git_status=" M demo")

    result = _run(workspace, environment, "media", "publish")

    assert result.returncode == 1
    assert "Demo media publication requires a clean worktree." in _output(result)
    assert _gh_commands(environment) == []


def test_demo_media_publish_rejects_capture_from_a_different_head(tmp_path: Path) -> None:
    workspace = _copy_public_demo(tmp_path)
    media_root = _write_fake_publish_workspace(workspace)
    (media_root / "manifest.json").write_text(
        '{"source_commit":"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb","source_worktree_clean":true}\n',
        encoding="utf-8",
    )
    environment, _command_log = _fake_environment(tmp_path)

    result = _run(workspace, environment, "media", "publish")

    assert result.returncode == 1
    assert "does not match current HEAD" in _output(result)
    assert _gh_commands(environment) == []


def test_demo_media_publish_distinguishes_missing_gh_from_release_probe_failure(tmp_path: Path) -> None:
    workspace = _copy_public_demo(tmp_path)
    _write_fake_publish_workspace(workspace)
    environment, _command_log = _fake_environment(tmp_path, gh_available=False)

    result = _run(workspace, environment, "media", "publish")

    assert result.returncode == 1
    assert "gh is required for Demo media publication." in _output(result)


def test_demo_media_publish_allows_an_exact_stable_tag_without_a_published_release(tmp_path: Path) -> None:
    workspace = _copy_public_demo(tmp_path)
    _write_fake_publish_workspace(workspace)
    environment, _command_log = _fake_environment(tmp_path, git_tag="v1.2.3", gh_stable_release_exists=False)

    result = _run(workspace, environment, "media", "publish")

    assert result.returncode == 0, _output(result)
    assert "has no published GitHub Release; continuing" in _output(result)


def test_demo_media_publish_stops_on_stable_release_infrastructure_failure(tmp_path: Path) -> None:
    workspace = _copy_public_demo(tmp_path)
    _write_fake_publish_workspace(workspace)
    environment, _command_log = _fake_environment(tmp_path, git_tag="v1.2.3", gh_failure="stable-view")

    result = _run(workspace, environment, "media", "publish")

    assert result.returncode == 5
    assert "Unable to verify the source release v1.2.3" in _output(result)


def test_demo_media_public_launcher_can_publish_a_production_shaped_capture(tmp_path: Path) -> None:
    workspace = _copy_public_demo(tmp_path)
    fixture_root = tmp_path / "fixture"
    fixture_root.mkdir()
    media_fixture = _write_real_media_fixture(workspace, fixture_root)
    environment, command_log = _fake_environment(
        tmp_path,
        media_outputs=True,
        media_fixture=media_fixture,
        dispatch_real_media_validator=True,
    )

    capture = _run(workspace, environment, "media")
    assert capture.returncode == 0, _output(capture)
    publication = _run(workspace, environment, "media", "publish")

    assert publication.returncode == 0, _output(publication)
    assert 'valid": true' in _output(publication)
    assert any(command.startswith("release upload demo-media") for command in _gh_commands(environment))
    assert _media_compose("down") in _commands(command_log)


def test_demo_media_capture_distinguishes_poster_copy_and_teardown_failures(tmp_path: Path) -> None:
    workspace = _copy_public_demo(tmp_path)
    environment, command_log = _fake_environment(tmp_path, media_outputs=True, cat_failure=True, git_status=" M demo")

    result = _run(workspace, environment, "media")

    assert result.returncode == 1
    assert "Demo media poster copy failed." in _output(result)
    assert _media_compose("down") not in _commands(command_log)

    teardown_root = tmp_path / "teardown"
    teardown_root.mkdir()
    workspace = _copy_public_demo(teardown_root)
    environment, command_log = _fake_environment(teardown_root, media_outputs=True, failure="teardown")

    result = _run(workspace, environment, "media")

    assert result.returncode == 79
    assert "Demo media teardown failed." in _output(result)
    assert _media_compose("down") in _commands(command_log)
