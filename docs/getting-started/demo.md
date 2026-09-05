# Credential-free Demo

The repository-root `./demo` launcher starts a deterministic, synthetic
Chitragupta stack without provider credentials. It uses the Clean profile by
default and keeps generated state under `.demo/state/clean`.

## Quick start

From a checkout, run:

```bash
git clone https://github.com/waliaabhishek/chitragupta.git
cd chitragupta
./demo
```

The launcher detects `docker compose` first and falls back to
`docker-compose`. The default services are reachable only from the local
machine:

| Service | URL |
|---|---|
| UI | <http://127.0.0.1:8081> |
| API | <http://127.0.0.1:8080> |

The one-shot generator fills the selected profile with synthetic data before
the API and UI are recreated and health-checked.

## Startup options

```text
./demo [--showcase] [--grafana] [--lan] [--build]
       [--ui-port PORT] [--api-port PORT] [--grafana-port PORT]
./demo reset [--clean|--showcase]
./demo status
./demo logs
./demo down
```

| Option | Behavior |
|---|---|
| `--showcase` | Selects the additive Showcase profile. Without it, startup uses Clean. |
| `--grafana` | Adds the optional Grafana service and prints its URL. |
| `--lan` | Binds selected ports to `0.0.0.0`, resolves a routable host IPv4, and prints LAN URLs. |
| `--build` | Uses `local` images and builds the backend and UI from this checkout. |
| `--ui-port PORT` | Changes the UI host port; default `8081`. |
| `--api-port PORT` | Changes the API host port; default `8080`. |
| `--grafana-port PORT` | Changes the Grafana host port; default `3000`. It is validated but does not enable Grafana. |

Port values are decimal integers from `1` through `65535`. Repeating a port
switch uses its last value. Duplicate selected ports are rejected before any
state or image work.

### Showcase and Grafana

Profiles have independent state directories. These examples select Showcase
and Grafana together:

```bash
./demo --showcase --grafana
```

Grafana is absent from a normal startup. When selected, it mounts the selected
Demo state read-only and reads the Confluent Cloud SQLite database at
`/var/lib/grafana/data/demo/confluent-cloud.db`. Shared dashboards and the
Demo datasource provisioning are also read-only. The default login is
`admin` / `password`; set `GF_ADMIN_PASSWORD` before startup to override the
password.

With `--grafana`, the default URL is
<http://127.0.0.1:3000>. Grafana reads the database directly and is not added
to the API/UI internal network.

### LAN exposure

`--lan` binds services to all IPv4 interfaces and resolves the primary
routable IPv4 address for display. After a successful health check it prints:

```text
WARNING: --lan exposes unauthenticated demo services and writable synthetic state to the local network.
UI: http://<resolved-lan-ip>:<ui-port>
API: http://<resolved-lan-ip>:<api-port>
Grafana: http://<resolved-lan-ip>:<grafana-port>
```

The Grafana line appears only when Grafana is selected. If a routable IPv4
cannot be resolved, startup stops before image acquisition. The displayed
URLs use the resolved address, never `0.0.0.0`.

## Images and source builds

Normal startup and reset resolve the image tag with:

```bash
git describe --tags --exact-match --match 'v*.*.*' HEAD
```

An exact matching release tag is used for the backend and UI GHCR images. An
untagged or otherwise unavailable result uses `latest`, then explicitly pulls
the selected backend/UI images (and Grafana when selected).

`--build` sets `DEMO_IMAGE_TAG=local`, builds `chitragupta` and
`chitragupta-ui` from this checkout, and skips backend/UI pulls. The backend
build also supplies the `demo-generator` image. If Grafana is selected, only
its published image is pulled.

## Lifecycle and failure behavior

| Command | Behavior |
|---|---|
| `./demo status` | Shows the Compose project status using both Compose definitions and returns its Compose status. |
| `./demo logs` | Prints the current aggregate log output once using both definitions; it does not follow logs. |
| `./demo down` | Removes Demo containers and networks but preserves `.demo`, databases, and the active-profile record. |
| `./demo reset` | Uses the last valid active profile, or Clean when none exists; preflights ports, stops the complete project, then deletes only the selected profile state. |
| `./demo reset --clean` | Explicitly resets Clean after successful preflight. |
| `./demo reset --showcase` | Explicitly resets Showcase after successful preflight. |

Reset accepts only its profile switch. Port and other startup switches are not
accepted with reset. Port preflight runs before reset stops containers or
deletes state. An invalid active-profile file requires an explicit reset
profile.

On pull, build, generator, health, or startup failure, the original nonzero
status is retained. Failed health-gated startup does not run `stop` or `down`,
does not record a successful profile, and does not print success URLs. The
launcher retains containers for diagnosis and prints:

```text
Demo startup failed. Containers were left in place for diagnosis.
Run './demo status' to inspect service state.
Run './demo logs' to inspect service output.
Run './demo down' to stop the stack.
```

If Compose reports an actual requested IPv4 publish failure after a deferred
native IPv6 wildcard listener was recorded, the launcher prints the matching
process/PID conflict before this retained-container guidance. Health and
other non-bind failures do not produce that port diagnosis.

## Demo media

![Chitragupta Demo dashboard](../assets/demo/chitragupta-demo-dashboard-poster.webp)

The media bundle uses the Showcase profile and fully synthetic data. It does not
call a provider or require provider credentials. The poster above is the only
capture asset tracked in this repository. Screenshots, the walkthrough video,
the generated database evidence, and the capture manifest are kept out of git;
the approved screenshots, video, and manifest are available from the stable
[`demo-media` GitHub Release](https://github.com/waliaabhishek/chitragupta/releases/tag/demo-media).

Stable release assets:

| Asset | Link |
|---|---|
| Dashboard screenshot | [chitragupta-demo-dashboard.png](https://github.com/waliaabhishek/chitragupta/releases/download/demo-media/chitragupta-demo-dashboard.png) |
| Cost explorer screenshot | [chitragupta-demo-cost-explorer.png](https://github.com/waliaabhishek/chitragupta/releases/download/demo-media/chitragupta-demo-cost-explorer.png) |
| Topic attribution screenshot | [chitragupta-demo-topic-attribution.png](https://github.com/waliaabhishek/chitragupta/releases/download/demo-media/chitragupta-demo-topic-attribution.png) |
| Pipeline status screenshot | [chitragupta-demo-pipeline-status.png](https://github.com/waliaabhishek/chitragupta/releases/download/demo-media/chitragupta-demo-pipeline-status.png) |
| FOCUS Mapping Preview screenshot | [chitragupta-demo-focus-mapping-preview.png](https://github.com/waliaabhishek/chitragupta/releases/download/demo-media/chitragupta-demo-focus-mapping-preview.png) |
| Walkthrough video | [chitragupta-demo-walkthrough.mp4](https://github.com/waliaabhishek/chitragupta/releases/download/demo-media/chitragupta-demo-walkthrough.mp4) |
| Capture manifest | [manifest.json](https://github.com/waliaabhishek/chitragupta/releases/download/demo-media/manifest.json) |

### Capture and operate the media stack

From the repository root, capture the current checkout with:

```bash
./demo media
```

This creates an isolated media workspace under `.demo/media`, generates fresh
Showcase state, captures the five still scenes and walkthrough, validates the
catalog and manifest, and copies the poster to
`docs/assets/demo/chitragupta-demo-dashboard-poster.webp`. A successful run
stops the API and UI before removing the media Compose containers and network;
the generated evidence remains under `.demo/media` for inspection.

Use these commands to inspect or clean up the media stack:

```bash
./demo media status
./demo media logs
./demo media down
```

If capture, validation, or teardown fails, the launcher retains the media
containers and prints the failure-specific diagnostic. Run `./demo media
status` and `./demo media logs` before cleanup; run `./demo media down` after
collecting the evidence. A retained media stack must be removed before a new
capture.

After a clean, reviewed capture, publish the existing validated assets
explicitly:

```bash
./demo media publish
```

Publication does not recapture the Demo. It requires a clean checkout, a
manifest produced from the current commit, and an authenticated GitHub CLI. It
uploads the stable screenshot, video, and manifest asset names listed above to
the `demo-media` release.

### Refresh media after a material change

Maintainers own the refresh when a UI, data, or capture change materially
changes the Demo experience:

1. Run `./demo media` and inspect every generated screenshot and the full video.
2. Review the visuals and the manifest values; correct the change if the
   capture no longer represents the intended experience.
3. Commit the implementation, documentation, and updated poster after review.
4. From that clean commit, run `./demo media` again to create the publishable
   capture and verify the result once more.
5. Run `./demo media publish` explicitly, then open the `demo-media` release and
   verify each published asset and its stable URL.

## Raw Compose escape hatch

Run these commands from the repository root. Set the same values that the
launcher would export so the Compose files select the intended profile,
state, ports, and image:

```bash
export DEMO_UID="$(id -u)"
export DEMO_GID="$(id -g)"
export DEMO_PROFILE=clean                 # or showcase
export DEMO_STATE_DIR="$PWD/.demo/state/${DEMO_PROFILE}"
export DEMO_BIND_ADDRESS=127.0.0.1        # use 0.0.0.0 for deliberate LAN exposure
export DEMO_UI_PORT=8081
export DEMO_API_PORT=8080
export DEMO_GRAFANA_PORT=3000
export DEMO_ANCHOR_DATE="$(date -u +%F)"
export DEMO_IMAGE_TAG="$(git describe --tags --exact-match --match 'v*.*.*' HEAD 2>/dev/null || printf latest)"
mkdir -p "$DEMO_STATE_DIR"
```

Use an absolute `DEMO_STATE_DIR` as above so both Compose frontends mount the
same selected state. Add `GF_ADMIN_PASSWORD` when using the Grafana override.

### Released images, base stack

The plugin and standalone commands are equivalent. The base file starts only
the backend and UI:

```bash
# Compose plugin
docker compose -f examples/demo/docker-compose.yml pull chitragupta chitragupta-ui
docker compose -f examples/demo/docker-compose.yml run --rm demo-generator
docker compose -f examples/demo/docker-compose.yml up --detach --wait --force-recreate --remove-orphans chitragupta chitragupta-ui

# Standalone Compose
docker-compose -f examples/demo/docker-compose.yml pull chitragupta chitragupta-ui
docker-compose -f examples/demo/docker-compose.yml run --rm demo-generator
docker-compose -f examples/demo/docker-compose.yml up --detach --wait --force-recreate --remove-orphans chitragupta chitragupta-ui
```

### Released images, Grafana stack

The override adds Grafana and mounts the same selected profile read-only:

```bash
# Compose plugin
docker compose -f examples/demo/docker-compose.yml -f examples/demo/docker-compose.grafana.yml pull chitragupta chitragupta-ui grafana
docker compose -f examples/demo/docker-compose.yml -f examples/demo/docker-compose.grafana.yml run --rm demo-generator
docker compose -f examples/demo/docker-compose.yml -f examples/demo/docker-compose.grafana.yml up --detach --wait --force-recreate chitragupta chitragupta-ui grafana

# Standalone Compose
docker-compose -f examples/demo/docker-compose.yml -f examples/demo/docker-compose.grafana.yml pull chitragupta chitragupta-ui grafana
docker-compose -f examples/demo/docker-compose.yml -f examples/demo/docker-compose.grafana.yml run --rm demo-generator
docker-compose -f examples/demo/docker-compose.yml -f examples/demo/docker-compose.grafana.yml up --detach --wait --force-recreate chitragupta chitragupta-ui grafana
```

For a source build, export `DEMO_IMAGE_TAG=local`, replace the selected
`pull` for backend/UI with `build chitragupta chitragupta-ui`, and keep the
generator and `up` commands. With Grafana selected, pull `grafana` separately
before `build`; the Grafana image is not built from this checkout.

For example, the build command is:

```bash
export DEMO_IMAGE_TAG=local

# Compose plugin
docker compose -f examples/demo/docker-compose.yml build chitragupta chitragupta-ui
docker compose -f examples/demo/docker-compose.yml -f examples/demo/docker-compose.grafana.yml pull grafana

# Standalone Compose
docker-compose -f examples/demo/docker-compose.yml build chitragupta chitragupta-ui
docker-compose -f examples/demo/docker-compose.yml -f examples/demo/docker-compose.grafana.yml pull grafana
```

Run the generator and `up` command from the matching base or Grafana block
above after the build (and Grafana pull, when selected).

### Operations

Operations load both definitions so they can inspect or remove a stack that
was started with or without Grafana. Loading the override does not create a
Grafana container:

```bash
# Compose plugin
docker compose -f examples/demo/docker-compose.yml -f examples/demo/docker-compose.grafana.yml ps
docker compose -f examples/demo/docker-compose.yml -f examples/demo/docker-compose.grafana.yml logs
docker compose -f examples/demo/docker-compose.yml -f examples/demo/docker-compose.grafana.yml down

# Standalone Compose
docker-compose -f examples/demo/docker-compose.yml -f examples/demo/docker-compose.grafana.yml ps
docker-compose -f examples/demo/docker-compose.yml -f examples/demo/docker-compose.grafana.yml logs
docker-compose -f examples/demo/docker-compose.yml -f examples/demo/docker-compose.grafana.yml down
```

These raw commands do not delete `.demo`. The launcher additionally performs
port preflight and reset ordering; use `./demo reset` when resetting a profile.

## Port and host troubleshooting

The launcher checks duplicate selected ports, the current Demo service
binding, running Docker mappings, and host listeners before image work.
Conflict messages include the affected Demo service and port:

```text
Port 8080 for API is in use by Docker container other-api (abc123).
Port 8081 for UI is in use by process python3 (PID 4127).
Port 8080 is requested by both API and UI; choose distinct ports.
```

Stop or reconfigure the named external container/process, or select another
host port. The current Demo service is allowed to be force-recreated; an
unrelated container with a similar name is not.

### Linux

Linux inspection reads listening rows from `/proc/net/tcp` and `/proc/net/tcp6`
and maps socket inodes to `/proc/<pid>/comm`. If inode ownership is restricted,
the launcher tries already available `lsof`, `ss`, and `fuser` results that
contain both a PID and command. An authoritative listener that cannot be
identified fails closed with an inspection-permission diagnostic.

Native IPv6 wildcard listeners are ambiguous because the kernel table does
not expose socket-specific `IPV6_V6ONLY`. They are deferred to the actual
Compose IPv4 bind: successful startup proves no conflict; a matching Compose
publish failure reports the saved owner before retained-container guidance.

### macOS

macOS resolves `/usr/sbin/lsof` first and then `PATH`, using:

```bash
lsof -nP +c 0 -iTCP:8080 -sTCP:LISTEN
```

`+c 0` keeps command names untruncated for diagnostics. Empty output, and a
genuine status-1 no-match with no diagnostics, means no listener. An
inspection error or record that cannot provide the designed PID/type/endpoint
fails closed with a service/port inspection diagnostic. Native IPv6 wildcard
rows remain deferred until Compose proves an IPv4 bind failure.

If Compose selection fails, install Docker with either the Compose plugin or
the standalone `docker-compose` command and rerun the launcher. The launcher
does not silently switch to a different frontend after one has been selected.
