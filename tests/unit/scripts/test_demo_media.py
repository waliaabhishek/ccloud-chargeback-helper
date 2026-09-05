from __future__ import annotations

import hashlib
import json
import os
import struct
import subprocess
import sys
import zlib
from collections.abc import Callable
from copy import deepcopy
from pathlib import Path
from typing import Any

import pytest
import yaml

PROJECT_ROOT = Path(__file__).resolve().parents[3]
MEDIA_DIR = PROJECT_ROOT / "examples/demo/media"
MEDIA_TOOL = MEDIA_DIR / "tool.py"
SPEC_PATH = MEDIA_DIR / "capture-spec.json"
MEDIA_COMPOSE = PROJECT_ROOT / "examples/demo/docker-compose.media.yml"
SOURCE_COMMIT = "a" * 40
PRIMARY_TENANT = {
    "name": "clean-confluent",
    "id": "northstar-confluent",
    "ecosystem": "confluent_cloud",
}
PNG_ASSETS = (
    "chitragupta-demo-dashboard.png",
    "chitragupta-demo-cost-explorer.png",
    "chitragupta-demo-topic-attribution.png",
    "chitragupta-demo-pipeline-status.png",
    "chitragupta-demo-focus-mapping-preview.png",
)
POSTER = "chitragupta-demo-dashboard-poster.webp"
VIDEO = "chitragupta-demo-walkthrough.mp4"
EXPECTED_ASSETS = (*PNG_ASSETS, POSTER, VIDEO)
VALIDATION_FIELDS = (
    "fresh_showcase_state",
    "database_matches_catalog",
    "api_identifiers_match_catalog",
    "dom_identifiers_match_catalog",
    "browser_requests_local_only",
    "screenshots_are_1600x900_png",
    "poster_is_960x540_webp",
    "video_duration_in_range",
    "video_has_no_audio",
    "captions_burned_in",
    "webm_removed",
    "complete",
)


def _capture_spec() -> dict[str, Any]:
    return {
        "schema_version": 1,
        "anchor_date": "2026-08-31",
        "profile": "showcase",
        "viewport": {"width": 1600, "height": 900},
        "poster": {"width": 960, "height": 540, "name": POSTER},
        "video": {
            "target_seconds": 75,
            "minimum_seconds": 60,
            "maximum_seconds": 90,
            "name": VIDEO,
        },
        "primary_tenant": PRIMARY_TENANT,
        "screenshots": [
            {"name": PNG_ASSETS[0], "route": "/dashboard"},
            {"name": PNG_ASSETS[1], "route": "/explorer"},
            {"name": PNG_ASSETS[2], "route": "/topic-attributions"},
            {"name": PNG_ASSETS[3], "route": "/pipeline"},
            {"name": PNG_ASSETS[4], "route": "/focus-preview"},
        ],
        "captions": [
            {"start": 0, "end": 13, "text": "Synthetic tenant cost reconciles across usage and shared spend."},
            {"start": 13, "end": 30, "text": "Tenant cost is the analytical root."},
            {
                "start": 30,
                "end": 44,
                "text": "Topic-level attribution exposes cost concentration and candidates for review.",
            },
            {"start": 44, "end": 56, "text": "Persisted processing state remains inspectable in API-only mode."},
            {
                "start": 56,
                "end": 75,
                "text": "Generate a FOCUS 1.4 mapping preview from synthetic persisted evidence.",
            },
        ],
    }


def _write_json(path: Path, contents: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(contents, indent=2), encoding="utf-8")


def _png(width: int = 1600, height: int = 900) -> bytes:
    def chunk(kind: bytes, contents: bytes) -> bytes:
        checksum = struct.pack(">I", zlib.crc32(kind + contents) & 0xFFFFFFFF)
        return struct.pack(">I", len(contents)) + kind + contents + checksum

    row = b"\x00" + (b"\x1a\x2b\x3c" * width)
    header = struct.pack(">IIBBBBB", width, height, 8, 2, 0, 0, 0)
    return b"\x89PNG\r\n\x1a\n" + b"".join(
        (chunk(b"IHDR", header), chunk(b"IDAT", zlib.compress(row * height)), chunk(b"IEND", b""))
    )


def _webp(width: int = 960, height: int = 540) -> bytes:
    payload = b"\x00\x00\x00\x00" + (width - 1).to_bytes(3, "little") + (height - 1).to_bytes(3, "little")
    chunk = b"VP8X" + struct.pack("<I", len(payload)) + payload
    return b"RIFF" + struct.pack("<I", len(b"WEBP") + len(chunk)) + b"WEBP" + chunk


def _asset_entry(path: Path, **properties: object) -> dict[str, object]:
    return {"sha256": hashlib.sha256(path.read_bytes()).hexdigest(), "bytes": path.stat().st_size, **properties}


def _captions_srt() -> str:
    return (
        "1\n00:00:00,000 --> 00:00:13,000\nSynthetic tenant cost reconciles across usage and shared spend.\n\n"
        "2\n00:00:13,000 --> 00:00:30,000\nTenant cost is the analytical root.\n\n"
        "3\n00:00:30,000 --> 00:00:44,000\n"
        "Topic-level attribution exposes cost concentration and candidates for review.\n\n"
        "4\n00:00:44,000 --> 00:00:56,000\nPersisted processing state remains inspectable in API-only mode.\n\n"
        "5\n00:00:56,000 --> 00:01:15,000\n"
        "Generate a FOCUS 1.4 mapping preview from synthetic persisted evidence.\n"
    )


def _write_media_workspace(tmp_path: Path) -> tuple[Path, Path]:
    spec_path = tmp_path / "capture-spec.json"
    _write_json(spec_path, _capture_spec())
    media_root = tmp_path / "media"
    assets = media_root / "assets"
    assets.mkdir(parents=True)
    asset_entries: dict[str, dict[str, object]] = {}
    for name in PNG_ASSETS:
        path = assets / name
        path.write_bytes(_png())
        asset_entries[name] = _asset_entry(path, width=1600, height=900, media_type="image/png")
    poster = assets / POSTER
    poster.write_bytes(_webp())
    asset_entries[POSTER] = _asset_entry(poster, width=960, height=540, media_type="image/webp")
    video = assets / VIDEO
    video.write_bytes(b"synthetic-test-mp4")
    asset_entries[VIDEO] = _asset_entry(
        video,
        duration_seconds=75,
        video_codec="h264",
        width=1600,
        height=900,
        frame_rate=30,
        audio_stream_count=0,
        media_type="video/mp4",
    )
    state_dir = media_root / "state"
    _write_json(
        state_dir / "demo-state.json",
        {"schema_version": 1, "generator_version": 1, "profile": "showcase", "anchor_date": "2026-08-31"},
    )
    database_entries = []
    for database_name, contents in (("confluent-cloud.db", b"confluent"), ("self-managed-kafka.db", b"self-managed")):
        database_path = state_dir / database_name
        database_path.write_bytes(contents)
        database_entries.append(
            {"path": database_name, "sha256": hashlib.sha256(contents).hexdigest(), "bytes": len(contents)}
        )
    _write_json(
        media_root / "work/browser-observations.json",
        {
            "ui_origin": "http://chitragupta-ui",
            "requests": [{"url": "http://chitragupta-ui/dashboard", "kind": "document"}],
            "api_identifiers_match_catalog": True,
            "dom_identifiers_match_catalog": True,
        },
    )
    (media_root / "work/captions.srt").write_text(_captions_srt(), encoding="utf-8")
    _write_json(
        media_root / "work/encoder-result.json",
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
    _write_json(
        media_root / "work/synthetic-catalog.json",
        {
            "schema_version": 1,
            "anchor_date": "2026-08-31",
            "profile": "showcase",
            "primary_tenant": PRIMARY_TENANT,
            "representative_identifiers": ["11111111-1111-4111-8111-111111111111"],
            "source_identifiers": [
                "11111111-1111-4111-8111-111111111111",
                "lkc-commerce:topic:orders.created.v1",
            ],
            "tenants": [],
            "scenarios": [
                {
                    "scenario": {
                        "organization_id": "11111111-1111-4111-8111-111111111111",
                        "resource_id": "lkc-commerce:topic:orders.created.v1",
                    }
                }
            ],
            "database_evidence": {
                "schema_version": 1,
                "validated": True,
                "state_metadata": {
                    "schema_version": 1,
                    "generator_version": 1,
                    "profile": "showcase",
                    "anchor_date": "2026-08-31",
                },
                "files": [
                    {
                        "path": "demo-state.json",
                        "sha256": hashlib.sha256((state_dir / "demo-state.json").read_bytes()).hexdigest(),
                        "bytes": (state_dir / "demo-state.json").stat().st_size,
                    },
                    *database_entries,
                ],
            },
        },
    )
    _write_json(
        media_root / "manifest.json",
        {
            "schema_version": 1,
            "source_commit": SOURCE_COMMIT,
            "source_worktree_clean": True,
            "anchor_date": "2026-08-31",
            "profile": "showcase",
            "primary_tenant": PRIMARY_TENANT,
            "expected_assets": list(EXPECTED_ASSETS),
            "assets": asset_entries,
            "validation": dict.fromkeys(VALIDATION_FIELDS, True),
        },
    )
    return spec_path, media_root


def _run_tool(*arguments: str) -> subprocess.CompletedProcess[str]:
    assert MEDIA_TOOL.is_file(), "media validator is not implemented"
    environment = {**os.environ, "PYTHONPATH": str(PROJECT_ROOT / "src")}
    return subprocess.run(
        [sys.executable, str(MEDIA_TOOL), *arguments],
        cwd=PROJECT_ROOT,
        env=environment,
        capture_output=True,
        text=True,
        check=False,
    )


def _validate_workspace(spec_path: Path, media_root: Path) -> subprocess.CompletedProcess[str]:
    return _run_tool("validate", "--spec", str(spec_path), "--media-root", str(media_root))


def test_committed_capture_specification_is_the_exact_reproducible_media_input() -> None:
    assert SPEC_PATH.is_file(), "committed media capture specification is not implemented"
    assert json.loads(SPEC_PATH.read_text(encoding="utf-8")) == _capture_spec()


def test_media_tool_reports_the_validated_showcase_profile_and_committed_anchor(tmp_path: Path) -> None:
    spec_path = tmp_path / "capture-spec.json"
    _write_json(spec_path, _capture_spec())

    result = _run_tool("spec", "--spec", str(spec_path))

    assert result.returncode == 0, result.stderr
    assert json.loads(result.stdout) == {"profile": "showcase", "anchor_date": "2026-08-31"}


@pytest.mark.parametrize(
    "mutate",
    [
        pytest.param(lambda spec: spec.pop("anchor_date"), id="missing-required-field"),
        pytest.param(lambda spec: spec.update({"unexpected": True}), id="unknown-field"),
        pytest.param(lambda spec: spec.update({"schema_version": "1"}), id="wrong-field-type"),
        pytest.param(lambda spec: spec.update({"profile": "clean"}), id="wrong-profile"),
        pytest.param(lambda spec: spec["screenshots"].pop(), id="missing-approved-screen"),
        pytest.param(
            lambda spec: spec["captions"].__setitem__(0, {"start": 1, "end": 13, "text": "shifted"}),
            id="gap",
        ),
    ],
)
def test_media_tool_rejects_noncanonical_capture_specifications(
    tmp_path: Path,
    mutate: Callable[[dict[str, Any]], object],
) -> None:
    specification = deepcopy(_capture_spec())
    mutate(specification)
    spec_path = tmp_path / "capture-spec.json"
    _write_json(spec_path, specification)

    result = _run_tool("spec", "--spec", str(spec_path))

    assert result.returncode != 0


def test_media_tool_catalog_projects_current_scenarios_with_both_tenants_and_no_external_source(tmp_path: Path) -> None:
    spec_path = tmp_path / "capture-spec.json"
    catalog_path = tmp_path / "synthetic-catalog.json"
    srt_path = tmp_path / "captions.srt"
    _write_json(spec_path, _capture_spec())

    result = _run_tool(
        "catalog",
        "--spec",
        str(spec_path),
        "--config",
        str(PROJECT_ROOT / "examples/demo/config.yaml"),
        "--output",
        str(catalog_path),
        "--srt-output",
        str(srt_path),
    )

    assert result.returncode == 0, result.stderr
    catalog = catalog_path.read_text(encoding="utf-8")
    assert catalog.index("clean-confluent") < catalog.index("clean-self-managed")
    for identifier in (
        "northstar-confluent",
        "northstar-self-managed",
        "env-commerce",
        "cluster-commerce",
        "topic-orders",
    ):
        assert identifier in catalog
    assert "prometheus.invalid" not in catalog
    assert "11111111-1111-4111-8111-111111111111" in catalog
    assert "lkc-commerce:topic:orders.created.v1" in catalog
    assert srt_path.read_text(encoding="utf-8") == (
        "1\n00:00:00,000 --> 00:00:13,000\nSynthetic tenant cost reconciles across usage and shared spend.\n\n"
        "2\n00:00:13,000 --> 00:00:30,000\nTenant cost is the analytical root.\n\n"
        "3\n00:00:30,000 --> 00:00:44,000\n"
        "Topic-level attribution exposes cost concentration and candidates for review.\n\n"
        "4\n00:00:44,000 --> 00:00:56,000\nPersisted processing state remains inspectable in API-only mode.\n\n"
        "5\n00:00:56,000 --> 00:01:15,000\n"
        "Generate a FOCUS 1.4 mapping preview from synthetic persisted evidence.\n"
    )


def test_media_tool_validate_accepts_a_complete_clean_manifest_and_exact_asset_set(tmp_path: Path) -> None:
    spec_path, media_root = _write_media_workspace(tmp_path)

    result = _validate_workspace(spec_path, media_root)

    assert result.returncode == 0, result.stderr


def test_capture_video_uses_one_recording_epoch_for_nominal_seventy_five_seconds() -> None:
    capture_source = (MEDIA_DIR / "capture.mjs").read_text(encoding="utf-8")

    assert "const recordingStarted = Date.now();" in capture_source
    assert "const started = Date.now();" not in capture_source
    assert "interval.end * 1000 - (Date.now() - recordingStarted)" in capture_source


def test_frontend_refine_disables_telemetry_for_the_public_demo() -> None:
    app_source = (PROJECT_ROOT / "frontend/src/App.tsx").read_text(encoding="utf-8")

    assert "options={{ disableTelemetry: true, syncWithLocation: true }}" in app_source


def test_capture_explorer_readiness_uses_current_stable_contracts() -> None:
    capture_source = (MEDIA_DIR / "capture.mjs").read_text(encoding="utf-8")

    assert 'markers: ["Cost Explorer"]' in capture_source
    assert 'markers: ["Cost Explorer", "Timeline"]' not in capture_source
    for selector in ("breadcrumb-trail", "graph-container", "timeline-scrubber"):
        assert f'[data-testid="{selector}"]' in capture_source
    assert "page.getByText(anchorDate, { exact: true })" in capture_source
    assert 'getByText("clean-confluent (confluent_cloud)", { exact: true })' in capture_source


def test_capture_pipeline_uses_icon_compatible_button_contract() -> None:
    capture_source = (MEDIA_DIR / "capture.mjs").read_text(encoding="utf-8")

    assert 'getByRole("button", { name: "Run Pipeline", exact: false })' in capture_source
    assert 'getByRole("button", { name: "Run Pipeline", exact: true })' not in capture_source
    assert 'for (const marker of ["Gathering", "Calculating", "Topic Attribution Stage", "Emitting"])' in capture_source
    assert "page.getByText(marker, { exact: true })" in capture_source
    assert "runPipeline.isDisabled()" in capture_source
    assert 'runPipeline.locator("..").hover()' in capture_source
    assert 'getByText("Pipeline execution is unavailable in API-only mode.", { exact: false })' in capture_source


def test_capture_waits_for_topic_attribution_network_activity_after_analytics() -> None:
    capture_source = (MEDIA_DIR / "capture.mjs").read_text(encoding="utf-8")
    prepare_source = capture_source[
        capture_source.index("async function prepareTopicAttribution") : capture_source.index(
            "async function preparePipeline"
        )
    ]

    click_position = prepare_source.index("await analyticsButton.click();")
    marker_position = prepare_source.index('getByText("Top Topics by Cost"')
    assert click_position < marker_position
    assert 'waitForLoadState("networkidle")' not in prepare_source


def test_capture_waits_for_shared_scene_quiescence_before_still_and_video_validation() -> None:
    capture_source = (MEDIA_DIR / "capture.mjs").read_text(encoding="utf-8")
    assert "const SCENE_QUIET_PERIOD_MS = 250;" in capture_source
    assert "const SCENE_QUIESCENCE_TIMEOUT_MS = 45_000;" in capture_source
    assert "async function waitForSceneQuiescence(inFlightRequests, responsePromises)" in capture_source
    assert "const deadline = Date.now() + SCENE_QUIESCENCE_TIMEOUT_MS;" in capture_source
    assert "inFlightRequests.size === 0" in capture_source
    assert "Date.now() - quietSince >= SCENE_QUIET_PERIOD_MS" in capture_source
    assert "await Promise.all(responsePromises);" in capture_source
    assert "capture scene did not quiesce within" in capture_source

    still_source = capture_source[
        capture_source.index("async function captureStillScenes") : capture_source.index("async function captureVideo")
    ]
    video_source = capture_source[
        capture_source.index("async function captureVideo") : capture_source.index("async function main")
    ]
    for source in (still_source, video_source):
        preparation_position = source.index("prepareFocusPreview")
        quiescence_position = source.index("await waitForSceneQuiescence(inFlightRequests, responsePromises);")
        body_position = source.index('const bodyText = await page.locator("body").innerText();')
        assert preparation_position < quiescence_position < body_position
    assert "await page.screenshot" in still_source
    assert "const remaining = interval.end" in video_source


def test_capture_tracks_request_lifecycle_without_suppressing_failures() -> None:
    capture_source = (MEDIA_DIR / "capture.mjs").read_text(encoding="utf-8")

    assert "const inFlightRequests = new Set();" in capture_source
    assert "inFlightRequests.add(request);" in capture_source
    assert 'page.on("requestfinished", (request) => inFlightRequests.delete(request));' in capture_source
    assert 'page.on("requestfailed", (request) => {' in capture_source
    assert "inFlightRequests.delete(request);" in capture_source
    assert "recordRequest(request, true);" in capture_source


def test_capture_allows_only_collected_runtime_preview_ids_in_api_and_dom_checks() -> None:
    capture_source = (MEDIA_DIR / "capture.mjs").read_text(encoding="utf-8")

    assert (
        "function collectRuntimePreviewIds(value, target = new Set(), invalid = new Set(), catalogStrings = new Set())"
        in capture_source
    )
    assert "RUNTIME_PREVIEW_ID_KEYS.has(key)" in capture_source
    assert '"calculation_id"' in capture_source
    assert "RUNTIME_PREVIEW_ID_FORMATS" in capture_source
    assert "format?.test(item)" in capture_source
    assert "catalogStrings.has(item)" in capture_source
    assert "invalid.add(`${key}=${JSON.stringify(item)}`)" in capture_source
    assert "target.add(item)" in capture_source
    assert "function sourceIdentifierTokens(value, runtimePreviewIds = new Set())" in capture_source
    assert "!runtimePreviewIds.has(token)" in capture_source
    assert "withoutRuntimePreviewIds" not in capture_source
    assert "const invalidRuntimePreviewIds = new Set();" in capture_source
    assert "API runtime Preview identifier is malformed" in capture_source
    assert (
        "collectRuntimePreviewIds(body, runtimePreviewIds, invalidRuntimePreviewIds, catalogStrings);" in capture_source
    )
    assert (
        "collectRuntimePreviewIds(queued, runtimePreviewIds, invalidQueuedRuntimePreviewIds, catalogStrings);"
        in capture_source
    )
    assert (
        "collectRuntimePreviewIds(status, runtimePreviewIds, invalidStatusRuntimePreviewIds, catalogStrings);"
        in capture_source
    )
    assert "sourceIdentifierTokens(bodyText, runtimePreviewIds)" in capture_source
    assert "if (!catalogStrings.has(token))" in capture_source


def test_capture_keeps_out_of_catalog_source_identifiers_and_malformed_preview_ids_blocking() -> None:
    capture_source = (MEDIA_DIR / "capture.mjs").read_text(encoding="utf-8")

    assert "const isCatalogCalculation =" in capture_source
    assert "Persisted scenario calculation identifiers remain source evidence." in capture_source
    assert "if (!catalogStrings.has(token))" in capture_source
    assert "DOM identifier is not in the synthetic catalog" in capture_source
    assert "API identifier is not in the synthetic catalog" in capture_source
    assert "FOCUS Mapping Preview submission contains malformed runtime identifiers" in capture_source
    assert "FOCUS Mapping Preview status contains malformed runtime identifiers" in capture_source


def test_capture_validates_topic_names_from_api_and_current_dom_cells_against_scenarios() -> None:
    capture_source = (MEDIA_DIR / "capture.mjs").read_text(encoding="utf-8")

    assert "function collectTopicNames(value, target = new Set())" in capture_source
    assert 'key === "topic_name" && typeof item === "string" && item' in capture_source
    assert "const catalogTopicNames = collectTopicNames(catalog.scenarios);" in capture_source
    assert "API topic_name is not in the synthetic catalog" in capture_source
    assert 'page.locator(\'[role="gridcell"][col-id="topic_name"]\')' in capture_source
    assert "DOM topic_name is not in the synthetic catalog" in capture_source
    assert 'error.startsWith("API topic_name")' in capture_source
    assert 'error.startsWith("DOM topic_name")' in capture_source


@pytest.mark.parametrize(
    "mutation",
    [
        pytest.param("captions", id="captions-drift"),
        pytest.param("database", id="database-drift"),
        pytest.param("catalog", id="catalog-identifiers-drift"),
    ],
)
def test_media_tool_manifest_rejects_catalog_caption_or_database_mutations(
    tmp_path: Path,
    mutation: str,
) -> None:
    spec_path, media_root = _write_media_workspace(tmp_path)
    if mutation == "captions":
        (media_root / "work/captions.srt").write_text("changed\n", encoding="utf-8")
    elif mutation == "database":
        (media_root / "state/confluent-cloud.db").write_bytes(b"mutated")
    else:
        catalog = json.loads((media_root / "work/synthetic-catalog.json").read_text(encoding="utf-8"))
        catalog["source_identifiers"] = ["unexpected"]
        _write_json(media_root / "work/synthetic-catalog.json", catalog)
    (media_root / "manifest.json").unlink()

    result = _run_tool(
        "manifest",
        "--spec",
        str(spec_path),
        "--media-root",
        str(media_root),
        "--source-commit",
        SOURCE_COMMIT,
        "--source-worktree-clean",
        "true",
    )

    assert result.returncode != 0
    assert not (media_root / "manifest.json").exists()


@pytest.mark.parametrize(
    "mutation",
    [
        pytest.param(lambda manifest, root: manifest.pop("anchor_date"), id="missing-field"),
        pytest.param(lambda manifest, root: manifest.update({"unexpected": True}), id="unknown-field"),
        pytest.param(lambda manifest, root: manifest.update({"schema_version": "1"}), id="wrong-type"),
        pytest.param(lambda manifest, root: manifest["expected_assets"].append(PNG_ASSETS[0]), id="duplicate-asset"),
        pytest.param(lambda manifest, root: manifest["expected_assets"].pop(), id="missing-asset"),
        pytest.param(
            lambda manifest, root: manifest["expected_assets"].append("unexpected.png"), id="unexpected-asset"
        ),
        pytest.param(
            lambda manifest, root: manifest["assets"][PNG_ASSETS[0]].update({"width": 1}), id="wrong-dimensions"
        ),
        pytest.param(
            lambda manifest, root: manifest["validation"].update({"complete": False}), id="incomplete-validation"
        ),
        pytest.param(lambda manifest, root: manifest.update({"source_worktree_clean": False}), id="dirty-capture"),
        pytest.param(lambda manifest, root: manifest.update({"source_commit": "b" * 39}), id="bad-source-commit"),
        pytest.param(
            lambda manifest, root: (root / "assets" / PNG_ASSETS[0]).write_bytes(b"modified"),
            id="modified-media",
        ),
    ],
)
def test_media_tool_validate_rejects_invalid_or_modified_capture_evidence(
    tmp_path: Path,
    mutation: Callable[[dict[str, Any], Path], object],
) -> None:
    spec_path, media_root = _write_media_workspace(tmp_path)
    manifest_path = media_root / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    mutation(manifest, media_root)
    _write_json(manifest_path, manifest)

    result = _validate_workspace(spec_path, media_root)

    assert result.returncode != 0


@pytest.mark.parametrize(
    "encoder_result",
    [
        pytest.param({"duration_seconds": 59}, id="below-minimum-duration"),
        pytest.param({"duration_seconds": 91}, id="above-maximum-duration"),
        pytest.param({"audio_stream_count": 1}, id="audio-present"),
        pytest.param({"width": 1280, "height": 720}, id="wrong-dimensions"),
        pytest.param({"video_codec": "vp9"}, id="wrong-codec"),
        pytest.param({"captions_burned_in": False}, id="captions-not-burned"),
        pytest.param({"webm_removed": False}, id="retained-webm"),
    ],
)
def test_media_tool_manifest_rejects_invalid_encoder_evidence(
    tmp_path: Path,
    encoder_result: dict[str, object],
) -> None:
    spec_path, media_root = _write_media_workspace(tmp_path)
    encoder_path = media_root / "work/encoder-result.json"
    expected = json.loads(encoder_path.read_text(encoding="utf-8"))
    expected.update(encoder_result)
    _write_json(encoder_path, expected)
    (media_root / "manifest.json").unlink()

    result = _run_tool(
        "manifest",
        "--spec",
        str(spec_path),
        "--media-root",
        str(media_root),
        "--source-commit",
        SOURCE_COMMIT,
        "--source-worktree-clean",
        "true",
    )

    assert result.returncode != 0
    assert not (media_root / "manifest.json").exists()


@pytest.mark.parametrize(
    ("observation", "passes"),
    [
        pytest.param({"url": "http://chitragupta-ui/dashboard", "kind": "document"}, True, id="exact-ui-origin"),
        pytest.param({"url": "/api/v1/costs", "kind": "fetch"}, True, id="relative-api"),
        pytest.param({"url": "data:image/svg+xml,synthetic", "kind": "image"}, True, id="data-url"),
        pytest.param({"url": "blob:synthetic-recording", "kind": "worker"}, True, id="blob-url"),
        pytest.param({"url": "https://external.invalid/redirect", "kind": "redirect"}, False, id="external-redirect"),
        pytest.param({"url": "https://external.invalid/data", "kind": "fetch"}, False, id="external-fetch"),
        pytest.param({"url": "https://external.invalid/worker.js", "kind": "worker"}, False, id="external-worker"),
        pytest.param({"url": "https://external.invalid/script.js", "kind": "script"}, False, id="external-script"),
        pytest.param(
            {"url": "https://external.invalid/pixel", "kind": "image", "failed": True},
            False,
            id="failed-external",
        ),
    ],
)
def test_media_tool_manifest_enforces_browser_same_origin_evidence(
    tmp_path: Path,
    observation: dict[str, object],
    passes: bool,
) -> None:
    spec_path, media_root = _write_media_workspace(tmp_path)
    observations_path = media_root / "work/browser-observations.json"
    observations = json.loads(observations_path.read_text(encoding="utf-8"))
    observations["requests"] = [observation]
    _write_json(observations_path, observations)

    result = _run_tool(
        "manifest",
        "--spec",
        str(spec_path),
        "--media-root",
        str(media_root),
        "--source-commit",
        SOURCE_COMMIT,
        "--source-worktree-clean",
        "true",
    )

    assert (result.returncode == 0) is passes, result.stderr


def test_media_compose_wires_current_images_read_only_inputs_and_isolated_networks() -> None:
    assert MEDIA_COMPOSE.is_file(), "media Compose production wiring is not implemented"
    compose = yaml.safe_load(MEDIA_COMPOSE.read_text(encoding="utf-8"))
    services = compose["services"]
    tool = services["media-tool"]
    capture = services["media-capture"]
    encoder = services["media-encoder"]
    assert tool["entrypoint"] == ["python", "/opt/chitragupta-demo-media/tool.py"]
    assert "/app/config/config.yaml" in "\n".join(tool["volumes"])
    assert "/opt/chitragupta-demo-media/capture-spec.json" in "\n".join(tool["volumes"])
    assert "/opt/chitragupta-demo-media/tool.py" in "\n".join(tool["volumes"])
    assert "${DEMO_MEDIA_DIR}/state:/app/data:rw" in tool["volumes"]
    assert tool["network_mode"] == "none"
    assert capture["build"] == {"context": "./media", "dockerfile": "Dockerfile"}
    assert "/opt/chitragupta-demo-media/capture-spec.json" in "\n".join(capture["volumes"])
    assert capture["networks"] == ["demo"]
    assert encoder["entrypoint"] == ["/bin/sh", "/opt/chitragupta-demo-media/encode.sh"]
    assert "/opt/chitragupta-demo-media/encode.sh" in "\n".join(encoder["volumes"])
    assert "/opt/chitragupta-demo-media/capture-spec.json" in "\n".join(encoder["volumes"])
    assert encoder["network_mode"] == "none"
    for service in (tool, capture, encoder):
        assert "${DEMO_MEDIA_DIR}:/app/media:rw" in service["volumes"]


def test_media_capture_image_and_encoder_reference_immutable_runtimes_and_owned_entrypoints() -> None:
    dockerfile = MEDIA_DIR / "Dockerfile"
    encoder_compose = MEDIA_COMPOSE
    assert dockerfile.is_file(), "media capture image is not implemented"
    assert encoder_compose.is_file(), "media encoder image is not implemented"
    dockerfile_text = dockerfile.read_text(encoding="utf-8")
    compose_text = encoder_compose.read_text(encoding="utf-8")
    assert "@sha256:" in dockerfile_text
    assert "COPY capture.mjs /opt/chitragupta-demo-media/capture.mjs" in dockerfile_text
    assert 'ENTRYPOINT ["node", "/opt/chitragupta-demo-media/capture.mjs"]' in dockerfile_text
    assert "@sha256:" in compose_text
    assert "/bin/sh" in compose_text


def test_media_capture_package_lock_matches_the_declared_browser_dependency() -> None:
    package = json.loads((MEDIA_DIR / "package.json").read_text(encoding="utf-8"))
    lock = json.loads((MEDIA_DIR / "package-lock.json").read_text(encoding="utf-8"))

    assert lock["lockfileVersion"] == 3
    assert lock["packages"][""]["dependencies"] == package["dependencies"]
    assert lock["packages"]["node_modules/playwright-core"]["version"] == package["dependencies"]["playwright-core"]


def test_media_compose_runtime_contract_is_pinned_and_explicit() -> None:
    compose = yaml.safe_load(MEDIA_COMPOSE.read_text(encoding="utf-8"))
    services = compose["services"]
    assert services["media-tool"]["image"] == "ghcr.io/waliaabhishek/chitragupta:${DEMO_IMAGE_TAG:-local}"
    assert services["media-capture"]["user"] == "${DEMO_UID}:${DEMO_GID}"
    assert services["media-encoder"]["user"] == "${DEMO_UID}:${DEMO_GID}"
    assert services["media-tool"]["user"] == "${DEMO_UID}:${DEMO_GID}"
    assert services["media-capture"]["networks"] == ["demo"]
    assert services["media-tool"]["network_mode"] == "none"
    assert services["media-encoder"]["network_mode"] == "none"
    dockerfile = (MEDIA_DIR / "Dockerfile").read_text(encoding="utf-8")
    assert "mcr.microsoft.com/playwright:v1.61.1-noble@sha256:" in dockerfile
    assert "jrottenberg/ffmpeg:7.1-ubuntu2404@sha256:" in MEDIA_COMPOSE.read_text(encoding="utf-8")
    encoder = (MEDIA_DIR / "encode.sh").read_text(encoding="utf-8")
    assert "ffmpeg" in encoder
    assert "ffprobe" in encoder
    assert "-an" in encoder
    assert "subtitles=$captions" in encoder
