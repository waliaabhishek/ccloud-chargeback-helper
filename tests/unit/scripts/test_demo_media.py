from __future__ import annotations

import hashlib
import json
import os
import shutil
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
FAKE_CAPTURE_RUNNER = Path(__file__).with_name("fixtures") / "demo_media_fake_runner.mjs"
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
FULL_VIDEO_PATH = f"assets/{VIDEO}"
DRAFT_VIDEO_PATH = "review/chitragupta-demo-investigation-draft.mp4"
DRAFT_REVIEW_PATH = ".demo/media/review/chitragupta-demo-investigation-draft.mp4"
RAW_VIDEO_PATH = "work/chitragupta-demo-walkthrough.webm"
CAPTIONS_PATH = "work/captions.srt"
EDIT_TIMELINE_PATH = "work/edit-timeline.json"
ENCODER_ARGUMENTS_PATH = "work/encoder-arguments.list"
RAW_CAPTURE_SPEED = 1.15
FULL_RENDER_SPEED = RAW_CAPTURE_SPEED * 1.25
DRAFT_RENDER_SPEED = RAW_CAPTURE_SPEED
MARKER_COLOR = "#00ff00"
MARKER_MINIMUM_SECONDS = 0.3
MARKER_MAXIMUM_SECONDS = 0.5
FAKE_RECORDING_BYTES = b"synthetic-browser-recording"
FULL_SCENES = (
    (
        "dashboard-summary",
        "/dashboard",
        "Synthetic Showcase, Aug 2–31: $600,362 total; shared cost is larger than usage cost.",
        3,
        3,
    ),
    (
        "dashboard-cost-trend",
        "/dashboard",
        "The 30-day trend gives us a concrete cost change to investigate.",
        3,
        3,
    ),
    (
        "explorer-commerce",
        "/explorer",
        "At the Aug 31 snapshot, focus the Commerce environment.",
        3,
        8,
    ),
    (
        "explorer-customer-kafka",
        "/explorer",
        "In the same snapshot, follow Commerce to Customer Kafka.",
        3,
        7,
    ),
    (
        "topic-topics",
        "/topic-attributions",
        "Back in the 30-day cost range, showcase-live-orders is the largest topic.",
        3,
        6,
    ),
    (
        "topic-filters",
        "/topic-attributions",
        "Scope the 30-day view to Customer Kafka and showcase-live-orders.",
        3,
        7,
    ),
    (
        "topic-composition",
        "/topic-attributions",
        "This topic costs $79,000 across the 30-day range, all from REST produce cost.",
        3,
        2,
    ),
    (
        "topic-movers",
        "/topic-attributions",
        "The top-movers view shows a $49,000 cost increase on Aug 31.",
        4,
        3,
    ),
    (
        "topic-table",
        "/topic-attributions",
        "Narrow to Aug 30–31: the table shows $50,000 on Aug 31 and $1,000 on Aug 30.",
        4,
        8,
    ),
    (
        "pipeline-status",
        "/pipeline",
        "Review the completed pipeline run and daily processing status.",
        3,
        5,
    ),
    (
        "focus-export",
        "/focus-preview",
        "Review the August FOCUS preview and download options.",
        3,
        7,
    ),
)
DRAFT_SCENES = (
    (
        "topic-filters",
        "/topic-attributions",
        "Scope the 30-day synthetic view to Customer Kafka and showcase-live-orders.",
        3,
        6,
    ),
    (
        "topic-composition",
        "/topic-attributions",
        "This topic costs $79,000 across the 30-day range, all from REST produce cost.",
        3,
        1.9,
    ),
    (
        "topic-movers",
        "/topic-attributions",
        "On Aug 31, cost rises by $49,000 from the previous day.",
        3,
        2.9,
    ),
)
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
        "schema_version": 2,
        "anchor_date": "2026-08-31",
        "profile": "showcase",
        "viewport": {"width": 1600, "height": 900},
        "poster": {"width": 960, "height": 540, "name": POSTER},
        "video": {
            "name": VIDEO,
            "content_height": 800,
            "caption_band_height": 100,
            "playback_width": 960,
            "playback_height": 540,
            "content_zoom_percent": 150,
            "caption_font_size": 32,
        },
        "primary_tenant": PRIMARY_TENANT,
        "screenshots": [
            {"name": PNG_ASSETS[0], "route": "/dashboard"},
            {"name": PNG_ASSETS[1], "route": "/explorer"},
            {"name": PNG_ASSETS[2], "route": "/topic-attributions"},
            {"name": PNG_ASSETS[3], "route": "/pipeline"},
            {"name": PNG_ASSETS[4], "route": "/focus-preview"},
        ],
        "storyboards": {
            "full": {
                "output_path": FULL_VIDEO_PATH,
                "maximum_seconds": 90,
                "scenes": _storyboard_scenes(FULL_SCENES),
            },
            "draft": {
                "output_path": DRAFT_VIDEO_PATH,
                "minimum_seconds": 15,
                "maximum_seconds": 20,
                "scenes": _storyboard_scenes(DRAFT_SCENES),
            },
        },
    }


def _storyboard_scenes(
    rows: tuple[tuple[str, str, str, int, int | float], ...],
) -> list[dict[str, str | int | float]]:
    return [
        {"id": scene_id, "route": route, "caption": caption, "read_seconds": read, "max_action_seconds": action}
        for scene_id, route, caption, read, action in rows
    ]


def _write_json(path: Path, contents: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(contents, indent=2), encoding="utf-8")


def _write_executable(path: Path, contents: str) -> None:
    path.write_text(contents, encoding="utf-8")
    path.chmod(0o755)


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
    return _timeline_srt("full", _timeline("full"))


def _write_media_workspace(tmp_path: Path) -> tuple[Path, Path]:
    spec_path = tmp_path / "capture-spec.json"
    _write_json(spec_path, _capture_spec())
    media_root = tmp_path / "media"
    assets = media_root / "assets"
    assets.mkdir(parents=True)
    timeline = _timeline("full")
    full_duration = _timeline_duration(timeline) / _render_speed("full")
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
        duration_seconds=full_duration,
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
    timeline_path = media_root / EDIT_TIMELINE_PATH
    _write_json(timeline_path, timeline)
    timeline_sha256 = hashlib.sha256(timeline_path.read_bytes()).hexdigest()
    (media_root / CAPTIONS_PATH).write_text(_timeline_srt("full", timeline), encoding="utf-8")
    _write_json(
        media_root / "work/encoder-result.json",
        {
            "duration_seconds": full_duration,
            "video_codec": "h264",
            "width": 1600,
            "height": 900,
            "frame_rate": 30,
            "audio_stream_count": 0,
            "captions_burned_in": True,
            "caption_filter": "subtitles",
            "caption_source": "captions.srt",
            "webm_removed": True,
            "story_first_frame": 24,
            "story_last_frame": 2273,
            "raw_frame_rate": 30,
            "mode": "full",
            "speed": _render_speed("full"),
            "edited_duration_seconds": full_duration,
            "timeline_sha256": timeline_sha256,
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


def _run_tool(*arguments: str, extra_environment: dict[str, str] | None = None) -> subprocess.CompletedProcess[str]:
    assert MEDIA_TOOL.is_file(), "media validator is not implemented"
    environment = {**os.environ, "PYTHONPATH": str(PROJECT_ROOT / "src"), **(extra_environment or {})}
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


def _run_capture_entrypoint(
    spec_path: Path,
    catalog_path: Path,
    output_path: Path,
    mode: str | None = "full",
) -> subprocess.CompletedProcess[str]:
    node = shutil.which("node")
    assert node is not None, "Node.js is required to execute the media capture entrypoint"
    return subprocess.run(
        [
            node,
            str(MEDIA_DIR / "capture.mjs"),
            "--spec",
            str(spec_path),
            "--catalog",
            str(catalog_path),
            "--output",
            str(output_path),
            *(() if mode is None else ("--mode", mode)),
        ],
        cwd=PROJECT_ROOT,
        capture_output=True,
        text=True,
        check=False,
    )


def _run_capture_with_fake_browser(tmp_path: Path, mode: str, grid_order: str = "newest") -> dict[str, Any]:
    result, result_path = _invoke_capture_with_fake_browser(tmp_path, mode, grid_order=grid_order)
    assert result.returncode == 0, result.stderr
    return json.loads(result_path.read_text(encoding="utf-8"))


def _invoke_capture_with_fake_browser(
    tmp_path: Path,
    mode: str,
    failure: str = "",
    grid_order: str = "newest",
) -> tuple[subprocess.CompletedProcess[str], Path]:
    node = shutil.which("node")
    assert node is not None, "Node.js is required to execute the capture runner"
    assert FAKE_CAPTURE_RUNNER.is_file(), "fake Chromium runner fixture is missing"
    spec_path = tmp_path / "capture-spec.json"
    catalog_path = tmp_path / "synthetic-catalog.json"
    media_root = tmp_path / "media"
    result_path = tmp_path / "capture-result.json"
    _write_json(spec_path, _capture_spec())
    _write_json(
        catalog_path,
        {
            "source_identifiers": ["env-commerce", "lkc-customer", "showcase-live-orders"],
            "scenarios": [
                {
                    "scenario": {
                        "environment_id": "env-commerce",
                        "cluster_id": "lkc-customer",
                        "topic_name": "showcase-live-orders",
                    }
                }
            ],
        },
    )
    result = subprocess.run(
        [
            node,
            str(FAKE_CAPTURE_RUNNER),
            str(MEDIA_DIR / "capture.mjs"),
            str(spec_path),
            mode,
            str(media_root),
            str(catalog_path),
            str(result_path),
            failure,
            grid_order,
        ],
        cwd=PROJECT_ROOT,
        capture_output=True,
        text=True,
        check=False,
    )
    return result, result_path


def _timeline(mode: str, *, action_ratio: float = 1.0) -> dict[str, object]:
    scenes = FULL_SCENES if mode == "full" else DRAFT_SCENES
    start_seconds = 0.0
    timeline_scenes: list[dict[str, object]] = []
    scrolling_scenes = {
        "dashboard-cost-trend",
        "topic-composition",
        "topic-movers",
        "topic-table",
        "pipeline-status",
        "focus-export",
    }
    for scene_id, _route, _caption, read_seconds, max_action_seconds in scenes:
        action_complete_seconds = start_seconds + float(max_action_seconds) * action_ratio * RAW_CAPTURE_SPEED
        end_seconds = action_complete_seconds + read_seconds * RAW_CAPTURE_SPEED
        scroll_samples: list[dict[str, float]] = []
        if scene_id in scrolling_scenes:
            scroll_seconds = 0.9 * RAW_CAPTURE_SPEED
            scroll_samples = [
                {"elapsed_seconds": 0.0, "offset": 0.0},
                {"elapsed_seconds": scroll_seconds / 2, "offset": 90.0},
                {"elapsed_seconds": scroll_seconds, "offset": 180.0},
            ]
        timeline_scenes.append(
            {
                "id": scene_id,
                "start_seconds": start_seconds,
                "action_complete_seconds": action_complete_seconds,
                "end_seconds": end_seconds,
                "scroll_samples": scroll_samples,
            }
        )
        start_seconds = end_seconds
    timeline: dict[str, object] = {
        "mode": mode,
        "speed": RAW_CAPTURE_SPEED,
        "markers": {
            "color": MARKER_COLOR,
            "plane_average_tolerance": 8,
            "within_plane_spread": 12,
            "runs": [
                {"duration_seconds": 0.4, "frames": 12},
                {"duration_seconds": 0.4, "frames": 12},
            ],
        },
        "scenes": timeline_scenes,
    }
    timeline["framing"] = {
        "zoom_percent": 150,
        "minimum_playback_text_pixels": 11,
        "minimum_playback_target_pixels": 24,
    }
    if mode == "full":
        timeline["evidence"] = {
            "topic_tooltip": "$79,000.00",
            "movers_tooltip": "2026-08-31 +$49,000.00 increase",
            "table_rows": [["2026-08-30", "$1,000.00"], ["2026-08-31", "$50,000.00"]],
        }
    return timeline


def _srt_timestamp(seconds: float) -> str:
    milliseconds = round(seconds * 1000)
    hours, remainder = divmod(milliseconds, 3_600_000)
    minutes, remainder = divmod(remainder, 60_000)
    seconds_part, milliseconds = divmod(remainder, 1000)
    return f"{hours:02d}:{minutes:02d}:{seconds_part:02d},{milliseconds:03d}"


def _timeline_srt(mode: str, timeline: dict[str, object]) -> str:
    rows = FULL_SCENES if mode == "full" else DRAFT_SCENES
    timeline_scenes = timeline["scenes"]
    assert isinstance(timeline_scenes, list)
    blocks: list[str] = []
    for number, (scene, (_, _route, caption, _read, _action)) in enumerate(zip(timeline_scenes, rows, strict=True), 1):
        assert isinstance(scene, dict)
        start_seconds = scene["start_seconds"]
        end_seconds = scene["end_seconds"]
        assert isinstance(start_seconds, float)
        assert isinstance(end_seconds, float)
        blocks.append(
            f"{number}\n{_srt_timestamp(start_seconds / _render_speed(mode))} --> "
            f"{_srt_timestamp(end_seconds / _render_speed(mode))}\n{caption}"
        )
    return "\n\n".join(blocks) + "\n"


def _encoder_vector(mode: str) -> list[str]:
    output_path = FULL_VIDEO_PATH if mode == "full" else DRAFT_VIDEO_PATH
    minimum_seconds = "0" if mode == "full" else "15"
    return [
        RAW_VIDEO_PATH,
        CAPTIONS_PATH,
        f"assets/{PNG_ASSETS[0]}",
        output_path,
        f"assets/{POSTER}",
        MARKER_COLOR,
        "8",
        "12",
        f"{_render_speed(mode):g}",
        "1600",
        "800",
        "100",
        "960",
        "540",
        "32",
        minimum_seconds,
        "90" if mode == "full" else "20",
    ]


def _render_speed(mode: str) -> float:
    return FULL_RENDER_SPEED if mode == "full" else DRAFT_RENDER_SPEED


def _run_edit(
    spec_path: Path,
    mode: str,
    timeline_path: Path,
    srt_path: Path,
    encoder_arguments_path: Path,
) -> subprocess.CompletedProcess[str]:
    return _run_tool(
        "edit",
        "--spec",
        str(spec_path),
        "--mode",
        mode,
        "--timeline",
        str(timeline_path),
        "--srt-output",
        str(srt_path),
        "--encoder-arguments-output",
        str(encoder_arguments_path),
    )


def _run_validate_encode(
    spec_path: Path,
    mode: str,
    timeline_path: Path,
    encoder_result_path: Path,
    video_path: Path,
) -> subprocess.CompletedProcess[str]:
    return _run_tool(
        "validate-encode",
        "--spec",
        str(spec_path),
        "--mode",
        mode,
        "--timeline",
        str(timeline_path),
        "--encoder-result",
        str(encoder_result_path),
        "--video",
        str(video_path),
    )


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
        pytest.param(lambda spec: spec["viewport"].update({"width": 1280}), id="viewport-width"),
        pytest.param(lambda spec: spec["viewport"].update({"height": 720}), id="viewport-height"),
        pytest.param(lambda spec: spec["poster"].update({"width": 1280}), id="poster-width"),
        pytest.param(lambda spec: spec["poster"].update({"height": 720}), id="poster-height"),
        pytest.param(lambda spec: spec["poster"].update({"name": "poster.webp"}), id="poster-name"),
        pytest.param(lambda spec: spec["video"].update({"name": "walkthrough.mp4"}), id="video-name"),
        pytest.param(lambda spec: spec["video"].update({"content_height": 801}), id="content-height"),
        pytest.param(lambda spec: spec["video"].update({"caption_band_height": 99}), id="caption-band-height"),
        pytest.param(lambda spec: spec["video"].update({"playback_width": 1280}), id="playback-width"),
        pytest.param(lambda spec: spec["video"].update({"playback_height": 720}), id="playback-height"),
        pytest.param(lambda spec: spec["video"].update({"content_zoom_percent": 125}), id="content-zoom"),
        pytest.param(lambda spec: spec["video"].update({"caption_font_size": 24}), id="caption-font-size"),
        pytest.param(lambda spec: spec["primary_tenant"].update({"name": "other"}), id="tenant-name"),
        pytest.param(lambda spec: spec["primary_tenant"].update({"id": "other"}), id="tenant-id"),
        pytest.param(
            lambda spec: spec["primary_tenant"].update({"ecosystem": "self_managed_kafka"}),
            id="tenant-ecosystem",
        ),
        pytest.param(lambda spec: spec["screenshots"].pop(), id="missing-approved-screen"),
        pytest.param(lambda spec: spec["screenshots"][0].update({"name": "other.png"}), id="screenshot-name"),
        pytest.param(lambda spec: spec["screenshots"][0].update({"route": "/other"}), id="screenshot-route"),
        pytest.param(lambda spec: spec["storyboards"].pop("full"), id="missing-full-storyboard"),
        pytest.param(lambda spec: spec["storyboards"].pop("draft"), id="missing-draft-storyboard"),
        pytest.param(
            lambda spec: spec["storyboards"]["full"]["scenes"].reverse(),
            id="full-scene-order",
        ),
        pytest.param(
            lambda spec: spec["storyboards"]["draft"]["scenes"][1].update({"route": "/dashboard"}),
            id="draft-scene-route",
        ),
        pytest.param(
            lambda spec: spec["storyboards"]["full"]["scenes"][0].update({"caption": "shifted"}),
            id="full-scene-caption",
        ),
        pytest.param(
            lambda spec: spec["storyboards"]["draft"]["scenes"][2].update({"read_seconds": 2}),
            id="draft-read-seconds",
        ),
        pytest.param(
            lambda spec: spec["storyboards"]["full"].update({"maximum_seconds": 89}),
            id="full-duration-cap",
        ),
        pytest.param(
            lambda spec: spec["storyboards"]["draft"].update({"minimum_seconds": 16}),
            id="draft-duration-floor",
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
    _write_json(spec_path, _capture_spec())

    result = _run_tool(
        "catalog",
        "--spec",
        str(spec_path),
        "--config",
        str(PROJECT_ROOT / "examples/demo/config.yaml"),
        "--output",
        str(catalog_path),
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
    assert not (tmp_path / "captions.srt").exists()
    assert not (tmp_path / "encoder-arguments.list").exists()


@pytest.mark.parametrize("mode", ("full", "draft"))
def test_media_tool_edit_converts_marker_relative_timeline_to_zero_based_speed_adjusted_srt_and_vector(
    tmp_path: Path,
    mode: str,
) -> None:
    spec_path = tmp_path / "capture-spec.json"
    timeline_path = tmp_path / EDIT_TIMELINE_PATH
    srt_path = tmp_path / CAPTIONS_PATH
    encoder_arguments_path = tmp_path / ENCODER_ARGUMENTS_PATH
    specification = _capture_spec()
    timeline = _timeline(mode)
    _write_json(spec_path, specification)
    _write_json(timeline_path, timeline)

    result = _run_edit(spec_path, mode, timeline_path, srt_path, encoder_arguments_path)

    assert result.returncode == 0, result.stderr
    assert srt_path.read_text(encoding="utf-8") == _timeline_srt(mode, timeline)
    assert encoder_arguments_path.read_text(encoding="utf-8").splitlines() == _encoder_vector(mode)


def test_media_tool_edit_allows_a_short_content_derived_full_cut_without_a_duration_floor(tmp_path: Path) -> None:
    spec_path = tmp_path / "capture-spec.json"
    timeline_path = tmp_path / EDIT_TIMELINE_PATH
    srt_path = tmp_path / CAPTIONS_PATH
    encoder_arguments_path = tmp_path / ENCODER_ARGUMENTS_PATH
    timeline = _timeline("full", action_ratio=0.05)
    _write_json(spec_path, _capture_spec())
    _write_json(timeline_path, timeline)

    result = _run_edit(spec_path, "full", timeline_path, srt_path, encoder_arguments_path)

    assert result.returncode == 0, result.stderr
    assert _timeline_duration(timeline) / _render_speed("full") < 60
    assert _timeline_duration(timeline) / _render_speed("full") <= 90


@pytest.mark.parametrize(
    ("mode", "mutate"),
    [
        pytest.param("unknown", lambda timeline: timeline, id="unknown-mode"),
        pytest.param("full", lambda timeline: timeline.update({"mode": "draft"}), id="timeline-mode-mismatch"),
        pytest.param("full", lambda timeline: timeline.update({"speed": 1.0}), id="speed-fallback"),
        pytest.param("full", lambda timeline: timeline.pop("framing"), id="missing-framing"),
        pytest.param("draft", lambda timeline: timeline.pop("framing"), id="draft-missing-framing"),
        pytest.param("full", lambda timeline: timeline.pop("evidence"), id="missing-full-evidence"),
        pytest.param(
            "draft",
            lambda timeline: timeline.update(
                {
                    "evidence": {
                        "topic_tooltip": "$79,000.00",
                        "movers_tooltip": "2026-08-31 +$49,000.00 increase",
                        "table_rows": [["2026-08-30", "$1,000.00"], ["2026-08-31", "$50,000.00"]],
                    }
                }
            ),
            id="draft-full-evidence",
        ),
        pytest.param(
            "full",
            lambda timeline: timeline["evidence"].update(
                {"table_rows": [["2026-08-30", "$1,000.00"], ["2026-08-31", "$50,000.00"], ["extra", "$0.00"]]}
            ),
            id="extra-table-evidence-row",
        ),
        pytest.param(
            "full",
            lambda timeline: timeline["evidence"].update(
                {"table_rows": [["2026-08-30"], ["2026-08-31", "$50,000.00"]]}
            ),
            id="malformed-table-evidence-row",
        ),
        pytest.param("full", lambda timeline: timeline["markers"].update({"color": "#ff00ff"}), id="marker-color"),
        pytest.param(
            "full",
            lambda timeline: timeline["markers"]["runs"].pop(),
            id="missing-marker-run",
        ),
        pytest.param(
            "full",
            lambda timeline: timeline["markers"]["runs"][0].update({"duration_seconds": 0.2}),
            id="short-marker-run",
        ),
        pytest.param(
            "full",
            lambda timeline: timeline["scenes"].reverse(),
            id="scene-order",
        ),
        pytest.param(
            "full",
            lambda timeline: timeline["scenes"][0].update({"id": "other"}),
            id="scene-id",
        ),
        pytest.param(
            "full",
            lambda timeline: timeline["scenes"][1].update({"start_seconds": 0.1}),
            id="non-contiguous-scenes",
        ),
        pytest.param(
            "full",
            lambda timeline: timeline["scenes"][0].update({"action_complete_seconds": 10.0}),
            id="action-overrun",
        ),
        pytest.param(
            "full",
            lambda timeline: timeline["scenes"][0].update({"end_seconds": 3.6}),
            id="read-hold-too-short",
        ),
        pytest.param(
            "full",
            lambda timeline: timeline["scenes"][6].update({"scroll_samples": []}),
            id="missing-composition-scroll",
        ),
        pytest.param(
            "full",
            lambda timeline: timeline["scenes"][7].update(
                {
                    "scroll_samples": [
                        {"elapsed_seconds": 0.0, "offset": 0.0},
                        {"elapsed_seconds": 1.035, "offset": 100.0},
                    ]
                }
            ),
            id="short-movers-scroll",
        ),
        pytest.param(
            "full",
            lambda timeline: timeline["scenes"].append(deepcopy(timeline["scenes"][0])),
            id="extra-scene",
        ),
        pytest.param(
            "draft",
            lambda timeline: _extend_draft_over_20_seconds(timeline),
            id="draft-over-maximum",
        ),
        pytest.param(
            "draft",
            lambda timeline: _shorten_draft_under_15_seconds(timeline),
            id="draft-under-minimum",
        ),
        pytest.param(
            "full",
            lambda timeline: _extend_full_over_90_seconds(timeline),
            id="full-over-maximum",
        ),
    ],
)
def test_media_tool_edit_rejects_invalid_mode_or_unapproved_timeline_evidence_before_outputs(
    tmp_path: Path,
    mode: str,
    mutate: Callable[[dict[str, Any]], object],
) -> None:
    spec_path = tmp_path / "capture-spec.json"
    timeline_path = tmp_path / EDIT_TIMELINE_PATH
    srt_path = tmp_path / CAPTIONS_PATH
    encoder_arguments_path = tmp_path / ENCODER_ARGUMENTS_PATH
    timeline = _timeline("draft" if mode == "draft" else "full")
    mutate(timeline)
    _write_json(spec_path, _capture_spec())
    _write_json(timeline_path, timeline)

    result = _run_edit(spec_path, mode, timeline_path, srt_path, encoder_arguments_path)

    assert result.returncode != 0
    assert not srt_path.exists()
    assert not encoder_arguments_path.exists()


@pytest.mark.parametrize(
    ("mode", "mutate", "diagnostic"),
    [
        pytest.param(
            "full",
            lambda timeline: timeline.pop("framing"),
            "edit timeline has invalid fields (missing framing)",
            id="missing-full-framing-diagnostic",
        ),
        pytest.param(
            "draft",
            lambda timeline: timeline.pop("framing"),
            "edit timeline has invalid fields (missing framing)",
            id="missing-draft-framing-diagnostic",
        ),
        pytest.param(
            "full",
            lambda timeline: timeline.pop("evidence"),
            "edit timeline has invalid fields (missing evidence)",
            id="missing-full-evidence-diagnostic",
        ),
        pytest.param(
            "draft",
            lambda timeline: timeline.update(
                {
                    "evidence": {
                        "topic_tooltip": "$79,000.00",
                        "movers_tooltip": "2026-08-31 +$49,000.00 increase",
                        "table_rows": [["2026-08-30", "$1,000.00"], ["2026-08-31", "$50,000.00"]],
                    }
                }
            ),
            "edit timeline has invalid fields (unknown evidence)",
            id="draft-full-evidence-diagnostic",
        ),
        pytest.param(
            "full",
            lambda timeline: timeline["evidence"].update(
                {"table_rows": [["2026-08-30", "$1,000.00"], ["2026-08-31", "$50,000.00"], ["extra", "$0.00"]]}
            ),
            "edit timeline table evidence does not support the approved date comparison",
            id="extra-table-evidence-row-diagnostic",
        ),
        pytest.param(
            "full",
            lambda timeline: timeline["evidence"].update(
                {"table_rows": [["2026-08-30"], ["2026-08-31", "$50,000.00"]]}
            ),
            "edit timeline table evidence row 0 must contain exactly two strings",
            id="malformed-table-evidence-row-diagnostic",
        ),
    ],
)
def test_media_tool_edit_reports_the_specific_timeline_contract_violation(
    tmp_path: Path,
    mode: str,
    mutate: Callable[[dict[str, Any]], object],
    diagnostic: str,
) -> None:
    spec_path = tmp_path / "capture-spec.json"
    timeline_path = tmp_path / EDIT_TIMELINE_PATH
    srt_path = tmp_path / CAPTIONS_PATH
    encoder_arguments_path = tmp_path / ENCODER_ARGUMENTS_PATH
    timeline = _timeline(mode)
    mutate(timeline)
    _write_json(spec_path, _capture_spec())
    _write_json(timeline_path, timeline)

    result = _run_edit(spec_path, mode, timeline_path, srt_path, encoder_arguments_path)

    assert result.returncode != 0
    assert diagnostic in result.stderr
    assert not srt_path.exists()
    assert not encoder_arguments_path.exists()


def _timeline_duration(timeline: dict[str, object]) -> float:
    scenes = timeline["scenes"]
    assert isinstance(scenes, list)
    last_scene = scenes[-1]
    assert isinstance(last_scene, dict)
    end_seconds = last_scene["end_seconds"]
    assert isinstance(end_seconds, float)
    return end_seconds


def _extend_draft_over_20_seconds(timeline: dict[str, Any]) -> None:
    for scene in timeline["scenes"]:
        scene["start_seconds"] += 2 * RAW_CAPTURE_SPEED
        scene["action_complete_seconds"] += 2 * RAW_CAPTURE_SPEED
        scene["end_seconds"] += 2 * RAW_CAPTURE_SPEED


def _shorten_draft_under_15_seconds(timeline: dict[str, Any]) -> None:
    for scene in timeline["scenes"]:
        scene["action_complete_seconds"] = scene["start_seconds"]
        scene["end_seconds"] = scene["start_seconds"] + 2 * RAW_CAPTURE_SPEED


def _extend_full_over_90_seconds(timeline: dict[str, Any]) -> None:
    last_scene = timeline["scenes"][-1]
    last_scene["end_seconds"] += 16 * RAW_CAPTURE_SPEED


def _rewrite_workspace_as_draft_capture(media_root: Path) -> None:
    timeline = _timeline("draft")
    timeline_path = media_root / EDIT_TIMELINE_PATH
    _write_json(timeline_path, timeline)
    (media_root / CAPTIONS_PATH).write_text(_timeline_srt("draft", timeline), encoding="utf-8")
    encoder_result_path = media_root / "work/encoder-result.json"
    encoder_result = json.loads(encoder_result_path.read_text(encoding="utf-8"))
    output_duration = _timeline_duration(timeline) / _render_speed("draft")
    encoder_result.update(
        {
            "duration_seconds": output_duration,
            "mode": "draft",
            "speed": _render_speed("draft"),
            "edited_duration_seconds": output_duration,
            "timeline_sha256": hashlib.sha256(timeline_path.read_bytes()).hexdigest(),
        }
    )
    _write_json(encoder_result_path, encoder_result)


@pytest.mark.parametrize("command", ("manifest", "validate"))
def test_media_tool_manifest_requires_full_encoder_and_structurally_validated_timeline(
    tmp_path: Path,
    command: str,
) -> None:
    spec_path, media_root = _write_media_workspace(tmp_path)
    _rewrite_workspace_as_draft_capture(media_root)
    if command == "manifest":
        (media_root / "manifest.json").unlink()
        arguments = (
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
    else:
        arguments = ("validate", "--spec", str(spec_path), "--media-root", str(media_root))

    result = _run_tool(*arguments)

    assert result.returncode != 0
    assert "encoder result must be a full-mode capture" in result.stderr
    if command == "manifest":
        assert not (media_root / "manifest.json").exists()


@pytest.mark.parametrize("command", ("manifest", "validate"))
def test_media_tool_manifest_revalidates_full_timeline_framing_and_evidence(
    tmp_path: Path,
    command: str,
) -> None:
    spec_path, media_root = _write_media_workspace(tmp_path)
    timeline_path = media_root / EDIT_TIMELINE_PATH
    timeline = json.loads(timeline_path.read_text(encoding="utf-8"))
    timeline.pop("evidence")
    _write_json(timeline_path, timeline)
    encoder_result_path = media_root / "work/encoder-result.json"
    encoder_result = json.loads(encoder_result_path.read_text(encoding="utf-8"))
    encoder_result["timeline_sha256"] = hashlib.sha256(timeline_path.read_bytes()).hexdigest()
    _write_json(encoder_result_path, encoder_result)
    if command == "manifest":
        (media_root / "manifest.json").unlink()
        arguments = (
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
    else:
        arguments = ("validate", "--spec", str(spec_path), "--media-root", str(media_root))

    result = _run_tool(*arguments)

    assert result.returncode != 0
    assert "edit timeline has invalid fields (missing evidence)" in result.stderr
    if command == "manifest":
        assert not (media_root / "manifest.json").exists()


def test_media_tool_validate_encode_accepts_matching_full_timeline_hash_mode_and_encoder_probe_duration(
    tmp_path: Path,
) -> None:
    spec_path, media_root = _write_media_workspace(tmp_path)
    result = _run_validate_encode(
        spec_path,
        "full",
        media_root / EDIT_TIMELINE_PATH,
        media_root / "work/encoder-result.json",
        media_root / "assets" / VIDEO,
    )

    assert result.returncode == 0, result.stderr


def test_media_tool_validate_encode_accepts_the_encoder_owned_actual_25fps_raw_rate(tmp_path: Path) -> None:
    spec_path, media_root = _write_media_workspace(tmp_path)
    timeline_path = media_root / EDIT_TIMELINE_PATH
    timeline = json.loads(timeline_path.read_text(encoding="utf-8"))
    timeline["markers"]["runs"] = [
        {"duration_seconds": 0.4, "frames": 10},
        {"duration_seconds": 0.4, "frames": 10},
    ]
    _write_json(timeline_path, timeline)
    encoder_result_path = media_root / "work/encoder-result.json"
    encoder_result = json.loads(encoder_result_path.read_text(encoding="utf-8"))
    encoder_result["raw_frame_rate"] = 25
    encoder_result["timeline_sha256"] = hashlib.sha256(timeline_path.read_bytes()).hexdigest()
    _write_json(encoder_result_path, encoder_result)

    result = _run_validate_encode(
        spec_path,
        "full",
        timeline_path,
        encoder_result_path,
        media_root / "assets" / VIDEO,
    )

    assert result.returncode == 0, result.stderr


def test_media_tool_validate_encode_rejects_an_encoded_video_outside_the_result_media_root(
    tmp_path: Path,
) -> None:
    spec_path, media_root = _write_media_workspace(tmp_path)
    outside_video = tmp_path / "outside.mp4"
    outside_video.write_bytes(b"outside-media-root")

    result = _run_validate_encode(
        spec_path,
        "full",
        media_root / EDIT_TIMELINE_PATH,
        media_root / "work/encoder-result.json",
        outside_video,
    )

    assert result.returncode != 0
    assert "timeline and encoded video paths must be inside the encoder result media root" in result.stderr
    assert "IndexError" not in result.stderr


def test_media_tool_validate_encode_rejects_an_encoder_result_outside_the_media_work_path(
    tmp_path: Path,
) -> None:
    spec_path, media_root = _write_media_workspace(tmp_path)
    shallow_result = tmp_path / "encoder-result.json"
    shallow_result.write_bytes((media_root / "work/encoder-result.json").read_bytes())

    result = _run_validate_encode(
        spec_path,
        "full",
        media_root / EDIT_TIMELINE_PATH,
        shallow_result,
        media_root / "assets" / VIDEO,
    )

    assert result.returncode != 0
    assert "encoder result path must be a work/encoder-result.json file" in result.stderr
    assert "IndexError" not in result.stderr


@pytest.mark.parametrize("raw_frame_rate", [0, -1])
def test_media_tool_validate_encode_rejects_invalid_encoder_owned_raw_rate(
    tmp_path: Path,
    raw_frame_rate: int,
) -> None:
    spec_path, media_root = _write_media_workspace(tmp_path)
    encoder_result_path = media_root / "work/encoder-result.json"
    encoder_result = json.loads(encoder_result_path.read_text(encoding="utf-8"))
    encoder_result["raw_frame_rate"] = raw_frame_rate
    _write_json(encoder_result_path, encoder_result)

    result = _run_validate_encode(
        spec_path,
        "full",
        media_root / EDIT_TIMELINE_PATH,
        encoder_result_path,
        media_root / "assets" / VIDEO,
    )

    assert result.returncode != 0
    assert "raw frame rate" in result.stderr


@pytest.mark.parametrize(
    ("mode", "timeline_mutation", "encoder_mutation"),
    [
        pytest.param("draft", None, None, id="mode-disagrees-with-timeline"),
        pytest.param("full", None, {"mode": "draft"}, id="encoder-mode-mismatch"),
        pytest.param("full", None, {"timeline_sha256": "f" * 64}, id="timeline-hash-mismatch"),
        pytest.param("full", None, {"edited_duration_seconds": 74}, id="encoded-duration-mismatch"),
        pytest.param("full", {"speed": 1.0}, None, id="timeline-speed-mismatch"),
    ],
)
def test_media_tool_validate_encode_rejects_mode_hash_timeline_or_encoded_duration_disagreement(
    tmp_path: Path,
    mode: str,
    timeline_mutation: dict[str, object] | None,
    encoder_mutation: dict[str, object] | None,
) -> None:
    spec_path, media_root = _write_media_workspace(tmp_path)
    timeline_path = media_root / EDIT_TIMELINE_PATH
    encoder_result_path = media_root / "work/encoder-result.json"
    if timeline_mutation is not None:
        timeline = json.loads(timeline_path.read_text(encoding="utf-8"))
        timeline.update(timeline_mutation)
        _write_json(timeline_path, timeline)
    if encoder_mutation is not None:
        encoder_result = json.loads(encoder_result_path.read_text(encoding="utf-8"))
        encoder_result.update(encoder_mutation)
        _write_json(encoder_result_path, encoder_result)

    result = _run_validate_encode(
        spec_path,
        mode,
        timeline_path,
        encoder_result_path,
        media_root / "assets" / VIDEO,
    )

    assert result.returncode != 0


def test_media_tool_validate_accepts_a_complete_clean_manifest_and_exact_asset_set(tmp_path: Path) -> None:
    spec_path, media_root = _write_media_workspace(tmp_path)

    result = _validate_workspace(spec_path, media_root)

    assert result.returncode == 0, result.stderr


def test_media_tool_manifest_constructs_the_exact_approved_asset_order_from_the_validated_specification(
    tmp_path: Path,
) -> None:
    spec_path, media_root = _write_media_workspace(tmp_path)
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

    assert result.returncode == 0, result.stderr
    manifest = json.loads((media_root / "manifest.json").read_text(encoding="utf-8"))
    assert manifest["schema_version"] == 1
    assert manifest["source_commit"] == SOURCE_COMMIT
    assert manifest["source_worktree_clean"] is True
    assert manifest["anchor_date"] == "2026-08-31"
    assert manifest["profile"] == "showcase"
    assert manifest["primary_tenant"] == PRIMARY_TENANT
    assert manifest["expected_assets"] == list(EXPECTED_ASSETS)
    assert list(manifest["assets"]) == list(EXPECTED_ASSETS)


def test_media_tool_validate_writes_only_the_exact_release_argument_vector_after_complete_revalidation(
    tmp_path: Path,
) -> None:
    spec_path, media_root = _write_media_workspace(tmp_path)
    publication_arguments_path = media_root / "work/publication-arguments.list"

    result = _run_tool(
        "validate",
        "--spec",
        str(spec_path),
        "--media-root",
        str(media_root),
        "--publication-arguments-output",
        str(publication_arguments_path),
    )

    assert result.returncode == 0, result.stderr
    assert publication_arguments_path.read_text(encoding="utf-8").splitlines() == [
        "assets/chitragupta-demo-dashboard.png",
        "assets/chitragupta-demo-cost-explorer.png",
        "assets/chitragupta-demo-topic-attribution.png",
        "assets/chitragupta-demo-pipeline-status.png",
        "assets/chitragupta-demo-focus-mapping-preview.png",
        "assets/chitragupta-demo-walkthrough.mp4",
        "manifest.json",
    ]


def test_media_tool_does_not_create_a_publication_vector_when_revalidation_rejects_a_decoy_asset(
    tmp_path: Path,
) -> None:
    spec_path, media_root = _write_media_workspace(tmp_path)
    publication_arguments_path = media_root / "work/publication-arguments.list"
    (media_root / "assets/unexpected.png").write_bytes(b"decoy")

    result = _run_tool(
        "validate",
        "--spec",
        str(spec_path),
        "--media-root",
        str(media_root),
        "--publication-arguments-output",
        str(publication_arguments_path),
    )

    assert result.returncode != 0
    assert not publication_arguments_path.exists()


@pytest.mark.parametrize(
    ("mutate", "expected_error"),
    [
        pytest.param(
            lambda spec: spec.update({"schema_version": 1}),
            "capture specification is not the approved Showcase input",
            id="schema-version",
        ),
        pytest.param(
            lambda spec: spec.update({"anchor_date": "2026-09-01"}),
            "capture specification is not the approved Showcase input",
            id="anchor",
        ),
        pytest.param(
            lambda spec: spec.update({"profile": "clean"}),
            "capture specification is not the approved Showcase input",
            id="profile",
        ),
        pytest.param(
            lambda spec: spec["viewport"].update({"width": 1280}),
            "capture specification is not the approved Showcase input",
            id="viewport",
        ),
        pytest.param(
            lambda spec: spec["primary_tenant"].update({"name": "other"}),
            "capture specification is not the approved Showcase input",
            id="primary-tenant",
        ),
        pytest.param(
            lambda spec: spec["screenshots"].pop(),
            "capture specification does not contain the approved storyboard",
            id="screenshot-count",
        ),
        pytest.param(
            lambda spec: spec["screenshots"][0].update({"name": "other.png"}),
            "capture specification does not contain the approved storyboard",
            id="screenshot-name",
        ),
        pytest.param(
            lambda spec: spec["screenshots"][0].update({"route": "/other"}),
            "capture specification does not contain the approved storyboard",
            id="screenshot-route",
        ),
        pytest.param(
            lambda spec: spec["storyboards"].pop("draft"),
            "capture specification does not contain the approved storyboard",
            id="draft-storyboard",
        ),
        pytest.param(
            lambda spec: spec["storyboards"]["full"]["scenes"].reverse(),
            "capture specification does not contain the approved storyboard",
            id="full-scene-order",
        ),
        pytest.param(
            lambda spec: spec["storyboards"]["draft"]["scenes"][0].update({"caption": "shifted"}),
            "capture specification does not contain the approved storyboard",
            id="draft-caption",
        ),
        pytest.param(
            lambda spec: spec["video"].update({"content_height": 799}),
            "capture specification is not the approved Showcase input",
            id="content-height",
        ),
        pytest.param(
            lambda spec: spec["storyboards"]["full"].update({"maximum_seconds": 91}),
            "capture specification does not contain the approved storyboard",
            id="full-maximum",
        ),
        pytest.param(
            lambda spec: spec["storyboards"]["draft"].update({"minimum_seconds": 14}),
            "capture specification does not contain the approved storyboard",
            id="draft-minimum",
        ),
        pytest.param(
            lambda spec: spec["storyboards"]["draft"].update({"maximum_seconds": 21}),
            "capture specification does not contain the approved storyboard",
            id="draft-maximum",
        ),
    ],
)
def test_capture_entrypoint_rejects_tampered_approved_inputs_before_playwright_or_output_creation(
    tmp_path: Path,
    mutate: Callable[[dict[str, Any]], object],
    expected_error: str,
) -> None:
    spec_path = tmp_path / "capture-spec.json"
    catalog_path = tmp_path / "synthetic-catalog.json"
    output_path = tmp_path / "media-output"
    specification = deepcopy(_capture_spec())
    mutate(specification)
    _write_json(spec_path, specification)
    _write_json(catalog_path, {})

    result = _run_capture_entrypoint(spec_path, catalog_path, output_path)

    assert result.returncode != 0
    assert expected_error in result.stderr
    assert not output_path.exists()


@pytest.mark.parametrize("mode", (None, "", "preview", "FULL"))
def test_capture_entrypoint_rejects_an_unknown_or_missing_mode_before_playwright_or_output_creation(
    tmp_path: Path,
    mode: str | None,
) -> None:
    spec_path = tmp_path / "capture-spec.json"
    catalog_path = tmp_path / "synthetic-catalog.json"
    output_path = tmp_path / "media-output"
    _write_json(spec_path, _capture_spec())
    _write_json(catalog_path, {"scenarios": []})

    result = _run_capture_entrypoint(spec_path, catalog_path, output_path, mode)

    assert result.returncode != 0
    assert "mode" in result.stderr.lower()
    assert not output_path.exists()


@pytest.mark.parametrize("mode", ("full", "draft"))
def test_imported_capture_runner_executes_the_approved_storyboard_through_the_browser_boundary(
    tmp_path: Path,
    mode: str,
) -> None:
    capture = _run_capture_with_fake_browser(tmp_path, mode)
    expected_scenes = FULL_SCENES if mode == "full" else DRAFT_SCENES
    timeline = capture["timeline"]
    log = capture["log"]

    assert timeline["mode"] == mode
    assert timeline["speed"] == RAW_CAPTURE_SPEED
    assert [scene["id"] for scene in timeline["scenes"]] == [scene[0] for scene in expected_scenes]
    assert all(
        set(scene) == {"id", "start_seconds", "action_complete_seconds", "end_seconds", "scroll_samples"}
        for scene in timeline["scenes"]
    )
    assert all(scene["end_seconds"] >= scene["action_complete_seconds"] for scene in timeline["scenes"])
    assert all(
        timeline_scene["end_seconds"] - timeline_scene["action_complete_seconds"]
        == pytest.approx(expected_scene[3] * RAW_CAPTURE_SPEED, abs=2 / 30)
        for timeline_scene, expected_scene in zip(timeline["scenes"], expected_scenes, strict=True)
    )
    gotos = [entry["url"] for entry in log if entry["event"] == "goto"]
    assert "http://chitragupta-ui/dashboard?start_date=2026-08-02&end_date=2026-08-31&timezone=UTC" in gotos
    assert "http://chitragupta-ui/explorer?at=2026-08-31" in gotos or mode == "draft"
    assert "http://chitragupta-ui/topic-attributions?start_date=2026-08-02&end_date=2026-08-31&timezone=UTC" in gotos
    if mode == "full":
        assert [scene[0] for scene in expected_scenes[-2:]] == ["pipeline-status", "focus-export"]
        assert "http://chitragupta-ui/pipeline" in gotos
        assert "http://chitragupta-ui/focus-preview" in gotos
    else:
        assert all(scene[1] not in {"/pipeline", "/focus-preview"} for scene in expected_scenes)
    assert [entry["event"] for entry in log].count("addInitScript") == 1
    video_gotos = [entry for entry in log if entry["event"] == "goto" and entry["initScripts"]]
    assert video_gotos and all(entry["initScripts"] == 1 for entry in video_gotos)
    assert capture["documentInitializations"] == len(video_gotos)


@pytest.mark.parametrize("mode", ("full", "draft"))
def test_imported_capture_runner_hands_off_the_browser_recording_to_the_validated_work_path(
    tmp_path: Path,
    mode: str,
) -> None:
    capture = _run_capture_with_fake_browser(tmp_path, mode)
    result = capture["result"]
    assert isinstance(result, dict)
    raw_video_path = Path(result["rawVideoPath"])
    assert raw_video_path == tmp_path / "media" / RAW_VIDEO_PATH
    assert raw_video_path.read_bytes() == FAKE_RECORDING_BYTES
    assert not (tmp_path / "media" / "work" / "fake-browser-recording.webm").exists()
    recording_events = [entry for entry in capture["log"] if entry["event"] == "recordingPath"]
    assert len(recording_events) == 1
    assert recording_events[0]["bytes"] == len(FAKE_RECORDING_BYTES)


@pytest.mark.parametrize("mode", ("full", "draft"))
def test_imported_capture_runner_records_actual_action_caps_read_holds_and_output_duration(
    tmp_path: Path,
    mode: str,
) -> None:
    capture = _run_capture_with_fake_browser(tmp_path, mode)
    timeline = capture["timeline"]
    expected_scenes = FULL_SCENES if mode == "full" else DRAFT_SCENES
    actual_scenes = timeline["scenes"]
    assert isinstance(actual_scenes, list)
    assert len(actual_scenes) == len(expected_scenes)

    previous_end = 0.0
    for actual, expected in zip(actual_scenes, expected_scenes, strict=True):
        assert isinstance(actual, dict)
        start = actual["start_seconds"]
        action_complete = actual["action_complete_seconds"]
        end = actual["end_seconds"]
        assert isinstance(start, (int, float))
        assert isinstance(action_complete, (int, float))
        assert isinstance(end, (int, float))
        assert start == pytest.approx(previous_end, abs=2 / 30)
        action_seconds = (float(action_complete) - float(start)) / RAW_CAPTURE_SPEED
        read_seconds = (float(end) - float(action_complete)) / RAW_CAPTURE_SPEED
        assert action_seconds > 0
        assert action_seconds <= expected[4] + 2 / 30
        assert read_seconds == pytest.approx(expected[3], abs=2 / 30)
        previous_end = float(end)

    edited_duration = previous_end / _render_speed(mode)
    if mode == "draft":
        assert 15 <= edited_duration <= 20
    else:
        assert edited_duration <= 90


def test_capture_storyboard_settles_before_marker_and_keeps_timeline_on_node_clock(tmp_path: Path) -> None:
    capture = _run_capture_with_fake_browser(tmp_path, "draft")
    timeline = capture["timeline"]
    log = capture["log"]

    marker_evaluations = [
        (index, entry)
        for index, entry in enumerate(log)
        if entry["event"] == "pageEvaluate" and "demo-capture-sync-marker" in entry["source"]
    ]
    assert len(marker_evaluations) >= 4
    first_marker_index, first_marker = marker_evaluations[0]
    video_goto_indices = [index for index, entry in enumerate(log) if entry["event"] == "goto" and entry["initScripts"]]
    assert video_goto_indices and video_goto_indices[0] < first_marker_index
    assert "2147483647" in first_marker["source"]
    frame_evaluations = [
        entry for entry in log if entry["event"] == "pageEvaluate" and "requestAnimationFrame" in entry["source"]
    ]
    assert frame_evaluations
    assert all("resolve(performance.now())" not in entry["source"] for entry in frame_evaluations)
    assert timeline["scenes"][0]["start_seconds"] == pytest.approx(0.0, abs=2 / 30)


@pytest.mark.parametrize("grid_order", ("newest", "oldest"))
def test_imported_capture_runner_makes_exact_visible_gestures_scrolls_and_evidence_claims(
    tmp_path: Path,
    grid_order: str,
) -> None:
    capture = _run_capture_with_fake_browser(tmp_path, "full", grid_order)
    timeline = capture["timeline"]
    log = capture["log"]
    timed_waits = [entry["milliseconds"] for entry in log if entry["event"] == "waitForTimeout"]
    typed = [entry for entry in log if entry["event"] == "pressSequentially"]
    filled = [entry["value"] for entry in log if entry["event"] == "fill"]
    hovers = [entry for entry in log if entry["event"] == "hover"]
    clicks = [entry for entry in log if entry["event"] == "click"]
    cluster_typed = [entry["value"] for entry in typed if "Any cluster" in entry["name"]]
    topic_typed = [entry["value"] for entry in typed if "Any topic" in entry["name"]]
    filter_input_commits = [
        entry
        for entry in log
        if entry["event"] == "waitForFunction"
        and entry["argument"].get("inputSelector")
        in {'input[placeholder="Any cluster"]', 'input[placeholder="Any topic"]'}
    ]

    assert typed and all(entry["options"]["delay"] == pytest.approx(0.04 * RAW_CAPTURE_SPEED * 1000) for entry in typed)
    assert "env-commerce" in set(filled) | {entry["value"] for entry in typed}
    assert "".join(cluster_typed) == "lkc-customer"
    assert "".join(topic_typed) == "showcase-live-orders"
    assert {
        (entry["argument"]["inputSelector"], entry["argument"]["expectedValue"]) for entry in filter_input_commits
    } >= {
        ('input[placeholder="Any cluster"]', "lkc-customer"),
        ('input[placeholder="Any topic"]', "showcase-live-orders"),
    }
    assert any("Analytics" in entry["name"] for entry in clicks)
    assert any("Table" in entry["name"] for entry in clicks)
    assert hovers
    matched_responses = [entry for entry in log if entry["event"] == "waitForResponseMatched"]
    text_lookups = [str(entry["value"]) for entry in log if entry["event"] == "getByText"]
    locator_values = [str(entry["value"]) for entry in log if entry["event"] == "locator"]
    assert matched_responses and all(entry["status"] in {200, 202} for entry in matched_responses)
    matched_urls = {entry["url"] for entry in matched_responses}
    assert any(
        entry["url"].endswith("/focus-preview/requests") and entry["status"] == 202 for entry in matched_responses
    )
    assert all(
        entry["status"] == 200 for entry in matched_responses if not entry["url"].endswith("/focus-preview/requests")
    )
    assert any("/graph/search?q=env-commerce" in url for url in matched_urls)
    assert any("/graph/search?q=lkc-customer" in url for url in matched_urls)
    assert any("focus=env-commerce" in url for url in matched_urls)
    assert any("focus=lkc-customer" in url for url in matched_urls)
    assert any(
        "/topic-attributions/aggregate" in url and "topic_name=showcase-live-orders" in url for url in matched_urls
    )
    assert any(
        "/topic-attributions?" in url and "start_date=2026-08-30" in url and "end_date=2026-08-31" in url
        for url in matched_urls
    )
    assert any(
        url.endswith("/focus-preview/requests/22222222-2222-4222-8222-222222222222/manifest") for url in matched_urls
    )
    assert {
        "Completed at 2026-08-31T23:59:59Z",
        "Last Run Summary",
        "Per-Date Processing Status",
    } <= set(text_lookups)
    assert {
        '[role="gridcell"][col-id="billing_gathered"]',
        '[role="gridcell"][col-id="resources_gathered"]',
        '[role="gridcell"][col-id="chargeback_calculated"]',
    } <= set(locator_values)
    assert sum(value == pytest.approx(0.7 * RAW_CAPTURE_SPEED * 1000) for value in timed_waits) >= 1
    assert sum(value == pytest.approx(0.3 * RAW_CAPTURE_SPEED * 1000) for value in timed_waits) >= 1
    scrolls = {scene["id"]: scene["scroll_samples"] for scene in timeline["scenes"]}
    for scene_id in (
        "dashboard-cost-trend",
        "topic-composition",
        "topic-movers",
        "topic-table",
        "pipeline-status",
        "focus-export",
    ):
        samples = scrolls[scene_id]
        assert len(samples) >= 3
        assert samples[0]["offset"] == 0
        assert abs(samples[-1]["offset"] - samples[0]["offset"]) >= 120
        assert samples[-1]["elapsed_seconds"] == pytest.approx(0.9 * RAW_CAPTURE_SPEED, abs=2 / 30)
    assert timeline["framing"]["zoom_percent"] == 150
    assert timeline["framing"]["minimum_playback_text_pixels"] >= 11
    assert timeline["framing"]["minimum_playback_target_pixels"] >= 24
    assert timeline["evidence"]["topic_tooltip"] == "$79,000.00"
    assert timeline["evidence"]["movers_tooltip"] == "2026-08-31 +$49,000.00 increase"
    assert set(tuple(row) for row in timeline["evidence"]["table_rows"]) == {
        ("2026-08-30", "$1,000.00"),
        ("2026-08-31", "$50,000.00"),
    }


@pytest.mark.parametrize(
    ("failure", "expected_detail"),
    [
        pytest.param("tooltip", "tooltip", id="missing-tooltip"),
        pytest.param("topic-tooltip", "showcase-live-orders", id="topic-tooltip-missing-name"),
        pytest.param("offscreen", "off-screen", id="offscreen-evidence"),
        pytest.param("overrun", "overrun", id="scene-overrun"),
    ],
)
def test_imported_capture_runner_rejects_missing_tooltip_offscreen_evidence_and_scene_overrun(
    tmp_path: Path,
    failure: str,
    expected_detail: str,
) -> None:
    result, result_path = _invoke_capture_with_fake_browser(tmp_path, "full", failure)

    assert result.returncode != 0
    assert "full" in result.stderr
    assert "scene" in result.stderr.lower()
    assert expected_detail in result.stderr.lower()
    assert not result_path.exists()


def test_imported_capture_runner_owns_aggregate_rejection_while_filter_typing_is_pending(tmp_path: Path) -> None:
    result, result_path = _invoke_capture_with_fake_browser(tmp_path, "full", "aggregate-rejection")

    assert result.returncode != 0
    assert "scene full topic-filters" in result.stderr.lower()
    assert "synthetic aggregate rejection while filter typing is pending" in result.stderr.lower()
    assert "unhandled" not in result.stderr.lower()
    assert not result_path.exists()


def test_encoder_rejects_an_incomplete_argument_vector_before_invoking_media_tools(tmp_path: Path) -> None:
    media_root = tmp_path / "media"
    command_log = tmp_path / "encoder-tools.log"
    fake_bin = tmp_path / "fake-bin"
    fake_bin.mkdir()
    _write_executable(
        fake_bin / "ffmpeg",
        "#!/usr/bin/env sh\nprintf 'ffmpeg\\n' >>\"$DEMO_ENCODER_TOOL_LOG\"\nexit 99\n",
    )
    _write_executable(
        fake_bin / "ffprobe",
        "#!/usr/bin/env sh\nprintf 'ffprobe\\n' >>\"$DEMO_ENCODER_TOOL_LOG\"\nexit 99\n",
    )
    environment = {
        **os.environ,
        "DEMO_ENCODER_TOOL_LOG": str(command_log),
        "PATH": f"{fake_bin}{os.pathsep}{os.environ['PATH']}",
    }

    result = subprocess.run(
        [
            "sh",
            str(MEDIA_DIR / "encode.sh"),
            str(media_root),
            "work/chitragupta-demo-walkthrough.webm",
            "work/captions.srt",
            f"assets/{PNG_ASSETS[0]}",
            f"assets/{VIDEO}",
            f"assets/{POSTER}",
            "1600",
            "900",
            "960",
            "540",
            "60",
        ],
        cwd=PROJECT_ROOT,
        env=environment,
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode != 0
    assert not command_log.exists()


def _write_marker_video(
    destination: Path,
    *,
    width: int = 1600,
    height: int = 800,
    frame_rate: int = 30,
    marker_runs: tuple[float, ...] = (0.4, 0.4),
    story_seconds: float = 18.0,
) -> None:
    ffmpeg = shutil.which("ffmpeg")
    assert ffmpeg is not None, "FFmpeg is required to generate encoder fixture input"
    segments = [("black", 0.4)]
    for index, run in enumerate(marker_runs):
        segments.append((MARKER_COLOR, run))
        if index + 1 < len(marker_runs):
            segments.append(("0x1d4ed8", story_seconds))
    segments.append(("black", 0.4))
    inputs: list[str] = []
    labels: list[str] = []
    for index, (color, duration) in enumerate(segments):
        inputs.extend(["-f", "lavfi", "-i", f"color=c={color}:s={width}x{height}:r={frame_rate}:d={duration}"])
        labels.append(f"[{index}:v]")
    result = subprocess.run(
        [
            ffmpeg,
            "-hide_banner",
            "-loglevel",
            "error",
            "-y",
            *inputs,
            "-filter_complex",
            f"{''.join(labels)}concat=n={len(labels)}:v=1:a=0[out]",
            "-map",
            "[out]",
            "-c:v",
            "libvpx-vp9",
            "-pix_fmt",
            "yuv420p",
            str(destination),
        ],
        cwd=PROJECT_ROOT,
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr


def _probe_video(path: Path) -> dict[str, Any]:
    ffprobe = shutil.which("ffprobe")
    assert ffprobe is not None, "FFprobe is required to validate encoder output"
    result = subprocess.run(
        [
            ffprobe,
            "-v",
            "error",
            "-show_entries",
            "format=duration:stream=codec_type,codec_name,width,height,avg_frame_rate",
            "-of",
            "json",
            str(path),
        ],
        cwd=PROJECT_ROOT,
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr
    return json.loads(result.stdout)


def _probe_signalstats(path: Path, *, timestamp: float, video_filter: str = "") -> dict[str, float]:
    """Read FFmpeg signal statistics from one decoded output frame."""
    ffmpeg = shutil.which("ffmpeg")
    assert ffmpeg is not None, "FFmpeg is required to inspect encoded output"
    filter_graph = ",".join(part for part in (video_filter, "signalstats", "metadata=mode=print:file=-") if part)
    result = subprocess.run(
        [
            ffmpeg,
            "-hide_banner",
            "-loglevel",
            "error",
            "-ss",
            f"{timestamp:.3f}",
            "-i",
            str(path),
            "-vf",
            filter_graph,
            "-frames:v",
            "1",
            "-f",
            "null",
            "-",
        ],
        cwd=PROJECT_ROOT,
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr
    stats: dict[str, float] = {}
    for line in result.stdout.splitlines():
        key, separator, value = line.partition("=")
        if separator and key.startswith("lavfi.signalstats."):
            stats[key.removeprefix("lavfi.signalstats.")] = float(value)
    assert stats, "FFmpeg did not report signal statistics"
    return stats


def _caption_band_active_rows(path: Path, *, timestamp: float) -> list[int]:
    """Return rows with visible caption pixels in the fixed 100-pixel band."""
    ffmpeg = shutil.which("ffmpeg")
    assert ffmpeg is not None, "FFmpeg is required to inspect caption pixels"
    result = subprocess.run(
        [
            ffmpeg,
            "-hide_banner",
            "-loglevel",
            "error",
            "-ss",
            f"{timestamp:.3f}",
            "-i",
            str(path),
            "-vf",
            "crop=1600:100:0:800,format=gray",
            "-frames:v",
            "1",
            "-f",
            "rawvideo",
            "-pix_fmt",
            "gray",
            "-",
        ],
        cwd=PROJECT_ROOT,
        capture_output=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr.decode(errors="replace")
    row_width = 1600
    row_count = 100
    pixels = result.stdout
    assert len(pixels) >= row_width * row_count
    return [row for row in range(row_count) if max(pixels[row * row_width : (row + 1) * row_width]) >= 80]


def test_encoder_uses_actual_ffmpeg_marker_boundaries_speed_and_caption_band_for_draft_output(tmp_path: Path) -> None:
    media_root = tmp_path / "media"
    work_dir = media_root / "work"
    assets_dir = media_root / "assets"
    work_dir.mkdir(parents=True)
    assets_dir.mkdir()
    raw_video = media_root / RAW_VIDEO_PATH
    video = media_root / DRAFT_VIDEO_PATH
    _write_marker_video(raw_video, frame_rate=25)
    draft_timeline = _timeline("draft")
    _write_json(media_root / EDIT_TIMELINE_PATH, draft_timeline)
    (media_root / CAPTIONS_PATH).write_text(_timeline_srt("draft", draft_timeline), encoding="utf-8")
    (assets_dir / PNG_ASSETS[0]).write_bytes(_png())

    result = subprocess.run(
        ["sh", str(MEDIA_DIR / "encode.sh"), str(media_root), *_encoder_vector("draft")],
        cwd=PROJECT_ROOT,
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr
    assert not raw_video.exists()
    assert video.is_file()
    assert (assets_dir / POSTER).is_file()
    probe = _probe_video(video)
    stream = probe["streams"][0]
    assert stream["codec_name"] == "h264"
    assert stream["codec_type"] == "video"
    assert stream["avg_frame_rate"] == "30/1"
    assert (stream["width"], stream["height"]) == (1600, 900)
    assert all(candidate.get("codec_type") != "audio" for candidate in probe["streams"])
    assert float(probe["format"]["duration"]) == pytest.approx(18 / _render_speed("draft"), abs=2 / 30)
    first_story_frame = _probe_signalstats(video, timestamp=0.0)
    assert 40 <= first_story_frame["YAVG"] <= 120
    assert first_story_frame["UAVG"] >= 160
    active_caption_rows = _caption_band_active_rows(video, timestamp=1.0)
    assert len(active_caption_rows) >= 20
    assert max(active_caption_rows) - min(active_caption_rows) >= 20
    assert min(active_caption_rows) >= 16
    assert max(active_caption_rows) <= 90
    encoder_result = json.loads((work_dir / "encoder-result.json").read_text(encoding="utf-8"))
    assert encoder_result["frame_rate"] == 30
    assert encoder_result["audio_stream_count"] == 0
    assert encoder_result["speed"] == _render_speed("draft")
    assert encoder_result["story_first_frame"] >= 8
    assert encoder_result["story_last_frame"] > encoder_result["story_first_frame"]
    assert encoder_result["raw_frame_rate"] == 25
    assert encoder_result["edited_duration_seconds"] == pytest.approx(18 / _render_speed("draft"), abs=2 / 30)
    timeline = json.loads((media_root / EDIT_TIMELINE_PATH).read_text(encoding="utf-8"))
    assert timeline["markers"]["runs"] == [
        {"duration_seconds": pytest.approx(0.4, abs=1 / 25), "frames": 10},
        {"duration_seconds": pytest.approx(0.4, abs=1 / 25), "frames": 10},
    ]


def test_encoder_accepts_a_valid_full_mode_cut_and_media_validation(tmp_path: Path) -> None:
    media_root = tmp_path / "media"
    work_dir = media_root / "work"
    assets_dir = media_root / "assets"
    work_dir.mkdir(parents=True)
    assets_dir.mkdir()
    spec_path = tmp_path / "capture-spec.json"
    _write_json(spec_path, _capture_spec())
    timeline = _timeline("full", action_ratio=0.05)
    raw_story_seconds = _timeline_duration(timeline)
    _write_marker_video(media_root / RAW_VIDEO_PATH, frame_rate=25, story_seconds=raw_story_seconds)
    timeline_path = media_root / EDIT_TIMELINE_PATH
    _write_json(timeline_path, timeline)
    (media_root / CAPTIONS_PATH).write_text(_timeline_srt("full", timeline), encoding="utf-8")
    (assets_dir / PNG_ASSETS[0]).write_bytes(_png())

    result = subprocess.run(
        ["sh", str(MEDIA_DIR / "encode.sh"), str(media_root), *_encoder_vector("full")],
        cwd=PROJECT_ROOT,
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr
    video = media_root / FULL_VIDEO_PATH
    assert video.is_file()
    probe = _probe_video(video)
    assert probe["streams"][0]["codec_type"] == "video"
    assert probe["streams"][0]["avg_frame_rate"] == "30/1"
    assert all(candidate.get("codec_type") != "audio" for candidate in probe["streams"])
    assert float(probe["format"]["duration"]) == pytest.approx(raw_story_seconds / _render_speed("full"), abs=2 / 30)

    validation = _run_tool(
        "validate-encode",
        "--spec",
        str(spec_path),
        "--mode",
        "full",
        "--timeline",
        str(timeline_path),
        "--encoder-result",
        str(work_dir / "encoder-result.json"),
        "--video",
        str(video),
    )
    assert validation.returncode == 0, validation.stderr
    assert not (media_root / RAW_VIDEO_PATH).exists()


@pytest.mark.parametrize(
    ("width", "height", "marker_runs", "output_path", "story_seconds", "expected_error"),
    [
        pytest.param(
            1600,
            900,
            (0.4, 0.4),
            DRAFT_VIDEO_PATH,
            1,
            "raw video dimensions are not 1600x800",
            id="wrong-raw-dimensions",
        ),
        pytest.param(
            1600,
            800,
            (),
            DRAFT_VIDEO_PATH,
            1,
            "expected exactly two approved marker runs, found 0",
            id="missing-markers",
        ),
        pytest.param(
            1600,
            800,
            (0.4, 0.4, 0.4),
            DRAFT_VIDEO_PATH,
            1,
            "expected exactly two approved marker runs, found 3",
            id="extra-markers",
        ),
        pytest.param(
            1600,
            800,
            (0.1, 0.4),
            DRAFT_VIDEO_PATH,
            1,
            "marker run is outside the approved 0.3-0.5 second range",
            id="short-marker",
        ),
        pytest.param(
            1600,
            800,
            (0.4, 0.4),
            DRAFT_VIDEO_PATH,
            1,
            "encoded video duration is outside 15-20 seconds",
            id="short-output-duration",
        ),
        pytest.param(
            1600,
            800,
            (0.4, 0.4),
            "../escaped.mp4",
            1,
            "encoder mode, duration bounds, or output path is not approved",
            id="unsafe-output-path",
        ),
    ],
)
def test_encoder_rejects_invalid_raw_dimensions_marker_runs_duration_and_output_paths(
    tmp_path: Path,
    width: int,
    height: int,
    marker_runs: tuple[float, ...],
    output_path: str,
    story_seconds: float,
    expected_error: str,
) -> None:
    media_root = tmp_path / "media"
    work_dir = media_root / "work"
    assets_dir = media_root / "assets"
    work_dir.mkdir(parents=True)
    assets_dir.mkdir()
    _write_marker_video(
        media_root / RAW_VIDEO_PATH,
        width=width,
        height=height,
        marker_runs=marker_runs,
        story_seconds=story_seconds,
    )
    draft_timeline = _timeline("draft")
    _write_json(media_root / EDIT_TIMELINE_PATH, draft_timeline)
    (media_root / CAPTIONS_PATH).write_text("1\n00:00:00,000 --> 00:00:01,000\nVisible story\n", encoding="utf-8")
    (assets_dir / PNG_ASSETS[0]).write_bytes(_png())
    vector = _encoder_vector("draft")
    vector[3] = output_path
    result = subprocess.run(
        ["sh", str(MEDIA_DIR / "encode.sh"), str(media_root), *vector],
        cwd=PROJECT_ROOT,
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode != 0
    assert expected_error in result.stderr
    assert (media_root / RAW_VIDEO_PATH).exists()
    assert not (work_dir / "encoder-result.json").exists()


def test_frontend_refine_disables_telemetry_for_the_public_demo() -> None:
    app_source = (PROJECT_ROOT / "frontend/src/App.tsx").read_text(encoding="utf-8")

    assert "options={{ disableTelemetry: true, syncWithLocation: true }}" in app_source


def test_imported_capture_runner_preserves_still_capture_readiness_pipeline_and_quiescence_contracts(
    tmp_path: Path,
) -> None:
    capture = _run_capture_with_fake_browser(tmp_path, "full")
    log = capture["log"]
    gotos = [entry["url"] for entry in log if entry["event"] == "goto"]
    roles = [entry for entry in log if entry["event"] == "getByRole"]
    locator_values = [entry["value"] for entry in log if entry["event"] == "locator"]
    text_values = [str(entry["value"]) for entry in log if entry["event"] == "getByText"]

    assert "http://chitragupta-ui/explorer?at=2026-08-31" in gotos
    assert "http://chitragupta-ui/pipeline" in gotos
    assert "http://chitragupta-ui/focus-preview" in gotos
    assert {"breadcrumb-trail", "graph-container", "timeline-scrubber"} <= {
        value.removeprefix('[data-testid="').removesuffix('"]') for value in locator_values
    }
    assert any(
        entry["value"] == "button" and entry["options"] == {"name": "Run Pipeline", "exact": False} for entry in roles
    )
    assert any(entry["event"] == "isDisabled" and "Run Pipeline" in entry["name"] for entry in log)
    assert any(entry["event"] == "hover" and "Run Pipeline" in entry["name"] for entry in log)
    assert "Top Topics by Cost" in text_values
    assert "Pipeline execution is unavailable in API-only mode." in text_values
    response_json_indexes = [index for index, entry in enumerate(log) if entry["event"] == "responseJson"]
    screenshot_indexes = [index for index, entry in enumerate(log) if entry["event"] == "screenshot"]
    assert len(screenshot_indexes) == len(PNG_ASSETS)
    assert response_json_indexes and min(response_json_indexes) < screenshot_indexes[-1]


@pytest.mark.parametrize(
    ("failure", "expected_detail"),
    [
        pytest.param("api-topic", "topic_name", id="api-topic-name"),
        pytest.param("dom-topic", "dom topic_name", id="dom-topic-name"),
        pytest.param("dom-identifier", "dom identifier", id="dom-source-identifier"),
        pytest.param("bad-preview", "runtime", id="malformed-preview-id"),
    ],
)
def test_imported_capture_runner_keeps_catalog_and_runtime_preview_validation_blocking(
    tmp_path: Path,
    failure: str,
    expected_detail: str,
) -> None:
    result, result_path = _invoke_capture_with_fake_browser(tmp_path, "full", failure)

    assert result.returncode != 0
    assert expected_detail in result.stderr.lower()
    assert not result_path.exists()


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
    assert "/opt/chitragupta-demo-media/capture-spec.json" not in "\n".join(encoder["volumes"])
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
