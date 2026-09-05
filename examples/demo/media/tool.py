#!/usr/bin/env python3
"""Validate and prepare the reproducible Demo Experience media workspace."""

from __future__ import annotations

import argparse
import dataclasses
import hashlib
import json
import math
import re
import struct
import sys
from collections.abc import Mapping, Sequence
from datetime import date, datetime
from decimal import Decimal
from enum import Enum
from pathlib import Path
from typing import TYPE_CHECKING, Any, NoReturn, cast
from urllib.parse import urlsplit

from core.config.loader import load_config
from core.preview.storage_availability import PreviewEvidenceAvailability, PreviewEvidenceAvailabilityState
from core.storage.backends.sqlmodel.unit_of_work import SQLModelBackend
from core.storage.registry import create_storage_backend
from demo.generator import (
    DemoProfile,
    _build_scenario,
    _database_file_names,
    _select_demo_tenants,
    _tenant_parts,
    _validate_ccloud_preview,
    _validate_persisted_scenario,
)
from plugins.confluent_cloud.demo.scenario import build_showcase_demo_scenario
from plugins.self_managed_kafka.demo.scenario import build_clean_self_managed_kafka_scenario

if TYPE_CHECKING:
    from plugins.confluent_cloud.demo.scenario import ConfluentDemoScenario

SPEC_KEYS = frozenset(
    {
        "schema_version",
        "anchor_date",
        "profile",
        "viewport",
        "poster",
        "video",
        "primary_tenant",
        "screenshots",
        "captions",
    }
)
VIEWPORT_KEYS = frozenset({"width", "height"})
POSTER_KEYS = frozenset({"width", "height", "name"})
VIDEO_KEYS = frozenset({"target_seconds", "minimum_seconds", "maximum_seconds", "name"})
TENANT_KEYS = frozenset({"name", "id", "ecosystem"})
SCREENSHOT_KEYS = frozenset({"name", "route"})
CAPTION_KEYS = frozenset({"start", "end", "text"})
STATE_METADATA_KEYS = frozenset({"schema_version", "generator_version", "profile", "anchor_date"})
DATABASE_EVIDENCE_KEYS = frozenset({"schema_version", "validated", "state_metadata", "files"})
DATABASE_FILE_KEYS = frozenset({"path", "sha256", "bytes"})

APPROVED_PNG_ASSETS = (
    "chitragupta-demo-dashboard.png",
    "chitragupta-demo-cost-explorer.png",
    "chitragupta-demo-topic-attribution.png",
    "chitragupta-demo-pipeline-status.png",
    "chitragupta-demo-focus-mapping-preview.png",
)
APPROVED_POSTER_ASSET = "chitragupta-demo-dashboard-poster.webp"
APPROVED_VIDEO_ASSET = "chitragupta-demo-walkthrough.mp4"
APPROVED_ASSETS = (*APPROVED_PNG_ASSETS, APPROVED_POSTER_ASSET, APPROVED_VIDEO_ASSET)
APPROVED_CAPTIONS = (
    (0, 13, "Synthetic tenant cost reconciles across usage and shared spend."),
    (13, 30, "Tenant cost is the analytical root."),
    (30, 44, "Topic-level attribution exposes cost concentration and candidates for review."),
    (44, 56, "Persisted processing state remains inspectable in API-only mode."),
    (56, 75, "Generate a FOCUS 1.4 mapping preview from synthetic persisted evidence."),
)
APPROVED_PRIMARY_TENANT = {
    "name": "clean-confluent",
    "id": "northstar-confluent",
    "ecosystem": "confluent_cloud",
}
APPROVED_SCREENSHOT_ROUTES = (
    (APPROVED_PNG_ASSETS[0], "/dashboard"),
    (APPROVED_PNG_ASSETS[1], "/explorer"),
    (APPROVED_PNG_ASSETS[2], "/topic-attributions"),
    (APPROVED_PNG_ASSETS[3], "/pipeline"),
    (APPROVED_PNG_ASSETS[4], "/focus-preview"),
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
MANIFEST_KEYS = frozenset(
    {
        "schema_version",
        "source_commit",
        "source_worktree_clean",
        "anchor_date",
        "profile",
        "primary_tenant",
        "expected_assets",
        "assets",
        "validation",
    }
)
BROWSER_OBSERVATION_KEYS = frozenset(
    {"ui_origin", "requests", "api_identifiers_match_catalog", "dom_identifiers_match_catalog"}
)
ENCODER_RESULT_KEYS = frozenset(
    {
        "duration_seconds",
        "video_codec",
        "width",
        "height",
        "frame_rate",
        "audio_stream_count",
        "captions_burned_in",
        "caption_filter",
        "caption_source",
        "webm_removed",
    }
)
SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
COMMIT_RE = re.compile(r"^[0-9a-f]{40}$")


class MediaError(ValueError):
    """Raised when committed input or generated media is not valid."""


def _fail(message: str) -> NoReturn:
    raise MediaError(message)


def _read_json(path: Path, label: str) -> object:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        _fail(f"{label} is not valid JSON: {exc}")


def _mapping(value: object, label: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        _fail(f"{label} must be an object")
    return value


def _keys(value: Mapping[str, Any], expected: frozenset[str], label: str) -> None:
    actual = set(value)
    if actual != expected:
        missing = sorted(expected - actual)
        unknown = sorted(actual - expected)
        details: list[str] = []
        if missing:
            details.append(f"missing {', '.join(missing)}")
        if unknown:
            details.append(f"unknown {', '.join(unknown)}")
        _fail(f"{label} has invalid fields ({'; '.join(details)})")


def _string(value: object, label: str) -> str:
    if not isinstance(value, str) or not value:
        _fail(f"{label} must be a non-empty string")
    return value


def _integer(value: object, label: str) -> int:
    if type(value) is not int:
        _fail(f"{label} must be an integer")
    return value


def _number(value: object, label: str) -> int | float:
    if not isinstance(value, (int, float)) or isinstance(value, bool) or not math.isfinite(float(value)):
        _fail(f"{label} must be a finite number")
    return int(value) if isinstance(value, int) else float(value)


def _boolean(value: object, label: str) -> bool:
    if type(value) is not bool:
        _fail(f"{label} must be a boolean")
    return value


def _validate_spec_payload(raw: object) -> dict[str, Any]:
    spec = _mapping(raw, "capture specification")
    _keys(spec, SPEC_KEYS, "capture specification")
    if _integer(spec["schema_version"], "schema_version") != 1:
        _fail("capture specification schema_version must be 1")
    if _string(spec["anchor_date"], "anchor_date") != "2026-08-31":
        _fail("capture specification anchor_date must be 2026-08-31")
    try:
        if date.fromisoformat(spec["anchor_date"]).isoformat() != spec["anchor_date"]:
            _fail("capture specification anchor_date must be an ISO date")
    except ValueError as exc:
        _fail(f"capture specification anchor_date is invalid: {exc}")
    if _string(spec["profile"], "profile") != "showcase":
        _fail("capture specification profile must be showcase")

    viewport = _mapping(spec["viewport"], "viewport")
    _keys(viewport, VIEWPORT_KEYS, "viewport")
    if (_integer(viewport["width"], "viewport.width"), _integer(viewport["height"], "viewport.height")) != (
        1600,
        900,
    ):
        _fail("capture specification viewport must be 1600x900")

    poster = _mapping(spec["poster"], "poster")
    _keys(poster, POSTER_KEYS, "poster")
    if (
        _integer(poster["width"], "poster.width"),
        _integer(poster["height"], "poster.height"),
        _string(poster["name"], "poster.name"),
    ) != (960, 540, APPROVED_POSTER_ASSET):
        _fail("capture specification poster is not canonical")

    video = _mapping(spec["video"], "video")
    _keys(video, VIDEO_KEYS, "video")
    if _string(video["name"], "video.name") != APPROVED_VIDEO_ASSET:
        _fail("capture specification video name is not canonical")
    for field in ("target_seconds", "minimum_seconds", "maximum_seconds"):
        _integer(video[field], f"video.{field}")
    if (video["target_seconds"], video["minimum_seconds"], video["maximum_seconds"]) != (75, 60, 90):
        _fail("capture specification video duration bounds are not canonical")

    tenant = _mapping(spec["primary_tenant"], "primary_tenant")
    _keys(tenant, TENANT_KEYS, "primary_tenant")
    if tenant != APPROVED_PRIMARY_TENANT:
        _fail("capture specification primary tenant is not canonical")

    screenshots = spec["screenshots"]
    if not isinstance(screenshots, list) or len(screenshots) != len(APPROVED_SCREENSHOT_ROUTES):
        _fail("capture specification must contain exactly five screenshots")
    for index, (expected_name, expected_route) in enumerate(APPROVED_SCREENSHOT_ROUTES):
        screenshot = _mapping(screenshots[index], f"screenshots[{index}]")
        _keys(screenshot, SCREENSHOT_KEYS, f"screenshots[{index}]")
        if (
            _string(screenshot["name"], f"screenshots[{index}].name"),
            _string(screenshot["route"], f"screenshots[{index}].route"),
        ) != (expected_name, expected_route):
            _fail(f"screenshots[{index}] is not canonical")

    captions = spec["captions"]
    if not isinstance(captions, list) or len(captions) != len(APPROVED_CAPTIONS):
        _fail("capture specification must contain exactly five contiguous captions")
    for index, (expected_start, expected_end, expected_text) in enumerate(APPROVED_CAPTIONS):
        caption = _mapping(captions[index], f"captions[{index}]")
        _keys(caption, CAPTION_KEYS, f"captions[{index}]")
        values = (
            _integer(caption["start"], f"captions[{index}].start"),
            _integer(caption["end"], f"captions[{index}].end"),
            _string(caption["text"], f"captions[{index}].text"),
        )
        if values != (expected_start, expected_end, expected_text):
            _fail(f"captions[{index}] is not canonical or has a gap")

    return spec


def load_spec(path: Path) -> dict[str, Any]:
    """Load and strictly validate a committed capture specification."""
    return _validate_spec_payload(_read_json(path, "capture specification"))


def _json_value(value: object) -> object:
    if dataclasses.is_dataclass(value) and not isinstance(value, type):
        return {field.name: _json_value(getattr(value, field.name)) for field in dataclasses.fields(value)}
    if isinstance(value, Mapping):
        return {str(key): _json_value(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set, frozenset)):
        return [_json_value(item) for item in value]
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, Decimal):
        return str(value)
    if hasattr(value, "model_dump"):
        return _json_value(value.model_dump(mode="json"))
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    return str(value)


def _write_json(path: Path, value: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, sort_keys=False) + "\n", encoding="utf-8")


def _srt_time(seconds: int) -> str:
    hours, remainder = divmod(seconds, 3600)
    minutes, whole_seconds = divmod(remainder, 60)
    return f"{hours:02d}:{minutes:02d}:{whole_seconds:02d},000"


def _captions_srt(spec: Mapping[str, Any]) -> str:
    entries = [
        f"{index}\n{_srt_time(caption['start'])} --> {_srt_time(caption['end'])}\n{caption['text']}"
        for index, caption in enumerate(spec["captions"], start=1)
    ]
    return "\n\n".join(entries) + "\n"


def _runtime_asset_names(spec: Mapping[str, Any]) -> tuple[str, ...]:
    """Return the validated runtime assets in their stable publication order."""
    screenshots = tuple(cast("Mapping[str, Any]", screenshot) for screenshot in spec["screenshots"])
    return (
        *(cast("str", screenshot["name"]) for screenshot in screenshots),
        cast("str", spec["poster"]["name"]),
        cast("str", spec["video"]["name"]),
    )


def _encoder_arguments(spec: Mapping[str, Any]) -> tuple[str, ...]:
    """Build the fixed, newline-safe encoder argument protocol."""
    video_name = cast("str", spec["video"]["name"])
    if not video_name.endswith(".mp4"):
        _fail("capture specification video name must end with .mp4")
    viewport = cast("Mapping[str, Any]", spec["viewport"])
    poster = cast("Mapping[str, Any]", spec["poster"])
    return (
        f"work/{video_name[:-4]}.webm",
        "work/captions.srt",
        f"assets/{cast('str', spec['screenshots'][0]['name'])}",
        f"assets/{video_name}",
        f"assets/{cast('str', poster['name'])}",
        str(viewport["width"]),
        str(viewport["height"]),
        str(poster["width"]),
        str(poster["height"]),
        str(spec["video"]["minimum_seconds"]),
        str(spec["video"]["maximum_seconds"]),
    )


def _publication_arguments(spec: Mapping[str, Any]) -> tuple[str, ...]:
    """Build the fixed, poster-free publication argument protocol."""
    names = _runtime_asset_names(spec)
    return tuple(f"assets/{name}" for name in names if name != spec["poster"]["name"]) + ("manifest.json",)


def _write_argument_vector(path: Path, values: Sequence[str]) -> None:
    """Write one validated shell argument per line without evaluation semantics."""
    records = tuple(values)
    for value in records:
        if not isinstance(value, str) or any(character in value for character in ("\n", "\r", "\x00")):
            _fail("argument vector contains a newline or NUL character")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("".join(f"{value}\n" for value in records), encoding="utf-8")


def _tenant_items(settings: Any) -> list[tuple[str, Any]]:
    tenants = list(settings.tenants.items())
    if len(tenants) != 2:
        _fail("demo configuration must contain exactly two tenants")
    ecosystems = {tenant.ecosystem for _name, tenant in tenants}
    if ecosystems != {"confluent_cloud", "self_managed_kafka"}:
        _fail("demo configuration must contain one Confluent Cloud and one self-managed Kafka tenant")
    return sorted(tenants, key=lambda item: item[1].ecosystem != "confluent_cloud")


def _state_metadata(media_state_dir: Path, spec: Mapping[str, Any]) -> dict[str, Any]:
    raw = _mapping(
        _read_json(media_state_dir / "demo-state.json", "Demo state metadata"),
        "Demo state metadata",
    )
    _keys(raw, STATE_METADATA_KEYS, "Demo state metadata")
    if raw != {
        "schema_version": 1,
        "generator_version": 1,
        "profile": spec["profile"],
        "anchor_date": spec["anchor_date"],
    }:
        _fail("media state is not fresh Showcase data at the committed anchor")
    return raw


def _database_evidence(
    *,
    config_path: Path,
    state_dir: Path,
    spec: Mapping[str, Any],
) -> dict[str, Any]:
    """Validate persisted state through the generator and fingerprint its inputs."""
    metadata = _state_metadata(state_dir, spec)
    try:
        settings = load_config(config_path)
        selected = _select_demo_tenants(settings)
        anchor = date.fromisoformat(spec["anchor_date"])
        for tenant_name, tenant_config in selected:
            scenario = _build_scenario(
                ecosystem=tenant_config.ecosystem,
                tenant_id=tenant_config.tenant_id,
                anchor_date=anchor,
                profile=DemoProfile.SHOWCASE,
            )
            ecosystem, tenant_id, storage_module = _tenant_parts(tenant_config)
            backend = create_storage_backend(
                tenant_config.storage,
                storage_module=storage_module,
                use_migrations=False,
                focus_preview_enabled=tenant_config.focus_preview_enabled,
            )
            if not isinstance(backend, SQLModelBackend):
                _fail("Demo state requires the SQLModel storage backend")
            try:
                with backend.create_read_only_unit_of_work() as uow:
                    persisted_dates = uow.chargebacks.get_distinct_dates(ecosystem, tenant_id)
                    if not persisted_dates:
                        _fail(f"persisted Showcase state has no chargeback dates for {tenant_name}")
                    if max(persisted_dates) != scenario.anchor_date:
                        _fail(f"persisted Showcase chargeback anchor does not match {tenant_name}")
                    _validate_persisted_scenario(
                        uow,
                        tenant_name,
                        scenario,
                        validate_generated_tags=False,
                    )
                if ecosystem == "confluent_cloud":
                    backend._preview_evidence_availability = PreviewEvidenceAvailability(
                        PreviewEvidenceAvailabilityState.READY
                    )
                    _validate_ccloud_preview(backend, cast("ConfluentDemoScenario", scenario))
            finally:
                backend.dispose()
    except Exception as exc:
        _fail(f"persisted Showcase state does not match the typed demo scenarios: {exc}")
    try:
        database_names = _database_file_names(selected)
    except (OSError, RuntimeError, ValueError, TypeError) as exc:
        _fail(f"unable to determine Demo database files: {exc}")
    evidence_files: list[dict[str, object]] = []
    paths = [state_dir / "demo-state.json", *(state_dir / name for name in sorted(database_names))]
    for path in paths:
        if not path.is_file() or path.is_symlink():
            _fail(f"Demo state evidence file is missing or not a regular file: {path.name}")
        evidence_files.append(
            {
                "path": path.name,
                "sha256": _sha256(path),
                "bytes": path.stat().st_size,
            }
        )
    return {
        "schema_version": 1,
        "validated": True,
        "state_metadata": metadata,
        "files": evidence_files,
    }


def _source_identifiers(value: object) -> set[str]:
    """Collect complete typed identifiers from the generated scenario projection."""
    identifiers: set[str] = set()
    uuid_pattern = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$")
    topic_resource_pattern = re.compile(r"^[a-z0-9-]+:topic:[a-z0-9][a-z0-9._-]*$")
    if isinstance(value, Mapping):
        for item in value.values():
            identifiers.update(_source_identifiers(item))
    elif isinstance(value, (list, tuple, set, frozenset)):
        for item in value:
            identifiers.update(_source_identifiers(item))
    elif isinstance(value, str) and (
        uuid_pattern.fullmatch(value.lower()) is not None or topic_resource_pattern.fullmatch(value.lower()) is not None
    ):
        identifiers.add(value)
    return identifiers


def build_catalog(
    spec_path: Path,
    config_path: Path,
    output_path: Path,
    srt_output_path: Path,
    encoder_arguments_output: Path,
    state_dir: Path | None = None,
) -> None:
    """Project the current pure demo scenarios into a capture allowlist."""
    spec = load_spec(spec_path)
    try:
        settings = load_config(config_path)
    except Exception as exc:
        _fail(f"unable to load Demo config for catalog: {exc}")
    anchor = date.fromisoformat(spec["anchor_date"])
    tenants = _tenant_items(settings)
    scenario_values: list[dict[str, object]] = []
    for name, tenant in tenants:
        scenario: object
        if tenant.ecosystem == "confluent_cloud":
            scenario = build_showcase_demo_scenario(tenant_id=tenant.tenant_id, anchor_date=anchor)
        else:
            scenario = build_clean_self_managed_kafka_scenario(tenant_id=tenant.tenant_id, anchor_date=anchor)
        scenario_values.append(
            {
                "name": name,
                "tenant_id": tenant.tenant_id,
                "ecosystem": tenant.ecosystem,
                "scenario": _json_value(scenario),
            }
        )
    scenario_identifiers = sorted(_source_identifiers(scenario_values))
    catalog: dict[str, object] = {
        "schema_version": 1,
        "anchor_date": spec["anchor_date"],
        "profile": spec["profile"],
        "primary_tenant": spec["primary_tenant"],
        "representative_identifiers": ["cluster-commerce", "topic-orders"],
        "source_identifiers": scenario_identifiers,
        "tenants": [{"name": name, "id": tenant.tenant_id, "ecosystem": tenant.ecosystem} for name, tenant in tenants],
        "scenarios": scenario_values,
    }
    if state_dir is not None:
        catalog["database_evidence"] = _database_evidence(config_path=config_path, state_dir=state_dir, spec=spec)
    _write_json(output_path, catalog)
    srt_output_path.parent.mkdir(parents=True, exist_ok=True)
    srt_output_path.write_text(_captions_srt(spec), encoding="utf-8")
    _write_argument_vector(encoder_arguments_output, _encoder_arguments(spec))


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _png_dimensions(path: Path) -> tuple[int, int] | None:
    try:
        header = path.read_bytes()[:24]
    except OSError:
        return None
    if len(header) != 24 or header[:8] != b"\x89PNG\r\n\x1a\n" or header[12:16] != b"IHDR":
        return None
    return struct.unpack(">II", header[16:24])


def _webp_dimensions(path: Path) -> tuple[int, int] | None:
    try:
        data = path.read_bytes()
    except OSError:
        return None
    if len(data) < 20 or data[:4] != b"RIFF" or data[8:12] != b"WEBP":
        return None
    chunk = data[12:16]
    payload = data[20:]
    if chunk == b"VP8X" and len(payload) >= 10:
        return (1 + int.from_bytes(payload[4:7], "little"), 1 + int.from_bytes(payload[7:10], "little"))
    if chunk == b"VP8 " or b"\x9d\x01\x2a" in payload:
        marker = payload.find(b"\x9d\x01\x2a")
        if marker >= 0 and len(payload) >= marker + 7:
            return (
                int.from_bytes(payload[marker + 3 : marker + 5], "little") & 0x3FFF,
                int.from_bytes(payload[marker + 5 : marker + 7], "little") & 0x3FFF,
            )
    if chunk == b"VP8L" and len(payload) >= 5 and payload[0] == 0x2F:
        width = 1 + ((payload[1] | (payload[2] << 8)) & 0x3FFF)
        height = 1 + (((payload[2] >> 6) | (payload[3] << 2) | ((payload[4] & 0x0F) << 10)) & 0x3FFF)
        return width, height
    return None


def _asset_path(media_root: Path, name: str) -> Path:
    path = media_root / "assets" / name
    if path.parent != (media_root / "assets") or path.name != name:
        _fail(f"asset path is outside the media asset directory: {name}")
    return path


def _asset_entry(path: Path, **properties: object) -> dict[str, object]:
    return {"sha256": _sha256(path), "bytes": path.stat().st_size, **properties}


def _validate_png(path: Path, name: str, width: int, height: int) -> dict[str, object]:
    dimensions = _png_dimensions(path)
    if dimensions != (width, height):
        _fail(f"{name} is not a {width}x{height} PNG")
    return _asset_entry(path, width=width, height=height, media_type="image/png")


def _validate_poster(path: Path, dashboard: Path, width: int, height: int) -> dict[str, object]:
    if _webp_dimensions(path) != (width, height):
        _fail(f"dashboard poster is not a {width}x{height} WebP")
    if path.stat().st_size >= dashboard.stat().st_size:
        _fail("dashboard poster must be smaller than the dashboard PNG")
    return _asset_entry(path, width=width, height=height, media_type="image/webp")


def _validate_encoder_result(media_root: Path, spec: Mapping[str, Any]) -> dict[str, Any]:
    raw = _mapping(_read_json(media_root / "work" / "encoder-result.json", "encoder result"), "encoder result")
    _keys(raw, ENCODER_RESULT_KEYS, "encoder result")
    duration = _number(raw["duration_seconds"], "encoder duration_seconds")
    minimum_seconds = spec["video"]["minimum_seconds"]
    maximum_seconds = spec["video"]["maximum_seconds"]
    if not minimum_seconds <= duration <= maximum_seconds:
        _fail(f"encoded video duration is outside the approved {minimum_seconds}-{maximum_seconds} second range")
    if _string(raw["video_codec"], "encoder video_codec") != "h264":
        _fail("encoded video codec must be h264")
    viewport = cast("Mapping[str, Any]", spec["viewport"])
    video_width = _integer(raw["width"], "encoder width")
    video_height = _integer(raw["height"], "encoder height")
    if (video_width, video_height) != (viewport["width"], viewport["height"]):
        _fail(f"encoded video dimensions must be {viewport['width']}x{viewport['height']}")
    frame_rate = _number(raw["frame_rate"], "encoder frame_rate")
    if frame_rate != 30:
        _fail("encoded video frame rate must be 30")
    if _integer(raw["audio_stream_count"], "encoder audio_stream_count") != 0:
        _fail("encoded video must not contain an audio stream")
    if not _boolean(raw["captions_burned_in"], "encoder captions_burned_in"):
        _fail("encoded video does not contain burned captions")
    if _string(raw["caption_filter"], "encoder caption_filter") != "subtitles":
        _fail("encoded video was not produced with the subtitles filter")
    if _string(raw["caption_source"], "encoder caption_source") != "captions.srt":
        _fail("encoded video was not produced from captions.srt")
    if not _boolean(raw["webm_removed"], "encoder webm_removed"):
        _fail("raw WebM was not removed after successful encoding")
    video_name = cast("str", spec["video"]["name"])
    if (media_root / "work" / f"{video_name[:-4]}.webm").exists():
        _fail("raw WebM was not removed after successful encoding")
    return raw


def _request_is_local(url: str) -> bool:
    if (url.startswith("/") and not url.startswith("//")) or url.startswith("data:") or url.startswith("blob:"):
        return True
    parsed = urlsplit(url)
    return parsed.scheme == "http" and parsed.netloc == "chitragupta-ui"


def _validate_browser_observations(media_root: Path) -> dict[str, Any]:
    raw = _mapping(
        _read_json(media_root / "work" / "browser-observations.json", "browser observations"),
        "browser observations",
    )
    _keys(raw, BROWSER_OBSERVATION_KEYS, "browser observations")
    if _string(raw["ui_origin"], "browser observations.ui_origin") != "http://chitragupta-ui":
        _fail("browser UI origin is not the media UI origin")
    requests = raw["requests"]
    if not isinstance(requests, list) or not requests:
        _fail("browser observations.requests must be a non-empty list")
    for index, request_value in enumerate(requests):
        request = _mapping(request_value, f"browser observations.requests[{index}]")
        request_keys = set(request)
        if not request_keys.issubset({"url", "kind", "failed"}) or "url" not in request_keys:
            _fail(f"browser observations.requests[{index}] has invalid fields")
        url = _string(request["url"], f"browser observations.requests[{index}].url")
        if not _request_is_local(url):
            _fail(f"browser request escaped the media UI origin: {url}")
        if "kind" in request:
            _string(request["kind"], f"browser observations.requests[{index}].kind")
        failed = (
            _boolean(request["failed"], f"browser observations.requests[{index}].failed")
            if "failed" in request
            else False
        )
        if failed:
            _fail(f"browser request failed: {url}")
    if not _boolean(raw["api_identifiers_match_catalog"], "browser observations.api_identifiers_match_catalog"):
        _fail("browser API identifiers do not match the synthetic catalog")
    if not _boolean(raw["dom_identifiers_match_catalog"], "browser observations.dom_identifiers_match_catalog"):
        _fail("browser DOM identifiers do not match the synthetic catalog")
    return raw


def _validate_state(media_root: Path, spec: Mapping[str, Any]) -> dict[str, Any]:
    return _state_metadata(media_root / "state", spec)


def _validate_captions(media_root: Path, spec: Mapping[str, Any]) -> None:
    captions_path = media_root / "work" / "captions.srt"
    try:
        captions = captions_path.read_text(encoding="utf-8")
    except (OSError, UnicodeDecodeError) as exc:
        _fail(f"caption file is not readable: {exc}")
    if captions != _captions_srt(spec):
        _fail("caption file does not exactly match the capture specification")


def _validate_database_evidence(media_root: Path, spec: Mapping[str, Any]) -> tuple[bool, bool]:
    catalog = _mapping(
        _read_json(media_root / "work" / "synthetic-catalog.json", "synthetic catalog"),
        "synthetic catalog",
    )
    expected_catalog_keys = frozenset(
        {
            "schema_version",
            "anchor_date",
            "profile",
            "primary_tenant",
            "representative_identifiers",
            "source_identifiers",
            "tenants",
            "scenarios",
            "database_evidence",
        }
    )
    _keys(catalog, expected_catalog_keys, "synthetic catalog")
    if (
        _integer(catalog["schema_version"], "synthetic catalog.schema_version") != 1
        or catalog["anchor_date"] != spec["anchor_date"]
        or catalog["profile"] != spec["profile"]
        or catalog["primary_tenant"] != spec["primary_tenant"]
    ):
        _fail("synthetic catalog does not match the capture specification")
    source_identifiers = catalog["source_identifiers"]
    if not isinstance(source_identifiers, list) or any(not isinstance(item, str) for item in source_identifiers):
        _fail("synthetic catalog source_identifiers must be typed strings")
    if source_identifiers != sorted(set(source_identifiers)):
        _fail("synthetic catalog source_identifiers must be sorted and unique")
    scenario_identifiers = sorted(_source_identifiers(catalog["scenarios"]))
    if source_identifiers != scenario_identifiers:
        _fail("synthetic catalog source_identifiers do not match the typed scenario projection")
    evidence = _mapping(catalog["database_evidence"], "synthetic catalog.database_evidence")
    _keys(evidence, DATABASE_EVIDENCE_KEYS, "synthetic catalog.database_evidence")
    if _integer(evidence["schema_version"], "database evidence.schema_version") != 1:
        _fail("database evidence schema_version must be 1")
    if not _boolean(evidence["validated"], "database evidence.validated"):
        _fail("database evidence was not validated by the generator")
    metadata = _mapping(evidence["state_metadata"], "database evidence.state_metadata")
    _keys(metadata, STATE_METADATA_KEYS, "database evidence.state_metadata")
    persisted_state = _validate_state(media_root, spec)
    fresh_showcase_state = (
        metadata == persisted_state
        and metadata["profile"] == spec["profile"]
        and metadata["anchor_date"] == spec["anchor_date"]
    )
    if not fresh_showcase_state:
        _fail("database evidence state metadata does not match persisted state")
    files = evidence["files"]
    if not isinstance(files, list) or len(files) < 3:
        _fail("database evidence must include metadata and both tenant databases")
    seen_paths: set[str] = set()
    for index, value in enumerate(files):
        entry = _mapping(value, f"database evidence.files[{index}]")
        _keys(entry, DATABASE_FILE_KEYS, f"database evidence.files[{index}]")
        relative_path = _string(entry["path"], f"database evidence.files[{index}].path")
        path = Path(relative_path)
        if path.is_absolute() or path.name != relative_path or relative_path in seen_paths:
            _fail("database evidence paths must be unique state-local files")
        seen_paths.add(relative_path)
        actual_path = media_root / "state" / relative_path
        if not actual_path.is_file() or actual_path.is_symlink():
            _fail(f"database evidence file is missing: {relative_path}")
        if _string(entry["sha256"], f"database evidence.files[{index}].sha256") != _sha256(actual_path):
            _fail(f"database evidence hash does not match: {relative_path}")
        if _integer(entry["bytes"], f"database evidence.files[{index}].bytes") != actual_path.stat().st_size:
            _fail(f"database evidence byte count does not match: {relative_path}")
    database_matches_catalog = (
        bool(evidence["validated"])
        and "demo-state.json" in seen_paths
        and any(path.endswith(".db") for path in seen_paths)
    )
    if not database_matches_catalog:
        _fail("database evidence must include demo-state.json and tenant database files")
    return fresh_showcase_state, database_matches_catalog


def _asset_entries(
    media_root: Path,
    spec: Mapping[str, Any],
    encoder: Mapping[str, Any],
) -> dict[str, dict[str, object]]:
    assets = media_root / "assets"
    entries: dict[str, dict[str, object]] = {}
    viewport = cast("Mapping[str, Any]", spec["viewport"])
    viewport_width = _integer(viewport["width"], "viewport.width")
    viewport_height = _integer(viewport["height"], "viewport.height")
    poster = cast("Mapping[str, Any]", spec["poster"])
    poster_width = _integer(poster["width"], "poster.width")
    poster_height = _integer(poster["height"], "poster.height")
    screenshot_names = tuple(cast("str", screenshot["name"]) for screenshot in spec["screenshots"])
    for name in screenshot_names:
        path = _asset_path(media_root, name)
        if not path.is_file():
            _fail(f"missing required screenshot: {name}")
        entries[name] = _validate_png(path, name, viewport_width, viewport_height)
    dashboard = _asset_path(media_root, screenshot_names[0])
    poster_path = _asset_path(media_root, cast("str", poster["name"]))
    if not poster_path.is_file():
        _fail(f"missing required poster: {poster['name']}")
    entries[cast("str", poster["name"])] = _validate_poster(
        poster_path,
        dashboard,
        poster_width,
        poster_height,
    )
    video_name = cast("str", spec["video"]["name"])
    video = _asset_path(media_root, video_name)
    if not video.is_file():
        _fail(f"missing required walkthrough: {video_name}")
    entries[video_name] = _asset_entry(
        video,
        duration_seconds=encoder["duration_seconds"],
        video_codec=encoder["video_codec"],
        width=encoder["width"],
        height=encoder["height"],
        frame_rate=encoder["frame_rate"],
        audio_stream_count=encoder["audio_stream_count"],
        media_type="video/mp4",
    )
    if set(path.name for path in assets.iterdir() if path.is_file()) != set(_runtime_asset_names(spec)):
        _fail("media assets contain an unexpected or missing publish asset")
    return entries


def _validation_payload(
    *,
    observations: Mapping[str, Any],
    encoder: Mapping[str, Any],
    fresh_showcase_state: bool,
    database_matches_catalog: bool,
) -> dict[str, bool]:
    return {
        "fresh_showcase_state": fresh_showcase_state,
        "database_matches_catalog": database_matches_catalog,
        "api_identifiers_match_catalog": bool(observations["api_identifiers_match_catalog"]),
        "dom_identifiers_match_catalog": bool(observations["dom_identifiers_match_catalog"]),
        "browser_requests_local_only": True,
        "screenshots_are_1600x900_png": True,
        "poster_is_960x540_webp": True,
        "video_duration_in_range": True,
        "video_has_no_audio": True,
        "captions_burned_in": bool(encoder["captions_burned_in"]),
        "webm_removed": bool(encoder["webm_removed"]),
        "complete": True,
    }


def _validate_manifest_payload(
    raw: object,
    spec: Mapping[str, Any],
    media_root: Path,
    *,
    require_clean: bool,
) -> dict[str, Any]:
    manifest = _mapping(raw, "media manifest")
    _keys(manifest, MANIFEST_KEYS, "media manifest")
    if _integer(manifest["schema_version"], "manifest.schema_version") != 1:
        _fail("media manifest schema_version must be 1")
    source_commit = _string(manifest["source_commit"], "manifest.source_commit")
    if COMMIT_RE.fullmatch(source_commit) is None:
        _fail("media manifest source_commit must be a 40-character lowercase SHA")
    source_clean = _boolean(manifest["source_worktree_clean"], "manifest.source_worktree_clean")
    if require_clean and not source_clean:
        _fail("media manifest was captured from a dirty worktree")
    if manifest["anchor_date"] != spec["anchor_date"] or manifest["profile"] != spec["profile"]:
        _fail("media manifest does not match the committed capture specification")
    if manifest["primary_tenant"] != spec["primary_tenant"]:
        _fail("media manifest primary tenant does not match the capture specification")
    expected_assets = manifest["expected_assets"]
    if not isinstance(expected_assets, list) or expected_assets != list(APPROVED_ASSETS):
        _fail("media manifest expected_assets is not the exact approved asset set")

    validation = _mapping(manifest["validation"], "manifest.validation")
    if set(validation) != set(VALIDATION_FIELDS):
        _fail("media manifest validation flags are incomplete")
    if any(not _boolean(value, f"manifest.validation.{key}") for key, value in validation.items()):
        _fail("media manifest validation is incomplete")

    raw_assets = _mapping(manifest["assets"], "manifest.assets")
    if set(raw_assets) != set(APPROVED_ASSETS):
        _fail("media manifest assets do not match the exact approved asset set")
    _validate_browser_observations(media_root)
    encoder = _validate_encoder_result(media_root, spec)
    _validate_captions(media_root, spec)
    fresh_showcase_state, database_matches_catalog = _validate_database_evidence(media_root, spec)
    if validation["fresh_showcase_state"] is not fresh_showcase_state:
        _fail("media manifest fresh_showcase_state evidence does not match persisted state")
    if validation["database_matches_catalog"] is not database_matches_catalog:
        _fail("media manifest database_matches_catalog evidence does not match the catalog")
    actual_assets = _asset_entries(media_root, spec, encoder)
    for name in APPROVED_ASSETS:
        entry = _mapping(raw_assets[name], f"manifest.assets.{name}")
        actual = actual_assets[name]
        if set(entry) != set(actual):
            _fail(f"media manifest asset properties are incomplete for {name}")
        for key, value in actual.items():
            if entry[key] != value:
                _fail(f"media manifest asset evidence does not match {name}")
        if not isinstance(entry["sha256"], str) or SHA256_RE.fullmatch(entry["sha256"]) is None:
            _fail(f"media manifest asset hash is invalid for {name}")
        if type(entry["bytes"]) is not int or entry["bytes"] < 1:
            _fail(f"media manifest asset byte count is invalid for {name}")
    return manifest


def create_manifest(
    spec_path: Path,
    media_root: Path,
    source_commit: str,
    source_worktree_clean: bool,
) -> None:
    """Validate generated media and atomically create its final manifest."""
    spec = load_spec(spec_path)
    if COMMIT_RE.fullmatch(source_commit) is None:
        _fail("source commit must be a 40-character lowercase SHA")
    observations = _validate_browser_observations(media_root)
    encoder = _validate_encoder_result(media_root, spec)
    _validate_captions(media_root, spec)
    fresh_showcase_state, database_matches_catalog = _validate_database_evidence(media_root, spec)
    assets = _asset_entries(media_root, spec, encoder)
    manifest = {
        "schema_version": 1,
        "source_commit": source_commit,
        "source_worktree_clean": source_worktree_clean,
        "anchor_date": spec["anchor_date"],
        "profile": spec["profile"],
        "primary_tenant": spec["primary_tenant"],
        "expected_assets": list(_runtime_asset_names(spec)),
        "assets": assets,
        "validation": _validation_payload(
            observations=observations,
            encoder=encoder,
            fresh_showcase_state=fresh_showcase_state,
            database_matches_catalog=database_matches_catalog,
        ),
    }
    _validate_manifest_payload(manifest, spec, media_root, require_clean=False)
    _write_json(media_root / "manifest.json", manifest)


def validate_manifest(
    spec_path: Path,
    media_root: Path,
    publication_arguments_output: Path | None = None,
) -> None:
    """Revalidate a complete, clean media capture without changing it."""
    spec = load_spec(spec_path)
    manifest = _read_json(media_root / "manifest.json", "media manifest")
    _validate_manifest_payload(manifest, spec, media_root, require_clean=True)
    if publication_arguments_output is not None:
        _write_argument_vector(publication_arguments_output, _publication_arguments(spec))
    print(json.dumps({"manifest": str(media_root / "manifest.json"), "valid": True}))


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)

    spec_parser = subparsers.add_parser("spec")
    spec_parser.add_argument("--spec", type=Path, required=True)

    catalog_parser = subparsers.add_parser("catalog")
    catalog_parser.add_argument("--spec", type=Path, required=True)
    catalog_parser.add_argument("--config", type=Path, required=True)
    catalog_parser.add_argument("--output", type=Path, required=True)
    catalog_parser.add_argument("--srt-output", type=Path, required=True)
    catalog_parser.add_argument("--encoder-arguments-output", type=Path, required=True)
    catalog_parser.add_argument("--state-dir", type=Path)

    manifest_parser = subparsers.add_parser("manifest")
    manifest_parser.add_argument("--spec", type=Path, required=True)
    manifest_parser.add_argument("--media-root", type=Path, required=True)
    manifest_parser.add_argument("--source-commit", required=True)
    manifest_parser.add_argument("--source-worktree-clean", choices=("true", "false"), required=True)

    validate_parser = subparsers.add_parser("validate")
    validate_parser.add_argument("--spec", type=Path, required=True)
    validate_parser.add_argument("--media-root", type=Path, required=True)
    validate_parser.add_argument("--publication-arguments-output", type=Path)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    """Run one media preparation or validation command."""
    args = _parser().parse_args(argv)
    try:
        if args.command == "spec":
            spec = load_spec(args.spec)
            print(json.dumps({"profile": spec["profile"], "anchor_date": spec["anchor_date"]}))
        elif args.command == "catalog":
            build_catalog(
                args.spec,
                args.config,
                args.output,
                args.srt_output,
                args.encoder_arguments_output,
                args.state_dir,
            )
        elif args.command == "manifest":
            create_manifest(args.spec, args.media_root, args.source_commit, args.source_worktree_clean == "true")
        elif args.command == "validate":
            validate_manifest(args.spec, args.media_root, args.publication_arguments_output)
        else:
            _fail(f"unsupported media command: {args.command}")
    except MediaError as exc:
        print(f"Demo media validation failed: {exc}", file=sys.stderr)
        return 1
    except (OSError, RuntimeError, ValueError, TypeError) as exc:
        print(f"Demo media validation failed: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
