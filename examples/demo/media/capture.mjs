#!/usr/bin/env node

import { access, mkdir, readFile, rename, unlink, writeFile } from "node:fs/promises";
import path from "node:path";
import { pathToFileURL } from "node:url";

const UI_ORIGIN = "http://chitragupta-ui";
const APPROVED_VIEWPORT = { width: 1600, height: 900 };
const APPROVED_SCREENSHOTS = [
  {
    name: "chitragupta-demo-dashboard.png",
    route: "/dashboard",
  },
  {
    name: "chitragupta-demo-cost-explorer.png",
    route: "/explorer",
  },
  {
    name: "chitragupta-demo-topic-attribution.png",
    route: "/topic-attributions",
  },
  {
    name: "chitragupta-demo-pipeline-status.png",
    route: "/pipeline",
  },
  {
    name: "chitragupta-demo-focus-mapping-preview.png",
    route: "/focus-preview",
  },
];
const APPROVED_VIDEO_NAME = "chitragupta-demo-walkthrough.mp4";
const APPROVED_VIDEO = {
  name: APPROVED_VIDEO_NAME,
  content_height: 800,
  caption_band_height: 100,
  playback_width: 960,
  playback_height: 540,
  content_zoom_percent: 150,
  caption_font_size: 32,
};
const APPROVED_FULL_STORYBOARD = [
  {
    id: "dashboard-summary",
    route: "/dashboard",
    caption: "Synthetic Showcase, Aug 2–31: $600,362 total; shared cost is larger than usage cost.",
    read_seconds: 3,
    max_action_seconds: 3,
  },
  {
    id: "dashboard-cost-trend",
    route: "/dashboard",
    caption: "The 30-day trend gives us a concrete cost change to investigate.",
    read_seconds: 3,
    max_action_seconds: 3,
  },
  {
    id: "explorer-commerce",
    route: "/explorer",
    caption: "At the Aug 31 snapshot, focus the Commerce environment.",
    read_seconds: 3,
    max_action_seconds: 8,
  },
  {
    id: "explorer-customer-kafka",
    route: "/explorer",
    caption: "In the same snapshot, follow Commerce to Customer Kafka.",
    read_seconds: 3,
    max_action_seconds: 7,
  },
  {
    id: "topic-topics",
    route: "/topic-attributions",
    caption: "Back in the 30-day cost range, showcase-live-orders is the largest topic.",
    read_seconds: 3,
    max_action_seconds: 6,
  },
  {
    id: "topic-filters",
    route: "/topic-attributions",
    caption: "Scope the 30-day view to Customer Kafka and showcase-live-orders.",
    read_seconds: 3,
    max_action_seconds: 7,
  },
  {
    id: "topic-composition",
    route: "/topic-attributions",
    caption: "This topic costs $79,000 across the 30-day range, all from REST produce cost.",
    read_seconds: 3,
    max_action_seconds: 2,
  },
  {
    id: "topic-movers",
    route: "/topic-attributions",
    caption: "The top-movers view shows a $49,000 cost increase on Aug 31.",
    read_seconds: 4,
    max_action_seconds: 3,
  },
  {
    id: "topic-table",
    route: "/topic-attributions",
    caption: "Narrow to Aug 30–31: the table shows $50,000 on Aug 31 and $1,000 on Aug 30.",
    read_seconds: 4,
    max_action_seconds: 8,
  },
  {
    id: "pipeline-status",
    route: "/pipeline",
    caption: "Review the completed pipeline run and daily processing status.",
    read_seconds: 3,
    max_action_seconds: 5,
  },
  {
    id: "focus-export",
    route: "/focus-preview",
    caption: "Review the August FOCUS preview and download options.",
    read_seconds: 3,
    max_action_seconds: 7,
  },
];
const APPROVED_DRAFT_STORYBOARD = [
  {
    id: "topic-filters",
    route: "/topic-attributions",
    caption: "Scope the 30-day synthetic view to Customer Kafka and showcase-live-orders.",
    read_seconds: 3,
    max_action_seconds: 6,
  },
  {
    id: "topic-composition",
    route: "/topic-attributions",
    caption: "This topic costs $79,000 across the 30-day range, all from REST produce cost.",
    read_seconds: 3,
    max_action_seconds: 1.9,
  },
  {
    id: "topic-movers",
    route: "/topic-attributions",
    caption: "On Aug 31, cost rises by $49,000 from the previous day.",
    read_seconds: 3,
    max_action_seconds: 2.9,
  },
];
const APPROVED_STORYBOARDS = { full: APPROVED_FULL_STORYBOARD, draft: APPROVED_DRAFT_STORYBOARD };
const APPROVED_OUTPUT_PATHS = {
  full: "assets/chitragupta-demo-walkthrough.mp4",
  draft: "review/chitragupta-demo-investigation-draft.mp4",
};
const APPROVED_SPEED = 1.15;
const MARKER_COLOR = "#00ff00";
const MARKER_DURATION_SECONDS = 0.4;
const MARKER_FRAMES = 12;
const SCROLL_DURATION_SECONDS = 0.9;
const POINTER_MOVE_SECONDS = 0.7;
const POINTER_CLICK_SECONDS = 0.3;
const TYPE_DELAY_SECONDS = 0.04;
const FRAME_TOLERANCE_SECONDS = 2 / 30;
const SCROLL_REQUIRED_SCENES = new Set([
  "dashboard-cost-trend",
  "topic-composition",
  "topic-movers",
  "topic-table",
  "pipeline-status",
  "focus-export",
]);
const SCENE_MARKERS = [
  { markers: ["Cost Dashboard", "Total Cost", "Usage Cost", "Shared Cost", "Cost Trend Over Time"] },
  { markers: ["Cost Explorer"] },
  { markers: ["Topic Attribution"] },
  { markers: ["Pipeline Status", "Last Run Summary", "Per-Date Processing Status"] },
  { markers: ["FOCUS Mapping Preview"] },
];
// These fields are generated for the asynchronous FOCUS Preview workflow and
// are intentionally excluded from the persisted source-identifier allowlist.
const RUNTIME_PREVIEW_ID_KEYS = new Set([
  "request_id",
  "revision_id",
  "supersedes_revision_id",
  "superseded_by_revision_id",
  "calculation_id",
  "artifact_id",
  "storage_key",
]);
const RUNTIME_PREVIEW_ID_FORMATS = new Map([
  [
    "request_id",
    /^(?:[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}|revision-generation-[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12})$/i,
  ],
  ["revision_id", /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i],
  ["supersedes_revision_id", /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i],
  ["superseded_by_revision_id", /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i],
  ["calculation_id", /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i],
  ["artifact_id", /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i],
  ["storage_key", /^v1-[0-9a-f]{64}-[0-9a-f]{32}$/],
]);
const NULLABLE_RUNTIME_PREVIEW_ID_KEYS = new Set([
  "supersedes_revision_id",
  "superseded_by_revision_id",
  "calculation_id",
  "artifact_id",
  "storage_key",
]);
const NON_IDENTIFIER_TOKENS = new Set(["topic-level"]);
const SCENE_QUIET_PERIOD_MS = 250;
const SCENE_QUIESCENCE_TIMEOUT_MS = 45_000;
const APPROVED_DASHBOARD_AMOUNTS = {
  "Total Cost": "$600,362.00",
  "Usage Cost": "$249,532.50",
  "Shared Cost": "$350,829.50",
};
const APPROVED_PIPELINE_RUN = {
  completedAt: "2026-08-31T23:59:59Z",
  datesGathered: "184",
  datesCalculated: "184",
  chargebackRowsWritten: "11960",
};

/** @typedef {"full" | "draft"} CaptureMode */

/**
 * @typedef {Object} CaptureViewport
 * @property {number} width
 * @property {number} height
 */

/**
 * @typedef {Object} CapturePoster
 * @property {number} width
 * @property {number} height
 * @property {string} name
 */

/**
 * @typedef {Object} CaptureVideoSettings
 * @property {string} name
 * @property {number} content_height
 * @property {number} caption_band_height
 * @property {number} playback_width
 * @property {number} playback_height
 * @property {number} content_zoom_percent
 * @property {number} caption_font_size
 */

/**
 * @typedef {Object} CaptureTenant
 * @property {string} name
 * @property {string} id
 * @property {string} ecosystem
 */

/**
 * @typedef {Object} CaptureScreenshot
 * @property {string} name
 * @property {string} route
 * @property {string[]} [markers]
 */

/**
 * @typedef {Object} CaptureStoryboardScene
 * @property {string} id
 * @property {string} route
 * @property {string} caption
 * @property {number} read_seconds
 * @property {number} max_action_seconds
 * @property {string[]} [markers]
 */

/**
 * @typedef {Object} CaptureStoryboard
 * @property {string} output_path
 * @property {number} maximum_seconds
 * @property {number} [minimum_seconds]
 * @property {CaptureStoryboardScene[]} scenes
 */

/** @typedef {CaptureStoryboardScene | CaptureScreenshot} CaptureRouteScene */

/**
 * @typedef {Object} CaptureSpec
 * @property {number} schema_version
 * @property {string} anchor_date
 * @property {string} profile
 * @property {CaptureViewport} viewport
 * @property {CapturePoster} poster
 * @property {CaptureVideoSettings} video
 * @property {CaptureTenant} primary_tenant
 * @property {CaptureScreenshot[]} screenshots
 * @property {{full: CaptureStoryboard, draft: CaptureStoryboard}} storyboards
 */

/**
 * The catalog is the JSON projection produced by media-tool. The capture
 * runner only needs its source allowlist and scenario values; other catalog
 * evidence is intentionally opaque to this browser boundary.
 * @typedef {Object} CaptureCatalog
 * @property {string[]} source_identifiers
 * @property {unknown[]} scenarios
 */

/**
 * @typedef {Object} CaptureWaitOptions
 * @property {number} [timeout]
 * @property {"commit" | "load" | "domcontentloaded" | "networkidle"} [waitUntil]
 * @property {"visible" | "hidden" | "attached" | "detached"} [state]
 */

/**
 * @typedef {Object} CaptureLocatorQueryOptions
 * @property {string} [name]
 * @property {boolean} [exact]
 */

/** @typedef {string | RegExp} CaptureTextMatcher */

/**
 * @typedef {Object} CaptureBoundingBox
 * @property {number} x
 * @property {number} y
 * @property {number} width
 * @property {number} height
 */

/**
 * @typedef {Object} CaptureScrollSample
 * @property {number} elapsed_seconds
 * @property {number} offset
 */

/**
 * @typedef {Object} CaptureSceneEvidence
 * @property {string} [topicTooltip]
 * @property {string} [topicTooltipText]
 * @property {string} [moversTooltip]
 * @property {[string, string][]} [tableRows]
 */

/**
 * @typedef {Object} CaptureSceneActionResult
 * @property {CaptureScrollSample[]} scrollSamples
 * @property {CaptureSceneEvidence} [evidence]
 */

/**
 * @typedef {Object} CaptureTimelineScene
 * @property {string} id
 * @property {number} start_seconds
 * @property {number} action_complete_seconds
 * @property {number} end_seconds
 * @property {CaptureScrollSample[]} scroll_samples
 */

/**
 * @typedef {Object} CaptureMarkerRun
 * @property {number} duration_seconds
 * @property {number} frames
 */

/**
 * @typedef {Object} CaptureTimelineMarkers
 * @property {string} color
 * @property {number} plane_average_tolerance
 * @property {number} within_plane_spread
 * @property {[CaptureMarkerRun, CaptureMarkerRun]} runs
 */

/**
 * @typedef {Object} CaptureTimeline
 * @property {CaptureMode} mode
 * @property {number} speed
 * @property {CaptureTimelineMarkers} markers
 * @property {CaptureTimelineScene[]} scenes
 * @property {{zoom_percent: number, minimum_playback_text_pixels: number, minimum_playback_target_pixels: number}} framing
 * @property {{topic_tooltip: string, movers_tooltip: string, table_rows: [string, string][]}} [evidence]
 */

/**
 * @typedef {Object} CaptureRequestFailure
 * @property {string} errorText
 */

/**
 * @typedef {Object} CaptureObservation
 * @property {string} url
 * @property {string} kind
 * @property {boolean} [failed]
 */

/**
 * @typedef {Object} CaptureRequest
 * @property {() => string} url
 * @property {() => string} resourceType
 * @property {() => string} method
 * @property {() => string | null} postData
 * @property {(() => CaptureRequestFailure | null)} [failure]
 */

/**
 * @typedef {Object} CaptureResponse
 * @property {() => boolean} ok
 * @property {() => number} status
 * @property {() => string} url
 * @property {() => Record<string, string>} headers
 * @property {() => Promise<unknown>} json
 * @property {() => CaptureRequest} request
 */

/**
 * @typedef {Object} CapturePageError
 * @property {string} message
 */

/**
 * @typedef {Object} CaptureConsoleMessage
 * @property {() => string} type
 * @property {() => string} text
 */

/**
 * @typedef {Object} CapturePageEventMap
 * @property {(request: CaptureRequest) => void} request
 * @property {(request: CaptureRequest) => void} requestfinished
 * @property {(request: CaptureRequest) => void} requestfailed
 * @property {(response: CaptureResponse) => void} response
 * @property {(error: CapturePageError) => void} pageerror
 * @property {(message: CaptureConsoleMessage) => void} console
 */

/**
 * @typedef {<Event extends keyof CapturePageEventMap>(event: Event, listener: CapturePageEventMap[Event]) => void} CapturePageOn
 */

/**
 * @typedef {<Result, Argument>(callback: (argument: Argument) => Result, argument?: Argument) => Promise<Result>} CapturePageEvaluate
 */

/**
 * @typedef {<Result, Argument>(callback: (argument: Argument) => Result, argument?: Argument, options?: CaptureWaitOptions) => Promise<Result>} CapturePageWaitForFunction
 */

/**
 * @typedef {<Result, Argument>(callback: (element: Element, argument: Argument) => Result, argument?: Argument) => Promise<Result>} CaptureLocatorEvaluate
 */

/**
 * @typedef {Object} CaptureLocator
 * @property {(options?: CaptureWaitOptions) => Promise<void>} waitFor
 * @property {() => Promise<number>} count
 * @property {() => Promise<boolean>} isVisible
 * @property {() => Promise<boolean>} isDisabled
 * @property {() => Promise<CaptureBoundingBox | null>} boundingBox
 * @property {() => Promise<string>} innerText
 * @property {() => Promise<string | null>} textContent
 * @property {() => Promise<string[]>} allTextContents
 * @property {(value: string) => Promise<void>} fill
 * @property {(value: string, options?: {delay?: number}) => Promise<void>} pressSequentially
 * @property {(key: string) => Promise<void>} press
 * @property {(options?: {timeout?: number}) => Promise<void>} click
 * @property {(options?: {timeout?: number}) => Promise<void>} hover
 * @property {(selector: string) => CaptureLocator} locator
 * @property {(text: CaptureTextMatcher, options?: CaptureLocatorQueryOptions) => CaptureLocator} getByText
 * @property {(role: string, options?: CaptureLocatorQueryOptions) => CaptureLocator} getByRole
 * @property {(label: string, options?: CaptureLocatorQueryOptions) => CaptureLocator} getByLabel
 * @property {(placeholder: string, options?: CaptureLocatorQueryOptions) => CaptureLocator} getByPlaceholder
 * @property {(testId: string) => CaptureLocator} getByTestId
 * @property {() => CaptureLocator} first
 * @property {() => CaptureLocator} last
 * @property {(index: number) => CaptureLocator} nth
 * @property {CaptureLocatorEvaluate} evaluate
 */

/**
 * @typedef {Object} CaptureMouse
 * @property {(x: number, y: number) => Promise<void>} move
 */

/**
 * @typedef {Object} CaptureVideo
 * @property {() => Promise<string>} path
 */

/**
 * @typedef {Object} CaptureCytoscapeNode
 * @property {() => boolean} isNode
 * @property {() => unknown} position
 */

/**
 * @typedef {(() => number) & ((options: {level: number, position: unknown}) => void)} CaptureCytoscapeZoom
 */

/**
 * @typedef {Object} CaptureCytoscape
 * @property {(id: string) => CaptureCytoscapeNode | null} getElementById
 * @property {() => number} maxZoom
 * @property {CaptureCytoscapeZoom} zoom
 * @property {(clearQueue: boolean) => void} stop
 * @property {(node: CaptureCytoscapeNode) => void} center
 * @property {(event: string, listener: () => void) => void} one
 */

/**
 * @typedef {Object} CapturePage
 * @property {CapturePageOn} on
 * @property {(url: string, options?: CaptureWaitOptions) => Promise<CaptureResponse | null>} goto
 * @property {(predicate: (response: CaptureResponse) => boolean, options?: CaptureWaitOptions) => Promise<CaptureResponse>} waitForResponse
 * @property {(predicate: (url: URL | string) => boolean, options?: CaptureWaitOptions) => Promise<void>} waitForURL
 * @property {CapturePageEvaluate} evaluate
 * @property {CapturePageWaitForFunction} waitForFunction
 * @property {(milliseconds: number) => Promise<void>} waitForTimeout
 * @property {(options: {content: string}) => Promise<unknown>} addStyleTag
 * @property {(options: {path: string, fullPage?: boolean}) => Promise<void>} screenshot
 * @property {() => CaptureVideo | null} video
 * @property {CaptureMouse} mouse
 * @property {(selector: string) => CaptureLocator} locator
 * @property {(text: CaptureTextMatcher, options?: CaptureLocatorQueryOptions) => CaptureLocator} getByText
 * @property {(role: string, options?: CaptureLocatorQueryOptions) => CaptureLocator} getByRole
 * @property {(label: string, options?: CaptureLocatorQueryOptions) => CaptureLocator} getByLabel
 * @property {(placeholder: string, options?: CaptureLocatorQueryOptions) => CaptureLocator} getByPlaceholder
 * @property {(testId: string) => CaptureLocator} getByTestId
 */

/**
 * @typedef {Object} CaptureBrowserContextOptions
 * @property {CaptureViewport} [viewport]
 * @property {{dir: string, size: CaptureViewport}} [recordVideo]
 * @property {string} [timezoneId]
 * @property {string} [locale]
 * @property {"dark" | "light"} [colorScheme]
 * @property {"reduce" | "no-preference"} [reducedMotion]
 * @property {{cookies: unknown[], origins: unknown[]}} [storageState]
 */

/**
 * @typedef {Object} CaptureBrowserContext
 * @property {(options: string) => Promise<void>} addInitScript
 * @property {() => Promise<CapturePage>} newPage
 * @property {() => Promise<void>} close
 */

/**
 * @typedef {Object} CaptureBrowser
 * @property {(options?: CaptureBrowserContextOptions) => Promise<CaptureBrowserContext>} newContext
 * @property {() => Promise<void>} close
 */

/**
 * @typedef {Object} CaptureRunInput
 * @property {CaptureSpec} captureSpec
 * @property {CaptureMode} mode
 * @property {CaptureBrowser} browser
 * @property {string} mediaRoot
 * @property {CaptureCatalog} catalog
 */

/**
 * @typedef {Object} CaptureRunResult
 * @property {CaptureMode} mode
 * @property {string} timelinePath
 * @property {string} rawVideoPath
 * @property {string} outputPath
 */

/**
 * @typedef {Object} CaptureStoryboardResult
 * @property {CaptureTimeline} timeline
 * @property {string} rawVideoPath
 */

/**
 * @param {string} message
 * @returns {never}
 */
function fail(message) {
  throw new Error(message);
}

/**
 * @param {unknown} value
 * @returns {value is Record<string, unknown>}
 */
function isRecord(value) {
  return Boolean(value && typeof value === "object" && !Array.isArray(value));
}

/**
 * @param {unknown} value
 * @param {readonly string[]} expected
 * @returns {boolean}
 */
function hasExactKeys(value, expected) {
  if (!value || typeof value !== "object" || Array.isArray(value)) return false;
  const actual = Object.keys(value).sort();
  const sortedExpected = [...expected].sort();
  return actual.length === sortedExpected.length && actual.every((key, index) => key === sortedExpected[index]);
}

/**
 * @param {string[]} argv
 * @returns {{spec: string, catalog: string, output: string, mode: CaptureMode}}
 */
function parseArguments(argv) {
  /** @type {{spec?: string, catalog?: string, output?: string, mode?: CaptureMode}} */
  const args = {};
  for (let index = 0; index < argv.length; index += 1) {
    const argument = argv[index];
    if (!["--spec", "--catalog", "--output", "--mode"].includes(argument)) {
      fail(`unknown argument: ${argument}`);
    }
    if (index + 1 >= argv.length) {
      fail(`missing value for ${argument}`);
    }
    args[argument.slice(2)] = argv[index + 1];
    index += 1;
  }
  if (!args.spec || !args.catalog || !args.output || !["full", "draft"].includes(args.mode)) {
    fail("usage: capture.mjs --spec SPEC_PATH --catalog CATALOG_PATH --output MEDIA_ROOT --mode full|draft");
  }
  return /** @type {{spec: string, catalog: string, output: string, mode: CaptureMode}} */ (args);
}

/**
 * @param {string} filename
 * @param {string} label
 * @returns {Promise<unknown>}
 */
async function readJson(filename, label) {
  try {
    return JSON.parse(await readFile(filename, "utf8"));
  } catch (error) {
    fail(`${label} is not valid JSON: ${error.message}`);
  }
}

/**
 * @param {CaptureSpec} spec
 * @returns {CaptureSpec}
 */
function assertSpecification(spec) {
  const baseKeys = [
    "schema_version",
    "anchor_date",
    "profile",
    "viewport",
    "poster",
    "video",
    "primary_tenant",
    "screenshots",
    "storyboards",
  ];
  if (
    !hasExactKeys(spec, baseKeys) ||
    spec.schema_version !== 2 ||
    spec.anchor_date !== "2026-08-31" ||
    spec.profile !== "showcase" ||
    spec.viewport?.width !== APPROVED_VIEWPORT.width ||
    spec.viewport?.height !== APPROVED_VIEWPORT.height ||
    spec.primary_tenant?.name !== "clean-confluent" ||
    spec.primary_tenant?.id !== "northstar-confluent" ||
    spec.primary_tenant?.ecosystem !== "confluent_cloud"
  ) {
    fail("capture specification is not the approved Showcase input");
  }
  if (
    !hasExactKeys(spec.viewport, ["width", "height"]) ||
    !hasExactKeys(spec.poster, ["width", "height", "name"]) ||
    spec.poster.width !== 960 ||
    spec.poster.height !== 540 ||
    spec.poster.name !== "chitragupta-demo-dashboard-poster.webp" ||
    !hasExactKeys(spec.video, Object.keys(APPROVED_VIDEO)) ||
    Object.entries(APPROVED_VIDEO).some(([key, value]) => spec.video?.[key] !== value) ||
    !hasExactKeys(spec.primary_tenant, ["name", "id", "ecosystem"])
  ) {
    fail("capture specification is not the approved Showcase input");
  }
  if (!Array.isArray(spec.screenshots) || spec.screenshots.length !== APPROVED_SCREENSHOTS.length) {
    fail("capture specification does not contain the approved storyboard");
  }
  if (
    spec.screenshots.some(
      (screenshot, index) =>
        !hasExactKeys(screenshot, ["name", "route"]) ||
        screenshot.name !== APPROVED_SCREENSHOTS[index].name ||
        screenshot.route !== APPROVED_SCREENSHOTS[index].route,
    )
  ) {
    fail("capture specification does not contain the approved storyboard");
  }
  if (!hasExactKeys(spec.storyboards, ["full", "draft"])) {
    fail("capture specification does not contain the approved storyboard");
  }
  for (const [mode, approvedScenes] of Object.entries(APPROVED_STORYBOARDS)) {
    const storyboard = spec.storyboards[mode];
    const expectedKeys = mode === "full" ? ["output_path", "maximum_seconds", "scenes"] : ["output_path", "minimum_seconds", "maximum_seconds", "scenes"];
    if (!hasExactKeys(storyboard, expectedKeys)) {
      fail("capture specification does not contain the approved storyboard");
    }
    const expectedOutput = APPROVED_OUTPUT_PATHS[mode];
    if (
      storyboard.output_path !== expectedOutput ||
      storyboard.maximum_seconds !== (mode === "full" ? 90 : 20) ||
      (mode === "draft" && storyboard.minimum_seconds !== 15) ||
      !Array.isArray(storyboard.scenes) ||
      storyboard.scenes.length !== approvedScenes.length
    ) {
      fail("capture specification does not contain the approved storyboard");
    }
    for (const [index, approved] of approvedScenes.entries()) {
      const scene = storyboard.scenes[index];
      if (
        !hasExactKeys(scene, ["id", "route", "caption", "read_seconds", "max_action_seconds"]) ||
        scene.id !== approved.id ||
        scene.route !== approved.route ||
        scene.caption !== approved.caption ||
        scene.read_seconds !== approved.read_seconds ||
        scene.max_action_seconds !== approved.max_action_seconds
      ) {
        fail("capture specification does not contain the approved storyboard");
      }
    }
  }
  return spec;
}

/**
 * @param {unknown} value
 * @param {Set<string>} [target]
 * @returns {Set<string>}
 */
function collectStrings(value, target = new Set()) {
  if (typeof value === "string") {
    target.add(value);
  } else if (Array.isArray(value)) {
    value.forEach((item) => collectStrings(item, target));
  } else if (value && typeof value === "object") {
    Object.values(value).forEach((item) => collectStrings(item, target));
  }
  return target;
}

/**
 * @param {unknown} value
 * @param {Set<string>} [target]
 * @returns {Set<string>}
 */
function collectTopicNames(value, target = new Set()) {
  if (Array.isArray(value)) {
    value.forEach((item) => collectTopicNames(item, target));
    return target;
  }
  if (value && typeof value === "object") {
    for (const [key, item] of Object.entries(value)) {
      if (key === "topic_name" && typeof item === "string" && item) {
        target.add(item);
      }
      collectTopicNames(item, target);
    }
  }
  return target;
}

/**
 * @param {unknown} value
 * @param {Set<string>} [target]
 * @param {Set<string>} [invalid]
 * @param {ReadonlySet<string>} [catalogStrings]
 * @returns {Set<string>}
 */
function collectRuntimePreviewIds(value, target = new Set(), invalid = new Set(), catalogStrings = new Set()) {
  if (Array.isArray(value)) {
    value.forEach((item) => collectRuntimePreviewIds(item, target, invalid, catalogStrings));
    return target;
  }
  if (value && typeof value === "object") {
    for (const [key, item] of Object.entries(value)) {
      if (RUNTIME_PREVIEW_ID_KEYS.has(key)) {
        const format = RUNTIME_PREVIEW_ID_FORMATS.get(key);
        const isCatalogCalculation =
          key === "calculation_id" && typeof item === "string" && catalogStrings.has(item);
        if (isCatalogCalculation) {
          // Persisted scenario calculation identifiers remain source evidence.
        } else if (item === null && NULLABLE_RUNTIME_PREVIEW_ID_KEYS.has(key)) {
          // Optional Preview fields are allowed to be absent.
        } else if (typeof item === "string" && format?.test(item)) {
          target.add(item);
        } else {
          invalid.add(`${key}=${JSON.stringify(item)}`);
        }
      }
      collectRuntimePreviewIds(item, target, invalid, catalogStrings);
    }
  }
  return target;
}

/**
 * @param {unknown} value
 * @param {ReadonlySet<string>} [runtimePreviewIds]
 * @returns {string[]}
 */
function sourceIdentifierTokens(value, runtimePreviewIds = new Set()) {
  let text;
  if (typeof value === "string") {
    text = value;
  } else if (value && typeof value === "object") {
    text = JSON.stringify(value);
  } else {
    text = JSON.stringify(value);
  }
  const tokens =
    text.match(
      /\b(?:[a-z0-9-]+:topic:[a-z0-9][a-z0-9._-]*|[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}|clean-[a-z0-9-]+|northstar-[a-z0-9-]+|env-[a-z0-9-]+|lkc-[a-z0-9-]+|cluster-[a-z0-9-]+|topic-[a-z0-9.-]+|sa-[a-z0-9-]+|user-[a-z0-9-]+|idp-[a-z0-9-]+|lsrc-[a-z0-9-]+|lfcp-[a-z0-9-]+|pool-[a-z0-9-]+|lcc-[a-z0-9-]+|clcc-[a-z0-9-]+|lksql-[a-z0-9-]+|lfstmt-[a-z0-9-]+|key-[a-z0-9-]+)\b/gi,
    ) ?? [];
  return tokens.filter(
    (token) => !NON_IDENTIFIER_TOKENS.has(token.toLowerCase()) && !runtimePreviewIds.has(token),
  );
}

/**
 * @param {CaptureRouteScene} scene
 * @returns {string}
 */
function scenePath(scene) {
  return scene.route.split("?", 1)[0];
}

/**
 * @param {string} anchorDate
 * @param {number} days
 * @returns {string}
 */
function dateDaysBefore(anchorDate, days) {
  const date = new Date(`${anchorDate}T00:00:00Z`);
  date.setUTCDate(date.getUTCDate() - days);
  return date.toISOString().slice(0, 10);
}

/**
 * @param {CaptureRouteScene} scene
 * @param {string} anchorDate
 * @returns {string}
 */
function sceneUrl(scene, anchorDate) {
  const url = new URL(`${UI_ORIGIN}${scene.route}`);
  const route = scenePath(scene);
  if (route === "/dashboard" || route === "/topic-attributions") {
    url.searchParams.set("start_date", dateDaysBefore(anchorDate, 29));
    url.searchParams.set("end_date", anchorDate);
    url.searchParams.set("timezone", "UTC");
  } else if (route === "/explorer") {
    url.searchParams.set("at", anchorDate);
  }
  return url.toString();
}

/**
 * @param {string} value
 * @returns {URL | null}
 */
function topicAggregateUrl(value) {
  try {
    const url = new URL(value);
    return url.origin === UI_ORIGIN && url.pathname.endsWith("/topic-attributions/aggregate") ? url : null;
  } catch {
    return null;
  }
}

/**
 * @param {CaptureRequest} request
 * @returns {boolean}
 */
function isExpectedSupersededAggregate(request) {
  let failure;
  try {
    failure = request.failure?.();
  } catch {
    return false;
  }
  if (failure?.errorText !== "net::ERR_ABORTED") return false;
  const url = topicAggregateUrl(request.url());
  const cluster = url?.searchParams.get("cluster_resource_id");
  return Boolean(cluster && cluster !== "lkc-customer" && "lkc-customer".startsWith(cluster));
}

/**
 * @param {{url: string}} candidate
 * @param {string} completed
 * @returns {boolean}
 */
function matchesCompletedAggregate(candidate, completed) {
  const candidateUrl = topicAggregateUrl(candidate.url);
  const completedUrl = topicAggregateUrl(completed);
  if (!candidateUrl || !completedUrl) return false;
  if (
    completedUrl.searchParams.get("cluster_resource_id") !== "lkc-customer" ||
    completedUrl.searchParams.get("topic_name") !== "showcase-live-orders"
  ) {
    return false;
  }
  const candidateCluster = candidateUrl.searchParams.get("cluster_resource_id");
  if (!candidateCluster || candidateCluster === "lkc-customer" || !"lkc-customer".startsWith(candidateCluster)) {
    return false;
  }
  const candidateTopic = candidateUrl.searchParams.get("topic_name");
  if (candidateTopic && !"showcase-live-orders".startsWith(candidateTopic)) return false;
  const keys = new Set([...candidateUrl.searchParams.keys(), ...completedUrl.searchParams.keys()]);
  for (const key of keys) {
    if (key === "cluster_resource_id" || key === "topic_name") continue;
    const candidateValues = [...candidateUrl.searchParams.getAll(key)].sort();
    const completedValues = [...completedUrl.searchParams.getAll(key)].sort();
    if (candidateValues.length !== completedValues.length || candidateValues.some((value, index) => value !== completedValues[index])) {
      return false;
    }
  }
  return true;
}

/**
 * Attach the request, response, and browser-error observers used by both
 * still and video capture.
 * @param {CapturePage} page
 * @param {CaptureObservation[]} observations
 * @param {ReadonlySet<string>} catalogStrings
 * @param {ReadonlySet<string>} catalogTopicNames
 * @param {string[]} errors
 * @param {Promise<unknown>[]} responsePromises
 * @param {Set<string>} runtimePreviewIds
 * @param {Set<CaptureRequest>} inFlightRequests
 * @returns {() => void}
 */
function monitorPage(
  page,
  observations,
  catalogStrings,
  catalogTopicNames,
  errors,
  responsePromises,
  runtimePreviewIds,
  inFlightRequests,
) {
  const deferredAggregateFailures = [];
  const deferredAggregateBodyErrors = [];
  const completedAggregateResponses = [];
  const recordRequest = (request, failed = false) => {
    const url = request.url();
    const kind = request.resourceType() || "request";
    const observation = { url, kind, ...(failed ? { failed: true } : {}) };
    observations.push(observation);
    if (url.startsWith("http://") || url.startsWith("https://")) {
      try {
        if (new URL(url).origin !== UI_ORIGIN) {
          errors.push(`browser request escaped the media UI origin: ${url}`);
        }
      } catch {
        errors.push(`browser request URL is invalid: ${url}`);
      }
    } else if (
      !((url.startsWith("/") && !url.startsWith("//")) || url.startsWith("data:") || url.startsWith("blob:"))
    ) {
      errors.push(`browser request URL is not local: ${url}`);
    }
    if (failed) {
      errors.push(`browser request failed: ${url}`);
    }
    return observation;
  };
  page.on("request", (request) => {
    inFlightRequests.add(request);
    recordRequest(request);
  });
  page.on("requestfinished", (request) => inFlightRequests.delete(request));
  page.on("requestfailed", (request) => {
    inFlightRequests.delete(request);
    if (isExpectedSupersededAggregate(request)) {
      deferredAggregateFailures.push({
        observation: recordRequest(request),
        url: request.url(),
      });
      return;
    }
    recordRequest(request, true);
  });
  page.on("response", (response) => {
    if (response.url().startsWith(`${UI_ORIGIN}/api/`) && !response.ok()) {
      errors.push(`API response failed with HTTP ${response.status()}: ${response.url()}`);
    }
    const contentType = response.headers()["content-type"] ?? "";
    if (contentType.includes("json")) {
      responsePromises.push(
        response
          .json()
          .then((body) => {
            const invalidRuntimePreviewIds = new Set();
            let bodyValidated = true;
            collectRuntimePreviewIds(body, runtimePreviewIds, invalidRuntimePreviewIds, catalogStrings);
            for (const identifier of invalidRuntimePreviewIds) {
              bodyValidated = false;
              errors.push(`API runtime Preview identifier is malformed: ${identifier}`);
            }
            for (const topicName of collectTopicNames(body)) {
              if (!catalogTopicNames.has(topicName)) {
                bodyValidated = false;
                errors.push(`API topic_name is not in the synthetic catalog: ${topicName}`);
              }
            }
            for (const token of sourceIdentifierTokens(body, runtimePreviewIds)) {
              if (!catalogStrings.has(token)) {
                bodyValidated = false;
                errors.push(`API identifier is not in the synthetic catalog: ${token}`);
              }
            }
            if (bodyValidated && topicAggregateUrl(response.url())) {
              completedAggregateResponses.push(response.url());
            }
          })
          .catch((error) => {
            const message = error instanceof Error ? error.message : String(error);
            if (
              topicAggregateUrl(response.url()) &&
              message.includes("No data found for resource with given identifier")
            ) {
              deferredAggregateBodyErrors.push({ url: response.url(), message });
              return;
            }
            errors.push(`JSON response could not be read from ${response.url()}: ${message}`);
          }),
      );
    }
  });
  page.on("pageerror", (error) => errors.push(`page error: ${error.message}`));
  page.on("console", (message) => {
    if (message.type() === "error") {
      errors.push(`console error: ${message.text()}`);
    }
  });
  return () => {
    for (const bodyError of deferredAggregateBodyErrors) {
      if (completedAggregateResponses.some((url) => matchesCompletedAggregate(bodyError, url))) continue;
      errors.push(`JSON response could not be read from ${bodyError.url}: ${bodyError.message}`);
    }
    for (const failure of deferredAggregateFailures) {
      if (completedAggregateResponses.some((url) => matchesCompletedAggregate(failure, url))) continue;
      failure.observation.failed = true;
      errors.push(`browser request failed: ${failure.url}`);
    }
  };
}

/**
 * @param {CapturePage} page
 * @param {CaptureRouteScene} scene
 * @param {CaptureSpec} captureSpec
 * @param {{video?: boolean, navigate?: boolean}} [options]
 * @returns {Promise<string>}
 */
async function waitForScene(page, scene, captureSpec, { video = false, navigate = true } = {}) {
  const anchorDate = captureSpec.anchor_date;
  let graphResponse;
  if (navigate && scenePath(scene) === "/explorer") {
    const expectedAt = `${anchorDate}T12:00:00Z`;
    graphResponse = page.waitForResponse(
      (response) => {
        try {
          const url = new URL(response.url());
          return (
            response.ok() &&
            url.origin === UI_ORIGIN &&
            url.pathname.endsWith("/graph") &&
            url.searchParams.get("at") === expectedAt
          );
        } catch {
          return false;
        }
      },
      { timeout: 45_000 },
    );
  }
  if (navigate) {
    await page.goto(sceneUrl(scene, anchorDate), { waitUntil: "networkidle" });
    if (graphResponse) {
      await graphResponse;
    }
    if (video) {
      await applyCaptureZoom(page, captureSpec);
      if (scenePath(scene) === "/explorer") {
        await applyExplorerCaptureFrame(page, captureSpec);
      }
    }
  }
  if (scenePath(scene) === "/explorer") {
    for (const selector of [
      '[data-testid="breadcrumb-trail"]',
      '[data-testid="graph-container"]',
      '[data-testid="timeline-scrubber"]',
    ]) {
      await page.locator(selector).waitFor({ state: "visible", timeout: 45_000 });
    }
    await page.getByText(anchorDate, { exact: true }).first().waitFor({ state: "visible", timeout: 45_000 });
  }
  for (const marker of scene.markers ?? []) {
    await page.getByText(marker, { exact: false }).first().waitFor({ state: "visible", timeout: 45_000 });
  }
  const primaryTenantLabel = `${captureSpec.primary_tenant.name} (${captureSpec.primary_tenant.ecosystem})`;
  await page.getByText(primaryTenantLabel, { exact: true })
    .first()
    .waitFor({ state: "visible", timeout: 45_000 });
  const bodyText = await page.locator("body").innerText();
  if (!bodyText.includes(primaryTenantLabel)) {
    fail(`capture did not select the primary ${primaryTenantLabel} tenant`);
  }
  return bodyText;
}

/** @param {CapturePage} page @returns {Promise<void>} */
async function prepareTopicAttribution(page) {
  const analyticsButton = page.getByRole("button", { name: "Analytics", exact: true });
  if (await analyticsButton.count()) {
    await analyticsButton.click();
  } else {
    const analyticsOption = page.getByText("Analytics", { exact: true }).last();
    if (!(await analyticsOption.count())) {
      fail("Topic Attribution Analytics control is unavailable");
    }
    await analyticsOption.click();
  }
  await page.getByText("Top Topics by Cost", { exact: false }).first().waitFor({ state: "visible", timeout: 45_000 });
}

/** @param {CapturePage} page @returns {Promise<void>} */
async function preparePipeline(page) {
  const runPipeline = page.getByRole("button", { name: "Run Pipeline", exact: false });
  await runPipeline.waitFor({ state: "visible", timeout: 45_000 });
  for (const marker of ["Gathering", "Calculating", "Topic Attribution Stage", "Emitting"]) {
    await page.getByText(marker, { exact: true }).first().waitFor({ state: "visible", timeout: 45_000 });
  }
  if (!(await runPipeline.isDisabled())) {
    fail("API-only Demo Pipeline control is unexpectedly enabled");
  }
  await runPipeline.locator("..").hover();
  await page
    .getByText("Pipeline execution is unavailable in API-only mode.", { exact: false })
    .first()
    .waitFor({ state: "visible", timeout: 45_000 });
}

/**
 * @param {string} query
 * @returns {(response: CaptureResponse) => boolean}
 */
function graphSearchResponse(query) {
  return (response) => {
    try {
      const url = new URL(response.url());
      return (
        response.ok() &&
        url.origin === UI_ORIGIN &&
        url.pathname.endsWith("/graph/search") &&
        url.searchParams.get("q") === query
      );
    } catch {
      return false;
    }
  };
}

/**
 * @param {string} query
 * @returns {(response: CaptureResponse) => boolean}
 */
function focusedGraphResponse(query) {
  return (response) => {
    try {
      const url = new URL(response.url());
      return (
        response.ok() &&
        url.origin === UI_ORIGIN &&
        url.pathname.endsWith("/graph") &&
        url.searchParams.get("focus") === query
      );
    } catch {
      return false;
    }
  };
}

/**
 * @param {CapturePage} page
 * @param {PointerController} pointer
 * @param {string} query
 * @param {string} resultText
 * @param {string} sceneId
 * @param {number} speed
 * @returns {Promise<void>}
 */
async function focusEntity(page, pointer, query, resultText, sceneId, speed) {
  const search = page.getByPlaceholder("Search entities… (⌘K)");
  await search.waitFor({ state: "visible", timeout: 45_000 });
  await pointer.moveTo(search, sceneId);
  const searchResponse = page.waitForResponse(graphSearchResponse(query), { timeout: 45_000 });
  await search.pressSequentially(query, { delay: TYPE_DELAY_SECONDS * speed * 1000 });
  await searchResponse;
  const result = search.locator("..").getByText(resultText, { exact: true }).first();
  await result.waitFor({ state: "visible", timeout: 45_000 });
  const focusedResponse = page.waitForResponse(focusedGraphResponse(query), { timeout: 45_000 });
  await pointer.click(result, sceneId);
  await focusedResponse;
}

/**
 * @param {CapturePage} page
 * @param {string} anchorDate
 * @param {Set<string>} runtimePreviewIds
 * @param {ReadonlySet<string>} catalogStrings
 * @returns {Promise<void>}
 */
async function prepareFocusPreview(page, anchorDate, runtimePreviewIds, catalogStrings) {
  const month = page.locator('input[type="month"]').first();
  if (await month.count()) {
    await month.fill(anchorDate.slice(0, 7));
  }
  const generate = page.getByRole("button", { name: "Generate preview", exact: true });
  await generate.waitFor({ state: "visible", timeout: 45_000 });
  const submitResponse = page.waitForResponse(
    (response) =>
      response.request().method() === "POST" &&
      response.url().endsWith("/focus-preview/requests") &&
      response.status() === 202,
    { timeout: 45_000 },
  );
  await generate.click();
  const queuedResponse = await submitResponse;
  const queued = await queuedResponse.json();
  if (!isRecord(queued)) {
    fail("FOCUS Mapping Preview submission did not return an object");
  }
  const invalidQueuedRuntimePreviewIds = new Set();
  collectRuntimePreviewIds(queued, runtimePreviewIds, invalidQueuedRuntimePreviewIds, catalogStrings);
  if (invalidQueuedRuntimePreviewIds.size) {
    fail(
      `FOCUS Mapping Preview submission contains malformed runtime identifiers: ${[
        ...invalidQueuedRuntimePreviewIds,
      ].join(", ")}`,
    );
  }
  if (typeof queued.request_id !== "string" || !queued.request_id) {
    fail("FOCUS Mapping Preview submission did not return a request identifier");
  }
  const requestPath = `/focus-preview/requests/${queued.request_id}`;
  let ready = false;
  const deadline = Date.now() + 90_000;
  while (!ready && Date.now() < deadline) {
    const statusResponse = await page.waitForResponse(
      (response) =>
        response.request().method() === "GET" &&
        response.url().includes(requestPath) &&
        response.status() === 200,
      { timeout: Math.max(1_000, deadline - Date.now()) },
    );
    const status = await statusResponse.json();
    if (!isRecord(status)) {
      fail("FOCUS Mapping Preview status did not return an object");
    }
    const invalidStatusRuntimePreviewIds = new Set();
    collectRuntimePreviewIds(status, runtimePreviewIds, invalidStatusRuntimePreviewIds, catalogStrings);
    if (invalidStatusRuntimePreviewIds.size) {
      fail(
        `FOCUS Mapping Preview status contains malformed runtime identifiers: ${[
          ...invalidStatusRuntimePreviewIds,
        ].join(", ")}`,
      );
    }
    if (status.status === "ready") {
      ready = true;
    } else if (status.status === "failed" || status.status === "expired") {
      fail(`FOCUS Mapping Preview request ended with status ${status.status}`);
    }
  }
  if (!ready) {
    fail("FOCUS Mapping Preview request did not reach terminal ready status");
  }
  await page.getByText("Status ready", { exact: false }).first().waitFor({ state: "visible", timeout: 45_000 });
  await page
    .getByText(/FOCUS Mapping Preview targets FOCUS 1\.4/, { exact: false })
    .first()
    .waitFor({ state: "visible", timeout: 45_000 });
  await page.getByText("Current authority gaps", { exact: false }).first().waitFor({ state: "visible", timeout: 45_000 });
  await page.getByText("Recent requests", { exact: false }).first().waitFor({ state: "visible", timeout: 45_000 });
}

/**
 * @param {Set<CaptureRequest>} inFlightRequests
 * @param {Promise<unknown>[]} responsePromises
 * @returns {Promise<void>}
 */
async function waitForSceneQuiescence(inFlightRequests, responsePromises) {
  const deadline = Date.now() + SCENE_QUIESCENCE_TIMEOUT_MS;
  let quietSince;
  while (Date.now() < deadline) {
    if (inFlightRequests.size === 0) {
      quietSince ??= Date.now();
      if (Date.now() - quietSince >= SCENE_QUIET_PERIOD_MS) {
        await Promise.all(responsePromises);
        if (inFlightRequests.size === 0) {
          return;
        }
        quietSince = undefined;
      }
    } else {
      quietSince = undefined;
    }
    await new Promise((resolve) => setTimeout(resolve, Math.min(50, deadline - Date.now())));
  }
  fail(`capture scene did not quiesce within ${SCENE_QUIESCENCE_TIMEOUT_MS}ms (${inFlightRequests.size} request(s) remain)`);
}

/**
 * @param {CapturePage} page
 * @param {ReadonlySet<string>} catalogTopicNames
 * @param {string[]} errors
 * @returns {Promise<void>}
 */
async function validateDomTopicNames(page, catalogTopicNames, errors) {
  const topicCells = await page.locator('[role="gridcell"][col-id="topic_name"]').allTextContents();
  for (const topicName of topicCells.map((value) => value.trim()).filter(Boolean)) {
    if (!catalogTopicNames.has(topicName)) {
      errors.push(`DOM topic_name is not in the synthetic catalog: ${topicName}`);
    }
  }
}

/**
 * @param {CapturePage} page
 * @param {string} outputRoot
 * @param {CaptureSpec} captureSpec
 * @param {ReadonlySet<string>} catalogStrings
 * @param {ReadonlySet<string>} catalogTopicNames
 * @param {string[]} errors
 * @param {Set<string>} runtimePreviewIds
 * @param {Set<CaptureRequest>} inFlightRequests
 * @param {Promise<unknown>[]} responsePromises
 * @returns {Promise<void>}
 */
async function captureStillScenes(
  page,
  outputRoot,
  captureSpec,
  catalogStrings,
  catalogTopicNames,
  errors,
  runtimePreviewIds,
  inFlightRequests,
  responsePromises,
) {
  for (const [index, screenshot] of captureSpec.screenshots.entries()) {
    const scene = { ...screenshot, ...SCENE_MARKERS[index] };
    await waitForScene(page, scene, captureSpec);
    if (scenePath(scene) === "/topic-attributions") {
      await prepareTopicAttribution(page);
    }
    if (scenePath(scene) === "/pipeline") {
      await preparePipeline(page);
    }
    if (scenePath(scene) === "/focus-preview") {
      await prepareFocusPreview(page, captureSpec.anchor_date, runtimePreviewIds, catalogStrings);
    }
    await waitForSceneQuiescence(inFlightRequests, responsePromises);
    await Promise.all(responsePromises);
    const bodyText = await page.locator("body").innerText();
    for (const token of sourceIdentifierTokens(bodyText, runtimePreviewIds)) {
      if (!catalogStrings.has(token)) {
        errors.push(`DOM identifier is not in the synthetic catalog: ${token}`);
      }
    }
    await validateDomTopicNames(page, catalogTopicNames, errors);
    await page.screenshot({ path: path.join(outputRoot, "assets", scene.name), fullPage: false });
  }
}

/**
 * @param {number} speed
 * @returns {string}
 */
function pointerInitScript(speed) {
  const moveDuration = POINTER_MOVE_SECONDS * speed;
  const clickDuration = POINTER_CLICK_SECONDS * speed;
  return `(() => {
  const installPointer = () => {
    const root = document.documentElement;
    if (!root) return;
    let pointer = document.getElementById("demo-capture-pointer");
    if (!pointer) {
      pointer = document.createElement("div");
      pointer.id = "demo-capture-pointer";
      pointer.setAttribute("aria-hidden", "true");
      pointer.style.position = "fixed";
      pointer.style.left = "0";
      pointer.style.top = "0";
      pointer.style.width = "24px";
      pointer.style.height = "24px";
      pointer.style.border = "2px solid #ffffff";
      pointer.style.borderRadius = "50%";
      pointer.style.background = "rgba(37, 99, 235, 0.9)";
      pointer.style.boxShadow = "0 0 0 2px rgba(255, 255, 255, 0.8), 0 2px 8px rgba(0, 0, 0, 0.45)";
      pointer.style.pointerEvents = "none";
      pointer.style.zIndex = "2147483646";
      pointer.style.setProperty("--demo-pointer-x", "-100px");
      pointer.style.setProperty("--demo-pointer-y", "-100px");
      pointer.style.transform = "translate(var(--demo-pointer-x), var(--demo-pointer-y))";
      pointer.style.transition = "transform ${moveDuration}s linear";
      root.appendChild(pointer);
    }
    if (!pointer.dataset.captureListeners) {
      pointer.dataset.captureListeners = "true";
      document.addEventListener("pointermove", (event) => {
        if (pointer.dataset.capturePointerSuppressed === "true") return;
        pointer.style.setProperty("--demo-pointer-x", (event.clientX - 12) + "px");
        pointer.style.setProperty("--demo-pointer-y", (event.clientY - 12) + "px");
      });
      document.addEventListener("click", () => {
        pointer.classList.remove("demo-capture-pointer-pulse");
        void pointer.offsetWidth;
        pointer.classList.add("demo-capture-pointer-pulse");
      });
      const style = document.createElement("style");
      style.textContent = "@keyframes demo-capture-pointer-pulse { 0%, 100% { transform: translate(var(--demo-pointer-x), var(--demo-pointer-y)) scale(1); } 50% { transform: translate(var(--demo-pointer-x), var(--demo-pointer-y)) scale(1.45); } } .demo-capture-pointer-pulse { animation: demo-capture-pointer-pulse ${clickDuration}s ease-out; }";
      root.appendChild(style);
    }
  };
  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", installPointer, { once: true });
  }
  installPointer();
})();`;
}

/**
 * @param {CaptureSpec} captureSpec
 * @returns {number}
 */
function applyCaptureZoomValue(captureSpec) {
  const value = Number(captureSpec.video?.content_zoom_percent);
  if (value !== APPROVED_VIDEO.content_zoom_percent) {
    fail("capture video zoom is not the approved 150 percent");
  }
  return value / 100;
}

/**
 * @param {CapturePage} page
 * @param {CaptureSpec} captureSpec
 * @returns {Promise<void>}
 */
async function applyCaptureZoom(page, captureSpec) {
  const zoom = applyCaptureZoomValue(captureSpec);
  await page.addStyleTag({ content: `body { zoom: ${zoom}; }` });
}

/**
 * Keep the Explorer shell inside the 800-pixel recording content area. The
 * layout changes are capture-only; they do not change the application layout
 * or graph data.
 *
 * @param {CapturePage} page
 * @param {CaptureSpec} captureSpec
 * @returns {Promise<void>}
 */
async function applyExplorerCaptureFrame(page, captureSpec) {
  const zoom = applyCaptureZoomValue(captureSpec);
  const contentHeight = Number(captureSpec.video.content_height);
  const layoutHeight = contentHeight / zoom;
  if (!Number.isFinite(layoutHeight) || layoutHeight <= 0) {
    fail("Explorer capture content height is invalid");
  }
  await page.addStyleTag({
    content: `
      html, body, #root {
        height: ${layoutHeight}px !important;
        max-height: ${layoutHeight}px !important;
        min-height: 0 !important;
        overflow: hidden !important;
      }
      #root > * {
        height: 100% !important;
        max-height: 100% !important;
        min-height: 0 !important;
      }
      .ant-layout {
        height: 100% !important;
        max-height: 100% !important;
        min-height: 0 !important;
      }
      .ant-layout-content {
        height: auto !important;
        min-height: 0 !important;
        overflow: hidden !important;
      }
      .ant-layout-content > div {
        height: 100% !important;
        min-height: 0 !important;
      }
    `,
  });
}

/**
 * Reframe an Explorer graph through its Cytoscape camera after the application
 * has fitted the focused graph. This keeps the selected investigation node
 * readable in the recording without scaling a bitmap or changing graph data.
 *
 * @param {CapturePage} page
 * @param {string} focusId
 * @param {number} zoomMultiplier
 * @returns {Promise<void>}
 */
async function reframeExplorerGraph(page, focusId, zoomMultiplier) {
  const result = await page.evaluate(
    ({ id, multiplier }) => {
      if (typeof document.querySelectorAll !== "function") {
        return { browserDom: false, found: false };
      }
      for (const element of document.querySelectorAll('[data-testid="graph-container"] *')) {
        const host = /** @type {{_cyreg?: {cy?: CaptureCytoscape}}} */ (element);
        const cy = host._cyreg?.cy;
        if (!cy) continue;
        const node = cy.getElementById(id);
        if (!node || !node.isNode()) {
          return { browserDom: true, found: false };
        }
        const level = Math.min(cy.maxZoom(), cy.zoom() * multiplier);
        const centerFocusedNode = () => {
          cy.stop(true);
          const currentNode = cy.getElementById(id);
          if (!currentNode || !currentNode.isNode()) return;
          cy.zoom({ level, position: currentNode.position() });
          cy.center(currentNode);
        };
        // The renderer's force layout can continue after the focused response
        // arrives. Reapply the camera when that existing layout emits its
        // completion event so the frame follows the final node position.
        cy.one("layoutstop", centerFocusedNode);
        centerFocusedNode();
        return { browserDom: true, found: true };
      }
      return { browserDom: true, found: false };
    },
    { id: focusId, multiplier: zoomMultiplier },
  );
  if (result?.browserDom && !result.found) {
    fail(`Explorer graph camera could not frame ${focusId}`);
  }
}

/** @param {CapturePage} page @returns {Promise<void>} */
async function selectVideoTheme(page) {
  // Keep still captures on their normal theme. The video uses the existing
  // application control so its persisted preference survives storyboard
  // navigations in this browser context.
  const lightThemeButton = page.locator('button[title="Switch to light mode"]');
  if (await lightThemeButton.count()) {
    await lightThemeButton.first().click();
  }
  await page
    .locator('button[title="Switch to dark mode"]')
    .first()
    .waitFor({ state: "visible", timeout: 45_000 });
}

/** Owns the one capture-only pointer lifecycle for a recorded browser context. */
class PointerController {
  /**
   * @param {CaptureBrowserContext} context
   * @param {CapturePage} page
   * @param {number} speed
   */
  constructor(context, page, speed) {
    /** @type {CaptureBrowserContext} */
    this.context = context;
    /** @type {CapturePage} */
    this.page = page;
    /** @type {number} */
    this.speed = speed;
    /** @type {CaptureLocator | null} */
    this.locator = null;
  }

  /** @returns {Promise<this>} */
  async start() {
    await this.context.addInitScript(pointerInitScript(this.speed));
    this.locator = this.page.locator("#demo-capture-pointer");
    return this;
  }

  /**
   * @param {string} sceneId
   * @returns {Promise<void>}
   */
  async assertVisible(sceneId) {
    if (!this.locator || !(await this.locator.isVisible())) {
      fail(`scene ${sceneId} pointer is off-screen`);
    }
  }

  /**
   * @param {CaptureLocator} target
   * @param {string} sceneId
   * @param {number} [xRatio]
   * @param {number} [yRatio]
   * @returns {Promise<void>}
   */
  async moveTo(target, sceneId, xRatio = 0.5, yRatio = 0.5) {
    const box = await target.boundingBox();
    if (!box) {
      fail(`scene ${sceneId} pointer target is off-screen`);
    }
    await this.assertVisible(sceneId);
    await this.page.mouse.move(box.x + box.width * xRatio, box.y + box.height * yRatio);
    await this.page.waitForTimeout(POINTER_MOVE_SECONDS * this.speed * 1000);
    await this.assertVisible(sceneId);
  }

  /**
   * @param {CaptureLocator} target
   * @param {string} sceneId
   * @returns {Promise<void>}
   */
  async click(target, sceneId) {
    await this.moveTo(target, sceneId);
    await target.click();
    await this.page.waitForTimeout(POINTER_CLICK_SECONDS * this.speed * 1000);
    await this.assertVisible(sceneId);
  }

  /**
   * @param {CaptureLocator} target
   * @param {string} sceneId
   * @returns {Promise<void>}
   */
  async hover(target, sceneId) {
    await this.moveTo(target, sceneId);
    await target.hover();
    await this.assertVisible(sceneId);
  }
}

/**
 * @param {CaptureLocator} locator
 * @param {string} sceneId
 * @param {CaptureSpec} captureSpec
 * @param {string} kind
 * @returns {Promise<void>}
 */
function assertFramed(locator, sceneId, captureSpec, kind) {
  return (async () => {
    const box = await locator.boundingBox();
    const contentHeight = Number(captureSpec.video.content_height);
    const viewportWidth = Number(captureSpec.viewport.width);
    if (
      !box ||
      box.x < 0 ||
      box.x + box.width > viewportWidth ||
      box.y < 0 ||
      box.y + box.height > contentHeight
    ) {
      fail(`scene ${sceneId} ${kind} is off-screen`);
    }
    const sourceZoom = applyCaptureZoomValue(captureSpec);
    const playbackScale = Number(captureSpec.video.playback_width) / Number(captureSpec.viewport.width);
    const fontPixels = Number(
      await locator.evaluate((element) => Number.parseFloat(window.getComputedStyle(element).fontSize), null),
    );
    if (!Number.isFinite(fontPixels) || fontPixels * sourceZoom * playbackScale < 11) {
      fail(`scene ${sceneId} ${kind} text is too small for playback`);
    }
    // Playwright bounding boxes already include CSS zoom. Applying the source
    // zoom a second time here would overstate the physical target size.
    if (kind.includes("target") && (box.width * playbackScale < 24 || box.height * playbackScale < 24)) {
      fail(`scene ${sceneId} ${kind} is too small for playback`);
    }
  })();
}

/**
 * @param {CaptureLocator} locator
 * @param {string} sceneId
 * @param {CaptureSpec} captureSpec
 * @param {number} speed
 * @param {"down" | "up"} [direction]
 * @returns {Promise<CaptureScrollSample[]>}
 */
async function smoothScrollTo(locator, sceneId, captureSpec, speed, direction = "down") {
  const durationMs = SCROLL_DURATION_SECONDS * speed * 1000;
  const samples = await locator.evaluate(
    (element, options) => {
      const durationMs = Number(options.durationMs);
      if (!Number.isFinite(durationMs) || durationMs <= 0) {
        throw new Error("smooth-scroll duration is invalid");
      }
      const startScroll = window.scrollY;
      const maxScroll = Math.max(0, document.documentElement.scrollHeight - window.innerHeight);
      const targetBox = element?.getBoundingClientRect?.();
      if (!targetBox) {
        throw new Error("smooth-scroll target is unavailable");
      }
      const targetTop = Number(targetBox.top);
      if (!Number.isFinite(targetTop)) {
        throw new Error("smooth-scroll target position is invalid");
      }
      const safeTop = Math.min(120, Math.max(0, window.innerHeight * 0.18));
      const targetFromElement = startScroll + targetTop - safeTop;
      const targetScroll =
        options.direction === "up"
          ? Math.min(startScroll, Math.max(0, targetFromElement))
          : Math.max(startScroll, Math.min(maxScroll, targetFromElement));
      if (Math.abs(targetScroll - startScroll) < 120) {
        throw new Error("smooth-scroll target is too close to the current position");
      }
      const startTime = performance.now();
      const samples = [{ elapsed_seconds: 0, offset: 0 }];
      return new Promise((resolve) => {
        const animate = (now) => {
          const elapsed = Math.min(durationMs, Math.max(0, now - startTime));
          const progress = Math.min(1, elapsed / durationMs);
          const eased = progress < 1 ? progress * (2 - progress) : 1;
          window.scrollTo(0, startScroll + (targetScroll - startScroll) * eased);
          samples.push({ elapsed_seconds: elapsed / 1000, offset: window.scrollY - startScroll });
          if (progress >= 1) {
            resolve(samples);
            return;
          }
          requestAnimationFrame(animate);
        };
        requestAnimationFrame(animate);
      });
    },
    { durationMs, direction },
  );
  if (!Array.isArray(samples) || samples.length < 3) {
    fail(`scene ${sceneId} smooth-scroll did not produce intermediate samples`);
  }
  const first = samples[0];
  const last = samples[samples.length - 1];
  if (
    !first ||
    !last ||
    Math.abs(Number(first.elapsed_seconds)) > FRAME_TOLERANCE_SECONDS ||
    Math.abs(Number(last.offset) - Number(first.offset)) < 120
  ) {
    fail(`scene ${sceneId} smooth-scroll travel is too short`);
  }
  // The Python editor repeats the same evidence checks. Keep this browser
  // assertion close to the action so an invalid capture cannot be serialized.
  if (Number(last.elapsed_seconds) < SCROLL_DURATION_SECONDS * speed - FRAME_TOLERANCE_SECONDS * speed) {
    fail(`scene ${sceneId} smooth-scroll duration is too short`);
  }
  return samples.map((sample) => ({
    elapsed_seconds: Number(sample.elapsed_seconds),
    offset: Number(sample.offset),
  }));
}

/** @param {CapturePage} page @returns {Promise<void>} */
async function showSyncMarker(page) {
  await page.evaluate((color) => {
    const root = document.documentElement;
    if (!root || typeof root.appendChild !== "function") return;
    document.getElementById("demo-capture-sync-marker")?.remove();
    const marker = document.createElement("div");
    marker.id = "demo-capture-sync-marker";
    marker.style.position = "fixed";
    marker.style.inset = "0";
    marker.style.background = color;
    // The marker is appended after the capture pointer and uses the highest
    // CSS stacking value so the recorded boundary is a solid frame even when
    // the pointer is visible at the same time.
    marker.style.zIndex = "2147483647";
    marker.style.pointerEvents = "none";
    root.appendChild(marker);
  }, MARKER_COLOR);
}

/** @param {CapturePage} page @returns {Promise<void>} */
async function hideSyncMarker(page) {
  await page.evaluate(
    () =>
      new Promise((resolve) => {
        requestAnimationFrame(() => {
          const marker = document.getElementById("demo-capture-sync-marker");
          if (marker && typeof marker.remove === "function") marker.remove();
          resolve();
        });
      }),
  );
}

/**
 * @param {CapturePage} page
 * @returns {Promise<number>}
 */
async function nextAnimationFrame(page) {
  // Keep the timeline clock in the Node runner. The value returned by
  // page.evaluate() belongs to a separate browser clock realm and cannot be
  // subtracted from Node timestamps after a navigation.
  await page.evaluate(() => new Promise((resolve) => requestAnimationFrame(() => resolve())));
  return performance.now();
}

/**
 * @param {CapturePage} page
 * @param {Record<string, string>} [expectedQuery]
 * @returns {Promise<void>}
 */
async function waitForAggregation(page, expectedQuery = {}) {
  const response = await page.waitForResponse(
    (candidate) => {
      try {
        const url = new URL(candidate.url());
        return (
          candidate.ok() &&
          url.origin === UI_ORIGIN &&
          url.pathname.endsWith("/topic-attributions/aggregate") &&
          Object.entries(expectedQuery).every(([key, value]) => url.searchParams.get(key) === value)
        );
      } catch {
        return false;
      }
    },
    { timeout: 45_000 },
  );
  if (!response.ok()) fail(`Topic Attribution aggregation returned HTTP ${response.status()}`);
}

/**
 * @param {CapturePage} page
 * @param {Record<string, string>} expectedFilters
 * @returns {Promise<void>}
 */
async function waitForTopicFilters(page, expectedFilters) {
  await page.waitForURL(
    (candidate) => {
      try {
        const url = candidate instanceof URL ? candidate : new URL(String(candidate));
        return (
          url.origin === UI_ORIGIN &&
          url.pathname === "/topic-attributions" &&
          Object.entries(expectedFilters).every(([key, value]) => url.searchParams.get(key) === value)
        );
      } catch {
        return false;
      }
    },
    { timeout: 45_000, waitUntil: "commit" },
  );
}

/**
 * @param {CapturePage} page
 * @param {CaptureLocator} locator
 * @param {string} placeholder
 * @param {string} value
 * @param {Record<string, string>} baseFilters
 * @param {string} filterKey
 * @param {number} speed
 * @returns {Promise<void>}
 */
async function typeControlledFilter(page, locator, placeholder, value, baseFilters, filterKey, speed) {
  const expectedFilters = { ...baseFilters, [filterKey]: value };
  const selector = `input[placeholder="${placeholder}"]`;
  let prefix = "";
  for (const character of value) {
    prefix += character;
    const inputCommit = page.waitForFunction(
      ({ inputSelector, expectedValue }) => {
        const input = document.querySelector(inputSelector);
        return Boolean(input && "value" in input && input.value === expectedValue);
      },
      { inputSelector: selector, expectedValue: prefix },
      { timeout: 45_000 },
    );
    // Controlled inputs rerender after every committed character. Waiting on
    // the input value keeps the visible authored typing delay while ensuring
    // the next character is sent only after the current value is present.
    await locator.pressSequentially(character, { delay: TYPE_DELAY_SECONDS * speed * 1000 });
    await inputCommit;
  }
  await waitForTopicFilters(page, expectedFilters);
}

/**
 * @param {CapturePage} page
 * @param {PointerController} pointer
 * @param {string} sceneId
 * @returns {Promise<CaptureLocator>}
 */
async function clickAnalytics(page, pointer, sceneId) {
  const analytics = page.getByRole("button", { name: "Analytics", exact: true });
  if (await analytics.count()) {
    await pointer.click(analytics, sceneId);
  } else {
    const option = page.getByText("Analytics", { exact: true }).last();
    if (!(await option.count())) fail(`scene ${sceneId} Analytics control is unavailable`);
    await pointer.click(option, sceneId);
  }
  const title = page.getByText("Top Topics by Cost", { exact: false }).first();
  await title.waitFor({ state: "visible", timeout: 45_000 });
  return title;
}

/**
 * @param {CapturePage} page
 * @param {PointerController} pointer
 * @param {CaptureLocator} chart
 * @param {string} sceneId
 * @param {string} expectedText
 * @param {number} [xRatio]
 * @param {number} [yRatio]
 * @param {CaptureSpec} [captureSpec]
 * @param {string} [requiredText]
 * @returns {Promise<string>}
 */
async function requireTooltip(
  page,
  pointer,
  chart,
  sceneId,
  expectedText,
  xRatio = 0.5,
  yRatio = 0.45,
  captureSpec,
  requiredText,
) {
  await pointer.moveTo(chart, sceneId, xRatio, yRatio);
  if (captureSpec) {
    const zoom = applyCaptureZoomValue(captureSpec);
    if (zoom !== 1) {
      const box = await chart.boundingBox();
      if (!box) fail(`scene ${sceneId} tooltip chart is off-screen`);
      const visualPoint = {
        x: box.x + box.width * xRatio,
        y: box.y + box.height * yRatio,
      };
      // CSS zoom scales the rendered canvas while ECharts hit testing uses
      // the unscaled local coordinates. Suppress the capture pointer's
      // pointermove listener while sending the corrected event, then put the
      // pointer back over the rendered data point in the same RAF turn.
      await page.evaluate(() => {
        const marker = document.getElementById("demo-capture-pointer");
        if (marker?.dataset) marker.dataset.capturePointerSuppressed = "true";
      });
      await page.mouse.move(
        box.x + (box.width * xRatio) / zoom,
        box.y + (box.height * yRatio) / zoom,
      );
      await page.evaluate(
        ({ x, y }) =>
          new Promise((resolve) => {
            const marker = document.getElementById("demo-capture-pointer");
            if (!marker) {
              resolve();
              return;
            }
            const transition = marker.style.transition;
            marker.style.transition = "none";
            if (typeof marker.style.setProperty === "function") {
              marker.style.setProperty("--demo-pointer-x", `${x - 12}px`);
              marker.style.setProperty("--demo-pointer-y", `${y - 12}px`);
            } else {
              marker.style["--demo-pointer-x"] = `${x - 12}px`;
              marker.style["--demo-pointer-y"] = `${y - 12}px`;
            }
            void marker.offsetWidth;
            requestAnimationFrame(() => {
              if (marker.dataset) delete marker.dataset.capturePointerSuppressed;
              marker.style.transition = transition;
              resolve();
            });
          }),
        visualPoint,
      );
    }
  }
  const tooltip = page.getByText(expectedText, { exact: false }).last();
  await tooltip.waitFor({ state: "visible", timeout: 45_000 });
  const text = await tooltip.innerText();
  for (const required of [expectedText, requiredText].filter(Boolean)) {
    if (!text.includes(required)) {
      fail(`scene ${sceneId} tooltip does not contain ${required}`);
    }
  }
  const box = await tooltip.boundingBox();
  const contentHeight = Number(captureSpec?.video?.content_height ?? 0);
  const viewportWidth = Number(captureSpec?.viewport?.width ?? 0);
  if (
    !box ||
    box.x < 0 ||
    box.y < 0 ||
    box.x + box.width > viewportWidth ||
    box.y + box.height > contentHeight
  ) {
    fail(`scene ${sceneId} tooltip is not fully visible in the content frame`);
  }
  await assertFramed(tooltip, sceneId, captureSpec, "tooltip target");
  return text;
}

/**
 * @param {CaptureLocator} title
 * @returns {CaptureLocator}
 */
function chartForTitle(title) {
  return title
    .locator("xpath=ancestor::*[contains(concat(' ', normalize-space(@class), ' '), ' ant-card ')][1]")
    .locator("canvas")
    .first();
}

/** @param {CaptureLocator} label @returns {CaptureLocator} */
function cardForLabel(label) {
  return label.locator(
    "xpath=ancestor::*[contains(concat(' ', normalize-space(@class), ' '), ' ant-card ')][1]",
  );
}

/** @param {unknown} value @returns {number | null} */
function currencyAmount(value) {
  const match = String(value).match(/\$([\d,]+(?:\.\d{2})?)/);
  if (!match) return null;
  const amount = Number(match[1].replaceAll(",", ""));
  return Number.isFinite(amount) ? amount : null;
}

/** @param {unknown} value @returns {string | null} */
function formattedCurrencyAmount(value) {
  const amount = currencyAmount(value);
  if (amount === null) return null;
  return new Intl.NumberFormat("en-US", {
    style: "currency",
    currency: "USD",
    minimumFractionDigits: 2,
    maximumFractionDigits: 2,
  }).format(amount);
}

/**
 * @param {CapturePage} page
 * @param {PointerController} pointer
 * @param {string} sceneId
 * @param {CaptureSpec} captureSpec
 * @returns {Promise<CaptureSceneActionResult>}
 */
async function actionDashboardSummary(page, pointer, sceneId, captureSpec) {
  const observed = {};
  for (const label of ["Total Cost", "Usage Cost", "Shared Cost"]) {
    const labelLocator = page.getByText(label, { exact: false }).first();
    const card = cardForLabel(labelLocator);
    const amount = formattedCurrencyAmount(await card.innerText());
    if (amount === null || amount !== APPROVED_DASHBOARD_AMOUNTS[label]) {
      fail(`scene ${sceneId} ${label} does not show the approved dashboard amount`);
    }
    observed[label] = currencyAmount(amount);
    await assertFramed(labelLocator, sceneId, captureSpec, `${label} evidence`);
    await pointer.hover(labelLocator, sceneId);
  }
  if (observed["Shared Cost"] <= observed["Usage Cost"]) {
    fail(`scene ${sceneId} shared cost is not larger than usage cost`);
  }
  if (observed["Total Cost"] !== observed["Usage Cost"] + observed["Shared Cost"]) {
    fail(`scene ${sceneId} dashboard totals do not reconcile`);
  }
  return { scrollSamples: [] };
}

/**
 * @param {CapturePage} page
 * @param {PointerController} pointer
 * @param {string} sceneId
 * @param {CaptureSpec} captureSpec
 * @param {number} speed
 * @returns {Promise<CaptureSceneActionResult>}
 */
async function actionDashboardTrend(page, pointer, sceneId, captureSpec, speed) {
  const dateRange = page.getByText("Date Range", { exact: true }).first();
  await dateRange.waitFor({ state: "visible", timeout: 45_000 });
  await assertFramed(dateRange, sceneId, captureSpec, "Date Range evidence");
  const title = page.getByText("Cost Trend Over Time", { exact: false }).first();
  const scrollSamples = await smoothScrollTo(title, sceneId, captureSpec, speed);
  await assertFramed(title, sceneId, captureSpec, "Cost Trend title");
  const chart = chartForTitle(title);
  await assertFramed(chart, sceneId, captureSpec, "Cost Trend target");
  // The final date label is rendered inside the canvas. Point to its final
  // data region while keeping the chart and its visible date axis framed;
  // unlike the topic and movers scenes, this scene makes no tooltip claim.
  await pointer.moveTo(chart, sceneId, 0.95, 0.45);
  return { scrollSamples };
}

/**
 * @param {CapturePage} page
 * @param {PointerController} pointer
 * @param {string} sceneId
 * @param {CaptureSpec} captureSpec
 * @param {number} speed
 * @returns {Promise<CaptureSceneActionResult>}
 */
async function actionExplorerCommerce(page, pointer, sceneId, captureSpec, speed) {
  await focusEntity(page, pointer, "env-commerce", "Commerce", sceneId, speed);
  await reframeExplorerGraph(page, "env-commerce", 1.5);
  const commerce = page.getByText("Commerce", { exact: true }).first();
  await assertFramed(commerce, sceneId, captureSpec, "Commerce evidence");
  await pointer.assertVisible(sceneId);
  return { scrollSamples: [] };
}

/**
 * @param {CapturePage} page
 * @param {PointerController} pointer
 * @param {string} sceneId
 * @param {CaptureSpec} captureSpec
 * @param {number} speed
 * @returns {Promise<CaptureSceneActionResult>}
 */
async function actionExplorerCustomer(page, pointer, sceneId, captureSpec, speed) {
  await focusEntity(page, pointer, "lkc-customer", "Customer Kafka", sceneId, speed);
  await reframeExplorerGraph(page, "lkc-customer", 2.2);
  const customer = page.getByText("Customer Kafka", { exact: true }).first();
  await assertFramed(customer, sceneId, captureSpec, "Customer Kafka evidence");
  await pointer.assertVisible(sceneId);
  return { scrollSamples: [] };
}

/**
 * @param {CapturePage} page
 * @param {PointerController} pointer
 * @param {string} sceneId
 * @param {CaptureSpec} captureSpec
 * @param {number} speed
 * @returns {Promise<CaptureSceneActionResult>}
 */
async function actionTopicTopics(page, pointer, sceneId, captureSpec, speed) {
  const title = await clickAnalytics(page, pointer, sceneId);
  const chart = chartForTitle(title);
  let scrollSamples = [];
  const chartBox = await chart.boundingBox();
  const contentHeight = Number(captureSpec.video.content_height);
  if (!chartBox || chartBox.y < 0 || chartBox.y + chartBox.height > contentHeight) {
    // The analytics canvas follows the filters and segmented control. In the
    // video frame it can begin below the fold after a fresh route navigation;
    // bring the title/card into view before testing the rendered tooltip.
    scrollSamples = await smoothScrollTo(title, sceneId, captureSpec, speed);
  }
  const topicTooltip = await requireTooltip(
    page,
    pointer,
    chart,
    sceneId,
    "$79,000.00",
    0.18,
    0.36,
    captureSpec,
    "showcase-live-orders",
  );
  await assertFramed(title, sceneId, captureSpec, "Top Topics title");
  await assertFramed(chart, sceneId, captureSpec, "Top Topics target");
  // Top Topics is a scene-local framing adjustment; the approved timeline
  // only persists scroll evidence for the four designated below-fold scenes.
  return { scrollSamples: [], evidence: { topicTooltip: "$79,000.00", topicTooltipText: topicTooltip } };
}

/**
 * @param {CapturePage} page
 * @param {PointerController} pointer
 * @param {string} sceneId
 * @param {CaptureSpec} captureSpec
 * @param {number} speed
 * @param {CaptureMode} mode
 * @returns {Promise<CaptureSceneActionResult>}
 */
async function actionTopicFilters(page, pointer, sceneId, captureSpec, speed, mode) {
  const cluster = page.getByPlaceholder("Any cluster", { exact: true });
  const topic = page.getByPlaceholder("Any topic", { exact: true });
  await cluster.waitFor({ state: "visible", timeout: 45_000 });
  await topic.waitFor({ state: "visible", timeout: 45_000 });
  const scrollY = await page.evaluate(() => window.scrollY, null);
  if (Number(scrollY) > 0) {
    // Full mode arrives here from the scrolled Top Topics scene. Draft mode
    // starts at the route origin, so it keeps its accepted choreography.
    await smoothScrollTo(cluster, sceneId, captureSpec, speed, "up");
  }
  const dateScope = {
    start_date: "2026-08-02",
    end_date: "2026-08-31",
    timezone: "UTC",
  };
  const clusterFilters = { ...dateScope, cluster_resource_id: "lkc-customer" };
  const topicFilters = { ...clusterFilters, topic_name: "showcase-live-orders" };
  // Register before either filter commit. Analytics is already selected in
  // the full walkthrough, so changing the filters itself may issue the final
  // aggregation request; in draft mode the same waiter catches the request
  // triggered when Analytics is selected below.
  const aggregateResponse = waitForAggregation(page, topicFilters);
  const filterInteraction = (async () => {
    const clusterNavigation = waitForTopicFilters(page, clusterFilters);
    await pointer.click(cluster, sceneId);
    await typeControlledFilter(
      page,
      cluster,
      "Any cluster",
      "lkc-customer",
      dateScope,
      "cluster_resource_id",
      speed,
    );
    await clusterNavigation;
    const topicNavigation = waitForTopicFilters(page, topicFilters);
    await pointer.click(topic, sceneId);
    await typeControlledFilter(
      page,
      topic,
      "Any topic",
      "showcase-live-orders",
      clusterFilters,
      "topic_name",
      speed,
    );
    await topicNavigation;
    const title =
      mode === "draft"
        ? await clickAnalytics(page, pointer, sceneId)
        : page.getByText("Top Topics by Cost", { exact: false }).first();
    await title.waitFor({ state: "visible", timeout: 45_000 });
    return title;
  })();
  // Promise.all installs rejection handlers on both operations immediately.
  // This preserves the first scene failure while also owning a late aggregate
  // timeout or HTTP failure instead of leaving it as an unhandled rejection.
  const [title] = await Promise.all([filterInteraction, aggregateResponse]);
  await assertFramed(cluster, sceneId, captureSpec, "Cluster target");
  await assertFramed(topic, sceneId, captureSpec, "Topic Name target");
  await assertFramed(title, sceneId, captureSpec, "filtered Top Topics title");
  return { scrollSamples: [] };
}

/**
 * @param {CapturePage} page
 * @param {PointerController} pointer
 * @param {string} sceneId
 * @param {CaptureSpec} captureSpec
 * @param {number} speed
 * @returns {Promise<CaptureSceneActionResult>}
 */
async function actionTopicComposition(page, pointer, sceneId, captureSpec, speed) {
  const title = page.getByText("Cost Composition by Product Type", { exact: false }).first();
  const scrollSamples = await smoothScrollTo(title, sceneId, captureSpec, speed);
  const chart = chartForTitle(title);
  await assertFramed(title, sceneId, captureSpec, "Cost Composition title");
  await assertFramed(chart, sceneId, captureSpec, "Cost Composition target");
  // ECharts draws the product legend into the canvas, so there is no HTML
  // locator or tooltip containing KAFKA_REST_PRODUCE. The approved filtered
  // chart has one legend item at the lower center; point to that rendered
  // canvas region while keeping the title, date axis, and value scale framed.
  await pointer.moveTo(chart, sceneId, 0.47, 0.96);
  return { scrollSamples };
}

/**
 * @param {CapturePage} page
 * @param {PointerController} pointer
 * @param {string} sceneId
 * @param {CaptureSpec} captureSpec
 * @param {number} speed
 * @returns {Promise<CaptureSceneActionResult>}
 */
async function actionTopicMovers(page, pointer, sceneId, captureSpec, speed) {
  const title = page.getByText("Cost Velocity (Top Movers)", { exact: false }).first();
  const scrollSamples = await smoothScrollTo(title, sceneId, captureSpec, speed);
  const chart = chartForTitle(title);
  const tooltipText = await requireTooltip(page, pointer, chart, sceneId, "+$49,000.00 increase", 0.887, 0.228, captureSpec);
  await page.waitForTimeout(0.5 * speed * 1000);
  if (!tooltipText.includes("2026-08-31")) {
    fail(`scene ${sceneId} tooltip does not contain 2026-08-31`);
  }
  await assertFramed(title, sceneId, captureSpec, "Cost Velocity title");
  await assertFramed(chart, sceneId, captureSpec, "Cost Velocity target");
  return {
    scrollSamples,
    evidence: { moversTooltip: "2026-08-31 +$49,000.00 increase" },
  };
}

/**
 * @param {unknown} value
 * @returns {string | null}
 */
function displayedDate(value) {
  const text = String(value);
  if (text.includes("2026-08-30") || /(?:^|\D)8\/30\/2026(?:\D|$)/.test(text)) return "2026-08-30";
  if (text.includes("2026-08-31") || /(?:^|\D)8\/31\/2026(?:\D|$)/.test(text)) return "2026-08-31";
  return null;
}

/**
 * @param {unknown} value
 * @returns {string | null}
 */
function amountValue(value) {
  return formattedCurrencyAmount(value);
}

/**
 * @param {CapturePage} page
 * @param {PointerController} pointer
 * @param {string} sceneId
 * @param {CaptureSpec} captureSpec
 * @param {number} speed
 * @returns {Promise<CaptureSceneActionResult>}
 */
async function actionTopicTable(page, pointer, sceneId, captureSpec, speed) {
  const dateRange = page.getByText("Date Range", { exact: true }).first();
  const scrollSamples = await smoothScrollTo(dateRange, sceneId, captureSpec, speed, "up");
  await assertFramed(dateRange, sceneId, captureSpec, "Date Range evidence");
  const table = page.getByText("Table", { exact: true }).first();
  await pointer.click(table, sceneId);
  const dateFilters = { start_date: "2026-08-30", end_date: "2026-08-31" };
  const dateResponse = page.waitForResponse(
    (response) => {
      try {
        const url = new URL(response.url());
        return (
          response.ok() &&
          url.origin === UI_ORIGIN &&
          url.pathname.endsWith("/topic-attributions") &&
          Object.entries(dateFilters).every(([key, value]) => url.searchParams.get(key) === value)
        );
      } catch {
        return false;
      }
    },
    { timeout: 45_000 },
  );
  const dateNavigation = waitForTopicFilters(page, dateFilters);
  const dateInputs = page.locator('input[placeholder="Start date"], input[placeholder="End date"]');
  const start = dateInputs.first();
  const end = dateInputs.last();
  const startPicker = start.locator(
    "xpath=ancestor::*[contains(concat(' ', normalize-space(@class), ' '), ' ant-picker ')][1]",
  );
  const endPicker = end.locator(
    "xpath=ancestor::*[contains(concat(' ', normalize-space(@class), ' '), ' ant-picker ')][1]",
  );
  await start.waitFor({ state: "visible", timeout: 45_000 });
  await end.waitFor({ state: "visible", timeout: 45_000 });
  await pointer.click(start, sceneId);
  await start.fill("2026-08-30");
  if (typeof start.press === "function") await start.press("Enter");
  await pointer.click(end, sceneId);
  await end.fill("2026-08-31");
  if (typeof end.press === "function") await end.press("Enter");
  // DatePicker keeps its popup open after an input commit in the real UI;
  // close it before checking that the newly scoped grid rows have rendered.
  if (typeof end.press === "function") await end.press("Escape");
  const topicHeading = page.getByRole("heading", { name: "Topic Attribution", exact: true }).first();
  await pointer.click(topicHeading, sceneId);
  await page.locator(".ant-picker-dropdown").waitFor({ state: "hidden", timeout: 45_000 });
  await Promise.all([dateResponse, dateNavigation]);
  const dateCells = page.locator('[role="gridcell"][col-id="timestamp"]');
  const amountCells = page.locator('[role="gridcell"][col-id="amount"]');
  await dateCells.first().waitFor({ state: "visible", timeout: 45_000 });
  await page.waitForFunction(
    ({ dateSelector, amountSelector }) => {
      if (typeof document === "undefined" || typeof document.querySelectorAll !== "function") return true;
      const dateNodes = [...document.querySelectorAll(dateSelector)];
      const amountNodes = [...document.querySelectorAll(amountSelector)];
      const hasDate = (value, month, day) => String(value).includes(`${month}/${day}/2026`);
      const hasAmount = (value, amount) => String(value).includes(amount);
      return (
        dateNodes.some((node, index) => hasDate(node.textContent, 8, 30) && hasAmount(amountNodes[index]?.textContent, "$1000.00")) &&
        dateNodes.some((node, index) => hasDate(node.textContent, 8, 31) && hasAmount(amountNodes[index]?.textContent, "$50000.00"))
      );
    },
    {
      dateSelector: '[role="gridcell"][col-id="timestamp"]',
      amountSelector: '[role="gridcell"][col-id="amount"]',
    },
    { timeout: 45_000 },
  );
  const dates = await dateCells.allTextContents();
  const amounts = await amountCells.allTextContents();
  /** @type {[string, string][]} */
  const rows = [];
  for (let index = 0; index < Math.max(dates.length, amounts.length); index += 1) {
    const dateValue = displayedDate(dates[index] ?? amounts[index] ?? "");
    const amount = amountValue(amounts[index] ?? dates[index] ?? "");
    if (dateValue && amount) rows.push([dateValue, amount]);
  }
  const expectedRows = new Set([
    "2026-08-30|$1,000.00",
    "2026-08-31|$50,000.00",
  ]);
  const actualRows = new Set(rows.map(([date, amount]) => `${date}|${amount}`));
  for (const row of expectedRows) {
    if (!actualRows.has(row)) fail(`scene ${sceneId} table is missing the expected row ${row.replace("|", " ")}`);
  }
  await assertFramed(start, sceneId, captureSpec, "start date evidence");
  await assertFramed(end, sceneId, captureSpec, "end date evidence");
  await assertFramed(startPicker, sceneId, captureSpec, "start date target");
  await assertFramed(endPicker, sceneId, captureSpec, "end date target");
  if (actualRows.size !== expectedRows.size || rows.length !== expectedRows.size) {
    fail(`scene ${sceneId} table contains unexpected date or amount rows`);
  }
  for (const [index, [date, amount]] of rows.entries()) {
    const dateCell = dateCells.nth(index);
    const amountCell = amountCells.nth(index);
    await dateCell.waitFor({ state: "visible", timeout: 45_000 });
    await amountCell.waitFor({ state: "visible", timeout: 45_000 });
    await assertFramed(dateCell, sceneId, captureSpec, `table ${date} date evidence`);
    await assertFramed(amountCell, sceneId, captureSpec, `table ${date} amount evidence`);
  }
  return {
    scrollSamples,
    evidence: { tableRows: rows },
  };
}

/**
 * @param {CapturePage} page
 * @param {PointerController} pointer
 * @param {string} sceneId
 * @param {CaptureSpec} captureSpec
 * @param {number} speed
 * @returns {Promise<CaptureSceneActionResult>}
 */
async function actionPipelineStatus(page, pointer, sceneId, captureSpec, speed) {
  const completed = page
    .getByText(`Completed at ${APPROVED_PIPELINE_RUN.completedAt}`, { exact: true })
    .first();
  await completed.waitFor({ state: "visible", timeout: 45_000 });
  await assertFramed(completed, sceneId, captureSpec, "pipeline completion evidence");

  for (const stage of ["Gathering", "Calculating", "Topic Attribution Stage", "Emitting"]) {
    const stageLocator = page.getByText(stage, { exact: true }).first();
    await stageLocator.waitFor({ state: "visible", timeout: 45_000 });
    await assertFramed(stageLocator, sceneId, captureSpec, `${stage} stage evidence`);
  }

  const summaryHeading = page.getByText("Last Run Summary", { exact: true }).first();
  await summaryHeading.waitFor({ state: "visible", timeout: 45_000 });
  const summaryCard = cardForLabel(summaryHeading);
  await assertFramed(summaryHeading, sceneId, captureSpec, "last run summary heading");
  await assertFramed(summaryCard, sceneId, captureSpec, "last run summary card");
  const summary = await summaryCard.innerText();
  for (const [label, value] of [
    ["Completed At", APPROVED_PIPELINE_RUN.completedAt],
    ["Dates Gathered", APPROVED_PIPELINE_RUN.datesGathered],
    ["Dates Calculated", APPROVED_PIPELINE_RUN.datesCalculated],
    ["Chargeback Rows Written", APPROVED_PIPELINE_RUN.chargebackRowsWritten],
  ]) {
    if (!summary.includes(label) || !summary.includes(value)) {
      fail(`scene ${sceneId} last run summary is missing ${label} ${value}`);
    }
  }
  await pointer.hover(completed, sceneId);

  const statusHeading = page.getByText("Per-Date Processing Status", { exact: true }).first();
  const scrollSamples = await smoothScrollTo(statusHeading, sceneId, captureSpec, speed);
  await assertFramed(statusHeading, sceneId, captureSpec, "per-date status heading");

  const dateCells = page.locator('[role="gridcell"][col-id="tracking_date"]');
  await dateCells.first().waitFor({ state: "visible", timeout: 45_000 });
  const visibleDateCount = await dateCells.count();
  if (visibleDateCount < 2) {
    fail(`scene ${sceneId} per-date status grid has too few visible dates`);
  }
  const latestDate = await dateCells.first().innerText();
  if (!latestDate.includes("2026-08-31")) {
    fail(`scene ${sceneId} per-date status grid does not show the anchor date`);
  }

  // The 150% capture frame cannot display the final narrow column edge to edge;
  // verify the first three visible status columns and point at their complete
  // grid cell so the success icon remains visible in playback.
  const visibleStatusColumns = ["billing_gathered", "resources_gathered", "chargeback_calculated"];
  for (const column of visibleStatusColumns) {
    const checks = page.locator(
      `[role="gridcell"][col-id="${column}"] [role="img"][aria-label="check-circle"]`,
    );
    if ((await checks.count()) < visibleDateCount) {
      fail(`scene ${sceneId} per-date ${column} status is not successful for every visible date`);
    }
    const firstStatusCell = page.locator(`[role="gridcell"][col-id="${column}"]`).first();
    await firstStatusCell.waitFor({ state: "visible", timeout: 45_000 });
    await assertFramed(firstStatusCell, sceneId, captureSpec, `per-date ${column} success target`);
  }
  const firstStatusCell = page.locator('[role="gridcell"][col-id="billing_gathered"]').first();
  await assertFramed(firstStatusCell, sceneId, captureSpec, "per-date success target");
  await pointer.hover(firstStatusCell, sceneId);
  return { scrollSamples };
}

/**
 * @param {CapturePage} page
 * @param {PointerController} pointer
 * @param {string} sceneId
 * @param {CaptureSpec} captureSpec
 * @param {number} speed
 * @returns {Promise<CaptureSceneActionResult>}
 */
async function actionFocusExport(page, pointer, sceneId, captureSpec, speed) {
  const request = page.locator('section[aria-label^="Preview request "]').first();
  await request.waitFor({ state: "visible", timeout: 45_000 });
  const status = request.getByText("Status ready", { exact: false }).first();
  await status.waitFor({ state: "visible", timeout: 45_000 });
  const statusText = await status.innerText();
  if (!statusText.includes("Status ready")) {
    fail(`scene ${sceneId} FOCUS preview request is not ready`);
  }
  const month = request.getByText("monthly 2026-08", { exact: false }).first();
  await month.waitFor({ state: "visible", timeout: 45_000 });

  const scrollSamples = await smoothScrollTo(request, sceneId, captureSpec, speed);
  await assertFramed(status, sceneId, captureSpec, "FOCUS ready status");
  await assertFramed(month, sceneId, captureSpec, "FOCUS August scope");

  const manifest = request.getByRole("button", { name: "Download manifest.json", exact: true });
  const downloadAll = request.getByRole("button", { name: "Download All", exact: true });
  await manifest.waitFor({ state: "visible", timeout: 45_000 });
  await downloadAll.waitFor({ state: "visible", timeout: 45_000 });
  await assertFramed(manifest, sceneId, captureSpec, "FOCUS manifest download target");
  await assertFramed(downloadAll, sceneId, captureSpec, "FOCUS archive download target");

  const artifactResponse = page.waitForResponse(
    (response) => {
      try {
        const url = new URL(response.url());
        return (
          response.ok() &&
          response.request().method() === "GET" &&
          url.origin === UI_ORIGIN &&
          url.pathname.includes("/focus-preview/requests/") &&
          url.pathname.endsWith("/manifest")
        );
      } catch {
        return false;
      }
    },
    { timeout: 45_000 },
  );
  await pointer.click(manifest, sceneId);
  await artifactResponse;
  return { scrollSamples };
}

/**
 * Dispatch one named storyboard scene to its bounded production handler.
 * @param {CapturePage} page
 * @param {PointerController} pointer
 * @param {CaptureStoryboardScene} scene
 * @param {CaptureSpec} captureSpec
 * @param {number} speed
 * @param {CaptureMode} mode
 * @returns {Promise<CaptureSceneActionResult>}
 */
async function runSceneAction(page, pointer, scene, captureSpec, speed, mode) {
  switch (scene.id) {
    case "dashboard-summary":
      return actionDashboardSummary(page, pointer, scene.id, captureSpec);
    case "dashboard-cost-trend":
      return actionDashboardTrend(page, pointer, scene.id, captureSpec, speed);
    case "explorer-commerce":
      return actionExplorerCommerce(page, pointer, scene.id, captureSpec, speed);
    case "explorer-customer-kafka":
      return actionExplorerCustomer(page, pointer, scene.id, captureSpec, speed);
    case "topic-topics":
      return actionTopicTopics(page, pointer, scene.id, captureSpec, speed);
    case "topic-filters":
      return actionTopicFilters(page, pointer, scene.id, captureSpec, speed, mode);
    case "topic-composition":
      return actionTopicComposition(page, pointer, scene.id, captureSpec, speed);
    case "topic-movers":
      return actionTopicMovers(page, pointer, scene.id, captureSpec, speed);
    case "topic-table":
      return actionTopicTable(page, pointer, scene.id, captureSpec, speed);
    case "pipeline-status":
      return actionPipelineStatus(page, pointer, scene.id, captureSpec, speed);
    case "focus-export":
      return actionFocusExport(page, pointer, scene.id, captureSpec, speed);
    default:
      fail(`scene ${scene.id} has no approved capture handler`);
  }
}

/**
 * @param {CapturePage} page
 * @param {CaptureSpec} captureSpec
 * @param {ReadonlySet<string>} catalogStrings
 * @param {ReadonlySet<string>} catalogTopicNames
 * @param {string[]} errors
 * @param {Set<string>} runtimePreviewIds
 * @param {Set<CaptureRequest>} inFlightRequests
 * @param {Promise<unknown>[]} responsePromises
 * @returns {Promise<void>}
 */
async function validateVideoScene(
  page,
  captureSpec,
  catalogStrings,
  catalogTopicNames,
  errors,
  runtimePreviewIds,
  inFlightRequests,
  responsePromises,
) {
  await waitForSceneQuiescence(inFlightRequests, responsePromises);
  await Promise.all(responsePromises);
  const bodyText = await page.locator("body").innerText();
  for (const token of sourceIdentifierTokens(bodyText, runtimePreviewIds)) {
    if (!catalogStrings.has(token)) errors.push(`DOM identifier is not in the synthetic catalog: ${token}`);
  }
  await validateDomTopicNames(page, catalogTopicNames, errors);
}

/**
 * @param {string} filename
 * @returns {Promise<boolean>}
 */
async function pathExists(filename) {
  try {
    await access(filename);
    return true;
  } catch (error) {
    if (error?.code === "ENOENT") return false;
    throw error;
  }
}

/**
 * Persist a capture artifact by replacing the destination in one rename.
 * @param {string} filename
 * @param {string} content
 * @returns {Promise<void>}
 */
async function writeFileAtomic(filename, content) {
  const temporary = path.join(path.dirname(filename), `.${path.basename(filename)}.${process.pid}.tmp`);
  try {
    await writeFile(temporary, content, "utf8");
    await rename(temporary, filename);
  } finally {
    if (await pathExists(temporary)) await unlink(temporary);
  }
}

/**
 * Require the full walkthrough's claims to come from scene handlers.
 * @param {CaptureSceneEvidence} evidence
 * @returns {{topic_tooltip: string, movers_tooltip: string, table_rows: [string, string][]}}
 */
function requiredFullEvidence(evidence) {
  const { topicTooltip, moversTooltip, tableRows } = evidence;
  if (typeof topicTooltip !== "string" || topicTooltip.length === 0) {
    fail("full capture did not record topic tooltip evidence");
  }
  if (typeof moversTooltip !== "string" || moversTooltip.length === 0) {
    fail("full capture did not record movers tooltip evidence");
  }
  if (
    !Array.isArray(tableRows) ||
    tableRows.length !== 2 ||
    tableRows.some(
      (row) => !Array.isArray(row) || row.length !== 2 || row.some((value) => typeof value !== "string" || value.length === 0),
    )
  ) {
    fail("full capture did not record table row evidence");
  }
  return { topic_tooltip: topicTooltip, movers_tooltip: moversTooltip, table_rows: tableRows };
}

/**
 * Capture one approved storyboard through the supplied Playwright boundary.
 * @param {CaptureRunInput} input
 * @returns {Promise<CaptureRunResult>}
 */
export async function runCapture({ captureSpec, mode, browser, mediaRoot, catalog }) {
  if (!(["full", "draft"].includes(mode))) fail("mode must be full or draft");
  assertSpecification(captureSpec);
  const outputRoot = path.resolve(mediaRoot);
  const assetsRoot = path.join(outputRoot, "assets");
  const workRoot = path.join(outputRoot, "work");
  await mkdir(assetsRoot, { recursive: true });
  await mkdir(workRoot, { recursive: true });
  const catalogStrings = collectStrings(catalog);
  const catalogTopicNames = collectTopicNames(catalog?.scenarios);
  const observations = [];
  const errors = [];
  const responsePromises = [];
  const runtimePreviewIds = new Set();
  const inFlightRequests = new Set();
  const monitorFinalizers = [];
  let monitorsFinalized = false;
  let apiIdentifiersMatchCatalog = true;
  let domIdentifiersMatchCatalog = true;
  try {
    const stillContext = await browser.newContext({
      viewport: captureSpec.viewport,
      timezoneId: "UTC",
      locale: "en-US",
      colorScheme: "dark",
      reducedMotion: "reduce",
      storageState: { cookies: [], origins: [] },
    });
    try {
      const stillPage = await stillContext.newPage();
      monitorFinalizers.push(monitorPage(
        stillPage,
        observations,
        catalogStrings,
        catalogTopicNames,
        errors,
        responsePromises,
        runtimePreviewIds,
        inFlightRequests,
      ));
      await captureStillScenes(
        stillPage,
        outputRoot,
        captureSpec,
        catalogStrings,
        catalogTopicNames,
        errors,
        runtimePreviewIds,
        inFlightRequests,
        responsePromises,
      );
    } finally {
      await stillContext.close();
    }

    const videoContext = await browser.newContext({
      viewport: { width: captureSpec.viewport.width, height: captureSpec.video.content_height },
      recordVideo: {
        dir: workRoot,
        size: { width: captureSpec.viewport.width, height: captureSpec.video.content_height },
      },
      timezoneId: "UTC",
      locale: "en-US",
      colorScheme: "dark",
      reducedMotion: "no-preference",
    });
    /** @type {CaptureStoryboardResult} */
    let captureResult;
    let videoContextClosed = false;
    try {
      const videoPage = await videoContext.newPage();
      monitorFinalizers.push(monitorPage(
        videoPage,
        observations,
        catalogStrings,
        catalogTopicNames,
        errors,
        responsePromises,
        runtimePreviewIds,
        inFlightRequests,
      ));
      const pointer = await new PointerController(videoContext, videoPage, APPROVED_SPEED).start();
      captureResult = await captureStoryboard(
        videoContext,
        videoPage,
        pointer,
        workRoot,
        captureSpec,
        mode,
        catalogStrings,
        catalogTopicNames,
        observations,
        errors,
        responsePromises,
        runtimePreviewIds,
        inFlightRequests,
      );
      videoContextClosed = true;
    } finally {
      if (!videoContextClosed) await videoContext.close();
    }
    await Promise.all(responsePromises);
    monitorFinalizers.forEach((finalize) => finalize());
    monitorsFinalized = true;
    apiIdentifiersMatchCatalog = !errors.some(
      (error) => error.startsWith("API identifier") || error.startsWith("API topic_name") || error.startsWith("API runtime"),
    );
    domIdentifiersMatchCatalog = !errors.some(
      (error) => error.startsWith("DOM identifier") || error.startsWith("DOM topic_name"),
    );
    if (errors.length) fail(`capture ${mode} failed:\n${errors.join("\n")}`);
    const timelinePath = path.join(workRoot, "edit-timeline.json");
    await writeFileAtomic(timelinePath, `${JSON.stringify(captureResult.timeline, null, 2)}\n`);
    return {
      mode,
      timelinePath,
      rawVideoPath: captureResult.rawVideoPath,
      outputPath: path.join(outputRoot, APPROVED_OUTPUT_PATHS[mode]),
    };
  } finally {
    if (!monitorsFinalized) {
      monitorFinalizers.forEach((finalize) => finalize());
      monitorsFinalized = true;
    }
    await writeFile(
      path.join(workRoot, "browser-observations.json"),
      `${JSON.stringify(
        {
          ui_origin: UI_ORIGIN,
          requests: observations,
          api_identifiers_match_catalog: apiIdentifiersMatchCatalog,
          dom_identifiers_match_catalog: domIdentifiersMatchCatalog,
        },
        null,
        2,
      )}\n`,
      "utf8",
    );
  }
}

/**
 * Run the selected storyboard and return its marker-relative timeline.
 * @param {CaptureBrowserContext} context
 * @param {CapturePage} page
 * @param {PointerController} pointer
 * @param {string} workRoot
 * @param {CaptureSpec} captureSpec
 * @param {CaptureMode} mode
 * @param {ReadonlySet<string>} catalogStrings
 * @param {ReadonlySet<string>} catalogTopicNames
 * @param {CaptureObservation[]} observations
 * @param {string[]} errors
 * @param {Promise<unknown>[]} responsePromises
 * @param {Set<string>} runtimePreviewIds
 * @param {Set<CaptureRequest>} inFlightRequests
 * @returns {Promise<CaptureStoryboardResult>}
 */
async function captureStoryboard(
  context,
  page,
  pointer,
  workRoot,
  captureSpec,
  mode,
  catalogStrings,
  catalogTopicNames,
  observations,
  errors,
  responsePromises,
  runtimePreviewIds,
  inFlightRequests,
) {
  const speed = APPROVED_SPEED;
  const storyboard = APPROVED_STORYBOARDS[mode];
  const recording = typeof page.video === "function" ? page.video() : null;

  // Navigation and the first route's requests are recorder pre-roll. Settle
  // that route before painting the opening marker so the first scene starts
  // at the marker-derived origin rather than on a blank page.
  const firstScene = storyboard[0];
  await waitForScene(page, firstScene, captureSpec, { video: true, navigate: true });
  await waitForSceneQuiescence(inFlightRequests, responsePromises);
  await Promise.all(responsePromises);
  await selectVideoTheme(page);
  let currentRoute = scenePath(firstScene);

  await showSyncMarker(page);
  // Give the recorder a rendered marker frame before starting its fixed hold.
  await nextAnimationFrame(page);
  await page.waitForTimeout(MARKER_DURATION_SECONDS * 1000);
  await hideSyncMarker(page);
  // nextAnimationFrame() and the page's RAF timestamp are deliberately not
  // used as the timeline clock. This is the Node monotonic clock, shared by
  // every navigation and browser wait in this capture.
  const storyOrigin = performance.now();
  const timelineScenes = [];
  const evidence = {};
  for (const [sceneIndex, scene] of storyboard.entries()) {
    try {
      const startClock = sceneIndex === 0 ? storyOrigin : performance.now();
      const navigate = currentRoute !== scenePath(scene);
      if (sceneIndex !== 0) {
        await waitForScene(page, scene, captureSpec, { video: true, navigate });
      }
      currentRoute = scenePath(scene);
      const action = await runSceneAction(page, pointer, scene, captureSpec, speed, mode);
      await validateVideoScene(
        page,
        captureSpec,
        catalogStrings,
        catalogTopicNames,
        errors,
        runtimePreviewIds,
        inFlightRequests,
        responsePromises,
      );
      const actionCompleteClock = performance.now();
      const actionElapsed = (actionCompleteClock - startClock) / 1000;
      const actionLimit = Number(scene.max_action_seconds) * speed + FRAME_TOLERANCE_SECONDS * speed;
      if (actionElapsed > actionLimit) {
        fail(`scene ${mode} ${scene.id} action overrun (${actionElapsed.toFixed(3)} seconds)`);
      }
      await page.waitForTimeout(Number(scene.read_seconds) * speed * 1000);
      const endClock = performance.now();
      timelineScenes.push({
        id: scene.id,
        start_seconds: (startClock - storyOrigin) / 1000,
        action_complete_seconds: (actionCompleteClock - storyOrigin) / 1000,
        end_seconds: (endClock - storyOrigin) / 1000,
        scroll_samples: action.scrollSamples ?? [],
      });
      if (action.evidence) Object.assign(evidence, action.evidence);
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      if (message.includes(`scene ${mode} ${scene.id}`)) throw error;
      fail(`scene ${mode} ${scene.id} ${message}`);
    }
  }
  await showSyncMarker(page);
  await page.waitForTimeout(MARKER_DURATION_SECONDS * 1000);
  await hideSyncMarker(page);
  // Let the recorder receive a post-marker frame before closing the context;
  // otherwise Chromium can repeat the final marker frame in its tail flush.
  await nextAnimationFrame(page);
  await nextAnimationFrame(page);
  /** @type {[CaptureMarkerRun, CaptureMarkerRun]} */
  const markerRuns = [
    { duration_seconds: MARKER_DURATION_SECONDS, frames: MARKER_FRAMES },
    { duration_seconds: MARKER_DURATION_SECONDS, frames: MARKER_FRAMES },
  ];
  const timeline = {
    mode,
    speed,
    markers: {
      color: MARKER_COLOR,
      plane_average_tolerance: 8,
      within_plane_spread: 12,
      runs: markerRuns,
    },
    scenes: timelineScenes,
    framing: {
      zoom_percent: APPROVED_VIDEO.content_zoom_percent,
      minimum_playback_text_pixels: 11,
      minimum_playback_target_pixels: 24,
    },
    ...(mode === "full" ? { evidence: requiredFullEvidence(evidence) } : {}),
  };
  const videoName = captureSpec.video.name.replace(/\.mp4$/, ".webm");
  const rawVideoPath = path.join(workRoot, videoName);
  await context.close();
  if (recording) {
    const source = await recording.path();
    if (await pathExists(source)) await rename(source, rawVideoPath);
  }
  return { timeline, rawVideoPath };
}

/** @returns {Promise<void>} */
async function main() {
  const args = parseArguments(process.argv.slice(2));
  const captureSpec = /** @type {CaptureSpec} */ (await readJson(args.spec, "capture specification"));
  assertSpecification(captureSpec);
  const catalog = /** @type {CaptureCatalog} */ (await readJson(args.catalog, "synthetic catalog"));
  const { chromium } = await import("playwright-core");
  const browser = await chromium.launch({ headless: true, args: ["--no-sandbox"] });
  try {
    await runCapture({
      captureSpec,
      mode: args.mode,
      browser,
      mediaRoot: args.output,
      catalog,
    });
  } finally {
    await browser.close();
  }
}

const isMainModule = process.argv[1] && pathToFileURL(path.resolve(process.argv[1])).href === import.meta.url;
if (isMainModule) {
  main().catch((error) => {
    console.error(`Demo media browser capture failed: ${error.message}`);
    process.exitCode = 1;
  });
}
