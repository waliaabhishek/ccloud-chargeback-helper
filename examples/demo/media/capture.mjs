#!/usr/bin/env node

import { mkdir, readFile, rename, writeFile } from "node:fs/promises";
import path from "node:path";

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
const APPROVED_CAPTIONS = [
  { start: 0, end: 13 },
  { start: 13, end: 30 },
  { start: 30, end: 44 },
  { start: 44, end: 56 },
  { start: 56, end: 75 },
];
const APPROVED_VIDEO_NAME = "chitragupta-demo-walkthrough.mp4";
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

function fail(message) {
  throw new Error(message);
}

function parseArguments(argv) {
  const args = {};
  for (let index = 0; index < argv.length; index += 1) {
    const argument = argv[index];
    if (!["--spec", "--catalog", "--output"].includes(argument)) {
      fail(`unknown argument: ${argument}`);
    }
    if (index + 1 >= argv.length) {
      fail(`missing value for ${argument}`);
    }
    args[argument.slice(2)] = argv[index + 1];
    index += 1;
  }
  if (!args.spec || !args.catalog || !args.output || Object.keys(args).length !== 3) {
    fail("usage: capture.mjs --spec SPEC_PATH --catalog CATALOG_PATH --output MEDIA_ROOT");
  }
  return args;
}

async function readJson(filename, label) {
  try {
    return JSON.parse(await readFile(filename, "utf8"));
  } catch (error) {
    fail(`${label} is not valid JSON: ${error.message}`);
  }
}

function assertSpecification(spec) {
  if (
    spec.schema_version !== 1 ||
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
  if (spec.screenshots?.length !== APPROVED_SCREENSHOTS.length || spec.captions?.length !== APPROVED_CAPTIONS.length) {
    fail("capture specification does not contain the approved storyboard");
  }
  if (
    spec.screenshots.some(
      (screenshot, index) =>
        screenshot.name !== APPROVED_SCREENSHOTS[index].name || screenshot.route !== APPROVED_SCREENSHOTS[index].route,
    )
  ) {
    fail("capture specification does not contain the approved storyboard");
  }
  if (
    spec.captions.some(
      (caption, index) => caption.start !== APPROVED_CAPTIONS[index].start || caption.end !== APPROVED_CAPTIONS[index].end,
    )
  ) {
    fail("capture specification caption timing does not match the approved storyboard");
  }
  if (
    spec.video?.name !== APPROVED_VIDEO_NAME ||
    spec.video?.target_seconds !== 75 ||
    spec.video?.minimum_seconds !== 60 ||
    spec.video?.maximum_seconds !== 90
  ) {
    fail("capture specification does not contain the approved video duration");
  }
}

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

function scenePath(scene) {
  return scene.route.split("?", 1)[0];
}

function dateDaysBefore(anchorDate, days) {
  const date = new Date(`${anchorDate}T00:00:00Z`);
  date.setUTCDate(date.getUTCDate() - days);
  return date.toISOString().slice(0, 10);
}

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
  const recordRequest = (request, failed = false) => {
    const url = request.url();
    const kind = request.resourceType() || "request";
    observations.push({ url, kind, ...(failed ? { failed: true } : {}) });
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
  };
  page.on("request", (request) => {
    inFlightRequests.add(request);
    recordRequest(request);
  });
  page.on("requestfinished", (request) => inFlightRequests.delete(request));
  page.on("requestfailed", (request) => {
    inFlightRequests.delete(request);
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
            collectRuntimePreviewIds(body, runtimePreviewIds, invalidRuntimePreviewIds, catalogStrings);
            for (const identifier of invalidRuntimePreviewIds) {
              errors.push(`API runtime Preview identifier is malformed: ${identifier}`);
            }
            for (const topicName of collectTopicNames(body)) {
              if (!catalogTopicNames.has(topicName)) {
                errors.push(`API topic_name is not in the synthetic catalog: ${topicName}`);
              }
            }
            for (const token of sourceIdentifierTokens(body, runtimePreviewIds)) {
              if (!catalogStrings.has(token)) {
                errors.push(`API identifier is not in the synthetic catalog: ${token}`);
              }
            }
          })
          .catch((error) => {
            errors.push(`JSON response could not be read from ${response.url()}: ${error.message}`);
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
}

async function waitForScene(page, scene, captureSpec) {
  const anchorDate = captureSpec.anchor_date;
  let graphResponse;
  if (scenePath(scene) === "/explorer") {
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
  await page.goto(sceneUrl(scene, anchorDate), { waitUntil: "networkidle" });
  if (graphResponse) {
    await graphResponse;
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
  for (const marker of scene.markers) {
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

async function focusCommerceEnvironment(page) {
  const search = page.getByPlaceholder("Search entities… (⌘K)");
  await search.waitFor({ state: "visible", timeout: 45_000 });
  const searchResponse = page.waitForResponse(
    (response) => {
      try {
        const url = new URL(response.url());
        return (
          response.ok() &&
          url.origin === UI_ORIGIN &&
          url.pathname.endsWith("/graph/search") &&
          url.searchParams.get("q") === "env-commerce"
        );
      } catch {
        return false;
      }
    },
    { timeout: 45_000 },
  );
  await search.fill("env-commerce");
  await searchResponse;
  const result = search.locator("..").getByText("Commerce", { exact: true }).first();
  await result.waitFor({ state: "visible", timeout: 45_000 });
  const focusedGraphResponse = page.waitForResponse(
    (response) => {
      try {
        const url = new URL(response.url());
        return (
          response.ok() &&
          url.origin === UI_ORIGIN &&
          url.pathname.endsWith("/graph") &&
          url.searchParams.get("focus") === "env-commerce"
        );
      } catch {
        return false;
      }
    },
    { timeout: 45_000 },
  );
  await result.click();
  await focusedGraphResponse;
}

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

async function validateDomTopicNames(page, catalogTopicNames, errors) {
  const topicCells = await page.locator('[role="gridcell"][col-id="topic_name"]').allTextContents();
  for (const topicName of topicCells.map((value) => value.trim()).filter(Boolean)) {
    if (!catalogTopicNames.has(topicName)) {
      errors.push(`DOM topic_name is not in the synthetic catalog: ${topicName}`);
    }
  }
}

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

async function captureVideo(
  browser,
  workRoot,
  captureSpec,
  catalogStrings,
  catalogTopicNames,
  observations,
  errors,
  responsePromises,
  runtimePreviewIds,
  inFlightRequests,
) {
  const context = await browser.newContext({
    viewport: captureSpec.viewport,
    recordVideo: { dir: workRoot, size: captureSpec.viewport },
    timezoneId: "UTC",
    locale: "en-US",
    colorScheme: "dark",
    reducedMotion: "reduce",
  });
  const page = await context.newPage();
  monitorPage(
    page,
    observations,
    catalogStrings,
    catalogTopicNames,
    errors,
    responsePromises,
    runtimePreviewIds,
    inFlightRequests,
  );
  try {
    const recordingStarted = Date.now();
    const finalCaption = captureSpec.captions[captureSpec.captions.length - 1];
    if (finalCaption.end !== captureSpec.video.target_seconds) {
      fail("capture caption timing does not reach the approved video target");
    }
    for (const [index, interval] of captureSpec.captions.entries()) {
      const scene = { ...captureSpec.screenshots[index], ...SCENE_MARKERS[index] };
      await waitForScene(page, scene, captureSpec);
      if (index === 1) {
        await focusCommerceEnvironment(page);
      }
      if (index === 2) {
        await prepareTopicAttribution(page);
      }
      if (index === 4) {
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
      const remaining = interval.end * 1000 - (Date.now() - recordingStarted);
      if (remaining > 0) {
        await page.waitForTimeout(remaining);
      }
    }
  } finally {
    const recording = page.video();
    await context.close();
    if (recording) {
      const source = await recording.path();
      const rawVideoName = captureSpec.video.name.replace(/\.mp4$/, ".webm");
      await rename(source, path.join(workRoot, rawVideoName));
    }
  }
}

async function main() {
  const args = parseArguments(process.argv.slice(2));
  const captureSpec = await readJson(args.spec, "capture specification");
  const catalog = await readJson(args.catalog, "synthetic catalog");
  assertSpecification(captureSpec);
  const { chromium } = await import("playwright-core");
  const catalogStrings = collectStrings(catalog);
  const catalogTopicNames = collectTopicNames(catalog.scenarios);
  const outputRoot = path.resolve(args.output);
  const assetsRoot = path.join(outputRoot, "assets");
  const workRoot = path.join(outputRoot, "work");
  await mkdir(assetsRoot, { recursive: true });
  await mkdir(workRoot, { recursive: true });
  const observations = [];
  const errors = [];
  const responsePromises = [];
  const runtimePreviewIds = new Set();
  const inFlightRequests = new Set();
  let apiIdentifiersMatchCatalog = true;
  let domIdentifiersMatchCatalog = true;
  let browser;
  try {
    browser = await chromium.launch({ headless: true, args: ["--no-sandbox"] });
    const context = await browser.newContext({
      viewport: captureSpec.viewport,
      timezoneId: "UTC",
      locale: "en-US",
      colorScheme: "dark",
      reducedMotion: "reduce",
      storageState: { cookies: [], origins: [] },
    });
    const page = await context.newPage();
    monitorPage(
      page,
      observations,
      catalogStrings,
      catalogTopicNames,
      errors,
      responsePromises,
      runtimePreviewIds,
      inFlightRequests,
    );
    await captureStillScenes(
      page,
      outputRoot,
      captureSpec,
      catalogStrings,
      catalogTopicNames,
      errors,
      runtimePreviewIds,
      inFlightRequests,
      responsePromises,
    );
    await context.close();
    await captureVideo(
      browser,
      workRoot,
      captureSpec,
      catalogStrings,
      catalogTopicNames,
      observations,
      errors,
      responsePromises,
      runtimePreviewIds,
      inFlightRequests,
    );
    await Promise.all(responsePromises);
    apiIdentifiersMatchCatalog = !errors.some(
      (error) => error.startsWith("API identifier") || error.startsWith("API topic_name"),
    );
    domIdentifiersMatchCatalog = !errors.some(
      (error) => error.startsWith("DOM identifier") || error.startsWith("DOM topic_name"),
    );
    if (errors.length) {
      fail(errors.join("\n"));
    }
  } finally {
    if (browser) {
      await browser.close();
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

main().catch((error) => {
  console.error(`Demo media browser capture failed: ${error.message}`);
  process.exitCode = 1;
});
