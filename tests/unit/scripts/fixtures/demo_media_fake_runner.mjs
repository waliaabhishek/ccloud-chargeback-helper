import fs from "node:fs/promises";
import path from "node:path";
import { pathToFileURL } from "node:url";

let elapsedMilliseconds = 0;
let pageElapsedMilliseconds = 10_000;
let failure = "";
let gridRows = ["2026-08-31 $50,000.00", "2026-08-30 $1,000.00"];
let activePage = null;
const RECORDING_BYTES = Buffer.from("synthetic-browser-recording");
const DASHBOARD_TREND_HIT = { xRatio: 0.95, yRatio: 0.45 };
const TOP_TOPICS_HIT = { xRatio: 0.18, yRatio: 0.36 };
const TOP_MOVERS_HIT = { xRatio: 0.887, yRatio: 0.228 };
const CAPTURE_ZOOM = 1.5;
const UI_ORIGIN = "http://chitragupta-ui";
const PIPELINE_SUMMARY = [
  "Completed At 2026-08-31T23:59:59Z",
  "Dates Gathered 184",
  "Dates Calculated 184",
  "Chargeback Rows Written 11960",
].join(" ");

function requestFor(url, { method = "GET", resourceType = "fetch", postData = null } = {}) {
  return {
    url: () => url,
    resourceType: () => resourceType,
    method: () => method,
    postData: () => postData,
  };
}

function responseFor(request, { status = 200, payload = {} } = {}) {
  return {
    ok: () => status >= 200 && status < 300,
    status: () => status,
    url: request.url,
    headers: () => ({ "content-type": "application/json" }),
    json: async () => {
      activePage?.log.push({ event: "responseJson", url: request.url() });
      return structuredClone(payload);
    },
    request: () => request,
  };
}

function targetBox(name) {
  if (name.includes("text=Top Topics by Cost") && name.includes("canvas")) {
    return { x: 180, y: 150, width: 1160, height: 440 };
  }
  if (name.includes("text=Cost Trend Over Time") && name.includes("canvas")) {
    return { x: 180, y: 150, width: 1160, height: 440 };
  }
  if (name.includes("text=Cost Composition by Product Type") && name.includes("canvas")) {
    return { x: 180, y: 170, width: 1160, height: 420 };
  }
  if (name.includes("text=Cost Velocity (Top Movers)") && name.includes("canvas")) {
    return { x: 180, y: 160, width: 1160, height: 450 };
  }
  if (name.includes("text=$79,000.00")) {
    return { x: 640, y: 400, width: 300, height: 52 };
  }
  if (name.includes("text=+$49,000.00 increase")) {
    return { x: 640, y: 410, width: 340, height: 52 };
  }
  return { x: 80, y: 120, width: 240, height: 72 };
}

function isChartTarget(name, title) {
  return name.includes(`text=${title}`) && name.includes("canvas");
}
Date.now = () => elapsedMilliseconds;
Object.defineProperty(globalThis, "performance", {
  configurable: true,
  value: { now: () => elapsedMilliseconds },
});
const fakeElement = {
  clientHeight: 800,
  clientWidth: 1600,
  scrollHeight: 2400,
  scrollTop: 0,
  style: { fontSize: "14px" },
  textContent: "showcase-live-orders $79,000.00 +$49,000.00 increase 2026-08-31",
  innerText: "clean-confluent (confluent_cloud) env-commerce lkc-customer showcase-live-orders $79,000.00 +$49,000.00 increase 2026-08-31",
  getBoundingClientRect: () => ({ x: 80, y: 120, top: 120, bottom: 420, left: 80, right: 320, width: 240, height: 72 }),
  scrollIntoView: () => {},
};
globalThis.window = {
  innerHeight: 800,
  innerWidth: 1600,
  scrollY: 0,
  getComputedStyle: () => ({ fontSize: "14px" }),
  scrollTo: (_x, y) => { globalThis.window.scrollY = y; },
};
globalThis.requestAnimationFrame = (callback) => {
  pageElapsedMilliseconds += 1000 / 60;
  return callback(pageElapsedMilliseconds);
};
globalThis.window.requestAnimationFrame = globalThis.requestAnimationFrame;
globalThis.setTimeout = (callback, milliseconds = 0) => {
  elapsedMilliseconds += Number(milliseconds);
  callback();
  return 1;
};
globalThis.document = {
  body: fakeElement,
  documentElement: fakeElement,
  getElementById: () => fakeElement,
  createElement: () => fakeElement,
  querySelector: (selector) => ({ value: activePage?.inputValue(selector) ?? "" }),
};

class FakeLocator {
  constructor(name, log) {
    this.name = name;
    this.log = log;
  }

  async click(options = {}) {
    this.log.push({ event: "click", name: this.name, options });
    if (activePage) activePage.handleClick(this.name);
  }

  async fill(value) {
    this.log.push({ event: "fill", name: this.name, value });
    if (activePage) activePage.updateDateInput(this.name, String(value));
  }

  async pressSequentially(value, options = {}) {
    elapsedMilliseconds += Number(options.delay ?? 0) * String(value).length;
    this.log.push({ event: "pressSequentially", name: this.name, value, options });
    await Promise.resolve();
    if (activePage) {
      if (this.name.includes("Search entities")) activePage.searchEntities(String(value));
      else activePage.updateFilter(this.name, String(value));
    }
  }

  async press(key) {
    this.log.push({ event: "press", name: this.name, key });
  }

  async hover(options = {}) {
    this.log.push({ event: "hover", name: this.name, options });
  }

  async waitFor(options = {}) {
    this.log.push({ event: "waitFor", name: this.name, options });
    if (activePage) activePage.assertTooltipVisible(this.name);
  }

  async isVisible() {
    this.log.push({ event: "isVisible", name: this.name });
    return failure !== "offscreen";
  }

  async isEnabled() {
    this.log.push({ event: "isEnabled", name: this.name });
    return true;
  }

  async isDisabled() {
    this.log.push({ event: "isDisabled", name: this.name });
    return this.name.includes("Run Pipeline");
  }

  async boundingBox() {
    this.log.push({ event: "boundingBox", name: this.name });
    if (failure === "offscreen") return null;
    const box = targetBox(this.name);
    if (activePage) activePage.rememberTarget(this.name, box);
    return box;
  }

  async innerText() {
    this.log.push({ event: "innerText", name: this.name });
    if (this.name === "body") {
      return failure === "dom-identifier"
        ? `${fakeElement.innerText} env-unknown`
        : fakeElement.innerText;
    }
    if (this.name.includes("text=$79,000.00")) {
      return failure === "topic-tooltip" ? "$79,000.00" : "showcase-live-orders $79,000.00";
    }
    if (this.name.includes("text=+$49,000.00 increase")) {
      return "2026-08-31 +$49,000.00 increase";
    }
    if (this.name.includes("text=2026-08-31")) {
      return "2026-08-31 $600,362.00";
    }
    if (this.name.includes("Total Cost")) return "Total Cost $600,362.00";
    if (this.name.includes("Usage Cost")) return "Usage Cost $249,532.50";
    if (this.name.includes("Shared Cost")) return "Shared Cost $350,829.50";
    if (this.name.includes("Last Run Summary")) return PIPELINE_SUMMARY;
    if (this.name.includes("tracking_date")) return "2026-08-31";
    if (this.name.includes("Status ready")) return "Status ready";
    if (this.name.includes("monthly 2026-08")) return "monthly 2026-08";
    return "showcase-live-orders KAFKA_REST_PRODUCE $79,000.00 +$49,000.00 increase 2026-08-31";
  }

  async textContent() {
    this.log.push({ event: "textContent", name: this.name });
    return "showcase-live-orders";
  }

  async allTextContents() {
    this.log.push({ event: "allTextContents", name: this.name });
    if (this.name.includes("topic_name")) {
      return [failure === "dom-topic" ? "unknown-topic" : "showcase-live-orders"];
    }
    return gridRows;
  }

  async count() {
    this.log.push({ event: "count", name: this.name });
    return 2;
  }

  first() {
    return new FakeLocator(`${this.name} >> first`, this.log);
  }

  last() {
    return new FakeLocator(`${this.name} >> last`, this.log);
  }

  nth(index) {
    return new FakeLocator(`${this.name} >> nth=${index}`, this.log);
  }

  locator(selector) {
    return new FakeLocator(`${this.name} >> ${selector}`, this.log);
  }

  getByText(text, options = {}) {
    return new FakeLocator(`${this.name} text=${text}`, this.logWith("getByText", text, options));
  }

  getByRole(role, options = {}) {
    return new FakeLocator(`${this.name} role=${role}:${options.name ?? ""}`, this.logWith("getByRole", role, options));
  }

  getByLabel(label, options = {}) {
    return new FakeLocator(`${this.name} label=${label}`, this.logWith("getByLabel", label, options));
  }

  getByPlaceholder(placeholder, options = {}) {
    return new FakeLocator(`${this.name} placeholder=${placeholder}`, this.logWith("getByPlaceholder", placeholder, options));
  }

  getByTestId(testId) {
    return new FakeLocator(`${this.name} testid=${testId}`, this.logWith("getByTestId", testId, {}));
  }

  filter(options = {}) {
    this.log.push({ event: "filter", name: this.name, options });
    return this;
  }

  async evaluate(callback, argument) {
    this.log.push({ event: "locatorEvaluate", name: this.name, argument });
    const targetElement =
      argument?.direction === "up"
        ? {
            ...fakeElement,
            getBoundingClientRect: () => ({ ...fakeElement.getBoundingClientRect(), y: -280, top: -280, bottom: -208 }),
          }
        : {
            ...fakeElement,
            getBoundingClientRect: () => ({ ...fakeElement.getBoundingClientRect(), y: 520, top: 520, bottom: 592 }),
          };
    const nodePerformance = globalThis.performance;
    Object.defineProperty(globalThis, "performance", {
      configurable: true,
      value: { now: () => pageElapsedMilliseconds },
    });
    try {
      return await callback(targetElement, argument);
    } finally {
      Object.defineProperty(globalThis, "performance", { configurable: true, value: nodePerformance });
    }
  }

  logWith(event, value, options) {
    this.log.push({ event, name: this.name, value, options });
    return this.log;
  }
}

class FakePage {
  constructor(context, log) {
    this.context = context;
    this.log = log;
    this.currentUrl = "";
    this.urlWaiters = [];
    this.functionWaiters = [];
    this.responseWaiters = [];
    this.requestWaiters = [];
    this.filterValues = {};
    this.lastTargetName = "";
    this.lastTargetBox = null;
    this.activeTooltip = "";
    this.focusRequestId = "22222222-2222-4222-8222-222222222222";
    this.focusStatusPending = false;
    this.aggregateRejection = null;
    activePage = this;
    this.mouse = {
      move: async (x, y, options = {}) => {
        elapsedMilliseconds += Number(options.duration ?? 0);
        this.log.push({ event: "mouseMove", x, y, options });
        this.updateTooltip(x, y);
      },
      click: async (x, y, options = {}) => {
        elapsedMilliseconds += Number(options.duration ?? 0);
        this.log.push({ event: "mouseClick", x, y, options });
      },
    };
    this.listeners = new Map();
  }

  on(event, listener) {
    this.log.push({ event: "on", value: event });
    const listeners = this.listeners.get(event) ?? [];
    listeners.push(listener);
    this.listeners.set(event, listeners);
  }

  emit(event, value) {
    for (const listener of this.listeners.get(event) ?? []) listener(value);
    if (event === "response") this.resolveWaiters(this.responseWaiters, value, "waitForResponse");
    if (event === "request") this.resolveWaiters(this.requestWaiters, value, "waitForRequest");
  }

  resolveWaiters(waiters, value, event) {
    const remaining = [];
    for (const waiter of waiters) {
      const matched = Boolean(waiter.predicate(value));
      if (matched) {
        this.log.push({ event: `${event}Matched`, url: value.url(), status: value.status?.() });
        waiter.resolve(value);
      } else {
        this.log.push({ event: `${event}Ignored`, url: value.url(), status: value.status?.() });
        remaining.push(waiter);
      }
    }
    waiters.splice(0, waiters.length, ...remaining);
  }

  rememberTarget(name, box) {
    this.lastTargetName = name;
    this.lastTargetBox = box;
  }

  updateTooltip(x, y) {
    this.activeTooltip = "";
    if (!this.lastTargetBox || failure === "tooltip" || failure === "topic-target") return;
    const target = this.lastTargetName;
    let hit;
    if (isChartTarget(target, "Cost Trend Over Time")) hit = DASHBOARD_TREND_HIT;
    if (isChartTarget(target, "Top Topics by Cost")) hit = TOP_TOPICS_HIT;
    if (isChartTarget(target, "Cost Velocity (Top Movers)")) hit = TOP_MOVERS_HIT;
    if (!hit) return;
    const expectedX = this.lastTargetBox.x + (this.lastTargetBox.width * hit.xRatio) / CAPTURE_ZOOM;
    const expectedY = this.lastTargetBox.y + (this.lastTargetBox.height * hit.yRatio) / CAPTURE_ZOOM;
    if (Math.abs(x - expectedX) <= 8 && Math.abs(y - expectedY) <= 8) {
      if (isChartTarget(target, "Cost Trend Over Time")) this.activeTooltip = "trend";
      else if (isChartTarget(target, "Top Topics by Cost")) this.activeTooltip = "topic";
      else this.activeTooltip = "movers";
    }
  }

  assertTooltipVisible(name) {
    if (name.includes("text=$79,000.00") && this.activeTooltip !== "topic") {
      throw new Error("tooltip for $79,000.00 is not visible");
    }
    if (name.includes("text=+$49,000.00 increase") && this.activeTooltip !== "movers") {
      throw new Error("tooltip for +$49,000.00 increase is not visible");
    }
  }

  emitExchange(url, { method = "GET", resourceType = "fetch", postData = null, status = 200, payload } = {}) {
    const request = requestFor(url, { method, resourceType, postData });
    this.emit("request", request);
    const response = responseFor(request, { status, payload: payload ?? this.responsePayload(url) });
    this.emit("response", response);
    this.emit("requestfinished", request);
    return response;
  }

  responsePayload(url) {
    const payload = {
      topic_name: "showcase-live-orders",
      cluster_id: "lkc-customer",
      environment_id: "env-commerce",
      status: "ready",
      request_id: "11111111-1111-4111-8111-111111111111",
    };
    if (failure === "api-topic") payload.topic_name = "unknown-topic";
    if (failure === "bad-preview") payload.calculation_id = "malformed preview identifier";
    return payload;
  }

  topicDataUrl() {
    const url = new URL(this.currentUrl);
    return `${UI_ORIGIN}/api/v1/topic-attributions${url.search}`;
  }

  searchEntities(query) {
    this.emitExchange(`${UI_ORIGIN}/api/v1/graph/search?q=${encodeURIComponent(query)}`);
  }

  handleClick(locatorName) {
    const route = this.currentUrl ? new URL(this.currentUrl).pathname : "";
    if (locatorName.includes("Generate preview")) {
      this.focusStatusPending = true;
      this.emitExchange(`${UI_ORIGIN}/api/v1/focus-preview/requests`, {
        method: "POST",
        status: 202,
        payload: { request_id: this.focusRequestId, status: "queued" },
      });
    } else if (route === "/explorer" && locatorName.includes("text=Commerce")) {
      this.emitExchange(`${UI_ORIGIN}/api/v1/graph?at=2026-08-31T12:00:00Z&focus=env-commerce`);
    } else if (route === "/explorer" && locatorName.includes("text=Customer Kafka")) {
      this.emitExchange(`${UI_ORIGIN}/api/v1/graph?at=2026-08-31T12:00:00Z&focus=lkc-customer`);
    } else if (route === "/topic-attributions" && locatorName.includes("Analytics")) {
      const url = new URL(`${UI_ORIGIN}/api/v1/topic-attributions/aggregate`);
      const current = new URL(this.currentUrl);
      current.searchParams.forEach((value, key) => url.searchParams.set(key, value));
      this.emitExchange(url.toString());
    } else if (locatorName.includes("Download manifest.json")) {
      this.emitExchange(`${UI_ORIGIN}/api/v1/focus-preview/requests/${this.focusRequestId}/manifest`);
    }
  }

  async goto(url, options = {}) {
    this.currentUrl = url;
    globalThis.window.scrollY = 0;
    this.filterValues = {};
    this.notifyUrlWaiters();
    this.context.documentInitializations += this.context.initScripts.length;
    this.log.push({ event: "goto", url, options, initScripts: this.context.initScripts.length });
    const documentRequest = requestFor(url, { resourceType: "document" });
    this.emit("request", documentRequest);
    this.emit("requestfinished", documentRequest);
    const route = new URL(url).pathname;
    let response;
    if (route === "/explorer") {
      response = this.emitExchange(`${UI_ORIGIN}/api/v1/graph?at=2026-08-31T12:00:00Z`);
    } else if (route === "/topic-attributions") {
      response = this.emitExchange(this.topicDataUrl());
    } else {
      response = this.emitExchange(`${UI_ORIGIN}/api/v1${route}`);
    }
    return response;
  }

  video() {
    return {
      path: async () => {
        const recordingPath = this.context.recordingPath;
        await fs.mkdir(path.dirname(recordingPath), { recursive: true });
        await fs.writeFile(recordingPath, RECORDING_BYTES);
        this.log.push({ event: "recordingPath", path: recordingPath, bytes: RECORDING_BYTES.length });
        return recordingPath;
      },
    };
  }

  async waitForTimeout(milliseconds) {
    elapsedMilliseconds += failure === "overrun" ? milliseconds * 100 : milliseconds;
    this.log.push({ event: "waitForTimeout", milliseconds });
  }

  async waitForResponse(predicate, options = {}) {
    this.log.push({ event: "waitForResponse", options });
    if (failure === "aggregate-rejection") {
      const expectedAggregate = responseFor(
        requestFor(
          `${UI_ORIGIN}/api/v1/topic-attributions/aggregate?start_date=2026-08-02&end_date=2026-08-31&timezone=UTC&cluster_resource_id=lkc-customer&topic_name=showcase-live-orders`,
        ),
      );
      if (predicate(expectedAggregate)) {
        return new Promise((_resolve, reject) => {
          this.aggregateRejection = reject;
        });
      }
    }
    const responsePromise = new Promise((resolve) => this.responseWaiters.push({ predicate, resolve }));
    if (this.focusStatusPending) {
      this.focusStatusPending = false;
      this.emitExchange(`${UI_ORIGIN}/api/v1/focus-preview/requests/${this.focusRequestId}`, {
        payload: { request_id: this.focusRequestId, status: "ready" },
      });
    }
    return responsePromise;
  }

  async waitForURL(predicate, options = {}) {
    this.log.push({ event: "waitForURL", options });
    const url = new URL(this.currentUrl);
    if (predicate(url)) return;
    await new Promise((resolve) => this.urlWaiters.push({ predicate, resolve }));
  }

  async waitForFunction(predicate, argument, options = {}) {
    this.log.push({ event: "waitForFunction", options, argument });
    if (predicate(argument)) return;
    await new Promise((resolve) => this.functionWaiters.push({ predicate, argument, resolve }));
  }

  updateFilter(locatorName, value) {
    if (!this.currentUrl) return;
    const url = new URL(this.currentUrl);
    if (locatorName.includes("Any cluster")) {
      this.filterValues.cluster_resource_id = `${this.filterValues.cluster_resource_id ?? ""}${value}`;
      url.searchParams.set("cluster_resource_id", this.filterValues.cluster_resource_id);
    }
    if (locatorName.includes("Any topic")) {
      this.filterValues.topic_name = `${this.filterValues.topic_name ?? ""}${value}`;
      url.searchParams.set("topic_name", this.filterValues.topic_name);
    }
    this.currentUrl = url.toString();
    this.notifyUrlWaiters();
    this.notifyFunctionWaiters();
    this.emitExchange(this.topicDataUrl());
    if (this.aggregateRejection) {
      const reject = this.aggregateRejection;
      this.aggregateRejection = null;
      reject(new Error("Synthetic aggregate rejection while filter typing is pending"));
      return;
    }
    if (
      this.filterValues.cluster_resource_id === "lkc-customer" &&
      this.filterValues.topic_name === "showcase-live-orders"
    ) {
      const aggregate = new URL(`${UI_ORIGIN}/api/v1/topic-attributions/aggregate`);
      new URL(this.currentUrl).searchParams.forEach((filterValue, key) => aggregate.searchParams.set(key, filterValue));
      this.emitExchange(aggregate.toString());
    }
  }

  updateDateInput(locatorName, value) {
    if (!this.currentUrl) return;
    const url = new URL(this.currentUrl);
    if (locatorName.includes(">> first")) url.searchParams.set("start_date", value);
    if (locatorName.includes(">> last")) url.searchParams.set("end_date", value);
    this.currentUrl = url.toString();
    this.notifyUrlWaiters();
    this.notifyFunctionWaiters();
    this.emitExchange(this.topicDataUrl());
  }

  inputValue(selector) {
    if (selector.includes("Any cluster")) return this.filterValues.cluster_resource_id ?? "";
    if (selector.includes("Any topic")) return this.filterValues.topic_name ?? "";
    return "";
  }

  notifyUrlWaiters() {
    if (!this.currentUrl) return;
    const url = new URL(this.currentUrl);
    const remaining = [];
    for (const waiter of this.urlWaiters) {
      if (waiter.predicate(url)) waiter.resolve();
      else remaining.push(waiter);
    }
    this.urlWaiters = remaining;
  }

  notifyFunctionWaiters() {
    const remaining = [];
    for (const waiter of this.functionWaiters) {
      if (waiter.predicate(waiter.argument)) waiter.resolve();
      else remaining.push(waiter);
    }
    this.functionWaiters = remaining;
  }

  async waitForRequest(predicate, options = {}) {
    this.log.push({ event: "waitForRequest", options });
    return new Promise((resolve) => this.requestWaiters.push({ predicate, resolve }));
  }

  async waitForLoadState(state, options = {}) {
    this.log.push({ event: "waitForLoadState", state, options });
  }

  getByText(text, options = {}) {
    this.log.push({ event: "getByText", value: text, options });
    return new FakeLocator(`text=${text}`, this.log);
  }

  getByRole(role, options = {}) {
    this.log.push({ event: "getByRole", value: role, options });
    return new FakeLocator(`role=${role}:${options.name ?? ""}`, this.log);
  }

  getByLabel(label, options = {}) {
    this.log.push({ event: "getByLabel", value: label, options });
    return new FakeLocator(`label=${label}`, this.log);
  }

  getByPlaceholder(placeholder, options = {}) {
    this.log.push({ event: "getByPlaceholder", value: placeholder, options });
    return new FakeLocator(`placeholder=${placeholder}`, this.log);
  }

  getByTestId(testId) {
    this.log.push({ event: "getByTestId", value: testId });
    return new FakeLocator(`testid=${testId}`, this.log);
  }

  locator(selector) {
    this.log.push({ event: "locator", value: selector });
    return new FakeLocator(selector, this.log);
  }

  async evaluate(callback, argument) {
    this.log.push({ event: "pageEvaluate", argument, source: String(callback) });
    const nodePerformance = globalThis.performance;
    Object.defineProperty(globalThis, "performance", {
      configurable: true,
      value: { now: () => pageElapsedMilliseconds },
    });
    try {
      return await callback(argument);
    } finally {
      Object.defineProperty(globalThis, "performance", { configurable: true, value: nodePerformance });
    }
  }

  async addStyleTag(options = {}) {
    this.log.push({ event: "addStyleTag", options });
  }

  async screenshot(options = {}) {
    this.log.push({ event: "screenshot", options });
  }
}

class FakeContext {
  constructor(log, mediaRoot) {
    this.log = log;
    this.mediaRoot = mediaRoot;
    this.recordingPath = path.join(mediaRoot, "work", "fake-browser-recording.webm");
    this.initScripts = [];
    this.documentInitializations = 0;
  }

  async addInitScript(script) {
    this.initScripts.push(script);
    this.log.push({ event: "addInitScript" });
  }

  async newPage() {
    this.log.push({ event: "newPage" });
    return new FakePage(this, this.log);
  }

  async close() {
    this.log.push({ event: "contextClose" });
  }
}

class FakeBrowser {
  constructor(log, mediaRoot) {
    this.log = log;
    this.mediaRoot = mediaRoot;
    this.context = null;
  }

  async newContext(options = {}) {
    this.log.push({ event: "newContext", options });
    this.context = new FakeContext(this.log, this.mediaRoot);
    return this.context;
  }

  async close() {
    this.log.push({ event: "browserClose" });
  }
}

const [capturePath, specPath, mode, mediaRoot, catalogPath, resultPath, requestedFailure = "", requestedGridOrder = "newest"] = process.argv.slice(2);
failure = requestedFailure;
if (requestedGridOrder === "oldest") gridRows = [...gridRows].reverse();
const { runCapture } = await import(pathToFileURL(path.resolve(capturePath)).href);
const captureSpec = JSON.parse(await fs.readFile(specPath, "utf8"));
const catalog = JSON.parse(await fs.readFile(catalogPath, "utf8"));
catalog.source_identifiers.push("clean-confluent", "confluent_cloud", "northstar-confluent");
const log = [];
const browser = new FakeBrowser(log, mediaRoot);
const result = await runCapture({ captureSpec, mode, browser, mediaRoot, catalog });
const timeline = JSON.parse(await fs.readFile(path.join(mediaRoot, "work", "edit-timeline.json"), "utf8"));
await fs.writeFile(
  resultPath,
  JSON.stringify({ result, log, timeline, documentInitializations: browser.context.documentInitializations }),
  "utf8",
);
