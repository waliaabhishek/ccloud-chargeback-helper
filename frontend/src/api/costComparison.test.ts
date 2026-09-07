import { http, HttpResponse } from "msw";
import { describe, expect, it } from "vitest";
import { server } from "../test/mocks/server";
import {
  fetchCostComparison,
  type CostComparisonRequest,
} from "./costComparison";

const BASELINE_AND_COMPARISON: CostComparisonRequest = {
  baseline_start: "2026-02-01",
  baseline_end: "2026-02-28",
  comparison_start: "2026-03-01",
  comparison_end: "2026-03-31",
  timezone: "America/Chicago",
  group_by: "principal",
  movement: "increase",
  sort_by: "absolute_change",
  sort_direction: "desc",
  limit: 100,
};

describe("fetchCostComparison", () => {
  it("sends every common and chargeback filter to the chargeback comparison endpoint", async () => {
    let capturedUrl = "";
    server.use(
      http.get(
        "/api/v1/tenants/acme/chargebacks/comparison",
        ({ request }) => {
          capturedUrl = request.url;
          return HttpResponse.json({ rows: [] });
        },
      ),
    );

    await fetchCostComparison("acme", "chargeback", {
      ...BASELINE_AND_COMPARISON,
      identity_id: "sa-123",
      product_type: "KAFKA_STORAGE",
      resource_id: "lkc-123",
      cost_type: "usage",
      tag_key: "cost_center",
      tag_value: "engineering",
    });

    const query = new URL(capturedUrl).searchParams;
    expect(query.get("baseline_start")).toBe("2026-02-01");
    expect(query.get("baseline_end")).toBe("2026-02-28");
    expect(query.get("comparison_start")).toBe("2026-03-01");
    expect(query.get("comparison_end")).toBe("2026-03-31");
    expect(query.get("timezone")).toBe("America/Chicago");
    expect(query.get("group_by")).toBe("principal");
    expect(query.get("movement")).toBe("increase");
    expect(query.get("sort_by")).toBe("absolute_change");
    expect(query.get("sort_direction")).toBe("desc");
    expect(query.get("limit")).toBe("100");
    expect(query.get("identity_id")).toBe("sa-123");
    expect(query.get("product_type")).toBe("KAFKA_STORAGE");
    expect(query.get("resource_id")).toBe("lkc-123");
    expect(query.get("cost_type")).toBe("usage");
    expect(query.get("tag_key")).toBe("cost_center");
    expect(query.get("tag_value")).toBe("engineering");
  });

  it("uses the Topic Attribution endpoint and only its supported filters", async () => {
    let capturedUrl = "";
    server.use(
      http.get(
        "/api/v1/tenants/acme/topic-attributions/comparison",
        ({ request }) => {
          capturedUrl = request.url;
          return HttpResponse.json({ rows: [] });
        },
      ),
    );

    await fetchCostComparison("acme", "topic_attribution", {
      ...BASELINE_AND_COMPARISON,
      group_by: "topic",
      cluster_resource_id: "lkc-123",
      topic_name: "orders",
      product_type: "KAFKA_STORAGE",
      attribution_method: "bytes_ratio",
      tag_key: "team",
      tag_value: "platform",
    });

    const query = new URL(capturedUrl).searchParams;
    expect(query.get("group_by")).toBe("topic");
    expect(query.get("cluster_resource_id")).toBe("lkc-123");
    expect(query.get("topic_name")).toBe("orders");
    expect(query.get("product_type")).toBe("KAFKA_STORAGE");
    expect(query.get("attribution_method")).toBe("bytes_ratio");
    expect(query.get("tag_key")).toBe("team");
    expect(query.get("tag_value")).toBe("platform");
  });

  it("returns financial JSON values as strings without client-side coercion", async () => {
    server.use(
      http.get("/api/v1/tenants/acme/chargebacks/comparison", () =>
        HttpResponse.json({
          summary: {
            baseline_amount: "0.3",
            comparison_amount: "-12345678901234567890.123456789",
            increases: "0.3",
            decreases: "-0.3",
            net_change: "0",
            percentage_change: null,
          },
          rows: [],
        }),
      ),
    );

    const result = await fetchCostComparison("acme", "chargeback", {
      ...BASELINE_AND_COMPARISON,
    });

    expect(result.summary.baseline_amount).toBe("0.3");
    expect(result.summary.comparison_amount).toBe(
      "-12345678901234567890.123456789",
    );
    expect(result.summary.percentage_change).toBeNull();
  });

  it("surfaces the server status when comparison retrieval fails", async () => {
    server.use(
      http.get(
        "/api/v1/tenants/acme/chargebacks/comparison",
        () => new HttpResponse(null, { status: 503, statusText: "Unavailable" }),
      ),
    );

    await expect(
      fetchCostComparison("acme", "chargeback", BASELINE_AND_COMPARISON),
    ).rejects.toThrow("HTTP 503: Unavailable");
  });
});
