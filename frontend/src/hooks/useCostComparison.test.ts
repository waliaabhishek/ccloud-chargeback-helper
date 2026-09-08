import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { renderHook, waitFor } from "@testing-library/react";
import { http, HttpResponse } from "msw";
import { createElement } from "react";
import type { ReactNode } from "react";
import { describe, expect, it } from "vitest";
import { server } from "../test/mocks/server";
import type { CostComparisonRequest } from "../api/costComparison";
import type { ComparisonSource } from "../types/api";
import { useCostComparison } from "./useCostComparison";

function createWrapper() {
  const client = new QueryClient({
    defaultOptions: { queries: { retry: false, gcTime: 0 } },
  });
  return function Wrapper({ children }: { children: ReactNode }) {
    return createElement(QueryClientProvider, { client }, children);
  };
}

const PARAMETERS: CostComparisonRequest = {
  baseline_start: "2026-02-01",
  baseline_end: "2026-02-28",
  comparison_start: "2026-03-01",
  comparison_end: "2026-03-31",
  timezone: "UTC",
  group_by: "principal",
  movement: "all",
  sort_by: "absolute_change",
  sort_direction: "desc",
  limit: 100,
  tag_key: "team",
  tag_value: "platform",
};

describe("useCostComparison", () => {
  it("does not reuse prior data when tenant, filters, or comparison parameters change", async () => {
    const requestedQueries: string[] = [];
    server.use(
      http.get("/api/v1/tenants/:tenant/chargebacks/comparison", ({ request }) => {
        requestedQueries.push(request.url);
        return HttpResponse.json({
          summary: { baseline_amount: "0", comparison_amount: "0" },
          rows: [],
        });
      }),
    );

    const { result, rerender } = renderHook(
      ({ tenantName, limit }: { tenantName: string; limit: number }) =>
        useCostComparison({
          tenantName,
          source: "chargeback",
          params: { ...PARAMETERS, limit },
        }),
      {
        wrapper: createWrapper(),
        initialProps: { tenantName: "acme", limit: 100 },
      },
    );

    await waitFor(() => expect(result.current.isLoading).toBe(false));
    expect(requestedQueries).toHaveLength(1);
    expect(requestedQueries[0]).toContain("limit=100");

    rerender({ tenantName: "other", limit: 25 });
    await waitFor(() => expect(requestedQueries).toHaveLength(2));
    expect(requestedQueries[1]).toContain("/tenants/other/");
    expect(requestedQueries[1]).toContain("limit=25");
  });

  it("does not request a result until the tenant and both periods are initialized", () => {
    const { result } = renderHook(
      () =>
        useCostComparison({
          tenantName: null,
          source: "chargeback",
          params: {
            ...PARAMETERS,
            baseline_start: null,
            comparison_end: null,
          },
        }),
      { wrapper: createWrapper() },
    );

    expect(result.current.data).toBeNull();
    expect(result.current.isLoading).toBe(false);
  });

  it("never exposes a late old-tenant result after switching tenants", async () => {
    let releaseOldTenant: (() => void) | undefined;
    server.use(
      http.get(
        "/api/v1/tenants/:tenant/chargebacks/comparison",
        async ({ params }) => {
          if (params.tenant === "acme") {
            await new Promise<void>((resolve) => {
              releaseOldTenant = resolve;
            });
            return HttpResponse.json({
              summary: { baseline_amount: "1", comparison_amount: "1" },
              rows: [{ key: "old-tenant-row" }],
            });
          }
          return HttpResponse.json({
            summary: { baseline_amount: "2", comparison_amount: "3" },
            rows: [{ key: "new-tenant-row" }],
          });
        },
      ),
    );

    const { result, rerender } = renderHook(
      ({ tenantName }: { tenantName: string }) =>
        useCostComparison({
          tenantName,
          source: "chargeback",
          params: PARAMETERS,
        }),
      {
        wrapper: createWrapper(),
        initialProps: { tenantName: "acme" },
      },
    );

    await waitFor(() => expect(releaseOldTenant).toBeTypeOf("function"));
    rerender({ tenantName: "other" });

    await waitFor(() => {
      expect(result.current.data?.rows[0]?.key).toBe("new-tenant-row");
    });
    releaseOldTenant?.();

    await waitFor(() => {
      expect(result.current.data?.rows[0]?.key).toBe("new-tenant-row");
      expect(result.current.data?.summary.comparison_amount).toBe("3");
    });
  });

  it("isolates every comparison query-key dimension", async () => {
    const queryClient = new QueryClient({
      defaultOptions: { queries: { retry: false, gcTime: 60_000 } },
    });
    const wrapper = ({ children }: { children: ReactNode }) =>
      createElement(QueryClientProvider, { client: queryClient }, children);
    type QueryProps = {
      tenantName: string;
      source: ComparisonSource;
      params: CostComparisonRequest;
    };
    const initialProps: QueryProps = {
      tenantName: "acme",
      source: "chargeback",
      params: PARAMETERS,
    };
    const variants: QueryProps[] = [
      { ...initialProps, tenantName: "other" },
      {
        ...initialProps,
        source: "topic_attribution",
        params: { ...PARAMETERS, group_by: "topic" },
      },
      {
        ...initialProps,
        params: { ...PARAMETERS, group_by: "resource" },
      },
      {
        ...initialProps,
        params: { ...PARAMETERS, baseline_start: "2026-02-02" },
      },
      {
        ...initialProps,
        params: { ...PARAMETERS, baseline_end: "2026-02-27" },
      },
      {
        ...initialProps,
        params: { ...PARAMETERS, comparison_start: "2026-03-02" },
      },
      {
        ...initialProps,
        params: { ...PARAMETERS, comparison_end: "2026-03-30" },
      },
      {
        ...initialProps,
        params: { ...PARAMETERS, timezone: "America/Los_Angeles" },
      },
      {
        ...initialProps,
        params: { ...PARAMETERS, movement: "increase" },
      },
      {
        ...initialProps,
        params: { ...PARAMETERS, sort_by: "baseline_amount" },
      },
      {
        ...initialProps,
        params: { ...PARAMETERS, sort_direction: "asc" },
      },
      {
        ...initialProps,
        params: { ...PARAMETERS, limit: 25 },
      },
      {
        ...initialProps,
        params: { ...PARAMETERS, identity_id: "sa-123" },
      },
      {
        ...initialProps,
        params: { ...PARAMETERS, product_type: "KAFKA_STORAGE" },
      },
      {
        ...initialProps,
        params: { ...PARAMETERS, resource_id: "lkc-123" },
      },
      {
        ...initialProps,
        params: { ...PARAMETERS, cost_type: "usage" },
      },
      {
        ...initialProps,
        params: { ...PARAMETERS, cluster_resource_id: "lkc-123" },
      },
      {
        ...initialProps,
        params: { ...PARAMETERS, topic_name: "orders" },
      },
      {
        ...initialProps,
        params: { ...PARAMETERS, attribution_method: "ratio" },
      },
      {
        ...initialProps,
        params: { ...PARAMETERS, tag_key: "owner" },
      },
      {
        ...initialProps,
        params: { ...PARAMETERS, tag_value: "analytics" },
      },
    ];

    const { rerender } = renderHook(
      (props: QueryProps) =>
        useCostComparison({ ...props, enabled: false }),
      { wrapper, initialProps },
    );
    const queryKeys = (): string[] =>
      queryClient
        .getQueryCache()
        .getAll()
        .map((query) => JSON.stringify(query.queryKey));
    const seen = new Set(queryKeys());

    for (const variant of variants) {
      rerender(variant);
      await waitFor(() =>
        expect(queryKeys()).toHaveLength(seen.size + 1),
      );
      const newKey = queryKeys().find((key) => !seen.has(key));
      expect(newKey).toBeDefined();
      seen.add(newKey as string);
    }

    expect(seen).toHaveLength(variants.length + 1);
  });
});
