import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { MemoryRouter } from "react-router";
import { http, HttpResponse } from "msw";
import { beforeEach, describe, expect, it, vi } from "vitest";
import type { CostComparisonResponse, TenantStatusSummary } from "../../types/api";
import { server } from "../../test/mocks/server";

const { gridSpy } = vi.hoisted(() => ({ gridSpy: vi.fn() }));

vi.mock("../../providers/TenantContext", () => ({
  useTenant: vi.fn(),
}));

vi.mock("../../components/topicAttributions/TopicAttributionGrid", () => ({
  TopicAttributionGrid: (props: {
    tenantName: string;
    filters: Record<string, string>;
  }) => {
    gridSpy(props);
    return <div data-testid="topic-attribution-grid" />;
  },
}));

vi.mock("../../components/topicAttributions/TopicAttributionFilterPanel", () => ({
  TopicAttributionFilterPanel: ({
    activeTab,
  }: {
    activeTab?: string;
  }) => <div data-testid="topic-attribution-filters" data-active-tab={activeTab} />,
}));

vi.mock("../../components/topicAttributions/TopicAttributionAnalytics", () => ({
  TopicAttributionAnalytics: () => <div data-testid="topic-attribution-analytics" />,
}));

import { useTenant } from "../../providers/TenantContext";
import { TopicAttributionPage } from "./list";

const FILTERS = {
  start_date: "2026-01-01",
  end_date: "2026-01-31",
  cluster_resource_id: "lkc-filtered",
  topic_name: "orders",
  product_type: "KAFKA_STORAGE",
  attribution_method: "bytes_ratio",
  timezone: "America/Chicago",
  tag_key: "team",
  tag_value: "platform",
} as const;

function tenant(tenantName: string): TenantStatusSummary {
  return {
    tenant_name: tenantName,
    tenant_id: `${tenantName}-id`,
    ecosystem: "ccloud",
    dates_pending: 0,
    dates_calculated: 10,
    last_calculated_date: null,
    chargeback_granularity: "daily",
    topic_attribution_status: "enabled",
    topic_attribution_error: null,
  };
}

function setTenant(tenantName: string): void {
  vi.mocked(useTenant).mockReturnValue({
    currentTenant: tenant(tenantName),
    tenants: [],
    isLoading: false,
    error: null,
    refetch: vi.fn(),
    setCurrentTenant: vi.fn(),
    isReadOnly: false,
  });
}

function comparisonResponse(tenantName: string): CostComparisonResponse {
  const period = (date: string) => ({
    start_date: date,
    end_date: date,
    start_at: `${date}T06:00:00Z`,
    end_at: `${date}T06:00:00Z`,
    duration_seconds: 86_400,
    coverage: {
      status: "complete" as const,
      expected_dates: [date],
      unknown_dates: [],
      incomplete_dates: [],
      retention_qualified_dates: [],
      availability_cutoff_at: null,
    },
  });
  return {
    source: "topic_attribution",
    granularity: "daily",
    group_by: "topic",
    timezone: "America/Chicago",
    coverage_evaluated_at: "2026-04-01T00:00:00Z",
    unequal_durations: false,
    baseline: period("2026-03-01"),
    comparison: period("2026-03-02"),
    summary: {
      baseline_amount: "1",
      comparison_amount: "2",
      increases: "1",
      decreases: "0",
      net_change: "1",
      percentage_change: "100",
    },
    reconciliation: {
      full_group_count: 1,
      selected_group_count: 1,
      returned_group_count: 1,
      movement_excluded_group_count: 0,
      row_limit_omitted_group_count: 0,
      returned_baseline_amount: "1",
      returned_comparison_amount: "2",
      returned_net_change: "1",
      movement_excluded_baseline_amount: "0",
      movement_excluded_comparison_amount: "0",
      movement_excluded_net_change: "0",
      row_limit_omitted_baseline_amount: "0",
      row_limit_omitted_comparison_amount: "0",
      row_limit_omitted_net_change: "0",
    },
    rows: [
      {
        key: `topic:${tenantName}:orders`,
        kind: "entity",
        dimensions: {
          cluster_resource_id: "lkc-filtered",
          topic_name: "orders",
        },
        baseline_amount: "1",
        comparison_amount: "2",
        change: "1",
        percentage_change: "100",
        baseline_row_count: 1,
        comparison_row_count: 1,
        observed_presence: "both",
      },
    ],
  };
}

function entry(): string {
  return `/topic-attributions?${new URLSearchParams(FILTERS).toString()}`;
}

function createQueryClient(): QueryClient {
  return new QueryClient({
    defaultOptions: { queries: { retry: false, gcTime: 60_000 } },
  });
}

function renderPage(queryClient: QueryClient): ReturnType<typeof render> {
  return render(
    <MemoryRouter initialEntries={[entry()]}>
      <QueryClientProvider client={queryClient}>
        <TopicAttributionPage />
      </QueryClientProvider>
    </MemoryRouter>,
  );
}

function latestGridProps(): {
  tenantName: string;
  filters: Record<string, string>;
} {
  const props = gridSpy.mock.calls.slice(-1)[0]?.[0] as
    | { tenantName: string; filters: Record<string, string> }
    | undefined;
  if (!props) throw new Error("topic attribution grid was not rendered");
  return props;
}

function assertComparisonFilters(url: URL): void {
  expect(url.searchParams.get("cluster_resource_id")).toBe(
    FILTERS.cluster_resource_id,
  );
  expect(url.searchParams.get("topic_name")).toBe(FILTERS.topic_name);
  expect(url.searchParams.get("product_type")).toBe(FILTERS.product_type);
  expect(url.searchParams.get("attribution_method")).toBe(
    FILTERS.attribution_method,
  );
  expect(url.searchParams.get("timezone")).toBe(FILTERS.timezone);
  expect(url.searchParams.get("tag_key")).toBe(FILTERS.tag_key);
  expect(url.searchParams.get("tag_value")).toBe(FILTERS.tag_value);
}

describe("TopicAttributionPage tenant switching", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    setTenant("acme");
  });

  it("reapplies every URL filter and resets comparison controls for the new tenant", async () => {
    const requestedUrls: URL[] = [];
    server.use(
      http.get(
        "/api/v1/tenants/:tenant/topic-attributions/comparison",
        ({ params, request }) => {
          requestedUrls.push(new URL(request.url));
          return HttpResponse.json(comparisonResponse(String(params.tenant)));
        },
      ),
    );

    const queryClient = createQueryClient();
    const rendered = renderPage(queryClient);
    await waitFor(() => {
      expect(latestGridProps().tenantName).toBe("acme");
    });

    setTenant("globex");
    rendered.rerender(
      <MemoryRouter initialEntries={[entry()]}>
        <QueryClientProvider client={queryClient}>
          <TopicAttributionPage />
        </QueryClientProvider>
      </MemoryRouter>,
    );
    await waitFor(() => {
      expect(latestGridProps().tenantName).toBe("globex");
    });
    expect(latestGridProps().filters).toEqual(FILTERS);

    await userEvent.click(screen.getByRole("radio", { name: "Compare" }));
    await waitFor(() => {
      expect(
        requestedUrls.some(
          (url) =>
            url.pathname ===
              "/api/v1/tenants/globex/topic-attributions/comparison",
        ),
      ).toBe(true);
    });
    const firstComparisonRequest = requestedUrls.find(
      (url) =>
        url.pathname ===
        "/api/v1/tenants/globex/topic-attributions/comparison",
    );
    if (!firstComparisonRequest) throw new Error("globex comparison was not requested");
    assertComparisonFilters(firstComparisonRequest);
    expect(firstComparisonRequest.searchParams.get("group_by")).toBe("topic");
    expect(firstComparisonRequest.searchParams.get("baseline_start")).not.toBe(
      FILTERS.start_date,
    );
    expect(firstComparisonRequest.searchParams.get("comparison_start")).not.toBe(
      FILTERS.start_date,
    );

    fireEvent.mouseDown(screen.getByRole("combobox", { name: "Group by" }));
    await userEvent.click(
      await screen.findByText("Cluster", {
        selector: ".ant-select-item-option-content",
      }),
    );
    await waitFor(() => {
      expect(
        requestedUrls.some(
          (url) =>
            url.pathname ===
              "/api/v1/tenants/globex/topic-attributions/comparison" &&
            url.searchParams.get("group_by") === "cluster",
        ),
      ).toBe(true);
    });

    setTenant("acme");
    rendered.rerender(
      <MemoryRouter initialEntries={[entry()]}>
        <QueryClientProvider client={queryClient}>
          <TopicAttributionPage />
        </QueryClientProvider>
      </MemoryRouter>,
    );
    await waitFor(() => {
      expect(
        requestedUrls.some(
          (url) =>
            url.pathname ===
              "/api/v1/tenants/acme/topic-attributions/comparison" &&
            url.searchParams.get("group_by") === "topic",
        ),
      ).toBe(true);
    });
    const resetComparisonRequest = requestedUrls.find(
      (url) =>
        url.pathname ===
          "/api/v1/tenants/acme/topic-attributions/comparison" &&
        url.searchParams.get("group_by") === "topic",
    );
    if (!resetComparisonRequest) throw new Error("reset comparison was not requested");
    assertComparisonFilters(resetComparisonRequest);
  });
});
