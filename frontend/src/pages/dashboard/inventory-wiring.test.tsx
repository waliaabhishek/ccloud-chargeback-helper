import type React from "react";
/**
 * Integration test: verifies that DashboardContent wires useInventorySummary
 * to InventoryCounters correctly — real hook + real component + MSW.
 *
 * Deliberately does NOT mock useInventorySummary or InventoryCounters so the
 * full data flow is exercised: fetch → hook state → component render.
 */
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import type { ReactNode } from "react";
import { MemoryRouter, useNavigate } from "react-router";
import type { NavigateFunction } from "react-router";
import { http, HttpResponse } from "msw";
import { describe, expect, it, vi } from "vitest";
import { server } from "../../test/mocks/server";
import type { CostComparisonResponse } from "../../types/api";
import { CostDashboardPage } from "./index";

vi.mock("react-router", async (importOriginal) => {
  const actual = await importOriginal<typeof import("react-router")>();
  return { ...actual, useNavigate: vi.fn() };
});

const COMPARISON_RESPONSE: CostComparisonResponse = {
  source: "chargeback",
  granularity: "daily",
  group_by: "principal",
  timezone: "UTC",
  coverage_evaluated_at: "2026-04-01T00:00:00Z",
  unequal_durations: false,
  baseline: {
    start_date: "2026-03-01",
    end_date: "2026-03-01",
    start_at: "2026-03-01T00:00:00Z",
    end_at: "2026-03-02T00:00:00Z",
    duration_seconds: 86400,
    coverage: {
      status: "complete",
      expected_dates: ["2026-03-01"],
      unknown_dates: [],
      incomplete_dates: [],
      retention_qualified_dates: [],
      availability_cutoff_at: null,
    },
  },
  comparison: {
    start_date: "2026-03-02",
    end_date: "2026-03-02",
    start_at: "2026-03-02T00:00:00Z",
    end_at: "2026-03-03T00:00:00Z",
    duration_seconds: 86400,
    coverage: {
      status: "complete",
      expected_dates: ["2026-03-02"],
      unknown_dates: [],
      incomplete_dates: [],
      retention_qualified_dates: [],
      availability_cutoff_at: null,
    },
  },
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
      key: "principal:sa-123",
      kind: "entity",
      dimensions: { identity_id: "sa-123" },
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

vi.mock("echarts-for-react", () => ({
  default: vi.fn(() => <div data-testid="echarts" />),
}));

vi.mock("../../hooks/useAggregation", () => ({
  useAggregation: vi.fn(() => ({
    data: null,
    isLoading: false,
    error: null,
    refetch: vi.fn(),
  })),
}));

vi.mock("../../hooks/useDataAvailability", () => ({
  useDataAvailability: vi.fn(() => ({
    data: null,
    isLoading: false,
    error: null,
    refetch: vi.fn(),
  })),
}));

vi.mock("../../hooks/useChargebackFilters", () => ({
  useChargebackFilters: vi.fn(() => ({
    filters: { start_date: null, end_date: null },
    setFilter: vi.fn(),
    setFilters: vi.fn(),
    resetFilters: vi.fn(),
    toQueryParams: vi.fn(() => ({})),
  })),
}));

vi.mock("../../providers/TenantContext", () => ({
  // GAP-100 Category B: appStatus/readiness removed — they move to useReadiness().
  useTenant: vi.fn(() => ({
    currentTenant: {
      tenant_name: "acme",
      tenant_id: "t-001",
      ecosystem: "ccloud",
      dates_pending: 0,
      dates_calculated: 10,
      last_calculated_date: null,
    },
    tenants: [],
    setCurrentTenant: vi.fn(),
    isLoading: false,
    error: null,
    refetch: vi.fn(),
    isReadOnly: false,
  })),
  useReadiness: vi.fn(() => ({
    appStatus: "ready" as const,
    readiness: null,
  })),
}));

vi.mock("../../components/charts/ChartCard", () => ({
  ChartCard: vi.fn(({ children }: { children: ReactNode }) => (
    <div data-testid="chart-card">{children}</div>
  )),
}));

vi.mock("../../components/chargebacks/FilterPanel", () => ({
  FilterPanel: vi.fn(() => <div data-testid="filter-panel" />),
}));

vi.mock("../../components/charts/CostTrendChart", () => ({
  CostTrendChart: vi.fn(() => <div />),
}));
vi.mock("../../components/charts/CostByIdentityChart", () => ({
  CostByIdentityChart: vi.fn(() => <div />),
}));
vi.mock("../../components/charts/CostByProductChart", () => ({
  CostByProductChart: vi.fn(() => <div />),
}));
vi.mock("../../components/charts/CostByResourceChart", () => ({
  CostByResourceChart: vi.fn(() => <div />),
}));
vi.mock("../../components/charts/DimensionPieChart", () => ({
  DimensionPieChart: vi.fn(() => <div />),
}));
vi.mock("../../components/charts/DataAvailabilityTimeline", () => ({
  DataAvailabilityTimeline: vi.fn(() => <div />),
}));
vi.mock("../../components/charts/ProductChartTypeToggle", () => ({
  ProductChartTypeToggle: vi.fn(() => <div />),
}));

vi.mock("../../components/dashboard/AllocationIssuesTable", () => ({
  AllocationIssuesTable: vi.fn(() => (
    <div data-testid="allocation-issues-table" />
  )),
}));

vi.mock("../../components/pivotPanel/TagPivotPanel", () => ({
  TagPivotPanel: vi.fn(() => null),
}));

// Partial antd mock — includes all components used by InventoryCounters
vi.mock("antd", () => ({
  Card: ({ children, title }: { children: ReactNode; title?: ReactNode }) => (
    <div data-testid="card">
      {title}
      {children}
    </div>
  ),
  Col: ({ children }: { children?: ReactNode }) => (
    <div data-testid="col">{children}</div>
  ),
  Row: ({ children }: { children: ReactNode }) => (
    <div data-testid="row">{children}</div>
  ),
  Skeleton: ({ active }: { active?: boolean }) => (
    <div data-testid="skeleton" data-active={active} />
  ),
  Statistic: ({ title, value }: { title: string; value: string | number }) => (
    <div data-testid="statistic">
      <div data-testid="statistic-title">{title}</div>
      <div data-testid="statistic-value">{value}</div>
    </div>
  ),
  Typography: {
    Title: ({ children }: { children: ReactNode }) => <h3>{children}</h3>,
    Text: ({ children }: { children: ReactNode }) => (
      <span data-testid="typography-text">{children}</span>
    ),
  },
  Segmented: ({
    options,
    value,
    onChange,
  }: {
    options: Array<{ label: string; value: string }>;
    value: string;
    onChange?: (value: string) => void;
  }) => (
    <div data-testid="segmented" data-value={value}>
      {options.map((option) => (
        <button
          key={option.value}
          onClick={() => onChange?.(option.value)}
        >
          {option.label}
        </button>
      ))}
    </div>
  ),
  Collapse: ({
    items,
  }: {
    items: Array<{ key: string; label: string; children: ReactNode }>;
  }) => (
    <div data-testid="collapse">
      {items.map((item) => (
        <div key={item.key}>
          <span data-testid="collapse-label">{item.label}</span>
          <div data-testid="collapse-content">{item.children}</div>
        </div>
      ))}
    </div>
  ),
  Empty: ({ description }: { description?: string }) => (
    <div data-testid="empty">{description}</div>
  ),
  Alert: ({ message }: { message?: ReactNode }) => (
    <div role="alert">{message}</div>
  ),
  Space: ({ children }: { children: ReactNode }) => <div>{children}</div>,
  Select: ({
    value,
    onChange,
    options,
    disabled,
    "aria-label": ariaLabel,
  }: {
    value?: string | number;
    onChange?: (value: string) => void;
    options?: Array<{
      label: string;
      value: string | number;
      disabled?: boolean;
    }>;
    disabled?: boolean;
    "aria-label"?: string;
  }) => (
    <select
      aria-label={ariaLabel}
      value={value}
      disabled={disabled}
      onChange={(event) => onChange?.(event.target.value)}
    >
      {options?.map((option) => (
        <option
          key={option.value}
          value={option.value}
          disabled={option.disabled}
        >
          {option.label}
        </option>
      ))}
    </select>
  ),
  Table: ({
    dataSource,
    columns,
    title,
  }: {
    dataSource: Array<Record<string, unknown>>;
    columns: Array<{
      title: ReactNode;
      dataIndex?: string;
      render?: (value: unknown, row: Record<string, unknown>) => ReactNode;
    }>;
    title?: () => ReactNode;
  }) => (
    <div data-testid="comparison-table">
      {title?.()}
      {columns.map((column, index) => (
        <span key={`header-${index}`}>{column.title}</span>
      ))}
      {dataSource.map((row, rowIndex) => (
        <div key={String(row.key ?? rowIndex)}>
          {columns.map((column, columnIndex) => (
            <span key={`${String(row.key ?? rowIndex)}-${columnIndex}`}>
              {column.render
                ? column.render(
                    column.dataIndex === undefined
                      ? undefined
                      : row[column.dataIndex],
                    row,
                  )
                : column.dataIndex === undefined
                  ? null
                  : String(row[column.dataIndex] ?? "")}
            </span>
          ))}
        </div>
      ))}
    </div>
  ),
  Radio: {
    Group: ({ children }: { children: ReactNode }) => <div>{children}</div>,
    Button: ({ children }: { children: ReactNode }) => (
      <button>{children}</button>
    ),
  },
}));

function wrapper({ children }: { children: ReactNode }): React.JSX.Element {
  const testQueryClient = new QueryClient({
    defaultOptions: { queries: { retry: false, gcTime: 0 } },
  });
  return (
    <QueryClientProvider client={testQueryClient}>
      <MemoryRouter>{children}</MemoryRouter>
    </QueryClientProvider>
  );
}

describe("InventoryCounters wiring integration", () => {
  it("renders inventory count cards from MSW response via real hook + real component", async () => {
    server.use(
      http.get("/api/v1/tenants/acme/inventory/summary", () =>
        HttpResponse.json({
          resource_counts: {
            kafka_cluster: { total: 5, active: 4, deleted: 1 },
            connector: { total: 3, active: 3, deleted: 0 },
          },
          identity_counts: {
            service_account: { total: 12, active: 10, deleted: 2 },
            user: { total: 3, active: 3, deleted: 0 },
          },
        }),
      ),
    );

    render(<CostDashboardPage />, { wrapper });

    await waitFor(() => {
      const titles = screen
        .getAllByTestId("statistic-title")
        .map((el) => el.textContent);
      expect(titles).toContain("Kafka Cluster");
    });

    const titles = screen
      .getAllByTestId("statistic-title")
      .map((el) => el.textContent);
    expect(titles).toContain("Kafka Cluster");
    expect(titles).toContain("Connector");
    expect(titles).toContain("Service Account");
    expect(titles).toContain("User");

    const values = screen
      .getAllByTestId("statistic-value")
      .map((el) => el.textContent);
    expect(values).toContain("5"); // kafka_cluster total
    expect(values).toContain("3"); // connector total
    expect(values).toContain("12"); // service_account total
  });

  it("renders comparison data through the real page, hook, API, and view", async () => {
    const requestedUrls: URL[] = [];
    const navigate = vi.fn();
    vi.mocked(useNavigate).mockReturnValue(
      navigate as unknown as NavigateFunction,
    );
    server.use(
      http.get(
        "/api/v1/tenants/acme/chargebacks/comparison",
        ({ request }) => {
          requestedUrls.push(new URL(request.url));
          return HttpResponse.json(COMPARISON_RESPONSE);
        },
      ),
    );

    render(<CostDashboardPage />, { wrapper });
    await userEvent.click(screen.getByRole("button", { name: "Compare" }));

    await waitFor(() => {
      expect(
        screen.getByText("Chargeback — allocated tenant costs"),
      ).toBeInTheDocument();
      expect(screen.getByText("Largest cost changes")).toBeInTheDocument();
    });
    expect(requestedUrls).toHaveLength(1);
    expect(requestedUrls[0]?.searchParams.get("group_by")).toBe("principal");
    expect(requestedUrls[0]?.searchParams.get("timezone")).toBe("UTC");

    await userEvent.click(
      screen.getByRole("button", { name: "Compare entity in Cost Explorer" }),
    );
    expect(navigate).toHaveBeenCalledTimes(1);
    const destination = navigate.mock.calls[0]?.[0];
    expect(String(destination)).toMatch(/^\/explorer\?/);
    const query = new URLSearchParams(String(destination).split("?")[1]);
    expect(query.get("focus")).toBe("principal:sa-123");
    expect(query.get("diff")).toBe("true");
    expect(query.get("from_start")).toBe("2026-03-01");
    expect(query.get("from_end")).toBe("2026-03-01");
    expect(query.get("to_start")).toBe("2026-03-02");
    expect(query.get("to_end")).toBe("2026-03-02");
    expect(query.get("timezone")).toBe("UTC");
  });
});
