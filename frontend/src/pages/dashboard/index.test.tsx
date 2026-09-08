import type React from "react";
import { act, render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import type { ReactNode } from "react";
import type { CostComparisonResponse } from "../../types/api";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { MemoryRouter, useNavigate } from "react-router";
import type { NavigateFunction } from "react-router";
import { useCostComparison } from "../../hooks/useCostComparison";
import { CostDashboardPage } from "./index";

vi.mock("react-router", async (importOriginal) => {
  const actual = await importOriginal<typeof import("react-router")>();
  return { ...actual, useNavigate: vi.fn() };
});

const DASHBOARD_COMPARISON_RESPONSE: CostComparisonResponse = {
  source: "chargeback",
  granularity: "daily",
  group_by: "principal",
  timezone: "America/Chicago",
  coverage_evaluated_at: "2026-04-01T00:00:00Z",
  unequal_durations: false,
  baseline: {
    start_date: "2026-03-01",
    end_date: "2026-03-01",
    start_at: "2026-03-01T06:00:00Z",
    end_at: "2026-03-02T06:00:00Z",
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
    start_at: "2026-03-02T06:00:00Z",
    end_at: "2026-03-03T06:00:00Z",
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

// Mock echarts-for-react globally for all chart components
vi.mock("echarts-for-react", () => ({
  default: vi.fn(() => <div data-testid="echarts" />),
}));

// Mock chart components to avoid deep ECharts rendering
vi.mock("../../components/charts/ProductChartTypeToggle", () => ({
  ProductChartTypeToggle: vi.fn(() => (
    <div data-testid="product-chart-type-toggle" />
  )),
}));
vi.mock("../../components/charts/CostTrendChart", () => ({
  CostTrendChart: vi.fn(() => <div data-testid="cost-trend-chart" />),
}));
vi.mock("../../components/charts/CostByIdentityChart", () => ({
  CostByIdentityChart: vi.fn(() => (
    <div data-testid="cost-by-identity-chart" />
  )),
}));
vi.mock("../../components/charts/CostByProductChart", () => ({
  CostByProductChart: vi.fn(() => <div data-testid="cost-by-product-chart" />),
}));
vi.mock("../../components/charts/CostByResourceChart", () => ({
  CostByResourceChart: vi.fn(() => (
    <div data-testid="cost-by-resource-chart" />
  )),
}));
vi.mock("../../components/charts/DimensionPieChart", () => ({
  DimensionPieChart: vi.fn(() => <div data-testid="dimension-pie-chart" />),
}));
vi.mock("../../components/charts/DataAvailabilityTimeline", () => ({
  DataAvailabilityTimeline: vi.fn(() => (
    <div data-testid="data-availability-timeline" />
  )),
}));

// Mock FilterPanel — expose onRefresh so tests can trigger it
vi.mock("../../components/chargebacks/FilterPanel", () => ({
  FilterPanel: vi.fn(
    ({
      onReset,
      onRefresh,
      filters,
      showDateRange = true,
    }: {
      onReset: () => void;
      onRefresh?: () => void;
      filters: { timezone?: string | null };
      showDateRange?: boolean;
    }) => (
      <div
        data-testid="filter-panel"
        data-timezone={filters.timezone ?? ""}
        data-show-date-range={String(showDateRange)}
      >
        <button onClick={onReset}>Reset</button>
        {onRefresh !== undefined && (
          <button data-testid="filter-refresh" onClick={onRefresh}>
            Refresh Data
          </button>
        )}
      </div>
    ),
  ),
}));

// Mock ChartCard — just render children
vi.mock("../../components/charts/ChartCard", () => ({
  ChartCard: vi.fn(
    ({
      title,
      children,
      loading,
    }: {
      title: string;
      children: ReactNode;
      loading?: boolean;
    }) =>
      loading ? (
        <div data-testid="chart-card-loading">{title}</div>
      ) : (
        <div data-testid="chart-card">
          <span>{title}</span>
          {children}
        </div>
      ),
  ),
}));

// Mock useAggregation so we can spy on calls
vi.mock("../../hooks/useAggregation", () => ({
  useAggregation: vi.fn(() => ({
    data: null,
    isLoading: false,
    error: null,
    refetch: vi.fn(),
  })),
}));

vi.mock("../../hooks/useCostComparison", () => ({
  useCostComparison: vi.fn(() => ({
    data: null,
    isLoading: false,
    error: null,
  })),
}));

vi.mock("../../hooks/useDataAvailability", () => ({
  useDataAvailability: vi.fn(() => ({
    dates: [],
    isLoading: false,
    error: null,
    refetch: vi.fn(),
  })),
}));

vi.mock("../../hooks/useInventorySummary", () => ({
  useInventorySummary: vi.fn(() => ({
    data: null,
    isLoading: false,
    error: null,
    refetch: vi.fn(),
  })),
}));

vi.mock("../../components/dashboard/InventoryCounters", () => ({
  InventoryCounters: vi.fn(() => <div data-testid="inventory-counters" />),
}));

vi.mock("../../components/dashboard/AllocationIssuesTable", () => ({
  AllocationIssuesTable: vi.fn(() => (
    <div data-testid="allocation-issues-table" />
  )),
}));

vi.mock("../../components/pivotPanel/TagPivotPanel", () => ({
  TagPivotPanel: vi.fn(() => <div data-testid="tag-pivot-panel" />),
}));

// Mock antd
vi.mock("antd", () => ({
  Typography: {
    Title: ({ children }: { children: ReactNode; level?: number }) => (
      <h3>{children}</h3>
    ),
    Text: ({ children }: { children: ReactNode; type?: string }) => (
      <span>{children}</span>
    ),
  },
  Row: ({ children }: { children: ReactNode; gutter?: number | number[] }) => (
    <div>{children}</div>
  ),
  Col: ({
    children,
  }: {
    children: ReactNode;
    span?: number;
    xs?: number;
    md?: number;
  }) => <div>{children}</div>,
  Card: ({ children }: { children: ReactNode }) => <div>{children}</div>,
  Alert: ({ message }: { message: ReactNode }) => <div>{message}</div>,
  Empty: ({ description }: { description?: ReactNode }) => (
    <div>{description}</div>
  ),
  Skeleton: () => <div data-testid="skeleton" />,
  Statistic: ({ title, value }: { title: string; value: string | number }) => (
    <div>
      <span>{title}</span>
      <span>{value}</span>
    </div>
  ),
  Radio: {
    Group: ({
      children,
      value,
      onChange,
    }: {
      children: ReactNode;
      value: string;
      onChange: (e: { target: { value: string } }) => void;
    }) => (
      <div data-testid="time-bucket-selector" data-value={value}>
        {children}
        <button
          onClick={() => onChange({ target: { value: "week" } })}
          data-testid="select-week"
        >
          Week
        </button>
      </div>
    ),
    Button: ({ children, value }: { children: ReactNode; value: string }) => (
      <button data-value={value}>{children}</button>
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
    <div data-testid="comparison-mode" data-value={value}>
      {options.map((option) => (
        <button
          key={option.value}
          aria-pressed={value === option.value}
          onClick={() => onChange?.(option.value)}
        >
          {option.label}
        </button>
      ))}
    </div>
  ),
  Space: ({ children }: { children: ReactNode }) => <div>{children}</div>,
  Table: ({
    dataSource,
    columns,
  }: {
    dataSource: Array<Record<string, unknown>>;
    columns: Array<{
      title: ReactNode;
      dataIndex?: string;
      render?: (value: unknown, row: Record<string, unknown>) => ReactNode;
    }>;
  }) => (
    <div>
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
}));

const mockTenant = {
  tenant_name: "acme",
  tenant_id: "t-001",
  ecosystem: "ccloud",
  dates_pending: 0,
  dates_calculated: 10,
  last_calculated_date: null,
  topic_attribution_status: "disabled" as const,
  topic_attribution_error: null,
};

vi.mock("../../providers/TenantContext", () => ({
  useTenant: vi.fn(() => ({
    currentTenant: null,
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

vi.mock("../../hooks/useChargebackFilters", () => ({
  useChargebackFilters: vi.fn(() => ({
    filters: { start_date: null, end_date: null },
    setFilter: vi.fn(),
    setFilters: vi.fn(),
    resetFilters: vi.fn(),
    toQueryParams: vi.fn(() => ({})),
  })),
}));

function wrapper({ children }: { children: ReactNode }): React.JSX.Element {
  return <MemoryRouter>{children}</MemoryRouter>;
}

describe("CostDashboardPage", () => {
  beforeEach(async () => {
    const { useTenant } = await import("../../providers/TenantContext");
    vi.mocked(useTenant).mockReturnValue({
      currentTenant: null,
      tenants: [],
      setCurrentTenant: vi.fn(),
      isLoading: false,
      error: null,
      refetch: vi.fn(),
      isReadOnly: false,
    });
  });

  afterEach(() => {
    vi.clearAllMocks();
  });

  it("shows placeholder when no tenant selected", () => {
    render(<CostDashboardPage />, { wrapper });
    expect(screen.getByText("Cost Dashboard")).toBeInTheDocument();
    expect(
      screen.getByText("Select a tenant to view cost analytics."),
    ).toBeInTheDocument();
    expect(screen.queryByTestId("filter-panel")).toBeNull();
    expect(screen.queryByTestId("time-bucket-selector")).toBeNull();
    expect(screen.queryByText("Cost Trend Over Time")).toBeNull();
  });

  it("renders all six chart cards when tenant is selected", async () => {
    const { useTenant } = await import("../../providers/TenantContext");
    vi.mocked(useTenant).mockReturnValue({
      currentTenant: mockTenant,
      tenants: [mockTenant],
      setCurrentTenant: vi.fn(),
      isLoading: false,
      error: null,
      refetch: vi.fn(),
      isReadOnly: false,
    });

    render(<CostDashboardPage />, { wrapper });

    await waitFor(() => {
      expect(screen.getByTestId("filter-panel")).toBeInTheDocument();
      expect(screen.getByTestId("time-bucket-selector")).toBeInTheDocument();
    });

    expect(screen.getByTestId("inventory-counters")).toBeInTheDocument();
    expect(screen.getByText("Data Availability")).toBeInTheDocument();
    expect(
      screen.getByTestId("data-availability-timeline"),
    ).toBeInTheDocument();
    expect(screen.getByText("Cost Trend Over Time")).toBeInTheDocument();
    expect(screen.getByText("Cost by Identity")).toBeInTheDocument();
    expect(screen.getByText("Cost by Environment")).toBeInTheDocument();
    expect(screen.getByText("Cost by Resource")).toBeInTheDocument();
    expect(screen.getByText("Cost by Product Type")).toBeInTheDocument();
    // GAP-100 verification item 8: title must be "Cost by Product Category" not Sub-Type.
    // FAILS in red state: dashboard/index.tsx still uses "Cost by Product Sub-Type".
    expect(screen.getByText("Cost by Product Category")).toBeInTheDocument();
    expect(screen.getByTestId("tag-pivot-panel")).toBeInTheDocument();
  });

  it("defaults to Overview while exposing Compare alongside the existing dashboard", async () => {
    const { useTenant } = await import("../../providers/TenantContext");
    vi.mocked(useTenant).mockReturnValue({
      currentTenant: mockTenant,
      tenants: [mockTenant],
      setCurrentTenant: vi.fn(),
      isLoading: false,
      error: null,
      refetch: vi.fn(),
      isReadOnly: false,
    });

    render(<CostDashboardPage />, { wrapper });

    await waitFor(() => {
      expect(screen.getByTestId("comparison-mode")).toHaveAttribute(
        "data-value",
        "overview",
      );
    });
    expect(screen.getByRole("button", { name: "Compare" })).toBeInTheDocument();
    expect(screen.getByTestId("cost-trend-chart")).toBeInTheDocument();
    expect(screen.getByTestId("filter-panel")).toBeInTheDocument();
  });

  it("passes inherited chargeback filters into the comparison request", async () => {
    const { useTenant } = await import("../../providers/TenantContext");
    const { useChargebackFilters } =
      await import("../../hooks/useChargebackFilters");
    const pageFilters = {
      start_date: "2026-01-01",
      end_date: "2026-01-31",
      identity_id: "sa-123",
      product_type: "KAFKA_STORAGE",
      resource_id: "lkc-123",
      cost_type: "usage",
      tag_key: "team",
      tag_value: "platform",
      timezone: "America/Chicago",
    };
    vi.mocked(useChargebackFilters).mockReturnValue({
      filters: pageFilters,
      setFilter: vi.fn(),
      setFilters: vi.fn(),
      resetFilters: vi.fn(),
      toQueryParams: vi.fn(() => pageFilters),
      queryParams: pageFilters,
    });
    vi.mocked(useTenant).mockReturnValue({
      currentTenant: mockTenant,
      tenants: [mockTenant],
      setCurrentTenant: vi.fn(),
      isLoading: false,
      error: null,
      refetch: vi.fn(),
      isReadOnly: false,
    });

    render(<CostDashboardPage />, { wrapper });
    await userEvent.click(screen.getByRole("button", { name: "Compare" }));

    await waitFor(() => {
      expect(
        vi
          .mocked(useCostComparison)
          .mock.calls.some(([request]) => request.enabled === true),
      ).toBe(true);
    });
    const comparisonRequest = vi
      .mocked(useCostComparison)
      .mock.calls.find(([request]) => request.enabled === true)?.[0];
    expect(comparisonRequest?.params).toMatchObject({
      identity_id: "sa-123",
      product_type: "KAFKA_STORAGE",
      resource_id: "lkc-123",
      cost_type: "usage",
      tag_key: "team",
      tag_value: "platform",
      timezone: "America/Chicago",
    });
    expect(comparisonRequest?.params.baseline_start).not.toBe("2026-01-01");
    expect(comparisonRequest?.params.comparison_start).not.toBe("2026-01-01");
  });

  it("opens a chargeback comparison row in Cost Explorer with both periods and timezone", async () => {
    const { useTenant } = await import("../../providers/TenantContext");
    const { useChargebackFilters } =
      await import("../../hooks/useChargebackFilters");
    vi.mocked(useTenant).mockReturnValue({
      currentTenant: mockTenant,
      tenants: [mockTenant],
      setCurrentTenant: vi.fn(),
      isLoading: false,
      error: null,
      refetch: vi.fn(),
      isReadOnly: false,
    });
    vi.mocked(useChargebackFilters).mockReturnValue({
      filters: {
        start_date: "2026-01-01",
        end_date: "2026-01-31",
        identity_id: null,
        product_type: null,
        resource_id: null,
        cost_type: null,
        tag_key: null,
        tag_value: null,
        timezone: "America/Chicago",
      },
      setFilter: vi.fn(),
      setFilters: vi.fn(),
      resetFilters: vi.fn(),
      toQueryParams: vi.fn(() => ({})),
      queryParams: {},
    });
    vi.mocked(useCostComparison).mockReturnValue({
      data: DASHBOARD_COMPARISON_RESPONSE,
      isLoading: false,
      error: null,
      refetch: vi.fn(),
    });
    const navigate = vi.fn();
    vi.mocked(useNavigate).mockReturnValue(
      navigate as unknown as NavigateFunction,
    );

    render(<CostDashboardPage />, { wrapper });
    await userEvent.click(screen.getByRole("button", { name: "Compare" }));
    await userEvent.click(
      screen.getByRole("button", { name: "Compare entity in Cost Explorer" }),
    );

    const destination =
      navigate.mock.calls[navigate.mock.calls.length - 1]?.[0];
    expect(typeof destination).toBe("string");
    const query = new URLSearchParams(String(destination).split("?")[1]);
    expect(query.get("focus")).toBe("principal:sa-123");
    expect(query.get("diff")).toBe("true");
    expect(query.get("from_start")).toBe("2026-03-01");
    expect(query.get("from_end")).toBe("2026-03-01");
    expect(query.get("to_start")).toBe("2026-03-02");
    expect(query.get("to_end")).toBe("2026-03-02");
    expect(query.get("timezone")).toBe("America/Chicago");
  });

  it("resets every comparison-local control for a new monthly tenant without rewriting page filters", async () => {
    const { useTenant } = await import("../../providers/TenantContext");
    const { useChargebackFilters } =
      await import("../../hooks/useChargebackFilters");
    const dailyTenant = {
      ...mockTenant,
      tenant_name: "daily",
      chargeback_granularity: "daily" as const,
    };
    const monthlyTenant = {
      ...mockTenant,
      tenant_name: "monthly",
      chargeback_granularity: "monthly" as const,
    };
    const pageFilters = {
      start_date: "2026-01-01",
      end_date: "2026-01-31",
      identity_id: "sa-123",
      product_type: "KAFKA_STORAGE",
      resource_id: "lkc-123",
      cost_type: "usage",
      tag_key: "team",
      tag_value: "platform",
      timezone: "America/Chicago",
    };
    vi.mocked(useChargebackFilters).mockReturnValue({
      filters: pageFilters,
      setFilter: vi.fn(),
      setFilters: vi.fn(),
      resetFilters: vi.fn(),
      toQueryParams: vi.fn(() => pageFilters),
      queryParams: pageFilters,
    });
    vi.mocked(useTenant).mockReturnValue({
      currentTenant: dailyTenant,
      tenants: [dailyTenant, monthlyTenant],
      setCurrentTenant: vi.fn(),
      isLoading: false,
      error: null,
      refetch: vi.fn(),
      isReadOnly: false,
    });

    const { rerender } = render(<CostDashboardPage />, { wrapper });
    await userEvent.click(screen.getByRole("button", { name: "Compare" }));
    await userEvent.selectOptions(screen.getByLabelText("Preset"), "custom");
    await userEvent.selectOptions(screen.getByLabelText("Group by"), "resource");
    await userEvent.click(screen.getByRole("button", { name: "Decreases" }));
    await userEvent.selectOptions(screen.getByLabelText("Sort by"), "entity");
    await userEvent.selectOptions(screen.getByLabelText("Rows"), "25");

    vi.mocked(useTenant).mockReturnValue({
      currentTenant: monthlyTenant,
      tenants: [dailyTenant, monthlyTenant],
      setCurrentTenant: vi.fn(),
      isLoading: false,
      error: null,
      refetch: vi.fn(),
      isReadOnly: false,
    });
    rerender(<CostDashboardPage />);

    expect(screen.getByLabelText("Preset")).toHaveValue("calendar_month");
    expect(screen.getByLabelText("Group by")).toHaveValue("principal");
    expect(screen.getByRole("button", { name: "All" })).toHaveAttribute(
      "aria-pressed",
      "true",
    );
    expect(screen.getByLabelText("Sort by")).toHaveValue("absolute_change");
    expect(screen.getByLabelText("Sort direction")).toHaveValue("desc");
    expect(screen.getByLabelText("Rows")).toHaveValue("100");
    expect(screen.getByLabelText("Comparison timezone")).toHaveValue("UTC");
    expect(screen.getByTestId("filter-panel")).toHaveAttribute(
      "data-timezone",
      "America/Chicago",
    );
    expect(screen.getByTestId("filter-panel")).toHaveAttribute(
      "data-show-date-range",
      "false",
    );
  });

  it("makes 6 useAggregation calls with tag:owner, environment_id, and product_category groupBy", async () => {
    const { useTenant } = await import("../../providers/TenantContext");
    vi.mocked(useTenant).mockReturnValue({
      currentTenant: mockTenant,
      tenants: [mockTenant],
      setCurrentTenant: vi.fn(),
      isLoading: false,
      error: null,
      refetch: vi.fn(),
      isReadOnly: false,
    });

    const { useAggregation } = await import("../../hooks/useAggregation");
    const mockUseAggregation = vi.mocked(useAggregation);

    render(<CostDashboardPage />, { wrapper });

    await waitFor(() => {
      expect(screen.getByTestId("filter-panel")).toBeInTheDocument();
    });

    expect(mockUseAggregation).toHaveBeenCalledTimes(6);

    const groupByValues = mockUseAggregation.mock.calls.map(
      (call) => call[0].groupBy,
    );
    expect(groupByValues.flat()).toContain("environment_id");
    // GAP-100 verification item 9: must use product_category, not product_sub_type.
    // FAILS in red state: dashboard/index.tsx still calls useAggregation with product_sub_type.
    expect(groupByValues.flat()).toContain("product_category");
    // TASK-216: must include tag:owner groupBy for TagPivotPanel
    expect(groupByValues).toContainEqual(["tag:owner", "product_type"]);
  });

  it("renders tag-pivot-panel and passes buckets from ownerData", async () => {
    const { useTenant } = await import("../../providers/TenantContext");
    vi.mocked(useTenant).mockReturnValue({
      currentTenant: mockTenant,
      tenants: [mockTenant],
      setCurrentTenant: vi.fn(),
      isLoading: false,
      error: null,
      refetch: vi.fn(),
      isReadOnly: false,
    });

    const { useAggregation } = await import("../../hooks/useAggregation");
    const mockUseAggregation = vi.mocked(useAggregation);

    const { TagPivotPanel } =
      await import("../../components/pivotPanel/TagPivotPanel");
    const mockTagPivotPanel = vi.mocked(TagPivotPanel);

    render(<CostDashboardPage />, { wrapper });

    await waitFor(() => {
      expect(screen.getByTestId("tag-pivot-panel")).toBeInTheDocument();
    });

    // One useAggregation call must use tag:owner + product_type groupBy
    const groupByValues = mockUseAggregation.mock.calls.map(
      (call) => call[0].groupBy,
    );
    expect(groupByValues).toContainEqual(["tag:owner", "product_type"]);

    // TagPivotPanel must receive buckets prop (empty array since mock returns null data)
    expect(mockTagPivotPanel.mock.lastCall![0].buckets).toEqual([]);
  });

  it("resets activeTagFilters to [] when onTagKeyChange is called", async () => {
    const { useTenant } = await import("../../providers/TenantContext");
    vi.mocked(useTenant).mockReturnValue({
      currentTenant: mockTenant,
      tenants: [mockTenant],
      setCurrentTenant: vi.fn(),
      isLoading: false,
      error: null,
      refetch: vi.fn(),
      isReadOnly: false,
    });

    const { TagPivotPanel } =
      await import("../../components/pivotPanel/TagPivotPanel");
    const mockTagPivotPanel = vi.mocked(TagPivotPanel);

    render(<CostDashboardPage />, { wrapper });

    await waitFor(() => {
      expect(screen.getByTestId("tag-pivot-panel")).toBeInTheDocument();
    });

    // Add a filter via the onFilterAdd prop
    const { onFilterAdd } = mockTagPivotPanel.mock.lastCall![0];
    act(() => {
      onFilterAdd("alice");
    });

    await waitFor(() => {
      expect(mockTagPivotPanel.mock.lastCall![0].activeTagFilters).toContain(
        "alice",
      );
    });

    // Change tag key — should reset activeTagFilters to []
    const { onTagKeyChange } = mockTagPivotPanel.mock.lastCall![0];
    act(() => {
      onTagKeyChange("team");
    });

    await waitFor(() => {
      expect(mockTagPivotPanel.mock.lastCall![0].activeTagFilters).toEqual([]);
    });
  });

  it("forwards explicit date filters to useAggregation", async () => {
    const { useTenant } = await import("../../providers/TenantContext");
    vi.mocked(useTenant).mockReturnValue({
      currentTenant: mockTenant,
      tenants: [mockTenant],
      setCurrentTenant: vi.fn(),
      isLoading: false,
      error: null,
      refetch: vi.fn(),
      isReadOnly: false,
    });

    const { useChargebackFilters } =
      await import("../../hooks/useChargebackFilters");
    vi.mocked(useChargebackFilters).mockReturnValue({
      filters: {
        start_date: "2026-01-01",
        end_date: "2026-01-31",
        identity_id: null,
        product_type: null,
        resource_id: null,
        cost_type: null,
        timezone: null,
        tag_key: null,
        tag_value: null,
      },
      setFilter: vi.fn(),
      setFilters: vi.fn(),
      resetFilters: vi.fn(),
      toQueryParams: vi.fn(() => ({})),
      queryParams: {},
    });

    const { useAggregation } = await import("../../hooks/useAggregation");
    const mockUseAggregation = vi.mocked(useAggregation);

    render(<CostDashboardPage />, { wrapper });

    await waitFor(() => {
      expect(screen.getByTestId("filter-panel")).toBeInTheDocument();
    });

    const calls = mockUseAggregation.mock.calls;
    expect(calls.length).toBeGreaterThan(0);
    expect(calls[0][0].startDate).toBe("2026-01-01");
    expect(calls[0][0].endDate).toBe("2026-01-31");
  });

  it("forwards timezone filter to useAggregation", async () => {
    const { useTenant } = await import("../../providers/TenantContext");
    vi.mocked(useTenant).mockReturnValue({
      currentTenant: mockTenant,
      tenants: [mockTenant],
      setCurrentTenant: vi.fn(),
      isLoading: false,
      error: null,
      refetch: vi.fn(),
      isReadOnly: false,
    });

    const { useChargebackFilters } =
      await import("../../hooks/useChargebackFilters");
    vi.mocked(useChargebackFilters).mockReturnValue({
      filters: {
        start_date: "2026-01-01",
        end_date: "2026-01-31",
        identity_id: null,
        product_type: null,
        resource_id: null,
        cost_type: null,
        timezone: "America/Chicago",
        tag_key: null,
        tag_value: null,
      },
      setFilter: vi.fn(),
      setFilters: vi.fn(),
      resetFilters: vi.fn(),
      toQueryParams: vi.fn(() => ({})),
      queryParams: {},
    });

    const { useAggregation } = await import("../../hooks/useAggregation");
    const mockUseAggregation = vi.mocked(useAggregation);

    render(<CostDashboardPage />, { wrapper });

    await waitFor(() => {
      expect(screen.getByTestId("filter-panel")).toBeInTheDocument();
    });

    const calls = mockUseAggregation.mock.calls;
    expect(calls.length).toBeGreaterThan(0);
    expect(calls[0][0].timezone).toBe("America/Chicago");
  });

  it("changes time bucket when selector is clicked", async () => {
    const { useTenant } = await import("../../providers/TenantContext");
    vi.mocked(useTenant).mockReturnValue({
      currentTenant: mockTenant,
      tenants: [mockTenant],
      setCurrentTenant: vi.fn(),
      isLoading: false,
      error: null,
      refetch: vi.fn(),
      isReadOnly: false,
    });

    render(<CostDashboardPage />, { wrapper });

    await waitFor(() => {
      expect(screen.getByTestId("time-bucket-selector")).toBeInTheDocument();
    });

    expect(
      screen.getByTestId("time-bucket-selector").getAttribute("data-value"),
    ).toBe("day");
    await userEvent.click(screen.getByTestId("select-week"));
    expect(
      screen.getByTestId("time-bucket-selector").getAttribute("data-value"),
    ).toBe("week");
  });

  it("passes onRefresh to FilterPanel and remounts DashboardContent on click", async () => {
    const { useTenant } = await import("../../providers/TenantContext");
    vi.mocked(useTenant).mockReturnValue({
      currentTenant: mockTenant,
      tenants: [mockTenant],
      setCurrentTenant: vi.fn(),
      isLoading: false,
      error: null,
      refetch: vi.fn(),
      isReadOnly: false,
    });

    const { useAggregation } = await import("../../hooks/useAggregation");
    vi.mocked(useAggregation).mockClear();

    render(<CostDashboardPage />, { wrapper });

    await waitFor(() => {
      expect(screen.getByTestId("filter-panel")).toBeInTheDocument();
    });

    // FilterPanel receives onRefresh — the Refresh Data button should render
    expect(screen.getByTestId("filter-refresh")).toBeInTheDocument();

    const callsAfterMount = vi.mocked(useAggregation).mock.calls.length;
    expect(callsAfterMount).toBeGreaterThan(0);

    // Clicking Refresh Data increments refreshKey → DashboardContent remounts
    await userEvent.click(screen.getByTestId("filter-refresh"));

    // DashboardContent remount triggers useAggregation calls again
    expect(vi.mocked(useAggregation).mock.calls.length).toBeGreaterThan(
      callsAfterMount,
    );
  });
});
