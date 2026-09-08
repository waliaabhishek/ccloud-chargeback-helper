import type { ReactNode } from "react";
import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { TopicAttributionPage } from "./list";
import type { ComparisonRow, TenantStatusSummary } from "../../types/api";
import type { SetURLSearchParams } from "react-router";

const { comparisonViewSpy, comparisonRow, clusterComparisonRow } = vi.hoisted(
  () => ({
    comparisonViewSpy: vi.fn(),
    comparisonRow: {
      key: "topic:lkc-primary:orders",
      kind: "entity" as const,
      dimensions: {
        cluster_resource_id: "lkc-primary",
        topic_name: "orders",
      },
      baseline_amount: "0",
      comparison_amount: "1",
      change: "1",
      percentage_change: "100",
      baseline_row_count: 0,
      comparison_row_count: 1,
      observed_presence: "comparison_only" as const,
    },
    clusterComparisonRow: {
      key: "cluster:lkc-primary",
      kind: "entity" as const,
      dimensions: {
        cluster_resource_id: "lkc-primary",
      },
      baseline_amount: "1",
      comparison_amount: "2",
      change: "1",
      percentage_change: "100",
      baseline_row_count: 1,
      comparison_row_count: 1,
      observed_presence: "both" as const,
    },
  }),
);

// ---------------------------------------------------------------------------
// Mock TopicAttributionGrid — captures filters prop for integration assertions
// ---------------------------------------------------------------------------

vi.mock("../../components/topicAttributions/TopicAttributionGrid", () => ({
  TopicAttributionGrid: ({ filters }: { filters: Record<string, string> }) => (
    <div data-testid="ag-grid" data-filters={JSON.stringify(filters)} />
  ),
}));

vi.mock(
  "../../components/topicAttributions/TopicAttributionFilterPanel",
  () => ({
    TopicAttributionFilterPanel: ({
      onRefresh,
      activeTab,
    }: {
      onRefresh?: () => void;
      activeTab: string;
      [key: string]: unknown;
    }) => (
      <div data-testid="filter-panel" data-active-tab={activeTab}>
        {onRefresh && (
          <button data-testid="refresh-btn" onClick={onRefresh}>
            Refresh
          </button>
        )}
      </div>
    ),
  }),
);

vi.mock("../../components/topicAttributions/TopicAttributionAnalytics", () => ({
  TopicAttributionAnalytics: () => <div data-testid="analytics-view" />,
}));

vi.mock("../../components/costComparison/CostComparisonView", () => ({
  CostComparisonView: ({
    onInvestigate,
    onGroupByChange,
    groupBy,
  }: {
    onInvestigate: (row: ComparisonRow) => void;
    onGroupByChange: (groupBy: "topic" | "cluster") => void;
    groupBy: "topic" | "cluster";
  }) => {
    comparisonViewSpy({ onInvestigate, onGroupByChange, groupBy });
    return (
      <>
        <button
          data-testid="open-filtered-topic-list"
          onClick={() => onInvestigate(comparisonRow)}
        >
          Open filtered Topic Attribution list
        </button>
        <button
          data-testid="select-cluster-comparison"
          onClick={() => onGroupByChange("cluster")}
        >
          Cluster grouping
        </button>
        <button
          data-testid="open-filtered-cluster-list"
          disabled={groupBy !== "cluster"}
          onClick={() => onInvestigate(clusterComparisonRow)}
        >
          Open filtered cluster list
        </button>
      </>
    );
  },
}));

// ---------------------------------------------------------------------------
// Mock ag-grid-react (needed by other components rendered in analytics tab)
// ---------------------------------------------------------------------------

vi.mock("ag-grid-react", () => ({
  AgGridReact: () => <div data-testid="ag-grid" />,
}));

// ---------------------------------------------------------------------------
// Mock ag-grid-community
// ---------------------------------------------------------------------------

vi.mock("ag-grid-community", () => ({
  themeAlpine: {
    withParams: () => ({ withParams: () => ({}) }),
  },
}));

vi.mock("../../utils/gridDefaults", () => ({
  gridTheme: {},
  defaultColDef: { sortable: true, resizable: true },
}));

// ---------------------------------------------------------------------------
// Mock antd
// ---------------------------------------------------------------------------

vi.mock("antd", () => ({
  Typography: {
    Title: ({ children, level }: { children: ReactNode; level?: number }) => (
      <h3 data-level={String(level)}>{children}</h3>
    ),
    Text: ({ children, type }: { children: ReactNode; type?: string }) => (
      <span data-type={type}>{children}</span>
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
      {options.map((opt) => (
        <button
          key={opt.value}
          data-testid={`tab-${opt.value}`}
          onClick={() => onChange?.(opt.value)}
        >
          {opt.label}
        </button>
      ))}
    </div>
  ),
  Button: ({
    children,
    disabled,
    onClick,
  }: {
    children: ReactNode;
    disabled?: boolean;
    onClick?: () => void;
  }) => (
    <button disabled={disabled} onClick={onClick}>
      {children}
    </button>
  ),
  Tooltip: ({ children, title }: { children: ReactNode; title?: string }) => (
    <div data-tooltip={title}>{children}</div>
  ),
  Form: Object.assign(
    ({ children }: { children: ReactNode }) => <form>{children}</form>,
    {
      Item: ({ children }: { children: ReactNode }) => <div>{children}</div>,
      useForm: () => [{}],
    },
  ),
  Space: ({ children }: { children: ReactNode }) => <div>{children}</div>,
  Select: ({ children }: { children: ReactNode }) => (
    <select>{children}</select>
  ),
  Input: (props: { placeholder?: string }) => (
    <input placeholder={props.placeholder} />
  ),
  DatePicker: Object.assign(() => <input type="date" />, {
    RangePicker: () => <input type="date" data-testid="range-picker" />,
  }),
  Radio: Object.assign(
    ({ children }: { children: ReactNode }) => <span>{children}</span>,
    {
      Group: ({ children }: { children: ReactNode }) => <div>{children}</div>,
      Button: ({ children }: { children: ReactNode }) => (
        <button>{children}</button>
      ),
    },
  ),
  Row: ({ children }: { children: ReactNode }) => <div>{children}</div>,
  Col: ({ children }: { children: ReactNode }) => <div>{children}</div>,
  Card: ({ children, title }: { children: ReactNode; title?: string }) => (
    <div data-testid="card">
      {title && <span>{title}</span>}
      {children}
    </div>
  ),
  Empty: () => <div data-testid="empty" />,
  notification: { error: vi.fn(), success: vi.fn() },
  Alert: ({
    type,
    message,
    description,
  }: {
    type: string;
    message: string;
    description?: ReactNode;
    showIcon?: boolean;
  }) => (
    <div data-testid="alert" data-type={type}>
      {message}
      {description}
    </div>
  ),
}));

// ---------------------------------------------------------------------------
// Mock @tanstack/react-query
// ---------------------------------------------------------------------------

vi.mock("@tanstack/react-query", () => ({
  useQuery: vi.fn(() => ({
    data: undefined,
    isLoading: false,
    isError: false,
  })),
  QueryClient: vi.fn(),
  QueryClientProvider: ({ children }: { children: ReactNode }) => (
    <>{children}</>
  ),
}));

// ---------------------------------------------------------------------------
// Mock icons
// ---------------------------------------------------------------------------

vi.mock("@ant-design/icons", () => ({
  ExportOutlined: () => <span />,
  ReloadOutlined: () => <span />,
  FilterOutlined: () => <span />,
}));

// ---------------------------------------------------------------------------
// Mock echarts
// ---------------------------------------------------------------------------

vi.mock("echarts-for-react", () => ({
  default: () => <div data-testid="echarts" />,
}));

vi.mock("echarts", () => ({}));

// ---------------------------------------------------------------------------
// Mock TenantContext
// ---------------------------------------------------------------------------

vi.mock("../../providers/TenantContext", () => ({
  useTenant: vi.fn(),
}));

import { useTenant } from "../../providers/TenantContext";

const mockUseTenant = vi.mocked(useTenant);

// ---------------------------------------------------------------------------
// Mock router hooks
// ---------------------------------------------------------------------------

vi.mock("react-router", () => ({
  useSearchParams: vi.fn(() => [new URLSearchParams(), vi.fn()]),
}));

import { useSearchParams } from "react-router";

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function setupTenantContext(
  tenantName: string | null = "acme",
  isReadOnly = false,
  topicAttributionStatus: "disabled" | "enabled" | "config_error" = "disabled",
  topicAttributionError: string | null = null,
): void {
  mockUseTenant.mockReturnValue({
    currentTenant: tenantName
      ? ({
          tenant_name: tenantName,
          tenant_id: "t-001",
          ecosystem: "ccloud",
          dates_pending: 0,
          dates_calculated: 10,
          last_calculated_date: null,
          topic_attribution_status: topicAttributionStatus,
          topic_attribution_error: topicAttributionError,
        } as TenantStatusSummary)
      : null,
    tenants: [],
    isLoading: false,
    error: null,
    refetch: vi.fn(),
    setCurrentTenant: vi.fn(),
    isReadOnly,
  });
}

function requireAppliedParams(value: URLSearchParams | null): URLSearchParams {
  if (value === null) throw new Error("comparison navigation was not applied");
  return value;
}

beforeEach(() => {
  vi.clearAllMocks();
});

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

describe("TopicAttributionPage", () => {
  it("renders title Topic Attribution", () => {
    setupTenantContext();
    render(<TopicAttributionPage />);
    expect(screen.getByText("Topic Attribution")).toBeTruthy();
  });

  it("no-tenant guard: renders select tenant prompt without fetching data", () => {
    setupTenantContext(null);
    render(<TopicAttributionPage />);
    expect(screen.getByText("Select a tenant to begin.")).toBeTruthy();
    // No grid should be rendered when no tenant
    expect(screen.queryByTestId("ag-grid")).toBeNull();
  });

  it("renders main content when tenant is selected", () => {
    setupTenantContext("acme", false, "enabled");
    render(<TopicAttributionPage />);
    // Grid or tabs should be present
    expect(screen.queryByText("Select a tenant to begin.")).toBeNull();
    expect(screen.getByTestId("segmented")).toBeTruthy();
  });

  it("Export button is disabled when isReadOnly=true", () => {
    setupTenantContext("acme", true, "enabled");
    render(<TopicAttributionPage />);
    // Find the export button by text or test-id and verify it is disabled
    const exportBtn = screen.getByText("Export CSV").closest("button");
    expect(exportBtn?.disabled).toBe(true);
  });
});

// ---------------------------------------------------------------------------
// Integration: filter param flow
// ---------------------------------------------------------------------------

describe("TopicAttributionPage — filter param integration", () => {
  afterEach(() => {
    vi.mocked(useSearchParams).mockImplementation(() => [
      new URLSearchParams(),
      vi.fn<SetURLSearchParams>(),
    ]);
  });

  it("passes URL filter params through useTopicAttributionFilters to grid", () => {
    vi.mocked(useSearchParams).mockImplementation(() => [
      new URLSearchParams(
        "cluster_resource_id=lkc-abc123&start_date=2026-01-01&end_date=2026-01-31",
      ),
      vi.fn<SetURLSearchParams>(),
    ]);
    setupTenantContext("acme", false, "enabled");
    render(<TopicAttributionPage />);
    const grid = screen.getByTestId("ag-grid");
    expect(grid).toBeTruthy();
    const filters = JSON.parse(
      grid.getAttribute("data-filters") ?? "{}",
    ) as Record<string, string>;
    expect(filters.cluster_resource_id).toBe("lkc-abc123");
    expect(filters.start_date).toBe("2026-01-01");
    expect(filters.end_date).toBe("2026-01-31");
  });

  it("grid is not rendered when no tenant is selected, even with filter params", () => {
    vi.mocked(useSearchParams).mockImplementation(() => [
      new URLSearchParams("cluster_resource_id=lkc-abc123"),
      vi.fn<SetURLSearchParams>(),
    ]);
    setupTenantContext(null);
    render(<TopicAttributionPage />);
    expect(screen.queryByTestId("ag-grid")).toBeNull();
    expect(screen.getByText("Select a tenant to begin.")).toBeTruthy();
  });
});

// ---------------------------------------------------------------------------
// TASK-187: topic_attribution_enabled flag — feature discovery
// ---------------------------------------------------------------------------

describe("TASK-187: topic_attribution_enabled flag", () => {
  it("topic_attribution_status=disabled → shows feature discovery alert instead of data page", () => {
    setupTenantContext("acme", false, "disabled");
    render(<TopicAttributionPage />);

    const alert = screen.getByTestId("alert");
    expect(alert).toBeTruthy();
    expect(alert.textContent?.toLowerCase()).toContain("not configured");
    expect(screen.queryByTestId("ag-grid")).toBeNull();
  });

  it("topic_attribution_status=enabled → shows normal data page", () => {
    setupTenantContext("acme", false, "enabled");
    render(<TopicAttributionPage />);

    expect(screen.queryByTestId("alert")).toBeNull();
    expect(screen.queryByText("Select a tenant to begin.")).toBeNull();
  });

  it("topic_attribution_status=config_error → shows error alert with error message", () => {
    setupTenantContext("acme", false, "config_error", "requires metrics");
    render(<TopicAttributionPage />);

    const alert = screen.getByTestId("alert");
    expect(alert).toBeTruthy();
    expect(alert.textContent?.toLowerCase()).toContain("config");
    expect(alert.textContent).toContain("requires metrics");
    expect(screen.queryByTestId("ag-grid")).toBeNull();
  });
});

// ---------------------------------------------------------------------------
// TASK-198: AG Grid 0px height fix
// ---------------------------------------------------------------------------

describe("TASK-198: AG Grid 0px height fix", () => {
  it("renders Segmented switcher instead of Tabs when enabled", () => {
    setupTenantContext("acme", false, "enabled");
    render(<TopicAttributionPage />);
    expect(screen.getByTestId("segmented")).toBeTruthy();
    expect(screen.queryByTestId("tabs")).toBeNull();
  });

  it("shows grid and hides analytics on Table tab (default)", () => {
    setupTenantContext("acme", false, "enabled");
    render(<TopicAttributionPage />);
    expect(screen.getByTestId("ag-grid")).toBeTruthy();
    expect(screen.queryByTestId("analytics-view")).toBeNull();
  });

  it("keeps Table and Analytics while adding Compare for enabled tenants", () => {
    setupTenantContext("acme", false, "enabled");
    render(<TopicAttributionPage />);

    expect(screen.getByTestId("tab-table")).toBeInTheDocument();
    expect(screen.getByTestId("tab-analytics")).toBeInTheDocument();
    expect(screen.getByTestId("tab-compare")).toBeInTheDocument();
    expect(screen.getByTestId("segmented")).toHaveAttribute(
      "data-value",
      "table",
    );
  });

  it("opens a comparison topic in Table atomically with its full stored filters", async () => {
    const original = new URLSearchParams(
      "cluster_resource_id=lkc-old&topic_name=old-topic&product_type=KAFKA_STORAGE&attribution_method=bytes_ratio&tag_key=team&tag_value=platform&timezone=America%2FChicago&start_date=2026-02-01&end_date=2026-02-28",
    );
    let applied: URLSearchParams | null = null;
    const setSearchParams = vi.fn<SetURLSearchParams>(
      (update) => {
        if (typeof update === "function") {
          applied = update(original) as URLSearchParams;
        }
      },
    );
    vi.mocked(useSearchParams).mockImplementation(() => [
      original,
      setSearchParams,
    ]);
    setupTenantContext("acme", false, "enabled");
    render(<TopicAttributionPage />);

    await userEvent.click(screen.getByTestId("tab-compare"));
    await userEvent.click(screen.getByTestId("open-filtered-topic-list"));

    expect(comparisonViewSpy).toHaveBeenCalled();
    expect(screen.getByTestId("segmented")).toHaveAttribute(
      "data-value",
      "table",
    );
    const appliedParams = requireAppliedParams(applied);
    expect(appliedParams.get("cluster_resource_id")).toBe("lkc-primary");
    expect(appliedParams.get("topic_name")).toBe("orders");
    expect(appliedParams.get("product_type")).toBe("KAFKA_STORAGE");
    expect(appliedParams.get("attribution_method")).toBe("bytes_ratio");
    expect(appliedParams.get("tag_key")).toBe("team");
    expect(appliedParams.get("tag_value")).toBe("platform");
    expect(appliedParams.get("timezone")).toBe("America/Chicago");
  });

  it("retains a constraining topic filter when opening a cluster comparison", async () => {
    const original = new URLSearchParams(
      "topic_name=orders&product_type=KAFKA_STORAGE&timezone=America%2FChicago",
    );
    let applied: URLSearchParams | null = null;
    const setSearchParams = vi.fn<SetURLSearchParams>((update) => {
      if (typeof update === "function") {
        applied = update(original) as URLSearchParams;
      }
    });
    vi.mocked(useSearchParams).mockImplementation(() => [
      original,
      setSearchParams,
    ]);
    setupTenantContext("acme", false, "enabled");
    render(<TopicAttributionPage />);

    await userEvent.click(screen.getByTestId("tab-compare"));
    await userEvent.click(screen.getByTestId("select-cluster-comparison"));
    await userEvent.click(screen.getByTestId("open-filtered-cluster-list"));

    const appliedParams = requireAppliedParams(applied);
    expect(appliedParams.get("cluster_resource_id")).toBe("lkc-primary");
    expect(appliedParams.get("topic_name")).toBe("orders");
  });

  it("does not serialize a topic value when opening an unconstrained cluster comparison", async () => {
    const original = new URLSearchParams(
      "cluster_resource_id=lkc-old&product_type=KAFKA_STORAGE&timezone=America%2FChicago",
    );
    let applied: URLSearchParams | null = null;
    const setSearchParams = vi.fn<SetURLSearchParams>((update) => {
      if (typeof update === "function") {
        applied = update(original) as URLSearchParams;
      }
    });
    vi.mocked(useSearchParams).mockImplementation(() => [
      original,
      setSearchParams,
    ]);
    setupTenantContext("acme", false, "enabled");
    render(<TopicAttributionPage />);

    await userEvent.click(screen.getByTestId("tab-compare"));
    await userEvent.click(screen.getByTestId("select-cluster-comparison"));
    await userEvent.click(screen.getByTestId("open-filtered-cluster-list"));

    const appliedParams = requireAppliedParams(applied);
    expect(appliedParams.get("cluster_resource_id")).toBe("lkc-primary");
    expect(appliedParams.get("topic_name")).toBeNull();
    expect(appliedParams.toString()).not.toContain("undefined");
  });

  it("clicking Analytics hides grid and shows analytics", async () => {
    setupTenantContext("acme", false, "enabled");
    render(<TopicAttributionPage />);
    const analyticsTab = screen.getByTestId("tab-analytics");
    await userEvent.click(analyticsTab);
    expect(screen.queryByTestId("ag-grid")).toBeNull();
    expect(screen.getByTestId("analytics-view")).toBeTruthy();
  });

  it("clicking Table after Analytics restores grid", async () => {
    setupTenantContext("acme", false, "enabled");
    render(<TopicAttributionPage />);
    await userEvent.click(screen.getByTestId("tab-analytics"));
    await userEvent.click(screen.getByTestId("tab-table"));
    expect(screen.getByTestId("ag-grid")).toBeTruthy();
    expect(screen.queryByTestId("analytics-view")).toBeNull();
  });

  it("onRefresh is defined when activeTab is table", async () => {
    setupTenantContext("acme", false, "enabled");
    render(<TopicAttributionPage />);
    const refreshBtn = screen.getByTestId("refresh-btn");
    expect(refreshBtn).toBeTruthy();
    await userEvent.click(refreshBtn);
  });

  it("onRefresh is undefined when activeTab is analytics", async () => {
    setupTenantContext("acme", false, "enabled");
    render(<TopicAttributionPage />);
    await userEvent.click(screen.getByTestId("tab-analytics"));
    expect(screen.queryByTestId("refresh-btn")).toBeNull();
  });
});
