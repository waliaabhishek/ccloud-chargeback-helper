import { fireEvent, render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it, vi } from "vitest";
import type { CostComparisonResponse } from "../../types/api";
import type { CostComparisonViewProps } from "./CostComparisonView";
import { CostComparisonView } from "./CostComparisonView";

const RESPONSE: CostComparisonResponse = {
  source: "topic_attribution",
  granularity: "daily",
  group_by: "topic",
  timezone: "America/Chicago",
  coverage_evaluated_at: "2026-04-01T00:00:00Z",
  unequal_durations: true,
  baseline: {
    start_date: "2026-03-07",
    end_date: "2026-03-08",
    start_at: "2026-03-07T06:00:00Z",
    end_at: "2026-03-09T05:00:00Z",
    duration_seconds: 169200,
    coverage: {
      status: "unknown",
      expected_dates: ["2026-03-07", "2026-03-08"],
      unknown_dates: ["2026-03-08"],
      incomplete_dates: ["2026-03-07"],
      retention_qualified_dates: ["2026-03-08"],
      availability_cutoff_at: null,
    },
  },
  comparison: {
    start_date: "2026-03-14",
    end_date: "2026-03-15",
    start_at: "2026-03-14T06:00:00Z",
    end_at: "2026-03-16T05:00:00Z",
    duration_seconds: 176400,
    coverage: {
      status: "complete",
      expected_dates: ["2026-03-14", "2026-03-15"],
      unknown_dates: [],
      incomplete_dates: [],
      retention_qualified_dates: [],
      availability_cutoff_at: null,
    },
  },
  summary: {
    baseline_amount: "0",
    comparison_amount: "0.3",
    increases: "12.4",
    decreases: "-12.1",
    net_change: "0.3",
    percentage_change: null,
  },
  reconciliation: {
    full_group_count: 4,
    selected_group_count: 3,
    returned_group_count: 1,
    movement_excluded_group_count: 2,
    row_limit_omitted_group_count: 1,
    returned_baseline_amount: "0",
    returned_comparison_amount: "0",
    returned_net_change: "0",
    movement_excluded_baseline_amount: "1",
    movement_excluded_comparison_amount: "1",
    movement_excluded_net_change: "0",
    row_limit_omitted_baseline_amount: "-1",
    row_limit_omitted_comparison_amount: "-1",
    row_limit_omitted_net_change: "0",
  },
  rows: [
    {
      key: "topic:lkc-a:orders",
      kind: "entity",
      dimensions: { cluster_resource_id: "lkc-a", topic_name: "orders" },
      baseline_amount: "0",
      comparison_amount: "0.3",
      change: "0.3",
      percentage_change: null,
      baseline_row_count: 0,
      comparison_row_count: 1,
      observed_presence: "comparison_only",
    },
    {
      key: "topic:lkc-b:orders",
      kind: "entity",
      dimensions: { cluster_resource_id: "lkc-b", topic_name: "orders" },
      baseline_amount: "3",
      comparison_amount: "2",
      change: "-1",
      percentage_change: "-33.3333",
      baseline_row_count: 1,
      comparison_row_count: 1,
      observed_presence: "both",
    },
    {
      key: "unassigned",
      kind: "unassigned",
      dimensions: { cluster_resource_id: null, topic_name: null },
      baseline_amount: "0",
      comparison_amount: "0",
      change: "0",
      percentage_change: null,
      baseline_row_count: 1,
      comparison_row_count: 1,
      observed_presence: "both",
    },
  ],
};

const CONTROL_PROPS: Omit<
  CostComparisonViewProps,
  "response" | "sourceLabel" | "isLoading" | "error" | "onInvestigate"
> = {
  source: "topic_attribution",
  granularity: "daily",
  preset: "previous_day",
  periods: {
    baseline: { start_date: "2026-03-07", end_date: "2026-03-08" },
    comparison: { start_date: "2026-03-14", end_date: "2026-03-15" },
    timezone: "America/Chicago",
  },
  groupBy: "topic",
  movement: "all",
  sortBy: "absolute_change",
  sortDirection: "desc",
  limit: 100,
  onPresetChange: vi.fn(),
  onPeriodsChange: vi.fn(),
  onGroupByChange: vi.fn(),
  onMovementChange: vi.fn(),
  onSortChange: vi.fn(),
  onLimitChange: vi.fn(),
};

function summaryValue(label: string): string | null {
  const value = screen
    .getByText(label, { selector: "dt" })
    .parentElement?.querySelector("dd");
  return value?.textContent ?? null;
}

describe("CostComparisonView", () => {
  async function chooseSelectOption(
    label: string,
    option: string,
  ): Promise<void> {
    fireEvent.mouseDown(screen.getByRole("combobox", { name: label }));
    await userEvent.click(
      await screen.findByText(option, {
        selector: ".ant-select-item-option-content",
      }),
    );
  }

  it("renders source-specific periods, financial summary, coverage qualifications, and zero-sum omissions", () => {
    render(
      <CostComparisonView
        {...CONTROL_PROPS}
        response={RESPONSE}
        sourceLabel="Topic Attribution — attributed Kafka costs, not the full tenant bill"
        isLoading={false}
        error={null}
        onInvestigate={vi.fn()}
      />,
    );

    expect(
      screen.getByText("Topic Attribution — attributed Kafka costs, not the full tenant bill"),
    ).toBeInTheDocument();
    expect(screen.getByText("2026-03-07 – 2026-03-08")).toBeInTheDocument();
    expect(screen.getAllByText("America/Chicago").length).toBeGreaterThan(0);
    expect(screen.getByText("$0.30")).toBeInTheDocument();
    expect(screen.getByText("Unavailable")).toBeInTheDocument();
    expect(screen.getByText(/unequal durations/i)).toBeInTheDocument();
    expect(screen.getByText(/unknown coverage/i)).toBeInTheDocument();
    expect(
      screen.getByText(/retention qualification applies to.*2026-03-08/i),
    ).toBeInTheDocument();
    expect(
      screen.getByText(/2 groups excluded by movement filter/i),
    ).toBeInTheDocument();
    expect(screen.getByText(/1 group outside top n/i)).toBeInTheDocument();
    expect(screen.getByText("Baseline total observed")).toBeInTheDocument();
    expect(screen.getByText("Comparison total observed")).toBeInTheDocument();
    expect(summaryValue("Baseline total observed")).toBe("$0.00");
    expect(summaryValue("Comparison total observed")).toBe("$0.30");
    expect(summaryValue("Increases observed")).toBe("$12.40");
    expect(summaryValue("Decreases observed")).toBe("$12.10");
    expect(summaryValue("Net change observed")).toBe("+$0.30");
    expect(summaryValue("Percentage change observed")).toBe("Unavailable");
    expect(
      screen.getByText(/financial values are observed totals/i),
    ).toBeInTheDocument();
    const reconciliation = screen.getByRole("region", {
      name: "Reconciliation",
    });
    expect(reconciliation).toHaveTextContent(
      "2 groups excluded by movement filter: $1.00 observed baseline, $1.00 observed comparison, $0.00 observed net.",
    );
    expect(reconciliation).toHaveTextContent(
      "1 group outside top N: -$1.00 observed baseline, -$1.00 observed comparison, $0.00 observed net.",
    );
    expect(screen.getByRole("radiogroup", { name: "Movement" })).toBeInTheDocument();
    expect(screen.getByRole("radio", { name: /All$/ })).toBeInTheDocument();
    expect(screen.getByRole("radio", { name: "Increases" })).toBeInTheDocument();
    expect(screen.getByRole("radio", { name: "Decreases" })).toBeInTheDocument();
  });

  it("keeps duplicate topic names distinct by cluster and disables invalid row navigation", async () => {
    const onInvestigate = vi.fn();
    render(
      <CostComparisonView
        {...CONTROL_PROPS}
        response={RESPONSE}
        sourceLabel="Topic Attribution — attributed Kafka costs, not the full tenant bill"
        isLoading={false}
        error={null}
        onInvestigate={onInvestigate}
      />,
    );

    expect(screen.getByText("lkc-a")).toBeInTheDocument();
    expect(screen.getByText("lkc-b")).toBeInTheDocument();
    const actions = screen.getAllByRole("button", {
      name: "Open filtered Topic Attribution list",
    });
    expect(actions).toHaveLength(2);

    await userEvent.click(actions[0]);
    expect(onInvestigate).toHaveBeenCalledWith(RESPONSE.rows[0]);
    expect(
      screen.queryAllByRole("button", {
        name: "Open filtered Topic Attribution list",
      }),
    ).toHaveLength(2);
  });

  it("changes movement, sorting, and row limit without replacing the full-scope summary", async () => {
    const onMovementChange = vi.fn();
    const onSortChange = vi.fn();
    const onLimitChange = vi.fn();
    render(
      <CostComparisonView
        {...CONTROL_PROPS}
        response={RESPONSE}
        sourceLabel="Topic Attribution — attributed Kafka costs, not the full tenant bill"
        isLoading={false}
        error={null}
        onInvestigate={vi.fn()}
        movement="all"
        sortBy="absolute_change"
        sortDirection="desc"
        limit={100}
        onMovementChange={onMovementChange}
        onSortChange={onSortChange}
        onLimitChange={onLimitChange}
      />,
    );

    const summaryBefore = screen.getByText("$0.30").textContent;
    await userEvent.click(screen.getByRole("radio", { name: "Increases" }));
    await chooseSelectOption("Sort by", "Baseline amount");
    await chooseSelectOption("Rows", "25");

    expect(onMovementChange).toHaveBeenCalledWith("increase");
    expect(onSortChange).toHaveBeenCalledWith("baseline_amount", "desc");
    expect(onLimitChange).toHaveBeenCalledWith(25);
    expect(screen.getByText("$0.30").textContent).toBe(summaryBefore);
  });

  it("renders loading, empty, and error states", () => {
    const { rerender } = render(
      <CostComparisonView
        {...CONTROL_PROPS}
        response={null}
        sourceLabel="Chargeback — allocated tenant costs"
        isLoading
        error={null}
        onInvestigate={vi.fn()}
      />,
    );
    expect(screen.getByText(/loading comparison/i)).toBeInTheDocument();

    rerender(
      <CostComparisonView
        {...CONTROL_PROPS}
        response={{ ...RESPONSE, rows: [] }}
        sourceLabel="Chargeback — allocated tenant costs"
        isLoading={false}
        error={null}
        onInvestigate={vi.fn()}
      />,
    );
    expect(screen.getByText(/no groups matched/i)).toBeInTheDocument();

    rerender(
      <CostComparisonView
        {...CONTROL_PROPS}
        response={null}
        sourceLabel="Chargeback — allocated tenant costs"
        isLoading={false}
        error="HTTP 503: Unavailable"
        onInvestigate={vi.fn()}
      />,
    );
    expect(screen.getByText("HTTP 503: Unavailable")).toBeInTheDocument();
  });

  it("names the absent period in qualified one-sided presence states", () => {
    const response: CostComparisonResponse = {
      ...RESPONSE,
      comparison: {
        ...RESPONSE.comparison,
        coverage: {
          ...RESPONSE.comparison.coverage,
          status: "unknown",
          unknown_dates: ["2026-03-15"],
        },
      },
      rows: [
        RESPONSE.rows[0],
        {
          key: "topic:lkc-c:payments",
          kind: "entity",
          dimensions: { cluster_resource_id: "lkc-c", topic_name: "payments" },
          baseline_amount: "0.3",
          comparison_amount: "0",
          change: "-0.3",
          percentage_change: "-100",
          baseline_row_count: 1,
          comparison_row_count: 0,
          observed_presence: "baseline_only",
        },
      ],
    };
    render(
      <CostComparisonView
        {...CONTROL_PROPS}
        response={response}
        sourceLabel="Topic Attribution — attributed Kafka costs, not the full tenant bill"
        isLoading={false}
        error={null}
        onInvestigate={vi.fn()}
      />,
    );

    expect(screen.getByText("No cost observed in baseline period")).toBeInTheDocument();
    expect(screen.getByText("No cost observed in comparison period")).toBeInTheDocument();
  });

  it("keeps previous day and week visible but disabled for monthly data", async () => {
    render(
      <CostComparisonView
        {...CONTROL_PROPS}
        source="chargeback"
        granularity="monthly"
        preset="calendar_month"
        periods={{ ...CONTROL_PROPS.periods, timezone: "UTC" }}
        groupBy="principal"
        response={null}
        sourceLabel="Chargeback — allocated tenant costs"
        isLoading={false}
        error={null}
        onInvestigate={vi.fn()}
      />,
    );

    fireEvent.mouseDown(screen.getByRole("combobox", { name: "Preset" }));
    const previousDay = await screen.findByText("Previous day", {
      selector: ".ant-select-item-option-content",
    });
    const previousWeek = await screen.findByText("Previous week", {
      selector: ".ant-select-item-option-content",
    });
    expect(previousDay.closest(".ant-select-item-option-disabled")).toBeTruthy();
    expect(previousWeek.closest(".ant-select-item-option-disabled")).toBeTruthy();
    expect(
      screen.getByRole("combobox", { name: "Comparison timezone" }),
    ).toBeDisabled();
  });

  it("edits custom dates and only warns for invalid monthly ranges", () => {
    const onPeriodsChange = vi.fn();
    const invalidPeriods = {
      baseline: { start_date: "2026-01-02", end_date: "2026-01-31" },
      comparison: { start_date: "2026-02-01", end_date: "2026-02-28" },
      timezone: "America/Chicago",
    };
    const validPeriods = {
      baseline: { start_date: "2026-01-01", end_date: "2026-01-31" },
      comparison: { start_date: "2026-02-01", end_date: "2026-02-28" },
      timezone: "UTC",
    };
    const { rerender } = render(
      <CostComparisonView
        {...CONTROL_PROPS}
        source="chargeback"
        granularity="monthly"
        preset="custom"
        periods={invalidPeriods}
        groupBy="principal"
        response={null}
        sourceLabel="Chargeback — allocated tenant costs"
        isLoading={false}
        error={null}
        onPeriodsChange={onPeriodsChange}
        onInvestigate={vi.fn()}
      />,
    );

    expect(screen.getByRole("alert")).toHaveTextContent(
      "Monthly comparison periods must contain complete UTC calendar months",
    );
    fireEvent.change(screen.getByLabelText("Baseline start date"), {
      target: { value: "2026-01-01" },
    });
    expect(onPeriodsChange).toHaveBeenLastCalledWith({
      ...invalidPeriods,
      baseline: { start_date: "2026-01-01", end_date: "2026-01-31" },
    });

    rerender(
      <CostComparisonView
        {...CONTROL_PROPS}
        source="chargeback"
        granularity="monthly"
        preset="custom"
        periods={validPeriods}
        groupBy="principal"
        response={null}
        sourceLabel="Chargeback — allocated tenant costs"
        isLoading={false}
        error={null}
        onPeriodsChange={onPeriodsChange}
        onInvestigate={vi.fn()}
      />,
    );
    expect(screen.queryByRole("alert")).not.toBeInTheDocument();
  });
});
