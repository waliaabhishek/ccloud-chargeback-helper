import { act, renderHook } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";
import type {
  ChargebackComparisonGroup,
  TenantStatusSummary,
} from "../types/api";
import type { ComparisonPeriodSelection } from "../utils/comparisonPeriods";
import { useComparisonController } from "./useComparisonController";

type TenantInput = Pick<
  TenantStatusSummary,
  "tenant_name" | "chargeback_granularity"
>;

const dailyTenant: TenantInput = {
  tenant_name: "daily",
  chargeback_granularity: "daily",
};

const monthlyTenant: TenantInput = {
  tenant_name: "monthly",
  chargeback_granularity: "monthly",
};

const customPeriods: ComparisonPeriodSelection = {
  baseline: { start_date: "2026-01-01", end_date: "2026-01-31" },
  comparison: { start_date: "2026-02-01", end_date: "2026-02-28" },
  timezone: "America/Los_Angeles",
};

afterEach(() => {
  vi.useRealTimers();
});

describe("useComparisonController", () => {
  it("shares defaults and resets all local state when tenant granularity changes", () => {
    const { result, rerender } = renderHook(
      ({ tenant, timezone }: { tenant: TenantInput | null; timezone: string }) =>
        useComparisonController<ChargebackComparisonGroup>({
          tenant,
          defaultGroup: "principal",
          pageTimezone: timezone,
        }),
      { initialProps: { tenant: dailyTenant, timezone: "UTC" } },
    );

    act(() => {
      result.current.onGroupByChange("resource");
      result.current.onMovementChange("decrease");
      result.current.onLimitChange(25);
    });
    expect(result.current.comparison?.groupBy).toBe("resource");
    expect(result.current.comparison?.movement).toBe("decrease");
    expect(result.current.comparison?.limit).toBe(25);

    rerender({ tenant: monthlyTenant, timezone: "America/Chicago" });

    expect(result.current.ready).toBe(true);
    expect(result.current.comparison).toMatchObject({
      tenantName: "monthly",
      granularity: "monthly",
      preset: "calendar_month",
      groupBy: "principal",
      movement: "all",
      sortBy: "absolute_change",
      sortDirection: "desc",
      limit: 100,
    });
    expect(result.current.comparison?.periods.timezone).toBe("UTC");
  });

  it("recomputes named periods in the selected timezone and preserves custom dates", () => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date("2026-04-01T01:30:00.000Z"));
    const { result } = renderHook(() =>
      useComparisonController<ChargebackComparisonGroup>({
        tenant: dailyTenant,
        defaultGroup: "principal",
        pageTimezone: "America/Los_Angeles",
      }),
    );

    expect(result.current.comparison?.periods.comparison.start_date).toBe(
      "2026-03-30",
    );
    act(() => {
      result.current.onPeriodsChange({
        baseline: { start_date: "", end_date: "" },
        comparison: { start_date: "", end_date: "" },
        timezone: "Asia/Tokyo",
      });
    });
    expect(result.current.comparison?.periods.timezone).toBe("Asia/Tokyo");
    expect(result.current.comparison?.periods.comparison.start_date).toBe(
      "2026-03-31",
    );

    act(() => {
      result.current.onPresetChange("custom");
      result.current.onPeriodsChange(customPeriods);
    });
    expect(result.current.comparison?.periods).toEqual(customPeriods);
    expect(result.current.periodsValid).toBe(true);
  });

  it("blocks a monthly custom request until both periods are complete UTC months", () => {
    const { result } = renderHook(() =>
      useComparisonController({
        tenant: monthlyTenant,
        defaultGroup: "principal",
        pageTimezone: "America/Los_Angeles",
      }),
    );

    act(() => {
      result.current.onPresetChange("custom");
      result.current.onPeriodsChange({
        baseline: { start_date: "2026-01-02", end_date: "2026-01-31" },
        comparison: { start_date: "2026-02-01", end_date: "2026-02-28" },
        timezone: "America/Los_Angeles",
      });
    });
    expect(result.current.periodsValid).toBe(false);
    expect(result.current.comparison?.periods.timezone).toBe("UTC");
  });
});
