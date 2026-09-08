import { describe, expect, it } from "vitest";
import {
  getComparisonPeriods,
  validateMonthlyCustomPeriods,
} from "./comparisonPeriods";

const APRIL_1_UTC = new Date("2026-04-01T00:30:00.000Z");

describe("getComparisonPeriods", () => {
  it("computes previous day from the selected timezone calendar date", () => {
    expect(
      getComparisonPeriods({
        preset: "previous_day",
        granularity: "daily",
        timezone: "America/Los_Angeles",
        now: APRIL_1_UTC,
      }),
    ).toEqual({
      baseline: { start_date: "2026-03-29", end_date: "2026-03-29" },
      comparison: { start_date: "2026-03-30", end_date: "2026-03-30" },
      timezone: "America/Los_Angeles",
    });
  });

  it("uses the two most recently completed Monday-Sunday weeks", () => {
    expect(
      getComparisonPeriods({
        preset: "previous_week",
        granularity: "hourly",
        timezone: "UTC",
        now: new Date("2026-04-01T12:00:00.000Z"),
      }),
    ).toEqual({
      baseline: { start_date: "2026-03-16", end_date: "2026-03-22" },
      comparison: { start_date: "2026-03-23", end_date: "2026-03-29" },
      timezone: "UTC",
    });
  });

  it("handles leap-year and year-boundary completed calendar months", () => {
    expect(
      getComparisonPeriods({
        preset: "calendar_month",
        granularity: "daily",
        timezone: "UTC",
        now: new Date("2024-03-01T00:00:00.000Z"),
      }),
    ).toEqual({
      baseline: { start_date: "2024-01-01", end_date: "2024-01-31" },
      comparison: { start_date: "2024-02-01", end_date: "2024-02-29" },
      timezone: "UTC",
    });

    expect(
      getComparisonPeriods({
        preset: "calendar_month",
        granularity: "daily",
        timezone: "Asia/Tokyo",
        now: new Date("2026-01-01T00:30:00.000Z"),
      }),
    ).toEqual({
      baseline: { start_date: "2025-11-01", end_date: "2025-11-30" },
      comparison: { start_date: "2025-12-01", end_date: "2025-12-31" },
      timezone: "Asia/Tokyo",
    });
  });

  it("preserves literal Custom dates while applying the newly selected timezone", () => {
    expect(
      getComparisonPeriods({
        preset: "custom",
        granularity: "daily",
        timezone: "Europe/London",
        now: APRIL_1_UTC,
        custom: {
          baseline: { start_date: "2026-03-07", end_date: "2026-03-08" },
          comparison: { start_date: "2026-03-14", end_date: "2026-03-15" },
        },
      }),
    ).toEqual({
      baseline: { start_date: "2026-03-07", end_date: "2026-03-08" },
      comparison: { start_date: "2026-03-14", end_date: "2026-03-15" },
      timezone: "Europe/London",
    });
  });

  it("forces UTC and offers only completed calendar months for monthly data", () => {
    expect(
      getComparisonPeriods({
        preset: "calendar_month",
        granularity: "monthly",
        timezone: "America/Los_Angeles",
        now: APRIL_1_UTC,
      }),
    ).toEqual({
      baseline: { start_date: "2026-02-01", end_date: "2026-02-28" },
      comparison: { start_date: "2026-03-01", end_date: "2026-03-31" },
      timezone: "UTC",
    });
  });
});

describe("validateMonthlyCustomPeriods", () => {
  it("accepts multi-month UTC calendar ranges and rejects partial months", () => {
    expect(
      validateMonthlyCustomPeriods({
        baseline: { start_date: "2026-01-01", end_date: "2026-02-28" },
        comparison: { start_date: "2026-03-01", end_date: "2026-04-30" },
      }),
    ).toEqual({ valid: true, message: null });

    expect(
      validateMonthlyCustomPeriods({
        baseline: { start_date: "2026-01-02", end_date: "2026-01-31" },
        comparison: { start_date: "2026-02-01", end_date: "2026-02-28" },
      }),
    ).toEqual({
      valid: false,
      message: "Monthly comparison periods must contain complete UTC calendar months",
    });
  });
});
