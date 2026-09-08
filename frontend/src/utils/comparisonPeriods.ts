import type {
  ComparisonDateRange,
  ComparisonGranularity,
  ComparisonPreset,
} from "../types/api";

export type { ComparisonPreset } from "../types/api";

export interface ComparisonPeriodSelection {
  baseline: ComparisonDateRange;
  comparison: ComparisonDateRange;
  timezone: string;
}

export interface ComparisonPeriodInput {
  preset: ComparisonPreset;
  granularity: ComparisonGranularity;
  timezone: string;
  now?: Date;
  custom?: {
    baseline: ComparisonDateRange;
    comparison: ComparisonDateRange;
  };
}

interface CivilDate {
  year: number;
  month: number;
  day: number;
}

function pad(value: number): string {
  return String(value).padStart(2, "0");
}

function formatCivilDate(date: CivilDate): string {
  return `${String(date.year).padStart(4, "0")}-${pad(date.month)}-${pad(date.day)}`;
}

function civilDateToEpoch(date: CivilDate): number {
  return Date.UTC(date.year, date.month - 1, date.day);
}

function epochToCivilDate(epoch: number): CivilDate {
  const date = new Date(epoch);
  return {
    year: date.getUTCFullYear(),
    month: date.getUTCMonth() + 1,
    day: date.getUTCDate(),
  };
}

function addCivilDays(date: CivilDate, days: number): CivilDate {
  return epochToCivilDate(civilDateToEpoch(date) + days * 86_400_000);
}

function startOfMonth(date: CivilDate): CivilDate {
  return { year: date.year, month: date.month, day: 1 };
}

function endOfMonth(date: CivilDate): CivilDate {
  return addCivilDays(
    {
      year: date.month === 12 ? date.year + 1 : date.year,
      month: date.month === 12 ? 1 : date.month + 1,
      day: 1,
    },
    -1,
  );
}

function getLocalCivilDate(now: Date, timezone: string): CivilDate {
  const parts = new Intl.DateTimeFormat("en-US", {
    timeZone: timezone,
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
  }).formatToParts(now);
  const values = Object.fromEntries(
    parts
      .filter((part) => part.type !== "literal")
      .map((part) => [part.type, part.value]),
  );
  return {
    year: Number(values.year),
    month: Number(values.month),
    day: Number(values.day),
  };
}

function period(
  start: CivilDate,
  end: CivilDate,
): ComparisonDateRange {
  return {
    start_date: formatCivilDate(start),
    end_date: formatCivilDate(end),
  };
}

/** Compute the two latest completed calendar periods from one captured instant. */
export function getComparisonPeriods({
  preset,
  granularity,
  timezone,
  now = new Date(),
  custom,
}: ComparisonPeriodInput): ComparisonPeriodSelection {
  const effectiveTimezone = granularity === "monthly" ? "UTC" : timezone;
  if (preset === "custom") {
    if (!custom) {
      throw new Error("Custom comparison periods require four dates");
    }
    return { ...custom, timezone: effectiveTimezone };
  }

  const today = getLocalCivilDate(now, effectiveTimezone);
  if (preset === "previous_day") {
    const comparison = addCivilDays(today, -1);
    const baseline = addCivilDays(comparison, -1);
    return {
      baseline: period(baseline, baseline),
      comparison: period(comparison, comparison),
      timezone: effectiveTimezone,
    };
  }

  if (preset === "previous_week") {
    const weekday = new Date(civilDateToEpoch(today)).getUTCDay();
    const daysSinceMonday = (weekday + 6) % 7;
    const comparisonEnd = addCivilDays(today, -daysSinceMonday - 1);
    const comparisonStart = addCivilDays(comparisonEnd, -6);
    return {
      baseline: period(addCivilDays(comparisonStart, -7), addCivilDays(comparisonEnd, -7)),
      comparison: period(comparisonStart, comparisonEnd),
      timezone: effectiveTimezone,
    };
  }

  const comparisonEnd = addCivilDays(startOfMonth(today), -1);
  const comparisonStart = startOfMonth(comparisonEnd);
  const baselineEnd = addCivilDays(comparisonStart, -1);
  return {
    baseline: period(startOfMonth(baselineEnd), baselineEnd),
    comparison: period(comparisonStart, comparisonEnd),
    timezone: effectiveTimezone,
  };
}

function parseCivilDate(value: string): CivilDate {
  const match = /^(\d{4})-(\d{2})-(\d{2})$/.exec(value);
  if (!match) throw new Error(`Invalid date: ${value}`);
  const date = {
    year: Number(match[1]),
    month: Number(match[2]),
    day: Number(match[3]),
  };
  if (
    date.month < 1 ||
    date.month > 12 ||
    date.day < 1 ||
    date.day > endOfMonth(date).day
  ) {
    throw new Error(`Invalid date: ${value}`);
  }
  return date;
}

function isCompleteUtcMonthRange(range: ComparisonDateRange): boolean {
  try {
    const start = parseCivilDate(range.start_date);
    const end = parseCivilDate(range.end_date);
    return start.day === 1 && end.day === endOfMonth(end).day && civilDateToEpoch(start) <= civilDateToEpoch(end);
  } catch {
    return false;
  }
}

export function validateMonthlyCustomPeriods({
  baseline,
  comparison,
}: {
  baseline: ComparisonDateRange;
  comparison: ComparisonDateRange;
}): { valid: boolean; message: string | null } {
  const valid =
    isCompleteUtcMonthRange(baseline) && isCompleteUtcMonthRange(comparison);
  return {
    valid,
    message: valid
      ? null
      : "Monthly comparison periods must contain complete UTC calendar months",
  };
}
