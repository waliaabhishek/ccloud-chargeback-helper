import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import type {
  ComparisonGranularity,
  ComparisonGroup,
  ComparisonMovement,
  ComparisonPreset,
  ComparisonSortBy,
  ComparisonSortDirection,
  TenantStatusSummary,
} from "../types/api";
import type { ComparisonPeriodSelection } from "../utils/comparisonPeriods";
import {
  getComparisonPeriods,
  validateMonthlyCustomPeriods,
} from "../utils/comparisonPeriods";

export interface ComparisonControllerState<Group extends ComparisonGroup> {
  tenantName: string;
  granularity: ComparisonGranularity;
  preset: ComparisonPreset;
  periods: ComparisonPeriodSelection;
  groupBy: Group;
  movement: ComparisonMovement;
  sortBy: ComparisonSortBy;
  sortDirection: ComparisonSortDirection;
  limit: number;
}

export interface UseComparisonControllerOptions<Group extends ComparisonGroup> {
  tenant: Pick<
    TenantStatusSummary,
    "tenant_name" | "chargeback_granularity"
  > | null;
  defaultGroup: Group;
  pageTimezone: string | null | undefined;
}

export interface UseComparisonControllerResult<
  Group extends ComparisonGroup,
> {
  comparison: ComparisonControllerState<Group> | null;
  ready: boolean;
  periodsValid: boolean;
  onPresetChange: (preset: ComparisonPreset) => void;
  onPeriodsChange: (periods: ComparisonPeriodSelection) => void;
  onGroupByChange: (groupBy: Group) => void;
  onMovementChange: (movement: ComparisonMovement) => void;
  onSortChange: (
    sortBy: ComparisonSortBy,
    sortDirection: ComparisonSortDirection,
  ) => void;
  onLimitChange: (limit: number) => void;
}

function tenantGranularity(
  value: TenantStatusSummary["chargeback_granularity"],
): ComparisonGranularity {
  return value === "hourly" || value === "monthly" ? value : "daily";
}

function createComparisonState<Group extends ComparisonGroup>(
  tenantName: string,
  granularity: ComparisonGranularity,
  pageTimezone: string | null | undefined,
  defaultGroup: Group,
): ComparisonControllerState<Group> {
  const preset: ComparisonPreset =
    granularity === "monthly" ? "calendar_month" : "previous_day";
  return {
    tenantName,
    granularity,
    preset,
    periods: getComparisonPeriods({
      preset,
      granularity,
      timezone: pageTimezone ?? "UTC",
    }),
    groupBy: defaultGroup,
    movement: "all",
    sortBy: "absolute_change",
    sortDirection: "desc",
    limit: 100,
  };
}

export function useComparisonController<
  Group extends ComparisonGroup,
>({
  tenant,
  defaultGroup,
  pageTimezone,
}: UseComparisonControllerOptions<Group>): UseComparisonControllerResult<Group> {
  const tenantName = tenant?.tenant_name ?? null;
  const granularity = tenantGranularity(tenant?.chargeback_granularity);
  const pageTimezoneRef = useRef(pageTimezone ?? "UTC");
  pageTimezoneRef.current = pageTimezone ?? "UTC";
  const [storedComparison, setStoredComparison] = useState<
    ComparisonControllerState<Group> | null
  >(() =>
    tenantName
      ? createComparisonState(
          tenantName,
          granularity,
          pageTimezoneRef.current,
          defaultGroup,
        )
      : null,
  );

  useEffect(() => {
    if (!tenantName) {
      setStoredComparison(null);
      return;
    }
    setStoredComparison((previous) => {
      if (
        previous?.tenantName === tenantName &&
        previous.granularity === granularity
      ) {
        return previous;
      }
      return createComparisonState(
        tenantName,
        granularity,
        pageTimezoneRef.current,
        defaultGroup,
      );
    });
  }, [defaultGroup, granularity, tenantName]);

  const activeComparison =
    storedComparison?.tenantName === tenantName &&
    storedComparison.granularity === granularity
      ? storedComparison
      : null;
  const comparison =
    activeComparison ??
    (tenantName
      ? createComparisonState(
          tenantName,
          granularity,
          pageTimezoneRef.current,
          defaultGroup,
        )
      : null);
  const ready = activeComparison !== null;

  const update = useCallback(
    (
      updater: (
        previous: ComparisonControllerState<Group>,
      ) => ComparisonControllerState<Group>,
    ): void => {
      setStoredComparison((previous) => {
        if (
          previous?.tenantName !== tenantName ||
          previous.granularity !== granularity
        ) {
          return previous;
        }
        return updater(previous);
      });
    },
    [granularity, tenantName],
  );

  const onPresetChange = useCallback(
    (preset: ComparisonPreset): void => {
      update((previous) =>
        preset === "custom"
          ? { ...previous, preset }
          : {
              ...previous,
              preset,
              periods: getComparisonPeriods({
                preset,
                granularity: previous.granularity,
                timezone: previous.periods.timezone,
              }),
            },
      );
    },
    [update],
  );

  const onPeriodsChange = useCallback(
    (periods: ComparisonPeriodSelection): void => {
      update((previous) => {
        const effectivePeriods =
          previous.granularity === "monthly"
            ? { ...periods, timezone: "UTC" }
            : periods;
        return previous.preset === "custom"
          ? { ...previous, periods: effectivePeriods }
          : {
              ...previous,
              periods: getComparisonPeriods({
                preset: previous.preset,
                granularity: previous.granularity,
                timezone: effectivePeriods.timezone,
              }),
            };
      });
    },
    [update],
  );

  const onGroupByChange = useCallback(
    (groupBy: Group): void => {
      update((previous) => ({ ...previous, groupBy }));
    },
    [update],
  );

  const onMovementChange = useCallback(
    (movement: ComparisonMovement): void => {
      update((previous) => ({ ...previous, movement }));
    },
    [update],
  );

  const onSortChange = useCallback(
    (
      sortBy: ComparisonSortBy,
      sortDirection: ComparisonSortDirection,
    ): void => {
      update((previous) => ({ ...previous, sortBy, sortDirection }));
    },
    [update],
  );

  const onLimitChange = useCallback(
    (limit: number): void => {
      update((previous) => ({ ...previous, limit }));
    },
    [update],
  );

  const periodsValid = useMemo(
    () =>
      ready &&
      !!comparison &&
      (comparison.preset !== "custom" ||
        comparison.granularity !== "monthly" ||
        validateMonthlyCustomPeriods(comparison.periods).valid),
    [comparison, ready],
  );

  return {
    comparison,
    ready,
    periodsValid,
    onPresetChange,
    onPeriodsChange,
    onGroupByChange,
    onMovementChange,
    onSortChange,
    onLimitChange,
  };
}
