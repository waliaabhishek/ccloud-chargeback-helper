import type React from "react";
import { Segmented, Select, Space } from "antd";
import type {
  ComparisonGroup,
  ComparisonGranularity,
  ComparisonMovement,
  ComparisonPreset,
  ComparisonSortBy,
  ComparisonSortDirection,
} from "../../types/api";
import type { ComparisonPeriodSelection } from "../../utils/comparisonPeriods";
import { validateMonthlyCustomPeriods } from "../../utils/comparisonPeriods";
import { TIMEZONE_OPTIONS } from "../../utils/timezoneOptions";

export interface ComparisonControlsProps {
  source: "chargeback" | "topic_attribution";
  granularity: ComparisonGranularity;
  preset: ComparisonPreset;
  periods: ComparisonPeriodSelection;
  groupBy: ComparisonGroup;
  movement: ComparisonMovement;
  sortBy: ComparisonSortBy;
  sortDirection: ComparisonSortDirection;
  limit: number;
  onPresetChange: (preset: ComparisonPreset) => void;
  onPeriodsChange: (periods: ComparisonPeriodSelection) => void;
  onGroupByChange: (groupBy: ComparisonGroup) => void;
  onMovementChange: (movement: ComparisonMovement) => void;
  onSortChange: (
    sortBy: ComparisonSortBy,
    sortDirection: ComparisonSortDirection,
  ) => void;
  onLimitChange: (limit: number) => void;
}

const SORT_OPTIONS: Array<{ label: string; value: ComparisonSortBy }> = [
  { label: "Absolute change", value: "absolute_change" },
  { label: "Entity", value: "entity" },
  { label: "Baseline amount", value: "baseline_amount" },
  { label: "Comparison amount", value: "comparison_amount" },
  { label: "Change", value: "change" },
  { label: "Percentage change", value: "percentage_change" },
];

const LIMIT_OPTIONS = [25, 50, 100, 250, 500];

function updatePeriod(
  periods: ComparisonPeriodSelection,
  periodName: "baseline" | "comparison",
  field: "start_date" | "end_date",
  value: string,
): ComparisonPeriodSelection {
  return {
    ...periods,
    [periodName]: { ...periods[periodName], [field]: value },
  };
}

export function ComparisonControls({
  source,
  granularity,
  preset,
  periods,
  groupBy,
  movement,
  sortBy,
  sortDirection,
  limit,
  onPresetChange,
  onPeriodsChange,
  onGroupByChange,
  onMovementChange,
  onSortChange,
  onLimitChange,
}: ComparisonControlsProps): React.JSX.Element {
  const monthly = granularity === "monthly";
  const timezoneOptions = TIMEZONE_OPTIONS.map(({ value }) => value);
  const groupOptions =
    source === "chargeback"
      ? ([
          { label: "Principal", value: "principal" },
          { label: "Resource", value: "resource" },
          { label: "Environment", value: "environment" },
        ] as const)
      : ([
          { label: "Topic", value: "topic" },
          { label: "Cluster", value: "cluster" },
        ] as const);

  const custom = preset === "custom";
  const monthlyCustomValidation =
    monthly && custom ? validateMonthlyCustomPeriods(periods) : null;
  const setPeriod = (
    periodName: "baseline" | "comparison",
    field: "start_date" | "end_date",
    value: string,
  ): void => {
    onPeriodsChange(updatePeriod(periods, periodName, field, value));
  };

  return (
    <div
      aria-label="Comparison controls"
      style={{ width: "100%" }}
    >
      <Space wrap size="small" style={{ width: "100%" }}>
          <label>
            Preset
            <Select
              aria-label="Preset"
              style={{ minWidth: 150, marginLeft: 4 }}
              value={preset}
              options={[
                { label: "Previous day", value: "previous_day", disabled: monthly },
                { label: "Previous week", value: "previous_week", disabled: monthly },
                { label: "Calendar month", value: "calendar_month" },
                { label: "Custom", value: "custom" },
              ]}
              onChange={(event) =>
                onPresetChange(event as ComparisonPreset)
              }
            />
          </label>
          <label>
            Group by
            <Select
              aria-label="Group by"
              style={{ minWidth: 140, marginLeft: 4 }}
              value={groupBy}
              options={[...groupOptions]}
              onChange={(value) => onGroupByChange(value as ComparisonGroup)}
            />
          </label>
          <label>
            Comparison timezone
            <Select
              aria-label="Comparison timezone"
              style={{ minWidth: 190, marginLeft: 4 }}
              value={monthly ? "UTC" : periods.timezone}
              disabled={monthly}
              options={(monthly && !timezoneOptions.includes("UTC")
                ? ["UTC", ...timezoneOptions]
                : timezoneOptions
              ).map((timezone) => ({ label: timezone, value: timezone }))}
              onChange={(value) =>
                onPeriodsChange({ ...periods, timezone: value })
              }
            />
          </label>
      </Space>

      {custom && (
        <Space wrap size="small" style={{ width: "100%", marginTop: 8 }}>
          <fieldset>
            <legend>Baseline period</legend>
            <label>
              Baseline start date
              <input
                aria-label="Baseline start date"
                type="date"
                value={periods.baseline.start_date}
                onChange={(event) =>
                  setPeriod("baseline", "start_date", event.target.value)
                }
              />
            </label>
            <label>
              Baseline end date
              <input
                aria-label="Baseline end date"
                type="date"
                value={periods.baseline.end_date}
                onChange={(event) =>
                  setPeriod("baseline", "end_date", event.target.value)
                }
              />
            </label>
          </fieldset>
          <fieldset>
            <legend>Comparison period</legend>
            <label>
              Comparison start date
              <input
                aria-label="Comparison start date"
                type="date"
                value={periods.comparison.start_date}
                onChange={(event) =>
                  setPeriod("comparison", "start_date", event.target.value)
                }
              />
            </label>
            <label>
              Comparison end date
              <input
                aria-label="Comparison end date"
                type="date"
                value={periods.comparison.end_date}
                onChange={(event) =>
                  setPeriod("comparison", "end_date", event.target.value)
                }
              />
            </label>
          </fieldset>
          {monthly && monthlyCustomValidation && !monthlyCustomValidation.valid && (
            <p role="alert" style={{ margin: 0 }}>
              {monthlyCustomValidation.message ??
                "Monthly comparison periods must contain complete UTC calendar months."}
            </p>
          )}
        </Space>
      )}

      <Space wrap size="small" style={{ width: "100%", marginTop: 8 }}>
        <div style={{ display: "flex", alignItems: "center", gap: 4 }}>
          <span>Movement</span>
          <Segmented
            aria-label="Movement"
            style={{ marginLeft: 4 }}
            options={[
              { label: "All", value: "all" },
              { label: "Increases", value: "increase" },
              { label: "Decreases", value: "decrease" },
            ]}
            value={movement}
            onChange={(value) => onMovementChange(value as ComparisonMovement)}
          />
        </div>
        <label>
          Sort by
          <Select
            aria-label="Sort by"
            style={{ minWidth: 160, marginLeft: 4 }}
            value={sortBy}
            options={SORT_OPTIONS}
            onChange={(value) =>
              onSortChange(value as ComparisonSortBy, sortDirection)
            }
          />
        </label>
        <label>
          Sort direction
          <Select
            aria-label="Sort direction"
            style={{ minWidth: 130, marginLeft: 4 }}
            value={sortDirection}
            options={[
              { label: "Descending", value: "desc" },
              { label: "Ascending", value: "asc" },
            ]}
            onChange={(value) =>
              onSortChange(sortBy, value as ComparisonSortDirection)
            }
          />
        </label>
        <label>
          Rows
          <Select
            aria-label="Rows"
            style={{ minWidth: 90, marginLeft: 4 }}
            value={String(limit)}
            options={LIMIT_OPTIONS.map((option) => ({
              label: String(option),
              value: String(option),
            }))}
            onChange={(value) => onLimitChange(Number(value))}
          />
        </label>
      </Space>
    </div>
  );
}
