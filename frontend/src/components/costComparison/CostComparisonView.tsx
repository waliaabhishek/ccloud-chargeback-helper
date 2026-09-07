import type React from "react";
import { Alert, Card, Empty, Space, Table, Typography } from "antd";
import type {
  ComparisonDateRange,
  ComparisonGranularity,
  ComparisonGroup,
  ComparisonMovement,
  ComparisonPreset,
  ComparisonRow,
  ComparisonSortBy,
  ComparisonSortDirection,
  CostComparisonResponse,
} from "../../types/api";
import type { ComparisonPeriodSelection } from "../../utils/comparisonPeriods";
import { formatDecimalCurrency } from "../../utils/decimalCurrency";
import { ComparisonControls } from "./ComparisonControls";

export interface CostComparisonViewProps<
  Group extends ComparisonGroup = ComparisonGroup,
> {
  response: CostComparisonResponse | null;
  source: "chargeback" | "topic_attribution";
  sourceLabel: string;
  granularity: ComparisonGranularity;
  preset: ComparisonPreset;
  periods: ComparisonPeriodSelection;
  groupBy: Group;
  isLoading: boolean;
  error: string | null;
  onInvestigate: (row: ComparisonRow) => void;
  onPresetChange: (preset: ComparisonPreset) => void;
  onPeriodsChange: (periods: ComparisonPeriodSelection) => void;
  onGroupByChange: (groupBy: Group) => void;
  movement: ComparisonMovement;
  sortBy: ComparisonSortBy;
  sortDirection: ComparisonSortDirection;
  limit: number;
  onMovementChange: (movement: ComparisonMovement) => void;
  onSortChange: (
    sortBy: ComparisonSortBy,
    sortDirection: ComparisonSortDirection,
  ) => void;
  onLimitChange: (limit: number) => void;
}

const { Title, Text } = Typography;

function periodLabel(period: ComparisonDateRange): string {
  return `${period.start_date} – ${period.end_date}`;
}

function formatPercent(value: string | null): string {
  return value === null ? "Unavailable" : `${value}%`;
}

function formatRowPercent(value: string | null): string {
  return value === null ? "Unavailable (baseline is zero)" : `${value}%`;
}

function absoluteDecimal(value: string): string {
  return value.startsWith("-") || value.startsWith("+")
    ? value.slice(1)
    : value;
}

function formatDecrease(value: string): string {
  return formatDecimalCurrency(absoluteDecimal(value));
}

function formatSignedCurrency(value: string): string {
  return value.startsWith("-")
    ? formatDecimalCurrency(value)
    : `+${formatDecimalCurrency(value)}`;
}

function coverageMessage(
  label: string,
  period: CostComparisonResponse["baseline"],
): React.JSX.Element {
  const { coverage } = period;
  const dates = [...coverage.unknown_dates, ...coverage.incomplete_dates];
  return (
    <div>
      <strong>
        {label}: {coverage.status} coverage
      </strong>
      {dates.length > 0 && (
        <span> ({dates.join(", ")})</span>
      )}
      {coverage.retention_qualified_dates.length > 0 && (
        <span>
          {" "}
          Retention qualification applies to {coverage.retention_qualified_dates.join(
            ", ",
          )}
          .
        </span>
      )}
      {coverage.availability_cutoff_at !== null && (
        <span> Availability cutoff: {coverage.availability_cutoff_at}.</span>
      )}
    </div>
  );
}

function rowDimensions(row: ComparisonRow): string[] {
  const values = Object.entries(row.dimensions)
    .filter(([, value]) => value !== null && value !== "")
    .map(([, value]) => value as string);
  if (values.length > 0) return values;
  if (row.kind === "unassigned") return ["Unassigned"];
  if (row.kind === "sentinel") return [row.key];
  return [row.key];
}

function presenceLabel(
  row: ComparisonRow,
  response: CostComparisonResponse,
): string {
  if (row.observed_presence === "both") return "Both periods";
  const absentPeriod =
    row.observed_presence === "baseline_only"
      ? response.comparison
      : response.baseline;
  const observedPeriodName =
    row.observed_presence === "baseline_only" ? "baseline" : "comparison";
  const absentPeriodName =
    row.observed_presence === "baseline_only" ? "comparison" : "baseline";
  return absentPeriod.coverage.status === "complete"
    ? `Cost only in ${observedPeriodName} period`
    : `No cost observed in ${absentPeriodName} period`;
}

function ComparisonSummary({
  response,
  observed,
}: {
  response: CostComparisonResponse;
  observed: boolean;
}): React.JSX.Element {
  const label = (value: string): string =>
    observed ? `${value} observed` : value;
  return (
    <section aria-label="Comparison summary">
      <h4>Comparison summary</h4>
      <dl>
        <div>
          <dt>{label("Baseline total")}</dt>
          <dd>{formatDecimalCurrency(response.summary.baseline_amount)}</dd>
        </div>
        <div>
          <dt>{label("Comparison total")}</dt>
          <dd>{formatDecimalCurrency(response.summary.comparison_amount)}</dd>
        </div>
        <div>
          <dt>{label("Increases")}</dt>
          <dd>{formatDecimalCurrency(response.summary.increases)}</dd>
        </div>
        <div>
          <dt>{label("Decreases")}</dt>
          <dd>{formatDecrease(response.summary.decreases)}</dd>
        </div>
        <div>
          <dt>{label("Net change")}</dt>
          <dd>{formatSignedCurrency(response.summary.net_change)}</dd>
        </div>
        <div>
          <dt>{label("Percentage change")}</dt>
          <dd>{formatPercent(response.summary.percentage_change)}</dd>
        </div>
      </dl>
    </section>
  );
}

function ComparisonTable({
  response,
  source,
  onInvestigate,
  observed,
}: {
  response: CostComparisonResponse;
  source: "chargeback" | "topic_attribution";
  onInvestigate: (row: ComparisonRow) => void;
  observed: boolean;
}): React.JSX.Element {
  const columnLabel = (value: string): string =>
    observed ? `${value} observed` : value;
  const columns = [
    {
      title: "Group",
      key: "group",
      render: (_value: unknown, row: ComparisonRow) => (
        <span>
          {rowDimensions(row).map((value, index) => (
            <span key={`${row.key}-${value}`}>
              {index > 0 && " / "}
              {value}
            </span>
          ))}
        </span>
      ),
    },
    {
      title: columnLabel("Baseline"),
      dataIndex: "baseline_amount",
      key: "baseline_amount",
      render: (value: string) => formatSignedCurrency(value),
    },
    {
      title: columnLabel("Comparison"),
      dataIndex: "comparison_amount",
      key: "comparison_amount",
      render: (value: string) => formatSignedCurrency(value),
    },
    {
      title: columnLabel("Change"),
      dataIndex: "change",
      key: "change",
      render: (value: string) => formatSignedCurrency(value),
    },
    {
      title: columnLabel("Percentage change"),
      dataIndex: "percentage_change",
      key: "percentage_change",
      render: (value: string | null) => formatRowPercent(value),
    },
    {
      title: "Presence",
      key: "presence",
      render: (_value: unknown, row: ComparisonRow) =>
        presenceLabel(row, response),
    },
    {
      title: "Investigation",
      key: "investigation",
      render: (_value: unknown, row: ComparisonRow) =>
        row.kind === "entity" ? (
          <button type="button" onClick={() => onInvestigate(row)}>
            {source === "chargeback"
              ? "Compare entity in Cost Explorer"
              : "Open filtered Topic Attribution list"}
          </button>
        ) : null,
    },
  ];

  return (
    <Table<ComparisonRow>
      size="small"
      rowKey="key"
      dataSource={response.rows}
      columns={columns}
      pagination={false}
      scroll={{ x: "max-content" }}
      title={() => "Largest cost changes"}
    />
  );
}

function Reconciliation({
  response,
  observed,
}: {
  response: CostComparisonResponse;
  observed: boolean;
}): React.JSX.Element {
  const { reconciliation } = response;
  const groupCountLabel = (count: number): string =>
    `${count} ${count === 1 ? "group" : "groups"}`;
  return (
    <section aria-label="Reconciliation">
      <Card
        size="small"
        title={observed ? "Reconciliation (observed totals)" : "Reconciliation"}
      >
        {observed && (
          <Text type="secondary">
            Financial values below are observed totals because coverage is not
            complete.
          </Text>
        )}
        <p>
          {reconciliation.returned_group_count} of {reconciliation.full_group_count} groups
          returned.
        </p>
        {reconciliation.movement_excluded_group_count > 0 && (
          <p>
            {groupCountLabel(reconciliation.movement_excluded_group_count)} excluded by
            movement filter: {formatDecimalCurrency(reconciliation.movement_excluded_baseline_amount)}
            {observed ? " observed baseline, " : " baseline, "}
            {formatDecimalCurrency(reconciliation.movement_excluded_comparison_amount)}
            {observed ? " observed comparison, " : " comparison, "}
            {formatDecimalCurrency(reconciliation.movement_excluded_net_change)}
            {observed ? " observed net." : " net."}
          </p>
        )}
        {reconciliation.row_limit_omitted_group_count > 0 && (
          <p>
            {groupCountLabel(reconciliation.row_limit_omitted_group_count)} outside top
            N: {formatDecimalCurrency(
              reconciliation.row_limit_omitted_baseline_amount,
            )}
            {observed ? " observed baseline, " : " baseline, "}
            {formatDecimalCurrency(reconciliation.row_limit_omitted_comparison_amount)}
            {observed ? " observed comparison, " : " comparison, "}
            {formatDecimalCurrency(reconciliation.row_limit_omitted_net_change)}
            {observed ? " observed net." : " net."}
          </p>
        )}
      </Card>
    </section>
  );
}

export function CostComparisonView<Group extends ComparisonGroup>({
  response,
  source,
  sourceLabel,
  granularity,
  preset,
  periods,
  groupBy,
  isLoading,
  error,
  onInvestigate,
  onPresetChange,
  onPeriodsChange,
  onGroupByChange,
  movement,
  sortBy,
  sortDirection,
  limit,
  onMovementChange,
  onSortChange,
  onLimitChange,
}: CostComparisonViewProps<Group>): React.JSX.Element {
  const observedTotals =
    response !== null &&
    (response.baseline.coverage.status !== "complete" ||
      response.comparison.coverage.status !== "complete");

  return (
    <section aria-label="Cost comparison">
      <Title level={4} style={{ marginTop: 0 }}>
        {sourceLabel}
      </Title>
      {source === "chargeback" && (
        <Text type="secondary">
          Cost Explorer opens the selected entity and both periods in the
          broader tenant neighborhood. Product, cost-type, and tag filters are
          not carried into Explorer.
        </Text>
      )}
      <ComparisonControls
        source={source}
        granularity={granularity}
        preset={preset}
        periods={periods}
        groupBy={groupBy}
        movement={movement}
        sortBy={sortBy}
        sortDirection={sortDirection}
        limit={limit}
        onPresetChange={onPresetChange}
        onPeriodsChange={onPeriodsChange}
        onGroupByChange={(nextGroup) =>
          onGroupByChange(nextGroup as Group)
        }
        onMovementChange={onMovementChange}
        onSortChange={onSortChange}
        onLimitChange={onLimitChange}
      />
      {response && (
        <Space direction="vertical" size="middle" style={{ width: "100%" }}>
          <Card size="small" title="Selected periods">
            <p>
              Baseline: <span>{periodLabel(response.baseline)}</span>
            </p>
            <p>
              Comparison: <span>{periodLabel(response.comparison)}</span>
            </p>
            <p>
              Timezone: <span>{response.timezone}</span>
            </p>
            <p>
              UTC bounds: {response.baseline.start_at} – {response.baseline.end_at};{" "}
              {response.comparison.start_at} – {response.comparison.end_at}
            </p>
            {response.unequal_durations && (
              <p>Selected periods have unequal durations.</p>
            )}
          </Card>
          <Card size="small" title="Coverage qualification">
            <Space direction="vertical" size="small" style={{ width: "100%" }}>
              {coverageMessage("Baseline", response.baseline)}
              {coverageMessage("Comparison", response.comparison)}
            </Space>
          </Card>
          {observedTotals && (
            <Text type="warning">
              Financial values are observed totals because coverage is not
              complete.
            </Text>
          )}
          <Card size="small" title="Comparison summary">
            <ComparisonSummary response={response} observed={observedTotals} />
          </Card>
          <Reconciliation response={response} observed={observedTotals} />
          {response.rows.length === 0 ? (
            <Empty description="No groups matched the selected filters." />
          ) : (
            <ComparisonTable
              response={response}
              source={source}
              onInvestigate={onInvestigate}
              observed={observedTotals}
            />
          )}
        </Space>
      )}
      {isLoading && <p>Loading comparison…</p>}
      {!isLoading && !response && error && (
        <Alert type="error" showIcon message={error} />
      )}
      {!isLoading && !response && !error && <p>Choose comparison periods.</p>}
    </section>
  );
}
