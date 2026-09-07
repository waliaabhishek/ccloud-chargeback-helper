import type React from "react";
import { Alert, Typography, Segmented } from "antd";
import { useCallback, useRef, useState } from "react";
import type { AgGridReact } from "ag-grid-react";
import { useSearchParams } from "react-router";
import { TopicAttributionGrid } from "../../components/topicAttributions/TopicAttributionGrid";
import { TopicAttributionFilterPanel } from "../../components/topicAttributions/TopicAttributionFilterPanel";
import { TopicAttributionExportButton } from "../../components/topicAttributions/TopicAttributionExportButton";
import { TopicAttributionAnalytics } from "../../components/topicAttributions/TopicAttributionAnalytics";
import { useTopicAttributionFilters } from "../../hooks/useTopicAttributionFilters";
import { useTenant } from "../../providers/TenantContext";
import { CostComparisonView } from "../../components/costComparison/CostComparisonView";
import { useCostComparison } from "../../hooks/useCostComparison";
import { useComparisonController } from "../../hooks/useComparisonController";
import type {
  ComparisonRow,
  TopicAttributionComparisonGroup,
} from "../../types/api";

const { Text, Title } = Typography;

const TAB_OPTIONS: Array<{
  label: string;
  value: "table" | "analytics" | "compare";
}> = [
  { label: "Table", value: "table" },
  { label: "Analytics", value: "analytics" },
  { label: "Compare", value: "compare" },
];

export function TopicAttributionPage(): React.JSX.Element {
  const { currentTenant, isReadOnly } = useTenant();
  const { filters, setFilter, setFilters, resetFilters, queryParams } =
    useTopicAttributionFilters();
  const [, setSearchParams] = useSearchParams();
  const gridRef = useRef<AgGridReact>(null);
  const [activeTab, setActiveTab] = useState<
    "table" | "analytics" | "compare"
  >("table");

  const tenantName = currentTenant?.tenant_name ?? null;
  const comparisonController = useComparisonController<TopicAttributionComparisonGroup>({
    tenant: currentTenant,
    defaultGroup: "topic",
    pageTimezone: filters.timezone,
  });
  const comparison = comparisonController.comparison;

  const comparisonParams =
    comparison && comparisonController.ready
      ? {
          baseline_start: comparison.periods.baseline.start_date || null,
          baseline_end: comparison.periods.baseline.end_date || null,
          comparison_start: comparison.periods.comparison.start_date || null,
          comparison_end: comparison.periods.comparison.end_date || null,
          timezone: comparison.periods.timezone,
          group_by: comparison.groupBy,
          movement: comparison.movement,
          sort_by: comparison.sortBy,
          sort_direction: comparison.sortDirection,
          limit: comparison.limit,
          cluster_resource_id: filters.cluster_resource_id,
          topic_name: filters.topic_name,
          product_type: filters.product_type,
          attribution_method: filters.attribution_method,
          tag_key: filters.tag_key,
          tag_value: filters.tag_value,
        }
      : {
          baseline_start: null,
          baseline_end: null,
          comparison_start: null,
          comparison_end: null,
          timezone: null,
          group_by: "topic" as const,
          movement: "all" as const,
          sort_by: "absolute_change" as const,
          sort_direction: "desc" as const,
          limit: 100,
        };

  const comparisonQuery = useCostComparison({
    tenantName,
    source: "topic_attribution",
    params: comparisonParams,
    enabled:
      activeTab === "compare" &&
      currentTenant?.topic_attribution_status === "enabled" &&
      comparisonController.ready &&
      comparisonController.periodsValid,
  });

  const investigate = useCallback(
    (row: ComparisonRow) => {
      if (row.kind !== "entity") return;
      const response = comparisonQuery.data;
      const baseline = response?.baseline ?? comparison?.periods.baseline;
      const selected = response?.comparison ?? comparison?.periods.comparison;
      const timezone = response?.timezone ?? comparison?.periods.timezone;
      if (!baseline || !selected || !timezone) return;
      const cluster = row.dimensions.cluster_resource_id;
      if (typeof cluster !== "string" || cluster === "") return;

      const groupBy = response?.group_by ?? comparison?.groupBy;
      let topic: string | null;
      if (groupBy === "topic") {
        const topicDimension = row.dimensions.topic_name;
        if (typeof topicDimension !== "string" || topicDimension === "") {
          return;
        }
        topic = topicDimension;
      } else if (groupBy === "cluster") {
        topic = filters.topic_name ?? null;
      } else {
        return;
      }

      setSearchParams((previous) => {
        const next = new URLSearchParams(previous);
        const setOrDelete = (key: string, value: string | null): void => {
          if (value === null || value === "") next.delete(key);
          else next.set(key, value);
        };
        setOrDelete("cluster_resource_id", cluster);
        setOrDelete("topic_name", topic);
        setOrDelete("product_type", filters.product_type);
        setOrDelete("attribution_method", filters.attribution_method);
        setOrDelete("tag_key", filters.tag_key);
        setOrDelete("tag_value", filters.tag_value);
        setOrDelete("timezone", timezone);
        setOrDelete("start_date", selected.start_date);
        setOrDelete("end_date", selected.end_date);
        return next;
      });
      setActiveTab("table");
    },
    [comparison, comparisonQuery.data, filters, setSearchParams],
  );

  if (!currentTenant) {
    return (
      <div>
        <Title level={3}>Topic Attribution</Title>
        <Text type="secondary">Select a tenant to begin.</Text>
      </div>
    );
  }

  if (currentTenant.topic_attribution_status === "config_error") {
    return (
      <div>
        <Title level={3}>Topic Attribution</Title>
        <Alert
          type="error"
          showIcon
          message="Topic Attribution configuration error"
          description={
            currentTenant.topic_attribution_error ??
            "Configuration validation failed."
          }
        />
      </div>
    );
  }

  if (currentTenant.topic_attribution_status === "disabled") {
    return (
      <div>
        <Title level={3}>Topic Attribution</Title>
        <Alert
          type="info"
          showIcon
          message="Topic Attribution is not configured"
          description={
            <span>
              Topic Attribution overlays Kafka topic-level cost attribution on
              top of chargeback data, enabling per-topic cost breakdowns across
              your Confluent Cloud environment. To enable it, add{" "}
              <code>topic_attribution.enabled: true</code> under your
              tenant&apos;s
              <code>plugin_settings</code> in the YAML config and restart the
              service.
            </span>
          }
        />
      </div>
    );
  }

  return (
    <div style={{ display: "flex", flexDirection: "column", height: "100%" }}>
      <div
        style={{
          display: "flex",
          alignItems: "center",
          justifyContent: "space-between",
          margin: "0 0 8px 0",
        }}
      >
        <Title level={3} style={{ margin: 0 }}>
          Topic Attribution
        </Title>
        <TopicAttributionExportButton
          filters={queryParams}
          tenantName={currentTenant.tenant_name}
          disabled={isReadOnly}
        />
      </div>
      <TopicAttributionFilterPanel
        tenantName={currentTenant.tenant_name}
        filters={filters}
        onChange={setFilter}
        onBatchChange={setFilters}
        onReset={resetFilters}
        activeTab={activeTab}
        showDateRange={activeTab !== "compare"}
        onRefresh={
          activeTab === "table"
            ? () => gridRef.current?.api?.refreshInfiniteCache()
            : undefined
        }
      />
      <Segmented
        options={TAB_OPTIONS}
        value={activeTab}
        onChange={(value) =>
          setActiveTab(value as "table" | "analytics" | "compare")
        }
        style={{ marginBottom: 8, alignSelf: "flex-start" }}
      />
      {activeTab === "table" && (
        <TopicAttributionGrid
          key={currentTenant.tenant_name}
          ref={gridRef}
          tenantName={currentTenant.tenant_name}
          filters={queryParams}
          showExcluded={currentTenant.ecosystem === "self_managed_kafka"}
        />
      )}
      {activeTab === "analytics" && (
        <TopicAttributionAnalytics
          tenantName={currentTenant.tenant_name}
          filters={filters}
        />
      )}
      {activeTab === "compare" && (
        <CostComparisonView
          source="topic_attribution"
          sourceLabel="Topic Attribution — attributed Kafka costs, not the full tenant bill"
          response={comparisonQuery.data}
          isLoading={comparisonQuery.isLoading}
          error={comparisonQuery.error}
          granularity={comparison?.granularity ?? "daily"}
          preset={comparison?.preset ?? "previous_day"}
          periods={
            comparison?.periods ?? {
              baseline: { start_date: "", end_date: "" },
              comparison: { start_date: "", end_date: "" },
              timezone: "UTC",
            }
          }
          groupBy={comparison?.groupBy ?? "topic"}
          movement={comparison?.movement ?? "all"}
          sortBy={comparison?.sortBy ?? "absolute_change"}
          sortDirection={comparison?.sortDirection ?? "desc"}
          limit={comparison?.limit ?? 100}
          onPresetChange={comparisonController.onPresetChange}
          onPeriodsChange={comparisonController.onPeriodsChange}
          onGroupByChange={comparisonController.onGroupByChange}
          onMovementChange={comparisonController.onMovementChange}
          onSortChange={comparisonController.onSortChange}
          onLimitChange={comparisonController.onLimitChange}
          onInvestigate={investigate}
        />
      )}
    </div>
  );
}
