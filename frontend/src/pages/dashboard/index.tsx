import type React from "react";
import { useCallback, useMemo, useState } from "react";
import { Col, Radio, Row, Segmented, Typography } from "antd";
import type {
  ChargebackComparisonGroup,
  ComparisonRow,
  TenantStatusSummary,
} from "../../types/api";
import { useTenant } from "../../providers/TenantContext";
import { useChargebackFilters } from "../../hooks/useChargebackFilters";
import type { UseAggregationParams } from "../../hooks/useAggregation";
import { useAggregation } from "../../hooks/useAggregation";
import { useDataAvailability } from "../../hooks/useDataAvailability";
import { useInventorySummary } from "../../hooks/useInventorySummary";
import { AllocationIssuesTable } from "../../components/dashboard/AllocationIssuesTable";
import { SummaryStatCards } from "../../components/dashboard/SummaryStatCards";
import { InventoryCounters } from "../../components/dashboard/InventoryCounters";
import { FilterPanel } from "../../components/chargebacks/FilterPanel";
import { ChartCard } from "../../components/charts/ChartCard";
import { DataAvailabilityTimeline } from "../../components/charts/DataAvailabilityTimeline";
import { CostTrendChart } from "../../components/charts/CostTrendChart";
import { CostByIdentityChart } from "../../components/charts/CostByIdentityChart";
import { CostByProductChart } from "../../components/charts/CostByProductChart";
import { CostByResourceChart } from "../../components/charts/CostByResourceChart";
import { ProductChartTypeToggle } from "../../components/charts/ProductChartTypeToggle";
import { DimensionPieChart } from "../../components/charts/DimensionPieChart";
import type { ChargebackFilters } from "../../types/filters";
import { TagPivotPanel } from "../../components/pivotPanel/TagPivotPanel";
import { CostComparisonView } from "../../components/costComparison/CostComparisonView";
import { useCostComparison } from "../../hooks/useCostComparison";
import { useComparisonController } from "../../hooks/useComparisonController";
import { useNavigate } from "react-router";

const { Title, Text } = Typography;

type TimeBucket = "day" | "week" | "month";

interface DashboardContentProps {
  tenant: TenantStatusSummary;
  filters: ChargebackFilters;
  timeBucket: TimeBucket;
}

/** Inner component: hooks are unconditionally called here (tenant is always set). */
function DashboardContent({
  tenant,
  filters,
  timeBucket,
}: DashboardContentProps): React.JSX.Element {
  const [productChartType, setProductChartType] = useState<"pie" | "treemap">(
    "pie",
  );
  const [ownerTagKey, setOwnerTagKey] = useState("owner");
  const [ownerTagFilters, setOwnerTagFilters] = useState<string[]>([]);

  const sharedParams: Omit<UseAggregationParams, "groupBy"> = useMemo(
    () => ({
      tenantName: tenant.tenant_name,
      timeBucket,
      startDate: filters.start_date ?? "",
      endDate: filters.end_date ?? "",
      identityId: filters.identity_id,
      productType: filters.product_type,
      resourceId: filters.resource_id,
      costType: filters.cost_type,
      timezone: filters.timezone,
    }),
    [tenant.tenant_name, timeBucket, filters],
  );

  const trendData = useAggregation({
    ...sharedParams,
    groupBy: ["identity_id"],
  });
  const productData = useAggregation({
    ...sharedParams,
    groupBy: ["product_type"],
  });
  const resourceData = useAggregation({
    ...sharedParams,
    groupBy: ["resource_id"],
  });
  const environmentData = useAggregation({
    ...sharedParams,
    groupBy: ["environment_id"],
  });
  const productCategoryData = useAggregation({
    ...sharedParams,
    groupBy: ["product_category"],
  });
  const ownerData = useAggregation({
    ...sharedParams,
    groupBy: [`tag:${ownerTagKey}`, "product_type"],
    tagFilters:
      ownerTagFilters.length > 0
        ? { [ownerTagKey]: ownerTagFilters }
        : undefined,
  });
  const availabilityData = useDataAvailability({
    tenantName: tenant.tenant_name,
  });
  const inventoryData = useInventorySummary({ tenantName: tenant.tenant_name });

  return (
    <Row gutter={[16, 16]}>
      <Col span={24}>
        <SummaryStatCards
          data={trendData.data}
          isLoading={trendData.isLoading}
          error={trendData.error}
        />
      </Col>

      <Col span={24}>
        <InventoryCounters
          data={inventoryData.data}
          isLoading={inventoryData.isLoading}
          error={inventoryData.error}
        />
      </Col>

      <Col span={24}>
        <ChartCard
          title="Data Availability"
          loading={availabilityData.isLoading}
          error={availabilityData.error}
          onRetry={availabilityData.refetch}
        >
          <DataAvailabilityTimeline
            dates={availabilityData.data?.dates ?? []}
            startDate={filters.start_date ?? ""}
            endDate={filters.end_date ?? ""}
          />
        </ChartCard>
      </Col>

      <Col span={24}>
        <ChartCard
          title="Cost Trend Over Time"
          loading={trendData.isLoading}
          error={trendData.error}
          onRetry={trendData.refetch}
        >
          <CostTrendChart
            data={trendData.data?.buckets ?? []}
            timeBucket={timeBucket}
          />
        </ChartCard>
      </Col>

      <Col span={24}>
        <ChartCard
          title="Cost by Identity"
          loading={trendData.isLoading}
          error={trendData.error}
          onRetry={trendData.refetch}
        >
          <CostByIdentityChart data={trendData.data?.buckets ?? []} />
        </ChartCard>
      </Col>

      <Col xs={24} sm={12} lg={6}>
        <ChartCard
          title="Cost by Environment"
          loading={environmentData.isLoading}
          error={environmentData.error}
          onRetry={environmentData.refetch}
        >
          <DimensionPieChart
            data={environmentData.data?.buckets ?? []}
            dimension="environment_id"
          />
        </ChartCard>
      </Col>

      <Col xs={24} sm={12} lg={6}>
        <ChartCard
          title="Cost by Resource"
          loading={resourceData.isLoading}
          error={resourceData.error}
          onRetry={resourceData.refetch}
        >
          <CostByResourceChart data={resourceData.data?.buckets ?? []} />
        </ChartCard>
      </Col>

      <Col xs={24} sm={12} lg={6}>
        <ChartCard
          title="Cost by Product Type"
          loading={productData.isLoading}
          error={productData.error}
          onRetry={productData.refetch}
          extra={
            <ProductChartTypeToggle
              value={productChartType}
              onChange={setProductChartType}
            />
          }
        >
          <CostByProductChart
            data={productData.data?.buckets ?? []}
            chartType={productChartType}
          />
        </ChartCard>
      </Col>

      <Col xs={24} sm={12} lg={6}>
        <ChartCard
          title="Cost by Product Category"
          loading={productCategoryData.isLoading}
          error={productCategoryData.error}
          onRetry={productCategoryData.refetch}
        >
          <DimensionPieChart
            data={productCategoryData.data?.buckets ?? []}
            dimension="product_category"
          />
        </ChartCard>
      </Col>

      <Col span={24}>
        <TagPivotPanel
          title="Cost by Owner"
          tenantName={tenant.tenant_name}
          buckets={ownerData.data?.buckets ?? []}
          isLoading={ownerData.isLoading}
          error={ownerData.error}
          onRefetch={ownerData.refetch}
          selectedTagKey={ownerTagKey}
          onTagKeyChange={(key) => {
            setOwnerTagKey(key);
            setOwnerTagFilters([]);
          }}
          activeTagFilters={ownerTagFilters}
          onFilterAdd={(v) => setOwnerTagFilters((prev) => [...prev, v])}
          onFilterRemove={(v) =>
            setOwnerTagFilters((prev) => prev.filter((f) => f !== v))
          }
        />
      </Col>

      <Col span={24}>
        <ChartCard title="Allocation Issues">
          <AllocationIssuesTable
            tenantName={tenant.tenant_name}
            filters={filters}
          />
        </ChartCard>
      </Col>
    </Row>
  );
}

/** Top-level page: handles tenant check and filter/time-bucket state. */
export function CostDashboardPage(): React.JSX.Element {
  const { currentTenant } = useTenant();
  const { filters, setFilter, setFilters, resetFilters } =
    useChargebackFilters();
  const [timeBucket, setTimeBucket] = useState<TimeBucket>("day");
  const [refreshKey, setRefreshKey] = useState(0);
  const [view, setView] = useState<"overview" | "compare">("overview");
  const navigate = useNavigate();

  const tenantName = currentTenant?.tenant_name ?? null;
  const comparisonController = useComparisonController<ChargebackComparisonGroup>({
    tenant: currentTenant,
    defaultGroup: "principal",
    pageTimezone: filters.timezone,
  });
  const comparison = comparisonController.comparison;

  const comparisonParams = useMemo(
    () =>
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
            identity_id: filters.identity_id,
            product_type: filters.product_type,
            resource_id: filters.resource_id,
            cost_type: filters.cost_type,
            tag_key: filters.tag_key,
            tag_value: filters.tag_value,
          }
        : {
            baseline_start: null,
            baseline_end: null,
            comparison_start: null,
            comparison_end: null,
            timezone: null,
            group_by: "principal" as const,
            movement: "all" as const,
            sort_by: "absolute_change" as const,
            sort_direction: "desc" as const,
            limit: 100,
          },
    [comparison, comparisonController.ready, filters],
  );

  const comparisonQuery = useCostComparison({
    tenantName,
    source: "chargeback",
    params: comparisonParams,
    enabled:
      view === "compare" &&
      comparisonController.ready &&
      comparisonController.periodsValid,
  });

  const investigate = useCallback(
    (row: ComparisonRow) => {
      if (!comparisonQuery.data || row.kind !== "entity") return;
      const query = new URLSearchParams({
        focus: row.key,
        diff: "true",
        from_start: comparisonQuery.data.baseline.start_date,
        from_end: comparisonQuery.data.baseline.end_date,
        to_start: comparisonQuery.data.comparison.start_date,
        to_end: comparisonQuery.data.comparison.end_date,
        timezone: comparisonQuery.data.timezone,
      });
      navigate(`/explorer?${query.toString()}`);
    },
    [comparisonQuery.data, navigate],
  );

  return (
    <div>
      <Title level={3}>Cost Dashboard</Title>

      {!currentTenant ? (
        <Text type="secondary">Select a tenant to view cost analytics.</Text>
      ) : (
        <>
          <Segmented
            options={[
              { label: "Overview", value: "overview" },
              { label: "Compare", value: "compare" },
            ]}
            value={view}
            onChange={(value) => setView(value as "overview" | "compare")}
          />
          <FilterPanel
            filters={filters}
            onChange={setFilter}
            onBatchChange={setFilters}
            onReset={resetFilters}
            onRefresh={() => setRefreshKey((k) => k + 1)}
            tenantName={currentTenant.tenant_name}
            showDateRange={view === "overview"}
          />
          {view === "overview" ? (
            <>
              <div style={{ margin: "12px 0" }}>
                <Radio.Group
                  value={timeBucket}
                  onChange={(e) => setTimeBucket(e.target.value as TimeBucket)}
                >
                  <Radio.Button value="day">Daily</Radio.Button>
                  <Radio.Button value="week">Weekly</Radio.Button>
                  <Radio.Button value="month">Monthly</Radio.Button>
                </Radio.Group>
              </div>
              <DashboardContent
                key={refreshKey}
                tenant={currentTenant}
                filters={filters}
                timeBucket={timeBucket}
              />
            </>
          ) : (
            <CostComparisonView
              source="chargeback"
              sourceLabel="Chargeback — allocated tenant costs"
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
              groupBy={comparison?.groupBy ?? "principal"}
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
        </>
      )}
    </div>
  );
}
