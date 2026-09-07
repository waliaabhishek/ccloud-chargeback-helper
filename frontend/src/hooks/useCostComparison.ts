import { useQuery } from "@tanstack/react-query";
import { fetchCostComparison } from "../api/costComparison";
import type { CostComparisonRequest } from "../api/costComparison";
import type { ComparisonSource, CostComparisonResponse } from "../types/api";

export interface UseCostComparisonParams {
  tenantName: string | null;
  source: ComparisonSource;
  params: CostComparisonRequest;
  enabled?: boolean;
}

export interface UseCostComparisonResult {
  data: CostComparisonResponse | null;
  isLoading: boolean;
  error: string | null;
  refetch: () => void;
}

export function useCostComparison({
  tenantName,
  source,
  params,
  enabled: explicitlyEnabled = true,
}: UseCostComparisonParams): UseCostComparisonResult {
  const hasRequest =
    !!tenantName &&
    !!params.baseline_start &&
    !!params.baseline_end &&
    !!params.comparison_start &&
    !!params.comparison_end;
  const enabled = explicitlyEnabled && hasRequest;

  const query = useQuery({
    queryKey: [
      "cost-comparison",
      tenantName,
      source,
      params.group_by,
      params.baseline_start,
      params.baseline_end,
      params.comparison_start,
      params.comparison_end,
      params.timezone,
      params.movement,
      params.sort_by,
      params.sort_direction,
      params.limit,
      params.identity_id ?? null,
      params.product_type ?? null,
      params.resource_id ?? null,
      params.cost_type ?? null,
      params.cluster_resource_id ?? null,
      params.topic_name ?? null,
      params.attribution_method ?? null,
      params.tag_key ?? null,
      params.tag_value ?? null,
    ],
    queryFn: ({ signal }) =>
      fetchCostComparison(tenantName!, source, params, signal),
    enabled,
  });

  return {
    data: enabled ? (query.data ?? null) : null,
    isLoading: enabled && query.isLoading,
    error: enabled ? (query.error?.message ?? null) : null,
    refetch: query.refetch,
  };
}
