import { API_URL } from "../config";
import type {
  ComparisonGroup,
  ComparisonMovement,
  ComparisonSortBy,
  ComparisonSortDirection,
  ComparisonSource,
  CostComparisonResponse,
} from "../types/api";

export interface CostComparisonRequest {
  baseline_start: string | null;
  baseline_end: string | null;
  comparison_start: string | null;
  comparison_end: string | null;
  timezone: string | null;
  group_by: ComparisonGroup;
  movement: ComparisonMovement;
  sort_by: ComparisonSortBy;
  sort_direction: ComparisonSortDirection;
  limit: number;
  identity_id?: string | null;
  product_type?: string | null;
  resource_id?: string | null;
  cost_type?: string | null;
  cluster_resource_id?: string | null;
  topic_name?: string | null;
  attribution_method?: string | null;
  tag_key?: string | null;
  tag_value?: string | null;
}

function setIfPresent(
  query: URLSearchParams,
  key: string,
  value: string | number | null | undefined,
): void {
  if (value !== null && value !== undefined && value !== "") {
    query.set(key, String(value));
  }
}

/** Fetch a server-ranked, source-specific two-period comparison. */
export async function fetchCostComparison(
  tenantName: string,
  source: ComparisonSource,
  params: CostComparisonRequest,
  signal?: AbortSignal,
): Promise<CostComparisonResponse> {
  const query = new URLSearchParams();
  setIfPresent(query, "baseline_start", params.baseline_start);
  setIfPresent(query, "baseline_end", params.baseline_end);
  setIfPresent(query, "comparison_start", params.comparison_start);
  setIfPresent(query, "comparison_end", params.comparison_end);
  setIfPresent(query, "timezone", params.timezone);
  setIfPresent(query, "group_by", params.group_by);
  setIfPresent(query, "movement", params.movement);
  setIfPresent(query, "sort_by", params.sort_by);
  setIfPresent(query, "sort_direction", params.sort_direction);
  setIfPresent(query, "limit", params.limit);

  if (source === "chargeback") {
    setIfPresent(query, "identity_id", params.identity_id);
    setIfPresent(query, "product_type", params.product_type);
    setIfPresent(query, "resource_id", params.resource_id);
    setIfPresent(query, "cost_type", params.cost_type);
  } else {
    setIfPresent(query, "cluster_resource_id", params.cluster_resource_id);
    setIfPresent(query, "topic_name", params.topic_name);
    setIfPresent(query, "product_type", params.product_type);
    setIfPresent(query, "attribution_method", params.attribution_method);
  }

  setIfPresent(query, "tag_key", params.tag_key);
  setIfPresent(query, "tag_value", params.tag_value);

  const response = await fetch(
    `${API_URL}/tenants/${tenantName}/${source === "chargeback" ? "chargebacks" : "topic-attributions"}/comparison?${query.toString()}`,
    { signal },
  );
  if (!response.ok) {
    throw new Error(`HTTP ${response.status}: ${response.statusText}`);
  }
  return response.json() as Promise<CostComparisonResponse>;
}
