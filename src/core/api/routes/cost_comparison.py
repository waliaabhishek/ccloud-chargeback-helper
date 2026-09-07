from __future__ import annotations

from calendar import monthrange
from collections.abc import Collection
from datetime import UTC, date, datetime, timedelta
from typing import TYPE_CHECKING, Annotated, Literal

from fastapi import APIRouter, HTTPException, Path, Query, Request
from pydantic import BaseModel, Field

from core.api.dependencies import get_backend_provider, get_settings, get_tenant_config, resolve_date_range
from core.api.schemas import (
    ComparisonCoverage,
    ComparisonPeriod,
    ComparisonReconciliation,
    ComparisonRow,
    ComparisonSummary,
    CostComparisonResponse,
)
from core.api.topic_attribution_status import resolve_topic_attribution_retention_days
from core.cost_comparison import (
    classify_comparison_coverage,
    compare_chargeback_periods,
    compare_topic_attribution_periods,
)
from core.models.cost_comparison import (
    ChargebackComparisonGroup,
    CostComparisonResult,
    PeriodBounds,
    ResolvedComparisonRequest,
    TopicAttributionComparisonGroup,
)
from core.models.cost_comparison import (
    ComparisonCoverage as DomainComparisonCoverage,
)
from core.storage.interface import ConsistentReadStorageBackend
from core.utils.tag_validation import is_valid_tag_key

if TYPE_CHECKING:
    from core.config.models import TenantConfig

router = APIRouter(tags=["cost-comparison"])

_Granularity = Literal["hourly", "daily", "monthly"]
_Movement = Literal["all", "increase", "decrease"]
_Sort = Literal[
    "absolute_change",
    "entity",
    "baseline_amount",
    "comparison_amount",
    "change",
    "percentage_change",
]
_SortDirection = Literal["asc", "desc"]


class _ComparisonQuery(BaseModel):
    baseline_start: date
    baseline_end: date
    comparison_start: date
    comparison_end: date
    timezone: str = "UTC"
    movement: _Movement = "all"
    sort_by: _Sort = "absolute_change"
    sort_direction: _SortDirection = "desc"
    limit: int = Field(default=100, ge=1, le=500)
    tag_key: str | None = None
    tag_value: str | None = None


class ChargebackComparisonQuery(_ComparisonQuery):
    group_by: ChargebackComparisonGroup = "principal"
    identity_id: str | None = None
    product_type: str | None = None
    resource_id: str | None = None
    cost_type: Literal["usage", "shared"] | None = None


class TopicAttributionComparisonQuery(_ComparisonQuery):
    group_by: TopicAttributionComparisonGroup = "topic"
    cluster_resource_id: str | None = None
    topic_name: str | None = None
    product_type: str | None = None
    attribution_method: str | None = None


def _next_month(value: date) -> date:
    if value.month == 12:
        return date(value.year + 1, 1, 1)
    return date(value.year, value.month + 1, 1)


def _is_complete_utc_month_range(start: date, end: date) -> bool:
    return start.day == 1 and end == date(end.year, end.month, monthrange(end.year, end.month)[1])


def _validate_query_periods(query: _ComparisonQuery, granularity: _Granularity) -> tuple[PeriodBounds, PeriodBounds]:
    if query.baseline_start > query.baseline_end:
        raise HTTPException(status_code=400, detail="baseline_start must be <= baseline_end")
    if query.comparison_start > query.comparison_end:
        raise HTTPException(status_code=400, detail="comparison_start must be <= comparison_end")
    if query.tag_value is not None and query.tag_key is None:
        raise HTTPException(status_code=400, detail="tag_value requires tag_key")
    if query.tag_key is not None and not is_valid_tag_key(query.tag_key):
        raise HTTPException(status_code=400, detail=f"Invalid tag key format: {query.tag_key!r}")

    baseline_start_at, baseline_end_at = resolve_date_range(
        query.baseline_start,
        query.baseline_end,
        timezone=query.timezone,
    )
    comparison_start_at, comparison_end_at = resolve_date_range(
        query.comparison_start,
        query.comparison_end,
        timezone=query.timezone,
    )
    if granularity == "monthly":
        if query.timezone != "UTC":
            raise HTTPException(status_code=400, detail="timezone must be UTC for monthly comparison data")
        if not _is_complete_utc_month_range(query.baseline_start, query.baseline_end):
            raise HTTPException(
                status_code=400,
                detail="baseline period must contain complete UTC calendar months for monthly comparison data",
            )
        if not _is_complete_utc_month_range(query.comparison_start, query.comparison_end):
            raise HTTPException(
                status_code=400,
                detail="comparison period must contain complete UTC calendar months for monthly comparison data",
            )
    return (
        PeriodBounds(query.baseline_start, query.baseline_end, baseline_start_at, baseline_end_at),
        PeriodBounds(query.comparison_start, query.comparison_end, comparison_start_at, comparison_end_at),
    )


def resolve_comparison_request(
    query: ChargebackComparisonQuery | TopicAttributionComparisonQuery,
    tenant_config: TenantConfig,
    source: Literal["chargeback", "topic_attribution"],
) -> ResolvedComparisonRequest:
    granularity: _Granularity = tenant_config.plugin_settings.chargeback_granularity
    baseline, comparison = _validate_query_periods(query, granularity)
    return ResolvedComparisonRequest(
        source=source,
        group_by=query.group_by,
        baseline=baseline,
        comparison=comparison,
        timezone="UTC" if granularity == "monthly" else query.timezone,
        granularity=granularity,
        movement=query.movement,
        sort_by=query.sort_by,
        sort_direction=query.sort_direction,
        limit=query.limit,
    )


def _floor_to_boundary(value: datetime, granularity: _Granularity) -> datetime:
    value = value.astimezone(UTC)
    if granularity == "hourly":
        return value.replace(minute=0, second=0, microsecond=0)
    if granularity == "daily":
        return value.replace(hour=0, minute=0, second=0, microsecond=0)
    return value.replace(day=1, hour=0, minute=0, second=0, microsecond=0)


def _expected_source_timestamps(period: PeriodBounds, granularity: _Granularity) -> list[datetime]:
    cursor = _floor_to_boundary(period.start_at, granularity)
    if cursor < period.start_at:
        if granularity == "hourly":
            cursor += timedelta(hours=1)
        elif granularity == "daily":
            cursor += timedelta(days=1)
        else:
            cursor = datetime.combine(_next_month(cursor.date()), datetime.min.time(), tzinfo=UTC)
    result: list[datetime] = []
    while cursor < period.end_at:
        result.append(cursor)
        if granularity == "hourly":
            cursor += timedelta(hours=1)
        elif granularity == "daily":
            cursor += timedelta(days=1)
        else:
            cursor = datetime.combine(_next_month(cursor.date()), datetime.min.time(), tzinfo=UTC)
    return result


def _coverage_response(coverage: DomainComparisonCoverage) -> ComparisonCoverage:
    # The domain model deliberately has no dependency on the API schema.
    return ComparisonCoverage(
        status=coverage.status,
        expected_dates=coverage.expected_dates,
        unknown_dates=coverage.unknown_dates,
        incomplete_dates=coverage.incomplete_dates,
        retention_qualified_dates=coverage.retention_qualified_dates,
        availability_cutoff_at=coverage.availability_cutoff_at,
    )


def _comparison_response(
    request: ResolvedComparisonRequest,
    evaluated_at: datetime,
    baseline_coverage: DomainComparisonCoverage,
    comparison_coverage: DomainComparisonCoverage,
    result: CostComparisonResult,
) -> CostComparisonResponse:
    summary = result.summary
    reconciliation = result.reconciliation
    return CostComparisonResponse(
        source=request.source,
        granularity=request.granularity,
        group_by=request.group_by,
        timezone=request.timezone,
        coverage_evaluated_at=evaluated_at,
        baseline=ComparisonPeriod(
            start_date=request.baseline.start_date,
            end_date=request.baseline.end_date,
            start_at=request.baseline.start_at,
            end_at=request.baseline.end_at,
            duration_seconds=request.baseline.duration_seconds,
            coverage=_coverage_response(baseline_coverage),
        ),
        comparison=ComparisonPeriod(
            start_date=request.comparison.start_date,
            end_date=request.comparison.end_date,
            start_at=request.comparison.start_at,
            end_at=request.comparison.end_at,
            duration_seconds=request.comparison.duration_seconds,
            coverage=_coverage_response(comparison_coverage),
        ),
        unequal_durations=request.baseline.duration_seconds != request.comparison.duration_seconds,
        summary=ComparisonSummary(
            baseline_amount=summary.baseline_amount,
            comparison_amount=summary.comparison_amount,
            increases=summary.increases,
            decreases=summary.decreases,
            net_change=summary.net_change,
            percentage_change=summary.percentage_change,
        ),
        reconciliation=ComparisonReconciliation(
            full_group_count=reconciliation.full_group_count,
            selected_group_count=reconciliation.selected_group_count,
            returned_group_count=reconciliation.returned_group_count,
            movement_excluded_group_count=reconciliation.movement_excluded_group_count,
            row_limit_omitted_group_count=reconciliation.row_limit_omitted_group_count,
            returned_baseline_amount=reconciliation.returned_baseline_amount,
            returned_comparison_amount=reconciliation.returned_comparison_amount,
            returned_net_change=reconciliation.returned_net_change,
            movement_excluded_baseline_amount=reconciliation.movement_excluded_baseline_amount,
            movement_excluded_comparison_amount=reconciliation.movement_excluded_comparison_amount,
            movement_excluded_net_change=reconciliation.movement_excluded_net_change,
            row_limit_omitted_baseline_amount=reconciliation.row_limit_omitted_baseline_amount,
            row_limit_omitted_comparison_amount=reconciliation.row_limit_omitted_comparison_amount,
            row_limit_omitted_net_change=reconciliation.row_limit_omitted_net_change,
        ),
        rows=[
            ComparisonRow(
                key=row.key,
                kind=row.kind,
                dimensions=row.dimensions,
                baseline_amount=row.baseline_amount,
                comparison_amount=row.comparison_amount,
                change=row.change,
                percentage_change=row.percentage_change,
                baseline_row_count=row.baseline_row_count,
                comparison_row_count=row.comparison_row_count,
                observed_presence=row.observed_presence,
            )
            for row in result.rows
        ],
    )


def _chargeback_cutoff(evaluated_at: datetime, retention_days: int) -> datetime:
    cutoff_date = (evaluated_at - timedelta(days=retention_days)).date()
    return datetime(cutoff_date.year, cutoff_date.month, cutoff_date.day, tzinfo=UTC)


def _topic_cutoff(evaluated_at: datetime, retention_days: int | None) -> datetime | None:
    if retention_days is None:
        return None
    return evaluated_at - timedelta(days=retention_days)


def _tracking_date_bounds(
    baseline_expected_timestamps: Collection[datetime],
    comparison_expected_timestamps: Collection[datetime],
) -> tuple[date, date]:
    start: date | None = None
    end: date | None = None
    for expected_timestamps in (baseline_expected_timestamps, comparison_expected_timestamps):
        for timestamp in expected_timestamps:
            tracking_date = timestamp.date()
            if start is None or tracking_date < start:
                start = tracking_date
            if end is None or tracking_date > end:
                end = tracking_date
    if start is None or end is None:
        raise ValueError("comparison periods must contain at least one expected source timestamp")
    return start, end + timedelta(days=1)


def _union_time_bounds(request: ResolvedComparisonRequest) -> tuple[datetime, datetime]:
    return (
        min(request.baseline.start_at, request.comparison.start_at),
        max(request.baseline.end_at, request.comparison.end_at),
    )


@router.get(
    "/tenants/{tenant_name}/chargebacks/comparison",
    response_model=CostComparisonResponse,
)
async def compare_chargebacks(
    request: Request,
    tenant_name: Annotated[str, Path(description="Tenant name from config")],
    query: Annotated[ChargebackComparisonQuery, Query()],
) -> CostComparisonResponse:
    settings = get_settings(request)
    tenant_config = get_tenant_config(tenant_name, settings)
    resolved = resolve_comparison_request(query, tenant_config, "chargeback")
    baseline_expected_timestamps = _expected_source_timestamps(resolved.baseline, resolved.granularity)
    comparison_expected_timestamps = _expected_source_timestamps(resolved.comparison, resolved.granularity)
    provider = get_backend_provider(request)
    evaluated_at = datetime.now(UTC)
    with provider.acquire_backend(tenant_name, tenant_config) as backend:
        if not isinstance(backend, ConsistentReadStorageBackend):
            raise HTTPException(status_code=503, detail="Storage backend does not support consistent comparison reads")
        with backend.create_consistent_read_unit_of_work() as uow:
            state_start, state_end = _tracking_date_bounds(
                baseline_expected_timestamps,
                comparison_expected_timestamps,
            )
            states = uow.pipeline_state.find_by_range(
                tenant_config.ecosystem,
                tenant_config.tenant_id,
                state_start,
                state_end,
            )
            baseline_rows = uow.chargebacks.iter_by_filters(
                tenant_config.ecosystem,
                tenant_config.tenant_id,
                start=resolved.baseline.start_at,
                end=resolved.baseline.end_at,
                identity_id=query.identity_id,
                product_type=query.product_type,
                resource_id=query.resource_id,
                cost_type=query.cost_type,
                batch_size=5000,
                tag_key=query.tag_key,
                tag_value=query.tag_value,
            )
            comparison_rows = uow.chargebacks.iter_by_filters(
                tenant_config.ecosystem,
                tenant_config.tenant_id,
                start=resolved.comparison.start_at,
                end=resolved.comparison.end_at,
                identity_id=query.identity_id,
                product_type=query.product_type,
                resource_id=query.resource_id,
                cost_type=query.cost_type,
                batch_size=5000,
                tag_key=query.tag_key,
                tag_value=query.tag_value,
            )
            baseline_coverage = classify_comparison_coverage(
                baseline_expected_timestamps,
                states,
                "chargeback",
                resolved.granularity,
                _chargeback_cutoff(evaluated_at, tenant_config.retention_days),
                None,
            )
            comparison_coverage = classify_comparison_coverage(
                comparison_expected_timestamps,
                states,
                "chargeback",
                resolved.granularity,
                _chargeback_cutoff(evaluated_at, tenant_config.retention_days),
                None,
            )
            result = compare_chargeback_periods(baseline_rows, comparison_rows, resolved)
    return _comparison_response(resolved, evaluated_at, baseline_coverage, comparison_coverage, result)


@router.get(
    "/tenants/{tenant_name}/topic-attributions/comparison",
    response_model=CostComparisonResponse,
)
async def compare_topic_attributions(
    request: Request,
    tenant_name: Annotated[str, Path(description="Tenant name from config")],
    query: Annotated[TopicAttributionComparisonQuery, Query()],
) -> CostComparisonResponse:
    settings = get_settings(request)
    tenant_config = get_tenant_config(tenant_name, settings)
    resolved = resolve_comparison_request(query, tenant_config, "topic_attribution")
    baseline_expected_timestamps = _expected_source_timestamps(resolved.baseline, resolved.granularity)
    comparison_expected_timestamps = _expected_source_timestamps(resolved.comparison, resolved.granularity)
    retention_days = resolve_topic_attribution_retention_days(
        tenant_config.plugin_settings,
        tenant_config.ecosystem,
    )
    provider = get_backend_provider(request)
    evaluated_at = datetime.now(UTC)
    with provider.acquire_backend(tenant_name, tenant_config) as backend:
        if not isinstance(backend, ConsistentReadStorageBackend):
            raise HTTPException(status_code=503, detail="Storage backend does not support consistent comparison reads")
        with backend.create_consistent_read_unit_of_work() as uow:
            state_start, state_end = _tracking_date_bounds(
                baseline_expected_timestamps,
                comparison_expected_timestamps,
            )
            states = uow.pipeline_state.find_by_range(
                tenant_config.ecosystem,
                tenant_config.tenant_id,
                state_start,
                state_end,
            )
            availability_start, availability_end = _union_time_bounds(resolved)
            available_timestamps = uow.topic_attributions.get_distinct_timestamps_in_range(
                tenant_config.ecosystem,
                tenant_config.tenant_id,
                availability_start,
                availability_end,
            )
            baseline_rows = uow.topic_attributions.iter_by_filters(
                tenant_config.ecosystem,
                tenant_config.tenant_id,
                start=resolved.baseline.start_at,
                end=resolved.baseline.end_at,
                cluster_resource_id=query.cluster_resource_id,
                topic_name=query.topic_name,
                product_type=query.product_type,
                attribution_method=query.attribution_method,
                batch_size=5000,
                tag_key=query.tag_key,
                tag_value=query.tag_value,
            )
            comparison_rows = uow.topic_attributions.iter_by_filters(
                tenant_config.ecosystem,
                tenant_config.tenant_id,
                start=resolved.comparison.start_at,
                end=resolved.comparison.end_at,
                cluster_resource_id=query.cluster_resource_id,
                topic_name=query.topic_name,
                product_type=query.product_type,
                attribution_method=query.attribution_method,
                batch_size=5000,
                tag_key=query.tag_key,
                tag_value=query.tag_value,
            )
            baseline_coverage = classify_comparison_coverage(
                baseline_expected_timestamps,
                states,
                "topic_attribution",
                resolved.granularity,
                _topic_cutoff(evaluated_at, retention_days),
                available_timestamps,
            )
            comparison_coverage = classify_comparison_coverage(
                comparison_expected_timestamps,
                states,
                "topic_attribution",
                resolved.granularity,
                _topic_cutoff(evaluated_at, retention_days),
                available_timestamps,
            )
            result = compare_topic_attribution_periods(baseline_rows, comparison_rows, resolved)
    return _comparison_response(resolved, evaluated_at, baseline_coverage, comparison_coverage, result)
