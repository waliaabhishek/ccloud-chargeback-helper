from __future__ import annotations

import heapq
from collections.abc import Callable, Collection, Iterable
from datetime import UTC, date, datetime
from decimal import Decimal, localcontext
from functools import cmp_to_key
from typing import TYPE_CHECKING, Literal, cast

from core.models.cost_comparison import (
    ChargebackComparisonGroup,
    ComparisonCoverage,
    ComparisonReconciliation,
    ComparisonRow,
    ComparisonSummary,
    CostComparisonResult,
    ResolvedComparisonRequest,
    TopicAttributionComparisonGroup,
)

if TYPE_CHECKING:
    from core.models.chargeback import ChargebackRow
    from core.models.pipeline import PipelineState
    from core.models.topic_attribution import TopicAttributionRow

_SENTINEL_KEYS = frozenset({"UNALLOCATED", "__UNATTRIBUTED__"})


class _Accumulator:
    def __init__(
        self,
        key: str,
        kind: Literal["entity", "unassigned", "sentinel"],
        dimensions: dict[str, str | None],
    ) -> None:
        self.key = key
        self.kind = kind
        self.dimensions = dimensions
        self.baseline_amount = Decimal(0)
        self.comparison_amount = Decimal(0)
        self.baseline_row_count = 0
        self.comparison_row_count = 0


def _as_decimal(value: Decimal) -> Decimal:
    return value if isinstance(value, Decimal) else Decimal(str(value))


def _addition_precision(left: Decimal, right: Decimal) -> int:
    """Return enough significant digits to add two finite Decimals exactly."""
    left_tuple = left.as_tuple()
    right_tuple = right.as_tuple()
    left_exponent = left_tuple.exponent
    right_exponent = right_tuple.exponent
    if not isinstance(left_exponent, int) or not isinstance(right_exponent, int):
        raise ValueError("comparison amounts must be finite decimals")
    least_significant_exponent = min(left_exponent, right_exponent)
    most_significant_digit = max(left.adjusted(), right.adjusted())
    # A carry can add one digit to the aligned coefficient.
    return max(1, most_significant_digit - least_significant_exponent + 2)


def _exact_add(left: Decimal, right: Decimal) -> Decimal:
    """Add Decimals without allowing the caller's context to discard digits."""
    with localcontext() as context:
        context.prec = _addition_precision(left, right)
        return left + right


def _exact_subtract(left: Decimal, right: Decimal) -> Decimal:
    """Subtract Decimals without applying the process-wide context."""
    return _exact_add(left, right.copy_negate())


def _canonical_decimal(value: Decimal) -> Decimal:
    """Strip insignificant zeroes while keeping a plain decimal wire value."""
    if value == 0:
        return Decimal(0)
    rendered = format(value, "f")
    if "." in rendered:
        rendered = rendered.rstrip("0").rstrip(".")
    return Decimal(rendered)


def _kind_for_key(key: str) -> Literal["entity", "unassigned", "sentinel"]:
    if key in _SENTINEL_KEYS or any(key.endswith(f":topic:{sentinel}") for sentinel in _SENTINEL_KEYS):
        return "sentinel"
    if not key:
        return "unassigned"
    return "entity"


def _percentage_change(baseline: Decimal, comparison: Decimal) -> Decimal | None:
    if baseline == 0:
        return None
    return _exact_subtract(comparison, baseline) / baseline * 100


def _accumulate(
    baseline_rows: Iterable[tuple[str, dict[str, str | None], Decimal]],
    comparison_rows: Iterable[tuple[str, dict[str, str | None], Decimal]],
) -> dict[str, _Accumulator]:
    groups: dict[str, _Accumulator] = {}
    for period_rows, is_baseline in ((baseline_rows, True), (comparison_rows, False)):
        for key, dimensions, raw_amount in period_rows:
            group = groups.get(key)
            if group is None:
                group = _Accumulator(key, _kind_for_key(key), dimensions)
                groups[key] = group
            if is_baseline:
                group.baseline_amount = _exact_add(group.baseline_amount, _as_decimal(raw_amount))
                group.baseline_row_count += 1
            else:
                group.comparison_amount = _exact_add(group.comparison_amount, _as_decimal(raw_amount))
                group.comparison_row_count += 1
    return groups


def _comparison_rows(groups: dict[str, _Accumulator]) -> Iterable[ComparisonRow]:
    for group in groups.values():
        change = _exact_subtract(group.comparison_amount, group.baseline_amount)
        if group.baseline_row_count and group.comparison_row_count:
            presence: Literal["both", "baseline_only", "comparison_only"] = "both"
        elif group.baseline_row_count:
            presence = "baseline_only"
        else:
            presence = "comparison_only"
        yield ComparisonRow(
            key=group.key,
            kind=group.kind,
            dimensions=group.dimensions,
            baseline_amount=_canonical_decimal(group.baseline_amount),
            comparison_amount=_canonical_decimal(group.comparison_amount),
            change=_canonical_decimal(change),
            percentage_change=_canonical_decimal(percentage)
            if (percentage := _percentage_change(group.baseline_amount, group.comparison_amount)) is not None
            else None,
            baseline_row_count=group.baseline_row_count,
            comparison_row_count=group.comparison_row_count,
            observed_presence=presence,
        )


def _row_comparator(request: ResolvedComparisonRequest) -> Callable[[ComparisonRow, ComparisonRow], int]:
    def value(row: ComparisonRow) -> Decimal | str | None:
        if request.sort_by == "entity":
            return row.key
        if request.sort_by == "absolute_change":
            return row.change.copy_abs()
        if request.sort_by == "baseline_amount":
            return row.baseline_amount
        if request.sort_by == "comparison_amount":
            return row.comparison_amount
        if request.sort_by == "change":
            return row.change
        return row.percentage_change

    def compare(left: ComparisonRow, right: ComparisonRow) -> int:
        left_value = value(left)
        right_value = value(right)
        if left_value is None and right_value is None:
            primary = 0
        elif left_value is None:
            return 1
        elif right_value is None:
            return -1
        elif isinstance(left_value, str) and isinstance(right_value, str):
            primary = (left_value > right_value) - (left_value < right_value)
        else:
            left_decimal = cast("Decimal", left_value)
            right_decimal = cast("Decimal", right_value)
            if left_decimal < right_decimal:
                primary = -1
            elif left_decimal > right_decimal:
                primary = 1
            else:
                primary = 0
        if primary and request.sort_direction == "desc":
            primary = -primary
        if primary:
            return primary
        return (left.key > right.key) - (left.key < right.key)

    return compare


def _sort_rows(rows: list[ComparisonRow], request: ResolvedComparisonRequest) -> list[ComparisonRow]:
    return sorted(rows, key=cmp_to_key(_row_comparator(request)))


def _bucket_amounts(rows: Iterable[ComparisonRow]) -> tuple[Decimal, Decimal, Decimal]:
    baseline = Decimal(0)
    comparison = Decimal(0)
    for row in rows:
        baseline = _exact_add(baseline, row.baseline_amount)
        comparison = _exact_add(comparison, row.comparison_amount)
    return (
        _canonical_decimal(baseline),
        _canonical_decimal(comparison),
        _canonical_decimal(_exact_subtract(comparison, baseline)),
    )


class _WorstRow:
    """Heap entry whose root is the least desirable returned row."""

    __slots__ = ("row", "_compare")

    def __init__(self, row: ComparisonRow, compare: Callable[[ComparisonRow, ComparisonRow], int]) -> None:
        self.row = row
        self._compare = compare

    def __lt__(self, other: _WorstRow) -> bool:
        return self._compare(self.row, other.row) > 0


def _finalize(groups: dict[str, _Accumulator], request: ResolvedComparisonRequest) -> CostComparisonResult:
    compare_rows = _row_comparator(request)
    top_rows: list[_WorstRow] = []
    baseline_total = Decimal(0)
    comparison_total = Decimal(0)
    increases = Decimal(0)
    decreases = Decimal(0)
    selected_baseline = Decimal(0)
    selected_comparison = Decimal(0)
    full_group_count = 0
    selected_group_count = 0

    for row in _comparison_rows(groups):
        full_group_count += 1
        baseline_total = _exact_add(baseline_total, row.baseline_amount)
        comparison_total = _exact_add(comparison_total, row.comparison_amount)
        if row.change > 0:
            increases = _exact_add(increases, row.change)
        elif row.change < 0:
            decreases = _exact_add(decreases, row.change)

        if request.movement == "increase" and row.change <= 0:
            continue
        if request.movement == "decrease" and row.change >= 0:
            continue

        selected_group_count += 1
        selected_baseline = _exact_add(selected_baseline, row.baseline_amount)
        selected_comparison = _exact_add(selected_comparison, row.comparison_amount)
        entry = _WorstRow(row, compare_rows)
        if len(top_rows) < request.limit:
            heapq.heappush(top_rows, entry)
        elif compare_rows(row, top_rows[0].row) < 0:
            heapq.heapreplace(top_rows, entry)

    returned = _sort_rows([entry.row for entry in top_rows], request)
    returned_baseline, returned_comparison, returned_net = _bucket_amounts(returned)
    movement_excluded_baseline = _exact_subtract(baseline_total, selected_baseline)
    movement_excluded_comparison = _exact_subtract(comparison_total, selected_comparison)
    movement_excluded_net = _exact_subtract(
        _exact_subtract(comparison_total, baseline_total),
        _exact_subtract(selected_comparison, selected_baseline),
    )
    omitted_baseline = _exact_subtract(selected_baseline, returned_baseline)
    omitted_comparison = _exact_subtract(selected_comparison, returned_comparison)
    omitted_net = _exact_subtract(
        _exact_subtract(selected_comparison, selected_baseline),
        returned_net,
    )
    baseline_total = _canonical_decimal(baseline_total)
    comparison_total = _canonical_decimal(comparison_total)
    net_change = _canonical_decimal(_exact_subtract(comparison_total, baseline_total))
    increases = _canonical_decimal(increases)
    decreases = _canonical_decimal(decreases)
    summary = ComparisonSummary(
        baseline_amount=baseline_total,
        comparison_amount=comparison_total,
        increases=increases,
        decreases=decreases,
        net_change=net_change,
        percentage_change=(
            _canonical_decimal(percentage)
            if (percentage := _percentage_change(baseline_total, comparison_total)) is not None
            else None
        ),
    )

    reconciliation = ComparisonReconciliation(
        full_group_count=full_group_count,
        selected_group_count=selected_group_count,
        returned_group_count=len(returned),
        movement_excluded_group_count=full_group_count - selected_group_count,
        row_limit_omitted_group_count=selected_group_count - len(returned),
        returned_baseline_amount=returned_baseline,
        returned_comparison_amount=returned_comparison,
        returned_net_change=returned_net,
        movement_excluded_baseline_amount=_canonical_decimal(movement_excluded_baseline),
        movement_excluded_comparison_amount=_canonical_decimal(movement_excluded_comparison),
        movement_excluded_net_change=_canonical_decimal(movement_excluded_net),
        row_limit_omitted_baseline_amount=_canonical_decimal(omitted_baseline),
        row_limit_omitted_comparison_amount=_canonical_decimal(omitted_comparison),
        row_limit_omitted_net_change=_canonical_decimal(omitted_net),
    )
    return CostComparisonResult(summary=summary, rows=returned, reconciliation=reconciliation)


def compare_chargeback_periods(
    baseline_rows: Iterable[ChargebackRow],
    comparison_rows: Iterable[ChargebackRow],
    request: ResolvedComparisonRequest,
) -> CostComparisonResult:
    """Compare chargeback facts using the requested stable dimension."""

    if request.source != "chargeback":
        raise ValueError("chargeback comparison requires the chargeback source")
    if request.group_by not in ("principal", "resource", "environment"):
        raise ValueError(f"Unsupported chargeback comparison group: {request.group_by!r}")
    group_by = cast("ChargebackComparisonGroup", request.group_by)

    def adapt(rows: Iterable[ChargebackRow]) -> Iterable[tuple[str, dict[str, str | None], Decimal]]:
        for row in rows:
            dimensions: dict[str, str | None]
            if group_by == "principal":
                key = row.identity_id or ""
                dimensions = {"identity_id": key or None}
            elif group_by == "resource":
                key = row.resource_id or ""
                dimensions = {"resource_id": key or None}
            elif group_by == "environment":
                metadata = row.metadata if isinstance(row.metadata, dict) else {}
                raw_environment = metadata.get("env_id", "")
                key = raw_environment if isinstance(raw_environment, str) else str(raw_environment or "")
                dimensions = {"environment_id": key or None}
            else:
                raise ValueError(f"Unsupported chargeback comparison group: {group_by!r}")
            yield key, dimensions, row.amount

    return _finalize(_accumulate(adapt(baseline_rows), adapt(comparison_rows)), request)


def compare_topic_attribution_periods(
    baseline_rows: Iterable[TopicAttributionRow],
    comparison_rows: Iterable[TopicAttributionRow],
    request: ResolvedComparisonRequest,
) -> CostComparisonResult:
    """Compare topic-attribution facts using scoped topic or cluster IDs."""

    if request.source != "topic_attribution":
        raise ValueError("topic attribution comparison requires the topic_attribution source")
    if request.group_by not in ("topic", "cluster"):
        raise ValueError(f"Unsupported topic attribution comparison group: {request.group_by!r}")
    group_by = cast("TopicAttributionComparisonGroup", request.group_by)

    def adapt(rows: Iterable[TopicAttributionRow]) -> Iterable[tuple[str, dict[str, str | None], Decimal]]:
        for row in rows:
            dimensions: dict[str, str | None]
            if group_by == "topic":
                key = row.resource_id
                dimensions = {
                    "cluster_resource_id": row.cluster_resource_id,
                    "topic_name": row.topic_name,
                }
            elif group_by == "cluster":
                key = row.cluster_resource_id or ""
                dimensions = {"cluster_resource_id": key or None}
            else:
                raise ValueError(f"Unsupported topic attribution comparison group: {group_by!r}")
            yield key, dimensions, row.amount

    return _finalize(_accumulate(adapt(baseline_rows), adapt(comparison_rows)), request)


def _utc(timestamp: datetime) -> datetime:
    if timestamp.tzinfo is None:
        return timestamp.replace(tzinfo=UTC)
    return timestamp.astimezone(UTC)


def _normalize_source_slot(timestamp: datetime, granularity: Literal["hourly", "daily", "monthly"]) -> datetime:
    timestamp = _utc(timestamp)
    if granularity == "hourly":
        return timestamp.replace(minute=0, second=0, microsecond=0)
    if granularity == "daily":
        return timestamp.replace(hour=0, minute=0, second=0, microsecond=0)
    return timestamp.replace(month=timestamp.month, day=1, hour=0, minute=0, second=0, microsecond=0)


def classify_comparison_coverage(
    expected_timestamps: Collection[datetime],
    states: Iterable[PipelineState],
    source: Literal["chargeback", "topic_attribution"],
    granularity: Literal["hourly", "daily", "monthly"],
    availability_cutoff_at: datetime | None,
    available_topic_timestamps: Collection[datetime] | None,
) -> ComparisonCoverage:
    """Classify source-slot evidence independently from filtered fact amounts."""
    expected_slots = {_normalize_source_slot(timestamp, granularity) for timestamp in expected_timestamps}
    expected_dates = sorted({slot.date() for slot in expected_slots})
    state_by_date = {state.tracking_date: state for state in states}
    available_slots = (
        {_normalize_source_slot(timestamp, granularity) for timestamp in available_topic_timestamps}
        if available_topic_timestamps is not None
        else set()
    )
    cutoff = _utc(availability_cutoff_at) if availability_cutoff_at is not None else None
    unknown: set[date] = set()
    incomplete: set[date] = set()
    retention_qualified: set[date] = set()

    for slot in sorted(expected_slots):
        tracking_date = slot.date()
        if cutoff is None or slot < cutoff:
            unknown.add(tracking_date)
            retention_qualified.add(tracking_date)

        state = state_by_date.get(tracking_date)
        if state is None:
            unknown.add(tracking_date)
            continue
        usable = state.has_usable_calculation
        if source == "topic_attribution":
            usable = usable and state.topic_overlay_gathered and state.topic_attribution_calculated
        if not usable:
            incomplete.add(tracking_date)
            continue
        if cutoff is None or slot < cutoff:
            continue
        if source == "topic_attribution" and slot not in available_slots:
            unknown.add(tracking_date)
            retention_qualified.add(tracking_date)

    if unknown:
        status: Literal["complete", "incomplete", "unknown"] = "unknown"
    elif incomplete:
        status = "incomplete"
    else:
        status = "complete"
    return ComparisonCoverage(
        status=status,
        expected_dates=expected_dates,
        unknown_dates=sorted(unknown),
        incomplete_dates=sorted(incomplete),
        retention_qualified_dates=sorted(retention_qualified),
        availability_cutoff_at=cutoff,
    )
