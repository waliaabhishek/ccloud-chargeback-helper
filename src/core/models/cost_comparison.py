from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime
from decimal import Decimal
from typing import Literal

ComparisonSource = Literal["chargeback", "topic_attribution"]
ChargebackComparisonGroup = Literal["principal", "resource", "environment"]
TopicAttributionComparisonGroup = Literal["topic", "cluster"]
ComparisonGroup = ChargebackComparisonGroup | TopicAttributionComparisonGroup
ComparisonGranularity = Literal["hourly", "daily", "monthly"]
ComparisonMovement = Literal["all", "increase", "decrease"]
ComparisonSort = Literal[
    "absolute_change",
    "entity",
    "baseline_amount",
    "comparison_amount",
    "change",
    "percentage_change",
]
ComparisonSortDirection = Literal["asc", "desc"]


@dataclass(frozen=True)
class PeriodBounds:
    """Requested inclusive dates and their resolved UTC half-open bounds."""

    start_date: date
    end_date: date
    start_at: datetime
    end_at: datetime

    @property
    def duration_seconds(self) -> int:
        return int((self.end_at - self.start_at).total_seconds())


@dataclass(frozen=True)
class ResolvedComparisonRequest:
    """Validated source-independent options used by the comparison service."""

    source: ComparisonSource
    group_by: ComparisonGroup
    baseline: PeriodBounds
    comparison: PeriodBounds
    timezone: str
    granularity: ComparisonGranularity
    movement: ComparisonMovement
    sort_by: ComparisonSort
    sort_direction: ComparisonSortDirection
    limit: int


@dataclass
class ComparisonCoverage:
    status: Literal["complete", "incomplete", "unknown"]
    expected_dates: list[date] = field(default_factory=list)
    unknown_dates: list[date] = field(default_factory=list)
    incomplete_dates: list[date] = field(default_factory=list)
    retention_qualified_dates: list[date] = field(default_factory=list)
    availability_cutoff_at: datetime | None = None


@dataclass
class ComparisonRow:
    key: str
    kind: Literal["entity", "unassigned", "sentinel"]
    dimensions: dict[str, str | None]
    baseline_amount: Decimal
    comparison_amount: Decimal
    change: Decimal
    percentage_change: Decimal | None
    baseline_row_count: int
    comparison_row_count: int
    observed_presence: Literal["both", "baseline_only", "comparison_only"]


@dataclass
class ComparisonSummary:
    baseline_amount: Decimal
    comparison_amount: Decimal
    increases: Decimal
    decreases: Decimal
    net_change: Decimal
    percentage_change: Decimal | None


@dataclass
class ComparisonReconciliation:
    full_group_count: int
    selected_group_count: int
    returned_group_count: int
    movement_excluded_group_count: int
    row_limit_omitted_group_count: int
    returned_baseline_amount: Decimal
    returned_comparison_amount: Decimal
    returned_net_change: Decimal
    movement_excluded_baseline_amount: Decimal
    movement_excluded_comparison_amount: Decimal
    movement_excluded_net_change: Decimal
    row_limit_omitted_baseline_amount: Decimal
    row_limit_omitted_comparison_amount: Decimal
    row_limit_omitted_net_change: Decimal


@dataclass
class CostComparisonResult:
    summary: ComparisonSummary
    rows: list[ComparisonRow]
    reconciliation: ComparisonReconciliation
