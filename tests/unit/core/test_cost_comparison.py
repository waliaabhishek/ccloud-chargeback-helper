from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from decimal import Decimal, localcontext

import pytest

from core.models.chargeback import ChargebackRow, CostType
from core.models.pipeline import PipelineState
from core.models.topic_attribution import TopicAttributionRow

_ECO = "eco"
_TENANT = "tenant"
_BASELINE_START = datetime(2026, 1, 1, tzinfo=UTC)
_COMPARISON_START = datetime(2026, 1, 2, tzinfo=UTC)


def _chargeback(
    identity_id: str,
    amount: str,
    *,
    timestamp: datetime = _BASELINE_START,
    resource_id: str | None = "resource-1",
    env_id: str = "env-1",
) -> ChargebackRow:
    return ChargebackRow(
        ecosystem=_ECO,
        tenant_id=_TENANT,
        timestamp=timestamp,
        resource_id=resource_id,
        product_category="kafka",
        product_type="KAFKA_BASE",
        identity_id=identity_id,
        cost_type=CostType.USAGE,
        amount=Decimal(amount),
        metadata={"env_id": env_id},
    )


def _topic(
    cluster_resource_id: str,
    topic_name: str,
    amount: str,
    *,
    timestamp: datetime = _BASELINE_START,
    product_type: str = "KAFKA_BASE",
    attribution_method: str = "bytes_ratio",
) -> TopicAttributionRow:
    return TopicAttributionRow(
        ecosystem=_ECO,
        tenant_id=_TENANT,
        timestamp=timestamp,
        env_id="env-1",
        cluster_resource_id=cluster_resource_id,
        topic_name=topic_name,
        product_category="kafka",
        product_type=product_type,
        attribution_method=attribution_method,
        amount=Decimal(amount),
    )


def _period(start: datetime, *, days: int = 1) -> object:
    from core.models.cost_comparison import PeriodBounds

    end = start + timedelta(days=days)
    return PeriodBounds(
        start_date=start.date(),
        end_date=(end - timedelta(days=1)).date(),
        start_at=start,
        end_at=end,
    )


def _request(
    *,
    source: str = "chargeback",
    group_by: str = "principal",
    movement: str = "all",
    sort_by: str = "absolute_change",
    sort_direction: str = "desc",
    limit: int = 100,
    granularity: str = "daily",
) -> object:
    from core.models.cost_comparison import ResolvedComparisonRequest

    return ResolvedComparisonRequest(
        source=source,
        group_by=group_by,
        baseline=_period(_BASELINE_START),
        comparison=_period(_COMPARISON_START),
        timezone="UTC",
        granularity=granularity,
        movement=movement,
        sort_by=sort_by,
        sort_direction=sort_direction,
        limit=limit,
    )


def _state(
    tracking_date: date,
    *,
    calculation_id: str | None = "calculation",
    completed_at: datetime | None = _COMPARISON_START,
    overlay_gathered: bool = True,
    attribution_calculated: bool = True,
) -> PipelineState:
    return PipelineState(
        ecosystem=_ECO,
        tenant_id=_TENANT,
        tracking_date=tracking_date,
        chargeback_calculated=True,
        calculation_id=calculation_id,
        calculation_completed_at=completed_at,
        topic_overlay_gathered=overlay_gathered,
        topic_attribution_calculated=attribution_calculated,
    )


class TestChargebackPeriodComparison:
    def test_preserves_long_decimal_totals_and_ranks_sub_28_digit_changes(self) -> None:
        from core.cost_comparison import compare_chargeback_periods

        with localcontext() as context:
            context.prec = 28
            precision_result = compare_chargeback_periods(
                [
                    _chargeback("precise", "12345678901234567890.123456789"),
                    _chargeback("precise", "0.000000001"),
                ],
                [
                    _chargeback("precise", "12345678901234567890.123456790", timestamp=_COMPARISON_START),
                ],
                _request(),
            )
            result = compare_chargeback_periods(
                [
                    _chargeback("near-a", "1"),
                    _chargeback("near-b", "1"),
                ],
                [
                    _chargeback("near-a", "1.000000000000000000000000001", timestamp=_COMPARISON_START),
                    _chargeback("near-b", "1.000000000000000000000000000", timestamp=_COMPARISON_START),
                ],
                _request(sort_by="change", limit=1),
            )

        assert precision_result.summary.baseline_amount == Decimal("12345678901234567890.123456790")
        assert precision_result.summary.comparison_amount == Decimal("12345678901234567890.123456790")
        assert precision_result.rows[0].baseline_amount == Decimal("12345678901234567890.123456790")
        assert result.reconciliation.row_limit_omitted_group_count == 1
        precise = {row.key: row for row in result.rows}
        assert precise["near-a"].change == Decimal("0.000000000000000000000000001")
        assert [row.key for row in result.rows] == ["near-a"]

    def test_keeps_signed_totals_exact_decimal_math_and_zero_baseline_percentage(self) -> None:
        from core.cost_comparison import compare_chargeback_periods

        result = compare_chargeback_periods(
            [
                _chargeback("alice", "0.1"),
                _chargeback("alice", "0.2"),
                _chargeback("credit", "-5"),
                _chargeback("zero", "0"),
            ],
            [
                _chargeback("alice", "0.3", timestamp=_COMPARISON_START),
                _chargeback("credit", "-7", timestamp=_COMPARISON_START),
                _chargeback("zero", "4", timestamp=_COMPARISON_START),
            ],
            _request(),
        )

        assert result.summary.baseline_amount == Decimal("-4.7")
        assert result.summary.comparison_amount == Decimal("-2.7")
        assert result.summary.increases == Decimal("4")
        assert result.summary.decreases == Decimal("-2")
        assert result.summary.net_change == Decimal("2")
        rows = {row.key: row for row in result.rows}
        assert rows["alice"].baseline_amount == Decimal("0.3")
        assert rows["alice"].comparison_amount == Decimal("0.3")
        assert rows["zero"].percentage_change is None
        assert rows["zero"].baseline_row_count == 1
        assert rows["zero"].observed_presence == "both"

    def test_retains_one_period_rows_and_unassigned_dimensions_by_row_presence(self) -> None:
        from core.cost_comparison import compare_chargeback_periods

        result = compare_chargeback_periods(
            [
                _chargeback("baseline-only", "0"),
                _chargeback("", "3", resource_id=None, env_id=""),
            ],
            [_chargeback("comparison-only", "5", timestamp=_COMPARISON_START)],
            _request(group_by="principal"),
        )

        rows = {row.key: row for row in result.rows}
        assert rows["baseline-only"].observed_presence == "baseline_only"
        assert rows["baseline-only"].baseline_row_count == 1
        assert rows["comparison-only"].observed_presence == "comparison_only"
        assert rows[""].kind == "unassigned"
        assert rows[""].baseline_amount == Decimal("3")

    @pytest.mark.parametrize(
        "group_by, expected_key",
        [
            ("principal", "alice"),
            ("resource", "resource-1"),
            ("environment", "env-1"),
        ],
    )
    def test_groups_chargeback_rows_by_the_requested_persisted_dimension(
        self, group_by: str, expected_key: str
    ) -> None:
        from core.cost_comparison import compare_chargeback_periods

        result = compare_chargeback_periods(
            [_chargeback("alice", "4")],
            [_chargeback("alice", "6", timestamp=_COMPARISON_START)],
            _request(group_by=group_by),
        )

        assert [row.key for row in result.rows] == [expected_key]
        assert result.rows[0].change == Decimal("2")

    def test_applies_movement_and_limit_without_changing_full_scope_summary(self) -> None:
        from core.cost_comparison import compare_chargeback_periods

        baseline = [_chargeback("a", "10"), _chargeback("b", "5"), _chargeback("c", "1")]
        comparison = [
            _chargeback("a", "12", timestamp=_COMPARISON_START),
            _chargeback("b", "1", timestamp=_COMPARISON_START),
            _chargeback("c", "1", timestamp=_COMPARISON_START),
        ]

        all_result = compare_chargeback_periods(baseline, comparison, _request(limit=1))
        increases = compare_chargeback_periods(baseline, comparison, _request(movement="increase", limit=1))

        assert all_result.summary == increases.summary
        assert [row.key for row in all_result.rows] == ["b"]
        assert [row.key for row in increases.rows] == ["a"]
        assert all_result.reconciliation.row_limit_omitted_group_count == 2
        assert increases.reconciliation.movement_excluded_group_count == 2
        assert increases.reconciliation.row_limit_omitted_group_count == 0

    def test_reconciliation_keeps_positive_omission_counts_when_amounts_cancel_to_zero(self) -> None:
        from core.cost_comparison import compare_chargeback_periods

        result = compare_chargeback_periods(
            [_chargeback("kept", "2"), _chargeback("up", "1"), _chargeback("down", "-1")],
            [
                _chargeback("kept", "4", timestamp=_COMPARISON_START),
                _chargeback("up", "2", timestamp=_COMPARISON_START),
                _chargeback("down", "-2", timestamp=_COMPARISON_START),
            ],
            _request(movement="increase", limit=1),
        )

        reconciliation = result.reconciliation
        assert reconciliation.movement_excluded_group_count == 1
        assert reconciliation.movement_excluded_baseline_amount == Decimal("-1")
        assert reconciliation.row_limit_omitted_group_count == 1
        assert reconciliation.row_limit_omitted_baseline_amount == Decimal("1")
        assert reconciliation.full_group_count == (
            reconciliation.returned_group_count
            + reconciliation.movement_excluded_group_count
            + reconciliation.row_limit_omitted_group_count
        )
        assert reconciliation.selected_group_count == (
            reconciliation.returned_group_count + reconciliation.row_limit_omitted_group_count
        )
        assert result.summary.baseline_amount == (
            reconciliation.returned_baseline_amount
            + reconciliation.movement_excluded_baseline_amount
            + reconciliation.row_limit_omitted_baseline_amount
        )
        assert result.summary.comparison_amount == (
            reconciliation.returned_comparison_amount
            + reconciliation.movement_excluded_comparison_amount
            + reconciliation.row_limit_omitted_comparison_amount
        )
        assert result.summary.net_change == (
            reconciliation.returned_net_change
            + reconciliation.movement_excluded_net_change
            + reconciliation.row_limit_omitted_net_change
        )

    @pytest.mark.parametrize(
        ("sort_by", "sort_direction", "expected_keys"),
        [
            ("entity", "asc", ["a", "b", "c"]),
            ("baseline_amount", "desc", ["b", "c", "a"]),
            ("comparison_amount", "asc", ["b", "a", "c"]),
            ("change", "desc", ["a", "c", "b"]),
            ("percentage_change", "desc", ["a", "c", "b"]),
        ],
    )
    def test_supports_each_server_sort_with_stable_key_ties(
        self, sort_by: str, sort_direction: str, expected_keys: list[str]
    ) -> None:
        from core.cost_comparison import compare_chargeback_periods

        baseline = [_chargeback("a", "1"), _chargeback("b", "4"), _chargeback("c", "2")]
        comparison = [
            _chargeback("a", "3", timestamp=_COMPARISON_START),
            _chargeback("b", "1", timestamp=_COMPARISON_START),
            _chargeback("c", "3", timestamp=_COMPARISON_START),
        ]

        result = compare_chargeback_periods(
            baseline,
            comparison,
            _request(sort_by=sort_by, sort_direction=sort_direction),
        )

        assert [row.key for row in result.rows] == expected_keys


class TestTopicAttributionPeriodComparison:
    def test_uses_scoped_topic_keys_for_duplicate_topic_names(self) -> None:
        from core.cost_comparison import compare_topic_attribution_periods

        result = compare_topic_attribution_periods(
            [_topic("cluster-a", "orders", "1"), _topic("cluster-b", "orders", "2")],
            [
                _topic("cluster-a", "orders", "3", timestamp=_COMPARISON_START),
                _topic("cluster-b", "orders", "5", timestamp=_COMPARISON_START),
            ],
            _request(source="topic_attribution", group_by="topic"),
        )

        rows = {row.key: row for row in result.rows}
        assert set(rows) == {"cluster-a:topic:orders", "cluster-b:topic:orders"}
        assert rows["cluster-a:topic:orders"].dimensions == {
            "cluster_resource_id": "cluster-a",
            "topic_name": "orders",
        }
        assert rows["cluster-b:topic:orders"].change == Decimal("3")

    def test_groups_topic_attribution_by_cluster_and_preserves_sentinel_rows(self) -> None:
        from core.cost_comparison import compare_topic_attribution_periods

        result = compare_topic_attribution_periods(
            [_topic("cluster-a", "__UNATTRIBUTED__", "2"), _topic("cluster-a", "payments", "1")],
            [_topic("cluster-a", "__UNATTRIBUTED__", "4", timestamp=_COMPARISON_START)],
            _request(source="topic_attribution", group_by="cluster"),
        )

        assert [row.key for row in result.rows] == ["cluster-a"]
        assert result.rows[0].baseline_amount == Decimal("3")
        assert result.rows[0].comparison_amount == Decimal("4")

    def test_topic_attribution_applies_movement_sort_limit_and_reconciles_full_scope(self) -> None:
        from core.cost_comparison import compare_topic_attribution_periods

        baseline = [
            _topic("cluster-a", "orders", "1"),
            _topic("cluster-b", "orders", "8"),
            _topic("cluster-c", "payments", "3"),
        ]
        comparison = [
            _topic("cluster-a", "orders", "4", timestamp=_COMPARISON_START),
            _topic("cluster-b", "orders", "2", timestamp=_COMPARISON_START),
            _topic("cluster-d", "events", "9", timestamp=_COMPARISON_START),
        ]

        result = compare_topic_attribution_periods(
            baseline,
            comparison,
            _request(
                source="topic_attribution",
                group_by="topic",
                movement="decrease",
                sort_by="change",
                sort_direction="asc",
                limit=1,
            ),
        )

        assert result.summary.baseline_amount == Decimal("12")
        assert result.summary.comparison_amount == Decimal("15")
        assert result.summary.increases == Decimal("12")
        assert result.summary.decreases == Decimal("-9")
        assert result.summary.net_change == Decimal("3")
        assert result.summary.percentage_change == Decimal("25")
        assert [row.key for row in result.rows] == ["cluster-b:topic:orders"]
        assert result.rows[0].baseline_amount == Decimal("8")
        assert result.rows[0].comparison_amount == Decimal("2")
        assert result.rows[0].change == Decimal("-6")

        reconciliation = result.reconciliation
        assert reconciliation.full_group_count == 4
        assert reconciliation.selected_group_count == 2
        assert reconciliation.returned_group_count == 1
        assert reconciliation.movement_excluded_group_count == 2
        assert reconciliation.row_limit_omitted_group_count == 1
        assert reconciliation.returned_baseline_amount == Decimal("8")
        assert reconciliation.returned_comparison_amount == Decimal("2")
        assert reconciliation.returned_net_change == Decimal("-6")
        assert reconciliation.movement_excluded_baseline_amount == Decimal("1")
        assert reconciliation.movement_excluded_comparison_amount == Decimal("13")
        assert reconciliation.movement_excluded_net_change == Decimal("12")
        assert reconciliation.row_limit_omitted_baseline_amount == Decimal("3")
        assert reconciliation.row_limit_omitted_comparison_amount == Decimal("0")
        assert reconciliation.row_limit_omitted_net_change == Decimal("-3")
        assert reconciliation.full_group_count == (
            reconciliation.selected_group_count + reconciliation.movement_excluded_group_count
        )
        assert reconciliation.selected_group_count == (
            reconciliation.returned_group_count + reconciliation.row_limit_omitted_group_count
        )
        assert result.summary.baseline_amount == (
            reconciliation.returned_baseline_amount
            + reconciliation.movement_excluded_baseline_amount
            + reconciliation.row_limit_omitted_baseline_amount
        )
        assert result.summary.comparison_amount == (
            reconciliation.returned_comparison_amount
            + reconciliation.movement_excluded_comparison_amount
            + reconciliation.row_limit_omitted_comparison_amount
        )
        assert result.summary.net_change == (
            reconciliation.returned_net_change
            + reconciliation.movement_excluded_net_change
            + reconciliation.row_limit_omitted_net_change
        )

    @pytest.mark.parametrize(
        ("source", "group_by", "expected_error"),
        [
            ("chargeback", "cluster", "Unsupported chargeback comparison group"),
            ("topic_attribution", "environment", "Unsupported topic attribution comparison group"),
        ],
    )
    def test_rejects_group_values_outside_the_source_contract(
        self, source: str, group_by: str, expected_error: str
    ) -> None:
        from core.cost_comparison import compare_chargeback_periods, compare_topic_attribution_periods

        with pytest.raises(ValueError, match=expected_error):
            if source == "chargeback":
                compare_chargeback_periods([], [], _request(source=source, group_by=group_by))
            else:
                compare_topic_attribution_periods([], [], _request(source=source, group_by=group_by))


class TestComparisonCoverage:
    def test_chargeback_successful_zero_fact_period_is_complete(self) -> None:
        from core.cost_comparison import classify_comparison_coverage

        coverage = classify_comparison_coverage(
            [_BASELINE_START],
            [_state(_BASELINE_START.date())],
            source="chargeback",
            granularity="daily",
            availability_cutoff_at=datetime(2025, 1, 1, tzinfo=UTC),
            available_topic_timestamps=None,
        )

        assert coverage.status == "complete"
        assert coverage.unknown_dates == []
        assert coverage.incomplete_dates == []

    @pytest.mark.parametrize(
        ("calculation_id", "completed_at"),
        [(None, _BASELINE_START), ("", _BASELINE_START), ("calculation", None)],
    )
    def test_chargeback_state_without_usable_calculation_is_incomplete(
        self, calculation_id: str | None, completed_at: datetime | None
    ) -> None:
        from core.cost_comparison import classify_comparison_coverage

        coverage = classify_comparison_coverage(
            [_BASELINE_START],
            [_state(_BASELINE_START.date(), calculation_id=calculation_id, completed_at=completed_at)],
            source="chargeback",
            granularity="daily",
            availability_cutoff_at=datetime(2025, 1, 1, tzinfo=UTC),
            available_topic_timestamps=None,
        )

        assert coverage.status == "incomplete"
        assert coverage.incomplete_dates == [_BASELINE_START.date()]

    @pytest.mark.parametrize(
        ("overlay_gathered", "attribution_calculated"),
        [(False, True), (True, False)],
    )
    def test_topic_attribution_requires_both_additional_completion_flags(
        self, overlay_gathered: bool, attribution_calculated: bool
    ) -> None:
        from core.cost_comparison import classify_comparison_coverage

        coverage = classify_comparison_coverage(
            [_BASELINE_START],
            [
                _state(
                    _BASELINE_START.date(),
                    overlay_gathered=overlay_gathered,
                    attribution_calculated=attribution_calculated,
                )
            ],
            source="topic_attribution",
            granularity="daily",
            availability_cutoff_at=datetime(2025, 1, 1, tzinfo=UTC),
            available_topic_timestamps={_BASELINE_START},
        )

        assert coverage.status == "incomplete"
        assert coverage.incomplete_dates == [_BASELINE_START.date()]

    @pytest.mark.parametrize(
        ("granularity", "expected", "available"),
        [
            ("hourly", datetime(2026, 1, 1, 12, tzinfo=UTC), datetime(2026, 1, 1, 12, 47, tzinfo=UTC)),
            ("daily", datetime(2026, 1, 1, tzinfo=UTC), datetime(2026, 1, 1, 18, 47, tzinfo=UTC)),
            ("monthly", datetime(2026, 1, 1, tzinfo=UTC), datetime(2026, 1, 19, 18, 47, tzinfo=UTC)),
        ],
    )
    def test_topic_availability_normalizes_slots_for_each_granularity(
        self, granularity: str, expected: datetime, available: datetime
    ) -> None:
        from core.cost_comparison import classify_comparison_coverage

        coverage = classify_comparison_coverage(
            [expected],
            [_state(expected.date())],
            source="topic_attribution",
            granularity=granularity,
            availability_cutoff_at=datetime(2025, 1, 1, tzinfo=UTC),
            available_topic_timestamps={available},
        )

        assert coverage.status == "complete"
        assert coverage.expected_dates == [expected.date()]

    def test_unknown_precedes_incomplete_and_includes_all_qualified_dates(self) -> None:
        from core.cost_comparison import classify_comparison_coverage

        eligible = _BASELINE_START + timedelta(days=1)
        coverage = classify_comparison_coverage(
            [_BASELINE_START, eligible],
            [_state(_BASELINE_START.date()), _state(eligible.date(), calculation_id=None)],
            source="chargeback",
            granularity="daily",
            availability_cutoff_at=eligible,
            available_topic_timestamps=None,
        )

        assert coverage.status == "unknown"
        assert coverage.unknown_dates == [_BASELINE_START.date()]
        assert coverage.retention_qualified_dates == [_BASELINE_START.date()]
        assert coverage.incomplete_dates == [eligible.date()]

    def test_unknown_topic_retention_policy_qualifies_every_expected_date(self) -> None:
        from core.cost_comparison import classify_comparison_coverage

        expected = [_BASELINE_START, _BASELINE_START + timedelta(days=1)]
        coverage = classify_comparison_coverage(
            expected,
            [_state(expected[0].date()), _state(expected[1].date(), calculation_id=None)],
            source="topic_attribution",
            granularity="daily",
            availability_cutoff_at=None,
            available_topic_timestamps=set(),
        )

        assert coverage.status == "unknown"
        assert coverage.unknown_dates == [item.date() for item in expected]
        assert coverage.retention_qualified_dates == [item.date() for item in expected]
        assert coverage.incomplete_dates == [expected[1].date()]

    def test_topic_attribution_requires_unfiltered_slot_evidence_even_after_retention_expands(self) -> None:
        from core.cost_comparison import classify_comparison_coverage

        coverage = classify_comparison_coverage(
            [_BASELINE_START],
            [_state(_BASELINE_START.date())],
            source="topic_attribution",
            granularity="daily",
            availability_cutoff_at=datetime(2025, 1, 1, tzinfo=UTC),
            available_topic_timestamps=set(),
        )

        assert coverage.status == "unknown"
        assert coverage.unknown_dates == [_BASELINE_START.date()]
        assert coverage.retention_qualified_dates == [_BASELINE_START.date()]
