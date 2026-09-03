"""Behavioral coverage for the additive Showcase Confluent demo profile."""

from __future__ import annotations

from collections import Counter, defaultdict
from collections.abc import Mapping
from dataclasses import fields, is_dataclass
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from itertools import pairwise
from typing import TYPE_CHECKING, Any

import pytest

from core.models import ChargebackRow, CoreIdentity, CoreResource, MetricRow
from core.models.resource import ResourceStatus
from plugins.confluent_cloud.demo.scenario import build_clean_demo_scenario

if TYPE_CHECKING:
    from collections.abc import Iterable

    from core.models.topic_attribution import TopicAttributionRow
    from plugins.confluent_cloud.demo.scenario import CleanDemoScenario, ShowcaseDemoScenario


_ANCHOR = date(2026, 9, 2)
_TENANT_ID = "northstar-confluent"
_CENT = Decimal("0.01")
_METRIC_KEYS = {"received_bytes", "sent_bytes", "retained_bytes", "partition_count"}
_PRINCIPAL_TYPES = frozenset({"service_account", "user", "principal", "identity_pool"})


def _clean() -> CleanDemoScenario:
    return build_clean_demo_scenario(tenant_id=_TENANT_ID, anchor_date=_ANCHOR)


def _showcase(anchor_date: date = _ANCHOR) -> ShowcaseDemoScenario:
    from plugins.confluent_cloud.demo.scenario import build_showcase_demo_scenario

    return build_showcase_demo_scenario(tenant_id=_TENANT_ID, anchor_date=anchor_date)


def _freeze(value: Any) -> object:
    """Make nested logical records hashable without discarding their fields."""
    if is_dataclass(value) and not isinstance(value, type):
        return (
            type(value).__qualname__,
            tuple((field.name, _freeze(getattr(value, field.name))) for field in fields(value)),
        )
    if isinstance(value, Mapping):
        return tuple(sorted((str(key), _freeze(item)) for key, item in value.items()))
    if isinstance(value, (list, tuple, set, frozenset)):
        return tuple(_freeze(item) for item in value)
    return value


def _assert_granular_subset(clean_records: Iterable[object], showcase_records: Iterable[object]) -> None:
    assert Counter(_freeze(record) for record in clean_records) <= Counter(
        _freeze(record) for record in showcase_records
    )


def _is_active_at(value: CoreResource | CoreIdentity, timestamp: datetime) -> bool:
    return (value.created_at is None or value.created_at <= timestamp) and (
        value.deleted_at is None or timestamp < value.deleted_at
    )


def _daily_chargeback_totals(rows: Iterable[ChargebackRow]) -> dict[date, Decimal]:
    totals: defaultdict[date, Decimal] = defaultdict(Decimal)
    for row in rows:
        totals[row.timestamp.date()] += row.amount
    return dict(totals)


def _daily_topic_totals(rows: Iterable[TopicAttributionRow]) -> dict[tuple[str, str], dict[date, Decimal]]:
    totals: defaultdict[tuple[str, str], defaultdict[date, Decimal]] = defaultdict(lambda: defaultdict(Decimal))
    for row in rows:
        totals[(row.cluster_resource_id, row.topic_name)][row.timestamp.date()] += row.amount
    return {key: dict(value) for key, value in totals.items()}


def _daily_principal_totals(rows: Iterable[ChargebackRow]) -> dict[str, dict[date, Decimal]]:
    totals: defaultdict[str, defaultdict[date, Decimal]] = defaultdict(lambda: defaultdict(Decimal))
    for row in rows:
        totals[row.identity_id][row.timestamp.date()] += row.amount
    return {key: dict(value) for key, value in totals.items()}


def _has_exact_baseline_then_spike(series: Mapping[date, Decimal], anchor_date: date) -> bool:
    baseline_dates = tuple(anchor_date - timedelta(days=offset) for offset in range(30, 0, -1))
    baseline = tuple(series.get(day) for day in baseline_dates)
    if any(value is None or value <= 0 for value in baseline):
        return False
    baseline_value = baseline[0]
    if baseline_value is None:
        return False
    return len(set(baseline)) == 1 and series.get(anchor_date, Decimal("0")) > baseline_value * 3


def _has_strict_trend(series: Mapping[date, Decimal], *, increasing: bool) -> bool:
    ordered_dates = tuple(sorted(series))
    for start in range(len(ordered_dates) - 29):
        values = tuple(series[day] for day in ordered_dates[start : start + 30])
        if all(value > 0 for value in values) and all(
            left < right if increasing else left > right for left, right in pairwise(values)
        ):
            return True
    return False


def _has_step(series: Mapping[date, Decimal], *, increasing: bool) -> bool:
    ordered_dates = tuple(sorted(series))
    for start in range(len(ordered_dates) - 14):
        before = tuple(series[day] for day in ordered_dates[start : start + 7])
        after = tuple(series[day] for day in ordered_dates[start + 7 : start + 14])
        if len(set(before)) != 1 or len(set(after)) != 1 or before[0] <= 0 or after[0] <= 0:
            continue
        if increasing and after[0] > before[0]:
            return True
        if not increasing and after[0] < before[0]:
            return True
    return False


def _topic_name(metric: MetricRow, topic_names: set[str]) -> str:
    matched = topic_names & set(metric.labels.values())
    assert len(matched) == 1
    return matched.pop()


def _active_scope_counts(
    showcase: ShowcaseDemoScenario,
    tracking_date: date,
) -> tuple[tuple[int, int], dict[str, tuple[int, int]], dict[str, tuple[int, int]]]:
    timestamp = datetime.combine(tracking_date, datetime.min.time(), tzinfo=UTC)
    resources_by_id = {resource.resource_id: resource for resource in showcase.resources}
    topics = tuple(resource for resource in showcase.resources if resource.resource_type == "topic")
    tenant_topic_count = 0
    environment_topic_counts: defaultdict[str, int] = defaultdict(int)
    cluster_topic_counts: defaultdict[str, int] = defaultdict(int)
    for topic in topics:
        current_id: str | None = topic.resource_id
        visited: set[str] = set()
        cluster_id: str | None = None
        environment_id: str | None = None
        while current_id is not None and current_id not in visited:
            visited.add(current_id)
            current = resources_by_id.get(current_id)
            if current is None:
                break
            if current.resource_type == "kafka_cluster":
                cluster_id = current.resource_id
            elif current.resource_type == "environment":
                environment_id = current.resource_id
            current_id = current.parent_id
        if not _is_active_at(topic, timestamp):
            continue
        tenant_topic_count += 1
        if environment_id is not None:
            environment_topic_counts[environment_id] += 1
        if cluster_id is not None:
            cluster_topic_counts[cluster_id] += 1

    identities_by_id = {identity.identity_id: identity for identity in showcase.identities}
    tenant_principal_ids: set[str] = set()
    environment_principal_ids: defaultdict[str, set[str]] = defaultdict(set)
    cluster_principal_ids: defaultdict[str, set[str]] = defaultdict(set)
    for chargeback_row in showcase.chargebacks:
        if chargeback_row.timestamp.date() != tracking_date:
            continue
        identity = identities_by_id.get(chargeback_row.identity_id)
        resource = resources_by_id.get(str(chargeback_row.resource_id))
        if (
            identity is None
            or identity.identity_type not in _PRINCIPAL_TYPES
            or not _is_active_at(identity, timestamp)
            or resource is None
            or not _is_active_at(resource, timestamp)
        ):
            continue
        tenant_principal_ids.add(identity.identity_id)
        chargeback_cluster_id: str | None = None
        chargeback_environment_id: str | None = None
        chargeback_current_id: str | None = resource.resource_id
        chargeback_visited: set[str] = set()
        while chargeback_current_id is not None and chargeback_current_id not in chargeback_visited:
            chargeback_visited.add(chargeback_current_id)
            current = resources_by_id.get(chargeback_current_id)
            if current is None:
                break
            if current.resource_type == "kafka_cluster":
                chargeback_cluster_id = current.resource_id
            elif current.resource_type == "environment":
                chargeback_environment_id = current.resource_id
            chargeback_current_id = current.parent_id
        if chargeback_environment_id is not None:
            environment_principal_ids[chargeback_environment_id].add(identity.identity_id)
        if chargeback_cluster_id is not None:
            cluster_principal_ids[chargeback_cluster_id].add(identity.identity_id)

    return (
        (tenant_topic_count, len(tenant_principal_ids)),
        {
            environment_id: (topic_count, len(environment_principal_ids[environment_id]))
            for environment_id, topic_count in environment_topic_counts.items()
        },
        {
            cluster_id: (topic_count, len(cluster_principal_ids[cluster_id]))
            for cluster_id, topic_count in cluster_topic_counts.items()
        },
    )


def test_showcase_is_deterministic_and_uses_the_same_rolling_window_as_clean() -> None:
    first = _showcase()
    second = _showcase()
    shifted = _showcase(date(2026, 8, 31))

    assert first == second
    assert (first.start_date, first.anchor_date) == (date(2026, 3, 3), _ANCHOR)
    assert (shifted.start_date, shifted.anchor_date) == (date(2026, 3, 1), date(2026, 8, 31))


def test_showcase_preserves_every_clean_logical_record_and_adds_only_lifecycle_records() -> None:
    clean = _clean()
    showcase = _showcase()

    _assert_granular_subset(clean.resources, showcase.resources)
    _assert_granular_subset(clean.identities, showcase.identities)
    _assert_granular_subset(clean.entity_tags, showcase.entity_tags)
    _assert_granular_subset(clean.billing_lines, showcase.billing_lines)
    _assert_granular_subset(clean.chargebacks, showcase.chargebacks)
    _assert_granular_subset(clean.topic_attributions, showcase.topic_attributions)
    _assert_granular_subset(clean.preview_source_capture.records, showcase.preview_source_capture.records)
    _assert_granular_subset(
        (capture for run in clean.allocation_lineage_runs for capture in run.captures),
        (capture for run in showcase.allocation_lineage_runs for capture in run.captures),
    )
    assert showcase.pipeline_states == clean.pipeline_states

    clean_resource_records = {_freeze(resource) for resource in clean.resources}
    clean_identity_records = {_freeze(identity) for identity in clean.identities}
    added_resources = tuple(
        resource for resource in showcase.resources if _freeze(resource) not in clean_resource_records
    )
    added_identities = tuple(
        identity for identity in showcase.identities if _freeze(identity) not in clean_identity_records
    )

    assert Counter(resource.resource_type for resource in added_resources) == {"topic": 2}
    assert Counter(resource.status for resource in added_resources) == {
        ResourceStatus.ACTIVE: 1,
        ResourceStatus.DELETED: 1,
    }
    assert Counter(identity.identity_type for identity in added_identities) == {"service_account": 2}
    assert sum(identity.deleted_at is None for identity in added_identities) == 1
    assert sum(identity.deleted_at is not None for identity in added_identities) == 1
    assert all(
        resource.deleted_at is not None for resource in added_resources if resource.status is ResourceStatus.DELETED
    )
    assert all(resource.deleted_at is None for resource in added_resources if resource.status is ResourceStatus.ACTIVE)
    assert all(tag.tag_key == "team" for tag in showcase.entity_tags)

    resources_by_id = {resource.resource_id: resource for resource in showcase.resources}
    identities_by_id = {identity.identity_id: identity for identity in showcase.identities}
    topics_by_name = {
        (resource.parent_id, resource.display_name): resource
        for resource in showcase.resources
        if resource.resource_type == "topic"
    }
    for line in showcase.billing_lines:
        assert _is_active_at(resources_by_id[line.resource_id], line.timestamp)
    for chargeback_row in showcase.chargebacks:
        assert chargeback_row.resource_id is not None
        assert _is_active_at(resources_by_id[chargeback_row.resource_id], chargeback_row.timestamp)
        assert _is_active_at(identities_by_id[chargeback_row.identity_id], chargeback_row.timestamp)
    for topic_row in showcase.topic_attributions:
        assert _is_active_at(topics_by_name[(topic_row.cluster_resource_id, topic_row.topic_name)], topic_row.timestamp)
    for topic in added_resources:
        attributed_dates = {
            row.timestamp.date() for row in showcase.topic_attributions if row.topic_name == topic.display_name
        }
        assert attributed_dates
        created_at = topic.created_at
        assert created_at is not None
        assert min(attributed_dates) >= created_at.date()
        deleted_at = topic.deleted_at
        if deleted_at is None:
            assert max(attributed_dates) == showcase.anchor_date
        else:
            assert max(attributed_dates) < deleted_at.date()
    for identity in added_identities:
        allocated_dates = {
            row.timestamp.date() for row in showcase.chargebacks if row.identity_id == identity.identity_id
        }
        assert allocated_dates
        created_at = identity.created_at
        assert created_at is not None
        assert min(allocated_dates) >= created_at.date()
        deleted_at = identity.deleted_at
        if deleted_at is None:
            assert max(allocated_dates) == showcase.anchor_date
        else:
            assert max(allocated_dates) < deleted_at.date()


def test_showcase_reconciles_additive_cost_evidence_and_exposes_comparison_source_conditions() -> None:
    showcase = _showcase()

    billing_totals: defaultdict[tuple[datetime, str, str, str, str], Decimal] = defaultdict(Decimal)
    chargeback_totals: defaultdict[tuple[datetime, str, str, str, str], Decimal] = defaultdict(Decimal)
    attribution_totals: defaultdict[tuple[datetime, str, str, str, str], Decimal] = defaultdict(Decimal)
    for line in showcase.billing_lines:
        billing_totals[(line.timestamp, line.env_id, line.resource_id, line.product_category, line.product_type)] += (
            line.total_cost
        )
    for chargeback_row in showcase.chargebacks:
        chargeback_totals[
            (
                chargeback_row.timestamp,
                str(chargeback_row.metadata["env_id"]),
                chargeback_row.resource_id or "",
                chargeback_row.product_category,
                chargeback_row.product_type,
            )
        ] += chargeback_row.amount
    for topic_row in showcase.topic_attributions:
        attribution_totals[
            (
                topic_row.timestamp,
                topic_row.env_id,
                topic_row.cluster_resource_id,
                topic_row.product_category,
                topic_row.product_type,
            )
        ] += topic_row.amount

    assert dict(chargeback_totals) == dict(billing_totals)
    assert dict(attribution_totals) == {key: amount for key, amount in billing_totals.items() if key[3] == "KAFKA"}
    assert len(showcase.preview_source_capture.records) == len(showcase.billing_lines)
    assert sum(len(run.captures) for run in showcase.allocation_lineage_runs) == len(showcase.billing_lines)

    per_line: defaultdict[tuple[str, str], dict[date, Decimal]] = defaultdict(dict)
    for line in showcase.billing_lines:
        per_line[(line.resource_id, line.product_type)][line.timestamp.date()] = line.total_cost
    assert any(_has_strict_trend(series, increasing=True) for series in per_line.values())
    assert any(_has_strict_trend(series, increasing=False) for series in per_line.values())
    assert any(_has_step(series, increasing=True) for series in per_line.values())
    assert any(_has_step(series, increasing=False) for series in per_line.values())
    assert max(len(series) for series in per_line.values()) >= 30

    daily_tenant = _daily_chargeback_totals(showcase.chargebacks)
    daily_topics = _daily_topic_totals(showcase.topic_attributions)
    daily_principals = _daily_principal_totals(showcase.chargebacks)
    assert _has_exact_baseline_then_spike(daily_tenant, showcase.anchor_date)
    assert any(_has_exact_baseline_then_spike(series, showcase.anchor_date) for series in daily_topics.values())
    assert any(_has_exact_baseline_then_spike(series, showcase.anchor_date) for series in daily_principals.values())

    final_30 = tuple(showcase.anchor_date - timedelta(days=offset) for offset in range(29, -1, -1))
    tenant_projection = sum((daily_tenant[day] for day in final_30), Decimal("0"))
    environment_projections: defaultdict[str, Decimal] = defaultdict(Decimal)
    cluster_projections: defaultdict[str, Decimal] = defaultdict(Decimal)
    for line in showcase.billing_lines:
        if line.timestamp.date() in final_30:
            environment_projections[line.env_id] += line.total_cost
            if line.resource_id.startswith("lkc-"):
                cluster_projections[line.resource_id] += line.total_cost
    assert tenant_projection / Decimal("1000000") < Decimal("0.80")
    assert any(
        Decimal("0.80") <= projection / Decimal("80000") <= Decimal("1.00")
        for projection in environment_projections.values()
    )
    assert any(projection / Decimal("30000") > Decimal("1.00") for projection in cluster_projections.values())
    assert all(value.quantize(_CENT) == value for value in daily_tenant.values())


def test_showcase_uses_existing_metric_rows_for_complete_unit_denominators_and_partition_efficiency() -> None:
    showcase = _showcase()
    source_metrics = showcase.source_metrics
    topic_resources = tuple(resource for resource in showcase.resources if resource.resource_type == "topic")
    topic_names = {str(resource.display_name) for resource in topic_resources}
    topic_by_name = {str(resource.display_name): resource for resource in topic_resources}
    cluster_by_id = {
        resource.resource_id: resource for resource in showcase.resources if resource.resource_type == "kafka_cluster"
    }

    assert source_metrics
    assert all(isinstance(row, MetricRow) for row in source_metrics)
    assert {row.metric_key for row in source_metrics} == _METRIC_KEYS
    assert all(row.value >= 0 and row.value != float("inf") and row.value != float("-inf") for row in source_metrics)

    final_90 = {showcase.anchor_date - timedelta(days=offset) for offset in range(90)}
    rows_by_topic_day: defaultdict[tuple[str, date], dict[str, MetricRow]] = defaultdict(dict)
    actual_topics_by_date: defaultdict[date, set[str]] = defaultdict(set)
    actual_topics_by_environment: defaultdict[tuple[date, str], set[str]] = defaultdict(set)
    actual_topics_by_cluster: defaultdict[tuple[date, str], set[str]] = defaultdict(set)
    actual_tenant_topic_counts: defaultdict[date, set[int]] = defaultdict(set)
    actual_tenant_principal_counts: defaultdict[date, set[int]] = defaultdict(set)
    actual_environment_topic_counts: defaultdict[tuple[date, str], set[int]] = defaultdict(set)
    actual_environment_principal_counts: defaultdict[tuple[date, str], set[int]] = defaultdict(set)
    actual_cluster_topic_counts: defaultdict[tuple[date, str], set[int]] = defaultdict(set)
    actual_cluster_principal_counts: defaultdict[tuple[date, str], set[int]] = defaultdict(set)
    for row in source_metrics:
        topic_name = _topic_name(row, topic_names)
        topic = topic_by_name[topic_name]
        cluster = cluster_by_id[str(topic.parent_id)]
        assert showcase.tenant_id in row.labels.values()
        assert topic.parent_id in row.labels.values()
        assert cluster.parent_id in row.labels.values()
        assert topic_name in row.labels.values()
        assert _is_active_at(topic, row.timestamp)
        rows_by_topic_day[(topic_name, row.timestamp.date())][row.metric_key] = row
        tracking_date = row.timestamp.date()
        environment_id = str(cluster.parent_id)
        cluster_id = cluster.resource_id
        actual_topics_by_date[tracking_date].add(topic_name)
        actual_topics_by_environment[(tracking_date, environment_id)].add(topic_name)
        actual_topics_by_cluster[(tracking_date, cluster_id)].add(topic_name)
        actual_tenant_topic_counts[tracking_date].add(int(row.labels["tenant_active_topic_count"]))
        actual_tenant_principal_counts[tracking_date].add(int(row.labels["tenant_active_principal_count"]))
        actual_environment_topic_counts[(tracking_date, environment_id)].add(
            int(row.labels["environment_active_topic_count"])
        )
        actual_environment_principal_counts[(tracking_date, environment_id)].add(
            int(row.labels["environment_active_principal_count"])
        )
        actual_cluster_topic_counts[(tracking_date, cluster_id)].add(int(row.labels["cluster_active_topic_count"]))
        actual_cluster_principal_counts[(tracking_date, cluster_id)].add(
            int(row.labels["cluster_active_principal_count"])
        )

    for topic in topic_resources:
        for day in final_90:
            expected_keys = (
                _METRIC_KEYS if _is_active_at(topic, datetime.combine(day, datetime.min.time(), tzinfo=UTC)) else set()
            )
            actual_keys = set(rows_by_topic_day.get((str(topic.display_name), day), {}))
            assert actual_keys == expected_keys

    expected_scope_counts_by_date = {day: _active_scope_counts(showcase, day) for day in final_90}
    for window in (30, 60, 90):
        window_days = {showcase.anchor_date - timedelta(days=offset) for offset in range(window)}
        for day in window_days:
            (expected_topic_count, expected_principal_count), expected_environments, expected_clusters = (
                expected_scope_counts_by_date[day]
            )
            assert len(actual_topics_by_date[day]) == expected_topic_count
            assert actual_tenant_topic_counts[day] == {expected_topic_count}
            assert actual_tenant_principal_counts[day] == {expected_principal_count}
            for environment_id, (topic_count, principal_count) in expected_environments.items():
                assert len(actual_topics_by_environment[(day, environment_id)]) == topic_count
                assert actual_environment_topic_counts[(day, environment_id)] == {topic_count}
                assert actual_environment_principal_counts[(day, environment_id)] == {principal_count}
            for cluster_id, (topic_count, principal_count) in expected_clusters.items():
                assert len(actual_topics_by_cluster[(day, cluster_id)]) == topic_count
                assert actual_cluster_topic_counts[(day, cluster_id)] == {topic_count}
                assert actual_cluster_principal_counts[(day, cluster_id)] == {principal_count}

        by_cluster: defaultdict[str, defaultdict[str, float]] = defaultdict(lambda: defaultdict(float))
        by_environment: defaultdict[str, defaultdict[str, float]] = defaultdict(lambda: defaultdict(float))
        tenant_totals: defaultdict[str, float] = defaultdict(float)
        for (topic_name, day), rows in rows_by_topic_day.items():
            if day not in window_days:
                continue
            topic = topic_by_name[topic_name]
            cluster_id = str(topic.parent_id)
            environment_id = str(cluster_by_id[cluster_id].parent_id)
            for metric_key, row in rows.items():
                by_cluster[cluster_id][metric_key] += row.value
                by_environment[environment_id][metric_key] += row.value
                tenant_totals[metric_key] += row.value
        for metric_key in _METRIC_KEYS:
            assert tenant_totals[metric_key] > 0
            assert sum(values[metric_key] for values in by_cluster.values()) == pytest.approx(tenant_totals[metric_key])
            assert sum(values[metric_key] for values in by_environment.values()) == pytest.approx(
                tenant_totals[metric_key]
            )

    final_seven = tuple(showcase.anchor_date - timedelta(days=offset) for offset in range(6, -1, -1))
    active_final_seven_topics = {
        topic_name
        for topic_name, topic in topic_by_name.items()
        if all(_is_active_at(topic, datetime.combine(day, datetime.min.time(), tzinfo=UTC)) for day in final_seven)
    }
    assert active_final_seven_topics
    low_throughput_topics: set[str] = set()
    idle_topics: set[str] = set()
    healthy_topics: set[str] = set()
    for topic_name in active_final_seven_topics:
        daily_rows = [rows_by_topic_day[(topic_name, day)] for day in final_seven]
        rates = [
            (rows["received_bytes"].value + rows["sent_bytes"].value) / 86_400 / rows["partition_count"].value
            for rows in daily_rows
        ]
        traffic = [rows["received_bytes"].value + rows["sent_bytes"].value for rows in daily_rows]
        if all(value > 0 for value in traffic) and all(rate < 1_024 for rate in rates):
            low_throughput_topics.add(topic_name)
        if all(value == 0 for value in traffic):
            idle_topics.add(topic_name)
        if all(rate >= 1_024 for rate in rates):
            healthy_topics.add(topic_name)

    assert low_throughput_topics
    assert idle_topics
    assert healthy_topics
    assert low_throughput_topics.isdisjoint(idle_topics)
    assert low_throughput_topics.isdisjoint(healthy_topics)
