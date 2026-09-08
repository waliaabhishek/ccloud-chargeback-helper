from __future__ import annotations

from collections import Counter, defaultdict
from datetime import date, datetime, timedelta
from decimal import ROUND_HALF_UP, Decimal
from typing import TYPE_CHECKING

from plugins.self_managed_kafka.demo.scenario import (
    build_clean_self_managed_kafka_scenario,
    validate_clean_self_managed_kafka_scenario,
)

if TYPE_CHECKING:
    from plugins.self_managed_kafka.demo.scenario import CleanSelfManagedKafkaScenario

_ANCHOR = date(2026, 9, 2)
_TENANT_ID = "northstar-self-managed"
_EXPECTED_START = date(2026, 3, 3)
_EXPECTED_PRODUCTS = {
    "SELF_KAFKA_COMPUTE",
    "SELF_KAFKA_STORAGE",
    "SELF_KAFKA_NETWORK_INGRESS",
    "SELF_KAFKA_NETWORK_EGRESS",
}


def _scenario() -> CleanSelfManagedKafkaScenario:
    return build_clean_self_managed_kafka_scenario(tenant_id=_TENANT_ID, anchor_date=_ANCHOR)


def test_clean_self_managed_scenario_is_deterministic_and_has_the_exact_approved_topology() -> None:
    first = _scenario()
    second = _scenario()
    resource_counts = Counter(resource.resource_type for resource in first.resources)
    clusters = [resource for resource in first.resources if resource.resource_type == "cluster"]
    topics = [resource for resource in first.resources if resource.resource_type == "topic"]

    assert first == second
    assert first.tenant_id == _TENANT_ID
    assert first.anchor_date == _ANCHOR
    assert first.start_date == _EXPECTED_START
    assert resource_counts == {"cluster": 2, "topic": 24}
    assert len(first.identities) == 10
    assert {identity.identity_type for identity in first.identities} == {"principal"}
    assert all(cluster.parent_id is None for cluster in clusters)
    assert Counter(topic.parent_id for topic in topics) == {cluster.resource_id: 12 for cluster in clusters}
    assert {topic.resource_id for topic in topics} == {
        f"{topic.parent_id}:topic:{topic.display_name}" for topic in topics
    }
    assert all(not resource.display_name.startswith(("topic-", "cluster-")) for resource in first.resources)


def test_clean_self_managed_scenario_has_complete_ownership_sparse_reconciled_costs_and_topic_attribution() -> None:
    scenario = _scenario()
    entity_ids = {("resource", resource.resource_id) for resource in scenario.resources} | {
        ("identity", identity.identity_id) for identity in scenario.identities
    }
    tags_by_entity = {(tag.entity_type, tag.entity_id): tag for tag in scenario.entity_tags}
    billing_totals: dict[tuple[datetime, str | None, str, str], Decimal] = {}
    allocation_totals: defaultdict[tuple[datetime, str | None, str, str], Decimal] = defaultdict(Decimal)
    topic_totals: defaultdict[tuple[datetime, str, str, str], Decimal] = defaultdict(Decimal)
    identity_ids = {identity.identity_id for identity in scenario.identities}

    assert set(tags_by_entity) == entity_ids
    assert {tag.tag_key for tag in scenario.entity_tags} == {"team"}
    assert len({tag.tag_value for tag in scenario.entity_tags}) == 4
    assert all(tag.created_by == "demo-generator" for tag in scenario.entity_tags)
    assert {line.product_type for line in scenario.billing_lines} == _EXPECTED_PRODUCTS
    assert {line.product_category for line in scenario.billing_lines} == {"kafka"}

    for line in scenario.billing_lines:
        billing_totals[(line.timestamp, line.resource_id, line.product_category, line.product_type)] = line.total_cost
        assert line.quantity > 0
        assert line.total_cost > 0
    for row in scenario.chargebacks:
        allocation_totals[(row.timestamp, row.resource_id, row.product_category, row.product_type)] += row.amount
        assert row.identity_id in identity_ids
        assert row.identity_id != "UNALLOCATED"
        assert row.amount > 0
        assert row.metadata in ({}, {"team": tags_by_entity[("identity", row.identity_id)].tag_value})
    for row in scenario.topic_attributions:
        topic_totals[(row.timestamp, row.cluster_resource_id, row.product_category, row.product_type)] += row.amount
        assert row.topic_name != "__UNATTRIBUTED__"
        assert row.amount > 0

    assert dict(allocation_totals) == billing_totals
    assert topic_totals == {
        (timestamp, resource_id, product_category, product_type): total
        for (timestamp, resource_id, product_category, product_type), total in billing_totals.items()
        if resource_id is not None
    }


def test_clean_self_managed_scenario_preserves_exact_cost_semantics_and_sparse_allocations() -> None:
    scenario = _scenario()
    expected_costs = {
        "SELF_KAFKA_COMPUTE": (Decimal("24.0"), Decimal("1.25")),
        "SELF_KAFKA_STORAGE": (Decimal("180.0"), Decimal("0.01")),
        "SELF_KAFKA_NETWORK_INGRESS": (Decimal("40.0"), Decimal("0.02")),
        "SELF_KAFKA_NETWORK_EGRESS": (Decimal("32.0"), Decimal("0.04")),
    }
    first_day_lines = [line for line in scenario.billing_lines if line.timestamp.date() == _EXPECTED_START]

    assert {(line.product_type, line.quantity, line.unit_price) for line in first_day_lines} == {
        (product_type, quantity, unit_price) for product_type, (quantity, unit_price) in expected_costs.items()
    }
    assert {line.product_type: line.unit_price for line in scenario.billing_lines} == {
        product_type: unit_price for product_type, (_quantity, unit_price) in expected_costs.items()
    }
    assert all(
        line.total_cost == (line.quantity * line.unit_price).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
        for line in scenario.billing_lines
    )

    allocation_counts = Counter(
        (row.timestamp, row.resource_id, row.product_category, row.product_type) for row in scenario.chargebacks
    )
    assert len(allocation_counts) == len(scenario.billing_lines)
    assert set(allocation_counts.values()) == {2}


def test_clean_self_managed_scenario_covers_each_day_with_healthy_topic_pipeline_state() -> None:
    scenario = _scenario()
    expected_dates = {
        scenario.start_date + timedelta(days=offset)
        for offset in range((scenario.anchor_date - scenario.start_date).days + 1)
    }

    assert {state.tracking_date for state in scenario.pipeline_states} == expected_dates
    assert all(state.has_usable_calculation for state in scenario.pipeline_states)
    assert all(state.topic_overlay_gathered for state in scenario.pipeline_states)
    assert all(state.topic_attribution_calculated for state in scenario.pipeline_states)
    assert not hasattr(scenario, "preview_source_capture")


def test_clean_self_managed_scenario_validator_accepts_the_constructed_clean_state() -> None:
    assert validate_clean_self_managed_kafka_scenario(_scenario()) is None
