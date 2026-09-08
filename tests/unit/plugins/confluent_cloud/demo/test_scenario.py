from __future__ import annotations

from collections import Counter, defaultdict
from dataclasses import replace
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from typing import TYPE_CHECKING

import pytest

from plugins.confluent_cloud.demo.scenario import (
    build_clean_demo_scenario,
    validate_clean_demo_scenario,
)
from plugins.confluent_cloud.models.billing import billing_natural_key

if TYPE_CHECKING:
    from core.models import ChargebackRow, CoreIdentity, CoreResource, EntityTag
    from plugins.confluent_cloud.demo.scenario import CleanDemoScenario
    from plugins.confluent_cloud.models.billing import CCloudBillingLineItem

_ANCHOR = date(2026, 9, 2)
_TENANT_ID = "northstar-confluent"
_EXPECTED_START = date(2026, 3, 3)
_EXPECTED_TOPIC_NAMES = {
    "orders.created.v1",
    "payments.authorized.v1",
    "customer.profile.v1",
    "inventory.reserved.v1",
    "fulfillment.shipped.v1",
    "logistics.tracking.v1",
}
_EXPECTED_TEAMS = {
    "orders",
    "payments",
    "fulfillment",
    "customer",
    "analytics",
    "platform",
    "security",
    "data",
}


def _scenario() -> CleanDemoScenario:
    return build_clean_demo_scenario(tenant_id=_TENANT_ID, anchor_date=_ANCHOR)


def _resources_of_type(scenario: CleanDemoScenario, resource_type: str) -> tuple[CoreResource, ...]:
    return tuple(resource for resource in scenario.resources if resource.resource_type == resource_type)


def _only_resource(scenario: CleanDemoScenario, resource_type: str) -> CoreResource:
    resources = _resources_of_type(scenario, resource_type)
    assert len(resources) == 1
    return resources[0]


def _identity_of_type(scenario: CleanDemoScenario, identity_type: str) -> CoreIdentity:
    identities = tuple(identity for identity in scenario.identities if identity.identity_type == identity_type)
    assert identities
    return identities[0]


def _with_resource(
    scenario: CleanDemoScenario,
    original: CoreResource,
    replacement: CoreResource,
) -> CleanDemoScenario:
    return replace(
        scenario,
        resources=tuple(replacement if resource is original else resource for resource in scenario.resources),
    )


def _with_identity(
    scenario: CleanDemoScenario,
    original: CoreIdentity,
    replacement: CoreIdentity,
) -> CleanDemoScenario:
    return replace(
        scenario,
        identities=tuple(replacement if identity is original else identity for identity in scenario.identities),
    )


def _with_tag(
    scenario: CleanDemoScenario,
    original: EntityTag,
    replacement: EntityTag,
) -> CleanDemoScenario:
    return replace(scenario, entity_tags=tuple(replacement if tag is original else tag for tag in scenario.entity_tags))


def _with_billing(
    scenario: CleanDemoScenario,
    original: CCloudBillingLineItem,
    replacement: CCloudBillingLineItem,
) -> CleanDemoScenario:
    return replace(
        scenario,
        billing_lines=tuple(line if line is not original else replacement for line in scenario.billing_lines),
    )


def _with_chargeback(
    scenario: CleanDemoScenario,
    original: ChargebackRow,
    replacement: ChargebackRow,
) -> CleanDemoScenario:
    return replace(
        scenario,
        chargebacks=tuple(row if row is not original else replacement for row in scenario.chargebacks),
    )


def _assert_invalid(scenario: CleanDemoScenario) -> None:
    with pytest.raises(ValueError):
        validate_clean_demo_scenario(scenario)


def _chargeback_billing_key(row: ChargebackRow) -> tuple[str, str, datetime, str, str, str, str]:
    return (
        row.ecosystem,
        row.tenant_id,
        row.timestamp,
        str(row.metadata["env_id"]),
        row.resource_id or "",
        row.product_type,
        row.product_category,
    )


def test_build_clean_demo_scenario_is_deterministic_logical_output() -> None:
    first = _scenario()
    second = _scenario()

    assert first == second
    assert first.tenant_id == _TENANT_ID
    assert first.anchor_date == _ANCHOR
    assert first.start_date == _EXPECTED_START

    assert all(tag.tag_id is None for tag in first.entity_tags)
    assert all(tag.created_at is None for tag in first.entity_tags)
    assert all(row.tags == {} for row in first.chargebacks)


@pytest.mark.parametrize(
    ("anchor_date", "expected_start"),
    [
        (date(2026, 9, 2), date(2026, 3, 3)),
        (date(2026, 8, 31), date(2026, 3, 1)),
        (date(2024, 8, 31), date(2024, 3, 1)),
        (date(2024, 2, 29), date(2023, 8, 30)),
        (date(2025, 2, 28), date(2024, 8, 29)),
    ],
)
def test_build_clean_demo_scenario_uses_six_calendar_month_window_with_clamping(
    anchor_date: date,
    expected_start: date,
) -> None:
    scenario = build_clean_demo_scenario(tenant_id=_TENANT_ID, anchor_date=anchor_date)

    assert scenario.anchor_date == anchor_date
    assert scenario.start_date == expected_start
    assert {line.timestamp.date() for line in scenario.billing_lines} == {
        expected_start + timedelta(days=offset) for offset in range((anchor_date - expected_start).days + 1)
    }


def test_clean_demo_scenario_contains_the_approved_confluent_topology_and_identity_kinds() -> None:
    scenario = _scenario()
    resource_counts = Counter(resource.resource_type for resource in scenario.resources)
    identity_counts = Counter(identity.identity_type for identity in scenario.identities)
    topics = _resources_of_type(scenario, "topic")

    assert resource_counts == {
        "organization": 1,
        "environment": 4,
        "kafka_cluster": 6,
        "topic": 120,
        "connector": 16,
        "schema_registry": 3,
        "ksqldb_cluster": 3,
        "flink_compute_pool": 2,
        "flink_statement": 3,
    }
    assert identity_counts == {
        "service_account": 8,
        "user": 8,
        "identity_provider": 2,
        "identity_pool": 2,
        "api_key": 12,
    }
    assert {topic.display_name for topic in topics} >= _EXPECTED_TOPIC_NAMES
    assert all(not topic.display_name.startswith("topic-") for topic in topics)
    assert {topic.resource_id for topic in topics} == {
        f"{topic.parent_id}:topic:{topic.display_name}" for topic in topics
    }
    assert Counter(topic.parent_id for topic in topics) == {
        "lkc-commerce": 14,
        "lkc-logistics": 30,
        "lkc-fulfillment": 28,
        "lkc-customer": 22,
        "lkc-platform": 18,
        "lkc-data": 8,
    }
    assert {cluster.resource_id: cluster.parent_id for cluster in _resources_of_type(scenario, "kafka_cluster")} == {
        "lkc-commerce": "env-commerce",
        "lkc-logistics": "env-logistics",
        "lkc-fulfillment": "env-fulfillment",
        "lkc-customer": "env-commerce",
        "lkc-platform": "env-analytics",
        "lkc-data": "env-logistics",
    }
    assert Counter(connector.parent_id for connector in _resources_of_type(scenario, "connector")) == {
        "lkc-commerce": 5,
        "lkc-logistics": 5,
        "lkc-fulfillment": 3,
        "lkc-customer": 1,
        "lkc-platform": 1,
        "lkc-data": 1,
    }
    assert Counter(registry.parent_id for registry in _resources_of_type(scenario, "schema_registry")) == {
        "env-commerce": 1,
        "env-logistics": 1,
        "env-fulfillment": 1,
    }
    assert {
        resource.resource_id: (resource.parent_id, resource.metadata["kafka_cluster_id"])
        for resource in _resources_of_type(scenario, "ksqldb_cluster")
    } == {
        "lksql-commerce": ("env-commerce", "lkc-commerce"),
        "lksql-logistics": ("env-logistics", "lkc-logistics"),
        "lksql-analytics": ("env-analytics", "lkc-platform"),
    }
    assert Counter(pool.parent_id for pool in _resources_of_type(scenario, "flink_compute_pool")) == {
        "env-logistics": 1,
        "env-analytics": 1,
    }
    assert Counter(statement.parent_id for statement in _resources_of_type(scenario, "flink_statement")) == {
        "env-logistics": 2,
        "env-analytics": 1,
    }
    assert all(resource.resource_id != scenario.tenant_id for resource in scenario.resources)


def test_clean_demo_scenario_contains_reconciled_service_network_and_shared_costs_with_healthy_pipeline_state() -> None:
    scenario = _scenario()
    dates = tuple(_EXPECTED_START + timedelta(days=offset) for offset in range(184))
    billing_per_day = Counter(line.timestamp.date() for line in scenario.billing_lines)
    pipeline_by_date = {state.tracking_date: state for state in scenario.pipeline_states}
    allocation_totals: defaultdict[tuple[str, str, datetime, str, str, str, str], Decimal] = defaultdict(Decimal)

    for row in scenario.chargebacks:
        allocation_totals[_chargeback_billing_key(row)] += row.amount

    assert tuple(billing_per_day) == dates
    assert all(billing_per_day[day] > 0 for day in dates)
    assert set(pipeline_by_date) == set(dates)
    assert len(pipeline_by_date) == len(dates)
    assert all(state.has_usable_calculation for state in pipeline_by_date.values())
    assert {"KAFKA", "CONNECT", "STREAM_GOVERNANCE", "KSQL", "FLINK"} <= {
        line.product_category for line in scenario.billing_lines
    }
    assert any("NETWORK" in line.product_type for line in scenario.billing_lines)
    assert all(line.total_cost.as_tuple().exponent >= -2 for line in scenario.billing_lines)
    assert all(
        row.allocation_detail in {"usage_ratio_allocation", "even_split_allocation"} for row in scenario.chargebacks
    )
    assert all("unallocated" not in row.identity_id.lower() for row in scenario.chargebacks)
    assert {billing_natural_key(line): line.total_cost for line in scenario.billing_lines} == dict(allocation_totals)
    assert {row.cost_type.value for row in scenario.chargebacks} == {"usage", "shared"}


def test_clean_demo_scenario_uses_utc_second_precision_timestamps() -> None:
    scenario = _scenario()
    timestamps = [
        timestamp
        for resource in scenario.resources
        for timestamp in (resource.created_at, resource.deleted_at, resource.last_seen_at)
        if timestamp is not None
    ]
    timestamps.extend(
        timestamp
        for identity in scenario.identities
        for timestamp in (identity.created_at, identity.deleted_at, identity.last_seen_at)
        if timestamp is not None
    )
    timestamps.extend(line.timestamp for line in scenario.billing_lines)
    timestamps.extend(row.timestamp for row in scenario.chargebacks)
    timestamps.extend(
        timestamp
        for state in scenario.pipeline_states
        for timestamp in (state.calculation_completed_at,)
        if timestamp is not None
    )

    assert timestamps
    assert all(timestamp.tzinfo is not None and timestamp.utcoffset() == timedelta(0) for timestamp in timestamps)
    assert all(timestamp.microsecond == 0 for timestamp in timestamps)


def test_clean_demo_scenario_assigns_one_complete_team_tag_to_every_persisted_resource_and_identity() -> None:
    scenario = _scenario()
    resource_ids = {resource.resource_id for resource in scenario.resources}
    identity_ids = {identity.identity_id for identity in scenario.identities}
    tag_keys = {(tag.entity_type, tag.entity_id, tag.tag_key) for tag in scenario.entity_tags}

    assert scenario.entity_tags
    assert all(tag.tenant_id == scenario.tenant_id for tag in scenario.entity_tags)
    assert {tag.entity_type for tag in scenario.entity_tags} == {"resource", "identity"}
    assert all(
        tag.entity_id in resource_ids if tag.entity_type == "resource" else tag.entity_id in identity_ids
        for tag in scenario.entity_tags
    )
    assert {tag.tag_key for tag in scenario.entity_tags} == {"team"}
    assert {tag.tag_value for tag in scenario.entity_tags} == _EXPECTED_TEAMS
    assert all(tag.created_by == "demo-generator" for tag in scenario.entity_tags)
    assert len(tag_keys) == len(scenario.entity_tags)
    tagged_entities = {(tag.entity_type, tag.entity_id) for tag in scenario.entity_tags}
    assert tagged_entities == {("resource", resource_id) for resource_id in resource_ids} | {
        ("identity", identity_id) for identity_id in identity_ids
    }


def test_clean_demo_scenario_contains_complete_topic_attribution_and_preview_evidence() -> None:
    scenario = _scenario()
    topic_totals: defaultdict[tuple[datetime, str, str, str, str], Decimal] = defaultdict(Decimal)
    billing_totals: dict[tuple[datetime, str, str, str, str], Decimal] = {}

    for row in scenario.topic_attributions:
        assert row.topic_name != "__UNATTRIBUTED__"
        assert row.amount > 0
        key = (row.timestamp, row.env_id, row.cluster_resource_id, row.product_category, row.product_type)
        topic_totals[key] += row.amount
    for line in scenario.billing_lines:
        key = (line.timestamp, line.env_id, line.resource_id, line.product_category, line.product_type)
        if key in topic_totals:
            billing_totals[key] = line.total_cost

    assert billing_totals == dict(topic_totals)
    assert scenario.preview_source_capture.records
    assert scenario.preview_source_capture.refresh_start <= scenario.preview_source_capture.refresh_end
    assert len(scenario.allocation_lineage_runs) == len(scenario.pipeline_states)
    assert {run.calculation_id for run in scenario.allocation_lineage_runs} == {
        state.calculation_id for state in scenario.pipeline_states
    }
    organization = _only_resource(scenario, "organization")
    assert scenario.organization_authority_id == organization.resource_id
    assert organization.metadata["organization_binding_state"] == "bound"


def test_validator_rejects_nonunique_or_tenant_colliding_organizations() -> None:
    scenario = _scenario()
    organization = _only_resource(scenario, "organization")

    _assert_invalid(_with_resource(scenario, organization, replace(organization, resource_id=scenario.tenant_id)))
    _assert_invalid(
        _with_resource(scenario, organization, replace(organization, resource_id="organization-not-a-uuid"))
    )
    duplicate = replace(organization, resource_id="2b87fa80-f233-4af0-b3d1-7adc3b77e9b6")
    _assert_invalid(replace(scenario, resources=(*scenario.resources, duplicate)))


def test_validator_rejects_environment_with_fabricated_organization_parent() -> None:
    scenario = _scenario()
    environment = _resources_of_type(scenario, "environment")[0]
    organization = _only_resource(scenario, "organization")

    _assert_invalid(_with_resource(scenario, environment, replace(environment, parent_id=organization.resource_id)))


def test_validator_rejects_kafka_cluster_without_a_real_environment_or_placement() -> None:
    scenario = _scenario()
    cluster = _resources_of_type(scenario, "kafka_cluster")[0]

    _assert_invalid(_with_resource(scenario, cluster, replace(cluster, parent_id="env-missing")))
    _assert_invalid(_with_resource(scenario, cluster, replace(cluster, metadata={**cluster.metadata, "cloud": ""})))
    _assert_invalid(_with_resource(scenario, cluster, replace(cluster, metadata={**cluster.metadata, "region": ""})))


def test_validator_rejects_topics_with_invalid_storage_ids_or_duplicate_cluster_names() -> None:
    scenario = _scenario()
    topics_by_cluster: defaultdict[str | None, list[CoreResource]] = defaultdict(list)
    for topic in _resources_of_type(scenario, "topic"):
        topics_by_cluster[topic.parent_id].append(topic)
    first, second = next(topics for topics in topics_by_cluster.values() if len(topics) >= 2)[:2]

    _assert_invalid(_with_resource(scenario, first, replace(first, resource_id="topic-not-a-storage-id")))
    duplicate = replace(
        second,
        resource_id=f"{second.parent_id}:topic:{first.display_name}",
        display_name=first.display_name,
    )
    _assert_invalid(_with_resource(scenario, second, duplicate))


def test_validator_rejects_connectors_with_inconsistent_parent_environment_prefix_or_authentication() -> None:
    scenario = _scenario()
    connector = next(
        connector
        for connector in _resources_of_type(scenario, "connector")
        if connector.metadata["connector_kind"] == "managed"
    )
    custom_connector = next(
        connector
        for connector in _resources_of_type(scenario, "connector")
        if connector.metadata["connector_kind"] == "custom"
    )

    _assert_invalid(_with_resource(scenario, connector, replace(connector, parent_id="lkc-missing")))
    _assert_invalid(
        _with_resource(
            scenario,
            connector,
            replace(connector, metadata={**connector.metadata, "env_id": "env-missing"}),
        )
    )
    _assert_invalid(_with_resource(scenario, connector, replace(connector, resource_id="connector-missing-prefix")))
    _assert_invalid(
        _with_resource(
            scenario,
            custom_connector,
            replace(custom_connector, resource_id=custom_connector.resource_id.replace("clcc-", "lcc-", 1)),
        )
    )
    _assert_invalid(
        _with_resource(
            scenario,
            connector,
            replace(connector, resource_id=connector.resource_id.replace("lcc-", "clcc-", 1)),
        )
    )
    _assert_invalid(
        _with_resource(
            scenario,
            connector,
            replace(
                connector,
                metadata={
                    **connector.metadata,
                    "kafka_auth_mode": "SERVICE_ACCOUNT",
                    "kafka_service_account_id": "sa-missing",
                },
            ),
        )
    )


def test_validator_rejects_schema_registries_with_duplicate_or_inconsistent_environment_placement() -> None:
    scenario = _scenario()
    registry = _resources_of_type(scenario, "schema_registry")[0]

    _assert_invalid(replace(scenario, resources=(*scenario.resources, replace(registry, resource_id="lsrc-duplicate"))))
    _assert_invalid(_with_resource(scenario, registry, replace(registry, parent_id="env-missing")))
    _assert_invalid(
        _with_resource(scenario, registry, replace(registry, metadata={**registry.metadata, "cloud": "invalid"}))
    )
    _assert_invalid(
        _with_resource(scenario, registry, replace(registry, metadata={**registry.metadata, "crn": "crn://invalid"}))
    )


def test_validator_rejects_ksqldb_without_a_matching_environment_kafka_association_and_owner() -> None:
    scenario = _scenario()
    ksqldb = _resources_of_type(scenario, "ksqldb_cluster")[0]
    api_key = _identity_of_type(scenario, "api_key")

    _assert_invalid(_with_resource(scenario, ksqldb, replace(ksqldb, parent_id="env-missing")))
    _assert_invalid(
        _with_resource(
            scenario,
            ksqldb,
            replace(ksqldb, metadata={**ksqldb.metadata, "kafka_cluster_id": "lkc-missing"}),
        )
    )
    _assert_invalid(_with_resource(scenario, ksqldb, replace(ksqldb, metadata={**ksqldb.metadata, "cloud": "aws"})))
    _assert_invalid(
        _with_resource(scenario, ksqldb, replace(ksqldb, metadata={**ksqldb.metadata, "region": "us-east-1"}))
    )
    _assert_invalid(_with_resource(scenario, ksqldb, replace(ksqldb, owner_id="sa-missing")))
    _assert_invalid(_with_resource(scenario, ksqldb, replace(ksqldb, owner_id=api_key.identity_id)))


def test_validator_rejects_flink_pool_without_matching_environment_cloud_region_or_crn() -> None:
    scenario = _scenario()
    pool = _resources_of_type(scenario, "flink_compute_pool")[0]

    _assert_invalid(_with_resource(scenario, pool, replace(pool, parent_id="env-missing")))
    _assert_invalid(_with_resource(scenario, pool, replace(pool, metadata={**pool.metadata, "cloud": ""})))
    _assert_invalid(_with_resource(scenario, pool, replace(pool, metadata={**pool.metadata, "region": ""})))
    _assert_invalid(_with_resource(scenario, pool, replace(pool, metadata={**pool.metadata, "cloud": "aws"})))
    _assert_invalid(_with_resource(scenario, pool, replace(pool, metadata={**pool.metadata, "region": "us-east-1"})))
    _assert_invalid(
        _with_resource(
            scenario,
            pool,
            replace(
                pool, metadata={**pool.metadata, "crn": str(pool.metadata["crn"]).replace("cloud=gcp", "cloud=aws")}
            ),
        )
    )
    _assert_invalid(
        _with_resource(
            scenario,
            pool,
            replace(
                pool,
                metadata={
                    **pool.metadata,
                    "crn": str(pool.metadata["crn"]).replace("region=us-central1", "region=us-east-1"),
                },
            ),
        )
    )
    _assert_invalid(_with_resource(scenario, pool, replace(pool, metadata={**pool.metadata, "crn": "crn://invalid"})))


def test_validator_rejects_flink_statement_without_environment_pool_or_owner_relationships() -> None:
    scenario = _scenario()
    statement = _resources_of_type(scenario, "flink_statement")[0]
    pool = next(
        resource
        for resource in _resources_of_type(scenario, "flink_compute_pool")
        if resource.resource_id == statement.metadata["compute_pool_id"]
    )
    api_key = _identity_of_type(scenario, "api_key")

    _assert_invalid(_with_resource(scenario, statement, replace(statement, parent_id=pool.resource_id)))
    _assert_invalid(
        _with_resource(
            scenario,
            statement,
            replace(statement, metadata={**statement.metadata, "compute_pool_id": "lfcp-missing"}),
        )
    )
    _assert_invalid(_with_resource(scenario, statement, replace(statement, owner_id="sa-missing")))
    _assert_invalid(_with_resource(scenario, statement, replace(statement, owner_id=api_key.identity_id)))


def test_validator_rejects_identity_provider_pool_api_key_reference_and_secret_violations() -> None:
    scenario = _scenario()
    pool = _identity_of_type(scenario, "identity_pool")
    api_key = _identity_of_type(scenario, "api_key")

    _assert_invalid(
        _with_identity(scenario, pool, replace(pool, metadata={**pool.metadata, "provider_id": "ip-missing"}))
    )
    _assert_invalid(
        _with_identity(scenario, api_key, replace(api_key, metadata={**api_key.metadata, "owner_id": "sa-missing"}))
    )
    _assert_invalid(
        _with_identity(
            scenario,
            api_key,
            replace(api_key, metadata={**api_key.metadata, "resource_id": "scope-missing"}),
        )
    )
    _assert_invalid(
        _with_identity(
            scenario,
            api_key,
            replace(
                api_key,
                metadata={**api_key.metadata, "api_secret": "forbidden"},  # pragma: allowlist secret
            ),
        )
    )


def test_validator_rejects_billing_with_an_environment_incompatible_with_its_resource() -> None:
    scenario = _scenario()
    billing = next(line for line in scenario.billing_lines if line.resource_id)
    resource = next(resource for resource in scenario.resources if resource.resource_id == billing.resource_id)
    other_environment = next(
        environment
        for environment in _resources_of_type(scenario, "environment")
        if environment.resource_id != resource.parent_id
    )

    _assert_invalid(_with_billing(scenario, billing, replace(billing, env_id=other_environment.resource_id)))


def test_validator_rejects_chargebacks_to_missing_or_nonallocatable_targets() -> None:
    scenario = _scenario()
    chargeback = scenario.chargebacks[0]
    api_key = _identity_of_type(scenario, "api_key")

    _assert_invalid(_with_chargeback(scenario, chargeback, replace(chargeback, identity_id="target-missing")))
    _assert_invalid(_with_chargeback(scenario, chargeback, replace(chargeback, identity_id=api_key.identity_id)))


def test_validator_rejects_chargebacks_that_do_not_reconcile_to_the_billing_line() -> None:
    scenario = _scenario()
    chargeback = scenario.chargebacks[0]

    _assert_invalid(
        _with_chargeback(scenario, chargeback, replace(chargeback, amount=chargeback.amount + Decimal("0.01")))
    )


def test_validator_rejects_resources_and_identities_outside_the_billed_lifetime() -> None:
    scenario = _scenario()
    billing = next(line for line in scenario.billing_lines if line.resource_id)
    resource = next(resource for resource in scenario.resources if resource.resource_id == billing.resource_id)
    billed_after_lifetime = datetime.combine(_ANCHOR + timedelta(days=1), datetime.min.time(), tzinfo=UTC)

    _assert_invalid(_with_resource(scenario, resource, replace(resource, created_at=billed_after_lifetime)))


def test_validator_rejects_entity_tags_with_invalid_assignment_fields() -> None:
    scenario = _scenario()
    tag = scenario.entity_tags[0]

    _assert_invalid(_with_tag(scenario, tag, replace(tag, tenant_id="other-tenant")))
    _assert_invalid(_with_tag(scenario, tag, replace(tag, entity_type="environment")))
    _assert_invalid(_with_tag(scenario, tag, replace(tag, entity_id="entity-missing")))
    _assert_invalid(_with_tag(scenario, tag, replace(tag, tag_key="cost_center")))
    _assert_invalid(_with_tag(scenario, tag, replace(tag, tag_value="not-an-approved-team")))
    _assert_invalid(_with_tag(scenario, tag, replace(tag, created_by="someone-else")))
    duplicate = replace(tag, tag_value="different-team")
    _assert_invalid(replace(scenario, entity_tags=(*scenario.entity_tags, duplicate)))


def test_validator_rejects_missing_team_coverage_and_chargeback_target_tags() -> None:
    scenario = _scenario()
    missing_team = scenario.entity_tags[0].tag_value
    no_team_coverage = replace(
        scenario,
        entity_tags=tuple(tag for tag in scenario.entity_tags if tag.tag_value != missing_team),
    )
    tagged_target = next(
        tag
        for tag in scenario.entity_tags
        if tag.entity_type == "identity" and any(row.identity_id == tag.entity_id for row in scenario.chargebacks)
    )
    missing_target_tag = replace(
        scenario,
        entity_tags=tuple(
            tag
            for tag in scenario.entity_tags
            if not (tag.entity_type == "identity" and tag.entity_id == tagged_target.entity_id)
        ),
    )

    _assert_invalid(no_team_coverage)
    _assert_invalid(missing_target_tag)
