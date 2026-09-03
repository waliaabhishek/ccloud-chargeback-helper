from __future__ import annotations

import calendar
import math
from collections import Counter, defaultdict
from dataclasses import dataclass, replace
from datetime import UTC, date, datetime, timedelta
from decimal import ROUND_HALF_UP, Decimal
from typing import Any, NoReturn
from uuid import UUID

from core.engine.allocation_lineage import build_allocation_lineage_capture
from core.engine.topic_attribution_models import TopicAttributionRowOutputContext, build_reconciled_topic_rows
from core.models import (
    ChargebackRow,
    CoreIdentity,
    CoreResource,
    CostType,
    EntityTag,
    MetricRow,
    PipelineState,
    ResourceStatus,
)
from core.models.topic_attribution import TopicAttributionRow
from core.preview.evidence_capture import NativeSourceWindow
from core.storage.interface import AllocationLineageRunCapture
from plugins.confluent_cloud.crn import parse_ccloud_crn
from plugins.confluent_cloud.models.billing import CCloudBillingLineItem, CCloudCostSourceRecord
from plugins.confluent_cloud.source_capture import CCloudNativeSourceEvidenceCapture

ECOSYSTEM = "confluent_cloud"
ORGANIZATION_ID = "11111111-1111-4111-8111-111111111111"
TEAM_VALUES: tuple[str, ...] = (
    "orders",
    "payments",
    "fulfillment",
    "customer",
    "analytics",
    "platform",
    "security",
    "data",
)

_ENVIRONMENTS: tuple[tuple[str, str], ...] = (
    ("env-commerce", "Commerce"),
    ("env-logistics", "Logistics"),
    ("env-fulfillment", "Fulfillment"),
    ("env-analytics", "Analytics"),
)
_CLUSTERS: tuple[tuple[str, str, str, str, str], ...] = (
    ("lkc-commerce", "env-commerce", "Commerce Kafka", "aws", "us-east-1"),
    ("lkc-logistics", "env-logistics", "Logistics Kafka", "gcp", "us-central1"),
    ("lkc-fulfillment", "env-fulfillment", "Fulfillment Kafka", "aws", "us-west-2"),
    ("lkc-customer", "env-commerce", "Customer Kafka", "aws", "us-east-1"),
    ("lkc-platform", "env-analytics", "Platform Kafka", "azure", "eastus2"),
    ("lkc-data", "env-logistics", "Data Kafka", "gcp", "us-west1"),
)
_TOPIC_COUNTS: tuple[int, ...] = (14, 30, 28, 22, 18, 8)
_TOPIC_SEEDS: tuple[tuple[str, ...], ...] = (
    (
        "orders.created.v1",
        "payments.authorized.v1",
        "customer.profile.v1",
        "orders.updated.v1",
        "payments.refunded.v1",
        "customer.preferences.v1",
    ),
    (
        "inventory.reserved.v1",
        "fulfillment.shipped.v1",
        "logistics.tracking.v1",
        "inventory.replenished.v1",
        "fulfillment.delayed.v1",
        "logistics.route.v1",
    ),
    (
        "fulfillment.picked.v1",
        "warehouse.received.v1",
        "delivery.dispatched.v1",
        "fulfillment.capacity.v1",
    ),
    (
        "customer.created.v1",
        "customer.updated.v1",
        "customer.consents.v1",
    ),
    (
        "analytics.events.v1",
        "analytics.aggregates.v1",
        "platform.usage.v1",
    ),
    (
        "data.quality.v1",
        "data.catalog.v1",
        "data.retention.v1",
    ),
)


def _topic_names_for_cluster(seed: tuple[str, ...], count: int) -> tuple[str, ...]:
    names = list(seed)
    prefix = seed[0].split(".")[0]
    while len(names) < count:
        names.append(f"{prefix}.events.{len(names) + 1:02d}.v1")
    return tuple(names[:count])


_TOPIC_NAMES: tuple[tuple[str, tuple[str, ...]], ...] = tuple(
    (cluster[0], _topic_names_for_cluster(seed, count))
    for cluster, seed, count in zip(_CLUSTERS, _TOPIC_SEEDS, _TOPIC_COUNTS, strict=True)
)
_CONNECTORS: tuple[tuple[str, str, str, str, str], ...] = (
    (
        "lcc-commerce-orders",
        "lkc-commerce",
        "env-commerce",
        "orders-jdbc-sink",
        "managed",
    ),
    (
        "clcc-logistics-shipping",
        "lkc-logistics",
        "env-logistics",
        "shipping-custom-sink",
        "custom",
    ),
    ("lcc-commerce-payments", "lkc-commerce", "env-commerce", "payments-sink", "managed"),
    ("lcc-commerce-customer", "lkc-commerce", "env-commerce", "customer-profile-sink", "managed"),
    ("clcc-commerce-analytics", "lkc-commerce", "env-commerce", "commerce-custom-source", "custom"),
    ("lcc-commerce-orders-archive", "lkc-commerce", "env-commerce", "orders-archive-sink", "managed"),
    ("lcc-logistics-inventory", "lkc-logistics", "env-logistics", "inventory-sink", "managed"),
    ("clcc-logistics-routing", "lkc-logistics", "env-logistics", "routing-custom-source", "custom"),
    ("lcc-logistics-archive", "lkc-logistics", "env-logistics", "logistics-archive-sink", "managed"),
    ("clcc-logistics-events", "lkc-logistics", "env-logistics", "events-custom-sink", "custom"),
    ("lcc-fulfillment-warehouse", "lkc-fulfillment", "env-fulfillment", "warehouse-sink", "managed"),
    ("clcc-fulfillment-delivery", "lkc-fulfillment", "env-fulfillment", "delivery-custom-sink", "custom"),
    ("lcc-fulfillment-returns", "lkc-fulfillment", "env-fulfillment", "returns-sink", "managed"),
    ("clcc-customer-profile", "lkc-customer", "env-commerce", "profile-custom-sink", "custom"),
    ("clcc-platform-telemetry", "lkc-platform", "env-analytics", "platform-custom-sink", "custom"),
    ("lcc-data-quality", "lkc-data", "env-logistics", "data-quality-sink", "managed"),
)
_SCHEMA_REGISTRIES: tuple[tuple[str, str, str, str, str], ...] = (
    ("lsrc-commerce", "env-commerce", "Commerce Schema Registry", "aws", "us-east-1"),
    ("lsrc-logistics", "env-logistics", "Logistics Schema Registry", "gcp", "us-central1"),
    ("lsrc-fulfillment", "env-fulfillment", "Fulfillment Schema Registry", "aws", "us-west-2"),
)
_ALLOCATABLE_IDENTITIES: tuple[str, ...] = (
    "sa-commerce",
    "sa-logistics",
    "sa-platform",
    "sa-fulfillment",
    "sa-customer",
    "sa-analytics",
    "sa-security",
    "sa-data",
    "user-alice",
    "user-bob",
    "user-carol",
    "user-diego",
    "user-eve",
    "user-frank",
    "user-grace",
    "user-security",
    "pool-workload",
    "pool-analytics",
)
_RESOURCE_COUNTS: dict[str, int] = {
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
_IDENTITY_COUNTS: dict[str, int] = {
    "service_account": 8,
    "user": 8,
    "identity_provider": 2,
    "identity_pool": 2,
    "api_key": 12,
}
_BILLING_CATEGORIES = {
    "KAFKA",
    "CONNECT",
    "STREAM_GOVERNANCE",
    "KSQL",
    "FLINK",
    "AUDIT_LOG",
    "SUPPORT_CLOUD_BASIC",
}
_CENT = Decimal("0.01")
# Keep generated costs on binary-exact quarter-dollar boundaries after
# allocation.  The API's SQLite aggregation reads decimal strings through the
# numeric affinity of ``SUM``; binary-exact values preserve the exact Decimal
# response expected by the demo's reconciliation contract.
_QUANTITY_PRECISION = Decimal("0.1")
_SEASONALITY: tuple[Decimal, ...] = (
    Decimal("0.96"),
    Decimal("1.02"),
    Decimal("1.00"),
    Decimal("1.08"),
    Decimal("1.04"),
    Decimal("0.98"),
    Decimal("1.06"),
)
_SHOWCASE_LINE_TYPES: tuple[str, str] = ("KAFKA_PARTITION", "KAFKA_REST_PRODUCE")
_SHOWCASE_LIVE_TOPIC_ID = "lkc-customer:topic:showcase-live-orders"
_SHOWCASE_LEGACY_TOPIC_ID = "lkc-data:topic:showcase-legacy-orders"
_SHOWCASE_LIVE_IDENTITY_ID = "sa-showcase-live"
_SHOWCASE_LEGACY_IDENTITY_ID = "sa-showcase-legacy"
_SHOWCASE_TOPIC_NAMES = {
    _SHOWCASE_LIVE_TOPIC_ID: "showcase-live-orders",
    _SHOWCASE_LEGACY_TOPIC_ID: "showcase-legacy-orders",
}
_SHOWCASE_PRINCIPAL_TYPES = frozenset({"service_account", "user", "principal", "identity_pool"})
_SHOWCASE_SCOPE_LABELS = (
    "tenant_active_topic_count",
    "tenant_active_principal_count",
    "environment_active_topic_count",
    "environment_active_principal_count",
    "cluster_active_topic_count",
    "cluster_active_principal_count",
)
_SHOWCASE_METRIC_KEYS = ("received_bytes", "sent_bytes", "retained_bytes", "partition_count")
_SHOWCASE_VALIDATION_DAYS = 90


@dataclass(frozen=True)
class ConfluentDemoScenario:
    """Profile-neutral deterministic logical state for a Confluent demo."""

    tenant_id: str
    anchor_date: date
    start_date: date
    resources: tuple[CoreResource, ...]
    identities: tuple[CoreIdentity, ...]
    entity_tags: tuple[EntityTag, ...]
    billing_lines: tuple[CCloudBillingLineItem, ...]
    chargebacks: tuple[ChargebackRow, ...]
    pipeline_states: tuple[PipelineState, ...]
    topic_attributions: tuple[TopicAttributionRow, ...]
    preview_source_capture: CCloudNativeSourceEvidenceCapture
    allocation_lineage_runs: tuple[AllocationLineageRunCapture, ...]
    organization_authority_id: str
    organization_authority_at: datetime


@dataclass(frozen=True)
class CleanDemoScenario(ConfluentDemoScenario):
    """Pure, deterministic logical state for the Clean Confluent demo."""


@dataclass(frozen=True)
class ShowcaseDemoScenario(ConfluentDemoScenario):
    """Additive analytical source conditions layered on the Clean profile."""

    source_metrics: tuple[MetricRow, ...]


def _subtract_calendar_months(value: date, months: int) -> date:
    """Subtract calendar months while clamping the day to the target month."""
    month_index = value.year * 12 + value.month - 1 - months
    year, month_zero_based = divmod(month_index, 12)
    month = month_zero_based + 1
    day = min(value.day, calendar.monthrange(year, month)[1])
    return date(year, month, day)


def _window_start(anchor_date: date) -> date:
    return _subtract_calendar_months(anchor_date, 6) + timedelta(days=1)


def _at_midnight(day: date) -> datetime:
    return datetime.combine(day, datetime.min.time(), tzinfo=UTC)


def _resource(
    *,
    tenant_id: str,
    resource_id: str,
    resource_type: str,
    display_name: str,
    created_at: datetime,
    parent_id: str | None = None,
    owner_id: str | None = None,
    metadata: dict[str, Any] | None = None,
) -> CoreResource:
    return CoreResource(
        ecosystem=ECOSYSTEM,
        tenant_id=tenant_id,
        resource_id=resource_id,
        resource_type=resource_type,
        display_name=display_name,
        parent_id=parent_id,
        owner_id=owner_id,
        status=ResourceStatus.ACTIVE,
        created_at=created_at,
        deleted_at=None,
        last_seen_at=created_at,
        metadata={} if metadata is None else metadata,
    )


def _identity(
    *,
    tenant_id: str,
    identity_id: str,
    identity_type: str,
    display_name: str,
    created_at: datetime,
    metadata: dict[str, Any] | None = None,
) -> CoreIdentity:
    return CoreIdentity(
        ecosystem=ECOSYSTEM,
        tenant_id=tenant_id,
        identity_id=identity_id,
        identity_type=identity_type,
        display_name=display_name,
        created_at=created_at,
        deleted_at=None,
        last_seen_at=created_at,
        metadata={} if metadata is None else metadata,
    )


def _ccloud_crn(
    organization_id: str,
    environment_id: str,
    resource_kind: str,
    resource_id: str,
    *,
    cloud: str | None = None,
    region: str | None = None,
) -> str:
    segments = [
        f"organization={organization_id}",
        f"environment={environment_id}",
    ]
    if cloud is not None:
        segments.append(f"cloud={cloud}")
    if region is not None:
        segments.append(f"region={region}")
    segments.append(f"{resource_kind}={resource_id}")
    return f"crn://confluent.cloud/{'/'.join(segments)}"


def _build_resources(tenant_id: str, created_at: datetime) -> tuple[CoreResource, ...]:
    """Build the exact-density synthetic Confluent resource topology."""
    resources: list[CoreResource] = [
        _resource(
            tenant_id=tenant_id,
            resource_id=ORGANIZATION_ID,
            resource_type="organization",
            display_name="Northstar Confluent Cloud",
            created_at=created_at,
            metadata={"organization_binding_state": "bound"},
        )
    ]
    resources.extend(
        _resource(
            tenant_id=tenant_id,
            resource_id=environment_id,
            resource_type="environment",
            display_name=display_name,
            created_at=created_at,
        )
        for environment_id, display_name in _ENVIRONMENTS
    )
    topics_by_cluster = dict(_TOPIC_NAMES)
    for cluster_id, environment_id, display_name, cloud, region in _CLUSTERS:
        resources.append(
            _resource(
                tenant_id=tenant_id,
                resource_id=cluster_id,
                resource_type="kafka_cluster",
                display_name=display_name,
                parent_id=environment_id,
                created_at=created_at,
                metadata={
                    "bootstrap_url": f"SASL_SSL://{cluster_id}.{region}.{cloud}.confluent.cloud:9092",
                    "cloud": cloud,
                    "region": region,
                    "provider_cloud": cloud.upper(),
                    "provider_region": region,
                },
            )
        )
        resources.extend(
            _resource(
                tenant_id=tenant_id,
                resource_id=f"{cluster_id}:topic:{topic_name}",
                resource_type="topic",
                display_name=topic_name,
                parent_id=cluster_id,
                created_at=created_at,
            )
            for topic_name in topics_by_cluster[cluster_id]
        )

    service_account_by_environment = {
        "env-commerce": "sa-commerce",
        "env-logistics": "sa-logistics",
        "env-fulfillment": "sa-fulfillment",
        "env-analytics": "sa-analytics",
    }
    for connector_id, cluster_id, environment_id, display_name, connector_kind in _CONNECTORS:
        resources.append(
            _resource(
                tenant_id=tenant_id,
                resource_id=connector_id,
                resource_type="connector",
                display_name=display_name,
                parent_id=cluster_id,
                created_at=created_at,
                metadata={
                    "connector_class": (
                        "io.confluent.connect.jdbc.JdbcSinkConnector"
                        if connector_kind == "managed"
                        else "com.northstar.connect.SyntheticConnector"
                    ),
                    "connector_kind": connector_kind,
                    "env_id": environment_id,
                    "kafka_auth_mode": "SERVICE_ACCOUNT",
                    "kafka_service_account_id": service_account_by_environment[environment_id],
                },
            )
        )

    for registry_id, environment_id, display_name, cloud, region in _SCHEMA_REGISTRIES:
        resources.append(
            _resource(
                tenant_id=tenant_id,
                resource_id=registry_id,
                resource_type="schema_registry",
                display_name=display_name,
                parent_id=environment_id,
                created_at=created_at,
                metadata={
                    "http_endpoint": f"https://{registry_id}.{region}.confluent.cloud",
                    "cloud": cloud,
                    "region": region,
                    "provider_cloud": cloud.upper(),
                    "provider_region": region,
                    "crn": _ccloud_crn(ORGANIZATION_ID, environment_id, "schema-registry", registry_id),
                },
            )
        )

    ksql_specs = (
        ("lksql-commerce", "env-commerce", "Commerce Stream Apps", "lkc-commerce", "sa-commerce", 2),
        ("lksql-logistics", "env-logistics", "Logistics Stream Apps", "lkc-logistics", "sa-logistics", 3),
        ("lksql-analytics", "env-analytics", "Analytics Stream Apps", "lkc-platform", "sa-platform", 1),
    )
    for resource_id, environment_id, display_name, cluster_id, owner_id, csu_count in ksql_specs:
        resources.append(
            _resource(
                tenant_id=tenant_id,
                resource_id=resource_id,
                resource_type="ksqldb_cluster",
                display_name=display_name,
                parent_id=environment_id,
                owner_id=owner_id,
                created_at=created_at,
                metadata={"kafka_cluster_id": cluster_id, "csu_count": csu_count},
            )
        )

    flink_pool_specs = (
        ("lfcp-logistics", "env-logistics", "Logistics Flink Pool", "gcp", "us-central1"),
        ("lfcp-analytics", "env-analytics", "Analytics Flink Pool", "azure", "eastus2"),
    )
    for resource_id, environment_id, display_name, cloud, region in flink_pool_specs:
        resources.append(
            _resource(
                tenant_id=tenant_id,
                resource_id=resource_id,
                resource_type="flink_compute_pool",
                display_name=display_name,
                parent_id=environment_id,
                created_at=created_at,
                metadata={
                    "cloud": cloud,
                    "region": region,
                    "provider_cloud": cloud.upper(),
                    "provider_region": region,
                    "crn": _ccloud_crn(
                        ORGANIZATION_ID,
                        environment_id,
                        "flink-compute-pool",
                        resource_id,
                        cloud=cloud,
                        region=region,
                    ),
                },
            )
        )

    flink_statement_specs = (
        ("lfstmt-logistics-eta", "env-logistics", "Shipment ETA Enrichment", "lfcp-logistics", "sa-logistics"),
        ("lfstmt-logistics-routing", "env-logistics", "Route Risk Enrichment", "lfcp-logistics", "sa-fulfillment"),
        ("lfstmt-analytics-session", "env-analytics", "Session Analytics", "lfcp-analytics", "sa-analytics"),
    )
    for resource_id, environment_id, display_name, pool_id, owner_id in flink_statement_specs:
        resources.append(
            _resource(
                tenant_id=tenant_id,
                resource_id=resource_id,
                resource_type="flink_statement",
                display_name=display_name,
                parent_id=environment_id,
                owner_id=owner_id,
                created_at=created_at,
                metadata={
                    "statement_name": display_name,
                    "compute_pool_id": pool_id,
                    "is_stopped": False,
                },
            )
        )
    return tuple(resources)


def _build_identities(tenant_id: str, created_at: datetime) -> tuple[CoreIdentity, ...]:
    """Build the exact-density synthetic identity topology."""
    service_accounts = (
        ("sa-commerce", "Commerce Workloads"),
        ("sa-logistics", "Logistics Workloads"),
        ("sa-platform", "Platform Operations"),
        ("sa-fulfillment", "Fulfillment Workloads"),
        ("sa-customer", "Customer Workloads"),
        ("sa-analytics", "Analytics Workloads"),
        ("sa-security", "Security Automation"),
        ("sa-data", "Data Quality Workloads"),
    )
    identities: list[CoreIdentity] = [
        _identity(
            tenant_id=tenant_id,
            identity_id=identity_id,
            identity_type="service_account",
            display_name=display_name,
            created_at=created_at,
            metadata={"description": f"Synthetic {display_name.casefold()}"},
        )
        for identity_id, display_name in service_accounts
    ]
    users = (
        ("user-alice", "Alice Northstar"),
        ("user-bob", "Bob Northstar"),
        ("user-carol", "Carol Northstar"),
        ("user-diego", "Diego Northstar"),
        ("user-eve", "Eve Northstar"),
        ("user-frank", "Frank Northstar"),
        ("user-grace", "Grace Northstar"),
        ("user-security", "Security Operations"),
    )
    identities.extend(
        _identity(
            tenant_id=tenant_id,
            identity_id=identity_id,
            identity_type="user",
            display_name=display_name,
            created_at=created_at,
            metadata={"crn": f"crn://confluent.cloud/user={identity_id}"},
        )
        for identity_id, display_name in users
    )
    identities.extend(
        (
            _identity(
                tenant_id=tenant_id,
                identity_id="idp-corporate",
                identity_type="identity_provider",
                display_name="Northstar Corporate IdP",
                created_at=created_at,
                metadata={
                    "description": "Synthetic corporate identity provider",
                    "crn": f"crn://confluent.cloud/organization={ORGANIZATION_ID}/identity-provider=idp-corporate",
                },
            ),
            _identity(
                tenant_id=tenant_id,
                identity_id="idp-partner",
                identity_type="identity_provider",
                display_name="Northstar Partner IdP",
                created_at=created_at,
                metadata={
                    "description": "Synthetic partner identity provider",
                    "crn": f"crn://confluent.cloud/organization={ORGANIZATION_ID}/identity-provider=idp-partner",
                },
            ),
            _identity(
                tenant_id=tenant_id,
                identity_id="pool-workload",
                identity_type="identity_pool",
                display_name="Northstar Workload Pool",
                created_at=created_at,
                metadata={"description": "Synthetic workload identity pool", "provider_id": "idp-corporate"},
            ),
            _identity(
                tenant_id=tenant_id,
                identity_id="pool-analytics",
                identity_type="identity_pool",
                display_name="Northstar Analytics Pool",
                created_at=created_at,
                metadata={"description": "Synthetic analytics identity pool", "provider_id": "idp-partner"},
            ),
        )
    )
    api_keys = (
        ("key-commerce", "sa-commerce", "lkc-commerce", "Commerce Kafka key"),
        ("key-logistics", "sa-logistics", "lkc-logistics", "Logistics Kafka key"),
        ("key-fulfillment", "sa-fulfillment", "lkc-fulfillment", "Fulfillment Kafka key"),
        ("key-customer", "sa-customer", "lkc-customer", "Customer Kafka key"),
        ("key-platform", "sa-platform", "lkc-platform", "Platform Kafka key"),
        ("key-data", "sa-data", "lkc-data", "Data Kafka key"),
        ("key-schema-commerce", "sa-platform", "lsrc-commerce", "Commerce Schema Registry key"),
        ("key-schema-logistics", "user-security", "lsrc-logistics", "Logistics Schema Registry key"),
        ("key-schema-fulfillment", "sa-security", "lsrc-fulfillment", "Fulfillment Schema Registry key"),
        ("key-connect", "pool-workload", "lcc-commerce-orders", "Connector service key"),
        ("key-analytics", "pool-analytics", "lksql-analytics", "Analytics service key"),
        ("key-operations", "user-alice", "lfcp-analytics", "Operations Flink key"),
    )
    identities.extend(
        _identity(
            tenant_id=tenant_id,
            identity_id=identity_id,
            identity_type="api_key",
            display_name=display_name,
            created_at=created_at,
            metadata={"owner_id": owner_id, "resource_id": resource_id},
        )
        for identity_id, owner_id, resource_id, display_name in api_keys
    )
    return tuple(identities)


def _build_tags(
    tenant_id: str,
    resources: tuple[CoreResource, ...],
    identities: tuple[CoreIdentity, ...],
) -> tuple[EntityTag, ...]:
    """Attach exactly one team tag to every generated entity."""
    resources_by_id = {resource.resource_id: resource for resource in resources}
    environment_team = {
        "env-commerce": "orders",
        "env-logistics": "data",
        "env-fulfillment": "fulfillment",
        "env-analytics": "analytics",
    }

    def team_for_resource(resource: CoreResource) -> str:
        if resource.resource_type == "organization":
            return "platform"
        explicit = {
            "lcc-commerce-payments": "payments",
            "lcc-commerce-customer": "customer",
            "lksql-analytics": "platform",
        }
        if resource.resource_id in explicit:
            return explicit[resource.resource_id]
        environment_id = _resource_environment(resource.resource_id, resources_by_id)
        if resource.resource_type == "connector" and resource.metadata.get("connector_kind") == "custom":
            return "security"
        return environment_team.get(environment_id or "", "platform")

    def team_for_identity(identity: CoreIdentity) -> str:
        explicit = {
            "sa-commerce": "orders",
            "sa-logistics": "data",
            "sa-platform": "platform",
            "sa-fulfillment": "fulfillment",
            "sa-customer": "customer",
            "sa-analytics": "analytics",
            "sa-security": "security",
            "sa-data": "data",
            "pool-workload": "data",
            "pool-analytics": "analytics",
            "user-security": "security",
            "idp-corporate": "security",
            "idp-partner": "security",
            "user-bob": "payments",
            "user-carol": "customer",
        }
        if identity.identity_id in explicit:
            return explicit[identity.identity_id]
        if identity.identity_type == "api_key":
            resource = resources_by_id[str(identity.metadata["resource_id"])]
            return team_for_resource(resource)
        return "customer"

    assignments = [("resource", resource.resource_id, team_for_resource(resource)) for resource in resources] + [
        ("identity", identity.identity_id, team_for_identity(identity)) for identity in identities
    ]
    return tuple(
        EntityTag(
            tag_id=None,
            tenant_id=tenant_id,
            entity_type=entity_type,
            entity_id=entity_id,
            tag_key="team",
            tag_value=team,
            created_by="demo-generator",
            created_at=None,
        )
        for entity_type, entity_id, team in assignments
    )


def _line_specs() -> tuple[tuple[str, str, str, str, str, Decimal, Decimal, str], ...]:
    """Return the native FOCUS product/line-type coverage for one day."""
    specs: list[tuple[str, str, str, str, str, Decimal, Decimal, str]] = []
    for cluster_id, environment_id, _display_name, _cloud, _region in _CLUSTERS:
        specs.extend(
            (
                (
                    environment_id,
                    cluster_id,
                    "KAFKA",
                    "KAFKA_NUM_CKUS",
                    "usage",
                    Decimal("2.0"),
                    Decimal("50.00"),
                    "usage",
                ),
                (
                    environment_id,
                    cluster_id,
                    "KAFKA",
                    "KAFKA_STORAGE",
                    "shared",
                    Decimal("10.0"),
                    Decimal("100.00"),
                    "shared",
                ),
                (
                    environment_id,
                    cluster_id,
                    "KAFKA",
                    "KAFKA_NETWORK_READ",
                    "usage",
                    Decimal("8.0"),
                    Decimal("2.50"),
                    "usage",
                ),
                (
                    environment_id,
                    cluster_id,
                    "KAFKA",
                    "KAFKA_NETWORK_WRITE",
                    "usage",
                    Decimal("6.0"),
                    Decimal("5.00"),
                    "usage",
                ),
            )
        )
    for connector_id, _cluster_id, environment_id, _display_name, connector_kind in _CONNECTORS:
        specs.append(
            (
                environment_id,
                connector_id,
                "CONNECT",
                "CONNECT_CAPACITY" if connector_kind == "managed" else "CUSTOM_CONNECT_NUM_TASKS",
                "shared",
                Decimal("1.0"),
                Decimal("150.00"),
                "shared",
            )
        )
    for registry_id, environment_id, _display_name, _cloud, _region in _SCHEMA_REGISTRIES:
        specs.append(
            (
                environment_id,
                registry_id,
                "STREAM_GOVERNANCE",
                "SCHEMA_REGISTRY",
                "shared",
                Decimal("1.0"),
                Decimal("200.00"),
                "shared",
            )
        )
    for resource_id, environment_id, _display_name, _cluster_id, _owner_id, _csu_count in (
        ("lksql-commerce", "env-commerce", "Commerce Stream Apps", "lkc-commerce", "sa-commerce", 2),
        ("lksql-logistics", "env-logistics", "Logistics Stream Apps", "lkc-logistics", "sa-logistics", 3),
        ("lksql-analytics", "env-analytics", "Analytics Stream Apps", "lkc-platform", "sa-platform", 1),
    ):
        specs.append(
            (
                environment_id,
                resource_id,
                "KSQL",
                "KSQL_NUM_CSUS",
                "shared",
                Decimal("2.0"),
                Decimal("250.00"),
                "shared",
            )
        )
    for resource_id, environment_id, _display_name, _cloud, _region in (
        ("lfcp-logistics", "env-logistics", "Logistics Flink Pool", "gcp", "us-central1"),
        ("lfcp-analytics", "env-analytics", "Analytics Flink Pool", "azure", "eastus2"),
    ):
        specs.append(
            (
                environment_id,
                resource_id,
                "FLINK",
                "FLINK_NUM_CFUS",
                "usage",
                Decimal("1.5"),
                Decimal("250.00"),
                "usage",
            )
        )
    for resource_id, environment_id, _display_name, _pool_id, _owner_id in (
        ("lfstmt-logistics-eta", "env-logistics", "Shipment ETA Enrichment", "lfcp-logistics", "sa-logistics"),
        ("lfstmt-logistics-routing", "env-logistics", "Route Risk Enrichment", "lfcp-logistics", "sa-fulfillment"),
        ("lfstmt-analytics-session", "env-analytics", "Session Analytics", "lfcp-analytics", "sa-analytics"),
    ):
        specs.append(
            (
                environment_id,
                resource_id,
                "FLINK",
                "FLINK_NUM_CFUS",
                "usage",
                Decimal("1.0"),
                Decimal("180.00"),
                "usage",
            )
        )
    specs.extend(
        (
            (
                "env-commerce",
                "env-commerce",
                "AUDIT_LOG",
                "AUDIT_LOG_READ",
                "shared",
                Decimal("1.0"),
                Decimal("20.00"),
                "shared",
            ),
            (
                "env-commerce",
                "env-commerce",
                "SUPPORT_CLOUD_BASIC",
                "SUPPORT",
                "shared",
                Decimal("1.0"),
                Decimal("75.00"),
                "shared",
            ),
        )
    )
    return tuple(specs)


def _build_billing_and_chargebacks(
    tenant_id: str,
    start_date: date,
    anchor_date: date,
) -> tuple[tuple[CCloudBillingLineItem, ...], tuple[ChargebackRow, ...]]:
    billing_lines: list[CCloudBillingLineItem] = []
    chargebacks: list[ChargebackRow] = []
    allocation_targets = (
        "sa-commerce",
        "user-bob",
        "sa-fulfillment",
        "user-carol",
        "sa-analytics",
        "sa-platform",
        "sa-security",
        "sa-data",
    )
    day_count = (anchor_date - start_date).days + 1
    for day_offset in range(day_count):
        tracking_date = start_date + timedelta(days=day_offset)
        timestamp = _at_midnight(tracking_date)
        scale = _SEASONALITY[day_offset % len(_SEASONALITY)] * (Decimal("1") + Decimal(day_offset) * Decimal("0.0005"))
        for line_index, (
            env_id,
            resource_id,
            product_category,
            product_type,
            allocation_kind,
            base_quantity,
            unit_price,
            _,
        ) in enumerate(_line_specs()):
            quantity = (base_quantity * scale).quantize(_QUANTITY_PRECISION, rounding=ROUND_HALF_UP)
            total_cost = (quantity * unit_price).quantize(_CENT, rounding=ROUND_HALF_UP)
            line = CCloudBillingLineItem(
                ecosystem=ECOSYSTEM,
                tenant_id=tenant_id,
                timestamp=timestamp,
                env_id=env_id,
                resource_id=resource_id,
                product_category=product_category,
                product_type=product_type,
                quantity=quantity,
                unit_price=unit_price,
                total_cost=total_cost,
                currency="USD",
                granularity="daily",
                metadata={},
            )
            billing_lines.append(line)
            targets = (allocation_targets[line_index % len(allocation_targets)],)
            portions = (Decimal("1.00"),)
            cost_type = CostType.USAGE if allocation_kind == "usage" else CostType.SHARED
            allocation_method = "usage_ratio" if allocation_kind == "usage" else "even_split"
            allocation_detail = "usage_ratio_allocation" if allocation_kind == "usage" else "even_split_allocation"
            allocated = Decimal("0")
            for target_index, (identity_id, ratio) in enumerate(zip(targets, portions, strict=True)):
                amount = (
                    total_cost - allocated
                    if target_index == len(targets) - 1
                    else (total_cost * ratio).quantize(_CENT, rounding=ROUND_HALF_UP)
                )
                allocated += amount
                chargebacks.append(
                    ChargebackRow(
                        ecosystem=ECOSYSTEM,
                        tenant_id=tenant_id,
                        timestamp=timestamp,
                        resource_id=resource_id,
                        product_category=product_category,
                        product_type=product_type,
                        identity_id=identity_id,
                        cost_type=cost_type,
                        amount=amount,
                        allocation_method=allocation_method,
                        allocation_detail=allocation_detail,
                        tags={},
                        metadata={"env_id": env_id},
                    )
                )
    return tuple(billing_lines), tuple(chargebacks)


def _build_pipeline_states(
    tenant_id: str,
    start_date: date,
    anchor_date: date,
) -> tuple[PipelineState, ...]:
    return tuple(
        PipelineState(
            ecosystem=ECOSYSTEM,
            tenant_id=tenant_id,
            tracking_date=tracking_date,
            billing_gathered=True,
            resources_gathered=True,
            chargeback_calculated=True,
            calculation_id=f"clean-calculation-{tracking_date.isoformat()}",
            calculation_completed_at=_at_midnight(tracking_date) + timedelta(hours=1),
            topic_overlay_gathered=True,
            topic_attribution_calculated=True,
        )
        for tracking_date in (
            start_date + timedelta(days=offset) for offset in range((anchor_date - start_date).days + 1)
        )
    )


def _build_topic_attributions(
    billing_lines: tuple[CCloudBillingLineItem, ...],
) -> tuple[TopicAttributionRow, ...]:
    topics_by_cluster = dict(_TOPIC_NAMES)
    rows: list[TopicAttributionRow] = []
    for line in billing_lines:
        if line.product_category != "KAFKA":
            continue
        topic_names = topics_by_cluster[line.resource_id][:2]
        rows.extend(
            build_reconciled_topic_rows(
                TopicAttributionRowOutputContext(
                    ecosystem=ECOSYSTEM,
                    tenant_id=line.tenant_id,
                    timestamp=line.timestamp,
                    env_id=line.env_id,
                    cluster_resource_id=line.resource_id,
                    product_category=line.product_category,
                    product_type=line.product_type,
                    cluster_cost=line.total_cost,
                ),
                cluster_quantity=line.quantity,
                pool_usage=Decimal("3"),
                topic_usage={topic_names[0]: Decimal("1"), topic_names[1]: Decimal("2")},
                attribution_method=(
                    "bytes_ratio"
                    if "NETWORK" in line.product_type or "CKU" in line.product_type
                    else "retained_bytes_ratio"
                ),
                residual_method="complete_topic_telemetry",
            )
        )
    return tuple(rows)


def _build_preview_source_capture(
    tenant_id: str,
    start_date: date,
    anchor_date: date,
    billing_lines: tuple[CCloudBillingLineItem, ...],
    resources: tuple[CoreResource, ...],
) -> CCloudNativeSourceEvidenceCapture:
    refresh_start = _at_midnight(start_date)
    refresh_end = _at_midnight(anchor_date + timedelta(days=1))
    resources_by_id = {resource.resource_id: resource for resource in resources}
    records = tuple(
        CCloudCostSourceRecord(
            ecosystem=ECOSYSTEM,
            tenant_id=tenant_id,
            source_record_id=f"northstar-source-{index:05d}",
            identity_scheme="confluent_cloud_cost_id",
            provider_cost_id=f"northstar-cost-{index:05d}",
            source_period_start=line.timestamp,
            source_period_end=line.timestamp + timedelta(days=1),
            collection_window_start=refresh_start,
            collection_window_end=refresh_end,
            evidence_scope_start=line.timestamp,
            evidence_scope_end=line.timestamp + timedelta(days=1),
            allocation_timestamp=line.timestamp,
            retention_timestamp=line.timestamp,
            granularity=line.granularity,
            product=line.product_category,
            line_type=line.product_type,
            amount=line.total_cost,
            original_amount=line.total_cost,
            discount_amount=Decimal("0.00"),
            price=line.unit_price,
            quantity=line.quantity,
            unit="unit",
            description=(
                "Cloud support subscription"
                if line.product_type == "SUPPORT"
                else "Audit log ingestion"
                if line.product_type == "AUDIT_LOG_READ"
                else f"Northstar {line.product_category.casefold()} {line.product_type.casefold()} usage"
            ),
            network_access_type="regional" if "NETWORK" in line.product_type else None,
            resource_id=line.resource_id,
            resource_name=resources_by_id[line.resource_id].display_name,
            environment_id=line.env_id,
            tier_dimensions={"billing_tier": "standard"},
            malformed=False,
            diagnostics=(),
            raw_payload={
                "provider": "synthetic-confluent-cloud",
                "source_record_id": f"northstar-source-{index:05d}",
                "product": line.product_category,
                "line_type": line.product_type,
                "amount": str(line.total_cost),
            },
            billing_timestamp=line.timestamp,
            billing_env_id=line.env_id,
            billing_resource_id=line.resource_id,
            billing_product_type=line.product_type,
            billing_product_category=line.product_category,
        )
        for index, line in enumerate(billing_lines, start=1)
    )
    return CCloudNativeSourceEvidenceCapture(
        ecosystem=ECOSYSTEM,
        tenant_id=tenant_id,
        refresh_start=refresh_start,
        refresh_end=refresh_end,
        windows=(NativeSourceWindow(refresh_start, refresh_end),),
        records=records,
    )


def _build_lineage_runs(
    tenant_id: str,
    start_date: date,
    anchor_date: date,
    billing_lines: tuple[CCloudBillingLineItem, ...],
    chargebacks: tuple[ChargebackRow, ...],
) -> tuple[AllocationLineageRunCapture, ...]:
    rows_by_key: dict[tuple[datetime, str, str, str, str], list[ChargebackRow]] = defaultdict(list)
    billing_by_date: defaultdict[date, list[CCloudBillingLineItem]] = defaultdict(list)
    for row in chargebacks:
        rows_by_key[
            (
                row.timestamp,
                str(row.metadata.get("env_id", "")),
                row.resource_id or "",
                row.product_type,
                row.product_category,
            )
        ].append(row)
    for line in billing_lines:
        billing_by_date[line.timestamp.date()].append(line)
    runs: list[AllocationLineageRunCapture] = []
    for offset in range((anchor_date - start_date).days + 1):
        tracking_date = start_date + timedelta(days=offset)
        captures = tuple(
            build_allocation_lineage_capture(
                origin=line,
                rows=tuple(
                    rows_by_key[
                        (line.timestamp, line.env_id, line.resource_id, line.product_type, line.product_category)
                    ]
                ),
            )
            for line in billing_by_date[tracking_date]
        )
        runs.append(
            AllocationLineageRunCapture(
                ecosystem=ECOSYSTEM,
                tenant_id=tenant_id,
                tracking_date=tracking_date,
                calculation_id=f"clean-calculation-{tracking_date.isoformat()}",
                captures=captures,
            )
        )
    return tuple(runs)


def _showcase_cutover(start_date: date) -> date:
    """Return the deterministic lifecycle cutover inside the generated window."""
    return start_date + timedelta(days=90)


def _showcase_resources_and_identities(
    tenant_id: str,
    clean: CleanDemoScenario,
    cutover: date,
) -> tuple[tuple[CoreResource, ...], tuple[CoreIdentity, ...]]:
    """Append one active and one deleted topic/principal pair to Clean state."""
    cutover_at = _at_midnight(cutover)
    legacy_deleted_at = cutover_at
    legacy_last_seen_at = cutover_at - timedelta(seconds=1)
    resources = list(clean.resources)
    resources.extend(
        (
            _resource(
                tenant_id=tenant_id,
                resource_id=_SHOWCASE_LIVE_TOPIC_ID,
                resource_type="topic",
                display_name=_SHOWCASE_TOPIC_NAMES[_SHOWCASE_LIVE_TOPIC_ID],
                parent_id="lkc-customer",
                created_at=cutover_at,
            ),
            replace(
                _resource(
                    tenant_id=tenant_id,
                    resource_id=_SHOWCASE_LEGACY_TOPIC_ID,
                    resource_type="topic",
                    display_name=_SHOWCASE_TOPIC_NAMES[_SHOWCASE_LEGACY_TOPIC_ID],
                    parent_id="lkc-data",
                    created_at=_at_midnight(clean.start_date - timedelta(days=30)),
                ),
                status=ResourceStatus.DELETED,
                deleted_at=legacy_deleted_at,
                last_seen_at=legacy_last_seen_at,
            ),
        )
    )

    identities = list(clean.identities)
    identities.extend(
        (
            _identity(
                tenant_id=tenant_id,
                identity_id=_SHOWCASE_LIVE_IDENTITY_ID,
                identity_type="service_account",
                display_name="Showcase Live Workload",
                created_at=cutover_at,
                metadata={"description": "Synthetic Showcase lifecycle workload"},
            ),
            replace(
                _identity(
                    tenant_id=tenant_id,
                    identity_id=_SHOWCASE_LEGACY_IDENTITY_ID,
                    identity_type="service_account",
                    display_name="Showcase Legacy Workload",
                    created_at=_at_midnight(clean.start_date - timedelta(days=30)),
                    metadata={"description": "Synthetic Showcase legacy workload"},
                ),
                deleted_at=legacy_deleted_at,
                last_seen_at=legacy_last_seen_at,
            ),
        )
    )
    return tuple(resources), tuple(identities)


def _showcase_line_amount(
    cluster_id: str,
    product_type: str,
    day_offset: int,
    day_count: int,
    anchor_date: date,
    tracking_date: date,
) -> Decimal | None:
    """Return the deterministic amount for one non-balancing Showcase line."""
    if cluster_id == "lkc-data" and product_type == "KAFKA_REST_PRODUCE":
        return None
    if cluster_id == "lkc-customer" and product_type == "KAFKA_REST_PRODUCE":
        if tracking_date == anchor_date:
            return Decimal("50000.00")
        if tracking_date >= anchor_date - timedelta(days=30):
            return Decimal("1000.00")
        return Decimal("250.00")
    if cluster_id == "lkc-commerce":
        return Decimal("80.00" if product_type == "KAFKA_PARTITION" else "120.00")
    if cluster_id == "lkc-logistics":
        if product_type == "KAFKA_PARTITION":
            return Decimal(80 + day_offset * 2)
        return Decimal("150.00")
    if cluster_id == "lkc-fulfillment":
        if product_type == "KAFKA_PARTITION":
            return Decimal(500 - day_offset)
        return Decimal("200.00")
    if cluster_id == "lkc-customer":
        if product_type == "KAFKA_PARTITION":
            return Decimal("100.00" if day_offset < day_count // 2 else "300.00")
        return Decimal("100.00")
    if cluster_id == "lkc-platform":
        if product_type == "KAFKA_PARTITION":
            return Decimal("500.00" if day_offset < day_count // 2 else "150.00")
        return Decimal("160.00")
    if cluster_id == "lkc-data":
        return Decimal("180.00" if product_type == "KAFKA_PARTITION" else "240.00")
    raise ValueError(f"unknown Showcase cluster: {cluster_id}")


def _showcase_line_identity(cluster_id: str, product_type: str, tracking_date: date, cutover: date) -> str:
    if cluster_id == "lkc-customer" and product_type == "KAFKA_REST_PRODUCE":
        return _SHOWCASE_LIVE_IDENTITY_ID if tracking_date >= cutover else "sa-customer"
    if cluster_id == "lkc-data" and product_type == "KAFKA_PARTITION":
        return _SHOWCASE_LEGACY_IDENTITY_ID if tracking_date < cutover else _SHOWCASE_LIVE_IDENTITY_ID
    return {
        "lkc-commerce": "sa-commerce",
        "lkc-logistics": "sa-logistics",
        "lkc-fulfillment": "sa-fulfillment",
        "lkc-customer": "sa-customer",
        "lkc-platform": "sa-platform",
        "lkc-data": "sa-data",
    }[cluster_id]


def _showcase_line_topic(cluster_id: str, product_type: str, tracking_date: date, cutover: date) -> str:
    if cluster_id == "lkc-customer" and product_type == "KAFKA_REST_PRODUCE":
        return (
            _SHOWCASE_TOPIC_NAMES[_SHOWCASE_LIVE_TOPIC_ID]
            if tracking_date >= cutover
            else dict(_TOPIC_NAMES)[cluster_id][0]
        )
    if cluster_id == "lkc-data" and product_type == "KAFKA_PARTITION" and tracking_date < cutover:
        return _SHOWCASE_TOPIC_NAMES[_SHOWCASE_LEGACY_TOPIC_ID]
    return dict(_TOPIC_NAMES)[cluster_id][0]


def _showcase_line_cohort(cluster_id: str, product_type: str) -> str:
    if cluster_id == "lkc-customer" and product_type == "KAFKA_REST_PRODUCE":
        return "anomaly"
    if cluster_id == "lkc-data" and product_type == "KAFKA_PARTITION":
        return "lifecycle"
    return {
        "lkc-commerce": "stable",
        "lkc-logistics": "increasing",
        "lkc-fulfillment": "decreasing",
        "lkc-customer": "step-increase",
        "lkc-platform": "step-decrease",
        "lkc-data": "balancing",
    }[cluster_id]


def _showcase_line(
    tenant_id: str,
    tracking_date: date,
    cluster_id: str,
    product_type: str,
    amount: Decimal,
    identities_by_id: dict[str, CoreIdentity],
    resources_by_id: dict[str, CoreResource],
    cutover: date,
) -> tuple[CCloudBillingLineItem, ChargebackRow, TopicAttributionRow]:
    """Build one reconciled Showcase billing, allocation, and topic row."""
    cluster = resources_by_id[cluster_id]
    environment_id = str(cluster.parent_id)
    timestamp = _at_midnight(tracking_date)
    quantity = (amount / Decimal("0.01")).quantize(_QUANTITY_PRECISION, rounding=ROUND_HALF_UP)
    line = CCloudBillingLineItem(
        ecosystem=ECOSYSTEM,
        tenant_id=tenant_id,
        timestamp=timestamp,
        env_id=environment_id,
        resource_id=cluster_id,
        product_category="KAFKA",
        product_type=product_type,
        quantity=quantity,
        unit_price=Decimal("0.01"),
        total_cost=(quantity * Decimal("0.01")).quantize(_CENT, rounding=ROUND_HALF_UP),
        currency="USD",
        granularity="daily",
        metadata={},
    )
    identity_id = _showcase_line_identity(cluster_id, product_type, tracking_date, cutover)
    identity = identities_by_id[identity_id]
    if not _active_for(identity, timestamp):
        raise ValueError("Showcase allocation identity is outside its lifetime")
    allocation_method = "usage_ratio"
    allocation_detail = "usage_ratio_allocation"
    chargeback = ChargebackRow(
        ecosystem=ECOSYSTEM,
        tenant_id=tenant_id,
        timestamp=timestamp,
        resource_id=cluster_id,
        product_category="KAFKA",
        product_type=product_type,
        identity_id=identity_id,
        cost_type=CostType.USAGE,
        amount=line.total_cost,
        allocation_method=allocation_method,
        allocation_detail=allocation_detail,
        tags={},
        metadata={"env_id": environment_id},
    )
    topic = _showcase_line_topic(cluster_id, product_type, tracking_date, cutover)
    attribution = TopicAttributionRow(
        ecosystem=ECOSYSTEM,
        tenant_id=tenant_id,
        timestamp=timestamp,
        env_id=environment_id,
        cluster_resource_id=cluster_id,
        topic_name=topic,
        product_category="KAFKA",
        product_type=product_type,
        attribution_method="bytes_ratio",
        amount=line.total_cost,
        metadata={},
    )
    return line, chargeback, attribution


def _build_showcase_billing_and_allocations(
    tenant_id: str,
    clean: CleanDemoScenario,
    resources: tuple[CoreResource, ...],
    identities: tuple[CoreIdentity, ...],
) -> tuple[tuple[CCloudBillingLineItem, ...], tuple[ChargebackRow, ...], tuple[TopicAttributionRow, ...]]:
    """Append twelve deterministic, reconciled billing rows per generated day."""
    day_count = (clean.anchor_date - clean.start_date).days + 1
    cutover = _showcase_cutover(clean.start_date)
    resources_by_id = {resource.resource_id: resource for resource in resources}
    identities_by_id = {identity.identity_id: identity for identity in identities}
    clean_totals = {
        tracking_date: sum(
            (line.total_cost for line in clean.billing_lines if line.timestamp.date() == tracking_date),
            Decimal("0"),
        )
        for tracking_date in (clean.start_date + timedelta(days=offset) for offset in range(day_count))
    }
    rows: list[tuple[date, str, str, Decimal | None]] = []
    for day_offset in range(day_count):
        tracking_date = clean.start_date + timedelta(days=day_offset)
        for cluster_id, *_ in _CLUSTERS:
            for product_type in _SHOWCASE_LINE_TYPES:
                rows.append(
                    (
                        tracking_date,
                        cluster_id,
                        product_type,
                        _showcase_line_amount(
                            cluster_id,
                            product_type,
                            day_offset,
                            day_count,
                            clean.anchor_date,
                            tracking_date,
                        ),
                    )
                )
    baseline_dates = {clean.anchor_date - timedelta(days=offset) for offset in range(30, 0, -1)}
    non_balancing_totals: defaultdict[date, Decimal] = defaultdict(Decimal)
    for tracking_date, _cluster_id, _product_type, amount in rows:
        if amount is not None:
            non_balancing_totals[tracking_date] += amount
    target = max(
        clean_totals[tracking_date] + non_balancing_totals[tracking_date] for tracking_date in baseline_dates
    ) + Decimal("100.00")

    billing: list[CCloudBillingLineItem] = []
    chargebacks: list[ChargebackRow] = []
    attributions: list[TopicAttributionRow] = []
    for tracking_date, cluster_id, product_type, amount in rows:
        if amount is None:
            amount = target - clean_totals[tracking_date] - non_balancing_totals[tracking_date]
            if tracking_date not in baseline_dates:
                amount = max(amount, Decimal("100.00"))
        if amount <= 0:
            raise ValueError("Showcase balancing amount must be positive")
        line, chargeback, attribution = _showcase_line(
            tenant_id,
            tracking_date,
            cluster_id,
            product_type,
            amount,
            identities_by_id,
            resources_by_id,
            cutover,
        )
        billing.append(line)
        chargebacks.append(chargeback)
        attributions.append(attribution)
    return tuple(billing), tuple(chargebacks), tuple(attributions)


def _showcase_metric_values(topic: CoreResource, day_offset: int) -> dict[str, float]:
    """Return deterministic unit-economics and partition-efficiency values."""
    if topic.display_name == "customer.profile.v1":
        return {
            "received_bytes": 0.0,
            "sent_bytes": 0.0,
            "retained_bytes": 0.0,
            "partition_count": 1.0,
        }
    if topic.resource_id == _SHOWCASE_LIVE_TOPIC_ID:
        return {
            "received_bytes": 80_000.0,
            "sent_bytes": 60_000.0,
            "retained_bytes": 140_000.0,
            "partition_count": 64.0,
        }
    if topic.display_name == "orders.created.v1":
        return {
            "received_bytes": 250_000_000.0,
            "sent_bytes": 200_000_000.0,
            "retained_bytes": 450_000_000.0,
            "partition_count": 2.0,
        }
    base = 5_000_000.0 + float((day_offset % 7) * 100_000)
    return {
        "received_bytes": base,
        "sent_bytes": base / 2,
        "retained_bytes": base * 1.5,
        "partition_count": 4.0,
    }


def _build_showcase_source_metrics(
    resources: tuple[CoreResource, ...],
    identities: tuple[CoreIdentity, ...],
    chargebacks: tuple[ChargebackRow, ...],
    anchor_date: date,
) -> tuple[MetricRow, ...]:
    """Build the final ninety complete days of raw source metric rows."""
    resources_by_id = {resource.resource_id: resource for resource in resources}
    metrics: list[MetricRow] = []
    final_start = anchor_date - timedelta(days=_SHOWCASE_VALIDATION_DAYS - 1)
    final_days = tuple(final_start + timedelta(days=offset) for offset in range(_SHOWCASE_VALIDATION_DAYS))
    scope_counts_by_date = _showcase_scope_counts_by_date(resources, identities, chargebacks, final_days)
    topics = tuple(resource for resource in resources if resource.resource_type == "topic")
    topic_scopes = {topic.resource_id: _showcase_scope_ids(topic, resources_by_id) for topic in topics}
    for day_offset, tracking_date in enumerate(final_days):
        timestamp = _at_midnight(tracking_date)
        scope_counts = scope_counts_by_date[tracking_date]
        for topic in topics:
            if not _active_for(topic, timestamp):
                continue
            cluster = resources_by_id[str(topic.parent_id)]
            environment_id = str(cluster.parent_id)
            cluster_id, _topic_environment_id = topic_scopes[topic.resource_id]
            if cluster_id is None:
                raise ValueError("Showcase source metric topic cluster scope is invalid")
            values = _showcase_metric_values(topic, day_offset)
            labels = {
                "tenant_id": topic.tenant_id,
                "environment_id": environment_id,
                "cluster_id": cluster.resource_id,
                "topic": str(topic.display_name),
                "tenant_active_topic_count": str(scope_counts.tenant_active_topic_count),
                "tenant_active_principal_count": str(scope_counts.tenant_active_principal_count),
                "environment_active_topic_count": str(
                    scope_counts.environment_active_topic_counts.get(environment_id, 0)
                ),
                "environment_active_principal_count": str(
                    scope_counts.environment_active_principal_counts.get(environment_id, 0)
                ),
                "cluster_active_topic_count": str(scope_counts.cluster_active_topic_counts.get(cluster_id, 0)),
                "cluster_active_principal_count": str(scope_counts.cluster_active_principal_counts.get(cluster_id, 0)),
            }
            metrics.extend(
                MetricRow(timestamp=timestamp, metric_key=metric_key, value=values[metric_key], labels=labels)
                for metric_key in _SHOWCASE_METRIC_KEYS
            )
    return tuple(metrics)


def build_clean_demo_scenario(*, tenant_id: str, anchor_date: date) -> CleanDemoScenario:
    """Build the deterministic Clean Confluent scenario for ``anchor_date``."""
    if not tenant_id.strip():
        raise ValueError("tenant_id must not be blank")
    start_date = _window_start(anchor_date)
    created_at = _at_midnight(start_date - timedelta(days=30))
    resources = _build_resources(tenant_id, created_at)
    identities = _build_identities(tenant_id, created_at)
    billing_lines, chargebacks = _build_billing_and_chargebacks(tenant_id, start_date, anchor_date)
    pipeline_states = _build_pipeline_states(tenant_id, start_date, anchor_date)
    scenario = CleanDemoScenario(
        tenant_id=tenant_id,
        anchor_date=anchor_date,
        start_date=start_date,
        resources=resources,
        identities=identities,
        entity_tags=_build_tags(tenant_id, resources, identities),
        billing_lines=billing_lines,
        chargebacks=chargebacks,
        pipeline_states=pipeline_states,
        topic_attributions=_build_topic_attributions(billing_lines),
        preview_source_capture=_build_preview_source_capture(
            tenant_id,
            start_date,
            anchor_date,
            billing_lines,
            resources,
        ),
        allocation_lineage_runs=_build_lineage_runs(
            tenant_id,
            start_date,
            anchor_date,
            billing_lines,
            chargebacks,
        ),
        organization_authority_id=ORGANIZATION_ID,
        organization_authority_at=created_at,
    )
    validate_clean_demo_scenario(scenario)
    return scenario


def build_showcase_demo_scenario(*, tenant_id: str, anchor_date: date) -> ShowcaseDemoScenario:
    """Build the deterministic additive Showcase profile from Clean state."""
    clean = build_clean_demo_scenario(tenant_id=tenant_id, anchor_date=anchor_date)
    cutover = _showcase_cutover(clean.start_date)
    resources, identities = _showcase_resources_and_identities(tenant_id, clean, cutover)
    added_billing, added_chargebacks, added_topics = _build_showcase_billing_and_allocations(
        tenant_id,
        clean,
        resources,
        identities,
    )
    billing_lines = clean.billing_lines + added_billing
    chargebacks = clean.chargebacks + added_chargebacks
    topic_attributions = clean.topic_attributions + added_topics
    scenario = ShowcaseDemoScenario(
        tenant_id=tenant_id,
        anchor_date=anchor_date,
        start_date=clean.start_date,
        resources=resources,
        identities=identities,
        entity_tags=_build_tags(tenant_id, resources, identities),
        billing_lines=billing_lines,
        chargebacks=chargebacks,
        pipeline_states=clean.pipeline_states,
        topic_attributions=topic_attributions,
        preview_source_capture=_build_preview_source_capture(
            tenant_id,
            clean.start_date,
            anchor_date,
            billing_lines,
            resources,
        ),
        allocation_lineage_runs=_build_lineage_runs(
            tenant_id,
            clean.start_date,
            anchor_date,
            billing_lines,
            chargebacks,
        ),
        organization_authority_id=clean.organization_authority_id,
        organization_authority_at=clean.organization_authority_at,
        source_metrics=_build_showcase_source_metrics(resources, identities, chargebacks, anchor_date),
    )
    validate_showcase_demo_scenario(scenario)
    return scenario


def _resource_environment(resource_id: str, resources_by_id: dict[str, CoreResource]) -> str | None:
    current_id: str | None = resource_id
    visited: set[str] = set()
    while current_id is not None and current_id not in visited:
        visited.add(current_id)
        resource = resources_by_id.get(current_id)
        if resource is None:
            return None
        if resource.resource_type == "environment":
            return resource.resource_id
        current_id = resource.parent_id
    return None


def _is_utc_second(value: datetime | None) -> bool:
    return (
        value is not None and value.tzinfo is not None and value.utcoffset() == timedelta(0) and value.microsecond == 0
    )


def _active_for(value: CoreResource | CoreIdentity, timestamp: datetime) -> bool:
    return (value.created_at is None or value.created_at <= timestamp) and (
        value.deleted_at is None or timestamp < value.deleted_at
    )


@dataclass(frozen=True)
class _ShowcaseScopeCounts:
    """Cached active unit denominators for one generated date."""

    tenant_active_topic_count: int
    tenant_active_principal_count: int
    environment_active_topic_counts: dict[str, int]
    environment_active_principal_counts: dict[str, int]
    cluster_active_topic_counts: dict[str, int]
    cluster_active_principal_counts: dict[str, int]


def _showcase_scope_ids(
    resource: CoreResource,
    resources_by_id: dict[str, CoreResource],
) -> tuple[str | None, str | None]:
    """Return the nearest Kafka-cluster and environment ancestors for a resource."""
    cluster_id: str | None = None
    environment_id: str | None = None
    current_id: str | None = resource.resource_id
    visited: set[str] = set()
    while current_id is not None and current_id not in visited:
        visited.add(current_id)
        current = resources_by_id.get(current_id)
        if current is None:
            break
        if current.resource_type == "kafka_cluster" and cluster_id is None:
            cluster_id = current.resource_id
        if current.resource_type == "environment" and environment_id is None:
            environment_id = current.resource_id
        current_id = current.parent_id
    return cluster_id, environment_id


def _showcase_scope_counts_by_date(
    resources: tuple[CoreResource, ...],
    identities: tuple[CoreIdentity, ...],
    chargebacks: tuple[ChargebackRow, ...],
    tracking_dates: tuple[date, ...],
) -> dict[date, _ShowcaseScopeCounts]:
    """Cache scope denominators once per generated date."""
    resources_by_id = {resource.resource_id: resource for resource in resources}
    identities_by_id = {identity.identity_id: identity for identity in identities}
    resource_scopes = {resource.resource_id: _showcase_scope_ids(resource, resources_by_id) for resource in resources}
    topics = tuple(resource for resource in resources if resource.resource_type == "topic")
    tracking_date_set = set(tracking_dates)

    active_principals_by_date: defaultdict[date, set[str]] = defaultdict(set)
    environment_principals_by_date: defaultdict[date, defaultdict[str, set[str]]] = defaultdict(
        lambda: defaultdict(set)
    )
    cluster_principals_by_date: defaultdict[date, defaultdict[str, set[str]]] = defaultdict(lambda: defaultdict(set))
    for row in chargebacks:
        tracking_date = row.timestamp.date()
        if tracking_date not in tracking_date_set:
            continue
        identity = identities_by_id.get(row.identity_id)
        resource = resources_by_id.get(str(row.resource_id))
        if (
            identity is None
            or identity.identity_type not in _SHOWCASE_PRINCIPAL_TYPES
            or not _active_for(identity, row.timestamp)
            or resource is None
            or not _active_for(resource, row.timestamp)
        ):
            continue
        active_principals_by_date[tracking_date].add(identity.identity_id)
        cluster_id, environment_id = resource_scopes[resource.resource_id]
        if environment_id is not None:
            environment_principals_by_date[tracking_date][environment_id].add(identity.identity_id)
        if cluster_id is not None:
            cluster_principals_by_date[tracking_date][cluster_id].add(identity.identity_id)

    counts_by_date: dict[date, _ShowcaseScopeCounts] = {}
    for tracking_date in tracking_dates:
        timestamp = _at_midnight(tracking_date)
        tenant_active_topic_count = 0
        environment_active_topic_counts: Counter[str] = Counter()
        cluster_active_topic_counts: Counter[str] = Counter()
        for topic in topics:
            if not _active_for(topic, timestamp):
                continue
            tenant_active_topic_count += 1
            cluster_id, environment_id = resource_scopes[topic.resource_id]
            if environment_id is not None:
                environment_active_topic_counts[environment_id] += 1
            if cluster_id is not None:
                cluster_active_topic_counts[cluster_id] += 1
        counts_by_date[tracking_date] = _ShowcaseScopeCounts(
            tenant_active_topic_count=tenant_active_topic_count,
            tenant_active_principal_count=len(active_principals_by_date[tracking_date]),
            environment_active_topic_counts=dict(environment_active_topic_counts),
            environment_active_principal_counts={
                environment_id: len(principal_ids)
                for environment_id, principal_ids in environment_principals_by_date[tracking_date].items()
            },
            cluster_active_topic_counts=dict(cluster_active_topic_counts),
            cluster_active_principal_counts={
                cluster_id: len(principal_ids)
                for cluster_id, principal_ids in cluster_principals_by_date[tracking_date].items()
            },
        )
    return counts_by_date


def _validate_billed_resource_lifetime(
    resource_id: str,
    timestamp: datetime,
    resources_by_id: dict[str, CoreResource],
    identities_by_id: dict[str, CoreIdentity],
) -> None:
    current_id: str | None = resource_id
    visited: set[str] = set()
    while current_id is not None:
        if current_id in visited:
            _raise("billed resource parent relationship contains a cycle")
        visited.add(current_id)
        resource = resources_by_id.get(current_id)
        if resource is None or not _active_for(resource, timestamp):
            _raise("billed resource lifetime does not contain its billed date")
        if resource.owner_id is not None:
            owner = identities_by_id.get(resource.owner_id)
            if owner is None or not _active_for(owner, timestamp):
                _raise("billed resource owner lifetime does not contain its billed date")
        if resource.resource_type == "connector":
            auth_id = resource.metadata.get("kafka_service_account_id") or resource.metadata.get("kafka_api_key")
            auth_identity = identities_by_id.get(str(auth_id))
            if auth_identity is None or not _active_for(auth_identity, timestamp):
                _raise("connector authentication lifetime does not contain its billed date")
        current_id = resource.parent_id


def _raise(message: str) -> NoReturn:
    raise ValueError(message)


def validate_clean_demo_scenario(scenario: CleanDemoScenario) -> None:
    """Validate the deterministic, complete Confluent demo state."""
    if not scenario.tenant_id.strip():
        _raise("scenario tenant_id must not be blank")
    if scenario.start_date != _window_start(scenario.anchor_date):
        _raise("scenario start_date must be the inclusive six-month window start")
    expected_dates = tuple(
        scenario.start_date + timedelta(days=offset)
        for offset in range((scenario.anchor_date - scenario.start_date).days + 1)
    )
    if not expected_dates:
        _raise("scenario date window must not be empty")

    resources_by_id: dict[str, CoreResource] = {}
    for resource in scenario.resources:
        if resource.ecosystem != ECOSYSTEM or resource.tenant_id != scenario.tenant_id:
            _raise("resource owner does not match the Clean scenario")
        if resource.resource_id in resources_by_id:
            _raise("resource IDs must be unique")
        if resource.resource_id == scenario.tenant_id:
            _raise("provider resource ID must not equal the tenant ID")
        if resource.status is not ResourceStatus.ACTIVE:
            _raise("Clean resources must be active")
        if any(
            value is not None and not _is_utc_second(value)
            for value in (resource.created_at, resource.deleted_at, resource.last_seen_at)
        ):
            _raise("resource timestamps must be UTC seconds")
        resources_by_id[resource.resource_id] = resource
    resource_counts = Counter(resource.resource_type for resource in scenario.resources)
    if dict(resource_counts) != _RESOURCE_COUNTS:
        _raise(f"Clean resource topology mismatch: {dict(resource_counts)!r}")

    organizations = [resource for resource in scenario.resources if resource.resource_type == "organization"]
    if len(organizations) != 1 or organizations[0].resource_id == scenario.tenant_id:
        _raise("Clean state requires one provider organization distinct from tenant_id")
    organization = organizations[0]
    try:
        UUID(organization.resource_id)
    except ValueError, TypeError, AttributeError:
        _raise("provider organization ID must be a UUID")
    if (
        scenario.organization_authority_id != organization.resource_id
        or not _is_utc_second(scenario.organization_authority_at)
        or organization.metadata.get("organization_binding_state") != "bound"
    ):
        _raise("organization authority must identify a bound provider organization")

    environment_ids = {environment_id for environment_id, _ in _ENVIRONMENTS}
    environments = [resource for resource in scenario.resources if resource.resource_type == "environment"]
    if {resource.resource_id for resource in environments} != environment_ids or any(
        resource.parent_id is not None for resource in environments
    ):
        _raise("Clean environment topology mismatch")

    cluster_by_id = {
        resource.resource_id: resource for resource in scenario.resources if resource.resource_type == "kafka_cluster"
    }
    expected_clusters = {
        cluster_id: (environment_id, cloud, region)
        for cluster_id, environment_id, _display_name, cloud, region in _CLUSTERS
    }
    if set(cluster_by_id) != set(expected_clusters):
        _raise("Kafka cluster topology mismatch")
    for expected_cluster_id, cluster in cluster_by_id.items():
        expected_environment, expected_cloud, expected_region = expected_clusters[expected_cluster_id]
        if (
            cluster.parent_id != expected_environment
            or cluster.metadata.get("cloud") != expected_cloud
            or cluster.metadata.get("region") != expected_region
            or cluster.metadata.get("provider_cloud") != expected_cloud.upper()
            or cluster.metadata.get("provider_region") != expected_region
        ):
            _raise("Kafka cluster placement is invalid")

    topics_by_cluster: dict[str, set[str]] = defaultdict(set)
    for topic in (resource for resource in scenario.resources if resource.resource_type == "topic"):
        topic_cluster_id = topic.parent_id
        if topic_cluster_id is None or topic_cluster_id not in cluster_by_id or not topic.display_name:
            _raise("topic must have a Kafka cluster parent and name")
        if (
            topic.resource_id != f"{topic_cluster_id}:topic:{topic.display_name}"
            or topic.display_name in topics_by_cluster[topic_cluster_id]
        ):
            _raise("topic storage ID or cluster-local name is invalid")
        topics_by_cluster[topic_cluster_id].add(topic.display_name)
    if {cluster_id: len(names) for cluster_id, names in topics_by_cluster.items()} != {
        cluster_id: count
        for (cluster_id, _environment, _name, _cloud, _region), count in zip(_CLUSTERS, _TOPIC_COUNTS, strict=True)
    }:
        _raise("topic topology mismatch")

    connector_by_id = {
        resource.resource_id: resource for resource in scenario.resources if resource.resource_type == "connector"
    }
    expected_connector_ids = {connector_id for connector_id, *_ in _CONNECTORS}
    if set(connector_by_id) != expected_connector_ids:
        _raise("connector topology mismatch")
    for connector_id, connector in connector_by_id.items():
        expected = next(item for item in _CONNECTORS if item[0] == connector_id)
        _connector_id, cluster_id, environment_id, _display_name, connector_kind = expected
        if (
            connector.parent_id != cluster_id
            or connector.metadata.get("env_id") != environment_id
            or connector.metadata.get("connector_kind") != connector_kind
            or not connector.resource_id.startswith("lcc-" if connector_kind == "managed" else "clcc-")
            or connector.metadata.get("kafka_auth_mode") != "SERVICE_ACCOUNT"
            or not isinstance(connector.metadata.get("kafka_service_account_id"), str)
        ):
            _raise("connector ID, parent, environment, or authentication is invalid")

    registry_by_id = {
        resource.resource_id: resource for resource in scenario.resources if resource.resource_type == "schema_registry"
    }
    expected_registries = {
        registry_id: (environment_id, cloud, region)
        for registry_id, environment_id, _display_name, cloud, region in _SCHEMA_REGISTRIES
    }
    if set(registry_by_id) != set(expected_registries):
        _raise("Schema Registry topology mismatch")
    for registry_id, registry in registry_by_id.items():
        environment_id, cloud, region = expected_registries[registry_id]
        parsed_crn = parse_ccloud_crn(str(registry.metadata.get("crn", "")))
        if (
            registry.parent_id != environment_id
            or registry.metadata.get("cloud") != cloud
            or registry.metadata.get("region") != region
            or parsed_crn.get("organization") != organization.resource_id
            or parsed_crn.get("environment") != environment_id
            or parsed_crn.get("schema-registry") != registry_id
        ):
            _raise("Schema Registry placement or CRN is invalid")

    expected_ksql = {
        "lksql-commerce": ("env-commerce", "lkc-commerce", "sa-commerce"),
        "lksql-logistics": ("env-logistics", "lkc-logistics", "sa-logistics"),
        "lksql-analytics": ("env-analytics", "lkc-platform", "sa-platform"),
    }
    ksql_by_id = {
        resource.resource_id: resource for resource in scenario.resources if resource.resource_type == "ksqldb_cluster"
    }
    if set(ksql_by_id) != set(expected_ksql):
        _raise("ksqlDB topology mismatch")
    for resource_id, ksql in ksql_by_id.items():
        environment_id, cluster_id, owner_id = expected_ksql[resource_id]
        if (
            ksql.parent_id != environment_id
            or ksql.owner_id != owner_id
            or ksql.metadata.get("kafka_cluster_id") != cluster_id
            or set(ksql.metadata) != {"kafka_cluster_id", "csu_count"}
        ):
            _raise("ksqlDB environment, owner, or Kafka association is invalid")

    expected_pools = {
        "lfcp-logistics": ("env-logistics", "gcp", "us-central1"),
        "lfcp-analytics": ("env-analytics", "azure", "eastus2"),
    }
    pool_by_id = {
        resource.resource_id: resource
        for resource in scenario.resources
        if resource.resource_type == "flink_compute_pool"
    }
    if set(pool_by_id) != set(expected_pools):
        _raise("Flink compute-pool topology mismatch")
    for resource_id, pool in pool_by_id.items():
        environment_id, cloud, region = expected_pools[resource_id]
        parsed_crn = parse_ccloud_crn(str(pool.metadata.get("crn", "")))
        if (
            pool.parent_id != environment_id
            or pool.metadata.get("cloud") != cloud
            or pool.metadata.get("region") != region
            or pool.metadata.get("provider_cloud") != cloud.upper()
            or pool.metadata.get("provider_region") != region
            or parsed_crn.get("organization") != organization.resource_id
            or parsed_crn.get("environment") != environment_id
            or parsed_crn.get("flink-compute-pool") != resource_id
            or parsed_crn.get("cloud") != cloud
            or parsed_crn.get("region") != region
        ):
            _raise("Flink compute-pool placement is invalid")

    expected_statements = {
        "lfstmt-logistics-eta": ("env-logistics", "lfcp-logistics", "sa-logistics"),
        "lfstmt-logistics-routing": ("env-logistics", "lfcp-logistics", "sa-fulfillment"),
        "lfstmt-analytics-session": ("env-analytics", "lfcp-analytics", "sa-analytics"),
    }
    statement_by_id = {
        resource.resource_id: resource for resource in scenario.resources if resource.resource_type == "flink_statement"
    }
    if set(statement_by_id) != set(expected_statements):
        _raise("Flink statement topology mismatch")
    for resource_id, statement in statement_by_id.items():
        environment_id, pool_id, owner_id = expected_statements[resource_id]
        if (
            statement.parent_id != environment_id
            or statement.owner_id != owner_id
            or statement.metadata.get("compute_pool_id") != pool_id
            or statement.metadata.get("is_stopped") is not False
        ):
            _raise("Flink statement environment, pool, or owner relationship is invalid")

    identities_by_id: dict[str, CoreIdentity] = {}
    identity_counts: Counter[str] = Counter()
    for identity in scenario.identities:
        if identity.ecosystem != ECOSYSTEM or identity.tenant_id != scenario.tenant_id:
            _raise("identity owner does not match the Clean scenario")
        if identity.identity_id in identities_by_id:
            _raise("identity IDs must be unique")
        if any(
            value is not None and not _is_utc_second(value)
            for value in (identity.created_at, identity.deleted_at, identity.last_seen_at)
        ):
            _raise("identity timestamps must be UTC seconds")
        if any(
            any(secret_word in key.lower() for secret_word in ("secret", "token", "password"))
            for key in identity.metadata
        ):
            _raise("identity metadata must not contain secret-like fields")
        identities_by_id[identity.identity_id] = identity
        identity_counts[identity.identity_type] += 1
    if dict(identity_counts) != _IDENTITY_COUNTS:
        _raise(f"Clean identity topology mismatch: {dict(identity_counts)!r}")
    principal_types = {"service_account", "user", "principal", "identity_pool"}
    for resource in scenario.resources:
        if resource.owner_id is not None:
            owner = identities_by_id.get(resource.owner_id)
            if owner is None or owner.identity_type not in principal_types:
                _raise("resource owner reference is invalid")
        if resource.resource_type == "connector":
            auth_id = resource.metadata.get("kafka_service_account_id")
            auth = identities_by_id.get(str(auth_id))
            if auth is None or auth.identity_type != "service_account":
                _raise("connector service-account authentication reference is invalid")
    identity_provider_ids = {
        identity.identity_id for identity in scenario.identities if identity.identity_type == "identity_provider"
    }
    for identity_pool in (identity for identity in scenario.identities if identity.identity_type == "identity_pool"):
        if identity_pool.metadata.get("provider_id") not in identity_provider_ids:
            _raise("identity pool provider reference is invalid")
    for api_key in (identity for identity in scenario.identities if identity.identity_type == "api_key"):
        api_key_owner_id: object = api_key.metadata.get("owner_id")
        api_key_scope_id: object = api_key.metadata.get("resource_id")
        if (
            set(api_key.metadata) != {"owner_id", "resource_id"}
            or api_key_owner_id not in identities_by_id
            or identities_by_id[str(api_key_owner_id)].identity_type not in principal_types
            or api_key_scope_id not in resources_by_id
            or resources_by_id[str(api_key_scope_id)].resource_type
            not in {"kafka_cluster", "schema_registry", "connector", "ksqldb_cluster", "flink_compute_pool"}
        ):
            _raise("API-key owner or scope reference is invalid")

    tag_by_entity: dict[tuple[str, str], EntityTag] = {}
    for tag in scenario.entity_tags:
        entity_key = (tag.entity_type, tag.entity_id)
        if (
            tag.tag_id is not None
            or tag.created_at is not None
            or tag.tenant_id != scenario.tenant_id
            or tag.entity_type not in {"resource", "identity"}
            or (tag.entity_type == "resource" and tag.entity_id not in resources_by_id)
            or (tag.entity_type == "identity" and tag.entity_id not in identities_by_id)
            or tag.tag_key != "team"
            or tag.tag_value not in TEAM_VALUES
            or tag.created_by != "demo-generator"
            or entity_key in tag_by_entity
        ):
            _raise("entity-tag assignment is invalid")
        tag_by_entity[entity_key] = tag
    expected_tag_entities = {("resource", resource_id) for resource_id in resources_by_id} | {
        ("identity", identity_id) for identity_id in identities_by_id
    }
    if set(tag_by_entity) != expected_tag_entities or {tag.tag_value for tag in tag_by_entity.values()} != set(
        TEAM_VALUES
    ):
        _raise("every Clean resource and identity must have one complete team tag")

    expected_line_keys = {
        (env_id, resource_id, product_category, product_type)
        for env_id, resource_id, product_category, product_type, *_ in _line_specs()
    }
    billing_by_key: dict[tuple[datetime, str, str, str, str], CCloudBillingLineItem] = {}
    billing_by_date: defaultdict[date, list[CCloudBillingLineItem]] = defaultdict(list)
    billing_dates: Counter[date] = Counter()
    for line in scenario.billing_lines:
        key = (line.timestamp, line.env_id, line.resource_id, line.product_category, line.product_type)
        if (
            line.ecosystem != ECOSYSTEM
            or line.tenant_id != scenario.tenant_id
            or not _is_utc_second(line.timestamp)
            or line.timestamp.time() != datetime.min.time()
            or line.timestamp.date() not in expected_dates
            or line.env_id not in environment_ids
            or line.resource_id not in resources_by_id
            or _resource_environment(line.resource_id, resources_by_id) != line.env_id
            or (line.env_id, line.resource_id, line.product_category, line.product_type) not in expected_line_keys
            or line.product_category not in _BILLING_CATEGORIES
            or line.quantity <= 0
            or line.unit_price < 0
            or line.total_cost <= 0
            or line.total_cost != line.total_cost.quantize(_CENT)
            or line.total_cost != (line.quantity * line.unit_price).quantize(_CENT, rounding=ROUND_HALF_UP)
            or billing_by_key.get(key) is not None
        ):
            _raise("billing line has invalid owner, placement, lifetime, or price")
        billing_by_key[key] = line
        billing_by_date[line.timestamp.date()].append(line)
        billing_dates[line.timestamp.date()] += 1
    if tuple(billing_dates) != expected_dates or set(billing_dates.values()) != {len(_line_specs())}:
        _raise("Clean billing must contain the complete native line set for every generated day")
    if {line.product_category for line in scenario.billing_lines} != _BILLING_CATEGORIES:
        _raise("Clean billing must cover all approved native product categories")

    chargeback_totals: defaultdict[tuple[datetime, str, str, str, str], Decimal] = defaultdict(Decimal)
    for chargeback_row in scenario.chargebacks:
        env_id = str(chargeback_row.metadata.get("env_id", ""))
        chargeback_key = (
            chargeback_row.timestamp,
            env_id,
            chargeback_row.resource_id or "",
            chargeback_row.product_category,
            chargeback_row.product_type,
        )
        target_identity = identities_by_id.get(chargeback_row.identity_id)
        if (
            target_identity is None
            or target_identity.identity_type not in principal_types
            or ("identity", chargeback_row.identity_id) not in tag_by_entity
            or not _active_for(target_identity, chargeback_row.timestamp)
            or chargeback_row.ecosystem != ECOSYSTEM
            or chargeback_row.tenant_id != scenario.tenant_id
            or not _is_utc_second(chargeback_row.timestamp)
            or chargeback_row.timestamp.date() not in expected_dates
            or chargeback_row.resource_id not in resources_by_id
            or env_id not in environment_ids
            or _resource_environment(str(chargeback_row.resource_id), resources_by_id) != env_id
            or chargeback_row.amount <= 0
            or chargeback_row.amount != chargeback_row.amount.quantize(_CENT)
            or chargeback_row.allocation_detail not in {"usage_ratio_allocation", "even_split_allocation"}
            or chargeback_row.allocation_method not in {"usage_ratio", "even_split"}
            or chargeback_row.tags
            or chargeback_key not in billing_by_key
        ):
            _raise("chargeback target or allocation row is invalid")
        chargeback_totals[chargeback_key] += chargeback_row.amount
    if dict(chargeback_totals) != {key: line.total_cost for key, line in billing_by_key.items()}:
        _raise("billing lines must reconcile exactly to chargebacks")
    for line in scenario.billing_lines:
        _validate_billed_resource_lifetime(line.resource_id, line.timestamp, resources_by_id, identities_by_id)

    topic_totals: defaultdict[tuple[datetime, str, str, str, str], Decimal] = defaultdict(Decimal)
    expected_topic_keys = {
        (line.timestamp, line.env_id, line.resource_id, line.product_category, line.product_type)
        for line in scenario.billing_lines
        if line.product_category == "KAFKA"
    }
    for topic_row in scenario.topic_attributions:
        topic_key = (
            topic_row.timestamp,
            topic_row.env_id,
            topic_row.cluster_resource_id,
            topic_row.product_category,
            topic_row.product_type,
        )
        if (
            topic_row.ecosystem != ECOSYSTEM
            or topic_row.tenant_id != scenario.tenant_id
            or not _is_utc_second(topic_row.timestamp)
            or topic_row.env_id not in environment_ids
            or topic_row.cluster_resource_id not in cluster_by_id
            or topic_row.topic_name == "__UNATTRIBUTED__"
            or topic_row.topic_name not in topics_by_cluster[topic_row.cluster_resource_id]
            or topic_row.product_category != "KAFKA"
            or topic_key not in expected_topic_keys
            or topic_row.amount <= 0
        ):
            _raise("topic attribution row is invalid")
        topic_totals[topic_key] += topic_row.amount
    expected_topic_totals = {
        key: line.total_cost for key, line in billing_by_key.items() if line.product_category == "KAFKA"
    }
    if dict(topic_totals) != expected_topic_totals:
        _raise("Kafka billing must reconcile exactly to topic attributions")

    capture = scenario.preview_source_capture
    if (
        capture.ecosystem != ECOSYSTEM
        or capture.tenant_id != scenario.tenant_id
        or capture.refresh_start != _at_midnight(scenario.start_date)
        or capture.refresh_end != _at_midnight(scenario.anchor_date + timedelta(days=1))
        or len(capture.records) != len(scenario.billing_lines)
        or len({record.source_record_id for record in capture.records}) != len(capture.records)
    ):
        _raise("preview source capture identity or bounds are invalid")
    for record in capture.records:
        key = (
            record.billing_timestamp or datetime.min.replace(tzinfo=UTC),
            record.billing_env_id or "",
            record.billing_resource_id or "",
            record.billing_product_category or "",
            record.billing_product_type or "",
        )
        if (
            record.ecosystem != ECOSYSTEM
            or record.tenant_id != scenario.tenant_id
            or record.source_period_start is None
            or record.source_period_end != record.source_period_start + timedelta(days=1)
            or record.collection_window_start != capture.refresh_start
            or record.collection_window_end != capture.refresh_end
            or record.evidence_scope_start >= record.evidence_scope_end
            or record.allocation_timestamp != record.source_period_start
            or record.retention_timestamp != record.allocation_timestamp
            or record.provider_cost_id is None
            or record.product is None
            or record.line_type is None
            or record.amount is None
            or record.original_amount != record.amount
            or record.discount_amount != Decimal("0.00")
            or record.billing_timestamp != record.allocation_timestamp
            or key not in billing_by_key
            or record.billing_product_category != record.product
            or record.billing_product_type != record.line_type
            or record.resource_id != record.billing_resource_id
            or record.environment_id != record.billing_env_id
            or record.malformed
            or record.diagnostics
        ):
            _raise("preview source evidence does not match billing")

    pipeline_by_date: dict[date, PipelineState] = {}
    for state in scenario.pipeline_states:
        if (
            state.ecosystem != ECOSYSTEM
            or state.tenant_id != scenario.tenant_id
            or state.tracking_date not in expected_dates
            or state.tracking_date in pipeline_by_date
            or not state.billing_gathered
            or not state.resources_gathered
            or not state.has_usable_calculation
            or not state.topic_overlay_gathered
            or not state.topic_attribution_calculated
            or not _is_utc_second(state.calculation_completed_at)
        ):
            _raise("pipeline state is incomplete or outside the Clean date window")
        pipeline_by_date[state.tracking_date] = state
    if tuple(pipeline_by_date) != expected_dates:
        _raise("Clean pipeline state must contain one usable row per generated day")

    lineage_by_date = {run.tracking_date: run for run in scenario.allocation_lineage_runs}
    if tuple(lineage_by_date) != expected_dates or len(lineage_by_date) != len(scenario.allocation_lineage_runs):
        _raise("allocation lineage must contain one run per generated day")
    for tracking_date, run in lineage_by_date.items():
        expected_lines = billing_by_date[tracking_date]
        if (
            run.ecosystem != ECOSYSTEM
            or run.tenant_id != scenario.tenant_id
            or run.calculation_id != pipeline_by_date[tracking_date].calculation_id
            or len(run.captures) != len(expected_lines)
        ):
            _raise("allocation lineage run is incomplete")
        captures_by_key = {
            (
                capture.origin_timestamp,
                capture.origin_env_id,
                capture.origin_resource_id,
                capture.origin_product_type,
                capture.origin_product_category,
            ): capture
            for capture in run.captures
        }
        if len(captures_by_key) != len(run.captures):
            _raise("allocation lineage origins must be unique")
        for line in expected_lines:
            lineage = captures_by_key.get(
                (line.timestamp, line.env_id, line.resource_id, line.product_type, line.product_category)
            )
            if lineage is None or lineage.status.value != "complete" or not lineage.facts:
                _raise("allocation lineage capture is incomplete")
            if sum((fact.allocated_cost for fact in lineage.facts), Decimal("0")) != line.total_cost:
                _raise("allocation lineage does not reconcile to billing")


def _showcase_billing_key(line: CCloudBillingLineItem) -> tuple[datetime, str, str, str, str]:
    return (line.timestamp, line.env_id, line.resource_id, line.product_category, line.product_type)


def _showcase_chargeback_key(row: ChargebackRow) -> tuple[datetime, str, str, str, str]:
    return (
        row.timestamp,
        str(row.metadata.get("env_id", "")),
        row.resource_id or "",
        row.product_category,
        row.product_type,
    )


def _showcase_topic_key(row: TopicAttributionRow) -> tuple[datetime, str, str, str, str]:
    return (row.timestamp, row.env_id, row.cluster_resource_id, row.product_category, row.product_type)


def _validate_showcase_metrics(scenario: ShowcaseDemoScenario) -> None:
    """Validate complete source windows and distinct partition-efficiency conditions."""
    resources_by_id = {resource.resource_id: resource for resource in scenario.resources}
    topics = tuple(resource for resource in scenario.resources if resource.resource_type == "topic")
    topics_by_name = {str(topic.display_name): topic for topic in topics}
    topic_names = set(topics_by_name)
    final_start = scenario.anchor_date - timedelta(days=_SHOWCASE_VALIDATION_DAYS - 1)
    final_days = tuple(final_start + timedelta(days=offset) for offset in range(_SHOWCASE_VALIDATION_DAYS))
    scope_counts_by_date = _showcase_scope_counts_by_date(
        scenario.resources,
        scenario.identities,
        scenario.chargebacks,
        final_days,
    )
    topic_scopes = {
        topic_name: _showcase_scope_ids(topic, resources_by_id) for topic_name, topic in topics_by_name.items()
    }
    active_topic_names_by_date: dict[date, set[str]] = {}
    active_topic_names_by_environment: defaultdict[tuple[date, str], set[str]] = defaultdict(set)
    active_topic_names_by_cluster: defaultdict[tuple[date, str], set[str]] = defaultdict(set)
    for tracking_date in final_days:
        timestamp = _at_midnight(tracking_date)
        active_names: set[str] = set()
        for topic_name, topic in topics_by_name.items():
            if not _active_for(topic, timestamp):
                continue
            active_names.add(topic_name)
            cluster_id, environment_id = topic_scopes[topic_name]
            if environment_id is not None:
                active_topic_names_by_environment[(tracking_date, environment_id)].add(topic_name)
            if cluster_id is not None:
                active_topic_names_by_cluster[(tracking_date, cluster_id)].add(topic_name)
        active_topic_names_by_date[tracking_date] = active_names

    rows_by_topic_day: dict[tuple[str, date], dict[str, MetricRow]] = defaultdict(dict)
    actual_topic_names_by_date: defaultdict[date, set[str]] = defaultdict(set)
    actual_topic_names_by_environment: defaultdict[tuple[date, str], set[str]] = defaultdict(set)
    actual_topic_names_by_cluster: defaultdict[tuple[date, str], set[str]] = defaultdict(set)
    actual_tenant_topic_counts: defaultdict[date, set[int]] = defaultdict(set)
    actual_tenant_principal_counts: defaultdict[date, set[int]] = defaultdict(set)
    actual_environment_topic_counts: defaultdict[tuple[date, str], set[int]] = defaultdict(set)
    actual_environment_principal_counts: defaultdict[tuple[date, str], set[int]] = defaultdict(set)
    actual_cluster_topic_counts: defaultdict[tuple[date, str], set[int]] = defaultdict(set)
    actual_cluster_principal_counts: defaultdict[tuple[date, str], set[int]] = defaultdict(set)
    daily_totals: defaultdict[date, defaultdict[str, float]] = defaultdict(lambda: defaultdict(float))
    for row in scenario.source_metrics:
        if not math.isfinite(row.value) or row.value < 0:
            _raise("Showcase source metric value must be finite and nonnegative")
        matching_topics = topic_names & set(row.labels.values())
        if len(matching_topics) != 1:
            _raise("Showcase source metric must identify one topic")
        topic_name = matching_topics.pop()
        topic = topics_by_name[topic_name]
        cluster = resources_by_id.get(str(topic.parent_id))
        if cluster is None or cluster.resource_type != "kafka_cluster":
            _raise("Showcase source metric topic parent is invalid")
        tracking_date = row.timestamp.date()
        scope_counts = scope_counts_by_date.get(tracking_date)
        if scope_counts is None:
            _raise("Showcase source metric timestamp is outside the validation window")
        cluster_id, environment_id = topic_scopes[topic_name]
        if cluster_id is None or environment_id is None:
            _raise("Showcase source metric topic scope is invalid")
        if (
            row.labels.get("tenant_id") != scenario.tenant_id
            or row.labels.get("environment_id") != environment_id
            or row.labels.get("cluster_id") != cluster_id
            or row.labels.get("tenant_active_topic_count") != str(scope_counts.tenant_active_topic_count)
            or row.labels.get("tenant_active_principal_count") != str(scope_counts.tenant_active_principal_count)
            or row.labels.get("environment_active_topic_count")
            != str(scope_counts.environment_active_topic_counts.get(environment_id, 0))
            or row.labels.get("environment_active_principal_count")
            != str(scope_counts.environment_active_principal_counts.get(environment_id, 0))
            or row.labels.get("cluster_active_topic_count")
            != str(scope_counts.cluster_active_topic_counts.get(cluster_id, 0))
            or row.labels.get("cluster_active_principal_count")
            != str(scope_counts.cluster_active_principal_counts.get(cluster_id, 0))
            or not _is_utc_second(row.timestamp)
            or not _active_for(topic, row.timestamp)
            or row.metric_key not in _SHOWCASE_METRIC_KEYS
        ):
            _raise("Showcase source metric labels or lifetime are invalid")
        topic_day_key = (topic_name, tracking_date)
        if row.metric_key in rows_by_topic_day[topic_day_key]:
            _raise("Showcase source metric keys must be unique per topic and day")
        rows_by_topic_day[topic_day_key][row.metric_key] = row
        actual_topic_names_by_date[tracking_date].add(topic_name)
        actual_topic_names_by_environment[(tracking_date, environment_id)].add(topic_name)
        actual_topic_names_by_cluster[(tracking_date, cluster_id)].add(topic_name)
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
        daily_totals[tracking_date][row.metric_key] += row.value

    for topic_name in topic_names:
        for tracking_date in final_days:
            expected_keys = (
                set(_SHOWCASE_METRIC_KEYS) if topic_name in active_topic_names_by_date[tracking_date] else set()
            )
            actual_keys = set(rows_by_topic_day.get((topic_name, tracking_date), {}))
            if actual_keys != expected_keys:
                _raise("Showcase source metric windows are incomplete")

    for window_size in (30, 60, 90):
        window_days = set(final_days[-window_size:])
        totals: defaultdict[str, float] = defaultdict(float)
        for tracking_date in window_days:
            for metric_name, value in daily_totals[tracking_date].items():
                totals[metric_name] += value
        if any(totals[key] <= 0 for key in _SHOWCASE_METRIC_KEYS):
            _raise("Showcase source metric denominators must be positive")
        for tracking_date in window_days:
            scope_counts = scope_counts_by_date[tracking_date]
            if (
                actual_topic_names_by_date[tracking_date] != active_topic_names_by_date[tracking_date]
                or actual_tenant_topic_counts[tracking_date] != {scope_counts.tenant_active_topic_count}
                or actual_tenant_principal_counts[tracking_date] != {scope_counts.tenant_active_principal_count}
            ):
                _raise("Showcase source metric unit denominators do not match resource lifetimes")
            for environment_id, expected_topic_count in scope_counts.environment_active_topic_counts.items():
                if (
                    actual_topic_names_by_environment[(tracking_date, environment_id)]
                    != active_topic_names_by_environment[(tracking_date, environment_id)]
                    or actual_environment_topic_counts[(tracking_date, environment_id)] != {expected_topic_count}
                    or actual_environment_principal_counts[(tracking_date, environment_id)]
                    != {scope_counts.environment_active_principal_counts.get(environment_id, 0)}
                ):
                    _raise("Showcase environment unit denominators do not match resource lifetimes")
            for cluster_id, expected_topic_count in scope_counts.cluster_active_topic_counts.items():
                if (
                    actual_topic_names_by_cluster[(tracking_date, cluster_id)]
                    != active_topic_names_by_cluster[(tracking_date, cluster_id)]
                    or actual_cluster_topic_counts[(tracking_date, cluster_id)] != {expected_topic_count}
                    or actual_cluster_principal_counts[(tracking_date, cluster_id)]
                    != {scope_counts.cluster_active_principal_counts.get(cluster_id, 0)}
                ):
                    _raise("Showcase cluster unit denominators do not match resource lifetimes")

    final_seven = final_days[-7:]
    low_throughput = False
    idle = False
    healthy = False
    for topic in topics:
        rows_by_day = [rows_by_topic_day.get((str(topic.display_name), day), {}) for day in final_seven]
        if any(set(day_rows) != set(_SHOWCASE_METRIC_KEYS) for day_rows in rows_by_day):
            continue
        rates = [
            (day_rows["received_bytes"].value + day_rows["sent_bytes"].value)
            / 86_400
            / day_rows["partition_count"].value
            for day_rows in rows_by_day
        ]
        traffic = [day_rows["received_bytes"].value + day_rows["sent_bytes"].value for day_rows in rows_by_day]
        low_throughput |= all(value > 0 for value in traffic) and all(rate < 1_024 for rate in rates)
        idle |= all(value == 0 for value in traffic)
        healthy |= all(rate >= 1_024 for rate in rates)
    if not low_throughput or not idle or not healthy:
        _raise("Showcase source metrics must contain distinct partition-efficiency conditions")


def validate_showcase_demo_scenario(scenario: ShowcaseDemoScenario) -> None:
    """Validate the additive Showcase profile and all declared source conditions."""
    if not scenario.tenant_id.strip():
        _raise("scenario tenant_id must not be blank")
    clean = build_clean_demo_scenario(tenant_id=scenario.tenant_id, anchor_date=scenario.anchor_date)
    if (
        scenario.start_date != clean.start_date
        or scenario.organization_authority_id != clean.organization_authority_id
        or scenario.organization_authority_at != clean.organization_authority_at
        or scenario.pipeline_states != clean.pipeline_states
    ):
        _raise("Showcase baseline metadata does not match Clean")
    if (
        scenario.resources[: len(clean.resources)] != clean.resources
        or scenario.identities[: len(clean.identities)] != clean.identities
        or scenario.billing_lines[: len(clean.billing_lines)] != clean.billing_lines
        or scenario.chargebacks[: len(clean.chargebacks)] != clean.chargebacks
        or scenario.topic_attributions[: len(clean.topic_attributions)] != clean.topic_attributions
    ):
        _raise("Showcase must preserve the complete Clean baseline")

    expected_resources, expected_identities = _showcase_resources_and_identities(
        scenario.tenant_id,
        clean,
        _showcase_cutover(clean.start_date),
    )
    if scenario.resources != expected_resources or scenario.identities != expected_identities:
        _raise("Showcase lifecycle topology does not match the deterministic profile")
    expected_tags = _build_tags(scenario.tenant_id, expected_resources, expected_identities)
    if scenario.entity_tags != expected_tags:
        _raise("Showcase entity tags do not match the deterministic assignments")

    expected_added_billing, expected_added_chargebacks, expected_added_topics = _build_showcase_billing_and_allocations(
        scenario.tenant_id,
        clean,
        expected_resources,
        expected_identities,
    )
    if scenario.billing_lines[len(clean.billing_lines) :] != expected_added_billing:
        _raise("Showcase billing cohorts do not match the deterministic profile")
    if scenario.chargebacks[len(clean.chargebacks) :] != expected_added_chargebacks:
        _raise("Showcase chargeback cohorts do not match the deterministic profile")
    if scenario.topic_attributions[len(clean.topic_attributions) :] != expected_added_topics:
        _raise("Showcase topic cohorts do not match the deterministic profile")

    expected_capture = _build_preview_source_capture(
        scenario.tenant_id,
        clean.start_date,
        scenario.anchor_date,
        scenario.billing_lines,
        scenario.resources,
    )
    if scenario.preview_source_capture != expected_capture:
        _raise("Showcase preview source evidence does not match billing")
    expected_lineage = _build_lineage_runs(
        scenario.tenant_id,
        clean.start_date,
        scenario.anchor_date,
        scenario.billing_lines,
        scenario.chargebacks,
    )
    if scenario.allocation_lineage_runs != expected_lineage:
        _raise("Showcase allocation lineage does not match billing")

    resources_by_id = {resource.resource_id: resource for resource in scenario.resources}
    identities_by_id = {identity.identity_id: identity for identity in scenario.identities}
    billing_by_key = {_showcase_billing_key(line): line for line in scenario.billing_lines}
    if len(billing_by_key) != len(scenario.billing_lines):
        _raise("Showcase billing natural keys must be unique")
    chargeback_totals: defaultdict[tuple[datetime, str, str, str, str], Decimal] = defaultdict(Decimal)
    for row in scenario.chargebacks:
        identity = identities_by_id.get(row.identity_id)
        resource = resources_by_id.get(str(row.resource_id))
        key = _showcase_chargeback_key(row)
        if (
            identity is None
            or resource is None
            or not _active_for(identity, row.timestamp)
            or not _active_for(resource, row.timestamp)
            or key not in billing_by_key
            or row.amount <= 0
            or row.amount != row.amount.quantize(_CENT)
            or row.metadata.get("env_id") != _resource_environment(resource.resource_id, resources_by_id)
        ):
            _raise("Showcase chargeback target or allocation row is invalid")
        chargeback_totals[key] += row.amount
    if dict(chargeback_totals) != {key: line.total_cost for key, line in billing_by_key.items()}:
        _raise("Showcase billing lines must reconcile exactly to chargebacks")

    topic_by_key: defaultdict[tuple[datetime, str, str, str, str], Decimal] = defaultdict(Decimal)
    for topic_row in scenario.topic_attributions:
        topic = resources_by_id.get(f"{topic_row.cluster_resource_id}:topic:{topic_row.topic_name}")
        key = _showcase_topic_key(topic_row)
        if (
            topic is None
            or not _active_for(topic, topic_row.timestamp)
            or key not in billing_by_key
            or topic_row.amount <= 0
        ):
            _raise("Showcase topic attribution row is invalid")
        topic_by_key[key] += topic_row.amount
    expected_topic_totals = {
        key: line.total_cost for key, line in billing_by_key.items() if line.product_category == "KAFKA"
    }
    if dict(topic_by_key) != expected_topic_totals:
        _raise("Showcase Kafka billing must reconcile exactly to topic attribution")

    day_count = (scenario.anchor_date - scenario.start_date).days + 1
    added_billing = scenario.billing_lines[len(clean.billing_lines) :]
    if len(added_billing) != day_count * len(_CLUSTERS) * len(_SHOWCASE_LINE_TYPES):
        _raise("Showcase billing must add twelve rows for every generated day")
    added_dates = Counter(line.timestamp.date() for line in added_billing)
    if set(added_dates) != {scenario.start_date + timedelta(days=offset) for offset in range(day_count)} or set(
        added_dates.values()
    ) != {len(_CLUSTERS) * len(_SHOWCASE_LINE_TYPES)}:
        _raise("Showcase billing must cover every generated day")

    expected_metrics = _build_showcase_source_metrics(
        scenario.resources,
        scenario.identities,
        scenario.chargebacks,
        scenario.anchor_date,
    )
    if scenario.source_metrics != expected_metrics:
        _raise("Showcase source metrics do not match the deterministic profile")
    _validate_showcase_metrics(scenario)
