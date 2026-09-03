from __future__ import annotations

import calendar
from collections import defaultdict
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from decimal import ROUND_HALF_UP, Decimal
from typing import Any, NoReturn
from uuid import UUID

from core.models import ChargebackRow, CoreIdentity, CoreResource, CostType, EntityTag, PipelineState, ResourceStatus
from plugins.confluent_cloud.crn import parse_ccloud_crn
from plugins.confluent_cloud.models.billing import CCloudBillingLineItem, billing_natural_key

ECOSYSTEM = "confluent_cloud"
ORGANIZATION_ID = "11111111-1111-4111-8111-111111111111"
TEAM_VALUES: tuple[str, ...] = ("commerce", "logistics", "platform", "security")

_ENVIRONMENTS: tuple[tuple[str, str], ...] = (
    ("env-commerce", "Commerce"),
    ("env-logistics", "Logistics"),
)
_CLUSTERS: tuple[tuple[str, str, str, str, str], ...] = (
    ("lkc-commerce", "env-commerce", "Commerce Kafka", "aws", "us-east-1"),
    ("lkc-logistics", "env-logistics", "Logistics Kafka", "gcp", "us-central1"),
)
_TOPIC_NAMES: tuple[tuple[str, tuple[str, ...]], ...] = (
    (
        "lkc-commerce",
        (
            "orders.created.v1",
            "payments.authorized.v1",
            "customer.profile.v1",
        ),
    ),
    (
        "lkc-logistics",
        (
            "inventory.reserved.v1",
            "fulfillment.shipped.v1",
            "logistics.tracking.v1",
        ),
    ),
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
)
_SCHEMA_REGISTRIES: tuple[tuple[str, str, str, str, str], ...] = (
    ("lsrc-commerce", "env-commerce", "Commerce Schema Registry", "aws", "us-east-1"),
    ("lsrc-logistics", "env-logistics", "Logistics Schema Registry", "gcp", "us-central1"),
)
_ALLOCATABLE_IDENTITIES: tuple[str, ...] = (
    "sa-commerce",
    "sa-logistics",
    "sa-platform",
    "user-security",
)
_RESOURCE_COUNTS: dict[str, int] = {
    "organization": 1,
    "environment": 2,
    "kafka_cluster": 2,
    "topic": 6,
    "connector": 2,
    "schema_registry": 2,
    "ksqldb_cluster": 1,
    "flink_compute_pool": 1,
    "flink_statement": 1,
}
_IDENTITY_COUNTS: dict[str, int] = {
    "service_account": 3,
    "user": 2,
    "identity_provider": 1,
    "identity_pool": 1,
    "api_key": 4,
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


@dataclass(frozen=True)
class CleanDemoScenario:
    """Pure, deterministic logical state for the Clean Confluent demo."""

    tenant_id: str
    anchor_date: date
    start_date: date
    resources: tuple[CoreResource, ...]
    identities: tuple[CoreIdentity, ...]
    entity_tags: tuple[EntityTag, ...]
    billing_lines: tuple[CCloudBillingLineItem, ...]
    chargebacks: tuple[ChargebackRow, ...]
    pipeline_states: tuple[PipelineState, ...]


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
    resources: list[CoreResource] = [
        _resource(
            tenant_id=tenant_id,
            resource_id=ORGANIZATION_ID,
            resource_type="organization",
            display_name="Northstar Confluent Cloud",
            created_at=created_at,
        ),
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
        for topic_name in next(names for cid, names in _TOPIC_NAMES if cid == cluster_id):
            resources.append(
                _resource(
                    tenant_id=tenant_id,
                    resource_id=f"{cluster_id}:topic:{topic_name}",
                    resource_type="topic",
                    display_name=topic_name,
                    parent_id=cluster_id,
                    created_at=created_at,
                )
            )

    for connector_id, cluster_id, environment_id, display_name, connector_kind in _CONNECTORS:
        service_account_id = "sa-commerce" if environment_id == "env-commerce" else "sa-logistics"
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
                        else "com.northstar.connect.ShippingSinkConnector"
                    ),
                    "connector_kind": connector_kind,
                    "env_id": environment_id,
                    "kafka_auth_mode": "SERVICE_ACCOUNT",
                    "kafka_service_account_id": service_account_id,
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

    resources.append(
        _resource(
            tenant_id=tenant_id,
            resource_id="lksql-commerce",
            resource_type="ksqldb_cluster",
            display_name="Commerce Stream Apps",
            parent_id="env-commerce",
            owner_id="sa-commerce",
            created_at=created_at,
            metadata={"kafka_cluster_id": "lkc-commerce", "csu_count": 2},
        )
    )
    resources.append(
        _resource(
            tenant_id=tenant_id,
            resource_id="lfcp-logistics",
            resource_type="flink_compute_pool",
            display_name="Logistics Flink Pool",
            parent_id="env-logistics",
            created_at=created_at,
            metadata={
                "cloud": "gcp",
                "region": "us-central1",
                "provider_cloud": "GCP",
                "provider_region": "us-central1",
                "crn": _ccloud_crn(
                    ORGANIZATION_ID,
                    "env-logistics",
                    "flink-compute-pool",
                    "lfcp-logistics",
                    cloud="gcp",
                    region="us-central1",
                ),
            },
        )
    )
    resources.append(
        _resource(
            tenant_id=tenant_id,
            resource_id="lfstmt-logistics",
            resource_type="flink_statement",
            display_name="Shipment ETA Enrichment",
            parent_id="env-logistics",
            owner_id="sa-logistics",
            created_at=created_at,
            metadata={
                "statement_name": "Shipment ETA Enrichment",
                "compute_pool_id": "lfcp-logistics",
                "is_stopped": False,
            },
        )
    )
    return tuple(resources)


def _build_identities(tenant_id: str, created_at: datetime) -> tuple[CoreIdentity, ...]:
    identities = [
        _identity(
            tenant_id=tenant_id,
            identity_id="sa-commerce",
            identity_type="service_account",
            display_name="Commerce Workloads",
            created_at=created_at,
            metadata={"description": "Commerce application workloads"},
        ),
        _identity(
            tenant_id=tenant_id,
            identity_id="sa-logistics",
            identity_type="service_account",
            display_name="Logistics Workloads",
            created_at=created_at,
            metadata={"description": "Logistics application workloads"},
        ),
        _identity(
            tenant_id=tenant_id,
            identity_id="sa-platform",
            identity_type="service_account",
            display_name="Platform Operations",
            created_at=created_at,
            metadata={"description": "Platform operations workloads"},
        ),
        _identity(
            tenant_id=tenant_id,
            identity_id="user-alice",
            identity_type="user",
            display_name="Alice Northstar",
            created_at=created_at,
            metadata={"crn": "crn://confluent.cloud/user=user-alice"},
        ),
        _identity(
            tenant_id=tenant_id,
            identity_id="user-security",
            identity_type="user",
            display_name="Security Operations",
            created_at=created_at,
            metadata={"crn": "crn://confluent.cloud/user=user-security"},
        ),
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
            identity_id="pool-workload",
            identity_type="identity_pool",
            display_name="Northstar Workload Pool",
            created_at=created_at,
            metadata={"description": "Synthetic workload identity pool", "provider_id": "idp-corporate"},
        ),
    ]
    for api_key_id, owner_id, resource_id, display_name in (
        ("key-commerce", "sa-commerce", "lkc-commerce", "Commerce Kafka key"),
        ("key-logistics", "sa-logistics", "lkc-logistics", "Logistics Kafka key"),
        ("key-schema-commerce", "sa-platform", "lsrc-commerce", "Commerce Schema Registry key"),
        ("key-schema-logistics", "user-security", "lsrc-logistics", "Logistics Schema Registry key"),
    ):
        identities.append(
            _identity(
                tenant_id=tenant_id,
                identity_id=api_key_id,
                identity_type="api_key",
                display_name=display_name,
                created_at=created_at,
                metadata={"owner_id": owner_id, "resource_id": resource_id},
            )
        )
    return tuple(identities)


def _build_tags(tenant_id: str) -> tuple[EntityTag, ...]:
    assignments = (
        ("resource", "env-commerce", "commerce"),
        ("resource", "env-logistics", "logistics"),
        ("resource", "lkc-commerce", "platform"),
        ("resource", "clcc-logistics-shipping", "security"),
        ("identity", "sa-commerce", "commerce"),
        ("identity", "sa-logistics", "logistics"),
        ("identity", "sa-platform", "platform"),
        ("identity", "user-security", "security"),
    )
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
    return (
        (
            "env-commerce",
            "lkc-commerce",
            "KAFKA",
            "KAFKA_NUM_CKU",
            "usage",
            Decimal("2.0"),
            Decimal("50.00"),
            "usage",
        ),
        (
            "env-commerce",
            "lkc-commerce",
            "KAFKA",
            "KAFKA_STORAGE",
            "even",
            Decimal("10.0"),
            Decimal("100.00"),
            "even",
        ),
        (
            "env-commerce",
            "lcc-commerce-orders",
            "CONNECT",
            "CONNECT_CAPACITY",
            "even",
            Decimal("1.0"),
            Decimal("150.00"),
            "even",
        ),
        (
            "env-commerce",
            "lsrc-commerce",
            "SCHEMA_REGISTRY",
            "SCHEMA_REGISTRY",
            "even",
            Decimal("1.0"),
            Decimal("200.00"),
            "even",
        ),
        (
            "env-commerce",
            "lksql-commerce",
            "KSQL",
            "KSQL_NUM_CSU",
            "even",
            Decimal("2.0"),
            Decimal("250.00"),
            "even",
        ),
        (
            "env-logistics",
            "lkc-logistics",
            "KAFKA",
            "KAFKA_NUM_CKUS",
            "usage",
            Decimal("1.5"),
            Decimal("50.00"),
            "usage",
        ),
        (
            "env-logistics",
            "lkc-logistics",
            "KAFKA",
            "KAFKA_STORAGE",
            "even",
            Decimal("12.0"),
            Decimal("100.00"),
            "even",
        ),
        (
            "env-logistics",
            "clcc-logistics-shipping",
            "CONNECT",
            "CUSTOM_CONNECT_PLUGIN",
            "even",
            Decimal("1.0"),
            Decimal("150.00"),
            "even",
        ),
        (
            "env-logistics",
            "lsrc-logistics",
            "SCHEMA_REGISTRY",
            "SCHEMA_REGISTRY",
            "even",
            Decimal("1.0"),
            Decimal("200.00"),
            "even",
        ),
        (
            "env-logistics",
            "lfcp-logistics",
            "FLINK",
            "FLINK_NUM_CFU",
            "usage",
            Decimal("1.5"),
            Decimal("250.00"),
            "usage",
        ),
    )


def _build_billing_and_chargebacks(
    tenant_id: str,
    start_date: date,
    anchor_date: date,
) -> tuple[tuple[CCloudBillingLineItem, ...], tuple[ChargebackRow, ...]]:
    billing_lines: list[CCloudBillingLineItem] = []
    chargebacks: list[ChargebackRow] = []
    day_count = (anchor_date - start_date).days + 1
    for day_offset in range(day_count):
        tracking_date = start_date + timedelta(days=day_offset)
        timestamp = _at_midnight(tracking_date)
        seasonality = _SEASONALITY[day_offset % len(_SEASONALITY)]
        trend = Decimal("1") + Decimal(day_offset) * Decimal("0.0005")
        scale = seasonality * trend
        for (
            env_id,
            resource_id,
            product_category,
            product_type,
            allocation_kind,
            base_quantity,
            unit_price,
            _,
        ) in _line_specs():
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
            targets = ("sa-commerce", "sa-platform") if env_id == "env-commerce" else ("sa-logistics", "user-security")
            if allocation_kind == "usage":
                portions = (Decimal("0.60"), Decimal("0.40"))
                cost_type = CostType.USAGE
                allocation_method = "usage_ratio"
                allocation_detail = "usage_ratio_allocation"
            else:
                portions = (Decimal("0.50"), Decimal("0.50"))
                cost_type = CostType.SHARED
                allocation_method = "even_split"
                allocation_detail = "even_split_allocation"
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


def _build_pipeline_states(tenant_id: str, start_date: date, anchor_date: date) -> tuple[PipelineState, ...]:
    return tuple(
        PipelineState(
            ecosystem=ECOSYSTEM,
            tenant_id=tenant_id,
            tracking_date=start_date + timedelta(days=day_offset),
            billing_gathered=True,
            resources_gathered=True,
            chargeback_calculated=True,
            calculation_id=f"clean-calculation-{(start_date + timedelta(days=day_offset)).isoformat()}",
            calculation_completed_at=_at_midnight(start_date + timedelta(days=day_offset)) + timedelta(hours=1),
        )
        for day_offset in range((anchor_date - start_date).days + 1)
    )


def build_clean_demo_scenario(*, tenant_id: str, anchor_date: date) -> CleanDemoScenario:
    """Build the deterministic Clean Confluent scenario for ``anchor_date``."""
    if not tenant_id.strip():
        raise ValueError("tenant_id must not be blank")
    start_date = _window_start(anchor_date)
    created_at = _at_midnight(start_date - timedelta(days=30))
    resources = _build_resources(tenant_id, created_at)
    identities = _build_identities(tenant_id, created_at)
    billing_lines, chargebacks = _build_billing_and_chargebacks(tenant_id, start_date, anchor_date)
    scenario = CleanDemoScenario(
        tenant_id=tenant_id,
        anchor_date=anchor_date,
        start_date=start_date,
        resources=resources,
        identities=identities,
        entity_tags=_build_tags(tenant_id),
        billing_lines=billing_lines,
        chargebacks=chargebacks,
        pipeline_states=_build_pipeline_states(tenant_id, start_date, anchor_date),
    )
    validate_clean_demo_scenario(scenario)
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
    """Validate all Clean topology, identity, tag, lifecycle, and cost invariants."""
    if not scenario.tenant_id.strip():
        _raise("scenario tenant_id must not be blank")
    expected_start = _window_start(scenario.anchor_date)
    if scenario.start_date != expected_start:
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
        if resource.created_at is not None and not _is_utc_second(resource.created_at):
            _raise("resource created_at must be a UTC second")
        if resource.deleted_at is not None and not _is_utc_second(resource.deleted_at):
            _raise("resource deleted_at must be a UTC second")
        if resource.last_seen_at is not None and not _is_utc_second(resource.last_seen_at):
            _raise("resource last_seen_at must be a UTC second")
        resources_by_id[resource.resource_id] = resource

    resource_counts: dict[str, int] = defaultdict(int)
    for resource in scenario.resources:
        resource_counts[resource.resource_type] += 1
    if dict(resource_counts) != _RESOURCE_COUNTS:
        _raise(f"Clean resource topology mismatch: {dict(resource_counts)!r}")

    organizations = [r for r in scenario.resources if r.resource_type == "organization"]
    if len(organizations) != 1 or organizations[0].resource_id == scenario.tenant_id:
        _raise("Clean state requires one provider organization distinct from tenant_id")
    organization_id = organizations[0].resource_id
    try:
        UUID(organization_id)
    except ValueError, TypeError, AttributeError:
        _raise("provider organization ID must be a UUID")

    environments = [r for r in scenario.resources if r.resource_type == "environment"]
    environment_ids = {r.resource_id for r in environments}
    if environment_ids != {environment_id for environment_id, _ in _ENVIRONMENTS}:
        _raise("Clean environment topology mismatch")
    if any(environment.parent_id is not None for environment in environments):
        _raise("environments must not fabricate an organization parent")

    cluster_by_id = {r.resource_id: r for r in scenario.resources if r.resource_type == "kafka_cluster"}
    for cluster in cluster_by_id.values():
        if cluster.parent_id not in environment_ids:
            _raise("Kafka cluster must have an existing environment parent")
        if not str(cluster.metadata.get("cloud", "")).strip() or not str(cluster.metadata.get("region", "")).strip():
            _raise("Kafka cluster must have cloud and region placement")

    topics_by_cluster: dict[str, set[str]] = defaultdict(set)
    for topic in (r for r in scenario.resources if r.resource_type == "topic"):
        cluster_id = topic.parent_id
        if cluster_id is None or cluster_id not in cluster_by_id or not topic.display_name:
            _raise("topic must have a Kafka cluster parent and name")
        expected_id = f"{cluster_id}:topic:{topic.display_name}"
        if topic.resource_id != expected_id or topic.display_name in topics_by_cluster[cluster_id]:
            _raise("topic storage ID or cluster-local name is invalid")
        topics_by_cluster[cluster_id].add(topic.display_name)

    connector_by_id = {r.resource_id: r for r in scenario.resources if r.resource_type == "connector"}
    connector_kinds = {str(connector.metadata.get("connector_kind", "")) for connector in connector_by_id.values()}
    if connector_kinds != {"managed", "custom"}:
        _raise("Clean connectors must include one managed and one custom connector")
    for connector in connector_by_id.values():
        cluster_id = connector.parent_id
        connector_kind = connector.metadata.get("connector_kind")
        expected_prefix = {"managed": "lcc-", "custom": "clcc-"}.get(str(connector_kind))
        if (
            expected_prefix is None
            or not connector.resource_id.startswith(expected_prefix)
            or cluster_id is None
            or cluster_id not in cluster_by_id
        ):
            _raise("connector ID or Kafka parent is invalid")
        parent_cluster = cluster_by_id[cluster_id]
        if connector.metadata.get("env_id") != parent_cluster.parent_id:
            _raise("connector environment metadata must match its Kafka parent")
        auth_mode = connector.metadata.get("kafka_auth_mode")
        if auth_mode == "SERVICE_ACCOUNT":
            auth_id = connector.metadata.get("kafka_service_account_id")
            if not isinstance(auth_id, str) or auth_id not in {
                identity.identity_id for identity in scenario.identities if identity.identity_type == "service_account"
            }:
                _raise("connector service-account authentication reference is invalid")
        elif auth_mode == "KAFKA_API_KEY":
            auth_id = connector.metadata.get("kafka_api_key")
            if not isinstance(auth_id, str) or auth_id not in {
                identity.identity_id for identity in scenario.identities if identity.identity_type == "api_key"
            }:
                _raise("connector API-key authentication reference is invalid")
        else:
            _raise("connector authentication mode is invalid")

    sr_by_id = {r.resource_id: r for r in scenario.resources if r.resource_type == "schema_registry"}
    sr_by_env: dict[str, CoreResource] = {}
    for registry in sr_by_id.values():
        environment_id = registry.parent_id
        if environment_id is None or environment_id not in environment_ids or environment_id in sr_by_env:
            _raise("Schema Registry environment placement or cardinality is invalid")
        sr_by_env[environment_id] = registry
        crn = registry.metadata.get("crn")
        parsed_crn = parse_ccloud_crn(crn if isinstance(crn, str) else "")
        if (
            parsed_crn.get("organization") != organization_id
            or parsed_crn.get("environment") != environment_id
            or parsed_crn.get("schema-registry") != registry.resource_id
        ):
            _raise("Schema Registry CRN does not match its placement")
        cluster = next(cluster for cluster in cluster_by_id.values() if cluster.parent_id == environment_id)
        if registry.metadata.get("cloud") != cluster.metadata.get("cloud") or registry.metadata.get(
            "region"
        ) != cluster.metadata.get("region"):
            _raise("Schema Registry cloud and region do not match its environment")

    ksqldb = next(r for r in scenario.resources if r.resource_type == "ksqldb_cluster")
    identities_by_id = {identity.identity_id: identity for identity in scenario.identities}
    if (
        ksqldb.parent_id not in environment_ids
        or ksqldb.owner_id not in identities_by_id
        or identities_by_id[ksqldb.owner_id].identity_type
        not in {"service_account", "user", "principal", "identity_pool"}
        or "cloud" in ksqldb.metadata
        or "region" in ksqldb.metadata
    ):
        _raise("ksqlDB environment or owner reference is invalid")
    ksql_cluster_id = ksqldb.metadata.get("kafka_cluster_id")
    if ksql_cluster_id not in cluster_by_id or cluster_by_id[ksql_cluster_id].parent_id != ksqldb.parent_id:
        _raise("ksqlDB Kafka association must remain in the same environment")

    flink_pool = next(r for r in scenario.resources if r.resource_type == "flink_compute_pool")
    flink_pool_crn = parse_ccloud_crn(str(flink_pool.metadata.get("crn", "")))
    if (
        flink_pool.parent_id not in environment_ids
        or not str(flink_pool.metadata.get("cloud", "")).strip()
        or not str(flink_pool.metadata.get("region", "")).strip()
        or flink_pool_crn.get("organization") != organization_id
        or flink_pool_crn.get("environment") != flink_pool.parent_id
        or flink_pool_crn.get("flink-compute-pool") != flink_pool.resource_id
        or flink_pool_crn.get("cloud") != flink_pool.metadata.get("cloud")
        or flink_pool_crn.get("region") != flink_pool.metadata.get("region")
    ):
        _raise("Flink compute pool placement is invalid")
    flink_statement = next(r for r in scenario.resources if r.resource_type == "flink_statement")
    if (
        flink_statement.parent_id not in environment_ids
        or flink_statement.parent_id != flink_pool.parent_id
        or flink_statement.metadata.get("compute_pool_id") != flink_pool.resource_id
        or flink_statement.owner_id not in identities_by_id
        or identities_by_id[flink_statement.owner_id].identity_type
        not in {"service_account", "user", "principal", "identity_pool"}
    ):
        _raise("Flink statement environment, pool, or owner reference is invalid")

    identity_counts: dict[str, int] = defaultdict(int)
    for identity in scenario.identities:
        if identity.ecosystem != ECOSYSTEM or identity.tenant_id != scenario.tenant_id:
            _raise("identity owner does not match the Clean scenario")
        if identity.identity_id in identities_by_id and identities_by_id[identity.identity_id] is not identity:
            _raise("identity IDs must be unique")
        if identity.created_at is not None and not _is_utc_second(identity.created_at):
            _raise("identity created_at must be a UTC second")
        if identity.deleted_at is not None and not _is_utc_second(identity.deleted_at):
            _raise("identity deleted_at must be a UTC second")
        if identity.last_seen_at is not None and not _is_utc_second(identity.last_seen_at):
            _raise("identity last_seen_at must be a UTC second")
        identity_counts[identity.identity_type] += 1
        for key in identity.metadata:
            if any(secret_word in key.lower() for secret_word in ("secret", "token", "password")):
                _raise("identity metadata must not contain secret-like fields")
    if dict(identity_counts) != _IDENTITY_COUNTS:
        _raise(f"Clean identity topology mismatch: {dict(identity_counts)!r}")

    principal_types = {"service_account", "user", "principal", "identity_pool"}
    for resource in scenario.resources:
        if resource.owner_id is not None:
            owner = identities_by_id.get(resource.owner_id)
            if owner is None or owner.identity_type not in principal_types:
                _raise("resource owner reference is invalid")

    identity_provider_ids = {i.identity_id for i in scenario.identities if i.identity_type == "identity_provider"}
    for identity_pool in (i for i in scenario.identities if i.identity_type == "identity_pool"):
        if identity_pool.metadata.get("provider_id") not in identity_provider_ids:
            _raise("identity pool provider reference is invalid")
    for api_key in (i for i in scenario.identities if i.identity_type == "api_key"):
        owner_id = api_key.metadata.get("owner_id")
        scope_id = api_key.metadata.get("resource_id")
        if (
            owner_id not in identities_by_id
            or identities_by_id[owner_id].identity_type not in {"service_account", "user", "identity_pool"}
            or scope_id not in resources_by_id
            or resources_by_id[scope_id].resource_type not in {"kafka_cluster", "schema_registry"}
            or set(api_key.metadata) != {"owner_id", "resource_id"}
        ):
            _raise("API-key owner or scope reference is invalid")
        if isinstance(owner_id, str) and isinstance(scope_id, str):
            owner = identities_by_id[owner_id]
            scope = resources_by_id[scope_id]
            for generated_date in expected_dates:
                scoped_timestamp = _at_midnight(generated_date)
                if not _active_for(owner, scoped_timestamp) or not _active_for(scope, scoped_timestamp):
                    _raise("API-key owner or scope lifetime does not contain generated dates")

    tag_by_entity: dict[tuple[str, str], EntityTag] = {}
    for tag in scenario.entity_tags:
        tag_entity_key = (tag.entity_type, tag.entity_id)
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
            or tag_entity_key in tag_by_entity
        ):
            _raise("entity-tag assignment is invalid")
        tag_by_entity[tag_entity_key] = tag
    if {tag.tag_value for tag in scenario.entity_tags} != set(TEAM_VALUES):
        _raise("all Clean team values must be represented by entity tags")

    billing_by_key: dict[tuple[str, str, datetime, str, str, str, str], CCloudBillingLineItem] = {}
    billing_dates: dict[date, int] = defaultdict(int)
    for line in scenario.billing_lines:
        if (
            line.ecosystem != ECOSYSTEM
            or line.tenant_id != scenario.tenant_id
            or not _is_utc_second(line.timestamp)
            or line.timestamp.time() != datetime.min.time()
            or line.timestamp.date() not in expected_dates
            or line.env_id not in environment_ids
            or line.resource_id not in resources_by_id
            or _resource_environment(line.resource_id, resources_by_id) != line.env_id
            or line.quantity <= 0
            or line.unit_price < 0
            or line.total_cost <= 0
            or line.total_cost != line.total_cost.quantize(_CENT)
            or line.total_cost != (line.quantity * line.unit_price).quantize(_CENT, rounding=ROUND_HALF_UP)
        ):
            _raise("billing line has invalid owner, placement, lifetime, or price")
        billing_key = billing_natural_key(line)
        if billing_key in billing_by_key:
            _raise("billing natural keys must be unique")
        billing_by_key[billing_key] = line
        billing_dates[line.timestamp.date()] += 1
    if tuple(billing_dates) != expected_dates or set(billing_dates.values()) != {10}:
        _raise("Clean billing must contain ten lines for every generated day")
    if {line.product_category for line in scenario.billing_lines} != {
        "KAFKA",
        "CONNECT",
        "SCHEMA_REGISTRY",
        "KSQL",
        "FLINK",
    }:
        _raise("Clean billing must cover all approved product categories")

    chargeback_totals: defaultdict[tuple[str, str, datetime, str, str, str, str], Decimal] = defaultdict(Decimal)
    allocatable_types = {"service_account", "user", "principal", "identity_pool"}
    for row in scenario.chargebacks:
        chargeback_key = (
            row.ecosystem,
            row.tenant_id,
            row.timestamp,
            str(row.metadata.get("env_id", "")),
            row.resource_id or "",
            row.product_type,
            row.product_category,
        )
        target_identity = identities_by_id.get(row.identity_id)
        target_resource = resources_by_id.get(row.identity_id)
        target_key = ("identity", row.identity_id) if target_identity is not None else ("resource", row.identity_id)
        target = target_identity if target_identity is not None else target_resource
        if target is None:
            _raise("chargeback target or allocation row is invalid")
        if target_identity is not None and target_identity.identity_type not in allocatable_types:
            _raise("chargeback target or allocation row is invalid")
        if target_key not in tag_by_entity or not _active_for(target, row.timestamp):
            _raise("chargeback target or allocation row is invalid")
        if (
            row.ecosystem != ECOSYSTEM
            or row.tenant_id != scenario.tenant_id
            or not _is_utc_second(row.timestamp)
            or row.timestamp.date() not in expected_dates
            or row.resource_id not in resources_by_id
            or str(row.metadata.get("env_id", "")) not in environment_ids
            or _resource_environment(row.resource_id, resources_by_id) != row.metadata.get("env_id")
            or row.amount <= 0
            or row.amount != row.amount.quantize(_CENT)
            or row.allocation_detail not in {"usage_ratio_allocation", "even_split_allocation"}
            or row.allocation_method not in {"usage_ratio", "even_split"}
            or row.tags
        ):
            _raise("chargeback target or allocation row is invalid")
        billing_key = (
            row.ecosystem,
            row.tenant_id,
            row.timestamp,
            str(row.metadata["env_id"]),
            row.resource_id or "",
            row.product_type,
            row.product_category,
        )
        if chargeback_key not in billing_by_key:
            _raise("chargeback row does not reference a billing natural key")
        chargeback_totals[chargeback_key] += row.amount

    expected_billing_totals = {key: line.total_cost for key, line in billing_by_key.items()}
    if dict(chargeback_totals) != expected_billing_totals:
        _raise("billing lines must reconcile exactly to chargebacks")

    for line in scenario.billing_lines:
        _validate_billed_resource_lifetime(line.resource_id, line.timestamp, resources_by_id, identities_by_id)

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
            or not _is_utc_second(state.calculation_completed_at)
        ):
            _raise("pipeline state is incomplete or outside the Clean date window")
        pipeline_by_date[state.tracking_date] = state
    if tuple(pipeline_by_date) != expected_dates:
        _raise("Clean pipeline state must contain one usable row per generated day")
